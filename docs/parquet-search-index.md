# Parquet-Derived Statistics & Embedded Search Index — Design Proposal

**Tickets:** BDC-892, BDC-1334
**Status:** Proposal / pre-PoC
**Branch:** `feat/parquet-poc`
**Depends on:** [`variables-and-aggregations.md`](https://github.com/uc-cdis/pypfb/blob/feat/gen3metadata-poc/docs/variables-and-aggregations.md)
— that proposal lives on the `feat/gen3metadata-poc` branch and is not yet on `master`,
so the link above is absolute. It becomes `./variables-and-aggregations.md` once both
branches land.

---

## Summary

A consumer holding a PFB file today can answer *"what nodes and fields exist?"* from the
Metadata record, but cannot answer *"which records have `population = CEU`?"* without
decoding every data record. For a 29k-record export that is a full scan; for production
exports it is worse.

This proposal embeds **one Parquet file per node** into the PFB and uses **Parquet's own
index structures as the search index**. Scope is search *within a single PFB file*;
cross-file catalog discovery is explicitly out of scope.

The central design choice is to write no index machinery of our own. Parquet already
specifies and implements everything needed — row-group statistics, a page index with
per-page bounds and byte offsets, bloom filters, and dictionary encoding. We configure
those features at write time and read them back at query time. Our code is a type mapper,
a container, and a thin query surface.

| Artifact | Lifetime | Location |
| --- | --- | --- |
| Per-node Parquet files (data + native indexes) | **Permanent — this is the index** | Entity records in the Avro stream (§4) |
| Summary statistics | Permanent | `Metadata` record (extends existing `variables` / `aggregations`) |
| Index manifest | Permanent | Avro file-level header metadata |

**The cost of this approach is duplication**: the PFB carries both its Avro rows and a
columnar copy. §2.4 argues this may be cheaper than it sounds, and §7 makes it a measured
decision gate rather than an assumption.

---

## 1. What Parquet gives us natively

This section is the core of the proposal. Each row is a capability we would otherwise have
to build, specify, version, and test.

| Parquet feature | What it provides | Query it serves |
| --- | --- | --- |
| **Row-group statistics** | Per column chunk: `min`, `max`, `null_count` | Coverage counts; range pruning at row-group granularity |
| **Page index** (`ColumnIndex` + `OffsetIndex`) | Per *data page*: min/max bounds, null counts, **and the page's byte offset + length** | Fine-grained skipping — read only the pages that can contain a match |
| **Bloom filters** | Per column chunk, probabilistic membership | `submitter_id = X` on high-cardinality ID columns, no false negatives |
| **Dictionary encoding** | Per column chunk dictionary page | The value domain of low-cardinality fields, stored once rather than per row |
| **`sorting_columns` metadata** | Declares sort order | Lets a reader know binary search / ordered merge is valid |

### 1.1 Why Parquet's index is relocatable (and ours would not be)

Parquet's `OffsetIndex` stores byte offsets **relative to the Parquet file itself**. When
that file is embedded as an opaque payload inside an Avro record, every internal offset
stays valid regardless of where in the PFB the payload lands or how large the Avro header
is.

This is the decisive technical argument. An Avro-block-level index of our own design
cannot do this: the index must be written before the data, but Avro block offsets are only
known after writing, and adding the index *shifts every absolute offset*. Working around
that requires storing block ordinals instead of offsets, recovering offsets by a skip-scan
at open time, and guaranteeing that two serializations of the same records produce
byte-identical blocking. All of that machinery exists solely to reinvent what Parquet's
`OffsetIndex` already does correctly.

### 1.2 Write-time configuration (not defaults)

The features above are **not all on by default** and must be requested explicitly:

```python
pq.write_table(
    table,
    sink,
    compression="zstd",
    # Page index is OFF by default in pyarrow - this is the one that matters most.
    write_page_index=True,
    # Bloom filters for high-cardinality identifier columns.
    bloom_filter_options={
        "submitter_id": {"ndv": table.num_rows, "fpp": 0.01},
        "participant_id": {"ndv": table.num_rows, "fpp": 0.01},
    },
    # Smaller pages = finer pruning granularity, slightly larger index.
    data_page_size=64 * 1024,
    # Keep dictionary encoding on for low-cardinality columns (default, but explicit).
    use_dictionary=True,
)
```

Notes:

- `write_page_index=True` is the single most important line; with it off, pruning
  granularity drops from pages to whole row groups.
- `bloom_filter_options` is a `write_table` parameter; on `ParquetWriter` it is passed
  through `**options`. Passing `True` uses defaults `ndv=1048576`, `fpp=0.05` — too loose
  for our purposes, hence explicit values.
- Guidance is that NDV should be set to the row count for high-cardinality columns.
- `data_page_size` is the main pruning-granularity-vs-index-size dial, and a PoC knob.

### 1.3 Known gap: `distinct_count` is usually absent

Parquet's statistics struct has a `distinct_count` field, but most writers — pyarrow
included — leave it unset. **It cannot be relied on.** Where a distinct count or value
domain is needed (the cardinality guard in §3.1, and the `variables` list), it must be
computed with an Arrow compute call over the column rather than read from the footer.

Similarly, there is no clean public pyarrow API for reading a column chunk's dictionary
page directly, so "read the dictionary to get the value domain for free" does not work in
practice. A columnar `unique()` over the relevant columns is cheap and is what the PoC
should do.

### 1.4 Predicate pushdown works on an in-memory buffer

Important for feasibility: the embedded Parquet payload does **not** need to be written to
a temp file to be queried. pyarrow reads from a buffer, and filters are pushed down into
the statistics and page index:

```python
import pyarrow as pa, pyarrow.parquet as pq

pf = pq.ParquetFile(pa.BufferReader(blob))          # zero-copy over embedded bytes
table = pq.read_table(
    pa.BufferReader(blob),
    filters=[("population", "=", "CEU")],           # pruned via ColumnIndex
    columns=["submitter_id", "population"],
)
```

Row-group and page skipping both apply, so a selective predicate reads a small fraction
of the payload.

---

## 2. Verified constraints

Tested against the real code and sample data on this branch.

### 2.1 `fastavro` header metadata takes `str`, not `bytes`

`fastavro.writer` accepts `metadata=`, and `fastavro.reader(...).metadata` reads it back
(this is how `avro.codec` is surfaced). Payloads live in the Avro **header**, outside the
record stream, so existing consumers ignore them with no schema change — verified by
iterating all records of a file carrying a 1 MB header payload.

But raw `bytes` values are rejected:

```text
AttributeError: 'bytes' object has no attribute 'encode'
  File "fastavro/_write.pyx", line 481, in fastavro._write.write_header
```

Base64 roundtrips non-UTF-8 binary losslessly, at **+33% size**.

**Consequence — and this reverses an earlier recommendation.** Header metadata is ideal
for a small JSON manifest. It is the *wrong* home for multi-megabyte Parquet payloads: a
33% tax on the entire columnar copy, paid on a payload the Avro header also loads eagerly.
Parquet payloads therefore go in the **record stream** as Avro `bytes`, which is binary-
native and incurs no encoding tax (§4).

### 2.2 Current exports are written uncompressed

`export_2026-08-06T10_36_03.avro` is 10.2 MB for 29,021 records with `avro.codec = null`.
`PFBWriter.write()` never passes `codec=`, so every PFB this SDK writes is uncompressed.

### 2.3 Enum values are encoded in the Avro payload

`encode_enum` / `decode_enum` in [`src/pfb/base.py`](../src/pfb/base.py) escape
non-alphanumerics into `_<codepoint>_` form (`bpm > 60` → `bpm_32__62__32_60`).
`PFBReader.__next__` already decodes on read.

**Consequence:** staging from `PFBReader` yields decoded values, so Parquet columns hold
**decoded** values and `pfb search` takes human-readable terms. This must be part of the
documented contract — Parquet built from a raw `fastavro.reader` would silently contain
`_32_`-escaped values, and its min/max statistics would sort incorrectly.

### 2.4 The duplication cost needs measuring, not assuming

Given §2.2, the comparison is *compressed columnar* against *uncompressed row-oriented*.
Parquet + zstd + dictionary encoding on tabular clinical data frequently lands 5–10×
smaller than uncompressed Avro. It is plausible that per-node Parquet copies of this export
total well under its 10.2 MB of Avro.

This is PoC step 2 and gates the whole proposal (§7).

---

## 3. Index tiers

Tiers 0–1 already exist as `variables` / `aggregations` on `feat/gen3metadata-poc` and are
reused unchanged. Tiers 2–3 are what Parquet provides.

| Tier | Capability | Source |
| --- | --- | --- |
| **0** | Field catalog | Existing `variables` |
| **1** | Coverage counts | Existing `aggregations`, cross-checked against row-group `null_count` |
| **2** | Value domains, numeric/date min-max, membership tests | Arrow `unique()` (§1.3) + row-group stats + bloom filters |
| **3** | **Record retrieval by boolean** | Parquet page index — read only matching pages |

Tier 3 is the capability that was previously going to require a custom postings table. It
now comes from `write_page_index=True`.

### 3.1 Cardinality guard

Addresses §7 of the variables-and-aggregations proposal (RECOVER: 150k+ variables). Per
field, one strategy is chosen and recorded in the manifest:

| Distinct count | Strategy | Stored |
| --- | --- | --- |
| ≤ `--domain-cap` (default 256) | `domain` | Full sorted value list in the manifest |
| > cap, string | `bloom` | Parquet bloom filter only (inside the Parquet payload) |
| > cap, numeric/date | `range` | min / max only |
| absent from data | `none` | Nothing |

Note the asymmetry from the Parquet-native approach: for `bloom` and `range`, **nothing is
added to the PFB at all** — the filter and the statistics live inside the Parquet payload
that is already there. Only `domain` lists add bytes, and they are capped. Index size is
bounded by field count and the cap, never by record count.

---

## 4. Container format

### 4.1 Where the Parquet payloads live

Per §2.1, header metadata is out for large binary. Three candidates were considered:

| Option | Mechanism | Verdict |
| --- | --- | --- |
| Avro header metadata | base64 `str` in the file header | **No** — +33% on megabytes, loaded eagerly |
| New field on `Metadata` record | `map<string, bytes>` keyed by node | **No** — binary-native, but `PFBReader.__enter__` decodes Metadata eagerly, so every open loads every payload |
| **New member of the `object` union** | `record NodeParquet { string node; bytes payload; ... }` streamed as its own Entity | **Yes** — binary-native, lazy, skippable, one record per node |

Streaming one `NodeParquet` entity per node means a consumer that wants `demographic`
decodes one record and skips the rest; a consumer that wants none skips all of them. That
laziness is only available in the record stream.

```avroidl
record NodeParquet {
    string node;              // node name this payload covers
    bytes  payload;           // a complete, self-contained Parquet file
    long   num_rows;
    string compression;       // "zstd"
    boolean page_index;       // was write_page_index enabled
    array<string> bloom_columns;
}
```

### 4.2 Backward-compatibility cost — the real trade-off

This is the one place the Parquet-native design is *more* invasive than a header-metadata
index. Adding a union member means consumers that switch on `record["name"]` now encounter
an unexpected value. Code shaped like:

```python
for record in reader:
    handle[record["name"]](record)      # KeyError on "NodeParquet"
```

will break. Known consumers to audit: the Gen3 data explorer, Pelican's reimport path,
`pfb to tsv` / `pfb to gremlin` in this repo, and the AnVIL/Terra PFB importers.

Mitigations, in preference order:

1. **Default off.** `--embed-parquet` is opt-in; PFBs are byte-identical to today unless
   requested. The index manifest in header metadata advertises whether payloads are present.
2. **Emit payloads last**, after all data records, so a truncating or early-exiting
   consumer never reaches them.
3. **Reader-side skip.** `PFBReader.__next__` filters `NodeParquet` entities out of normal
   iteration and exposes them via a separate accessor, so every consumer using this SDK is
   unaffected without changing their code.

Mitigation 3 is what makes this tractable: in-SDK consumers see no change at all.

### 4.3 Manifest in header metadata

Small, plain JSON (no base64, no Parquet reader needed) so a consumer can discover index
presence and shape cheaply:

```json
{
  "version": 1,
  "built_at": "2026-10-06T14:22:03Z",
  "built_by": "pypfb 0.7.0",
  "enum_values": "decoded",
  "parquet": {
    "embedded": true,
    "compression": "zstd",
    "page_index": true,
    "data_page_size": 65536,
    "nodes": {
      "subject":     {"rows": 3202, "bytes": 184320, "bloom": ["submitter_id"]},
      "demographic": {"rows": 3202, "bytes": 141312, "bloom": ["submitter_id"]}
    }
  },
  "domains": {
    "demographic.population": ["ACB", "ASW", "CEU", "..."],
    "demographic.annotated_sex": ["female", "male"]
  },
  "ranges": {
    "subject.age_at_enrollment": {"min": 18, "max": 89}
  }
}
```

Domains and ranges are duplicated here deliberately: they are tiny, and putting them in
the manifest means *field-level* discovery needs no Parquet reader and no pyarrow — only
record retrieval (tier 3) does.

---

## 5. Required code changes

### 5.1 `pyproject.toml`

pyarrow stays optional, but note the shift: under the Parquet-native design, pyarrow is
needed to **build** an index *and* to do tier-3 retrieval. Tiers 0–2 still need nothing —
they are served by the JSON manifest.

```toml
[tool.poetry.dependencies]
pyarrow = { version = ">=15.0.0", optional = true }

[tool.poetry.extras]
parquet = ["pyarrow"]

[tool.poetry.plugins."pfb.plugins"]
# ... existing entries unchanged ...
"index" = "pfb.commands.index"
"search" = "pfb.commands.search"
"to_parquet" = "pfb.exporters.parquet"
```

Commands are auto-registered by the entry-point loop at the bottom of
[`src/pfb/cli.py`](../src/pfb/cli.py), so `cli.py` needs no change.

### 5.2 New package `src/pfb/index/`

Markedly smaller than the custom-index version of this design — there is no postings
builder and no block-assignment pass.

```text
src/pfb/index/
  __init__.py    # build_index(), load_index()
  arrow.py       # Avro schema -> Arrow schema mapping   <- the hard part
  staging.py     # records -> per-node Parquet, with index features enabled
  stats.py       # row-group stats + unique() -> variables / aggregations / domains
  query.py       # predicate -> pyarrow filters -> matching rows
```

**`arrow.py`** — the fiddliest piece and the most likely source of surprises. PFB node
schemas are Avro with heavy nullable-union and enum use:

```python
def avro_field_to_arrow(field):
    """Map a PFB node field type to an Arrow type.

    PFB specifics:
      - fields are typically union<null, X>  -> nullable Arrow type
      - enums become string; values are already decoded by PFBReader (base.py)
      - array<X> becomes list<X>
    """
    types = field["type"] if isinstance(field["type"], list) else [field["type"]]
    non_null = [t for t in types if t != "null"]
    if not non_null:
        return pa.null()
    if len(non_null) > 1:
        raise UnsupportedPFBType(f"{field['name']}: multi-branch union {non_null}")
    t = non_null[0]

    if isinstance(t, dict):
        if t["type"] == "enum":
            return pa.string()
        if t["type"] == "array":
            inner = t["items"]
            if isinstance(inner, dict) and inner["type"] == "enum":
                return pa.list_(pa.string())
            return pa.list_(_PRIMITIVES[inner])
        if t["type"] == "map":
            return pa.map_(pa.string(), pa.string())
    return _PRIMITIVES[t]


_PRIMITIVES = {
    "string": pa.string(), "long": pa.int64(), "int": pa.int32(),
    "double": pa.float64(), "float": pa.float32(),
    "boolean": pa.bool_(), "bytes": pa.binary(), "null": pa.null(),
}
```

The explicit raise on multi-branch unions matters: current Gen3 dictionaries appear not to
produce them, but silently taking `non_null[0]` would corrupt data. Fail loudly in the PoC
and find out.

**`staging.py`** — buckets records by node and flushes row groups so memory stays bounded.
This is where §1.2's index configuration is applied:

```python
class ParquetStager:
    """Bucket PFB records by node into one indexed Parquet file per node."""

    BLOOM_COLUMNS = ("submitter_id", "participant_id", "specimen_id")

    def __init__(self, schema, outdir, batch_size=10_000,
                 compression="zstd", data_page_size=64 * 1024):
        self._arrow_schemas = {n["name"]: node_to_arrow_schema(n) for n in schema}
        self._buffers = defaultdict(list)
        self._writers = {}
        self.row_counts = Counter()
        ...

    def _writer_for(self, node):
        if node not in self._writers:
            schema = self._arrow_schemas[node]
            bloom = {c: {"ndv": 1 << 20, "fpp": 0.01}
                     for c in self.BLOOM_COLUMNS if c in schema.names}
            self._writers[node] = pq.ParquetWriter(
                self._path(node), schema,
                compression=self._compression,
                write_page_index=True,            # off by default - see 1.2
                data_page_size=self._data_page_size,
                use_dictionary=True,
                bloom_filter_options=bloom or None,   # via **options
            )
        return self._writers[node]

    def add(self, record):
        row = dict(record["object"])
        row["__pfb_id"] = record["id"]
        row["__pfb_relations"] = [
            (r["dst_name"], r["dst_id"]) for r in record["relations"]
        ]
        ...
```

`__pfb_id` and `__pfb_relations` are carried so retrieval can map a Parquet hit back to
the Avro record, and so edge counts (`<node>-><node>` keys) derive without a second pass.

**`stats.py`** — footer statistics read no data pages:

```python
def field_stats(parquet_path):
    """Per-column stats from the Parquet footer. Reads no data pages."""
    md = pq.ParquetFile(parquet_path).metadata
    out = defaultdict(lambda: {"non_null": 0, "null_count": 0,
                               "min": None, "max": None})
    for rg in range(md.num_row_groups):
        for col in range(md.num_columns):
            cm = md.row_group(rg).column(col)
            s = out[cm.path_in_schema]
            if not cm.is_stats_set:
                continue
            st = cm.statistics
            s["null_count"] += st.null_count or 0
            s["non_null"] += cm.num_values - (st.null_count or 0)
            s["min"] = st.min if s["min"] is None else min(s["min"], st.min)
            s["max"] = st.max if s["max"] is None else max(s["max"], st.max)
            # NOTE: st.distinct_count is almost always None - see 1.3.
            # Domains are computed with pc.unique() over capped columns instead.
    return out
```

**`query.py`** — translates a CLI predicate into pyarrow filters and lets the page index do
the pruning (§1.4). No hand-written pruning logic.

### 5.3 `src/pfb/base.py`

Mirrors the accessor pattern `feat/gen3metadata-poc` established:

```python
 class PFBBase(object):
     def __init__(self, file_or_path):
         ...
         self._aggregations = None
+        self._index_manifest = None   # dict, from Avro header metadata
+        self._parquet = {}            # node -> bytes, lazily populated

+    @property
+    def index_manifest(self):
+        return self._index_manifest
+
+    def set_index_manifest(self, manifest):
+        self._index_manifest = manifest
```

### 5.4 `src/pfb/writer.py`

Two changes. First, `make_avro_schema` gains the union member from §4.1 — this is the
schema change the design costs us:

```python
             {
                 "name": "object",
                 "type": [
                     {"type": "record", "name": "Metadata", "fields": [...]},
+                    {
+                        "type": "record",
+                        "name": "NodeParquet",
+                        "fields": [
+                            {"name": "node", "type": "string"},
+                            {"name": "payload", "type": "bytes"},
+                            {"name": "num_rows", "type": "long"},
+                            {"name": "compression", "type": "string"},
+                            {"name": "page_index", "type": "boolean"},
+                            {"name": "bloom_columns",
+                             "type": {"type": "array", "items": "string"}},
+                        ],
+                    },
                 ] + encoded_schema,
             },
```

Second, `write()` emits payloads last (§4.2, mitigation 2) and passes the manifest as
header metadata:

```python
     def write(self, iterable=None, metadata=True):
         def _iter():
             if metadata:
                 yield avro_record(None, "Metadata", metadata_payload, [])
             if iterable is not None:
                 for record in iterable:
                     ...
                     yield record
+            # Parquet payloads last, so early-exiting consumers never see them.
+            for node, blob in (self._parquet or {}).items():
+                yield avro_record(None, "NodeParquet", {...}, [])

-        writer(self._file_obj, make_avro_schema(self.schema), _iter())
+        writer(
+            self._file_obj,
+            make_avro_schema(self.schema),
+            _iter(),
+            codec=self._codec,
+            metadata={"pfb.index.v1.manifest": json.dumps(self._index_manifest)}
+                     if self._index_manifest else None,
+        )
```

Header metadata values must be `str` per §2.1; the manifest is JSON text, so no base64 is
needed anywhere in this design.

### 5.5 `src/pfb/reader.py`

Manifest is parsed eagerly (it is small); Parquet payloads are never materialized unless
asked for, and are filtered out of normal iteration per §4.2 mitigation 3:

```python
     def __enter__(self):
         ...
         self.set_aggregations(metadata_record.get("aggregations") ...)
+        raw = self._reader.metadata.get("pfb.index.v1.manifest")
+        self.set_index_manifest(json.loads(raw) if raw else None)
         return rv

     def __next__(self):
         rv = next(self._reader)
+        # Transparently skip embedded Parquet so existing consumers are unaffected.
+        while rv["name"] == "NodeParquet":
+            self._parquet[rv["object"]["node"]] = rv["object"]["payload"]
+            rv = next(self._reader)
         ...

+    def parquet(self, node):
+        """Return the embedded Parquet payload for `node` as a pyarrow ParquetFile."""
+        import pyarrow as pa, pyarrow.parquet as pq
+        return pq.ParquetFile(pa.BufferReader(self._parquet[node]))
```

The skip loop has a wrinkle worth calling out: because payloads are emitted *last*,
`self._parquet` only populates once iteration reaches the end. A consumer calling
`.parquet()` without having iterated gets nothing. Options are an explicit
`load_parquet()` that seeks to the tail, or emitting payloads immediately after Metadata
and accepting that a truncating consumer hits them early. **This is an open question (§8)**
— the two mitigations in §4.2 are in tension.

### 5.6 `src/pfb/commands/index.py`

```python
@index.command("build")
@click.option("-i", "--input", "input_file", type=click.File("rb"), default="-")
@click.option("-o", "--output", type=click.File("wb"), required=True)
@click.option("--embed-parquet/--no-embed-parquet", default=False, show_default=True,
              help="Embed per-node Parquet payloads (enables record retrieval).")
@click.option("--domain-cap", default=256, show_default=True,
              help="Max distinct values before a field falls back to bloom/range.")
@click.option("--data-page-size", default=64 * 1024, show_default=True,
              help="Parquet page size: pruning granularity vs index size.")
def build(input_file, output, embed_parquet, domain_cap, data_page_size):
    """Read a PFB, derive statistics and an index, write an indexed PFB."""
```

Taking an existing PFB as input is the primary path, not creation-time indexing — see §6.

### 5.7 `src/pfb/commands/search.py`

```python
@main.command(short_help="Search records within a PFB file.")
@click.option("-i", "--input", "input_file", type=click.File("rb"), default="-")
@click.option("-w", "--where", multiple=True, metavar="NODE.FIELD[=|>|<]VALUE",
              help="Predicate; repeatable (AND).")
@click.option("--explain", is_flag=True,
              help="Report row groups and pages pruned instead of records.")
def search(input_file, where, explain):
    """Find records matching a predicate.

    Uses embedded Parquet and its page index when present; falls back to a full
    Avro scan with a warning when absent.
    """
```

`--explain` reports **pages read vs pages skipped**, straight from the page index. That is
the number that decides whether `--embed-parquet` earns its bytes.

### 5.8 `src/pfb/exporters/parquet.py` — `pfb to parquet`

Exposes the staging layer standalone. Useful on its own (analysts want the Parquet) and
makes the type mapping testable without the container work.

### 5.9 `src/pfb/commands/show.py`

```python
@show.command(short_help="Show the search index manifest.")
@click.option("--node", help="Show Parquet metadata for one node instead.")
def index(ctx, node):
    """Show the index manifest, or one node's Parquet schema and row-group stats."""
```

Named `index` to avoid colliding with the existing `pfb show stats`.

### 5.10 Tests

| Test | Asserts |
| --- | --- |
| `test_parquet_roundtrip` | PFB → Parquet → values identical, for every Avro type in the sample dictionary |
| `test_index_backward_compat` | **A master-version `PFBReader` reads an indexed PFB and yields identical data records** |
| `test_reader_skips_parquet` | `NodeParquet` entities never surface in normal iteration (§4.2) |
| `test_page_index_written` | `write_page_index=True` actually produced a `ColumnIndex` — guards against the §1.2 default |
| `test_search_equals_scan` | `pfb search` results == brute-force filter, for every indexed field |
| `test_enum_values_decoded` | Parquet columns hold decoded values, not `_32_` escapes (§2.3) |
| `test_cardinality_guard` | A 10k-distinct field falls back to `bloom`, not `domain` |
| `test_multi_branch_union_raises` | `UnsupportedPFBType`, not silent data loss |
| `test_no_index_fallback` | `pfb search` on an unindexed PFB warns and still returns correct results |

`test_index_backward_compat`, `test_search_equals_scan`, and `test_page_index_written` are
the three that must not be skipped — they guard the compatibility claim, the correctness
claim, and the central performance assumption.

---

## 6. Interaction with Pelican

Per §4 of the variables-and-aggregations proposal, production PFBs are created by Pelican,
which already obtains counts from Guppy and needs to *pass them in* rather than have pypfb
recompute them.

- **Counts** (tiers 0–1): Pelican supplies; pypfb accepts. Already the plan.
- **Parquet payloads and their indexes** (tiers 2–3): Guppy cannot supply these, and they
  must be built where the columnar copy is built.

Unlike the custom-index design, nothing here depends on the *Avro* file's block layout, so
indexing is cleanly separable from writing. `pfb index build -i existing.pfb -o indexed.pfb`
is the integration path: Pelican writes PFBs exactly as it does today, and an indexing pass
runs afterward. Pelican needs no changes for v1.

---

## 7. Proof of concept

Scripts on `feat/parquet-poc`, no library surgery, against
`export_2026-08-06T10_36_03.avro` (29,021 records, 14 populated nodes, 10.2 MB,
uncompressed).

| Step | Deliverable | Decides |
| --- | --- | --- |
| 1 | `pfb → parquet` per node via `PFBReader` | Whether the Avro→Arrow mapping has gaps; whether multi-branch unions occur in practice |
| 2 | Size table: Parquet zstd vs snappy vs the 10.2 MB Avro | **Whether `--embed-parquet` is viable at all** (§2.4) |
| 3 | Footer stats vs `aggregations` from the other branch | That Parquet-derived counts agree with the hand-rolled ones; confirms §1.3 on `distinct_count` |
| 4 | Manifest with capped domains + ranges; measure as % of PFB | Tier-2 cost. Target: **< 1% of PFB size** |
| 5 | 5 representative queries with `--explain` at 3 `data_page_size` values | Pages pruned; the right page-size default; whether tier 3 earns its bytes |
| 6 | Read the indexed PFB with **master's** `PFBReader` | Backward compatibility (§4.2) — non-negotiable |

### Decision gates

- **Parquet total > Avro size** → `--embed-parquet` stays off by default, documented as a
  small-export convenience; tiers 0–2 ship regardless.
- **Pruning < 50% of pages on selective queries** → page-level retrieval is not paying for
  itself on unsorted exports; revisit with `sorting_columns` and record clustering first.
- **Manifest > 5% of PFB size** → tighten `--domain-cap` before anything else.
- **Any consumer in §4.2 breaks on the new union member** → fall back to shipping tiers
  0–2 only, with the manifest in header metadata and no schema change at all.

That last gate is the useful property of this plan: tiers 0–2 require **no Avro schema
change** and carry essentially no size or compatibility risk, so they can ship even if
embedding is rejected outright.

---

## 8. Open questions

| Question | Options |
| --- | --- |
| Payloads first (after Metadata) or last? | §4.2 wants last for truncating consumers; §5.5 wants early so `.parquet()` works without full iteration. A `load_parquet()` that seeks the tail resolves it, at the cost of needing a seekable input (stdin is not) |
| Sort rows before writing Parquet? | `sorting_columns` plus clustering would sharply improve page pruning, but changes record order on round-trip — likely unacceptable for a format whose point is fidelity |
| One Parquet file per node, or one per node per row group budget? | Very large nodes may warrant splitting for partial fetch |
| Should the Parquet copy become authoritative, with Avro rows dropped? | A much larger format change (PFBv2); worth noting as the logical endpoint but out of scope |
| Index multi-valued (array) fields? | Natural for `subject.consent_codes`; Parquet handles `list<string>` and can prune on it, but pyarrow filter support for list membership is limited |
| Index `relations` for graph traversal ("samples of subject X")? | High value for within-file navigation, and `__pfb_relations` is already staged |
| Index staleness after `pfb add` / `pfb rename`? | **Real correctness hazard** — both mutate records through `PFBWriter` hooks and would carry a stale Parquet copy forward. Minimum answer: drop the index and warn unless `--reindex` is passed. A content hash in the manifest would let consumers detect it |
