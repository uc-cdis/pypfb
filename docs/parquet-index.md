# Parquet Search Index — Implementation Notes

**Branch:** `feat/parquet-index`
**Implements:** [`parquet-search-index.md`](./parquet-search-index.md) §5, with the
corrections from [`parquet-poc-results.md`](./parquet-poc-results.md)
**Adds:** `pfb index build`, `pfb index show`, `pfb search`, `pfb to parquet`
**Requires:** `pip install pypfb[parquet]` (pyarrow), for indexing only — reading an
indexed PFB needs nothing new

---

## Basic example

```bash
# Tiers 0-2: statistics only.  No Avro schema change, +0.3%, and the manifest
# is readable with fastavro alone -- no pyarrow needed to consume it.
pfb index build -i export.avro -o indexed.pfb
# indexed 14 nodes, 293 fields
# tiers 0-2 only; pass --embed-parquet for record retrieval

pfb index show -i indexed.pfb
pfb index show -i indexed.pfb --node demographic

# Tier 3: embed per-node Parquet, enabling record retrieval.  +26.8%, and a
# breaking schema change for consumers outside this SDK.
pfb index build -i export.avro -o embedded.pfb --embed-parquet
# embedded 2,694,279 bytes of Parquet across 14 payloads

pfb search -i embedded.pfb -w 'demographic.population=CEU' --count
# 179

pfb search -i embedded.pfb -w 'demographic.population=CEU' --explain

# On a tiers 0-2 file the manifest can still rule a query out with no payload
# and no record read at all.
pfb search -i indexed.pfb -w 'demographic.population=NOPE' --count
# 0

# The staging layer on its own, no PFB involved.
pfb to -i export.avro parquet ./parquet-out

# An embedded file is still readable by existing consumers.
pfb show -i embedded.pfb nodes | wc -l   # 40, same as the input
pfb to -i embedded.pfb tsv ./out         # exit 0, all 14 files
```

From Python:

```python
from pfb.manifest import answerable_from_manifest, parse_predicate
from pfb.reader import PFBReader
from pfb.parquet import search_payload

with PFBReader("embedded.pfb") as reader:
    manifest = reader.index_manifest
    predicate = parse_predicate("demographic.population=CEU")

    # Tier 2: can the manifest alone prove there is no match?
    if answerable_from_manifest(manifest, predicate) is False:
        rows = []
    else:
        # Tier 3: fetch just the one Avro block holding the payload.
        rows = search_payload(reader.parquet("demographic"), [predicate]).rows

print(len(rows))
```

Building an index programmatically:

```python
from pfb.parquet import build_index, write_index

result = build_index("export.avro")
with open("embedded.pfb", "wb") as out:
    manifest = write_index(out, result, embed=True)
```

---

## What this is

Four tiers of capability over a PFB, each a superset of the last:

| Tier | Capability | Source |
| --- | --- | --- |
| 0 | Field catalog | manifest |
| 1 | Coverage counts | Parquet footer |
| 2 | Value domains, min/max, membership tests | `pc.unique()` + footer stats |
| 3 | **Record retrieval by predicate** | embedded per-node Parquet |

Tiers 0–2 are the default. They put a JSON manifest in the Avro file header and change
nothing else — no schema change, no new records, nothing a consumer can trip over.

Tier 3 (`--embed-parquet`) additionally streams one `NodeParquet` entity per node
carrying a complete Parquet file. That **is** a breaking schema change for consumers
outside this SDK, and no amount of care on our side fixes that. The section on
compatibility below is specific about who breaks and how.

Measured on `export_2026-08-06T10_36_03.avro` (10.17 MB, 29,021 records, 14 populated
nodes):

| | Size | vs original |
| --- | ---: | ---: |
| Original | 10,173,528 | — |
| Tiers 0–2 | 10,208,174 | +0.3% |
| Tier 3 (embedded) | 12,857,984 | +26.4% |

| Query path | Time |
| --- | ---: |
| Full Avro scan | 331 ms |
| Payload extract (seek to block) | 14 ms |
| **Indexed retrieval, end to end** | **17 ms** |
| Indexed retrieval via full iteration (what the proposal implied) | 842 ms |

---

## The three defects the PoC found, and what was done about each

### 1. Multi-branch unions — the type mapper aborted on real data

The proposal's `avro_field_to_arrow` raised `UnsupportedPFBType` on any union with more
than one non-null branch, on the belief that current Gen3 dictionaries don't produce
them. They do: **every `state` field is `union<enum, enum, null>`**, and there are 36 of
them in the sample dictionary in `tests/pfb-data/test.avro` alone. The proposed code
aborts on the first node of any real export.

Taking `non_null[0]` instead would silently corrupt a genuinely heterogeneous union, so
`src/pfb/index/arrow.py` does neither. It **maps every branch and compares the results**:

```python
mapped = [_map_branch(t) for t in non_null]
first = mapped[0]
for other in mapped[1:]:
    if other != first:
        raise UnsupportedPFBType(...)
return first
```

`union<enum, enum, null>` → both branches map to `pa.string()` → collapses cleanly.
`union<string, long, null>` → `string` vs `int64` → still raises, with both the Avro
branch names and the Arrow types in the message.

`test_every_node_in_the_sample_dictionary_maps` runs the whole 40-node dictionary
through the mapper, which is the regression test the proposal needed and didn't have.

### 2. The reader invented a 41st node

The PoC predicted `pfb to tsv` would fail with `KeyError` from a `record["name"]`
dispatch. It failed with `StopIteration` from somewhere else entirely, and the reason is
in `PFBReader.__enter__`:

```python
it = iter(f["type"])
next(it)          # "skip metadata"
for node in it:   # everything else is assumed to be a node
```

Skipping exactly **one** union member means `NodeParquet` lands in `reader.schema` as if
it were a node. `fields_by_name[name]` then *succeeds*, and the failure surfaces one line
later at a bare `next()` over `reader.metadata["nodes"]`, where `NodeParquet` is absent.
The observable damage was wider than the exporter: `pfb show nodes` listed `NodeParquet`,
`pfb show schema` included it, and the node count came out 41 instead of 40.

`src/pfb/reader.py` now filters by name against an explicit `NON_NODE_MEMBERS` set, so no
future union member can produce a phantom node. `__next__` additionally skips
`NodeParquet` records so iteration yields exactly what it did before.

Verified on the real export with payloads embedded: `pfb show nodes` → 40,
`pfb to tsv` → **exit 0, all 14 files**, and records identical through `PFBReader`
(29,021 before and after).

### 3. The payload placement conflict — payloads last *and* fast

This was the one the PoC called a prerequisite rather than an open question, and it is
the most substantial change from the proposal.

The proposal offered two placements and measured both:

| | Retrieval | `pfb to tsv` |
| --- | ---: | --- |
| Payloads last (§4.2 mitigation 2) | 514 ms — slower than the 504 ms scan | 14 files, then exit 1 |
| Payloads first (§5.5) | 10 ms | **0 files**, exit 1 |

Last is useless because reaching the payload means decoding every record first. First is
destructive. The PoC concluded that resolving this needs a way to read payloads
**without iterating the record stream**, and noted the obstacle: the manifest is written
in the Avro header, *before* the data, so it cannot carry byte offsets that are only
known afterwards.

The fix in `src/pfb/index/container.py` is to record **block ordinals instead of byte
offsets**. An Avro file is a sequence of blocks, each prefixed with a record count and a
byte length, so `fastavro.block_reader` can skip a block without decoding it. The
manifest stores `{node: block_ordinal}`, and `PFBReader.parquet(node)` walks block
headers (a few ms) and decodes exactly one block.

Ordinals dissolve the circularity — they don't depend on the header's size, only on the
block layout. Knowing them still requires having written the blocks, so the file is built
in **two passes**: write once to a staging buffer, read back which block each payload
landed in, then re-emit with the ordinals in the header. The second pass copies blocks
**byte for byte**, which is both why it's cheap and why the ordinals stay valid —
re-encoding could move block boundaries and silently invalidate every one of them.

The result is that payloads stay **last**, where they don't disturb consumers, and
retrieval still costs 17 ms rather than 842 ms. The two requirements the PoC showed were
in tension are both met.

---

## Where the code lives

No sub-package. Two flat modules sit alongside `reader.py` and `writer.py`, and
everything else goes into the file that already owns that responsibility.

The split is driven by one constraint worth stating plainly: **`cli.py` loads
every `pfb.plugins` entry point on every invocation, with no `try`/`except`.**
A module-scope `import pyarrow` in any `commands/` or `exporters/` module would
therefore break `pfb --help` outright on an install without the extra, and add
~0.7 s to every command on one with it. So pyarrow lives in exactly one module,
below the plugins and above `reader`/`writer`, imported lazily by its callers.

```text
base.py        unchanged except for two accessors
manifest.py    NEW - manifest format, predicates, tier-2 lookups   (stdlib only)
reader.py      manifest parsing, union filtering, payload fetch     (no pyarrow)
writer.py      NodeParquet emission, the two-pass indexed write     (no pyarrow)
parquet.py     NEW - Arrow mapping, staging, stats, build, query    (pyarrow)
commands/, exporters/   thin CLI; `from ..parquet import ...` inside callbacks
```

### `src/pfb/manifest.py` — the format, and what it can answer alone

Stdlib only, because this is the part a consumer can use with nothing but
`fastavro`.

The manifest is JSON under the header key `pfb.index.v1.manifest`. fastavro
rejects `bytes` header values, so a manifest over 16 KB is deflated and
base64-encoded into a separate key (`...manifest.z`); exactly one is ever
written and `load_manifest` accepts either. This matters here — the real
export's manifest is 171 KB of JSON.

`load_manifest` returns `None`, never raises, for missing, corrupt, or
future-versioned manifests, so a v1 reader meeting a v2 file degrades to a scan
rather than refusing to open it.

`NODE_PARQUET` and `NODE_PARQUET_SCHEMA` live here too. That **removes a real
duplication**: the same Avro literal previously existed in both the container
module and `writer.py`. Putting it here lets `reader.py` take the name and
`writer.py` the schema without either importing the other.

`Predicate` is a plain value object — `node`, `field`, `op`, `value`, and
comparison against Python values. It deliberately knows nothing about Arrow;
turning one into a pushdown expression is `parquet.predicate_to_expression`.
Previously `Predicate.coerce` did a function-scope `import pyarrow` inside a
type that otherwise needs none.

`answerable_from_manifest()` is tier 2 doing real work: from the manifest alone
it can sometimes prove **no record matches** — a value outside a listed domain,
or outside a numeric range — and answer without reading a payload at all. It
returns `False` (nothing matches) or `None` (can't tell), and deliberately
**never returns `True`**: a value being in the domain says some record has it,
not which one.

### `src/pfb/parquet.py` — everything that needs pyarrow

**Avro → Arrow mapping.** Covered under the defects below. Also defines the two
carried columns: `__pfb_id`, so a Parquet hit maps back to an Avro record, and
`__pfb_relations`, so edge traversal and edge counts need no second pass.

**`ParquetStager`** buckets records by node and flushes row groups at
`batch_size`, bounding peak memory by (nodes × batch size) rather than by the
export. This is where the write-time index configuration lives, none of which is
on by default in pyarrow: `write_page_index=True`, `use_dictionary=True`, zstd,
a 64 KiB `data_page_size`, and `bloom_filter_options` for identifier columns.
Note `bloom_filter_options` reaches `ParquetWriter` through `**options` — there
is no `write_bloom_filter` parameter, despite how it reads.

Bloom filters are restricted to `submitter_id` / `participant_id` /
`specimen_id` because the PoC priced them at 3.5% of the payload, 8× what the
page index costs. `--no-bloom` turns them off.

**Statistics and the cardinality guard.** `footer_stats()` reads null counts and
min/max from the footer without touching a data page, and deliberately ignores
`distinct_count` — pyarrow leaves it unset, so domains come from `pc.unique()`.
`field_summaries()` then keeps the manifest bounded by field count rather than
record count:

| Distinct values | Column | Strategy | Stored in the manifest |
| --- | --- | --- | --- |
| ≤ `--domain-cap` | any | `domain` | full sorted value list |
| > cap | a bloom column | `bloom` | nothing — the filter is in the payload |
| > cap | numeric/temporal | `range` | min and max only |
| > cap | anything else | `none` | nothing |

On the real export at the default cap of 256: 115 `domain`, 4 `range`,
174 `none`.

**`search_payload()`** pushes the predicate into `pq.read_table(filters=...)`
and lets pyarrow consult the row-group statistics and page index. No
hand-written pruning logic.

**`build_index()`** is the orchestration, and lives here rather than in a click
command so that a caller indexing a PFB programmatically never has to import a
CLI module. It reads through `PFBReader` rather than raw Avro so enum values are
decoded before they reach Parquet.

### `src/pfb/writer.py` — the Avro container work

All of it pyarrow-free: payloads are handled as opaque bytes throughout.

`make_avro_schema()` takes `with_parquet=False`; the `NodeParquet` union member
is added only when payloads are actually embedded, so a file written without
`--embed-parquet` is byte-compatible with one written by master.

`set_parquet_payloads()` takes **plain data** (`{node: bytes}` plus a meta dict).
It previously held a `ParquetStager` and duck-typed `.row_counts` /
`.bloom_columns_for()` off it — the pyarrow layer reaching down into the writer
— and its `stager=None` fallback hardcoded `compression="zstd"` and
`page_index=True`, silently contradicting `--compression`.

`write_indexed()` is the two-pass write described in the placement fix below.
`write_to()` is a new sibling of `write()` that takes an explicit file object;
the two-pass build needs it because it must read its staging buffer back, and
the previous code called `__enter__()` without `__exit__()` to stop `PFBWriter`
closing the buffer.

### `src/pfb/reader.py` — reading it back

Name-based union filtering and `NodeParquet` skipping (see the defects below),
manifest parsing on open, and `parquet()` / `parquet_file()` for payload fetch.

`parquet()` reads through a **duplicated file handle**. See the bug fix section.

### CLI

```
pfb index build -i in.pfb -o out.pfb [--embed-parquet] [--domain-cap N]
                                     [--data-page-size N] [--compression C] [--no-bloom]
pfb index show  -i in.pfb [--node NAME] [--full]
pfb search      -i in.pfb -w 'demographic.population=CEU' [--count] [--explain]
pfb to -i in.pfb parquet OUTPUT_DIR
```

`pfb search` tries three paths in order: the manifest alone (can it rule the
query out?), the embedded payload, then a full scan with a warning. An unindexed
file always returns correct results, and the fallback now shares
`Predicate.matches_record` with the indexed path so the two cannot drift apart.

`pfb to parquet` exposes the staging layer standalone — useful in its own right,
and it makes the type mapping testable without any container work.

---

## A fourth defect, found while relocating this code

`PFBReader.parquet()` called `seek(0)` on the **same file handle** the live
`fastavro` reader was using. Iterating the reader afterwards therefore read from
a rewound file with desynchronised reader state.

The only code path that both fetches a payload and then iterates is the
`pfb search` fallback — reached when the manifest claims an embedded payload but
the block ordinal is stale — which is exactly why the original tests missed it.
It is a latent corruption in the one path specifically written to be safe.

`parquet()` now reads through an independent handle: `os.dup` of the descriptor
where there is one, re-opening by name otherwise, and a copy for in-memory
buffers. A stream that is none of those (notably stdin) yields `None`, which is
honest — seeking is impossible there regardless.

Three regression tests cover it, and all three fail against the old behaviour:

| Test | Asserts |
| --- | --- |
| `test_parquet_does_not_disturb_a_fresh_reader` | fetch, then iterate → every record identical to an unindexed read |
| `test_parquet_mid_iteration_does_not_lose_records` | fetch *during* iteration → no record lost or repeated |
| `test_repeated_payload_fetches_are_stable` | two fetches return the same bytes |

---

## On `--explain`

The proposal said `--explain` would report pages read versus skipped "straight from the
page index". **There is no such API.** pyarrow 26 exposes no `ColumnIndex` or
`OffsetIndex` accessor and no counter for pages or row groups actually skipped; the PoC
had to decode the Thrift footer by hand to measure it.

Rather than print a number that is secretly just the total, `--explain` reports what it
can and names what it cannot:

```json
{
  "source": "parquet",
  "row_groups_in_payload": 1,
  "rows_in_payload": 3202,
  "rows_matched": 179,
  "pages_skipped": "not reportable: pyarrow exposes no page-index API"
}
```

If realized pruning needs to be a supported number, it requires vendoring a footer
decoder like the PoC's `pqindex.py`. That is not in this branch.

---

## Honest limits

**Tier 3 breaks consumers outside this SDK.** The fixes above protect everything that
goes through `PFBReader`. They do nothing for an external importer that enumerates the
`object` union itself — which is most of §4.2's audit list. If a PFB with
`--embed-parquet` is distributed, those consumers will see a record type they do not
know. This is inherent to putting payloads in the union and is not solvable here; it is
the reason tiers 0–2 are the default.

**Page-level pruning does not materialize at this scale.** The PoC found pages and row
groups come out 1:1 on nodes of a few thousand rows, because dictionary-encoded pages are
dense enough that one page covers thousands of values. `write_page_index=True` is enabled
and costs ~0.4%, but on an export this size it buys nothing over the row-group statistics
that are free and on by default. It starts to matter at much larger node counts.

**Pruning still depends on clustering.** Parquet's statistics are min/max bounds, so they
only exclude a row group when the data is ordered. On unsorted export data the PoC
measured 0% pruning for `population = CEU`. Nothing in this branch sorts records — that
remains open, and it is the single change that would most improve tier 3.

**The size comparison depends on a baseline that should move.** The embedded copy adds
26% here, but PFBs ship uncompressed; `PFBWriter.write()` never passes `codec=`. Setting
`codec="deflate"` is a one-line 72% reduction, after which the embedded payload is larger
than the entire compressed file it lives in. That change is not in this branch because it
is independent of indexing, but it should be weighed before tier 3 ships.

**Ordinals are positional.** Any tool that rewrites block boundaries invalidates
`payload_blocks`. The two-pass copy avoids this internally, and payload fetch fails soft,
but there is no checksum binding a manifest to the blocks it describes. Worth adding
before this is relied on in production.

**`pfb search -i -` cannot fetch a payload.** `click.File("rb")` with `default="-"`
yields non-seekable stdin, and payload fetch fundamentally requires seeking. Today it
degrades to the scan path rather than erroring, which is correct but silently slow.
Rejecting `-` for `search` with a clear message would be better; it is deliberately not
in this change.

**One failing plugin breaks the whole CLI.** `cli.py` calls `ep.load()` for every
`pfb.plugins` entry point with no `try`/`except`, so any plugin whose dependency is
missing takes down `pfb --help` along with it. This feature works around it by keeping
pyarrow out of every plugin module, but the underlying fragility is still there and is
worth guarding independently.

---

## Tests

`tests/test_parquet_index.py`, 49 tests, all passing alongside the existing 30.

Mapped against the proposal's §5.10 table:

| Proposal's test | Here | Notes |
| --- | --- | --- |
| `test_parquet_roundtrip` | `test_payload_values_match_the_records` | every field of every demographic record |
| `test_index_backward_compat` | `test_records_are_identical_through_the_reader` | plus the real-export check below |
| `test_reader_skips_parquet` | `test_iteration_never_yields_payload_records` | |
| `test_page_index_written` | `test_page_index_is_actually_written` | via footer offsets, since pyarrow has no accessor |
| `test_search_equals_scan` | `test_search_matches_a_brute_force_scan` | |
| `test_enum_values_decoded` | `test_enum_values_are_stored_decoded` | checked by decode idempotence |
| `test_cardinality_guard` | `test_cardinality_guard_falls_back_above_the_cap` | plus `..._bloom_columns...` |
| `test_multi_branch_union_raises` | `test_heterogeneous_union_raises` | **and** `test_multi_branch_union_of_one_arrow_type_collapses`, which the proposal would have failed |
| `test_no_index_fallback` | covered in `pfb search`'s scan path | |

New tests the proposal did not anticipate:

| Test | Guards |
| --- | --- |
| `test_every_node_in_the_sample_dictionary_maps` | the 36-union abort |
| `test_reader_reports_the_same_nodes_with_and_without_payloads` | the phantom 41st node |
| `test_payload_is_fetched_without_full_iteration` | the placement fix |
| `test_payload_block_ordinals_are_recorded` | the two-pass contract |
| `test_unembedded_output_has_no_new_union_member` | tiers 0–2 really are inert |
| `test_manifest_is_readable_without_pyarrow` | tier 0–2 consumers need only fastavro |
| `test_manifest_cannot_confirm_a_present_value` | the domain check never returns `True` |
| `test_parquet_does_not_disturb_a_fresh_reader` | the seek bug above |
| `test_parquet_mid_iteration_does_not_lose_records` | the seek bug, interleaved |

Checked against the real export outside the suite, with `--embed-parquet`:
`pfb show nodes` → 40 nodes, `pfb to tsv` → exit 0 and 14 files, `pfb show stats` → 14
nodes / 29,020 edges, and `PFBReader` output identical record-for-record.
