# Block Search Index — Implementation Notes

**Branch:** `feat/block-index`
**Adds:** `pfb index build`, `pfb index show`, `pfb search`
**Changes to existing behaviour:** none. No Avro schema change, no new record type,
nothing added to the record stream.

---

## The idea in one paragraph

An Avro object container file is not an opaque blob of records — it is a sequence of
**blocks**, and each block is prefixed with a record count and a byte length.
`fastavro.block_reader` can walk those prefixes and skip over any block's payload
without decoding the records inside it. On a 10 MB / 29k-record export, walking every
block header takes **6 ms**; decoding every record takes **330 ms**. So if you know
which blocks contain your answer, you can jump to them and ignore the rest. This branch
builds that "which blocks" map at write time and stores it in the Avro file header,
where readers that don't know about it simply ignore it.

Measured on `export_2026-08-06T10_36_03.avro`, searching through the same code path
with and without the index:

| Predicate | Scan | Indexed | Blocks read | Speedup |
| --- | ---: | ---: | ---: | ---: |
| `demographic.population=CEU` | 338 ms | **50 ms** | 46 / 627 | 6.8× |
| `demographic.annotated_sex=female` | 341 ms | **58 ms** | 47 / 627 | 5.9× |
| `aliquot.analyte_type=DNA` (no matches) | 332 ms | **39 ms** | 44 / 627 | 8.5× |

The index costs **74 KB on a 10.17 MB file (0.73%)** and takes 775 ms to build.

---

## Why this shape, and not a columnar copy

Two design constraints drove everything here.

**Postings, not min/max bounds.** Parquet's row-group and page statistics are
`[min, max]` ranges. A range only excludes a page when the data is *clustered*, which is
why a columnar design needs `sorting_columns` — and why you can only ever optimise for
one predicate, since records have one physical order. A postings list (`CEU → blocks
[3, 17, 44]`) names the blocks explicitly, so it works on unsorted data and you can hold
one per field simultaneously. Sorting is not required anywhere in this design.

**Nothing in the record stream.** Every byte of the index lives in the Avro file header.
That is the entire compatibility story: a reader that has never heard of this feature
sees a normal PFB. Verified on the real export — `pfb show nodes` reports the same 40
nodes, `pfb to tsv` exits 0 and writes all 14 files, and a plain `fastavro.reader` over
the indexed file yields records byte-identical to the original.

---

## What each file does

### `src/pfb/blockindex/manifest.py` — the stored format

Defines the manifest and the read-side `BlockIndex` wrapper.

The manifest is JSON under the header key `pfb.blockindex.v1`. fastavro rejects `bytes`
header values, so when the JSON exceeds 16 KB it is deflated and base64-encoded into a
*separate* key, `pfb.blockindex.v1.z`. Exactly one of the two is ever written, and
`load_manifest` accepts either. This matters at scale: the manifest for the real export
is 317 KB of JSON, which compresses to the 74 KB actually stored.

`load_manifest` returns `None` — rather than raising — for a missing, corrupt, or
future-versioned manifest. A reader built against v1 that meets a v2 file degrades to a
full scan instead of refusing to open it.

`BlockIndex.candidate_blocks()` is the core read-side call, and it is deliberately
**conservative**: it returns a *superset* of the blocks that could match, never a
subset. Callers re-check every decoded record against the predicate anyway, so a
too-large superset costs time while a too-small one would cost correctness. Every
fallback path in the class widens to the node's full block set.

### `src/pfb/blockindex/schema.py` — schema introspection

Two jobs the builder needs that `PFBReader` normally does for you.

`field_traits()` walks the writer schema and tags each field `enum` / `numeric` /
`array`. `enum` drives value decoding, `numeric` selects the range strategy, `array`
tells the builder to index each element rather than the list object.

`node_schemas()` deserves a note. `PFBReader.__enter__` finds node schemas by skipping
**exactly one** union member and treating everything after it as a node — which is why,
in the Parquet design, a new union member gets misreported as a 41st node. This version
filters by name instead, so adding union members later cannot produce phantom nodes.

`decode_value()` applies `base.decode_enum`. The builder reads records straight out of
Avro blocks rather than through `PFBReader`, so enum values arrive still escaped
(`_32_` forms). Indexing them raw would key the postings on strings no user would ever
type as a predicate.

### `src/pfb/blockindex/builder.py` — building and writing

`build_index()` makes one pass with `block_reader`, and for each record accumulates two
things per field: a postings map, and (for numeric fields) per-block `[min, max]`
bounds. Both are collected at once because the choice between them depends on the
distinct count, which isn't known until the file has been read.

`_FieldAccumulator` implements the **cardinality guard**. Once a field exceeds
`--domain-cap` distinct values it drops its partial postings map and stops collecting,
so a UUID column cannot blow up memory or the manifest. What it falls back to depends on
the type:

| Distinct values | Type | Strategy | Stored |
| --- | --- | --- | --- |
| ≤ cap | any | `postings` | value → block ordinals |
| > cap | numeric | `range` | per-block `[min, max]` |
| > cap | anything else | `none` | nothing; queries use the node block map |

On the real export that yields 117 `postings`, 4 `range`, and 31 `none`.

`write_indexed()` is the second pass, and the reason the whole thing is cheap. Blocks are
copied **byte for byte** via a small `_RawBlock` adapter — `Writer.write_block` only
reads `num_records` and `bytes_`, never the records — so nothing is re-encoded. That is
not just a speed trick: re-encoding could shift where block boundaries fall, which would
silently invalidate every ordinal in the manifest just written.

The input codec is carried through unchanged, because block payloads are stored
already-compressed; writing them under a different codec would produce a file whose
declared codec doesn't match its contents.

This two-pass structure also sidesteps a problem the columnar design ran into. The
header is written before the data, so it cannot contain byte offsets that are only known
afterwards. Storing **ordinals** instead of byte offsets removes the circularity
entirely — and the backfill costs 20 ms, because it is a byte copy.

`rechunk()` re-encodes records into blocks of a given size. This is the real tuning knob
(see below), and unlike `write_indexed` it *does* re-encode, so it must run before the
index is built.

### `src/pfb/blockindex/query.py` — predicates and retrieval

`Predicate` parses `NODE.FIELD OP VALUE` for `= != > >= < <=`. Comparison is numeric when
both sides parse as numbers and lexical otherwise, so `age>9` doesn't match `10` as a
string. `=` against an array field means "contains".

`overlaps_range()` answers "could a value in `[low, high]` satisfy this?" against
per-block bounds, and errs toward `True` throughout — a block is only skipped when it
provably cannot match.

`search()` intersects candidate blocks across predicates, then walks the file decoding
only the survivors. Two details worth knowing:

- **All predicates must target one node.** They are ANDed against a single record, and a
  record belongs to exactly one node, so a cross-node conjunction has no meaning. This
  raises rather than silently returning nothing.
- **Only the fields a predicate mentions get decoded** during the scan; the full record
  object is built only for matches. Decoding every field of every candidate cost roughly
  2× on the real export (65 ms → 50 ms when fixed).

A file with no index still returns correct results — every block is read — so callers
never have to special-case one. `SearchResult.indexed` reports which path was taken.

### `src/pfb/base.py` and `src/pfb/reader.py` — reader integration

Nine lines and five lines respectively. `PFBBase` gains the `block_index` property and
its setter, matching the existing `schema` / `metadata` accessor pattern.
`PFBReader.__enter__` parses the manifest out of the header — eagerly, because it is
already in memory with the header and it is small — and leaves it `None` when absent.

Normal iteration is untouched. Nothing was added to the stream, so there is nothing to
filter out of it.

### `src/pfb/commands/index.py` and `src/pfb/commands/search.py` — CLI

```
pfb index build -i in.pfb -o out.pfb [--domain-cap N] [--field NODE.FIELD] [--sync-interval N]
pfb index show  -i in.pfb [--full]
pfb search      -i in.pfb -w 'demographic.population=CEU' [--count] [--explain]
```

`--explain` reports blocks read, blocks pruned, and records scanned versus matched —
the numbers that tell you whether the index is earning its place on *your* data:

```json
{
  "blocks_total": 627, "blocks_read": 46, "blocks_pruned": 581,
  "prune_ratio": 0.9266, "records_scanned": 3133, "records_matched": 179,
  "indexed": true
}
```

Both are registered as `pfb.plugins` entry points, like every other subcommand.

---

## The tuning knob: block size

This is where the honest limits live.

Pruning comes from two independent sources. The first is **node grouping**, which you get
for free: `PFBWriter` emits records grouped by node, so 625 of 627 blocks in the real
export hold exactly one node type. Any predicate immediately skips every block belonging
to another node — that alone is the 92.7% above, with no configuration at all.

The second is **value locality within a node**, and that one depends on your data. In the
real export, 179 CEU records are spread across 3,202 demographic records in ~46-record
blocks, so nearly every demographic block contains at least one. Postings can't help when
a value is genuinely everywhere.

`--sync-interval` is the lever. Smaller blocks mean finer granularity, but more block
headers to walk:

| `--sync-interval` | Blocks | Pruned | Records scanned | Walk | Query | Index |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default (~16 KB) | 627 | 92.7% | 3,133 | 2.4 ms | 47.9 ms | 72 KB |
| 4 KB | 2,442 | 95.1% | 2,135 | 3.5 ms | 38.5 ms | 128 KB |
| **1 KB** | 8,600 | **98.2%** | 795 | 7.2 ms | **30.7 ms** | 267 KB |
| 250 B | 21,015 | 99.2% | 350 | 15.0 ms | 44.8 ms | 497 KB |

Pruning keeps improving all the way down, but walk cost and index size both rise with
block count, so query time is U-shaped and bottoms out around 1 KB — 30.7 ms, or 11×
the 338 ms scan. Past that the walk dominates and it gets worse again. The default
leaves block boundaries alone, because re-chunking rewrites the file.

`tests/test_blockindex.py::test_scattered_values_in_coarse_blocks_do_not_prune` pins this
limitation deliberately, using a pathologically interleaved fixture.

### Things this does not do

- **No bloom filters.** A high-cardinality string field gets no index at all, so queries
  on it fall back to scanning that node's blocks. Still better than a full scan, but not
  as good as a columnar design would manage.
- **Range pruning is weak in practice.** It is implemented and correct, but a numeric
  field spread evenly across blocks gives every block overlapping bounds. On the real
  export, `file_size>10000000000` prunes to node level and no further.
- **Low-selectivity predicates can't be helped.** `annotated_sex=female` matches half the
  records; no index fixes that.
- **Ordinals are positional.** Anything that rewrites block boundaries invalidates the
  index. `write_indexed` copies blocks verbatim specifically to avoid this, but a file
  re-written by other tooling needs `pfb index build` run again. There is currently no
  checksum tying a manifest to the blocks it describes — worth adding before this is
  relied on in production.

---

## Tests

`tests/test_blockindex.py`, 42 tests, all passing alongside the existing 30.

The ones that matter most:

| Test | Guards |
| --- | --- |
| `test_search_matches_brute_force_scan` | index results == full scan |
| `test_indexed_file_has_identical_records` | the copy changes no data |
| `test_block_boundaries_survive_the_copy` | ordinals stay valid |
| `test_reader_sees_no_extra_nodes` | the compatibility claim |
| `test_search_without_an_index_still_works` | unindexed fallback is correct |
| `test_enum_values_are_indexed_decoded` | postings key on `CEU`, not `_32_` |
| `test_cardinality_guard_*` | a UUID column doesn't become a 600-key map |
| `test_codec_is_preserved` | verbatim blocks aren't mislabelled |

Compatibility was also checked against the real export outside the suite: `pfb show
nodes` → 40 nodes before and after, `pfb to tsv` → exit 0 and 14 files, and
`fastavro.reader` output compared record-for-record (`True`, 29,022 records).
