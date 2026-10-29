# Parquet Search Index — Proof of Concept Results

**Companion to:** [`parquet-search-index.md`](./parquet-search-index.md) §7
**Branch:** `feat/parquet-poc` · **Scripts:** [`poc/parquet/`](../poc/parquet/)
**Measured:** 2026-10-20, against `export_2026-08-06T10_36_03.avro`
(10,173,528 bytes · 29,021 data records · 40 declared nodes · 14 populated)
**Environment:** Python 3.13.0 · fastavro 1.11.1 · **pyarrow 26.0.0**

Step 3 was skipped by agreement (there is no `aggregations` producer to compare
against; the counts it would have checked are reported in `step1_column_stats.json`).

---

## Verdict

| Gate (§7) | Threshold | Measured | Result |
| --- | --- | --- | --- |
| **1 — size** | Parquet total > Avro → off by default | 3,317,254 vs 10,173,528 (32.6%) | **Passes as written, but see below** |
| **2 — pruning** | < 50% of pages pruned → not paying for itself | **0%** unsorted / 71–75% sorted | **Trips on unsorted data** |
| **3 — manifest** | > 5% of PFB → tighten `--domain-cap` | 1.53% of PFB | **Passes** (misses §7's <1% target) |
| **4 — compatibility** | Any §4.2 consumer breaks → tiers 0–2 only | **`pfb to tsv` exits 1** | **Breaks** |

Tiers 0–2 are vindicated and cheap. **Tier 3 is not justified by these numbers**
on this export, for three independent reasons: the columnar copy costs more than
the entire compressed PFB, page-level pruning is structurally unavailable at this
record count, and the union member breaks a shipped consumer.

---

## 1. The doc's claims, checked

| § | Claim | Verdict |
| --- | --- | --- |
| 5.2 | "current Gen3 dictionaries appear not to produce" multi-branch unions | **Wrong.** 36 occur — every `state` field is `union<enum, enum, null>`. The proposed `raise UnsupportedPFBType` aborts on the first node. Also `"null"` is **last** in those unions, so nothing may match on union position |
| 1.2 | `bloom_filter_options` works on `ParquetWriter` "through `**options`" | **Correct.** Accepted, and confirmed to actually write filters (`bloom_filter_offset` present in the footer). `write_bloom_filter` is not a parameter |
| 1.3 | `distinct_count` is unset and cannot be relied on | **Correct.** Returns `None` throughout; domains must be computed with `pc.unique()` |
| 1.4 | Predicate pushdown works on an in-memory buffer | **Correct**, and page-level pruning is real — an 80% byte reduction on a sorted, high-cardinality 400k-row column |
| 1.4 | "Row-group and page skipping **both** apply" | **Correct in principle, irrelevant here.** At PFB scale pages and row groups are 1:1 — see §3 |
| 2.1 | Header metadata takes `str`, not `bytes`; payloads belong in the record stream | **Correct.** The 155 KB JSON manifest rides in the header with no base64 |
| 2.3 | `PFBReader` yields decoded enum values | **Correct**, and the round-trip survives re-encoding — all 29,021 records come back byte-identical through the two-enum `state` unions |
| 5.7 | `--explain` reports pages read vs skipped "straight from the page index" | **No such API.** pyarrow 26 exposes no `ColumnIndex`/`OffsetIndex` accessor at all. Measuring this required decoding the Thrift footer ourselves ([`pqindex.py`](../poc/parquet/pqindex.py)) |
| 4.2 | Consumers break via `handle[record["name"]]` → `KeyError` | **Right conclusion, wrong mechanism** — see §4 |

---

## 2. Gate 1 — size

The headline comparison passes: per-node Parquet totals 3.32 MB against a 10.17 MB
Avro, exactly as §2.4 hoped. But §2.2 established that current PFBs are written
uncompressed, and the moment that baseline is corrected the result inverts.

| What you would actually ship | Bytes | vs. today |
| --- | ---: | ---: |
| PFB as shipped (`codec=null`) | 10,173,528 | — |
| **PFB with `codec=deflate`** | **2,864,894** | **−71.8%** |
| PFB with `codec=xz` | 2,491,454 | −75.5% |
| Per-node Parquet, zstd (the embedded copy) | 3,317,254 | — |
| As-shipped **+** embedded Parquet | 13,647,837 | +34.2% |
| Deflate **+** embedded Parquet | ≈ 6,182,148 | −39.2% |

**The columnar copy is larger than the entire compressed PFB it would live
inside.** `PFBWriter.write()` never passes `codec=`; adding it recovers 7.3 MB for
one argument, no schema change and no compatibility risk. Embedding then more than
doubles the result (2.86 MB → 6.18 MB) to buy tier 3.

Gate 1's honest restatement: *duplication is affordable in absolute terms, but it
is the second-most valuable thing you could do with those bytes, and the first is
nearly free.*

**Index tax**, isolated by writing each cell with the feature on and off
(pyarrow exposes no accessor, so differencing is the only way to price it):

| Feature | Bytes | Share of the Parquet copy |
| --- | ---: | ---: |
| Page index (`write_page_index=True`) | 14,522 | 0.4% |
| Bloom filters | 116,621 | 3.5% |

The page index is genuinely cheap, as §1.2 implies. Bloom filters cost 8× more and
the doc never prices them — worth noting, since §3.1 leans on them for every
high-cardinality field.

`data_page_size` had **no effect on size** between 8 KiB and 1 MiB. The large
column chunks are mostly *dictionary pages* (3,202 distinct UUIDs), which
`data_page_size` does not govern; the actual data pages hold 2-byte dictionary
indices, so 3,202 rows fit in a single 8 KiB page.

---

## 3. Gate 2 — does retrieval by predicate pay?

pyarrow reports no pruning statistics, so this was measured three ways:
**A** bytes actually read (byte-counted, and *refused unless calibrated*),
**B** pages prunable, computed by evaluating each predicate against per-page
bounds from the decoded `ColumnIndex`, and **C** row groups prunable from footer
statistics. The decoder was cross-validated against `ParquetFile.metadata` on
every overlapping field before any number below was believed.

At `data_page_size=512`, `row_group_size=512`:

| # | Predicate | Unsorted | Sorted | Note |
| --- | --- | ---: | ---: | --- |
| 1 | `demographic.population = 'CEU'` | **0%** | 71.4% | the doc's motivating query |
| 2 | `demographic.annotated_sex = 'female'` | **0%** | 42.9% | control — correctly does not prune |
| 3 | `demographic.submitter_id = <id>` | 85.7% | 85.7% | already clustered in the export |
| 4 | `subject.created_datetime > <median>` | **0%** | 42.9% | range pruning |
| 5 | `subject.consent_codes` contains X | n/a | n/a | not filterable, **not sortable** |
| 6 | `__pfb_rel0_dst_id = <subject id>` | 85.7% | 85.7% | edge traversal |

**Gate 2 trips on the motivating query.** Unsorted, 26 population values are spread
across 3,202 rows, so every page's `[min, max]` spans `ACB..YRI` and no page can be
excluded. Sorting lifts it to 71–75%, clearing the 50% bar — so the gate's
"revisit with `sorting_columns`" is the right instinct, and §8's open question
about reordering records becomes the deciding one.

### The structural finding

**Pages and row groups are 1:1 at this scale.** Across nearly every configuration
`pages_total == rowgroups_total`; only at a 512-byte page size with a single row
group does the page index become finer. So on 3,202-row nodes `write_page_index`
buys nothing over the row-group statistics that are already on by default and
free. §1's central argument — that the page index provides skipping finer than row
groups — is true of Parquet and inapplicable to PFBs of this size.

The deeper cause is dictionary encoding: `ColumnIndex` bounds are per *data page*,
and dictionary-encoded data pages are so dense (2 bytes per value) that one page
covers thousands of rows. Fine pruning granularity and good compression are in
direct tension here.

### Angle A rarely calibrates, and that is a result

A byte counter can only resolve pruning when a column's own bytes dominate the
read. For low-cardinality dictionary columns they never do — `population`
compresses to ~1 KB while the surrounding footer and page index are 18× that. So
realized pruning is **unmeasurable by byte accounting** for exactly the query the
proposal is motivated by, at any file size. This is why Angle B was necessary
rather than optional.

The one query that did calibrate, #6, shows **85.7% available vs 42.6% realized —
a 43-point gap**: pyarrow read substantially more than the index permitted it to
skip. On this evidence the limit on tier 3 is not only the data's clustering but
also how fully pyarrow exploits the index Parquet wrote.

---

## 3a. Does it actually work? End-to-end retrieval

Yes — and correctly. [`search_demo.py`](../poc/parquet/search_demo.py) answers
`demographic.population = 'CEU'` from the embedded payload alone and returns
**179 records, identical to a brute-force scan of all 29,021 Avro records.**

But the speedup depends entirely on payload placement, and the result is stark
(warm, steady-state; the Parquet query itself is 1–3 ms):

| Path | Payload extract | Query | **Total** |
| --- | ---: | ---: | ---: |
| Full Avro scan (what consumers do today) | — | — | **504 ms** |
| Indexed, payloads **last** (§4.2 mitigation 2) | 510 ms | 3 ms | **514 ms** |
| Indexed, payloads **first** | 9 ms | 1 ms | **10 ms** |

**With payloads last, tier 3 is no faster than the scan it replaces** — 514 ms
against 504 ms. The payload is at the end of the record stream, so reaching it
means decoding every record first: exactly the full scan the index exists to
avoid. The §5.5 wrinkle is not a usability nit, it voids the feature.

With payloads first the index delivers what it promises: **49× faster**, 10 ms
against 504 ms.

### This is the central unresolved conflict

The two placements trade off against each other completely:

| | Retrieval speed | `pfb to tsv` output |
| --- | --- | --- |
| Payloads **last** | 514 ms — no gain | 14 of 14 files, then exit 1 |
| Payloads **first** | 10 ms — 49× gain | **0 files**, exit 1 |

The configuration that makes tier 3 worth having is the one that breaks consumers
hardest. §8 lists payload ordering as an open question and §4.2 lists "emit
payloads last" as a mitigation; these numbers show the two cannot both hold.

Resolving it requires reading payloads **without iterating the record stream** —
§5.5's `load_parquet()` that seeks to the tail. Note this re-encounters §1.1's own
argument: the manifest is written in the Avro header, before the data, so it
cannot carry payload byte offsets, which are only known afterwards. The options
are a second write pass to backfill the header, a trailer after the payloads, or
payloads-first plus a properly implemented reader-side skip (mitigation 3) that
filters `NodeParquet` from `reader.schema` as well as from iteration — which
protects in-SDK consumers but not the external importers in §4.2's audit list.

**This should be promoted from §8's open-questions table to a prerequisite.**

---

## 4. Gate 4 — compatibility

Master's `PFBReader`, run from a clean worktree, reads an indexed PFB and returns
**all 29,021 records identical** (ids, decoded objects, relations) with metadata
unchanged. The 155 KB manifest is readable straight from the Avro header with no
Parquet reader and no pyarrow. Iteration does not crash.

But a shipped consumer does break, and **not by the mechanism §4.2 predicts**:

```
$ pfb to -i indexed.pfb tsv /tmp/out     # exit 1
  File ".../src/pfb/exporters/tsv.py", line 98, in _to_tsv
    node_index = next( ... )
StopIteration
```

§4.2 anticipates `handle[record["name"]](record)` raising `KeyError`. That never
fires — because `PFBReader.__enter__` skips exactly **one** union member before
treating the rest as node schemas, so `NodeParquet` lands in `reader.schema` as if
it were a node (41 nodes reported instead of 40) and `fields_by_name[name]`
*succeeds*. The failure happens one line later, at a bare `next()` over
`reader.metadata["nodes"]`, where `NodeParquet` is absent.

Consequences:

- **The §4.2 audit list is too narrow.** It must cover anything that enumerates
  `reader.schema` or cross-references it against `metadata["nodes"]`, not just
  `record["name"]` dispatchers. Observed today: `pfb show nodes` lists
  `NodeParquet` as a node, `pfb show schema` includes it, `pfb show stats` reports
  `NodeParquet: 14`, and `pfb to tsv` exits 1.
- **Mitigation 3 must filter the schema list**, not only iteration.
- Placing the member first is no escape — `Metadata` then becomes the skipped
  member instead.

### §8's ordering question, settled on evidence

| Payload position | `pfb to tsv` | TSVs produced |
| --- | --- | --- |
| **Last** (§4.2 mitigation 2) | exit 1 | **14 of 14** |
| First (after `Metadata`) | exit 1 | **0** |

Emitting last does not prevent the failure for any consumer that iterates to
completion, but it does confine it: all output is produced before the crash. The
tension between §4.2 (last) and §5.5 (first, so `.parquet()` works without full
iteration) resolves in favour of **last**, with a seeking `load_parquet()` for the
reader side.

---

## 5. Recommendation

1. **Ship a codec.** `PFBWriter.write()` passing `codec="deflate"` is a one-line,
   zero-risk 72% size reduction and is worth more than everything else here.
2. **Ship tiers 0–2.** The manifest costs 1.53% of the PFB, needs no Avro schema
   change, no pyarrow to read, and carries no compatibility risk. Note it is 5.4%
   of a *deflate* PFB, so re-check gate 3 against the compressed baseline and
   tighten `--domain-cap`.
3. **Do not ship tier 3 as specified.** It costs more than the compressed PFB,
   delivers 0% pruning on the motivating query without reordering records, gains
   nothing from the page index at this record count, and breaks `pfb to tsv`.
4. **If tier 3 is pursued,** the prerequisites are: a way to read payloads
   without iterating the stream (§3a — without it the feature is worthless), a
   decision on record reordering (§8), a reader-side fix that filters
   `NodeParquet` from `reader.schema` as well as iteration, and a fix to the
   cardinality guard, which currently emits full 150-value UUID domain lists for
   identifier columns in small nodes purely because their distinct count is under
   the cap.

## 6. Also worth fixing

- `avro_field_to_arrow` must collapse multi-branch unions whose branches map to
  the same Arrow type and raise only on heterogeneous ones; the unconditional
  raise is unshippable.
- `tsv.py:98`'s bare `next()` has no default and will raise `StopIteration` for
  any record whose name is missing from `metadata["nodes"]` — a latent bug
  independent of this proposal.
- `PFBWriter.write()`'s `to_update = {}` / `obj.update(to_update)` pair
  (`writer.py:233,243`) is dead code.
