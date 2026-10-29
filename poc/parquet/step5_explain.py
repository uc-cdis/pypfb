#!/usr/bin/env python
"""PoC step 5: --explain. GATE 2 - does record retrieval by predicate pay?

    "Pruning < 50% of pages on selective queries -> page-level retrieval is not
     paying for itself on unsorted exports."

pyarrow reports no pruning statistics, so this measures from three independent
angles. Their disagreement is itself a result:

  A  bytes actually read   - what pyarrow DID. Byte-counted via CountingReader,
                             and REFUSED unless calibration passes (an unfiltered
                             single-column read must be dominated by that
                             column's own bytes, not by footer/page-index/
                             read-ahead). On low-cardinality dictionary columns
                             calibration cannot pass at any file size.
  B  pages prunable        - what was POSSIBLE. Computed by evaluating the
                             predicate against per-page bounds from the
                             ColumnIndex, decoded by pqindex (cross-validated).
  C  row groups prunable   - coarse upper bound, pure public API.

gap = available - realized separates "Parquet cannot prune this data" from
"pyarrow does not exploit the index Parquet wrote". Those lead to completely
different next steps.
"""
import argparse
import csv
import json
import pathlib
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import (  # noqa: E402
    COL_REL0_ID,
    CountingReader,
    column_compressed_bytes,
    load_capabilities,
    read_cached,
    write_json,
    writer_options,
)
from pqindex import (  # noqa: E402
    cross_validate,
    decode_bound,
    read_column_index,
    read_footer,
    read_offset_index,
)

PAGE_SIZES = (512, 8192, 65536)
ROW_GROUPS = (512, None)


def build_queries(stats):
    """Pick predicates from measured cardinalities rather than guessing."""

    def pick(node, field, idx=0):
        d = stats[node][field].get("domain")
        if d:
            return d[min(idx, len(d) - 1)]
        return stats[node][field].get("min")

    demo, subj = stats["demographic"], stats["subject"]
    median_dt = pick(
        "subject",
        "created_datetime",
        len(subj["created_datetime"].get("domain", [1])) // 2,
    )
    return [
        {
            "q": 1,
            "node": "demographic",
            "field": "population",
            "op": "=",
            "value": "CEU",
            "note": "the doc's motivating query",
        },
        {
            "q": 2,
            "node": "demographic",
            "field": "annotated_sex",
            "op": "=",
            "value": pick("demographic", "annotated_sex"),
            "note": "CONTROL - must not prune",
        },
        {
            "q": 3,
            "node": "demographic",
            "field": "submitter_id",
            "op": "=",
            "value": demo["submitter_id"]["min"],
            "note": "bloom-filter case",
        },
        {
            "q": 4,
            "node": "subject",
            "field": "created_datetime",
            "op": ">",
            "value": median_dt,
            "note": "range pruning on ISO strings",
        },
        {
            "q": 5,
            "node": "subject",
            "field": "consent_codes",
            "op": "contains",
            "value": None,
            "note": "list membership (§8)",
        },
        {
            "q": 6,
            "node": "demographic",
            "field": COL_REL0_ID,
            "op": "=",
            "value": demo[COL_REL0_ID]["min"],
            "note": "edge traversal (§8)",
        },
    ]


def candidate_pages(ci, oi, physical_type, op, value):
    """Evaluate the predicate against per-page bounds - the doc's actual metric."""
    hits, total_bytes, cand_bytes = [], 0, 0
    n = ci["n_pages"]
    for i in range(n):
        size = oi["page_locations"][i]["compressed_page_size"]
        total_bytes += size
        if ci["null_pages"][i]:
            continue
        lo = decode_bound(ci["min_values"][i], physical_type)
        hi = decode_bound(ci["max_values"][i], physical_type)
        if lo is None or hi is None:
            keep = True
        elif op == "=":
            keep = lo <= value <= hi
        elif op == ">":
            keep = hi > value
        elif op == "<":
            keep = lo < value
        else:
            keep = True
        if keep:
            hits.append(i)
            cand_bytes += size
    return {
        "pages_total": n,
        "pages_candidate": len(hits),
        "page_bytes_total": total_bytes,
        "page_bytes_candidate": cand_bytes,
    }


def candidate_row_groups(footer, column, op, value):
    total = cand = 0
    for rg in footer["row_groups"]:
        for ch in rg["columns"]:
            cm = ch["meta_data"]
            if cm["path_in_schema"] != column:
                continue
            total += 1
            st = cm.get("statistics") or {}
            lo = decode_bound(st.get("min_value"), cm["physical_type"])
            hi = decode_bound(st.get("max_value"), cm["physical_type"])
            if lo is None or hi is None:
                cand += 1
            elif op == "=":
                cand += bool(lo <= value <= hi)
            elif op == ">":
                cand += bool(hi > value)
            elif op == "<":
                cand += bool(lo < value)
            else:
                cand += 1
    return {"rowgroups_total": total, "rowgroups_candidate": cand}


def measure(
    table, query, page_size, row_group_size, do_sort, caps, use_dictionary=True
):
    col = query["field"]
    leaf = col
    row = {
        "q": query["q"],
        "node": query["node"],
        "field": col,
        "op": query["op"],
        "data_page_size": page_size,
        "row_group_size": row_group_size or "all",
        "sorted": do_sort,
        "use_dictionary": use_dictionary,
        "note": query["note"],
    }
    if col not in table.schema.names:
        row["error"] = "column absent"
        return row

    tbl = table
    if do_sort:
        try:
            tbl = table.sort_by(col)
        except (pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
            row["error"] = f"not sortable: {str(e)[:60]}"
            return row

    opts = writer_options(
        tbl.schema,
        caps,
        compression="zstd",
        data_page_size=page_size,
        ndv=max(tbl.num_rows, 1),
    )
    opts["use_dictionary"] = use_dictionary
    sink = pa.BufferOutputStream()
    with pq.ParquetWriter(sink, tbl.schema, **opts) as w:
        w.write_table(tbl, row_group_size=row_group_size or tbl.num_rows)
    blob = sink.getvalue().to_pybytes()
    row["file_bytes"] = len(blob)

    md = pq.ParquetFile(pa.BufferReader(blob)).metadata
    ok, probs = cross_validate(blob, md)
    if not ok:
        row["error"] = f"decoder cross-validation FAILED: {probs[:2]}"
        return row
    footer = read_footer(blob)

    # Angle B - what pruning was possible, per data page.
    if query["op"] in ("=", ">", "<"):
        agg = {
            "pages_total": 0,
            "pages_candidate": 0,
            "page_bytes_total": 0,
            "page_bytes_candidate": 0,
        }
        physical = None
        for rg in footer["row_groups"]:
            for ch in rg["columns"]:
                cm = ch["meta_data"]
                if cm["path_in_schema"] != leaf:
                    continue
                physical = cm["physical_type"]
                ci, oi = read_column_index(blob, ch), read_offset_index(blob, ch)
                if ci and oi:
                    c = candidate_pages(ci, oi, physical, query["op"], query["value"])
                    for k in agg:
                        agg[k] += c[k]
        row.update(agg)
        if agg["pages_total"]:
            row["pruning_available_pct"] = round(
                100 * (1 - agg["pages_candidate"] / agg["pages_total"]), 2
            )
        row.update(candidate_row_groups(footer, leaf, query["op"], query["value"]))

    # Angle A - what pyarrow actually read, with calibration.
    col_bytes = column_compressed_bytes(md, leaf) or 1
    row["column_compressed_bytes"] = col_bytes
    reads = {}
    for label, filters in (
        ("unfiltered", None),
        (
            "filtered",
            (
                [(col, query["op"], query["value"])]
                if query["op"] in ("=", ">", "<")
                else None
            ),
        ),
    ):
        if filters is None and label == "filtered":
            continue
        r = CountingReader(blob)
        t0 = time.perf_counter()
        try:
            tt = pq.read_table(r, columns=[col], filters=filters, pre_buffer=False)
            reads[label] = {
                "bytes": r.bytes_read,
                "rows": tt.num_rows,
                "ms": round((time.perf_counter() - t0) * 1000, 2),
            }
        except Exception as e:
            reads[label] = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
    row["bytes_read_unfiltered"] = reads.get("unfiltered", {}).get("bytes")
    row["bytes_read_filtered"] = reads.get("filtered", {}).get("bytes")
    row["rows_matched"] = reads.get("filtered", {}).get("rows")
    row["wall_ms"] = reads.get("filtered", {}).get("ms")

    bu = row["bytes_read_unfiltered"]
    if bu:
        row["calibration_read_over_column"] = round(bu / col_bytes, 2)
        row["calibrated"] = bu < col_bytes * 2.0
        if row["calibrated"] and row.get("bytes_read_filtered"):
            row["pruning_realized_pct"] = round(
                100 * (1 - row["bytes_read_filtered"] / bu), 2
            )
            if row.get("pruning_available_pct") is not None:
                row["gap_pct"] = round(
                    row["pruning_available_pct"] - row["pruning_realized_pct"], 2
                )
        else:
            row["pruning_realized_pct"] = None
            row["calibration_note"] = (
                "footer/page-index bytes swamp this column; Angle A cannot "
                "resolve pruning here at any file size"
            )
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    args = ap.parse_args()
    outdir = pathlib.Path(args.outdir)
    caps = load_capabilities(outdir)
    stats = json.loads((outdir / "step1_column_stats.json").read_text())
    queries = build_queries(stats)

    rows = []
    for query in queries:
        table = read_cached(outdir / "raw", query["node"])
        for page_size in PAGE_SIZES:
            for rg in ROW_GROUPS:
                for do_sort in (False, True):
                    rows.append(measure(table, query, page_size, rg, do_sort, caps))
        # Dictionary encoding is what collapses page granularity: data pages hold
        # 2-byte dictionary indices, so thousands of rows fit in one page. Test
        # the counterfactual at the finest page size.
        for do_sort in (False, True):
            rows.append(
                measure(table, query, 512, 512, do_sort, caps, use_dictionary=False)
            )

    fields = sorted({k for r in rows for k in r})
    with (outdir / "step5_explain.csv").open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        wr.writerows(rows)
    write_json(outdir / "step5_explain.json", rows)

    print(
        f"{'q':>2} {'field':22} {'pg':>6} {'rg':>5} {'srt':>4} {'dict':>5} "
        f"{'pages':>11} {'avail%':>7} {'real%':>7} {'cal':>4}"
    )
    for r in rows:
        if r.get("error"):
            print(f"{r['q']:>2} {r['field'][:22]:22} -> {r['error'][:60]}")
            continue
        pages = f"{r.get('pages_candidate','?')}/{r.get('pages_total','?')}"
        print(
            f"{r['q']:>2} {r['field'][:22]:22} {r['data_page_size']:>6} "
            f"{str(r['row_group_size']):>5} {str(r['sorted'])[:4]:>4} "
            f"{str(r['use_dictionary'])[:5]:>5} {pages:>11} "
            f"{str(r.get('pruning_available_pct','-')):>7} "
            f"{str(r.get('pruning_realized_pct','-')):>7} "
            f"{str(r.get('calibrated','-'))[:4]:>4}"
        )
    print(f"\n{outdir / 'step5_explain.csv'}")


if __name__ == "__main__":
    main()
