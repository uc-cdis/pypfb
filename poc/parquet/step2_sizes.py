#!/usr/bin/env python
"""PoC step 2: the size table. THE GATE for the whole proposal (§7, gate 1).

    "Parquet total > Avro size -> --embed-parquet stays off by default."

Built entirely off step 1's Arrow cache, so no Avro rescan.

Two things this measures that §7 does not ask for but a reviewer will:

1. The index tax, by DIFFERENCING. pyarrow 26 exposes no ColumnIndex/OffsetIndex
   accessor, so the page index cannot be priced directly - but writing the same
   table with write_page_index on and off and subtracting gives it exactly, with
   no Thrift decoding. Same trick for bloom filters.

2. Compressed-Avro baselines. §2.2 is right that current PFBs are written
   uncompressed, but comparing compressed columnar against UNCOMPRESSED
   row-oriented flatters the proposal. If codec="deflate" alone shrinks the PFB
   more than the Parquet copies add, gate 1's honest framing changes.
"""
import argparse
import csv
import io
import pathlib
import sys
import time

import fastavro
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import (  # noqa: E402
    cached_nodes,
    load_capabilities,
    read_cached,
    write_json,
    writer_options,
)

COMPRESSIONS = ("none", "snappy", "zstd")
PAGE_SIZES = (8 * 1024, 64 * 1024, 1024 * 1024)
ROW_GROUPS = (512, 4096, None)  # None = one row group per node
REFERENCE = ("zstd", 64 * 1024, 4096)  # where the differential cells are taken


def _write_cell(
    tables, cellpath, compression, page_size, row_group_size, page_index, bloom, caps
):
    cellpath.mkdir(parents=True, exist_ok=True)
    total = 0
    per_node = {}
    for node, table in tables.items():
        opts = writer_options(
            table.schema,
            caps,
            compression=compression,
            data_page_size=page_size,
            bloom=bloom,
            ndv=max(table.num_rows, 1),
        )
        if not page_index:
            opts["write_page_index"] = False
        path = cellpath / f"{node}.parquet"
        with pq.ParquetWriter(path, table.schema, **opts) as w:
            w.write_table(table, row_group_size=row_group_size or table.num_rows)
        size = path.stat().st_size
        md = pq.ParquetFile(path).metadata
        chunk_bytes = sum(
            md.row_group(i).column(c).total_compressed_size
            for i in range(md.num_row_groups)
            for c in range(md.row_group(i).num_columns)
        )
        per_node[node] = {
            "bytes": size,
            "rows": md.num_rows,
            "columns": md.num_columns,
            "row_groups": md.num_row_groups,
            "data_bytes": chunk_bytes,
            # Everything that is not a column chunk: footer + page index + blooms.
            "overhead_bytes": size - chunk_bytes,
        }
        total += size
    return total, per_node


def _avro_baselines(input_path, outdir):
    """Re-encode the source PFB with each fastavro codec that is available."""
    out = {}
    src = pathlib.Path(input_path)
    out["null (as shipped)"] = src.stat().st_size
    with src.open("rb") as f:
        r = fastavro.reader(f)
        schema = r.writer_schema
        records = list(r)
    for codec in ("deflate", "bzip2", "xz", "zstandard", "snappy"):
        path = pathlib.Path(outdir) / f"avro_{codec}.avro"
        try:
            with path.open("wb") as f:
                fastavro.writer(f, schema, records, codec=codec)
            out[codec] = path.stat().st_size
        except Exception as e:
            out[codec] = f"unavailable: {type(e).__name__}: {str(e)[:80]}"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="export_2026-08-06T10_36_03.avro")
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    ap.add_argument(
        "--keep-files",
        action="store_true",
        help="Keep every grid cell's parquet files (hundreds of MB).",
    )
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    caps = load_capabilities(outdir)
    rawdir = outdir / "raw"
    nodes = cached_nodes(rawdir)
    if not nodes:
        raise SystemExit("no cache - run step1_stage.py first")
    tables = {n: read_cached(rawdir, n) for n in nodes}
    avro_bytes = pathlib.Path(args.input).stat().st_size

    griddir = outdir / "step2_grid"
    rows = []
    t0 = time.time()

    # Main grid, page index and blooms both on (the §1.2 recommended shape).
    for comp in COMPRESSIONS:
        for page in PAGE_SIZES:
            for rg in ROW_GROUPS:
                label = f"{comp}_p{page}_rg{rg or 'all'}"
                total, per_node = _write_cell(
                    tables, griddir / label, comp, page, rg, True, True, caps
                )
                rows.append(
                    {
                        "cell": label,
                        "compression": comp,
                        "data_page_size": page,
                        "row_group_size": rg or "all",
                        "page_index": True,
                        "bloom": True,
                        "total_bytes": total,
                        "ratio_vs_avro": round(total / avro_bytes, 4),
                        "data_bytes": sum(v["data_bytes"] for v in per_node.values()),
                        "overhead_bytes": sum(
                            v["overhead_bytes"] for v in per_node.values()
                        ),
                        "row_groups": sum(v["row_groups"] for v in per_node.values()),
                    }
                )
                write_json(griddir / label / "_per_node.json", per_node)

    # Differential cells: isolate the page-index and bloom cost at the reference
    # configuration by turning each off in turn.
    comp, page, rg = REFERENCE
    for page_index, bloom in (
        (True, True),
        (False, True),
        (True, False),
        (False, False),
    ):
        label = f"diff_pi{int(page_index)}_bf{int(bloom)}"
        total, per_node = _write_cell(
            tables, griddir / label, comp, page, rg, page_index, bloom, caps
        )
        rows.append(
            {
                "cell": label,
                "compression": comp,
                "data_page_size": page,
                "row_group_size": rg,
                "page_index": page_index,
                "bloom": bloom,
                "total_bytes": total,
                "ratio_vs_avro": round(total / avro_bytes, 4),
                "data_bytes": sum(v["data_bytes"] for v in per_node.values()),
                "overhead_bytes": sum(v["overhead_bytes"] for v in per_node.values()),
                "row_groups": sum(v["row_groups"] for v in per_node.values()),
            }
        )

    baselines = _avro_baselines(args.input, outdir)

    with (outdir / "step2_sizes.csv").open("w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0]))
        wr.writeheader()
        wr.writerows(rows)
    write_json(outdir / "step2_baselines.json", baselines)

    by_cell = {r["cell"]: r for r in rows}
    summary = {
        "avro_bytes": avro_bytes,
        "avro_baselines": baselines,
        "best_cell": min(rows, key=lambda r: r["total_bytes"]),
        "worst_cell": max(rows, key=lambda r: r["total_bytes"]),
        "page_index_cost_bytes": by_cell["diff_pi1_bf1"]["total_bytes"]
        - by_cell["diff_pi0_bf1"]["total_bytes"],
        "bloom_cost_bytes": by_cell["diff_pi1_bf1"]["total_bytes"]
        - by_cell["diff_pi1_bf0"]["total_bytes"],
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    write_json(outdir / "step2_summary.json", summary)

    print(f"Avro as shipped: {avro_bytes:,} bytes\n")
    print("-- Avro re-encoded with a codec (the comparison §7 omits) --")
    for k, v in baselines.items():
        if isinstance(v, int):
            print(f"  {k:18} {v:>12,}  ({v / avro_bytes:.2%} of as-shipped)")
        else:
            print(f"  {k:18} {v}")
    print("\n-- Parquet grid, total across 14 nodes --")
    print(f"  {'cell':28} {'bytes':>12} {'vs avro':>9} {'overhead':>11}")
    for r in sorted(rows, key=lambda r: r["total_bytes"]):
        if r["cell"].startswith("diff_"):
            continue
        print(
            f"  {r['cell']:28} {r['total_bytes']:>12,} "
            f"{r['ratio_vs_avro']:>8.1%} {r['overhead_bytes']:>11,}"
        )
    print(
        "\n-- Index tax, by differencing at "
        f"{REFERENCE[0]}/p{REFERENCE[1]}/rg{REFERENCE[2]} --"
    )
    for label in ("diff_pi1_bf1", "diff_pi0_bf1", "diff_pi1_bf0", "diff_pi0_bf0"):
        r = by_cell[label]
        print(
            f"  page_index={str(r['page_index']):5} bloom={str(r['bloom']):5} "
            f"{r['total_bytes']:>12,}"
        )
    print(f"  => page index costs {summary['page_index_cost_bytes']:,} bytes")
    print(f"  => bloom filters cost {summary['bloom_cost_bytes']:,} bytes")

    if not args.keep_files:
        import shutil

        shutil.rmtree(griddir / "step2_grid", ignore_errors=True)
    print(f"\n{outdir / 'step2_sizes.csv'}  ({summary['elapsed_seconds']}s)")


if __name__ == "__main__":
    main()
