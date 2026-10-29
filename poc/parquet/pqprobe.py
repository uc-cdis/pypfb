#!/usr/bin/env python
"""Resolve the pyarrow API uncertainties in docs/parquet-search-index.md before
any writer configuration is chosen.

The design doc states several pyarrow behaviours as fact (bloom_filter_options on
ParquetWriter "via **options", page-level predicate pushdown, "--explain reports
pages read vs skipped straight from the page index"). None of those were verified
against a real install. This script generates the capability table rather than
hand-maintaining it, and writes out/capabilities.json, which every later step
reads to skip configurations this version cannot do.
"""
import argparse
import inspect
import io
import json
import os
import pathlib
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from poc_common import CountingReader, column_compressed_bytes

# A column per type we actually need, plus enough rows that a tiny data_page_size
# produces more than one page (which is what the page-index probes need).
_N = 2000


def _sample_table():
    return pa.table(
        {
            "__pfb_id": [f"id-{i:05d}" for i in range(_N)],
            "population": [["CEU", "ASW", "ACB", "YRI"][i % 4] for i in range(_N)],
            "annotated_sex": [["female", "male"][i % 2] for i in range(_N)],
            "age": [float(i % 90) for i in range(_N)],
            "flag": [i % 2 == 0 for i in range(_N)],
            "codes": [["a", "b"] for _ in range(_N)],
            "all_null": pa.nulls(_N, pa.string()),
        }
    )


# (kwarg, value) pairs the doc depends on. SENTINEL values are filled in at runtime
# because they need the schema.
CANDIDATE_WRITER_KWARGS = [
    ("compression", "zstd"),
    ("compression_level", "SENTINEL_ZSTD_LEVEL"),
    ("use_dictionary", True),
    ("write_statistics", True),
    ("write_page_index", True),
    ("data_page_size", 8 * 1024),
    ("store_schema", True),
    ("bloom_filter_options", {"__pfb_id": {"ndv": _N, "fpp": 0.01}}),
    ("write_bloom_filter", True),
    ("sorting_columns", "SENTINEL_SORTING"),
    ("store_decimal_as_integer", True),
]

CANDIDATE_READ_KWARGS = [
    ("pre_buffer", False),
    ("buffer_size", 0),
    ("use_threads", False),
    ("memory_map", False),
]


def _probe_writer_kwargs(table, tmpdir):
    """Construct a real ParquetWriter per kwarg and record what happens.

    Accepting a kwarg at construction time is necessary but not sufficient - some
    are silently swallowed by **options - so we also write a table and close, which
    is where a bad value actually surfaces.
    """
    results = {}
    for name, value in CANDIDATE_WRITER_KWARGS:
        extra = {}
        if value == "SENTINEL_SORTING":
            value = [pq.SortingColumn(0)]
        if value == "SENTINEL_ZSTD_LEVEL":
            value, extra = 9, {"compression": "zstd"}
        path = os.path.join(tmpdir, f"probe_{name}.parquet")
        try:
            with pq.ParquetWriter(path, table.schema, **{name: value}, **extra) as w:
                w.write_table(table)
            results[name] = {
                "accepted": True,
                "error": None,
                "bytes": os.path.getsize(path),
            }
        except Exception as e:
            results[name] = {
                "accepted": False,
                "error": f"{type(e).__name__}: {e}"[:300],
            }
    return results


def _probe_write_table_kwargs(table, tmpdir):
    results = {}
    for name, value in CANDIDATE_WRITER_KWARGS:
        extra = {}
        if value == "SENTINEL_SORTING":
            value = [pq.SortingColumn(0)]
        if value == "SENTINEL_ZSTD_LEVEL":
            value, extra = 9, {"compression": "zstd"}
        path = os.path.join(tmpdir, f"probe_wt_{name}.parquet")
        try:
            pq.write_table(table, path, **{name: value}, **extra)
            results[name] = {
                "accepted": True,
                "error": None,
                "bytes": os.path.getsize(path),
            }
        except Exception as e:
            results[name] = {
                "accepted": False,
                "error": f"{type(e).__name__}: {e}"[:300],
            }
    return results


def _probe_page_index_api(table, tmpdir):
    """The decisive question for step 5 Angle B: can Python read ColumnIndex /
    OffsetIndex at all? If not, we need a Thrift footer decoder."""
    path = os.path.join(tmpdir, "page_index.parquet")
    pq.write_table(
        table,
        path,
        write_page_index=True,
        data_page_size=8 * 1024,
        compression="zstd",
        row_group_size=512,
    )
    pf = pq.ParquetFile(path)
    md = pf.metadata
    rg0 = md.row_group(0)
    col0 = rg0.column(0)

    out = {
        "num_row_groups": md.num_row_groups,
        "num_rows": md.num_rows,
        "num_columns": md.num_columns,
        "ParquetFile_attrs": sorted(a for a in dir(pf) if not a.startswith("_")),
        "FileMetaData_attrs": sorted(a for a in dir(md) if not a.startswith("_")),
        "RowGroupMetaData_attrs": sorted(a for a in dir(rg0) if not a.startswith("_")),
        "ColumnChunkMetaData_attrs": sorted(
            a for a in dir(col0) if not a.startswith("_")
        ),
    }

    # Named accessors we hope exist, each tried individually.
    for probe, call in {
        "ParquetFile.read_column_index": lambda: pf.read_column_index(0, 0),
        "ParquetFile.read_offset_index": lambda: pf.read_offset_index(0, 0),
        "ParquetFile.column_index": lambda: pf.column_index(0, 0),
        "ParquetFile.offset_index": lambda: pf.offset_index(0, 0),
        "ColumnChunkMetaData.column_index_offset": lambda: col0.column_index_offset,
        "ColumnChunkMetaData.column_index_length": lambda: col0.column_index_length,
        "ColumnChunkMetaData.offset_index_offset": lambda: col0.offset_index_offset,
        "ColumnChunkMetaData.offset_index_length": lambda: col0.offset_index_length,
        "ColumnChunkMetaData.has_bloom_filter": lambda: col0.has_bloom_filter,
        "ColumnChunkMetaData.bloom_filter_offset": lambda: col0.bloom_filter_offset,
        "ColumnChunkMetaData.statistics.distinct_count": lambda: col0.statistics.distinct_count,
        "ColumnChunkMetaData.total_compressed_size": lambda: col0.total_compressed_size,
        "ColumnChunkMetaData.num_values": lambda: col0.num_values,
        "ColumnChunkMetaData.path_in_schema": lambda: col0.path_in_schema,
    }.items():
        try:
            v = call()
            out[probe] = {"available": True, "value": repr(v)[:200]}
        except Exception as e:
            out[probe] = {"available": False, "error": f"{type(e).__name__}: {e}"[:200]}

    # pyarrow._parquet internals, in case the public surface is bare.
    try:
        import pyarrow._parquet as _p

        out["_parquet_module_members"] = sorted(
            a for a in dir(_p) if "ndex" in a or "loom" in a or "Sorting" in a
        )
    except Exception as e:
        out["_parquet_module_members"] = f"error: {e}"

    return out


def _probe_pushdown(tmpdir):
    """Does `filters=` prune at page level, or only at row group?

    This is §1.4's load-bearing claim ("Row-group and page skipping both apply").
    It is measured, not asserted, via byte accounting - but a byte counter is
    meaningless until calibrated. A small file is read whole by pyarrow regardless
    of the predicate, so we build a file large enough that one column's data
    dwarfs the footer and any read-ahead, then verify that an unfiltered read
    reconciles with the footer's total_compressed_size before believing any ratio.

    The file is sorted on the predicate column, so page bounds are maximally tight
    and pruning is maximally POSSIBLE. If pyarrow cannot prune here, it cannot
    prune anywhere, and the unsorted real-data case can only be worse.
    """
    n = 400_000
    tbl = pa.table(
        {
            "population": [["ACB", "ASW", "CEU", "YRI"][i % 4] for i in range(n)],
            "payload": [f"filler-value-{i:09d}" for i in range(n)],
        }
    ).sort_by("payload")

    # Two targets with opposite characteristics:
    #   payload    - high cardinality, sorted, compresses poorly -> column data
    #                dominates, so byte accounting is meaningful AND page bounds
    #                are perfectly tight. Best case for pruning.
    #   population - low cardinality, dictionary-encoded to ~1 KB, so the page
    #                index alone is tens of times larger than the data. Byte
    #                accounting cannot resolve pruning here at any file size.
    targets = [("payload", f"filler-value-{n // 2:09d}"), ("population", "CEU")]

    out = {"rows": n, "sorted_by": "payload"}
    for page_size in (8 * 1024, 64 * 1024):
        path = os.path.join(tmpdir, f"pushdown_{page_size}.parquet")
        pq.write_table(
            tbl,
            path,
            write_page_index=True,
            data_page_size=page_size,
            compression="zstd",
            row_group_size=50_000,
        )
        blob = pathlib.Path(path).read_bytes()
        md = pq.ParquetFile(pa.BufferReader(blob)).metadata

        for col, needle in targets:
            col_bytes = column_compressed_bytes(md, col)
            cell = {
                "file_bytes": len(blob),
                "num_row_groups": md.num_row_groups,
                "column_compressed_bytes": col_bytes,
                "predicate": f"{col} = {needle!r}",
            }
            for label, filters in [
                ("unfiltered", None),
                ("filtered", [(col, "=", needle)]),
            ]:
                r = CountingReader(blob)
                try:
                    t = pq.read_table(
                        r, columns=[col], filters=filters, pre_buffer=False
                    )
                    cell[label] = {
                        "bytes_read": r.bytes_read,
                        "rows": t.num_rows,
                        "n_reads": r.n_reads,
                    }
                except Exception as e:
                    cell[label] = {"error": f"{type(e).__name__}: {e}"[:300]}

            if "bytes_read" in cell["unfiltered"]:
                br = cell["unfiltered"]["bytes_read"]
                cell["calibration_read_over_column"] = round(br / col_bytes, 3)
                cell["calibration_read_over_file"] = round(br / len(blob), 3)
                # Calibrated when an unfiltered single-column read is dominated by
                # that column's own bytes rather than by footer/index/read-ahead.
                cell["calibrated"] = br < col_bytes * 2.0
                if cell["calibrated"] and "bytes_read" in cell["filtered"]:
                    ratio = cell["filtered"]["bytes_read"] / br
                    cell["pushdown_ratio"] = round(ratio, 4)
                    cell["verdict"] = (
                        "page-level pruning observed"
                        if ratio < 0.75
                        else "NO pruning beyond whole-column read"
                    )
                else:
                    cell["verdict"] = (
                        "UNCALIBRATED - footer/page-index bytes "
                        "swamp this column; ratio withheld"
                    )
            out[f"page{page_size}_{col}"] = cell
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    args = ap.parse_args()
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    table = _sample_table()
    caps = {
        "pyarrow_version": pa.__version__,
        "python_version": sys.version,
        "parquet_cpp_version": getattr(pa, "cpp_version", None),
        "ParquetWriter_signature": str(inspect.signature(pq.ParquetWriter.__init__)),
        "write_table_signature": str(inspect.signature(pq.write_table)),
        "read_table_signature": str(inspect.signature(pq.read_table)),
    }
    with tempfile.TemporaryDirectory() as tmp:
        caps["ParquetWriter_kwargs"] = _probe_writer_kwargs(table, tmp)
        caps["write_table_kwargs"] = _probe_write_table_kwargs(table, tmp)
        caps["page_index_api"] = _probe_page_index_api(table, tmp)
        caps["pushdown"] = _probe_pushdown(tmp)
        caps["read_kwargs"] = {}
        for name, value in CANDIDATE_READ_KWARGS:
            p = os.path.join(tmp, "rk.parquet")
            pq.write_table(table, p)
            try:
                pq.read_table(p, **{name: value})
                caps["read_kwargs"][name] = {"accepted": True}
            except Exception as e:
                caps["read_kwargs"][name] = {
                    "accepted": False,
                    "error": f"{type(e).__name__}: {e}"[:200],
                }

    (outdir / "capabilities.json").write_text(json.dumps(caps, indent=2, default=str))
    print(f"pyarrow {caps['pyarrow_version']}  ->  {outdir / 'capabilities.json'}")
    print("\n-- ParquetWriter kwargs --")
    for k, v in caps["ParquetWriter_kwargs"].items():
        print(f"  {'OK ' if v['accepted'] else 'NO '} {k:28} {v['error'] or ''}")
    print("\n-- write_table-only kwargs (accepted there, rejected on writer) --")
    for k, v in caps["write_table_kwargs"].items():
        if v["accepted"] and not caps["ParquetWriter_kwargs"][k]["accepted"]:
            print(f"  write_table-only: {k}")
    print("\n-- page index API --")
    for k, v in caps["page_index_api"].items():
        if isinstance(v, dict) and "available" in v:
            print(
                f"  {'OK ' if v['available'] else 'NO '} {k:52} "
                f"{v.get('value') or v.get('error', '')}"
            )
    print("\n-- pushdown --")
    for cell_name, cell in caps["pushdown"].items():
        if not isinstance(cell, dict):
            continue
        print(f"  [{cell_name}]")
        for k in (
            "file_bytes",
            "population_compressed_bytes",
            "unfiltered",
            "eq_CEU",
            "calibration_read_over_column",
            "calibration_read_over_file",
            "calibrated",
            "pushdown_ratio",
            "verdict",
        ):
            if k in cell:
                print(f"    {k:30} {cell[k]}")


if __name__ == "__main__":
    main()
