#!/usr/bin/env python
"""PoC step 1: PFB -> per-node Arrow, plus the schema/cardinality report.

Decides, per docs/parquet-search-index.md §7: whether the Avro->Arrow mapping has
gaps, and whether multi-branch unions occur in practice. (They do: 36 of them.)

Scans the Avro exactly once, writing out/raw/<node>.arrow as the cache for every
later step, then a reference Parquet set at the §1.2 recommended settings.
"""
import argparse
import collections
import json
import pathlib
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import (  # noqa: E402
    COL_ID,
    ArrowStager,
    UnsupportedPFBType,
    cached_nodes,
    load_capabilities,
    node_to_arrow_schema,
    read_cached,
    write_json,
    writer_options,
)

from pfb.reader import PFBReader  # noqa: E402


def _strict_union_report(schema):
    """Reproduce the doc's §5.2 behaviour to enumerate what it would reject."""
    failures = []
    for node in schema:
        try:
            node_to_arrow_schema(node, strict_unions=True)
        except UnsupportedPFBType as e:
            failures.append(str(e))
    return failures


def _type_census(schema):
    census = collections.Counter()
    for node in schema:
        for f in node["fields"]:
            types = f["type"] if isinstance(f["type"], list) else [f["type"]]
            for t in types:
                if t == "null":
                    continue
                census[t if isinstance(t, str) else t.get("type")] += 1
    return dict(census)


def _column_stats(table, domain_cap):
    """Per-column non-null / distinct / min / max, computed with Arrow compute.

    distinct_count is NOT read from the Parquet footer: §1.3 is correct that most
    writers leave it unset, and pyarrow 26 confirms it comes back None. It has to
    be computed over the column.
    """
    out = {}
    for name in table.schema.names:
        col = table.column(name)
        entry = {
            "type": str(col.type),
            "non_null": col.length() - col.null_count,
            "null_count": col.null_count,
        }
        if entry["non_null"] and not pa.types.is_nested(col.type):
            try:
                uniq = pc.unique(col.combine_chunks())
                entry["distinct"] = len(uniq)
                if entry["distinct"] <= domain_cap:
                    vals = [v for v in uniq.to_pylist() if v is not None]
                    entry["domain"] = sorted(vals, key=lambda x: (x is None, x))
                mn, mx = pc.min_max(col).as_py().values()
                entry["min"], entry["max"] = mn, mx
            except pa.ArrowNotImplementedError as e:
                entry["stats_error"] = str(e)[:120]
        out[name] = entry
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="export_2026-08-06T10_36_03.avro")
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    ap.add_argument("--batch-size", type=int, default=4096)
    ap.add_argument("--domain-cap", type=int, default=256)
    ap.add_argument(
        "--strict-unions",
        action="store_true",
        help="Reproduce the doc's §5.2 raise. Expected to abort.",
    )
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    rawdir = outdir / "raw"
    caps = load_capabilities(outdir)

    if rawdir.exists() and cached_nodes(rawdir) and not args.force:
        print(
            f"cache present at {rawdir} ({len(cached_nodes(rawdir))} nodes); "
            f"--force to rebuild"
        )
    else:
        t0 = time.time()
        with PFBReader(args.input) as reader:
            schema = reader.schema
            report = {
                "input": args.input,
                "input_bytes": pathlib.Path(args.input).stat().st_size,
                "declared_nodes": len(schema),
                "type_census": _type_census(schema),
                "strict_union_failures": _strict_union_report(schema),
            }
            with ArrowStager(
                schema,
                rawdir,
                batch_size=args.batch_size,
                strict_unions=args.strict_unions,
            ) as stager:
                n = 0
                for record in reader:
                    stager.add(record)
                    n += 1
            report.update(
                {
                    "data_records": n,
                    "populated_nodes": len(stager.row_counts),
                    "row_counts": dict(sorted(stager.row_counts.items())),
                    "relations_arity": dict(sorted(stager.arity.items())),
                    "null_ids": stager.null_ids,
                    "union_collapse_notes": stager.notes,
                    "metadata_nodes": len(reader.metadata.get("nodes", [])),
                    "scan_seconds": round(time.time() - t0, 2),
                }
            )
        write_json(outdir / "step1_report.json", report)
        print(f"staged {n} records -> {rawdir} in {report['scan_seconds']}s")

    # Per-column statistics off the cache, and a reference Parquet set.
    stats, pqdir = {}, outdir / "parquet_ref"
    pqdir.mkdir(parents=True, exist_ok=True)
    for node in cached_nodes(rawdir):
        table = read_cached(rawdir, node)
        stats[node] = _column_stats(table, args.domain_cap)
        opts = writer_options(table.schema, caps, ndv=max(table.num_rows, 1))
        with pq.ParquetWriter(pqdir / f"{node}.parquet", table.schema, **opts) as w:
            w.write_table(table, row_group_size=args.batch_size)
    write_json(outdir / "step1_column_stats.json", stats)

    rep = json.loads((outdir / "step1_report.json").read_text())
    print(
        f"\ndeclared nodes {rep['declared_nodes']}, populated "
        f"{rep['populated_nodes']}, records {rep['data_records']}"
    )
    print(f"type census: {rep['type_census']}")
    print(f"relations arity: {rep['relations_arity']}  null ids: {rep['null_ids']}")
    print(
        f"unions collapsed: {len(rep['union_collapse_notes'])}  "
        f"(doc's strict raise would reject {len(rep['strict_union_failures'])})"
    )
    print(f"\nreference parquet -> {pqdir}")
    d = stats.get("demographic", {})
    interesting = [
        (k, v["non_null"], v.get("distinct")) for k, v in d.items() if v["non_null"]
    ]
    print(f"demographic: {len(d)} cols, {len(interesting)} populated")
    for k, nn, dc in interesting:
        print(f"    {k:28} non_null={nn:<6} distinct={dc}")


if __name__ == "__main__":
    main()
