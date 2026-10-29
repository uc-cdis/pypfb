#!/usr/bin/env python
"""PoC step 4: the index manifest (§4.3), measured as a percentage of the PFB.

Gate 3: "Manifest > 5% of PFB size -> tighten --domain-cap before anything else."

Implements §3.1's cardinality guard. The point worth testing is the asymmetry
claim: only `domain` fields add bytes, and they are capped, so manifest size is
bounded by field count and the cap - never by record count.
"""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import (  # noqa: E402
    AVRO_EXPORT_BYTES,
    SYNTHETIC_PREFIX,
    cached_nodes,
    read_cached,
    write_json,
)

NUMERIC_PREFIXES = ("int", "float", "double", "halffloat", "decimal")
ID_HINTS = ("submitter_id", "participant_id", "specimen_id", "object_id", "id")


def _strategy(name, entry, domain_cap):
    """§3.1: one strategy per field, recorded in the manifest."""
    if not entry["non_null"]:
        return "none"
    distinct = entry.get("distinct")
    if distinct is None:  # nested (list) column - no unique() support
        return "unsupported"
    if distinct <= domain_cap:
        return "domain"
    t = entry["type"]
    if any(t.startswith(p) for p in NUMERIC_PREFIXES) or "timestamp" in t:
        return "range"
    return "bloom"  # high-cardinality string: Parquet bloom only


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    ap.add_argument("--domain-cap", type=int, default=256)
    ap.add_argument("--pfb-bytes", type=int, default=AVRO_EXPORT_BYTES)
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    stats = json.loads((outdir / "step1_column_stats.json").read_text())
    parquet_ref = outdir / "parquet_ref"

    manifest = {
        "version": 1,
        "built_by": "pypfb-poc",
        "enum_values": "decoded",
        "parquet": {
            "embedded": True,
            "compression": "zstd",
            "page_index": True,
            "data_page_size": 65536,
            "nodes": {},
        },
        "domains": {},
        "ranges": {},
        "strategies": {},
    }
    counts = {}
    for node in cached_nodes(outdir / "raw"):
        table = read_cached(outdir / "raw", node)
        pq_path = parquet_ref / f"{node}.parquet"
        manifest["parquet"]["nodes"][node] = {
            "rows": table.num_rows,
            "bytes": pq_path.stat().st_size if pq_path.exists() else None,
        }
        for field, entry in stats[node].items():
            if field.startswith(SYNTHETIC_PREFIX):
                continue
            strat = _strategy(field, entry, args.domain_cap)
            counts[strat] = counts.get(strat, 0) + 1
            key = f"{node}.{field}"
            manifest["strategies"][key] = strat
            if strat == "domain":
                manifest["domains"][key] = entry.get("domain", [])
            elif strat == "range":
                manifest["ranges"][key] = {
                    "min": entry.get("min"),
                    "max": entry.get("max"),
                }

    blob = json.dumps(manifest, separators=(",", ":"), default=str)
    write_json(outdir / "step4_manifest.json", manifest)
    (outdir / "step4_manifest.min.json").write_text(blob)

    # Strategies-only variant: what the manifest costs if domains are dropped and
    # every high-cardinality field relies on the Parquet payload's own bloom.
    lean = dict(manifest, domains={}, ranges={})
    lean_blob = json.dumps(lean, separators=(",", ":"), default=str)

    summary = {
        "pfb_bytes": args.pfb_bytes,
        "manifest_bytes": len(blob.encode()),
        "manifest_pct_of_pfb": round(100 * len(blob.encode()) / args.pfb_bytes, 4),
        "lean_manifest_bytes": len(lean_blob.encode()),
        "domains_count": len(manifest["domains"]),
        "ranges_count": len(manifest["ranges"]),
        "strategy_counts": counts,
        "largest_domains": sorted(
            ((len(v), k) for k, v in manifest["domains"].items()), reverse=True
        )[:8],
    }
    write_json(outdir / "step4_summary.json", summary)

    print(
        f"manifest {summary['manifest_bytes']:,} bytes = "
        f"{summary['manifest_pct_of_pfb']:.3f}% of the {args.pfb_bytes:,}-byte PFB"
    )
    print(f"  (gate 3 threshold is 5%)")
    print(f"strategies: {counts}")
    print(f"domains {summary['domains_count']}, ranges {summary['ranges_count']}")
    print("largest domains:")
    for n, k in summary["largest_domains"]:
        print(f"    {n:>4} values  {k}")


if __name__ == "__main__":
    main()
