#!/usr/bin/env python
"""PoC step 6b: read the indexed PFB with master's PFBReader. GATE 4.

    "Any consumer in §4.2 breaks on the new union member -> fall back to
     shipping tiers 0-2 only."

Run with PYTHONPATH pointing at a master worktree's src/. The install's
pypfb.pth APPENDS to sys.path while PYTHONPATH PREPENDS, so the worktree wins -
but that is asserted at runtime rather than trusted.

§4.2 anticipates breakage in consumers that switch on record["name"]. This also
checks a quieter one: PFBReader.__enter__ skips exactly ONE union member before
treating the rest as node schemas, so any added member lands in reader.schema as
if it were a node.
"""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import write_json  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orig", default="export_2026-08-06T10_36_03.avro")
    ap.add_argument("--indexed", required=True)
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    ap.add_argument(
        "--expect-worktree", default=None, help="Assert pfb is imported from this path."
    )
    args = ap.parse_args()

    import pfb
    from pfb.reader import PFBReader

    pfb_path = str(pathlib.Path(pfb.__file__).resolve())
    if args.expect_worktree:
        expected = str(pathlib.Path(args.expect_worktree).resolve())
        if not pfb_path.startswith(expected):
            raise SystemExit(
                f"FATAL: pfb imported from {pfb_path}, not the worktree "
                f"{expected}. PYTHONPATH did not shadow the install."
            )

    findings = {"pfb_module": pfb_path}

    # 1. Original, as the reference.
    with PFBReader(args.orig) as r:
        orig_schema = [n["name"] for n in r.schema]
        orig_meta = json.dumps(r.metadata, sort_keys=True, default=str)
        orig_records = [
            (
                x["id"],
                x["name"],
                json.dumps(x["object"], sort_keys=True, default=str),
                json.dumps(x["relations"], sort_keys=True, default=str),
            )
            for x in r
        ]

    # 2. Indexed, read by this (master) reader.
    node_parquet_records = []
    with PFBReader(args.indexed) as r:
        idx_schema = [n["name"] for n in r.schema]
        idx_meta = json.dumps(r.metadata, sort_keys=True, default=str)
        idx_records = []
        for i, x in enumerate(r):
            if x["name"] == "NodeParquet":
                node_parquet_records.append(i)
                continue
            idx_records.append(
                (
                    x["id"],
                    x["name"],
                    json.dumps(x["object"], sort_keys=True, default=str),
                    json.dumps(x["relations"], sort_keys=True, default=str),
                )
            )
        raw_meta = getattr(r._reader, "metadata", {})

    findings["crashed_on_iteration"] = False
    findings["original_record_count"] = len(orig_records)
    findings["indexed_data_record_count"] = len(idx_records)
    findings["node_parquet_records_surfaced"] = len(node_parquet_records)
    findings["node_parquet_first_index"] = (
        node_parquet_records[0] if node_parquet_records else None
    )
    findings["records_identical"] = orig_records == idx_records
    if not findings["records_identical"]:
        diffs = [(a, b) for a, b in zip(orig_records, idx_records) if a != b]
        findings["first_record_diff"] = str(diffs[0])[:400] if diffs else "length"
    findings["metadata_identical"] = orig_meta == idx_meta

    # The quieter break: does the new union member appear as a node schema?
    findings["original_schema_nodes"] = len(orig_schema)
    findings["indexed_schema_nodes"] = len(idx_schema)
    findings["NodeParquet_in_reader_schema"] = "NodeParquet" in idx_schema
    findings["schema_nodes_added"] = sorted(set(idx_schema) - set(orig_schema))

    # Header manifest readable without any Parquet reader.
    key = "pfb.index.v1.manifest"
    raw = raw_meta.get(key)
    findings["manifest_in_header"] = raw is not None
    if raw:
        m = json.loads(raw)
        findings["manifest_bytes"] = len(raw)
        findings["manifest_keys"] = sorted(m)
        findings["manifest_nodes"] = len(m.get("parquet", {}).get("nodes", {}))

    write_json(pathlib.Path(args.outdir) / "step6_verify.json", findings)

    print(f"pfb imported from: {pfb_path}\n")
    for k in (
        "crashed_on_iteration",
        "original_record_count",
        "indexed_data_record_count",
        "records_identical",
        "metadata_identical",
        "node_parquet_records_surfaced",
        "node_parquet_first_index",
        "original_schema_nodes",
        "indexed_schema_nodes",
        "NodeParquet_in_reader_schema",
        "schema_nodes_added",
        "manifest_in_header",
        "manifest_bytes",
        "manifest_nodes",
    ):
        if k in findings:
            print(f"  {k:34} {findings[k]}")


if __name__ == "__main__":
    main()
