#!/usr/bin/env python
"""Does an indexed PFB actually answer a predicate? End-to-end demonstration.

Reads ONLY the embedded Parquet payload out of indexed.pfb and answers
"which records have population = CEU?" - then checks the answer against a
brute-force scan of the Avro records, which is what a consumer must do today.
"""
import argparse
import json
import pathlib
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from pfb.reader import PFBReader  # noqa: E402


def search_via_parquet(path, node, field, value):
    """Tier 3: pull one node's payload, push the predicate into Parquet."""
    t0 = time.perf_counter()
    payload = None
    with PFBReader(path) as reader:
        manifest = json.loads(
            reader._reader.metadata.get("pfb.index.v1.manifest", "{}")
        )
        for record in reader:
            if record["name"] == "NodeParquet" and record["object"]["node"] == node:
                payload = record["object"]["payload"]
                break
    if payload is None:
        raise SystemExit(f"no embedded payload for node {node!r}")
    extract_ms = (time.perf_counter() - t0) * 1000

    t1 = time.perf_counter()
    table = pq.read_table(
        pa.BufferReader(payload),
        filters=[(field, "=", value)],
        columns=["__pfb_id", "submitter_id", field],
    )
    query_ms = (time.perf_counter() - t1) * 1000
    return table, extract_ms, query_ms, manifest, len(payload)


def search_via_scan(path, node, field, value):
    """What a consumer must do today: decode every record and test it."""
    t0 = time.perf_counter()
    hits = []
    with PFBReader(path) as reader:
        for record in reader:
            if record["name"] == node and record["object"].get(field) == value:
                hits.append(record["id"])
    return hits, (time.perf_counter() - t0) * 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--indexed", default="poc/parquet/out/indexed.pfb")
    ap.add_argument("--original", default="export_2026-08-06T10_36_03.avro")
    ap.add_argument("--node", default="demographic")
    ap.add_argument("--field", default="population")
    ap.add_argument("--value", default="CEU")
    args = ap.parse_args()

    table, extract_ms, query_ms, manifest, payload_bytes = search_via_parquet(
        args.indexed, args.node, args.field, args.value
    )
    print(f"QUERY: {args.node}.{args.field} = {args.value!r}\n")
    print(f"via embedded Parquet  -> {table.num_rows} records")
    print(
        f"   payload extract: {extract_ms:8.1f} ms "
        f"({payload_bytes:,} bytes for node {args.node!r})"
    )
    print(f"   parquet query:   {query_ms:8.1f} ms")
    print(f"   first 3 ids: {table.column('__pfb_id').to_pylist()[:3]}")
    print(f"   submitter_ids: {table.column('submitter_id').to_pylist()[:3]}")

    hits, scan_ms = search_via_scan(args.original, args.node, args.field, args.value)
    print(f"\nvia full Avro scan    -> {len(hits)} records")
    print(f"   scan: {scan_ms:8.1f} ms (29,021 records decoded)")

    same = sorted(table.column("__pfb_id").to_pylist()) == sorted(hits)
    print(f"\nANSWERS AGREE: {same}")

    # Tier 2 needs no Parquet reader at all - just the header JSON.
    dom = manifest.get("domains", {}).get(f"{args.node}.{args.field}")
    print(f"\ntier 2 from the header manifest alone (no pyarrow, no payload):")
    print(f"   {args.node}.{args.field} domain = {dom}")
    print(f"   is {args.value!r} present? {args.value in (dom or [])}")


if __name__ == "__main__":
    main()
