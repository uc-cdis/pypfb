#!/usr/bin/env python
"""PoC step 6a: write an indexed PFB carrying embedded Parquet payloads (§4).

Payloads go in the RECORD STREAM as a new `object` union member, not in the Avro
header: §2.1 established that fastavro header metadata rejects raw bytes and
base64 costs +33% on every megabyte. The small JSON manifest does go in the
header, where no base64 is needed.
"""
import argparse
import json
import pathlib
import sys

import fastavro
import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from poc_common import (  # noqa: E402
    BLOOM_COLUMNS,
    cached_nodes,
    indexed_pfb_records,
    load_capabilities,
    make_indexed_avro_schema,
    read_cached,
    write_json,
    writer_options,
)

from pfb.reader import PFBReader  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="export_2026-08-06T10_36_03.avro")
    ap.add_argument("--outdir", default=str(pathlib.Path(__file__).parent / "out"))
    ap.add_argument("--output", default=None)
    ap.add_argument(
        "--codec", default=None, help="fastavro codec for the Avro stream, e.g. deflate"
    )
    ap.add_argument(
        "--payloads-first",
        action="store_true",
        help="§8 open question: payloads after Metadata instead of last",
    )
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    out = pathlib.Path(args.output or (outdir / "indexed.pfb"))
    caps = load_capabilities(outdir)
    manifest = json.loads((outdir / "step4_manifest.json").read_text())

    payloads = {}
    for node in cached_nodes(outdir / "raw"):
        table = read_cached(outdir / "raw", node)
        opts = writer_options(table.schema, caps, ndv=max(table.num_rows, 1))
        sink = pathlib.Path(outdir / "parquet_ref" / f"{node}.parquet")
        blob = sink.read_bytes()
        payloads[node] = {
            "payload": blob,
            "num_rows": table.num_rows,
            "compression": opts.get("compression", "none"),
            "page_index": bool(opts.get("write_page_index")),
            "bloom_columns": sorted(opts.get("bloom_filter_options", {}).keys()),
        }

    with PFBReader(args.input) as reader:
        avro_schema = make_indexed_avro_schema(
            reader.schema, "first" if args.payloads_first else "last"
        )
        reader.prepare_encode_cache()
        stream = indexed_pfb_records(
            reader.metadata, reader, payloads, reader.is_encode, args.payloads_first
        )
        with out.open("wb") as f:
            fastavro.writer(
                f,
                avro_schema,
                stream,
                codec=args.codec or "null",
                metadata={
                    "pfb.index.v1.manifest": json.dumps(
                        manifest, separators=(",", ":"), default=str
                    )
                },
            )

    orig = pathlib.Path(args.input).stat().st_size
    size = out.stat().st_size
    payload_bytes = sum(len(p["payload"]) for p in payloads.values())
    summary = {
        "output": str(out),
        "codec": args.codec or "null",
        "payloads_first": args.payloads_first,
        "original_bytes": orig,
        "indexed_bytes": size,
        "payload_bytes": payload_bytes,
        "growth_pct": round(100 * (size - orig) / orig, 2),
        "nodes_embedded": len(payloads),
    }
    write_json(outdir / "step6_embed.json", summary)
    print(f"wrote {out}")
    print(f"  original {orig:,} -> indexed {size:,} " f"(+{summary['growth_pct']}%)")
    print(f"  {len(payloads)} parquet payloads, {payload_bytes:,} bytes")


if __name__ == "__main__":
    main()
