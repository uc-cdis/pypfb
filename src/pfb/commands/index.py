# -*- coding: utf-8 -*-
"""``pfb index`` — build and inspect the Parquet-derived search index."""

import json
import sys

import click

from ..cli import main
from ..manifest import load_manifest


@main.group(short_help="Build or inspect a PFB search index.")
def index():
    """Build or inspect the Parquet-derived search index of a PFB file."""


@index.command(short_help="Write an indexed copy of a PFB file.")
@click.option(
    "-i",
    "--input",
    "input_file",
    type=click.File("rb"),
    default="-",
    help="The input PFB file.  [default: <stdin>]",
)
@click.option(
    "-o",
    "--output",
    type=click.File("wb"),
    required=True,
    help="The indexed PFB file to write.",
)
@click.option(
    "--embed-parquet/--no-embed-parquet",
    default=False,
    show_default=True,
    help=(
        "Embed per-node Parquet payloads, enabling record retrieval (tier 3). "
        "This adds a union member to the Avro schema and will break consumers "
        "outside this SDK."
    ),
)
@click.option(
    "--domain-cap",
    default=256,
    show_default=True,
    help="Max distinct values before a field falls back to bloom or range.",
)
@click.option(
    "--data-page-size",
    default=64 * 1024,
    show_default=True,
    help="Parquet page size: pruning granularity against index size.",
)
@click.option("--compression", default="zstd", show_default=True, help="Parquet codec.")
@click.option(
    "--no-bloom", is_flag=True, help="Skip bloom filters (they cost ~3.5% of payload)."
)
def build(
    input_file, output, embed_parquet, domain_cap, data_page_size, compression, no_bloom
):
    """Read a PFB, derive statistics and an index, write an indexed PFB.

    Without --embed-parquet this writes tiers 0-2: the manifest goes into the
    Avro header and nothing else about the file changes.
    """
    try:
        from ..parquet import DEFAULT_BLOOM_COLUMNS, build_index, write_index
    except ImportError:
        raise click.ClickException(
            "pyarrow is required for indexing; install with: pip install pypfb[parquet]"
        )

    result = build_index(
        input_file,
        domain_cap=domain_cap,
        compression=compression,
        data_page_size=data_page_size,
        bloom_columns=() if no_bloom else DEFAULT_BLOOM_COLUMNS,
    )
    manifest = write_index(output, result, embed=embed_parquet)

    fields = sum(len(n["fields"]) for n in manifest["nodes"].values())
    click.echo(
        "indexed {} nodes, {} fields".format(len(manifest["nodes"]), fields), err=True
    )
    if embed_parquet:
        click.echo(
            "embedded {:,} bytes of Parquet across {} payloads".format(
                result.payload_bytes, len(result.payloads)
            ),
            err=True,
        )
    else:
        click.echo(
            "tiers 0-2 only; pass --embed-parquet for record retrieval", err=True
        )


@index.command("show", short_help="Show the index manifest of a PFB file.")
@click.option(
    "-i",
    "--input",
    "input_file",
    type=click.File("rb"),
    default="-",
    help="The PFB file.  [default: <stdin>]",
)
@click.option("--node", help="Show one node's field summaries instead.")
@click.option("--full", is_flag=True, help="Print the whole manifest.")
def show_index(input_file, node, full):
    """Show the index manifest, or one node's field summaries."""
    import fastavro

    manifest = load_manifest(fastavro.block_reader(input_file).metadata)
    if manifest is None:
        click.echo("no index in this file", err=True)
        raise SystemExit(1)

    if full:
        payload = manifest
    elif node:
        if node not in manifest["nodes"]:
            raise click.ClickException("no such indexed node: {}".format(node))
        payload = manifest["nodes"][node]
    else:
        payload = {
            "version": manifest["version"],
            "embedded": manifest["embedded"],
            "domain_cap": manifest.get("domain_cap"),
            "nodes": {
                name: {
                    "num_rows": entry["num_rows"],
                    "payload_bytes": entry["payload_bytes"],
                    "fields": len(entry["fields"]),
                }
                for name, entry in sorted(manifest["nodes"].items())
            },
        }
    json.dump(payload, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
