# -*- coding: utf-8 -*-
"""``pfb index`` — build and inspect the block search index."""

import json
import sys

import click

from ..blockindex import BlockIndex, load_manifest
from ..blockindex.builder import (
    DEFAULT_DOMAIN_CAP,
    build_index,
    rechunk,
    write_indexed,
)
from ..cli import main


@main.group(short_help="Build or inspect a PFB block search index.")
def index():
    """Build or inspect the block-level search index of a PFB file."""


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
    "--domain-cap",
    default=DEFAULT_DOMAIN_CAP,
    show_default=True,
    help="Max distinct values before a field falls back to range or no index.",
)
@click.option(
    "--field",
    "fields",
    multiple=True,
    metavar="NODE.FIELD",
    help="Index only these fields; repeatable.  [default: all eligible fields]",
)
@click.option(
    "--sync-interval",
    type=int,
    default=None,
    help=(
        "Re-chunk records into blocks of roughly this many bytes before "
        "indexing.  Smaller blocks prune better but cost more to walk."
    ),
)
def build(input_file, output, domain_cap, fields, sync_interval):
    """Read a PFB, build a block index, and write an indexed copy.

    The records themselves are untouched: blocks are copied verbatim and the
    index is added to the file header, so the output is readable by any
    existing PFB consumer.
    """
    source = input_file
    if sync_interval is not None:
        # Re-chunking re-encodes, so it has to happen before the index is built
        # or the block ordinals would describe the wrong layout.
        source = rechunk(input_file, sync_interval)

    result = build_index(source, domain_cap=domain_cap, fields=fields or None)
    write_indexed(output, result)

    manifest = result.manifest
    strategies = {}
    for entry in manifest["fields"].values():
        strategies[entry["s"]] = strategies.get(entry["s"], 0) + 1
    click.echo(
        "indexed {} records in {} blocks across {} nodes".format(
            manifest["num_records"], manifest["num_blocks"], len(manifest["nodes"])
        ),
        err=True,
    )
    click.echo(
        "fields: "
        + (
            ", ".join(
                "{} {}".format(count, name)
                for name, count in sorted(strategies.items())
            )
            or "none"
        ),
        err=True,
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
@click.option(
    "--full", is_flag=True, help="Print the whole manifest including block lists."
)
def show_index(input_file, full):
    """Summarize the block index, or dump it in full with ``--full``."""
    import fastavro

    block_reader = fastavro.block_reader(input_file)
    manifest = load_manifest(block_reader.metadata)
    if manifest is None:
        click.echo("no block index in this file", err=True)
        raise SystemExit(1)

    if full:
        json.dump(manifest, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return

    parsed = BlockIndex(manifest)
    summary = {
        "version": manifest["version"],
        "num_blocks": parsed.num_blocks,
        "num_records": parsed.num_records,
        "domain_cap": manifest["domain_cap"],
        "nodes": {node: len(manifest["nodes"][node]) for node in parsed.nodes},
        "fields": parsed.indexed_fields(),
    }
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
