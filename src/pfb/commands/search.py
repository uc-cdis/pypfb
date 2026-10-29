# -*- coding: utf-8 -*-
"""``pfb search`` — find records by predicate using the embedded index."""

import json
import sys

import click

from ..cli import main
from ..manifest import (
    PredicateError,
    answerable_from_manifest,
    parse_predicate,
)
from ..reader import PFBReader


@main.command(short_help="Search records within a PFB file.")
@click.option(
    "-i",
    "--input",
    "input_file",
    type=click.File("rb"),
    default="-",
    help="The PFB file.  [default: <stdin>]",
)
@click.option(
    "-w",
    "--where",
    multiple=True,
    required=True,
    metavar="NODE.FIELD[=|!=|>|>=|<|<=]VALUE",
    help="Predicate; repeatable, combined with AND.",
)
@click.option("--count", is_flag=True, help="Print only the number of matches.")
@click.option(
    "--explain", is_flag=True, help="Report how the answer was reached, not the rows."
)
def search(input_file, where, count, explain):
    """Find records matching every predicate.

    Uses the embedded Parquet payload and its page index when present, and
    falls back to a full Avro scan with a warning when it is absent.  All
    predicates must target the same node.
    """
    try:
        predicates = [parse_predicate(w) for w in where]
    except PredicateError as e:
        raise click.UsageError(str(e))

    nodes = {p.node for p in predicates}
    if len(nodes) > 1:
        raise click.UsageError(
            "all predicates must target one node, got: " + ", ".join(sorted(nodes))
        )
    node = nodes.pop()

    with PFBReader(input_file) as reader:
        manifest = reader.index_manifest

        # Tier 2: the manifest alone can sometimes prove there is no match,
        # without reading a payload or a record.
        if manifest:
            for predicate in predicates:
                if answerable_from_manifest(manifest, predicate) is False:
                    _emit([], count, explain, source="manifest")
                    return

        payload = reader.parquet(node) if manifest else None
        if payload is not None:
            try:
                from ..parquet import search_payload

                result = search_payload(payload, predicates)
            except ImportError:
                raise click.ClickException(
                    "pyarrow is required to read an embedded payload; "
                    "install with: pip install pypfb[parquet]"
                )
            except PredicateError as e:
                raise click.UsageError(str(e))
            _emit(result.rows, count, explain, source="parquet", result=result)
            return

        if manifest and manifest.get("embedded"):
            click.echo(
                "warning: manifest claims an embedded payload for {} but it "
                "could not be read; falling back to a scan".format(node),
                err=True,
            )
        else:
            click.echo(
                "warning: no embedded Parquet in this file; scanning every "
                "record. Run 'pfb index build --embed-parquet' to add one.",
                err=True,
            )
        rows = _scan(reader, node, predicates)
        _emit(rows, count, explain, source="scan")


def _scan(reader, node, predicates):
    """Brute-force fallback, so an unindexed file still answers correctly.

    Comparison is ``Predicate.matches_record`` so that the fallback and the
    Parquet path cannot drift apart.
    """
    rows = []
    for record in reader:
        if record["name"] != node:
            continue
        obj = record["object"]
        if all(p.matches_record(obj) for p in predicates):
            row = dict(obj)
            row["__pfb_id"] = record["id"]
            rows.append(row)
    return rows


def _emit(rows, count, explain, source, result=None):
    if explain:
        payload = result.explain() if result else {"source": source}
        payload["rows_matched"] = len(rows)
        json.dump(payload, sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
        return
    if count:
        sys.stdout.write("{}\n".format(len(rows)))
        return
    for row in rows:
        json.dump(row, sys.stdout, default=str)
        sys.stdout.write("\n")
