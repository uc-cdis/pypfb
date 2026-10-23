# -*- coding: utf-8 -*-
"""``pfb search`` — find records by predicate, using the block index."""

import json
import sys

import click

from ..blockindex.query import PredicateError, parse_predicate, search as run_search
from ..cli import main


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
@click.option(
    "--count", is_flag=True, help="Print only the number of matching records."
)
@click.option(
    "--explain",
    is_flag=True,
    help="Print how many blocks the index pruned instead of the records.",
)
def search(input_file, where, count, explain):
    """Find records matching every predicate.

    Uses the block index when the file has one and falls back to reading every
    block when it does not; results are the same either way.  All predicates
    must target the same node, since they are ANDed against a single record.
    """
    try:
        predicates = [parse_predicate(w) for w in where]
        result = run_search(input_file, predicates)
    except PredicateError as e:
        raise click.UsageError(str(e))

    if not result.indexed:
        click.echo(
            "warning: no block index in this file; scanning every block. "
            "Run 'pfb index build' to add one.",
            err=True,
        )

    if explain:
        json.dump(result.explain(), sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return

    if count:
        sys.stdout.write("{}\n".format(len(result.records)))
        return

    for record in result.records:
        json.dump(record, sys.stdout)
        sys.stdout.write("\n")
