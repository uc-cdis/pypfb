import itertools
import json
import sys

import click

from ..cli import main
from ..reader import PFBReader


@main.group(
    invoke_without_command=True, short_help="Show different parts of a PFB file."
)
@click.option(
    "-i",
    "--input",
    "input_file",
    type=click.File("rb"),
    default="-",
    help="The PFB file.  [default: <stdin>]",
)
@click.option(
    "-n",
    "--limit",
    type=int,
    help="How many records to show, ignored for sub-commands.  [default: no limit]",
)
@click.pass_context
def show(ctx, input_file, limit):
    """Show records of the PFB file.

    Specify a sub-command to show other information.
    """
    ctx.ensure_object(dict)
    ctx.obj["reader"] = PFBReader(input_file)
    if ctx.invoked_subcommand is None:
        with ctx.obj["reader"] as reader:
            for r in itertools.islice(reader, limit):
                json.dump(r, sys.stdout)
                sys.stdout.write("\n")


@show.command()
@click.pass_context
def nodes(ctx):
    """Show all the node names in the PFB file."""
    with ctx.obj["reader"] as reader:
        for node in reader.schema:
            sys.stdout.write(node["name"])
            sys.stdout.write("\n")


@show.command(short_help="Show the schema of the PFB file.")
@click.argument("name", metavar="NODE", required=False)
@click.pass_context
def schema(ctx, name):
    """Show the schema of the NODE in the PFB file.

    If NODE is not specified, the whole schema will be shown.
    """
    with ctx.obj["reader"] as reader:
        if name:
            for node in reader.schema:
                if node["name"] == name:
                    json.dump(node, sys.stdout)
                    sys.stdout.write("\n")
                    return
        else:
            json.dump(reader.schema, sys.stdout)
            sys.stdout.write("\n")


@show.command(
    short_help="Show the encoded schema of the PFB file usually for java avro validation."
)
@click.argument("name", metavar="NODE", required=False)
@click.pass_context
def encoded_schema(ctx, name):
    """Show the encoded schema of the NODE in the PFB file."""
    with ctx.obj["reader"] as reader:
        json.dump(reader._encoded_schema, sys.stdout)
        sys.stdout.write("\n")


@show.command(short_help="Show the metadata of the PFB file.")
@click.argument("name", metavar="NODE", required=False)
@click.pass_context
def metadata(ctx, name):
    """Show the metadata of NODE in the PFB file.

    If NODE is not specified, the whole metadata will be shown.
    """
    with ctx.obj["reader"] as reader:
        if name:
            for node in reader.metadata["nodes"]:
                if node["name"] == name:
                    json.dump(node, sys.stdout)
                    sys.stdout.write("\n")
                    return
        else:
            json.dump(reader.metadata, sys.stdout)
            sys.stdout.write("\n")


@show.command(short_help="Show the quick stats of the PFB file.")
@click.argument("name", metavar="NODE", required=False)
@click.pass_context
def stats(ctx, name):
    """Show the stats of the nodes in the PFB file.

    the whole stats will be shown.
    """
    with ctx.obj["reader"] as reader:
        stats = {}
        edges = 0
        for r in reader:
            if r["name"] in stats:
                stats[r["name"]] += 1
            else:
                stats[r["name"]] = 1

            edges += len(r["relations"])

        sys.stdout.write("Total number of nodes: " + str(len(stats)) + "\n")

        sys.stdout.write("Total number of edges: " + str(edges) + "\n")

        for key in stats:
            sys.stdout.write(key + ": " + str(stats[key]) + "\n")
