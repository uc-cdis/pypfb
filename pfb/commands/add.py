import json
import sys

import click

from ..base import str_hook
from ..cli import main
from ..reader import PFBReader
from ..writer import PFBWriter


@main.command(short_help="Make a blank record for add.")
@click.argument("name")
@click.option(
    "-i",
    "--input",
    "path",
    metavar="PFB",
    type=click.File("rb"),
    default="-",
    help="Read schema from this PFB file.  [default: <stdin>]",
)
def make(name, path):
    """Make a blank record according to given NODE schema in the PFB file."""
    with PFBReader(path) as reader:
        json.dump(reader.make_empty_record(name), sys.stdout)
        sys.stdout.write("\n")


@main.command(short_help="Add records into a PFB file.")
@click.option(
    "-i",
    "--input",
    "json_file",
    metavar="JSON",
    type=click.File("rb"),
    default="-",
    help="The JSON file to add.  [default: <stdin>]",
)
@click.argument("pfb_file", metavar="PFB", type=click.File("a+b"))
def add(json_file, pfb_file):
    """Add records from a minified JSON file to the PFB file."""

    def data():
        for line in json_file:
            yield json.loads(line, object_pairs_hook=str_hook)

    pos = pfb_file.tell()
    pfb_file.seek(0)
    with PFBReader(pfb_file) as reader:
        with PFBWriter(pfb_file) as writer:
            writer.copy_schema(reader)
            pfb_file.seek(pos)
            writer.write(data(), metadata=False)
