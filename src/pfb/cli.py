import logging.config
import os

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

import click
import yaml

from .reader import PFBReader
from .writer import PFBWriter

default_level = logging.INFO
config_path = "config.yml"

if os.path.exists(config_path):
    with open(config_path, "rt") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level)


@click.group()
def main():
    """PFB: Portable Format for Biomedical Data."""


@main.group("from")
@click.option(
    "-o",
    "--output",
    type=click.File("wb"),
    default="-",
    help="The output PFB file.  [default: <stdout>]",
)
@click.pass_context
def from_command(ctx, output):
    """Generate PFB from other data formats."""
    ctx.ensure_object(dict)
    ctx.obj["writer"] = PFBWriter(output)


@main.group("to")
@click.option(
    "-i",
    "--input",
    "input_file",
    type=click.File("rb"),
    default="-",
    help="The input PFB file.  [default: <stdin>]",
)
@click.pass_context
def to_command(ctx, input_file):
    """Convert PFB into other data formats."""
    ctx.ensure_object(dict)
    ctx.obj["reader"] = PFBReader(input_file)


# load plug-ins from entry_points
for ep in entry_points().get("pfb.plugins", []):
    ep.load()

if __name__ == "__main__":
    main()
