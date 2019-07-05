import itertools
import json
import logging.config
import os

import click
import yaml

from .avro_utils.avro_schema import AvroSchema
from .exporters import PFB2GremlinExporter
from .pfb import Gen3PFB

try:
    from .utils.dictionary import init_dictionary
except ImportError:
    init_dictionary = None

default_level = logging.INFO
config_path = "config.yml"

if os.path.exists(config_path):
    with open(config_path, "rt") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level)

log = logging.getLogger()


@click.group()
def main():
    """PFB: Portable Format for Biomedical Data."""


@main.command()
@click.argument("input_file", metavar="PFB")
@click.option("-s", "--schema", is_flag=True, help="Show PFB file schema.")
@click.option(
    "--limit",
    type=int,
    help='How many entries to show, -1 for all; ignored for "schema".',
)
def show(input_file, schema, limit):
    """Show schema or records of the PFB file."""
    if schema:
        print(json.dumps(Gen3PFB(input_file).read_metadata()))
    else:
        limit = limit if limit != -1 else None
        for r in itertools.islice(Gen3PFB(input_file).read_records(), limit):
            print(json.dumps(r))


@main.group("from")
def from_command():
    """Generate PFB from other data formats."""


if init_dictionary is not None:

    @from_command.command("dict")
    @click.argument("url")
    @click.option(
        "-o", "--output", required=True, type=click.File("wb"), help="Output PFB file."
    )
    def dict_command(url, output):
        """Convert Gen3 data dictionary at URL into PFB file."""
        dictionary = init_dictionary(url)
        schema = dictionary.schema

        log.info("Using dictionary: {}".format(url))

        avro_schema = AvroSchema.from_dictionary(schema)
        avro_schema.write(output)


@from_command.command("json")
@click.argument("path")
@click.option(
    "-s",
    "--schema",
    metavar="FILENAME",
    required=True,
    help="The PFB file to load the schema from.",
)
@click.option(
    "-o", "--output", metavar="FILENAME", required=True, help="The result PFB file."
)
@click.option("--program", required=True, help="Name of the program.")
@click.option("--project", required=True, help="Name of the project.")
def json_command(path, schema, output, program, project):
    """Convert JSON files under PATH into a PFB file."""
    Gen3PFB.from_json(schema, path, output, program, project)


@main.group("to")
def to_command():
    """Convert PFB into other data formats."""


@to_command.command()
@click.argument("pfb", type=click.File("rb"))
@click.option(
    "-o",
    "--output",
    default="./gremlin/",
    show_default=True,
    help="Directory to store the output files.",
)
@click.option(
    "--gzip/--no-gzip",
    "gzipped",
    default=True,
    help="Whether gzip the output.  [default: yes]",
)
def gremlin(pfb, output, gzipped):
    """Convert PFB into CSV files for Neptune bulk load (Gremlin)."""
    with PFB2GremlinExporter(pfb) as exporter:
        exporter.export_files(output, gzipped)


@main.command()
@click.argument("path", metavar="PFB")
@click.option("-n", "--node", required=True, help="Node to create.")
def make(path, node):
    """Make blank record from the PFB file."""
    Gen3PFB(path).make_record(node)


@main.command()
@click.argument("json_file", metavar="JSON")
@click.argument("pfb_file", metavar="PFB")
def add(json_file, pfb_file):
    """Add a record from a minified JSON file to the PFB file."""
    Gen3PFB(pfb_file).add_record(json_file)


@main.group()
@click.option(
    "-i",
    "--input",
    "input_file",
    metavar="FILENAME",
    required=True,
    help="Source PFB file.",
)
@click.option(
    "-o",
    "--output",
    "output_file",
    metavar="FILENAME",
    required=True,
    help="Destination PFB file.",
)
@click.pass_context
def rename(ctx, input_file, output_file):
    """Rename different parts of schema."""
    ctx.ensure_object(dict)
    ctx.obj["pfb"] = Gen3PFB(input_file)
    ctx.obj["output"] = output_file


@rename.command("node")
@click.option("--from", "name_from", required=True, help="")
@click.option("--to", "name_to", required=True, help="")
@click.pass_context
def node_command(ctx, name_from, name_to):
    """Rename node."""
    ctx.obj["pfb"].rename_node(ctx.obj["output"], name_from, name_to)


@rename.command("type")
@click.option("--from", "name_from", required=True, help="")
@click.option("--to", "name_to", required=True, help="")
@click.pass_context
def type_command(ctx, name_from, name_to):
    """Rename type (not implemented)."""


@rename.command("enum")
@click.option("--field", required=True, help="")
@click.option("--from", "val_from", required=True, help="")
@click.option("--to", "val_to", required=True, help="")
@click.pass_context
def enum_command(ctx, field, val_from, val_to):
    """Rename enum."""
    ctx.obj["pfb"].rename_field_enum(ctx.obj["output"], field, val_from, val_to)


if __name__ == "__main__":
    main()
