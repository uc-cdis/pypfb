import click

from ..cli import main
from ..reader import PFBReader
from ..writer import PFBWriter


@main.group()
@click.option(
    "-i",
    "--input",
    "input_file",
    metavar="FILENAME",
    type=click.File("rb"),
    default="-",
    help="Source PFB file.  [default: <stdin>]",
)
@click.option(
    "-o",
    "--output",
    "output_file",
    metavar="FILENAME",
    type=click.File("wb"),
    default="-",
    help="Destination PFB file.  [default: <stdout>]",
)
@click.pass_context
def rename(ctx, input_file, output_file):
    """Rename different parts of schema."""
    ctx.ensure_object(dict)
    ctx.obj["reader"] = PFBReader(input_file)
    ctx.obj["writer"] = PFBWriter(output_file)


@rename.command("node", short_help="Rename node.")
@click.argument("old")
@click.argument("new")
@click.pass_context
def node_command(ctx, old, new):
    """Rename node from OLD to NEW."""
    try:
        with ctx.obj["reader"] as reader, ctx.obj["writer"] as writer:
            writer.copy_schema(reader)
            writer.prepare_encode_cache()
            writer.rename_node(old.encode("utf-8"), new.encode("utf-8"))
            writer.write(reader)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)


@rename.command("type")
@click.option("--from", "name_from", required=True, help="")
@click.option("--to", "name_to", required=True, help="")
@click.pass_context
def type_command(ctx, name_from, name_to):
    """Rename type (not implemented)."""


@rename.command("enum", short_help="Rename enum.")
@click.argument("field")
@click.argument("old")
@click.argument("new")
@click.pass_context
def enum_command(ctx, field, old, new):
    """Rename enum of FIELD from OLD to NEW."""
    try:
        with ctx.obj["reader"] as reader, ctx.obj["writer"] as writer:
            writer.copy_schema(reader)
            writer.prepare_encode_cache()
            writer.rename_enum(field, old.encode("utf-8"), new.encode("utf-8"))
            writer.write(reader)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)
