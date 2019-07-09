from __future__ import absolute_import

import glob
import json
import os

import click

from ..base import avro_record
from ..cli import from_command
from ..reader import PFBReader


@from_command.command("json", short_help="Convert JSON files into a PFB file.")
@click.argument("path", default=".", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-s",
    "--schema",
    required=True,
    type=click.File("rb"),
    help="The PFB file to load the schema from.",
)
@click.option("--program", required=True, help="Name of the program.")
@click.option("--project", required=True, help="Name of the project.")
@click.pass_context
def from_json(ctx, path, schema, program, project):
    """Convert JSON files under PATH into a PFB file.

    The JSON files are expected to be directly under PATH, and match "*.json". Each file
    should be a single JSON list of objects that matches the specified schema. Also, for
    now it is hard-coded that each object should contain at least the "submitter_id", or
    it will be ignored.
    """
    try:
        with ctx.obj["writer"] as writer:
            if writer.isatty:
                click.secho(
                    "Error: cannot output to TTY.", fg="red", bold=True, err=True
                )
                return

            click.secho("Loading schema...", fg="cyan", err=True)
            with PFBReader(schema) as reader:
                writer.copy_schema(reader)

            writer.write(_from_json(writer.metadata, path, program, project))
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)


def _from_json(metadata, path, program, project):
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }

    order = glob.glob(os.path.join(path, "*.json"))

    total = len(order)

    for i, o in enumerate(order):
        o = os.path.basename(o).replace(".json", "").strip()
        click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
        click.secho(o, fg="white", err=True)

        with open(os.path.join(path, o + ".json"), "r") as f:
            json_data = json.load(f)

        node_name = o

        if isinstance(json_data, dict):
            json_data = [json_data]
        for json_record in json_data:
            record = _convert_json(node_name, json_record, program, project, link_dests)
            yield record


def _convert_json(node_name, json_record, program, project, link_dests):
    relations = []
    try:
        node_id = json_record["submitter_id"]
    except KeyError:
        node_id = json_record["code"]
    vals = json_record

    to_del = None
    for item in json_record:
        if type(json_record[item]) == dict and "submitter_id" in json_record[item]:
            to_del = item
            v = item
            relations.append(
                {
                    "dst_id": json_record[item]["submitter_id"],
                    "dst_name": link_dests[node_name][v],
                }
            )

    if to_del in vals:
        del vals[to_del]

    vals["project_id"] = "{}-{}".format(program, project)

    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)
