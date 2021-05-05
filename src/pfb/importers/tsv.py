from __future__ import absolute_import

import glob
import os
import csv

import click

from ..base import avro_record
from ..cli import from_command
from ..reader import PFBReader


@from_command.command("tsv", short_help="Convert TSV files into a PFB file.")
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
def from_tsv(ctx, path, schema, program, project):
    """Convert TSV files under PATH into a PFB file.

    The TSV files are expected to be directly under PATH, and match "*.tsv". Each file
    should be a single TSV list of objects that matches the specified schema. Also, for
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

            writer.write(_from_tsv(writer.metadata, path, program, project))
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)


def _from_tsv(metadata, path, program, project):
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }

    order = glob.glob(os.path.join(path, "*.tsv"))

    total = len(order)

    for i, o in enumerate(order):
        o = os.path.basename(o).replace(".tsv", "").strip()
        click.secho("{}/{}: ".format(i + 1, total), fg="blue", nl=False, err=True)
        click.secho(o, fg="white", err=True)

        tsv_data = list(
            csv.DictReader(open(os.path.join(path, o + ".tsv")), delimiter="\t")
        )

        node_name = o

        if isinstance(tsv_data, dict):
            tsv_data = [tsv_data]
        for tsv_record in tsv_data:
            for k, v in tsv_record.items():
                tsv_record[k] = convert_types(v)
            record = _convert_tsv(node_name, tsv_record, program, project, link_dests)
            yield record


def convert_types(val):
    if val is None or val.strip() == "":
        return None
    if val.lower() == "false":
        return False
    if val.lower() == "true":
        return True
    try:
        v = float(val)
        # for fields that require type long
        if int(v) == v:
            v = int(v)
        return v
    except ValueError:
        pass
    return val


def _convert_tsv(node_name, tsv_record, program, project, link_dests):
    relations = []
    try:
        node_id = tsv_record["submitter_id"]
    except KeyError:
        if node_name == "program":
            node_id = tsv_record["dbgap_accession_number"]
        else:
            node_id = tsv_record["code"]

    vals = tsv_record

    to_del = []
    for item in tsv_record:
        if type(tsv_record[item]) == dict and "submitter_id" in tsv_record[item]:
            to_del.append(item)
            v = item
            relations.append(
                {
                    "dst_id": tsv_record[item]["submitter_id"],
                    "dst_name": link_dests[node_name][v],
                }
            )
        # array typing being passed off as string
        if (
            type(tsv_record[item]) == str
            and "[" in tsv_record[item]
            and "]" in tsv_record[item]
        ):
            arrayStrip = tsv_record[item].strip("[']")
            vals[item] = arrayStrip.split(",")

        if ".submitter_id" in item:
            relations.append(
                {"dst_id": tsv_record[item], "dst_name": item.split(".")[0]}
            )
            to_del.append(item)

    for i in to_del:
        if i in vals:
            del vals[i]

    vals["project_id"] = "{}-{}".format(program, project)

    return avro_record(node_id, node_name, vals, relations)
