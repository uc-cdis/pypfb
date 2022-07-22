import csv
import os
import json

import click

from ..cli import to_command


@to_command.command("tsv", short_help="Convert PFB to tsv.")
@click.argument("output", default="./tsvs/", type=click.Path(file_okay=False))
@click.pass_context
def tsv(ctx, output):
    """Convert PFB into TSVs yielding one TSV per node.

    The default OUTPUT is ./tsvs/.
    """
    handlers_by_name = {}
    try:
        with ctx.obj["reader"] as reader:
            num_files = _to_tsv(reader, output, handlers_by_name)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    finally:
        for f, w in list(handlers_by_name.values()):
            f.close()
    click.secho(
        "Done, created %d files under: " % num_files,
        fg="green",
        err=True,
        nl=False,
        bold=True,
    )
    click.secho(output, fg="white", err=True, bold=True)


node_submitter_ids = {}


def _to_tsv(reader, dir_path, handlers_by_name):
    project_ids = []
    num_files = 0

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    # Extract the relations information from the metadata and build a dictionary out of it with the information necessary to build the header
    # eg: {"lab": {"case": {"id": "cases.id", "submitter_id": "cases.submitter_id"}}, "other node": {"other node parent link": {"id":"...", "submiter_id: "..."}}, ... }
    relations_by_node = {}
    for node in reader.metadata["nodes"]:
        if node["name"] not in relations_by_node:
            relations_by_node[node["name"]] = {}

        for link in node["links"]:
            if link["dst"] not in relations_by_node[node["name"]]:
                relations_by_node[node["name"]][link["dst"]] = {}

            name_id = link["name"] + ".id"
            if "id" not in relations_by_node[node["name"]][link["dst"]]:
                relations_by_node[node["name"]][link["dst"]]["id"] = name_id
            name_submitter_id = link["name"] + ".submitter_id"
            if "submitter_id" not in relations_by_node[node["name"]][link["dst"]]:
                relations_by_node[node["name"]][link["dst"]]["submitter_id"] = name_submitter_id


    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}

    # Add the relation fields to the lis of fields by node
    for node_name, links in relations_by_node.items():
        for linked_node_name, linked_values in links.items():
            for attr_name, attr_value in linked_values.items():
                if {'name': attr_value, 'type': ['null', 'string'],} not in fields_by_name[node_name]:
                    fields_by_name[node_name].append({'name': attr_value, 'type': ['null', 'string'],})


    for row in reader:
        name = row["name"]
        record_id = row["id"]
        fields = fields_by_name[name]

        node_index = next(
            (
                index
                for (index, d) in enumerate(reader.metadata["nodes"])
                if d["name"] == name
            )
        )

        obj = row["object"]

        if "submitter_id" in obj:
            node_submitter_ids[record_id] = obj["submitter_id"]

        if {"name": "id", "type": ["null", "string"],} not in fields:
            fields.append(
                {"name": "id", "type": ["null", "string"],}
            )

        obj["id"] = record_id

        for r in row["relations"]:
            parent_node = r["dst_name"]
            parent_id = r["dst_id"]

            if relations_by_node[name][parent_node]["id"]:
                obj[relations_by_node[name][parent_node]["id"]] = r["dst_id"]

            if relations_by_node[name][parent_node]["submitter_id"]:
                if parent_id in node_submitter_ids:
                    obj[relations_by_node[name][parent_node]["submitter_id"]] = node_submitter_ids[parent_id]
                else:
                    obj[relations_by_node[name][parent_node]["submitter_id"]] = "null"

        if "sample" in node_submitter_ids:
            print(node_submitter_ids["sample"])

        # get the TSV writer for this row, create one if not created
        pair = handlers_by_name.get(name)
        if pair is None:
            header_row = _make_header_row(fields)
            path = os.path.join(dir_path, name + ".tsv")
            click.secho("Creating ", fg="blue", err=True, nl=False)
            click.secho(path, fg="white", err=True)
            f = open(path, "wt")
            num_files += 1
            w = csv.writer(f, delimiter="\t")
            w.writerow(header_row)
            handlers_by_name[name] = f, w
        else:
            w = pair[1]

        # write data into TSV
        data_row = [name]
        for field in fields:
            if field["name"] == "project_id":
                project_ids.append([name, obj["project_id"]])
                data_row.append(obj[field["name"]])
            else:
                # adding logic for multi-sample records that contain either project.submitter_id or samplie.submitter_id
                if (
                    field["name"] == "samples.id"
                    or field["name"] == "projects.id"
                    or field["name"] == "samples.submitter_id"
                    or field["name"] == "projects.submitter_id"
                ):
                    if field["name"] not in obj:
                        continue
                value = obj[field["name"]] if field["name"] in obj else None 
                data_row.append(value)

        w.writerow(data_row)

    return num_files


def _make_header_row(fields):
    header_row = ["type"]
    for field in fields:
        header_row.append(field["name"])
    return header_row
