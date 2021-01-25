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
    num_files = 1

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}
    for row in reader:
        name = row["name"]
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
            if row["name"] not in node_submitter_ids:
                node_submitter_ids[row["name"]] = []
            node_submitter_ids[row["name"]].append(obj["submitter_id"])

        


        for r in row["relations"]:
            # print(r)
            if {
                "name": r["dst_name"] + ".id",
                "type": ["null", "string"],
            } not in fields:
                fields.append(
                    {
                        "name": r["dst_name"] + ".id",
                        "type": ["null", "string"],
                    }
                )

            obj[r["dst_name"] + ".id"] = r["dst_id"]
            
            parent_node = r["dst_name"]

            if parent_node in node_submitter_ids:
                if {
                    "name": r["dst_name"] + ".submitter_id",
                    "type": ["null", "string"],
                } not in fields:
                    fields.append(
                        {
                            "name": r["dst_name"] + ".submitter_id",
                            "type": ["null", "string"],
                        }
                    )

                obj[parent_node + ".submitter_id"] = "null"

                for i in node_submitter_ids[parent_node]:
                    if i in obj["submitter_id"]:
                        obj[parent_node + ".submitter_id"] = i
                        break






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
                if field["name"] == "sample.id" or field["name"] == "project.id":
                    if field["name"] not in obj:
                        continue
                value = obj[field["name"]]
                data_row.append(value)

        w.writerow(data_row)

    return num_files


def _make_header_row(fields):
    header_row = ["type"]
    for field in fields:
        header_row.append(field["name"])
    return header_row
