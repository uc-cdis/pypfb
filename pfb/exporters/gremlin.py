import csv
import gzip
import os
from uuid import uuid4

import click

from ..cli import to_command

TYPE_MAPPING = dict(
    boolean="Boolean",
    enum="String",
    integer="Long",
    long="Long",
    md5sum="String",
    float="Double",
    number="Double",
    string="String",
)


@to_command.command("gremlin", short_help="Convert PFB to Neptune (gremlin).")
@click.argument("output", default="./gremlin/", type=click.Path(file_okay=False))
@click.option(
    "--gzip/--no-gzip",
    "gzipped",
    default=True,
    help="Whether gzip the output.  [default: yes]",
)
@click.pass_context
def to_gremlin(ctx, output, gzipped):
    """Convert PFB into CSV files under OUTPUT for Neptune bulk load (Gremlin).

    The default OUTPUT is ./gremlin/.
    """
    handlers_by_name = {}
    try:
        with ctx.obj["reader"] as reader:
            num_files = _to_gremlin(reader, output, gzipped, handlers_by_name)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    finally:
        for f, w in handlers_by_name.itervalues():
            f.close()
    click.secho(
        "Done, created %d files under: " % num_files,
        fg="green",
        err=True,
        nl=False,
        bold=True,
    )
    click.secho(output, fg="white", err=True, bold=True)


def _to_gremlin(reader, dir_path, gzipped, handlers_by_name):
    uuids = {}
    project_ids = []
    edges = []
    num_files = 1

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    path = os.path.join(dir_path, "gremlin_edges.csv")
    open_func = open
    if gzipped:
        path += ".gz"
        open_func = gzip.open
    click.secho("Creating ", fg="blue", err=True, nl=False)
    click.secho(path, fg="white", err=True)
    f = open_func(path, "wb")
    ew = csv.writer(f)
    ew.writerow(["~id", "~from", "~to", "~label"])
    handlers_by_name["~edges"] = f, ew

    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}
    for row in reader:
        name = row["name"]
        fields = fields_by_name[name]

        row_id = row["id"]
        obj = row["object"]
        relations = row["relations"]

        pair = handlers_by_name.get(name)
        if pair is None:
            header_row = ["~label", "~id"]
            for field in fields:
                field_type = "string"
                for field_type in field["type"]:
                    if field_type != "null":
                        break
                if isinstance(field_type, (str, unicode)):
                    field_type = TYPE_MAPPING[field_type]
                else:
                    field_type = "String"
                header_row.append(field["name"] + ":" + field_type)
            path = os.path.join(dir_path, name + ".csv")
            if gzipped:
                path += ".gz"
            click.secho("Creating ", fg="blue", err=True, nl=False)
            click.secho(path, fg="white", err=True)
            f = open_func(path, "wb")
            num_files += 1
            w = csv.writer(f)
            w.writerow(header_row)
            handlers_by_name[name] = f, w
        else:
            w = pair[1]

        uuid = uuids[(name, row_id)] = str(uuid4())
        row = [name, uuid]
        for field in fields:
            if field["name"] == "project_id":
                project_ids.append([name, uuid, obj["project_id"]])
                row.append(obj[field["name"]])
            else:
                value = obj[field["name"]]
                row.append(value)
        w.writerow(row)
        for relation in relations:
            key = relation["dst_name"], relation["dst_id"]
            to_uuid = uuids.get(key)
            if to_uuid is None:
                edges.append((key, uuid))
            else:
                ew.writerow([str(uuid4()), uuid, to_uuid, relation["dst_name"]])
    click.secho("Writing remaining edges...", fg="cyan", err=True)
    for edge in edges:
        ew.writerow([str(uuid4()), edge[1], uuids[edge[0]], edge[0][0]])
    return num_files
