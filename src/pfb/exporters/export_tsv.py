import csv
import os

import click

from ..cli import to_command


@to_command.command("export_tsv", short_help="Convert PFB to tsv.")
@click.argument("output", default="./tsvs/", type=click.Path(file_okay=False))
@click.pass_context
def export_tsv(ctx, output):
    """Convert PFB into CSV files under OUTPUT for Neptune bulk load (Gremlin).

    The default OUTPUT is ./gremlin/.
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


def _to_tsv(reader, dir_path, handlers_by_name):
    uuids = {}
    project_ids = []
    edges = []
    num_files = 1

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}
    for row in reader:
        name = row["name"]
        fields = fields_by_name[name]

        # row_id = row["id"]
        obj = row["object"]
        # relations = row["relations"]

        # get the CSV writer for this row, create one if not created
        pair = handlers_by_name.get(name)
        if pair is None:
            header_row = _make_header_row(fields)
            path = os.path.join(dir_path, name + ".csv")
            click.secho("Creating ", fg="blue", err=True, nl=False)
            click.secho(path, fg="white", err=True)
            f = open(path, "wt")
            num_files += 1
            w = csv.writer(f)
            w.writerow(header_row)
            handlers_by_name[name] = f, w
        else:
            w = pair[1]

        # write data into CSV
        row = [name]
        for field in fields:
            if field["name"] == "project_id":
                project_ids.append([name, obj["project_id"]])
                row.append(obj[field["name"]])
            else:
                value = obj[field["name"]]
                row.append(value)
        w.writerow(row)

    return num_files


def _make_header_row(fields):
    header_row = ["type"]
    for field in fields:
        header_row.append(field["name"])
    return header_row
