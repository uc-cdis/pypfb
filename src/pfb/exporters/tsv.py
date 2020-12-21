import csv
import os


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


def _to_tsv(reader, dir_path, handlers_by_name):
    project_ids = []
    num_files = 0

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}
    for row in reader:
        name = row["name"]
        row_id = row["id"]
        fields = fields_by_name[name]

        node_index = next(
            (
                index
                for (index, d) in enumerate(reader.metadata["nodes"])
                if d["name"] == name
            )
        )

        obj = row["object"]

        for r in row["relations"]:
            #rel_name = r["dst_name"] + "_ref_id"
            rel_name = "parent_id"
            if {
                "name": rel_name,
                "type": ["null", "string"],
            } not in fields:
                fields.append(
                    {
                        "name": rel_name,
                        "type": ["null", "string"],
                    }
                )
                fields.append(
                    {
                        "name": "parent_type",
                        "type": ["null", "string"],
                    }
                )
            obj[rel_name] = r["dst_id"]
            obj["parent_type"] = r["dst_name"]

        # get the TSV writer for this row, create one if not created
        pair = handlers_by_name.get(name)
        if pair is None:
            print('writing header for {}'.format(name))
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
        data_row = [row_id]
        for field in fields:
            if field["name"] == "project_id":
                project_ids.append([name, obj["project_id"]])
                data_row.append(obj[field["name"]])
            else:
                value = obj[field["name"]]
                data_row.append(value)

        w.writerow(data_row)

    return num_files


def _make_header_row(fields):
    header_row = ["id"]
    for field in fields:
        header_row.append(field["name"])
    return header_row
