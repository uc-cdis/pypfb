# -*- coding: utf-8 -*-
"""``pfb to parquet`` — write one Parquet file per node, outside any PFB.

Exposes the staging layer standalone.  Useful in its own right, and it makes
the Avro-to-Arrow type mapping testable without any of the container work.
"""

import os

import click

from ..cli import to_command


@to_command.command("parquet", short_help="Convert PFB into per-node Parquet files.")
@click.argument("output", metavar="OUTPUT_DIR", type=click.Path(file_okay=False))
@click.option("--compression", default="zstd", show_default=True)
@click.option("--data-page-size", default=64 * 1024, show_default=True)
@click.option(
    "--no-page-index", is_flag=True, help="Skip the page index (it costs ~0.4%)."
)
@click.pass_context
def to_parquet(ctx, output, compression, data_page_size, no_page_index):
    """Write one Parquet file per node into OUTPUT_DIR."""
    try:
        from ..index.staging import ParquetStager
    except ImportError:
        raise click.ClickException(
            "pyarrow is required; install with: pip install pypfb[parquet]"
        )

    if not os.path.exists(output):
        os.makedirs(output)

    with ctx.obj["reader"] as reader:
        stager = ParquetStager(
            reader.schema,
            compression=compression,
            data_page_size=data_page_size,
            write_page_index=not no_page_index,
        )
        for record in reader:
            stager.add(record)
        payloads = stager.finish()

    for node, payload in sorted(payloads.items()):
        path = os.path.join(output, "{}.parquet".format(node))
        with open(path, "wb") as f:
            f.write(payload)
        click.echo(
            "Created {} ({:,} rows, {:,} bytes)".format(
                path, stager.row_counts[node], len(payload)
            )
        )
    click.echo("Done, created {} files under: {}".format(len(payloads), output))
