import asyncio
import click

from ..cli import main
from ..etl.etl import ETL


@main.command(short_help="ETL")
@click.option(
    "-u",
    "--url",
    "url",
    metavar="URL",
    type=str,
    default="-",
    help="base es url",
)
@click.option(
    "-t",
    "--token",
    "access_token",
    metavar="TOKEN",
    type=str,
    default="-",
    help="access token",
)
@click.option(
    "-n",
    "--node",
    "node",
    metavar="NODE",
    type=str,
    default="-",
    help="root node for ETL",
)
@click.argument("path", required=True, metavar="PFB", type=str)
def etl(url, access_token, node, path):
    """ETL the PFB file"""
    etl = ETL(url, access_token, path, node)
    asyncio.run(etl.etl())
