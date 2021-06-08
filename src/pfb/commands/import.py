import click
import requests
import json

from ..cli import main
from ..reader import PFBReader
from ..writer import PFBWriter
from gen3.auth import Gen3Auth
from gen3.file import Gen3File


@main.group()
@click.option(
    "-c",
    "--commons",
    "data_commons",
    metavar="URL",
    default="-",
    help="Data commons to import pfb to",
)
@click.option(
    "-d",
    "--database",
    "database_name",
    metavar="DATABASE",
    default="-",
    help="New database to import PFB file to",
)
@click.option(
    "-g",
    "--guid",
    "guid",
    metavar="GUID",
    default="-",
    help="GUID of the PFB file that you would like too import",
)
@click.option(
    "-s",
    "--credentials",
    "credentials_file",
    metavar="CREDENTIALS",
    default="-",
    help="API credentials file from commons [default: <stdin>]",
)
@click.pass_context
def importer(ctx, data_commons, database_name, guid, credentials_file):
    """Create job to import PFB to commons."""
    ctx.ensure_object(dict)
    ctx.obj["data_commons"] = data_commons
    ctx.obj["database_name"] = database_name
    ctx.obj["guid"] = guid
    ctx.obj["credentials_file"] = credentials_file


@importer.command()
@click.pass_context
def run(ctx):
    ctx.ensure_object(dict)
    try:
        auth = Gen3Auth(
            ctx.obj["data_commons"], refresh_file=ctx.obj["credentials_file"]
        )
        auth_url = ctx.obj["data_commons"] + "/user/credentials/cdis/access_token"

        # get access token
        auth_request = requests.post(auth_url, json=auth._refresh_token)
        access_token = auth_request.json()["access_token"]

        # set up sower requests
        headers = {"Authorization": "Bearer " + access_token}

        data = {
            "Action": "import",
            "input": {
                "guid": ctx.obj["guid"],
                "db": ctx.obj["database_name"],
            },
        }

        sower_url = ctx.obj["data_commons"] + "/job/dispatch"
        sower_request = requests.post(sower_url, json=data, headers=headers)

        if sower_request.status_code != 200:
            print("sower job start failed, below is the error report")
            print(sower_request.text)
            raise
        else:
            print(json.dumps(sower_request.json()))
            print("Job hass been started")
            job_id = sower_request.json()["uid"]

        job_status_url = ctx.obj["data_commons"] + "/job/status" + job_id

    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)


@importer.command()
@click.pass_context
def dry(ctx):
    ctx.ensure_object(dict)
    try:
        print("Checking Credentials\n")
        auth = Gen3Auth(
            ctx.obj["data_commons"], refresh_file=ctx.obj["credentials_file"]
        )
        auth_url = ctx.obj["data_commons"] + "/user/credentials/cdis/access_token"

        # get access token
        auth_request = requests.post(auth_url, json=auth._refresh_token)
        token_flag = True
        if auth_request.status_code != 200:
            click.secho(
                "There was an error with getting an access token. Are your api credentials valid and are you using the correct commons?",
                fg="red",
                bold=True,
                err=True,
            )
            token_flag = False
        else:
            click.secho("Access Token OK!\n", fg="green", err=True, bold=True)

        access_token = auth_request.json()["access_token"]

        file = Gen3File(ctx.obj["data_commons"], auth)

        print("Checking GUID:\n")
        guid_flag = True
        signed_url = file.get_presigned_url(ctx.obj["guid"], protocol="s3")

        if not isinstance(signed_url, dict):
            click.secho(
                "Could not get a presigned url check your GUID",
                fg="red",
                bold=True,
                err=True,
            )
            guid_flag = False

        if guid_flag:
            click.secho("GUID OK!\n", fg="green", err=True, bold=True)

        # set up sower requests
        headers = {"Authorization": "Bearer " + access_token}

        print("Checking sower permissions:\n")
        sower_job_list = ctx.obj["data_commons"] + "job/list"
        sower_test = requests.get(sower_job_list, headers=headers)

        sower_flag = True

        if sower_test.status_code != 200:
            click.secho(
                "There was an error with sower access. Check sower permissions!",
                fg="red",
                bold=True,
                err=True,
            )
            sower_flag = False
        else:
            click.secho("Sower permissions OK!\n", fg="green", err=True, bold=True)

        if not guid_flag or not sower_flag:
            click.secho(
                "Something is wrong. Check GUID or Sower permissions",
                fg="red",
                bold=True,
                err=True,
            )

    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise
    else:
        click.secho("Done!", fg="green", err=True, bold=True)
