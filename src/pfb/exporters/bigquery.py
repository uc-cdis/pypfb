import os

from google.cloud import bigquery as bq

import click

from ..cli import to_command

# see the TODO
#from .tsv import tsv
#from ..reader import PFBReader


@to_command.command("bigquery", short_help="Convert PFB to bigquery tables.")
@click.argument("output", default="isb", type=click.Path(file_okay=False))
@click.pass_context
def bigquery(ctx, output):
    """Convert PFB into bigQuery yielding one table per node.

    Dataset will correspond to the project.
    """
    handlers_by_name = {}
    try:
        with ctx.obj["reader"] as reader:
            num_tables = _to_bigquery(reader, output, handlers_by_name)
    except Exception:
        click.secho("Failed!", fg="red", bold=True, err=True)
        raise

    click.secho(
        "Done, created %d tables under: " % num_tables,
        fg="green",
        err=True,
        nl=False,
        bold=True,
    )
    click.secho(output, fg="white", err=True, bold=True)


def _to_bigquery(reader, dir_path, handlers_by_name):
    num_tables = 0


    fields_by_name = {node["name"]: node["fields"] for node in reader.schema}
    for row in reader:
        name = row["name"]
        fields = fields_by_name[name]

        obj = row["object"]

        for r in row["relations"]:

            rel_name = r["dst_name"] + "_ref_id"
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
            obj[rel_name] = r["dst_id"]

        # get the bq table for this row, create one if not created
        t = handlers_by_name.get(name)
        if t is None:
            table_id = "{}.{}".format(dir_path, name) 
            client, table = _make_table(table_id, fields)

            click.secho("Creating ", fg="blue", err=True, nl=False)
            click.secho(table_id, fg="white", err=True)
            num_tables += 1
            handlers_by_name[name] = table_id

    temptsvs = './tsvs'
    #TODO get the following working to create the tsvs as part of this job
    # or use some in memory method of staging the tables.
    # Perhaps use bigquery client.load_table_from_dataframe
    # Dataframes might be a route to lots of export formats :-)
    #tsvReader = PFBReader(reader._file_or_path)
    #tsv(tsvReader, temptsvs)

    for table, table_id in handlers_by_name.items():
        filepath =  '{}/{}.tsv'.format(temptsvs, table)
        print ('loading {}'.format(filepath))
        loadBQTable(client, table_id, filepath, skip_leading_rows=1)
        
    return num_tables

def fixName(name):
    name = name.replace(".", "_")
    name = name.replace("-", "_")
    return name
    
def _make_table(table_id, fields):
    
    schema = []
    schema.append(bq.SchemaField('id', 'string', mode="NULLABLE",description=''))
    
    for field in fields:
        field_type = "string"
        avro_type = field["type"]
        if isinstance(avro_type, list):
            for field_type in avro_type:
                if field_type != "null":
                    break
        if  not isinstance(field_type, str):
            # structural types are simply treated as string
            field_type = "String"
            
        #header_row.append(field["name"] + ":" + field_type)
        
        #print ('Processing column:',field)    

        name = fixName(field["name"])
            
        if "doc" in field:
            desc = field["doc"]
        else:
            desc = ""
        
        
        field_type = field_type.lower()
        if field_type == 'long': field_type = 'integer'
        schema.append(bq.SchemaField(name, field_type, mode="NULLABLE",description=desc))

    #print(table_id)
    client = bq.Client()
    table = bq.Table(table_id, schema=schema)
    #table.description = dataDict.getTableDesc()
    table = client.create_table(table, exists_ok=True)  # API request
    return client, table
    

def loadBQTable(client, table_ref, filename, skip_leading_rows=1):

    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.CSV
    job_config.skip_leading_rows = skip_leading_rows
    job_config.autodetect = False
#    job_config.allowJaggedRows = True
    job_config.field_delimiter = '\t'

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="US",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request

    job.result()  # Waits for table load to complete

    print("Loaded {} rows into {}.".format(job.output_rows, table_ref))    
