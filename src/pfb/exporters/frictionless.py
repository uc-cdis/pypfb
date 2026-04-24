import click
from ..cli import to_command
from collections import OrderedDict
from contextlib import ExitStack
from pathlib import Path
import json
import csv
import os

@to_command.command('frictionless',
                    short_help='Convert PFB to frictionless data package(s)')
@click.argument('output', default='./frictionless/',
                type=click.Path(file_okay=False))
@click.pass_context
def frictionless(ctx, output):
    """Convert PFB into frictionless data package(s), one per study
    
    Each data package contains one or more tabular data resources, one per
    node. Nodes without data will yield an empty file so that package provides
    a complete representation of the project.
    """
    
    try:
        with ctx.obj['reader'] as reader:
            schemas = _get_schemas(reader)
            pkgs = _write_tables(schemas, reader, output)
            for pkg in pkgs:
                _write_descriptor(pkg, pkgs[pkg], schemas)
    except Exception as e:
        click.secho('Package creation failed!', fg='red', bold=True, err=True)
        raise e
    
    click.secho(f'Created {len(pkgs)} package(s) under: ', fg='green',
                err=True, nl=False, bold=True)
    click.secho(output, fg='white', err=True, bold=True)

def _add_type(field, descriptor):
    """Add field type to field descriptor"""
    
    type_map = {
        'string':'string',
        'float':'number',
        'long':'integer',
        'boolean':'boolean'
    }
    
    for type in field['type']:
        
        try:
            if type.get('type')=='enum':
                descriptor['type'] = 'string'
                descriptor['constraints'] = OrderedDict(enum=type['symbols'])
        except AttributeError:
            if type in type_map:
                descriptor['type'] = type_map[type]

def _field_descriptor(field, meta):
    """Generate Frictionless field descriptor"""
    
    # Use OrderedDict to improve readability of resulting schema
    descriptor = OrderedDict(name=field['name'])
    _add_type(field, descriptor)
    try:
        if meta.get('name')==descriptor['name']:
            if meta.get('ontology_reference'):
                descriptor['ontology_reference'] = meta['ontology_reference']
            if meta.get('values'):
                descriptor.update(meta['values'])
    except AttributeError:
        pass
    
    return descriptor

def _add_foreign_keys(schema, links):
    """Add foreign keys constructed from links"""
    
    foreign_keys = []
    for link in links:
        key = OrderedDict(fields=link['name'])
        key['reference'] = OrderedDict(resource=link['dst'])
        key['reference']['fields'] = 'submitter_id'
        foreign_keys.append(key)
        
        # TODO Handle case of one-to-many and many-to-many relationships
        # Add field to schema
        schema['fields'].append(OrderedDict(name=link['name']))
    
    schema['foreignKeys'] = foreign_keys

def _get_schema(record, meta):
    """Translate Gen3 node schema into Frictionless Table Schema"""
    
    fld_meta = {p['name']: p for p in meta['properties']}
    fields = []
    for field in record['fields']:
        fields.append(_field_descriptor(field, fld_meta.get(field['name'])))
    
    schema = OrderedDict(fields=fields)
    if 'submitter_id' in [f['name'] for f in fields]:
        schema['primaryKey'] = 'submitter_id'
    
    if meta.get('links'):
        _add_foreign_keys(schema, meta['links'])
    
    if meta.get('ontology_reference'):
        schema['ontology_reference'] = meta['ontology_reference']
    if meta.get('values'):
        schema.update(meta['values'])
    
    return schema

def _get_schemas(reader):
    """Get Frictionless Table Schemas"""
    
    meta = {node['name']: node for node in reader.metadata['nodes']}
    schemas = {}
    for node in reader.schema:
        schemas[node['name']] = _get_schema(node, meta[node['name']])
    
    return schemas

def _write_schema(name, schema, dirpath):
    """Write Frictionless table schema"""
    
    with open(dirpath / f'{name.lower()}.json', 'w') as f:
        f.write(json.dumps(schema, indent=4))

def _write_record(writer, record, schema):
    """Write record to file"""
    
    dict = record['object']
    relations = {i['dst_name']:i['dst_id'] for i in record['relations']}
    
    if 'foreignKeys' in schema:
        for k in schema['foreignKeys']:
            dict[k['fields']] = relations.get(k['reference']['resource'])
    
    writer.writerow(dict)

def _write_tables(schemas, reader, outdir):
    """Write tables containing data, one for each node within each project"""
    
    pkgs = {}
    writers = {}
    with ExitStack() as stack:
        for record in reader:
            project_id = record.get('object').get('project_id')
            if project_id not in pkgs:
                pkgs[project_id] = Path(outdir) / project_id.lower()
                click.secho('Creating package: ', fg='blue', err=True, nl=False)
                click.secho(pkgs[project_id].name.lower(), fg='white', err=True)
                
                for table in schemas:
                    path = pkgs[project_id] / 'data' / f'{table.lower()}.tsv'
                    os.makedirs(path.parent, exist_ok=True)
                    _write_schema(table, schemas[table], path.parent)
                    fieldnames = [f['name'] for f in schemas[table]['fields']]
                    writers[(project_id, table)] = csv.DictWriter(
                        stack.enter_context(open(path, 'w')), fieldnames,
                        delimiter='\t')
                    writers[(project_id, table)].writeheader()
            
            name = record.get('name')
            _write_record(writers[(project_id, name)], record, schemas[name])
    
    return pkgs

def _write_descriptor(project_id, path, schemas):
    """Write package descriptor file"""
    
    dialect = OrderedDict(delimiter='\t')
    resources = [OrderedDict(profile='tabular-data-resource',
                             name=schema.lower(),
                             path=f'data/{schema}.tsv',
                             format='csv',
                             mediatype='text/csv',
                             encoding='utf-8',
                             dialect=dialect,
                             schema=f'data/{schema}.json')
                 for schema in schemas]
    descriptor = OrderedDict(name=project_id.lower(),
                             resources=resources)
    
    with open(path / 'datapackage.json', 'w') as f:
        f.write(json.dumps(descriptor, indent=4))
