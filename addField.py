#!/usr/bin/python

import argparse
import json
import subprocess
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Add a new field to PFB schema')
parser.add_argument('PFB_file', type=argparse.FileType('rb'), default='test.pfb', help='pfb file to add record to. Default = test.pfb')
parser.add_argument('parentField', type=str, help='parent of field wanting to add to schema')
parser.add_argument('field', type=str, help='field to add to the schema')
parser.add_argument('fieldType', type=str, help='type for new field')
parser.add_argument('fieldDefault', type=str, help='default value for new field')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)

schema = avro_reader.schema

newField = {u'default':u''+args.fieldDefault,u'type':u''+args.fieldType, u'name':u''+args.field}

print "updating records from PFB by addding " + args.field + "with default value of" + args.fieldDefault
records = []
for record in avro_reader:
	if record['name'] == args.parentField:
		record['val'][u''+args.field] = u''+args.fieldDefault
	records.append(record)
print "records updated with new field \n"

print "updating schema with " + args.field
x = 0
schemaParents = len(schema['fields'][2]['type'])
while x < schemaParents:
	if schema['fields'][2]['type'][x]['name'] == args.parentField:
		schema['fields'][2]['type'][x]['fields'].append(newField)
		break
	x += 1
print "schema updated with new field"

with open('new.pfb', 'wb+') as out:
	writer(out, schema, records)



