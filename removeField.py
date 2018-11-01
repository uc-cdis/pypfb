#!/usr/bin/python

import argparse
import json
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Remove a field from PFB schema')
parser.add_argument('PFB_file', type=argparse.FileType('rb'), default='test.pfb', help='pfb file to remove field from. Default = test.pfb')
parser.add_argument('parentField', type=str, help='parent of field wanting to remove from schema')
parser.add_argument('field', type=str, help='field to remove to the schema')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)

schema = avro_reader.schema

print "updating records from PFB file by removing" + args.field
records = []
for record in avro_reader:
	if record['name'] == args.parentField:
		del record['val'][args.field]
	records.append(record)

x = 0
schemaParents = len(schema['fields'][2]['type'])
while x < schemaParents:
	if schema['fields'][2]['type'][x]['name'] == args.parentField:
		for y in schema['fields'][2]['type'][x]['fields']:
			if y['name'] == args.field:
				print "removing " + args.field + " from schema"
				schema['fields'][2]['type'][x]['fields'].remove(y)
				break
		break
	x += 1

print "writing to new file rm.pfb"
with open('rm.pfb', 'wb+') as out:
	writer(out, schema, records)