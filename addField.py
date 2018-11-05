#!/usr/bin/python

import argparse
import json
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Add a new field to PFB schema')
parser.add_argument('PFB_file', type=str, default='test.pfb', help='pfb file to add record to. Default = test.pfb')
parser.add_argument('parentField', type=str, help='parent of field wanting to add to schema')
parser.add_argument('field', type=str, help='field to add to the schema')
parser.add_argument('fieldType', type=str, help='type for new field')
parser.add_argument('fieldDefault', type=str, help='default value for new field')


def add(pfbFile, parField, newField, newFieldType, newFieldDefault):
	pfb = open(pfbFile, 'rb')
	avro_reader = reader(pfb)

	schema = avro_reader.schema

	newFieldDict = {u'default':u''+newFieldDefault,u'type':u''+newFieldType, u'name':u''+newField}

	print "updating records from PFB by addding " + newField + " with default value of " + newFieldDefault
	records = []
	for record in avro_reader:
		if record['name'] == parField:
			record['val'][u''+newField] = u''+newFieldDefault
		records.append(record)
	print "records updated with new field \n"

	print "updating schema with " + newField
	x = 0
	schemaParents = len(schema['fields'][2]['type'])
	while x < schemaParents:
		if schema['fields'][2]['type'][x]['name'] == parField:
			schema['fields'][2]['type'][x]['fields'].append(newFieldDict)
			break
		x += 1
	print "schema updated with new field"

	with open('new.pfb', 'wb+') as out:
		writer(out, schema, records)

if __name__ == '__main__':
	args = parser.parse_args()
	add(args.PFB_file, args.parentField, args.field, args.fieldType, args.fieldDefault)
