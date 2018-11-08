#!/usr/bin/python

import argparse
import json
from fastavro import reader, writer

def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for k, v in obj}

parser = argparse.ArgumentParser(description='Add a record to PFB file from a minified JSON file')
parser.add_argument('PFB_file', type=str, default='test.pfb', help='pfb file to add record to. Default = test.pfb')
parser.add_argument('JSON_file', type=str, default='test.json', help='JSON file to add into the pfb file. Default = test.json')

def add(pfbFile, jsonFile):
	pfb = open(pfbFile, 'a+b')
	jsonF = open(jsonFile, 'rb')
	schema = reader(pfb).schema
	schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)

	records = []
	print "adding records from JSON file"
	for line in jsonF:
		print line
		jsonLine = json.loads(line, object_pairs_hook=str_hook)
		jsonInsert = {
			'id': jsonLine['id'],
			'name': jsonLine['name'],
			'val': (jsonLine['name'], jsonLine['val']),
			'relations': jsonLine['relations']
		}
		records.append(jsonInsert)
	writer(pfb, schema, records)

if __name__ == '__main__':
	args = parser.parse_args()
	add(args.PFB_file, args.JSON_file)