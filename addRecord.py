#!/usr/bin/python

import argparse
import json
import subprocess
from fastavro import reader, writer

def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for k, v in obj}

parser = argparse.ArgumentParser(description='Add a record to PFB file from a JSON file')
parser.add_argument('PFB_file', type=argparse.FileType('a+b'), default='test.pfb', help='pfb file to add record to. Default = test.pfb')
parser.add_argument('JSON_file', type=argparse.FileType('rb'), default='test.json', help='JSON file to add into the pfb file. Default = test.json')
args = parser.parse_args()

schema = reader(args.PFB_file).schema
schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)

records = []
for line in args.JSON_file:
	jsonLine = json.loads(line, object_pairs_hook=str_hook)
	jsonInsert = {
		'id': jsonLine['id'],
		'name': jsonLine['name'],
		'val': (jsonLine['name'], jsonLine['val']),
		'relations': jsonLine['relations']
	}
	records.append(jsonInsert)
	
print "adding records from JSON file"
writer(args.PFB_file, schema, records)