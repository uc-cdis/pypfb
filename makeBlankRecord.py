import uuid
import json
import argparse
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Create a blank record from a pfb schema')
parser.add_argument('PFB_file', type=argparse.FileType('a+b'), default='test.pfb', help='pfb file to read schema from')
parser.add_argument('node', type=str, help='Node to add record to')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)
schema = avro_reader.schema

fields = []
x = 0
schemaParents = len(schema['fields'][2]['type'])
while x < schemaParents:
	if schema['fields'][2]['type'][x]['name'] == args.node:
		for y in schema['fields'][2]['type'][x]['fields']:
			fields.append(y['name'])
	x += 1

uid = uuid.uuid4()
name = args.node

val = {}
for x in fields:

	val[x] = ""
record  = {}
record["id"] = str(uid)
record["name"] = args.node 
record["relations"] = []
record["val"] = val


record = json.dumps(record)
loadedRecord = json.loads(record)

print "Creating blank record for " + args.node + " in blank.json file"

with open('blank.json', 'wb+') as out:
	json.dump(loadedRecord, out)