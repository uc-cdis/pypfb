import uuid
import json
import argparse
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Create a blank record from a pfb schema')
parser.add_argument('PFB_file', type=str, default='test.pfb', help='pfb file to read schema from')
parser.add_argument('node', type=str, help='Node to add record to')

def makeRecord(pfbFile, node):
	pfb = open(pfbFile, 'a+b')
	avro_reader = reader(pfb)
	schema = avro_reader.schema

	fields = {}
	x = 0
	schemaParents = len(schema['fields'][2]['type'])
	while x < schemaParents:
		if schema['fields'][2]['type'][x]['name'] == node:
			for y in schema['fields'][2]['type'][x]['fields']:
				fields[y["name"]] = y["type"]
		x += 1

	uid = uuid.uuid4()
	name = node

	val = {}
	for x in fields:
		if fields[x] == "long":
			val[x] = 0
		else:
			val[x] = ""


	record  = {}
	record["id"] = str(uid)
	record["name"] = node 
	record["relations"] = []
	record["val"] = val


	record = json.dumps(record)
	loadedRecord = json.loads(record)

	print "Creating blank record for " + node + " in blank.json file"
	print loadedRecord

	with open('blank.json', 'wb+') as out:
		json.dump(loadedRecord, out)


if __name__ == '__main__':
	args = parser.parse_args()
	makeRecord(args.PFB_file, args.node)
