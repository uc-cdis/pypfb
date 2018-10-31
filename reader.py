#!/usr/bin/python

import argparse
import subprocess
from fastavro import reader, schemaless_reader, parse_schema

parser = argparse.ArgumentParser(description='Read a PFB file in json format')
parser.add_argument('PFB_file', type=argparse.FileType('rb'), default='test.pfb', help='pfb file to add record to. Default = test.pfb')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)
x = 0
for record in avro_reader:
	# if record['name'] == 'simple_germline_variation':
	x += 1
	print record
# pfbSchema = reader(args.PFB_file).schema
# print(pfbSchema)
# schema = parse_schema(pfbSchema)

# record = schemaless_reader(args.PFB_file, pfbSchema)
# print record