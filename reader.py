#!/usr/bin/python

import argparse
from fastavro import reader, schemaless_reader, parse_schema

parser = argparse.ArgumentParser(description='Read a PFB file in json format')
parser.add_argument('PFB_file', type=argparse.FileType('rb'), default='test.pfb', help='pfb file to add record to. Default = test.pfb')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)

for record in avro_reader:
	print record
