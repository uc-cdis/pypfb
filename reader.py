#!/usr/bin/python

import argparse
from fastavro import reader, schemaless_reader, parse_schema

parser = argparse.ArgumentParser(description='Read a PFB file in json format')
parser.add_argument('PFB_file', type=str, default='test.pfb', help='pfb file to add record to. Default = test.pfb')


def read(pfbFile):
	pfbFile = open(pfbFile, 'rb')
	avro_reader = reader(pfbFile)
	for record in avro_reader:
		print record

if __name__ == '__main__':
	args = parser.parse_args()
	read(args.PFB_file)