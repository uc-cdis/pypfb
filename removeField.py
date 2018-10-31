#!/usr/bin/python

import argparse
import json
import subprocess
from fastavro import reader, writer

parser = argparse.ArgumentParser(description='Remove a field from PFB schema')
parser.add_argument('PFB_file', type=argparse.FileType('rb'), default='test.pfb', help='pfb file to add record to. Default = test.pfb')
parser.add_argument('parentField', type=str, help='parent of field wanting to add to schema')
parser.add_argument('field', type=str, help='field to add to the schema')

args = parser.parse_args()

avro_reader = reader(args.PFB_file)

schema = avro_reader.schema
