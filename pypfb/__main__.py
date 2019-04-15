import argparse
import itertools
import logging.config

import yaml

from avro_utils.avro_schema import AvroSchema
from pypfb.pfb import *
from utils.dictionary import init_dictionary

default_level = logging.INFO
config_path = 'config.yml'

if os.path.exists(config_path):
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level)

log = logging.getLogger()


def main():
    parser = argparse.ArgumentParser(description='PFB tool')

    subparsers = parser.add_subparsers(dest='cmd')

    read_cmd = subparsers.add_parser('show', help='')
    read_cmd.add_argument('input', type=str, help='Path to PFB file')
    read_cmd.add_argument('-s', '--schema', action='store_true', help='Show PFB file schema')
    read_cmd.add_argument('--limit', type=int, required=False,
                          help='How many entries to show, -1 for all; ignored for "schema"')

    dict2pfb_cmd = subparsers.add_parser('dict2pfb', help='')
    dict2pfb_cmd.add_argument('-d', '--dictionary', required=True, type=str, help='Link to dictionary URL')
    dict2pfb_cmd.add_argument('-o', '--output', required=True, type=argparse.FileType('wb'), help='Output PFB file')

    json2pfb_cmd = subparsers.add_parser('json2pfb', help='')
    json2pfb_cmd.add_argument('dir', type=str, help='Path to directory with input JSON files')
    json2pfb_cmd.add_argument('-s', '--schema', type=str, required=True, help='Filename for raw PFB file')
    json2pfb_cmd.add_argument('-o', '--output', type=str, required=True, help='Filename for resulting PFB file')
    json2pfb_cmd.add_argument('--program', type=str, required=True, help='Name of the program')
    json2pfb_cmd.add_argument('--project', type=str, required=True, help='Name of the project')

    rename = subparsers.add_parser('rename', help='')

    sub_rename = rename.add_subparsers(dest='rename')
    node_rename = sub_rename.add_parser('node')
    node_rename.add_argument('-i', '--input', type=str, required=True, help='')
    node_rename.add_argument('-o', '--output', type=str, required=True, help='')
    node_rename.add_argument('--name_from', type=str, required=True, help='')
    node_rename.add_argument('--name_to', type=str, required=True, help='')

    type_rename = sub_rename.add_parser('type')
    type_rename.add_argument('-i', '--input', type=str, required=True, help='')
    type_rename.add_argument('-o', '--output', type=str, required=True, help='')
    type_rename.add_argument('--name_from', type=str, required=True, help='')
    type_rename.add_argument('--name_to', type=str, required=True, help='')

    enum_rename = sub_rename.add_parser('type')
    enum_rename.add_argument('-i', '--input', type=str, required=True, help='')
    enum_rename.add_argument('-o', '--output', type=str, required=True, help='')
    enum_rename.add_argument('--name_from', type=str, required=True, help='')
    enum_rename.add_argument('--name_to', type=str, required=True, help='')

    args = parser.parse_args()

    if args.cmd == 'show':
        if args.schema:
            # print(pfb.schema)
            print(read_metadata(args.input))
        else:
            limit = args.limit if args.limit != -1 else None
            for r in itertools.islice(read_records(args.input), limit):
                print(r)

    elif args.cmd == 'dict2pfb':
        dictionary, _ = init_dictionary(args.dictionary)
        schema = dictionary.schema

        log.info('Using dictionary: {}'.format(args.dictionary))

        avro_schema = AvroSchema.from_dictionary(schema)
        avro_schema.write(args.output)

    elif args.cmd == 'json2pfb':
        PFBFile.from_json(args.schema, args.dir, args.output, args.program, args.project)

    elif args.cmd == 'rename':
        if args.rename == 'node':
            rename_node(args.input, args.output, args.name_from, args.name_to)


if __name__ == '__main__':
    main()
