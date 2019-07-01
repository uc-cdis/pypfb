import os
import argparse
import itertools
import logging.config

import yaml

from avro_utils.avro_schema import AvroSchema
from pypfb.pfb import Gen3PFB
from utils.dictionary import init_dictionary

default_level = logging.INFO
config_path = "config.yml"

if os.path.exists(config_path):
    with open(config_path, "rt") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level)

log = logging.getLogger()


def main():
    parser = argparse.ArgumentParser(description="PFB tool")

    subparsers = parser.add_subparsers(dest="cmd")

    read_cmd = subparsers.add_parser(
        "show", help="Show schema or records of the PFB file"
    )
    read_cmd.add_argument("input", type=str, help="Path to PFB file")
    read_cmd.add_argument(
        "-s", "--schema", action="store_true", help="Show PFB file schema"
    )
    read_cmd.add_argument(
        "--limit",
        type=int,
        required=False,
        help='How many entries to show, -1 for all; ignored for "schema"',
    )

    dict2pfb_cmd = subparsers.add_parser(
        "dict2pfb", help="Convert datadictionary into PFB file with schema"
    )
    dict2pfb_cmd.add_argument(
        "-d", "--dictionary", required=True, type=str, help="Link to dictionary URL"
    )
    dict2pfb_cmd.add_argument(
        "-o",
        "--output",
        required=True,
        type=argparse.FileType("wb"),
        help="Output PFB file",
    )

    json2pfb_cmd = subparsers.add_parser(
        "json2pfb", help="Convert JSON files correspond to datadictionary into PFB file"
    )
    json2pfb_cmd.add_argument(
        "dir", type=str, help="Path to directory with input JSON files"
    )
    json2pfb_cmd.add_argument(
        "-s", "--schema", type=str, required=True, help="Filename for schema PFB file"
    )
    json2pfb_cmd.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Filename for resulting PFB file",
    )
    json2pfb_cmd.add_argument(
        "--program", type=str, required=True, help="Name of the program"
    )
    json2pfb_cmd.add_argument(
        "--project", type=str, required=True, help="Name of the project"
    )

    make_cmd = subparsers.add_parser("make", help="Make blank record")
    make_cmd.add_argument("input", type=str, help="Path to PFB file")
    make_cmd.add_argument("-n", "--node", type=str, help="Node to create")

    add_cmd = subparsers.add_parser(
        "add", description="Add a record to PFB file from a minified JSON file"
    )
    add_cmd.add_argument(
        "PFB_file",
        type=str,
        default="test.pfb",
        help="pfb file to add record to. Default = test.pfb",
    )
    add_cmd.add_argument(
        "JSON_file",
        type=str,
        default="test.json",
        help="JSON file to add into the pfb file. Default = test.json",
    )

    rename = subparsers.add_parser("rename", help="Rename different parts of schema")

    sub_rename = rename.add_subparsers(dest="rename")
    node_rename = sub_rename.add_parser("node", help="Rename node")
    node_rename.add_argument("-i", "--input", type=str, required=True, help="")
    node_rename.add_argument("-o", "--output", type=str, required=True, help="")
    node_rename.add_argument("--name_from", type=str, required=True, help="")
    node_rename.add_argument("--name_to", type=str, required=True, help="")

    type_rename = sub_rename.add_parser("type", help="Rename type (not implemented)")
    type_rename.add_argument("-i", "--input", type=str, required=True, help="")
    type_rename.add_argument("-o", "--output", type=str, required=True, help="")
    type_rename.add_argument("--name_from", type=str, required=True, help="")
    type_rename.add_argument("--name_to", type=str, required=True, help="")

    enum_rename = sub_rename.add_parser("enum", help="Rename enum (not implemented)")
    enum_rename.add_argument("-i", "--input", type=str, required=True, help="")
    enum_rename.add_argument("-o", "--output", type=str, required=True, help="")
    enum_rename.add_argument("--field_name", type=str, required=True, help="")
    enum_rename.add_argument("--val_from", type=str, required=True, help="")
    enum_rename.add_argument("--val_to", type=str, required=True, help="")

    args = parser.parse_args()

    if args.cmd == "dict2pfb":
        dictionary, _ = init_dictionary(args.dictionary)
        schema = dictionary.schema

        log.info("Using dictionary: {}".format(args.dictionary))

        avro_schema = AvroSchema.from_dictionary(schema)
        avro_schema.write(args.output)

    elif args.cmd == "json2pfb":
        Gen3PFB.from_json(
            args.schema, args.dir, args.output, args.program, args.project
        )

    elif args.cmd == "show":
        if args.schema:
            print(Gen3PFB(args.input).read_metadata())
        else:
            limit = args.limit if args.limit != -1 else None
            for r in itertools.islice(Gen3PFB(args.input).read_records(), limit):
                print(r)
    elif args.cmd == "make":
        Gen3PFB(args.input).make_record(args.node)

    elif args.cmd == "add":
        Gen3PFB(args.PFB_file).add_record(args.JSON_file)

    elif args.cmd == "rename":
        if args.rename == "node":
            Gen3PFB(args.input).rename_node(args.output, args.name_from, args.name_to)
        elif args.rename == "enum":
            Gen3PFB(args.input).rename_field_enum(
                args.output, args.field_name, args.val_from, args.val_to
            )


if __name__ == "__main__":
    main()
