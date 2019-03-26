import argparse

from pypfb.pfb import *


def main():
    parser = argparse.ArgumentParser(description="PFB tool")

    subparsers = parser.add_subparsers(dest="cmd")

    read_cmd = subparsers.add_parser("show", help="")
    read_cmd.add_argument("input", type=str, help="Path to PFB file")
    read_cmd.add_argument("-s", "--schema", action='store_true', help="Show PFB file schema")

    json2pfb_cmd = subparsers.add_parser("json2pfb", help="")
    json2pfb_cmd.add_argument("dir", type=str, help="Path to directory with input JSON files")
    json2pfb_cmd.add_argument("-s", "--schema", type=str, required=True, help="Filename for raw PFB file")
    json2pfb_cmd.add_argument("-o", "--output", type=str, required=True, help="Filename for resulting PFB file")
    json2pfb_cmd.add_argument("--program", type=str, required=True, help="Name of the program")
    json2pfb_cmd.add_argument("--project", type=str, required=True, help="Name of the project")

    args = parser.parse_args()

    if args.cmd == "show":
        pfb = PFBFile(args.input)
        if args.schema:
            # print(pfb.schema)
            print(pfb.metadata)
        else:
            for r in pfb.read():
                print(r)

    elif args.cmd == "json2pfb":
        PFBFile.from_json(args.schema, args.dir, args.output, args.program, args.project)


if __name__ == "__main__":
    main()
