import argparse

from python_pfb_sdk.reader import records


def main():
    parser = argparse.ArgumentParser(description='PFB tool')

    # parser.add_argument('-i', '--input', type=argparse.FileType('rb'), help='')
    parser.add_argument('-i', '--input', type=str, help='')

    subparsers = parser.add_subparsers(dest='cmd')

    read_cmd = subparsers.add_parser('read', help='')

    # to_subparser = subparsers.add_parser('to-psql', help='Export to PostgreSQL from PFB file')
    # to_subparser.add_argument('-t', '--table', type=str, default='export',
    #                           help='Optional table name for export in PostgreSQL')
    # to_subparser.add_argument('-i', '--input', type=argparse.FileType('rb'),
    #                           help='PFB file with PostgreSQL data for export')

    args = parser.parse_args()

    for record in records(args.input):
        print(record)


if __name__ == '__main__':
    main()
