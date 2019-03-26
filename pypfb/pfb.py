import json
import glob
import os

from fastavro import reader, writer, parse_schema
from inflection import singularize

from utils.str import str_hook, encode


def read_schema(filename):
    with open(filename, 'rb') as pfb:
        schema = reader(pfb).writer_schema
        return schema


def read_metadata(filename):
    with open(filename, 'rb') as pfb:
        metadata = next(reader(pfb))
        metadata = {
            'id': metadata['id'],
            'name': metadata['name'],
            'object': ('Metadata', metadata['object']),
            'relations': metadata['relations']
        }
        return metadata


def write_records(filename, schema, records):
    with open(filename, 'w+b') as pfb:
        writer(pfb, schema, records)


def append_records(filename, schema, records):
    with open(filename, 'a+b') as pfb:
        writer(pfb, schema, records)


def convert_json(node_name, json_record, program, project):
    relations = []
    node_id = json_record['submitter_id']
    vals = json_record

    to_del = None
    for item in json_record:
        if type(json_record[item]) == dict and 'submitter_id' in json_record[item]:
            to_del = item
            v = item
            relations.append(
                {'dst_id': json_record[item]['submitter_id'],
                 'dst_name': singularize(v).replace('_file', '')})

        if type(json_record[item]) == unicode:
            json_record[item] = encode(json_record[item])

    if to_del in vals:
        del vals[to_del]

    vals['project_id'] = '{}-{}'.format(program, project)

    vals['created_datetime'] = None
    vals['updated_datetime'] = None

    record = {
        'id': node_id,
        'name': node_name,
        'object': (node_name, vals),
        'relations': relations
    }
    return record


# class AvroFileWrapper:
#     def __init__(self, filename):
#         self.filename = filename
#         self.fh = None
#         self.reader = None
#         self.reader_schema = None
#         self.writer_schema = None
#         self.metadata = None
#
#     def __enter__(self, mode='read'):
#         self.fh = open(self.filename, mode)
#         self.reader = reader(self.fh)
#         self.writer_schema = self.reader.writer_schema
#         self.metadata = next(self.reader)
#         return self
#
#     def __exit__(self, exc_type, exc_value, traceback):
#         self.fh.close()
#
#     def write(self, records):
#         with open(self.filename, 'w+b') as fh:
#             writer(fh, self.writer_schema, records)
#
#     def append(self, filename, schema, records):
#         pass


class PFBFile:
    def __init__(self, filename):
        self.filename = filename

    # def __getattr__(self, item):
    #     with PFBFileWrapper(self.filename, mode='rb') as pfb:
    #         return getattr(pfb, item)
    #
    # def read(self):
    #     with PFBFileWrapper(self.filename, mode='rb') as pfb:
    #         for r in pfb.records:
    #             yield r
    #
    # def write(self, records, mode='wb'):
    #     pass

    @staticmethod
    def from_json(source_pfb_filename, input_dir, output_pfb_filename, program, project):
        schema = read_schema(source_pfb_filename)
        schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)
        parsed_schema = parse_schema(schema)

        metadata = [read_metadata(source_pfb_filename)]

        write_records(output_pfb_filename, parsed_schema, metadata)

        order = glob.glob(input_dir + '/*.json')

        total = len(order)
        i = 1

        for o in order:
            input_data = []

            o = os.path.basename(o).replace('.json', '').strip()
            print('{}/{}: {}'.format(i, total, o))
            i = i + 1

            json_data = json.load(open(os.path.join(input_dir, o + '.json'), 'r'))

            node_name = o

            for json_record in json_data:
                record = convert_json(node_name, json_record, program, project)
                input_data.append(record)

            append_records(output_pfb_filename, parsed_schema, input_data)
