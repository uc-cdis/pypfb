import glob
import json
import os
import uuid

from fastavro import reader, writer, parse_schema
from inflection import singularize

from utils.str import str_hook, encode


def read_schema(filename):
    with open(filename, "rb") as pfb:
        schema = reader(pfb).writer_schema
        return schema


def read_metadata(filename):
    with open(filename, "rb") as pfb:
        metadata = next(reader(pfb))
        return metadata["object"]


def read_records(filename, reader_schema=None):
    """
    Read records from pfb file
    :param filename: the path of pfb
    :param reader_schema: the pfb schema
    :return:
    """
    with open(filename, "rb") as pfb:
        if reader_schema:
            rr = reader(pfb, reader_schema)
        else:
            rr = reader(pfb)
        for r in rr:
            yield r


def write_records(filename, schema, records):
    with open(filename, "w+b") as pfb:
        writer(pfb, schema, records)


def append_records(filename, schema, records):
    with open(filename, "a+b") as pfb:
        writer(pfb, schema, records)


def add_record(pfbFile, jsonFile):
    """
    Add records from json file

    :param pfbFile: the path of pfb file
    :param jsonFile: the path of json file
    :return: None
    """
    pfb = open(pfbFile, "a+b")
    jsonF = open(jsonFile, "rb")
    schema = reader(pfb).schema
    schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)

    records = []
    print("adding records from JSON file")
    for line in jsonF:
        print(line)
        jsonLine = json.loads(line, object_pairs_hook=str_hook)
        jsonInsert = {
            "id": jsonLine["id"],
            "name": jsonLine["name"],
            "val": (jsonLine["name"], jsonLine["val"]),
            "relations": jsonLine["relations"],
        }
        records.append(jsonInsert)
    writer(pfb, schema, records)


def make_record(pfbFile, node):
    """
    Make a record
    :param pfbFile: the path to pfb file
    :param node: the node name
    :return: None
    """
    pfb = open(pfbFile, "a+b")
    avro_reader = reader(pfb)
    schema = avro_reader.schema

    fields = {}
    x = 0
    schema_parents = len(schema["fields"][2]["type"])
    while x < schema_parents:
        if schema["fields"][2]["type"][x]["name"] == node:
            for y in schema["fields"][2]["type"][x]["fields"]:
                fields[y["name"]] = y["type"]
        x += 1

    uid = uuid.uuid4()

    val = {}
    for x in fields:
        if fields[x] == "long":
            val[x] = 0
        else:
            val[x] = ""

    record = {"id": str(uid), "name": node, "relations": [], "val": val}

    record = json.dumps(record)
    loaded_record = json.loads(record)

    print("Creating blank record for " + node + " in blank.json file")
    print(loaded_record)

    with open("blank.json", "wb+") as out:
        json.dump(loaded_record, out)


def _rename_node_in_records(filename_in, name_from, name_to):
    """
    rename a node in all data

    :param filename_in: the input
    :param filename_out: the outpu
    :param name_from: the old name

    :return: iterable object
    """
    for record in list(read_records(filename_in)):
        if record["name"] == name_from:
            record["name"] = name_to
        yield record


def _rename_node_in_schema(filename_in, name_from, name_to):
    """
    rename a node in schema
    
    :param filename_in: the input
    :param filename_out: the outpu
    :param name_from: the old name

    :return: schema
    """
    source_schema = read_schema(filename_in)
    for node in source_schema['fields'][2]['type']:
        if node["name"] == name_from:
            node["aliases"] = [name_from]
            node["name"] = name_to
            for fields in node["fields"]:
                for type in fields["type"]:
                    if isinstance(type, dict) and type.get('type') == "enum":
                        type["name"] = type["name"].replace(name_from, name_to)

    return source_schema

def rename_node(filename_in, filename_out, name_from, name_to):
    """
    rename a node

    :param filename_in: the input avro file
    :param filename_out: the output file
    :param name_from: the old name
    :param name_to: the new name 
    :return: None
    """
    source_schema = _rename_node_in_schema(filename_in, name_from, name_to)
    write_records(filename_out, source_schema, _rename_node_in_records(filename_in, name_from, name_to))


def convert_json(node_name, json_record, program, project):
    relations = []
    node_id = json_record["submitter_id"]
    vals = json_record

    to_del = None
    for item in json_record:
        if type(json_record[item]) == dict and "submitter_id" in json_record[item]:
            to_del = item
            v = item
            relations.append(
                {
                    "dst_id": json_record[item]["submitter_id"],
                    "dst_name": singularize(v).replace("_file", ""),
                }
            )

        if type(json_record[item]) == unicode:
            json_record[item] = encode(json_record[item])

    if to_del in vals:
        del vals[to_del]

    vals["project_id"] = "{}-{}".format(program, project)

    vals["created_datetime"] = None
    vals["updated_datetime"] = None

    return avro_record(node_id, node_name, vals, relations)


def avro_record(node_id, node_name, values, relations):
    node = {
        "id": node_id,
        "name": node_name,
        "object": (node_name, values),
        "relations": relations,
    }
    return node


# class AvroFileWrapper:
#     def __init__(self, filename):
#         self.filename = filename
#         self.fh = None
#         self.reader = None
#         self.reader_schema = None
#         self.writer_schema = None
#         self.metadata = None
#
#     def __enter__(self):
#         if self.fh is not None:
#             self.fh = open(self.filename)
#         else:
#             self.fh.seek(0)
#
#         self.reader = reader(self.fh)
#         self.writer_schema = self.reader.writer_schema
#         self.metadata = next(self.reader)
#         return self
#
#     def __exit__(self, exc_type, exc_value, traceback):
#         self.fh.close()
#         self.fh = None
#
#     def _write_avro(self, records, mode):
#         with open(self.filename, mode) as fh:
#             writer(fh, self.writer_schema, records)
#
#     def write(self, records):
#         self._write_avro(records, mode='w+b')
#
#     def append(self, records):
#         self._write_avro(records, mode='a+b')


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
    def from_json(
        source_pfb_filename, input_dir, output_pfb_filename, program, project
    ):
        schema = read_schema(source_pfb_filename)
        schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)
        parsed_schema = parse_schema(schema)

        metadata = [
            avro_record(None, "Metadata", read_metadata(source_pfb_filename), [])
        ]

        write_records(output_pfb_filename, parsed_schema, metadata)

        order = glob.glob(input_dir + "/*.json")

        total = len(order)
        i = 1

        for o in order:
            input_data = []

            o = os.path.basename(o).replace(".json", "").strip()
            print("{}/{}: {}".format(i, total, o))
            i = i + 1

            json_data = json.load(open(os.path.join(input_dir, o + ".json"), "r"))

            node_name = o

            for json_record in json_data:
                record = convert_json(node_name, json_record, program, project)
                input_data.append(record)

            append_records(output_pfb_filename, parsed_schema, input_data)
