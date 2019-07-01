import glob
import json
import os
import uuid

from fastavro import reader, writer, parse_schema
from inflection import singularize

from utils.str import str_hook, encode, decode


def _read_schema(filename):
    """
    Read schema from a file
    :param filename(str): file name
    :return:
    """
    with open(filename, "rb") as pfb:
        schema = reader(pfb).writer_schema
        return schema


def _read_metadata(filename):
    with open(filename, "rb") as pfb:
        metadata = next(reader(pfb))
        return metadata["object"]


def _read_records(filename, reader_schema=None):
    """
    Read records from pfb file
    :param filename: the path of pfb
    :param reader_schema: the pfb schema
    :return:generator record object
    """
    with open(filename, "rb") as pfb:
        if reader_schema:
            rr = reader(pfb, reader_schema)
        else:
            rr = reader(pfb)
        for r in rr:
            yield r


def _write_records(filename, schema, records):
    with open(filename, "w+b") as pfb:
        writer(pfb, schema, records)


def _append_records(filename, schema, records):
    with open(filename, "a+b") as pfb:
        writer(pfb, schema, records)


def _add_record(pfbFile, jsonFile):
    """
    Add records from json file

    :param pfbFile: the path of pfb file
    :param jsonFile: the path of json file
    :return: None
    """

    pfb = open(pfbFile, "a+b")
    jsonF = open(jsonFile, "rb")
    schema = _read_schema(pfbFile)
    schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)

    records = []
    print("adding records from JSON file")
    for line in jsonF:
        print(line)
        json_line = json.loads(line, object_pairs_hook=str_hook)
        json_insert = {
            "id": json_line["id"],
            "name": json_line["name"],
            "val": (json_line["name"], json_line["val"]),
            "relations": json_line["relations"],
        }
        records.append(json_insert)
    writer(pfb, schema, records)


def _make_record(pfbFile, node, output):
    """
    Make a record
    :param pfbFile: the path to pfb file
    :param node: the node name
    :param output: the output filename storing the new node
    :return: None
    """
    pfb = open(pfbFile, "r+b")
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

    print("Creating blank record for " + node + " " + output)
    print(loaded_record)

    with open(output, "wb+") as out:
        json.dump(loaded_record, out)


def _rename_node_in_records(filename_in, name_from, name_to):
    """
    rename a node in all data

    :param filename_in: the input
    :param filename_out: the outpu
    :param name_from: the old name

    :return: iterable object
    """
    for record in list(_read_records(filename_in)):
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
    source_schema = _read_schema(filename_in)
    for node in source_schema["fields"][2]["type"]:
        if node["name"] == name_from:
            node["aliases"] = [name_to]
            node["name"] = name_to
            for fields in node["fields"]:
                for type in fields["type"]:
                    if isinstance(type, dict) and type.get("type") == "enum":
                        type["name"] = type["name"].replace(name_from, name_to)

    return source_schema


# Deprecated function
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
    _write_records(
        filename_out,
        source_schema,
        _rename_node_in_records(filename_in, name_from, name_to),
    )


def _rename_enum_in_schema(filename_in, field_name, val_from, val_to):
    """
    rename enum in schema
    filename_in: pfb file path
    :param val_from: original value
    :param val_to: new value
    """
    source_schema = _read_schema(filename_in)
    for type in source_schema["fields"][2]["type"]:
        for field in type["fields"]:
            if isinstance(field, dict) and field["name"] == field_name:
                for element in field.get("type", []):
                    try:
                        if element.get("type") == "enum":
                            for idx, s in enumerate(element["symbols"]):
                                if decode(s) == val_from:
                                    element["symbols"][idx] = encode(val_to)
                    except Exception as e:
                        pass

    return source_schema


def _rename_enum_in_data(filename_in, field_name, val_from, val_to):
    """
    rename enum in data records
    :param filename_in: pfb file path
    :param val_from: original value
    :param val_to: new value
    :return:
    """
    for record in _read_records(filename_in):
        if field_name in record["object"]:
            if decode(record["object"][field_name]) == val_from:
                record["object"][field_name] = encode(val_to)
        yield record


def convert_json(node_name, node_schema, json_record, program, project):
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

        is_enum = False

        current_node_schema = [x for x in node_schema["fields"] if x["name"] == item]

        if not current_node_schema:
            print("Undefined schema for node: {}".format(item))
        else:
            assert len(current_node_schema) == 1
            current_node_schema = current_node_schema[0]

            if isinstance(current_node_schema["type"], list):
                is_enum = False
                for x in current_node_schema["type"]:
                    if "type" in x:
                        is_enum = is_enum or (x["type"] == "enum")
            elif (
                "type" in current_node_schema["type"]
                and current_node_schema["type"]["type"] == "enum"
            ):
                is_enum = True

        if is_enum and json_record[item] is not None:
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


class Gen3PFB(object):
    def __init__(self, pfbfile):
        self.pfbfile = pfbfile
        self.schema = _read_schema(pfbfile)

    @staticmethod
    def from_json(
        source_pfb_filename, input_dir, output_pfb_filename, program, project
    ):
        schema = _read_schema(source_pfb_filename)
        schema = json.loads(json.dumps(schema), object_pairs_hook=str_hook)
        parsed_schema = parse_schema(schema)

        metadata = [
            avro_record(None, "Metadata", _read_metadata(source_pfb_filename), [])
        ]

        _write_records(output_pfb_filename, parsed_schema, metadata)

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

            node_schema = [x for x in parsed_schema["fields"][2]["type"] if x["name"] == node_name]

            if not node_schema:
                print("Empty schema for node {}".format(node_name))
                print("Probably an incorrect node name either in JSON or in PFB?")
            else:
                assert len(node_schema) == 1
                node_schema = node_schema[0]

            for json_record in json_data:
                record = convert_json(node_name, node_schema, json_record, program, project)
                input_data.append(record)

            _append_records(output_pfb_filename, parsed_schema, input_data)

    def read_schema(self):
        """
        Read schema from a pfb file
        :param filename(str): file name
        """
        return _read_schema(self.pfbfile)

    def read_metadata(self):
        """
        Read metadata from a pfb file
        """
        return _read_metadata(self.pfbfile)

    def read_records(self, schema=None):
        """
        Read records from pfb file
        :param schema: the pfb schema
        :return:generator record object
        """
        return _read_records(self.pfbfile, schema)

    def make_record(self, node, output="blank.json"):
        """
        :param node:
        :param output:
        :return:
        """
        _make_record(self.pfbfile, node, output)

    def add_record(self, json_file):
        """
        Add records from json file
        :param json_file: the path of json file
        """
        _add_record(self.pfbfile, json_file)

    def rename_node(self, output, name_from, name_to):
        """
        rename a node
        :param name_from: the old name
        :param name_to: the new name
        """
        source_schema = _rename_node_in_schema(self.pfbfile, name_from, name_to)
        records = _rename_node_in_records(self.pfbfile, name_from, name_to)
        _write_records(output, source_schema, records)

    def rename_field_enum(self, output, field_name, val_from, val_to):
        """
        rename a value of enum type
        :param output:
        :param field_name:
        :param val_from:
        :param val_to:
        :return:
        """
        source_schema = _rename_enum_in_schema(
            self.pfbfile, field_name, val_from, val_to
        )
        records = _rename_enum_in_data(self.pfbfile, field_name, val_from, val_to)
        _write_records(output, source_schema, records)

    def rename_property(self, property_from, property_to):
        raise NotImplementedError()

    def rename_type(self, type_from, type_to):
        raise NotImplementedError()
