import os
import json
from pfb.writer import PFBWriter
from pfb.importers.gen3dict import _from_dict
from pfb.reader import PFBReader
from pfb.importers.json import _convert_json
from fastavro import reader as avro_reader
import glob
import itertools
import sys


def test_example_bdc_schema(runner):
    """
    mimics
    invoke("from", "-o", "minimal_schema.avro", "dict", "<url>")
    """
    with runner.isolated_filesystem():
        url = "https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/4.4.3/schema.json"
        file_path = "schema_example.avro"
        try:
            writer_pre = PFBWriter(file_path)
            with writer_pre as writer:
                _from_dict(writer, url)
                with open(file_path, "rb") as output_file:
                    r = avro_reader(output_file)
                    entity = r.writer_schema["entity"]
                    # test_schema(r)
                    assert entity == "Entity"
        except Exception as e:
            print("Exception: ", str(e))


def from_json(metadata, path, program, project):
    """
    stolen from elsewhere
    """
    link_dests = {
        node["name"]: {link["name"]: link["dst"] for link in node["links"]}
        for node in metadata["nodes"]
    }
    order = glob.glob(os.path.join(path, "*.json"))
    total = len(order)
    final_json = []
    enumerated_order = enumerate(order)
    for i, o in enumerated_order:
        o = os.path.basename(o).replace(".json", "").strip()

        with open(os.path.join(path, o + ".json"), "r") as f:
            json_data = json.load(f)

        node_name = o

        if isinstance(json_data, dict):
            json_data = [json_data]
        json_records = []
        for json_record in json_data:
            record = _convert_json(node_name, json_record, program, project, link_dests)
            json_records.append(record)
        final_json.append(json_records)
    return final_json


def test_pfb_import(runner, invoke, path_join):
    """
    mimics
    invoke(
        "from",
        "-o",
        "./output/example_reference_file.avro",
        "json",
        "-s",
        "./minimal_schema.avro",
        "--program",
        "NSRR",
        "--project",
        "CFS",
        "json/")
    """
    try:
        with PFBReader("avro/minimal_schema.avro") as s_reader:
            data_from_json = from_json(s_reader.metadata, "json/", "NSRR", "CFS")
            with runner.isolated_filesystem():
                with PFBWriter("minimal_data.avro") as d_writer:
                    d_writer.copy_schema(s_reader)
                    for entry in data_from_json:
                        d_writer.write(entry)
                with PFBReader("minimal_data.avro") as d_reader:
                    for r in itertools.islice(d_reader, None):
                        json.dump(r, sys.stdout)
                        sys.stdout.write("\n")
    except Exception as e:
        print("Failed! -> ", e)
        raise


def test_reference_file_nodes(runner, invoke, path_join):
    """
    mimics the command
    invoke("show", "-i", "./minimal_data.avro.old", "nodes")
    """
    schema_location = "./minimal_data.avro"
    try:
        with PFBReader(schema_location) as d_reader:
            for node in d_reader.schema:
                print(node["name"])
    except FileNotFoundError:
        print("File not found!")
    except StopIteration:
        print("\nIteration exhausted!")
    except Exception as e:
        print("Unrecognized exception: ", e)