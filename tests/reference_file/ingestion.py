def make_schema():
    # result = invoke(
    #     "from",
    #     "-o",
    #     "minimal_schema.avro",
    #     "dict",
    #     url)

    url = "https://s3.amazonaws.com/dictionary-artifacts/gtexdictionary/4.4.3/schema.json"
    file_path = "reference_file/minimal_schema.avro"

    try:
        writer_pre = PFBWriter(file_path)
        with writer_pre as writer:
            output = _from_dict(writer, url)
    except Exception as e:
        print("Exception: ", str(e))

def make_file():
    # result = invoke(
    #     "from",
    #     "-o",
    #     "minimal_data.avro.old",
    #     "json",
    #     "-s",
    #     "minimal_schema.avro",
    #     "--program",
    #     "DEV",
    #     "--project",
    #     "test",
    #     "./pfb-data/ref_file/json"
    # )
    output_location = "minimal_data.avro.old"
    schema_location = "minimal_schema.avro"
    location_of_json_data_to_import = "./pfb-data/ref_file/json"

    try:
        writer_pre = PFBWriter(output_location)
        with writer_pre as writer:
            with PFBReader(schema_location) as reader_e:
                writer.copy_schema(reader_e)
            writer.write(_from_json(writer.metadata, location_of_json_data_to_import, "DEV", "test"))
    except Exception as e:
        print("Exception: ", str(e))

def show_file(invoke):
    result = invoke(
        "show",
        "-i",
        "./minimal_data.avro.old",
        "nodes"
    )
    return result

def test_thing():
    print("\n")
    print("test")


import sys
import glob


def from_json(metadata, path, program, project):
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
    # result = invoke(
    #     "from",
    #     "-o",
    #     "minimal_data.avro",
    #     "json",
    #     "-s",
    #     "./minimal_schema.avro",
    #     "--program",
    #     "NSRR",
    #     "--project",
    #     "CFS",
    #     "./pfb-data/ref_file/json/"
    # )

    try:
        with PFBWriter("reference_file/minimal_data.avro") as writer:
            with PFBReader("reference_file/minimal_schema.avro") as avro_reader:
                print("abc")
                writer.copy_schema(avro_reader)
            data_from_json = from_json(writer.metadata, "./pfb-data/ref_file/json/", "NSRR", "CFS")
            for entry in data_from_json:
                writer.write(entry)
            # writer.write(data_from_json)
    except Exception as e:
        print("Failed! -> ", e)
        raise
    else:
        print("Done!")
    print("result")

def test_pfb_nodes(runner, invoke, path_join):
    # pfb from -o minimal_schema.avro dict minimal_file.json
    # pfb from -o minimal_data.avro.old json -s minimal_schema.avro --program DEV --project test sample_file_json/
    # file_path = "./pfb-data/ref_file/ref_file_schema.avro"
    # make_schema()
    # make_file()
    # result = show_file(invoke)
    schema_location = "minimal_data.avro.old"
    # question: why does hardcoding schema location work, but just doing the command throw an error?
    try:
        with PFBReader(schema_location) as d_reader:
            for node in d_reader.schema:
                sys.stdout.write(node["name"])
                sys.stdout.write("\n")
    except StopIteration:
        print("\nIteration exhausted!")
