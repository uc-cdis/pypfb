import csv
import gzip
import json
import yaml
import os
import shutil

from fastavro import reader

from pfb.base import decode_enum, encode_enum, str_hook


def _test_schema(r):
    for node in r.writer_schema["fields"][2]["type"]:
        if node["name"] == "experiment_metadata":
            for field in node["fields"]:
                if field["name"] == "state":
                    assert decode_enum(field["default"]) == "validated"
                    for type_ in field["type"]:
                        if isinstance(type_, dict) and type_["type"] == "enum":
                            for symbol in type_["symbols"]:
                                decode_enum(symbol)


def test_from_dict(runner, invoke):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "dict",
            "http://s3.amazonaws.com/dictionary-artifacts/kf-dictionary/1.1.0/schema.json",
        )
        assert result.exit_code == 0, result.output

        with open("output.avro") as f:
            r = reader(f)
            _test_schema(r)
            assert len(list(r)) == 1


def test_from_json(runner, invoke, path_join):
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "json",
            path_join("data"),
            "-s",
            path_join("schema", "kf.avro"),
            "--program",
            "DEV",
            "--project",
            "test",
        )
        assert result.exit_code == 0, result.output
        with open("output.avro") as f:
            r = reader(f)
            _test_schema(r)
            data = list(r)
            assert len(data) == 37
            for record in data:
                if record["name"] == "submitted_aligned_reads":
                    obj = record["object"]
                    if "soixantine_counterimpulse" in obj["submitter_id"]:
                        assert decode_enum(obj["state"]) == "validated"
                        assert decode_enum(obj["data_type"]) == "Aligned Reads"
                        assert decode_enum(obj["data_category"]) == "Sequencing Reads"
                        assert decode_enum(obj["file_state"]) == "registered"
                        assert obj["file_format"] == "thumb_cotranspire"
                        assert obj["file_name"] == "virtuosi_conticent"


def test_to_gremlin(runner, invoke, path_join, test_avro):
    with runner.isolated_filesystem():
        result = invoke("to", "gremlin", "./output", input=test_avro)
        assert result.exit_code == 0, result.output
        with gzip.open(os.path.join("output", "demographic.csv.gz")) as f:
            result = list(csv.DictReader(f))
            assert len(result) == 1
            result = result[0]
            result.pop("~id")
            assert result == {
                "ethnicity:String": "not hispanic or latino",
                "age_at_last_follow_up_days:Long": "18074",
                "cause_of_death:String": "Not Reported",
                "gender:String": "female",
                "~label": "demographic",
                "updated_datetime:String": "",
                "vital_status:String": "Dead",
                "submitter_id:String": "demographic_duteousness_unassailing",
                "project_id:String": "DEV-test",
                "created_datetime:String": "",
                "race:String": "Native Hawaiian or Other Pacific Islander",
                "state:String": "validated",
            }


def test_make(invoke, path_join):
    result = invoke("make", "-i", path_join("schema", "kf.avro"), "sample")
    assert result.exit_code == 0, result.output
    record = json.loads(result.output, object_pairs_hook=str_hook)
    record.pop("id")
    print(record)
    assert record == {
        "relations": [],
        "object": {
            "updated_datetime": "",
            "time_between_excision_and_freezing": 0,
            "submitter_id": "",
            "intermediate_dimension": 0,
            "created_datetime": "",
            "tumor_descriptor": 'Metastatic',
            "biospecimen_anatomic_site": 'Abdomen',
            "state": 'uploading',
            "diagnosis_pathologically_confirmed": 'Yes',
            "project_id": "",
            "current_weight": 0,
            "age_at_event_days": 0,
            "time_between_clamping_and_freezing": 0,
            "shortest_dimension": 0,
            "method_of_sample_procurement": 'Abdomino-perineal Resection of Rectum',
            "tissue_type": 'Tumor',
            "uberon_id_anatomical_site": "",
            "days_to_sample_procurement": 0,
            "spatial_descriptor": "",
            "ncit_id_tissue_type": "",
            "preservation_method": 'Cryopreserved',
            "composition": "Blood",
            "days_to_collection": 0,
            "ncit_id_anatomical_site": "",
            "initial_weight": 0,
            "external_id": "",
            "longest_dimension": 0,
        },
        "name": "sample",
    }
    return result.output


def test_add(runner, invoke, path_join):
    with runner.isolated_filesystem():
        shutil.copyfile(path_join("pfb-data", "test.avro"), "test.avro")
        with open("test.avro", "rb") as f:
            assert len(list(reader(f))) == 37
        result = invoke("add", "test.avro", input=test_make(invoke, path_join))
        assert result.exit_code == 0, result.output
        with open("test.avro", "rb") as f:
            assert len(list(reader(f))) == 38


def test_show(invoke, test_avro):
    result = invoke("show", input=test_avro)
    assert result.exit_code == 0, result.output
    assert len(result.output.splitlines()) == 36

    result = invoke("show", "-n", "1", input=test_avro)
    assert result.exit_code == 0, result.output
    result = json.loads(result.output, object_pairs_hook=str_hook)
    # result["object"].pop("md5sum")  # mute truffles
    print(json.dumps(result))
    assert result == {
        "object": {
            "cause_of_death": "Not Reported",
            "updated_datetime": None,
            "created_datetime": None,
            "gender": "female",
            "submitter_id": "demographic_duteousness_unassailing",
            "state": "validated",
            "race": "Native Hawaiian or Other Pacific Islander",
            "age_at_last_follow_up_days": 18074,
            "vital_status": "Dead",
            "project_id": "DEV-test",
            "ethnicity": "not hispanic or latino"
        },
        "id": "demographic_duteousness_unassailing",
        "relations": [
            {"dst_id": "participant_metalinguistics_monofilm","dst_name": "participant"}
        ],
        "name": "demographic"
    }

    result = invoke("show", "nodes", input=test_avro)
    assert result.exit_code == 0, result.output
    assert len(result.output.splitlines()) == 40
    assert "submitted_aligned_reads" in result.output.splitlines()

    result = invoke("show", "schema", input=test_avro)
    assert result.exit_code == 0, result.output

    result = invoke("show", "schema", "sample", input=test_avro)
    assert result.exit_code == 0, result.output

    result = invoke("show", "metadata", input=test_avro)
    assert result.exit_code == 0, result.output

    result = invoke("show", "metadata", "sample", input=test_avro)
    assert result.exit_code == 0, result.output


def test_rename_node(runner, invoke, test_avro):
    with runner.isolated_filesystem():
        result = invoke(
            "rename",
            "-o",
            "output.avro",
            "node",
            "outcome",
            "outcome2",
            input=test_avro,
        )
        assert result.exit_code == 0, result.output
        with open("output.avro", "rb") as f:
            r = reader(f)
            _test_schema(r)
            found = False
            for node in r.writer_schema["fields"][2]["type"]:
                if node["name"] == "outcome":
                    assert False
                if node["name"] == "outcome2":
                    found = True
                    break
            assert found
            assert "outcome" in node["aliases"]

            found = False
            for record in r:
                if record["name"] == "outcome":
                    assert False
                if record["name"] == "outcome2":
                    found = True
                    break
            assert found


def test_rename_enum(runner, invoke, test_avro):
    with runner.isolated_filesystem():
        result = invoke(
            "rename",
            "-o",
            "output.avro",
            "enum",
            "state",
            "validated",
            "validated2",
            input=test_avro,
        )
        assert result.exit_code == 0, result.output
        new = encode_enum("validated2")
        with open("output.avro", "rb") as f:
            r = reader(f)
            found = False
            for node in r.writer_schema["fields"][2]["type"]:
                if node["name"] == "outcome":
                    for field in node["fields"]:
                        if field["name"] == "state":
                            for t in field["type"]:
                                if isinstance(t, dict) and t["type"] == "enum":
                                    if new in t["symbols"]:
                                        found = True
                            if found and field.get("default"):
                                assert field["default"] == new
            assert found

            found = False
            for record in r:
                if record["name"] == "outcome":
                    assert record["object"]["state"] == new
                    found = True
                    break
            assert found
