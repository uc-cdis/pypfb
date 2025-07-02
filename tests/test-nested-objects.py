import json
import pathlib
import pytest
from fastavro import reader


def _test_schema(r):
    names = []
    for node in r.writer_schema["fields"][2]["type"]:
        assert "name" in node, f"Expected 'name' in node, got {node}"
        names.append(node["name"])
    assert "person" in names, "person is not in the schema"


@pytest.fixture()
def gen3_schema_path():
    """Fixture to provide the path to the Gen3 schema."""
    return "tests/nested-objects/nested-objects-gen3-schema.json"


@pytest.fixture()
def avro_data_path():
    """Fixture to provide the path to the output Avro file."""
    return "tests/nested-objects/output.avro"


@pytest.fixture()
def avro_schema_path():
    """Fixture to provide the path to the output Avro file."""
    return "tests/nested-objects/schema.avro"


def _create_schema(avro_schema_path, gen3_schema_path, invoke):
    """Create the schema for the test."""
    # delete the output file if it exists
    pathlib.Path(avro_schema_path).unlink(missing_ok=True)

    params = [
        "from",
        "-o",
        avro_schema_path,
        "dict",
        gen3_schema_path,
    ]
    print(f"Creating schema with params: pfb {' '.join(params)}")
    # convert to pfb
    result = invoke(*params)
    assert result.exit_code == 0, result.output


def test_gen3_schema(invoke, gen3_schema_path, avro_schema_path):
    """Test the gen3 to pfb schema."""
    # basic test to check if the schema exists

    _create_schema(avro_schema_path, gen3_schema_path, invoke)

    with open(avro_schema_path, "rb") as f:
        r = reader(f)
        _test_schema(r)
        assert len(list(r)) == 1


def test_nested_data(invoke, gen3_schema_path, avro_schema_path, avro_data_path):
    """Test the gen3 to pfb schema."""
    _create_schema(avro_schema_path, gen3_schema_path, invoke)

    result = invoke(
        "from",
        "-o",
        avro_data_path,
        "json",
        "-s",
        avro_schema_path,
        "--program",
        "DEV",
        "--project",
        "test",
        "tests/nested-objects/data",
    )

    assert result.exit_code == 0, result.output

    params = ["show",
              "-i",
              avro_data_path]

    print(f"Showing data with params: pfb {' '.join(params)}")
    result = invoke(*params)

    assert result.exit_code == 0, result.output
    lines = result.output.rstrip().split('\n')
    assert len(lines) == 2, f"Expected 2 lines, got {len(lines)}:\n{lines}"
    for line in lines:
        person = json.loads(line)
        assert person["name"] == "person", f"Expected person, got {person['name']}"
        assert "submitter_id" in person["object"], f"Expected person with submitter_id, got {person}"
        assert person["object"]["submitter_id"], f"Expected person with submitter_id, got {person}"
        assert "address" in person["object"], f"Expected person with address, got {person}"
        assert person["object"]["address"].keys() == {'city': 'Wonderland', 'street': '123 Main St', 'zip_code': '12345', 'coordinates': {'latitude': 37.7749, 'longitude': -122.4194}, 'directions': ['left', 'right', 'straight'], 'is_primary': True}.keys(), f"Expected person with address, got {person['object']['address']}"


def test_deeply_nested_data(invoke, gen3_schema_path, avro_schema_path, avro_data_path):
    """Test the gen3 to pfb schema."""
    _create_schema(avro_schema_path, gen3_schema_path, invoke)

    params = ["from",
              "-o",
              avro_data_path,
              "json",
              "-s",
              avro_schema_path,
              "--program",
              "DEV",
              "--project",
              "test",
              "tests/nested-objects/deeply-nested-data", ]

    print(f"Loading data with params: pfb {' '.join(params)}")

    result = invoke(*params)

    assert result.exit_code == 0, result.output

    params = ["show",
              "-i",
              avro_data_path]

    print(f"Showing data with params: pfb {' '.join(params)}")
    result = invoke(*params)

    assert result.exit_code == 0, result.output
    lines = result.output.rstrip().split('\n')
    assert len(lines) == 1, f"Expected 2 lines, got {len(lines)}:\n{lines}"
    for line in lines:
        person = json.loads(line)
        assert person["name"] == "person", f"Expected person, got {person['name']}"
        assert "submitter_id" in person["object"], f"Expected person with submitter_id, got {person}"
        assert person["object"]["submitter_id"], f"Expected person with submitter_id, got {person}"
        assert "address" in person["object"], f"Expected person with address, got {person}"
        assert person["object"]["address"].keys() == {'city': 'Wonderland', 'street': '123 Main St', 'zip_code': '12345', 'coordinates': {'latitude': 37.7749, 'longitude': -122.4194}, 'directions': ['left', 'right', 'straight'], 'is_primary': True, "test_map": {}}.keys(), f"Expected person with address, got {person['object']['address']}"
