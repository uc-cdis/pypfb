import json
import pathlib

import pytest
from fastavro import reader


def _test_schema(r):
    names = []
    for node in r.writer_schema["fields"][2]["type"]:
        names.append(node["name"])
    assert "person" in names, "person is not in the schema"


@pytest.fixture()
def gen3_schema_path():
    """Fixture to provide the path to the Gen3 schema."""
    return "tests/foaf/gen3/foaf_schema.json"


@pytest.fixture()
def avro_data_path():
    """Fixture to provide the path to the output Avro file."""
    return "tests/foaf/output.avro"


@pytest.fixture()
def avro_schema_path():
    """Fixture to provide the path to the output Avro file."""
    return "tests/foaf/schema.avro"


def _create_schema(avro_schema_path, gen3_schema_path, invoke):
    """Create the schema for the test."""
    # delete the output file if it exists
    pathlib.Path(avro_schema_path).unlink(missing_ok=True)

    # convert to pfb
    result = invoke(
        "from",
        "-o",
        avro_schema_path,
        "dict",
        gen3_schema_path,
    )
    assert result.exit_code == 0, result.output


def test_gen3_schema(invoke, gen3_schema_path, avro_schema_path):
    """Test the gen3 to pfb schema."""
    # basic test to check if the schema exists

    _create_schema(avro_schema_path, gen3_schema_path, invoke)

    with open(avro_schema_path, "rb") as f:
        r = reader(f)
        _test_schema(r)
        assert len(list(r)) == 1


def test_foaf_data_no_links(invoke, gen3_schema_path, avro_schema_path, avro_data_path):
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
        "tests/foaf/data/no-links",
    )

    assert result.exit_code == 0, result.output

    result = invoke(
        "show",
        "-i",
        avro_data_path
    )

    assert result.exit_code == 0, result.output
    lines = result.output.rstrip().split('\n')
    assert len(lines) == 2, f"Expected 2 lines, got {len(lines)}:\n{lines}"
    for line in lines:
        person = json.loads(line)
        assert person["name"] == "person", f"Expected person, got {person['name']}"
        assert person["object"]["submitter_id"], f"Expected person with submitter_id, got {person}"


def test_foaf_data_links(invoke, gen3_schema_path, avro_schema_path, avro_data_path):
    """Test the gen3 to pfb schema, link `label`."""
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
        "tests/foaf/data/links",
    )

    assert result.exit_code == 0, result.output

    result = invoke(
        "show",
        "-i",
        avro_data_path
    )

    assert result.exit_code == 0, result.output
    lines = result.output.rstrip().split('\n')
    assert len(lines) == 2, f"Expected 2 lines, got {len(lines)}:\n{lines}"
    for line in lines:
        person = json.loads(line)
        _assert_links(person)


def test_foaf_data_links_and_properties(invoke, gen3_schema_path, avro_schema_path, avro_data_path):
    """Test the gen3 to pfb schema, link `label`."""
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
        "tests/foaf/data/links-with-properties",
    )

    assert result.exit_code == 0, result.output

    result = invoke(
        "show",
        "-i",
        avro_data_path
    )

    assert result.exit_code == 0, result.output
    lines = result.output.rstrip().split('\n')
    assert len(lines) == 2, f"Expected 2 lines, got {len(lines)}:\n{lines}"
    for line in lines:
        person = json.loads(line)
        print(f"DEBUG: person={person}")
        _assert_links(person, check_link_properties=True)


def _assert_links(person, check_link_properties=False):
    """Helper function to assert links in the person object."""
    print(f"DEBUG: person={person}")
    assert person["name"] == "person", f"Expected person, got {person['name']}"
    assert person["object"]["submitter_id"], f"Expected person with submitter_id, got {person}"
    # check links
    expected_links = sorted([{'dst_name': 'person', 'label': 'knows', 'properties': {'since': '2020-01-01', 'how_well': 5}},
                             {'dst_name': 'person', 'label': 'colleagues', 'properties': {'workplace': 'acme_corp'}}],
                            key=lambda x: x['label'])
    actual_links = sorted(person['relations'], key=lambda x: x['label'])
    assert len(actual_links) == len(actual_links)
    for actual, expected in zip(actual_links, expected_links):
        assert actual['dst_name'] == expected[
            'dst_name'], f"Expected dst_name {expected['dst_name']}, got {actual['dst_name']}"
        assert actual['label'] == expected['label'], f"Expected label {expected['label']}, got {actual['label']}"
        if check_link_properties:
            assert 'properties' in actual, f"Expected properties in link, got {actual}"
            assert isinstance(actual['properties'], dict), f"Expected properties to be a dict, got {type(actual['properties'])}"
            assert actual['properties'] == expected['properties'], f"Expected properties {expected['properties']}, got {actual['properties']}"
