"""Tests for LinkML importer."""

from fastavro import reader

from pfb.base import str_hook
from pfb.importers.linkml_utils import (
    to_snake_case,
    pluralize,
    infer_link_label,
    guess_category,
    is_downloadable,
    linkml2gen3_dict,
)


# Utility function tests


def test_to_snake_case():
    """Test CamelCase to snake_case conversion."""
    assert to_snake_case("ResearchStudy") == "research_study"
    assert to_snake_case("Person") == "person"
    assert to_snake_case("MeasurementObservation") == "measurement_observation"
    assert to_snake_case("DrugExposure") == "drug_exposure"


def test_pluralize():
    """Test word pluralization."""
    assert pluralize("person") == "people"
    assert pluralize("specimen") == "specimens"
    assert pluralize("study") == "studies"
    assert pluralize("observation") == "observations"


def test_infer_link_label():
    """Test link label inference from slot names."""
    assert infer_link_label("associated_person") == "associated_with"
    assert infer_link_label("member_of_research_study") == "member_of"
    assert infer_link_label("part_of") == "part_of"
    assert infer_link_label("derived_from") == "derived_from"
    assert infer_link_label("source_specimen") == "derived_from"
    assert infer_link_label("unknown_slot") == "related_to"


def test_guess_category():
    """Test category guessing based on class name."""
    assert guess_category("ResearchStudy") == "administrative"
    assert guess_category("Organization") == "administrative"
    assert guess_category("Specimen") == "biospecimen"
    assert guess_category("Sample") == "biospecimen"
    assert guess_category("ImagingFile") == "data_file"
    assert guess_category("Document") == "data_file"
    assert guess_category("Demography") == "clinical"


def test_is_downloadable():
    """Test downloadable field determination."""
    assert is_downloadable("ImagingFile") is True
    assert is_downloadable("Document") is True
    assert is_downloadable("Specimen") is False
    assert is_downloadable("Person") is False


# Schema conversion tests


def test_linkml2gen3_dict_conversion(linkml_schema):
    """Test conversion of LinkML schema to Gen3 dict format."""
    gen3_dict = linkml2gen3_dict(linkml_schema)

    # Check that Program node is created
    assert "program" in gen3_dict
    program_node = gen3_dict["program"]

    # Check basic structure
    assert program_node["id"] == "program"
    assert program_node["title"] == "Program"
    assert program_node["type"] == "object"
    assert "properties" in program_node
    assert "links" in program_node

    # Check that Project node is created
    assert "project" in gen3_dict
    project_node = gen3_dict["project"]

    # Check that Project has a link to Program
    assert any(link["target_type"] == "program" for link in project_node["links"])

    # Check that Participant node is created
    assert "participant" in gen3_dict
    participant_node = gen3_dict["participant"]

    # Check that Participant has a link to Project
    assert any(link["target_type"] == "project" for link in participant_node["links"])


# CLI integration tests


def test_from_linkml_command(runner, invoke, linkml_schema, linkml_data):
    """Test LinkML import via CLI command."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            linkml_schema,
            "--program",
            "TEST",
            "--project",
            "demo",
            linkml_data,
        )
        assert result.exit_code == 0, result.output

        # Verify output file was created
        with open("output.avro", "rb") as f:
            r = reader(f)

            # Read all records
            records = list(r)
            assert len(records) > 0

            # Check for expected node types
            node_names = {record["name"] for record in records}
            assert "program" in node_names
            assert "project" in node_names
            assert "participant" in node_names

            # Verify record structure
            for record in records:
                assert "id" in record
                assert "name" in record
                assert "object" in record
                assert "relations" in record
                assert isinstance(record["relations"], list)


def test_from_linkml_creates_valid_schema(runner, invoke, linkml_schema, linkml_data):
    """Test that LinkML import creates a valid AVRO schema."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            linkml_schema,
            "--program",
            "TEST",
            "--project",
            "demo",
            linkml_data,
        )
        assert result.exit_code == 0, result.output

        with open("output.avro", "rb") as f:
            r = reader(f)

            # Verify schema structure
            schema = r.writer_schema
            assert "fields" in schema
            assert len(schema["fields"]) >= 3

            # The third field should be the union of all node types
            nodes_field = schema["fields"][2]
            assert nodes_field["name"] == "object"

            # Verify all expected node types are in schema
            node_schemas = nodes_field["type"]
            node_names = {
                node["name"] for node in node_schemas if isinstance(node, dict)
            }
            assert "program" in node_names
            assert "project" in node_names
            assert "participant" in node_names
