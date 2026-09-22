"""Tests for the LinkML importer (``pfb from linkml``).

Two schemas are exercised:

* ``tests/linkml/test_schema.yaml`` -- a small hand-written schema used for the
  unit-level conversion tests and the LinkML-to-Gen3 mapping rules.
* ``tests/schema/kf_schema.yaml`` -- a LinkML representation of the Kids First
  Gen3 data dictionary in ``tests/schema/kf.json``.  These tests parallel
  test_commands.py::test_from_json / test_from_tsv but drive the
  ``pfb from linkml`` code path end-to-end.

Cross-path parity between ``pfb from dict`` and ``pfb from linkml`` (identical
AVRO schema, enum encoding, link topology and multiplicity) is asserted in
test_commands.py; this module covers the LinkML side in isolation.
"""

import csv
import json
import logging
import os
import pathlib
import re

import pytest
from fastavro import reader

from pfb.base import decode_enum
from pfb.importers.linkml_utils import (
    to_snake_case,
    pluralize,
    infer_link_label,
    guess_category,
    is_downloadable,
    iter_link_members,
    linkml2gen3_dict,
    resolve_type_range,
    slot_max_cardinality,
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


# ---------------------------------------------------------------------------
# LinkML-to-Gen3 mapping rules
# ---------------------------------------------------------------------------


CUSTOM_TYPE_SCHEMA = """
id: https://example.org/mapping
name: mapping
default_prefix: m
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  m: https://example.org/mapping/
imports:
  - linkml:types
types:
  StudyURL:
    typeof: uriorcurie
    pattern: "^https://"
  Weight:
    typeof: decimal
  PreciseWeight:
    typeof: Weight
enums:
  ColourEnum:
    permissible_values:
      red: {}
      green: {}
classes:
  Thing:
    attributes:
      submitter_id:
        identifier: true
      name:
        range: string
      count:
        range: integer
      ratio:
        range: float
      mass:
        range: decimal
      flag:
        range: boolean
      born:
        range: date
      seen_at:
        range: datetime
      homepage:
        range: uri
      ref:
        range: uriorcurie
      study_url:
        range: StudyURL
      weight:
        range: Weight
      precise_weight:
        range: PreciseWeight
      colours:
        range: ColourEnum
        multivalued: true
"""


@pytest.fixture()
def custom_type_schema(tmp_path):
    """Write CUSTOM_TYPE_SCHEMA to a temp file and return its path."""
    path = tmp_path / "mapping.yaml"
    path.write_text(CUSTOM_TYPE_SCHEMA)
    return str(path)


def test_primitive_type_mapping(custom_type_schema):
    """LinkML primitives map to the Gen3 types given in the design document."""
    props = linkml2gen3_dict(custom_type_schema)["thing"]["properties"]

    assert props["name"] == {"type": "string"}
    assert props["count"] == {"type": "integer"}
    assert props["ratio"] == {"type": "number"}
    assert props["mass"] == {"type": "number"}
    assert props["flag"] == {"type": "boolean"}
    assert props["born"] == {"type": "string", "format": "date"}
    assert props["seen_at"] == {"type": "string", "format": "date-time"}
    assert props["homepage"] == {"type": "string", "format": "uri"}
    assert props["ref"] == {"type": "string", "format": "uri"}


def test_custom_types_resolve_through_typeof(custom_type_schema):
    """Custom types resolve to their base LinkML type before mapping.

    ``StudyURL: {typeof: uriorcurie}`` must map exactly like ``uriorcurie``, and
    a multi-level chain (``PreciseWeight -> Weight -> decimal``) must resolve all
    the way down.
    """
    props = linkml2gen3_dict(custom_type_schema)["thing"]["properties"]

    assert props["study_url"] == {"type": "string", "format": "uri"}
    assert props["weight"] == {"type": "number"}
    assert props["precise_weight"] == {"type": "number"}


def test_resolve_type_range_returns_none_for_non_types(custom_type_schema):
    """resolve_type_range only handles types, so classes/enums fall through."""
    from linkml_runtime import SchemaView

    sv = SchemaView(custom_type_schema)
    assert resolve_type_range("string", sv) == ("string", None)
    assert resolve_type_range("StudyURL", sv) == ("string", "uri")
    assert resolve_type_range("ColourEnum", sv) is None
    assert resolve_type_range("Thing", sv) is None


def test_multivalued_enum_becomes_array_of_enum(custom_type_schema):
    """A multivalued enum slot becomes a Gen3 array whose items carry the enum."""
    props = linkml2gen3_dict(custom_type_schema)["thing"]["properties"]

    colours = props["colours"]
    assert colours["type"] == "array"
    assert colours["items"]["enum"] == ["red", "green"]


POLYMORPHIC_SCHEMA = """
id: https://example.org/poly
name: poly
default_prefix: p
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  p: https://example.org/poly/
imports:
  - linkml:types
classes:
  Value:
    abstract: true
    attributes:
      submitter_id:
        identifier: true
  StringValue:
    is_a: Value
  IntegerValue:
    is_a: Value
  SoleValue:
    abstract: true
    attributes:
      submitter_id:
        identifier: true
  OnlyChild:
    is_a: SoleValue
  Item:
    attributes:
      submitter_id:
        identifier: true
      response_value:
        range: Value
        required: true
      lonely:
        range: SoleValue
"""


@pytest.fixture()
def polymorphic_schema(tmp_path):
    """Write POLYMORPHIC_SCHEMA to a temp file and return its path."""
    path = tmp_path / "poly.yaml"
    path.write_text(POLYMORPHIC_SCHEMA)
    return str(path)


def test_abstract_range_expands_to_concrete_targets(polymorphic_schema):
    """An abstract range becomes links to its concrete descendants.

    Abstract classes are not emitted as nodes, and a slot ranging on one must
    not be dropped: it expands into a Gen3 link subgroup naming each concrete
    descendant.  Member names are qualified by the slot name so two polymorphic
    slots on one class cannot collide.
    """
    gen3 = linkml2gen3_dict(polymorphic_schema)

    # Abstract classes are not nodes.
    assert "value" not in gen3
    assert "sole_value" not in gen3
    assert {"string_value", "integer_value", "only_child", "item"} <= set(gen3)

    # A subgroup records the originating LinkML slot name, so the importer can
    # match data that refers to the relationship by slot name.
    groups = [
        link for link in gen3["item"]["links"] if link.get("name") == "response_value"
    ]
    assert (
        len(groups) == 1
    ), f"expected one response_value group: {gen3['item']['links']}"
    group = groups[0]
    assert "subgroup" in group
    assert group["required"] is True
    # Single-valued polymorphic slot -> the choice is exclusive.
    assert group["exclusive"] is True

    targets = {m["target_type"] for m in group["subgroup"]}
    assert targets == {"string_value", "integer_value"}

    names = {m["name"] for m in group["subgroup"]}
    assert names == {"response_value_string_value", "response_value_integer_value"}

    # Every expanded link needs a matching node property for Gen3 validation.
    for name in names:
        assert name in gen3["item"]["properties"]

    # Subgroup members stay out of `required`; the group carries it.
    assert not names & set(gen3["item"]["required"])


def test_abstract_range_with_one_descendant_is_a_plain_link(polymorphic_schema):
    """A sole concrete descendant needs no subgroup - emit a normal link."""
    gen3 = linkml2gen3_dict(polymorphic_schema)

    lonely = [link for link in gen3["item"]["links"] if link.get("name") == "lonely"]
    assert len(lonely) == 1, f"expected one 'lonely' link, got {gen3['item']['links']}"
    assert "subgroup" not in lonely[0]
    assert lonely[0]["target_type"] == "only_child"


def test_iter_link_members_flattens_subgroups(polymorphic_schema):
    """iter_link_members walks into subgroups and yields concrete links."""
    gen3 = linkml2gen3_dict(polymorphic_schema)

    members = [m for link in gen3["item"]["links"] for m in iter_link_members(link)]
    assert all("subgroup" not in m for m in members)
    assert {"only_child", "string_value", "integer_value"} <= {
        m["target_type"] for m in members
    }


# ---------------------------------------------------------------------------
# Multiplicity derivation
# ---------------------------------------------------------------------------


MULTIPLICITY_SCHEMA = """
id: https://example.org/mult
name: mult
default_prefix: q
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  q: https://example.org/mult/
imports:
  - linkml:types
classes:
  Target:
    attributes:
      submitter_id:
        identifier: true
  Unique:
    attributes:
      submitter_id:
        identifier: true
      target:
        range: Target
    unique_keys:
      target_key:
        unique_key_slots:
          - target
  Shared:
    attributes:
      submitter_id:
        identifier: true
      target:
        range: Target
  UniqueMulti:
    attributes:
      submitter_id:
        identifier: true
      targets:
        range: Target
        multivalued: true
    unique_keys:
      targets_key:
        unique_key_slots:
          - targets
  SharedMulti:
    attributes:
      submitter_id:
        identifier: true
      targets:
        range: Target
        multivalued: true
  Bounded:
    attributes:
      submitter_id:
        identifier: true
      targets:
        range: Target
        multivalued: true
        maximum_cardinality: 1
"""


@pytest.fixture()
def multiplicity_schema(tmp_path):
    """Write MULTIPLICITY_SCHEMA to a temp file and return its path."""
    path = tmp_path / "mult.yaml"
    path.write_text(MULTIPLICITY_SCHEMA)
    return str(path)


def _multiplicity(gen3, node, link_name):
    for link in gen3[node]["links"]:
        for member in iter_link_members(link):
            if member["name"] == link_name:
                return member["multiplicity"]
    raise AssertionError(f"no link {link_name!r} on {node!r}: {gen3[node]['links']}")


def test_multiplicity_from_source_and_inverse_cardinality(multiplicity_schema):
    """Gen3 multiplicity is "<sources per target>_to_<targets per source>".

    A single-slot ``unique_keys`` entry over the relationship slot bounds the
    inverse side at one; without it the inverse defaults to many.
    """
    gen3 = linkml2gen3_dict(multiplicity_schema)

    # single-valued source; unique -> inverse one; not unique -> inverse many
    assert _multiplicity(gen3, "unique", "target") == "one_to_one"
    assert _multiplicity(gen3, "shared", "target") == "many_to_one"

    # multivalued source
    assert _multiplicity(gen3, "unique_multi", "targets") == "one_to_many"
    assert _multiplicity(gen3, "shared_multi", "targets") == "many_to_many"

    # maximum_cardinality: 1 narrows a multivalued source back to "one"
    assert _multiplicity(gen3, "bounded", "targets") == "many_to_one"


def test_slot_max_cardinality_honours_cardinality_bounds(multiplicity_schema):
    """slot_max_cardinality prefers explicit cardinality over `multivalued`."""
    from linkml_runtime import SchemaView

    sv = SchemaView(multiplicity_schema)
    assert slot_max_cardinality(sv.induced_slot("target", "Shared")) == "one"
    assert slot_max_cardinality(sv.induced_slot("targets", "SharedMulti")) == "many"
    assert slot_max_cardinality(sv.induced_slot("targets", "Bounded")) == "one"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def kf_schema(path_join):
    """Path to the generated KF LinkML schema file."""
    return path_join("schema", "kf_schema.yaml")


@pytest.fixture()
def kf_data(path_join):
    """Path to the JSON data directory."""
    return path_join("data")


@pytest.fixture()
def kf_tsv_data(path_join):
    """Path to the TSV data directory."""
    return path_join("tsv_data")


# ---------------------------------------------------------------------------
# Schema compilation tests
# ---------------------------------------------------------------------------


def test_kf_schema_compiles(kf_schema):
    """The KF LinkML schema must convert to a valid Gen3 dict without errors."""
    gen3 = linkml2gen3_dict(kf_schema)

    # All 38 clinical/data nodes should be present (no _definitions/_terms)
    assert len(gen3) >= 36, f"Expected at least 36 nodes, got {len(gen3)}"
    assert "_definitions" not in gen3
    assert "_terms" not in gen3

    # Core nodes
    for node in (
        "participant",
        "demographic",
        "sample",
        "aliquot",
        "read_group",
        "submitted_aligned_reads",
    ):
        assert node in gen3, f"Expected node '{node}' in gen3 dict"


def test_kf_schema_categories(kf_schema):
    """Critical nodes must have correct Gen3 categories from annotations."""
    gen3 = linkml2gen3_dict(kf_schema)

    assert gen3["submitted_aligned_reads"]["category"] == "data_file"
    assert gen3["aligned_reads"]["category"] == "data_file"
    assert gen3["experiment_metadata"]["category"] == "metadata_file"
    assert gen3["germline_mutation_calling_workflow"]["category"] == "analysis"
    assert gen3["alignment_workflow"]["category"] == "analysis"
    assert gen3["sample"]["category"] == "biospecimen"
    assert gen3["aliquot"]["category"] == "biospecimen"
    assert gen3["demographic"]["category"] == "clinical"


def test_kf_schema_submittable_annotation(kf_schema):
    """Nodes marked submittable: false in kf.json must keep that flag."""
    gen3 = linkml2gen3_dict(kf_schema)

    # germline_mutation_calling_workflow is NOT submittable in the source dict
    assert gen3["germline_mutation_calling_workflow"]["submittable"] is False


def test_kf_schema_links(kf_schema):
    """Key node links must be correctly resolved."""
    gen3 = linkml2gen3_dict(kf_schema)

    # demographic → participant
    demo_links = gen3["demographic"]["links"]
    assert any(
        lk["name"] == "participants" and lk["target_type"] == "participant"
        for lk in demo_links
    ), f"Expected participants→participant link, got {demo_links}"

    # submitted_aligned_reads → read_group
    sar_links = gen3["submitted_aligned_reads"]["links"]
    assert any(
        lk["name"] == "read_groups" and lk["target_type"] == "read_group"
        for lk in sar_links
    ), f"Expected read_groups→read_group link, got {sar_links}"

    # germline_mutation_calling_workflow has subgroup links flattened
    gmcw_links = gen3["germline_mutation_calling_workflow"]["links"]
    link_names = {lk["name"] for lk in gmcw_links}
    assert (
        "submitted_aligned_reads_files" in link_names
        or "aligned_reads_files" in link_names
    ), f"Expected workflow links, got {link_names}"


def test_kf_schema_file_node_properties(kf_schema):
    """Data-file nodes must contain the standard file properties."""
    gen3 = linkml2gen3_dict(kf_schema)
    sar_props = gen3["submitted_aligned_reads"]["properties"]

    for required_prop in (
        "data_type",
        "data_format",
        "data_category",
        "file_name",
        "file_size",
        "md5sum",
    ):
        assert (
            required_prop in sar_props
        ), f"Expected '{required_prop}' in submitted_aligned_reads properties"


# ---------------------------------------------------------------------------
# CLI integration tests – JSON data
# ---------------------------------------------------------------------------


def test_from_linkml_kf_json(runner, invoke, kf_schema, kf_data):
    """``pfb from linkml`` with JSON data produces 37 records matching test_from_json."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            kf_data,
        )
        assert result.exit_code == 0, result.output

        with open("output.avro", "rb") as f:
            r = reader(f)
            data = list(r)

        assert len(data) == 37, f"Expected 37 records, got {len(data)}"

        # Verify all expected node types are present (metadata + 36 data nodes)
        names = {rec["name"] for rec in data}
        for node in (
            "demographic",
            "participant",
            "sample",
            "aliquot",
            "read_group",
            "submitted_aligned_reads",
            "diagnosis",
        ):
            assert node in names, f"Expected node '{node}' in output"

        # Verify the submitted_aligned_reads record has correct field values
        for rec in data:
            if rec["name"] == "submitted_aligned_reads":
                obj = rec["object"]
                if "soixantine_counterimpulse" in (obj.get("submitter_id") or ""):
                    assert decode_enum(obj["data_type"]) == "Aligned Reads"
                    assert decode_enum(obj["data_category"]) == "Sequencing Reads"
                    assert decode_enum(obj["data_format"]) == "BAM"
                    assert obj["file_name"] == "virtuosi_conticent"
                    assert obj["file_size"] == 54
                    # Relation to read_group must be present
                    assert any(
                        rel["dst_name"] == "read_group" for rel in rec["relations"]
                    ), f"Expected relation to read_group, got {rec['relations']}"
                    break

        # Verify demographic record
        for rec in data:
            if rec["name"] == "demographic":
                obj = rec["object"]
                if "duteousness_unassailing" in (obj.get("submitter_id") or ""):
                    assert (
                        decode_enum(obj["race"])
                        == "Native Hawaiian or Other Pacific Islander"
                    )
                    assert decode_enum(obj["gender"]) == "female"
                    assert decode_enum(obj["vital_status"]) == "Dead"
                    break


def test_from_linkml_kf_json_schema_structure(runner, invoke, kf_schema, kf_data):
    """The AVRO schema produced by the LinkML path must include all KF node types."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            kf_data,
        )
        assert result.exit_code == 0, result.output

        with open("output.avro", "rb") as f:
            r = reader(f)
            schema = r.writer_schema

        # Top-level schema structure
        assert "fields" in schema
        object_field = schema["fields"][2]
        assert object_field["name"] == "object"

        # The union must contain all expected node schemas
        node_schemas = object_field["type"]
        node_names = {
            n["name"] for n in node_schemas if isinstance(n, dict) and "name" in n
        }
        for node in ("participant", "demographic", "sample", "submitted_aligned_reads"):
            assert (
                node in node_names
            ), f"Expected '{node}' in AVRO schema union, got {sorted(node_names)}"


# ---------------------------------------------------------------------------
# CLI integration tests – TSV data
# ---------------------------------------------------------------------------


def test_from_linkml_kf_tsv(runner, invoke, kf_schema, kf_tsv_data):
    """``pfb from linkml`` with TSV data produces 37 records matching test_from_tsv."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            kf_tsv_data,
        )
        assert result.exit_code == 0, result.output

        with open("output.avro", "rb") as f:
            r = reader(f)
            data = list(r)

        assert len(data) == 37, f"Expected 37 records, got {len(data)}"

        # Verify node coverage
        names = {rec["name"] for rec in data}
        for node in ("demographic", "participant", "sample", "submitted_aligned_reads"):
            assert node in names, f"Expected node '{node}' in TSV output"

        # Verify submitted_aligned_reads key fields from TSV
        for rec in data:
            if rec["name"] == "submitted_aligned_reads":
                obj = rec["object"]
                if "soixantine_counterimpulse" in (obj.get("submitter_id") or ""):
                    assert decode_enum(obj["data_type"]) == "Aligned Reads"
                    assert decode_enum(obj["data_category"]) == "Sequencing Reads"
                    assert obj["file_name"] == "virtuosi_conticent"
                    break


# ---------------------------------------------------------------------------
# Identifier slots and inlining
# ---------------------------------------------------------------------------


def test_identifier_slot_becomes_a_unique_key(custom_type_schema):
    """A LinkML identifier slot becomes a unique key on the Gen3 node."""
    node = linkml2gen3_dict(custom_type_schema)["thing"]

    assert ["submitter_id"] in node["uniqueKeys"]
    # Gen3's own system keys are still present.
    assert ["id"] in node["uniqueKeys"]
    assert ["project_id", "submitter_id"] in node["uniqueKeys"]


def test_entity_id_comes_from_the_identifier_slot(
    runner, invoke, linkml_schema, linkml_data
):
    """PFB entity IDs are taken from the schema's identifier slot.

    tests/linkml/test_schema.yaml declares ``id`` as the identifier, and the
    data carries both ``id`` and ``submitter_id``, so this pins down which one
    is used.
    """
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
            ids = {
                record["name"]: record["id"]
                for record in reader(f)
                if record["name"] != "Metadata"
            }

    assert ids == {"program": "prog1", "project": "proj1", "participant": "part1"}


INLINED_SCHEMA = """
id: https://example.org/inline
name: inline
default_prefix: i
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  i: https://example.org/inline/
imports:
  - linkml:types
classes:
  Identified:
    attributes:
      submitter_id:
        identifier: true
      label:
        range: string
  Anonymous:
    attributes:
      label:
        range: string
  Holder:
    attributes:
      submitter_id:
        identifier: true
      named:
        range: Identified
        inlined: true
      blob:
        range: Anonymous
        inlined: true
"""


def test_inlined_identified_target_becomes_a_link(tmp_path):
    """An inlined but identified target normalizes to its own node plus a link."""
    path = tmp_path / "inline.yaml"
    path.write_text(INLINED_SCHEMA)
    gen3 = linkml2gen3_dict(str(path))

    assert "identified" in gen3
    names = {
        member["name"]
        for link in gen3["holder"]["links"]
        for member in iter_link_members(link)
    }
    assert "named" in names


def test_unmappable_constructs_are_reported(tmp_path, caplog):
    """Constructs the conversion cannot represent faithfully are logged.

    ``Anonymous`` has no identifier, so ``Holder.blob`` inlines a structure that
    cannot be given a stable PFB entity ID.  Silently dropping it would leave no
    trace that the PFB is an incomplete view of the source model.
    """
    path = tmp_path / "inline.yaml"
    path.write_text(INLINED_SCHEMA)

    with caplog.at_level(logging.WARNING, logger="pfb.importers.linkml_utils"):
        linkml2gen3_dict(str(path))

    assert "were not mapped faithfully to Gen3" in caplog.text
    assert "blob" in caplog.text and "Anonymous" in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# PFB -> TSV round-trip
# ---------------------------------------------------------------------------


def test_kf_linkml_pfb_exports_to_tsv(runner, invoke, kf_schema, kf_data):
    """``pfb to tsv`` returns the data that was serialized into a LinkML PFB.

    Covers the design document's test-plan requirement that the ``to`` command
    round-trips LinkML-sourced data.  Enum values must come back in their
    original form, not AVRO's escaped enum-symbol encoding.
    """
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            kf_data,
        )
        assert result.exit_code == 0, result.output

        result = invoke("to", "-i", "output.avro", "tsv", "tsvout")
        assert result.exit_code == 0, result.output

        with open(os.path.join("tsvout", "demographic.tsv")) as f:
            rows = list(csv.DictReader(f, delimiter="\t"))

    assert rows, "expected at least one demographic row"
    row = rows[0]

    # Enum-valued fields are decoded back to their original values.
    assert row["race"] == "Native Hawaiian or Other Pacific Islander"
    assert row["gender"] == "female"
    assert row["vital_status"] == "Dead"
    assert row["age_at_last_follow_up_days"] == "18074"

    # The link to participant survives the round-trip.  Which of the
    # `participants.*` columns carries it depends on the exporter's key choice,
    # so assert the relation is present rather than pinning the column.
    participant_columns = {
        key: value for key, value in row.items() if key.startswith("participants.")
    }
    assert participant_columns, f"no participants.* column in {sorted(row)}"
    assert (
        "participant_metalinguistics_monofilm" in participant_columns.values()
    ), f"participant link lost in round-trip: {participant_columns}"

    # And the entity id is the identifier slot value.
    assert row["submitter_id"] == "demographic_duteousness_unassailing"


# ---------------------------------------------------------------------------
# Synthetic data generator (tools/generate_linkml_data.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def datagen():
    """Import tools/generate_linkml_data.py, which is a script, not a package."""
    import importlib.util

    script = (
        pathlib.Path(__file__).resolve().parent.parent
        / "tools"
        / "generate_linkml_data.py"
    )
    spec = importlib.util.spec_from_file_location("generate_linkml_data", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_datagen_is_deterministic(datagen, linkml_schema, tmp_path):
    """The same seed must produce byte-identical files; a different seed must not."""
    runs = {}
    for label, seed in (("a", 0), ("b", 0), ("c", 7)):
        out = tmp_path / label
        datagen.main(
            ["-s", linkml_schema, "-o", str(out), "--count", "3", "--seed", str(seed)]
        )
        runs[label] = {
            path.name: path.read_bytes() for path in sorted(out.glob("*.json"))
        }

    assert runs["a"], "generator produced no files"
    assert runs["a"] == runs["b"], "same seed produced different output"
    assert runs["a"] != runs["c"], "different seeds produced identical output"


def test_datagen_output_loads_into_a_pfb(datagen, runner, invoke, kf_schema, tmp_path):
    """Generated data must be accepted by ``pfb from linkml`` and carry relations.

    This is the real contract: the generator builds records against the compiled
    Gen3 schema, so anything it emits has to satisfy the AVRO schema derived from
    that same conversion.
    """
    out = tmp_path / "data"
    datagen.main(["-s", kf_schema, "-o", str(out), "--count", "2"])

    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            str(out),
        )
        assert result.exit_code == 0, result.output

        with open("output.avro", "rb") as f:
            records = [r for r in reader(f) if r["name"] != "Metadata"]

    assert (
        len(records) == 76
    ), f"expected 2 per node across 38 nodes, got {len(records)}"

    # Entity IDs follow the generator's <node>_<index> convention.
    demographics = [r for r in records if r["name"] == "demographic"]
    assert {r["id"] for r in demographics} == {
        "demographic_0000",
        "demographic_0001",
    }

    # Enum values round-trip through the AVRO enum encoding.
    node = linkml2gen3_dict(kf_schema)["demographic"]
    assert (
        decode_enum(demographics[0]["object"]["race"])
        in node["properties"]["race"]["enum"]
    )

    # Links become relations.
    assert any(
        rel["dst_name"] == "participant" for rel in demographics[0]["relations"]
    ), f"expected a participant relation, got {demographics[0]['relations']}"


def test_datagen_emits_linkml_slot_names_for_polymorphic_slots(datagen, tmp_path):
    """A polymorphic slot is written under its LinkML slot name.

    The generator reads the raw schema, so a slot ranging on an abstract class is
    emitted as ``response_value`` -- the name a LinkML-validated data file would
    use -- referencing exactly one concrete descendant.  It deliberately does not
    use the ``response_value_<target>`` names that pypfb's compiler invents when
    it expands the abstract range into Gen3 links.
    """
    schema = tmp_path / "poly.yaml"
    schema.write_text(POLYMORPHIC_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--count", "2"])

    rows = json.loads((out / "item.json").read_text())
    assert rows, "no records generated for Item"
    for row in rows:
        assert "response_value" in row, sorted(row)
        assert not [key for key in row if key.startswith("response_value_")]

        # A non-inlined reference is the ID of one concrete descendant, as
        # LinkML serialises it -- not an inline object.
        reference = row["response_value"]
        assert isinstance(reference, str), reference
        assert reference.startswith(("string_value_", "integer_value_")), reference


REFERENCE_FORMS_SCHEMA = """
id: https://example.org/refs
name: refs
default_prefix: r
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  r: https://example.org/refs/
imports:
  - linkml:types
classes:
  Tag:
    attributes:
      id:
        identifier: true
      label:
        required: true
  Holder:
    attributes:
      id:
        identifier: true
      one_ref:
        range: Tag
      many_refs:
        range: Tag
        multivalued: true
      inlined_list:
        range: Tag
        multivalued: true
        inlined_as_list: true
      inlined_dict:
        range: Tag
        multivalued: true
        inlined: true
"""


def test_datagen_serialises_references_in_linkml_form(datagen, tmp_path):
    """Each class-valued slot takes the shape LinkML itself expects.

    Non-inlined references are identifier strings -- a list when multivalued --
    and inlined ones embed the target, as a list or as a dict keyed by
    identifier.  The generated records must pass ``linkml validate``.
    """
    from linkml.validator import validate

    schema = tmp_path / "refs.yaml"
    schema.write_text(REFERENCE_FORMS_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--count", "3"])

    tags = {row["id"] for row in json.loads((out / "tag.json").read_text())}
    rows = json.loads((out / "holder.json").read_text())
    for row in rows:
        assert row["one_ref"] in tags, row
        assert row["many_refs"] and set(row["many_refs"]) <= tags, row
        # Inlined targets carry their required slots, not just an identifier.
        assert row["inlined_list"] and all(
            obj["id"] in tags and "label" in obj for obj in row["inlined_list"]
        ), row
        assert row["inlined_dict"] and set(row["inlined_dict"]) <= tags, row
        assert all("label" in obj for obj in row["inlined_dict"].values()), row

        report = validate(row, str(schema), "Holder")
        assert not report.results, [r.message for r in report.results]


REQUIRED_CYCLE_SCHEMA = """
id: https://example.org/cycle
name: cycle
default_prefix: c
default_range: string
prefixes:
  linkml: https://w3id.org/linkml/
  c: https://example.org/cycle/
imports:
  - linkml:types
classes:
  Thing:
    abstract: true
    attributes:
      id:
        identifier: true
  Obs:
    is_a: Thing
    attributes:
      focus:
        range: Thing
  ObsSet:
    is_a: Thing
    attributes:
      observations:
        range: Obs
        multivalued: true
        required: true
"""


def test_datagen_fills_required_references_through_cycles(datagen, tmp_path):
    """A required reference is filled even when a cycle reaches its class first.

    ``Obs.focus`` ranges on the abstract ``Thing``, so it can point at an
    ``ObsSet``, which in turn requires ``observations``.  Ordering classes by
    every reference built ``ObsSet`` before any ``Obs`` existed and silently
    dropped the required slot -- BDCHM's ``MeasurementObservationSet`` hit this.
    """
    from linkml.validator import validate

    schema = tmp_path / "cycle.yaml"
    schema.write_text(REQUIRED_CYCLE_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--count", "2"])

    rows = json.loads((out / "obs_set.json").read_text())
    assert rows
    for row in rows:
        assert row.get("observations"), row
        report = validate(row, str(schema), "ObsSet")
        assert not report.results, [r.message for r in report.results]


def test_multivalued_references_become_one_relation_each(
    datagen, runner, invoke, tmp_path
):
    """Every reference in a multivalued slot becomes its own relation.

    Covers each serialised form: an identifier string, a list of them, a list
    of inlined objects, and a dict of inlined objects keyed by identifier.
    """
    schema = tmp_path / "refs.yaml"
    schema.write_text(REFERENCE_FORMS_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--count", "3"])

    rows = {row["id"]: row for row in json.loads((out / "holder.json").read_text())}
    records = _import_poly(runner, invoke, str(schema), str(out))

    for record in (r for r in records if r["name"] == "holder"):
        row = rows[record["id"]]
        expected = (
            {row["one_ref"]}
            | set(row["many_refs"])
            | {obj["id"] for obj in row["inlined_list"]}
            | set(row["inlined_dict"])
        )
        assert {rel["dst_id"] for rel in record["relations"]} == expected
        assert {rel["dst_name"] for rel in record["relations"]} == {"tag"}
        # Link slots are relations, not properties.
        for slot in ("one_ref", "many_refs", "inlined_list", "inlined_dict"):
            assert slot not in record["object"], record["object"]


def test_datagen_generates_only_concrete_classes(datagen, tmp_path):
    """Abstract and mixin classes get no data file; concrete descendants do."""
    schema = tmp_path / "poly.yaml"
    schema.write_text(POLYMORPHIC_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--count", "1"])

    written = {path.stem for path in out.glob("*.json")}
    assert written == {"item", "string_value", "integer_value", "only_child"}


# ---------------------------------------------------------------------------
# Polymorphic reference resolution
# ---------------------------------------------------------------------------


def _write_poly_dataset(tmp_path, item_rows):
    """Write POLYMORPHIC_SCHEMA plus a data directory, returning both paths."""
    schema = tmp_path / "poly.yaml"
    schema.write_text(POLYMORPHIC_SCHEMA)

    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    (data / "string_value.json").write_text(
        json.dumps([{"submitter_id": "sv1"}, {"submitter_id": "shared"}])
    )
    (data / "integer_value.json").write_text(
        json.dumps([{"submitter_id": "iv1"}, {"submitter_id": "shared"}])
    )
    (data / "only_child.json").write_text(json.dumps([{"submitter_id": "oc1"}]))
    (data / "item.json").write_text(json.dumps(item_rows))
    return str(schema), str(data)


def _import_poly(runner, invoke, schema, data):
    """Run ``pfb from linkml`` over a polymorphic dataset, returning its records."""
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "output.avro",
            "linkml",
            "-s",
            schema,
            "--program",
            "DEV",
            "--project",
            "test",
            data,
        )
        assert result.exit_code == 0, result.output
        with open("output.avro", "rb") as f:
            return [r for r in reader(f) if r["name"] != "Metadata"]


def test_polymorphic_reference_resolves_to_the_right_target(runner, invoke, tmp_path):
    """A reference under the LinkML slot name resolves to the owning node type.

    The data says ``response_value: {"submitter_id": "iv1"}`` -- the slot name a
    LinkML-validated file would use -- and the importer has to recover that
    ``iv1`` belongs to ``integer_value``, not ``string_value``.
    """
    schema, data = _write_poly_dataset(
        tmp_path,
        [
            {"submitter_id": "item1", "response_value": {"submitter_id": "iv1"}},
            {"submitter_id": "item2", "response_value": {"submitter_id": "sv1"}},
        ],
    )
    records = _import_poly(runner, invoke, schema, data)

    items = {r["id"]: r for r in records if r["name"] == "item"}
    assert ("integer_value", "iv1") in {
        (rel["dst_name"], rel["dst_id"]) for rel in items["item1"]["relations"]
    }, items["item1"]["relations"]
    assert ("string_value", "sv1") in {
        (rel["dst_name"], rel["dst_id"]) for rel in items["item2"]["relations"]
    }, items["item2"]["relations"]


def test_ambiguous_polymorphic_reference_is_reported(runner, invoke, tmp_path, caplog):
    """An ID present in two candidate targets is reported, not guessed at.

    Entity IDs need not be unique across node types in a PFB, so this is a real
    possibility rather than a malformed-input case.
    """
    schema, data = _write_poly_dataset(
        tmp_path,
        [{"submitter_id": "item1", "response_value": {"submitter_id": "shared"}}],
    )
    with caplog.at_level(logging.WARNING, logger="pfb.importers.linkml"):
        records = _import_poly(runner, invoke, schema, data)

    assert "ambiguous" in caplog.text, caplog.text
    item = next(r for r in records if r["name"] == "item")
    assert not [
        rel for rel in item["relations"] if rel["dst_id"] == "shared"
    ], "an ambiguous reference must not produce a relation"


def test_unresolvable_polymorphic_reference_is_reported(
    runner, invoke, tmp_path, caplog
):
    """A reference matching no record is reported rather than silently dropped."""
    schema, data = _write_poly_dataset(
        tmp_path,
        [{"submitter_id": "item1", "response_value": {"submitter_id": "nope"}}],
    )
    with caplog.at_level(logging.WARNING, logger="pfb.importers.linkml"):
        records = _import_poly(runner, invoke, schema, data)

    assert "matches no record" in caplog.text, caplog.text
    item = next(r for r in records if r["name"] == "item")
    assert not [rel for rel in item["relations"] if rel["dst_id"] == "nope"]


def test_generated_bdchm_data_resolves_polymorphic_links(
    datagen, runner, invoke, path_join, tmp_path
):
    """End-to-end: generated BDCHM data keeps its polymorphic relationships.

    BDCHM has 14 slots ranging on abstract classes.  Before the importer learned
    to resolve references by slot name these edges were silently lost, so this
    pins the behaviour on a real schema rather than only a synthetic one.
    """
    schema = path_join("schema", "bdchm.yaml")
    out = tmp_path / "data"
    datagen.main(["-s", schema, "-o", str(out), "--count", "2"])

    records = _import_poly(runner, invoke, schema, str(out))

    item = next(r for r in records if r["name"] == "questionnaire_response_item")
    targets = {rel["dst_name"] for rel in item["relations"]}
    assert any(
        target.startswith("questionnaire_response_value") for target in targets
    ), f"polymorphic response_value link lost: {item['relations']}"


def test_tsv_link_columns_become_relations(runner, invoke, kf_schema, kf_data):
    """``<link>.<field>`` TSV columns must become relations.

    A TSV cell is always text, so the JSON path's nested-object convention cannot
    apply; ``pfb to tsv`` writes links as ``participants.submitter_id`` columns
    instead.  These were previously dropped during column normalisation, so the
    LinkML TSV path produced no relations at all.  Round-tripping through
    ``pfb to tsv`` is the cleanest way to get such a file.
    """
    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "json.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            kf_data,
        )
        assert result.exit_code == 0, result.output

        result = invoke("to", "-i", "json.avro", "tsv", "tsvout")
        assert result.exit_code == 0, result.output

        # The exported TSV must actually carry link columns, or this proves nothing.
        with open(os.path.join("tsvout", "demographic.tsv")) as f:
            header = f.readline().rstrip("\n").split("\t")
        assert any(col.startswith("participants.") for col in header), header

        result = invoke(
            "from",
            "-o",
            "back.avro",
            "linkml",
            "-s",
            kf_schema,
            "--program",
            "DEV",
            "--project",
            "test",
            "tsvout",
        )
        assert result.exit_code == 0, result.output

        with open("back.avro", "rb") as f:
            records = [r for r in reader(f) if r["name"] != "Metadata"]

    total = sum(len(r["relations"]) for r in records)
    assert total > 0, "LinkML TSV import produced no relations at all"

    # dst_name must be the target node, not the link name.
    demographic = next(r for r in records if r["name"] == "demographic")
    assert [rel["dst_name"] for rel in demographic["relations"]] == [
        "participant"
    ], demographic["relations"]


def test_datagen_values_match_their_declared_types(datagen, tmp_path):
    """Generated values must match the shape their LinkML type implies.

    Without this, a value kind can silently degrade to a plain string -- which is
    exactly what happened when the base-type table was refactored and `datetime`
    started producing filler text.
    """
    schema = tmp_path / "mapping.yaml"
    schema.write_text(CUSTOM_TYPE_SCHEMA)
    out = tmp_path / "data"
    datagen.main(["-s", str(schema), "-o", str(out), "--seed", "3"])

    row = json.loads((out / "thing.json").read_text())[0]

    assert isinstance(row["count"], int)
    assert isinstance(row["ratio"], float)
    assert isinstance(row["mass"], float)
    assert isinstance(row["flag"], bool)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["born"]), row["born"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", row["seen_at"]), row[
        "seen_at"
    ]
    assert row["homepage"].startswith("https://"), row["homepage"]
    assert row["ref"].startswith("https://"), row["ref"]

    # Custom types resolve through `typeof` to the same shapes.
    assert row["study_url"].startswith("https://"), row["study_url"]
    assert isinstance(row["weight"], float)
    assert isinstance(row["precise_weight"], float)

    # Multivalued enum -> list drawn from the permissible values.
    assert isinstance(row["colours"], list) and row["colours"]
    assert set(row["colours"]) <= {"red", "green"}
