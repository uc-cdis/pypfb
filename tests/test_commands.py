import csv
import gzip
import io
import json
import os
import shutil

from dictionaryutils import DataDictionary
from fastavro import reader

from pfb.base import decode_enum, encode_enum, str_hook


def _test_schema(r):
    """Validate the AVRO schema's Gen3 enum encoding on experiment_metadata.state.

    Both ``pfb from dict`` and ``pfb from linkml`` run the dictionary through
    ``gen3dict._parse_dictionary``, so this applies to either source.
    """
    for node in r.writer_schema["fields"][2]["type"]:
        if node["name"] == "experiment_metadata":
            for field in node["fields"]:
                if field["name"] == "state":
                    assert decode_enum(field["default"]) == "validated"
                    for type_ in field["type"]:
                        if isinstance(type_, dict) and type_["type"] == "enum":
                            for symbol in type_["symbols"]:
                                decode_enum(symbol)


def test_from_dict(kf_schema_only_pfb):
    """Schema-only PFB contains exactly one record (the Metadata record)."""
    _source, avro_bytes = kf_schema_only_pfb
    r = reader(io.BytesIO(avro_bytes))
    _test_schema(r)
    assert len(list(r)) == 1


def test_from_json(kf_json_pfb):
    """JSON data produces 37 records with correct field values.

    The assertions are identical for both sources: a LinkML schema and the
    equivalent Gen3 dictionary produce the same PFB schema and encoding.
    """
    _source, avro_bytes = kf_json_pfb
    r = reader(io.BytesIO(avro_bytes))
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
                assert obj["file_size"] == 54


def test_from_tsv(kf_tsv_pfb):
    """TSV data produces 37 records with correct field values.

    Assertions mirror test_from_json and apply to both sources alike.
    """
    _source, avro_bytes = kf_tsv_pfb
    r = reader(io.BytesIO(avro_bytes))
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
                assert obj["file_size"] == 54


def test_to_gremlin(runner, invoke, path_join, test_avro):
    with runner.isolated_filesystem():
        result = invoke("to", "gremlin", "./output", input=test_avro)
        assert result.exit_code == 0, result.output
        with gzip.open(os.path.join("output", "demographic.csv.gz"), "rt") as f:
            result = list(csv.DictReader(f))
            assert len(result) == 1
            result = result[0]
            result.pop("~id")
            result = dict(result)
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


def test_to_tsv(runner, invoke, test_avro):
    with runner.isolated_filesystem():
        result = invoke("to", "tsv", "./tsvs", input=test_avro)
        assert result.exit_code == 0, result.output
        with open(os.path.join("tsvs", "demographic.tsv"), "rt") as f:
            result = list(csv.DictReader(f, delimiter="\t"))
            assert len(result) == 1
            result = result[0]
            result = dict(result)
            assert result == {
                "ethnicity": "not hispanic or latino",
                "age_at_last_follow_up_days": "18074",
                "cause_of_death": "Not Reported",
                "gender": "female",
                "participants.id": "participant_metalinguistics_monofilm",
                "type": "demographic",
                "updated_datetime": "",
                "vital_status": "Dead",
                "submitter_id": "demographic_duteousness_unassailing",
                "project_id": "DEV-test",
                "created_datetime": "",
                "race": "Native Hawaiian or Other Pacific Islander",
                "state": "validated",
                "participants.submitter_id": "null",
                "id": "demographic_duteousness_unassailing",
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
            "tumor_descriptor": "Metastatic",
            "biospecimen_anatomic_site": "Abdomen",
            "state": "uploading",
            "diagnosis_pathologically_confirmed": "Yes",
            "project_id": "",
            "current_weight": 0,
            "age_at_event_days": 0,
            "time_between_clamping_and_freezing": 0,
            "shortest_dimension": 0,
            "method_of_sample_procurement": "Abdomino-perineal Resection of Rectum",
            "tissue_type": "Tumor",
            "uberon_id_anatomical_site": "",
            "days_to_sample_procurement": 0,
            "spatial_descriptor": "",
            "ncit_id_tissue_type": "",
            "preservation_method": "Cryopreserved",
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
    formatted_result = result.output.splitlines()
    assert len(formatted_result) == 36
    assert (
        '"time_between_clamping_and_freezing": 83.9'
        in [x for x in formatted_result if '"id": "sample_Wagnerism_buccally"' in x][0]
    )

    result = invoke("show", "-n", "1", input=test_avro)
    assert result.exit_code == 0, result.output
    result = json.loads(result.output, object_pairs_hook=str_hook)
    # result["object"].pop("md5sum")  # mute truffles
    print(json.dumps(result))
    assert result == {
        "id": "submitted_aligned_reads_soixantine_counterimpulse",
        "name": "submitted_aligned_reads",
        "object": {
            "data_category": "Sequencing Reads",
            "data_type": "Aligned Reads",
            "experimental_strategy": "miRNA-Seq",
            "data_format": "BAM",
            "state": "validated",
            "updated_datetime": None,
            "created_datetime": None,
            "project_id": "DEV-test",
            "submitter_id": "submitted_aligned_reads_soixantine_counterimpulse",
            "md5sum": "bb1d504e1c10c2865fc67746371af516",
            "file_format": "thumb_cotranspire",
            "file_name": "virtuosi_conticent",
            "error_type": None,
            "file_url": "docility_cryophile",
            "object_id": None,
            "file_size": 54,
            "state_comment": None,
            "file_state": "registered",
        },
        "relations": [
            {"dst_id": "read_group_ethnicon_fordless", "dst_name": "read_group"}
        ],
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


# ---------------------------------------------------------------------------
# KF Gen3 path vs LinkML path comparison tests
#
# Both ``pfb from dict kf.json`` and ``pfb from linkml -s kf_schema.yaml``
# are derived from the same Kids First data model.  These tests verify that
# the LinkML round-trip (kf.json → kf_schema.yaml → in-memory Gen3 dict →
# PFB) produces structurally equivalent output for the parts that can be
# compared between the two code paths.
#
# Known acceptable differences (by design):
#   • Gen3 adds ``root`` / ``data_release`` infrastructure nodes; LinkML
#     produces only the 38 KF domain nodes.
#   • Link multiplicity does not round-trip: kf.json ``one_to_one`` /
#     ``one_to_many`` distinctions become ``many_to_one`` / ``many_to_many``
#     in the LinkML path (LinkML ``multivalued`` is a binary flag).
#   • LinkML adds a ``core_metadata_collections`` link to all file nodes;
#     kf.json does not define this link for those nodes.
#   • Gen3 adds system fields (``state``, ``project_id``, ``created_datetime``,
#     ``updated_datetime``) via ``$ref`` expansion; the LinkML path omits them.
#   • LinkML includes link fields as nullable AVRO stringIdentifying links between nodes
# When a slot’s range resolves to another LinkML class, the compiler will treat it as a relationship rather than a normal property. The name of the link will be derived from the source slot name, the target_type from the slot’s range and the required status directly from slot.required (from SchemaView)
#
# Multiplicity is determined from the SlotDefintion (from SchemaView).  However, LinkML only requires that one side of  the relationship between two classes is described whereas Gen3 describes both ends of the relationship between two nodes.
#
# Below is how we can derive the Gen3 multiplicity if both sides are described in LinkML.
#
# Sourceslot maximum
# Inverse slot maximum
# Gen3 multiplicity
# At most one
# At most one
# one_to_one
# At most one
# Many/unbounded
# many_to_one
# Many/unbounded
# At most one
# one_to_many
# Many/unbounded
# Many/unbounded
# many_to_many
#
#
# If we only have one side of the relationship defined in LinkML, then we can still derive the Gen3 multiplicity with sensible assumptions:
#
#
# Effective source slot
# Source cardinality
# Default Gen3 multiplicity
# required: false, multivalued: false
# 0..1
# one_to_one
# required: true, multivalued: false
# 1
# one_to_one
# required: false, multivalued: true
# 0..*
# one_to_many
# required: true, multivalued: true
# 1..*
# one_to_many
# multivalued: true, maximum_cardinality: 1
# 0..1 or 1
# one_to_one
# multivalued: true, maximum_cardinality: n, where n > 1
# 0..n or 1..n
# many_to_many
# multivalued: true, exact_cardinality: 1
# exactly 1
# one_to_one
# multivalued: true, exact_cardinality: n, where n > 1
# exactly n
# many_to_many fields on each record
#     (e.g. ``participants`` on ``demographic``); the Gen3 path does not.
#   • Six nodes have additional minor field-level differences documented in
#     ``_FIELD_COMPARISON_SKIP`` below.
# ---------------------------------------------------------------------------


# Gen3 infrastructure nodes absent from the LinkML PFB.
# Gen3 declares system nodes in the dictionary itself via ``category: internal``
# (``root`` and ``data_release``, which dictionaryutils contributes to every
# dictionary).  Derive them rather than hardcoding names, so the comparison
# tracks whatever the dictionary says.
def _internal_node_ids(path_join):
    """Return node IDs Gen3 marks as system/internal (``category: internal``)."""
    d = DataDictionary(local_file=path_join("schema", "kf.json"))
    return {
        node_id
        for node_id, node in d.schema.items()
        if node.get("category") == "internal"
    }


# ``consent_codes`` exists in the canonical Gen3 ``data_file_properties`` that
# pypfb supplies for the LinkML path (pfb.importers.gen3_definitions), but not in
# the Kids First dictionary's own ``data_file_properties``.  It therefore shows up
# on LinkML file nodes only -- the one known consequence of supplying a single
# canonical set of definitions, since a dictionary may extend them.
_VENDORED_DEFINITION_EXTRAS = {"consent_codes"}

_LINK_COMPARISON_SKIP = {"program"}

# Nodes excluded from strict domain-field equality.  Every entry is a fidelity
# gap in the generated tests/schema/kf_schema.yaml, not in the
# ``pfb from linkml`` code path.
#
# tools/generate_kf_schema.py reads kf.json with a plain json.load() and never
# resolves $refs, so any property whose *value* is a bare $ref is invisible to
# it and never reaches the LinkML schema:
#   aligned_reads   – ``platform`` is a cross-file $ref (read_group.yaml#/properties/platform)
#   annotation      – ``legacy_*_datetime`` are $ref: _definitions.yaml#/datetime
#   read_group      – ``sequencing_date`` is $ref: _definitions.yaml#/datetime
#   read_group_qc   – 12 QC fields are $ref: _definitions.yaml#/qc_metrics_state
#                     (an enum, which is also why they never became LinkML enums)
#
# The two Gen3 hierarchy roots differ for unrelated reasons:
#   program         – kf_schema.yaml models it as ``is_a: Entity``, which gives it
#                     a ``submitter_id`` that Gen3's program node does not have
#   project         – same ``submitter_id`` difference, plus ``state``: kf.json
#                     overrides ubiquitous ``state`` with its own enum
#                     (open/review/.../legacy) and the generator strips ``state``
#                     as a system property on every node, losing the override
_FIELD_COMPARISON_SKIP = {
    "aligned_reads",
    "annotation",
    "read_group",
    "read_group_qc",
    "program",
    "project",
}


def _build_kf_pfbs(runner, invoke, path_join):
    """Build Gen3 and LinkML PFBs for the KF schema and return their bytes.

    Run ``pfb from dict kf.json`` and ``pfb from linkml -s kf_schema.yaml``
    inside an isolated filesystem and return ``(gen3_bytes, linkml_bytes)``.
    """
    kf_json = path_join("schema", "kf.json")
    kf_schema = path_join("schema", "kf_schema.yaml")
    data_dir = path_join("data")

    with runner.isolated_filesystem():
        result = invoke(
            "from",
            "-o",
            "gen3.avro",
            "dict",
            kf_json,
        )
        assert result.exit_code == 0, f"pfb from dict failed:\n{result.output}"

        result = invoke(
            "from",
            "-o",
            "linkml.avro",
            "linkml",
            "-s",
            kf_schema,
            data_dir,
            "--program",
            "DEV",
            "--project",
            "test",
        )
        assert result.exit_code == 0, f"pfb from linkml failed:\n{result.output}"

        with open("gen3.avro", "rb") as f:
            gen3_bytes = f.read()
        with open("linkml.avro", "rb") as f:
            linkml_bytes = f.read()

    return gen3_bytes, linkml_bytes


def _kf_domain_nodes(path_join):
    """Return the set of 38 KF domain node IDs from kf.json."""
    with open(path_join("schema", "kf.json")) as f:
        kf = json.load(f)
    return {v["id"] for k, v in kf.items() if not k.startswith("_") and "id" in v}


def _metadata_nodes(avro_bytes):
    """Parse a PFB and return the metadata node dict.

    Returns ``{node_name: {"link_pairs": set_of_(name,dst,multiplicity)_tuples}}``.

    Multiplicity is included: LinkML describes only one side of a relationship
    while Gen3 describes both ends, so linkml_utils resolves the inverse side
    from an explicit ``inverse``, a reciprocal slot, or a single-slot
    ``unique_keys`` constraint (see CLAUDE_MULTIPLICITY_GUIDANCE.md).  With the
    ``unique_keys`` declarations in kf_schema.yaml this round-trips exactly.
    """
    r = reader(io.BytesIO(avro_bytes))
    meta_record = list(r)[0]
    assert meta_record["name"] == "Metadata"

    result = {}
    for node in meta_record["object"]["nodes"]:
        link_pairs = {
            (lnk["name"], lnk["dst"], str(lnk["multiplicity"])) for lnk in node["links"]
        }
        result[node["name"]] = {"link_pairs": link_pairs}
    return result


def _schema_nodes(avro_bytes):
    """Parse a PFB and return ``{node_name: set_of_field_names}`` from the AVRO schema union."""
    r = reader(io.BytesIO(avro_bytes))
    union = r.writer_schema["fields"][2]["type"]
    return {
        n["name"]: {f["name"] for f in n.get("fields", [])}
        for n in union
        if isinstance(n, dict)
    }


def _schema_enum_types(avro_bytes):
    """Return ``{(node, field): (enum_name, frozenset_of_symbols)}`` for AVRO enums.

    Only fields whose type union contains an AVRO ``enum`` are included, so this
    isolates exactly the Gen3 enum encoding.

    Symbols are compared as a set, not a sequence: the two paths must agree on
    enum *completeness*, but AVRO symbol order is not meaningful for this
    comparison (tools/generate_kf_schema.py de-duplicates shared permissible-value
    sets, which reorders them).
    """
    r = reader(io.BytesIO(avro_bytes))
    union = r.writer_schema["fields"][2]["type"]
    enums = {}
    for node in union:
        if not isinstance(node, dict):
            continue
        for field in node.get("fields", []):
            types = field["type"]
            if not isinstance(types, list):
                types = [types]
            for type_ in types:
                if isinstance(type_, dict) and type_.get("type") == "enum":
                    enums[(node["name"], field["name"])] = (
                        type_["name"],
                        frozenset(type_["symbols"]),
                    )
                    break
    return enums


def test_kf_gen3_vs_linkml_metadata(runner, invoke, path_join):
    """Both PFBs define the same 38 KF domain nodes and link topology in their
    metadata records.

    Verifies:
    1. All 38 KF domain node names appear in both PFB metadata records.
    2. Gen3 system nodes (``category: internal``) are present in BOTH PFBs and are
       excluded from the domain comparison -- Gen3 declares which nodes are
       system nodes, so they are derived rather than hardcoded.
    3. For each KF domain node the link ``(name, target, multiplicity)`` sets are
       *equal* -- including multiplicity, which round-trips via the inverse
       cardinality signals described in _metadata_nodes.

       ``core_metadata_collection`` is intentionally absent from the LinkML side:
       it is a Gen3 Schema construct, so linkml_utils does not synthesize it. A
       LinkML schema that declares a CoreMetadataCollection class gets the link
       like any other class reference.
    """
    gen3_bytes, linkml_bytes = _build_kf_pfbs(runner, invoke, path_join)
    domain_nodes = _kf_domain_nodes(path_join)

    gen3_meta = _metadata_nodes(gen3_bytes)
    linkml_meta = _metadata_nodes(linkml_bytes)

    # 1. All 38 KF domain nodes appear in both metadata records.
    missing_from_gen3 = domain_nodes - set(gen3_meta)
    missing_from_linkml = domain_nodes - set(linkml_meta)
    assert (
        not missing_from_gen3
    ), f"Domain nodes missing from Gen3 PFB metadata: {missing_from_gen3}"
    assert (
        not missing_from_linkml
    ), f"Domain nodes missing from LinkML PFB metadata: {missing_from_linkml}"

    # 2. Gen3 system nodes appear in both (both paths share the dictionaryutils
    #    loader) and are not part of the data model under comparison.
    internal = _internal_node_ids(path_join)
    assert internal, "expected Gen3 to declare at least one category: internal node"
    for node_id in internal:
        assert node_id in gen3_meta, f"{node_id!r} expected in Gen3 metadata"
        assert node_id in linkml_meta, f"{node_id!r} expected in LinkML metadata"
    assert not (
        domain_nodes & internal
    ), f"system nodes leaked into the domain comparison: {domain_nodes & internal}"

    # 3. Link sets match exactly, except for documented gaps.
    link_mismatches = []
    for node_name in sorted(domain_nodes - _LINK_COMPARISON_SKIP):
        gen3_links = gen3_meta[node_name]["link_pairs"]
        linkml_links = linkml_meta[node_name]["link_pairs"]
        if gen3_links != linkml_links:
            link_mismatches.append(
                f"{node_name}: gen3_only={sorted(gen3_links - linkml_links)}, "
                f"linkml_only={sorted(linkml_links - gen3_links)}"
            )

    assert (
        not link_mismatches
    ), "Link topology differs between the Gen3 and LinkML paths:\n" + "\n".join(
        f"  {m}" for m in link_mismatches
    )


def test_kf_gen3_vs_linkml_schema(runner, invoke, path_join):
    """Both PFBs expose the same 38 KF domain nodes in their AVRO schema union,
    and the domain-specific field sets on each node match.

    Verifies:
    1. All 38 KF domain node names appear in both AVRO schema unions.
    2. Gen3 system nodes (``category: internal``) are present in both schemas.
    3. For each comparable domain node (see ``_FIELD_COMPARISON_SKIP``), the field
       sets are identical -- with nothing subtracted from either side except the
       one known vendored-definition extra.  The LinkML path resolves
       ``ubiquitous_properties``/``data_file_properties`` and excludes link
       properties exactly like ``from dict``.
    """
    gen3_bytes, linkml_bytes = _build_kf_pfbs(runner, invoke, path_join)
    domain_nodes = _kf_domain_nodes(path_join)

    gen3_schema = _schema_nodes(gen3_bytes)
    linkml_schema = _schema_nodes(linkml_bytes)

    # 1. All 38 KF domain nodes appear in both AVRO schema unions.
    missing_from_gen3 = domain_nodes - set(gen3_schema)
    missing_from_linkml = domain_nodes - set(linkml_schema)
    assert (
        not missing_from_gen3
    ), f"Domain nodes missing from Gen3 AVRO schema: {missing_from_gen3}"
    assert (
        not missing_from_linkml
    ), f"Domain nodes missing from LinkML AVRO schema: {missing_from_linkml}"

    # 2. Gen3 system nodes appear in both schemas.
    for node_id in _internal_node_ids(path_join):
        assert node_id in gen3_schema, f"{node_id!r} expected in Gen3 schema"
        assert node_id in linkml_schema, f"{node_id!r} expected in LinkML schema"

    # 3. Domain field equality for comparable nodes.
    field_mismatches = []
    for node_name in sorted(domain_nodes - _FIELD_COMPARISON_SKIP):
        gen3_domain = gen3_schema.get(node_name, set())
        linkml_domain = (
            linkml_schema.get(node_name, set()) - _VENDORED_DEFINITION_EXTRAS
        )

        if gen3_domain != linkml_domain:
            field_mismatches.append(
                f"{node_name}: gen3_only={sorted(gen3_domain - linkml_domain)}, "
                f"linkml_only={sorted(linkml_domain - gen3_domain)}"
            )

    assert (
        not field_mismatches
    ), "Domain field mismatches between Gen3 and LinkML schemas:\n" + "\n".join(
        f"  {m}" for m in field_mismatches
    )


def test_kf_gen3_vs_linkml_enum_encoding(runner, invoke, path_join):
    """The LinkML path uses Gen3's AVRO enum encoding, not plain strings.

    A LinkML schema and the equivalent Gen3 dictionary describe the same enums,
    so wherever both paths emit an AVRO enum for the same node/field it must
    carry the same generated enum name and the same symbol list.

    This is the assertion that would have caught the original defect: the LinkML
    path used to type every enum property as a bare ``["null", "string"]``,
    so ``linkml_enums`` would have been empty.
    """
    gen3_bytes, linkml_bytes = _build_kf_pfbs(runner, invoke, path_join)

    gen3_enums = _schema_enum_types(gen3_bytes)
    linkml_enums = _schema_enum_types(linkml_bytes)

    # Guard against a vacuous pass: the LinkML path must emit real AVRO enums.
    assert len(linkml_enums) > 100, (
        f"LinkML path emitted only {len(linkml_enums)} AVRO enum fields; "
        "enum properties are probably being flattened to plain strings again"
    )

    shared = sorted(set(gen3_enums) & set(linkml_enums))
    assert len(shared) > 100, f"Only {len(shared)} enum fields in common"

    mismatches = [
        f"{node}.{field}: gen3={gen3_enums[(node, field)]} "
        f"linkml={linkml_enums[(node, field)]}"
        for node, field in shared
        if gen3_enums[(node, field)] != linkml_enums[(node, field)]
    ]

    assert (
        not mismatches
    ), "AVRO enum encoding differs between the Gen3 and LinkML paths:\n" + "\n".join(
        f"  {m}" for m in mismatches
    )
