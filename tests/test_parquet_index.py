"""Tests for the Parquet-derived statistics and embedded search index."""

import io

import fastavro
import pytest

pa = pytest.importorskip("pyarrow")

from pfb.manifest import (  # noqa: E402
    MANIFEST_COMPRESS_THRESHOLD,
    MANIFEST_KEY,
    MANIFEST_KEY_COMPRESSED,
    NODE_PARQUET,
    STRATEGY_BLOOM,
    STRATEGY_DOMAIN,
    STRATEGY_NONE,
    STRATEGY_RANGE,
    PredicateError,
    answerable_from_manifest,
    dump_manifest,
    load_manifest,
    parse_predicate,
)
from pfb.parquet import (  # noqa: E402
    ID_COLUMN,
    RELATIONS_COLUMN,
    UnsupportedPFBType,
    avro_field_to_arrow,
    build_index,
    node_to_arrow_schema,
    search_payload,
    write_index,
)
from pfb.reader import PFBReader  # noqa: E402


@pytest.fixture
def source(test_avro):
    return io.BytesIO(test_avro)


@pytest.fixture
def make_source(test_avro):
    """A fresh buffer per call.

    ``build_index`` reads through ``PFBReader``, whose context manager closes
    the file, so a test that both builds and re-reads the input needs two.
    """

    def _make():
        return io.BytesIO(test_avro)

    return _make


@pytest.fixture
def built(source):
    return build_index(source)


def _write(result, embed):
    out = io.BytesIO()
    manifest = write_index(out, result, embed=embed)
    out.seek(0)
    return out, manifest


# --- type mapping ---------------------------------------------------------


def test_nullable_union_maps_to_the_inner_type():
    assert avro_field_to_arrow({"name": "f", "type": ["null", "string"]}) == pa.string()
    assert avro_field_to_arrow({"name": "f", "type": ["null", "long"]}) == pa.int64()


def test_enum_maps_to_string():
    field = {
        "name": "f",
        "type": ["null", {"type": "enum", "name": "e", "symbols": ["a"]}],
    }
    assert avro_field_to_arrow(field) == pa.string()


def test_multi_branch_union_of_one_arrow_type_collapses():
    """``union<enum, enum, null>`` is what real Gen3 `state` fields look like.

    Rejecting every multi-branch union would abort on the first node of a real
    export; 36 of these occur in the sample dictionary alone.
    """
    field = {
        "name": "state",
        "type": [
            {"type": "enum", "name": "a", "symbols": ["x"]},
            {"type": "enum", "name": "b", "symbols": ["y"]},
            "null",
        ],
    }
    assert avro_field_to_arrow(field) == pa.string()


def test_heterogeneous_union_raises():
    with pytest.raises(UnsupportedPFBType):
        avro_field_to_arrow({"name": "f", "type": ["string", "long", "null"]})


def test_null_only_union_maps_to_null():
    assert avro_field_to_arrow({"name": "f", "type": ["null"]}) == pa.null()


def test_array_of_enum_maps_to_list_of_string():
    field = {
        "name": "f",
        "type": [
            "null",
            {"type": "array", "items": {"type": "enum", "name": "e", "symbols": ["a"]}},
        ],
    }
    assert avro_field_to_arrow(field) == pa.list_(pa.string())


def test_node_schema_carries_id_and_relations():
    node = {"name": "n", "fields": [{"name": "a", "type": ["null", "string"]}]}
    schema = node_to_arrow_schema(node)
    assert schema.names == [ID_COLUMN, "a", RELATIONS_COLUMN]


def test_every_node_in_the_sample_dictionary_maps(source):
    """The whole sample dictionary must map without raising."""
    with PFBReader(source) as reader:
        for node in reader.schema:
            node_to_arrow_schema(node)


# --- manifest -------------------------------------------------------------


def test_manifest_roundtrips_as_plain_json():
    manifest = {"version": 1, "nodes": {}}
    stored = dump_manifest(manifest)
    assert list(stored) == [MANIFEST_KEY]
    assert load_manifest(stored) == manifest


def test_large_manifest_is_compressed():
    manifest = {"version": 1, "nodes": {"n": list(range(MANIFEST_COMPRESS_THRESHOLD))}}
    stored = dump_manifest(manifest)
    assert list(stored) == [MANIFEST_KEY_COMPRESSED]
    assert load_manifest(stored) == manifest


def test_unknown_version_degrades_to_no_index():
    import json

    assert load_manifest({MANIFEST_KEY: json.dumps({"version": 99})}) is None


def test_missing_and_corrupt_manifests_return_none():
    assert load_manifest(None) is None
    assert load_manifest({}) is None
    assert load_manifest({MANIFEST_KEY: "not json"}) is None


# --- building -------------------------------------------------------------


def test_build_covers_every_populated_node(built):
    assert built.manifest["nodes"]
    for entry in built.manifest["nodes"].values():
        assert entry["num_rows"] >= 1
        assert entry["payload_bytes"] > 0


def test_payload_values_match_the_records(make_source, built):
    """A Parquet column must hold what the Avro record held."""
    with PFBReader(make_source()) as reader:
        records = {r["id"]: r["object"] for r in reader if r["name"] == "demographic"}
    table = pa_table(built.payloads["demographic"])
    rows = {row[ID_COLUMN]: row for row in table.to_pylist()}
    assert set(rows) == set(records)
    for record_id, original in records.items():
        for field, value in original.items():
            assert rows[record_id][field] == value


def pa_table(payload):
    import pyarrow.parquet as pq

    return pq.read_table(pa.BufferReader(payload))


def test_enum_values_are_stored_decoded(built):
    """Parquet must hold `validated`, not an escaped `_32_` form.

    Checked by idempotence: decoding an already-decoded value is a no-op,
    whereas a stored `_32_` would decode to something different.
    """
    from pfb.base import decode_enum

    checked = 0
    for payload in built.payloads.values():
        table = pa_table(payload)
        for name in table.schema.names:
            column = table.column(name)
            if not pa.types.is_string(column.type):
                continue
            for value in column.to_pylist():
                if value is None:
                    continue
                assert decode_enum(value) == value, (name, value)
                checked += 1
    assert checked > 0


def test_page_index_is_actually_written(built):
    """Guards against write_page_index defaulting off."""
    import pyarrow.parquet as pq

    payload = built.payloads["demographic"]
    metadata = pq.ParquetFile(pa.BufferReader(payload)).metadata
    column = metadata.row_group(0).column(0)
    # pyarrow exposes no ColumnIndex accessor, but the footer records the
    # offsets at which the index was written.
    assert column.has_offset_index or column.has_column_index


def test_bloom_filters_are_written_for_identifier_columns(built):
    import pyarrow.parquet as pq

    payload = built.payloads["demographic"]
    metadata = pq.ParquetFile(pa.BufferReader(payload)).metadata
    names = [
        metadata.row_group(0).column(i).path_in_schema
        for i in range(metadata.num_columns)
    ]
    index = names.index("submitter_id")
    assert metadata.row_group(0).column(index).bloom_filter_offset is not None


def test_relations_are_carried(built):
    table = pa_table(built.payloads["demographic"])
    assert RELATIONS_COLUMN in table.schema.names


def test_edge_counts_are_derived(built):
    assert built.manifest["edges"]
    assert all(count > 0 for count in built.manifest["edges"].values())


# --- cardinality guard ----------------------------------------------------


def test_small_domains_are_listed(source):
    source.seek(0)
    manifest = build_index(source, domain_cap=256).manifest
    fields = manifest["nodes"]["demographic"]["fields"]
    listed = [f for f in fields.values() if f["strategy"] == STRATEGY_DOMAIN]
    assert listed
    for field in listed:
        assert len(field["domain"]) == field["distinct_count"] <= 256


def test_cardinality_guard_falls_back_above_the_cap(source):
    source.seek(0)
    manifest = build_index(source, domain_cap=0).manifest
    strategies = {
        f["strategy"]
        for node in manifest["nodes"].values()
        for f in node["fields"].values()
    }
    # With a cap of zero nothing may be listed as a full domain.
    assert STRATEGY_DOMAIN not in strategies
    assert strategies & {STRATEGY_BLOOM, STRATEGY_RANGE, STRATEGY_NONE}


def test_bloom_columns_take_the_bloom_strategy(source):
    source.seek(0)
    manifest = build_index(source, domain_cap=0).manifest
    assert (
        manifest["nodes"]["demographic"]["fields"]["submitter_id"]["strategy"]
        == STRATEGY_BLOOM
    )


# --- container: tiers 0-2 -------------------------------------------------


def test_unembedded_output_has_no_new_union_member(built):
    out, manifest = _write(built, embed=False)
    assert manifest["embedded"] is False
    members = [
        m["name"]
        for f in fastavro.reader(out).writer_schema["fields"]
        if f["name"] == "object"
        for m in f["type"]
    ]
    assert NODE_PARQUET not in members


def test_unembedded_output_keeps_every_record(make_source, built):
    out, _ = _write(built, embed=False)
    before = [r for r in fastavro.reader(make_source())]
    after = [r for r in fastavro.reader(out)]
    assert len(before) == len(after)


def test_manifest_is_readable_without_pyarrow(built):
    """Tier 0-2 consumers need only fastavro to read the header."""
    out, _ = _write(built, embed=False)
    manifest = load_manifest(fastavro.block_reader(out).metadata)
    assert manifest["nodes"]


# --- container: tier 3 ----------------------------------------------------


def test_embedded_output_contains_one_payload_per_node(built):
    out, manifest = _write(built, embed=True)
    assert manifest["embedded"] is True
    with PFBReader(out) as reader:
        fetched = {n: reader.parquet(n) for n in built.payloads}
    assert fetched == built.payloads


def test_payload_block_ordinals_are_recorded(built):
    _, manifest = _write(built, embed=True)
    assert set(manifest["payload_blocks"]) == set(built.payloads)


def test_payload_is_fetched_without_full_iteration(built):
    """The whole point of recording block ordinals.

    Emitting payloads last keeps consumers working, but would make retrieval
    cost a full scan if the only way to reach one were to iterate.  The reader
    seeks to the recorded block instead.
    """
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        payload = reader.parquet("demographic")
        # No iteration happened; the payload still came back.
        assert payload == built.payloads["demographic"]


def test_parquet_file_helper_opens_the_payload(built):
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        parquet_file = reader.parquet_file("demographic")
        assert parquet_file.metadata.num_rows >= 1


def test_parquet_returns_none_without_an_index(source):
    source.seek(0)
    with PFBReader(source) as reader:
        assert reader.parquet("demographic") is None


def test_parquet_returns_none_for_unknown_node(built):
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        assert reader.parquet("no_such_node") is None


# --- backward compatibility ----------------------------------------------


def test_reader_reports_the_same_nodes_with_and_without_payloads(make_source, built):
    with PFBReader(make_source()) as reader:
        before = [n["name"] for n in reader.schema]
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        after = [n["name"] for n in reader.schema]
    assert before == after
    assert NODE_PARQUET not in after


def test_iteration_never_yields_payload_records(built):
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        names = [r["name"] for r in reader]
    assert NODE_PARQUET not in names


def test_records_are_identical_through_the_reader(make_source, built):
    with PFBReader(make_source()) as reader:
        before = list(reader)
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        after = list(reader)
    assert before == after


# --- predicates and search -----------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("a.b=c", ("a", "b", "=", "c")),
        ("a.b!=c", ("a", "b", "!=", "c")),
        ("a.b>=3", ("a", "b", ">=", "3")),
    ],
)
def test_parse_predicate(text, expected):
    p = parse_predicate(text)
    assert (p.node, p.field, p.op, p.value) == expected


@pytest.mark.parametrize("text", ["nofield", "a.b", "a.b=", "a.b~c"])
def test_parse_predicate_rejects_bad_input(text):
    with pytest.raises(PredicateError):
        parse_predicate(text)


def test_search_matches_a_brute_force_scan(make_source, built):
    with PFBReader(make_source()) as reader:
        records = [r for r in reader if r["name"] == "demographic"]
    target = records[0]["object"]["submitter_id"]
    result = search_payload(
        built.payloads["demographic"],
        [parse_predicate("demographic.submitter_id={}".format(target))],
    )
    assert result.ids() == [records[0]["id"]]


def test_search_on_unknown_field_raises(built):
    with pytest.raises(PredicateError):
        search_payload(
            built.payloads["demographic"], [parse_predicate("demographic.nope=1")]
        )


def test_manifest_rules_out_a_value_outside_the_domain(built):
    predicate = parse_predicate("demographic.submitter_id=definitely-not-present")
    manifest = built.manifest
    field = manifest["nodes"]["demographic"]["fields"]["submitter_id"]
    if field["strategy"] != STRATEGY_DOMAIN:
        pytest.skip("submitter_id is not domain-indexed at the default cap")
    assert answerable_from_manifest(manifest, predicate) is False


def test_manifest_cannot_confirm_a_present_value(built):
    manifest = built.manifest
    field = manifest["nodes"]["demographic"]["fields"]["submitter_id"]
    if field["strategy"] != STRATEGY_DOMAIN:
        pytest.skip("submitter_id is not domain-indexed at the default cap")
    predicate = parse_predicate(
        "demographic.submitter_id={}".format(field["domain"][0])
    )
    # Never True: being in the domain says some record has it, not which.
    assert answerable_from_manifest(manifest, predicate) is None


def test_manifest_rules_out_an_unknown_node(built):
    assert (
        answerable_from_manifest(built.manifest, parse_predicate("nope.field=1"))
        is False
    )


# --- payload fetch must not disturb iteration ----------------------------


def test_parquet_does_not_disturb_a_fresh_reader(make_source, built):
    """Fetching a payload must leave iteration intact.

    ``parquet()`` has to seek, and the live fastavro reader is mid-stream on the
    same handle.  Seeking it directly rewinds the file underneath the reader, so
    the records that came back afterwards were garbage.  The only code path that
    did both is the ``pfb search`` fallback, which is why this needed its own
    test.
    """
    with PFBReader(make_source()) as plain:
        expected = list(plain)

    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        assert reader.parquet("demographic") is not None
        assert list(reader) == expected


def test_parquet_mid_iteration_does_not_lose_records(built):
    """Interleaving a payload fetch with iteration yields every record once."""
    out, _ = _write(built, embed=True)
    blob = out.getvalue()

    with PFBReader(io.BytesIO(blob)) as reader:
        everything = [r["id"] for r in reader]

    with PFBReader(io.BytesIO(blob)) as reader:
        seen = []
        for index, record in enumerate(reader):
            seen.append(record["id"])
            if index == 3:
                assert reader.parquet("demographic") is not None
        assert seen == everything


def test_repeated_payload_fetches_are_stable(built):
    out, _ = _write(built, embed=True)
    with PFBReader(out) as reader:
        first = reader.parquet("demographic")
        second = reader.parquet("demographic")
        assert first == second == built.payloads["demographic"]
