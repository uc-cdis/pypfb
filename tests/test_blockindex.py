"""Tests for the block-level search index."""

import io
import json

import fastavro
import pytest

from pfb.base import (
    MANIFEST_COMPRESS_THRESHOLD,
    MANIFEST_KEY,
    MANIFEST_KEY_COMPRESSED,
    STRATEGY_NONE,
    STRATEGY_POSTINGS,
    STRATEGY_RANGE,
    BlockIndex,
    PredicateError,
    dump_manifest,
    encode_enum,
    load_manifest,
    parse_predicate,
)
from pfb.reader import PFBReader, search
from pfb.writer import build_index, rechunk, write_indexed

POPULATIONS = ["CEU", "YRI", "JPT", "CHB"]


def _schema():
    """A PFB-shaped schema: object is a union of Metadata and node records."""
    return {
        "type": "record",
        "name": "Entity",
        "fields": [
            {"name": "id", "type": ["null", "string"], "default": None},
            {"name": "name", "type": "string"},
            {
                "name": "object",
                "type": [
                    {
                        "type": "record",
                        "name": "Metadata",
                        "fields": [{"name": "misc", "type": "string"}],
                    },
                    {
                        "type": "record",
                        "name": "demographic",
                        "fields": [
                            {
                                "name": "population",
                                "type": [
                                    "null",
                                    {
                                        "type": "enum",
                                        "name": "population_enum",
                                        "symbols": [
                                            encode_enum(p) for p in POPULATIONS
                                        ],
                                    },
                                ],
                                "default": None,
                            },
                            {
                                "name": "submitter_id",
                                "type": ["null", "string"],
                                "default": None,
                            },
                            {"name": "age", "type": ["null", "long"], "default": None},
                            {
                                "name": "consent_codes",
                                "type": {"type": "array", "items": "string"},
                                "default": [],
                            },
                        ],
                    },
                    {
                        "type": "record",
                        "name": "sample",
                        "fields": [
                            {
                                "name": "tissue",
                                "type": ["null", "string"],
                                "default": None,
                            }
                        ],
                    },
                ],
            },
            {
                "name": "relations",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Relation",
                        "fields": [
                            {"name": "dst_id", "type": "string"},
                            {"name": "dst_name", "type": "string"},
                        ],
                    },
                },
                "default": [],
            },
        ],
    }


def _demographic(i):
    return {
        "id": "d{}".format(i),
        "name": "demographic",
        "object": (
            "demographic",
            {
                # Enum values go on the wire escaped, as PFBWriter writes them.
                "population": encode_enum(POPULATIONS[i % len(POPULATIONS)]),
                "submitter_id": "subject-{}".format(i),
                "age": i % 90,
                "consent_codes": ["c{}".format(i % 3), "open"],
            },
        ),
        "relations": [],
    }


def _sample(i):
    return {
        "id": "s{}".format(i),
        "name": "sample",
        "object": ("sample", {"tissue": "blood" if i % 2 else "saliva"}),
        "relations": [],
    }


def _records(count=600, interleave=False):
    """Records in the layout ``PFBWriter`` produces: grouped by node.

    Grouping is what makes the node-level block map worth having, and it is
    what real exports look like.  ``interleave=True`` produces the worst case
    instead, where every block contains every node.
    """
    yield {
        "id": None,
        "name": "Metadata",
        "object": ("Metadata", {"misc": "x"}),
        "relations": [],
    }
    if interleave:
        for i in range(count):
            yield _demographic(i)
            yield _sample(i)
        return
    for i in range(count):
        yield _demographic(i)
    for i in range(count):
        yield _sample(i)


@pytest.fixture
def plain_pfb():
    buffer = io.BytesIO()
    fastavro.writer(
        buffer, _schema(), list(_records()), codec="null", sync_interval=2000
    )
    buffer.seek(0)
    return buffer


@pytest.fixture
def interleaved_pfb():
    """Worst case: every block holds every node and every value."""
    buffer = io.BytesIO()
    fastavro.writer(
        buffer,
        _schema(),
        list(_records(interleave=True)),
        codec="null",
        sync_interval=2000,
    )
    buffer.seek(0)
    result = build_index(buffer, domain_cap=8)
    out = io.BytesIO()
    write_indexed(out, result)
    out.seek(0)
    return out


@pytest.fixture
def indexed_pfb(plain_pfb):
    result = build_index(plain_pfb, domain_cap=8)
    out = io.BytesIO()
    write_indexed(out, result)
    out.seek(0)
    return out


def _scan(buffer, node, field, value):
    """Brute-force reference implementation to compare the index against."""
    buffer.seek(0)
    from pfb.base import decode_enum

    out = []
    for record in fastavro.reader(buffer):
        if record["name"] != node:
            continue
        raw = record["object"].get(field)
        if isinstance(raw, str):
            raw = decode_enum(raw)
        if raw == value:
            out.append(record)
    return out


# --- manifest -------------------------------------------------------------


def test_manifest_roundtrips_as_plain_json():
    manifest = {"version": 1, "nodes": {}, "fields": {}}
    stored = dump_manifest(manifest)
    assert list(stored) == [MANIFEST_KEY]
    assert load_manifest(stored) == manifest


def test_large_manifest_is_compressed():
    manifest = {
        "version": 1,
        "nodes": {"n": list(range(MANIFEST_COMPRESS_THRESHOLD))},
        "fields": {},
    }
    stored = dump_manifest(manifest)
    assert list(stored) == [MANIFEST_KEY_COMPRESSED]
    assert load_manifest(stored) == manifest


def test_unknown_version_degrades_to_no_index():
    stored = {MANIFEST_KEY: json.dumps({"version": 99})}
    assert load_manifest(stored) is None


def test_missing_and_corrupt_manifests_return_none():
    assert load_manifest(None) is None
    assert load_manifest({}) is None
    assert load_manifest({MANIFEST_KEY: "not json"}) is None


# --- building -------------------------------------------------------------


def test_build_produces_multiple_blocks(plain_pfb):
    result = build_index(plain_pfb)
    assert result.manifest["num_blocks"] > 1
    assert result.manifest["num_records"] == 1201


def test_node_block_map_covers_every_node(plain_pfb):
    manifest = build_index(plain_pfb).manifest
    assert set(manifest["nodes"]) == {"Metadata", "demographic", "sample"}
    # Grouped writing means the two data nodes barely share a block.
    shared = set(manifest["nodes"]["demographic"]) & set(manifest["nodes"]["sample"])
    assert len(shared) <= 1


def test_enum_values_are_indexed_decoded(plain_pfb):
    manifest = build_index(plain_pfb).manifest
    entry = manifest["fields"]["demographic.population"]
    assert entry["s"] == STRATEGY_POSTINGS
    assert set(entry["v"]) == set(POPULATIONS)


def test_cardinality_guard_drops_high_cardinality_strings(plain_pfb):
    manifest = build_index(plain_pfb, domain_cap=8).manifest
    # 600 distinct submitter_ids, not orderable -> no index, not a 600-key map.
    assert manifest["fields"]["demographic.submitter_id"]["s"] == STRATEGY_NONE


def test_cardinality_guard_falls_back_to_range_for_numbers(plain_pfb):
    manifest = build_index(plain_pfb, domain_cap=8).manifest
    entry = manifest["fields"]["demographic.age"]
    assert entry["s"] == STRATEGY_RANGE
    for low, high in entry["r"].values():
        assert low <= high


def test_array_fields_are_indexed_per_element(plain_pfb):
    manifest = build_index(plain_pfb, domain_cap=8).manifest
    entry = manifest["fields"]["demographic.consent_codes"]
    assert entry["s"] == STRATEGY_POSTINGS
    assert set(entry["v"]) == {"c0", "c1", "c2", "open"}


def test_field_selection_limits_what_is_indexed(plain_pfb):
    manifest = build_index(plain_pfb, fields=["demographic.population"]).manifest
    assert list(manifest["fields"]) == ["demographic.population"]
    # The node map is always built; it is what prunes other nodes.
    assert manifest["nodes"]["sample"]


# --- container ------------------------------------------------------------


def test_indexed_file_has_identical_records(plain_pfb, indexed_pfb):
    plain_pfb.seek(0)
    before = list(fastavro.reader(plain_pfb))
    indexed_pfb.seek(0)
    after = list(fastavro.reader(indexed_pfb))
    assert before == after


def test_indexed_file_schema_is_unchanged(plain_pfb, indexed_pfb):
    plain_pfb.seek(0)
    indexed_pfb.seek(0)
    assert (
        fastavro.reader(plain_pfb).writer_schema
        == fastavro.reader(indexed_pfb).writer_schema
    )


def test_block_boundaries_survive_the_copy(plain_pfb, indexed_pfb):
    plain_pfb.seek(0)
    before = [b.num_records for b in fastavro.block_reader(plain_pfb)]
    indexed_pfb.seek(0)
    after = [b.num_records for b in fastavro.block_reader(indexed_pfb)]
    assert before == after


def test_codec_is_preserved():
    buffer = io.BytesIO()
    fastavro.writer(buffer, _schema(), list(_records(50)), codec="deflate")
    buffer.seek(0)
    result = build_index(buffer)
    assert result.codec == "deflate"
    out = io.BytesIO()
    write_indexed(out, result)
    out.seek(0)
    reader = fastavro.reader(out)
    assert reader.codec == "deflate"
    assert len(list(reader)) == 101


def test_rechunk_changes_block_count_not_records(plain_pfb):
    plain_pfb.seek(0)
    original = list(fastavro.reader(plain_pfb))
    fine = rechunk(plain_pfb, 256)
    assert list(fastavro.reader(fine)) == original
    fine.seek(0)
    coarse_blocks = build_index(plain_pfb).manifest["num_blocks"]
    assert build_index(fine).manifest["num_blocks"] > coarse_blocks


# --- predicates -----------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("a.b=c", ("a", "b", "=", "c")),
        ("a.b!=c", ("a", "b", "!=", "c")),
        ("a.b>=3", ("a", "b", ">=", "3")),
        ("a.b<=3", ("a", "b", "<=", "3")),
        ("a.b>3", ("a", "b", ">", "3")),
        ("node.nested.field=v", ("node", "nested.field", "=", "v")),
    ],
)
def test_parse_predicate(text, expected):
    predicate = parse_predicate(text)
    assert (
        predicate.node,
        predicate.field,
        predicate.op,
        predicate.value,
    ) == expected


@pytest.mark.parametrize("text", ["nofield", "a.b", "a.b=", "a.b~c"])
def test_parse_predicate_rejects_bad_input(text):
    with pytest.raises(PredicateError):
        parse_predicate(text)


def test_numeric_comparison_is_not_lexical():
    predicate = parse_predicate("n.age>9")
    assert predicate.matches_record({"age": 10})
    assert not predicate.matches_record({"age": 5})


# --- search ---------------------------------------------------------------


def test_search_matches_brute_force_scan(plain_pfb, indexed_pfb):
    expected = _scan(plain_pfb, "demographic", "population", "CEU")
    result = search(indexed_pfb, [parse_predicate("demographic.population=CEU")])
    assert result.indexed
    assert len(result.records) == len(expected) == 150
    assert [r["id"] for r in result.records] == [r["id"] for r in expected]


def test_search_prunes_blocks(indexed_pfb):
    """Node grouping alone prunes every block that holds no demographic."""
    result = search(indexed_pfb, [parse_predicate("demographic.population=CEU")])
    assert result.blocks_read < result.blocks_total
    assert result.prune_ratio > 0.3


def test_scattered_values_in_coarse_blocks_do_not_prune(interleaved_pfb):
    """A value present in every block cannot be pruned for, by design.

    The index returns a superset of candidate blocks; when the data is spread
    evenly that superset is every block.  This is the limitation that block
    size, not the index format, is the lever against.
    """
    result = search(interleaved_pfb, [parse_predicate("demographic.population=CEU")])
    assert result.blocks_read == result.blocks_total
    assert len(result.records) == 150


def test_search_returns_decoded_enum_values(indexed_pfb):
    result = search(indexed_pfb, [parse_predicate("demographic.population=CEU")])
    assert {r["object"]["population"] for r in result.records} == {"CEU"}


def test_unknown_value_prunes_every_block(indexed_pfb):
    result = search(indexed_pfb, [parse_predicate("demographic.population=NOPE")])
    assert result.records == []
    assert result.blocks_read == 0


def test_search_without_an_index_still_works(plain_pfb):
    result = search(plain_pfb, [parse_predicate("demographic.population=CEU")])
    assert not result.indexed
    assert len(result.records) == 150
    assert result.blocks_read == result.blocks_total


def test_search_on_unindexed_field_falls_back_to_node_blocks(indexed_pfb):
    # submitter_id is over the cap, so only the node map can prune.
    result = search(
        indexed_pfb, [parse_predicate("demographic.submitter_id=subject-7")]
    )
    assert len(result.records) == 1
    assert result.records[0]["id"] == "d7"


def test_range_predicate_matches_scan(indexed_pfb):
    result = search(indexed_pfb, [parse_predicate("demographic.age>=80")])
    assert len(result.records) == len([i for i in range(600) if i % 90 >= 80])


def test_array_membership(indexed_pfb):
    result = search(indexed_pfb, [parse_predicate("demographic.consent_codes=c1")])
    assert len(result.records) == len([i for i in range(600) if i % 3 == 1])


def test_predicates_are_anded(indexed_pfb):
    both = search(
        indexed_pfb,
        [
            parse_predicate("demographic.population=CEU"),
            parse_predicate("demographic.age<10"),
        ],
    )
    for record in both.records:
        assert record["object"]["population"] == "CEU"
        assert record["object"]["age"] < 10


def test_search_rejects_predicates_across_nodes(indexed_pfb):
    with pytest.raises(PredicateError):
        search(
            indexed_pfb,
            [
                parse_predicate("demographic.population=CEU"),
                parse_predicate("sample.tissue=blood"),
            ],
        )


def test_search_requires_a_predicate(indexed_pfb):
    with pytest.raises(PredicateError):
        search(indexed_pfb, [])


# --- reader integration ---------------------------------------------------


def test_reader_exposes_block_index(indexed_pfb):
    with PFBReader(indexed_pfb) as reader:
        assert isinstance(reader.block_index, BlockIndex)
        assert "demographic" in reader.block_index.nodes


def test_reader_without_index_reports_none(plain_pfb):
    with PFBReader(plain_pfb) as reader:
        assert reader.block_index is None


def test_reader_sees_no_extra_nodes(plain_pfb, indexed_pfb):
    # build_index consumed plain_pfb while making the indexed fixture.
    plain_pfb.seek(0)
    with PFBReader(plain_pfb) as plain:
        before = [n["name"] for n in plain.schema]
    with PFBReader(indexed_pfb) as indexed:
        after = [n["name"] for n in indexed.schema]
    assert before == after
