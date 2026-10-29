import json
from copy import deepcopy

import fastavro
from fastavro import reader

from .base import (
    PY3,
    BlockIndex,
    PFBBase,
    PredicateError,
    decode_enum,
    decode_value,
    field_traits,
    handle_schema_field_unicode,
    load_manifest,
    str_hook,
)


class PFBReader(PFBBase):
    open_mode = "rb"

    def __enter__(self):
        rv = super(PFBReader, self).__enter__()
        self._reader = reader(self._file_obj)
        schema = []
        self.set_encoded_schema(self._reader.writer_schema)
        for f in self._reader.writer_schema["fields"]:
            if f["name"] == "object":
                it = iter(f["type"])
                # skip metadata
                next(it)
                for node in it:
                    node = deepcopy(node)
                    schema.append(node)
                    for field in node["fields"]:
                        handle_schema_field_unicode(field, encode=False)
        self.set_schema(json.loads(json.dumps(schema), object_pairs_hook=str_hook))
        self.set_metadata(next(self._reader)["object"])
        # The manifest is small and arrives in memory with the header, so it is
        # parsed eagerly.  A file without one simply leaves this None.
        manifest = load_manifest(self._reader.metadata)
        self.set_block_index(BlockIndex(manifest) if manifest else None)
        return rv

    def __iter__(self):
        return self

    def __next__(self):
        rv = next(self._reader)
        obj = rv["object"]
        to_update = {}
        for name, value in list(obj.items()):
            if value and self.is_encode(rv["name"], name):
                if isinstance(value, list):
                    thing = []
                    for val in value:
                        thing.append(decode_enum(val))
                    to_update[name] = thing
                else:
                    to_update[name] = decode_enum(value)
        obj.update(to_update)
        return rv

    if not PY3:
        next = __next__


class SearchResult(object):
    """Records found, plus what the index managed to skip getting there."""

    def __init__(self, records, blocks_total, blocks_read, records_scanned, indexed):
        self.records = records
        self.blocks_total = blocks_total
        self.blocks_read = blocks_read
        self.records_scanned = records_scanned
        self.indexed = indexed

    @property
    def blocks_pruned(self):
        return self.blocks_total - self.blocks_read

    @property
    def prune_ratio(self):
        if not self.blocks_total:
            return 0.0
        return self.blocks_pruned / float(self.blocks_total)

    def explain(self):
        return {
            "indexed": self.indexed,
            "blocks_total": self.blocks_total,
            "blocks_read": self.blocks_read,
            "blocks_pruned": self.blocks_pruned,
            "prune_ratio": round(self.prune_ratio, 4),
            "records_scanned": self.records_scanned,
            "records_matched": len(self.records),
        }


def search(file_obj, predicates):
    """Return records matching every predicate in ``predicates``.

    Decodes only the Avro blocks the index says could match.  All predicates
    must target the same node: they are ANDed against a single record, and a
    record belongs to exactly one node.

    When the file carries no index this still returns correct results by
    reading every block, so callers never have to special-case an unindexed
    file; ``SearchResult.indexed`` reports which path was taken.
    """
    if not predicates:
        raise PredicateError("at least one predicate is required")
    nodes = {p.node for p in predicates}
    if len(nodes) > 1:
        raise PredicateError(
            "all predicates must target one node, got: {}".format(
                ", ".join(sorted(nodes))
            )
        )
    node = nodes.pop()

    file_obj.seek(0)
    block_reader = fastavro.block_reader(file_obj)
    manifest = load_manifest(block_reader.metadata)
    traits = field_traits(block_reader.writer_schema).get(node, {})

    wanted = None
    if manifest is not None:
        index = BlockIndex(manifest)
        for predicate in predicates:
            blocks = index.candidate_blocks(predicate)
            wanted = blocks if wanted is None else (wanted & blocks)
            if not wanted:
                break

    # Only the fields a predicate mentions need decoding to test a record.
    # Decoding the whole object for every candidate roughly doubled query time
    # on a 29k-record export, so the full object is built only for matches.
    tested = {p.field for p in predicates}
    enum_fields = {name for name, trait in traits.items() if trait.get("enum", False)}

    records = []
    blocks_total = 0
    blocks_read = 0
    records_scanned = 0
    for ordinal, block in enumerate(block_reader):
        blocks_total += 1
        if wanted is not None and ordinal not in wanted:
            # Not decoded: block_reader skips the payload by its length prefix.
            continue
        blocks_read += 1
        for record in block:
            if record["name"] != node:
                continue
            records_scanned += 1
            raw = record["object"]
            probe = {
                name: decode_value(raw[name], name in enum_fields)
                for name in tested
                if name in raw
            }
            if not all(p.matches_record(probe) for p in predicates):
                continue
            record = dict(record)
            record["object"] = {
                name: decode_value(value, name in enum_fields)
                for name, value in raw.items()
            }
            records.append(record)

    return SearchResult(
        records=records,
        blocks_total=blocks_total,
        blocks_read=blocks_read,
        records_scanned=records_scanned,
        indexed=manifest is not None,
    )
