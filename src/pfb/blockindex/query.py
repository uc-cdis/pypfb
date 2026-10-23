# -*- coding: utf-8 -*-
"""Predicate parsing and index-driven record retrieval."""

import operator
import re

import fastavro

from .manifest import BlockIndex, load_manifest
from .schema import decode_value, field_traits

#: Longest operators first so ``>=`` is not parsed as ``>``.
_OPS = ["!=", ">=", "<=", "=", ">", "<"]

_COMPARE = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

_PREDICATE_RE = re.compile(
    r"^(?P<node>[^.]+)\.(?P<field>.+?)(?P<op>!=|>=|<=|=|>|<)(?P<value>.*)$"
)


class PredicateError(ValueError):
    """Raised for a predicate that cannot be parsed or applied."""


def _as_number(value):
    """Return ``value`` as a float, or ``None`` if it is not numeric."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class Predicate(object):
    """A single ``node.field OP value`` test.

    Comparison is numeric when both sides parse as numbers and lexical
    otherwise, which keeps ``age>50`` working without the caller having to
    declare types.  ``=`` against an array field means "contains".
    """

    def __init__(self, node, field, op, value):
        if op not in _COMPARE:
            raise PredicateError("unsupported operator: {}".format(op))
        self.node = node
        self.field = field
        self.op = op
        self.value = value
        self._number = _as_number(value)

    def __repr__(self):
        return "<Predicate {}.{}{}{}>".format(
            self.node, self.field, self.op, self.value
        )

    def as_text(self):
        """The right-hand side as it is keyed in a postings map."""
        return str(self.value)

    def _compare(self, left):
        if left is None:
            return self.op == "!="
        if self._number is not None:
            left_number = _as_number(left)
            if left_number is not None:
                return _COMPARE[self.op](left_number, self._number)
        return _COMPARE[self.op](str(left), str(self.value))

    def matches_value(self, value):
        """Test one scalar (or one element of an array field)."""
        return self._compare(value)

    def matches_record(self, obj):
        """Test a decoded record ``object`` mapping."""
        if self.field not in obj:
            return self.op == "!="
        value = obj[self.field]
        if isinstance(value, list):
            # "=" on a list is a membership test; "!=" means "does not contain".
            if self.op == "!=":
                return not any(self._compare_eq(v) for v in value)
            return any(self.matches_value(v) for v in value)
        return self.matches_value(value)

    def _compare_eq(self, value):
        return str(value) == str(self.value)

    def overlaps_range(self, low, high):
        """Could a value in ``[low, high]`` satisfy this predicate?

        Used against per-block bounds.  Errs toward ``True`` so that a block is
        only skipped when it provably cannot match.
        """
        if low is None or high is None:
            return True
        if self.op == "!=":
            # Only prunable when the block holds exactly one distinct value.
            return not (low == high and self._compare_eq(low))
        low_number, high_number = _as_number(low), _as_number(high)
        if (
            self._number is not None
            and low_number is not None
            and high_number is not None
        ):
            low_value, high_value, target = low_number, high_number, self._number
        else:
            low_value, high_value, target = str(low), str(high), str(self.value)
        if self.op == "=":
            return low_value <= target <= high_value
        if self.op == ">":
            return high_value > target
        if self.op == ">=":
            return high_value >= target
        if self.op == "<":
            return low_value < target
        if self.op == "<=":
            return low_value <= target
        return True


def parse_predicate(text):
    """Parse ``NODE.FIELD[=|!=|>|>=|<|<=]VALUE`` into a :class:`Predicate`."""
    match = _PREDICATE_RE.match(text.strip())
    if not match:
        raise PredicateError(
            "cannot parse predicate {!r}; expected NODE.FIELD=VALUE".format(text)
        )
    parts = match.groupdict()
    if not parts["value"]:
        raise PredicateError("predicate {!r} has an empty value".format(text))
    return Predicate(parts["node"], parts["field"], parts["op"], parts["value"])


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

    All predicates must target the same node: they are ANDed against a single
    record, and a record belongs to exactly one node.

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
