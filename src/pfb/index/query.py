# -*- coding: utf-8 -*-
"""Predicate handling: manifest-level answers, then Parquet-level retrieval."""

import re

import pyarrow.dataset as ds

from .arrow import ID_COLUMN
from .stats import BLOOM, DOMAIN, NONE, RANGE, open_payload

_PREDICATE_RE = re.compile(
    r"^(?P<node>[^.]+)\.(?P<field>.+?)(?P<op>!=|>=|<=|=|>|<)(?P<value>.*)$"
)


class PredicateError(ValueError):
    """Raised for a predicate that cannot be parsed or applied."""


class Predicate(object):
    """A single ``node.field OP value`` test."""

    def __init__(self, node, field, op, value):
        self.node = node
        self.field = field
        self.op = op
        self.value = value

    def __repr__(self):
        return "<Predicate {}.{}{}{}>".format(
            self.node, self.field, self.op, self.value
        )

    def coerce(self, arrow_type):
        """Cast the right-hand side to match the column it is compared against."""
        import pyarrow as pa

        text = self.value
        try:
            if pa.types.is_integer(arrow_type):
                return int(text)
            if pa.types.is_floating(arrow_type):
                return float(text)
            if pa.types.is_boolean(arrow_type):
                return text.lower() in ("true", "1", "yes")
        except ValueError:
            raise PredicateError(
                "{!r} is not valid for column {} of type {}".format(
                    text, self.field, arrow_type
                )
            )
        return text

    def to_expression(self, arrow_type):
        """Build the pyarrow dataset expression that does the pushdown."""
        field = ds.field(self.field)
        value = self.coerce(arrow_type)
        if self.op == "=":
            return field == value
        if self.op == "!=":
            return field != value
        if self.op == ">":
            return field > value
        if self.op == ">=":
            return field >= value
        if self.op == "<":
            return field < value
        if self.op == "<=":
            return field <= value
        raise PredicateError("unsupported operator: {}".format(self.op))


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


def answerable_from_manifest(manifest, predicate):
    """Can the manifest alone prove this predicate matches nothing?

    Returns ``False`` when no record can match and ``None`` when the manifest
    cannot tell, which is the common case.  It never returns ``True``: a value
    being in the domain says only that some record has it, not which.

    This is tier 2 doing useful work without reading a payload at all.
    """
    node = manifest.get("nodes", {}).get(predicate.node)
    if not node:
        return False
    summary = node.get("fields", {}).get(predicate.field)
    if not summary:
        return None
    if predicate.op != "=":
        return None
    strategy = summary.get("strategy")
    if strategy == DOMAIN:
        domain = {str(v) for v in summary.get("domain", [])}
        return None if predicate.value in domain else False
    if strategy == RANGE:
        low, high = summary.get("min"), summary.get("max")
        if low is None or high is None:
            return None
        try:
            target = float(predicate.value)
            low, high = float(low), float(high)
        except (TypeError, ValueError):
            return None
        return None if low <= target <= high else False
    if strategy in (BLOOM, NONE):
        return None
    return None


class SearchResult(object):
    """Matching rows, plus how the answer was reached."""

    def __init__(self, rows, source, row_groups=0, num_rows=0, pages_note=None):
        self.rows = rows
        self.source = source
        self.row_groups = row_groups
        self.num_rows = num_rows
        self.pages_note = pages_note

    def ids(self):
        return [row[ID_COLUMN] for row in self.rows]

    def explain(self):
        """What the index did, and what cannot be reported.

        pyarrow 26 exposes no ``ColumnIndex``/``OffsetIndex`` accessor and no
        counter for pages or row groups actually skipped, so there is no honest
        way to report realized pruning from inside this process.  Rather than
        print a number that is really just the total, the fields that cannot be
        measured are named as such.
        """
        return {
            "source": self.source,
            "row_groups_in_payload": self.row_groups,
            "rows_in_payload": self.num_rows,
            "rows_matched": len(self.rows),
            "pages_skipped": "not reportable: pyarrow exposes no page-index API",
            "note": self.pages_note,
        }


def search_payload(payload, predicates):
    """Filter one node's Parquet payload by every predicate, ANDed.

    Pushdown is left entirely to pyarrow: the dataset API consults row-group
    statistics and the page index that :mod:`pfb.index.staging` enabled at
    write time.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_file = open_payload(payload)
    schema = parquet_file.schema_arrow
    expression = None
    for predicate in predicates:
        if predicate.field not in schema.names:
            raise PredicateError(
                "{} has no field {}".format(predicate.node, predicate.field)
            )
        term = predicate.to_expression(schema.field(predicate.field).type)
        expression = term if expression is None else (expression & term)

    # read_table applies the filter through the dataset layer, which consults
    # the row-group statistics and page index written by staging.py.
    table = pq.read_table(pa.BufferReader(payload), filters=expression)
    return SearchResult(
        rows=table.to_pylist(),
        source="parquet",
        row_groups=parquet_file.metadata.num_row_groups,
        num_rows=parquet_file.metadata.num_rows,
    )
