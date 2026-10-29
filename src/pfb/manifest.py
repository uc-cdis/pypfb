# -*- coding: utf-8 -*-
"""The index manifest: its on-disk form, and what can be concluded from it.

Everything here is stdlib-only and deliberately so.  The manifest is the part of
the index that both the read and the write path need, and that a consumer can
use with nothing but ``fastavro`` -- no pyarrow, no Parquet reader.  Anything
requiring pyarrow lives in :mod:`pfb.parquet` instead.
"""

import base64
import json
import operator
import re
import zlib

# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

#: Header key holding the manifest as plain JSON text.
MANIFEST_KEY = "pfb.index.v1.manifest"
#: Header key holding the manifest as base64(deflate(JSON)).
MANIFEST_KEY_COMPRESSED = "pfb.index.v1.manifest.z"

#: Above this many bytes of JSON, the manifest is stored compressed instead.
MANIFEST_COMPRESS_THRESHOLD = 16 * 1024

#: Manifest schema version.  A reader meeting a higher version ignores the
#: index rather than guessing at it.
INDEX_VERSION = 1

# --------------------------------------------------------------------------
# The embedded-payload union member
# --------------------------------------------------------------------------
#
# Defined here so that `reader.py` can take the name and `writer.py` the schema
# without either importing the other.

#: Name of the ``object`` union member carrying one node's Parquet payload.
NODE_PARQUET = "NodeParquet"

#: Avro schema of that union member.
NODE_PARQUET_SCHEMA = {
    "type": "record",
    "name": NODE_PARQUET,
    "fields": [
        {"name": "node", "type": "string"},
        {"name": "payload", "type": "bytes"},
        {"name": "num_rows", "type": "long"},
        {"name": "compression", "type": "string"},
        {"name": "page_index", "type": "boolean"},
        {"name": "bloom_columns", "type": {"type": "array", "items": "string"}},
    ],
}

# --------------------------------------------------------------------------
# Per-field index strategies
# --------------------------------------------------------------------------

STRATEGY_DOMAIN = "domain"  # full sorted value list in the manifest
STRATEGY_BLOOM = "bloom"  # bloom filter, inside the Parquet payload
STRATEGY_RANGE = "range"  # min/max only
STRATEGY_NONE = "none"  # nothing stored


def dump_manifest(manifest):
    """Serialize a manifest into ``{header_key: str}`` for fastavro.

    fastavro requires header metadata values to be ``str``; the manifest is
    JSON text, so no base64 is needed unless it is large enough to be worth
    deflating, in which case it moves to a separate key.  Exactly one of the
    two keys is ever written.
    """
    text = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
    if len(text) <= MANIFEST_COMPRESS_THRESHOLD:
        return {MANIFEST_KEY: text}
    packed = base64.b64encode(zlib.compress(text.encode("utf-8"), 9))
    return {MANIFEST_KEY_COMPRESSED: packed.decode("ascii")}


def load_manifest(header_metadata):
    """Pull a manifest out of fastavro header metadata, or return ``None``.

    A missing, unreadable, or future-versioned manifest yields ``None`` rather
    than an exception, so a reader built against v1 degrades to a full scan
    instead of refusing to open the file.
    """
    if not header_metadata:
        return None
    raw = header_metadata.get(MANIFEST_KEY)
    if raw is None:
        packed = header_metadata.get(MANIFEST_KEY_COMPRESSED)
        if packed is None:
            return None
        raw = zlib.decompress(base64.b64decode(packed)).decode("utf-8")
    try:
        manifest = json.loads(raw)
    except ValueError:
        return None
    if manifest.get("version") != INDEX_VERSION:
        return None
    return manifest


def build_manifest(nodes, embedded, payload_blocks=None):
    """Assemble the manifest.

    ``payload_blocks`` maps a node to the ordinal of the Avro block holding its
    ``NodeParquet`` record.  It is what lets a reader fetch one payload without
    iterating the record stream; see :meth:`pfb.reader.PFBReader.parquet`.
    """
    return {
        "version": INDEX_VERSION,
        "embedded": bool(embedded),
        "nodes": nodes,
        "payload_blocks": payload_blocks or {},
    }


# --------------------------------------------------------------------------
# Predicates
# --------------------------------------------------------------------------

_COMPARE = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

#: Longest operators first so ``>=`` is not parsed as ``>``.
_PREDICATE_RE = re.compile(
    r"^(?P<node>[^.]+)\.(?P<field>.+?)(?P<op>!=|>=|<=|=|>|<)(?P<value>.*)$"
)


class PredicateError(ValueError):
    """Raised for a predicate that cannot be parsed or applied."""


class Predicate(object):
    """A single ``node.field OP value`` test.

    A plain value object: it holds the parsed parts and compares Python values,
    and knows nothing about Arrow.  Turning one into a pushdown expression is
    :func:`pfb.parquet.predicate_to_expression`, which keeps pyarrow out of a
    type that otherwise needs none.
    """

    __slots__ = ("node", "field", "op", "value")

    def __init__(self, node, field, op, value):
        if op not in _COMPARE:
            raise PredicateError("unsupported operator: {}".format(op))
        self.node = node
        self.field = field
        self.op = op
        self.value = value

    def __repr__(self):
        return "<Predicate {}.{}{}{}>".format(
            self.node, self.field, self.op, self.value
        )

    def matches(self, left):
        """Compare one Python value against this predicate.

        Numeric when both sides parse as numbers and lexical otherwise, so
        ``age>9`` does not match ``10`` as a string.
        """
        if left is None:
            return self.op == "!="
        left_number, right_number = _as_number(left), _as_number(self.value)
        if left_number is not None and right_number is not None:
            return _COMPARE[self.op](left_number, right_number)
        return _COMPARE[self.op](str(left), str(self.value))

    def matches_record(self, obj):
        """Test a decoded record ``object`` mapping."""
        if self.field not in obj:
            return self.op == "!="
        value = obj[self.field]
        if isinstance(value, list):
            # "=" on a list is a membership test; "!=" means "does not contain".
            if self.op == "!=":
                return not any(str(v) == str(self.value) for v in value)
            return any(self.matches(v) for v in value)
        return self.matches(value)


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


# --------------------------------------------------------------------------
# What the manifest alone can answer
# --------------------------------------------------------------------------


def answerable_from_manifest(manifest, predicate):
    """Can the manifest alone prove this predicate matches nothing?

    Returns ``False`` when no record can match and ``None`` when the manifest
    cannot tell, which is the common case.  It never returns ``True``: a value
    being in the domain says only that *some* record has it, not which.

    This is the tier-2 index doing useful work without reading a payload or a
    single record.
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
    if strategy == STRATEGY_DOMAIN:
        domain = {str(v) for v in summary.get("domain", [])}
        return None if predicate.value in domain else False
    if strategy == STRATEGY_RANGE:
        low, high = summary.get("min"), summary.get("max")
        if low is None or high is None:
            return None
        try:
            target, low, high = float(predicate.value), float(low), float(high)
        except (TypeError, ValueError):
            return None
        return None if low <= target <= high else False
    return None
