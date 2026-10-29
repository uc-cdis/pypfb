# -*- coding: utf-8 -*-

import base64
import json
import operator
import sys
import uuid
import re
import zlib
from collections import deque

if sys.version_info[0] == 3:
    PY3 = True
else:
    PY3 = False


# https://stackoverflow.com/a/42377964/1030110
# required to load JSON without 'unicode' keys and values in Python 2
#
# TODO: remove in Python 3
def str_hook(obj):
    if PY3:
        return {k: v for k, v in obj}
    return {
        k.encode("utf-8") if isinstance(k, unicode) else k: (
            v.encode("utf-8") if isinstance(v, unicode) else v
        )
        for k, v in obj
    }


def unicode_encode(matchobj):
    unicodeRep = str(hex(ord(matchobj.group(0))))[2:]
    return "_" + unicodeRep + "_"


def unicode_decode(matchobj):
    unicodeRep = matchobj.group(0)
    unicodeRep = unicodeRep.strip("_")

    if len(unicodeRep) == 2:
        unicodeRep = "\\u00" + unicodeRep
    elif len(unicodeRep) == 3:
        unicodeRep = "\\u0" + unicodeRep
    else:
        unicodeRep = "\\u" + unicodeRep

    unicodeRep = unicodeRep.encode().decode("unicode_escape")
    return unicodeRep


def encode_enum(enumValue):
    encodedValue = re.sub(
        "^[0-9]|[^A-Za-z0-9]", unicode_encode, str(enumValue), flags=re.UNICODE
    )
    return encodedValue


def decode_enum(enumValue):
    decodedValue = re.sub("_[a-f0-9]{2,}_", unicode_decode, enumValue, flags=re.UNICODE)
    return decodedValue


def is_enum(data_type):
    data_type = [data_type]
    while data_type:
        type_ = data_type.pop()
        if isinstance(type_, list):
            data_type.extend(type_)
        elif isinstance(type_, dict) and type_["type"] == "enum":
            return True
        elif (
            isinstance(type_, dict)
            and type_["type"] == "array"
            and isinstance(type_["items"], dict)
            and type_["items"]["type"] == "enum"
        ):
            return True
    return False


def handle_schema_field_unicode(field, encode=True):
    method = encode_enum if encode else decode_enum
    is_enum_ = False
    stack = deque([field["type"]])
    while stack:
        t = stack.pop()
        if isinstance(t, list):
            stack.extend(t)
        elif isinstance(t, dict) and t["type"] == "enum":
            is_enum_ = True
            symbols = []
            for symbol in t["symbols"]:
                if symbol == None:
                    continue
                symbols.append(method(symbol))
            t["symbols"] = symbols
        elif isinstance(t, dict) and t["type"] == "array":
            if isinstance(t["items"], dict) and t["items"]["type"] == "enum":
                is_enum = True
                symbols = []
                for symbol in t["items"]["symbols"]:
                    if symbol == None:
                        continue
                    symbols.append(method(symbol))
                if "name" in t["items"]:
                    t["items"]["name"] = method(t["items"]["name"])
                t["items"]["symbols"] = symbols
    default = field.get("default")
    if is_enum_ and default:
        field["default"] = method(default)


# --------------------------------------------------------------------------
# Schema introspection
# --------------------------------------------------------------------------

#: Avro primitives that support ordered range comparison.
NUMERIC_TYPES = frozenset(["int", "long", "float", "double"])

#: The record in the ``object`` union that is not a data node.
METADATA_RECORD = "Metadata"


def node_schemas(writer_schema):
    """Yield the per-node record schemas from a PFB writer schema.

    Unlike ``PFBReader.__enter__``, which skips exactly one union member and so
    mistakes any other non-node member for a node, this filters by name.  Any
    member that is not a data node is excluded explicitly.
    """
    for field in writer_schema.get("fields", []):
        if field["name"] != "object":
            continue
        for member in field["type"]:
            if not isinstance(member, dict) or member.get("type") != "record":
                continue
            if member["name"] == METADATA_RECORD:
                continue
            yield member


def _walk_types(type_):
    stack = deque([type_])
    while stack:
        t = stack.pop()
        if isinstance(t, list):
            stack.extend(t)
        else:
            yield t
            if isinstance(t, dict) and t.get("type") == "array":
                stack.append(t["items"])


def field_traits(writer_schema):
    """Map ``node -> field -> {"enum", "numeric", "array"}`` flags.

    ``enum`` drives value decoding, ``numeric`` selects the range strategy, and
    ``array`` tells the builder to index each element rather than the list.
    """
    traits = {}
    for node in node_schemas(writer_schema):
        per_field = traits.setdefault(node["name"], {})
        for field in node.get("fields", []):
            is_enum_field = False
            is_numeric = False
            is_array = False
            for t in _walk_types(field["type"]):
                if isinstance(t, dict):
                    if t.get("type") == "enum":
                        is_enum_field = True
                    elif t.get("type") == "array":
                        is_array = True
                elif t in NUMERIC_TYPES:
                    is_numeric = True
            per_field[field["name"]] = {
                "enum": is_enum_field,
                # An array of numbers is indexed by element, not by range.
                "numeric": is_numeric and not is_array,
                "array": is_array,
            }
    return traits


def decode_value(value, is_enum_field):
    """Return the value as a consumer of ``PFBReader`` sees it.

    Enum symbols are escaped on the wire (:func:`encode_enum`), so an index
    built from raw block data would otherwise key on ``_32_`` forms that no
    caller would ever type as a predicate.
    """
    if not is_enum_field or value is None:
        return value
    if isinstance(value, list):
        return [decode_enum(v) if isinstance(v, str) else v for v in value]
    if isinstance(value, str):
        return decode_enum(value)
    return value


def avro_record(node_id, node_name, values, relations):
    node = {"id": node_id, "name": node_name, "object": values, "relations": relations}
    return node


class PFBBase(object):
    open_mode = "r"

    def __init__(self, file_or_path):
        self._file_or_path = file_or_path
        self._is_encode = None
        self._schema = None
        self._metadata = None
        self._encoded_schema = None
        self._block_index = None

    def __enter__(self):
        self._file_obj = (
            self._file_or_path
            if hasattr(self._file_or_path, "read")
            else open(self._file_or_path, self.open_mode)
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_obj.close()

    @property
    def name(self):
        return self._file_obj.name

    @property
    def isatty(self):
        return self._file_obj.isatty()

    @property
    def schema(self):
        return self._schema

    @property
    def metadata(self):
        return self._metadata

    @property
    def block_index(self):
        """The parsed block-index manifest, or ``None`` if the file has none."""
        return self._block_index

    def set_schema(self, schema):
        self._schema = schema

    def set_encoded_schema(self, encoded_schema):
        self._encoded_schema = encoded_schema

    def set_metadata(self, metadata):
        self._metadata = metadata

    def set_block_index(self, block_index):
        self._block_index = block_index

    def prepare_encode_cache(self):
        if self._is_encode is None:
            self._is_encode = {}
            for node in self._schema:
                self._is_encode[node["name"]] = fields = {}
                for field in node["fields"]:
                    fields[field["name"]] = is_enum(field["type"])

    def is_encode(self, node_name, field_name):
        self.prepare_encode_cache()
        return self._is_encode[node_name].get(field_name, False)

    def make_empty_record(self, node_name):
        values = {}
        for node in self.schema:
            if node["name"] == node_name:
                for field in node["fields"]:
                    val = field.get("default", "")
                    stack = deque([field["type"]])
                    while stack:
                        t = stack.pop()
                        if isinstance(t, list):
                            stack.extend(t)
                        elif t in ["long", "double"]:
                            val = 0
                            break
                        elif t in ["string"]:
                            val = ""
                            break
                        elif isinstance(t, dict) and t["type"] == "enum":
                            val = t["symbols"][0]
                            break

                    values[field["name"]] = val

        return avro_record(str(uuid.uuid4()), node_name, values, [])


# --------------------------------------------------------------------------
# Block index: manifest format
# --------------------------------------------------------------------------
#
# The manifest is JSON stored in Avro file-level header metadata.  Header
# values must be ``str`` (fastavro rejects ``bytes``), so when the JSON is
# large it is deflated and base64-encoded into a second, separate key.
# Exactly one of the two keys is ever written.

#: Header key holding the manifest as plain JSON text.
MANIFEST_KEY = "pfb.blockindex.v1"
#: Header key holding the manifest as base64(deflate(JSON)).
MANIFEST_KEY_COMPRESSED = "pfb.blockindex.v1.z"

#: Above this many bytes of JSON, the manifest is stored compressed instead.
MANIFEST_COMPRESS_THRESHOLD = 16 * 1024

#: Manifest schema version.  A reader meeting a higher version ignores the
#: index rather than guessing at it.
INDEX_VERSION = 1

# Per-field index strategies.  Stored under the short key "s" because the tag
# repeats once per indexed field.
STRATEGY_POSTINGS = "postings"  # value -> block ordinals
STRATEGY_RANGE = "range"  # block ordinal -> [min, max]
STRATEGY_NONE = "none"  # not indexed; queries fall back to the node's blocks


def dump_manifest(manifest):
    """Serialize a manifest dict into ``{header_key: str}`` for fastavro.

    Returns a single-entry dict so the caller can splat it into the writer's
    ``metadata=`` argument alongside anything else it wants to store.
    """
    text = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
    if len(text) <= MANIFEST_COMPRESS_THRESHOLD:
        return {MANIFEST_KEY: text}
    packed = base64.b64encode(zlib.compress(text.encode("utf-8"), 9))
    return {MANIFEST_KEY_COMPRESSED: packed.decode("ascii")}


def load_manifest(header_metadata):
    """Pull a manifest out of ``fastavro`` header metadata, or return ``None``.

    Accepts either storage form.  A missing, corrupt, or future-versioned
    manifest yields ``None`` rather than an exception, so a reader built
    against v1 degrades to a full scan instead of refusing to open the file.
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


class BlockIndex(object):
    """Read-side view over a manifest.

    Resolves a field to the set of block ordinals that may contain matching
    records.  Every method is deliberately *conservative*: when the index
    cannot answer precisely it returns a superset (usually the node's whole
    block set), never a subset.  Callers re-check each decoded record against
    the predicate, so a superset costs time and a subset would cost
    correctness.
    """

    def __init__(self, manifest):
        self._m = manifest
        self._nodes = manifest.get("nodes", {})
        self._fields = manifest.get("fields", {})

    @property
    def manifest(self):
        return self._m

    @property
    def num_blocks(self):
        return self._m.get("num_blocks", 0)

    @property
    def num_records(self):
        return self._m.get("num_records", 0)

    @property
    def nodes(self):
        return sorted(self._nodes)

    def node_blocks(self, node):
        """Block ordinals containing at least one record of ``node``."""
        return set(self._nodes.get(node, ()))

    def field_strategy(self, node, field):
        entry = self._fields.get("{}.{}".format(node, field))
        return entry["s"] if entry else STRATEGY_NONE

    def indexed_fields(self, node=None):
        prefix = None if node is None else node + "."
        out = {}
        for key, entry in self._fields.items():
            if prefix and not key.startswith(prefix):
                continue
            out[key] = entry["s"]
        return out

    def candidate_blocks(self, predicate):
        """Blocks that may contain a record satisfying ``predicate``.

        Falls back to the node's full block set whenever the field carries no
        usable index, which still prunes every block belonging to other nodes.
        """
        node_blocks = self.node_blocks(predicate.node)
        entry = self._fields.get("{}.{}".format(predicate.node, predicate.field))
        if entry is None:
            return node_blocks
        if entry["s"] == STRATEGY_POSTINGS:
            return node_blocks & self._postings_blocks(entry, predicate)
        if entry["s"] == STRATEGY_RANGE:
            return node_blocks & self._range_blocks(entry, predicate)
        return node_blocks

    @staticmethod
    def _postings_blocks(entry, predicate):
        values = entry["v"]
        if predicate.op == "=":
            # A value absent from the postings appears in no block at all, so
            # an unknown value legitimately prunes everything.
            return set(values.get(predicate.as_text(), ()))
        out = set()
        for raw, blocks in values.items():
            if predicate.matches_value(raw):
                out.update(blocks)
        return out

    @staticmethod
    def _range_blocks(entry, predicate):
        out = set()
        for block, bounds in entry["r"].items():
            low, high = bounds
            if predicate.overlaps_range(low, high):
                out.add(int(block))
        return out


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

    def _compare_eq(self, value):
        return str(value) == str(self.value)

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
