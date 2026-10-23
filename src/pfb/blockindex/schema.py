# -*- coding: utf-8 -*-
"""Schema helpers shared by the index builder and the query path.

The builder reads records straight out of Avro blocks rather than through
:class:`~pfb.reader.PFBReader`, so it has to do for itself the two things the
reader normally does: know which fields are enums (their values are escaped in
the stream) and know which are numeric (they get range bounds instead of
postings).
"""

from collections import deque

from ..base import decode_enum

#: Avro primitives that support ordered range comparison.
NUMERIC_TYPES = frozenset(["int", "long", "float", "double"])

#: Name of the record in the ``object`` union that is not a data node.
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
            is_enum = False
            is_numeric = False
            is_array = False
            for t in _walk_types(field["type"]):
                if isinstance(t, dict):
                    if t.get("type") == "enum":
                        is_enum = True
                    elif t.get("type") == "array":
                        is_array = True
                elif t in NUMERIC_TYPES:
                    is_numeric = True
            per_field[field["name"]] = {
                "enum": is_enum,
                # An array of numbers is indexed by element, not by range.
                "numeric": is_numeric and not is_array,
                "array": is_array,
            }
    return traits


def decode_value(value, is_enum):
    """Return the value as a consumer of :class:`~pfb.reader.PFBReader` sees it.

    Enum symbols are escaped on the wire (``base.encode_enum``), so an index
    built from raw block data would otherwise key on ``_32_`` forms that no
    caller would ever type as a predicate.
    """
    if not is_enum or value is None:
        return value
    if isinstance(value, list):
        return [decode_enum(v) if isinstance(v, str) else v for v in value]
    if isinstance(value, str):
        return decode_enum(value)
    return value
