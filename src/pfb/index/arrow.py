# -*- coding: utf-8 -*-
"""Mapping PFB (Avro) node schemas onto Arrow schemas.

This is the fiddliest part of the design.  PFB node schemas are Avro with heavy
nullable-union and enum use, and the unions are not always two-branch: every
``state`` field in a current Gen3 dictionary is ``union<enum, enum, null>``.
"""

import pyarrow as pa

#: Column holding the Avro record id, so a Parquet hit maps back to a record.
ID_COLUMN = "__pfb_id"
#: Column holding ``[(dst_name, dst_id), ...]`` for edge traversal.
RELATIONS_COLUMN = "__pfb_relations"

_RELATION_TYPE = pa.list_(
    pa.struct([("dst_name", pa.string()), ("dst_id", pa.string())])
)

_PRIMITIVES = {
    "string": pa.string(),
    "long": pa.int64(),
    "int": pa.int32(),
    "double": pa.float64(),
    "float": pa.float32(),
    "boolean": pa.bool_(),
    "bytes": pa.binary(),
    "null": pa.null(),
}


class UnsupportedPFBType(TypeError):
    """Raised for an Avro type with no unambiguous Arrow equivalent."""


def _map_branch(type_):
    """Map one non-null union branch to an Arrow type."""
    if isinstance(type_, dict):
        kind = type_.get("type")
        if kind == "enum":
            # Enum symbols are plain strings once PFBReader has decoded them.
            return pa.string()
        if kind == "array":
            return pa.list_(_map_branch(type_["items"]))
        if kind == "map":
            return pa.map_(pa.string(), pa.string())
        if kind == "record":
            raise UnsupportedPFBType("nested record types are not supported")
        if kind in _PRIMITIVES:
            return _PRIMITIVES[kind]
        raise UnsupportedPFBType("unknown Avro type: {!r}".format(kind))
    if type_ in _PRIMITIVES:
        return _PRIMITIVES[type_]
    raise UnsupportedPFBType("unknown Avro type: {!r}".format(type_))


def avro_field_to_arrow(field):
    """Map a PFB node field type to an Arrow type.

    PFB specifics:
      - fields are typically ``union<null, X>`` -> nullable Arrow type
      - enums become string; values are decoded by ``PFBReader`` before staging
      - ``array<X>`` becomes ``list<X>``

    Multi-branch unions are **collapsed when every branch maps to the same Arrow
    type** and rejected otherwise.  Collapsing matters in practice: ``state``
    fields are ``union<enum, enum, null>``, and 36 of them occur in a single
    real export, so rejecting every multi-branch union outright would abort on
    the first node.  Taking ``non_null[0]`` instead would silently corrupt a
    genuinely heterogeneous union, hence the explicit equality check.
    """
    types = field["type"] if isinstance(field["type"], list) else [field["type"]]
    non_null = [t for t in types if t != "null"]
    if not non_null:
        return pa.null()

    mapped = [_map_branch(t) for t in non_null]
    first = mapped[0]
    for other in mapped[1:]:
        if other != first:
            raise UnsupportedPFBType(
                "{}: heterogeneous union {} maps to {}".format(
                    field["name"],
                    [_describe(t) for t in non_null],
                    sorted({str(m) for m in mapped}),
                )
            )
    return first


def _describe(type_):
    if isinstance(type_, dict):
        return type_.get("name") or type_.get("type")
    return type_


def node_to_arrow_schema(node):
    """Build the Arrow schema for one PFB node, including the carried columns."""
    fields = [pa.field(ID_COLUMN, pa.string())]
    for field in node["fields"]:
        fields.append(pa.field(field["name"], avro_field_to_arrow(field)))
    fields.append(pa.field(RELATIONS_COLUMN, _RELATION_TYPE))
    return pa.schema(fields)
