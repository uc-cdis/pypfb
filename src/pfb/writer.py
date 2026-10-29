import io
from collections import defaultdict
from copy import deepcopy

import fastavro
from fastavro import writer
from fastavro.write import Writer

from .base import (
    STRATEGY_NONE,
    STRATEGY_POSTINGS,
    STRATEGY_RANGE,
    INDEX_VERSION,
    PFBBase,
    avro_record,
    decode_value,
    dump_manifest,
    encode_enum,
    field_traits,
    handle_schema_field_unicode,
)


# def add(pfbFile, parField, newField, newFieldType, newFieldDefault):
#     pfb = open(pfbFile, "rb")
#     avro_reader = reader(pfb)
#
#     schema = avro_reader.schema
#
#     newFieldDict = {
#         u"default": u"" + newFieldDefault,
#         u"type": u"" + newFieldType,
#         u"name": u"" + newField,
#     }
#
#     print(
#         "updating records from PFB by addding "
#         + newField
#         + " with default value of "
#         + newFieldDefault
#     )
#     records = []
#     for record in avro_reader:
#         if record["name"] == parField:
#             record["val"][u"" + newField] = u"" + newFieldDefault
#         records.append(record)
#     print("records updated with new field \n")
#
#     print("updating schema with " + newField)
#     x = 0
#     schemaParents = len(schema["fields"][2]["type"])
#     while x < schemaParents:
#         if schema["fields"][2]["type"][x]["name"] == parField:
#             schema["fields"][2]["type"][x]["fields"].append(newFieldDict)
#             break
#         x += 1
#     print("schema updated with new field")
#
#     with open("new.pfb", "wb+") as out:
#         writer(out, schema, records)
#
#
# def remove(pfbFile, parField, rmField):
#     pfb = open(pfbFile, "rb")
#     avro_reader = reader(pfb)
#
#     schema = avro_reader.schema
#
#     print("updating records from PFB file by removing " + rmField)
#     records = []
#     for record in avro_reader:
#         if record["name"] == parField:
#             del record["val"][rmField]
#         records.append(record)
#
#     x = 0
#     schemaParents = len(schema["fields"][2]["type"])
#     while x < schemaParents:
#         if schema["fields"][2]["type"][x]["name"] == parField:
#             for y in schema["fields"][2]["type"][x]["fields"]:
#                 if y["name"] == rmField:
#                     print("removing " + rmField + " from schema")
#                     schema["fields"][2]["type"][x]["fields"].remove(y)
#                     break
#             break
#         x += 1
#
#     print("writing to new file rm.pfb")
#     with open("rm.pfb", "wb+") as out:
#         writer(out, schema, records)


def make_avro_schema(schema):
    encoded_schema = []
    for node in schema:
        node = deepcopy(node)
        encoded_schema.append(node)
        for field in node["fields"]:
            handle_schema_field_unicode(field, encode=True)

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
                        "fields": [
                            {
                                "name": "nodes",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "Node",
                                        "fields": [
                                            {"name": "name", "type": "string"},
                                            {
                                                "name": "ontology_reference",
                                                "type": "string",
                                            },
                                            {
                                                "name": "values",
                                                "type": {
                                                    "type": "map",
                                                    "values": "string",
                                                },
                                            },
                                            {
                                                "name": "links",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Link",
                                                        "fields": [
                                                            {
                                                                "name": "multiplicity",
                                                                "type": {
                                                                    "type": "enum",
                                                                    "name": "Multiplicity",
                                                                    "symbols": [
                                                                        "ONE_TO_ONE",
                                                                        "ONE_TO_MANY",
                                                                        "MANY_TO_ONE",
                                                                        "MANY_TO_MANY",
                                                                    ],
                                                                },
                                                            },
                                                            {
                                                                "name": "dst",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                "name": "properties",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Property",
                                                        "fields": [
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "ontology_reference",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "values",
                                                                "type": {
                                                                    "type": "map",
                                                                    "values": "string",
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                "name": "misc",
                                "type": {"type": "map", "values": "string"},
                            },
                        ],
                    }
                ]
                + encoded_schema,
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


class PFBWriter(PFBBase):
    open_mode = "wb"

    def __init__(self, file_or_path):
        super(PFBWriter, self).__init__(file_or_path)
        self._hooks = []

    def copy_schema(self, reader):
        self.set_schema(deepcopy(reader.schema))
        self.set_metadata(reader.metadata)

    def write(self, iterable=None, metadata=True):
        def _iter():
            if metadata:
                yield avro_record(None, "Metadata", self._metadata, [])
            if iterable is not None:
                for record in iterable:
                    obj = record["object"]
                    name = record["name"]
                    record["object"] = (name, obj)
                    for hook in self._hooks:
                        record = hook(record)

                    to_update = {}
                    for field, value in list(obj.items()):
                        if value is not None and self.is_encode(name, field):
                            if isinstance(value, list):
                                # this is needed to not encode list brackets
                                obj[field] = []
                                for element in value:
                                    obj[field].append(encode_enum(element))
                            else:
                                obj[field] = encode_enum(value)
                    obj.update(to_update)
                    yield record

        writer(self._file_obj, make_avro_schema(self.schema), _iter())

    def rename_node(self, name_from, name_to):
        if type(name_from) == bytes:
            name_from = name_from.decode()
        if type(name_to) == bytes:
            name_to = name_to.decode()
        for node in self.schema:
            if node["name"] == name_from:
                node["aliases"] = node.get("aliases", []) + [name_from]
                node["name"] = name_to
                for fields in node["fields"]:
                    for type_ in fields["type"]:
                        if isinstance(type_, dict) and type_.get("type") == "enum":
                            type_["name"] = type_["name"].replace(name_from, name_to)

        def _rename_node(record):
            if record["name"] == name_from:
                record["name"] = name_to
                record["object"] = (name_to, record["object"][1])
            return record

        self._hooks.append(_rename_node)

    def rename_enum(self, field_name, val_from, val_to):
        if type(val_from) == bytes:
            val_from = val_from.decode()
        if type(val_to) == bytes:
            val_to = val_to.decode()
        renamed = set()
        for node in self.schema:
            for field in node["fields"]:
                if isinstance(field, dict) and field["name"] == field_name:
                    for element in field.get("type", []):
                        if isinstance(element, dict) and element.get("type") == "enum":
                            try:
                                idx = element["symbols"].index(val_from)
                            except ValueError:
                                pass
                            else:
                                element["symbols"][idx] = val_to
                                renamed.add(node["name"])
                    if node["name"] in renamed and field["default"] == val_from:
                        field["default"] = val_to

        def _rename_enum(record):
            obj = record["object"][1]
            if obj.get(field_name) == val_from:
                obj[field_name] = val_to
            return record

        self._hooks.append(_rename_enum)


# --------------------------------------------------------------------------
# Block index: building
# --------------------------------------------------------------------------

#: Fields with more distinct values than this get range bounds (numeric) or no
#: index at all (everything else), so the manifest stays bounded by field count
#: rather than by record count.
DEFAULT_DOMAIN_CAP = 256


class BuildResult(object):
    """A built index plus everything needed to re-emit the file around it."""

    __slots__ = ("manifest", "blocks", "writer_schema", "codec")

    def __init__(self, manifest, blocks, writer_schema, codec):
        self.manifest = manifest
        self.blocks = blocks
        self.writer_schema = writer_schema
        self.codec = codec


class _FieldAccumulator(object):
    """Collects what one field needs for whichever strategy it ends up using.

    Both strategies are accumulated at once because the choice between them
    depends on the distinct count, which is only known after the whole file has
    been read.  Postings collection stops once the cap is exceeded so a
    high-cardinality field cannot blow up memory.
    """

    def __init__(self, traits, domain_cap):
        self.numeric = traits.get("numeric", False)
        self.array = traits.get("array", False)
        self.enum = traits.get("enum", False)
        self._cap = domain_cap
        self.postings = defaultdict(set)
        self.over_cap = False
        self.bounds = {}
        self.present = False

    def observe(self, value, block):
        if value is None:
            return
        self.present = True
        values = value if isinstance(value, list) else [value]
        for item in values:
            if item is None:
                continue
            if self.numeric:
                self._observe_bounds(item, block)
            if self.over_cap:
                continue
            key = str(item)
            self.postings[key].add(block)
            if len(self.postings) > self._cap:
                # Release the partial map; it is unusable and potentially large.
                self.over_cap = True
                self.postings = defaultdict(set)

    def _observe_bounds(self, item, block):
        current = self.bounds.get(block)
        if current is None:
            self.bounds[block] = [item, item]
        else:
            if item < current[0]:
                current[0] = item
            if item > current[1]:
                current[1] = item

    def entry(self):
        """The manifest entry for this field, or ``None`` to omit it."""
        if not self.present:
            return None
        if not self.over_cap:
            return {
                "s": STRATEGY_POSTINGS,
                "v": {value: sorted(blocks) for value, blocks in self.postings.items()},
            }
        if self.numeric and self.bounds:
            return {
                "s": STRATEGY_RANGE,
                "r": {str(block): bounds for block, bounds in self.bounds.items()},
            }
        # Over cap and not orderable: the node-level block list is all we have.
        return {"s": STRATEGY_NONE}


def build_index(file_obj, domain_cap=DEFAULT_DOMAIN_CAP, fields=None):
    """Scan an Avro file and return a :class:`BuildResult`.

    The result carries the raw block bytes alongside the manifest so that
    :func:`write_indexed` can copy them into the output verbatim instead of
    re-encoding every record.

    ``fields`` optionally restricts indexing to an iterable of ``"node.field"``
    names; the node-level block map is always built because it is what prunes
    whole nodes and costs almost nothing.
    """
    file_obj.seek(0)
    block_reader = fastavro.block_reader(file_obj)
    writer_schema = block_reader.writer_schema
    traits = field_traits(writer_schema)
    selected = None if fields is None else set(fields)

    node_blocks = defaultdict(set)
    accumulators = {}
    blocks = []
    num_records = 0

    for ordinal, block in enumerate(block_reader):
        raw = block.bytes_.getvalue()
        count = block.num_records
        for record in block:
            node = record["name"]
            num_records += 1
            node_blocks[node].add(ordinal)
            node_traits = traits.get(node)
            if node_traits is None:
                # Metadata, or a union member this build does not understand.
                continue
            for name, value in record["object"].items():
                key = "{}.{}".format(node, name)
                if selected is not None and key not in selected:
                    continue
                field_trait = node_traits.get(name)
                if field_trait is None:
                    continue
                accumulator = accumulators.get(key)
                if accumulator is None:
                    accumulator = accumulators[key] = _FieldAccumulator(
                        field_trait, domain_cap
                    )
                accumulator.observe(decode_value(value, field_trait["enum"]), ordinal)
        blocks.append((count, raw))

    manifest = {
        "version": INDEX_VERSION,
        "num_blocks": len(blocks),
        "num_records": num_records,
        "domain_cap": domain_cap,
        "nodes": {node: sorted(ords) for node, ords in node_blocks.items()},
        "fields": {},
    }
    for key, accumulator in accumulators.items():
        entry = accumulator.entry()
        if entry is not None:
            manifest["fields"][key] = entry
    return BuildResult(
        manifest=manifest,
        blocks=blocks,
        writer_schema=writer_schema,
        codec=block_reader.codec,
    )


class _RawBlock(object):
    """Adapter presenting pre-encoded block bytes to ``Writer.write_block``.

    ``write_block`` only reads ``num_records`` and ``bytes_``; it never iterates
    the records, so the encoded payload passes through untouched and the block
    boundaries -- and therefore the ordinals in the manifest -- survive exactly.
    """

    __slots__ = ("num_records", "bytes_")

    def __init__(self, num_records, raw):
        self.num_records = num_records
        self.bytes_ = io.BytesIO(raw)


def write_indexed(out_file, build, extra_metadata=None):
    """Write a :class:`BuildResult` out as an indexed PFB.

    The blocks are copied byte for byte.  That is what makes the two-pass build
    cheap, and it is also what keeps the index valid: re-encoding could change
    where block boundaries fall and silently invalidate every ordinal.

    The input codec is carried over unchanged.  Block payloads are stored
    already-compressed, so writing them under a different codec would produce a
    file whose declared codec does not match its contents.
    """
    metadata = dict(extra_metadata or {})
    metadata.update(dump_manifest(build.manifest))
    out = Writer(out_file, build.writer_schema, codec=build.codec, metadata=metadata)
    for count, raw in build.blocks:
        out.write_block(_RawBlock(count, raw))
    out.flush()
    return out


def rechunk(file_obj, sync_interval):
    """Re-encode a file's records into blocks of ``sync_interval`` bytes.

    Smaller blocks prune better but cost more to walk, so this is the knob that
    trades index granularity against per-query overhead.  Unlike
    :func:`write_indexed` this re-encodes, so it must run *before* the index is
    built.
    """
    file_obj.seek(0)
    source = fastavro.reader(file_obj)
    writer_schema = source.writer_schema
    records = list(source)
    buffer = io.BytesIO()
    writer(
        buffer,
        writer_schema,
        records,
        codec=source.codec,
        sync_interval=sync_interval,
    )
    buffer.seek(0)
    return buffer
