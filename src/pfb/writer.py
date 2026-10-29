import io
from copy import deepcopy

import fastavro
from fastavro import writer
from fastavro.write import Writer

from .base import PFBBase, encode_enum, avro_record, handle_schema_field_unicode
from .manifest import NODE_PARQUET, NODE_PARQUET_SCHEMA, dump_manifest


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


def make_avro_schema(schema, with_parquet=False):
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
                + ([NODE_PARQUET_SCHEMA] if with_parquet else [])
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
        self._parquet_payloads = None
        self._parquet_meta = {}

    def set_parquet_payloads(self, payloads, payload_meta=None):
        """Attach ``{node: parquet_bytes}`` to embed in the record stream.

        ``payload_meta`` is ``{node: {num_rows, compression, page_index,
        bloom_columns}}``.  Plain data rather than a stager object, so the
        writer never reaches into the pyarrow layer and never has to guess at
        values the caller actually knows.
        """
        self._parquet_payloads = payloads
        self._parquet_meta = payload_meta or {}

    def copy_schema(self, reader):
        self.set_schema(deepcopy(reader.schema))
        self.set_metadata(reader.metadata)

    def write(self, iterable=None, metadata=True):
        """Write the PFB to the handle opened by the context manager."""
        self.write_to(self._file_obj, iterable, metadata=metadata)

    def write_to(self, file_obj, iterable=None, metadata=True):
        """Write the PFB to an explicitly supplied file object.

        :func:`write_indexed` needs this: its first pass writes to a staging
        buffer that it then has to read back, so it cannot let the context
        manager close the handle.
        """

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

                    for field, value in list(obj.items()):
                        if value is not None and self.is_encode(name, field):
                            if isinstance(value, list):
                                # this is needed to not encode list brackets
                                obj[field] = []
                                for element in value:
                                    obj[field].append(encode_enum(element))
                            else:
                                obj[field] = encode_enum(value)
                    yield record
            # Payloads last: a consumer that iterates to completion produces
            # all of its output before reaching them.  write_indexed() below
            # explains how a reader still fetches one without a full scan.
            for node, payload in (self._parquet_payloads or {}).items():
                meta = self._parquet_meta.get(node, {})
                yield avro_record(
                    None,
                    NODE_PARQUET,
                    {
                        "node": node,
                        "payload": payload,
                        "num_rows": meta.get("num_rows", 0),
                        "compression": meta.get("compression", "zstd"),
                        "page_index": meta.get("page_index", True),
                        "bloom_columns": meta.get("bloom_columns", []),
                    },
                    [],
                )

        header_metadata = None
        if self._index_manifest is not None:
            # Header metadata values must be str; the manifest is JSON text.
            header_metadata = dump_manifest(self._index_manifest)

        writer(
            file_obj,
            make_avro_schema(self.schema, with_parquet=bool(self._parquet_payloads)),
            _iter(),
            metadata=header_metadata,
        )

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
# Writing an indexed PFB
# --------------------------------------------------------------------------
#
# The design streams one `NodeParquet` entity per node in the Avro record
# stream, which makes payloads lazy and skippable in a way header metadata
# could not be.  That creates a placement problem, and this is where it is
# solved.
#
# Payloads are emitted *last*, so a consumer that iterates to completion
# produces all of its output before it reaches them.  But "last" also means a
# reader wanting a payload would have to decode every data record first --
# exactly the full scan the index exists to avoid, which on a real export made
# indexed retrieval (514 ms) slower than the scan it replaced (504 ms).
#
# The fix is to record, for each node, the ordinal of the Avro *block* its
# payload lands in.  `fastavro.block_reader` skips a block by its length prefix
# without decoding it, so fetching one payload walks block headers (a few ms)
# and decodes exactly one block.  Ordinals rather than byte offsets avoid a
# circularity: the header is written before the data, so it cannot contain
# offsets that are only known afterwards.
#
# Knowing the ordinals requires having written the blocks, so the file is built
# in two passes.  The second copies every block byte for byte -- nothing is
# re-encoded, which is both what makes it cheap and what keeps the ordinals
# valid, since re-encoding could move block boundaries.


class _RawBlock(object):
    """Adapter presenting pre-encoded block bytes to ``Writer.write_block``."""

    __slots__ = ("num_records", "bytes_")

    def __init__(self, num_records, raw):
        self.num_records = num_records
        self.bytes_ = io.BytesIO(raw)


def locate_payload_blocks(file_obj):
    """Return ``(payload_blocks, blocks, writer_schema, codec)`` for a staged PFB.

    One pass.  Every block must be read to find the payload records, but the
    blocks come back as raw bytes so the caller can re-emit them without
    re-encoding.
    """
    file_obj.seek(0)
    block_reader = fastavro.block_reader(file_obj)
    blocks = []
    payload_blocks = {}
    for ordinal, block in enumerate(block_reader):
        raw = block.bytes_.getvalue()
        count = block.num_records
        for record in block:
            if record["name"] == NODE_PARQUET:
                payload_blocks[record["object"]["node"]] = ordinal
        blocks.append((count, raw))
    return payload_blocks, blocks, block_reader.writer_schema, block_reader.codec


def rewrite_with_manifest(out_file, writer_schema, codec, blocks, metadata):
    """Copy ``blocks`` verbatim under a header carrying ``metadata``."""
    out = Writer(out_file, writer_schema, codec=codec, metadata=metadata)
    for count, raw in blocks:
        out.write_block(_RawBlock(count, raw))
    out.flush()
    return out


def write_indexed(
    out_file, schema, metadata, records, manifest, payloads=None, payload_meta=None
):
    """Write an indexed PFB and return the manifest as actually stored.

    Without ``payloads`` this is tiers 0-2: the manifest goes in the Avro header
    and the file is otherwise unchanged, so no consumer can tell the difference.

    With ``payloads`` it is tier 3, written in the two passes described above.
    """
    if not payloads:
        _write_pfb(out_file, schema, metadata, records, manifest)
        return manifest

    staging = io.BytesIO()
    _write_pfb(staging, schema, metadata, records, manifest, payloads, payload_meta)

    payload_blocks, blocks, writer_schema, codec = locate_payload_blocks(staging)
    stored = dict(manifest)
    stored["embedded"] = True
    stored["payload_blocks"] = payload_blocks
    rewrite_with_manifest(out_file, writer_schema, codec, blocks, dump_manifest(stored))
    return stored


def _write_pfb(
    out_file, schema, metadata, records, manifest, payloads=None, payload_meta=None
):
    pfb_writer = PFBWriter(out_file)
    pfb_writer.set_schema(schema)
    pfb_writer.set_metadata(metadata)
    pfb_writer.set_index_manifest(manifest)
    if payloads:
        pfb_writer.set_parquet_payloads(payloads, payload_meta)
    pfb_writer.write_to(out_file, records)
