# -*- coding: utf-8 -*-
"""Build a block index for a PFB file and write an indexed copy."""

import io
from collections import defaultdict

import fastavro
from fastavro.write import Writer

from .manifest import NONE, POSTINGS, RANGE, VERSION, dump_manifest
from .schema import decode_value, field_traits

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
                "s": POSTINGS,
                "v": {value: sorted(blocks) for value, blocks in self.postings.items()},
            }
        if self.numeric and self.bounds:
            return {
                "s": RANGE,
                "r": {str(block): bounds for block, bounds in self.bounds.items()},
            }
        # Over cap and not orderable: the node-level block list is all we have.
        return {"s": NONE}


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
        "version": VERSION,
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
    boundaries — and therefore the ordinals in the manifest — survive exactly.
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
    writer = Writer(out_file, build.writer_schema, codec=build.codec, metadata=metadata)
    for count, raw in build.blocks:
        writer.write_block(_RawBlock(count, raw))
    writer.flush()
    return writer


def rechunk(file_obj, sync_interval):
    """Re-encode a file's records into blocks of ``sync_interval`` bytes.

    Smaller blocks prune better but cost more to walk, so this is the knob that
    trades index granularity against per-query overhead.  Unlike
    :func:`write_indexed` this re-encodes, so it must run *before* the index is
    built.
    """
    file_obj.seek(0)
    reader = fastavro.reader(file_obj)
    writer_schema = reader.writer_schema
    records = list(reader)
    buffer = io.BytesIO()
    fastavro.writer(
        buffer,
        writer_schema,
        records,
        codec=reader.codec,
        sync_interval=sync_interval,
    )
    buffer.seek(0)
    return buffer
