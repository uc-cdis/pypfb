# -*- coding: utf-8 -*-
"""Embedding Parquet payloads in a PFB, and getting them back out.

The design streams one ``NodeParquet`` entity per node in the Avro record
stream, which makes payloads lazy and skippable in a way header metadata could
not be.  That creates a placement problem, and this module is where it is
solved.

Payloads are emitted **last**, so a consumer that iterates to completion
produces all of its output before it reaches them.  But "last" also means a
reader that wants a payload would have to decode every data record first --
exactly the full scan the index exists to avoid, which on a real export made
indexed retrieval (514 ms) slower than the scan it replaced (504 ms).

The fix is to record, for each node, the ordinal of the Avro *block* its
payload lands in.  ``fastavro.block_reader`` can skip a block by its length
prefix without decoding it, so fetching one payload walks block headers (a few
ms) and decodes exactly one block.  Ordinals rather than byte offsets avoid a
circularity: the header is written before the data, so it cannot contain
offsets that are only known afterwards.

Knowing the ordinals requires having written the blocks, so the file is built
in two passes.  The second pass copies every block byte for byte -- nothing is
re-encoded, which is both what makes it cheap and what keeps the ordinals
valid, since re-encoding could move block boundaries.
"""

import io

import fastavro
from fastavro.write import Writer

#: Name of the union member carrying an embedded payload.
NODE_PARQUET = "NodeParquet"

#: The Avro schema of that union member.
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


class _RawBlock(object):
    """Adapter presenting pre-encoded block bytes to ``Writer.write_block``."""

    __slots__ = ("num_records", "bytes_")

    def __init__(self, num_records, raw):
        self.num_records = num_records
        self.bytes_ = io.BytesIO(raw)


def locate_payload_blocks(file_obj):
    """Return ``{node: block_ordinal}`` plus the raw blocks of ``file_obj``.

    One pass, decoding only what it must: every block has to be read to find
    the payload records, but the blocks come back as raw bytes so the caller
    can re-emit them without re-encoding.
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
    writer = Writer(out_file, writer_schema, codec=codec, metadata=metadata)
    for count, raw in blocks:
        writer.write_block(_RawBlock(count, raw))
    writer.flush()
    return writer


def read_payload(file_obj, node, block_ordinal):
    """Fetch one node's payload, decoding only the block that holds it.

    Returns ``None`` if the payload is not where the manifest says it is, which
    is the safe response to a file whose blocks were rewritten by other tooling
    after the index was built.
    """
    file_obj.seek(0)
    for ordinal, block in enumerate(fastavro.block_reader(file_obj)):
        if ordinal != block_ordinal:
            # Skipped by length prefix; the records are never decoded.
            continue
        for record in block:
            if record["name"] == NODE_PARQUET and record["object"]["node"] == node:
                return record["object"]
        return None
    return None


def iter_payloads(file_obj):
    """Yield every ``NodeParquet`` object in the file, for tooling and tests."""
    file_obj.seek(0)
    for record in fastavro.reader(file_obj):
        if record["name"] == NODE_PARQUET:
            yield record["object"]
