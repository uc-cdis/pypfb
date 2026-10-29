import io
import json
import os
from contextlib import contextmanager
from copy import deepcopy

import fastavro
from fastavro import reader

from .base import PY3, PFBBase, decode_enum, handle_schema_field_unicode, str_hook
from .manifest import NODE_PARQUET, load_manifest

#: Union members that are not data nodes and must never appear in `schema`.
NON_NODE_MEMBERS = frozenset(["Metadata", NODE_PARQUET])


class PFBReader(PFBBase):
    open_mode = "rb"

    def __enter__(self):
        rv = super(PFBReader, self).__enter__()
        self._reader = reader(self._file_obj)
        schema = []
        self.set_encoded_schema(self._reader.writer_schema)
        for f in self._reader.writer_schema["fields"]:
            if f["name"] == "object":
                # Filter by name rather than skipping a fixed number of union
                # members.  Skipping exactly one made NodeParquet look like a
                # node, so `schema` reported 41 nodes for a 40-node export and
                # `pfb to tsv` failed cross-referencing it against metadata.
                for node in f["type"]:
                    if node["name"] in NON_NODE_MEMBERS:
                        continue
                    node = deepcopy(node)
                    schema.append(node)
                    for field in node["fields"]:
                        handle_schema_field_unicode(field, encode=False)
        self.set_schema(json.loads(json.dumps(schema), object_pairs_hook=str_hook))
        self.set_metadata(next(self._reader)["object"])
        # Small, and already in memory with the header, so parsed eagerly.
        self.set_index_manifest(load_manifest(self._reader.metadata))
        return rv

    def __iter__(self):
        return self

    def __next__(self):
        rv = next(self._reader)
        # Embedded payloads are not data records; skip them so that consumers
        # written against master see exactly the records they expect.
        while rv["name"] == NODE_PARQUET:
            rv = next(self._reader)
        obj = rv["object"]
        to_update = {}
        for name, value in list(obj.items()):
            if value and self.is_encode(rv["name"], name):
                if isinstance(value, list):
                    thing = []
                    for val in value:
                        thing.append(decode_enum(val))
                    to_update[name] = thing
                else:
                    to_update[name] = decode_enum(value)
        obj.update(to_update)
        return rv

    if not PY3:
        next = __next__

    def parquet(self, node):
        """Return one node's embedded Parquet payload as bytes, or ``None``.

        Decodes only the single Avro block holding that payload: the manifest
        records its block ordinal, and ``block_reader`` skips every other block
        by its length prefix.  This is what keeps retrieval from costing a full
        scan even though payloads are written last.

        Reads through a **duplicated file handle**, so seeking to the payload
        does not disturb the position or internal state of the ``fastavro``
        reader driving :meth:`__next__`; callers may interleave this with
        iteration freely.

        Returns ``None`` when the file has no embedded payloads, or when the
        payload is not where the manifest says it is -- the safe response to a
        file whose blocks were rewritten after the index was built.
        """
        manifest = self.index_manifest
        if not manifest or not manifest.get("embedded"):
            return None
        ordinal = manifest.get("payload_blocks", {}).get(node)
        if ordinal is None:
            return None
        payload = self._read_payload(node, ordinal)
        return payload["payload"] if payload else None

    def parquet_file(self, node):
        """``self.parquet(node)`` opened as a ``pyarrow.parquet.ParquetFile``."""
        payload = self.parquet(node)
        if payload is None:
            return None
        from .parquet import open_payload

        return open_payload(payload)

    def _read_payload(self, node, block_ordinal):
        """Decode just the block at ``block_ordinal`` and find ``node``'s payload."""
        with _independent_read_handle(self._file_obj) as handle:
            if handle is None:
                return None
            for ordinal, block in enumerate(fastavro.block_reader(handle)):
                if ordinal != block_ordinal:
                    # Skipped by length prefix; records are never decoded.
                    continue
                for record in block:
                    if (
                        record["name"] == NODE_PARQUET
                        and record["object"]["node"] == node
                    ):
                        return record["object"]
                return None
        return None


@contextmanager
def _independent_read_handle(file_obj):
    """Yield a second read handle over ``file_obj``, positioned at the start.

    Payload fetch has to seek, and the live ``fastavro`` reader is mid-stream on
    the same handle; seeking it directly desynchronises iteration.  Duplicating
    the descriptor avoids that.  Falls back to re-opening by name, then to
    copying an in-memory buffer, and finally yields ``None`` for a stream that
    is none of those -- notably stdin, which cannot be seeked anyway.
    """
    try:
        fileno = file_obj.fileno()
    except (AttributeError, OSError):
        fileno = None

    if fileno is not None:
        try:
            duplicate = os.fdopen(os.dup(fileno), "rb")
        except OSError:
            duplicate = None
        if duplicate is not None:
            try:
                duplicate.seek(0)
                yield duplicate
            finally:
                duplicate.close()
            return

    name = getattr(file_obj, "name", None)
    if isinstance(name, str) and os.path.exists(name):
        with open(name, "rb") as reopened:
            yield reopened
        return

    if hasattr(file_obj, "getvalue"):
        yield io.BytesIO(file_obj.getvalue())
        return

    yield None
