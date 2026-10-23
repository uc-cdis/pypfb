# -*- coding: utf-8 -*-
"""The on-disk form of the block index, and the object wrapping it.

The manifest is JSON stored in Avro file-level header metadata.  Header values
must be ``str`` (fastavro rejects ``bytes``), so when the JSON is large it is
deflated and base64-encoded into a second, separate key.  Exactly one of the two
keys is written.
"""

import base64
import json
import zlib

#: Header key holding the manifest as plain JSON text.
MANIFEST_KEY = "pfb.blockindex.v1"
#: Header key holding the manifest as base64(deflate(JSON)).
MANIFEST_KEY_COMPRESSED = "pfb.blockindex.v1.z"

#: Above this many bytes of JSON, the manifest is stored compressed instead.
COMPRESS_THRESHOLD = 16 * 1024

VERSION = 1

# Strategy tags, kept short because they repeat once per indexed field.
POSTINGS = "postings"  # value -> block ordinals
RANGE = "range"  # block ordinal -> [min, max]
NONE = "none"  # not indexed; queries fall back to the node's blocks


def dump_manifest(manifest):
    """Serialize a manifest dict into ``{header_key: str}`` for fastavro.

    Returns a single-entry dict so the caller can splat it into the writer's
    ``metadata=`` argument alongside anything else it wants to store.
    """
    text = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
    if len(text) <= COMPRESS_THRESHOLD:
        return {MANIFEST_KEY: text}
    packed = base64.b64encode(zlib.compress(text.encode("utf-8"), 9))
    return {MANIFEST_KEY_COMPRESSED: packed.decode("ascii")}


def load_manifest(header_metadata):
    """Pull a manifest out of ``fastavro`` header metadata, or return ``None``.

    Accepts either storage form.  Unknown or future versions return ``None``
    rather than raising, so that a reader built against v1 degrades to a full
    scan instead of refusing to open the file.
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
    if manifest.get("version") != VERSION:
        return None
    return manifest


class BlockIndex(object):
    """Read-side view over a manifest.

    Resolves a field to the set of block ordinals that may contain matching
    records.  Every method is deliberately *conservative*: when the index cannot
    answer precisely it returns a superset (usually the node's whole block set),
    never a subset.  Callers re-check each decoded record against the predicate,
    so a superset costs time and a subset would cost correctness.
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
        return entry["s"] if entry else NONE

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
        if entry["s"] == POSTINGS:
            return node_blocks & self._postings_blocks(entry, predicate)
        if entry["s"] == RANGE:
            return node_blocks & self._range_blocks(entry, predicate)
        return node_blocks

    @staticmethod
    def _postings_blocks(entry, predicate):
        values = entry["v"]
        if predicate.op == "=":
            # A value absent from the postings appears in no block at all, so an
            # unknown value legitimately prunes everything.
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
