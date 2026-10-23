# -*- coding: utf-8 -*-
"""The index manifest, stored as JSON in Avro file-level header metadata."""

import base64
import json
import zlib

#: Header key holding the manifest as plain JSON text.
MANIFEST_KEY = "pfb.index.v1.manifest"
#: Header key holding the manifest as base64(deflate(JSON)).
MANIFEST_KEY_COMPRESSED = "pfb.index.v1.manifest.z"

#: Above this many bytes of JSON, the manifest is stored compressed instead.
COMPRESS_THRESHOLD = 16 * 1024

VERSION = 1


def dump_manifest(manifest):
    """Serialize a manifest into ``{header_key: str}`` for fastavro.

    fastavro requires header metadata values to be ``str``; the manifest is
    JSON text, so no base64 is needed unless it is large enough to be worth
    deflating, in which case it moves to a separate key.
    """
    text = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
    if len(text) <= COMPRESS_THRESHOLD:
        return {MANIFEST_KEY: text}
    packed = base64.b64encode(zlib.compress(text.encode("utf-8"), 9))
    return {MANIFEST_KEY_COMPRESSED: packed.decode("ascii")}


def load_manifest(header_metadata):
    """Pull a manifest out of fastavro header metadata, or return ``None``.

    An unreadable or future-versioned manifest yields ``None`` rather than an
    exception, so a reader built against v1 degrades to a full scan instead of
    refusing to open the file.
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


def build_manifest(nodes, embedded, payload_blocks=None):
    """Assemble the manifest.

    ``payload_blocks`` maps a node to the ordinal of the Avro block holding its
    ``NodeParquet`` record.  It is what lets a reader fetch one payload without
    iterating the record stream; see :mod:`pfb.index.container`.
    """
    return {
        "version": VERSION,
        "embedded": bool(embedded),
        "nodes": nodes,
        "payload_blocks": payload_blocks or {},
    }
