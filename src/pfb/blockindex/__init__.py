# -*- coding: utf-8 -*-
"""Block-level search index for PFB files.

An Avro object container file is a sequence of *blocks*, each prefixed with a
record count and a byte length.  ``fastavro.block_reader`` can walk those
prefixes without decoding the records inside them, which costs roughly 2% of a
full decode.  This package builds an index that maps predicates to the block
ordinals containing matching records, so a query decodes only those blocks.

Nothing is added to the Avro schema or the record stream: the index lives in the
file-level header metadata, which readers that do not know about it ignore.
"""

from .manifest import (
    MANIFEST_KEY,
    MANIFEST_KEY_COMPRESSED,
    BlockIndex,
    load_manifest,
    dump_manifest,
)
from .builder import build_index, write_indexed
from .query import Predicate, parse_predicate, search

__all__ = [
    "MANIFEST_KEY",
    "MANIFEST_KEY_COMPRESSED",
    "BlockIndex",
    "load_manifest",
    "dump_manifest",
    "build_index",
    "write_indexed",
    "Predicate",
    "parse_predicate",
    "search",
]
