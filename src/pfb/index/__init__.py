# -*- coding: utf-8 -*-
"""Parquet-derived statistics and an embedded search index for PFB files.

Four tiers of capability, each a superset of the last:

===== ============================================ ==================
Tier  Capability                                   Source
===== ============================================ ==================
0     Field catalog                                manifest
1     Coverage counts                              Parquet footer
2     Value domains, min/max, membership tests     ``pc.unique`` + footer
3     Record retrieval by predicate                embedded Parquet
===== ============================================ ==================

Tiers 0-2 cost a few KB of header metadata and change nothing else about the
file.  Tier 3 adds a ``NodeParquet`` member to the ``object`` union and streams
one payload record per node, which is a breaking schema change for consumers
outside this SDK -- see ``docs/parquet-index.md``.
"""

# pyarrow is an optional extra, so the modules that need it are resolved
# lazily.  `pfb.reader` imports from this package on every open and must keep
# working in an install without the `parquet` extra.
from .manifest import MANIFEST_KEY, build_manifest, dump_manifest, load_manifest

_LAZY = {
    "UnsupportedPFBType": ".arrow",
    "avro_field_to_arrow": ".arrow",
    "node_to_arrow_schema": ".arrow",
    "build_index": ".builder",
    "write_indexed": ".builder",
    "Predicate": ".query",
    "PredicateError": ".query",
    "parse_predicate": ".query",
}


def __getattr__(name):
    if name in _LAZY:
        from importlib import import_module

        module = import_module(_LAZY[name], __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))


__all__ = [
    "UnsupportedPFBType",
    "avro_field_to_arrow",
    "node_to_arrow_schema",
    "build_index",
    "write_indexed",
    "MANIFEST_KEY",
    "build_manifest",
    "dump_manifest",
    "load_manifest",
    "Predicate",
    "PredicateError",
    "parse_predicate",
]
