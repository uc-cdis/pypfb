# -*- coding: utf-8 -*-
"""Driving a full index build: stage, summarize, embed, backfill."""

import io

from ..reader import PFBReader
from ..writer import PFBWriter
from .container import locate_payload_blocks, rewrite_with_manifest
from .manifest import build_manifest, dump_manifest
from .staging import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_BLOOM_COLUMNS,
    DEFAULT_COMPRESSION,
    DEFAULT_DATA_PAGE_SIZE,
    ParquetStager,
)
from .stats import DEFAULT_DOMAIN_CAP, field_summaries


class IndexBuildResult(object):
    """Everything a build produced, before it is written anywhere."""

    __slots__ = ("manifest", "payloads", "schema", "metadata", "records", "stager")

    def __init__(self, manifest, payloads, schema, metadata, records, stager):
        self.manifest = manifest
        self.payloads = payloads
        self.schema = schema
        self.metadata = metadata
        self.records = records
        self.stager = stager

    @property
    def payload_bytes(self):
        return sum(len(p) for p in self.payloads.values())


def build_index(
    input_file,
    domain_cap=DEFAULT_DOMAIN_CAP,
    compression=DEFAULT_COMPRESSION,
    data_page_size=DEFAULT_DATA_PAGE_SIZE,
    batch_size=DEFAULT_BATCH_SIZE,
    bloom_columns=DEFAULT_BLOOM_COLUMNS,
    write_page_index=True,
):
    """Read a PFB, stage per-node Parquet, and summarize every field.

    Records are read through :class:`~pfb.reader.PFBReader` rather than raw
    Avro so that enum values are decoded before they reach Parquet; indexing
    the escaped ``_32_`` forms would key domains on strings no caller would
    ever type as a predicate.
    """
    with PFBReader(input_file) as reader:
        schema = reader.schema
        metadata = reader.metadata
        stager = ParquetStager(
            schema,
            batch_size=batch_size,
            compression=compression,
            data_page_size=data_page_size,
            bloom_columns=bloom_columns,
            write_page_index=write_page_index,
        )
        records = []
        for record in reader:
            records.append(record)
            stager.add(record)
        payloads = stager.finish()

    nodes = {}
    for node, payload in payloads.items():
        nodes[node] = {
            "num_rows": stager.row_counts[node],
            "compression": compression,
            "page_index": write_page_index,
            "bloom_columns": stager.bloom_columns_for(node),
            "payload_bytes": len(payload),
            "fields": field_summaries(
                payload,
                bloom_columns=stager.bloom_columns_for(node),
                domain_cap=domain_cap,
            ),
        }

    manifest = build_manifest(nodes, embedded=False)
    manifest["edges"] = dict(stager.edge_counts)
    manifest["domain_cap"] = domain_cap
    return IndexBuildResult(manifest, payloads, schema, metadata, records, stager)


def write_indexed(out_file, result, embed=False):
    """Write the indexed PFB.

    Without ``embed`` this is tiers 0-2: the manifest goes in the Avro header
    and the file is otherwise unchanged, so no consumer can tell the difference.

    With ``embed`` it is tier 3.  The file is written twice: once to learn which
    Avro block each payload lands in, then again with those ordinals in the
    manifest.  The second pass copies blocks verbatim, so nothing is re-encoded
    and the ordinals it records stay valid.  See :mod:`pfb.index.container` for
    why ordinals rather than byte offsets.
    """
    if not embed:
        _write_pfb(out_file, result, payloads=None, manifest=result.manifest)
        return result.manifest

    staging_buffer = io.BytesIO()
    _write_pfb(
        staging_buffer, result, payloads=result.payloads, manifest=result.manifest
    )

    payload_blocks, blocks, writer_schema, codec = locate_payload_blocks(staging_buffer)
    manifest = dict(result.manifest)
    manifest["embedded"] = True
    manifest["payload_blocks"] = payload_blocks
    rewrite_with_manifest(
        out_file, writer_schema, codec, blocks, dump_manifest(manifest)
    )
    return manifest


def _write_pfb(out_file, result, payloads, manifest):
    writer = PFBWriter(out_file)
    writer.set_schema(result.schema)
    writer.set_metadata(result.metadata)
    writer.set_index_manifest(manifest)
    if payloads:
        writer.set_parquet_payloads(payloads, result.stager)
    # __enter__ without __exit__: the caller owns `out_file` and the two-pass
    # build still has to read the staging buffer back after this returns.
    writer.__enter__()
    writer.write(result.records)
