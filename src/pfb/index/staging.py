# -*- coding: utf-8 -*-
"""Turning PFB records into one indexed Parquet file per node."""

from collections import Counter, defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

from .arrow import ID_COLUMN, RELATIONS_COLUMN, node_to_arrow_schema

#: Columns given a bloom filter when present.  High-cardinality identifier
#: columns are the only ones where a filter earns its bytes: on a real export
#: the filters cost 3.5% of the Parquet payload, 8x what the page index costs.
DEFAULT_BLOOM_COLUMNS = ("submitter_id", "participant_id", "specimen_id")

DEFAULT_BATCH_SIZE = 10000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_DATA_PAGE_SIZE = 64 * 1024


class ParquetStager(object):
    """Bucket PFB records by node into one indexed Parquet file per node.

    Buffers up to ``batch_size`` rows per node and flushes them as a row group,
    so peak memory is bounded by (nodes x batch size) rather than by the size
    of the export.
    """

    def __init__(
        self,
        schema,
        batch_size=DEFAULT_BATCH_SIZE,
        compression=DEFAULT_COMPRESSION,
        data_page_size=DEFAULT_DATA_PAGE_SIZE,
        bloom_columns=DEFAULT_BLOOM_COLUMNS,
        write_page_index=True,
    ):
        self._arrow_schemas = {n["name"]: node_to_arrow_schema(n) for n in schema}
        self._batch_size = batch_size
        self._compression = compression
        self._data_page_size = data_page_size
        self._bloom_columns = tuple(bloom_columns or ())
        self._write_page_index = write_page_index

        self._buffers = defaultdict(list)
        self._sinks = {}
        self._writers = {}
        self._bloom_used = {}
        self.row_counts = Counter()
        #: ``"<src>-><dst>" -> count``, derived while staging so edge counts
        #: need no second pass over the records.
        self.edge_counts = Counter()

    @property
    def nodes(self):
        return sorted(self._writers)

    def bloom_columns_for(self, node):
        return self._bloom_used.get(node, [])

    def _writer_for(self, node):
        if node not in self._writers:
            schema = self._arrow_schemas[node]
            bloom = [c for c in self._bloom_columns if c in schema.names]
            self._bloom_used[node] = bloom
            self._sinks[node] = pa.BufferOutputStream()
            options = {}
            if bloom:
                # bloom_filter_options reaches ParquetWriter through **options;
                # there is no `write_bloom_filter` parameter.
                options["bloom_filter_options"] = {
                    c: {"ndv": 1 << 20, "fpp": 0.01} for c in bloom
                }
            self._writers[node] = pq.ParquetWriter(
                self._sinks[node],
                schema,
                compression=self._compression,
                write_page_index=self._write_page_index,
                data_page_size=self._data_page_size,
                use_dictionary=True,
                **options
            )
        return self._writers[node]

    def add(self, record):
        """Stage one decoded PFB record."""
        node = record["name"]
        if node not in self._arrow_schemas:
            return False
        row = dict(record["object"])
        row[ID_COLUMN] = record["id"]
        relations = record.get("relations") or []
        row[RELATIONS_COLUMN] = [
            {"dst_name": r["dst_name"], "dst_id": r["dst_id"]} for r in relations
        ]
        for relation in relations:
            self.edge_counts["{}->{}".format(node, relation["dst_name"])] += 1

        self._buffers[node].append(row)
        self.row_counts[node] += 1
        if len(self._buffers[node]) >= self._batch_size:
            self._flush(node)
        return True

    def _flush(self, node):
        rows = self._buffers.pop(node, None)
        if not rows:
            return
        schema = self._arrow_schemas[node]
        columns = {name: [row.get(name) for row in rows] for name in schema.names}
        table = pa.Table.from_pydict(columns, schema=schema)
        self._writer_for(node).write_table(table)

    def finish(self):
        """Flush everything and return ``{node: parquet_bytes}``."""
        for node in list(self._buffers):
            self._flush(node)
        payloads = {}
        for node, writer in self._writers.items():
            writer.close()
            payloads[node] = self._sinks[node].getvalue().to_pybytes()
        return payloads
