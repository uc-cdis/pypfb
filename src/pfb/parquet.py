# -*- coding: utf-8 -*-
"""Parquet staging, statistics, and index building.

Everything in this module needs pyarrow, which is an optional extra
(``pip install pypfb[parquet]``).  It therefore imports pyarrow at module scope
and is imported *lazily* by its callers -- one line inside each click callback.

That split is deliberate.  ``cli.py`` loads every ``pfb.plugins`` entry point on
every invocation with no ``try``/``except``, so a module-scope ``import pyarrow``
in any ``commands/`` or ``exporters/`` module would break the whole CLI on an
install without the extra, and would add ~0.7s to every command on one with it.
Keeping pyarrow here, below the plugins and above ``reader``/``writer``, is what
lets ``pfb show`` and ``pfb to tsv`` stay pyarrow-free by construction.
"""

from collections import Counter, defaultdict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .manifest import (
    STRATEGY_BLOOM,
    STRATEGY_DOMAIN,
    STRATEGY_NONE,
    STRATEGY_RANGE,
    PredicateError,
    build_manifest,
)
from .reader import PFBReader
from .writer import PFBWriter, write_indexed

# --------------------------------------------------------------------------
# Carried columns
# --------------------------------------------------------------------------

#: Column holding the Avro record id, so a Parquet hit maps back to a record.
ID_COLUMN = "__pfb_id"
#: Column holding ``[{dst_name, dst_id}, ...]`` for edge traversal.
RELATIONS_COLUMN = "__pfb_relations"

_RELATION_TYPE = pa.list_(
    pa.struct([("dst_name", pa.string()), ("dst_id", pa.string())])
)

_PRIMITIVES = {
    "string": pa.string(),
    "long": pa.int64(),
    "int": pa.int32(),
    "double": pa.float64(),
    "float": pa.float32(),
    "boolean": pa.bool_(),
    "bytes": pa.binary(),
    "null": pa.null(),
}


class UnsupportedPFBType(TypeError):
    """Raised for an Avro type with no unambiguous Arrow equivalent."""


# --------------------------------------------------------------------------
# Avro -> Arrow type mapping
# --------------------------------------------------------------------------


def _map_branch(type_):
    """Map one non-null union branch to an Arrow type."""
    if isinstance(type_, dict):
        kind = type_.get("type")
        if kind == "enum":
            # Enum symbols are plain strings once PFBReader has decoded them.
            return pa.string()
        if kind == "array":
            return pa.list_(_map_branch(type_["items"]))
        if kind == "map":
            return pa.map_(pa.string(), pa.string())
        if kind == "record":
            raise UnsupportedPFBType("nested record types are not supported")
        if kind in _PRIMITIVES:
            return _PRIMITIVES[kind]
        raise UnsupportedPFBType("unknown Avro type: {!r}".format(kind))
    if type_ in _PRIMITIVES:
        return _PRIMITIVES[type_]
    raise UnsupportedPFBType("unknown Avro type: {!r}".format(type_))


def avro_field_to_arrow(field):
    """Map a PFB node field type to an Arrow type.

    PFB specifics:
      - fields are typically ``union<null, X>`` -> nullable Arrow type
      - enums become string; values are decoded by ``PFBReader`` before staging
      - ``array<X>`` becomes ``list<X>``

    Multi-branch unions are **collapsed when every branch maps to the same Arrow
    type** and rejected otherwise.  Collapsing matters in practice: ``state``
    fields are ``union<enum, enum, null>``, and 36 of them occur in the sample
    dictionary alone, so rejecting every multi-branch union outright would abort
    on the first node of a real export.  Taking ``non_null[0]`` instead would
    silently corrupt a genuinely heterogeneous union, hence the equality check.
    """
    types = field["type"] if isinstance(field["type"], list) else [field["type"]]
    non_null = [t for t in types if t != "null"]
    if not non_null:
        return pa.null()

    mapped = [_map_branch(t) for t in non_null]
    first = mapped[0]
    for other in mapped[1:]:
        if other != first:
            raise UnsupportedPFBType(
                "{}: heterogeneous union {} maps to {}".format(
                    field["name"],
                    [_describe(t) for t in non_null],
                    sorted({str(m) for m in mapped}),
                )
            )
    return first


def _describe(type_):
    if isinstance(type_, dict):
        return type_.get("name") or type_.get("type")
    return type_


def node_to_arrow_schema(node):
    """Build the Arrow schema for one PFB node, including the carried columns."""
    fields = [pa.field(ID_COLUMN, pa.string())]
    for field in node["fields"]:
        fields.append(pa.field(field["name"], avro_field_to_arrow(field)))
    fields.append(pa.field(RELATIONS_COLUMN, _RELATION_TYPE))
    return pa.schema(fields)


# --------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------

#: Columns given a bloom filter when present.  High-cardinality identifier
#: columns are the only ones where a filter earns its bytes: on a real export
#: the filters cost 3.5% of the Parquet payload, 8x what the page index costs.
DEFAULT_BLOOM_COLUMNS = ("submitter_id", "participant_id", "specimen_id")

DEFAULT_BATCH_SIZE = 10000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_DATA_PAGE_SIZE = 64 * 1024
DEFAULT_DOMAIN_CAP = 256


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
    def compression(self):
        return self._compression

    @property
    def write_page_index(self):
        return self._write_page_index

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


# --------------------------------------------------------------------------
# Statistics and the cardinality guard
# --------------------------------------------------------------------------

_NUMERIC_CHECKS = (pa.types.is_integer, pa.types.is_floating, pa.types.is_temporal)


def _is_numeric(type_):
    return any(check(type_) for check in _NUMERIC_CHECKS)


def open_payload(payload):
    """Open an in-memory Parquet payload without touching the filesystem."""
    return pq.ParquetFile(pa.BufferReader(payload))


def footer_stats(parquet_file):
    """Per-column null counts and min/max from the footer.  Reads no data pages.

    ``distinct_count`` is deliberately not used: pyarrow leaves it unset, so
    domains are computed with :func:`pyarrow.compute.unique` over the column
    instead.
    """
    metadata = parquet_file.metadata
    out = {}
    for row_group in range(metadata.num_row_groups):
        group = metadata.row_group(row_group)
        for column in range(metadata.num_columns):
            chunk = group.column(column)
            entry = out.setdefault(
                chunk.path_in_schema,
                {"non_null": 0, "null_count": 0, "min": None, "max": None},
            )
            if not chunk.is_stats_set:
                entry["non_null"] += chunk.num_values
                continue
            statistics = chunk.statistics
            nulls = statistics.null_count or 0
            entry["null_count"] += nulls
            entry["non_null"] += chunk.num_values - nulls
            if statistics.has_min_max:
                low, high = statistics.min, statistics.max
                entry["min"] = low if entry["min"] is None else min(entry["min"], low)
                entry["max"] = high if entry["max"] is None else max(entry["max"], high)
    return out


def field_summaries(payload, bloom_columns=(), domain_cap=DEFAULT_DOMAIN_CAP):
    """Summarize every column of one node's payload.

    Applies the cardinality guard: a column with few enough distinct values gets
    its full domain listed in the manifest, and anything larger falls back to a
    bloom filter (already inside the payload) or to min/max, so the manifest
    stays bounded by field count rather than by record count.
    """
    parquet_file = open_payload(payload)
    stats = footer_stats(parquet_file)
    table = parquet_file.read()
    summaries = {}

    for name in table.schema.names:
        if name in (ID_COLUMN, RELATIONS_COLUMN):
            continue
        column = table.column(name)
        entry = dict(stats.get(name, {"non_null": 0, "null_count": 0}))
        summary = {
            "non_null": entry.get("non_null", 0),
            "null_count": entry.get("null_count", 0),
        }
        type_ = column.type
        if pa.types.is_list(type_) or pa.types.is_map(type_):
            # Nested columns have no usable domain; the footer carries their
            # leaf statistics under a different path name.
            summary["strategy"] = STRATEGY_NONE
            summaries[name] = summary
            continue
        if summary["non_null"] == 0:
            summary["strategy"] = STRATEGY_NONE
            summaries[name] = summary
            continue

        distinct = _distinct_values(column)
        if distinct is not None and len(distinct) <= domain_cap:
            summary["strategy"] = STRATEGY_DOMAIN
            summary["distinct_count"] = len(distinct)
            summary["domain"] = distinct
        elif name in bloom_columns:
            summary["strategy"] = STRATEGY_BLOOM
        elif _is_numeric(type_):
            summary["strategy"] = STRATEGY_RANGE
            summary["min"] = _scalar(entry.get("min"))
            summary["max"] = _scalar(entry.get("max"))
        else:
            summary["strategy"] = STRATEGY_NONE
        summaries[name] = summary
    return summaries


def _distinct_values(column):
    """Sorted distinct values, or ``None`` if the column is not hashable."""
    try:
        unique = pc.unique(column.combine_chunks())
    except pa.ArrowNotImplementedError:
        return None
    values = [v for v in unique.to_pylist() if v is not None]
    try:
        return sorted(values)
    except TypeError:
        return sorted(values, key=str)


def _scalar(value):
    """Make a footer statistic JSON-serializable."""
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# --------------------------------------------------------------------------
# Querying a payload
# --------------------------------------------------------------------------


def predicate_to_expression(predicate, arrow_type):
    """Turn a :class:`~pfb.manifest.Predicate` into a pyarrow filter expression.

    Lives here rather than on ``Predicate`` so that the predicate itself stays a
    dependency-free value object.
    """
    field = ds.field(predicate.field)
    value = _coerce(predicate, arrow_type)
    if predicate.op == "=":
        return field == value
    if predicate.op == "!=":
        return field != value
    if predicate.op == ">":
        return field > value
    if predicate.op == ">=":
        return field >= value
    if predicate.op == "<":
        return field < value
    if predicate.op == "<=":
        return field <= value
    raise PredicateError("unsupported operator: {}".format(predicate.op))


def _coerce(predicate, arrow_type):
    """Cast a predicate's right-hand side to the column's type."""
    text = predicate.value
    try:
        if pa.types.is_integer(arrow_type):
            return int(text)
        if pa.types.is_floating(arrow_type):
            return float(text)
        if pa.types.is_boolean(arrow_type):
            return text.lower() in ("true", "1", "yes")
    except ValueError:
        raise PredicateError(
            "{!r} is not valid for column {} of type {}".format(
                text, predicate.field, arrow_type
            )
        )
    return text


class SearchResult(object):
    """Matching rows, plus how the answer was reached."""

    def __init__(self, rows, source, row_groups=0, num_rows=0):
        self.rows = rows
        self.source = source
        self.row_groups = row_groups
        self.num_rows = num_rows

    def ids(self):
        return [row[ID_COLUMN] for row in self.rows]

    def explain(self):
        """What the index did, and what cannot be reported.

        pyarrow exposes no ``ColumnIndex``/``OffsetIndex`` accessor and no
        counter for pages or row groups actually skipped, so there is no honest
        way to report realized pruning from inside this process.  Rather than
        print a number that is really just the total, the fields that cannot be
        measured are named as such.
        """
        return {
            "source": self.source,
            "row_groups_in_payload": self.row_groups,
            "rows_in_payload": self.num_rows,
            "rows_matched": len(self.rows),
            "pages_skipped": "not reportable: pyarrow exposes no page-index API",
        }


def search_payload(payload, predicates):
    """Filter one node's Parquet payload by every predicate, ANDed.

    Pushdown is left entirely to pyarrow: the dataset layer consults the
    row-group statistics and the page index enabled at write time.
    """
    parquet_file = open_payload(payload)
    schema = parquet_file.schema_arrow
    expression = None
    for predicate in predicates:
        if predicate.field not in schema.names:
            raise PredicateError(
                "{} has no field {}".format(predicate.node, predicate.field)
            )
        term = predicate_to_expression(predicate, schema.field(predicate.field).type)
        expression = term if expression is None else (expression & term)

    table = pq.read_table(pa.BufferReader(payload), filters=expression)
    return SearchResult(
        rows=table.to_pylist(),
        source="parquet",
        row_groups=parquet_file.metadata.num_row_groups,
        num_rows=parquet_file.metadata.num_rows,
    )


# --------------------------------------------------------------------------
# Building an index
# --------------------------------------------------------------------------


class IndexBuildResult(object):
    """Everything a build produced, before it is written anywhere."""

    __slots__ = (
        "manifest",
        "payloads",
        "payload_meta",
        "schema",
        "metadata",
        "records",
    )

    def __init__(self, manifest, payloads, payload_meta, schema, metadata, records):
        self.manifest = manifest
        self.payloads = payloads
        self.payload_meta = payload_meta
        self.schema = schema
        self.metadata = metadata
        self.records = records

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

    Records are read through :class:`~pfb.reader.PFBReader` rather than raw Avro
    so that enum values are decoded before they reach Parquet; indexing the
    escaped ``_32_`` forms would key domains on strings no caller would ever
    type as a predicate.
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
    payload_meta = {}
    for node, payload in payloads.items():
        bloom = stager.bloom_columns_for(node)
        payload_meta[node] = {
            "num_rows": stager.row_counts[node],
            "compression": compression,
            "page_index": write_page_index,
            "bloom_columns": bloom,
        }
        nodes[node] = dict(
            payload_meta[node],
            payload_bytes=len(payload),
            fields=field_summaries(payload, bloom_columns=bloom, domain_cap=domain_cap),
        )

    manifest = build_manifest(nodes, embedded=False)
    manifest["edges"] = dict(stager.edge_counts)
    manifest["domain_cap"] = domain_cap
    return IndexBuildResult(manifest, payloads, payload_meta, schema, metadata, records)


def write_index(out_file, result, embed=False):
    """Write the indexed PFB.  Thin wrapper over :func:`pfb.writer.write_indexed`.

    Exists so callers have a single entry point that takes an
    :class:`IndexBuildResult`; all of the Avro container work lives in
    ``writer.py``, which needs no pyarrow.
    """
    return write_indexed(
        out_file,
        schema=result.schema,
        metadata=result.metadata,
        records=result.records,
        manifest=result.manifest,
        payloads=result.payloads if embed else None,
        payload_meta=result.payload_meta,
    )
