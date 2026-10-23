# -*- coding: utf-8 -*-
"""Per-field statistics derived from a staged Parquet payload."""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .arrow import ID_COLUMN, RELATIONS_COLUMN

#: Strategy tags recorded per field in the manifest.
DOMAIN = "domain"
BLOOM = "bloom"
RANGE = "range"
NONE = "none"

DEFAULT_DOMAIN_CAP = 256

_NUMERIC = (pa.types.is_integer, pa.types.is_floating, pa.types.is_temporal)


def _is_numeric(type_):
    return any(check(type_) for check in _NUMERIC)


def open_payload(payload):
    """Open an in-memory Parquet payload without touching the filesystem."""
    return pq.ParquetFile(pa.BufferReader(payload))


def footer_stats(parquet_file):
    """Per-column null counts and min/max from the footer.  Reads no data pages.

    ``distinct_count`` is deliberately not used: pyarrow leaves it unset, so
    domains are computed with :func:`pc.unique` over the column instead.
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

    Applies the cardinality guard: a column with few enough distinct values
    gets its full domain listed in the manifest, and anything larger falls back
    to a bloom filter (already inside the payload) or to min/max, so the
    manifest stays bounded by field count rather than by record count.
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
            summary["strategy"] = NONE
            summaries[name] = summary
            continue
        if summary["non_null"] == 0:
            summary["strategy"] = NONE
            summaries[name] = summary
            continue

        distinct = _distinct_values(column)
        if distinct is not None and len(distinct) <= domain_cap:
            summary["strategy"] = DOMAIN
            summary["distinct_count"] = len(distinct)
            summary["domain"] = distinct
        elif name in bloom_columns:
            summary["strategy"] = BLOOM
        elif _is_numeric(type_):
            summary["strategy"] = RANGE
            summary["min"] = _scalar(entry.get("min"))
            summary["max"] = _scalar(entry.get("max"))
        else:
            summary["strategy"] = NONE
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
