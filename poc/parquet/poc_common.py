"""Shared machinery for the parquet-search-index PoC (docs/parquet-search-index.md §7).

Nothing here is library code. It deliberately lives outside src/ so the PoC can be
thrown away, and so `pfb` itself is unchanged while the §7 gates are measured.
"""

import io
import json
import pathlib

import pyarrow as pa

AVRO_EXPORT_BYTES = 10_173_528  # export_2026-08-06T10_36_03.avro, the §7 baseline

# Synthetic columns. Prefixed so a collision with a real PFB field is detectable.
COL_ID = "__pfb_id"
COL_REL_NAMES = "__pfb_rel_dst_name"
COL_REL_IDS = "__pfb_rel_dst_id"
COL_REL0_NAME = "__pfb_rel0_dst_name"
COL_REL0_ID = "__pfb_rel0_dst_id"
SYNTHETIC_PREFIX = "__pfb_"


class UnsupportedPFBType(Exception):
    """Raised when a PFB field cannot be mapped to Arrow without data loss."""


# --------------------------------------------------------------------------
# Byte accounting
# --------------------------------------------------------------------------
class CountingReader(io.RawIOBase):
    """A seekable file object over an in-memory buffer that records every read.

    Step 5 Angle A ("what did pyarrow actually read?") depends entirely on this,
    and it is only trustworthy once calibrated: an unfiltered single-column read
    must reconcile with that column's total_compressed_size from the footer. On a
    small file it will not, because pyarrow reads the whole thing regardless - see
    calibrate() and do not report ratios from an uncalibrated reader.
    """

    def __init__(self, buf):
        self._buf = buf
        self._pos = 0
        self.ranges = []

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, off, whence=0):
        base = {0: 0, 1: self._pos, 2: len(self._buf)}[whence]
        self._pos = base + off
        return self._pos

    def readinto(self, b):
        n = min(len(b), len(self._buf) - self._pos)
        b[:n] = self._buf[self._pos : self._pos + n]
        self.ranges.append((self._pos, n))
        self._pos += n
        return n

    @property
    def bytes_read(self):
        return sum(n for _, n in self.ranges)

    @property
    def n_reads(self):
        return len(self.ranges)

    def reset(self):
        self._pos = 0
        self.ranges = []


def column_compressed_bytes(metadata, column_name):
    """Sum total_compressed_size across row groups for one leaf column."""
    total = 0
    for rg in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg)
        for c in range(row_group.num_columns):
            chunk = row_group.column(c)
            if chunk.path_in_schema == column_name:
                total += chunk.total_compressed_size
    return total


def write_json(path, obj):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))
    return path


def load_capabilities(outdir):
    p = pathlib.Path(outdir) / "capabilities.json"
    if not p.exists():
        raise SystemExit(f"{p} missing - run pqprobe.py first")
    return json.loads(p.read_text())


# --------------------------------------------------------------------------
# Avro -> Arrow mapping  (docs/parquet-search-index.md §5.2, corrected)
# --------------------------------------------------------------------------
# The doc's sketch raises on any multi-branch union. Against the real export that
# aborts on the first node: 36 fields (every `state`) are union<enum, enum, null>,
# where BOTH enums map to pa.string(). So the rule here is collapse-if-identical,
# raise-if-not - the heterogeneous case is the one that would silently corrupt
# data, and it does not occur. --strict-unions restores the doc's behaviour so the
# offender list can be produced for the writeup.
#
# Note also that "null" is LAST in those unions, not first, so nothing may assume
# union position.

_PRIMITIVES = {
    "string": pa.string(),
    "long": pa.int64(),
    "int": pa.int32(),
    "double": pa.float64(),
    "float": pa.float32(),
    "boolean": pa.bool_(),
    "bytes": pa.binary(),
}


def _branch_to_arrow(t, path):
    """Map one non-null Avro union branch to an Arrow type."""
    if isinstance(t, str):
        try:
            return _PRIMITIVES[t]
        except KeyError:
            raise UnsupportedPFBType(f"{path}: unknown primitive {t!r}")
    if not isinstance(t, dict):
        raise UnsupportedPFBType(f"{path}: unhandled type node {t!r}")

    kind = t.get("type")
    if kind == "enum":
        # Plain string, not pa.dictionary(): Parquet applies its own dictionary
        # encoding to string columns (use_dictionary=True), which is what §1.2
        # actually asks for, and `filters=` support on Arrow dictionary columns is
        # inconsistent across versions.
        return pa.string()
    if kind == "array":
        return pa.list_(_branch_to_arrow(t["items"], path + "[]"))
    if kind == "map":
        # Not present in this export - untested path, flagged rather than trusted.
        return pa.map_(pa.string(), pa.string())
    if kind == "record":
        raise UnsupportedPFBType(f"{path}: nested record not supported")
    if kind in _PRIMITIVES:
        if "logicalType" in t:
            raise UnsupportedPFBType(
                f"{path}: logicalType {t['logicalType']!r} not supported"
            )
        return _PRIMITIVES[kind]
    raise UnsupportedPFBType(f"{path}: unknown complex type {kind!r}")


def _enum_symbols(branches):
    """Union of symbols across all enum branches (13 for the two-enum `state`)."""
    symbols = []
    for b in branches:
        if isinstance(b, dict) and b.get("type") == "enum":
            symbols.extend(b.get("symbols", []))
        elif isinstance(b, dict) and b.get("type") == "array":
            item = b.get("items")
            if isinstance(item, dict) and item.get("type") == "enum":
                symbols.extend(item.get("symbols", []))
    return sorted(set(symbols)) or None


def avro_field_to_arrow(field, node_name, strict_unions=False, notes=None):
    """Map a PFB node field to (arrow_type, enum_symbols_or_None)."""
    types = field["type"] if isinstance(field["type"], list) else [field["type"]]
    path = f"{node_name}.{field['name']}"
    branches = [t for t in types if t != "null"]
    if not branches:
        return pa.null(), None

    mapped = [_branch_to_arrow(b, path) for b in branches]
    if len(branches) > 1:
        if strict_unions:
            kinds = [b if isinstance(b, str) else b.get("type") for b in branches]
            raise UnsupportedPFBType(f"{path}: multi-branch union {kinds}")
        if any(not m.equals(mapped[0]) for m in mapped[1:]):
            raise UnsupportedPFBType(
                f"{path}: heterogeneous multi-branch union -> "
                f"{[str(m) for m in mapped]}"
            )
        if notes is not None:
            notes.append(
                {
                    "field": path,
                    "issue": "union_collapsed",
                    "branches": len(branches),
                    "arrow": str(mapped[0]),
                }
            )
    return mapped[0], _enum_symbols(branches)


def node_to_arrow_schema(node, strict_unions=False, notes=None):
    """Build the Arrow schema for one PFB node, including synthetic columns.

    Every field is nullable unconditionally - including fields whose Avro union has
    no "null" branch. Records in the wild omit keys, and a non-nullable Arrow field
    turns an omitted key into a hard write error mid-stage.
    """
    name = node["name"]
    fields = [pa.field(COL_ID, pa.string(), nullable=True)]
    symbols = {}
    for f in node["fields"]:
        if f["name"].startswith(SYNTHETIC_PREFIX):
            raise UnsupportedPFBType(
                f"{name}.{f['name']}: collides with synthetic column prefix "
                f"{SYNTHETIC_PREFIX!r}"
            )
        arrow_type, syms = avro_field_to_arrow(f, name, strict_unions, notes)
        fields.append(pa.field(f["name"], arrow_type, nullable=True))
        if syms:
            symbols[f["name"]] = syms
    fields += [
        pa.field(COL_REL_NAMES, pa.list_(pa.string()), nullable=True),
        pa.field(COL_REL_IDS, pa.list_(pa.string()), nullable=True),
        pa.field(COL_REL0_NAME, pa.string(), nullable=True),
        pa.field(COL_REL0_ID, pa.string(), nullable=True),
    ]
    return pa.schema(fields), symbols


def flatten_record(record, field_names, strict_arity=True):
    """PFB record -> flat dict matching node_to_arrow_schema's field order.

    `relations` is emitted twice: as two parallel list<string> columns (exact
    round-trip at any arity) and as two flattened scalars (directly filterable -
    pq.read_table(filters=) addresses top-level columns only and cannot reach a
    struct field nested inside a list). Measured arity in the real export is
    {0: 1, 1: 29020}, which is what justifies the flattening.
    """
    obj = record["object"]
    extra = set(obj) - set(field_names)
    if extra:
        raise UnsupportedPFBType(
            f"{record['name']}: record has keys absent from schema: " f"{sorted(extra)}"
        )

    relations = record["relations"] or []
    if strict_arity and len(relations) > 1:
        raise UnsupportedPFBType(
            f"{record['name']} id={record['id']}: relations arity "
            f"{len(relations)} > 1; flattening would drop edges"
        )

    names = [r["dst_name"] for r in relations]
    ids = [r["dst_id"] for r in relations]
    row = {COL_ID: record["id"]}
    for f in field_names:
        row[f] = obj.get(f)
    row[COL_REL_NAMES] = names
    row[COL_REL_IDS] = ids
    row[COL_REL0_NAME] = names[0] if names else None
    row[COL_REL0_ID] = ids[0] if ids else None
    return row


# --------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------
BLOOM_COLUMNS = (COL_ID, "submitter_id", "participant_id", "specimen_id", COL_REL0_ID)


def writer_options(
    schema,
    capabilities=None,
    compression="zstd",
    data_page_size=64 * 1024,
    bloom=True,
    ndv=1 << 20,
    compression_level=None,
    sorting_columns=None,
):
    """Build ParquetWriter kwargs, gated on what the probe says this build accepts.

    Verified against pyarrow 26.0.0: write_page_index, data_page_size,
    bloom_filter_options, sorting_columns and compression_level are ALL accepted
    on ParquetWriter (the doc's §1.2 note about bloom_filter_options arriving
    "through **options" is correct). write_bloom_filter is not a thing.
    """
    accepted = set()
    if capabilities:
        accepted = {
            k
            for k, v in capabilities.get("ParquetWriter_kwargs", {}).items()
            if v.get("accepted")
        }

    def ok(name):
        return not accepted or name in accepted

    opts = {
        "compression": compression,
        "use_dictionary": True,
        "write_statistics": True,
    }
    if ok("write_page_index"):
        # Off by default in pyarrow; §1.2 is right that this is the line that
        # matters most. Without it, pruning granularity drops to whole row groups.
        opts["write_page_index"] = True
    if ok("data_page_size"):
        opts["data_page_size"] = data_page_size
    if compression_level is not None and ok("compression_level"):
        opts["compression_level"] = compression_level
    if sorting_columns is not None and ok("sorting_columns"):
        opts["sorting_columns"] = sorting_columns
    if bloom and ok("bloom_filter_options"):
        cols = [c for c in BLOOM_COLUMNS if c in schema.names]
        if cols:
            opts["bloom_filter_options"] = {c: {"ndv": ndv, "fpp": 0.01} for c in cols}
    return opts


class ArrowStager:
    """Bucket PFB records by node and stream them into one Arrow IPC file per node.

    The IPC files are the PoC's cache: the 10.2 MB Avro is scanned exactly once
    here, and steps 2/4/5 then sweep dozens of Parquet configurations off the
    cache without ever touching PFBReader again.

    Memory is bounded by batch_size * nodes. The export fits in RAM comfortably,
    but the streaming shape is kept because production exports will not.
    """

    def __init__(
        self, schema, outdir, batch_size=4096, strict_unions=False, strict_arity=True
    ):
        self.notes = []
        self.schemas = {}
        self.enum_symbols = {}
        for node in schema:
            arrow_schema, symbols = node_to_arrow_schema(
                node, strict_unions=strict_unions, notes=self.notes
            )
            self.schemas[node["name"]] = arrow_schema
            self.enum_symbols[node["name"]] = symbols
        self.field_names = {
            node["name"]: [f["name"] for f in node["fields"]] for node in schema
        }
        self.outdir = pathlib.Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.strict_arity = strict_arity
        self._buffers = {}
        self._writers = {}
        self.row_counts = {}
        self.arity = {}
        self.null_ids = 0

    def add(self, record):
        node = record["name"]
        row = flatten_record(record, self.field_names[node], self.strict_arity)
        if row[COL_ID] is None:
            self.null_ids += 1
        n = len(record["relations"] or [])
        self.arity[n] = self.arity.get(n, 0) + 1
        buf = self._buffers.setdefault(node, [])
        buf.append(row)
        if len(buf) >= self.batch_size:
            self._flush(node)

    def _flush(self, node):
        rows = self._buffers.get(node)
        if not rows:
            return
        schema = self.schemas[node]
        # The schema MUST be passed explicitly: an all-None column would otherwise
        # be inferred as null type and fail the writer's schema equality check.
        batch = pa.RecordBatch.from_pylist(rows, schema=schema)
        if node not in self._writers:
            sink = (self.outdir / f"{node}.arrow").open("wb")
            self._writers[node] = (pa.ipc.new_file(sink, schema), sink)
        self._writers[node][0].write_batch(batch)
        self.row_counts[node] = self.row_counts.get(node, 0) + len(rows)
        rows.clear()

    def close(self):
        for node in list(self._buffers):
            self._flush(node)
        for node, (w, sink) in self._writers.items():
            w.close()
            sink.close()
        self._writers.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def read_cached(outdir, node):
    """Read one node's staged Arrow IPC file back as a Table."""
    path = pathlib.Path(outdir) / f"{node}.arrow"
    with pa.ipc.open_file(path) as f:
        return f.read_all()


def cached_nodes(outdir):
    return sorted(p.stem for p in pathlib.Path(outdir).glob("*.arrow"))


# --------------------------------------------------------------------------
# Container format (§4.1) - PoC only, by subclassing rather than editing src/
# --------------------------------------------------------------------------
NODE_PARQUET_SCHEMA = {
    "type": "record",
    "name": "NodeParquet",
    "fields": [
        {"name": "node", "type": "string"},
        {"name": "payload", "type": "bytes"},
        {"name": "num_rows", "type": "long"},
        {"name": "compression", "type": "string"},
        {"name": "page_index", "type": "boolean"},
        {"name": "bloom_columns", "type": {"type": "array", "items": "string"}},
    ],
}


def make_indexed_avro_schema(schema, position="last"):
    """make_avro_schema() plus the NodeParquet union member.

    Calls the unmodified library function and post-processes its output, so the
    PoC needs no changes under src/.
    """
    from pfb.writer import make_avro_schema

    avro_schema = make_avro_schema(schema)
    obj = next(f for f in avro_schema["fields"] if f["name"] == "object")
    if position == "first":
        obj["type"].insert(1, NODE_PARQUET_SCHEMA)  # after Metadata
    else:
        obj["type"].append(NODE_PARQUET_SCHEMA)
    return avro_schema


def indexed_pfb_records(
    reader_metadata, records, payloads, is_encode, payloads_first=False
):
    """Yield the full record stream for an indexed PFB.

    The enum re-encode loop is copied verbatim from PFBWriter.write
    (src/pfb/writer.py:221-246) rather than reimplemented - reusing it is the
    point, since it guarantees the round-trip re-encodes enums exactly as the
    library would, including the 36 fields whose Avro type is a two-enum union.
    """
    from pfb.base import avro_record, encode_enum

    def _payload_records():
        for node, info in payloads.items():
            yield avro_record(
                None,
                "NodeParquet",
                (
                    "NodeParquet",
                    {
                        "node": node,
                        "payload": info["payload"],
                        "num_rows": info["num_rows"],
                        "compression": info["compression"],
                        "page_index": info["page_index"],
                        "bloom_columns": info["bloom_columns"],
                    },
                ),
                [],
            )

    yield avro_record(None, "Metadata", reader_metadata, [])
    if payloads_first:
        yield from _payload_records()
    for record in records:
        obj = record["object"]
        name = record["name"]
        record["object"] = (name, obj)
        for field, value in list(obj.items()):
            if value is not None and is_encode(name, field):
                if isinstance(value, list):
                    obj[field] = [encode_enum(e) for e in value]
                else:
                    obj[field] = encode_enum(value)
        yield record
    if not payloads_first:
        yield from _payload_records()
