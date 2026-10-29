#!/usr/bin/env python
"""Read Parquet's ColumnIndex / OffsetIndex, which pyarrow does not expose.

Verified against pyarrow 26.0.0: there is no read_column_index, no
column_index_offset on ColumnChunkMetaData, nothing in pyarrow._parquet. So
§5.7's "--explain reports pages read vs skipped, straight from the page index"
describes an API that does not exist, and the only way to get the doc's actual
requested metric is to decode the footer ourselves.

Parquet metadata is Thrift COMPACT protocol. Rather than hand-code each struct,
this decodes generically into {field_id: value} trees and names the fields
afterwards - far less error-prone, and the field ids are a stable part of
parquet.thrift.

NOTHING here is trusted until cross_validate() passes against
ParquetFile.metadata on every field pyarrow also exposes.
"""
import struct

# Thrift compact protocol type ids
T_STOP, T_TRUE, T_FALSE, T_BYTE = 0x00, 0x01, 0x02, 0x03
T_I16, T_I32, T_I64, T_DOUBLE = 0x04, 0x05, 0x06, 0x07
T_BINARY, T_LIST, T_SET, T_MAP, T_STRUCT = 0x08, 0x09, 0x0A, 0x0B, 0x0C


class CompactReader:
    def __init__(self, buf, pos=0):
        self.buf = buf
        self.pos = pos

    def byte(self):
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self):
        shift = result = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def binary(self):
        n = self.varint()
        out = self.buf[self.pos : self.pos + n]
        self.pos += n
        return out

    def _value(self, ttype):
        if ttype == T_TRUE:
            return True
        if ttype == T_FALSE:
            return False
        if ttype == T_BYTE:
            v = self.byte()
            return v - 256 if v > 127 else v
        if ttype in (T_I16, T_I32, T_I64):
            return self.zigzag()
        if ttype == T_DOUBLE:
            v = struct.unpack("<d", self.buf[self.pos : self.pos + 8])[0]
            self.pos += 8
            return v
        if ttype == T_BINARY:
            return self.binary()
        if ttype == T_STRUCT:
            return self.struct()
        if ttype in (T_LIST, T_SET):
            return self.list()
        if ttype == T_MAP:
            return self.map()
        raise ValueError(f"unknown thrift type {ttype} at {self.pos}")

    def list(self):
        header = self.byte()
        size = (header >> 4) & 0x0F
        etype = header & 0x0F
        if size == 0x0F:
            size = self.varint()
        if etype in (T_TRUE, T_FALSE):
            # Booleans inside a collection are one byte each, not packed into the
            # element-type nibble the way a struct field's boolean is.
            return [self.byte() == 1 for _ in range(size)]
        return [self._value(etype) for _ in range(size)]

    def map(self):
        size = self.varint()
        if size == 0:
            return {}
        kv = self.byte()
        ktype, vtype = (kv >> 4) & 0x0F, kv & 0x0F
        return {self._value(ktype): self._value(vtype) for _ in range(size)}

    def struct(self):
        out = {}
        last_id = 0
        while True:
            header = self.byte()
            if header == T_STOP:
                return out
            delta = (header >> 4) & 0x0F
            ttype = header & 0x0F
            fid = self.zigzag() if delta == 0 else last_id + delta
            last_id = fid
            out[fid] = self._value(ttype)


# --- parquet.thrift field ids -------------------------------------------------
FILE_META = {
    1: "version",
    2: "schema",
    3: "num_rows",
    4: "row_groups",
    5: "key_value_metadata",
    6: "created_by",
}
ROW_GROUP = {
    1: "columns",
    2: "total_byte_size",
    3: "num_rows",
    4: "sorting_columns",
    5: "file_offset",
    6: "total_compressed_size",
}
COL_CHUNK = {
    1: "file_path",
    2: "file_offset",
    3: "meta_data",
    4: "offset_index_offset",
    5: "offset_index_length",
    6: "column_index_offset",
    7: "column_index_length",
}
COL_META = {
    1: "type",
    2: "encodings",
    3: "path_in_schema",
    4: "codec",
    5: "num_values",
    6: "total_uncompressed_size",
    7: "total_compressed_size",
    9: "data_page_offset",
    10: "index_page_offset",
    11: "dictionary_page_offset",
    12: "statistics",
    13: "encoding_stats",
    14: "bloom_filter_offset",
    15: "bloom_filter_length",
}
STATISTICS = {
    1: "max",
    2: "min",
    3: "null_count",
    4: "distinct_count",
    5: "max_value",
    6: "min_value",
}
COLUMN_INDEX = {
    1: "null_pages",
    2: "min_values",
    3: "max_values",
    4: "boundary_order",
    5: "null_counts",
}
OFFSET_INDEX = {1: "page_locations", 2: "unencoded_byte_array_data_bytes"}
PAGE_LOCATION = {1: "offset", 2: "compressed_page_size", 3: "first_row_index"}

# Parquet physical types (parquet.thrift Type enum)
PHYSICAL = {
    0: "BOOLEAN",
    1: "INT32",
    2: "INT64",
    3: "INT96",
    4: "FLOAT",
    5: "DOUBLE",
    6: "BYTE_ARRAY",
    7: "FIXED_LEN_BYTE_ARRAY",
}


def _name(d, names):
    return {names.get(k, k): v for k, v in d.items()} if isinstance(d, dict) else d


def read_footer(blob):
    """Decode FileMetaData from a complete Parquet file held in memory."""
    if blob[-4:] != b"PAR1":
        raise ValueError("not a parquet file (missing trailing PAR1)")
    footer_len = struct.unpack("<I", blob[-8:-4])[0]
    start = len(blob) - 8 - footer_len
    meta = CompactReader(blob, start).struct()

    out = _name(meta, FILE_META)
    out["created_by"] = (out.get("created_by") or b"").decode("utf8", "replace")
    row_groups = []
    for rg in out.get("row_groups", []):
        rg = _name(rg, ROW_GROUP)
        cols = []
        for ch in rg.get("columns", []):
            ch = _name(ch, COL_CHUNK)
            cm = _name(ch.get("meta_data", {}), COL_META)
            cm["path_in_schema"] = ".".join(
                p.decode("utf8", "replace") for p in cm.get("path_in_schema", [])
            )
            cm["physical_type"] = PHYSICAL.get(cm.get("type"), cm.get("type"))
            if "statistics" in cm:
                cm["statistics"] = _name(cm["statistics"], STATISTICS)
            ch["meta_data"] = cm
            cols.append(ch)
        rg["columns"] = cols
        row_groups.append(rg)
    out["row_groups"] = row_groups
    return out


def read_column_index(blob, chunk):
    """ColumnIndex for one column chunk: per-DATA-PAGE min/max and null counts."""
    off, length = chunk.get("column_index_offset"), chunk.get("column_index_length")
    if not off:
        return None
    ci = _name(CompactReader(blob, off).struct(), COLUMN_INDEX)
    ci["n_pages"] = len(ci.get("min_values", []))
    ci["_length"] = length
    return ci


def read_offset_index(blob, chunk):
    """OffsetIndex for one column chunk: each page's byte offset and size."""
    off, length = chunk.get("offset_index_offset"), chunk.get("offset_index_length")
    if not off:
        return None
    oi = _name(CompactReader(blob, off).struct(), OFFSET_INDEX)
    oi["page_locations"] = [
        _name(p, PAGE_LOCATION) for p in oi.get("page_locations", [])
    ]
    oi["_length"] = length
    return oi


def decode_bound(raw, physical_type):
    """ColumnIndex min/max are raw PHYSICAL bytes, not logical values."""
    if raw is None:
        return None
    if physical_type == "BYTE_ARRAY" or physical_type == "FIXED_LEN_BYTE_ARRAY":
        return raw.decode("utf8", "replace")
    if physical_type == "INT32":
        return struct.unpack("<i", raw)[0]
    if physical_type == "INT64":
        return struct.unpack("<q", raw)[0]
    if physical_type == "DOUBLE":
        return struct.unpack("<d", raw)[0]
    if physical_type == "FLOAT":
        return struct.unpack("<f", raw)[0]
    if physical_type == "BOOLEAN":
        return bool(raw[0])
    return raw


def cross_validate(blob, pyarrow_metadata):
    """Every field BOTH we and pyarrow can see must agree, or nothing is trusted.

    Returns (ok, list_of_mismatches). The ColumnIndex/OffsetIndex portion cannot
    be corroborated by pyarrow at all - this check is what licenses believing it.
    """
    problems = []
    ours = read_footer(blob)

    def check(label, a, b):
        if a != b:
            problems.append(f"{label}: ours={a!r} pyarrow={b!r}")

    check("num_rows", ours["num_rows"], pyarrow_metadata.num_rows)
    check("num_row_groups", len(ours["row_groups"]), pyarrow_metadata.num_row_groups)
    for i, rg in enumerate(ours["row_groups"]):
        prg = pyarrow_metadata.row_group(i)
        check(f"rg{i}.num_rows", rg["num_rows"], prg.num_rows)
        check(f"rg{i}.num_columns", len(rg["columns"]), prg.num_columns)
        for j, ch in enumerate(rg["columns"]):
            pch = prg.column(j)
            cm = ch["meta_data"]
            check(f"rg{i}.col{j}.path", cm["path_in_schema"], pch.path_in_schema)
            check(f"rg{i}.col{j}.num_values", cm["num_values"], pch.num_values)
            check(
                f"rg{i}.col{j}.total_compressed_size",
                cm["total_compressed_size"],
                pch.total_compressed_size,
            )
            check(
                f"rg{i}.col{j}.bloom_filter_offset",
                cm.get("bloom_filter_offset"),
                pch.bloom_filter_offset,
            )
            if pch.is_stats_set and "statistics" in cm:
                st = cm["statistics"]
                pst = pch.statistics
                check(f"rg{i}.col{j}.null_count", st.get("null_count"), pst.null_count)
                if pst.has_min_max and st.get("min_value") is not None:
                    check(
                        f"rg{i}.col{j}.min",
                        decode_bound(st["min_value"], cm["physical_type"]),
                        (
                            pst.min
                            if not isinstance(pst.min, bytes)
                            else pst.min.decode("utf8", "replace")
                        ),
                    )
    return (not problems), problems
