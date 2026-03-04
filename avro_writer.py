"""
avro_writer.py — Pure-stdlib AVRO OCF (Object Container File) writer.

Implements the minimum AVRO binary encoding needed to produce valid .avro files
without any third-party dependencies. Supports: null, boolean, int, long, float,
double, string, bytes, enum, array, map, union, and record types.

Reference: https://avro.apache.org/docs/current/spec.html
"""

from __future__ import annotations
import io
import json
import struct
import zlib
import os
import uuid
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Low-level AVRO primitive encoders
# ---------------------------------------------------------------------------

def encode_null(_value: None) -> bytes:
    return b""


def encode_boolean(value: bool) -> bytes:
    return b"\x01" if value else b"\x00"


def _encode_long_raw(n: int) -> bytes:
    """Encode a Python int as AVRO zigzag-encoded varint (long)."""
    # Zigzag encode
    n = (n << 1) ^ (n >> 63)
    buf = []
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            buf.append(b | 0x80)
        else:
            buf.append(b)
            break
    return bytes(buf)


def encode_int(value: int) -> bytes:
    return _encode_long_raw(value)


def encode_long(value: int) -> bytes:
    return _encode_long_raw(value)


def encode_float(value: float) -> bytes:
    return struct.pack("<f", value)


def encode_double(value: float) -> bytes:
    return struct.pack("<d", value)


def encode_bytes(value: bytes) -> bytes:
    return _encode_long_raw(len(value)) + value


def encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _encode_long_raw(len(encoded)) + encoded


def encode_enum(value: str, schema: Dict) -> bytes:
    symbols = schema["symbols"]
    idx = symbols.index(value)
    return encode_int(idx)


def encode_array(value: List, item_schema: Any, schema_registry: Dict) -> bytes:
    if not value:
        return encode_long(0)
    buf = encode_long(len(value))
    for item in value:
        buf += encode_value(item, item_schema, schema_registry)
    buf += encode_long(0)  # block terminator
    return buf


def encode_map(value: Dict, value_schema: Any, schema_registry: Dict) -> bytes:
    if not value:
        return encode_long(0)
    buf = encode_long(len(value))
    for k, v in value.items():
        buf += encode_string(k)
        buf += encode_value(v, value_schema, schema_registry)
    buf += encode_long(0)
    return buf


def encode_union(value: Any, union_schemas: List, schema_registry: Dict) -> bytes:
    """Encode a union value. Selects branch by Python type or explicit dict."""
    # If value is None → null branch
    if value is None:
        # find null index
        for i, s in enumerate(union_schemas):
            if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
                return encode_long(i)
        raise ValueError("None value but no null branch in union")

    # Otherwise try each non-null branch
    for i, s in enumerate(union_schemas):
        if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
            continue
        try:
            encoded = encode_value(value, s, schema_registry)
            return encode_long(i) + encoded
        except Exception:
            continue
    raise ValueError(f"Cannot encode union value: {value!r} with schemas {union_schemas}")


# ---------------------------------------------------------------------------
# Central dispatcher
# ---------------------------------------------------------------------------

def encode_value(value: Any, schema: Any, schema_registry: Dict) -> bytes:
    """Recursively encode a Python value according to an AVRO schema."""

    # Resolve named type references
    if isinstance(schema, str):
        if schema in schema_registry:
            return encode_value(value, schema_registry[schema], schema_registry)
        # Primitives
        if schema == "null":    return encode_null(value)
        if schema == "boolean": return encode_boolean(value)
        if schema == "int":     return encode_int(value)
        if schema == "long":    return encode_long(value)
        if schema == "float":   return encode_float(value)
        if schema == "double":  return encode_double(value)
        if schema == "bytes":   return encode_bytes(value)
        if schema == "string":  return encode_string(value)
        raise ValueError(f"Unknown primitive schema: {schema}")

    if isinstance(schema, list):
        return encode_union(value, schema, schema_registry)

    if isinstance(schema, dict):
        t = schema.get("type")

        if t == "record":
            return encode_record(value, schema, schema_registry)
        if t == "enum":
            return encode_enum(value, schema)
        if t == "array":
            return encode_array(value, schema["items"], schema_registry)
        if t == "map":
            return encode_map(value, schema["values"], schema_registry)

        # Logical types — encode as underlying type
        if t in ("null", "boolean", "int", "long", "float", "double", "bytes", "string"):
            return encode_value(value, t, schema_registry)

    raise ValueError(f"Cannot encode value {value!r} with schema {schema!r}")


def encode_record(record: Dict, schema: Dict, schema_registry: Dict) -> bytes:
    buf = b""
    for field in schema["fields"]:
        fname = field["name"]
        fschema = field["type"]
        val = record.get(fname, field.get("default"))
        buf += encode_value(val, fschema, schema_registry)
    return buf


# ---------------------------------------------------------------------------
# Schema registry builder (resolves named types)
# ---------------------------------------------------------------------------

def build_schema_registry(schema: Dict, registry: Dict = None) -> Dict:
    """Walk schema tree and register all named types by their full name."""
    if registry is None:
        registry = {}
    if isinstance(schema, dict):
        t = schema.get("type")
        if t in ("record", "enum", "fixed"):
            ns = schema.get("namespace", "")
            name = schema.get("name", "")
            full_name = f"{ns}.{name}" if ns else name
            registry[full_name] = schema
            registry[name] = schema  # also register short name
        for v in schema.values():
            build_schema_registry(v, registry)
    elif isinstance(schema, list):
        for item in schema:
            build_schema_registry(item, registry)
    return registry


# ---------------------------------------------------------------------------
# AVRO Object Container File (OCF) writer
# ---------------------------------------------------------------------------

AVRO_MAGIC = b"Obj\x01"
AVRO_SYNC_MARKER_SIZE = 16


class AvroWriter:
    """
    Writes an AVRO OCF file with 'null' codec (no compression).
    Usage:
        with AvroWriter(schema_dict, filepath) as w:
            w.write(record_dict)
    """

    def __init__(self, schema: Dict, filepath: str, codec: str = "null"):
        self.schema = schema
        self.filepath = filepath
        self.codec = codec
        self.registry = build_schema_registry(schema)
        self.sync_marker = os.urandom(AVRO_SYNC_MARKER_SIZE)
        self._buffer: List[bytes] = []
        self._record_count = 0
        self._f = open(filepath, "wb")
        self._write_header()

    def _write_header(self):
        schema_json = json.dumps(self.schema).encode("utf-8")
        meta = {
            "avro.schema": schema_json,
            "avro.codec": self.codec.encode("utf-8"),
        }
        header = AVRO_MAGIC
        # Meta map
        header += encode_long(len(meta))
        for k, v in meta.items():
            header += encode_string(k)
            header += encode_bytes(v)
        header += encode_long(0)  # end of map
        header += self.sync_marker
        self._f.write(header)

    def write(self, record: Dict):
        encoded = encode_record(record, self.schema, self.registry)
        self._buffer.append(encoded)
        self._record_count += 1
        if len(self._buffer) >= 100:
            self._flush_block()

    def _flush_block(self):
        if not self._buffer:
            return
        data = b"".join(self._buffer)
        count = len(self._buffer)
        block = encode_long(count) + encode_long(len(data)) + data + self.sync_marker
        self._f.write(block)
        self._buffer = []

    def close(self):
        self._flush_block()
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
