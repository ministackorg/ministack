# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Firehose delivery to Apache Iceberg tables registered in MiniStack's Glue Data Catalog.

Appends one buffer of JSON records to a format-version 2 table as one Iceberg commit: Parquet data files
per identity partition, a manifest, a manifest list that keeps the parent snapshot's manifests, a new
metadata.json, and a compare-and-swap of the Glue table's ``metadata_location``.

Table files are read from and written to MiniStack's own S3, or, when ``FIREHOSE_ICEBERG_S3_ENDPOINT`` is
set, to that S3-compatible endpoint (e.g. MinIO next to Trino), signed with
``FIREHOSE_ICEBERG_S3_ACCESS_KEY_ID`` / ``FIREHOSE_ICEBERG_S3_SECRET_ACCESS_KEY``.
"""

import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import os
import random
import re
import struct
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zlib

MALFORMED_CODE = "Iceberg.MalformedFieldWithinRecord"
MALFORMED_MESSAGE = ("Firehose is unable to convert column data in your record to the column type specified within "
                     "the schema.")
INVALID_JSON = ("Iceberg.InvalidPayload",
                "Delivery failed due to invalid input format. Make sure that the payload provided is in JSON Format.")
NOT_A_ROW = ("Iceberg.InvalidPayload", "Cannot de-serialize proper row from event data.")

# Held from the check that the table hasn't moved through writing the next metadata.json and swapping it in, so two
# Firehose commits never write the same metadata file name (``vN`` tables have no unique part).
_COMMIT_LOCK = threading.Lock()


class CommitConflict(Exception):
    pass


class MalformedRecord(Exception):
    pass


# ─── S3 I/O ───────────────────────────────────────────────────────────────────


def split_uri(uri):
    for scheme in ("s3://", "s3a://", "s3n://"):
        if uri.startswith(scheme):
            bucket, _, key = uri[len(scheme):].partition("/")
            return bucket, key
    raise ValueError(f"not an S3 location: {uri}")


def _signed_request(method, bucket, key, body=b""):
    endpoint = os.environ["FIREHOSE_ICEBERG_S3_ENDPOINT"].rstrip("/")
    access = os.environ.get("FIREHOSE_ICEBERG_S3_ACCESS_KEY_ID", "test")
    secret = os.environ.get("FIREHOSE_ICEBERG_S3_SECRET_ACCESS_KEY", "test")
    region = os.environ.get("FIREHOSE_ICEBERG_S3_REGION", "us-east-1")
    path = "/" + urllib.parse.quote(f"{bucket}/{key}", safe="/-_.~")
    host = urllib.parse.urlsplit(endpoint).netloc
    amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {"content-type": "application/octet-stream", "host": host, "x-amz-content-sha256": payload_hash,
               "x-amz-date": amz_date}
    signed = ";".join(sorted(headers))
    canonical = "\n".join([method, path, "", *(f"{k}:{headers[k]}" for k in sorted(headers)), "", signed,
                           payload_hash])
    scope = f"{amz_date[:8]}/{region}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = f"AWS4{secret}".encode()
    for part in (amz_date[:8], region, "s3", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, "
                                f"Signature={signature}")
    del headers["host"]
    request = urllib.request.Request(endpoint + path, data=body if method == "PUT" else None, method=method,
                                     headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def s3_get(uri):
    bucket, key = split_uri(uri)
    if os.environ.get("FIREHOSE_ICEBERG_S3_ENDPOINT"):
        return _signed_request("GET", bucket, key)
    from ministack.services import s3 as s3_svc

    data = s3_svc._get_object_data(bucket, key)
    if data is None:
        raise FileNotFoundError(uri)
    return data


def s3_put(uri, body):
    bucket, key = split_uri(uri)
    if os.environ.get("FIREHOSE_ICEBERG_S3_ENDPOINT"):
        _signed_request("PUT", bucket, key, body)
        return
    from ministack.services import s3 as s3_svc

    headers = {"content-type": "application/octet-stream", "content-length": str(len(body)), "host": "s3.localhost"}
    status, _, response = asyncio.run(
        s3_svc.handle_request("PUT", f"/{bucket}/{key}", headers, body, {}, internal=True))
    if status >= 300:
        raise OSError(f"S3 PUT {uri} failed with {status}: {response[:200]!r}")


# ─── Avro (object container files, the subset Iceberg manifests use) ──────────


def _zigzag(n):
    out = bytearray()
    n = (n << 1) ^ (n >> 63)
    while n & ~0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def _encode(schema, value, out):
    if isinstance(schema, list):
        branch = 0 if value is None and "null" in schema else next(i for i, s in enumerate(schema) if s != "null")
        out += _zigzag(branch)
        _encode(schema[branch], value, out)
        return
    kind = schema["type"] if isinstance(schema, dict) else schema
    if isinstance(kind, (dict, list)):
        _encode(kind, value, out)
    elif kind == "null":
        pass
    elif kind == "boolean":
        out.append(1 if value else 0)
    elif kind in ("int", "long"):
        out += _zigzag(int(value))
    elif kind == "float":
        out += struct.pack("<f", value)
    elif kind == "double":
        out += struct.pack("<d", value)
    elif kind in ("bytes", "string"):
        raw = value.encode() if isinstance(value, str) else bytes(value)
        out += _zigzag(len(raw)) + raw
    elif kind == "record":
        for field in schema["fields"]:
            _encode(field["type"], (value or {}).get(field["name"]), out)
    elif kind == "array":
        if value:
            out += _zigzag(len(value))
            for item in value:
                _encode(schema["items"], item, out)
        out += _zigzag(0)
    else:
        raise ValueError(f"unsupported Avro type {kind}")


class _Reader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def long(self):
        shift = result = 0
        while True:
            b = self.data[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return (result >> 1) ^ -(result & 1)
            shift += 7

    def take(self, n):
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk


def _decode(schema, r, named):
    if isinstance(schema, list):
        return _decode(schema[r.long()], r, named)
    if isinstance(schema, str) and schema in named:
        return _decode(named[schema], r, named)
    kind = schema["type"] if isinstance(schema, dict) else schema
    if isinstance(kind, (dict, list)):
        return _decode(kind, r, named)
    if kind == "null":
        return None
    if kind == "boolean":
        return r.take(1) == b"\x01"
    if kind in ("int", "long"):
        return r.long()
    if kind == "float":
        return struct.unpack("<f", r.take(4))[0]
    if kind == "double":
        return struct.unpack("<d", r.take(8))[0]
    if kind == "bytes":
        return r.take(r.long())
    if kind == "string":
        return r.take(r.long()).decode()
    if kind == "fixed":
        named.setdefault(schema["name"], schema)
        return r.take(schema["size"])
    if kind == "enum":
        named.setdefault(schema["name"], schema)
        return schema["symbols"][r.long()]
    if kind == "record":
        named.setdefault(schema["name"], schema)
        return {f["name"]: _decode(f["type"], r, named) for f in schema["fields"]}
    if kind in ("array", "map"):
        items = [] if kind == "array" else {}
        while (count := r.long()) != 0:
            if count < 0:
                count = -count
                r.long()
            for _ in range(count):
                if kind == "array":
                    items.append(_decode(schema["items"], r, named))
                else:
                    key = r.take(r.long()).decode()
                    items[key] = _decode(schema["values"], r, named)
        return items
    raise ValueError(f"unsupported Avro type {kind}")


_AVRO_MAGIC = b"Obj\x01"
_META_SCHEMA = {"type": "map", "values": "bytes"}


def avro_read(data):
    """Records of an Avro object container file (null or deflate codec)."""
    r = _Reader(data)
    if r.take(4) != _AVRO_MAGIC:
        raise ValueError("not an Avro file")
    meta = _decode(_META_SCHEMA, r, {})
    schema = json.loads(meta["avro.schema"])
    codec = meta.get("avro.codec", b"null").decode()
    if codec not in ("null", "deflate"):
        raise ValueError(f"unsupported Avro codec {codec}")
    sync = r.take(16)
    records = []
    while r.pos < len(data):
        count = r.long()
        block = r.take(r.long())
        if codec == "deflate":
            block = zlib.decompress(block, -15)
        br = _Reader(block)
        records.extend(_decode(schema, br, {}) for _ in range(count))
        if r.take(16) != sync:
            raise ValueError("Avro sync marker mismatch")
    return records


def avro_write(schema, records, metadata):
    out = bytearray(_AVRO_MAGIC)
    meta = {**metadata, "avro.schema": json.dumps(schema), "avro.codec": "null"}
    _encode({"type": "array", "items": {"type": "record", "name": "kv", "fields": [
        {"name": "k", "type": "string"}, {"name": "v", "type": "bytes"}]}},
        [{"k": k, "v": v.encode() if isinstance(v, str) else v} for k, v in meta.items()], out)
    sync = os.urandom(16)
    out += sync
    block = bytearray()
    for record in records:
        _encode(schema, record, block)
    out += _zigzag(len(records)) + _zigzag(len(block)) + block + sync
    return bytes(out)


# A map is written as an array of {key, value} pairs: the bytes are the same as an Avro map's.
def _avro_map(field_id, name, key_id, value_id, value_type):
    return {"name": name, "field-id": field_id, "default": None, "type": ["null", {
        "type": "array", "logicalType": "map", "items": {
            "type": "record", "name": f"k{key_id}_v{value_id}", "fields": [
                {"name": "key", "type": "int", "field-id": key_id},
                {"name": "value", "type": value_type, "field-id": value_id}]}}]}


def _manifest_entry_schema(partition_fields):
    return {"type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "default": None, "field-id": 1},
        {"name": "sequence_number", "type": ["null", "long"], "default": None, "field-id": 3},
        {"name": "file_sequence_number", "type": ["null", "long"], "default": None, "field-id": 4},
        {"name": "data_file", "field-id": 2, "type": {"type": "record", "name": "r2", "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "field-id": 102,
             "type": {"type": "record", "name": "r102", "fields": partition_fields}},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            _avro_map(108, "column_sizes", 117, 118, "long"),
            _avro_map(109, "value_counts", 119, 120, "long"),
            _avro_map(110, "null_value_counts", 121, 122, "long"),
            _avro_map(137, "nan_value_counts", 138, 139, "long"),
            _avro_map(125, "lower_bounds", 126, 127, "bytes"),
            _avro_map(128, "upper_bounds", 129, 130, "bytes"),
            {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 131},
            {"name": "split_offsets", "default": None, "field-id": 132,
             "type": ["null", {"type": "array", "items": "long", "element-id": 133}]},
            {"name": "equality_ids", "default": None, "field-id": 135,
             "type": ["null", {"type": "array", "items": "int", "element-id": 136}]},
            {"name": "sort_order_id", "type": ["null", "int"], "default": None, "field-id": 140},
        ]}},
    ]}


_MANIFEST_FILE_SCHEMA = {"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "default": None, "field-id": 507, "type": ["null", {
        "type": "array", "element-id": 508, "items": {"type": "record", "name": "r508", "fields": [
            {"name": "contains_null", "type": "boolean", "field-id": 509},
            {"name": "contains_nan", "type": ["null", "boolean"], "default": None, "field-id": 518},
            {"name": "lower_bound", "type": ["null", "bytes"], "default": None, "field-id": 510},
            {"name": "upper_bound", "type": ["null", "bytes"], "default": None, "field-id": 511}]}}]},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 519},
]}


# ─── Iceberg types: JSON value → Python value, Avro type, single-value bytes ──

# Firehose keeps the local date-time of an ISO string and ignores a trailing Z or offset; seconds and the
# fraction are optional and digits past microseconds are dropped. A space instead of "T" is malformed.
_TS_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?(Z|[+-]\d{2}:\d{2})?")
_AVRO_PRIMITIVE = {
    "boolean": "boolean", "int": "int", "long": "long", "float": "float", "double": "double", "string": "string",
    "date": {"type": "int", "logicalType": "date"},
    "timestamp": {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": False},
    "timestamptz": {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": True},
}
_DUCKDB_TYPE = {"boolean": "BOOLEAN", "int": "INTEGER", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
                "string": "VARCHAR", "date": "DATE", "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMPTZ"}
_EPOCH = datetime.datetime(1970, 1, 1)
_EPOCH_TZ = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _parse_timestamp(value, with_zone):
    if isinstance(value, int) and not isinstance(value, bool):
        base = _EPOCH_TZ if with_zone else _EPOCH
        return base + datetime.timedelta(microseconds=value)
    m = _TS_RE.fullmatch(value) if isinstance(value, str) else None
    if not m:
        raise MalformedRecord(value)
    year, month, day, hour, minute, second = (int(g or 0) for g in m.groups()[:6])
    micros = int((m.group(7) or "").ljust(6, "0")[:6])
    try:
        parsed = datetime.datetime(year, month, day, hour, minute, second, micros)
    except ValueError as exc:
        raise MalformedRecord(value) from exc
    return parsed.replace(tzinfo=datetime.timezone.utc) if with_zone else parsed


def convert(iceberg_type, value):
    """A JSON value as the Python value DuckDB writes for this column type; MalformedRecord if it can't be."""
    if value is None:
        return None
    if iceberg_type == "string":
        text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        text.encode()  # a lone surrogate from a JSON "\ud800" escape can't be written as UTF-8
        return text
    if iceberg_type in ("timestamp", "timestamptz"):
        return _parse_timestamp(value, iceberg_type == "timestamptz")
    if iceberg_type == "boolean":
        if isinstance(value, bool):
            return value
    elif iceberg_type in ("int", "long"):
        if isinstance(value, int) and not isinstance(value, bool):
            limit = 2 ** 31 if iceberg_type == "int" else 2 ** 63
            if -limit <= value < limit:
                return value
    elif iceberg_type in ("float", "double"):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if iceberg_type == "double" or abs(number) <= 3.4028234663852886e38 or number != number:
                return number
    elif iceberg_type == "date" and isinstance(value, str):
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass
    raise MalformedRecord(value)


def _single_value(iceberg_type, value):
    """Iceberg's binary single-value serialization, used for bounds."""
    if iceberg_type == "string":
        return value.encode()
    if iceberg_type == "boolean":
        return b"\x01" if value else b"\x00"
    if iceberg_type == "int":
        return struct.pack("<i", value)
    if iceberg_type == "long":
        return struct.pack("<q", value)
    if iceberg_type == "float":
        return struct.pack("<f", value)
    if iceberg_type == "double":
        return struct.pack("<d", value)
    if iceberg_type == "date":
        return struct.pack("<i", (value - _EPOCH.date()).days)
    return struct.pack("<q", _micros(value))


def _micros(value):
    delta = value - (_EPOCH_TZ if value.tzinfo else _EPOCH)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _avro_partition_value(iceberg_type, value):
    if value is None:
        return None
    if iceberg_type == "date":
        return (value - _EPOCH.date()).days
    if iceberg_type in ("timestamp", "timestamptz"):
        return _micros(value)
    return value


def _partition_path_value(iceberg_type, value):
    if value is None:
        return "null"
    if iceberg_type in ("timestamp", "timestamptz"):
        return value.isoformat()
    if iceberg_type == "boolean":
        return "true" if value else "false"
    return str(value)


# Iceberg's default write.metadata.metrics.default is truncate(16).
_TRUNCATE = 16


def _bounds(iceberg_type, values):
    present = [v for v in values if v is not None and not (isinstance(v, float) and v != v)]
    if not present:
        return None, None
    low, high = min(present), max(present)
    if iceberg_type != "string":
        return _single_value(iceberg_type, low), _single_value(iceberg_type, high)
    low = low[:_TRUNCATE]
    if len(high) > _TRUNCATE:
        chars = list(high[:_TRUNCATE])
        while chars:
            code = ord(chars[-1]) + 1
            if code <= 0x10FFFF and not 0xD800 <= code <= 0xDFFF:
                chars[-1] = chr(code)
                break
            chars.pop()
        if not chars:
            return low.encode(), None
        high = "".join(chars)
    return low.encode(), high.encode()


# ─── the commit ───────────────────────────────────────────────────────────────


def _current_schema(metadata):
    schema_id = metadata.get("current-schema-id", 0)
    return next(s for s in metadata["schemas"] if s.get("schema-id", 0) == schema_id)


def _default_spec(metadata):
    spec_id = metadata.get("default-spec-id", 0)
    return next(s for s in metadata["partition-specs"] if s.get("spec-id", 0) == spec_id)


def _next_metadata_name(location):
    base = location.rsplit("/", 1)[-1]
    m = re.match(r"(\d+)-", base)
    if m:
        return f"{int(m.group(1)) + 1:05d}-{uuid.uuid4()}.metadata.json"
    m = re.match(r"v(\d+)\.", base)
    if m:
        return f"v{int(m.group(1)) + 1}.metadata.json"
    return f"00001-{uuid.uuid4()}.metadata.json"


def glue_table(database, table):
    from ministack.services import glue as glue_svc

    entry = glue_svc._tables.get(f"{database}/{table}")
    location = ((entry or {}).get("Parameters") or {}).get("metadata_location")
    if not location:
        raise LookupError(f"Iceberg table {database}.{table} not found in the Glue Data Catalog")
    return location


def _first_json_value(raw):
    """The first JSON value of a record; Firehose ignores anything after it."""
    text = raw.decode("utf-8")
    return json.JSONDecoder().raw_decode(text, len(text) - len(text.lstrip()))[0]


def prepare_rows(metadata, records):
    """Split ``records`` [(raw bytes, arrival ms)] into rows in the table's schema and error-output documents."""
    columns = _current_schema(metadata)["fields"]
    for column in columns:
        if not isinstance(column["type"], str) or column["type"] not in _DUCKDB_TYPE:
            raise ValueError(f"column {column['name']} has type {column['type']}, which MiniStack's Firehose "
                             "cannot write")
    rows, failed = [], []
    for raw, arrival_ms in records:
        try:
            document = _first_json_value(raw)
        except (ValueError, UnicodeDecodeError):
            failed.append(_error_document(raw, arrival_ms, *INVALID_JSON))
            continue
        if not isinstance(document, dict):
            failed.append(_error_document(raw, arrival_ms, *NOT_A_ROW))
            continue
        try:
            row = {}
            for column in columns:
                value = convert(column["type"], document.get(column["name"]))
                if value is None and column.get("required"):
                    raise MalformedRecord(column["name"])
                row[column["name"]] = value
            rows.append(row)
        except (MalformedRecord, OverflowError, ValueError, UnicodeError):
            failed.append(_error_document(raw, arrival_ms, MALFORMED_CODE, MALFORMED_MESSAGE, attempted=True))
    return rows, failed


def _write_data_files(metadata, rows, sequence):
    """One Parquet file per partition; returns the manifest's data_file records and the changed partitions."""
    import duckdb

    columns = _current_schema(metadata)["fields"]
    by_id = {c["id"]: c for c in columns}
    spec = _default_spec(metadata)
    for field in spec["fields"]:
        if field["transform"] != "identity":
            raise ValueError(f"partition transform {field['transform']} is not supported by MiniStack's Firehose")
    groups = {}
    for row in rows:
        key = tuple(row[by_id[f["source-id"]]["name"]] for f in spec["fields"])
        groups.setdefault(key, []).append(row)

    data_path = metadata.get("properties", {}).get("write.data.path") or f"{metadata['location'].rstrip('/')}/data"
    files = []
    def ident(name):
        return '"' + name.replace('"', '""') + '"'

    select = ", ".join(f'unnest(${i + 1}::{_DUCKDB_TYPE[c["type"]]}[]) AS {ident(c["name"])}'
                       for i, c in enumerate(columns))
    field_ids = ", ".join(f'{ident(c["name"])}: {c["id"]}' for c in columns)
    query_id = uuid.uuid4()
    with tempfile.TemporaryDirectory() as tmp:
        con = duckdb.connect()
        try:
            for n, (key, group) in enumerate(groups.items(), start=1):
                local = os.path.join(tmp, f"{n}.parquet")
                values = [[row[c["name"]] for row in group] for c in columns]
                con.execute(f"COPY (SELECT {select}) TO '{local}' (FORMAT parquet, COMPRESSION zstd, "
                            f"FIELD_IDS {{{field_ids}}})", values)
                with open(local, "rb") as fh:
                    body = fh.read()
                partition_dir = "/".join(
                    f"{f['name']}={urllib.parse.quote(_partition_path_value(by_id[f['source-id']]['type'], v), safe='')}"
                    for f, v in zip(spec["fields"], key))
                name = f"{sequence:05d}-{n}-{query_id}-{n:05d}.parquet"
                path = f"{data_path}/{partition_dir}/{name}" if partition_dir else f"{data_path}/{name}"
                s3_put(path, body)
                lower, upper = {}, {}
                for c, column_values in zip(columns, values):
                    low, high = _bounds(c["type"], column_values)
                    if low is not None:
                        lower[c["id"]] = low
                    if high is not None:
                        upper[c["id"]] = high
                files.append({
                    "content": 0, "file_path": path, "file_format": "PARQUET",
                    "partition": {f["name"]: _avro_partition_value(by_id[f["source-id"]]["type"], v)
                                  for f, v in zip(spec["fields"], key)},
                    "record_count": len(group), "file_size_in_bytes": len(body),
                    "value_counts": [{"key": c["id"], "value": len(group)} for c in columns],
                    "null_value_counts": [{"key": c["id"], "value": sum(v is None for v in vals)}
                                          for c, vals in zip(columns, values)],
                    "lower_bounds": [{"key": k, "value": v} for k, v in lower.items()],
                    "upper_bounds": [{"key": k, "value": v} for k, v in upper.items()],
                    "_key": key,
                })
        finally:
            con.close()
    return files


def _partition_summaries(metadata, files):
    columns = {c["id"]: c for c in _current_schema(metadata)["fields"]}
    summaries = []
    for i, field in enumerate(_default_spec(metadata)["fields"]):
        iceberg_type = columns[field["source-id"]]["type"]
        values = [f["_key"][i] for f in files]
        present = [v for v in values if v is not None]
        summaries.append({"contains_null": len(present) < len(values), "contains_nan": False,
                          "lower_bound": _single_value(iceberg_type, min(present)) if present else None,
                          "upper_bound": _single_value(iceberg_type, max(present)) if present else None})
    return summaries


def append(database, table, rows, summary_extra):
    """Append ``rows`` to the table as one snapshot; returns the new metadata location. Retries Glue conflicts."""
    location = glue_table(database, table)
    metadata = json.loads(s3_get(location))
    if metadata.get("format-version") != 2:
        raise ValueError(f"{database}.{table} is Iceberg format-version {metadata.get('format-version')}; "
                         "MiniStack's Firehose writes format-version 2 tables")
    base = metadata["location"].rstrip("/")
    files = _write_data_files(metadata, rows, metadata.get("last-sequence-number", 0) + 1)
    snapshot_id = random.getrandbits(63) or 1
    spec = _default_spec(metadata)
    schema = _current_schema(metadata)
    columns = {c["id"]: c for c in _current_schema(metadata)["fields"]}
    partition_fields = [{"name": f["name"], "field-id": f["field-id"], "default": None,
                         "type": ["null", _AVRO_PRIMITIVE[columns[f["source-id"]]["type"]]]} for f in spec["fields"]]
    entry_schema = _manifest_entry_schema(partition_fields)
    manifest = avro_write(entry_schema, [
        {"status": 1, "snapshot_id": snapshot_id, "data_file": {k: v for k, v in f.items() if k != "_key"}}
        for f in files], {
        "schema": json.dumps(_current_schema(metadata)), "schema-id": str(_current_schema(metadata).get("schema-id", 0)),
        "partition-spec": json.dumps(spec["fields"]), "partition-spec-id": str(spec.get("spec-id", 0)),
        "format-version": "2", "content": "data"})
    manifest_path = f"{base}/metadata/{uuid.uuid4()}-m0.avro"
    s3_put(manifest_path, manifest)
    added_records = sum(f["record_count"] for f in files)
    added_size = sum(f["file_size_in_bytes"] for f in files)

    retries = int(metadata.get("properties", {}).get("commit.retry.num-retries", 4))
    for attempt in range(retries + 1):
        if attempt:
            location = glue_table(database, table)
            metadata = json.loads(s3_get(location))
        sequence = metadata.get("last-sequence-number", 0) + 1
        parent_id = metadata.get("current-snapshot-id")
        parent = next((s for s in metadata.get("snapshots", []) if s["snapshot-id"] == parent_id), None)
        if parent is None and parent_id not in (None, -1):
            raise ValueError(f"{database}.{table}: current snapshot {parent_id} is not in its metadata")
        if (_current_schema(metadata) != schema or _default_spec(metadata) != spec):
            raise ValueError(f"{database}.{table}: schema or partition spec changed during the commit")
        existing = avro_read(s3_get(parent["manifest-list"])) if parent else []
        manifest_entry = {
            "manifest_path": manifest_path, "manifest_length": len(manifest),
            "partition_spec_id": spec.get("spec-id", 0), "content": 0, "sequence_number": sequence,
            "min_sequence_number": sequence, "added_snapshot_id": snapshot_id, "added_files_count": len(files),
            "existing_files_count": 0, "deleted_files_count": 0, "added_rows_count": added_records,
            "existing_rows_count": 0, "deleted_rows_count": 0,
            "partitions": _partition_summaries(metadata, files)}
        manifest_list_path = f"{base}/metadata/snap-{snapshot_id}-1-{uuid.uuid4()}.avro"
        s3_put(manifest_list_path, avro_write(_MANIFEST_FILE_SCHEMA, [manifest_entry, *existing], {
            "snapshot-id": str(snapshot_id), "parent-snapshot-id": str(parent_id if parent else "null"),
            "sequence-number": str(sequence), "format-version": "2"}))

        totals = (parent or {}).get("summary", {})
        now_ms = int(time.time() * 1000)
        summary = {"operation": "append", **summary_extra,
                   "added-data-files": str(len(files)), "added-records": str(added_records),
                   "added-files-size": str(added_size), "changed-partition-count": str(len(files)),
                   "total-records": str(int(totals.get("total-records", 0)) + added_records),
                   "total-files-size": str(int(totals.get("total-files-size", 0)) + added_size),
                   "total-data-files": str(int(totals.get("total-data-files", 0)) + len(files)),
                   "total-delete-files": totals.get("total-delete-files", "0"),
                   "total-position-deletes": totals.get("total-position-deletes", "0"),
                   "total-equality-deletes": totals.get("total-equality-deletes", "0")}
        snapshot = {"sequence-number": sequence, "snapshot-id": snapshot_id, "timestamp-ms": now_ms,
                    "summary": summary, "manifest-list": manifest_list_path,
                    "schema-id": _current_schema(metadata).get("schema-id", 0)}
        if parent:
            snapshot["parent-snapshot-id"] = parent_id
        new = dict(metadata)
        new["last-sequence-number"] = sequence
        new["last-updated-ms"] = now_ms
        new["current-snapshot-id"] = snapshot_id
        new["snapshots"] = [*metadata.get("snapshots", []), snapshot]
        new["snapshot-log"] = [*metadata.get("snapshot-log", []), {"timestamp-ms": now_ms, "snapshot-id": snapshot_id}]
        new["refs"] = {**metadata.get("refs", {}), "main": {"snapshot-id": snapshot_id, "type": "branch"}}
        keep = int(metadata.get("properties", {}).get("write.metadata.previous-versions-max", 100))
        new["metadata-log"] = [*metadata.get("metadata-log", []),
                               {"timestamp-ms": metadata.get("last-updated-ms", now_ms), "metadata-file": location}
                               ][-keep:]
        new_location = f"{base}/metadata/{_next_metadata_name(location)}"
        from ministack.services import glue as glue_svc

        with _COMMIT_LOCK:
            if glue_table(database, table) == location:
                s3_put(new_location, json.dumps(new).encode())
                if glue_svc.swap_iceberg_metadata_location(database, table, location, new_location):
                    return new_location
        if attempt == retries:
            raise CommitConflict(f"{database}.{table} kept changing; gave up after {retries + 1} attempts")
        time.sleep(min(0.1 * 2 ** attempt, 2))


def _error_document(raw, arrival_ms, code, message, attempted=False):
    # A payload that isn't a JSON object is rejected before any attempt: AWS reports 0 attempts and a 0 end time.
    return {"attemptsMade": 1 if attempted else 0, "arrivalTimestamp": arrival_ms, "errorCode": code,
            "errorMessage": message, "attemptEndingTimestamp": int(time.time() * 1000) if attempted else 0,
            "rawData": base64.b64encode(raw).decode()}
