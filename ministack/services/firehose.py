# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon Data Firehose (formerly Kinesis Data Firehose) Emulator.
JSON-based API via X-Amz-Target (Firehose_20150804).

Supports:
  CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream,
  ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination,
  TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream,
  StartDeliveryStreamEncryption, StopDeliveryStreamEncryption.

Destinations supported: ExtendedS3, S3 (deprecated alias), HttpEndpoint.
Records put to an S3 destination are written synchronously to the local S3
emulator (bucket must already exist).  All other destinations buffer records
in-memory (accessible for testing via PutRecord/PutRecordBatch round-trip).
"""

import asyncio
import base64
import copy
import json
import logging
import os
import re
import threading
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import run_reentrant, spawn_background
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_epoch,
)

logger = logging.getLogger("firehose")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_S3_BUCKET_NAME_RE = re.compile(
    r"^(?!\d+\.\d+\.\d+\.\d+$)(?!.*\.\.)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
)

# ─── in-memory state ──────────────────────────────────────────────────────────

_streams = AccountRegionScopedDict()  # (account, region, name) -> stream descriptor
_lock = threading.Lock()
_dest_counter = 0


def reset():
    global _dest_counter
    with _lock:
        _streams.clear()
        _dest_counter = 0


def get_state() -> dict:
    return copy.deepcopy({"_streams": _streams, "_dest_counter": _dest_counter})


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data: dict):
    global _dest_counter
    _streams.update(data.get("_streams", {}))
    _dest_counter = data.get("_dest_counter", _dest_counter)




# ─── helpers ─────────────────────────────────────────────────────────────────


def _stream_arn(name: str) -> str:
    return f"arn:aws:firehose:{get_region()}:{get_account_id()}:deliverystream/{name}"


def _s3_bucket_from_arn(bucket_arn: str) -> str:
    try:
        spec = parse_arn(bucket_arn)
    except ArnParseError as exc:
        raise ValueError("BucketARN must be an S3 bucket ARN.") from exc

    if (
        spec.service != "s3"
        or spec.region
        or spec.account_id
        or not spec.resource
        or "/" in spec.resource
        or not _S3_BUCKET_NAME_RE.fullmatch(spec.resource)
    ):
        raise ValueError("BucketARN must be an S3 bucket ARN.")
    return spec.resource


def _validate_s3_destination_config(dtype: str, cfg: dict):
    if dtype not in ("ExtendedS3", "S3") or "BucketARN" not in cfg:
        return None
    try:
        _s3_bucket_from_arn(cfg.get("BucketARN", ""))
    except ValueError as exc:
        return _invalid(str(exc))
    return None


def _next_dest_id() -> str:
    """Must be called while holding _lock."""
    global _dest_counter
    _dest_counter += 1
    return f"destinationId-{_dest_counter:012d}"


def _not_found(name: str):
    return error_response_json(
        "ResourceNotFoundException",
        f"Firehose {name} under account {get_account_id()} not found.",
        400,
    )


def _in_use(name: str):
    return error_response_json(
        "ResourceInUseException",
        f"Firehose {name} is not in the ACTIVE state.",
        400,
    )


def _invalid(msg: str):
    return error_response_json("InvalidArgumentException", msg, 400)


def _dest_description(dest: dict) -> dict:
    """Return the destination description block for DescribeDeliveryStream."""
    dtype = dest["type"]
    out = {"DestinationId": dest["id"]}
    if dtype in ("ExtendedS3", "S3"):
        key = "ExtendedS3DestinationDescription" if dtype == "ExtendedS3" else "S3DestinationDescription"
        cfg = dest["config"]
        desc = {
            "BucketARN": cfg.get("BucketARN", ""),
            "RoleARN": cfg.get("RoleARN", ""),
            "BufferingHints": cfg.get("BufferingHints", {"SizeInMBs": 5, "IntervalInSeconds": 300}),
            "CompressionFormat": cfg.get("CompressionFormat", "UNCOMPRESSED"),
            "EncryptionConfiguration": cfg.get("EncryptionConfiguration", {"NoEncryptionConfig": "NoEncryption"}),
            "Prefix": cfg.get("Prefix", ""),
            "ErrorOutputPrefix": cfg.get("ErrorOutputPrefix", ""),
            "S3BackupMode": cfg.get("S3BackupMode", "Disabled"),
        }
        for opt in (
            "ProcessingConfiguration",
            "CloudWatchLoggingOptions",
            "DataFormatConversionConfiguration",
            "DynamicPartitioningConfiguration",
        ):
            if opt in cfg:
                desc[opt] = cfg[opt]
        out[key] = desc
    elif dtype == "HttpEndpoint":
        cfg = dest["config"]
        out["HttpEndpointDestinationDescription"] = {
            "EndpointConfiguration": cfg.get("EndpointConfiguration", {}),
            "BufferingHints": cfg.get("BufferingHints", {"SizeInMBs": 5, "IntervalInSeconds": 300}),
            "S3BackupMode": cfg.get("S3BackupMode", "FailedDataOnly"),
        }
    else:
        out[f"{dtype}DestinationDescription"] = dest["config"]
    return out


def _build_description(stream: dict) -> dict:
    desc = {
        "DeliveryStreamName": stream["name"],
        "DeliveryStreamARN": stream["arn"],
        "DeliveryStreamStatus": stream["status"],
        "DeliveryStreamType": stream["type"],
        "VersionId": str(stream["version"]),
        "CreateTimestamp": stream["created_at"],
        "LastUpdateTimestamp": stream["updated_at"],
        "HasMoreDestinations": False,
        "Destinations": [_dest_description(d) for d in stream["destinations"]],
    }
    enc = stream.get("encryption")
    if enc:
        desc["DeliveryStreamEncryptionConfiguration"] = enc
    # Source block — only present for non-DirectPut streams
    if stream["type"] == "KinesisStreamAsSource" and stream.get("kinesis_source"):
        desc["Source"] = {"KinesisStreamSourceDescription": stream["kinesis_source"]}
    return desc


def _resolve_dest_type_and_config(data: dict):
    """Extract destination type and config from CreateDeliveryStream / UpdateDestination request."""
    for key, dtype in (
        ("ExtendedS3DestinationConfiguration", "ExtendedS3"),
        ("S3DestinationConfiguration", "S3"),
        ("HttpEndpointDestinationConfiguration", "HttpEndpoint"),
        ("RedshiftDestinationConfiguration", "Redshift"),
        ("ElasticsearchDestinationConfiguration", "Elasticsearch"),
        ("AmazonopensearchserviceDestinationConfiguration", "AmazonOpenSearch"),
        ("AmazonOpenSearchServerlessDestinationConfiguration", "AmazonOpenSearchServerless"),
        ("SplunkDestinationConfiguration", "Splunk"),
        ("SnowflakeDestinationConfiguration", "Snowflake"),
        ("IcebergDestinationConfiguration", "Iceberg"),
    ):
        if key in data:
            return dtype, data[key]
    return None, None


def _resolve_dest_update_config(data: dict):
    """Extract destination type and config from UpdateDestination request."""
    for key, dtype in (
        ("ExtendedS3DestinationUpdate", "ExtendedS3"),
        ("S3DestinationUpdate", "S3"),
        ("HttpEndpointDestinationUpdate", "HttpEndpoint"),
        ("RedshiftDestinationUpdate", "Redshift"),
        ("ElasticsearchDestinationUpdate", "Elasticsearch"),
        ("AmazonopensearchserviceDestinationUpdate", "AmazonOpenSearch"),
        ("AmazonOpenSearchServerlessDestinationUpdate", "AmazonOpenSearchServerless"),
        ("SplunkDestinationUpdate", "Splunk"),
        ("SnowflakeDestinationUpdate", "Snowflake"),
        ("IcebergDestinationUpdate", "Iceberg"),
    ):
        if key in data:
            return dtype, data[key]
    return None, None


def _apply_lambda_processors(stream: dict, dest: dict, records: list, metadata_sink: dict = None,
                             partition_sink: dict = None) -> list:
    """Apply a destination's ProcessingConfiguration Lambda processors to a
    batch of records.

    ``records``: list of ``(recordId, raw_bytes)`` — pre-decoded so the
    Lambda invocation matches AWS's contract (base64 over the wire).

    Returns the post-processing list of ``(recordId, raw_bytes)`` to deliver
    downstream. Per the AWS Firehose Lambda processor contract:
      - ``result == "Ok"`` → use the Lambda's returned (base64) data.
      - ``result == "Dropped"`` → omit from the output entirely.
      - ``result == "ProcessingFailed"`` → omit; AWS routes to the
        S3 backup destination if configured (ministack: omit + warn).

    On any Lambda lookup / invocation / response-parsing error the original
    record is passed through. Firehose is best-effort by AWS contract — a
    processor problem must never break the producer side.
    """
    cfg = (dest.get("config") or {}).get("ProcessingConfiguration") or {}
    if not cfg.get("Enabled"):
        return records
    processors = cfg.get("Processors") or []
    lambda_arns = []
    for proc in processors:
        if proc.get("Type") != "Lambda":
            continue
        for p in proc.get("Parameters") or []:
            if p.get("ParameterName") == "LambdaArn":
                arn = p.get("ParameterValue", "")
                if arn:
                    lambda_arns.append(arn)
                break
    if not lambda_arns:
        return records

    from ministack.services import lambda_svc

    current = list(records)
    stream_arn = stream.get("arn", "")
    stream_name = stream.get("name", "?")
    for arn in lambda_arns:
        try:
            name, qualifier = lambda_svc._resolve_name_and_qualifier(arn)
            func_record, _ = lambda_svc._get_func_record_for_qualifier(name, qualifier)
        except Exception:
            func_record = None
        if func_record is None:
            logger.warning(
                "Firehose %s: processor Lambda %s not found; passing records through",
                stream_name,
                arn,
            )
            continue
        now_ms = int(time.time() * 1000)
        event = {
            "invocationId": new_uuid(),
            "deliveryStreamArn": stream_arn,
            "region": get_region(),
            "records": [
                {
                    "recordId": rid,
                    "approximateArrivalTimestamp": now_ms,
                    "data": base64.b64encode(raw).decode(),
                }
                for rid, raw in current
            ],
        }
        try:
            result = lambda_svc._execute_function(func_record, event)
        except Exception as exc:
            logger.warning(
                "Firehose %s: processor Lambda %s invocation failed: %s; passing through", stream_name, arn, exc
            )
            continue
        if result.get("error"):
            logger.warning(
                "Firehose %s: processor Lambda %s returned error: %s; passing through",
                stream_name,
                arn,
                result.get("body"),
            )
            continue
        body = result.get("body")
        if isinstance(body, (str, bytes)):
            try:
                body = json.loads(body)
            except (ValueError, TypeError):
                body = None
        if not isinstance(body, dict) or "records" not in body:
            logger.warning(
                "Firehose %s: processor Lambda %s returned malformed body; passing through", stream_name, arn
            )
            continue
        by_id = {r.get("recordId"): r for r in body.get("records", []) if isinstance(r, dict)}
        next_round = []
        for rid, raw in current:
            r = by_id.get(rid)
            if r is None:
                # Lambda didn't include this recordId — treat as pass-through
                # rather than silently dropping.
                next_round.append((rid, raw))
                continue
            # Iceberg routing: capture the record's otfMetadata
            # (destinationDatabaseName / destinationTableName / operation) that
            # a transform Lambda attaches under `metadata.otfMetadata`.
            if metadata_sink is not None:
                otf = (r.get("metadata") or {}).get("otfMetadata")
                if isinstance(otf, dict):
                    metadata_sink[rid] = otf
            if partition_sink is not None:
                keys = (r.get("metadata") or {}).get("partitionKeys")
                if isinstance(keys, dict):
                    partition_sink[rid] = {str(k): str(v) for k, v in keys.items()}
            outcome = r.get("result", "Ok")
            if outcome in ("Dropped", "ProcessingFailed"):
                continue
            new_data_b64 = r.get("data")
            if new_data_b64:
                try:
                    next_round.append((rid, base64.b64decode(new_data_b64)))
                    continue
                except Exception:
                    pass
            next_round.append((rid, raw))
        current = next_round
        if not current:
            break
    return current


# ─── S3 delivery: prefixes, dynamic partitioning, record format conversion ────


class _DeliveryError(Exception):
    """A record goes to the error prefix with this error-output-type."""

    def __init__(self, output_type, message, code=None):
        super().__init__(message)
        self.output_type = output_type
        self.message = message
        self.code = code


_PREFIX_EXPR_RE = re.compile(r"!\{([A-Za-z]+):([^}]*)\}")
_MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September",
           "October", "November", "December")


def _java_time(pattern, t):
    """Format ``t`` (struct_time) with a Java DateTimeFormatter pattern: y M d H m s D, 'quoted' literals."""
    out, i = [], 0
    while i < len(pattern):
        c = pattern[i]
        if c == "'":
            end = pattern.find("'", i + 1)
            if end == i + 1:
                out.append("'")
                i += 2
                continue
            out.append(pattern[i + 1:] if end < 0 else pattern[i + 1:end])
            i = len(pattern) if end < 0 else end + 1
            continue
        if c in "yMdHmsD":
            n = 1
            while i + n < len(pattern) and pattern[i + n] == c:
                n += 1
            value = {"y": t.tm_year, "M": t.tm_mon, "d": t.tm_mday, "H": t.tm_hour,
                     "m": t.tm_min, "s": t.tm_sec, "D": t.tm_yday}[c]
            if c == "y" and n == 2:
                out.append(f"{value % 100:02d}")
            elif c == "M" and n >= 3:
                out.append(_MONTHS[value - 1][:3] if n == 3 else _MONTHS[value - 1])
            else:
                out.append(str(value).zfill(n))
            i += n
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _evaluate_prefix(template, t, query_keys=None, lambda_keys=None, error_type=None):
    """Evaluate a Prefix / ErrorOutputPrefix; ``yyyy/MM/dd/HH/`` is appended when it has no timestamp expression."""
    has_timestamp = False

    def expand(match):
        nonlocal has_timestamp
        namespace, value = match.group(1), match.group(2)
        if namespace == "timestamp":
            has_timestamp = True
            return _java_time(value, t)
        if namespace == "firehose" and value == "random-string":
            return new_uuid().replace("-", "")[:11]
        if namespace == "firehose" and value == "error-output-type" and error_type:
            return error_type
        keys = {"partitionKeyFromQuery": query_keys, "partitionKeyFromLambda": lambda_keys}.get(namespace)
        if keys is not None and value in keys:
            return keys[value]
        raise _DeliveryError("processing-failed", f"Cannot evaluate !{{{namespace}:{value}}} in the S3 prefix.")

    evaluated = _PREFIX_EXPR_RE.sub(expand, template or "")
    if not has_timestamp:
        evaluated += _java_time("yyyy/MM/dd/HH/", t)
    return evaluated


# jq 1.6, the subset Firehose metadata extraction uses: an object of
# key: .path (fields, ["key"], [n], [m:n]) optionally piped to strftime("fmt").
_JQ_TOKEN_RE = re.compile(r'\s*(?:("(?:[^"\\]|\\.)*")|(-?\d+)|([A-Za-z_][A-Za-z0-9_]*)|([{}\[\]:,.|()]))')


def _jq_tokens(expression):
    tokens, pos = [], 0
    while pos < len(expression):
        if expression[pos:].strip() == "":
            break
        match = _JQ_TOKEN_RE.match(expression, pos)
        if not match:
            raise ValueError(f"unsupported jq syntax at {expression[pos:]!r}")
        string, number, ident, punct = match.groups()
        if string is not None:
            tokens.append(("str", json.loads(string)))
        elif number is not None:
            tokens.append(("num", int(number)))
        elif ident is not None:
            tokens.append(("ident", ident))
        else:
            tokens.append(("punct", punct))
        pos = match.end()
    return tokens


def _jq_parse(expression):
    """Parse ``{key: .path | strftime("fmt"), ...}`` into ``[(key, steps, functions)]``."""
    tokens, i = _jq_tokens(expression), 0

    def peek(kind=None, value=None):
        if i >= len(tokens):
            return False
        tk, tv = tokens[i]
        return (kind is None or tk == kind) and (value is None or tv == value)

    def take(kind=None, value=None):
        nonlocal i
        if not peek(kind, value):
            raise ValueError(f"unsupported jq expression: {expression!r}")
        i += 1
        return tokens[i - 1][1]

    def path():
        steps = []
        take("punct", ".")
        if peek("ident") or peek("str"):
            steps.append(("key", take()))
        while peek("punct", ".") or peek("punct", "["):
            if peek("punct", "."):
                take()
                steps.append(("key", take("str") if peek("str") else take("ident")))
                continue
            take("punct", "[")
            if peek("str"):
                steps.append(("key", take()))
            else:
                start = take("num") if peek("num") else None
                if peek("punct", ":"):
                    take()
                    end = take("num") if peek("num") else None
                    steps.append(("slice", start, end))
                elif start is None:
                    raise ValueError(f"unsupported jq expression: {expression!r}")
                else:
                    steps.append(("index", start))
            take("punct", "]")
        return steps

    pairs = []
    take("punct", "{")
    while not peek("punct", "}"):
        key = take("str") if peek("str") else take("ident")
        if peek("punct", ":"):
            take()
            steps, functions = path(), []
            while peek("punct", "|"):
                take()
                name = take("ident")
                if name != "strftime":
                    raise ValueError(f"unsupported jq function: {name}")
                take("punct", "(")
                functions.append(("strftime", take("str")))
                take("punct", ")")
        else:
            steps, functions = [("key", key)], []
        pairs.append((key, steps, functions))
        if not peek("punct", ","):
            break
        take()
    take("punct", "}")
    if i != len(tokens):
        raise ValueError(f"unsupported jq expression: {expression!r}")
    return pairs


def _jq_value(document, steps, functions):
    value = document
    for step in steps:
        if value is None:
            return None
        if step[0] == "key":
            if not isinstance(value, dict):
                raise ValueError(f"Cannot index {type(value).__name__} with \"{step[1]}\"")
            value = value.get(step[1])
        elif step[0] == "index":
            if not isinstance(value, list):
                raise ValueError(f"Cannot index {type(value).__name__} with number")
            value = value[step[1]] if -len(value) <= step[1] < len(value) else None
        else:
            if not isinstance(value, (list, str)):
                raise ValueError(f"Cannot index {type(value).__name__} with object")
            value = value[step[1]:step[2]]
    for _name, fmt in functions:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("strftime/1 requires parsed datetime inputs")
        value = time.strftime(fmt, time.gmtime(value))
    return value


def _metadata_extraction_keys(cfg, data):
    """Partition keys from a MetadataExtraction processor, or None when there is none."""
    processing = cfg.get("ProcessingConfiguration") or {}
    if not processing.get("Enabled"):
        return None
    query = None
    for processor in processing.get("Processors") or []:
        if processor.get("Type") == "MetadataExtraction":
            for parameter in processor.get("Parameters") or []:
                if parameter.get("ParameterName") == "MetadataExtractionQuery":
                    query = parameter.get("ParameterValue")
    if query is None:
        return None
    try:
        document = json.loads(data)
        if not isinstance(document, dict):
            raise ValueError("the record is not a JSON object")
        keys = {}
        for key, steps, functions in _jq_parse(query):
            value = _jq_value(document, steps, functions)
            if value is None or isinstance(value, (dict, list)):
                raise ValueError(f"partition key {key} has no scalar value")
            keys[key] = value if isinstance(value, str) else json.dumps(value)
        return keys
    except (ValueError, UnicodeDecodeError) as exc:
        raise _DeliveryError("processing-failed", f"Metadata extraction failed: {exc}") from exc


_CONVERSION_ERRORS = {
    "empty": ("DataFormatConversion.MalformedData", "The record was empty or contained only whitespace."),
    "primitive": ("DataFormatConversion.MalformedData",
                  "The input JSON contained a primitive at the top level. The top level must be an object or array."),
    "malformed": ("DataFormatConversion.ParseError", "Encountered malformed JSON."),
    "mismatch": ("DataFormatConversion.MalformedData", "Data does not match the schema."),
    "table": ("DataFormatConversion.EntityNotFound",
              "The specified table/database could not be found. Please ensure that the table/database exists and "
              "that the values provided in the schema configuration are correct, especially with regards to casing."),
    "unsupported": ("DataFormatConversion.ConversionFailureException", "ConversionFailureException"),
}


def _conversion_error(kind):
    code, message = _CONVERSION_ERRORS[kind]
    return _DeliveryError("format-conversion-failed", message, code)


def _hive_to_duckdb_type(hive_type):
    """Glue/Hive column type to a DuckDB type, nested types included."""
    text = hive_type.strip()
    lower = text.lower()

    def split_args(inner):
        parts, depth, start = [], 0, 0
        for pos, ch in enumerate(inner):
            if ch in "<(":
                depth += 1
            elif ch in ">)":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append(inner[start:pos])
                start = pos + 1
        parts.append(inner[start:])
        return [p.strip() for p in parts]

    if lower.startswith("array<") and lower.endswith(">"):
        return f"{_hive_to_duckdb_type(text[6:-1])}[]"
    if lower.startswith("map<") and lower.endswith(">"):
        key, value = split_args(text[4:-1])
        return f"MAP({_hive_to_duckdb_type(key)}, {_hive_to_duckdb_type(value)})"
    if lower.startswith("struct<") and lower.endswith(">"):
        fields = []
        for field in split_args(text[7:-1]):
            name, _, ftype = field.partition(":")
            fields.append(f'"{name.strip()}" {_hive_to_duckdb_type(ftype)}')
        return f"STRUCT({', '.join(fields)})"
    if lower.startswith("decimal"):
        return text.upper()
    from ministack.services.athena import _DUCKDB_TYPE_BY_ATHENA

    return _DUCKDB_TYPE_BY_ATHENA.get(lower.split("(")[0], "VARCHAR")


def _json_documents(data):
    """The JSON objects in a record: one, or several concatenated."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _conversion_error("malformed") from exc
    if not text.strip():
        raise _conversion_error("empty")
    decoder, pos, documents = json.JSONDecoder(), 0, []
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text):
            return documents
        try:
            document, pos = decoder.raw_decode(text, pos)
        except ValueError as exc:
            raise _conversion_error("malformed") from exc
        if not isinstance(document, dict):
            raise _conversion_error("primitive" if not isinstance(document, list) else "mismatch")
        documents.append(document)


def _convert_to_parquet(conversion, data):
    """JSON record to a Parquet object with the Glue table's columns, or a _DeliveryError."""
    serializer = ((conversion.get("OutputFormatConfiguration") or {}).get("Serializer") or {})
    if "ParquetSerDe" not in serializer:
        raise _conversion_error("unsupported")
    schema = conversion.get("SchemaConfiguration") or {}
    from ministack.services import glue as glue_svc

    table = glue_svc._tables.get(f"{schema.get('DatabaseName', '')}/{schema.get('TableName', '')}")
    columns = ((table or {}).get("StorageDescriptor") or {}).get("Columns") or []
    if not columns:
        raise _conversion_error("table")
    deserializer = ((conversion.get("InputFormatConfiguration") or {}).get("Deserializer") or {})
    openx = deserializer.get("OpenXJsonSerDe")
    case_insensitive = True if openx is None else openx.get("CaseInsensitive", True)
    dots_to_underscores = bool(openx and openx.get("ConvertDotsInJsonKeysToUnderscores"))
    key_mappings = (openx or {}).get("ColumnToJsonKeyMappings") or {}

    rows = []
    for document in _json_documents(data):
        fields = {}
        for key, value in document.items():
            name = key.replace(".", "_") if dots_to_underscores else key
            fields[name.lower() if case_insensitive else name] = value
        row = []
        for column in columns:
            name = column["Name"]
            source = key_mappings.get(name, name)
            row.append(fields.get(source.lower() if case_insensitive else source))
        rows.append(row)

    try:
        import duckdb
    except ImportError as exc:
        logger.warning("Firehose record format conversion needs DuckDB (the full image)")
        raise _conversion_error("unsupported") from exc
    import tempfile

    compression = {"GZIP": "gzip", "UNCOMPRESSED": "uncompressed"}.get(
        (serializer.get("ParquetSerDe") or {}).get("Compression", "SNAPPY"), "snappy")
    definition = ", ".join(f'"{c["Name"]}" {_hive_to_duckdb_type(c.get("Type", "string"))}' for c in columns)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "out.parquet")
        con = duckdb.connect()
        try:
            con.execute(f"CREATE TABLE t ({definition})")
            con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in columns)})", rows)
            con.execute(f"COPY t TO '{path}' (FORMAT PARQUET, COMPRESSION {compression})")
        except duckdb.Error as exc:
            raise _conversion_error("mismatch") from exc
        finally:
            con.close()
        with open(path, "rb") as fh:
            return fh.read()


def _put_s3_object(bucket, key, body):
    from ministack.services import s3 as s3_svc

    async def _put():
        headers = {"content-type": "application/octet-stream", "content-length": str(len(body)),
                   "host": "s3.localhost"}
        await s3_svc.handle_request("PUT", f"/{bucket}/{key}", headers, body, {})

    try:
        asyncio.get_running_loop().create_task(_put())
    except RuntimeError:
        asyncio.run(_put())


def _arrival_time(cfg):
    zone = cfg.get("CustomTimeZone")
    if zone and zone != "UTC":
        try:
            import datetime
            from zoneinfo import ZoneInfo

            return datetime.datetime.now(ZoneInfo(zone)).timetuple()
        except Exception:
            pass
    return time.gmtime()


def _deliver_to_s3(stream: dict, dest: dict, record_data: bytes, lambda_keys=None):
    """Deliver one record: partition keys, format conversion, evaluated prefix, AWS object name."""
    try:
        cfg = dest["config"]
        bucket = _s3_bucket_from_arn(cfg.get("BucketARN", ""))
        t = _arrival_time(cfg)
        suffix = f"{stream['name']}-{stream.get('version', 1)}-{_java_time('yyyy-MM-dd-HH-mm-ss', t)}-{new_uuid()}"
        conversion = cfg.get("DataFormatConversionConfiguration") or {}
        try:
            query_keys = None
            if (cfg.get("DynamicPartitioningConfiguration") or {}).get("Enabled"):
                query_keys = _metadata_extraction_keys(cfg, record_data)
            prefix = _evaluate_prefix(cfg.get("Prefix", ""), t, query_keys, lambda_keys)
            body, extension = record_data, ""
            if conversion and conversion.get("Enabled", True):
                body, extension = _convert_to_parquet(conversion, record_data), ".parquet"
            _put_s3_object(bucket, f"{prefix}{suffix}{cfg.get('FileExtension') or extension}", body)
        except _DeliveryError as err:
            now_ms = int(time.time() * 1000)
            document = {"attemptsMade": 1, "arrivalTimestamp": now_ms, "attemptEndingTimestamp": now_ms,
                        "rawData": base64.b64encode(record_data).decode()}
            if err.code:
                document["ErrorCode"] = err.code
            document["ErrorMessage"] = err.message
            if err.output_type == "format-conversion-failed":
                schema = conversion.get("SchemaConfiguration") or {}
                document["dataCatalogTable"] = {
                    "catalogId": schema.get("CatalogId") or get_account_id(),
                    "databaseName": schema.get("DatabaseName"), "tableName": schema.get("TableName"),
                    "region": schema.get("Region") or get_region(), "versionId": schema.get("VersionId", "LATEST")}
            error_prefix = cfg.get("ErrorOutputPrefix")
            if error_prefix:
                prefix = _evaluate_prefix(error_prefix, t, error_type=err.output_type)
            else:
                prefix = f"{cfg.get('Prefix', '')}{err.output_type}/{_java_time('yyyy/MM/dd/HH/', t)}"
            _put_s3_object(bucket, f"{prefix}{suffix}", (json.dumps(document) + "\n").encode())
    except Exception as e:
        logger.warning("Firehose S3 delivery failed: %s", e)


def _deliver_records_to_s3(stream: dict, dest: dict, records: list):
    """Run the processors over ``records`` [(recordId, bytes)] and deliver each result to S3."""
    partition_keys = {}
    for rid, payload in _apply_lambda_processors(stream, dest, records, partition_sink=partition_keys):
        _deliver_to_s3(stream, dest, payload, partition_keys.get(rid))


def _gateway_port() -> str:
    return os.environ.get("GATEWAY_PORT", "4566")


# Iceberg commits use optimistic concurrency: two writes racing on the same
# table conflict and one loses. Real Firehose buffers and writes in batches;
# MiniStack delivers per record, so serialize the commits to avoid conflicts.
_ICEBERG_WRITE_LOCK = threading.Lock()


def _iceberg_unique_keys(table_cfgs: list, db: str, table: str) -> list:
    for tc in table_cfgs:
        if tc.get("DestinationDatabaseName") == db and tc.get("DestinationTableName") == table:
            return list(tc.get("UniqueKeys") or [])
    return []


def _iceberg_write_group(warehouse: str, db: str, table: str, keys: list, group: dict):
    """Write one (db, table) batch into the Iceberg table via DuckDB, the
    engine MiniStack already ships. Runs on a worker thread (it makes loopback
    HTTP calls back into the gateway, so it must not run on the event loop).

    Mirrors real Firehose Merge-on-Read semantics: ``insert`` appends,
    ``update``/``delete`` match on the destination table's UniqueKeys.
    """
    import os as _os
    import tempfile

    import duckdb

    port = _gateway_port()
    region = get_region()
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    tbl = f'__cat."{db}"."{table}"'

    def _tmp(recs):
        fh = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(recs, fh)
        fh.close()
        return fh.name

    _ICEBERG_WRITE_LOCK.acquire()
    try:
        con.execute(
            f"CREATE SECRET __ms_fh (TYPE S3, KEY_ID 'test', SECRET 'test', "
            f"ENDPOINT 'localhost:{port}', URL_STYLE 'path', USE_SSL false, "
            f"REGION '{region}')"
        )
        con.execute(
            f"ATTACH '{warehouse}' AS __cat (TYPE ICEBERG, "
            f"ENDPOINT 'http://localhost:{port}/iceberg', AUTHORIZATION_TYPE 'none')"
        )
        if group.get("insert"):
            path = _tmp(group["insert"])
            con.execute(f"INSERT INTO {tbl} BY NAME " f"SELECT * FROM read_json(?, format='array')", [path])
            _os.unlink(path)
        if group.get("update") and keys:
            cols = [row[0] for row in con.execute(f"DESCRIBE {tbl}").fetchall()]
            set_cols = [c for c in cols if c not in keys] or keys
            on = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
            set_clause = ", ".join(f'"{c}" = s."{c}"' for c in set_cols)
            path = _tmp(group["update"])
            con.execute(
                f"MERGE INTO {tbl} AS t USING "
                f"(SELECT * FROM read_json(?, format='array')) AS s ON {on} "
                f"WHEN MATCHED THEN UPDATE SET {set_clause}",
                [path],
            )
            _os.unlink(path)
        if group.get("delete") and keys:
            on = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
            path = _tmp(group["delete"])
            con.execute(
                f"MERGE INTO {tbl} AS t USING "
                f"(SELECT * FROM read_json(?, format='array')) AS s ON {on} "
                f"WHEN MATCHED THEN DELETE",
                [path],
            )
            _os.unlink(path)
    finally:
        try:
            con.close()
        finally:
            _ICEBERG_WRITE_LOCK.release()


def _deliver_to_iceberg(stream: dict, dest: dict, records: list):
    """Deliver a batch of records to an Apache Iceberg destination (S3 Tables).

    Per-record routing follows real Firehose: a transform Lambda's
    ``otfMetadata`` (destinationDatabaseName / destinationTableName / operation)
    takes precedence, otherwise the single ``DestinationTableConfigurationList``
    entry is used. Operation defaults to ``insert``; ``update``/``delete``
    require the destination table's ``UniqueKeys`` (otherwise AWS routes the
    record to the S3 error bucket — here we log and skip).
    """
    cfg = dest.get("config") or {}
    warehouse = (cfg.get("CatalogConfiguration") or {}).get("CatalogARN", "")
    table_cfgs = cfg.get("DestinationTableConfigurationList") or []
    default_tc = table_cfgs[0] if table_cfgs else {}
    name = stream.get("name", "?")

    meta: dict = {}
    processed = _apply_lambda_processors(stream, dest, records, metadata_sink=meta)

    groups: dict = {}
    for rid, payload in processed:
        try:
            record = json.loads(payload)
        except (ValueError, TypeError):
            logger.warning(
                "Firehose %s: non-JSON record for Iceberg " "destination dropped (AWS routes to S3 error bucket)", name
            )
            continue
        otf = meta.get(rid) or {}
        db = otf.get("destinationDatabaseName") or default_tc.get("DestinationDatabaseName")
        table = otf.get("destinationTableName") or default_tc.get("DestinationTableName")
        op = (otf.get("operation") or "insert").lower()
        if op not in ("insert", "update", "delete"):
            op = "insert"
        if not db or not table:
            logger.warning("Firehose %s: record has no destination database/table; " "routed to S3 error bucket", name)
            continue
        keys = _iceberg_unique_keys(table_cfgs, db, table)
        if op in ("update", "delete") and not keys:
            logger.warning(
                "Firehose %s: '%s' on %s.%s requires UniqueKeys; " "record routed to S3 error bucket",
                name,
                op,
                db,
                table,
            )
            continue
        g = groups.setdefault(
            (warehouse, db, table, tuple(keys)),
            {"insert": [], "update": [], "delete": []},
        )
        g[op].append(record)

    if not groups:
        return

    def _deliver_iceberg_groups():
        for (wh, db, table, keys), group in groups.items():
            try:
                _iceberg_write_group(wh, db, table, list(keys), group)
            except Exception as exc:
                logger.warning("Firehose %s: Iceberg delivery to %s.%s failed: %s", name, db, table, exc)

    # Inline if no thread can be started: losing delivery records silently is
    # worse than blocking this caller. (Before this ran on `spawn_background`,
    # the fallback existed for a different reason — `get_running_loop()` raising
    # off the event loop — which can no longer happen.)
    try:
        spawn_background(_deliver_iceberg_groups, thread_name="ministack-firehose-iceberg")
    except RuntimeError:
        _deliver_iceberg_groups()


def _record_id() -> str:
    """Generate a Firehose-style RecordId (long numeric string)."""
    ts = int(time.time() * 1000)
    uid = new_uuid().replace("-", "")
    return f"{ts:020d}{uid}"


# ─── Kinesis-source ingestion ────────────────────────────────────────────────
# Public hook called by kinesis.py whenever PutRecord / PutRecords lands a
# record. For any ACTIVE delivery stream whose Source is configured to the
# Kinesis stream ARN, the record is forwarded to the configured destination
# (currently S3 / ExtendedS3 — others buffer in-memory the same way the
# direct PutRecord path does).
#
# AWS parity notes:
# - AWS only forwards records that arrived at or after the delivery stream's
#   DeliveryStartTimestamp. Records older than that are skipped, matching
#   how the AWS shard iterator opens at that timestamp.
# - Delivery is best-effort (any S3 / runtime error is logged and swallowed)
#   so a Firehose problem can never break the Kinesis write path.
# - The `records` argument is a list of `(partition_key, raw_bytes)` tuples;
#   `raw_bytes` is the already-decoded record payload (matches what AWS would
#   read off the Kinesis shard).
def ingest_from_kinesis_source(stream_arn: str, records: list) -> None:
    if not stream_arn or not records:
        return
    now_ts = now_epoch()
    with _lock:
        targets = [
            s
            for s in _streams.values()
            if s.get("type") == "KinesisStreamAsSource"
            and s.get("status") == "ACTIVE"
            and (s.get("kinesis_source") or {}).get("KinesisStreamARN") == stream_arn
        ]
        # Snapshot delivery targets so the lock can be released before we
        # schedule S3 writes (which dispatch on the event loop and should
        # never run under this lock).
        plan = []
        for stream in targets:
            start_ts = (stream.get("kinesis_source") or {}).get("DeliveryStartTimestamp") or 0
            if now_ts < start_ts:
                continue
            for dest in stream.get("destinations", []):
                if dest.get("type") not in ("ExtendedS3", "S3"):
                    continue
                for _pkey, raw in records:
                    rid = _record_id()
                    dest["records"].append({"id": rid, "data": raw, "ts": now_ts})
                    plan.append((stream, dest, rid, raw))
    for stream, dest, rid, raw in plan:
        try:
            _deliver_records_to_s3(stream, dest, [(rid, raw)])
        except Exception as exc:
            logger.warning(
                "Firehose Kinesis-source delivery to %s failed: %s",
                stream.get("name"),
                exc,
            )


# ─── operations ──────────────────────────────────────────────────────────────


def _create_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    if not name:
        return _invalid("DeliveryStreamName is required.")

    with _lock:
        if name in _streams:
            return error_response_json(
                "ResourceInUseException",
                f"Delivery stream {name} already exists.",
                400,
            )
        if len(_streams) >= 5000:
            return error_response_json(
                "LimitExceededException",
                "You have reached the limit on the number of delivery streams.",
                400,
            )

        dtype, cfg = _resolve_dest_type_and_config(data)
        # A stream with no destination is valid (destination added later via UpdateDestination)
        destinations = []
        if dtype and cfg is not None:
            validation_error = _validate_s3_destination_config(dtype, cfg)
            if validation_error:
                return validation_error
            destinations.append(
                {
                    "id": _next_dest_id(),
                    "type": dtype,
                    "config": cfg,
                    "records": [],
                }
            )

        stream_type = data.get("DeliveryStreamType", "DirectPut")
        now = now_epoch()
        stream = {
            "name": name,
            "arn": _stream_arn(name),
            "status": "ACTIVE",
            "type": stream_type,
            "version": 1,
            "created_at": now,
            "updated_at": now,
            "destinations": destinations,
            "tags": {t["Key"]: t.get("Value", "") for t in data.get("Tags", [])},
            "encryption": None,
            "kinesis_source": None,
        }

        # Capture Kinesis source config for Source block in DescribeDeliveryStream
        if stream_type == "KinesisStreamAsSource":
            ks_cfg = data.get("KinesisStreamSourceConfiguration", {})
            _fh_role = ks_cfg.get("RoleARN", "")
            if _fh_role:
                from ministack.core.iam_evaluator import validate_role_arn

                _fh_role_err = validate_role_arn(_fh_role)
                if _fh_role_err:
                    return _invalid(_fh_role_err)
            stream["kinesis_source"] = {
                "KinesisStreamARN": ks_cfg.get("KinesisStreamARN", ""),
                "RoleARN": _fh_role,
                "DeliveryStartTimestamp": now,
            }

        enc_input = data.get("DeliveryStreamEncryptionConfigurationInput")
        if enc_input:
            enc = {"Status": "ENABLED", "KeyType": enc_input.get("KeyType", "AWS_OWNED_CMK")}
            if "KeyARN" in enc_input:
                enc["KeyARN"] = enc_input["KeyARN"]
            stream["encryption"] = enc

        _streams[name] = stream

    return json_response({"DeliveryStreamARN": stream["arn"]})


def _delete_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    with _lock:
        if name not in _streams:
            return _not_found(name)
        stream = _streams[name]
        if stream["status"] == "CREATING":
            return _in_use(name)
        del _streams[name]
    return json_response({})


def _describe_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        desc = _build_description(stream)
    return json_response({"DeliveryStreamDescription": desc})


def _list_delivery_streams(data: dict):
    dtype_filter = data.get("DeliveryStreamType")
    limit = min(int(data.get("Limit", 10)), 10000)
    start = data.get("ExclusiveStartDeliveryStreamName")

    with _lock:
        if dtype_filter:
            names = sorted(n for n, s in _streams.items() if s["type"] == dtype_filter)
        else:
            names = sorted(_streams.keys())

    if start:
        try:
            idx = names.index(start)
            names = names[idx + 1 :]
        except ValueError:
            pass

    has_more = len(names) > limit
    return json_response(
        {
            "DeliveryStreamNames": names[:limit],
            "HasMoreDeliveryStreams": has_more,
        }
    )


def _put_record(data: dict):
    name = data.get("DeliveryStreamName", "")
    record = data.get("Record", {})
    raw_data = record.get("Data", "")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if stream["status"] != "ACTIVE":
            return error_response_json("ServiceUnavailableException", "Service unavailable.", 503)

        try:
            decoded = base64.b64decode(raw_data)
        except Exception:
            return _invalid("Record.Data must be valid base64.")

        if len(decoded) > 1024 * 1000:
            return _invalid("Record size exceeds 1,000 KiB limit.")

        record_id = _record_id()
        for dest in stream["destinations"]:
            dest["records"].append({"id": record_id, "data": raw_data, "ts": now_epoch()})
            if dest["type"] in ("ExtendedS3", "S3"):
                _deliver_records_to_s3(stream, dest, [(record_id, decoded)])
            elif dest["type"] == "Iceberg":
                _deliver_to_iceberg(stream, dest, [(record_id, decoded)])

    return json_response({"RecordId": record_id, "Encrypted": False})


def _put_record_batch(data: dict):
    name = data.get("DeliveryStreamName", "")
    records = data.get("Records", [])

    if not records:
        return _invalid("Records must not be empty.")
    if len(records) > 500:
        return _invalid("A maximum of 500 records can be sent per batch.")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if stream["status"] != "ACTIVE":
            return error_response_json("ServiceUnavailableException", "Service unavailable.", 503)

        responses = []
        failed = 0
        for rec in records:
            raw_data = rec.get("Data", "")
            try:
                decoded = base64.b64decode(raw_data)
                if len(decoded) > 1024 * 1000:
                    raise ValueError("Record too large")
                record_id = _record_id()
                for dest in stream["destinations"]:
                    dest["records"].append({"id": record_id, "data": raw_data, "ts": now_epoch()})
                    if dest["type"] in ("ExtendedS3", "S3"):
                        _deliver_records_to_s3(stream, dest, [(record_id, decoded)])
                    elif dest["type"] == "Iceberg":
                        _deliver_to_iceberg(stream, dest, [(record_id, decoded)])
                responses.append({"RecordId": record_id, "Encrypted": False})
            except Exception as e:
                failed += 1
                responses.append(
                    {
                        "ErrorCode": "ServiceUnavailableException",
                        "ErrorMessage": str(e),
                    }
                )

    return json_response(
        {
            "FailedPutCount": failed,
            "Encrypted": False,
            "RequestResponses": responses,
        }
    )


def _update_destination(data: dict):
    name = data.get("DeliveryStreamName", "")
    dest_id = data.get("DestinationId", "")
    version_id = data.get("CurrentDeliveryStreamVersionId", "")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if str(stream["version"]) != str(version_id):
            return error_response_json(
                "ConcurrentModificationException",
                "Request includes an invalid stream version ID.",
                400,
            )
        dest = next((d for d in stream["destinations"] if d["id"] == dest_id), None)
        if not dest:
            return error_response_json(
                "ResourceNotFoundException",
                f"Destination {dest_id} not found in stream {name}.",
                400,
            )

        dtype, cfg = _resolve_dest_update_config(data)
        if dtype and cfg is not None:
            validation_error = _validate_s3_destination_config(dtype, cfg)
            if validation_error:
                return validation_error
            if dtype == dest["type"]:
                # Same destination type — merge fields (AWS behaviour)
                dest["config"] = {**dest["config"], **cfg}
            else:
                # Destination type change — full replacement
                dest["type"] = dtype
                dest["config"] = cfg

        stream["version"] += 1
        stream["updated_at"] = now_epoch()

    return json_response({})


def _tag_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    tags = data.get("Tags", [])
    if not tags:
        return _invalid("Tags must not be empty.")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if len(stream["tags"]) + len(tags) > 50:
            return error_response_json(
                "LimitExceededException",
                "A delivery stream cannot have more than 50 tags.",
                400,
            )
        for tag in tags:
            stream["tags"][tag["Key"]] = tag.get("Value", "")

    return json_response({})


def _untag_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    keys = data.get("TagKeys", [])
    if not keys:
        return _invalid("TagKeys must not be empty.")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        for k in keys:
            stream["tags"].pop(k, None)

    return json_response({})


def _list_tags_for_delivery_stream(data: dict):
    name = data.get("DeliveryStreamName", "")
    limit = min(int(data.get("Limit", 50)), 50)
    start = data.get("ExclusiveStartTagKey")

    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        all_tags = [{"Key": k, "Value": v} for k, v in sorted(stream["tags"].items())]

    if start:
        try:
            idx = next(i for i, t in enumerate(all_tags) if t["Key"] == start)
            all_tags = all_tags[idx + 1 :]
        except StopIteration:
            pass

    has_more = len(all_tags) > limit
    return json_response(
        {
            "Tags": all_tags[:limit],
            "HasMoreTags": has_more,
        }
    )


def _start_delivery_stream_encryption(data: dict):
    name = data.get("DeliveryStreamName", "")
    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if stream["status"] != "ACTIVE":
            return _in_use(name)
        enc_input = data.get("DeliveryStreamEncryptionConfigurationInput", {})
        stream["encryption"] = {
            "Status": "ENABLED",
            "KeyType": enc_input.get("KeyType", "AWS_OWNED_CMK"),
        }
        if "KeyARN" in enc_input:
            stream["encryption"]["KeyARN"] = enc_input["KeyARN"]
        stream["updated_at"] = now_epoch()
    return json_response({})


def _stop_delivery_stream_encryption(data: dict):
    name = data.get("DeliveryStreamName", "")
    with _lock:
        stream = _streams.get(name)
        if not stream:
            return _not_found(name)
        if stream["status"] != "ACTIVE":
            return _in_use(name)
        stream["encryption"] = {"Status": "DISABLED"}
        stream["updated_at"] = now_epoch()
    return json_response({})


# ─── dispatch ────────────────────────────────────────────────────────────────

_HANDLERS = {
    "CreateDeliveryStream": _create_delivery_stream,
    "DeleteDeliveryStream": _delete_delivery_stream,
    "DescribeDeliveryStream": _describe_delivery_stream,
    "ListDeliveryStreams": _list_delivery_streams,
    "PutRecord": _put_record,
    "PutRecordBatch": _put_record_batch,
    "UpdateDestination": _update_destination,
    "TagDeliveryStream": _tag_delivery_stream,
    "UntagDeliveryStream": _untag_delivery_stream,
    "ListTagsForDeliveryStream": _list_tags_for_delivery_stream,
    "StartDeliveryStreamEncryption": _start_delivery_stream_encryption,
    "StopDeliveryStreamEncryption": _stop_delivery_stream_encryption,
}


def _handle_request_sync(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    if not action:
        return error_response_json("InvalidArgumentException", "Missing X-Amz-Target header.", 400)

    handler = _HANDLERS.get(action)
    if not handler:
        return error_response_json("InvalidArgumentException", f"Unknown operation: {action}", 400)

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("InvalidArgumentException", "Request body is not valid JSON.", 400)

    return handler(data)


async def handle_request(method, path, headers, body, query_params):
    """Dispatch off the event loop.

    PutRecord with an Iceberg destination delivers via DuckDB, which makes
    loopback HTTP calls back to the gateway. Running the handler on the event
    loop thread deadlocks: the background thread's POST waits for the event
    loop, which is blocked on handler(). run_reentrant gives the handler its
    own thread so the event loop stays free for the loopback.
    """
    return await run_reentrant(
        _handle_request_sync, method, path, headers, body, query_params, thread_name="ministack-firehose-dispatch"
    )
