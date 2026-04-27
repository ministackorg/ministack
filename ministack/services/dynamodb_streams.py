"""
DynamoDB Streams Emulator.
Supports: ListStreams, DescribeStream, GetShardIterator, GetRecords.
Uses X-Amz-Target header for action routing (JSON API, prefix
DynamoDBStreams_20120810). Shares the ``dynamodb`` credential scope and reads
stream records directly from ministack.services.dynamodb (single synthetic
shard per stream; no duplicate storage).
"""

import base64
import json
import logging

from ministack.core.responses import error_response_json, json_response
from ministack.services import dynamodb as _ddb

logger = logging.getLogger("dynamodb_streams")

# Single synthetic shard per stream — MiniStack does not model shard splitting.
_DEFAULT_SHARD_ID = "shardId-00000000000000000000-00000000"

_ITERATOR_TYPES = {
    "TRIM_HORIZON",
    "LATEST",
    "AT_SEQUENCE_NUMBER",
    "AFTER_SEQUENCE_NUMBER",
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "ListStreams": _list_streams,
        "DescribeStream": _describe_stream,
        "GetShardIterator": _get_shard_iterator,
        "GetRecords": _get_records,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("UnknownOperationException", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _encode_iterator(table_name: str, shard_id: str, position: int) -> str:
    """Encode an opaque shard iterator.

    The token is intentionally opaque: callers must treat it as bytes and pass
    it back unmodified. We base64-url-encode a small JSON payload so it stays
    short enough to fit in AWS's 2 KB iterator limit.
    """
    payload = json.dumps({"t": table_name, "s": shard_id, "p": position}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


def _decode_iterator(token: str) -> dict | None:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        obj = json.loads(raw)
        if not isinstance(obj, dict) or "t" not in obj or "p" not in obj:
            return None
        return obj
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _table_from_stream_arn(stream_arn: str) -> str | None:
    """Extract the table name from a stream ARN of the form
    ``arn:aws:dynamodb:{region}:{account}:table/{name}/stream/{label}``."""
    if not stream_arn or "/stream/" not in stream_arn:
        return None
    table_part = stream_arn.split("/stream/", 1)[0]
    if ":table/" in table_part:
        return table_part.split(":table/", 1)[1]
    return None


def _records_for(table_name: str) -> list:
    """Return the raw list of stream records for a table, or an empty list.

    Reads directly from the dynamodb module so TransactWriteItems,
    BatchWriteItem, and the single-item ops all stay in sync automatically.
    """
    return _ddb._stream_records.get(table_name, [])


def _enabled_stream_info(table_name: str) -> dict | None:
    """Return a dict describing a table's current stream, or ``None`` when
    the table does not have an enabled stream."""
    table = _ddb._tables.get(table_name)
    if not table:
        return None
    spec = table.get("StreamSpecification")
    if not spec or not spec.get("StreamEnabled"):
        return None
    stream_arn = table.get("LatestStreamArn")
    stream_label = table.get("LatestStreamLabel")
    if not stream_arn or not stream_label:
        return None
    return {
        "TableName": table_name,
        "StreamArn": stream_arn,
        "StreamLabel": stream_label,
        "StreamViewType": spec.get("StreamViewType", "NEW_AND_OLD_IMAGES"),
    }


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

def _list_streams(data):
    """List streams, optionally filtered by TableName.

    Pagination: the real API returns up to 100 streams per page with
    ``LastEvaluatedStreamArn``. MiniStack deployments have a small number of
    tables so we return everything in one page but still honour ``Limit``.
    """
    table_filter = data.get("TableName")
    limit = int(data.get("Limit", 100) or 100)
    exclusive_start = data.get("ExclusiveStartStreamArn")

    streams = []
    names = sorted(_ddb._tables.keys())
    if table_filter:
        names = [n for n in names if n == table_filter]

    for name in names:
        info = _enabled_stream_info(name)
        if info is None:
            continue
        streams.append({
            "StreamArn": info["StreamArn"],
            "TableName": info["TableName"],
            "StreamLabel": info["StreamLabel"],
        })

    if exclusive_start:
        idx = next((i for i, s in enumerate(streams) if s["StreamArn"] == exclusive_start), -1)
        if idx >= 0:
            streams = streams[idx + 1:]

    page = streams[:limit]
    response: dict = {"Streams": page}
    if len(page) == limit and len(streams) > limit:
        response["LastEvaluatedStreamArn"] = page[-1]["StreamArn"]
    return json_response(response)


def _describe_stream(data):
    stream_arn = data.get("StreamArn")
    if not stream_arn:
        return error_response_json("ValidationException", "StreamArn is required", 400)

    table_name = _table_from_stream_arn(stream_arn)
    if not table_name:
        return error_response_json(
            "ResourceNotFoundException", f"Stream not found: {stream_arn}", 400
        )

    info = _enabled_stream_info(table_name)
    if info is None or info["StreamArn"] != stream_arn:
        return error_response_json(
            "ResourceNotFoundException", f"Stream not found: {stream_arn}", 400
        )

    records = _records_for(table_name)
    starting_seq = records[0]["dynamodb"]["SequenceNumber"] if records else None

    shard: dict = {
        "ShardId": _DEFAULT_SHARD_ID,
        "SequenceNumberRange": {},
    }
    if starting_seq:
        shard["SequenceNumberRange"]["StartingSequenceNumber"] = starting_seq

    table = _ddb._tables.get(table_name, {})
    key_schema = table.get("KeySchema", [])

    description = {
        "StreamArn": stream_arn,
        "StreamLabel": info["StreamLabel"],
        "StreamStatus": "ENABLED",
        "StreamViewType": info["StreamViewType"],
        "CreationRequestDateTime": table.get("CreationDateTime", 0),
        "TableName": table_name,
        "KeySchema": key_schema,
        "Shards": [shard],
    }
    return json_response({"StreamDescription": description})


def _get_shard_iterator(data):
    stream_arn = data.get("StreamArn")
    shard_id = data.get("ShardId") or _DEFAULT_SHARD_ID
    iterator_type = data.get("ShardIteratorType")
    seq_number = data.get("SequenceNumber")

    if not stream_arn:
        return error_response_json("ValidationException", "StreamArn is required", 400)
    if iterator_type not in _ITERATOR_TYPES:
        return error_response_json(
            "ValidationException",
            f"ShardIteratorType must be one of {sorted(_ITERATOR_TYPES)}",
            400,
        )

    table_name = _table_from_stream_arn(stream_arn)
    if not table_name:
        return error_response_json(
            "ResourceNotFoundException", f"Stream not found: {stream_arn}", 400
        )

    info = _enabled_stream_info(table_name)
    if info is None or info["StreamArn"] != stream_arn:
        return error_response_json(
            "ResourceNotFoundException", f"Stream not found: {stream_arn}", 400
        )

    records = _records_for(table_name)
    position = 0
    if iterator_type == "TRIM_HORIZON":
        position = 0
    elif iterator_type == "LATEST":
        position = len(records)
    elif iterator_type in ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"):
        if not seq_number:
            return error_response_json(
                "ValidationException",
                f"SequenceNumber is required for {iterator_type}",
                400,
            )
        idx = next(
            (i for i, r in enumerate(records)
             if r["dynamodb"]["SequenceNumber"] == seq_number),
            None,
        )
        if idx is None:
            return error_response_json(
                "TrimmedDataAccessException",
                f"Sequence number {seq_number} not found on stream",
                400,
            )
        position = idx if iterator_type == "AT_SEQUENCE_NUMBER" else idx + 1

    iterator = _encode_iterator(table_name, shard_id, position)
    return json_response({"ShardIterator": iterator})


def _get_records(data):
    iterator = data.get("ShardIterator")
    limit = int(data.get("Limit", 1000) or 1000)
    if limit <= 0:
        limit = 1000
    limit = min(limit, 1000)

    if not iterator:
        return error_response_json("ValidationException", "ShardIterator is required", 400)

    decoded = _decode_iterator(iterator)
    if decoded is None:
        return error_response_json(
            "ValidationException", "ShardIterator is not valid", 400
        )

    table_name = decoded["t"]
    shard_id = decoded.get("s", _DEFAULT_SHARD_ID)
    position = int(decoded.get("p", 0))

    if _enabled_stream_info(table_name) is None:
        return error_response_json(
            "ExpiredIteratorException",
            "Iterator references a stream that is no longer enabled",
            400,
        )

    records = _records_for(table_name)
    page = records[position:position + limit]
    next_position = position + len(page)
    next_iterator = _encode_iterator(table_name, shard_id, next_position)

    return json_response({
        "Records": page,
        "NextShardIterator": next_iterator,
    })


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def get_state():
    # All state lives in the dynamodb module; nothing to persist here.
    return {}


def restore_state(data):
    pass


def reset():
    # Stream records are cleared by dynamodb.reset(); keep this a no-op so
    # ordering between reset callers never matters.
    pass
