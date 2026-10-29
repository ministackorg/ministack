# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Kinesis Data Streams Emulator.
JSON-based API via X-Amz-Target (Kinesis_20131202).
Supports: CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary,
          ListStreams, PutRecord, PutRecords, GetShardIterator, GetRecords,
          MergeShards, SplitShard, UpdateShardCount, ListShards,
          IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod,
          AddTagsToStream, RemoveTagsFromStream, ListTagsForStream,
          RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers,
          DescribeStreamConsumer, SubscribeToShard,
          StartStreamEncryption, StopStreamEncryption,
          EnableEnhancedMonitoring, DisableEnhancedMonitoring.
"""

import base64
import copy
import hashlib
import json
import logging
import os
import re
import threading
import time
import zlib

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import esm_wake
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    StreamingResponse,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("kinesis")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
MAX_HASH_KEY = (2**128) - 1
ITERATOR_EXPIRY_SECONDS = 300


_streams = AccountRegionScopedDict()
_shard_iterators = AccountRegionScopedDict()
_consumers = AccountRegionScopedDict()
_resource_policies = AccountRegionScopedDict()  # stream or consumer ARN -> policy document as AWS stores it
# (ConsumerARN, ShardId) -> {"id", "started"} of the live SubscribeToShard
_subscriptions = {}
SUBSCRIPTION_SECONDS = 300
_SUBSCRIPTION_TAKEOVER_SECONDS = 5
_SUBSCRIPTION_IDLE_SECONDS = 5
_sequence_counter = 0
_sequence_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "streams": copy.deepcopy(_streams),
        "shard_iterators": copy.deepcopy(_shard_iterators),
        "consumers": copy.deepcopy(_consumers),
        "resource_policies": copy.deepcopy(_resource_policies),
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if data:
        _restore_stream_store(data.get("streams", {}))
        _restore_shard_iterator_store(data.get("shard_iterators", {}))
        _restore_consumer_store(data.get("consumers", {}))
        policies = data.get("resource_policies", {})
        if isinstance(policies, AccountRegionScopedDict):
            _resource_policies.update(policies)


def _kinesis_arn_scope(value, default_account_id: str | None = None) -> tuple[str, str]:
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return default_account_id or get_account_id(), get_region()
    if spec.service != "kinesis":
        return default_account_id or get_account_id(), get_region()
    return spec.account_id or default_account_id or get_account_id(), spec.region or get_region()


def _stream_record_scope(stream: dict, default_account_id: str | None = None) -> tuple[str, str]:
    return _kinesis_arn_scope(stream.get("StreamARN", ""), default_account_id)


def _consumer_record_scope(consumer: dict, default_account_id: str | None = None) -> tuple[str, str]:
    return _kinesis_arn_scope(
        consumer.get("ConsumerARN") or consumer.get("StreamARN", ""),
        default_account_id,
    )


def _shard_iterator_record_scope(state: dict, default_account_id: str | None = None) -> tuple[str, str]:
    stream_arn = state.get("stream_arn")
    if stream_arn:
        return _kinesis_arn_scope(stream_arn, default_account_id)

    stream_name = state.get("stream")
    if stream_name:
        expected_account_id = default_account_id or get_account_id()
        for (account_id, region, name), _stream in _streams.all_items():
            if account_id == expected_account_id and name == stream_name:
                return account_id, region
    return default_account_id or get_account_id(), get_region()


def _restore_stream_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _streams.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, name), stream in data._data.items():
            restored_account_id, region = _stream_record_scope(stream, account_id)
            _streams.set_scoped(restored_account_id, region, name, copy.deepcopy(stream))
        return
    if isinstance(data, dict):
        for key, stream in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, name = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, name = key
                account_id, region = _stream_record_scope(stream, account_id)
            else:
                name = key
                account_id, region = _stream_record_scope(stream)
            _streams.set_scoped(account_id, region, name, copy.deepcopy(stream))


def _restore_shard_iterator_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _shard_iterators.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, token), state in data._data.items():
            restored_account_id, region = _shard_iterator_record_scope(state, account_id)
            _shard_iterators.set_scoped(restored_account_id, region, token, copy.deepcopy(state))
        return
    if isinstance(data, dict):
        for key, state in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, token = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, token = key
                account_id, region = _shard_iterator_record_scope(state, account_id)
            else:
                token = key
                account_id, region = _shard_iterator_record_scope(state)
            _shard_iterators.set_scoped(account_id, region, token, copy.deepcopy(state))


def _restore_consumer_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _consumers.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, consumer_arn), consumer in data._data.items():
            restored_account_id, region = _consumer_record_scope(consumer, account_id)
            _consumers.set_scoped(restored_account_id, region, consumer_arn, copy.deepcopy(consumer))
        return
    if isinstance(data, dict):
        for key, consumer in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, consumer_arn = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, consumer_arn = key
                account_id, region = _consumer_record_scope(consumer, account_id)
            else:
                consumer_arn = key
                account_id, region = _consumer_record_scope(consumer)
            _consumers.set_scoped(account_id, region, consumer_arn, copy.deepcopy(consumer))




def _next_sequence_number():
    global _sequence_counter
    with _sequence_lock:
        _sequence_counter += 1
        ts_millis = int(time.time() * 1000)
        return f"{ts_millis:020d}{_sequence_counter:010d}"


def _compute_hash_ranges(shard_count):
    range_size = (MAX_HASH_KEY + 1) // shard_count
    ranges = []
    for i in range(shard_count):
        start = i * range_size
        end = ((i + 1) * range_size - 1) if i < shard_count - 1 else MAX_HASH_KEY
        ranges.append((str(start), str(end)))
    return ranges


def _build_shards(shard_count, start_index=0):
    ranges = _compute_hash_ranges(shard_count)
    shards = {}
    for i in range(shard_count):
        sid = f"shardId-{start_index + i:012d}"
        shards[sid] = {
            "records": [],
            "starting_hash_key": ranges[i][0],
            "ending_hash_key": ranges[i][1],
            "starting_sequence_number": _next_sequence_number(),
            "parent_shard_id": None,
            "adjacent_parent_shard_id": None,
        }
    return shards


def _partition_key_to_hash(partition_key: str) -> int:
    return int(hashlib.md5(partition_key.encode("utf-8")).hexdigest(), 16)


def _route_to_shard(hash_key_int: int, stream: dict) -> str:
    for sid, shard in stream["shards"].items():
        if int(shard["starting_hash_key"]) <= hash_key_int <= int(shard["ending_hash_key"]):
            return sid
    return next(iter(stream["shards"]))


def _kinesis_resource_tail(value, resource_type):
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return None
    if (
        spec.partition != "aws"
        or spec.service != "kinesis"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None
    prefix = f"{resource_type}/"
    if not spec.resource.startswith(prefix):
        return None
    tail = spec.resource[len(prefix):]
    return tail or None


def _stream_name_from_arn(stream_arn):
    tail = _kinesis_resource_tail(stream_arn, "stream")
    if not tail or "/" in tail:
        return None
    return tail


def _resolve_stream_by_arn(stream_arn):
    name = _stream_name_from_arn(stream_arn)
    if not name:
        return None
    stream = _streams.get(name)
    if stream and stream.get("StreamARN") == stream_arn:
        return stream
    return None


def _consumer_from_arn(consumer_arn):
    tail = _kinesis_resource_tail(consumer_arn, "stream")
    if not tail:
        return None
    parts = tail.split("/")
    if len(parts) != 3 or parts[1] != "consumer" or not parts[0] or not parts[2]:
        return None
    return _consumers.get(consumer_arn)


def _consumer_by_stream_and_name(stream_arn, consumer_name):
    if not _resolve_stream_by_arn(stream_arn):
        return None
    return next(
        (
            c for c in _consumers.values()
            if c["StreamARN"] == stream_arn and c["ConsumerName"] == consumer_name
        ),
        None,
    )


def _expire_records(stream):
    cutoff = time.time() - stream["RetentionPeriodHours"] * 3600
    for shard in stream["shards"].values():
        shard["records"] = [r for r in shard["records"] if r["ApproximateArrivalTimestamp"] >= cutoff]


def _expire_iterators():
    now = time.time()
    expired = [tok for tok, st in _shard_iterators.items()
               if now - st["created_at"] > ITERATOR_EXPIRY_SECONDS]
    for tok in expired:
        del _shard_iterators[tok]


def _ensure_active(stream):
    if stream["StreamStatus"] == "CREATING":
        stream["StreamStatus"] = "ACTIVE"


def _resolve_stream(data):
    name = data.get("StreamName")
    arn = data.get("StreamARN")
    if name and name in _streams:
        return name, _streams[name]
    if arn:
        stream = _resolve_stream_by_arn(arn)
        if stream:
            return stream["StreamName"], stream
    return name or arn, None


def _max_shard_index(stream):
    return max((int(sid.split("-")[1]) for sid in stream["shards"]), default=-1)


def put_record_internal(stream_arn: str, partition_key: str, data: bytes) -> bool:
    """Append a record to a stream looked up by ARN — used by cross-service
    emitters such as DynamoDB's Kinesis streaming destination fan-out.

    Returns ``True`` on success, ``False`` silently when the stream is gone
    or not ACTIVE (matches AWS behaviour where delivery to a disabled
    destination is dropped without surfacing an error on the writer).
    """
    stream = _resolve_stream_by_arn(stream_arn)
    if not stream or stream.get("StreamStatus") != "ACTIVE":
        return False
    _expire_records(stream)
    hash_int = _partition_key_to_hash(partition_key)
    shard_id = _route_to_shard(hash_int, stream)
    stream["shards"][shard_id]["records"].append({
        "SequenceNumber": _next_sequence_number(),
        "ApproximateArrivalTimestamp": round(time.time(), 3),
        "Data": base64.b64encode(data).decode("ascii"),
        "PartitionKey": partition_key,
    })
    esm_wake.set()
    return True


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    content_type = headers.get("content-type", "")
    is_cbor = "cbor" in content_type

    try:
        if is_cbor and body:
            import cbor2
            data = cbor2.loads(body)
            # CBOR Data field arrives as raw bytes; base64-encode for uniform handling
            if "Data" in data and isinstance(data["Data"], (bytes, bytearray)):
                data["Data"] = base64.b64encode(data["Data"]).decode("ascii")
            for rec in data.get("Records", []):
                if "Data" in rec and isinstance(rec["Data"], (bytes, bytearray)):
                    rec["Data"] = base64.b64encode(rec["Data"]).decode("ascii")
        else:
            data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)
    except Exception as e:
        logger.error("Failed to decode request body: %s", e)
        return error_response_json("SerializationException", f"Could not decode request body: {e}", 400)

    _expire_iterators()

    handlers = {
        "CreateStream": _create_stream,
        "DeleteStream": _delete_stream,
        "DescribeStream": _describe_stream,
        "DescribeStreamSummary": _describe_stream_summary,
        "ListStreams": _list_streams,
        "ListShards": _list_shards,
        "PutRecord": _put_record,
        "PutRecords": _put_records,
        "GetShardIterator": _get_shard_iterator,
        "GetRecords": _get_records,
        "IncreaseStreamRetentionPeriod": _increase_retention,
        "DecreaseStreamRetentionPeriod": _decrease_retention,
        "AddTagsToStream": _add_tags,
        "RemoveTagsFromStream": _remove_tags,
        "ListTagsForStream": _list_tags,
        "MergeShards": _merge_shards,
        "SplitShard": _split_shard,
        "UpdateShardCount": _update_shard_count,
        "RegisterStreamConsumer": _register_consumer,
        "DeregisterStreamConsumer": _deregister_consumer,
        "ListStreamConsumers": _list_consumers,
        "DescribeStreamConsumer": _describe_stream_consumer,
        "SubscribeToShard": lambda d: _subscribe_to_shard(d, is_cbor),
        "StartStreamEncryption": _start_stream_encryption,
        "StopStreamEncryption": _stop_stream_encryption,
        "EnableEnhancedMonitoring": _enable_enhanced_monitoring,
        "DisableEnhancedMonitoring": _disable_enhanced_monitoring,
        "PutResourcePolicy": lambda d: _put_resource_policy(d, headers),
        "GetResourcePolicy": lambda d: _get_resource_policy(d, headers),
        "DeleteResourcePolicy": lambda d: _delete_resource_policy(d, headers),
    }

    handler = handlers.get(action)
    if not handler:
        if is_cbor:
            return _cbor_response({"__type": "InvalidAction", "message": f"Unknown action: {action}"}, 400)
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)

    status, resp_headers, resp_body = handler(data)
    if isinstance(resp_body, StreamingResponse):
        return status, resp_headers, resp_body
    if is_cbor:
        import cbor2
        # Re-encode JSON response body as CBOR
        try:
            json_data = json.loads(resp_body) if isinstance(resp_body, (str, bytes)) else resp_body
        except (json.JSONDecodeError, TypeError):
            json_data = {}
        cbor_body = cbor2.dumps(json_data)
        resp_headers["Content-Type"] = "application/x-amz-cbor-1.1"
        return status, resp_headers, cbor_body
    return status, resp_headers, resp_body


# ---------------------------------------------------------------------------
# Stream lifecycle
# ---------------------------------------------------------------------------

def _create_stream(data):
    name = data.get("StreamName")
    shard_count = data.get("ShardCount", 1)
    if not name:
        return error_response_json("ValidationException", "StreamName is required", 400)
    if name in _streams:
        return error_response_json("ResourceInUseException", f"Stream {name} already exists", 400)
    if shard_count < 1:
        return error_response_json("ValidationException", "ShardCount must be at least 1", 400)

    arn = f"arn:aws:kinesis:{get_region()}:{get_account_id()}:stream/{name}"
    mode = data.get("StreamModeDetails", {}).get("StreamMode", "PROVISIONED")
    _streams[name] = {
        "StreamName": name,
        "StreamARN": arn,
        "StreamStatus": "ACTIVE",
        "StreamModeDetails": {"StreamMode": mode},
        "RetentionPeriodHours": 24,
        "shards": _build_shards(shard_count),
        "tags": {},
        "CreationTimestamp": int(time.time()),
        "EncryptionType": "NONE",
    }
    return json_response({})


def _delete_stream(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    stream["StreamStatus"] = "DELETING"
    for tok in [t for t, s in _shard_iterators.items() if s["stream"] == name]:
        del _shard_iterators[tok]
    for carn in [a for a, c in _consumers.items() if c["StreamARN"] == stream["StreamARN"]]:
        del _consumers[carn]
        _resource_policies.pop(carn, None)
    _resource_policies.pop(stream["StreamARN"], None)
    del _streams[name]
    return json_response({})


# ---------------------------------------------------------------------------
# Describe / List
# ---------------------------------------------------------------------------

def _describe_stream(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    _ensure_active(stream)
    _expire_records(stream)

    limit = data.get("Limit", 100)
    exclusive_start = data.get("ExclusiveStartShardId")
    shard_ids = sorted(stream["shards"].keys())
    if exclusive_start:
        shard_ids = [s for s in shard_ids if s > exclusive_start]
    page = shard_ids[:limit]
    has_more = len(shard_ids) > limit

    desc = _stream_desc(stream, page)
    desc["HasMoreShards"] = has_more
    return json_response({"StreamDescription": desc})


def _describe_stream_summary(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    _ensure_active(stream)
    consumer_count = sum(1 for c in _consumers.values() if c["StreamARN"] == stream["StreamARN"])
    return json_response({"StreamDescriptionSummary": {
        "StreamName": stream["StreamName"],
        "StreamARN": stream["StreamARN"],
        "StreamStatus": stream["StreamStatus"],
        "StreamModeDetails": stream.get("StreamModeDetails", {"StreamMode": "PROVISIONED"}),
        "RetentionPeriodHours": stream["RetentionPeriodHours"],
        "StreamCreationTimestamp": stream["CreationTimestamp"],
        "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
        "EncryptionType": stream.get("EncryptionType", "NONE"),
        **({"KeyId": stream["KeyId"]} if stream.get("KeyId") else {}),
        "OpenShardCount": len(stream["shards"]),
        "ConsumerCount": consumer_count,
    }})


def _list_streams(data):
    limit = data.get("Limit", 100)
    exclusive_start = data.get("ExclusiveStartStreamName")
    names = sorted(_streams.keys())
    if exclusive_start:
        names = [n for n in names if n > exclusive_start]
    page = names[:limit]
    has_more = len(names) > limit
    summaries = []
    for n in page:
        s = _streams[n]
        summaries.append({
            "StreamName": n,
            "StreamARN": s["StreamARN"],
            "StreamStatus": s["StreamStatus"],
            "StreamModeDetails": s.get("StreamModeDetails", {"StreamMode": "PROVISIONED"}),
            "StreamCreationTimestamp": s["CreationTimestamp"],
        })
    return json_response({"StreamNames": page, "StreamSummaries": summaries, "HasMoreStreams": has_more})


_LIST_SHARDS_TOKEN_TTL = 300  # AWS contract: tokens expire after 300 seconds.


def _encode_list_shards_token(after_shard_id: str) -> str:
    """Encode an opaque pagination token for ListShards. AWS uses an opaque
    string; consumers must round-trip it without inspection. We base64url-
    encode a small JSON payload so we can also enforce the AWS 300s TTL."""
    payload = json.dumps({"a": after_shard_id, "t": int(time.time())}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")


def _decode_list_shards_token(token: str) -> tuple[str | None, str | None]:
    """Return (after_shard_id, error_code). error_code is "ExpiredNextTokenException"
    if the token is past TTL, "InvalidArgumentException" if malformed, else None."""
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        obj = json.loads(raw)
        if not isinstance(obj, dict) or "a" not in obj or "t" not in obj:
            return None, "InvalidArgumentException"
        if int(time.time()) - int(obj["t"]) > _LIST_SHARDS_TOKEN_TTL:
            return None, "ExpiredNextTokenException"
        return str(obj["a"]), None
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None, "InvalidArgumentException"


def _list_shards(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    _ensure_active(stream)

    max_results = data.get("MaxResults", 10000)
    next_token = data.get("NextToken")
    exclusive_start = data.get("ExclusiveStartShardId")

    shard_ids = sorted(stream["shards"].keys())
    if exclusive_start:
        shard_ids = [s for s in shard_ids if s > exclusive_start]
    if next_token:
        after, err = _decode_list_shards_token(next_token)
        if err == "ExpiredNextTokenException":
            return error_response_json(
                "ExpiredNextTokenException",
                "NextToken has expired. Tokens expire after 300 seconds.",
                400,
            )
        if err or after is None:
            return error_response_json(
                "InvalidArgumentException", "Invalid NextToken", 400
            )
        shard_ids = [s for s in shard_ids if s > after]

    page = shard_ids[:max_results]
    result = {"Shards": [_shard_out(sid, stream["shards"][sid]) for sid in page]}
    if len(shard_ids) > max_results:
        result["NextToken"] = _encode_list_shards_token(page[-1])
    return json_response(result)


# ---------------------------------------------------------------------------
# Put records
# ---------------------------------------------------------------------------

def _put_record(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    if stream["StreamStatus"] != "ACTIVE":
        return error_response_json("ResourceInUseException", f"Stream {name} is {stream['StreamStatus']}", 400)

    _expire_records(stream)

    partition_key = data.get("PartitionKey", "")
    record_data = data.get("Data", "")
    explicit_hash = data.get("ExplicitHashKey")
    if not partition_key:
        return error_response_json("ValidationException", "PartitionKey is required", 400)
    if len(partition_key) > 256:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'partitionKey' failed to satisfy constraint: "
            "Member must have length less than or equal to 256", 400)
    raw = b""
    if record_data:
        try:
            raw = base64.b64decode(record_data)
        except Exception:
            raw = record_data.encode() if isinstance(record_data, str) else record_data
        if len(raw) > 1_048_576:
            return error_response_json("ValidationException",
                "1 validation error detected: Value at 'data' failed to satisfy constraint: "
                "Member must have length less than or equal to 1048576", 400)

    hash_int = int(explicit_hash) if explicit_hash else _partition_key_to_hash(partition_key)
    shard_id = _route_to_shard(hash_int, stream)
    seq = _next_sequence_number()

    stream["shards"][shard_id]["records"].append({
        "SequenceNumber": seq,
        "ApproximateArrivalTimestamp": round(time.time(), 3),
        "Data": record_data,
        "PartitionKey": partition_key,
    })
    esm_wake.set()

    # Fan out to any Firehose delivery stream configured with this Kinesis
    # stream as its source. Best-effort, must not break this PutRecord.
    try:
        from ministack.services import firehose as _firehose
        _firehose.ingest_from_kinesis_source(
            stream["StreamARN"], [(partition_key, raw)],
        )
    except Exception:
        logger.exception("Firehose fan-out from Kinesis PutRecord failed")

    return json_response({
        "ShardId": shard_id,
        "SequenceNumber": seq,
        "EncryptionType": stream.get("EncryptionType", "NONE"),
    })


def _put_records(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    if stream["StreamStatus"] != "ACTIVE":
        return error_response_json("ResourceInUseException", f"Stream {name} is {stream['StreamStatus']}", 400)

    _expire_records(stream)

    records = data.get("Records", [])
    if len(records) > 500:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'records' failed to satisfy constraint: "
            "Member must have length less than or equal to 500", 400)
    total_size = 0
    for rec in records:
        pk = rec.get("PartitionKey", "")
        rd = rec.get("Data", "")
        if len(pk) > 256:
            return error_response_json("ValidationException",
                "1 validation error detected: Value at 'partitionKey' failed to satisfy constraint: "
                "Member must have length less than or equal to 256", 400)
        try:
            raw = base64.b64decode(rd) if rd else b""
        except Exception:
            raw = rd.encode() if isinstance(rd, str) else rd
        rec_size = len(raw) + len(pk.encode())
        if len(raw) > 1_048_576:
            return error_response_json("ValidationException",
                "1 validation error detected: Value at 'data' failed to satisfy constraint: "
                "Member must have length less than or equal to 1048576", 400)
        total_size += rec_size
    if total_size > 5_242_880:
        return error_response_json("ValidationException",
            "Records total payload size exceeds 5 MB limit", 400)

    results = []
    fanout_pairs = []
    for rec in records:
        pk = rec.get("PartitionKey", "")
        rd = rec.get("Data", "")
        eh = rec.get("ExplicitHashKey")
        hash_int = int(eh) if eh else _partition_key_to_hash(pk)
        sid = _route_to_shard(hash_int, stream)
        seq = _next_sequence_number()
        stream["shards"][sid]["records"].append({
            "SequenceNumber": seq,
            "ApproximateArrivalTimestamp": round(time.time(), 3),
            "Data": rd,
            "PartitionKey": pk,
        })
        results.append({
            "SequenceNumber": seq,
            "ShardId": sid,
            "EncryptionType": stream.get("EncryptionType", "NONE"),
        })
        try:
            raw = base64.b64decode(rd) if rd else b""
        except Exception:
            raw = rd.encode() if isinstance(rd, str) else rd
        fanout_pairs.append((pk, raw))
    esm_wake.set()

    # Fan out the whole batch to any Firehose delivery stream configured with
    # this Kinesis stream as its source. Best-effort, must not break PutRecords.
    try:
        from ministack.services import firehose as _firehose
        _firehose.ingest_from_kinesis_source(stream["StreamARN"], fanout_pairs)
    except Exception:
        logger.exception("Firehose fan-out from Kinesis PutRecords failed")

    return json_response({
        "FailedRecordCount": 0,
        "Records": results,
        "EncryptionType": stream.get("EncryptionType", "NONE"),
    })


# ---------------------------------------------------------------------------
# Shard iterators / GetRecords
# ---------------------------------------------------------------------------

def _get_shard_iterator(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)

    shard_id = data.get("ShardId")
    if shard_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException", f"Shard {shard_id} not found", 400)

    _expire_records(stream)
    shard = stream["shards"][shard_id]
    records = shard["records"]
    it_type = data.get("ShardIteratorType", "LATEST")
    seq = data.get("StartingSequenceNumber", "")
    at_ts = data.get("Timestamp")

    if it_type == "AT_TIMESTAMP" and at_ts is None:
        return error_response_json("ValidationException", "Timestamp required for AT_TIMESTAMP", 400)
    position = _start_position(records, it_type, seq, at_ts)
    if position is None:
        return error_response_json("ValidationException", f"Invalid ShardIteratorType: {it_type}", 400)

    resolved_name = name if name else next((n for n, s in _streams.items() if s is stream), "")
    token = new_uuid()
    _shard_iterators[token] = {
        "stream": resolved_name,
        "stream_arn": stream["StreamARN"],
        "shard_id": shard_id,
        "position": position,
        "created_at": time.time(),
    }
    return json_response({"ShardIterator": token})


def _start_position(records, it_type, seq, at_ts):
    """Index into ``records`` where an iterator type starts; None for an unknown type."""
    if it_type == "TRIM_HORIZON":
        return 0
    if it_type == "LATEST":
        return len(records)
    if it_type == "AT_SEQUENCE_NUMBER":
        return next((i for i, r in enumerate(records) if r["SequenceNumber"] >= seq), len(records))
    if it_type == "AFTER_SEQUENCE_NUMBER":
        return next((i for i, r in enumerate(records) if r["SequenceNumber"] > seq), len(records))
    if it_type == "AT_TIMESTAMP":
        ts_val = float(at_ts)
        return next((i for i, r in enumerate(records)
                     if r["ApproximateArrivalTimestamp"] >= ts_val), len(records))
    return None


def _ensure_base64(value):
    """Return a base64-encoded string regardless of input format."""
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, str):
        try:
            base64.b64decode(value, validate=True)
            return value
        except Exception:
            return base64.b64encode(value.encode("utf-8")).decode("ascii")
    return base64.b64encode(str(value).encode("utf-8")).decode("ascii")


def _get_records(data):
    iterator = data.get("ShardIterator")
    limit = min(data.get("Limit", 10000), 10000)

    state = _shard_iterators.get(iterator)
    if not state:
        return error_response_json("ExpiredIteratorException", "Iterator has expired or is invalid", 400)
    if time.time() - state["created_at"] > ITERATOR_EXPIRY_SECONDS:
        del _shard_iterators[iterator]
        return error_response_json("ExpiredIteratorException", "Iterator has expired", 400)

    stream = _streams.get(state["stream"])
    if not stream:
        return error_response_json("ResourceNotFoundException", "Stream not found", 400)

    _expire_records(stream)
    shard = stream["shards"].get(state["shard_id"])
    if not shard:
        return error_response_json("ResourceNotFoundException", "Shard not found", 400)

    pos = min(state["position"], len(shard["records"]))
    raw = shard["records"][pos:pos + limit]
    new_pos = pos + len(raw)

    out_records = [{
        "SequenceNumber": r["SequenceNumber"],
        "ApproximateArrivalTimestamp": r["ApproximateArrivalTimestamp"],
        "Data": _ensure_base64(r["Data"]),
        "PartitionKey": r["PartitionKey"],
        "EncryptionType": stream.get("EncryptionType", "NONE"),
    } for r in raw]

    millis_behind = 0
    if shard["records"] and new_pos < len(shard["records"]):
        millis_behind = max(0, int((time.time() - shard["records"][new_pos]["ApproximateArrivalTimestamp"]) * 1000))

    # Retire the current iterator and issue a new one with advanced position,
    # matching AWS behavior: each GetRecords call returns a NextShardIterator.
    # The old iterator remains valid until it expires naturally (5 min TTL),
    # allowing client retries to succeed.
    next_token = new_uuid()
    _shard_iterators[next_token] = {
        "stream": state["stream"],
        "stream_arn": state.get("stream_arn") or stream.get("StreamARN"),
        "shard_id": state["shard_id"],
        "position": new_pos,
        "created_at": time.time(),
    }
    return json_response({
        "Records": out_records,
        "NextShardIterator": next_token,
        "MillisBehindLatest": millis_behind,
    })


# ---------------------------------------------------------------------------
# Retention period
# ---------------------------------------------------------------------------

def _increase_retention(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    hours = data.get("RetentionPeriodHours")
    if hours is None:
        return error_response_json("ValidationException", "RetentionPeriodHours is required", 400)
    hours = int(hours)
    if hours == stream["RetentionPeriodHours"]:
        return json_response({})  # no-op: same value is fine
    if hours < stream["RetentionPeriodHours"]:
        return error_response_json("ValidationException",
                                   "RetentionPeriodHours must be greater than current value", 400)
    if hours > 8760:
        return error_response_json("ValidationException",
                                   "RetentionPeriodHours cannot exceed 8760", 400)
    stream["RetentionPeriodHours"] = hours
    return json_response({})


def _decrease_retention(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    hours = data.get("RetentionPeriodHours")
    if hours is None:
        return error_response_json("ValidationException", "RetentionPeriodHours is required", 400)
    hours = int(hours)
    if hours >= stream["RetentionPeriodHours"]:
        return error_response_json("ValidationException",
                                   "RetentionPeriodHours must be less than current value", 400)
    if hours < 24:
        return error_response_json("ValidationException",
                                   "RetentionPeriodHours cannot be less than 24", 400)
    stream["RetentionPeriodHours"] = hours
    return json_response({})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _add_tags(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    stream["tags"].update(data.get("Tags", {}))
    return json_response({})


def _remove_tags(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    for key in data.get("TagKeys", []):
        stream["tags"].pop(key, None)
    return json_response({})


def _list_tags(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    limit = data.get("Limit", 50)
    exclusive_start = data.get("ExclusiveStartTagKey")
    items = sorted(stream["tags"].items())
    if exclusive_start:
        items = [(k, v) for k, v in items if k > exclusive_start]
    page = items[:limit]
    return json_response({
        "Tags": [{"Key": k, "Value": v} for k, v in page],
        "HasMoreTags": len(items) > limit,
    })


# ---------------------------------------------------------------------------
# MergeShards / SplitShard / UpdateShardCount
# ---------------------------------------------------------------------------

def _merge_shards(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    s1_id = data.get("ShardToMerge")
    s2_id = data.get("AdjacentShardToMerge")
    if s1_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException", f"Shard {s1_id} not found", 400)
    if s2_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException", f"Shard {s2_id} not found", 400)

    s1, s2 = stream["shards"][s1_id], stream["shards"][s2_id]
    new_start = str(min(int(s1["starting_hash_key"]), int(s2["starting_hash_key"])))
    new_end = str(max(int(s1["ending_hash_key"]), int(s2["ending_hash_key"])))

    new_idx = _max_shard_index(stream) + 1
    new_sid = f"shardId-{new_idx:012d}"
    stream["shards"][new_sid] = {
        "records": [],
        "starting_hash_key": new_start,
        "ending_hash_key": new_end,
        "starting_sequence_number": _next_sequence_number(),
        "parent_shard_id": s1_id,
        "adjacent_parent_shard_id": s2_id,
    }
    del stream["shards"][s1_id]
    del stream["shards"][s2_id]
    return json_response({})


def _split_shard(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    shard_id = data.get("ShardToSplit")
    new_hash = data.get("NewStartingHashKey")
    if shard_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException", f"Shard {shard_id} not found", 400)
    if not new_hash:
        return error_response_json("ValidationException", "NewStartingHashKey is required", 400)

    old = stream["shards"][shard_id]
    split_pt = int(new_hash)
    old_start, old_end = int(old["starting_hash_key"]), int(old["ending_hash_key"])
    if split_pt <= old_start or split_pt > old_end:
        return error_response_json("ValidationException",
                                   "NewStartingHashKey must be within the shard range", 400)

    base = _max_shard_index(stream) + 1
    c1 = f"shardId-{base:012d}"
    c2 = f"shardId-{base + 1:012d}"
    stream["shards"][c1] = {
        "records": [],
        "starting_hash_key": str(old_start),
        "ending_hash_key": str(split_pt - 1),
        "starting_sequence_number": _next_sequence_number(),
        "parent_shard_id": shard_id,
        "adjacent_parent_shard_id": None,
    }
    stream["shards"][c2] = {
        "records": [],
        "starting_hash_key": str(split_pt),
        "ending_hash_key": str(old_end),
        "starting_sequence_number": _next_sequence_number(),
        "parent_shard_id": shard_id,
        "adjacent_parent_shard_id": None,
    }
    del stream["shards"][shard_id]
    return json_response({})


def _update_shard_count(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    target = data.get("TargetShardCount")
    if target is None:
        return error_response_json("ValidationException", "TargetShardCount is required", 400)
    target = int(target)
    if target < 1:
        return error_response_json("ValidationException", "TargetShardCount must be >= 1", 400)

    current = len(stream["shards"])
    stream["shards"] = _build_shards(target)
    return json_response({
        "StreamName": stream["StreamName"],
        "CurrentShardCount": current,
        "TargetShardCount": target,
        "StreamARN": stream["StreamARN"],
    })


# ---------------------------------------------------------------------------
# Enhanced fan-out consumers
# ---------------------------------------------------------------------------

def _register_consumer(data):
    stream_arn = data.get("StreamARN")
    consumer_name = data.get("ConsumerName")
    if not stream_arn or not consumer_name:
        return error_response_json("ValidationException",
                                   "StreamARN and ConsumerName are required", 400)
    stream = _resolve_stream_by_arn(stream_arn)
    if not stream:
        return error_response_json("ResourceNotFoundException",
                                   f"Stream with ARN {stream_arn} not found", 400)
    for c in _consumers.values():
        if c["StreamARN"] == stream_arn and c["ConsumerName"] == consumer_name:
            return error_response_json("ResourceInUseException",
                                       f"Consumer {consumer_name} already exists", 400)

    consumer_arn = f"{stream_arn}/consumer/{consumer_name}:{int(time.time())}"
    now = int(time.time())
    _consumers[consumer_arn] = {
        "ConsumerName": consumer_name,
        "ConsumerARN": consumer_arn,
        "ConsumerStatus": "ACTIVE",
        "ConsumerCreationTimestamp": now,
        "StreamARN": stream_arn,
    }
    return json_response({"Consumer": {
        "ConsumerName": consumer_name,
        "ConsumerARN": consumer_arn,
        "ConsumerStatus": "ACTIVE",
        "ConsumerCreationTimestamp": now,
    }})


def _deregister_consumer(data):
    consumer_arn = data.get("ConsumerARN")
    stream_arn = data.get("StreamARN")
    consumer_name = data.get("ConsumerName")
    if consumer_arn:
        consumer = _consumer_from_arn(consumer_arn)
        if not consumer:
            return error_response_json("ResourceNotFoundException", "Consumer not found", 400)
        del _consumers[consumer["ConsumerARN"]]
        _resource_policies.pop(consumer["ConsumerARN"], None)
    elif stream_arn and consumer_name:
        consumer = _consumer_by_stream_and_name(stream_arn, consumer_name)
        if not consumer:
            return error_response_json("ResourceNotFoundException", "Consumer not found", 400)
        del _consumers[consumer["ConsumerARN"]]
        _resource_policies.pop(consumer["ConsumerARN"], None)
    else:
        return error_response_json("ValidationException",
                                   "ConsumerARN or StreamARN+ConsumerName required", 400)
    return json_response({})


def _list_consumers(data):
    stream_arn = data.get("StreamARN")
    if not stream_arn:
        return error_response_json("ValidationException", "StreamARN is required", 400)
    if not _resolve_stream_by_arn(stream_arn):
        return error_response_json("ResourceNotFoundException", f"Stream with ARN {stream_arn} not found", 400)
    max_results = data.get("MaxResults", 100)
    next_token = data.get("NextToken")

    items = [{
        "ConsumerName": c["ConsumerName"],
        "ConsumerARN": c["ConsumerARN"],
        "ConsumerStatus": c["ConsumerStatus"],
        "ConsumerCreationTimestamp": c["ConsumerCreationTimestamp"],
    } for c in _consumers.values() if c["StreamARN"] == stream_arn]

    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = items[start:start + max_results]
    result = {"Consumers": page}
    if start + max_results < len(items):
        result["NextToken"] = str(start + max_results)
    return json_response(result)


def _describe_stream_consumer(data):
    consumer_arn = data.get("ConsumerARN")
    stream_arn = data.get("StreamARN")
    consumer_name = data.get("ConsumerName")

    consumer = None
    if consumer_arn:
        consumer = _consumer_from_arn(consumer_arn)
    elif stream_arn and consumer_name:
        consumer = _consumer_by_stream_and_name(stream_arn, consumer_name)

    if not consumer:
        return error_response_json("ResourceNotFoundException", "Consumer not found", 400)

    return json_response({"ConsumerDescription": {
        "ConsumerName": consumer["ConsumerName"],
        "ConsumerARN": consumer["ConsumerARN"],
        "ConsumerStatus": consumer["ConsumerStatus"],
        "ConsumerCreationTimestamp": consumer["ConsumerCreationTimestamp"],
        "StreamARN": consumer["StreamARN"],
    }})


# ---------------------------------------------------------------------------
# Stream encryption
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SubscribeToShard (enhanced fan-out eventstream)
# ---------------------------------------------------------------------------

def _eventstream_message(headers, payload):
    """One ``application/vnd.amazon.eventstream`` message with string headers."""
    raw = bytearray()
    for name, value in headers.items():
        name_b, value_b = name.encode(), value.encode()
        raw += bytes([len(name_b)]) + name_b + b"\x07" + len(value_b).to_bytes(2, "big") + value_b
    prelude = (12 + len(raw) + len(payload) + 4).to_bytes(4, "big") + len(raw).to_bytes(4, "big")
    head = prelude + zlib.crc32(prelude).to_bytes(4, "big") + bytes(raw) + payload
    return head + zlib.crc32(head).to_bytes(4, "big")


def _subscribe_to_shard(data, is_cbor):
    consumer_arn = data.get("ConsumerARN")
    shard_id = data.get("ShardId")
    starting = data.get("StartingPosition") or {}
    it_type = starting.get("Type")
    if not consumer_arn or not shard_id or not it_type:
        return error_response_json("InvalidArgumentException",
                                   "ConsumerARN, ShardId and StartingPosition.Type are required", 400)
    consumer = _consumer_from_arn(consumer_arn)
    if not consumer:
        return error_response_json("ResourceNotFoundException",
                                   f"Consumer {consumer_arn} not found", 400)
    if consumer.get("ConsumerStatus") != "ACTIVE":
        return error_response_json("ResourceInUseException",
                                   f"Consumer {consumer_arn} is not ACTIVE", 400)
    stream = _resolve_stream_by_arn(consumer["StreamARN"])
    if not stream or shard_id not in stream["shards"]:
        return error_response_json("ResourceNotFoundException",
                                   f"Shard {shard_id} in stream {consumer['StreamARN']} not found", 400)
    if it_type in ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER") and not starting.get("SequenceNumber"):
        return error_response_json("InvalidArgumentException",
                                   f"SequenceNumber is required for {it_type}", 400)
    if it_type == "AT_TIMESTAMP" and starting.get("Timestamp") is None:
        return error_response_json("InvalidArgumentException",
                                   "Timestamp is required for AT_TIMESTAMP", 400)
    _expire_records(stream)
    position = _start_position(stream["shards"][shard_id]["records"], it_type,
                               starting.get("SequenceNumber", ""), starting.get("Timestamp"))
    if position is None:
        return error_response_json("InvalidArgumentException",
                                   f"Invalid StartingPosition.Type: {it_type}", 400)

    # Capture the selected boundary before the runner starts. Retention can
    # replace the record list and shift its indices while the response is open.
    shard = stream["shards"][shard_id]
    records = shard["records"]
    boundary = records[position - 1]["SequenceNumber"] if position else None
    initial_continuation = boundary if boundary is not None else shard["starting_sequence_number"]

    # A second call within 5 seconds is refused; later, it takes the subscription over.
    key = (consumer_arn, shard_id)
    now = time.time()
    live = _subscriptions.get(key)
    if live and now - live["started"] < _SUBSCRIPTION_TAKEOVER_SECONDS:
        return error_response_json("ResourceInUseException",
                                   f"Another active subscription exists for consumer {consumer_arn} "
                                   f"and shard {shard_id}", 400)
    subscription_id = new_uuid()
    _subscriptions[key] = {"id": subscription_id, "started": now}
    content_type = "application/x-amz-cbor-1.1" if is_cbor else "application/json"
    account_id, region = get_account_id(), get_region()
    stream_name = stream["StreamName"]

    def encode(payload):
        if is_cbor:
            import cbor2
            return cbor2.dumps(payload)
        return json.dumps(payload).encode()

    def event(event_type, payload):
        return _eventstream_message({":message-type": "event", ":event-type": event_type,
                                     ":content-type": content_type}, encode(payload))

    def exception(error_type, message):
        return _eventstream_message({":message-type": "exception", ":exception-type": error_type,
                                     ":content-type": content_type}, encode({"message": message}))

    def record_out(r, encryption):
        raw = base64.b64decode(_ensure_base64(r["Data"]))
        return {"SequenceNumber": r["SequenceNumber"],
                "ApproximateArrivalTimestamp": r["ApproximateArrivalTimestamp"],
                "Data": raw if is_cbor else base64.b64encode(raw).decode("ascii"),
                "PartitionKey": r["PartitionKey"],
                "EncryptionType": encryption}

    async def run(send, receive):
        import asyncio

        async def chunk(body, more=True):
            await send({"type": "http.response.body", "body": body, "more_body": more})

        async def disconnected():
            while (await receive()).get("type") != "http.disconnect":
                pass

        after_sequence = boundary
        continuation = initial_continuation
        deadline = now + SUBSCRIPTION_SECONDS
        last_sent = 0.0
        watcher = asyncio.create_task(disconnected())
        try:
            await chunk(_eventstream_message({":message-type": "event", ":event-type": "initial-response",
                                              ":content-type": content_type}, encode({})))
            while not watcher.done() and time.time() < deadline:
                if (_subscriptions.get(key) or {}).get("id") != subscription_id:
                    await chunk(exception("ResourceInUseException",
                                          f"Subscription to shard {shard_id} was taken over"))
                    return
                current = _streams.get_scoped(account_id, region, stream_name)
                if current is not stream or _consumers.get_scoped(account_id, region, consumer_arn) is None:
                    await chunk(exception("ResourceNotFoundException",
                                          f"Stream {stream_name} or consumer {consumer_arn} not found"))
                    return
                shard = stream["shards"].get(shard_id)
                if shard is None:
                    children = [{"ShardId": sid, "ParentShards": [p for p in (s.get("parent_shard_id"),
                                                                              s.get("adjacent_parent_shard_id")) if p],
                                 "HashKeyRange": {"StartingHashKey": s["starting_hash_key"],
                                                  "EndingHashKey": s["ending_hash_key"]}}
                                for sid, s in sorted(stream["shards"].items())
                                if shard_id in (s.get("parent_shard_id"), s.get("adjacent_parent_shard_id"))]
                    await chunk(event("SubscribeToShardEvent", {
                        "Records": [], "ContinuationSequenceNumber": continuation,
                        "MillisBehindLatest": 0, "ChildShards": children}))
                    return
                records = shard["records"]
                batch = [r for r in records
                         if after_sequence is None or r["SequenceNumber"] > after_sequence]
                if batch or not last_sent or time.time() - last_sent >= _SUBSCRIPTION_IDLE_SECONDS:
                    encryption = stream.get("EncryptionType", "NONE")
                    if batch:
                        after_sequence = continuation = batch[-1]["SequenceNumber"]
                    await chunk(event("SubscribeToShardEvent", {
                        "Records": [record_out(r, encryption) for r in batch],
                        "ContinuationSequenceNumber": continuation,
                        "MillisBehindLatest": 0}))
                    last_sent = time.time()
                await asyncio.wait({watcher}, timeout=0.2)
        finally:
            if (_subscriptions.get(key) or {}).get("id") == subscription_id:
                _subscriptions.pop(key, None)
            watcher.cancel()
            try:
                await chunk(b"", more=False)
            except Exception:
                pass

    return 200, {"Content-Type": "application/vnd.amazon.eventstream"}, StreamingResponse(run)


def _start_stream_encryption(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    encryption_type = data.get("EncryptionType", "KMS")
    key_id = data.get("KeyId", "")
    stream["EncryptionType"] = encryption_type
    stream["KeyId"] = key_id
    return json_response({})


def _stop_stream_encryption(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    stream["EncryptionType"] = "NONE"
    stream.pop("KeyId", None)
    return json_response({})


# ---------------------------------------------------------------------------
# Enhanced monitoring
# ---------------------------------------------------------------------------

def _enable_enhanced_monitoring(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    desired = data.get("ShardLevelMetrics", [])
    current = stream.get("ShardLevelMetrics", [])
    merged = list(set(current) | set(desired))
    stream["ShardLevelMetrics"] = merged
    return json_response({
        "StreamName": stream["StreamName"],
        "StreamARN": stream["StreamARN"],
        "CurrentShardLevelMetrics": current,
        "DesiredShardLevelMetrics": merged,
    })


def _disable_enhanced_monitoring(data):
    name, stream = _resolve_stream(data)
    if not stream:
        return error_response_json("ResourceNotFoundException", f"Stream {name} not found", 400)
    to_disable = set(data.get("ShardLevelMetrics", []))
    current = stream.get("ShardLevelMetrics", [])
    remaining = [m for m in current if m not in to_disable]
    stream["ShardLevelMetrics"] = remaining
    return json_response({
        "StreamName": stream["StreamName"],
        "StreamARN": stream["StreamARN"],
        "CurrentShardLevelMetrics": current,
        "DesiredShardLevelMetrics": remaining,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _shard_out(shard_id, shard):
    result = {
        "ShardId": shard_id,
        "HashKeyRange": {
            "StartingHashKey": shard["starting_hash_key"],
            "EndingHashKey": shard["ending_hash_key"],
        },
        "SequenceNumberRange": {
            "StartingSequenceNumber": shard["starting_sequence_number"],
        },
    }
    if shard.get("parent_shard_id"):
        result["ParentShardId"] = shard["parent_shard_id"]
    if shard.get("adjacent_parent_shard_id"):
        result["AdjacentParentShardId"] = shard["adjacent_parent_shard_id"]
    return result


def _stream_desc(stream, shard_ids=None):
    if shard_ids is None:
        shard_ids = sorted(stream["shards"].keys())
    return {
        "StreamName": stream["StreamName"],
        "StreamARN": stream["StreamARN"],
        "StreamStatus": stream["StreamStatus"],
        "StreamModeDetails": stream.get("StreamModeDetails", {"StreamMode": "PROVISIONED"}),
        "RetentionPeriodHours": stream["RetentionPeriodHours"],
        "StreamCreationTimestamp": stream["CreationTimestamp"],
        "Shards": [_shard_out(sid, stream["shards"][sid]) for sid in shard_ids],
        "HasMoreShards": False,
        "EnhancedMonitoring": [{"ShardLevelMetrics": []}],
        "EncryptionType": stream.get("EncryptionType", "NONE"),
        # KeyId accompanies EncryptionType on AWS, and is omitted entirely when
        # the stream is not encrypted. Storing it without echoing it here leaves
        # kms_key_id reading empty on an encrypted stream.
        **({"KeyId": stream["KeyId"]} if stream.get("KeyId") else {}),
    }


def _cbor_response(data: dict, status: int = 200):
    import cbor2
    body = cbor2.dumps(data)
    return status, {"Content-Type": "application/x-amz-cbor-1.1"}, body


# ---------------------------------------------------------------------------
# Resource policies (codes and messages as AWS answered them, us-east-1, 2026-10-26)
# ---------------------------------------------------------------------------

# The actions a policy may share (Kinesis Data Streams developer guide, "Controlling access", and
# DescribeStream, which its cross-account Lambda section says to include).
_POLICY_ACTIONS = {
    "stream": {"kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
               "kinesis:GetShardIterator", "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"},
    "consumer": {"kinesis:DescribeStreamConsumer", "kinesis:SubscribeToShard"},
}
_POLICY_ACTIONS_LOWER = {kind: {a.lower() for a in actions} for kind, actions in _POLICY_ACTIONS.items()}
_STATEMENT_FIELDS = ("Sid", "Effect", "Principal", "Action", "Resource", "Condition")
_CONDITION_OPERATORS = {op.lower() for op in (
    "StringEquals", "StringNotEquals", "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase", "StringLike",
    "StringNotLike", "NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
    "NumericGreaterThan", "NumericGreaterThanEquals", "DateEquals", "DateNotEquals", "DateLessThan",
    "DateLessThanEquals", "DateGreaterThan", "DateGreaterThanEquals", "Bool", "BinaryEquals", "IpAddress",
    "NotIpAddress", "ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike", "Null")}
_NO_PRINCIPALS = ("Invalid policy document: aws.auth.client.error.ARCInvalidArgumentException: "
                  "com.amazon.balsa.error.InvalidPolicyException: The passed in policy has a statement with no principals!")


def _skip_ws(text, i):
    while i < len(text) and text[i] in " \t\r\n":
        i += 1
    return i


def _members(text, i):
    """(key, value_start, value_end) for each member of the JSON object starting at text[i]."""
    decoder, out = json.JSONDecoder(), []
    i = _skip_ws(text, i + 1)
    while i < len(text) and text[i] == '"':
        key, i = json.decoder.scanstring(text, i + 1)
        i = _skip_ws(text, _skip_ws(text, i) + 1)
        _value, end = decoder.raw_decode(text, i)
        out.append((key, i, end))
        i = _skip_ws(text, end)
        if i < len(text) and text[i] == ",":
            i = _skip_ws(text, i + 1)
    return out


def _line_col(text, offset):
    """1-based (line, column) of *offset*. CRLF, CR and LF each end a line."""
    head = text[:offset].replace("\r\n", "\n").replace("\r", "\n")
    return head.count("\n") + 1, len(head) - (head.rfind("\n") + 1) + 1


def _statement_positions(text):
    """For each Statement entry: the offset AWS reports it at (just past the "[", "{" or previous
    entry's "}" before it) and the entry's (key, value_start, value_end) members."""
    decoder = json.JSONDecoder()
    for key, start, end in _members(text, _skip_ws(text, 0)):
        if key != "Statement":
            continue
        if text[start] == "{":
            return [(start + 1, _members(text, start))]
        out, i, pre = [], _skip_ws(text, start + 1), start + 1
        while i < end and text[i] != "]":
            _value, item_end = decoder.raw_decode(text, i)
            out.append((pre, _members(text, i) if text[i] == "{" else []))
            pre = item_end
            i = _skip_ws(text, item_end)
            if i < end and text[i] == ",":
                i = _skip_ws(text, i + 1)
        return out
    return []


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _reject_constant(name):
    raise ValueError(name)


def _policy_error(text, resource_arn, kind):
    """(message, parsed): AWS's validation message for this policy, or None with (document, statements).
    Checks run in the order Kinesis runs them (recorded in us-east-1)."""
    size = len(text.encode("utf-8"))
    if size == 0 or size > 20000:
        return "Resource policy size should be greater than 0 and less than 20000 bytes", None
    try:
        document = json.loads(text, parse_constant=_reject_constant)
    except (TypeError, ValueError):
        return "This policy contains invalid Json", None
    if not isinstance(document, dict):
        return "This policy contains invalid Json", None
    if "Version" not in document:
        return "Missing required field Version", None
    if document["Version"] not in ("2012-10-17", "2008-10-17"):
        return "The policy must contain a valid version string", None
    unknown = [k for k in document if k not in ("Version", "Id", "Statement")]
    if unknown:
        return f"Unknown field {unknown[0]}", None
    if "Statement" not in document:
        return "Missing required field Statement", None
    statements = document["Statement"]
    if isinstance(statements, list) and not statements:
        return "Could not parse the policy: Statement is empty!", None
    if not isinstance(statements, (list, dict)):
        return "Could not parse the policy: Statement is not well formatted.", None
    statements = _as_list(statements)
    positions = _statement_positions(text)

    def syntax(offset):
        return "Syntax error at position ({},{})".format(*_line_col(text, offset))

    # Fields and their types, statement by statement.
    for statement, (_pre, members) in zip(statements, positions):
        if not isinstance(statement, dict):
            return "Could not parse the policy: Statement is not well formatted.", None
        span = {key: (start, end) for key, start, end in members}

        def read_end(key):
            # Where the parser stands after reading the field's value.
            start, end = span[key]
            return start + 1 if text[start] in "{[" else end

        for key in statement:
            if key in ("NotPrincipal", "NotAction", "NotResource"):
                return f"Has prohibited field {key}", None
            if key not in _STATEMENT_FIELDS:
                return syntax(span[key][0] + 1), None
        if "Sid" in statement and not isinstance(statement["Sid"], str):
            return syntax(read_end("Sid")), None
        for field in ("Effect", "Principal", "Action", "Resource"):
            if field not in statement:
                return f"Missing required field {field}", None
        if not isinstance(statement["Effect"], str):
            return syntax(read_end("Effect")), None
        if statement["Effect"] not in ("Allow", "Deny"):
            return f"Invalid effect: {statement['Effect']}", None
        principal = statement["Principal"]
        if isinstance(principal, str) and principal != "*":
            return syntax(span["Principal"][1]), None
        if not isinstance(principal, (str, dict)) or (isinstance(principal, dict) and not all(
                isinstance(v, str) or (isinstance(v, list) and all(isinstance(x, str) for x in v))
                for v in principal.values())):
            return syntax(read_end("Principal")), None
        for field in ("Action", "Resource"):
            value = statement[field]
            if not (isinstance(value, str) or (isinstance(value, list) and all(isinstance(x, str) for x in value))):
                return syntax(read_end(field)), None
        actions = _as_list(statement["Action"])
        if not actions:
            return "Missing required field Action cannot be empty!", None
        if any(a != "*" and ":" not in a for a in actions):
            return "Could not parse vendor in action", None
        condition = statement.get("Condition")
        if condition is not None:
            if not isinstance(condition, dict):
                return syntax(read_end("Condition")), None
            for operator in condition:
                base = operator.split(":", 1)[-1]
                base = base[:-len("IfExists")] if base.endswith("IfExists") else base
                if base.lower() not in _CONDITION_OPERATORS:
                    return f"Invalid Condition type : {operator}", None

    # The rules AWS reports at a statement's position, in its order.
    for statement, (pre, _members) in zip(statements, positions):
        where = "Resource policy contains invalid statement at line {}, column {}: ".format(*_line_col(text, pre))
        sid = statement.get("Sid")
        if sid is not None and not re.fullmatch(r"[A-Za-z0-9]*", sid):
            return where + f"The following Statement Ids are invalid: {sid}, ", None
        actions = _as_list(statement["Action"])
        if any(a != "*" and a.split(":", 1)[0].lower() != "kinesis" for a in actions):
            return where + "Action field includes AWS services that are inconsistent with specified vendor", None
        wildcards = [a for a in actions if "*" in a or "?" in a]
        if wildcards:
            return where + f"The resource policy cannot contain invalid actions: [{', '.join(wildcards)}].", None
        invalid = [a for a in actions if a.lower() not in _POLICY_ACTIONS_LOWER[kind]]
        if invalid:
            return where + "The following action names are invalid: " + ", ".join(f'"{a}"' for a in invalid), None
        principal = statement["Principal"]
        if principal == "*" or (isinstance(principal, dict) and "*" in _as_list(principal.get("AWS", []))):
            return where + "The resource policy cannot contain the wildcard principal.", None
        for principal_kind in (principal if isinstance(principal, dict) else {}):
            if principal_kind != "AWS":
                return where + f"The resource policy cannot contain {principal_kind} principals.", None
        resources = _as_list(statement["Resource"])
        if len(resources) != 1:
            return where + "The resource policy must contain exactly one resource.", None
        if resources[0] != resource_arn:
            return where + "The resource policy's resource must be the same as the resource ARN.", None
    seen = set()
    for statement, (pre, _members) in zip(statements, positions):
        sid = statement.get("Sid")
        if sid and sid in seen:
            return ("Resource policy contains invalid statement at line {}, column {}: "
                    "The Statement Ids in the policy are not unique").format(*_line_col(text, pre)), None
        if sid:
            seen.add(sid)
    return None, (document, statements)


def _account_exists(account_id):
    """Whether MiniStack knows the account: the caller's own, or one that holds an IAM identity or a
    Kinesis stream. AWS answers from its real account list, which MiniStack does not have."""
    from ministack.services import iam

    if account_id == get_account_id():
        return True
    stores = (iam._roles._data, iam._users._data, _streams._data)
    return any(key[0] == account_id for store in stores for key in list(store))


def _principal_exists(principal):
    """An AWS principal names an account that exists, or an IAM role or user by its exact ARN."""
    from ministack.services import iam

    m = re.fullmatch(r"(\d{12})|arn:aws:iam::(\d{12}):root", principal)
    if m:
        return _account_exists(m.group(1) or m.group(2))
    m = re.fullmatch(r"arn:aws:iam::(\d{12}):(role|user)/.+", principal)
    if not m:
        return False
    store = iam._roles if m.group(2) == "role" else iam._users
    return any(key[0] == m.group(1) and isinstance(rec, dict) and rec.get("Arn") == principal
               for key, rec in list(store._data.items()))


def _stored_policy(document, statements):
    """The document as GetResourcePolicy returns it on AWS: compact, in AWS's key order, with one-item
    lists collapsed, account ids as root ARNs and repeated principals dropped. AWS's order of several
    principals changes between calls; this keeps the order they were sent in."""
    def one_or_list(values):
        return values[0] if len(values) == 1 else values

    def condition_value(value):
        return one_or_list([("true" if x else "false") if isinstance(x, bool)
                            else str(x) if isinstance(x, (int, float)) else x for x in _as_list(value)])

    out_statements = []
    for statement in statements:
        out = {}
        if "Sid" in statement:
            out["Sid"] = statement["Sid"]
        out["Effect"] = statement["Effect"]
        out["Principal"] = {kind: one_or_list(list(dict.fromkeys(
            f"arn:aws:iam::{name}:root" if re.fullmatch(r"\d{12}", name) else name
            for name in _as_list(names)))) for kind, names in statement["Principal"].items()}
        out["Action"] = one_or_list(_as_list(statement["Action"]))
        out["Resource"] = _as_list(statement["Resource"])[0]
        if "Condition" in statement:
            out["Condition"] = {op: {k: condition_value(v) for k, v in keys.items()}
                                for op, keys in statement["Condition"].items()}
        out_statements.append(out)
    stored = {"Version": document["Version"]}
    if "Id" in document:
        stored["Id"] = document["Id"]
    stored["Statement"] = out_statements
    return json.dumps(stored, separators=(",", ":"), ensure_ascii=False)


def _policy_resource(resource_arn, action, headers, policy=None):
    """(kind, error, stored): checks the ARN, its region and account, then the policy and its
    principals, then that the resource exists, in AWS's order."""
    if not isinstance(resource_arn, str) or not re.fullmatch(
            r"arn:aws[^:]*:kinesis:[^:]*:\d{12}:stream/[^/]+(/consumer/[^/]+:\d+)?", resource_arn):
        return None, error_response_json(
            "ValidationException",
            f"1 validation error detected: Value '{resource_arn}' at 'resourceARN' failed to satisfy constraint: "
            "Member must satisfy regular expression pattern: arn:aws.*:kinesis:.*:\\d{12}:.*(stream|channel|queue)/\\S+",
            400), None
    spec = parse_arn(resource_arn)
    if spec.region != get_region():
        return None, error_response_json(
            "InvalidArgumentException",
            f"The region specified in the ARN '{resource_arn}' does not match the endpoint region", 400), None
    if spec.account_id != get_account_id():
        if not _account_exists(spec.account_id):
            return None, error_response_json(
                "UnrecognizedClientException", "No account found for the given parameters", 400), None
        from ministack.services.sts import _caller_identity

        identity = _caller_identity(headers)
        caller = identity.get("userArn") if isinstance(identity, dict) else f"arn:aws:iam::{get_account_id()}:root"
        return None, error_response_json(
            "AccessDeniedException",
            f"User: {caller} is not authorized to perform: kinesis:{action} on resource: {resource_arn} "
            f"because no resource-based policy allows the kinesis:{action} action", 400), None
    kind = "consumer" if "/consumer/" in spec.resource else "stream"
    stored = None
    if policy is not None:
        message, parsed = _policy_error(policy, resource_arn, kind)
        if message:
            return None, error_response_json("InvalidArgumentException", f"Policy validation error: {message}", 400), None
        document, statements = parsed
        if any(isinstance(v, list) and not v for s in statements for v in s["Principal"].values()):
            return None, error_response_json("InvalidArgumentException", _NO_PRINCIPALS, 400), None
        principals = [n for s in statements for v in s["Principal"].values() for n in _as_list(v)]
        if not all(_principal_exists(p) for p in principals):
            return None, error_response_json("InvalidArgumentException", "Invalid principal in policy document.", 400), None
        stored = _stored_policy(document, statements)
    stream_name, _, consumer = spec.resource.split("/", 1)[1].partition("/consumer/")
    if kind == "stream" and not _resolve_stream_by_arn(resource_arn):
        return None, error_response_json(
            "ResourceNotFoundException", f"Stream {stream_name} under account {spec.account_id} not found.", 400), None
    if kind == "consumer" and not _consumer_from_arn(resource_arn):
        consumer_name, _, created = consumer.rpartition(":")
        return None, error_response_json(
            "ResourceNotFoundException", f"Consumer {consumer_name} with creation timestamp {created} under stream "
            f"{stream_name} and account {spec.account_id} not found.", 400), None
    return kind, None, stored


def stream_policy_for_arn(resource_arn):
    """The resource policy and owner account of a stream or consumer, for the IAM evaluator; looked up
    in the ARN's own account and region, since a cross-account caller's scope is its own account."""
    from ministack.core.responses import request_scope

    account_id, region = _kinesis_arn_scope(resource_arn)
    with request_scope(account_id, region):
        if not (_resolve_stream_by_arn(resource_arn) or _consumer_from_arn(resource_arn)):
            return None
        return _resource_policies.get(resource_arn) or "", account_id


# What an event source mapping reads from a stream (Lambda developer guide, Kinesis permissions).
_MAPPING_READ_ACTIONS = ("kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
                         "kinesis:GetShardIterator", "kinesis:ListShards")


def mapping_read_denied(stream_arn, role_arn, function_name, mapping_account):
    """The first read a cross-account mapping's execution role may not make on the stream, or None.
    Only the stream's resource policy can grant another account."""
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy

    found = stream_policy_for_arn(stream_arn)
    policy = found[0] if found else ""
    role_name = role_arn.rsplit("/", 1)[-1] if ":role/" in role_arn else ""
    session = f"arn:aws:sts::{mapping_account}:assumed-role/{role_name}/{function_name}"
    for action in _MAPPING_READ_ACTIONS:
        ctx = EvalContext(principal_arn=session, principal_type="AssumedRole", principal_account=mapping_account,
                          action=action, resource_arn=stream_arn, region=_kinesis_arn_scope(stream_arn)[1])
        if not policy or evaluate_resource_policy(policy, ctx).decision != "Allow":
            return session, action
    return None


def _put_resource_policy(data, headers):
    resource_arn, policy = data.get("ResourceARN"), data.get("Policy")
    _kind, err, stored = _policy_resource(resource_arn, "PutResourcePolicy", headers,
                                          policy if isinstance(policy, str) else "")
    if err:
        return err
    _resource_policies[resource_arn] = stored
    return json_response({})


def _get_resource_policy(data, headers):
    resource_arn = data.get("ResourceARN")
    _kind, err, _stored = _policy_resource(resource_arn, "GetResourcePolicy", headers)
    if err:
        return err
    return json_response({"Policy": _resource_policies.get(resource_arn) or "{}"})


def _delete_resource_policy(data, headers):
    resource_arn = data.get("ResourceARN")
    _kind, err, _stored = _policy_resource(resource_arn, "DeleteResourcePolicy", headers)
    if err:
        return err
    if resource_arn not in _resource_policies:
        return error_response_json(
            "ResourceNotFoundException", f"No resource policy found for resource ARN {resource_arn}.", 400)
    del _resource_policies[resource_arn]
    return json_response({})


def reset():
    _streams.clear()
    _shard_iterators.clear()
    _consumers.clear()
    _resource_policies.clear()
    _subscriptions.clear()
