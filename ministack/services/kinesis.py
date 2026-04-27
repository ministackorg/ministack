"""
Kinesis Data Streams Emulator.
JSON-based API via X-Amz-Target (Kinesis_20131202).
Supports: CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary,
          ListStreams, PutRecord, PutRecords, GetShardIterator, GetRecords,
          MergeShards, SplitShard, UpdateShardCount, ListShards,
          IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod,
          AddTagsToStream, RemoveTagsFromStream, ListTagsForStream,
          RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers,
          DescribeStreamConsumer,
          StartStreamEncryption, StopStreamEncryption,
          EnableEnhancedMonitoring, DisableEnhancedMonitoring.
"""

import base64
import copy
import os
import hashlib
import json
import logging
import threading
import time

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, get_region

logger = logging.getLogger("kinesis")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
MAX_HASH_KEY = (2**128) - 1
ITERATOR_EXPIRY_SECONDS = 300

from ministack.core.persistence import load_state, PERSIST_STATE

_streams = AccountScopedDict()
_shard_iterators = AccountScopedDict()
_consumers = AccountScopedDict()
_sequence_counter = 0
_sequence_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "streams": copy.deepcopy(_streams),
        "consumers": copy.deepcopy(_consumers),
    }


def restore_state(data):
    if data:
        _streams.update(data.get("streams", {}))
        _consumers.update(data.get("consumers", {}))


try:
    _restored = load_state("kinesis")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


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
        for n, s in _streams.items():
            if s["StreamARN"] == arn:
                return n, s
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
    stream = next((s for s in _streams.values() if s.get("StreamARN") == stream_arn), None)
    if not stream or stream.get("StreamStatus") != "ACTIVE":
        return False
    _expire_records(stream)
    hash_int = _partition_key_to_hash(partition_key)
    shard_id = _route_to_shard(hash_int, stream)
    stream["shards"][shard_id]["records"].append({
        "SequenceNumber": _next_sequence_number(),
        "ApproximateArrivalTimestamp": int(time.time()),
        "Data": base64.b64encode(data).decode("ascii"),
        "PartitionKey": partition_key,
    })
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
        "StartStreamEncryption": _start_stream_encryption,
        "StopStreamEncryption": _stop_stream_encryption,
        "EnableEnhancedMonitoring": _enable_enhanced_monitoring,
        "DisableEnhancedMonitoring": _disable_enhanced_monitoring,
    }

    handler = handlers.get(action)
    if not handler:
        if is_cbor:
            return _cbor_response({"__type": "InvalidAction", "message": f"Unknown action: {action}"}, 400)
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)

    status, resp_headers, resp_body = handler(data)
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
        shard_ids = [s for s in shard_ids if s > next_token]

    page = shard_ids[:max_results]
    result = {"Shards": [_shard_out(sid, stream["shards"][sid]) for sid in page]}
    if len(shard_ids) > max_results:
        result["NextToken"] = page[-1]
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
        "ApproximateArrivalTimestamp": int(time.time()),
        "Data": record_data,
        "PartitionKey": partition_key,
    })
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
    for rec in records:
        pk = rec.get("PartitionKey", "")
        rd = rec.get("Data", "")
        eh = rec.get("ExplicitHashKey")
        hash_int = int(eh) if eh else _partition_key_to_hash(pk)
        sid = _route_to_shard(hash_int, stream)
        seq = _next_sequence_number()
        stream["shards"][sid]["records"].append({
            "SequenceNumber": seq,
            "ApproximateArrivalTimestamp": int(time.time()),
            "Data": rd,
            "PartitionKey": pk,
        })
        results.append({
            "SequenceNumber": seq,
            "ShardId": sid,
            "EncryptionType": stream.get("EncryptionType", "NONE"),
        })
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

    if it_type == "TRIM_HORIZON":
        position = 0
    elif it_type == "LATEST":
        position = len(records)
    elif it_type == "AT_SEQUENCE_NUMBER":
        position = next((i for i, r in enumerate(records) if r["SequenceNumber"] >= seq), len(records))
    elif it_type == "AFTER_SEQUENCE_NUMBER":
        position = next((i for i, r in enumerate(records) if r["SequenceNumber"] > seq), len(records))
    elif it_type == "AT_TIMESTAMP":
        if at_ts is None:
            return error_response_json("ValidationException", "Timestamp required for AT_TIMESTAMP", 400)
        ts_val = float(at_ts)
        position = next((i for i, r in enumerate(records) if r["ApproximateArrivalTimestamp"] >= ts_val), len(records))
    else:
        return error_response_json("ValidationException", f"Invalid ShardIteratorType: {it_type}", 400)

    resolved_name = name if name else next((n for n, s in _streams.items() if s is stream), "")
    token = new_uuid()
    _shard_iterators[token] = {
        "stream": resolved_name,
        "shard_id": shard_id,
        "position": position,
        "created_at": time.time(),
    }
    return json_response({"ShardIterator": token})


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
    stream = next((s for s in _streams.values() if s["StreamARN"] == stream_arn), None)
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
        if consumer_arn not in _consumers:
            return error_response_json("ResourceNotFoundException", "Consumer not found", 400)
        del _consumers[consumer_arn]
    elif stream_arn and consumer_name:
        found = next((a for a, c in _consumers.items()
                       if c["StreamARN"] == stream_arn and c["ConsumerName"] == consumer_name), None)
        if not found:
            return error_response_json("ResourceNotFoundException", "Consumer not found", 400)
        del _consumers[found]
    else:
        return error_response_json("ValidationException",
                                   "ConsumerARN or StreamARN+ConsumerName required", 400)
    return json_response({})


def _list_consumers(data):
    stream_arn = data.get("StreamARN")
    if not stream_arn:
        return error_response_json("ValidationException", "StreamARN is required", 400)
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
        consumer = _consumers.get(consumer_arn)
    elif stream_arn and consumer_name:
        consumer = next(
            (c for c in _consumers.values()
             if c["StreamARN"] == stream_arn and c["ConsumerName"] == consumer_name),
            None,
        )

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
    }


def _cbor_response(data: dict, status: int = 200):
    import cbor2
    body = cbor2.dumps(data)
    return status, {"Content-Type": "application/x-amz-cbor-1.1"}, body


def reset():
    _streams.clear()
    _shard_iterators.clear()
    _consumers.clear()
