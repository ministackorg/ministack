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
import threading
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_epoch,
)

logger = logging.getLogger("firehose")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ─── in-memory state ──────────────────────────────────────────────────────────

_streams = AccountScopedDict()          # name -> stream descriptor
_lock = threading.Lock()
_dest_counter = 0


def reset():
    global _streams, _dest_counter
    with _lock:
        _streams = {}
        _dest_counter = 0


def get_state() -> dict:
    return copy.deepcopy({"_streams": _streams, "_dest_counter": _dest_counter})


def restore_state(data: dict):
    global _streams, _dest_counter
    _streams.update(data.get("_streams", {}))
    _dest_counter = data.get("_dest_counter", _dest_counter)


try:
    _restored = load_state("firehose")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ─── helpers ─────────────────────────────────────────────────────────────────

def _stream_arn(name: str) -> str:
    return f"arn:aws:firehose:{get_region()}:{get_account_id()}:deliverystream/{name}"


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
        for opt in ("ProcessingConfiguration", "CloudWatchLoggingOptions",
                    "DataFormatConversionConfiguration", "DynamicPartitioningConfiguration"):
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


def _deliver_to_s3(stream: dict, dest: dict, record_data: bytes):
    """Best-effort delivery of a record to the local S3 emulator.

    Called while holding _lock so must not block the event loop.
    Schedules a coroutine on the running loop (fire-and-forget).
    """
    try:
        from ministack.services import s3 as s3_svc

        cfg = dest["config"]
        bucket_arn = cfg.get("BucketARN", "")
        bucket = bucket_arn.split(":::")[-1] if ":::" in bucket_arn else bucket_arn
        prefix = cfg.get("Prefix", "")
        ts = time.strftime("%Y/%m/%d/%H", time.gmtime())
        key = f"{prefix}{ts}/{stream['name']}-{new_uuid()}"

        async def _put():
            fake_headers = {
                "content-type": "application/octet-stream",
                "content-length": str(len(record_data)),
                "host": "s3.localhost",
            }
            await s3_svc.handle_request("PUT", f"/{bucket}/{key}", fake_headers, record_data, {})

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_put())
        except RuntimeError:
            asyncio.run(_put())
    except Exception as e:
        logger.warning("Firehose S3 delivery failed: %s", e)


def _record_id() -> str:
    """Generate a Firehose-style RecordId (long numeric string)."""
    ts = int(time.time() * 1000)
    uid = new_uuid().replace("-", "")
    return f"{ts:020d}{uid}"


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
            destinations.append({
                "id": _next_dest_id(),
                "type": dtype,
                "config": cfg,
                "records": [],
            })

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
            stream["kinesis_source"] = {
                "KinesisStreamARN": ks_cfg.get("KinesisStreamARN", ""),
                "RoleARN": ks_cfg.get("RoleARN", ""),
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
            names = names[idx + 1:]
        except ValueError:
            pass

    has_more = len(names) > limit
    return json_response({
        "DeliveryStreamNames": names[:limit],
        "HasMoreDeliveryStreams": has_more,
    })


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
                _deliver_to_s3(stream, dest, decoded)

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
                        _deliver_to_s3(stream, dest, decoded)
                responses.append({"RecordId": record_id, "Encrypted": False})
            except Exception as e:
                failed += 1
                responses.append({
                    "ErrorCode": "ServiceUnavailableException",
                    "ErrorMessage": str(e),
                })

    return json_response({
        "FailedPutCount": failed,
        "Encrypted": False,
        "RequestResponses": responses,
    })


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
            all_tags = all_tags[idx + 1:]
        except StopIteration:
            pass

    has_more = len(all_tags) > limit
    return json_response({
        "Tags": all_tags[:limit],
        "HasMoreTags": has_more,
    })


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


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    # Target format: Firehose_20150804.OperationName
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
