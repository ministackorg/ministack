"""
CloudWatch Logs Service Emulator.
JSON-based API via X-Amz-Target (Logs_20140328).
Supports: CreateLogGroup, DeleteLogGroup, DescribeLogGroups,
          CreateLogStream, DeleteLogStream, DescribeLogStreams,
          PutLogEvents, GetLogEvents, FilterLogEvents, GetLogRecord, GetLogGroupFields,
          StartLiveTail,
          PutRetentionPolicy, DeleteRetentionPolicy,
          PutSubscriptionFilter, DeleteSubscriptionFilter, DescribeSubscriptionFilters,
          TagLogGroup, UntagLogGroup, ListTagsLogGroup,
          TagResource, UntagResource, ListTagsForResource,
          PutDestination, DeleteDestination, DescribeDestinations,
          PutDestinationPolicy,
          PutMetricFilter, DeleteMetricFilter, DescribeMetricFilters,
          StartQuery, GetQueryResults, StopQuery,
          PutDeliverySource, GetDeliverySource, DeleteDeliverySource, DescribeDeliverySources,
          PutDeliveryDestination, GetDeliveryDestination, DeleteDeliveryDestination, DescribeDeliveryDestinations,
          CreateDelivery, GetDelivery, DeleteDelivery, DescribeDeliveries.
"""

import base64
import contextlib
import copy
import fnmatch
import json
import logging
import os
import re
import time
import zlib
from datetime import datetime, timezone

from ministack.core.arn import ArnParseError, parse_arn
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

logger = logging.getLogger("logs")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import load_state

_log_groups = AccountRegionScopedDict()
# group_name -> {
#   arn, creationTime, retentionInDays (int|None), tags: {str: str},
#   subscriptionFilters: {filterName: {filterName, logGroupName, filterPattern,
#                                      destinationArn, roleArn, distribution, creationTime}},
#   streams: {stream_name: {events: [{timestamp, message, ingestionTime}],
#             uploadSequenceToken, creationTime,
#             firstEventTimestamp, lastEventTimestamp, lastIngestionTime}},
# }

# Region-scoped: CW Logs destinations and the vended-logs delivery resources
# are region-specific in AWS (were account-only → leaked across regions). Each
# carries an ARN, so legacy account-scoped state migrates to its ARN's region.
_destinations = AccountRegionScopedDict()
# dest_name -> {destinationName, targetArn, roleArn, accessPolicy, arn, creationTime}

_metric_filters = AccountRegionScopedDict()
# (log_group_name, filter_name) -> {filterName, logGroupName, filterPattern, metricTransformations, creationTime}

_queries = AccountRegionScopedDict()
# query_id -> {queryId, logGroupName, startTime, endTime, queryString, status,
#              results, statistics}

# Opaque Insights/GetLogRecord pointers → full transformed log record fields.
# Keys are UUID strings assigned at PutLogEvents time (AWS-shaped: opaque).
_log_records = AccountRegionScopedDict()
# ptr -> {
#   "@ptr", "@timestamp", "@message", "@logStream", "@log",
#   "_timestamp_ms" (internal; stripped from GetLogRecord responses)
# }

_delivery_sources = AccountRegionScopedDict()
# source_name -> {name, arn, resourceArns: [str], logType, service, tags}

_delivery_destinations = AccountRegionScopedDict()
# dest_name -> {name, arn, deliveryDestinationType, outputFormat,
#               deliveryDestinationConfiguration: {destinationResourceArn}, tags}

_deliveries = AccountRegionScopedDict()
# delivery_id -> {id, arn, deliverySourceName, deliveryDestinationArn,
#                 deliveryDestinationType, recordFields, fieldDelimiter,
#                 s3DeliveryConfiguration, tags}

# Active StartLiveTail sessions (not persisted). session_id -> session dict.
# Populated while an ASGI stream is open; PutLogEvents fans matching events in.
_live_tail_sessions: dict[str, dict] = {}

# AWS Live Tail: one sessionUpdate per second (empty when idle), and at most
# 10 buffered sessionUpdate events before the oldest is dropped.
_LIVE_TAIL_IDLE_SECONDS = 1.0
_LIVE_TAIL_QUEUE_MAX = 10


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "log_groups": copy.deepcopy(_log_groups),
        "destinations": copy.deepcopy(_destinations),
        "metric_filters": copy.deepcopy(_metric_filters),
        "queries": copy.deepcopy(_queries),
        "log_records": copy.deepcopy(_log_records),
        "delivery_sources": copy.deepcopy(_delivery_sources),
        "delivery_destinations": copy.deepcopy(_delivery_destinations),
        "deliveries": copy.deepcopy(_deliveries),
    }


def _region_for_log_group(account_id: str, log_group_name: str | None) -> str | None:
    if not log_group_name:
        return None
    for (acct, region, name), _group in _log_groups.all_items():
        if acct == account_id and name == log_group_name:
            return region
    return None


def _metric_filter_log_group_name(key, value) -> str | None:
    if isinstance(key, (list, tuple)) and key:
        return key[0]
    if isinstance(value, dict):
        return value.get("logGroupName")
    return None


def _metric_filter_restore_region(account_id: str, key, value) -> str:
    group_name = _metric_filter_log_group_name(key, value)
    return (
        _region_for_log_group(account_id, group_name)
        or _metric_filters._region_for_legacy_value(key, value)
    )


def _query_restore_region(account_id: str, value) -> str:
    if isinstance(value, dict):
        group_names = []
        if value.get("logGroupName"):
            group_names.append(value["logGroupName"])
        group_names.extend(value.get("logGroupNames") or [])
        for group_name in group_names:
            region = _region_for_log_group(account_id, group_name)
            if region:
                return region
    return get_region()


def _restore_metric_filters(metric_filters):
    if isinstance(metric_filters, AccountRegionScopedDict):
        _metric_filters.update(metric_filters)
        return
    if isinstance(metric_filters, AccountScopedDict):
        for (account_id, key), value in metric_filters._data.items():
            region = _metric_filter_restore_region(account_id, key, value)
            _metric_filters.set_scoped(account_id, region, key, value)
        return
    if isinstance(metric_filters, dict):
        account_id = get_account_id()
        for key, value in metric_filters.items():
            region = _metric_filter_restore_region(account_id, key, value)
            _metric_filters.set_scoped(account_id, region, key, value)


def _restore_queries(queries):
    if isinstance(queries, AccountRegionScopedDict):
        _queries.update(queries)
        return
    if isinstance(queries, AccountScopedDict):
        for (account_id, key), value in queries._data.items():
            _queries.set_scoped(account_id, _query_restore_region(account_id, value), key, value)
        return
    if isinstance(queries, dict):
        account_id = get_account_id()
        for key, value in queries.items():
            if isinstance(key, tuple) and len(key) == 3:
                _queries.set_scoped(key[0], key[1], key[2], value)
            elif isinstance(key, tuple) and len(key) == 2:
                _queries.set_scoped(key[0], _query_restore_region(key[0], value), key[1], value)
            else:
                _queries.set_scoped(account_id, _query_restore_region(account_id, value), key, value)


def restore_state(data):
    if data:
        _log_groups.update(data.get("log_groups", {}))
        _destinations.update(data.get("destinations", {}))
        _restore_metric_filters(data.get("metric_filters", {}))
        _restore_queries(data.get("queries", {}))
        _log_records.update(data.get("log_records", {}))
        _delivery_sources.update(data.get("delivery_sources", {}))
        _delivery_destinations.update(data.get("delivery_destinations", {}))
        _deliveries.update(data.get("deliveries", {}))


try:
    _restored = load_state("cloudwatch_logs")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group_arn(name):
    return f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{name}:*"


def _make_group_arn_no_star(name):
    """ARN form accepted by StartLiveTail (no trailing ``:*``)."""
    return f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{name}"


def _resolve_group_by_arn(arn):
    """Return the group name whose ARN matches, or None.
    Accepts both 'arn:...:log-group:name' and 'arn:...:log-group:name:*'
    since Terraform and the AWS console use both forms."""
    arn_normalized = arn.rstrip(":*")
    for name, g in _log_groups.items():
        if g["arn"].rstrip(":*") == arn_normalized:
            return name
    return None


def _log_group_name_from_identifier_arn(identifier: str) -> str | None:
    try:
        spec = parse_arn(identifier)
    except ArnParseError:
        return None
    if (
        spec.service != "logs"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None
    prefix = "log-group:"
    if not spec.resource.startswith(prefix):
        return None
    name = spec.resource[len(prefix):]
    if name.endswith(":*"):
        name = name[:-2]
    return name or None


def _decode_token(token):
    """Decode a pagination token to an integer offset."""
    if not token:
        return 0
    try:
        return int(base64.b64decode(token))
    except Exception:
        return 0


def _encode_token(offset):
    return base64.b64encode(str(offset).encode()).decode()


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateLogGroup": _create_log_group,
        "DeleteLogGroup": _delete_log_group,
        "DescribeLogGroups": _describe_log_groups,
        "CreateLogStream": _create_log_stream,
        "DeleteLogStream": _delete_log_stream,
        "DescribeLogStreams": _describe_log_streams,
        "PutLogEvents": _put_log_events,
        "GetLogEvents": _get_log_events,
        "FilterLogEvents": _filter_log_events,
        "GetLogRecord": _get_log_record,
        "GetLogGroupFields": _get_log_group_fields,
        "StartLiveTail": _start_live_tail,
        "PutRetentionPolicy": _put_retention_policy,
        "DeleteRetentionPolicy": _delete_retention_policy,
        "PutSubscriptionFilter": _put_subscription_filter,
        "DeleteSubscriptionFilter": _delete_subscription_filter,
        "DescribeSubscriptionFilters": _describe_subscription_filters,
        "TagLogGroup": _tag_log_group,
        "UntagLogGroup": _untag_log_group,
        "ListTagsLogGroup": _list_tags_log_group,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "PutDestination": _put_destination,
        "DeleteDestination": _delete_destination,
        "DescribeDestinations": _describe_destinations,
        "PutDestinationPolicy": _put_destination_policy,
        "PutMetricFilter": _put_metric_filter,
        "DeleteMetricFilter": _delete_metric_filter,
        "DescribeMetricFilters": _describe_metric_filters,
        "StartQuery": _start_query,
        "GetQueryResults": _get_query_results,
        "StopQuery": _stop_query,
        "PutDeliverySource": _put_delivery_source,
        "GetDeliverySource": _get_delivery_source,
        "DeleteDeliverySource": _delete_delivery_source,
        "DescribeDeliverySources": _describe_delivery_sources,
        "PutDeliveryDestination": _put_delivery_destination,
        "GetDeliveryDestination": _get_delivery_destination,
        "DeleteDeliveryDestination": _delete_delivery_destination,
        "DescribeDeliveryDestinations": _describe_delivery_destinations,
        "CreateDelivery": _create_delivery,
        "GetDelivery": _get_delivery,
        "DeleteDelivery": _delete_delivery,
        "DescribeDeliveries": _describe_deliveries,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidOperationException", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Log groups
# ---------------------------------------------------------------------------

def _create_log_group(data):
    name = data.get("logGroupName")
    if not name:
        return error_response_json("InvalidParameterException", "logGroupName is required.", 400)
    if name in _log_groups:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"The specified log group already exists: {name}", 400,
        )
    _log_groups[name] = {
        "arn": _make_group_arn(name),
        "creationTime": int(time.time() * 1000),
        "retentionInDays": None,
        "tags": dict(data.get("tags", {})),
        "subscriptionFilters": {},
        "streams": {},
    }
    return json_response({})


def _delete_log_group(data):
    name = data.get("logGroupName")
    if name not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {name}", 400,
        )
    _forget_log_records_for_group(name)
    del _log_groups[name]
    return json_response({})


def _describe_log_groups(data):
    prefix = data.get("logGroupNamePrefix")
    pattern = data.get("logGroupNamePattern")
    limit = min(data.get("limit", 50), 50)
    token = data.get("nextToken")

    if prefix and pattern:
        return error_response_json(
            "InvalidParameterException",
            "logGroupNamePrefix and logGroupNamePattern are mutually exclusive.", 400,
        )

    names = sorted(_log_groups.keys())
    if prefix:
        names = [n for n in names if n.startswith(prefix)]
    elif pattern:
        pat = pattern.lower()
        names = [n for n in names if pat in n.lower()]

    start = _decode_token(token)
    page = names[start:start + limit]

    groups = []
    for n in page:
        g = _log_groups[n]
        entry = {
            "logGroupName": n,
            "arn": g["arn"],
            "logGroupArn": _make_group_arn_no_star(n),
            "creationTime": g["creationTime"],
            "storedBytes": sum(
                sum(len(e.get("message", "")) for e in s["events"])
                for s in g["streams"].values()
            ),
            "metricFilterCount": sum(1 for k in _metric_filters if k[0] == n),
        }
        if g.get("retentionInDays") is not None:
            entry["retentionInDays"] = g["retentionInDays"]
        groups.append(entry)

    resp: dict = {"logGroups": groups}
    end = start + limit
    if end < len(names):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Log streams
# ---------------------------------------------------------------------------

def _create_log_stream(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    if not group or not stream:
        return error_response_json(
            "InvalidParameterException", "logGroupName and logStreamName are required.", 400,
        )
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if stream in _log_groups[group]["streams"]:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"The specified log stream already exists: {stream}", 400,
        )
    _log_groups[group]["streams"][stream] = {
        "events": [],
        "uploadSequenceToken": "1",
        "creationTime": int(time.time() * 1000),
        "firstEventTimestamp": None,
        "lastEventTimestamp": None,
        "lastIngestionTime": None,
    }
    return json_response({})


def _delete_log_stream(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if stream not in _log_groups[group]["streams"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log stream does not exist: {stream}", 400,
        )
    _forget_log_records_for_stream(group, stream)
    del _log_groups[group]["streams"][stream]
    return json_response({})


def _describe_log_streams(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )

    prefix = data.get("logStreamNamePrefix", "")
    order = data.get("orderBy", "LogStreamName")
    descending = data.get("descending", False)
    limit = min(data.get("limit", 50), 50)
    token = data.get("nextToken")

    all_streams = _log_groups[group]["streams"]
    names = sorted(all_streams.keys())

    if prefix:
        names = [n for n in names if n.startswith(prefix)]

    if order == "LastEventTime":
        names.sort(key=lambda n: all_streams[n].get("lastEventTimestamp") or 0, reverse=descending)
    elif descending:
        names.reverse()

    start = _decode_token(token)
    page = names[start:start + limit]

    streams = []
    for n in page:
        s = all_streams[n]
        entry = {
            "logStreamName": n,
            "creationTime": s["creationTime"],
            "storedBytes": sum(len(e.get("message", "")) for e in s["events"]),
            "uploadSequenceToken": s["uploadSequenceToken"],
            "arn": f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{group}:log-stream:{n}",
        }
        if s.get("firstEventTimestamp") is not None:
            entry["firstEventTimestamp"] = s["firstEventTimestamp"]
        if s.get("lastEventTimestamp") is not None:
            entry["lastEventTimestamp"] = s["lastEventTimestamp"]
        if s.get("lastIngestionTime") is not None:
            entry["lastIngestionTime"] = s["lastIngestionTime"]
        streams.append(entry)

    resp: dict = {"logStreams": streams}
    end = start + limit
    if end < len(names):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Log events
# ---------------------------------------------------------------------------

def _put_log_events(data):
    group = data.get("logGroupName")
    stream = data.get("logStreamName")
    events = data.get("logEvents", [])

    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if stream not in _log_groups[group]["streams"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log stream does not exist: {stream}", 400,
        )

    s = _log_groups[group]["streams"][stream]
    now_ms = int(time.time() * 1000)

    for e in events:
        ts = e.get("timestamp", now_ms)
        msg = e.get("message", "")
        ptr = new_uuid()
        s["events"].append({
            "timestamp": ts,
            "message": msg,
            "ingestionTime": now_ms,
            "eventId": ptr,
        })
        _log_records[ptr] = _build_log_record(
            ptr=ptr,
            timestamp_ms=ts,
            message=msg,
            log_group=group,
            log_stream=stream,
        )

        if s["firstEventTimestamp"] is None or ts < s["firstEventTimestamp"]:
            s["firstEventTimestamp"] = ts
        if s["lastEventTimestamp"] is None or ts > s["lastEventTimestamp"]:
            s["lastEventTimestamp"] = ts
        s["lastIngestionTime"] = now_ms

    token = str(int(s["uploadSequenceToken"]) + 1)
    s["uploadSequenceToken"] = token

    _fanout_to_subscription_filters(group, stream, events)
    # Fan out the stored shape (ingestionTime filled) to any Live Tail sessions.
    stored = s["events"][-len(events) :] if events else []
    _fanout_to_live_tail_sessions(group, stream, stored)
    return json_response({"nextSequenceToken": token})


def _format_insights_timestamp(timestamp_ms: int) -> str:
    """AWS Logs Insights @timestamp string (UTC, millisecond precision)."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _build_log_record(*, ptr, timestamp_ms, message, log_group, log_stream):
    return {
        "@ptr": ptr,
        "@timestamp": _format_insights_timestamp(timestamp_ms),
        "@message": message,
        "@logStream": log_stream,
        "@log": log_group,
        "_timestamp_ms": timestamp_ms,
    }


def _public_log_record(record: dict) -> dict:
    """Strip MiniStack-internal fields before returning GetLogRecord payload."""
    return {k: v for k, v in record.items() if not k.startswith("_")}


def _forget_log_records_for_stream(group_name: str, stream_name: str) -> None:
    to_delete = [
        ptr
        for ptr, record in list(_log_records.items())
        if record.get("@log") == group_name and record.get("@logStream") == stream_name
    ]
    for ptr in to_delete:
        del _log_records[ptr]


def _forget_log_records_for_group(group_name: str) -> None:
    to_delete = [
        ptr for ptr, record in list(_log_records.items()) if record.get("@log") == group_name
    ]
    for ptr in to_delete:
        del _log_records[ptr]


def _get_log_record(data):
    """Retrieve the full transformed field map for one Insights ``@ptr``.

    AWS accepts ``unmask``; MiniStack does not implement field masking, so the
    flag is accepted and ignored. Invalid/missing pointers raise
    ``InvalidParameterException`` (AWS behavior).
    """
    ptr = data.get("logRecordPointer")
    if not ptr:
        return error_response_json(
            "InvalidParameterException",
            "1 validation error detected: Value null at 'logRecordPointer' "
            "failed to satisfy constraint: Member must not be null",
            400,
        )
    # Accepted for SDK/AWS parity; masking is not implemented.
    _ = data.get("unmask", False)

    record = _log_records.get(ptr)
    if not record:
        return error_response_json(
            "InvalidParameterException",
            "Invalid logRecordPointer provided",
            400,
        )
    return json_response({"logRecord": _public_log_record(record)})


_SYSTEM_LOG_GROUP_FIELDS = (
    "@timestamp",
    "@message",
    "@logStream",
    "@ptr",
    "@log",
)


def _json_field_names(message: str) -> set[str]:
    """Discover dotted field names from a JSON object message (AWS-style flatten)."""
    try:
        parsed = json.loads(message)
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(parsed, dict):
        return set()

    names: set[str] = set()

    def walk(obj, prefix: str = "") -> None:
        if not isinstance(obj, dict):
            return
        for key, value in obj.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            names.add(name)
            if isinstance(value, dict):
                walk(value, name)
            elif isinstance(value, list):
                # AWS Insights discovers array members with numeric index segments
                # (e.g. items.0.id), not flattened under the bare parent key.
                for idx, item in enumerate(value):
                    indexed = f"{name}.{idx}"
                    if isinstance(item, dict):
                        names.add(indexed)
                        walk(item, indexed)
                    else:
                        names.add(indexed)

    walk(parsed)
    return names


def _resolve_log_group_fields_target(data) -> tuple[str | None, object | None]:
    """Return (group_name, error_response). Exactly one of name/identifier required."""
    name = data.get("logGroupName")
    identifier = data.get("logGroupIdentifier")
    has_name = bool(name)
    has_identifier = bool(identifier)
    if has_name == has_identifier:
        return None, error_response_json(
            "InvalidParameterException",
            "Exactly one of logGroupName or logGroupIdentifier must be specified",
            400,
        )
    if has_name:
        return name, None
    if identifier.startswith("arn:") or ":log-group:" in identifier:
        resolved = _log_group_name_from_identifier_arn(identifier)
        if not resolved:
            return None, error_response_json(
                "ResourceNotFoundException",
                f"The specified log group does not exist: {identifier}",
                400,
            )
        return resolved, None
    return identifier, None


def _get_log_group_fields(data):
    """Return field names present in recent events, with rough presence percent.

    Time window matches AWS: optional ``time`` (epoch seconds) searches ±8 minutes
    around that center; otherwise the most recent 15 minutes. JSON object messages
    contribute flattened dotted keys; system ``@*`` fields are counted when present
    on the stored Insights record.
    """
    group_name, err = _resolve_log_group_fields_target(data)
    if err is not None:
        return err
    if group_name not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group_name}",
            400,
        )

    if data.get("time") is not None:
        center = int(data["time"])
        start_s = center - 8 * 60
        end_s = center + 8 * 60
    else:
        # _collect_query_records takes epoch seconds and uses end_s*1000 as an
        # inclusive ms bound. Flooring "now" to seconds would drop events in the
        # current partial second (timestamp > end_s*1000), so extend end by 1s.
        end_s = int(time.time()) + 1
        start_s = end_s - 15 * 60

    records = _collect_query_records([group_name], start_s, end_s)
    total = len(records)
    if total == 0:
        return json_response({"logGroupFields": []})

    counts: dict[str, int] = {}
    for record in records:
        present: set[str] = set()
        for field in _SYSTEM_LOG_GROUP_FIELDS:
            if field in record and record.get(field) is not None:
                present.add(field)
        present.update(_json_field_names(record.get("@message", "")))
        for field in present:
            counts[field] = counts.get(field, 0) + 1

    fields = []
    for name, count in counts.items():
        percent = int(round(100.0 * count / total))
        percent = max(0, min(100, percent))
        fields.append({"name": name, "percent": percent})
    fields.sort(key=lambda item: (-item["percent"], item["name"]))
    return json_response({"logGroupFields": fields})


# ---------------------------------------------------------------------------
# Live Tail (StartLiveTail eventstream)
# ---------------------------------------------------------------------------

def _es_encode_message(headers: dict[str, str], payload: bytes) -> bytes:
    """Encode one ``application/vnd.amazon.eventstream`` message (AWS wire format)."""
    hdr_bytes = bytearray()
    for name, value in headers.items():
        name_b = name.encode("utf-8")
        val_b = value.encode("utf-8")
        hdr_bytes.append(len(name_b))
        hdr_bytes.extend(name_b)
        hdr_bytes.append(7)  # string
        hdr_bytes.extend(len(val_b).to_bytes(2, "big"))
        hdr_bytes.extend(val_b)

    headers_length = len(hdr_bytes)
    total_length = 12 + headers_length + len(payload) + 4
    prelude = total_length.to_bytes(4, "big") + headers_length.to_bytes(4, "big")
    prelude_crc = zlib.crc32(prelude).to_bytes(4, "big")
    msg_head = prelude + prelude_crc + bytes(hdr_bytes) + payload
    message_crc = zlib.crc32(msg_head).to_bytes(4, "big")
    return msg_head + message_crc


def _es_event(event_type: str, payload: dict) -> bytes:
    return _es_encode_message(
        {
            ":message-type": "event",
            ":event-type": event_type,
            ":content-type": "application/json",
        },
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def _resolve_live_tail_group_name(identifier: str) -> str | None:
    """Resolve a StartLiveTail logGroupIdentifier (ARN without ``:*``, or bare name)."""
    if not identifier:
        return None
    if identifier.startswith("arn:"):
        if identifier.endswith(":*"):
            return None  # AWS rejects trailing :* for Live Tail identifiers
        return _log_group_name_from_identifier_arn(identifier) or _resolve_group_by_arn(identifier)
    if identifier in _log_groups:
        return identifier
    return None


def _live_tail_stream_allowed(stream_name: str, stream_names, stream_prefixes) -> bool:
    if stream_names:
        return stream_name in stream_names
    if stream_prefixes:
        return any(stream_name.startswith(prefix) for prefix in stream_prefixes)
    return True


def _fanout_to_live_tail_sessions(group_name: str, stream_name: str, events: list[dict]) -> None:
    """Push matching PutLogEvents into active Live Tail session queues."""
    import asyncio

    if not events or not _live_tail_sessions:
        return
    account_id = get_account_id()
    region = get_region()
    for session in list(_live_tail_sessions.values()):
        if session.get("account_id") != account_id or session.get("region") != region:
            continue
        if group_name not in session["group_names"]:
            continue
        if not _live_tail_stream_allowed(
            stream_name, session.get("stream_names"), session.get("stream_prefixes")
        ):
            continue
        identifier = session["identifiers_by_name"][group_name]
        filter_pattern = session.get("filter_pattern") or ""
        matched = []
        for event in events:
            message = event.get("message", "")
            if not _subscription_pattern_matches(filter_pattern, message):
                continue
            matched.append(
                {
                    "logStreamName": stream_name,
                    "logGroupIdentifier": identifier,
                    "message": message,
                    "timestamp": event.get("timestamp"),
                    "ingestionTime": event.get("ingestionTime"),
                }
            )
        if not matched:
            continue
        queue = session["queue"]
        item = (matched, False)
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            # AWS keeps at most 10 LiveTailSessionUpdate events; drop oldest.
            session["sampled"] = True
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait((matched, True))


async def _await_http_disconnect(receive) -> None:
    while True:
        message = await receive()
        if message.get("type") == "http.disconnect":
            return


def _start_live_tail(data):
    """Start a Live Tail session.

    Holds the HTTP eventstream open until the client disconnects. Matching
    ``PutLogEvents`` calls fan into ``sessionUpdate`` frames via an asyncio
    queue (same pattern as API Gateway WebSocket outboxes). Idle empty
    ``sessionUpdate`` frames are emitted once per second (AWS cadence). At most
    10 updates are buffered; overflow drops the oldest and sets ``sampled``.
    """
    import asyncio

    identifiers = data.get("logGroupIdentifiers") or []
    if not identifiers:
        return error_response_json(
            "InvalidParameterException",
            "1 validation error detected: Value null at 'logGroupIdentifiers' "
            "failed to satisfy constraint: Member must not be null",
            400,
        )
    if len(identifiers) > 10:
        return error_response_json(
            "InvalidParameterException",
            "logGroupIdentifiers may contain at most 10 items",
            400,
        )

    stream_names = data.get("logStreamNames") or None
    stream_prefixes = data.get("logStreamNamePrefixes") or None
    if stream_names and stream_prefixes:
        return error_response_json(
            "InvalidParameterException",
            "logStreamNames and logStreamNamePrefixes are mutually exclusive",
            400,
        )
    if (stream_names or stream_prefixes) and len(identifiers) != 1:
        return error_response_json(
            "InvalidParameterException",
            "logStreamNames/logStreamNamePrefixes require exactly one log group",
            400,
        )

    resolved_names: list[str] = []
    identifiers_by_name: dict[str, str] = {}
    for identifier in identifiers:
        name = _resolve_live_tail_group_name(identifier)
        if not name or name not in _log_groups:
            return error_response_json(
                "ResourceNotFoundException",
                f"The specified log group does not exist: {identifier}",
                400,
            )
        if name not in resolved_names:
            resolved_names.append(name)
            identifiers_by_name[name] = (
                identifier if identifier.startswith("arn:") else _make_group_arn_no_star(name)
            )

    filter_pattern = data.get("logEventFilterPattern") or ""
    request_id = new_uuid()
    session_id = new_uuid()
    start_payload = {
        "requestId": request_id,
        "sessionId": session_id,
        "logGroupIdentifiers": [identifiers_by_name[n] for n in resolved_names],
    }
    if stream_names:
        start_payload["logStreamNames"] = list(stream_names)
    if stream_prefixes:
        start_payload["logStreamNamePrefixes"] = list(stream_prefixes)
    if filter_pattern:
        start_payload["logEventFilterPattern"] = filter_pattern

    account_id = get_account_id()
    region = get_region()

    initial_bytes = _es_encode_message(
        {
            ":message-type": "event",
            ":event-type": "initial-response",
            ":content-type": "application/json",
        },
        b"{}",
    )
    start_bytes = _es_event("sessionStart", start_payload)

    async def _run(send, receive):
        async def _send_chunk(payload: bytes, *, more: bool = True) -> None:
            await send({"type": "http.response.body", "body": payload, "more_body": more})

        # Register before the first await so concurrent PutLogEvents cannot race
        # past sessionStart into a missing session.
        queue: asyncio.Queue = asyncio.Queue(maxsize=_LIVE_TAIL_QUEUE_MAX)
        session = {
            "session_id": session_id,
            "account_id": account_id,
            "region": region,
            "group_names": set(resolved_names),
            "identifiers_by_name": identifiers_by_name,
            "stream_names": list(stream_names) if stream_names else None,
            "stream_prefixes": list(stream_prefixes) if stream_prefixes else None,
            "filter_pattern": filter_pattern,
            "queue": queue,
            "sampled": False,
        }
        _live_tail_sessions[session_id] = session

        await _send_chunk(initial_bytes)
        await _send_chunk(start_bytes)

        disconnect_task = asyncio.create_task(_await_http_disconnect(receive))
        try:
            while not disconnect_task.done():
                get_task = asyncio.create_task(queue.get())
                done, _pending = await asyncio.wait(
                    {get_task, disconnect_task},
                    timeout=_LIVE_TAIL_IDLE_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if disconnect_task in done:
                    get_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await get_task
                    break
                if get_task in done:
                    item = get_task.result()
                    if item is None:
                        break
                    results, sampled_flag = item
                    sampled = bool(sampled_flag or session.get("sampled"))
                    session["sampled"] = False
                    await _send_chunk(
                        _es_event(
                            "sessionUpdate",
                            {
                                "sessionMetadata": {"sampled": sampled},
                                "sessionResults": results,
                            },
                        )
                    )
                else:
                    get_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await get_task
                    await _send_chunk(
                        _es_event(
                            "sessionUpdate",
                            {
                                "sessionMetadata": {"sampled": False},
                                "sessionResults": [],
                            },
                        )
                    )
        finally:
            _live_tail_sessions.pop(session_id, None)
            disconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await disconnect_task
            with contextlib.suppress(Exception):
                await _send_chunk(b"", more=False)

    return (
        200,
        {
            "Content-Type": "application/vnd.amazon.eventstream",
            "x-amzn-RequestId": request_id,
        },
        StreamingResponse(_run),
    )


def _subscription_pattern_matches(pattern, message):
    """Minimal CloudWatch Logs filter-pattern match: an empty pattern matches
    every event; otherwise every bare term in the pattern must appear in the
    message. The full filter-pattern grammar is intentionally not implemented."""
    if not pattern or not pattern.strip():
        return True
    import re
    terms = re.findall(r"[A-Za-z0-9_./:-]+", pattern)
    return all(t in (message or "") for t in terms) if terms else True


def _fanout_to_subscription_filters(group_name, stream_name, events):
    """Forward matching log events to each subscription filter's destination
    Lambda, in AWS's `awslogs` gzip+base64 envelope (#896). Best-effort — a
    delivery failure must never break log ingestion. Only Lambda destinations
    are delivered; Kinesis/Firehose destinations are stored but not delivered."""
    grp = _log_groups.get(group_name)
    if not grp or not events:
        return
    filters = grp.get("subscriptionFilters") or {}
    if not filters:
        return
    import gzip
    import threading
    now_ms = int(time.time() * 1000)
    for f in filters.values():
        # Best-effort per filter: a delivery error must NEVER break log
        # ingestion (PutLogEvents / Lambda log emit both call this).
        try:
            dest = f.get("destinationArn", "")
            if ":function:" not in dest:
                continue  # only Lambda destinations are delivered
            fn = dest.split(":function:")[-1].split(":")[0]
            # Guard the self-feeding loop: a filter on /aws/lambda/<fn> pointing
            # back at <fn> would invoke→log→invoke forever.
            if group_name == f"/aws/lambda/{fn}":
                continue
            matched = [e for e in events
                       if _subscription_pattern_matches(f.get("filterPattern", ""), e.get("message", ""))]
            if not matched:
                continue
            payload = {
                "messageType": "DATA_MESSAGE",
                "owner": get_account_id(),
                "logGroup": group_name,
                "logStream": stream_name,
                "subscriptionFilters": [f.get("filterName", "")],
                "logEvents": [
                    {"id": new_uuid().replace("-", ""),
                     "timestamp": e.get("timestamp", now_ms),
                     "message": e.get("message", "")}
                    for e in matched
                ],
            }
            awslogs_event = {"awslogs": {
                "data": base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()
            }}
            from ministack.services import lambda_svc
            rec = lambda_svc._functions.get(fn)
            if rec:
                # Scope the invoke from the function's own config: a bare
                # thread has an empty context, so plain _execute_function would
                # run the subscriber (and its side effects — own log group,
                # downstream calls) under the default account for any
                # non-default tenant.
                threading.Thread(target=lambda_svc._execute_function_with_config_scope,
                                 args=(rec, awslogs_event), daemon=True).start()
            else:
                logger.warning("subscription filter %s: destination Lambda %s not found",
                               f.get("filterName"), fn)
        except Exception as exc:
            logger.debug("subscription filter delivery failed: %s", exc)


def _resolve_log_group_name(data):
    """Per AWS: GetLogEvents / FilterLogEvents accept either `logGroupName` or
    `logGroupIdentifier` (name or ARN), but not both. Returns the resolved name
    or None."""
    name = data.get("logGroupName")
    if name:
        return name
    ident = data.get("logGroupIdentifier")
    if not ident:
        return None
    if ident.startswith("arn:"):
        return _log_group_name_from_identifier_arn(ident) or ident
    return ident


def _get_log_events(data):
    group = _resolve_log_group_name(data)
    stream = data.get("logStreamName")
    limit = min(data.get("limit", 10000), 10000)
    start_from_head = data.get("startFromHead", False)
    start_time = data.get("startTime")
    end_time = data.get("endTime")
    next_token = data.get("nextToken")

    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if stream not in _log_groups[group]["streams"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log stream does not exist: {stream}", 400,
        )

    all_events = _log_groups[group]["streams"][stream]["events"]

    filtered = all_events
    if start_time is not None:
        filtered = [e for e in filtered if e["timestamp"] >= start_time]
    if end_time is not None:
        filtered = [e for e in filtered if e["timestamp"] <= end_time]

    # Parse offset from token: f/<offset> for forward, b/<offset> for backward
    offset = 0
    if next_token:
        try:
            offset = int(next_token.split("/", 1)[1])
        except (IndexError, ValueError):
            offset = 0

    if start_from_head or (next_token and next_token.startswith("f/")):
        page = filtered[offset:offset + limit]
        new_forward = f"f/{offset + len(page)}"
        new_backward = f"b/{offset}"
    else:
        end = len(filtered) - offset if next_token and next_token.startswith("b/") else len(filtered)
        start = max(0, end - limit)
        page = filtered[start:end]
        new_forward = f"f/{end}"
        new_backward = f"b/{len(filtered) - start}"

    # AWS behaviour: when at end of stream, return the caller's token
    # so SDK clients stop paginating
    forward_token = next_token if (next_token and len(page) < limit) else new_forward
    backward_token = next_token if (next_token and offset == 0 and next_token.startswith("b/")) else new_backward

    # OutputLogEvent is only {timestamp, message, ingestionTime}; strip internal
    # fields (e.g. the eventId used by FilterLogEvents / GetLogRecord).
    return json_response({
        "events": [
            {"timestamp": e["timestamp"], "message": e["message"], "ingestionTime": e["ingestionTime"]}
            for e in page
        ],
        "nextForwardToken": forward_token,
        "nextBackwardToken": backward_token,
    })


def _compile_filter_pattern(raw: str):
    """Convert a CloudWatch Logs filterPattern to a matcher function.
    Supports: empty (match all), quoted phrases, term inclusion (+term),
    term exclusion (-term), and glob wildcards (* and ?)."""
    if not raw:
        return lambda msg: True
    raw = raw.strip()
    # JSON-style patterns (starts with {) — treat as match-all for emulation
    if raw.startswith("{"):
        return lambda msg: True
    terms = raw.split()
    include = []
    exclude = []
    for t in terms:
        if t.startswith("-"):
            exclude.append(t[1:].strip('"').lower())
        else:
            include.append(t.lstrip("+").strip('"').lower())

    def _matches(msg: str) -> bool:
        m = msg.lower()
        for p in include:
            if not fnmatch.fnmatch(m, f"*{p}*") and p not in m:
                return False
        for p in exclude:
            if fnmatch.fnmatch(m, f"*{p}*") or p in m:
                return False
        return True

    return _matches


def _filter_log_events(data):
    group = _resolve_log_group_name(data)
    raw_pattern = data.get("filterPattern", "")
    pattern_fn = _compile_filter_pattern(raw_pattern)
    limit = min(data.get("limit", 10000), 10000)
    start_time = data.get("startTime")
    end_time = data.get("endTime")
    stream_names = data.get("logStreamNames")

    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )

    events = []
    searched = []
    streams = _log_groups[group]["streams"]
    target_streams = stream_names if stream_names else list(streams.keys())

    for sn in target_streams:
        if sn not in streams:
            continue
        searched.append({"logStreamName": sn, "searchedCompletely": True})
        for e in streams[sn]["events"]:
            ts = e["timestamp"]
            if start_time is not None and ts < start_time:
                continue
            if end_time is not None and ts > end_time:
                continue
            if not pattern_fn(e.get("message", "")):
                continue
            events.append({**e, "logStreamName": sn})
            if len(events) >= limit:
                break

    events.sort(key=lambda ev: ev["timestamp"])
    return json_response({"events": events[:limit], "searchedLogStreams": searched})


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

_VALID_RETENTION_DAYS = frozenset({
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180,
    365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653,
})


def _put_retention_policy(data):
    group = data.get("logGroupName")
    days = data.get("retentionInDays")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if days not in _VALID_RETENTION_DAYS:
        return error_response_json(
            "InvalidParameterException",
            f"Invalid retentionInDays value: {days}.", 400,
        )
    _log_groups[group]["retentionInDays"] = days
    return json_response({})


def _delete_retention_policy(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    _log_groups[group]["retentionInDays"] = None
    return json_response({})


# ---------------------------------------------------------------------------
# Subscription filters
# ---------------------------------------------------------------------------

def _put_subscription_filter(data):
    group = data.get("logGroupName")
    filter_name = data.get("filterName")
    if not group or not filter_name:
        return error_response_json(
            "InvalidParameterException",
            "logGroupName and filterName are required.", 400,
        )
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    _log_groups[group]["subscriptionFilters"][filter_name] = {
        "filterName": filter_name,
        "logGroupName": group,
        "filterPattern": data.get("filterPattern", ""),
        "destinationArn": data.get("destinationArn", ""),
        "roleArn": data.get("roleArn", ""),
        "distribution": data.get("distribution", "ByLogStream"),
        "creationTime": int(time.time() * 1000),
    }
    return json_response({})


def _delete_subscription_filter(data):
    group = data.get("logGroupName")
    filter_name = data.get("filterName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    if filter_name not in _log_groups[group].get("subscriptionFilters", {}):
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified subscription filter does not exist: {filter_name}", 400,
        )
    del _log_groups[group]["subscriptionFilters"][filter_name]
    return json_response({})


def _describe_subscription_filters(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    prefix = data.get("filterNamePrefix", "")
    limit = min(data.get("limit", 50), 50)
    token = data.get("nextToken")

    all_filters = sorted(
        _log_groups[group]["subscriptionFilters"].values(),
        key=lambda f: f["filterName"],
    )
    if prefix:
        all_filters = [f for f in all_filters if f["filterName"].startswith(prefix)]

    start = _decode_token(token)
    page = all_filters[start:start + limit]

    resp: dict = {"subscriptionFilters": page}
    end = start + limit
    if end < len(all_filters):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Tags – legacy log-group-name APIs
# ---------------------------------------------------------------------------

def _tag_log_group(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    _log_groups[group]["tags"].update(data.get("tags", {}))
    return json_response({})


def _untag_log_group(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    for key in data.get("tags", []):
        _log_groups[group]["tags"].pop(key, None)
    return json_response({})


def _list_tags_log_group(data):
    group = data.get("logGroupName")
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    return json_response({"tags": dict(_log_groups[group]["tags"])})


# ---------------------------------------------------------------------------
# Tags – modern ARN-based APIs
# ---------------------------------------------------------------------------

def _resolve_tag_record_by_arn(arn: str):
    """Resolve a logs ARN to the mutable dict holding its ``tags``. The
    ARN-based tag operations accept log groups AND the vended-delivery trio
    (delivery sources, delivery destinations, deliveries) — the AWS provider
    reads tags back on every one of them after create."""
    group = _resolve_group_by_arn(arn)
    if group:
        return _log_groups[group].setdefault("tags", {})
    for marker, store in (
        (":delivery-source:", _delivery_sources),
        (":delivery-destination:", _delivery_destinations),
        (":delivery:", _deliveries),
    ):
        if marker in arn:
            record = store.get(arn.rsplit(":", 1)[-1])
            if record is not None:
                return record.setdefault("tags", {})
    return None


def _tag_resource(data):
    arn = data.get("resourceArn", "")
    tags = _resolve_tag_record_by_arn(arn)
    if tags is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    tags.update(data.get("tags", {}))
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn", "")
    tags = _resolve_tag_record_by_arn(arn)
    if tags is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    for key in data.get("tagKeys", []):
        tags.pop(key, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("resourceArn", "")
    tags = _resolve_tag_record_by_arn(arn)
    if tags is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    return json_response({"tags": dict(tags)})


# ---------------------------------------------------------------------------
# Destinations (stubs)
# ---------------------------------------------------------------------------

def _put_destination(data):
    name = data.get("destinationName")
    if not name:
        return error_response_json("InvalidParameterException", "destinationName is required.", 400)
    dest_arn = f"arn:aws:logs:{get_region()}:{get_account_id()}:destination:{name}"
    _destinations[name] = {
        "destinationName": name,
        "targetArn": data.get("targetArn", ""),
        "roleArn": data.get("roleArn", ""),
        "accessPolicy": data.get("accessPolicy", ""),
        "arn": dest_arn,
        "creationTime": int(time.time() * 1000),
    }
    return json_response({"destination": _destinations[name]})


def _delete_destination(data):
    name = data.get("destinationName")
    if name not in _destinations:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified destination does not exist: {name}", 400,
        )
    del _destinations[name]
    return json_response({})


def _describe_destinations(data):
    prefix = data.get("DestinationNamePrefix", "")
    limit = min(data.get("limit", 50), 50)
    token = data.get("nextToken")

    all_dests = sorted(_destinations.keys())
    if prefix:
        all_dests = [n for n in all_dests if n.startswith(prefix)]

    start = _decode_token(token)
    page = all_dests[start:start + limit]

    resp: dict = {"destinations": [_destinations[n] for n in page]}
    end = start + limit
    if end < len(all_dests):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


def _put_destination_policy(data):
    name = data.get("destinationName") or data.get("DestinationName")
    policy = data.get("accessPolicy") or data.get("AccessPolicy", "")
    if not name:
        return error_response_json("InvalidParameterException", "destinationName is required.", 400)
    if name not in _destinations:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified destination does not exist: {name}", 400,
        )
    _destinations[name]["accessPolicy"] = policy
    return json_response({})


# ---------------------------------------------------------------------------
# Metric Filters
# ---------------------------------------------------------------------------

def _put_metric_filter(data):
    group = data.get("logGroupName")
    filter_name = data.get("filterName")
    if not group or not filter_name:
        return error_response_json(
            "InvalidParameterException",
            "logGroupName and filterName are required.", 400,
        )
    if group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )
    _metric_filters[(group, filter_name)] = {
        "filterName": filter_name,
        "logGroupName": group,
        "filterPattern": data.get("filterPattern", ""),
        "metricTransformations": data.get("metricTransformations", []),
        "creationTime": int(time.time() * 1000),
    }
    return json_response({})


def _delete_metric_filter(data):
    group = data.get("logGroupName")
    filter_name = data.get("filterName")
    key = (group, filter_name)
    if key not in _metric_filters:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified metric filter does not exist: {filter_name}", 400,
        )
    del _metric_filters[key]
    return json_response({})


def _describe_metric_filters(data):
    group = data.get("logGroupName")
    prefix = data.get("filterNamePrefix", "")
    limit = min(data.get("limit", 50), 50)
    token = data.get("nextToken")

    if group and group not in _log_groups:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified log group does not exist: {group}", 400,
        )

    filters = sorted(
        (mf for mf in _metric_filters.values()
         if (not group or mf["logGroupName"] == group)
         and (not prefix or mf["filterName"].startswith(prefix))),
        key=lambda f: f["filterName"],
    )

    start = _decode_token(token)
    page = filters[start:start + limit]

    resp: dict = {"metricFilters": page}
    end = start + limit
    if end < len(filters):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


# ---------------------------------------------------------------------------
# CloudWatch Logs Insights
# ---------------------------------------------------------------------------

def _split_insights_pipes(query_string: str) -> list[str]:
    """Split a CWLI query on ``|`` outside quotes and ``/regex/`` literals."""
    text = query_string or ""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "|":
            parts.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            i += 1
            while i < n:
                c = text[i]
                buf.append(c)
                if c == "\\" and i + 1 < n:
                    buf.append(text[i + 1])
                    i += 2
                    continue
                if c == quote:
                    i += 1
                    break
                i += 1
            continue
        if ch == "/":
            buf.append(ch)
            i += 1
            while i < n:
                c = text[i]
                buf.append(c)
                if c == "\\" and i + 1 < n:
                    buf.append(text[i + 1])
                    i += 2
                    continue
                if c == "/":
                    i += 1
                    # optional flags (e.g. i)
                    while i < n and text[i].isalpha():
                        buf.append(text[i])
                        i += 1
                    break
                i += 1
            continue
        buf.append(ch)
        i += 1
    parts.append("".join(buf).strip())
    return [p for p in parts if p]


def _parse_insights_fields(query_string: str) -> list[str]:
    """Extract field names from a simple ``fields a, b | ...`` Insights query.

    Falls back to the CWLT/default set when the query has no fields clause.
    Supports a CWLI subset: ``fields``, ``filter``, ``sort @timestamp``, ``limit``.
    """
    for stage in _split_insights_pipes(query_string):
        match = re.match(r"(?i)^fields\s+(.+)$", stage.strip())
        if match:
            fields = [part.strip() for part in match.group(1).split(",") if part.strip()]
            if "@ptr" not in fields:
                fields.append("@ptr")
            return fields
    return ["@timestamp", "@message", "@logStream", "@log", "@ptr"]


def _clamp_insights_limit(value) -> int | None:
    try:
        return max(1, min(int(value), 10000))
    except (TypeError, ValueError):
        return None


def _parse_insights_limit(query_string: str, explicit_limit) -> int:
    """Effective Insights limit: ``min(query | limit, StartQuery limit)``.

    Both sides default to 10000 when omitted. The query-string limit must apply
    to the post-filter result set and must not be bypassed by a large API limit.
    """
    pipe_limit = None
    for stage in _split_insights_pipes(query_string):
        match = re.match(r"(?i)^limit\s+(\d+)\s*$", stage.strip())
        if match:
            pipe_limit = _clamp_insights_limit(match.group(1))
    api_limit = _clamp_insights_limit(explicit_limit) if explicit_limit is not None else None
    if pipe_limit is None and api_limit is None:
        return 10000
    if pipe_limit is None:
        return api_limit
    if api_limit is None:
        return pipe_limit
    return min(pipe_limit, api_limit)


def _compile_insights_filter(stage: str):
    """Compile one ``filter`` stage into a predicate ``record -> bool``.

    Supported forms:
    - ``filter @logStream = 'exact'``
    - ``filter @message like /pattern/`` or ``/pattern/i``
    - ``filter toMillis(@timestamp) <=|>=|<|>|=|!= <epoch_ms>``
    """
    text = stage.strip()
    if not re.match(r"(?i)^filter\b", text):
        return None
    body = re.sub(r"(?i)^filter\s+", "", text, count=1).strip()

    eq = re.match(r"^(@\w+)\s*=\s*'((?:\\'|[^'])*)'\s*$", body)
    if eq:
        field, raw = eq.group(1), eq.group(2).replace("\\'", "'")

        def _eq_pred(record, _field=field, _raw=raw):
            public = _public_log_record(record)
            return str(public.get(_field, "")) == _raw

        return _eq_pred

    like = re.match(r"^(@\w+)\s+like\s+/((?:\\.|[^/])*)/([i]*)\s*$", body, re.IGNORECASE)
    if like:
        field, pattern, flags_s = like.group(1), like.group(2), like.group(3)
        flags = re.IGNORECASE if "i" in (flags_s or "").lower() else 0
        try:
            compiled = re.compile(pattern, flags)
        except re.error:
            return lambda _record: False

        def _like_pred(record, _field=field, _compiled=compiled):
            public = _public_log_record(record)
            return _compiled.search(str(public.get(_field, ""))) is not None

        return _like_pred

    # CWLT surround queries bound neighbors with toMillis(@timestamp).
    # Silently ignoring this form made ``after`` return the earliest events
    # in the stream (sort asc + limit) instead of events at/after the pivot.
    tomillis = re.match(
        r"^toMillis\(@timestamp\)\s*(<=|>=|<|>|!=|=)\s*(-?\d+)\s*$",
        body,
    )
    if tomillis:
        op, bound = tomillis.group(1), int(tomillis.group(2))
        comparators = {
            "<=": lambda ts, b: ts <= b,
            ">=": lambda ts, b: ts >= b,
            "<": lambda ts, b: ts < b,
            ">": lambda ts, b: ts > b,
            "=": lambda ts, b: ts == b,
            "!=": lambda ts, b: ts != b,
        }
        compare = comparators[op]

        def _tomillis_pred(record, _compare=compare, _bound=bound):
            return _compare(int(record.get("_timestamp_ms", 0)), _bound)

        return _tomillis_pred

    # Unsupported filter form — ignore (subset emulator).
    return None


def _parse_insights_sort(query_string: str) -> tuple[str, bool] | None:
    """Return ``(field, ascending)`` for ``sort @timestamp [asc|desc]``, else None."""
    for stage in _split_insights_pipes(query_string):
        match = re.match(
            r"(?i)^sort\s+(@\w+)(?:\s+(asc|desc))?\s*$",
            stage.strip(),
        )
        if match:
            field = match.group(1)
            direction = (match.group(2) or "asc").lower()
            return field, direction != "desc"
    return None


def _insights_filter_predicates(query_string: str) -> list:
    preds = []
    for stage in _split_insights_pipes(query_string):
        if not re.match(r"(?i)^filter\b", stage.strip()):
            continue
        pred = _compile_insights_filter(stage)
        if pred is not None:
            preds.append(pred)
    return preds


def _apply_insights_sort(records: list[dict], query_string: str) -> list[dict]:
    sort_spec = _parse_insights_sort(query_string)
    if not sort_spec:
        return records
    field, ascending = sort_spec
    if field == "@timestamp":
        return sorted(
            records,
            key=lambda r: r.get("_timestamp_ms", 0),
            reverse=not ascending,
        )
    return sorted(
        records,
        key=lambda r: str(_public_log_record(r).get(field, "")),
        reverse=not ascending,
    )


def _resolve_query_log_groups(data) -> list[str]:
    groups: list[str] = []
    if data.get("logGroupName"):
        groups.append(data["logGroupName"])
    for name in data.get("logGroupNames") or []:
        if name and name not in groups:
            groups.append(name)
    for identifier in data.get("logGroupIdentifiers") or []:
        # Accept plain names or ARNs (...:log-group:NAME[:*]).
        name = identifier
        if ":log-group:" in identifier:
            name = identifier.split(":log-group:", 1)[1]
            if name.endswith(":*"):
                name = name[:-2]
            elif ":" in name:
                name = name.split(":", 1)[0]
        if name and name not in groups:
            groups.append(name)
    return groups


def _collect_query_records(group_names, start_time_s, end_time_s):
    """Collect stored log records in [start, end] (epoch seconds, AWS Insights)."""
    start_ms = int(start_time_s) * 1000
    end_ms = int(end_time_s) * 1000
    collected = []
    for group_name in group_names:
        group = _log_groups.get(group_name)
        if not group:
            continue
        for stream_name, stream in group.get("streams", {}).items():
            for event in stream.get("events", []):
                ts = event.get("timestamp", 0)
                if ts < start_ms or ts > end_ms:
                    continue
                ptr = event.get("eventId")
                record = _log_records.get(ptr) if ptr else None
                if not record:
                    # Rebuild for events ingested before ptr indexing existed.
                    ptr = ptr or new_uuid()
                    record = _build_log_record(
                        ptr=ptr,
                        timestamp_ms=ts,
                        message=event.get("message", ""),
                        log_group=group_name,
                        log_stream=stream_name,
                    )
                    _log_records[ptr] = record
                    event["eventId"] = ptr
                collected.append(record)
    collected.sort(key=lambda r: r.get("_timestamp_ms", 0))
    return collected


def _row_from_record(record: dict, fields: list[str]) -> list[dict]:
    public = _public_log_record(record)
    return [{"field": field, "value": str(public.get(field, ""))} for field in fields]


def _start_query(data):
    """Run a CloudWatch Logs Insights query (CWLI subset).

    Supported: ``fields``, chained ``filter`` (``@field = '…'``,
    ``@field like /regex/[i]``, ``toMillis(@timestamp)`` comparisons),
    ``sort @timestamp asc|desc``, and ``limit``.
    Filters are AND-ed; ``limit`` applies after filter+sort as
    ``min(query limit, StartQuery limit)``.
    """
    group_names = _resolve_query_log_groups(data)
    if not group_names:
        return error_response_json(
            "InvalidParameterException",
            "logGroupName, logGroupNames, or logGroupIdentifiers is required",
            400,
        )
    for name in group_names:
        if name not in _log_groups:
            return error_response_json(
                "ResourceNotFoundException",
                f"The specified log group does not exist: {name}",
                400,
            )

    query_string = data.get("queryString", "")
    fields = _parse_insights_fields(query_string)
    limit = _parse_insights_limit(query_string, data.get("limit"))
    start_time = data.get("startTime", 0)
    end_time = data.get("endTime", int(time.time()))

    scanned = _collect_query_records(group_names, start_time, end_time)
    predicates = _insights_filter_predicates(query_string)
    if predicates:
        filtered = [r for r in scanned if all(pred(r) for pred in predicates)]
    else:
        filtered = list(scanned)
    ordered = _apply_insights_sort(filtered, query_string)
    limited = ordered[:limit]
    results = [_row_from_record(record, fields) for record in limited]
    bytes_scanned = float(sum(len(r.get("@message", "")) for r in scanned))

    query_id = new_uuid()
    _queries[query_id] = {
        "queryId": query_id,
        "logGroupName": data.get("logGroupName", ""),
        "logGroupNames": data.get("logGroupNames", []),
        "logGroupIdentifiers": data.get("logGroupIdentifiers", []),
        "startTime": start_time,
        "endTime": end_time,
        "queryString": query_string,
        "status": "Complete",
        "results": results,
        "statistics": {
            "recordsMatched": float(len(filtered)),
            "recordsScanned": float(len(scanned)),
            "bytesScanned": bytes_scanned,
        },
    }
    return json_response({"queryId": query_id})


def _get_query_results(data):
    query_id = data.get("queryId")
    query = _queries.get(query_id)
    if not query:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified query does not exist: {query_id}", 400,
        )
    all_results = query.get("results", [])
    statistics = query.get(
        "statistics",
        {"recordsMatched": 0.0, "recordsScanned": 0.0, "bytesScanned": 0.0},
    )
    start = _decode_token(data.get("nextToken"))
    max_items = data.get("maxItems")
    if max_items is None:
        page = all_results[start:]
        return json_response({
            "status": query["status"],
            "results": page,
            "statistics": statistics,
        })
    try:
        limit = max(1, int(max_items))
    except (TypeError, ValueError):
        limit = len(all_results) - start if start < len(all_results) else 0
        limit = max(1, limit) if limit else 1
    end = start + limit
    page = all_results[start:end]
    resp = {
        "status": query["status"],
        "results": page,
        "statistics": statistics,
    }
    if end < len(all_results):
        resp["nextToken"] = _encode_token(end)
    return json_response(resp)


def _stop_query(data):
    query_id = data.get("queryId")
    if query_id in _queries:
        _queries[query_id]["status"] = "Cancelled"
    return json_response({"success": True})


def reset():
    _log_groups.clear()
    _destinations.clear()
    _metric_filters.clear()
    _queries.clear()
    _log_records.clear()
    _delivery_sources.clear()
    _delivery_destinations.clear()
    _deliveries.clear()
    # Wake any held Live Tail streams so they exit on reset.
    for session in list(_live_tail_sessions.values()):
        queue = session.get("queue")
        if queue is not None:
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
    _live_tail_sessions.clear()


# ---------------------------------------------------------------------------
# Log Delivery API — Sources, Destinations, Deliveries
# AWS's 2023-era replacement for subscription filters; lets services like
# Bedrock, AppSync, and CodeWhisperer ship vended logs to S3 / CloudWatch
# Logs / Firehose.
# ---------------------------------------------------------------------------

def _make_delivery_source_arn(name):
    return f"arn:aws:logs:{get_region()}:{get_account_id()}:delivery-source:{name}"


def _make_delivery_destination_arn(name):
    return f"arn:aws:logs:{get_region()}:{get_account_id()}:delivery-destination:{name}"


def _make_delivery_arn(delivery_id):
    return f"arn:aws:logs:{get_region()}:{get_account_id()}:delivery:{delivery_id}"


# AWS derives the "service" label of a delivery source from the ARN's
# service component (e.g. arn:aws:bedrock:... -> "bedrock"). Callers do
# not supply it; any value in the request is ignored. The field is
# always server-computed so it stays stable across describe calls.
def _derive_service_from_arn(arn):
    try:
        return parse_arn(arn).service
    except ArnParseError:
        return ""


# AWS derives deliveryDestinationType from the destination resource ARN
# (S3 / CWL / FH); callers cannot override it.
def _derive_destination_type_from_arn(arn):
    """Parse arn:aws:<svc>:region:acct:<resource>/... and map to the
    deliveryDestinationType label AWS returns. Returns None if the ARN
    doesn't match a supported target service."""
    try:
        svc = parse_arn(arn).service
    except ArnParseError:
        return None
    if svc == "s3":
        return "S3"
    if svc == "logs":
        return "CWL"
    if svc == "firehose":
        return "FH"
    return None


_VALID_OUTPUT_FORMATS = {"json", "plain", "w3c", "raw", "parquet"}
_DELIVERY_SOURCE_NON_SOURCES = {"firehose", "lambda", "logs", "s3", "sns", "sqs"}


def _validation_error(message):
    return error_response_json("ValidationException", message, 400)


def _delivery_source_spec(resource_arn):
    try:
        spec = parse_arn(resource_arn)
    except ArnParseError:
        return None, _validation_error("Invalid ARN provided.")
    if spec.region and spec.region != get_region():
        return None, _validation_error("Cross-region Delivery Source is not supported. Please use a different region.")
    if spec.account_id and spec.account_id != get_account_id():
        return None, _validation_error("Account id from identity does not match the resourceArn.")
    if spec.service in _DELIVERY_SOURCE_NON_SOURCES:
        return None, error_response_json("ResourceNotFoundException", "Cannot access provided service.", 400)
    return spec, None


def _delivery_destination_resource_spec(destination_resource_arn):
    try:
        spec = parse_arn(destination_resource_arn)
    except ArnParseError:
        return None, _validation_error("Invalid ARN provided.")
    if spec.service not in ("firehose", "logs", "s3"):
        return None, _validation_error("Delivery Destination Resource ARN is of unsupported service.")
    if spec.service == "s3":
        if spec.region or spec.account_id:
            return None, _validation_error("Invalid ARN provided.")
        return spec, None
    if spec.region != get_region():
        return None, _validation_error("Region from identity does not match the Destination Resource ARN.")
    if spec.account_id != get_account_id():
        return None, _validation_error("Account id from identity does not match the Destination Resource ARN.")
    return spec, None


def _delivery_destination_spec(delivery_destination_arn):
    try:
        spec = parse_arn(delivery_destination_arn)
    except ArnParseError:
        return None, _validation_error("Invalid ARN provided.")
    if spec.service != "logs" or not spec.resource.startswith("delivery-destination:"):
        return None, _validation_error("Action logs:CreateDelivery should have a valid resource ARN to authorize against.")
    if spec.region != get_region():
        return None, _validation_error("Cross-region Delivery Destination is not supported. Please use a different region.")
    if spec.account_id != get_account_id():
        return None, error_response_json(
            "AccessDeniedException",
            f"User is not authorized to perform: logs:CreateDelivery on resource: {delivery_destination_arn}",
            400,
        )
    return spec, None


def _put_delivery_source(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required.", 400)
    resource_arn = data.get("resourceArn")
    if not resource_arn:
        return error_response_json("ValidationException", "resourceArn is required.", 400)
    log_type = data.get("logType")
    if not log_type:
        return error_response_json("ValidationException", "logType is required.", 400)

    # AWS derives the service label from the resource ARN; ignore any
    # caller-supplied value.
    resource_spec, err = _delivery_source_spec(resource_arn)
    if err:
        return err
    derived_service = resource_spec.service

    existing = _delivery_sources.get(name)
    if existing:
        existing["resourceArns"] = [resource_arn]
        existing["logType"] = log_type
        existing["service"] = derived_service
        if "tags" in data:
            existing["tags"] = dict(data["tags"])
        source = existing
    else:
        source = {
            "name": name,
            "arn": _make_delivery_source_arn(name),
            "resourceArns": [resource_arn],
            "logType": log_type,
            "service": derived_service,
            "tags": dict(data.get("tags", {})),
        }
        _delivery_sources[name] = source
    return json_response({"deliverySource": _format_delivery_source(source)})


def _format_delivery_source(source):
    return {
        "name": source["name"],
        "arn": source["arn"],
        "resourceArns": list(source.get("resourceArns", [])),
        "service": source.get("service", ""),
        "logType": source.get("logType", ""),
        "tags": dict(source.get("tags", {})),
    }


def _get_delivery_source(data):
    name = data.get("name")
    source = _delivery_sources.get(name)
    if not source:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery source does not exist: {name}", 400,
        )
    return json_response({"deliverySource": _format_delivery_source(source)})


def _delete_delivery_source(data):
    name = data.get("name")
    if name not in _delivery_sources:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery source does not exist: {name}", 400,
        )
    del _delivery_sources[name]
    return json_response({})


def _describe_delivery_sources(data):
    sources = [_format_delivery_source(s) for s in _delivery_sources.values()]
    return json_response({"deliverySources": sources})


def _put_delivery_destination(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required.", 400)
    config = data.get("deliveryDestinationConfiguration", {}) or {}
    destination_resource_arn = config.get("destinationResourceArn")
    if not destination_resource_arn:
        return error_response_json(
            "ValidationException",
            "deliveryDestinationConfiguration.destinationResourceArn is required.", 400,
        )

    # AWS derives deliveryDestinationType from the destination resource
    # ARN (s3 -> S3, logs -> CWL, firehose -> FH); callers cannot
    # override it.
    _dest_resource_spec, err = _delivery_destination_resource_spec(destination_resource_arn)
    if err:
        return err
    derived_type = _derive_destination_type_from_arn(destination_resource_arn)
    if derived_type is None:
        return error_response_json(
            "ValidationException",
            "deliveryDestinationConfiguration.destinationResourceArn must target "
            "S3 (arn:aws:s3:::...), CloudWatch Logs (arn:aws:logs:...:log-group:...), "
            "or Firehose (arn:aws:firehose:...:deliverystream/...).", 400,
        )

    output_format = data.get("outputFormat", "json")
    # AWS enforces the enum; reject unknown values so callers fail early.
    if output_format not in _VALID_OUTPUT_FORMATS:
        return error_response_json(
            "ValidationException",
            f"outputFormat must be one of {sorted(_VALID_OUTPUT_FORMATS)}; got {output_format!r}.",
            400,
        )

    existing = _delivery_destinations.get(name)
    if existing:
        existing["deliveryDestinationConfiguration"] = {"destinationResourceArn": destination_resource_arn}
        existing["outputFormat"] = output_format
        existing["deliveryDestinationType"] = derived_type
        if "tags" in data:
            existing["tags"] = dict(data["tags"])
        dest = existing
    else:
        dest = {
            "name": name,
            "arn": _make_delivery_destination_arn(name),
            "deliveryDestinationType": derived_type,
            "outputFormat": output_format,
            "deliveryDestinationConfiguration": {
                "destinationResourceArn": destination_resource_arn,
            },
            "tags": dict(data.get("tags", {})),
        }
        _delivery_destinations[name] = dest
    return json_response({"deliveryDestination": _format_delivery_destination(dest)})


def _format_delivery_destination(dest):
    return {
        "name": dest["name"],
        "arn": dest["arn"],
        "deliveryDestinationType": dest.get("deliveryDestinationType", "CWL"),
        "outputFormat": dest.get("outputFormat", "json"),
        "deliveryDestinationConfiguration": dict(dest.get("deliveryDestinationConfiguration", {})),
        "tags": dict(dest.get("tags", {})),
    }


def _get_delivery_destination(data):
    name = data.get("name")
    dest = _delivery_destinations.get(name)
    if not dest:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery destination does not exist: {name}", 400,
        )
    return json_response({"deliveryDestination": _format_delivery_destination(dest)})


def _delete_delivery_destination(data):
    name = data.get("name")
    if name not in _delivery_destinations:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery destination does not exist: {name}", 400,
        )
    del _delivery_destinations[name]
    return json_response({})


def _describe_delivery_destinations(data):
    dests = [_format_delivery_destination(d) for d in _delivery_destinations.values()]
    return json_response({"deliveryDestinations": dests})


def _create_delivery(data):
    source_name = data.get("deliverySourceName")
    dest_arn = data.get("deliveryDestinationArn")
    if not source_name:
        return error_response_json("ValidationException", "deliverySourceName is required.", 400)
    if not dest_arn:
        return error_response_json("ValidationException", "deliveryDestinationArn is required.", 400)

    _dest_spec, err = _delivery_destination_spec(dest_arn)
    if err:
        return err

    # AWS rejects CreateDelivery unless the destination ARN resolves to a
    # destination we've previously recorded via PutDeliveryDestination —
    # the API cannot ship logs to an unknown sink.
    dest_type = None
    for d in _delivery_destinations.values():
        if d["arn"] == dest_arn:
            dest_type = d.get("deliveryDestinationType")
            break
    if dest_type is None:
        return error_response_json(
            "ResourceNotFoundException",
            "Requested Delivery Destination does not exist in this account.", 400,
        )
    if source_name not in _delivery_sources:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery source does not exist: {source_name}", 400,
        )

    # AWS allows at most one Delivery per (deliverySourceName,
    # deliveryDestinationArn) pair; CreateDelivery on an existing pair
    # raises ConflictException.
    for existing in _deliveries.values():
        if (existing["deliverySourceName"] == source_name
                and existing["deliveryDestinationArn"] == dest_arn):
            return error_response_json(
                "ConflictException",
                f"A delivery already exists for source {source_name!r} → "
                f"destination {dest_arn!r}.",
                400,
            )

    delivery_id = new_uuid()
    delivery = {
        "id": delivery_id,
        "arn": _make_delivery_arn(delivery_id),
        "deliverySourceName": source_name,
        "deliveryDestinationArn": dest_arn,
        "deliveryDestinationType": dest_type,
        "recordFields": list(data.get("recordFields", [])),
        "fieldDelimiter": data.get("fieldDelimiter", ""),
        "s3DeliveryConfiguration": dict(data.get("s3DeliveryConfiguration", {})),
        "tags": dict(data.get("tags", {})),
    }
    _deliveries[delivery_id] = delivery
    return json_response({"delivery": _format_delivery(delivery)})


def _format_delivery(delivery):
    return {
        "id": delivery["id"],
        "arn": delivery["arn"],
        "deliverySourceName": delivery["deliverySourceName"],
        "deliveryDestinationArn": delivery["deliveryDestinationArn"],
        "deliveryDestinationType": delivery.get("deliveryDestinationType", "CWL"),
        "recordFields": list(delivery.get("recordFields", [])),
        "fieldDelimiter": delivery.get("fieldDelimiter", ""),
        "s3DeliveryConfiguration": dict(delivery.get("s3DeliveryConfiguration", {})),
        "tags": dict(delivery.get("tags", {})),
    }


def _get_delivery(data):
    delivery_id = data.get("id")
    delivery = _deliveries.get(delivery_id)
    if not delivery:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery does not exist: {delivery_id}", 400,
        )
    return json_response({"delivery": _format_delivery(delivery)})


def _delete_delivery(data):
    delivery_id = data.get("id")
    if delivery_id not in _deliveries:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery does not exist: {delivery_id}", 400,
        )
    del _deliveries[delivery_id]
    return json_response({})


def _describe_deliveries(data):
    deliveries = [_format_delivery(d) for d in _deliveries.values()]
    return json_response({"deliveries": deliveries})
