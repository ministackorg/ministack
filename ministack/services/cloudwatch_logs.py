"""
CloudWatch Logs Service Emulator.
JSON-based API via X-Amz-Target (Logs_20140328).
Supports: CreateLogGroup, DeleteLogGroup, DescribeLogGroups,
          CreateLogStream, DeleteLogStream, DescribeLogStreams,
          PutLogEvents, GetLogEvents, FilterLogEvents,
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
import copy
import os
import fnmatch
import json
import logging
import time

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, get_region

logger = logging.getLogger("logs")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import load_state, PERSIST_STATE

_log_groups = AccountScopedDict()
# group_name -> {
#   arn, creationTime, retentionInDays (int|None), tags: {str: str},
#   subscriptionFilters: {filterName: {filterName, logGroupName, filterPattern,
#                                      destinationArn, roleArn, distribution, creationTime}},
#   streams: {stream_name: {events: [{timestamp, message, ingestionTime}],
#             uploadSequenceToken, creationTime,
#             firstEventTimestamp, lastEventTimestamp, lastIngestionTime}},
# }

_destinations = AccountScopedDict()
# dest_name -> {destinationName, targetArn, roleArn, accessPolicy, arn, creationTime}

_metric_filters = AccountScopedDict()
# (log_group_name, filter_name) -> {filterName, logGroupName, filterPattern, metricTransformations, creationTime}

_queries = AccountScopedDict()
# query_id -> {queryId, logGroupName, startTime, endTime, queryString, status}

_delivery_sources = AccountScopedDict()
# source_name -> {name, arn, resourceArns: [str], logType, service, tags}

_delivery_destinations = AccountScopedDict()
# dest_name -> {name, arn, deliveryDestinationType, outputFormat,
#               deliveryDestinationConfiguration: {destinationResourceArn}, tags}

_deliveries = AccountScopedDict()
# delivery_id -> {id, arn, deliverySourceName, deliveryDestinationArn,
#                 deliveryDestinationType, recordFields, fieldDelimiter,
#                 s3DeliveryConfiguration, tags}


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "log_groups": copy.deepcopy(_log_groups),
        "delivery_sources": copy.deepcopy(_delivery_sources),
        "delivery_destinations": copy.deepcopy(_delivery_destinations),
        "deliveries": copy.deepcopy(_deliveries),
    }


def restore_state(data):
    if data:
        _log_groups.update(data.get("log_groups", {}))
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


def _resolve_group_by_arn(arn):
    """Return the group name whose ARN matches, or None.
    Accepts both 'arn:...:log-group:name' and 'arn:...:log-group:name:*'
    since Terraform and the AWS console use both forms."""
    arn_normalized = arn.rstrip(":*")
    for name, g in _log_groups.items():
        if g["arn"].rstrip(":*") == arn_normalized:
            return name
    return None


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
        s["events"].append({"timestamp": ts, "message": msg, "ingestionTime": now_ms})

        if s["firstEventTimestamp"] is None or ts < s["firstEventTimestamp"]:
            s["firstEventTimestamp"] = ts
        if s["lastEventTimestamp"] is None or ts > s["lastEventTimestamp"]:
            s["lastEventTimestamp"] = ts
        s["lastIngestionTime"] = now_ms

    token = str(int(s["uploadSequenceToken"]) + 1)
    s["uploadSequenceToken"] = token
    return json_response({"nextSequenceToken": token})


def _get_log_events(data):
    group = data.get("logGroupName")
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

    return json_response({
        "events": page,
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
    group = data.get("logGroupName")
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

def _tag_resource(data):
    arn = data.get("resourceArn", "")
    group = _resolve_group_by_arn(arn)
    if not group:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    _log_groups[group]["tags"].update(data.get("tags", {}))
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn", "")
    group = _resolve_group_by_arn(arn)
    if not group:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    for key in data.get("tagKeys", []):
        _log_groups[group]["tags"].pop(key, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("resourceArn", "")
    group = _resolve_group_by_arn(arn)
    if not group:
        return error_response_json(
            "ResourceNotFoundException",
            f"The specified resource does not exist: {arn}", 400,
        )
    return json_response({"tags": dict(_log_groups[group]["tags"])})


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
# CloudWatch Logs Insights (stubs)
# ---------------------------------------------------------------------------

def _start_query(data):
    query_id = new_uuid()
    _queries[query_id] = {
        "queryId": query_id,
        "logGroupName": data.get("logGroupName", ""),
        "logGroupNames": data.get("logGroupNames", []),
        "startTime": data.get("startTime", 0),
        "endTime": data.get("endTime", 0),
        "queryString": data.get("queryString", ""),
        "status": "Complete",
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
    return json_response({
        "status": query["status"],
        "results": [],
        "statistics": {"recordsMatched": 0.0, "recordsScanned": 0.0, "bytesScanned": 0.0},
    })


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
    _delivery_sources.clear()
    _delivery_destinations.clear()
    _deliveries.clear()


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
    parts = arn.split(":", 5)
    return parts[2] if len(parts) >= 3 else ""


# AWS derives deliveryDestinationType from the destination resource ARN
# (S3 / CWL / FH); callers cannot override it.
def _derive_destination_type_from_arn(arn):
    """Parse arn:aws:<svc>:region:acct:<resource>/... and map to the
    deliveryDestinationType label AWS returns. Returns None if the ARN
    doesn't match a supported target service."""
    parts = arn.split(":", 5)
    if len(parts) < 3:
        return None
    svc = parts[2]
    if svc == "s3":
        return "S3"
    if svc == "logs":
        return "CWL"
    if svc == "firehose":
        return "FH"
    return None


_VALID_OUTPUT_FORMATS = {"json", "plain", "w3c", "raw", "parquet"}


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
    derived_service = _derive_service_from_arn(resource_arn)

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
    if source_name not in _delivery_sources:
        return error_response_json(
            "ResourceNotFoundException",
            f"Delivery source does not exist: {source_name}", 400,
        )

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
            f"Delivery destination does not exist: {dest_arn}", 400,
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
