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
          StartQuery, GetQueryResults, StopQuery.
"""

import json
import time
import logging
import base64

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("logs")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_log_groups: dict = {}
# group_name -> {
#   arn, creationTime, retentionInDays (int|None), tags: {str: str},
#   subscriptionFilters: {filterName: {filterName, logGroupName, filterPattern,
#                                      destinationArn, roleArn, distribution, creationTime}},
#   streams: {stream_name: {events: [{timestamp, message, ingestionTime}],
#             uploadSequenceToken, creationTime,
#             firstEventTimestamp, lastEventTimestamp, lastIngestionTime}},
# }

_destinations: dict = {}
# dest_name -> {destinationName, targetArn, roleArn, accessPolicy, arn, creationTime}

_metric_filters: dict = {}
# (log_group_name, filter_name) -> {filterName, logGroupName, filterPattern, metricTransformations, creationTime}

_queries: dict = {}
# query_id -> {queryId, logGroupName, startTime, endTime, queryString, status}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group_arn(name):
    return f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{name}:*"


def _resolve_group_by_arn(arn):
    """Return the group name whose ARN matches, or None."""
    for name, g in _log_groups.items():
        if g["arn"] == arn:
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
            "arn": f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{group}:log-stream:{n}",
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

    if start_from_head:
        page = filtered[:limit]
    else:
        page = filtered[-limit:]

    return json_response({
        "events": page,
        "nextForwardToken": f"f/{len(all_events)}",
        "nextBackwardToken": "b/0",
    })


def _filter_log_events(data):
    group = data.get("logGroupName")
    pattern = data.get("filterPattern", "").lower()
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
            if pattern and pattern not in e.get("message", "").lower():
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
    dest_arn = f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:destination:{name}"
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
