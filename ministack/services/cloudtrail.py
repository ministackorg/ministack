"""
AWS CloudTrail Service Emulator.
In-memory audit log — records all API calls and exposes LookupEvents for test assertions.

Recording is off by default. Enable with CLOUDTRAIL_RECORDING=1 or via
POST /_ministack/config {"cloudtrail._recording_enabled": "true"}.
Cap the per-account ring buffer with CLOUDTRAIL_MAX_EVENTS (default 10000).

Supported operations:
  Audit log:     LookupEvents
  Control plane: CreateTrail, DeleteTrail, GetTrail, DescribeTrails,
                 GetTrailStatus, StartLogging, StopLogging,
                 PutEventSelectors, GetEventSelectors,
                 AddTags, ListTags, RemoveTags
"""

import collections
import json
import logging
import os
import time

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, get_account_id, get_region, new_uuid

logger = logging.getLogger("cloudtrail")

_recording_enabled: bool = os.environ.get("CLOUDTRAIL_RECORDING", "0") == "1"
_MAX_EVENTS: int = int(os.environ.get("CLOUDTRAIL_MAX_EVENTS", "10000"))

_events = AccountScopedDict()           # "events" -> deque[event_dict]
_trails = AccountScopedDict()           # trail_name -> trail_record
_event_selectors = AccountScopedDict()  # trail_name -> list[EventSelector]
_trail_tags = AccountScopedDict()       # trail_arn -> {tag_key: tag_value}

_SCRUB_KEYS = frozenset(
    {
        "secretaccesskey",
        "password",
        "authtoken",
        "signature",
        "authorization",
        "x-amz-security-token",
        "credentials",
        "secretstring",
        "secretbinary",
    }
)


def reset():
    _events.clear()
    _trails.clear()
    _event_selectors.clear()
    _trail_tags.clear()


def get_state():
    # Trail config persists; events are ephemeral (timestamps meaningless after restart).
    return {
        "trails": {k: v for k, v in _trails.items()},
        "event_selectors": {k: v for k, v in _event_selectors.items()},
        "trail_tags": {k: v for k, v in _trail_tags.items()},
    }


def restore_state(data):
    if not isinstance(data, dict):
        return
    for k, v in data.get("trails", {}).items():
        _trails[k] = v
    for k, v in data.get("event_selectors", {}).items():
        _event_selectors[k] = v
    for k, v in data.get("trail_tags", {}).items():
        _trail_tags[k] = v


def load_persisted_state(data):
    restore_state(data)


def _scrub(params: dict) -> dict:
    if not isinstance(params, dict):
        return {}
    return {
        k: "***REDACTED***" if k.lower() in _SCRUB_KEYS else v
        for k, v in params.items()
    }


def _trail_arn(name: str) -> str:
    return f"arn:aws:cloudtrail:{get_region()}:{get_account_id()}:trail/{name}"


def _get_event_queue() -> collections.deque:
    q = _events.get("events")
    if q is None:
        q = collections.deque(maxlen=_MAX_EVENTS)
        _events["events"] = q
    return q


def record_event(
    service: str,
    event_name: str,
    username: str,
    access_key_id: str,
    resources: list,
    region: str,
    request_id: str,
    user_agent: str,
    request_params: dict,
    method: str,
):
    """Append an API call as a CloudTrail event. Called from the ASGI dispatch loop.
    Only runs when _recording_enabled is True."""
    account_id = get_account_id()
    event_id = new_uuid()
    ts = time.time()
    readonly = _is_readonly(method, event_name)

    event_source = f"{service}.amazonaws.com"

    ct_event = {
        "eventVersion": "1.08",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": access_key_id or "AIDATEST",
            "arn": f"arn:aws:iam::{account_id}:user/{username or 'test'}",
            "accountId": account_id,
            "accessKeyId": access_key_id or "",
            "userName": username or "test",
        },
        "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
        "eventSource": event_source,
        "eventName": event_name,
        "awsRegion": region,
        "sourceIPAddress": "127.0.0.1",
        "userAgent": user_agent or "boto3",
        "requestParameters": _scrub(request_params),
        "responseElements": None,
        "requestID": request_id,
        "eventID": event_id,
        "eventType": "AwsApiCall",
        "readOnly": readonly,
        "recipientAccountId": account_id,
    }

    event_record = {
        "EventId": event_id,
        "EventName": event_name,
        "EventSource": event_source,
        "EventTime": ts,
        "Username": username or "test",
        "AccessKeyId": access_key_id or "",
        "ReadOnly": readonly,
        "Resources": list(resources),
        "CloudTrailEvent": json.dumps(ct_event),
    }

    _get_event_queue().append(event_record)


def _is_readonly(method: str, event_name: str) -> str:
    if method in ("GET", "HEAD"):
        return "true"
    for prefix in ("Get", "List", "Describe", "Head"):
        if event_name.startswith(prefix):
            return "true"
    return "false"


def _ok(body: dict):
    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps(body).encode()


def _err(code: str, msg: str, status: int = 400):
    return (
        status,
        {"Content-Type": "application/x-amz-json-1.1"},
        json.dumps({"__type": code, "message": msg}).encode(),
    )


def _lookup_events(body: dict):
    event_queue = _get_event_queue()

    attrs = {a["AttributeKey"]: a["AttributeValue"] for a in body.get("LookupAttributes", [])}
    start_time = body.get("StartTime")
    end_time = body.get("EndTime")
    try:
        max_results = min(int(body.get("MaxResults", 50)), 50)
    except (TypeError, ValueError):
        max_results = 50

    filtered = []
    for ev in reversed(list(event_queue)):  # newest first
        t = ev["EventTime"]
        if start_time is not None and t < float(start_time):
            continue
        if end_time is not None and t > float(end_time):
            continue
        if "EventName" in attrs and ev["EventName"] != attrs["EventName"]:
            continue
        if "Username" in attrs and ev["Username"] != attrs["Username"]:
            continue
        if "AccessKeyId" in attrs and ev.get("AccessKeyId", "") != attrs["AccessKeyId"]:
            continue
        if "ReadOnly" in attrs and ev.get("ReadOnly", "false") != attrs["ReadOnly"]:
            continue
        if "EventId" in attrs and ev["EventId"] != attrs["EventId"]:
            continue
        if "ResourceName" in attrs:
            rn = attrs["ResourceName"]
            if not any(r.get("ResourceName") == rn for r in ev.get("Resources", [])):
                continue
        if "ResourceType" in attrs:
            rt = attrs["ResourceType"]
            if not any(r.get("ResourceType") == rt for r in ev.get("Resources", [])):
                continue
        if "EventSource" in attrs and ev.get("EventSource", "") != attrs["EventSource"]:
            continue
        filtered.append(ev)
        if len(filtered) >= max_results:
            break

    return _ok({"Events": filtered})


def _resolve_trail_name(name: str) -> str:
    if name.startswith("arn:"):
        return name.rsplit("/", 1)[-1]
    return name


def _create_trail(body: dict):
    name = body.get("Name", "").strip()
    if not name:
        return _err("InvalidTrailNameException", "Trail name is required.")
    if _trails.get(name) is not None:
        return _err("TrailAlreadyExistsException", f"Trail {name!r} already exists.")
    arn = _trail_arn(name)
    trail = {
        "Name": name,
        "S3BucketName": body.get("S3BucketName", ""),
        "S3KeyPrefix": body.get("S3KeyPrefix", ""),
        "SnsTopicName": body.get("SnsTopicName", ""),
        "IncludeGlobalServiceEvents": body.get("IncludeGlobalServiceEvents", True),
        "IsMultiRegionTrail": body.get("IsMultiRegionTrail", False),
        "LogFileValidationEnabled": body.get("EnableLogFileValidation", False),
        "HomeRegion": get_region(),
        "TrailARN": arn,
        "HasCustomEventSelectors": False,
        "HasInsightSelectors": False,
        "IsOrganizationTrail": body.get("IsOrganizationTrail", False),
        "IsLogging": True,
    }
    _trails[name] = trail
    return _ok(
        {
            "Name": name,
            "S3BucketName": trail["S3BucketName"],
            "S3KeyPrefix": trail["S3KeyPrefix"],
            "IncludeGlobalServiceEvents": trail["IncludeGlobalServiceEvents"],
            "IsMultiRegionTrail": trail["IsMultiRegionTrail"],
            "TrailARN": arn,
            "LogFileValidationEnabled": trail["LogFileValidationEnabled"],
            "IsOrganizationTrail": trail["IsOrganizationTrail"],
        }
    )


def _delete_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    if _trails.get(name) is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    del _trails[name]
    _event_selectors.pop(name, None)
    return _ok({})


def _get_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    trail = _trails.get(name)
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    return _ok({"Trail": trail})


def _describe_trails(body: dict):
    trail_names = body.get("trailNameList", [])
    all_trails = [v for _, v in _trails.items()]
    if trail_names:
        resolved = {_resolve_trail_name(n) for n in trail_names}
        all_trails = [t for t in all_trails if t["Name"] in resolved]
    return _ok({"trailList": all_trails})


def _get_trail_status(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    trail = _trails.get(name)
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    now = int(time.time())
    is_logging = bool(trail.get("IsLogging", True))
    return _ok(
        {
            "IsLogging": is_logging,
            "LatestDeliveryTime": now if is_logging else trail.get("_StoppedAt", now),
            "StartLoggingTime": trail.get("_StartedAt", now - 3600),
            "StopLoggingTime": trail.get("_StoppedAt") if not is_logging else None,
            "LatestDeliveryError": "",
            "LatestNotificationError": "",
        }
    )


def _start_logging(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    trail = _trails.get(name)
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    trail["IsLogging"] = True
    trail["_StartedAt"] = int(time.time())
    trail.pop("_StoppedAt", None)
    return _ok({})


def _stop_logging(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    trail = _trails.get(name)
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    trail["IsLogging"] = False
    trail["_StoppedAt"] = int(time.time())
    return _ok({})


def _list_trails(body: dict):
    """ListTrails: paginated summary list with TrailARN, Name, HomeRegion."""
    summaries = [
        {
            "TrailARN": t["TrailARN"],
            "Name": t["Name"],
            "HomeRegion": t.get("HomeRegion", get_region()),
        }
        for t in _trails.values()
    ]
    out = {"Trails": summaries}
    return _ok(out)


def _update_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    trail = _trails.get(name)
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    for src, dst in (
        ("S3BucketName", "S3BucketName"),
        ("S3KeyPrefix", "S3KeyPrefix"),
        ("SnsTopicName", "SnsTopicName"),
        ("IncludeGlobalServiceEvents", "IncludeGlobalServiceEvents"),
        ("IsMultiRegionTrail", "IsMultiRegionTrail"),
        ("EnableLogFileValidation", "LogFileValidationEnabled"),
        ("CloudWatchLogsLogGroupArn", "CloudWatchLogsLogGroupArn"),
        ("CloudWatchLogsRoleArn", "CloudWatchLogsRoleArn"),
        ("KmsKeyId", "KmsKeyId"),
        ("IsOrganizationTrail", "IsOrganizationTrail"),
    ):
        if src in body:
            trail[dst] = body[src]
    return _ok(
        {
            "Name": name,
            "S3BucketName": trail.get("S3BucketName", ""),
            "S3KeyPrefix": trail.get("S3KeyPrefix", ""),
            "SnsTopicName": trail.get("SnsTopicName", ""),
            "SnsTopicARN": trail.get("SnsTopicARN", ""),
            "IncludeGlobalServiceEvents": trail.get("IncludeGlobalServiceEvents", True),
            "IsMultiRegionTrail": trail.get("IsMultiRegionTrail", False),
            "TrailARN": trail["TrailARN"],
            "LogFileValidationEnabled": trail.get("LogFileValidationEnabled", False),
            "CloudWatchLogsLogGroupArn": trail.get("CloudWatchLogsLogGroupArn", ""),
            "CloudWatchLogsRoleArn": trail.get("CloudWatchLogsRoleArn", ""),
            "KmsKeyId": trail.get("KmsKeyId", ""),
            "IsOrganizationTrail": trail.get("IsOrganizationTrail", False),
        }
    )


def _put_event_selectors(body: dict):
    raw = body.get("TrailName", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    selectors = body.get("EventSelectors", [])
    _event_selectors[name] = selectors
    return _ok({"TrailARN": _trail_arn(name), "EventSelectors": selectors})


def _get_event_selectors(body: dict):
    raw = body.get("TrailName", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name = _resolve_trail_name(raw)
    selectors = _event_selectors.get(name) or []
    return _ok(
        {
            "TrailARN": _trail_arn(name),
            "EventSelectors": selectors,
            "AdvancedEventSelectors": [],
        }
    )


def _add_tags(body: dict):
    arn = body.get("ResourceId", "").strip()
    if not arn:
        return _err("CloudTrailARNInvalidException", "ResourceId (trail ARN) is required.")
    existing = _trail_tags.get(arn) or {}
    for tag in body.get("TagsList", []):
        existing[tag["Key"]] = tag["Value"]
    _trail_tags[arn] = existing
    return _ok({})


def _list_tags(body: dict):
    arns = body.get("ResourceIdList", [])
    result = [
        {
            "ResourceId": arn,
            "TagsList": [{"Key": k, "Value": v} for k, v in (_trail_tags.get(arn) or {}).items()],
        }
        for arn in arns
    ]
    return _ok({"ResourceTagList": result})


def _remove_tags(body: dict):
    arn = body.get("ResourceId", "").strip()
    if not arn:
        return _err("CloudTrailARNInvalidException", "ResourceId (trail ARN) is required.")
    existing = _trail_tags.get(arn) or {}
    for tag in body.get("TagsList", []):
        existing.pop(tag.get("Key", ""), None)
    _trail_tags[arn] = existing
    return _ok({})


_DISPATCH = {
    "LookupEvents": _lookup_events,
    "CreateTrail": _create_trail,
    "DeleteTrail": _delete_trail,
    "GetTrail": _get_trail,
    "DescribeTrails": _describe_trails,
    "GetTrailStatus": _get_trail_status,
    "ListTrails": _list_trails,
    "UpdateTrail": _update_trail,
    "StartLogging": _start_logging,
    "StopLogging": _stop_logging,
    "PutEventSelectors": _put_event_selectors,
    "GetEventSelectors": _get_event_selectors,
    "AddTags": _add_tags,
    "ListTags": _list_tags,
    "RemoveTags": _remove_tags,
}


async def handle_request(method, path, headers, body_bytes, query_params):
    target = headers.get("x-amz-target", "")
    action = target.rsplit(".", 1)[-1] if "." in target else target

    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    handler = _DISPATCH.get(action)
    if handler is None:
        logger.warning("cloudtrail: unknown action %r", action)
        return _err("InvalidParameterException", f"Unknown CloudTrail action: {action!r}")
    return handler(body)


try:
    _restored = load_state("cloudtrail")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted cloudtrail state; continuing fresh")
