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
import copy
import json
import logging
import os
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("cloudtrail")

_recording_enabled: bool = os.environ.get("CLOUDTRAIL_RECORDING", "0") == "1"
_MAX_EVENTS: int = int(os.environ.get("CLOUDTRAIL_MAX_EVENTS", "10000"))

_events = AccountRegionScopedDict()           # "events" -> deque[event_dict]
_trails = AccountRegionScopedDict()           # trail_name -> trail_record, scoped to HomeRegion
_event_selectors = AccountRegionScopedDict()  # trail_name -> list[EventSelector], scoped to HomeRegion
_trail_tags = AccountScopedDict()             # trail_arn -> {tag_key: tag_value}

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
        "trails": copy.deepcopy(_trails),
        "event_selectors": copy.deepcopy(_event_selectors),
        "trail_tags": copy.deepcopy(_trail_tags),
    }


def restore_state(data):
    if not isinstance(data, dict):
        return
    trail_regions = _restore_trails(data.get("trails", {}))
    _restore_event_selectors(data.get("event_selectors", {}), trail_regions)
    _restore_trail_tags(data.get("trail_tags", {}))


def load_persisted_state(data):
    restore_state(data)


def _restore_trails(saved) -> dict[tuple[str, str], str]:
    trail_regions: dict[tuple[str, str], str] = {}
    if isinstance(saved, AccountRegionScopedDict):
        _trails.update(saved)
        for (account_id, region, name), trail in saved.all_items():
            trail_regions[(account_id, name)] = _trail_home_region(trail, fallback_region=region)
        return trail_regions

    for account_id, name, trail in _legacy_account_items(saved):
        trail_copy = copy.deepcopy(trail)
        region = _trail_home_region(trail_copy)
        if isinstance(trail_copy, dict):
            trail_copy.setdefault("HomeRegion", region)
        _trails.set_scoped(account_id, region, name, trail_copy)
        trail_regions[(account_id, name)] = region
    return trail_regions


def _restore_event_selectors(saved, trail_regions: dict[tuple[str, str], str]):
    if isinstance(saved, AccountRegionScopedDict):
        _event_selectors.update(saved)
        return

    boot_region = get_region()
    for account_id, name, selectors in _legacy_account_items(saved):
        region = trail_regions.get((account_id, name), boot_region)
        _event_selectors.set_scoped(account_id, region, name, copy.deepcopy(selectors))


def _restore_trail_tags(saved):
    if isinstance(saved, AccountScopedDict):
        _trail_tags.update(saved)
        return
    if isinstance(saved, dict):
        for arn, tags in saved.items():
            _trail_tags[arn] = copy.deepcopy(tags)


def _legacy_account_items(saved):
    if isinstance(saved, AccountScopedDict):
        for (account_id, key), value in saved._data.items():
            yield account_id, key, value
    elif isinstance(saved, dict):
        account_id = get_account_id()
        for key, value in saved.items():
            yield account_id, key, value


def _scrub(params: dict) -> dict:
    if not isinstance(params, dict):
        return {}
    return {
        k: "***REDACTED***" if k.lower() in _SCRUB_KEYS else v
        for k, v in params.items()
    }


def _trail_arn(name: str) -> str:
    return f"arn:aws:cloudtrail:{get_region()}:{get_account_id()}:trail/{name}"


def _trail_home_region(trail: dict | None, fallback_region: str | None = None) -> str:
    if isinstance(trail, dict):
        if trail.get("HomeRegion"):
            return trail["HomeRegion"]
        arn = trail.get("TrailARN")
        if arn:
            try:
                spec, _ = _parse_trail_arn(arn)
                if spec.region:
                    return spec.region
            except ValueError:
                pass
    return fallback_region or get_region()


def _account_trail_items(account_id: str | None = None):
    account_id = account_id or get_account_id()
    for (trail_account_id, region, name), trail in _trails.all_items():
        if trail_account_id == account_id:
            yield region, name, trail


def _visible_trail_items(*, include_shadow_trails: bool = True):
    account_id = get_account_id()
    region = get_region()
    visible = []
    seen = set()
    for name, trail in _trails.items_scoped(account_id, region):
        visible.append((region, name, trail))
        seen.add(name)
    for home_region, name, trail in _account_trail_items(account_id):
        if home_region == region or name in seen:
            continue
        if include_shadow_trails and trail.get("IsMultiRegionTrail", False):
            visible.append((home_region, name, trail))
            seen.add(name)
    return visible


def _find_any_account_trail(name: str):
    for home_region, trail_name, trail in _account_trail_items():
        if trail_name == name:
            return home_region, trail
    return None, None


def _has_peer_region_trail(name: str, home_region: str) -> bool:
    for peer_region, trail_name, _trail in _account_trail_items():
        if trail_name == name and peer_region != home_region:
            return True
    return False


def _find_visible_trail(raw: str):
    if raw.startswith("arn:"):
        spec, name = _parse_trail_arn(raw)
        if spec.account_id != get_account_id():
            return name, None, None
        trail = _trails.get_scoped(spec.account_id, spec.region, name)
        if trail is None or trail.get("TrailARN") != str(spec):
            return name, None, None
        return name, spec.region, trail

    trail = _trails.get(raw)
    if trail is not None:
        return raw, get_region(), trail

    for home_region, name, trail in _account_trail_items():
        if name == raw and trail.get("IsMultiRegionTrail", False):
            return name, home_region, trail
    return raw, None, None


def _invalid_home_region_error(name: str, home_region: str):
    return _err(
        "InvalidHomeRegionException",
        f"Trail {name!r} must be managed from its home region {home_region}.",
    )


def _find_mutable_trail(raw: str):
    if raw.startswith("arn:"):
        try:
            spec, name = _parse_trail_arn(raw)
        except ValueError as exc:
            return None, None, None, _err("CloudTrailARNInvalidException", str(exc))
        if spec.account_id != get_account_id():
            return name, None, None, _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)
        trail = _trails.get_scoped(spec.account_id, spec.region, name)
        if trail is None or trail.get("TrailARN") != str(spec):
            return name, None, None, _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)
        if spec.region != get_region():
            if trail.get("IsMultiRegionTrail", False):
                return name, spec.region, trail, _invalid_home_region_error(name, spec.region)
            return name, None, None, _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)
        return name, spec.region, trail, None

    trail = _trails.get(raw)
    if trail is not None:
        return raw, get_region(), trail, None

    home_region, trail = _find_any_account_trail(raw)
    if trail is not None and trail.get("IsMultiRegionTrail", False):
        return raw, home_region, trail, _invalid_home_region_error(raw, home_region)
    return raw, None, None, _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)


def _parse_trail_arn(arn: str):
    try:
        spec = parse_arn(arn)
    except ArnParseError as exc:
        raise ValueError("Invalid CloudTrail trail ARN.") from exc

    if spec.service != "cloudtrail" or not spec.resource.startswith("trail/"):
        raise ValueError("Invalid CloudTrail trail ARN.")
    trail_name = spec.resource[len("trail/"):]
    if not trail_name or "/" in trail_name:
        raise ValueError("Invalid CloudTrail trail ARN.")
    return spec, trail_name


def _trail_name_from_arn(arn: str) -> str | None:
    spec, trail_name = _parse_trail_arn(arn)
    if spec.region != get_region() or spec.account_id != get_account_id():
        return None
    return trail_name


def _trail_name_from_read_arn(arn: str) -> str | None:
    spec, trail_name = _parse_trail_arn(arn)
    if spec.account_id != get_account_id():
        return None
    trail = _trails.get_scoped(spec.account_id, spec.region, trail_name)
    if trail is None or trail.get("TrailARN") != str(spec):
        return None
    return trail_name


def _normalize_kms_key_id(value: str) -> str:
    """Echo the CMK as real AWS does — a full key ARN. A bare key id is expanded to
    its ARN; an ARN or an ``alias/...`` reference is kept as sent (resolving an alias
    to its target key ARN would need a KMS lookup we don't do). Returns "" when unset."""
    if not value or value.startswith("arn:") or value.startswith("alias/"):
        return value
    return f"arn:aws:kms:{get_region()}:{get_account_id()}:key/{value}"


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


def _resolve_trail_name(name: str, *, allow_cross_region_arn: bool = False) -> str | None:
    if name.startswith("arn:"):
        if allow_cross_region_arn:
            return _trail_name_from_read_arn(name)
        return _trail_name_from_arn(name)
    return name


def _resolve_trail_name_or_error(name: str, *, allow_cross_region_arn: bool = False):
    try:
        return _resolve_trail_name(name, allow_cross_region_arn=allow_cross_region_arn), None
    except ValueError as exc:
        return None, _err("CloudTrailARNInvalidException", str(exc))


def _is_non_aws_trail_arn_partition(raw: str) -> bool:
    try:
        spec, _ = _parse_trail_arn(raw)
    except ValueError:
        return False
    return spec.partition != "aws"


def _validate_trail_arn(arn: str, *, require_existing: bool = False):
    try:
        spec, trail_name = _parse_trail_arn(arn)
    except ValueError as exc:
        return _err("CloudTrailARNInvalidException", str(exc))

    if spec.partition != "aws" or spec.region != get_region() or spec.account_id != get_account_id():
        return _err("CloudTrailARNInvalidException", "Invalid CloudTrail trail ARN.")

    if require_existing:
        trail = _trails.get_scoped(spec.account_id, spec.region, trail_name)
        if trail is None or trail.get("TrailARN") != str(spec):
            return _err("ResourceNotFoundException", f"Unknown trail: {arn!r}")

    return None


def _resolve_existing_trail_name_or_error(raw: str, *, allow_cross_region_arn: bool = False):
    name, error = _resolve_trail_name_or_error(raw, allow_cross_region_arn=allow_cross_region_arn)
    if error:
        return None, error
    if name is None or _trails.get(name) is None:
        return None, _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)
    return name, None


def _create_trail(body: dict):
    name = body.get("Name", "").strip()
    if not name:
        return _err("InvalidTrailNameException", "Trail name is required.")
    is_multi_region = body.get("IsMultiRegionTrail", False)
    _, visible_trail = _find_any_account_trail(name) if is_multi_region else (None, None)
    if visible_trail is not None or _find_visible_trail(name)[2] is not None:
        return _err("TrailAlreadyExistsException", f"Trail {name!r} already exists.")
    arn = _trail_arn(name)
    trail = {
        "Name": name,
        "S3BucketName": body.get("S3BucketName", ""),
        "S3KeyPrefix": body.get("S3KeyPrefix", ""),
        "SnsTopicName": body.get("SnsTopicName", ""),
        "IncludeGlobalServiceEvents": body.get("IncludeGlobalServiceEvents", True),
        "IsMultiRegionTrail": is_multi_region,
        "LogFileValidationEnabled": body.get("EnableLogFileValidation", False),
        "HomeRegion": get_region(),
        "TrailARN": arn,
        "HasCustomEventSelectors": False,
        "HasInsightSelectors": False,
        "IsOrganizationTrail": body.get("IsOrganizationTrail", False),
        "IsLogging": True,
    }
    # CreateTrail must persist KmsKeyId so DescribeTrails/GetTrail echo it; otherwise a
    # CMK set at create is dropped and Terraform's aws_cloudtrail needs a second apply to
    # converge (only UpdateTrail stored it). Stored normalized to a key ARN, and only when
    # set — AWS omits the field when there is no CMK, and emitting "" yields a spurious diff.
    kms = _normalize_kms_key_id(body.get("KmsKeyId", ""))
    if kms:
        trail["KmsKeyId"] = kms
    _trails[name] = trail
    resp = {
        "Name": name,
        "S3BucketName": trail["S3BucketName"],
        "S3KeyPrefix": trail["S3KeyPrefix"],
        "IncludeGlobalServiceEvents": trail["IncludeGlobalServiceEvents"],
        "IsMultiRegionTrail": trail["IsMultiRegionTrail"],
        "TrailARN": arn,
        "LogFileValidationEnabled": trail["LogFileValidationEnabled"],
        "IsOrganizationTrail": trail["IsOrganizationTrail"],
    }
    if kms:
        resp["KmsKeyId"] = kms
    return _ok(resp)


def _delete_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name, home_region, _trail, error = _find_mutable_trail(raw)
    if error:
        return error
    _trails.pop_scoped(get_account_id(), home_region, name, None)
    _event_selectors.pop_scoped(get_account_id(), home_region, name, None)
    return _ok({})


def _get_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    if raw.startswith("arn:") and _is_non_aws_trail_arn_partition(raw):
        return _err("InvalidTrailNameException", "Invalid trail name.")
    try:
        name, _home_region, trail = _find_visible_trail(raw)
    except ValueError as exc:
        return _err("CloudTrailARNInvalidException", str(exc))
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {name!r}", 404)
    return _ok({"Trail": trail})


def _describe_trails(body: dict):
    trail_names = body.get("trailNameList", [])
    include_shadow_trails = body.get("includeShadowTrails", True)
    all_trails = [
        trail
        for _home_region, _name, trail in _visible_trail_items(
            include_shadow_trails=include_shadow_trails
        )
    ]
    if trail_names:
        resolved = []
        for trail_name in trail_names:
            try:
                _name, home_region, trail = _find_visible_trail(trail_name)
            except ValueError as exc:
                return _err("CloudTrailARNInvalidException", str(exc))
            if (
                trail is not None
                and (
                    include_shadow_trails
                    or home_region == get_region()
                    or not trail.get("IsMultiRegionTrail", False)
                )
            ):
                resolved.append(trail)
        all_trails = resolved
    return _ok({"trailList": all_trails})


def _get_trail_status(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    try:
        name, _home_region, trail = _find_visible_trail(raw)
    except ValueError as exc:
        return _err("CloudTrailARNInvalidException", str(exc))
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
    _name, _home_region, trail, error = _find_mutable_trail(raw)
    if error:
        return error
    trail["IsLogging"] = True
    trail["_StartedAt"] = int(time.time())
    trail.pop("_StoppedAt", None)
    return _ok({})


def _stop_logging(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    _name, _home_region, trail, error = _find_mutable_trail(raw)
    if error:
        return error
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
        for _home_region, _name, t in _visible_trail_items()
    ]
    out = {"Trails": summaries}
    return _ok(out)


def _update_trail(body: dict):
    raw = body.get("Name", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name, home_region, trail, error = _find_mutable_trail(raw)
    if error:
        return error
    if (
        body.get("IsMultiRegionTrail") is True
        and not trail.get("IsMultiRegionTrail", False)
        and _has_peer_region_trail(name, home_region)
    ):
        return _err("TrailAlreadyExistsException", f"Trail {name!r} already exists.")
    for src, dst in (
        ("S3BucketName", "S3BucketName"),
        ("S3KeyPrefix", "S3KeyPrefix"),
        ("SnsTopicName", "SnsTopicName"),
        ("IncludeGlobalServiceEvents", "IncludeGlobalServiceEvents"),
        ("IsMultiRegionTrail", "IsMultiRegionTrail"),
        ("EnableLogFileValidation", "LogFileValidationEnabled"),
        ("CloudWatchLogsLogGroupArn", "CloudWatchLogsLogGroupArn"),
        ("CloudWatchLogsRoleArn", "CloudWatchLogsRoleArn"),
        ("IsOrganizationTrail", "IsOrganizationTrail"),
    ):
        if src in body:
            trail[dst] = body[src]
    # KmsKeyId normalized like CreateTrail; cleared (not stored as "") when unset so the
    # read-back stays AWS-shaped and Terraform sees no spurious diff.
    if "KmsKeyId" in body:
        kms = _normalize_kms_key_id(body["KmsKeyId"])
        if kms:
            trail["KmsKeyId"] = kms
        else:
            trail.pop("KmsKeyId", None)
    resp = {
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
        "IsOrganizationTrail": trail.get("IsOrganizationTrail", False),
    }
    # Omit KmsKeyId when unset, matching CreateTrail and the DescribeTrails/GetTrail
    # read-back: AWS omits the field when there is no CMK, and an empty "" is not a
    # valid ARN (the Terraform aws provider fails parsing it).
    if trail.get("KmsKeyId"):
        resp["KmsKeyId"] = trail["KmsKeyId"]
    return _ok(resp)


def _put_event_selectors(body: dict):
    raw = body.get("TrailName", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    name, home_region, trail, error = _find_mutable_trail(raw)
    if error:
        return error
    selectors = body.get("EventSelectors", [])
    _event_selectors.set_scoped(get_account_id(), home_region, name, selectors)
    return _ok({"TrailARN": trail["TrailARN"], "EventSelectors": selectors})


def _get_event_selectors(body: dict):
    raw = body.get("TrailName", "").strip()
    if not raw:
        return _err("InvalidTrailNameException", "Trail name is required.")
    try:
        name, home_region, trail = _find_visible_trail(raw)
    except ValueError as exc:
        return _err("CloudTrailARNInvalidException", str(exc))
    if trail is None:
        return _err("TrailNotFoundException", f"Unknown trail: {raw!r}", 404)
    selectors = _event_selectors.get_scoped(get_account_id(), home_region, name) or []
    return _ok(
        {
            "TrailARN": trail.get("TrailARN", _trail_arn(name)),
            "EventSelectors": selectors,
            "AdvancedEventSelectors": [],
        }
    )


def _add_tags(body: dict):
    arn = body.get("ResourceId", "").strip()
    if not arn:
        return _err("CloudTrailARNInvalidException", "ResourceId (trail ARN) is required.")
    error = _validate_trail_arn(arn, require_existing=True)
    if error:
        return error
    existing = _trail_tags.get(arn) or {}
    for tag in body.get("TagsList", []):
        existing[tag["Key"]] = tag["Value"]
    _trail_tags[arn] = existing
    return _ok({})


def _list_tags(body: dict):
    arns = body.get("ResourceIdList", [])
    for arn in arns:
        error = _validate_trail_arn(arn, require_existing=True)
        if error:
            return error
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
    error = _validate_trail_arn(arn, require_existing=True)
    if error:
        return error
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
