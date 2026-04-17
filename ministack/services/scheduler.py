"""
EventBridge Scheduler Service Emulator.
REST/JSON protocol — /schedules/* and /schedule-groups/* paths.

Supports:
  Schedules:  CreateSchedule, GetSchedule, ListSchedules,
              UpdateSchedule, DeleteSchedule
  Groups:     CreateScheduleGroup, GetScheduleGroup,
              ListScheduleGroups, DeleteScheduleGroup
  Tags:       TagResource, UntagResource, ListTagsForResource
"""

import copy
import json
import logging
import os
import re
import time

from ministack.core.responses import AccountScopedDict, get_account_id

logger = logging.getLogger("scheduler")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_schedules = AccountScopedDict()       # (group, name) -> schedule record
_schedule_groups = AccountScopedDict()  # group_name -> group record
_tags = AccountScopedDict()            # arn -> {key: value}


def reset():
    _schedules.clear()
    _schedule_groups.clear()
    _tags.clear()


def get_state():
    return copy.deepcopy({
        "schedules": dict(_schedules),
        "schedule_groups": dict(_schedule_groups),
        "tags": dict(_tags),
    })


def restore_state(data):
    _schedules.update(data.get("schedules", {}))
    _schedule_groups.update(data.get("schedule_groups", {}))
    _tags.update(data.get("tags", {}))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return time.time()


def _schedule_arn(group, name):
    return f"arn:aws:scheduler:{REGION}:{get_account_id()}:schedule/{group}/{name}"


def _group_arn(name):
    return f"arn:aws:scheduler:{REGION}:{get_account_id()}:schedule-group/{name}"


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return _json_resp(status, {"__type": code, "Message": message})


def _ensure_default_group():
    """Ensure the 'default' group exists."""
    key = "default"
    if key not in _schedule_groups:
        _schedule_groups[key] = {
            "Arn": _group_arn("default"),
            "Name": "default",
            "State": "ACTIVE",
            "CreationDate": _now(),
            "LastModificationDate": _now(),
        }


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

def _create_schedule(name, body):
    _ensure_default_group()

    group = body.get("GroupName", "default")
    key = f"{group}/{name}"

    if key in _schedules:
        return _error(409, "ConflictException",
                       f"Schedule {name} already exists in group {group}.")

    # Validate required fields
    if not body.get("ScheduleExpression"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'scheduleExpression' failed to satisfy constraint.")
    if not body.get("FlexibleTimeWindow"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'flexibleTimeWindow' failed to satisfy constraint.")
    if not body.get("Target"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'target' failed to satisfy constraint.")

    target = body.get("Target", {})
    if not target.get("Arn") or not target.get("RoleArn"):
        return _error(400, "ValidationException",
                       "Target Arn and RoleArn are required.")

    # Validate group exists
    if group != "default" and group not in _schedule_groups:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule group {group} does not exist.")

    now = _now()
    arn = _schedule_arn(group, name)

    _schedules[key] = {
        "Arn": arn,
        "Name": name,
        "GroupName": group,
        "ScheduleExpression": body["ScheduleExpression"],
        "ScheduleExpressionTimezone": body.get("ScheduleExpressionTimezone", "UTC"),
        "FlexibleTimeWindow": body["FlexibleTimeWindow"],
        "Target": target,
        "State": body.get("State", "ENABLED"),
        "ActionAfterCompletion": body.get("ActionAfterCompletion", "NONE"),
        "Description": body.get("Description", ""),
        "StartDate": body.get("StartDate"),
        "EndDate": body.get("EndDate"),
        "KmsKeyArn": body.get("KmsKeyArn"),
        "CreationDate": now,
        "LastModificationDate": now,
    }

    return _json_resp(200, {"ScheduleArn": arn})


def _update_schedule(name, body):
    group = body.get("GroupName", "default")
    key = f"{group}/{name}"

    if key not in _schedules:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule {name} does not exist in group {group}.")

    if not body.get("ScheduleExpression"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'scheduleExpression' failed to satisfy constraint.")
    if not body.get("FlexibleTimeWindow"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'flexibleTimeWindow' failed to satisfy constraint.")
    if not body.get("Target"):
        return _error(400, "ValidationException",
                       "1 validation error detected: Value at 'target' failed to satisfy constraint.")

    target = body.get("Target", {})
    existing = _schedules[key]
    arn = existing["Arn"]

    _schedules[key] = {
        "Arn": arn,
        "Name": name,
        "GroupName": group,
        "ScheduleExpression": body["ScheduleExpression"],
        "ScheduleExpressionTimezone": body.get("ScheduleExpressionTimezone", "UTC"),
        "FlexibleTimeWindow": body["FlexibleTimeWindow"],
        "Target": target,
        "State": body.get("State", "ENABLED"),
        "ActionAfterCompletion": body.get("ActionAfterCompletion", "NONE"),
        "Description": body.get("Description", ""),
        "StartDate": body.get("StartDate"),
        "EndDate": body.get("EndDate"),
        "KmsKeyArn": body.get("KmsKeyArn"),
        "CreationDate": existing["CreationDate"],
        "LastModificationDate": _now(),
    }

    return _json_resp(200, {"ScheduleArn": arn})


def _get_schedule(name, query):
    group = query.get("groupName", "default")
    key = f"{group}/{name}"

    sched = _schedules.get(key)
    if not sched:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule {name} does not exist in group {group}.")

    result = {k: v for k, v in sched.items() if v is not None}
    return _json_resp(200, result)


def _list_schedules(query):
    _ensure_default_group()

    group_filter = query.get("ScheduleGroup")
    name_prefix = query.get("NamePrefix", "")
    state_filter = query.get("State")
    max_results = int(query.get("MaxResults", 100))

    results = []
    for key, sched in _schedules.items():
        if group_filter and sched["GroupName"] != group_filter:
            continue
        if name_prefix and not sched["Name"].startswith(name_prefix):
            continue
        if state_filter and sched["State"] != state_filter:
            continue
        results.append({
            "Arn": sched["Arn"],
            "Name": sched["Name"],
            "GroupName": sched["GroupName"],
            "State": sched["State"],
            "CreationDate": sched["CreationDate"],
            "LastModificationDate": sched["LastModificationDate"],
            "Target": {"Arn": sched["Target"].get("Arn", "")},
        })

    results = results[:max_results]
    return _json_resp(200, {"Schedules": results})


def _delete_schedule(name, query):
    group = query.get("groupName", "default")
    key = f"{group}/{name}"

    if key not in _schedules:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule {name} does not exist in group {group}.")

    arn = _schedules[key]["Arn"]
    del _schedules[key]
    _tags.pop(arn, None)

    return _json_resp(200, {})


# ---------------------------------------------------------------------------
# Schedule Groups
# ---------------------------------------------------------------------------

def _create_schedule_group(name, body):
    _ensure_default_group()

    if name in _schedule_groups:
        return _error(409, "ConflictException",
                       f"Schedule group {name} already exists.")

    now = _now()
    arn = _group_arn(name)

    _schedule_groups[name] = {
        "Arn": arn,
        "Name": name,
        "State": "ACTIVE",
        "CreationDate": now,
        "LastModificationDate": now,
    }

    # Handle tags
    tags = body.get("Tags", [])
    if tags:
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}

    return _json_resp(200, {"ScheduleGroupArn": arn})


def _get_schedule_group(name):
    _ensure_default_group()

    group = _schedule_groups.get(name)
    if not group:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule group {name} does not exist.")

    return _json_resp(200, group)


def _list_schedule_groups(query):
    _ensure_default_group()

    name_prefix = query.get("NamePrefix", "")
    max_results = int(query.get("MaxResults", 100))

    results = []
    for name, group in _schedule_groups.items():
        if name_prefix and not name.startswith(name_prefix):
            continue
        results.append(group)

    results = results[:max_results]
    return _json_resp(200, {"ScheduleGroups": results})


def _delete_schedule_group(name, query):
    if name == "default":
        return _error(400, "ValidationException",
                       "The default schedule group cannot be deleted.")

    if name not in _schedule_groups:
        return _error(404, "ResourceNotFoundException",
                       f"Schedule group {name} does not exist.")

    # Delete all schedules in this group
    keys_to_delete = [k for k, v in _schedules.items() if v["GroupName"] == name]
    for k in keys_to_delete:
        arn = _schedules[k]["Arn"]
        del _schedules[k]
        _tags.pop(arn, None)

    arn = _schedule_groups[name]["Arn"]
    del _schedule_groups[name]
    _tags.pop(arn, None)

    return _json_resp(200, {})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(arn, body):
    tags = body.get("Tags", [])
    existing = _tags.get(arn, {})
    existing.update({t["Key"]: t["Value"] for t in tags})
    _tags[arn] = existing
    return _json_resp(200, {})


def _untag_resource(arn, query):
    keys = query.get("TagKeys", [])
    if isinstance(keys, str):
        keys = [keys]
    existing = _tags.get(arn, {})
    for k in keys:
        existing.pop(k, None)
    if existing:
        _tags[arn] = existing
    else:
        _tags.pop(arn, None)
    return _json_resp(200, {})


def _list_tags(arn):
    existing = _tags.get(arn, {})
    tags = [{"Key": k, "Value": v} for k, v in existing.items()]
    return _json_resp(200, {"Tags": tags})


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # Schedule routes: /schedules and /schedules/{name}
    m = re.fullmatch(r"/schedules/([A-Za-z0-9_.@-]+)", path)
    if m:
        name = m.group(1)
        if method == "POST":
            return _create_schedule(name, body)
        if method == "GET":
            return _get_schedule(name, query)
        if method == "PUT":
            return _update_schedule(name, body)
        if method == "DELETE":
            return _delete_schedule(name, query)

    if path == "/schedules" and method == "GET":
        return _list_schedules(query)

    # Schedule group routes: /schedule-groups and /schedule-groups/{name}
    m = re.fullmatch(r"/schedule-groups/([A-Za-z0-9_.@-]+)", path)
    if m:
        name = m.group(1)
        if method == "POST":
            return _create_schedule_group(name, body)
        if method == "GET":
            return _get_schedule_group(name)
        if method == "DELETE":
            return _delete_schedule_group(name, query)

    if path == "/schedule-groups" and method == "GET":
        return _list_schedule_groups(query)

    # Tags routes: /tags/{arn+}
    if path.startswith("/tags/"):
        arn = path[6:]  # Everything after /tags/
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query)

    return _error(400, "ValidationException", f"No route for {method} {path}")
