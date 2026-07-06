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
import threading
import time
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
    set_request_account_id,
)

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
    # Preserve AccountScopedDict wrappers; casting to a plain dict drops the
    # per-account scoping and would persist only the current request's
    # tenants. AccountScopedDict has a JSON encoder hook in core/persistence
    # that round-trips the (account, key) tuple correctly.
    return {
        "schedules": copy.deepcopy(_schedules),
        "schedule_groups": copy.deepcopy(_schedule_groups),
        "tags": copy.deepcopy(_tags),
    }


def restore_state(data):
    _schedules.update(data.get("schedules", {}))
    _schedule_groups.update(data.get("schedule_groups", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("scheduler")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted scheduler state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return time.time()


def _schedule_arn(group, name):
    return f"arn:aws:scheduler:{get_region()}:{get_account_id()}:schedule/{group}/{name}"


def _group_arn(name):
    return f"arn:aws:scheduler:{get_region()}:{get_account_id()}:schedule-group/{name}"


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return status, {"Content-Type": "application/json", "x-amzn-errortype": code}, json.dumps({"__type": code, "Message": message}).encode()


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


def _tag_resource_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _error(400, "ValidationException", f"Invalid ResourceArn: {arn}")

    if (
        spec.partition != "aws"
        or spec.service != "scheduler"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None, _error(404, "ResourceNotFoundException", f"Resource {arn} does not exist.")

    parts = spec.resource.split("/")
    if len(parts) == 3 and parts[0] == "schedule":
        key = f"{parts[1]}/{parts[2]}"
        schedule = _schedules.get(key)
        if schedule and schedule.get("Arn") == arn:
            return schedule["Arn"], None
        return None, _error(404, "ResourceNotFoundException", f"Resource {arn} does not exist.")

    if len(parts) == 2 and parts[0] == "schedule-group":
        if parts[1] == "default":
            _ensure_default_group()
        group = _schedule_groups.get(parts[1])
        if group and group.get("Arn") == arn:
            return group["Arn"], None
        return None, _error(404, "ResourceNotFoundException", f"Resource {arn} does not exist.")

    return None, _error(404, "ResourceNotFoundException", f"Resource {arn} does not exist.")


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
    arn, err = _tag_resource_arn(arn)
    if err:
        return err
    tags = body.get("Tags", [])
    existing = _tags.get(arn, {})
    existing.update({t["Key"]: t["Value"] for t in tags})
    _tags[arn] = existing
    return _json_resp(200, {})


def _untag_resource(arn, query):
    arn, err = _tag_resource_arn(arn)
    if err:
        return err
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
    arn, err = _tag_resource_arn(arn)
    if err:
        return err
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
        arn = unquote(path[6:])  # Everything after /tags/
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query)


# ---------------------------------------------------------------------------
# Schedule firing (issue #958)
# ---------------------------------------------------------------------------
# A background sweep over ``_schedules`` that fires each due schedule's target,
# mirroring ``eventbridge._tick_scheduled_rules`` but over the Scheduler store
# and its single-``Target`` shape. Rate/cron parsing and target dispatch are
# reused from EventBridge so behavior matches the rules path exactly; the extras
# unique to Scheduler are handled here: ``at()`` one-time expressions,
# ``ActionAfterCompletion`` one-shot delete, ``State`` and ``StartDate``/``EndDate``.

_SCHEDULE_TICK_INTERVAL = 10  # seconds between sweeps
_schedule_last_fired: dict = {}  # (account_id, (group, name)) -> epoch; not persisted
_ticker_thread: "threading.Thread | None" = None


def _to_epoch(value):
    """StartDate/EndDate arrive as rest-json unix timestamps (numbers); tolerate
    ISO-8601 strings too. Returns a float epoch, or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _at_time_epoch(expr: str):
    """Parse an ``at(yyyy-mm-ddThh:mm:ss)`` one-time expression to a UTC epoch,
    or None when the expression is not an ``at()`` form."""
    m = re.match(r"^\s*at\((.+)\)\s*$", expr)
    if not m:
        return None
    try:
        dt = datetime.fromisoformat(m.group(1).strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _tick_schedules():
    """Fire every ENABLED schedule that is due, honoring State, Start/EndDate,
    at()/rate()/cron() expressions and ActionAfterCompletion one-shot delete."""
    from ministack.services import eventbridge as _eb

    now = time.time()
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    for state_key, sched in list(_schedules._data.items()):
        account_id, key = state_key
        if sched.get("State") != "ENABLED":
            continue
        end = _to_epoch(sched.get("EndDate"))
        if end is not None and now > end:
            # A recurring schedule past its EndDate has "completed".
            if sched.get("ActionAfterCompletion") == "DELETE":
                _schedules._data.pop(state_key, None)
                _schedule_last_fired.pop(state_key, None)
            continue
        start = _to_epoch(sched.get("StartDate"))
        if start is not None and now < start:
            continue

        expr = sched.get("ScheduleExpression", "")
        one_shot = False

        at_epoch = _at_time_epoch(expr)
        if at_epoch is not None:
            # One-time at(): fire once when the moment has passed.
            if now < at_epoch or state_key in _schedule_last_fired:
                continue
            one_shot = True
        else:
            interval = _eb._parse_rate_seconds(expr)
            if interval is not None:
                # rate(): countdown anchored to creation, then every interval.
                if state_key not in _schedule_last_fired:
                    _schedule_last_fired[state_key] = sched.get("CreationDate", now)
                if now - _schedule_last_fired[state_key] < interval:
                    continue
            else:
                fields = _eb._parse_cron_fields(expr)
                if fields is None:
                    continue  # unsupported / unparseable expression
                # cron(): fire once per scheduled occurrence.
                if state_key not in _schedule_last_fired:
                    _schedule_last_fired[state_key] = now
                    continue
                last_dt = datetime.fromtimestamp(_schedule_last_fired[state_key], tz=timezone.utc)
                next_fire = _eb._cron_next_fire(fields, last_dt)
                if next_fire is None or now_dt < next_fire:
                    continue

        _schedule_last_fired[state_key] = now
        target = sched.get("Target") or {}
        if not target.get("Arn"):
            continue
        # Fire under the schedule's own tenant so ARN-building / dispatch scope right.
        set_request_account_id(account_id)
        event = {
            "EventId": new_uuid(),
            "Source": "aws.scheduler",
            "DetailType": "Scheduled Event",
            "Detail": "{}",
            "Time": now,
            "Resources": [sched.get("Arn", "")],
            "Account": account_id,
            "Region": get_region(),
        }
        try:
            _eb._invoke_target(target, event, sched)
        except Exception:
            logger.exception("Scheduler dispatch error for %s (account %s)", key, account_id)

        # ActionAfterCompletion=DELETE on a one-time at() schedule: remove after
        # firing. Recurring DELETE schedules "complete" at EndDate (handled above);
        # at() with NONE stays but never refires (guarded by _schedule_last_fired).
        if one_shot and sched.get("ActionAfterCompletion") == "DELETE":
            _schedules._data.pop(state_key, None)
            _schedule_last_fired.pop(state_key, None)


def _ticker_loop():
    while True:
        time.sleep(_SCHEDULE_TICK_INTERVAL)
        try:
            _tick_schedules()
        except Exception:
            logger.exception("Scheduler ticker error")


def start_scheduler() -> None:
    """Start the schedule-firing daemon (idempotent). Wired from the gateway
    lifespan.startup, mirroring ``eventbridge.start_scheduler``."""
    global _ticker_thread
    if _ticker_thread is not None and _ticker_thread.is_alive():
        return
    _ticker_thread = threading.Thread(
        target=_ticker_loop, daemon=True, name="evb-scheduler-ticker"
    )
    _ticker_thread.start()

    return _error(400, "ValidationException", f"No route for {method} {path}")
