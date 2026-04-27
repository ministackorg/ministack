"""
EventBridge Service Emulator.
JSON-based API via X-Amz-Target (AmazonEventBridge / AWSEvents).
Supports: CreateEventBus, UpdateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus,
          PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule,
          PutTargets, RemoveTargets, ListTargetsByRule, ListRuleNamesByTarget,
          PutEvents, TestEventPattern,
          TagResource, UntagResource, ListTagsForResource,
          CreateArchive, DeleteArchive, DescribeArchive, UpdateArchive, ListArchives,
          PutPermission, RemovePermission,
          CreateConnection, DescribeConnection, DeleteConnection, ListConnections,
          UpdateConnection, DeauthorizeConnection,
          CreateApiDestination, DescribeApiDestination, DeleteApiDestination,
          ListApiDestinations, UpdateApiDestination,
          StartReplay, DescribeReplay, ListReplays, CancelReplay,
          CreateEndpoint, DeleteEndpoint, DescribeEndpoint, ListEndpoints, UpdateEndpoint,
          ActivateEventSource, DeactivateEventSource, DescribeEventSource,
          CreatePartnerEventSource, DeletePartnerEventSource, DescribePartnerEventSource,
          ListPartnerEventSources, ListPartnerEventSourceAccounts,
          ListEventSources, PutPartnerEvents.
"""

import copy
import fnmatch
import hashlib
import json
import logging
import os
import re
import threading
import time
from datetime import datetime

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, get_region

logger = logging.getLogger("events")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


def _now_ts() -> float:
    return time.time()


def _coerce_timestamp(value):
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return value
    return value


from ministack.core.persistence import load_state, PERSIST_STATE

# Per-account bus registry. The "default" bus is lazily created per account
# on first access so every tenant has its own default bus with an ARN whose
# account-id segment matches the caller.
_event_buses = AccountScopedDict()
_rules = AccountScopedDict()
_targets = AccountScopedDict()
# Per-account event log — AccountScopedDict under "entries" keeps the list
# semantics while scoping reads to the caller's account.
_events_log = AccountScopedDict()
_tags = AccountScopedDict()
_archives = AccountScopedDict()
_event_bus_policies = AccountScopedDict()  # bus_name -> {Statement: [...]}
_connections = AccountScopedDict()         # connection_name -> {...}
_api_destinations = AccountScopedDict()    # destination_name -> {...}
_replays = AccountScopedDict()              # replay_name -> replay record
_endpoints = AccountScopedDict()            # endpoint name -> endpoint record
# Partner event sources, per-account (key: "account|name" pattern inside each tenant).
_partner_event_sources = AccountScopedDict()


def _ensure_default_bus():
    """Lazily create the caller's account's 'default' event bus on first access.
    Matches real AWS — every account has a pre-existing default bus."""
    if "default" not in _event_buses:
        _event_buses["default"] = {
            "Name": "default",
            "Arn": f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/default",
            "CreationTime": _now_ts(),
            "LastModifiedTime": _now_ts(),
        }


def _events_log_list() -> list:
    entries = _events_log.get("entries")
    if entries is None:
        entries = []
        _events_log["entries"] = entries
    return entries


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "buses": copy.deepcopy(_event_buses),
        "rules": copy.deepcopy(_rules),
        "targets": copy.deepcopy(_targets),
        "tags": copy.deepcopy(_tags),
        "archives": copy.deepcopy(_archives),
        "replays": copy.deepcopy(_replays),
        "endpoints": copy.deepcopy(_endpoints),
        "partner_event_sources": copy.deepcopy(_partner_event_sources),
        "event_bus_policies": copy.deepcopy(_event_bus_policies),
        "connections": copy.deepcopy(_connections),
        "api_destinations": copy.deepcopy(_api_destinations),
    }


def restore_state(data):
    if data:
        _event_buses.update(data.get("buses", {}))
        _rules.update(data.get("rules", {}))
        _targets.update(data.get("targets", {}))
        _tags.update(data.get("tags", {}))
        _archives.update(data.get("archives", {}))
        _replays.update(data.get("replays", {}))
        _endpoints.update(data.get("endpoints", {}))
        _event_bus_policies.update(data.get("event_bus_policies", {}))
        _connections.update(data.get("connections", {}))
        _api_destinations.update(data.get("api_destinations", {}))
        pe = data.get("partner_event_sources")
        if pe is not None:
            _partner_event_sources.clear()
            _partner_event_sources.update(pe)

        for bus in _event_buses.values():
            if "CreationTime" in bus:
                bus["CreationTime"] = _coerce_timestamp(bus["CreationTime"])
            if "LastModifiedTime" in bus:
                bus["LastModifiedTime"] = _coerce_timestamp(bus["LastModifiedTime"])

        for rule in _rules.values():
            if "CreationTime" in rule:
                rule["CreationTime"] = _coerce_timestamp(rule["CreationTime"])

        for rep in _replays.values():
            for tk in ("ReplayStartTime", "ReplayEndTime", "EventStartTime", "EventEndTime"):
                if tk in rep and rep[tk] is not None:
                    rep[tk] = _coerce_timestamp(rep[tk])
            # Replays whose dispatch thread was running at shutdown can't
            # resume — the thread is gone. Flip them to FAILED so persisted
            # state never carries zombie RUNNING replays across restarts.
            # Same precedent as Step Functions executions (stepfunctions.py).
            if rep.get("State") in ("STARTING", "RUNNING"):
                rep["State"] = "FAILED"
                rep["ReplayEndTime"] = _now_ts()


try:
    _restored = load_state("eventbridge")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


async def handle_request(method, path, headers, body, query_params):
    # Every account has a pre-existing default bus in real AWS — make sure
    # the caller's tenant has one before routing the request.
    _ensure_default_bus()

    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateEventBus": _create_event_bus,
        "UpdateEventBus": _update_event_bus,
        "DeleteEventBus": _delete_event_bus,
        "ListEventBuses": _list_event_buses,
        "DescribeEventBus": _describe_event_bus,
        "PutRule": _put_rule,
        "DeleteRule": _delete_rule,
        "ListRules": _list_rules,
        "DescribeRule": _describe_rule,
        "EnableRule": _enable_rule,
        "DisableRule": _disable_rule,
        "PutTargets": _put_targets,
        "RemoveTargets": _remove_targets,
        "ListTargetsByRule": _list_targets_by_rule,
        "ListRuleNamesByTarget": _list_rule_names_by_target,
        "TestEventPattern": _test_event_pattern,
        "PutEvents": _put_events,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "CreateArchive": _create_archive,
        "DeleteArchive": _delete_archive,
        "DescribeArchive": _describe_archive,
        "UpdateArchive": _update_archive,
        "ListArchives": _list_archives,
        "StartReplay": _start_replay,
        "DescribeReplay": _describe_replay,
        "ListReplays": _list_replays,
        "CancelReplay": _cancel_replay,
        "CreateEndpoint": _create_endpoint,
        "DeleteEndpoint": _delete_endpoint,
        "DescribeEndpoint": _describe_endpoint,
        "ListEndpoints": _list_endpoints,
        "UpdateEndpoint": _update_endpoint,
        "ActivateEventSource": _activate_event_source,
        "DeactivateEventSource": _deactivate_event_source,
        "DescribeEventSource": _describe_event_source,
        "CreatePartnerEventSource": _create_partner_event_source,
        "DeletePartnerEventSource": _delete_partner_event_source,
        "DescribePartnerEventSource": _describe_partner_event_source,
        "ListPartnerEventSources": _list_partner_event_sources,
        "ListPartnerEventSourceAccounts": _list_partner_event_source_accounts,
        "ListEventSources": _list_event_sources,
        "PutPartnerEvents": _put_partner_events,
        "PutPermission": _put_permission,
        "RemovePermission": _remove_permission,
        "CreateConnection": _create_connection,
        "DescribeConnection": _describe_connection,
        "DeleteConnection": _delete_connection,
        "ListConnections": _list_connections,
        "UpdateConnection": _update_connection,
        "DeauthorizeConnection": _deauthorize_connection,
        "CreateApiDestination": _create_api_destination,
        "DescribeApiDestination": _describe_api_destination,
        "DeleteApiDestination": _delete_api_destination,
        "ListApiDestinations": _list_api_destinations,
        "UpdateApiDestination": _update_api_destination,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Event Buses
# ---------------------------------------------------------------------------

def _create_event_bus(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name in _event_buses:
        return error_response_json("ResourceAlreadyExistsException", f"Event bus {name} already exists", 400)
    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/{name}"
    description = data.get("Description", "")
    _event_buses[name] = {
        "Name": name,
        "Arn": arn,
        "Description": description,
        "CreationTime": _now_ts(),
        "LastModifiedTime": _now_ts(),
    }
    tags = data.get("Tags", [])
    if tags:
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}
    return json_response({"EventBusArn": arn})


def _delete_event_bus(data):
    name = data.get("Name")
    if name == "default":
        return error_response_json("ValidationException", "Cannot delete the default event bus", 400)
    bus = _event_buses.pop(name, None)
    if bus:
        _tags.pop(bus["Arn"], None)
        rules_to_delete = [n for n, r in _rules.items() if r.get("EventBusName") == name]
        for rn in rules_to_delete:
            _rules.pop(rn, None)
            _targets.pop(rn, None)
    return json_response({})


def _list_event_buses(data):
    prefix = data.get("NamePrefix", "")
    buses = []
    for n, b in _event_buses.items():
        if n.startswith(prefix):
            policy = _event_bus_policies.get(n)
            buses.append({
                "Name": b["Name"],
                "Arn": b["Arn"],
                "Description": b.get("Description", ""),
                "CreationTime": b["CreationTime"],
                "LastModifiedTime": b.get("LastModifiedTime", b.get("CreationTime")),
                "Policy": json.dumps(policy) if policy else ""
            })
    return json_response({"EventBuses": buses})


def _describe_event_bus(data):
    name = data.get("Name", "default")
    bus = _event_buses.get(name)
    if not bus:
        return error_response_json("ResourceNotFoundException", f"Event bus {name} not found", 400)
    policy = _event_bus_policies.get(name)
    return json_response({
        "Name": bus["Name"],
        "Arn": bus["Arn"],
        "Description": bus.get("Description", ""),
        "CreationTime": bus["CreationTime"],
        "LastModifiedTime": bus.get("LastModifiedTime", bus.get("CreationTime")),
        "Policy": json.dumps(policy) if policy else "",
    })


def _update_event_bus(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)

    if name not in _event_buses:
        return error_response_json("ResourceNotFoundException", f"Event bus {name} not found", 400)

    bus = _event_buses[name]
    now = _now_ts()

    # Allow updating a few mutable attributes (extendable).
    if "EventSourceName" in data:
        bus["EventSourceName"] = data.get("EventSourceName")
    if "Description" in data:
        bus["Description"] = data.get("Description")

    # Update tags if provided
    tags = data.get("Tags")
    if tags:
        _tags[bus["Arn"]] = {t["Key"]: t["Value"] for t in tags}

    bus["LastModifiedTime"] = now

    return json_response({
        "EventBusArn": bus["Arn"],
        "LastModifiedTime": bus["LastModifiedTime"],
    })


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def _rule_arn(rule_name: str, bus_name: str) -> str:
    if bus_name == "default":
        return f"arn:aws:events:{get_region()}:{get_account_id()}:rule/{rule_name}"
    return f"arn:aws:events:{get_region()}:{get_account_id()}:rule/{bus_name}/{rule_name}"


def _rule_key(rule_name: str, bus_name: str) -> str:
    return f"{bus_name}|{rule_name}"


def _validate_schedule_expression(expr: str) -> bool:
    if not expr:
        return True
    rate_pattern = re.compile(r"^rate\(\d+\s+(minute|minutes|hour|hours|day|days)\)$")
    cron_pattern = re.compile(r"^cron\(.+\)$")
    return bool(rate_pattern.match(expr) or cron_pattern.match(expr))


def _put_rule(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    bus = data.get("EventBusName", "default")

    if bus not in _event_buses:
        return error_response_json("ResourceNotFoundException", f"Event bus {bus} does not exist.", 400)

    schedule = data.get("ScheduleExpression", "")
    if schedule and not _validate_schedule_expression(schedule):
        return error_response_json(
            "ValidationException",
            "Parameter ScheduleExpression is not valid.",
            400,
        )

    event_pattern = data.get("EventPattern", "")
    if event_pattern and isinstance(event_pattern, str):
        try:
            json.loads(event_pattern)
        except json.JSONDecodeError:
            return error_response_json(
                "InvalidEventPatternException",
                "Event pattern is not valid JSON",
                400,
            )

    arn = _rule_arn(name, bus)
    key = _rule_key(name, bus)

    existing = _rules.get(key, {})
    _rules[key] = {
        "Name": name,
        "Arn": arn,
        "EventBusName": bus,
        "ScheduleExpression": schedule,
        "EventPattern": event_pattern,
        "State": data.get("State", existing.get("State", "ENABLED")),
        "Description": data.get("Description", existing.get("Description", "")),
        "RoleArn": data.get("RoleArn", existing.get("RoleArn", "")),
        "ManagedBy": existing.get("ManagedBy", ""),
        "CreatedBy": get_account_id(),
        "CreationTime": existing.get("CreationTime", _now_ts()),
    }

    tags = data.get("Tags", [])
    if tags:
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}

    return json_response({"RuleArn": arn})


def _delete_rule(data):
    name = data.get("Name")
    bus = data.get("EventBusName", "default")
    key = _rule_key(name, bus)
    rule = _rules.pop(key, None)
    _targets.pop(key, None)
    if rule:
        _tags.pop(rule["Arn"], None)
    return json_response({})


def _list_rules(data):
    prefix = data.get("NamePrefix", "")
    bus = data.get("EventBusName", "default")
    rules = []
    for key, r in _rules.items():
        if r.get("EventBusName", "default") != bus:
            continue
        if prefix and not r["Name"].startswith(prefix):
            continue
        rules.append(_rule_out(r))
    return json_response({"Rules": rules})


def _describe_rule(data):
    name = data.get("Name")
    bus = data.get("EventBusName", "default")
    key = _rule_key(name, bus)
    rule = _rules.get(key)
    if not rule:
        return error_response_json("ResourceNotFoundException", f"Rule {name} does not exist.", 400)
    return json_response(_rule_out(rule))


def _enable_rule(data):
    name = data.get("Name")
    bus = data.get("EventBusName", "default")
    key = _rule_key(name, bus)
    if key in _rules:
        _rules[key]["State"] = "ENABLED"
    return json_response({})


def _disable_rule(data):
    name = data.get("Name")
    bus = data.get("EventBusName", "default")
    key = _rule_key(name, bus)
    if key in _rules:
        _rules[key]["State"] = "DISABLED"
    return json_response({})


def _rule_out(rule):
    out = {
        "Name": rule["Name"],
        "Arn": rule["Arn"],
        "EventBusName": rule["EventBusName"],
        "State": rule["State"],
    }
    if rule.get("ScheduleExpression"):
        out["ScheduleExpression"] = rule["ScheduleExpression"]
    if rule.get("EventPattern"):
        out["EventPattern"] = rule["EventPattern"]
    if rule.get("Description"):
        out["Description"] = rule["Description"]
    if rule.get("RoleArn"):
        out["RoleArn"] = rule["RoleArn"]
    return out


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

def _put_targets(data):
    rule_name = data.get("Rule")
    bus = data.get("EventBusName", "default")
    targets = data.get("Targets", [])
    key = _rule_key(rule_name, bus)

    if key not in _rules:
        return error_response_json("ResourceNotFoundException", f"Rule {rule_name} does not exist.", 400)

    if key not in _targets:
        _targets[key] = []
    existing_ids = {t["Id"] for t in _targets[key]}
    for t in targets:
        if t["Id"] in existing_ids:
            _targets[key] = [x for x in _targets[key] if x["Id"] != t["Id"]]
        _targets[key].append(t)
    return json_response({"FailedEntryCount": 0, "FailedEntries": []})


def _remove_targets(data):
    rule_name = data.get("Rule")
    bus = data.get("EventBusName", "default")
    ids = set(data.get("Ids", []))
    key = _rule_key(rule_name, bus)
    if key in _targets:
        _targets[key] = [t for t in _targets[key] if t["Id"] not in ids]
    return json_response({"FailedEntryCount": 0, "FailedEntries": []})


def _list_targets_by_rule(data):
    rule_name = data.get("Rule")
    bus = data.get("EventBusName", "default")
    key = _rule_key(rule_name, bus)
    targets = _targets.get(key, [])
    return json_response({"Targets": targets})


def _list_rule_names_by_target(data):
    target_arn = data.get("TargetArn", "")
    if not target_arn:
        return error_response_json("ValidationException", "TargetArn is required", 400)
    bus_filter = data.get("EventBusName", "")
    limit = int(data.get("Limit", 100))
    if limit < 1:
        limit = 100
    if limit > 100:
        limit = 100
    next_token = data.get("NextToken", "")

    matched = []
    for key, tlist in _targets.items():
        bus_name, rule_name = key.split("|", 1) if "|" in key else ("default", key)
        if bus_filter and bus_name != bus_filter:
            continue
        if not any(t.get("Arn") == target_arn for t in tlist):
            continue
        if key in _rules:
            matched.append(_rules[key]["Name"])

    matched = sorted(set(matched))
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = matched[start:start + limit]
    resp = {"RuleNames": page}
    if start + limit < len(matched):
        resp["NextToken"] = str(start + limit)
    return json_response(resp)


def _event_from_test_payload(event_obj: dict) -> dict:
    """Map CloudWatch Events-shaped JSON to internal fields used by _matches_pattern."""
    detail = event_obj.get("detail", event_obj.get("Detail", {}))
    if isinstance(detail, dict):
        detail = json.dumps(detail)
    elif detail is None:
        detail = "{}"
    else:
        detail = str(detail)
    return {
        "Source": event_obj.get("source", event_obj.get("Source", "")),
        "DetailType": event_obj.get("detail-type", event_obj.get("DetailType", "")),
        "Detail": detail,
        "Account": event_obj.get("account", event_obj.get("Account", get_account_id())),
        "Region": event_obj.get("region", event_obj.get("Region", get_region())),
        "Resources": event_obj.get("resources", event_obj.get("Resources", [])),
    }


def _test_event_pattern(data):
    event_str = data.get("Event", "")
    pattern_str = data.get("EventPattern", "")
    if not event_str:
        return error_response_json("ValidationException", "Event is required", 400)
    if not pattern_str:
        return error_response_json("ValidationException", "EventPattern is required", 400)
    try:
        event_obj = json.loads(event_str) if isinstance(event_str, str) else event_str
    except (json.JSONDecodeError, TypeError):
        return error_response_json("InvalidEventPatternException", "Event is not valid JSON", 400)
    if not isinstance(event_obj, dict):
        return error_response_json("InvalidEventPatternException", "Event must be a JSON object", 400)

    synthetic = _event_from_test_payload(event_obj)
    matched = _matches_pattern(pattern_str, synthetic)
    return json_response({"Result": bool(matched)})


# ---------------------------------------------------------------------------
# PutEvents + event pattern matching + target dispatch
# ---------------------------------------------------------------------------

def _normalize_bus_name(name):
    if name and name.startswith("arn:"):
        return name.split("/")[-1]
    return name


def _put_events(data):
    entries = data.get("Entries", [])
    results = []
    for entry in entries:
        event_id = new_uuid()
        bus_name = _normalize_bus_name(entry.get("EventBusName", "default"))
        event_time = _now_ts()

        event_record = {
            "EventId": event_id,
            "Source": entry.get("Source", ""),
            "DetailType": entry.get("DetailType", ""),
            "Detail": entry.get("Detail", "{}"),
            "EventBusName": bus_name,
            "Time": event_time,
            "Resources": entry.get("Resources", []),
            "Account": get_account_id(),
            "Region": get_region(),
        }
        _events_log_list().append(event_record)
        results.append({"EventId": event_id})
        logger.debug("EventBridge event: %s / %s", entry.get('Source'), entry.get('DetailType'))

        _dispatch_event(event_record)
        _archive_event(event_record)

    return json_response({"FailedEntryCount": 0, "Entries": results})


def _archive_event(event):
    bus_name = event.get("EventBusName", "default")
    bus_arn = f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/{bus_name}"
    for archive in _archives.values():
        if archive.get("EventSourceArn") != bus_arn:
            continue
        pattern = archive.get("EventPattern", "")
        if pattern and not _matches_pattern(pattern, event):
            continue
        archive.setdefault("Events", []).append(event)
        archive["EventCount"] = archive.get("EventCount", 0) + 1


def _dispatch_event(event):
    bus_name = event.get("EventBusName", "default")

    for key, rule in _rules.items():
        if rule.get("EventBusName", "default") != bus_name:
            continue
        if rule.get("State") != "ENABLED":
            continue
        if not rule.get("EventPattern"):
            continue

        if _matches_pattern(rule["EventPattern"], event):
            rule_targets = _targets.get(key, [])
            for target in rule_targets:
                _invoke_target(target, event, rule)


def _matches_pattern(pattern_str, event):
    try:
        if isinstance(pattern_str, str):
            pattern = json.loads(pattern_str)
        else:
            pattern = pattern_str
    except (json.JSONDecodeError, TypeError):
        return False

    if "source" in pattern:
        if not _matches_field(event.get("Source", ""), pattern["source"]):
            return False

    if "detail-type" in pattern:
        if not _matches_field(event.get("DetailType", ""), pattern["detail-type"]):
            return False

    if "detail" in pattern:
        try:
            detail = json.loads(event.get("Detail", "{}")) if isinstance(event.get("Detail"), str) else event.get("Detail", {})
        except (json.JSONDecodeError, TypeError):
            detail = {}
        if not _matches_detail(detail, pattern["detail"]):
            return False

    if "account" in pattern:
        if not _matches_field(event.get("Account", get_account_id()), pattern["account"]):
            return False

    if "region" in pattern:
        if not _matches_field(event.get("Region", get_region()), pattern["region"]):
            return False

    if "resources" in pattern:
        event_resources = event.get("Resources", [])
        for required in pattern["resources"]:
            if required not in event_resources:
                return False

    return True


def _matches_field(value, pattern_values):
    if isinstance(pattern_values, list):
        for item in pattern_values:
            if isinstance(item, dict):
                # Content-based filter (wildcard, prefix, suffix, etc.)
                if _matches_content_filter(value, item):
                    return True
            elif value == item:
                return True
        return False
    return value == pattern_values


def _matches_detail(detail, pattern):
    if not isinstance(pattern, dict):
        return True
    for key, expected in pattern.items():
        actual = detail.get(key)
        if isinstance(expected, list):
            if actual is None:
                return False
            if isinstance(actual, (str, int, float, bool)):
                matched = False
                for item in expected:
                    if isinstance(item, dict):
                        matched = matched or _matches_content_filter(actual, item)
                    elif actual == item or str(actual) == str(item):
                        matched = True
                if not matched:
                    return False
            elif isinstance(actual, list):
                if not any(a in expected for a in actual):
                    return False
        elif isinstance(expected, dict):
            if not isinstance(actual, dict):
                return False
            if not _matches_detail(actual, expected):
                return False
    return True


def _matches_content_filter(value, filter_rule):
    if "wildcard" in filter_rule:
        return isinstance(value, str) and fnmatch.fnmatch(value, filter_rule["wildcard"])
    if "prefix" in filter_rule:
        return isinstance(value, str) and value.startswith(filter_rule["prefix"])
    if "suffix" in filter_rule:
        return isinstance(value, str) and value.endswith(filter_rule["suffix"])
    if "anything-but" in filter_rule:
        excluded = filter_rule["anything-but"]
        if isinstance(excluded, list):
            return value not in excluded
        return value != excluded
    if "numeric" in filter_rule:
        ops = filter_rule["numeric"]
        try:
            num = float(value)
        except (ValueError, TypeError):
            return False
        i = 0
        while i < len(ops) - 1:
            op, threshold = ops[i], float(ops[i + 1])
            if op == ">" and not (num > threshold):
                return False
            if op == ">=" and not (num >= threshold):
                return False
            if op == "<" and not (num < threshold):
                return False
            if op == "<=" and not (num <= threshold):
                return False
            if op == "=" and not (num == threshold):
                return False
            i += 2
        return True
    if "exists" in filter_rule:
        return filter_rule["exists"] == (value is not None)
    return False


def _invoke_target(target, event, rule):
    arn = target.get("Arn", "")

    event_payload = json.dumps({
        "version": "0",
        "id": event["EventId"],
        "source": event["Source"],
        "account": get_account_id(),
        "time": event["Time"],
        "region": get_region(),
        "resources": event.get("Resources", []),
        "detail-type": event["DetailType"],
        "detail": json.loads(event["Detail"]) if isinstance(event["Detail"], str) else event["Detail"],
    })

    input_transformer = target.get("InputTransformer")
    if input_transformer:
        event_payload = _apply_input_transformer(input_transformer, event)
    elif target.get("Input"):
        event_payload = target["Input"]
    elif target.get("InputPath"):
        try:
            full = json.loads(event_payload)
            parts = target["InputPath"].strip("$.").split(".")
            val = full
            for p in parts:
                if p:
                    val = val[p]
            event_payload = json.dumps(val)
        except Exception:
            pass

    try:
        if ":lambda:" in arn or ":function:" in arn:
            _dispatch_to_lambda(arn, event_payload)
        elif ":sqs:" in arn:
            _dispatch_to_sqs(arn, event_payload)
        elif ":sns:" in arn:
            _dispatch_to_sns(arn, event_payload)
        else:
            logger.warning("EventBridge: unsupported target type for ARN %s", arn)
    except Exception as e:
        logger.error("EventBridge target dispatch error for %s: %s", arn, e)


def _apply_input_transformer(transformer, event):
    input_paths = transformer.get("InputPathsMap", {})
    template = transformer.get("InputTemplate", "")

    try:
        full = json.loads(event.get("Detail", "{}")) if isinstance(event.get("Detail"), str) else event.get("Detail", {})
    except Exception:
        full = {}

    event_envelope = {
        "source": event.get("Source", ""),
        "detail-type": event.get("DetailType", ""),
        "detail": full,
        "account": get_account_id(),
        "region": get_region(),
        "time": event.get("Time", ""),
        "id": event.get("EventId", ""),
        "resources": event.get("Resources", []),
    }

    replacements = {}
    for var_name, jpath in input_paths.items():
        parts = jpath.strip("$.").split(".")
        val = event_envelope
        try:
            for p in parts:
                if p:
                    val = val[p]
            replacements[var_name] = val if isinstance(val, str) else json.dumps(val)
        except (KeyError, TypeError, IndexError):
            replacements[var_name] = ""

    result = template
    for var_name, val in replacements.items():
        result = result.replace(f"<{var_name}>", str(val))

    return result


def _dispatch_to_lambda(arn, payload):
    from ministack.services import lambda_svc

    parts = arn.split(":")
    func_name = parts[-1].split("/")[-1] if "/" in parts[-1] else parts[-1]
    if func_name.startswith("function:"):
        func_name = func_name[len("function:"):]

    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        event = {"body": payload}

    func = lambda_svc._functions.get(func_name)
    if not func:
        logger.warning("EventBridge → Lambda: function %s not found", func_name)
        return
    threading.Thread(
        target=lambda_svc._execute_function, args=(func, event), daemon=True
    ).start()
    logger.info("EventBridge → Lambda %s: dispatched", func_name)


def _dispatch_to_sqs(arn, payload):
    from ministack.services import sqs as _sqs

    queue_name = arn.split(":")[-1]
    queue_url = _sqs._queue_url(queue_name)
    queue = _sqs._queues.get(queue_url)
    if not queue:
        logger.warning("EventBridge → SQS: queue %s not found", queue_name)
        return

    msg_id = new_uuid()
    md5 = hashlib.md5(payload.encode()).hexdigest()
    now = time.time()
    queue["messages"].append({
        "id": msg_id,
        "body": payload,
        "md5_body": md5,
        "receipt_handle": None,
        "sent_at": now,
        "visible_at": now,
        "receive_count": 0,
        "attributes": {},
        "message_attributes": {},
        "sys": {
            "SenderId": "AROAEXAMPLE",
            "SentTimestamp": str(int(now * 1000)),
        },
    })
    if hasattr(_sqs, "_ensure_msg_fields"):
        _sqs._ensure_msg_fields(queue["messages"][-1])
    logger.info("EventBridge → SQS %s", queue_name)


def _dispatch_to_sns(arn, payload):
    from ministack.services import sns as _sns

    topic = _sns._topics.get(arn)
    if not topic:
        logger.warning("EventBridge → SNS: topic %s not found", arn)
        return

    msg_id = new_uuid()
    topic["messages"].append({
        "id": msg_id,
        "message": payload,
        "subject": "EventBridge Notification",
        "timestamp": int(time.time()),
    })
    _sns._fanout(arn, msg_id, payload, "EventBridge Notification")
    logger.info("EventBridge → SNS %s", arn)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    tags = data.get("Tags", [])
    if arn not in _tags:
        _tags[arn] = {}
    for t in tags:
        _tags[arn][t["Key"]] = t["Value"]
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    keys = data.get("TagKeys", [])
    if arn in _tags:
        for k in keys:
            _tags[arn].pop(k, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    tag_dict = _tags.get(arn, {})
    tag_list = [{"Key": k, "Value": v} for k, v in tag_dict.items()]
    return json_response({"Tags": tag_list})


# ---------------------------------------------------------------------------
# Archives (stubs)
# ---------------------------------------------------------------------------

def _create_archive(data):
    name = data.get("ArchiveName")
    if not name:
        return error_response_json("ValidationException", "ArchiveName is required", 400)
    if name in _archives:
        return error_response_json("ResourceAlreadyExistsException", f"Archive {name} already exists", 400)

    source_arn = data.get("EventSourceArn", "")
    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:archive/{name}"
    _archives[name] = {
        "ArchiveName": name,
        "ArchiveArn": arn,
        "EventSourceArn": source_arn,
        "Description": data.get("Description", ""),
        "EventPattern": data.get("EventPattern", ""),
        "RetentionDays": data.get("RetentionDays", 0),
        "State": "ENABLED",
        "CreationTime": _now_ts(),
        "EventCount": 0,
        "SizeBytes": 0,
    }
    return json_response({"ArchiveArn": arn, "State": "ENABLED", "CreationTime": _archives[name]["CreationTime"]})


def _delete_archive(data):
    name = data.get("ArchiveName")
    if name not in _archives:
        return error_response_json("ResourceNotFoundException", f"Archive {name} does not exist.", 400)
    del _archives[name]
    return json_response({})


def _describe_archive(data):
    name = data.get("ArchiveName")
    archive = _archives.get(name)
    if not archive:
        return error_response_json("ResourceNotFoundException", f"Archive {name} does not exist.", 400)
    return json_response(archive)


def _update_archive(data):
    name = data.get("ArchiveName")
    if not name:
        return error_response_json("ValidationException", "ArchiveName is required", 400)
    archive = _archives.get(name)
    if not archive:
        return error_response_json("ResourceNotFoundException", f"Archive {name} does not exist.", 400)

    if "Description" in data:
        archive["Description"] = data["Description"]
    if "EventPattern" in data:
        ep = data["EventPattern"]
        if isinstance(ep, str) and ep:
            try:
                json.loads(ep)
            except json.JSONDecodeError:
                return error_response_json(
                    "InvalidEventPatternException",
                    "Event pattern is not valid JSON",
                    400,
                )
        archive["EventPattern"] = ep
    if "RetentionDays" in data:
        archive["RetentionDays"] = int(data["RetentionDays"])

    archive["LastUpdatedTime"] = _now_ts()
    return json_response({
        "ArchiveArn": archive["ArchiveArn"],
        "State": archive.get("State", "ENABLED"),
        "CreationTime": archive["CreationTime"],
    })


def _list_archives(data):
    prefix = data.get("NamePrefix", "")
    source_arn = data.get("EventSourceArn", "")
    state = data.get("State", "")
    results = []
    for name, archive in _archives.items():
        if prefix and not name.startswith(prefix):
            continue
        if source_arn and archive.get("EventSourceArn") != source_arn:
            continue
        if state and archive.get("State") != state:
            continue
        results.append(archive)
    return json_response({"Archives": results})


# ---------------------------------------------------------------------------
# Replays
# ---------------------------------------------------------------------------

def _start_replay(data):
    name = data.get("ReplayName")
    if not name:
        return error_response_json("ValidationException", "ReplayName is required", 400)
    if name in _replays:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"Replay {name} already exists",
            400,
        )
    dest = data.get("Destination") or {}
    if not dest.get("Arn"):
        return error_response_json(
            "ValidationException",
            "Destination.Arn is required",
            400,
        )

    source_arn = data.get("EventSourceArn", "")
    # source_arn format: arn:aws:events:{region}:{account}:archive/{name}
    archive_name = source_arn.split("/")[-1] if "/" in source_arn else ""
    archive = _archives.get(archive_name)
    if not archive:
        return error_response_json(
            "ResourceNotFoundException",
            f"Archive {archive_name} does not exist.",
            400,
        )

    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:replay/{name}"
    now = _now_ts()
    event_start = _coerce_timestamp(data.get("EventStartTime", now))
    event_end = _coerce_timestamp(data.get("EventEndTime", now))
    replay = {
        "ReplayName": name,
        "ReplayArn": arn,
        "Description": data.get("Description", ""),
        "EventSourceArn": source_arn,
        "EventStartTime": event_start,
        "EventEndTime": event_end,
        "Destination": dest,
        "State": "STARTING",
        "ReplayStartTime": now,
    }
    _replays[name] = replay

    dest_bus_name = _normalize_bus_name(dest.get("Arn", ""))

    def _run():
        replay["State"] = "RUNNING"
        for event in list(archive.get("Events", [])):
            ts = event.get("Time", 0)
            if not (event_start <= ts <= event_end):
                continue
            replayed = dict(event)
            replayed["EventBusName"] = dest_bus_name
            _dispatch_event(replayed)
        replay["State"] = "COMPLETED"
        replay["ReplayEndTime"] = _now_ts()

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return json_response({"ReplayArn": arn, "State": "RUNNING"})


def _describe_replay(data):
    name = data.get("ReplayName")
    if not name:
        return error_response_json("ValidationException", "ReplayName is required", 400)
    rep = _replays.get(name)
    if not rep:
        return error_response_json("ResourceNotFoundException", f"Replay {name} does not exist.", 400)
    return json_response(dict(rep))


def _list_replays(data):
    prefix = data.get("NamePrefix", "")
    state_f = data.get("State", "")
    source_f = data.get("EventSourceArn", "")
    results = []
    for n in sorted(_replays.keys()):
        rep = _replays[n]
        if prefix and not n.startswith(prefix):
            continue
        if state_f and rep.get("State") != state_f:
            continue
        if source_f and rep.get("EventSourceArn") != source_f:
            continue
        results.append({
            "ReplayName": rep["ReplayName"],
            "ReplayArn": rep["ReplayArn"],
            "State": rep["State"],
            "EventSourceArn": rep.get("EventSourceArn", ""),
            "ReplayStartTime": rep.get("ReplayStartTime", ""),
        })
    return json_response({"Replays": results})


def _cancel_replay(data):
    name = data.get("ReplayName")
    if not name:
        return error_response_json("ValidationException", "ReplayName is required", 400)
    rep = _replays.get(name)
    if not rep:
        return error_response_json("ResourceNotFoundException", f"Replay {name} does not exist.", 400)
    if rep["State"] == "COMPLETED":
        return error_response_json(
            "ValidationException",
            "Replay is already completed",
            400,
        )
    if rep["State"] == "CANCELLED":
        return json_response({"ReplayArn": rep["ReplayArn"], "State": "CANCELLED"})
    rep["State"] = "CANCELLED"
    rep["ReplayEndTime"] = _now_ts()
    return json_response({"ReplayArn": rep["ReplayArn"], "State": "CANCELLED"})


# ---------------------------------------------------------------------------
# Global endpoints + SaaS partner event sources (minimal / stub)
# ---------------------------------------------------------------------------

def _create_endpoint(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name in _endpoints:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"Endpoint {name} already exists", 400)
    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:endpoint/{name}"
    now = _now_ts()
    _endpoints[name] = {
        "Name": name,
        "Description": data.get("Description", ""),
        "RoutingConfig": data.get("RoutingConfig", {}),
        "ReplicationConfig": data.get("ReplicationConfig", {}),
        "EventBuses": data.get("EventBuses", []),
        "RoleArn": data.get("RoleArn", ""),
        "Arn": arn,
        "EndpointUrl": f"https://{name}.global-events.{get_region()}.amazonaws.com",
        "State": "ACTIVE",
        "CreationTime": now,
        "LastModifiedTime": now,
    }
    ep = _endpoints[name]
    return json_response({
        "Name": ep["Name"],
        "Arn": ep["Arn"],
        "RoutingConfig": ep["RoutingConfig"],
        "ReplicationConfig": ep["ReplicationConfig"],
        "EventBuses": ep["EventBuses"],
        "RoleArn": ep["RoleArn"],
        "State": ep["State"],
    })


def _delete_endpoint(data):
    name = data.get("Name")
    if name not in _endpoints:
        return error_response_json("ResourceNotFoundException",
                                   f"Endpoint {name} does not exist.", 400)
    del _endpoints[name]
    return json_response({})


def _describe_endpoint(data):
    name = data.get("Name")
    ep = _endpoints.get(name)
    if not ep:
        return error_response_json("ResourceNotFoundException",
                                   f"Endpoint {name} does not exist.", 400)
    return json_response({
        "Name": ep["Name"],
        "Description": ep.get("Description", ""),
        "Arn": ep["Arn"],
        "RoutingConfig": ep.get("RoutingConfig", {}),
        "ReplicationConfig": ep.get("ReplicationConfig", {}),
        "EventBuses": ep.get("EventBuses", []),
        "RoleArn": ep.get("RoleArn", ""),
        "EndpointId": ep["Name"],
        "EndpointUrl": ep["EndpointUrl"],
        "State": ep["State"],
        "StateReason": "",
        "CreationTime": ep["CreationTime"],
        "LastModifiedTime": ep.get("LastModifiedTime", ep["CreationTime"]),
    })


def _list_endpoints(data):
    prefix = data.get("NamePrefix", "")
    home = data.get("HomeRegion", "")
    results = []
    for n in sorted(_endpoints.keys()):
        ep = _endpoints[n]
        if prefix and not n.startswith(prefix):
            continue
        if home and get_region() != home:
            continue
        results.append({
            "Name": ep["Name"],
            "Arn": ep["Arn"],
            "EndpointUrl": ep["EndpointUrl"],
            "State": ep["State"],
            "CreationTime": ep["CreationTime"],
        })
    return json_response({"Endpoints": results})


def _update_endpoint(data):
    name = data.get("Name")
    if name not in _endpoints:
        return error_response_json("ResourceNotFoundException",
                                   f"Endpoint {name} does not exist.", 400)
    ep = _endpoints[name]
    now = _now_ts()
    for key in ("Description", "RoutingConfig", "ReplicationConfig", "EventBuses", "RoleArn"):
        if key in data:
            ep[key] = data[key]
    ep["LastModifiedTime"] = now
    return json_response({
        "Name": ep["Name"],
        "Arn": ep["Arn"],
        "RoutingConfig": ep["RoutingConfig"],
        "ReplicationConfig": ep["ReplicationConfig"],
        "EventBuses": ep["EventBuses"],
        "RoleArn": ep["RoleArn"],
        "EndpointId": ep["Name"],
        "EndpointUrl": ep["EndpointUrl"],
        "State": ep["State"],
    })


def _activate_event_source(data):
    _ = data.get("Name", "")
    return json_response({})


def _deactivate_event_source(data):
    _ = data.get("Name", "")
    return json_response({})


def _describe_event_source(data):
    name = data.get("Name", "")
    return json_response({
        "Name": name,
        "State": "ENABLED",
        "Arn": f"arn:aws:events:{get_region()}::event-source/{name}" if name else "",
    })


def _partner_key(account: str, name: str) -> str:
    return f"{account}|{name}"


def _create_partner_event_source(data):
    name = data.get("Name")
    account = data.get("Account", "")
    if not name or not account:
        return error_response_json("ValidationException", "Name and Account are required", 400)
    pk = _partner_key(account, name)
    if pk in _partner_event_sources:
        return error_response_json("ResourceAlreadyExistsException",
                                   "Partner event source already exists", 400)
    arn = f"arn:aws:events:{get_region()}:{account}:event-source/{name}"
    _partner_event_sources[pk] = {
        "Name": name,
        "Account": account,
        "EventSourceArn": arn,
    }
    return json_response({"EventSourceArn": arn})


def _delete_partner_event_source(data):
    name = data.get("Name")
    account = data.get("Account", "")
    pk = _partner_key(account, name)
    if pk not in _partner_event_sources:
        return error_response_json("ResourceNotFoundException",
                                   "Partner event source does not exist.", 400)
    del _partner_event_sources[pk]
    return json_response({})


def _describe_partner_event_source(data):
    name = data.get("Name")
    for pk, rec in _partner_event_sources.items():
        if rec["Name"] == name:
            return json_response({
                "Name": rec["Name"],
                "Arn": rec["EventSourceArn"],
                "State": "ACTIVE",
            })
    return error_response_json("ResourceNotFoundException",
                               f"Partner event source {name} does not exist.", 400)


def _list_partner_event_sources(data):
    prefix = data.get("NamePrefix", "")
    results = []
    for rec in _partner_event_sources.values():
        if prefix and not rec["Name"].startswith(prefix):
            continue
        results.append({
            "Name": rec["Name"],
            "Arn": rec["EventSourceArn"],
            "State": "ACTIVE",
        })
    return json_response({"PartnerEventSources": results})


def _list_partner_event_source_accounts(data):
    _ = data.get("EventSourceName", "")
    return json_response({"PartnerEventSourceAccounts": [], "NextToken": ""})


def _list_event_sources(data):
    prefix = data.get("NamePrefix", "")
    _ = prefix
    return json_response({"EventSources": []})


def _put_partner_events(data):
    entries = data.get("Entries", [])
    results = [{"EventId": new_uuid()} for _ in entries]
    return json_response({"FailedEntryCount": 0, "Entries": results})


# ---------------------------------------------------------------------------
# Permissions (resource policies)
# ---------------------------------------------------------------------------

def _put_permission(data):
    bus_name = data.get("EventBusName", "default")
    statement_id = data.get("StatementId") or new_uuid()

    if bus_name not in _event_bus_policies:
        _event_bus_policies[bus_name] = {"Version": "2012-10-17", "Statement": []}

    policy = _event_bus_policies[bus_name]
    policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != statement_id]

    statement = {
        "Sid": statement_id,
        "Effect": "Allow",
        "Principal": data.get("Principal", "*"),
        "Action": data.get("Action", "events:PutEvents"),
        "Resource": f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/{bus_name}",
    }
    condition = data.get("Condition")
    if condition:
        statement["Condition"] = condition
    policy["Statement"].append(statement)

    return json_response({})


def _remove_permission(data):
    bus_name = data.get("EventBusName", "default")
    statement_id = data.get("StatementId")
    remove_all = data.get("RemoveAllPermissions", False)

    if remove_all:
        _event_bus_policies.pop(bus_name, None)
        return json_response({})

    if bus_name in _event_bus_policies:
        policy = _event_bus_policies[bus_name]
        policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != statement_id]
        if not policy["Statement"]:
            del _event_bus_policies[bus_name]

    return json_response({})


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def _create_connection(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name in _connections:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"Connection {name} already exists", 400)

    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:connection/{name}"
    now = _now_ts()
    _connections[name] = {
        "Name": name,
        "ConnectionArn": arn,
        "ConnectionState": "AUTHORIZED",
        "AuthorizationType": data.get("AuthorizationType", ""),
        "AuthParameters": data.get("AuthParameters", {}),
        "Description": data.get("Description", ""),
        "CreationTime": now,
        "LastModifiedTime": now,
        "LastAuthorizedTime": now,
    }
    return json_response({
        "ConnectionArn": arn,
        "ConnectionState": "AUTHORIZED",
        "CreationTime": now,
    })


def _describe_connection(data):
    name = data.get("Name")
    conn = _connections.get(name)
    if not conn:
        return error_response_json("ResourceNotFoundException",
                                   f"Connection {name} does not exist.", 400)
    return json_response(conn)


def _delete_connection(data):
    name = data.get("Name")
    conn = _connections.pop(name, None)
    if not conn:
        return error_response_json("ResourceNotFoundException",
                                   f"Connection {name} does not exist.", 400)
    return json_response({
        "ConnectionArn": conn["ConnectionArn"],
        "ConnectionState": "DELETING",
        "LastModifiedTime": _now_ts(),
    })


def _list_connections(data):
    prefix = data.get("NamePrefix", "")
    state = data.get("ConnectionState", "")
    results = []
    for name in sorted(_connections):
        conn = _connections[name]
        if prefix and not name.startswith(prefix):
            continue
        if state and conn.get("ConnectionState") != state:
            continue
        results.append({
            "Name": conn["Name"],
            "ConnectionArn": conn["ConnectionArn"],
            "ConnectionState": conn["ConnectionState"],
            "AuthorizationType": conn["AuthorizationType"],
            "CreationTime": conn["CreationTime"],
            "LastModifiedTime": conn["LastModifiedTime"],
            "LastAuthorizedTime": conn.get("LastAuthorizedTime", ""),
        })
    return json_response({"Connections": results})


def _update_connection(data):
    name = data.get("Name")
    if name not in _connections:
        return error_response_json("ResourceNotFoundException",
                                   f"Connection {name} does not exist.", 400)
    conn = _connections[name]
    now = _now_ts()
    for key in ("AuthorizationType", "AuthParameters", "Description"):
        if key in data:
            conn[key] = data[key]
    conn["LastModifiedTime"] = now
    conn["ConnectionState"] = "AUTHORIZED"
    conn["LastAuthorizedTime"] = now

    return json_response({
        "ConnectionArn": conn["ConnectionArn"],
        "ConnectionState": conn["ConnectionState"],
        "LastModifiedTime": now,
    })


def _deauthorize_connection(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    conn = _connections.get(name)
    if not conn:
        return error_response_json("ResourceNotFoundException",
                                   f"Connection {name} does not exist.", 400)
    now = _now_ts()
    conn["ConnectionState"] = "DEAUTHORIZED"
    conn["LastModifiedTime"] = now
    conn.pop("LastAuthorizedTime", None)
    return json_response({
        "ConnectionArn": conn["ConnectionArn"],
        "ConnectionState": conn["ConnectionState"],
        "LastModifiedTime": now,
    })


# ---------------------------------------------------------------------------
# API Destinations
# ---------------------------------------------------------------------------

def _create_api_destination(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name in _api_destinations:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"ApiDestination {name} already exists", 400)

    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:api-destination/{name}"
    now = _now_ts()
    _api_destinations[name] = {
        "Name": name,
        "ApiDestinationArn": arn,
        "ApiDestinationState": "ACTIVE",
        "ConnectionArn": data.get("ConnectionArn", ""),
        "InvocationEndpoint": data.get("InvocationEndpoint", ""),
        "HttpMethod": data.get("HttpMethod", ""),
        "InvocationRateLimitPerSecond": data.get("InvocationRateLimitPerSecond", 300),
        "Description": data.get("Description", ""),
        "CreationTime": now,
        "LastModifiedTime": now,
    }
    return json_response({
        "ApiDestinationArn": arn,
        "ApiDestinationState": "ACTIVE",
        "CreationTime": now,
        "LastModifiedTime": now,
    })


def _describe_api_destination(data):
    name = data.get("Name")
    dest = _api_destinations.get(name)
    if not dest:
        return error_response_json("ResourceNotFoundException",
                                   f"ApiDestination {name} does not exist.", 400)
    return json_response(dest)


def _delete_api_destination(data):
    name = data.get("Name")
    if name not in _api_destinations:
        return error_response_json("ResourceNotFoundException",
                                   f"ApiDestination {name} does not exist.", 400)
    del _api_destinations[name]
    return json_response({})


def _list_api_destinations(data):
    prefix = data.get("NamePrefix", "")
    conn_arn = data.get("ConnectionArn", "")
    results = []
    for name in sorted(_api_destinations):
        dest = _api_destinations[name]
        if prefix and not name.startswith(prefix):
            continue
        if conn_arn and dest.get("ConnectionArn") != conn_arn:
            continue
        results.append({
            "Name": dest["Name"],
            "ApiDestinationArn": dest["ApiDestinationArn"],
            "ApiDestinationState": dest["ApiDestinationState"],
            "ConnectionArn": dest["ConnectionArn"],
            "InvocationEndpoint": dest["InvocationEndpoint"],
            "HttpMethod": dest["HttpMethod"],
            "CreationTime": dest["CreationTime"],
            "LastModifiedTime": dest["LastModifiedTime"],
        })
    return json_response({"ApiDestinations": results})


def _update_api_destination(data):
    name = data.get("Name")
    if name not in _api_destinations:
        return error_response_json("ResourceNotFoundException",
                                   f"ApiDestination {name} does not exist.", 400)
    dest = _api_destinations[name]
    now = _now_ts()
    for key in ("ConnectionArn", "InvocationEndpoint", "HttpMethod",
                "InvocationRateLimitPerSecond", "Description"):
        if key in data:
            dest[key] = data[key]
    dest["LastModifiedTime"] = now

    return json_response({
        "ApiDestinationArn": dest["ApiDestinationArn"],
        "ApiDestinationState": dest["ApiDestinationState"],
        "LastModifiedTime": now,
    })


def reset():
    _rules.clear()
    _targets.clear()
    _events_log.clear()
    _tags.clear()
    _archives.clear()
    _event_bus_policies.clear()
    _connections.clear()
    _api_destinations.clear()
    _replays.clear()
    _endpoints.clear()
    _partner_event_sources.clear()
    _event_buses.clear()
    # The "default" bus is lazily recreated per-account on next access via
    # _ensure_default_bus(), so nothing to re-seed here.
