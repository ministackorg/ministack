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

import calendar
import copy
import functools
import hashlib
import ipaddress
import json
import logging
import operator
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    set_request_account_id,
    set_request_region,
)

logger = logging.getLogger("events")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_SUPPORTED_TARGET_SERVICES = {
    "appsync",
    "batch",
    "codebuild",
    "codepipeline",
    "ecs",
    "events",
    "execute-api",
    "firehose",
    "glue",
    "imagebuilder",
    "inspector",
    "kinesis",
    "lambda",
    "logs",
    "redshift",
    "redshift-serverless",
    "sagemaker",
    "sns",
    "sqs",
    "ssm",
    "ssm-incidents",
    "states",
}


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


from ministack.core.persistence import load_state

# Per-account and per-region registries. The "default" bus is lazily created
# per account/region on first access so every tenant has its own default bus
# with ARN account-id and region segments matching the caller.
_event_buses = AccountRegionScopedDict()
_rules = AccountRegionScopedDict()
_targets = AccountRegionScopedDict()
# AccountRegionScopedDict under "entries" keeps list semantics while scoping
# reads to the caller's account and region.
_events_log = AccountRegionScopedDict()
_tags = AccountRegionScopedDict()
_archives = AccountRegionScopedDict()
_event_bus_policies = AccountRegionScopedDict()  # bus_name -> {Statement: [...]}
_connections = AccountRegionScopedDict()         # connection_name -> {...}
_api_destinations = AccountRegionScopedDict()    # destination_name -> {...}
_replays = AccountRegionScopedDict()             # replay_name -> replay record
_endpoints = AccountRegionScopedDict()           # endpoint name -> endpoint record
# Partner event sources, per-account/region (key: "account|name" pattern inside each tenant).
_partner_event_sources = AccountRegionScopedDict()

# Tracks when each scheduled rule last fired: {(account_id, region, rule_key): timestamp}.
# Plain dict (not AccountScopedDict) because the scheduler thread owns it globally.
_rule_last_fired: dict = {}


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
        _restore_targets_store(data.get("targets", {}))
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

        for bus in _event_buses.all_values():
            if "CreationTime" in bus:
                bus["CreationTime"] = _coerce_timestamp(bus["CreationTime"])
            if "LastModifiedTime" in bus:
                bus["LastModifiedTime"] = _coerce_timestamp(bus["LastModifiedTime"])

        for rule in _rules.all_values():
            if "CreationTime" in rule:
                rule["CreationTime"] = _coerce_timestamp(rule["CreationTime"])

        for rep in _replays.all_values():
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


def _events_record_scope(record: dict | None, default_account_id: str | None = None) -> tuple[str, str]:
    if isinstance(record, dict):
        for key in (
            "Arn",
            "ArchiveArn",
            "ReplayArn",
            "ConnectionArn",
            "ApiDestinationArn",
            "EventSourceArn",
        ):
            arn = record.get(key)
            if not isinstance(arn, str) or not arn.startswith("arn:"):
                continue
            try:
                spec = parse_arn(arn)
            except ArnParseError:
                continue
            if spec.service == "events":
                return spec.account_id or default_account_id or get_account_id(), spec.region or get_region()
    return default_account_id or get_account_id(), get_region()


def _rule_scope(rule_key: str, default_account_id: str | None = None) -> tuple[str, str]:
    for (account_id, region, key), rule in _rules.all_items():
        if key != rule_key:
            continue
        if default_account_id is not None and account_id != default_account_id:
            continue
        scoped_account_id, scoped_region = _events_record_scope(rule, account_id)
        return scoped_account_id, scoped_region
    return default_account_id or get_account_id(), get_region()


def _restore_targets_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _targets.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, rule_key), targets in data._data.items():
            scoped_account_id, region = _rule_scope(rule_key, account_id)
            _targets.set_scoped(scoped_account_id, region, rule_key, copy.deepcopy(targets))
        return
    if isinstance(data, dict):
        for key, targets in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, rule_key = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, rule_key = key
                account_id, region = _rule_scope(rule_key, account_id)
            else:
                rule_key = key
                account_id, region = _rule_scope(rule_key)
            _targets.set_scoped(account_id, region, rule_key, copy.deepcopy(targets))


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
    bus_record = {
        "Name": name,
        "Arn": arn,
        "Description": description,
        "CreationTime": _now_ts(),
        "LastModifiedTime": _now_ts(),
    }
    # Optional 2026-03 additive fields — accept-and-echo so SDK callers
    # configuring rule-match logging round-trip cleanly.
    for k in ("LogConfig", "DeadLetterConfig", "KmsKeyIdentifier"):
        if k in data:
            bus_record[k] = data[k]
    _event_buses[name] = bus_record
    tags = data.get("Tags", [])
    if tags:
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}
    out = {"EventBusArn": arn}
    if "LogConfig" in bus_record:
        out["LogConfig"] = bus_record["LogConfig"]
    return json_response(out)


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
            entry = {
                "Name": b["Name"],
                "Arn": b["Arn"],
                "Description": b.get("Description", ""),
                "CreationTime": b["CreationTime"],
                "LastModifiedTime": b.get("LastModifiedTime", b.get("CreationTime")),
            }
            # AWS spec: Policy is optional; omit when no policy is set rather
            # than returning an empty string (Java SDK v2 sees a stray empty).
            policy = _event_bus_policies.get(n)
            if policy:
                entry["Policy"] = json.dumps(policy)
            buses.append(entry)
    return json_response({"EventBuses": buses})


def _describe_event_bus(data):
    name = data.get("Name", "default")
    bus = _event_buses.get(name)
    if not bus:
        return error_response_json("ResourceNotFoundException", f"Event bus {name} not found", 400)
    out = {
        "Name": bus["Name"],
        "Arn": bus["Arn"],
        "Description": bus.get("Description", ""),
        "CreationTime": bus["CreationTime"],
        "LastModifiedTime": bus.get("LastModifiedTime", bus.get("CreationTime")),
    }
    # AWS spec: Policy is optional; omit when no policy is set.
    policy = _event_bus_policies.get(name)
    if policy:
        out["Policy"] = json.dumps(policy)
    for k in ("LogConfig", "DeadLetterConfig", "KmsKeyIdentifier"):
        if k in bus:
            out[k] = bus[k]
    return json_response(out)


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
    for k in ("LogConfig", "DeadLetterConfig", "KmsKeyIdentifier"):
        if k in data:
            bus[k] = data[k]

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


_RATE_RE = re.compile(r"^rate\(\d+\s+(minute|minutes|hour|hours|day|days)\)$")


def _validate_schedule_expression(expr: str) -> bool:
    if not expr:
        return True
    if _RATE_RE.match(expr):
        return True
    if expr.startswith("cron("):
        # Reuses the structural parser so PutRule rejects bad cron syntax (DoM/DoW
        # both non-'?', unknown tokens, malformed L/W/#) the same way real AWS does.
        return _parse_cron_fields(expr) is not None
    return False


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
    pattern_error = _event_pattern_error(event_pattern)
    if pattern_error:
        return _invalid_event_pattern(pattern_error)

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


def _opaque_offset_encode(offset: int) -> str:
    """AWS NextToken values are opaque (base64-style) per the EventBridge spec —
    not a raw integer offset. Encode the cursor so SDKs that round-trip and
    inspect the value see something opaque rather than a leakable index."""
    import base64 as _b64
    return _b64.urlsafe_b64encode(str(offset).encode("ascii")).decode("ascii").rstrip("=")


def _opaque_offset_decode(token: str) -> int:
    import base64 as _b64
    if not token:
        return 0
    try:
        padded = token + "=" * (-len(token) % 4)
        return int(_b64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii"))
    except (ValueError, UnicodeDecodeError):
        return 0


def _list_rules(data):
    prefix = data.get("NamePrefix", "")
    bus = data.get("EventBusName", "default")
    # AWS spec: ListRules supports NextToken + Limit (1..100, default 100).
    limit = int(data.get("Limit", 100))
    if limit < 1 or limit > 100:
        limit = 100
    rules = []
    for key, r in _rules.items():
        if r.get("EventBusName", "default") != bus:
            continue
        if prefix and not r["Name"].startswith(prefix):
            continue
        rules.append(_rule_out(r))
    rules.sort(key=lambda x: x["Name"])
    start = _opaque_offset_decode(data.get("NextToken", ""))
    page = rules[start:start + limit]
    resp = {"Rules": page}
    if start + limit < len(rules):
        resp["NextToken"] = _opaque_offset_encode(start + limit)
    return json_response(resp)


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
    # AWS spec members on DescribeRule/RuleResponse — emit when populated.
    if rule.get("ManagedBy"):
        out["ManagedBy"] = rule["ManagedBy"]
    if rule.get("CreatedBy"):
        out["CreatedBy"] = rule["CreatedBy"]
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

    for target in targets:
        error = _validate_target_arn(target)
        if error:
            return error

    if key not in _targets:
        _targets[key] = []
    existing_ids = {t["Id"] for t in _targets[key]}
    for t in targets:
        if t["Id"] in existing_ids:
            _targets[key] = [x for x in _targets[key] if x["Id"] != t["Id"]]
        _targets[key].append(t)
    return json_response({"FailedEntryCount": 0, "FailedEntries": []})


def _validate_target_arn(target):
    arn = target.get("Arn", "")
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return error_response_json(
            "ValidationException",
            f"Parameter {arn} is not valid. Reason: Provided Arn is not in correct format.",
            400,
        )

    if spec.service not in _SUPPORTED_TARGET_SERVICES:
        return error_response_json(
            "ValidationException",
            f"{spec.service} is not a supported service for a target.",
            400,
        )

    if (
        spec.service == "events"
        and spec.resource.startswith("event-bus/")
        and (spec.account_id != get_account_id() or spec.region != get_region())
        and not target.get("RoleArn")
    ):
        return error_response_json("ValidationException", "RoleArn is required", 400)

    return None


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
    start = _opaque_offset_decode(next_token)
    page = matched[start:start + limit]
    resp = {"RuleNames": page}
    if start + limit < len(matched):
        resp["NextToken"] = _opaque_offset_encode(start + limit)
    return json_response(resp)



def _event_pattern_error(pattern_str):
    """The reason AWS would refuse this event pattern, or ``None`` if it accepts
    it. Shared by every API that takes one, so they cannot disagree."""
    if not pattern_str:
        return None
    if not isinstance(pattern_str, str):
        return "Invalid JSON"
    return _parse_pattern_text(pattern_str)[1]


def _invalid_event_pattern(reason: str):
    return error_response_json(
        "InvalidEventPatternException", f"Event pattern is not valid. Reason: {reason}", 400)


@functools.lru_cache(maxsize=1024)
def _iso_time(epoch_seconds) -> str:
    """The ISO-8601 rendering of an epoch second, or ``""`` when the number is
    not a time at all. Memoized: every event in the same second renders alike.

    The guard is not decoration — a ``time`` no platform clock can represent
    (``1e30``) makes ``fromtimestamp`` raise, and this runs while building the
    view every pattern is matched against, so it would surface as a 500 out of
    the enclosing PutEvents."""
    try:
        return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return ""


def _event_time_string(event) -> str:
    """The event's ``time`` in the form the envelope carries it. ``Time`` is
    stored as int epoch seconds while a pattern filters on the ISO-8601 string
    the target receives, so matcher and delivery share this one rendering."""
    raw = event.get("Time", "")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return _iso_time(raw)
    return raw if isinstance(raw, str) else ""


# The envelope names ``_event_from_test_payload`` models. Anything else a
# caller puts at the top level of a ``TestEventPattern`` event is carried
# through as an extra path rather than dropped: AWS compiles a pattern to a set
# of paths and has no envelope allow-list, so a pattern naming an unmodelled key
# must be able to match an event that actually carries it. ``PutEvents`` cannot
# produce one — its entries are a fixed shape — so this only ever fires here.
_TEST_PAYLOAD_MODELLED_KEYS = frozenset({
    "detail", "Detail", "source", "Source", "detail-type", "DetailType",
    "account", "Account", "region", "Region", "resources", "Resources",
    "id", "EventId", "time", "Time", "version", "Version",
    "replay-name", "ReplayName",
})


def _event_from_test_payload(event_obj: dict) -> dict:
    """Map CloudWatch Events-shaped JSON to internal fields used by _matches_pattern."""
    detail = event_obj.get("detail", event_obj.get("Detail", {}))
    if not isinstance(detail, str):
        # Re-encoded, not ``str()``: repr would render a null as ``None`` and a
        # list in Python syntax, and a pattern can ask about either.
        detail = json.dumps(detail)
    synthetic = {
        "Source": event_obj.get("source", event_obj.get("Source", "")),
        "DetailType": event_obj.get("detail-type", event_obj.get("DetailType", "")),
        "Detail": detail,
        "Account": event_obj.get("account", event_obj.get("Account", get_account_id())),
        "Region": event_obj.get("region", event_obj.get("Region", get_region())),
        "Resources": event_obj.get("resources", event_obj.get("Resources", [])),
        "EventId": event_obj.get("id", event_obj.get("EventId", "")),
        "Time": event_obj.get("time", event_obj.get("Time", "")),
        "Version": event_obj.get("version", event_obj.get("Version", "0")),
    }
    # Absent rather than empty on a normal event; see ``_pattern_event_view``.
    replay_name = event_obj.get("replay-name", event_obj.get("ReplayName"))
    if replay_name is not None:
        synthetic["ReplayName"] = replay_name
    extra = {key: value for key, value in event_obj.items()
             if key not in _TEST_PAYLOAD_MODELLED_KEYS}
    if extra:
        synthetic["_ExtraEnvelope"] = extra
    return synthetic


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

    pattern_error = _event_pattern_error(pattern_str)
    if pattern_error:
        return _invalid_event_pattern(pattern_error)

    synthetic = _event_from_test_payload(event_obj)
    matched = _matches_pattern(pattern_str, synthetic)
    return json_response({"Result": bool(matched)})


# ---------------------------------------------------------------------------
# PutEvents + event pattern matching + target dispatch
# ---------------------------------------------------------------------------

def _events_resource_name_from_ref(value, prefix, label):
    if not value or not value.startswith("arn:"):
        return value, None
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return "", (
            "ValidationException",
            f"Parameter {label} is not valid. Reason: Provided Arn is not in correct format.",
        )
    if spec.service != "events" or not spec.resource.startswith(prefix):
        return "", (
            "ValidationException",
            f"Parameter {label} is not valid. Reason: Provided Arn is not an EventBridge {label}.",
        )
    if spec.account_id != get_account_id() or spec.region != get_region():
        return "", (
            "ResourceNotFoundException",
            f"EventBridge {label} {value} does not exist.",
        )
    return spec.resource.split("/", 1)[1], None


def _event_bus_name_from_ref(name):
    return _events_resource_name_from_ref(name, "event-bus/", "EventBusName")


def _archive_name_from_ref(source_arn):
    if not source_arn or not source_arn.startswith("arn:"):
        return "", (
            "ValidationException",
            "Parameter EventSourceArn is not valid. Reason: Provided Arn is not in correct format.",
        )
    return _events_resource_name_from_ref(source_arn, "archive/", "EventSourceArn")


def _resolve_taggable_events_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, error_response_json(
            "ValidationException",
            "Parameter ResourceARN is not valid. Reason: Provided Arn is not in correct format.",
            400,
        )

    if spec.partition != "aws" or spec.service != "events":
        return None, error_response_json(
            "ValidationException",
            "Parameter ResourceARN is not valid. Reason: Provided Arn is not an EventBridge ResourceARN.",
            400,
        )
    if spec.region != get_region() or spec.account_id != get_account_id():
        return None, error_response_json(
            "ResourceNotFoundException",
            f"EventBridge resource {arn} does not exist.",
            400,
        )

    resource = spec.resource
    if resource.startswith("event-bus/"):
        name = resource.split("/", 1)[1]
        if name == "default":
            exists = arn == f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/default"
        else:
            record = _event_buses.get(name)
            exists = bool(record) and record.get("Arn") == arn
    elif resource.startswith("rule/"):
        parts = resource.split("/")
        if len(parts) == 2:
            bus, rule = "default", parts[1]
        elif len(parts) == 3:
            _, bus, rule = parts
        else:
            bus, rule = "", ""
        record = _rules.get(_rule_key(rule, bus)) if rule else None
        exists = bool(record) and record.get("Arn") == arn
    elif resource.startswith("archive/"):
        name = resource.split("/", 1)[1]
        record = _archives.get(name)
        exists = bool(record) and record.get("ArchiveArn") == arn
    elif resource.startswith("replay/"):
        name = resource.split("/", 1)[1]
        record = _replays.get(name)
        exists = bool(record) and record.get("ReplayArn") == arn
    elif resource.startswith("endpoint/"):
        name = resource.split("/", 1)[1]
        record = _endpoints.get(name)
        exists = bool(record) and record.get("Arn") == arn
    elif resource.startswith("connection/"):
        name = resource.split("/", 1)[1]
        record = _connections.get(name)
        exists = bool(record) and record.get("ConnectionArn") == arn
    elif resource.startswith("api-destination/"):
        name = resource.split("/", 1)[1]
        record = _api_destinations.get(name)
        exists = bool(record) and record.get("ApiDestinationArn") == arn
    else:
        return None, error_response_json(
            "ValidationException",
            "Parameter ResourceARN is not valid. Reason: Provided Arn is not a taggable EventBridge resource.",
            400,
        )

    if not exists:
        return None, error_response_json(
            "ResourceNotFoundException",
            f"EventBridge resource {arn} does not exist.",
            400,
        )
    return arn, None


def _put_events(data):
    entries = data.get("Entries", [])
    # AWS spec: PutEvents.Entries list min=1 max=10. Real AWS rejects with
    # ValidationException; matching that here so SDKs see the same constraint.
    if len(entries) > 10:
        return error_response_json(
            "ValidationException",
            "1 validation error detected: Value '%d' at 'entries' failed to satisfy constraint: "
            "Member must have length less than or equal to 10" % len(entries),
            400,
        )
    results = []
    failed = 0
    for entry in entries:
        event_id = new_uuid()
        bus_name, bus_error = _event_bus_name_from_ref(entry.get("EventBusName", "default"))
        if bus_error:
            code, message = bus_error
            results.append({"ErrorCode": code, "ErrorMessage": message})
            failed += 1
            continue
        # AWS Time is a timestamp shape; ministack convention is int epoch seconds
        # (Java SDK v2 chokes on floats). Persisted in the event_record so
        # archive replay also dispatches the int form.
        event_time = int(_now_ts())

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

        # One view for both walks; rebuilding it re-parses ``Detail``.
        view = _pattern_event_view(event_record)
        _dispatch_event(event_record, view)
        _archive_event(event_record, view)

    return json_response({"FailedEntryCount": failed, "Entries": results})


def _archive_event(event, view=None):
    bus_name = event.get("EventBusName", "default")
    bus_arn = f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/{bus_name}"
    for archive in _archives.values():
        if archive.get("EventSourceArn") != bus_arn:
            continue
        pattern = archive.get("EventPattern", "")
        if pattern:
            view = view or _pattern_event_view(event)
            if not _matches_pattern_view(
                    pattern, view, f"archive {archive.get('ArchiveName', '?')}"):
                continue
        archive.setdefault("Events", []).append(event)
        archive["EventCount"] = archive.get("EventCount", 0) + 1


def _dispatch_event(event, view=None):
    bus_name = event.get("EventBusName", "default")
    event_path = set(event.get("_DispatchPath") or [])
    view = view or _pattern_event_view(event)

    for key, rule in _rules.items():
        if rule.get("EventBusName", "default") != bus_name:
            continue
        if key in event_path:
            logger.warning("EventBridge: recursive rule dispatch skipped for %s", key)
            continue
        if rule.get("State") != "ENABLED":
            continue
        if not rule.get("EventPattern"):
            continue

        if _matches_pattern_view(rule["EventPattern"], view, f"rule {key}"):
            rule_targets = _targets.get(key, [])
            for target in rule_targets:
                _invoke_target(target, event, rule, view)


def _reject_json_constant(literal: str):
    """``json.loads(parse_constant=...)`` hook. Raising here is what turns
    ``NaN``/``Infinity``/``-Infinity`` — extensions Python's decoder allows and
    JSON does not define — back into the ``Invalid JSON`` refusal AWS gives."""
    raise ValueError(f"Invalid JSON literal: {literal}")


def _parse_pattern_text(pattern_str: str):
    """``(alternatives, None)`` for a pattern AWS accepts, ``(None, reason)`` for
    one it refuses. One parse answers both the validator's question and the
    matcher's, so the two cannot come to disagree on what counts as a refusal.
    See ``_reject_json_constant`` for the one place this is stricter than
    Python's decoder.

    ``ValueError``, not ``JSONDecodeError``: an integer literal of more digits
    than Python will convert raises the plain parent class. A ``RecursionError``
    is the same refusal, and it happens before any depth bound of ours applies.

    ``parse_constant`` refuses ``NaN``/``Infinity``/``-Infinity``, which Python's
    decoder accepts by default and JSON does not define. AWS parses a pattern
    with Jackson, which rejects them unless it is asked not to, so accepting one
    here would take a pattern that is a `400` in production and leave it matching
    nothing locally — the silence this whole entry exists to remove."""
    if isinstance(pattern_str, str) and not pattern_str.strip():
        return None, "Filter is not an object"
    try:
        pattern = json.loads(pattern_str, parse_constant=_reject_json_constant)
    except (ValueError, TypeError, RecursionError):
        return None, "Invalid JSON"
    try:
        return _compile_pattern(pattern), None
    except _InvalidPattern as exc:
        return None, str(exc)
    except RecursionError:
        return None, "Event pattern is nested too deeply"


@functools.lru_cache(maxsize=1024)
def _compiled_pattern(pattern_str: str):
    """The alternatives a pattern string compiles to, or ``None`` when AWS would
    refuse it. Cached because rule patterns are stable while events are not, and
    this runs per rule per event on the dispatch path. The returned lists are
    shared with every later caller: read them, never mutate them.

    A refusal is cached rather than raised. A pattern only reaches here from a
    rule restored from persisted state, which is loaded verbatim and never
    revalidated — and one AWS would have refused matches nothing. So does a
    pattern whose parse exhausted the stack."""
    return _parse_pattern_text(pattern_str)[0]


def _matches_pattern(pattern_str, event):
    """Whether ``event`` matches ``pattern_str`` — the single-shot form; dispatch
    builds the view once and calls ``_matches_pattern_view`` directly."""
    return _matches_pattern_view(pattern_str, _pattern_event_view(event))


def _matches_pattern_view(pattern_str, view, owner="rule"):
    """Whether ``view`` matches ``pattern_str``, the pattern as the JSON text the
    API takes it in and the rule and archive stores keep across a restart.

    A stored pattern that is not text belongs to a record this build's validation
    never saw; it matches nothing, and is logged rather than dropped in silence.
    ``owner`` names the record in that log line, so the operator can go and fix
    the one that is skipping. Handing the pattern to the memoized compiler would
    be worse than skipping it: ``lru_cache`` answers an unhashable key with
    ``TypeError``, and dispatch runs inside ``PutEvents``, so that is a 500 on the
    whole batch instead of one skipped rule."""
    if not isinstance(pattern_str, str):
        logger.warning("EventBridge: %s has a pattern that is not JSON text (%s); skipped",
                       owner, type(pattern_str).__name__)
        return False
    alternatives = _compiled_pattern(pattern_str)
    if alternatives is None:
        return False
    return any(_matches_alternative(view, alternative) for alternative in alternatives)


def _matches_alternative(view, alternative) -> bool:
    """One compiled alternative against one event, envelope keys before ``detail``,
    so a rule that fails on ``source`` never decodes the payload. The split is
    exact because a pattern's top-level keys are independent: ``_matches_detail``
    ANDs them against the same tree, so asking in two batches asks the same
    question."""
    if "detail" not in alternative:
        return _matches_detail(view.envelope(), alternative)
    if len(alternative) == 1:
        return _matches_detail(view.event_with_detail(), alternative)
    envelope = {key: value for key, value in alternative.items() if key != "detail"}
    return (_matches_detail(view.envelope(), envelope) and
            _matches_detail(view.event_with_detail(), {"detail": alternative["detail"]}))


def _decoded_detail(raw):
    """An event's ``Detail`` as a JSON value. One that is not JSON is the leaf it
    is: an empty object would answer a question about a field the event does not
    have. Shared by the matcher's view and the delivered envelope, so the two
    agree on one reading."""
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError, RecursionError):
            return raw
    return raw


class _EventView:
    """The event a pattern is matched against and a target is delivered, with
    ``detail`` decoded only once, and only for the alternatives that ask about it.

    One object serves both because they are the same tree: a rule must not match
    on a field whose delivered value differs, and the surest way to guarantee
    that is to have nothing to keep in step.

    The deferral costs nothing because ``_matches_detail`` reads a key only when
    the pattern names it — and AWS prunes the same way: Event Ruler skips a field
    no rule uses rather than parsing into it, so a field nothing asks about is
    absent from the tree a pattern sees, never present as a null.

    Not a ``dict`` subclass holding a placeholder until first read: CPython reads
    a subclass's own storage for ``dict(view)``, ``{**view}`` and ``items()``, so
    the placeholder would reach consumers as a ``detail`` of ``null`` — a leaf a
    pattern can match on."""

    __slots__ = ("_envelope", "_raw_detail", "_with_detail")

    def __init__(self, envelope, raw_detail):
        self._envelope = envelope
        self._raw_detail = raw_detail
        self._with_detail = None

    def envelope(self) -> dict:
        """The event without ``detail`` — what a pattern naming no ``detail`` is
        matched against, and what ``<aws.events.event>`` renders."""
        return self._envelope

    def event_with_detail(self) -> dict:
        """The whole event: the tree a target is delivered, and the one an
        alternative naming ``detail`` is matched against."""
        if self._with_detail is None:
            self._with_detail = dict(self._envelope, detail=_decoded_detail(self._raw_detail))
        return self._with_detail


def _pattern_event_view(event):
    """The event as a pattern sees it, which is exactly the envelope a target is
    delivered — one builder, because two hand-kept copies drift.

    A pattern is matched against one whole tree rather than field by field
    because that is what AWS does — it compiles a pattern to a set of *paths* and
    has no notion of which top-level names are legal. Two consequences fall out
    of that and out of nothing else: an unrecognized top-level key is simply a
    path the event does not have, so ``{"nonesuch": ["x"]}`` matches nothing while
    ``{"nonesuch": [{"exists": false}]}`` matches *everything*; and an envelope
    field may be written as an object path (``{"source": {"x": ["y"]}}`` is the
    path ``source.x``) without that being an error."""
    fields = {
        "version": str(event.get("Version", "0")),
        "id": event.get("EventId", ""),
        "source": event.get("Source", ""),
        "account": event.get("Account", get_account_id()),
        "time": _event_time_string(event),
        "region": event.get("Region", get_region()),
        "resources": event.get("Resources", []),
        "detail-type": event.get("DetailType", ""),
    }
    if event.get("ReplayName") is not None:
        # Present only on a replayed event, which is what makes AWS's own
        # ``{"replay-name": [{"exists": false}]}`` mean "live traffic only".
        fields["replay-name"] = event["ReplayName"]
    extra = event.get("_ExtraEnvelope")
    if extra:
        # Unmodelled top-level keys, which only ``TestEventPattern`` can supply.
        # Written under the modelled names so a caller cannot shadow one.
        fields = dict(extra, **fields)
    return _EventView(fields, event.get("Detail", "{}"))

# Field names AWS reserves inside a ``$or`` branch: a ``$or`` holding any of them
# is not the OR operator. Wider than SNS's equivalent — it also reserves the bare
# comparison symbols and the names of operators AWS has not shipped yet, so
# adding one later cannot change what an existing pattern means.
_OR_RESERVED_MEMBER_KEYS = frozenset({
    "anything-but", "prefix", "suffix", "equals-ignore-case", "numeric",
    "exists", "cidr", "wildcard", "exactly", "=", "<", "<=", ">", ">=",
    "regex", "not-wildcard", "not-equals-ignore-case", "date-after",
    "date-on-or-after", "date-before", "date-on-or-before", "in-date-range",
    "ip-address-in-range", "ip-address-not-in-range",
})


class _InvalidPattern(Exception):
    """An event pattern AWS refuses at rule creation.

    The message is the one EventBridge reports, which is the underlying
    Event Ruler text verbatim — a real service response reads
    ``Event pattern is not valid. Reason: exists match pattern must be either
    true or false.`` — so the strings here are deliberately AWS's wording
    rather than our own."""


class _PatternTooComplex(_InvalidPattern):
    """A limit was reached rather than the pattern being malformed: the ``$or``
    combination cap, or the nesting guard. Kept distinct because neither is
    retried against the fallback compiler — re-reading ``$or`` as a field name
    cannot answer a product over the ``$or`` arrays, and for a depth failure it
    re-descends the same hundred levels the guard exists to bound."""


# AWS's cap on the sub-patterns a ``$or`` may expand to. Its own wording is
# "over 1000 rule combinations".
_MAX_OR_COMBINATIONS = 1000
# A guard on pattern nesting, not an AWS rule — AWS documents no depth limit.
# Compilation and matching both recurse per level, on a pooled worker thread
# whose remaining stack is nothing like the interpreter's nominal limit, and a
# RecursionError there does not stay inside its own request. Set above any real
# pattern and below where the handler's own JSON parse gives out.
_MAX_PATTERN_DEPTH = 100


# ---------------------------------------------------------------------------
# Event pattern compilation — $or expansion into flat alternatives
# ---------------------------------------------------------------------------


def _compile_pattern(pattern):
    """Expand an event pattern into the list of flat alternatives AWS compiles
    it to, raising ``_InvalidPattern`` for anything it would reject.

    ``$or`` is not evaluated at match time on AWS — it is expanded here, into
    one alternative per branch, and the rule matches if *any* alternative does.
    That is not merely a different route to the same answer. Because each branch
    is built by writing leaves into the alternatives accumulated so far, a leaf
    constrained both inside and outside a ``$or`` keeps only the LAST value
    written, in document order. So ``{"a":["1"],"$or":[{"a":["2"]},{"b":["3"]}]}``
    matches ``{"a":"2"}`` and does not match ``{"a":"1"}`` — and writing the two
    keys the other way round reverses both answers. A recursive evaluator
    answers order-independently and gets those cases wrong.

    A ``$or`` that is not the operator falls back to being read as an ordinary
    field name, which is what EventBridge does: it compiles with its ``$or``-aware
    compiler and on failure retries with its pre-``$or`` one."""
    # Outside the retry deliberately: AWS counts the product over the ``$or``
    # arrays the pattern text holds, so a ``$or`` that turns out not to be the
    # operator still counts toward the cap.
    product = _or_combination_product(pattern)
    if product > _MAX_OR_COMBINATIONS:
        raise _PatternTooComplex(
            f"Event pattern contains more than {_MAX_OR_COMBINATIONS} rule combinations")
    try:
        return _compile_alternatives(pattern, expand_or=True)
    except _PatternTooComplex:
        raise
    except _InvalidPattern:
        return _compile_alternatives(pattern, expand_or=False)


def _or_combination_product(node, depth: int = 0):
    """AWS's documented rule-combination count: the product of the lengths of
    every ``$or`` array. It over-counts what the expansion actually produces for
    a nested ``$or``, deliberately — counting only the alternatives would accept
    a pattern AWS refuses, so both are checked."""
    if not isinstance(node, dict) or depth > _MAX_PATTERN_DEPTH:
        return 1
    product = 1
    for key, value in node.items():
        if key == "$or" and isinstance(value, list):
            product *= max(len(value), 1)
            for member in value:
                product *= _or_combination_product(member, depth + 1)
        elif isinstance(value, dict):
            product *= _or_combination_product(value, depth + 1)
        if product > _MAX_OR_COMBINATIONS:
            return product
    return product


def _compile_alternatives(pattern, expand_or: bool):
    alternatives = []
    _compile_object(alternatives, [], pattern, expand_or, inside_or=False, depth=0)
    return alternatives or [{}]


def _copy_alternative(alternative):
    """A copy deep enough to fork on. The value lists are never mutated — a
    leaf write replaces one wholesale — so only the object spine is copied."""
    return {key: _copy_alternative(value) if isinstance(value, dict) else value
            for key, value in alternative.items()}


def _compile_object(alternatives, path, obj, expand_or, inside_or, depth):
    if depth > _MAX_PATTERN_DEPTH:
        raise _PatternTooComplex("Event pattern is nested too deeply")
    if not isinstance(obj, dict):
        raise _InvalidPattern("Filter is not an object")
    if not obj:
        raise _InvalidPattern("Empty objects are not allowed")
    # Document order decides which of two writes to the same leaf survives, so
    # the keys are walked in the order the JSON carried them and never sorted.
    for key, value in obj.items():
        if key == "$or" and expand_or:
            _compile_or(alternatives, path, value, depth)
            continue
        if inside_or and key in _OR_RESERVED_MEMBER_KEYS:
            raise _InvalidPattern(
                f"{key} is Ruler reserved fieldName which cannot be used inside $or.")
        # Event Ruler joins keys with ".": a dotted key is the nested path.
        segments = key.split(".") if "." in key else (key,)
        if isinstance(value, dict):
            _compile_object(alternatives, [*path, *segments], value, expand_or, inside_or,
                            depth + 1)
        elif isinstance(value, list):
            _validate_value_list(value)
            _write_leaf(alternatives, [*path, *segments], value)
        else:
            raise _InvalidPattern(f'"{key}" must be an object or an array')


def _compile_or(alternatives, path, members, depth):
    prefix = [_copy_alternative(a) for a in alternatives]
    alternatives.clear()
    if not isinstance(members, list):
        raise _InvalidPattern("It must be an Array followed with $or.")
    for member in members:
        if not isinstance(member, dict):
            raise _InvalidPattern("Only JSON object is allowed in array of $or relationship.")
        forked = [_copy_alternative(a) for a in prefix] or [{}]
        _compile_object(forked, path, member, True, True, depth + 1)
        alternatives.extend(forked)
        if len(alternatives) > _MAX_OR_COMBINATIONS:
            raise _PatternTooComplex(
                f"Event pattern contains more than {_MAX_OR_COMBINATIONS} rule combinations")
    if len(members) < 2:
        raise _InvalidPattern("There must have at least 2 Objects in $or relationship.")


# Marks an alternative no event can satisfy. AWS keys its sub-rules by the full
# dotted path, so a path constrained both as a leaf and as an object keeps BOTH
# constraints and matches nothing. Alternatives here are trees, which cannot
# hold the two at once, so the collision is recorded rather than one side
# dropped; an ``object()`` key cannot collide with a pattern key.
_UNSATISFIABLE = object()


def _write_leaf(alternatives, path, values):
    """Record one leaf constraint in every alternative built so far. A leaf
    already constrained at this path is overwritten, which is where
    last-write-wins comes from — but only when both writes are leaves."""
    if not alternatives:
        alternatives.append({})
    for alternative in alternatives:
        node = alternative
        for part in path[:-1]:
            child = node.get(part)
            if child is None:
                child = {}
                node[part] = child
            elif not isinstance(child, dict):
                # An ancestor is already a leaf; see ``_UNSATISFIABLE``.
                alternative[_UNSATISFIABLE] = True
                break
            node = child
        else:
            if isinstance(node.get(path[-1]), dict):
                alternative[_UNSATISFIABLE] = True
            else:
                node[path[-1]] = values


# The operators ``anything-but`` may negate, and the ones a ``prefix``/``suffix``
# operand may itself be written as.
_ANYTHING_BUT_NESTABLE = frozenset({"prefix", "suffix", "equals-ignore-case", "wildcard"})
_AFFIX_NESTABLE = frozenset({"equals-ignore-case"})
# The numeric comparison spelled out, for the operand error messages.
_NUMERIC_OP_NAMES = {"=": "equals", ">": ">", ">=": ">=", "<": "<", "<=": "<="}
# A range's lower bound may be followed by an upper bound; every other operator
# terminates the expression.
_NUMERIC_RANGE_OPENERS = frozenset({">", ">="})
_NUMERIC_RANGE_CLOSERS = frozenset({"<", "<="})


def _require_type(operand, kind, message: str):
    """The whole of what several operators ask of their operand: that it is one
    JSON type, refused with AWS's own wording for that operator when it is not."""
    if not isinstance(operand, kind):
        raise _InvalidPattern(message)


# ---------------------------------------------------------------------------
# Event pattern validation — AWS's operand grammar, and its wording
# ---------------------------------------------------------------------------


def _validate_value_list(values: list):
    """Validate one field's list of alternatives. AWS rejects the whole pattern
    for any of these, so this raises rather than returning a verdict."""
    if not values:
        raise _InvalidPattern("Empty arrays are not allowed")
    for item in values:
        if isinstance(item, dict):
            _validate_match_expression(item)
        elif isinstance(item, list):
            raise _InvalidPattern("Match value must be String, number, true, false, or null")


def _validate_match_expression(expression: dict):
    if not expression:
        raise _InvalidPattern("Match expression name not found")
    operator_name = next(iter(expression))
    if operator_name not in _MATCH_OPERATORS:
        raise _InvalidPattern(f"Unrecognized match type {operator_name}")
    # Operand before arity: AWS's parser dispatches on the first key and consumes
    # its operand before it sees a second, so a two-key expression whose first
    # operand is also wrong reports the operand, not the arity.
    _OPERAND_VALIDATORS[operator_name](expression[operator_name])
    if len(expression) > 1:
        # These two reach a different guard on AWS, which reports the arity of
        # the operand it was already reading.
        if operator_name in ("numeric", "anything-but"):
            raise _InvalidPattern("Too many elements in numeric expression")
        raise _InvalidPattern("Only one key allowed in match expression")


def _validate_affix(operator_name: str, operand):
    """``prefix``/``suffix`` take a string, or a nested ``equals-ignore-case``
    for a case-insensitive affix.

    An empty operand is accepted in both forms and is not the do-nothing rule it
    looks like: AWS matches an affix against the *quoted* form of the value, so
    an empty ``prefix`` compiles to the opening quote alone — which every JSON
    string carries and no number, boolean or null does. It reads "this field has
    a string value", and ``_matches_affix`` answers it that way.
    ``Null prefix/suffix not allowed`` is AWS's rule for ``anything-but``'s
    nested affix alone, so refusing an empty operand here would reject a pattern
    real AWS compiles.

    The type complaint names the one operator it read where the nested form says
    ``prefix/suffix``; both are AWS's own wording."""
    if isinstance(operand, dict):
        if not operand:
            raise _InvalidPattern(f"{operator_name.capitalize()} expression name not found")
        inner = next(iter(operand))
        if len(operand) > 1:
            raise _InvalidPattern("Only one key allowed in match expression")
        if inner not in _AFFIX_NESTABLE:
            raise _InvalidPattern(f"Unsupported {operator_name} pattern: {inner}")
        _require_type(operand[inner], str, "equals-ignore-case match pattern must be a string")
        return
    _require_type(operand, str, f"{operator_name} match pattern must be a string")


def _wildcard_position(pattern: str, index: int) -> int:
    """AWS reports a wildcard fault as a 1-based offset into the *quoted* form of
    the operand, counted in UTF-8 bytes — so the opening quote is position 0 and
    a multi-byte character before the fault shifts it by its encoded length.

    ``errors="replace"`` is load-bearing: a lone surrogate out of a JSON escape
    cannot be UTF-8 encoded at all, so encoding strictly raises — and this is
    reached from the matcher as well as the validator. It is also what AWS
    counts, its encoder substituting one ``?`` for an unpaired surrogate."""
    return len(pattern[:index].encode("utf-8", errors="replace")) + 1


def _validate_wildcard(operand):
    _require_type(operand, str, "wildcard match pattern must be a string")
    index = 0
    while index < len(operand):
        char = operand[index]
        if char == "\\":
            # A trailing backslash escapes the closing quote AWS appends, which
            # is why it is an invalid escape rather than a dangling one.
            if index + 1 >= len(operand) or operand[index + 1] not in ("*", "\\"):
                raise _InvalidPattern(
                    f"Invalid escape character at pos {_wildcard_position(operand, index)}")
            index += 2
            continue
        if char == "*" and index + 1 < len(operand) and operand[index + 1] == "*":
            raise _InvalidPattern(
                f"Consecutive wildcard characters at pos {_wildcard_position(operand, index)}")
        index += 1


# A pre-filter for ``ipaddress``, on both the operand and the value side: its
# job is to reject what ``ipaddress`` accepts and AWS does not — a zone id, a
# dotted IPv4-mapped IPv6 address, anything not plainly one family. The IPv6
# form is one character class plus a separate colon test, not the two
# overlapping classes the shape suggests: those go quadratic on a long
# colon-only string, on the dispatch path ``_wildcard_to_regex`` protects.
_CIDR_IPV4_RE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\Z")
_CIDR_IPV6_RE = re.compile(r"[0-9a-fA-F:]+\Z")
# What AWS's integer parse of the mask accepts, which is not what Python's
# ``int()`` accepts: no surrounding space, no underscore separators, no
# non-ASCII digits, but a leading sign is fine (``/+24`` and ``/-0`` are real).
_CIDR_MASK_RE = re.compile(r"[+-]?[0-9]+\Z")


def _validate_cidr(operand):
    # The wrong operator in this message is AWS's own copy-paste, kept so the
    # text a caller sees is the text the service sends.
    _require_type(operand, str, "prefix match pattern must be a string")
    address, separator, mask = operand.partition("/")
    # A trailing slash leaves no mask field at all for AWS's split, so it is the
    # missing-slash complaint rather than a bad-integer one.
    if not separator or "/" in mask or not mask:
        raise _InvalidPattern("Malformed CIDR, one '/' required")
    if not _CIDR_MASK_RE.match(mask):
        raise _InvalidPattern("Malformed CIDR, mask bits must be an integer")
    try:
        bits = int(mask)
    except ValueError:
        raise _InvalidPattern("Malformed CIDR, mask bits must be an integer") from None
    if not -2 ** 31 <= bits < 2 ** 31:
        # Outside the width AWS's integer parse accepts, so it never reaches the
        # mask-width check.
        raise _InvalidPattern("Malformed CIDR, mask bits must be an integer")
    if bits < 0:
        raise _InvalidPattern("Malformed CIDR, mask bits must not be negative")
    looks_v4 = _CIDR_IPV4_RE.match(address) is not None
    looks_v6 = ":" in address and _CIDR_IPV6_RE.match(address) is not None
    if not (looks_v4 or looks_v6):
        raise _InvalidPattern(f"Nonstandard IP address: {address}")
    try:
        parsed = ipaddress.ip_address(_normalized_ipv4(address) if looks_v4 else address)
    except ValueError:
        raise _InvalidPattern(f"Invalid IP address: {address}") from None
    if parsed.version == 4 and bits > 31:
        raise _InvalidPattern("IPv4 mask bits must be < 32")
    if parsed.version == 6 and bits > 127:
        raise _InvalidPattern("IPv6 mask bits must be < 128")


def _normalized_ipv4(address: str) -> str:
    """A dotted quad with its octets read as decimal, which is how AWS reads
    them: ``0177.0.0.1`` is ``177.0.0.1``, not octal. Python's parser refuses a
    leading zero outright, so an operand AWS accepts would otherwise be refused."""
    octets = []
    for octet in address.split("."):
        try:
            value = int(octet)
        except ValueError:
            # Only an octet of more digits than Python will convert; not an
            # address either way.
            return address
        if not 0 <= value <= 255:
            return address
        octets.append(str(value))
    return ".".join(octets)


def _numeric_token(value) -> str:
    """How AWS names an offending token in a numeric operand — the raw JSON
    text, which is what its streaming parser has to hand."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, list):
        return "["
    if isinstance(value, dict):
        return "{"
    return str(value)


def _validate_numeric_threshold(operand_name: str, value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _InvalidPattern(f"Value of {operand_name} must be numeric")
    try:
        as_double = float(value)
    except (OverflowError, ValueError):
        raise _InvalidPattern(f"Cannot compare number : {value}") from None
    if as_double in (float("inf"), float("-inf")) or as_double != as_double:
        raise _InvalidPattern(f"Cannot compare number : {value}")
    if isinstance(value, int) and int(as_double) != value:
        # AWS refuses a threshold the double conversion would silently move.
        # Only checkable for an integer: a fractional literal was already
        # rounded by the JSON parse, so its original text is gone.
        raise _InvalidPattern(f"Cannot compare number : {value}")
    return as_double


def _validate_numeric(operand):
    """``numeric`` is a small grammar, not a list of pairs: a single comparison,
    or a lower bound followed by an upper bound. ``=``, ``<`` and ``<=``
    terminate the expression, so a range has to be written lower bound first."""
    _require_type(operand, list, "Value of numeric must be an array.")
    if not operand:
        raise _InvalidPattern("Invalid member in numeric match: ]")
    first = operand[0]
    _require_type(first, str, f"Invalid member in numeric match: {_numeric_token(first)}")
    if first not in _NUMERIC_OP_NAMES:
        raise _InvalidPattern(f"Unrecognized numeric range operator: {first}")
    if len(operand) < 2:
        raise _InvalidPattern(f"Value of {_NUMERIC_OP_NAMES[first]} must be numeric")
    bottom = _validate_numeric_threshold(_NUMERIC_OP_NAMES[first], operand[1])
    if len(operand) == 2:
        return
    if first not in _NUMERIC_RANGE_OPENERS:
        raise _InvalidPattern("Too many elements in numeric expression")
    if len(operand) > 4:
        raise _InvalidPattern("Too many terms in numeric range expression")
    second = operand[2]
    _require_type(second, str, f"Bad value in numeric range: {_numeric_token(second)}")
    if second not in _NUMERIC_RANGE_CLOSERS:
        raise _InvalidPattern(f"Bad numeric range operator: {second}")
    if len(operand) < 4:
        raise _InvalidPattern(f"Value of {_NUMERIC_OP_NAMES[second]} must be numeric")
    top = _validate_numeric_threshold(_NUMERIC_OP_NAMES[second], operand[3])
    if not bottom < top:
        raise _InvalidPattern("Bottom must be less than top")


def _validate_anything_but(operand):
    if isinstance(operand, dict):
        if not operand:
            raise _InvalidPattern("Anything-But expression name not found")
        inner = next(iter(operand))
        if len(operand) > 1:
            raise _InvalidPattern("Only one key allowed in match expression")
        if inner not in _ANYTHING_BUT_NESTABLE:
            raise _InvalidPattern(f"Unsupported anything-but pattern: {inner}")
        _validate_anything_but_nested(inner, operand[inner])
        return
    if isinstance(operand, list):
        # The list is walked in order, as AWS's streaming parser walks it, so an
        # unsupported element is reported before the type-homogeneity check.
        for value in operand:
            if isinstance(value, bool) or value is None or isinstance(value, (list, dict)):
                raise _InvalidPattern(
                    "Inside anything but list, start|null|boolean is not supported.")
        has_number = any(isinstance(v, (int, float)) for v in operand)
        has_string = any(isinstance(v, str) for v in operand)
        if not operand or (has_number and has_string):
            raise _InvalidPattern(
                "Inside anything but list, either all values are number or string, "
                "mixed type is not supported")
        for value in operand:
            if isinstance(value, (int, float)):
                _validate_numeric_threshold("anything-but", value)
        return
    if isinstance(operand, bool) or operand is None:
        raise _InvalidPattern(
            "Value of anything-but must be an array or single string/number value.")
    if isinstance(operand, (int, float)):
        _validate_numeric_threshold("anything-but", operand)


def _validate_anything_but_nested(inner: str, operand):
    """The nested form takes one value or a list of them. AWS's outer type guard
    fires first, so a boolean or null operand is reported against
    ``anything-but`` rather than against the nested operator."""
    candidates = operand if isinstance(operand, list) else [operand]
    if not candidates:
        # A deliberate divergence: AWS accepts an empty nested list and then
        # fails evaluating the rule, so there is no behaviour to copy — and an
        # ``anything-but`` with nothing to exclude inverts into "every event".
        raise _InvalidPattern("Empty arrays are not allowed")
    for candidate in candidates:
        if isinstance(candidate, bool) or candidate is None:
            raise _InvalidPattern(
                "Value of anything-but must be an array or single string/number value.")
    if inner in ("prefix", "suffix"):
        for candidate in candidates:
            _require_type(candidate, str, "prefix/suffix match pattern must be a string")
            if not candidate:
                # This position only — ``{"prefix": ""}`` is a pattern AWS
                # compiles, so do not hoist this into ``_validate_affix``.
                raise _InvalidPattern("Null prefix/suffix not allowed")
    elif inner == "equals-ignore-case":
        for candidate in candidates:
            _require_type(candidate, str, "Inside anything-but/equals-ignore-case list, "
                                          "number|start|null|boolean is not supported.")
    elif inner == "wildcard":
        for candidate in candidates:
            _validate_wildcard(candidate)


# Every operator a match expression may name, with the check its operand has to
# pass. ``exactly`` is Event Ruler's explicit spelling of a plain equality value;
# the rest are the documented ones.
_OPERAND_VALIDATORS = {
    "exactly": functools.partial(
        _require_type, kind=str, message="exact match pattern must be a string"),
    "prefix": functools.partial(_validate_affix, "prefix"),
    "suffix": functools.partial(_validate_affix, "suffix"),
    "equals-ignore-case": functools.partial(
        _require_type, kind=str, message="equals-ignore-case match pattern must be a string"),
    "wildcard": _validate_wildcard,
    "cidr": _validate_cidr,
    "numeric": _validate_numeric,
    "exists": functools.partial(
        _require_type, kind=bool, message="exists match pattern must be either true or false."),
    "anything-but": _validate_anything_but,
}
_MATCH_OPERATORS = frozenset(_OPERAND_VALIDATORS)


# How deep an event value's arrays are followed when flattening. Generous next to
# any real event, and small next to the stack a handler has left.
_MAX_VALUE_DEPTH = 50


# ---------------------------------------------------------------------------
# Event pattern matching — value-level operators
# ---------------------------------------------------------------------------


def _flatten(value, objects: bool, depth: int = 0) -> list:
    """What a pattern is matched against for this event value: the scalars it
    offers, or — one level of the tree up, for a nested pattern object — the
    objects. AWS flattens an array value and offers each element to the matcher
    on its own, recursively, so the field matches when *any* element does.

    Whatever is not the wanted leaf yields nothing and so matches nothing: an
    object has no scalar to offer, a scalar has no object, and an empty array
    has neither — including under ``anything-but``, which needs at least one
    surviving element rather than the absence of a failing one.

    The depth bound is for the event side: an event may nest arrays as deep as
    its author likes, and past the bound the nesting yields nothing rather than
    exhausting the stack."""
    if isinstance(value, list):
        if depth >= _MAX_VALUE_DEPTH:
            return []
        flat = []
        for element in value:
            flat.extend(_flatten(element, objects, depth + 1))
        return flat
    return [value] if isinstance(value, dict) == objects else []


def _json_equal(left, right) -> bool:
    """JSON equality, which Python's ``==`` is not. AWS compares as JSON, so a
    string never equals a number and ``true`` never equals ``1`` — where Python
    holds ``True == 1``. Numbers of different width do still compare equal."""
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def _matches_alternatives(value, alternatives) -> bool:
    """Whether one scalar satisfies any alternative in a pattern's value list.
    ``exists`` is skipped: it is answered from the key's presence, not from the
    value, and its caller has already dealt with it."""
    for item in alternatives:
        if isinstance(item, dict):
            if "exists" in item:
                continue
            if _matches_content_filter(value, item):
                return True
        elif _json_equal(value, item):
            return True
    return False


def _matches_detail(detail, pattern):
    """Match one ``$or``-free alternative against an object. Every ``$or`` is
    gone by the time this runs — ``_compile_pattern`` expanded it — so this is a
    plain AND over the pattern's keys."""
    if isinstance(pattern, dict) and _UNSATISFIABLE in pattern:
        return False
    if not isinstance(pattern, dict) or not pattern:
        # Both are refused at rule creation; an empty pattern object constrains
        # nothing, so matching it would be the fail-open answer.
        return False
    for key, expected in pattern.items():
        present = isinstance(detail, dict) and key in detail
        actual = detail.get(key) if isinstance(detail, dict) else None
        if isinstance(expected, list):
            # ``exists`` asks whether the path reaches a LEAF, not whether the
            # key is there: AWS's operators "only work on leaf nodes", so an
            # object-valued field, an empty array and an empty object are all
            # indistinguishable from a missing key, while a null exists — it is
            # a leaf. ``_flatten`` answers exactly that question. Evaluated
            # before the value filters, because an absent key must not
            # short-circuit past it.
            scalars = _flatten(actual, False) if present else []
            has_leaf = bool(scalars)
            exists_filters = [item for item in expected
                              if isinstance(item, dict) and "exists" in item]
            if exists_filters:
                # ``is``, not ``==``: a non-bool operand — reachable only from
                # unrevalidated state — settles nothing and must not be coerced.
                if any(item["exists"] is has_leaf for item in exists_filters):
                    continue
                if len(exists_filters) == len(expected):
                    return False
            if not present:
                return False
            # Every value shape must be judged: an unjudged key is a matched
            # key, and a null or object detail used to reach no arm at all, so
            # a rule guarding on an operator fired on what it excluded.
            if not any(_matches_alternatives(scalar, expected)
                       for scalar in scalars):
                return False
        elif isinstance(expected, dict):
            # Two pattern fields must come from the SAME array element: each
            # element is offered the whole nested pattern.
            #
            # A non-object value still gets the pattern evaluated, against
            # nothing — so ``{"detail": {"state": [{"exists": false}]}}`` is
            # satisfied by an event whose ``detail`` is a string, as on AWS.
            candidates = _flatten(actual, True) or [{}]
            if not any(_matches_detail(candidate, expected)
                       for candidate in candidates):
                return False
        else:
            # A pattern value must be an array of alternatives or a nested
            # object; a bare scalar is neither, and leaving it unjudged is how a
            # ``$or`` read as an ordinary field could match every event.
            return False
    return True


def _wildcard_to_regex(pattern: str) -> str:
    """Translate an AWS EventBridge wildcard pattern to an anchored regex. Only
    ``*`` is special (any sequence); ``\\`` escapes the next character, so
    ``\\*`` is a literal asterisk and ``\\\\`` a literal backslash. Unlike
    fnmatch, ``?`` and ``[seq]`` are literal — matching AWS exactly.

    The literal run after each interior ``*`` is emitted as fnmatch's
    ``(?=(?P<gN>.*?lit))(?P=gN)`` rather than a plain ``.*lit``. The lookahead
    plus backreference commits to the leftmost match — glob semantics anyway —
    and stops the engine backtracking. A naive ``.*a.*a.*b`` chain is
    exponential against a long non-matching value: ``*/*/*/*/*.json`` over a
    70-character key that just misses takes seconds, per rule per event.

    Anchoring is ``^``/``\\Z``, not ``$``, which would also match just before a
    trailing newline."""
    # One token is an escape pair, a bare ``*``, or a run of ordinary characters.
    # A trailing lone backslash matches nothing after it, so ``\\[\s\S]?`` leaves
    # it as the escape of an empty string — the same literal backslash the
    # character walk produced.
    segments = [""]
    for token in re.findall(r"\\[\s\S]?|\*|[^\\*]+", pattern):
        if token == "*":
            segments.append("")
        elif token[0] == "\\":
            segments[-1] += re.escape(token[1:]) if len(token) > 1 else "\\\\"
        else:
            segments[-1] += re.escape(token)

    out = ["^", segments[0]]
    interior = [s for s in segments[1:-1] if s]
    for n, seg in enumerate(interior):
        out.append(f"(?=(?P<g{n}>.*?{seg}))(?P=g{n})")
    if len(segments) > 1:
        out.append(".*")
        out.append(segments[-1])
    out.append(r"\Z")
    return "".join(out)


@functools.lru_cache(maxsize=1024)
def _wildcard_regex(pattern: str):
    """Compiled form of ``pattern``, cached like ``_compiled_pattern``."""
    return re.compile(_wildcard_to_regex(pattern), re.DOTALL)


def _string_operands(operand):
    """The pattern strings an operand denotes — one, or a list of them, any of
    which may match — or ``None`` when it is malformed. A malformed operand
    never matches rather than raising: the matcher runs inside ``PutEvents``, so
    a raise is a 500 that fails the whole batch, not one skipped rule. An empty
    list is malformed for this purpose — it has no pattern to match, and
    answering true would make ``anything-but`` invert it into a rule that fires
    on every event. The list form is accepted at any depth, where AWS accepts it
    only under ``anything-but``."""
    candidates = operand if isinstance(operand, list) else [operand]
    if not candidates or not all(isinstance(c, str) for c in candidates):
        return None
    return candidates


def _matches_wildcard(value, pattern) -> bool:
    """Match ``value`` against an AWS wildcard operand."""
    if not isinstance(value, str):
        return False
    patterns = _string_operands(pattern)
    return patterns is not None and any(_wildcard_regex(p).match(value) is not None for p in patterns)


def _ignore_case_alternatives(char: str) -> tuple:
    """The forms AWS accepts at one operand position: the character's lower
    and upper case, deduplicated. Each is taken on the character alone, so a
    mapping that changes length counts — ``ß`` uppercases to ``SS``."""
    lower, upper = char.lower(), char.upper()
    return (lower,) if lower == upper else (lower, upper)


def _ignore_case_reach(value: str, operand: str, backwards: bool = False) -> set:
    """The offsets in ``value`` the operand's alternation can reach — consuming
    forwards from the start, or backwards from the end when ``backwards``.

    Three questions are the one walk with different anchoring: ``prefix`` asks
    whether the forward walk reaches anywhere, ``suffix`` whether the backward
    one does, and ``equals-ignore-case`` whether the forward one reaches the end.

    Carrying the whole reachable set is what keeps it linear: the alternatives at
    a position differ in length (``ß`` uppercases to ``SS``), so a greedy scan
    gives the wrong answer and a regex alternation reintroduces the backtracking
    ``_wildcard_to_regex`` goes out of its way to avoid. The backward walk exists
    for the same reason — running the forward one from every start offset would
    be quadratic in the two lengths, on the same dispatch path."""
    reachable = {len(value) if backwards else 0}
    for char in reversed(operand) if backwards else operand:
        moved = set()
        for alternative in _ignore_case_alternatives(char):
            width = len(alternative)
            for offset in reachable:
                start = offset - width if backwards else offset
                if start >= 0 and value.startswith(alternative, start):
                    moved.add(start if backwards else offset + width)
        if not moved:
            return set()
        reachable = moved
    return reachable


def _matches_affix(value, operand, at_start: bool) -> bool:
    """``prefix`` when ``at_start``, ``suffix`` otherwise. Only a string can
    match: AWS matches an affix against the quoted form of the value, so
    ``{"prefix": "5"}`` never reaches the number ``5`` (see ``_validate_affix``
    for what that makes an empty operand mean). A nested ``equals-ignore-case``
    is AWS's spelling of a case-insensitive affix."""
    if not isinstance(value, str):
        return False
    ignore_case = isinstance(operand, dict)
    if ignore_case:
        if len(operand) != 1 or "equals-ignore-case" not in operand:
            return False
        operand = operand["equals-ignore-case"]
    candidates = _string_operands(operand)
    if candidates is None:
        return False
    if ignore_case:
        return any(_ignore_case_reach(value, c, not at_start) for c in candidates)
    return any(value.startswith(c) if at_start else value.endswith(c) for c in candidates)


def _matches_equals_ignore_case(value, operand) -> bool:
    """Match an ``equals-ignore-case`` operand against the whole value.

    AWS accepts either case of each operand character independently, which is
    neither ``value.lower() == operand.lower()`` nor ``casefold`` — the two are
    wrong in opposite directions. ``lower()`` under-matches where the cases
    differ in length (it rejects ``straße`` against ``STRASSE``) and
    over-matches by making the relation symmetric, which AWS's is not: the
    operand ``ẞ`` matches the value ``ß``, the reverse does not.
    ``casefold()`` over-matches outright, accepting ``strasse`` for that same
    operand. So the alternation is walked character by character.

    Only a string value can match. One deliberate divergence: an operand
    character outside the Basic Multilingual Plane matches itself, where AWS
    mangles it to ``?`` and so fails to match the very value it was written
    for."""
    if not isinstance(value, str):
        return False
    operands = _string_operands(operand)
    # Anchored at both ends: the walk must consume the operand AND land exactly on
    # the end of the value, which is what makes this equality rather than a prefix.
    return operands is not None and any(
        len(value) in _ignore_case_reach(value, o) for o in operands)



def _cidr_address(text: str):
    """``text`` as an address, or ``None`` if it is not one AWS would read. The
    same admission test gates the operand and the value, since two sides spelled
    alike have to agree."""
    if _CIDR_IPV4_RE.match(text):
        text = _normalized_ipv4(text)
    elif not (":" in text and _CIDR_IPV6_RE.match(text)):
        return None
    try:
        return ipaddress.ip_address(text)
    except ValueError:
        return None


@functools.lru_cache(maxsize=1024)
def _cidr_network(operand: str):
    """The network a ``cidr`` operand denotes, or ``None`` where AWS rejects the
    operand at rule creation. Cached like ``_compiled_pattern``.

    The grammar is read from ``_validate_cidr`` rather than spelled a second
    time — the two drifting apart is a rule that fails creation and still
    matches, or the reverse. ``ipaddress`` cannot stand in for it, being both
    too permissive (a bare ``10.0.0.1`` read as a ``/32``, a dotted netmask, a
    zone id) and too strict (it raises on host bits set, which AWS silently
    floors, so ``10.0.0.5/24`` is the ``10.0.0.0/24`` block)."""
    try:
        _validate_cidr(operand)
    except _InvalidPattern:
        return None
    address, _, mask = operand.partition("/")
    if _CIDR_IPV4_RE.match(address):
        # Decimal octets, as AWS reads them; see ``_normalized_ipv4``.
        address = _normalized_ipv4(address)
    try:
        return ipaddress.ip_network(f"{address}/{int(mask)}", strict=False)
    except ValueError:
        # Unreachable through the validated grammar; a raising matcher fails
        # the whole batch.
        return None


def _matches_cidr(value, operand) -> bool:
    """Whether ``value`` is an address inside the ``cidr`` operand's block.

    Both sides must be strings. ``ipaddress`` reads an integer as an address —
    ``167772161`` is ``10.0.0.1``, and ``True`` is ``0.0.0.1`` — so an
    unguarded numeric detail value would match a block AWS never matches it
    against. Only one operand string is read: AWS takes one block per ``cidr``
    object, several being written as sibling objects in the field's value list.

    An address of the other family does not match. The version is compared
    before the containment test rather than leaning on ``in`` to answer it,
    since that behaviour is a property of the stdlib rather than of this
    matcher's contract."""
    if not isinstance(value, str) or not isinstance(operand, str):
        return False
    network = _cidr_network(operand)
    if network is None:
        return False
    address = _cidr_address(value)
    if address is None:
        return False
    return address.version == network.version and address in network


# ``numeric`` comparison operators, in the [op, threshold, ...] operand order.
_NUMERIC_OPS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "=": operator.eq,
}


def _numeric_conditions(ops):
    """Normalize a ``numeric`` operand into ``[(op, threshold), ...]``, or
    ``None`` when it is malformed. ``{"numeric": 5}`` and
    ``{"numeric": [">", "abc"]}`` both used to escape as a 500 InternalError.
    The operator is type-checked before the lookup because an unhashable one
    raises from the ``in`` test itself, and the threshold conversion catches
    OverflowError because that, not ValueError, is what ``float()`` raises for
    an integer literal too large for a double."""
    if not isinstance(ops, list) or not ops or len(ops) % 2:
        return None
    conditions = []
    for i in range(0, len(ops), 2):
        op = ops[i]
        if not isinstance(op, str) or op not in _NUMERIC_OPS:
            return None
        try:
            conditions.append((op, float(ops[i + 1])))
        except (TypeError, ValueError, OverflowError):
            return None
    return conditions


def _matches_numeric(value, operand) -> bool:
    conditions = _numeric_conditions(operand)
    if conditions is None:
        return False
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        # A JSON number and nothing else: the text "50" and ``True`` both
        # convert happily in Python, so without this the rule over-matched.
        return False
    try:
        num = float(value)
    except (ValueError, TypeError, OverflowError):
        # OverflowError: an integer too large for a double. Non-match, not 500.
        return False
    return all(_NUMERIC_OPS[op](num, threshold) for op, threshold in conditions)


def _matches_exactly(value, operand) -> bool:
    """``exactly``'s operand is a string and nothing else — AWS refuses any other
    at rule creation — so a stored ``{"exactly": 5}`` matches nothing rather than
    standing in for the numeric equality it was not written as."""
    return isinstance(operand, str) and _json_equal(value, operand)


def _matches_anything_but(value, operand) -> bool:
    """``anything-but`` takes a literal or list of literals, OR one nested
    content matcher (not ``cidr``; see ``_nested_matcher_ok``). AWS rejects a
    list mixing literals and nested matchers at rule creation, so only the
    strict form is matched."""
    if isinstance(operand, dict):
        return _nested_matcher_ok(operand) and not _matches_content_filter(value, operand)
    if isinstance(operand, list):
        return not any(_json_equal(value, item) for item in operand)
    return not _json_equal(value, operand)


# Every operator answered from the *value*, mapped to its matcher. Dict order is
# the try order, but only ever consulted for one key: AWS refuses a two-operator
# expression at rule creation, and a pattern is validated before it is matched.
#
# ``exists`` has no entry — it asks about the key, not the value, so
# ``_matches_detail`` settles it before any value gets here.
_CONTENT_MATCHERS = {
    "wildcard": _matches_wildcard,
    "prefix": functools.partial(_matches_affix, at_start=True),
    "suffix": functools.partial(_matches_affix, at_start=False),
    "exactly": _matches_exactly,
    "equals-ignore-case": _matches_equals_ignore_case,
    "cidr": _matches_cidr,
    "anything-but": _matches_anything_but,
    "numeric": _matches_numeric,
}
_CONTENT_MATCHER_KEYS = tuple(_CONTENT_MATCHERS)
# The invertible subset, derived rather than spelled a second time: a gate
# approving a key the validator refuses is what reopens the inversion.
_NESTED_MATCHER_KEYS = tuple(k for k in _CONTENT_MATCHER_KEYS if k in _ANYTHING_BUT_NESTABLE)


def _nested_matcher_ok(filter_rule) -> bool:
    """Whether ``anything-but``'s nested operand is one AWS lets it invert and
    this emulator can evaluate.

    ``cidr`` and ``numeric`` are the interesting exclusions: both are supported
    as matchers, but AWS nests only ``prefix``, ``suffix``, ``wildcard`` and
    ``equals-ignore-case``. Inverting an operand that can only answer "no
    match" — malformed, or not invertible — would turn a bad rule into one that
    matches *every* event and fans it out to its targets, so ``anything-but``
    declines instead. Validation refuses these outright now; this is the
    backstop for a rule restored from persisted state, which is never
    revalidated.

    Anything other than exactly one invertible key is declined, so the answer
    cannot depend on which key the dispatch reaches first. The scan is over
    ``_matches_content_filter``'s own keys, so the key approved here is the key
    that answers there — and ``exists``, having no registry entry, is invisible
    to it: ``{"exists": true, "prefix": "a"}`` is approved on the prefix.
    Validation is what refuses that, reporting "Only one key allowed in match
    expression" for any operand dict with more than one key, so the gate never
    sees one."""
    if not isinstance(filter_rule, dict):
        return False
    present = [k for k in _CONTENT_MATCHER_KEYS if k in filter_rule]
    if len(present) != 1 or present[0] not in _NESTED_MATCHER_KEYS:
        return False
    # All four nestable operators take a pattern string or a list of them, so
    # one arm covers them.
    return _string_operands(filter_rule[present[0]]) is not None


def _matches_content_filter(value, filter_rule):
    """Whether one scalar satisfies one match expression — the first operator it
    names in registry order, since AWS allows it only one."""
    for key, matcher in _CONTENT_MATCHERS.items():
        if key in filter_rule:
            return matcher(value, filter_rule[key])
    return False


def _invoke_target(target, event, rule, view=None):
    arn = target.get("Arn", "")
    event_path = set(event.get("_DispatchPath") or [])

    # The tree the rule matched on, so a rule cannot fire on a field whose
    # delivered value differs. Decoded once for the whole event.
    view = view or _pattern_event_view(event)
    event_payload = json.dumps(view.event_with_detail())

    # An ``InputPath`` that does not resolve leaves the whole event in place.
    target_input_payload = None
    if target.get("InputTransformer"):
        target_input_payload = _apply_input_transformer(target["InputTransformer"], rule, view)
    elif target.get("Input"):
        target_input_payload = target["Input"]
    elif target.get("InputPath"):
        try:
            val = json.loads(event_payload)
            for part in target["InputPath"].strip("$.").split("."):
                if part:
                    val = val[part]
            target_input_payload = json.dumps(val)
        except Exception:
            pass
    if target_input_payload is not None:
        event_payload = target_input_payload

    try:
        try:
            spec = parse_arn(arn)
        except ArnParseError:
            logger.warning("EventBridge: invalid target ARN %s", arn)
            return

        if spec.service == "events" and spec.resource.startswith("api-destination/"):
            _dispatch_to_api_destination(spec, event_payload, target)
        elif spec.service == "events":
            _dispatch_to_event_bus(spec, event, rule, event_path, target_input_payload)
        elif spec.service == "states":
            _dispatch_to_stepfunctions(arn, event_payload)
        elif not _target_matches_request_scope(spec):
            # AWS parity: EventBridge accepts cross-region SNS/SQS targets at PutTargets, then records a
            # FailedInvocations delivery failure without cross-region delivery. MiniStack does not model
            # that metric or DLQs, so the faithful observable outcome is to accept the target and drop
            # the invocation here. Confirmed against AWS on 2026-07-13; see
            # RESEARCH/MINISTACK_CROSSREGION_SNS_TARGET_POLICY.md.
            logger.warning(
                "EventBridge: target %s not delivered: cross-region invocation failed "
                "(FailedInvocation-equivalent)",
                arn,
            )
        elif spec.service == "lambda":
            _dispatch_to_lambda(arn, event_payload)
        elif spec.service == "sqs":
            _dispatch_to_sqs(spec, event_payload, target.get("SqsParameters") or {})
        elif spec.service == "sns":
            _dispatch_to_sns(arn, event_payload)
        else:
            logger.warning("EventBridge: unsupported target type for ARN %s", arn)
    except Exception as e:
        logger.error("EventBridge target dispatch error for %s: %s", arn, e)


def _target_matches_request_scope(spec) -> bool:
    # Cross-region targets are accepted at PutTargets but fail delivery here for AWS parity.
    return spec.account_id == get_account_id() and spec.region == get_region()


def _dispatch_to_event_bus(spec, event, rule, event_path, target_input_payload=None):
    if spec.service != "events" or not spec.resource.startswith("event-bus/"):
        logger.warning("EventBridge -> Event bus: unsupported event target ARN %s", spec)
        return

    if spec.account_id != get_account_id() or spec.region != get_region():
        logger.warning("EventBridge -> Event bus: event bus %s not found", spec)
        return

    bus_name = spec.resource.split("/", 1)[1]
    if bus_name not in _event_buses:
        logger.warning("EventBridge -> Event bus: event bus %s not found", spec)
        return
    source_rule_key = _rule_key(rule.get("Name"), rule.get("EventBusName", "default"))
    if source_rule_key in event_path:
        logger.warning("EventBridge -> Event bus: recursive target dispatch skipped for %s", spec)
        return
    forwarded = dict(event)
    forwarded["EventBusName"] = bus_name
    forwarded["Region"] = spec.region
    forwarded["_DispatchPath"] = [*event_path, source_rule_key]
    if target_input_payload is not None:
        forwarded["Detail"] = target_input_payload
    _dispatch_event(forwarded)
    archived = dict(forwarded)
    archived.pop("_DispatchPath", None)
    _archive_event(archived)
    logger.info("EventBridge -> Event bus %s: dispatched", spec)


def _apply_input_transformer(transformer, rule, view):
    input_paths = transformer.get("InputPathsMap", {})
    template = transformer.get("InputTemplate", "")

    # The same tree the rule matched on and the target is delivered:
    # hand-building a third copy here is how ``time`` came to render as a raw
    # epoch second while the payload alongside it carried the ISO-8601 string.
    event_envelope = view.event_with_detail()

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

    # AWS generates ingestion-time when the event is received by EventBridge
    # (always present, ISO-8601) — it is NOT the event's own `time` field and
    # cannot be overwritten by an InputPathsMap entry.
    ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    reserved = {
        "aws.events.event.json": json.dumps(event_envelope),
        "aws.events.event": json.dumps(view.envelope()),
        "aws.events.event.ingestion-time": ingestion_time,
    }
    if rule:
        reserved["aws.events.rule-name"] = rule.get("Name", "")
        reserved["aws.events.rule-arn"] = rule.get("Arn", "")
    for k, v in reserved.items():
        replacements.setdefault(k, v)
    # ingestion-time is reserved and uneditable, even if InputPathsMap declares it.
    replacements["aws.events.event.ingestion-time"] = ingestion_time

    result = template
    for var_name, val in replacements.items():
        result = result.replace(f"<{var_name}>", str(val))

    return result


def _dispatch_to_lambda(arn, payload):
    from ministack.services import lambda_svc

    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        event = {"body": payload}

    func, config, func_name = lambda_svc._get_func_record_for_ref(arn)
    if not func or not config:
        logger.warning("EventBridge → Lambda: function %s not found", func_name)
        return
    exec_record = lambda_svc._execution_record_for_config(func, config)
    threading.Thread(
        target=lambda_svc._execute_function_with_config_scope, args=(exec_record, event), daemon=True
    ).start()
    logger.info("EventBridge → Lambda %s: dispatched", func_name)


def _dispatch_to_sqs(spec, payload, sqs_parameters=None):
    """Dispatch an EventBridge event to an SQS queue.

    ``sqs_parameters`` carries the target's ``SqsParameters`` block from the
    rule definition. For FIFO target queues, ``SqsParameters.MessageGroupId``
    is required by AWS and must be stamped on the delivered message; for
    standard queues it is ignored. Real EventBridge also derives a
    content-based ``MessageDeduplicationId`` for FIFO queues when content
    deduplication is enabled — we mirror that here.
    """
    from ministack.services import sqs as _sqs

    queue_name = spec.resource
    queue = _sqs._queue_by_arn(str(spec))
    if not queue:
        logger.warning("EventBridge → SQS: queue %s not found", queue_name)
        return

    sqs_parameters = sqs_parameters or {}
    msg_id = new_uuid()
    md5 = hashlib.md5(payload.encode()).hexdigest()
    now = time.time()
    msg = {
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
    }
    if queue.get("is_fifo"):
        group_id = sqs_parameters.get("MessageGroupId") or ""
        if not group_id:
            logger.warning(
                "EventBridge → SQS %s is FIFO but target SqsParameters.MessageGroupId is empty; "
                "real AWS would refuse to deliver. Generating a fallback so the message is not lost.",
                queue_name,
            )
            group_id = "ministack-eventbridge-default"
        msg["group_id"] = group_id
        # Mirror real EventBridge: derive a content-based dedup ID when none is
        # supplied so retries are idempotent within the FIFO dedup window.
        msg["dedup_id"] = hashlib.sha256(payload.encode()).hexdigest()
        # Maintain sequence numbering so subsequent ReceiveMessage calls see
        # the same ordering as native SQS FIFO deliveries.
        queue["fifo_seq"] = queue.get("fifo_seq", 0) + 1
        msg["seq"] = str(queue["fifo_seq"]).zfill(20)
    queue["messages"].append(msg)
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


def _dispatch_to_stepfunctions(arn, payload):
    from ministack.services import stepfunctions as _sfn

    # Accept all three SFN target ARN shapes EventBridge supports in real
    # AWS: base state machine, published version, and alias. The resolver
    # walks all three stores; ``None`` means the target ARN doesn't match
    # any state machine the caller's account can see.
    if _sfn._resolve_state_machine_arn(arn) is None:
        logger.warning("EventBridge → Step Functions: state machine %s not found", arn)
        return

    sm_name = arn.rsplit(":", 1)[-1]
    _sfn._start_execution({
        "stateMachineArn": arn,
        "input": payload,
    })
    logger.info("EventBridge → Step Functions %s: dispatched", sm_name)


# ---------------------------------------------------------------------------
# API destination dispatch
# ---------------------------------------------------------------------------

# OAuth access tokens per connection: {(account, region, connection_name): entry}.
# Plain dict (not AccountRegionScopedDict) because delivery worker threads read
# it outside a request scope; guarded by _oauth_tokens_lock.
_oauth_tokens: dict = {}
_oauth_tokens_lock = threading.Lock()

# Stamped on every API destination request. Real EventBridge documents
# User-Agent and Range as non-overridable and sends Accept-Encoding /
# Connection itself; Content-Type defaults below only when no custom value is
# configured on the connection or target.
_API_DEST_FIXED_HEADERS = {
    "User-Agent": "Amazon/EventBridge/ApiDestinations",
    "Range": "bytes=0-1048575",
    "Accept-Encoding": "gzip,deflate",
    "Connection": "close",
}
_API_DEST_DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8"
# Same 1 MiB ceiling the Range header above imposes on a delivery response,
# applied to the OAuth token response — which this emulator does read.
_MAX_OAUTH_RESPONSE_BYTES = 1048576

# Real EventBridge removes these headers from API destination requests, so
# connection/target parameters cannot smuggle them in (transport-owned headers
# such as Host and Content-Length are set by the HTTP client itself).
_API_DEST_REMOVED_HEADERS = {
    "a-im", "accept-charset", "accept-datetime", "accept-encoding",
    "cache-control", "connection", "content-encoding", "content-length",
    "content-md5", "date", "expect", "forwarded", "from", "host",
    "http2-settings", "if-match", "if-modified-since", "if-none-match",
    "if-range", "if-unmodified-since", "max-forwards", "origin", "pragma",
    "proxy-authorization", "range", "referer", "te", "trailer",
    "transfer-encoding", "user-agent", "upgrade", "via", "warning",
}


class _NoFollowRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow HTTP redirects. urllib preserves the Authorization header
    across a cross-host 3xx hop, which would leak a connection's credentials to
    the redirect target. Real EventBridge does not follow redirects — a 3xx is
    a (non-retryable) delivery failure — so declining to redirect lets urllib's
    default error handler surface the 3xx as an ``HTTPError``, which the
    delivery path reports as that (non-retryable) status code."""

    def redirect_request(self, *_args, **_kwargs):
        return None


_api_dest_opener = urllib.request.build_opener(_NoFollowRedirect())


def _http_open(req: urllib.request.Request, timeout: int = 5):
    """Open one outbound API-destination / OAuth request without following
    redirects and without honoring non-HTTP(S) schemes, so a caller-controlled
    endpoint cannot turn a delivery into a ``file://`` read. A scheme guard, not
    a network guard: delivering to ``127.0.0.1`` is the normal local case, so
    private, loopback, and link-local hosts stay reachable over http(s). The
    5-second default is AWS parity — API destination requests have a maximum
    client execution timeout of 5 seconds."""
    scheme = urllib.parse.urlsplit(req.full_url).scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"unsupported URL scheme for API destination request: {scheme or '(none)'}")
    return _api_dest_opener.open(req, timeout=timeout)


def _connection_params_map(params) -> dict:
    """Flatten a ConnectionHttpParameters list (``[{Key, Value, IsValueSecret}]``)
    into a plain map. Connection-side parameters use the list-of-objects shape,
    unlike the string maps on target HttpParameters."""
    out = {}
    for item in params or []:
        key = item.get("Key")
        if key:
            out[key] = item.get("Value", "")
    return out


def _apply_path_parameters(url: str, values) -> str:
    """Populate ``*`` path wildcards in the invocation endpoint from the
    target's PathParameterValues, in order — the same contract as real
    EventBridge."""
    if not values:
        return url
    parsed = urllib.parse.urlsplit(url)
    path = parsed.path
    for value in values:
        if "*" not in path:
            break
        path = path.replace("*", urllib.parse.quote(str(value), safe=""), 1)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def _merge_query(url: str, params: dict) -> str:
    if not params:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query.update(params)
    encoded = urllib.parse.urlencode(query)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, encoded, parsed.fragment))


def _merge_body_parameters(payload: str, body_params: dict) -> str:
    """Fold connection BodyParameters into a JSON-object body. Real EventBridge
    documents body parameters as included in every invocation; how they combine
    with a non-object body is undocumented there, so those bodies are left
    untouched."""
    if not body_params:
        return payload
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        return payload
    if not isinstance(parsed, dict):
        return payload
    parsed.update(body_params)
    return json.dumps(parsed)


def _fetch_oauth_token(oauth: dict) -> dict:
    """client_credentials exchange against the connection's authorization
    endpoint: ClientID/ClientSecret ride the request body (query for GET),
    OAuthHttpParameters contribute extra header/query/body parameters, and
    ``access_token`` / ``token_type`` / ``expires_in`` are read from the JSON
    response. ``grant_type`` defaults to ``client_credentials`` when
    OAuthHttpParameters does not set one — AWS's own examples pass it
    explicitly through body parameters."""
    method = (oauth.get("HttpMethod") or "POST").upper()
    client = oauth.get("ClientParameters") or {}
    http_params = oauth.get("OAuthHttpParameters") or {}
    headers = _connection_params_map(http_params.get("HeaderParameters"))
    form = {
        "grant_type": "client_credentials",
        "client_id": client.get("ClientID", ""),
        "client_secret": client.get("ClientSecret", ""),
    }
    form.update(_connection_params_map(http_params.get("BodyParameters")))
    url = _merge_query(
        oauth.get("AuthorizationEndpoint", ""),
        _connection_params_map(http_params.get("QueryStringParameters")),
    )
    data = None
    if method == "GET":
        url = _merge_query(url, form)
    else:
        data = urllib.parse.urlencode(form).encode("utf-8")
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with _http_open(req) as resp:
        # Bounded read: the authorization endpoint is caller-supplied, and an
        # unbounded read of whatever it returns is a memory hazard on a delivery
        # thread. 1 MiB is EventBridge's own ceiling for an API destination
        # response (the fixed "Range: bytes=0-1048575" header); a token document
        # that large is not one.
        token = json.loads(resp.read(_MAX_OAUTH_RESPONSE_BYTES).decode("utf-8"))
    token_type = token.get("token_type") or "Bearer"
    if token_type.lower() == "bearer":
        token_type = "Bearer"
    expires_in = token.get("expires_in")
    return {
        "token": token.get("access_token", ""),
        "type": token_type,
        "expires_at": time.time() + float(expires_in) if expires_in else None,
    }


def _oauth_authorization_header(conn: dict, token_key, force_refresh: bool = False) -> str:
    oauth = (conn.get("AuthParameters") or {}).get("OAuthParameters") or {}
    with _oauth_tokens_lock:
        cached = _oauth_tokens.get(token_key)
    # Real EventBridge refreshes proactively when the token expires within 60
    # seconds of an invocation — synchronously, on the event path.
    fresh = cached and (cached["expires_at"] is None or cached["expires_at"] - time.time() > 60)
    if force_refresh or not fresh:
        cached = _fetch_oauth_token(oauth)
        with _oauth_tokens_lock:
            _oauth_tokens[token_key] = cached
    return f"{cached['type']} {cached['token']}"


def _evict_oauth_token(name: str):
    """Drop a connection's cached access token.

    The cache is keyed by connection NAME, and names are reusable: on real
    EventBridge the token lives and dies with the connection, so a connection
    that is deleted (and possibly recreated under the same name), has its auth
    parameters replaced, or is deauthorized must perform a fresh
    client_credentials exchange rather than keep serving a token minted from
    credentials that no longer apply.
    """
    with _oauth_tokens_lock:
        _oauth_tokens.pop((get_account_id(), get_region(), name), None)


def _connection_auth_headers(conn: dict, token_key, force_refresh: bool = False) -> dict:
    """Authorization header(s) for one delivery, mirroring how real EventBridge
    populates them from the connection secret: BASIC → ``Authorization:
    Basic``, API_KEY → the configured header, OAUTH_CLIENT_CREDENTIALS → a
    managed Bearer token."""
    import base64 as _b64

    auth_type = conn.get("AuthorizationType", "")
    params = conn.get("AuthParameters") or {}
    if auth_type == "BASIC":
        basic = params.get("BasicAuthParameters") or {}
        raw = f"{basic.get('Username', '')}:{basic.get('Password', '')}".encode("utf-8")
        return {"Authorization": "Basic " + _b64.b64encode(raw).decode("ascii")}
    if auth_type == "API_KEY":
        api_key = params.get("ApiKeyAuthParameters") or {}
        name = api_key.get("ApiKeyName")
        return {name: api_key.get("ApiKeyValue", "")} if name else {}
    if auth_type == "OAUTH_CLIENT_CREDENTIALS":
        return {"Authorization": _oauth_authorization_header(conn, token_key, force_refresh)}
    return {}


def _finalize_api_dest_headers(headers: dict) -> dict:
    """Drop the headers real EventBridge strips from API destination requests
    and stamp its non-overridable ones. Applied after *every* merge — target
    and connection parameters first, then the connection auth headers — so no
    configured name can win, e.g. an ApiKeyName of ``User-Agent``/``Host``.
    Names are matched trimmed, because ``"User-Agent "`` is not in the removed
    set but is a legal header name to http.client, which only rejects leading
    whitespace — so the padded spelling would ride out alongside the real one."""
    out = {k.strip(): v for k, v in headers.items()
           if k.strip().lower() not in _API_DEST_REMOVED_HEADERS}
    out.update(_API_DEST_FIXED_HEADERS)
    return out


def _build_api_destination_request(dest: dict, conn: dict, target_http_params: dict, payload: str) -> dict:
    invocation = (conn.get("AuthParameters") or {}).get("InvocationHttpParameters") or {}
    conn_headers = _connection_params_map(invocation.get("HeaderParameters"))
    conn_query = _connection_params_map(invocation.get("QueryStringParameters"))
    conn_body = _connection_params_map(invocation.get("BodyParameters"))

    # Target HttpParameters are merged with the connection's invocation
    # parameters, "with any values from the Connection taking precedence"
    # (EventBridge API reference, HttpParameters).
    headers = {**(target_http_params.get("HeaderParameters") or {}), **conn_headers}
    query = {**(target_http_params.get("QueryStringParameters") or {}), **conn_query}

    url = _apply_path_parameters(
        dest.get("InvocationEndpoint", ""), target_http_params.get("PathParameterValues")
    )
    url = _merge_query(url, query)
    body = _merge_body_parameters(payload, conn_body)

    headers = _finalize_api_dest_headers(headers)
    if not any(k.lower() == "content-type" for k in headers):
        headers["Content-Type"] = _API_DEST_DEFAULT_CONTENT_TYPE

    return {
        "url": url,
        "method": (dest.get("HttpMethod") or "POST").upper(),
        "headers": headers,
        "body": body,
    }


def _api_destination_send_sync(request: dict, auth_headers: dict) -> int:
    """Blocking HTTP send for one API destination delivery. Runs on a worker
    thread so the event loop stays unblocked; stdlib only (see the SNS HTTP
    delivery note — aiohttp is not a declared dependency). ``_http_open``
    carries the AWS-parity 5-second client execution timeout."""
    headers = _finalize_api_dest_headers({**request["headers"], **auth_headers})
    body = request["body"]
    req = urllib.request.Request(
        request["url"],
        data=body.encode("utf-8") if body is not None else None,
        headers=headers,
        method=request["method"],
    )
    try:
        with _http_open(req) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def _deliver_to_api_destination(name: str, request: dict, conn: dict, token_key):
    is_oauth = conn.get("AuthorizationType") == "OAUTH_CLIENT_CREDENTIALS"
    try:
        status = _api_destination_send_sync(request, _connection_auth_headers(conn, token_key))
        if status in (401, 407) and is_oauth:
            # Real EventBridge refreshes the OAuth token when a 401/407 comes
            # back and retries the delivery.
            status = _api_destination_send_sync(
                request, _connection_auth_headers(conn, token_key, force_refresh=True)
            )
    except Exception as exc:
        logger.warning("EventBridge → API destination %s delivery failed: %s", name, exc)
        return
    if 200 <= status < 300:
        logger.info("EventBridge → API destination %s: HTTP %s", name, status)
    elif status in (401, 407, 409, 429) or status >= 500:
        # AWS parity: these statuses re-enter the 24h/185-attempt retry pipeline
        # (DLQ on exhaustion, Retry-After honored). MiniStack does not model
        # that queue — the same policy as cross-region FailedInvocations — so
        # the outcome is logged and the event dropped.
        logger.warning(
            "EventBridge → API destination %s: retryable HTTP %s (retry pipeline not modeled; dropped)",
            name,
            status,
        )
    else:
        logger.warning("EventBridge → API destination %s: HTTP %s (not retryable; dropped)", name, status)


def _dispatch_to_api_destination(spec, payload, target):
    if spec.account_id != get_account_id() or spec.region != get_region():
        logger.warning(
            "EventBridge → API destination: %s is outside the current account/region scope", spec
        )
        return
    # Resource is "api-destination/<name>" (real AWS appends "/<uuid>"; tolerate both).
    name = spec.resource.split("/", 2)[1]
    dest = _api_destinations.get(name)
    if not dest:
        logger.warning("EventBridge → API destination: %s not found", name)
        return
    if dest.get("ApiDestinationState") != "ACTIVE":
        # Real EventBridge does not invoke INACTIVE destinations; the delivery
        # fails into the retry pipeline instead. Log-and-drop, as above.
        logger.warning(
            "EventBridge → API destination %s: state %s; not invoked",
            name,
            dest.get("ApiDestinationState"),
        )
        return
    conn_name = (dest.get("ConnectionArn") or "").rsplit("/", 1)[-1]
    conn = _connections.get(conn_name) if conn_name else None
    if conn is None:
        logger.warning(
            "EventBridge → API destination %s: connection %s not found", name, conn_name or "<unset>"
        )
        return
    if conn.get("ConnectionState") != "AUTHORIZED":
        # The mirror of the ApiDestinationState check above. DeauthorizeConnection
        # already clears the stored credentials, but a connection restored from
        # persisted state can be DEAUTHORIZED with parameters still attached — and
        # delivering unauthenticated is not a better outcome than not delivering.
        logger.warning(
            "EventBridge → API destination %s: connection %s is %s; not invoked",
            name, conn_name, conn.get("ConnectionState"),
        )
        return
    # InvocationRateLimitPerSecond is accepted at CreateApiDestination but not
    # enforced here — MiniStack delivers immediately and does not model the
    # delivery queue that rate limiting implies.
    request = _build_api_destination_request(dest, conn, target.get("HttpParameters") or {}, payload)
    token_key = (get_account_id(), get_region(), conn_name)
    # Deliver on a background daemon thread, mirroring the SNS HTTP(S) path:
    # PutEvents returns as soon as the event is accepted and must not block on
    # the destination endpoint.
    threading.Thread(
        target=_deliver_to_api_destination,
        args=(name, request, copy.deepcopy(conn), token_key),
        daemon=True,
    ).start()


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_taggable_events_arn(arn)
    if err:
        return err
    tags = data.get("Tags", [])
    if arn not in _tags:
        _tags[arn] = {}
    for t in tags:
        _tags[arn][t["Key"]] = t["Value"]
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_taggable_events_arn(arn)
    if err:
        return err
    keys = data.get("TagKeys", [])
    if arn in _tags:
        for k in keys:
            _tags[arn].pop(k, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_taggable_events_arn(arn)
    if err:
        return err
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

    pattern_error = _event_pattern_error(data.get("EventPattern", ""))
    if pattern_error:
        return _invalid_event_pattern(pattern_error)

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
        pattern_error = _event_pattern_error(ep)
        if pattern_error:
            return _invalid_event_pattern(pattern_error)
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
    archive_name, source_error = _archive_name_from_ref(source_arn)
    if source_error:
        code, message = source_error
        return error_response_json(code, message, 400)
    archive = _archives.get(archive_name)
    if not archive:
        return error_response_json(
            "ResourceNotFoundException",
            f"Archive {archive_name} does not exist.",
            400,
        )
    dest_bus_name, dest_error = _event_bus_name_from_ref(dest.get("Arn", ""))
    if dest_error:
        code, message = dest_error
        return error_response_json(code, message, 400)
    if dest.get("Arn") != archive.get("EventSourceArn"):
        return error_response_json(
            "ValidationException",
            "Destination.Arn must match the archive event source.",
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
    replay_account_id = get_account_id()
    replay_region = get_region()

    def _run():
        previous_account = get_account_id()
        previous_region = get_region()
        set_request_account_id(replay_account_id)
        set_request_region(replay_region)
        try:
            replay["State"] = "RUNNING"
            for event in list(archive.get("Events", [])):
                ts = event.get("Time", 0)
                if not (event_start <= ts <= event_end):
                    continue
                replayed = dict(event)
                replayed["EventBusName"] = dest_bus_name
                # AWS stamps a replayed event with the replay's name, which is
                # how a rule tells replayed traffic from live traffic — either
                # to act on it or, more often, to filter it out.
                replayed["ReplayName"] = name
                _dispatch_event(replayed)
            replay["State"] = "COMPLETED"
            replay["ReplayEndTime"] = _now_ts()
        finally:
            set_request_account_id(previous_account)
            set_request_region(previous_region)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    # Real AWS StartReplay returns the initial state STARTING; the
    # replay flips to RUNNING in the background dispatch thread above.
    return json_response({"ReplayArn": arn, "State": "STARTING"})


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
    # AWS EventSourceState enum: PENDING | ACTIVE | DELETED. "ENABLED" is not
    # a valid value (Java/Go SDK v2 strict enum parsers reject it).
    return json_response({
        "Name": name,
        "State": "ACTIVE",
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

# An http(s) URL with a non-empty authority. Schemes are case-insensitive per
# RFC 3986 — "HTTPS://host" is the same endpoint urllib would dial, so the API
# must not reject what the outbound opener accepts. Control characters and
# spaces are rejected separately: urlsplit strips CR/LF/TAB before it reports a
# hostname (bpo-43882), so the pattern alone would pass them through.
_HTTP_ENDPOINT_RE = re.compile(r"^https?://[^/?#\s]+", re.IGNORECASE)
_ENDPOINT_CTL_RE = re.compile(r"[\x00-\x20\x7f]")

# The other two caller-supplied values that ride the outbound request: the
# method goes on the request line, and the authorization type decides which
# credentials are attached. Both are enums on the API model, so an unrecognized
# value is a 400 on AWS — here it used to be stored and then silently mean
# "send no credentials" or "POST".
_API_DEST_HTTP_METHODS = ("DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT")
_CONNECTION_AUTH_TYPES = ("API_KEY", "BASIC", "OAUTH_CLIENT_CREDENTIALS")


def _validate_enum(value, allowed, param: str):
    """Reject a value outside an API-model enum, in the shape AWS's own
    validator emits. Absent is not invalid — required-ness is checked by the
    caller that needs it."""
    if value is None or value in allowed:
        return None
    return error_response_json(
        "ValidationException",
        f"1 validation error detected: Value '{value}' at '{param}' failed to satisfy "
        f"constraint: Member must satisfy enum value set: [{', '.join(allowed)}]",
        400,
    )


def _validate_http_endpoint(endpoint, param: str):
    """Reject a caller-supplied endpoint that is not a dialable http(s) URL, as
    real AWS does at CreateApiDestination / CreateConnection. Keeping non-http
    schemes out of stored state also keeps them off the outbound opener —
    defense in depth with ``_http_open``'s scheme guard, which is the backstop
    for records restored from persistence. "Dialable" is the whole point, so a
    non-empty authority is not sufficient: ``http://@`` and ``http://:80`` carry
    no host, and ``http://ho\\nst/x`` reaches the connect call as something the
    caller never wrote."""
    ok = (
        isinstance(endpoint, str)
        and _HTTP_ENDPOINT_RE.match(endpoint) is not None
        and _ENDPOINT_CTL_RE.search(endpoint) is None
    )
    if ok:
        try:
            ok = bool(urllib.parse.urlsplit(endpoint).hostname)
        except ValueError:
            ok = False   # malformed authority, e.g. an unclosed IPv6 bracket
    if not ok:
        return error_response_json(
            "ValidationException",
            f"Parameter {param} is not valid. "
            "Reason: Must be an http:// or https:// endpoint.",
            400,
        )
    return None


def _validate_oauth_authorization_endpoint(auth_params):
    """The OAuth AuthorizationEndpoint is the second caller-controlled URL on
    this path, and the one the client_id/client_secret are POSTed to — so it
    gets the same http(s) check as an API destination's InvocationEndpoint
    rather than only failing (silently, on a delivery thread) at token time.

    ``auth_params`` is raw caller JSON, so it is type-checked rather than
    dereferenced: CreateConnection stored a non-object AuthParameters verbatim
    and answered 200, and a validation step must not turn that into a 500."""
    oauth = auth_params.get("OAuthParameters") if isinstance(auth_params, dict) else None
    if not isinstance(oauth, dict) or "AuthorizationEndpoint" not in oauth:
        return None
    return _validate_http_endpoint(oauth["AuthorizationEndpoint"], "AuthorizationEndpoint")


def _create_connection(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    auth_type_error = _validate_enum(
        data.get("AuthorizationType"), _CONNECTION_AUTH_TYPES, "authorizationType")
    if auth_type_error:
        return auth_type_error
    endpoint_error = _validate_oauth_authorization_endpoint(data.get("AuthParameters"))
    if endpoint_error:
        return endpoint_error
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
    _evict_oauth_token(name)
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
    if "AuthorizationType" in data:
        auth_type_error = _validate_enum(
            data["AuthorizationType"], _CONNECTION_AUTH_TYPES, "authorizationType")
        if auth_type_error:
            return auth_type_error
    if "AuthParameters" in data:
        endpoint_error = _validate_oauth_authorization_endpoint(data["AuthParameters"])
        if endpoint_error:
            return endpoint_error
    conn = _connections[name]
    now = _now_ts()
    for key in ("AuthorizationType", "AuthParameters", "Description"):
        if key in data:
            conn[key] = data[key]
    if "AuthParameters" in data or "AuthorizationType" in data:
        # Re-authorization: the new credentials, not the cached token, decide
        # what the next invocation carries. A description-only update does not
        # re-authorize, so it keeps the token.
        _evict_oauth_token(name)
    conn["LastModifiedTime"] = now
    conn["ConnectionState"] = "AUTHORIZED"
    conn["LastAuthorizedTime"] = now

    return json_response({
        "ConnectionArn": conn["ConnectionArn"],
        "ConnectionState": conn["ConnectionState"],
        "LastModifiedTime": now,
    })


def _deauthorize_connection(data):
    """Per the API reference this "removes all authorization parameters from the
    connection … so you can reuse it without having to create a new connection",
    so the stored credentials go with the state flip. Keeping them meant a
    deauthorized connection carried on presenting its Basic/API-key credentials
    on every delivery, and evicting the OAuth token achieved nothing because the
    next delivery simply fetched a fresh one."""
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
    conn["AuthParameters"] = {}
    _evict_oauth_token(name)
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
    endpoint_error = _validate_http_endpoint(data.get("InvocationEndpoint", ""), "InvocationEndpoint")
    if endpoint_error:
        return endpoint_error
    method_error = _validate_enum(data.get("HttpMethod"), _API_DEST_HTTP_METHODS, "httpMethod")
    if method_error:
        return method_error
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
    if "InvocationEndpoint" in data:
        endpoint_error = _validate_http_endpoint(data["InvocationEndpoint"], "InvocationEndpoint")
        if endpoint_error:
            return endpoint_error
    if "HttpMethod" in data:
        method_error = _validate_enum(data["HttpMethod"], _API_DEST_HTTP_METHODS, "httpMethod")
        if method_error:
            return method_error
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


# ---------------------------------------------------------------------------
# Scheduled rule background ticker
# ---------------------------------------------------------------------------

_SCHEDULER_TICK_INTERVAL = 10  # seconds between sweeps


def _parse_rate_seconds(expr: str) -> int | None:
    """Return the interval in seconds for a rate() expression, or None."""
    m = re.match(r"^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$", expr)
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    if unit in ("minute", "minutes"):
        return n * 60
    if unit in ("hour", "hours"):
        return n * 3600
    return n * 86400


_MONTH_NAMES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,  "MAY": 5,  "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
# AWS DoW: 1=SUN, 2=MON, 3=TUE, 4=WED, 5=THU, 6=FRI, 7=SAT
_DOW_NAMES = {"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}


def _cron_field(field: str, lo: int, hi: int, names: dict | None = None) -> frozenset:
    """Expand a single AWS cron field token into a frozenset of matching integers."""
    if field in ("*", "?"):
        return frozenset(range(lo, hi + 1))

    def resolve(tok: str) -> int:
        upper = tok.upper()
        if names and upper in names:
            return names[upper]
        return int(upper)

    result: set = set()
    for part in field.upper().split(","):
        if "/" in part:
            base, step_s = part.rsplit("/", 1)
            step = int(step_s)
            if base in ("*", "?"):
                start, end = lo, hi
            elif "-" in base:
                a, b = base.split("-", 1)
                start, end = resolve(a), resolve(b)
            else:
                start, end = resolve(base), hi
            result.update(range(start, end + 1, step))
        elif "-" in part:
            a, b = part.split("-", 1)
            result.update(range(resolve(a), resolve(b) + 1))
        else:
            result.add(resolve(part))
    return frozenset(result)


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _last_weekday_of_month(year: int, month: int) -> int:
    """Day-of-month of the last Mon-Fri (used by AWS cron LW)."""
    last = _last_day_of_month(year, month)
    for d in range(last, 0, -1):
        if datetime(year, month, d).isoweekday() <= 5:
            return d
    return 1  # unreachable for any real month


def _nearest_weekday(year: int, month: int, target_day: int) -> int:
    """AWS ``<n>W``: weekday nearest to ``target_day``, never crossing month boundary."""
    last = _last_day_of_month(year, month)
    target_day = min(max(target_day, 1), last)
    iso = datetime(year, month, target_day).isoweekday()
    if iso <= 5:
        return target_day
    if iso == 6:  # Saturday → Friday (back 1) unless that crosses month start
        return target_day - 1 if target_day - 1 >= 1 else target_day + 2
    return target_day + 1 if target_day + 1 <= last else target_day - 2  # Sunday → Monday


def _parse_dom_field(field: str) -> dict | None:
    """Parse AWS cron DoM field. Returns dict with ``days`` set + ``last`` / ``last_weekday`` /
    ``weekday_of`` markers for L, LW, and ``<n>W`` operators. ``None`` on invalid input."""
    if field in ("*", "?"):
        return {"days": frozenset(range(1, 32)), "last": False, "last_weekday": False, "weekday_of": []}
    days: set[int] = set()
    last = False
    last_weekday = False
    weekday_of: list[int] = []
    for part in field.upper().split(","):
        if part == "L":
            last = True
        elif part == "LW":
            last_weekday = True
        elif part.endswith("W"):
            try:
                n = int(part[:-1])
            except ValueError:
                return None
            if not 1 <= n <= 31:
                return None
            weekday_of.append(n)
        else:
            try:
                days.update(_cron_field(part, 1, 31))
            except (ValueError, KeyError):
                return None
    return {"days": frozenset(days), "last": last, "last_weekday": last_weekday, "weekday_of": weekday_of}


def _parse_dow_field(field: str) -> dict | None:
    """Parse AWS cron DoW field. Returns dict with ``days`` set + ``last_of`` (``<n>L`` = last
    <n> of month) and ``nth`` (``<n>#<k>`` = kth <n> of month). ``None`` on invalid input.
    AWS DoW: 1=SUN..7=SAT."""
    if field in ("*", "?"):
        return {"days": frozenset(range(1, 8)), "last_of": [], "nth": []}
    days: set[int] = set()
    last_of: list[int] = []
    nth: list[tuple[int, int]] = []

    def _resolve_dow(tok: str) -> int | None:
        u = tok.upper()
        if u in _DOW_NAMES:
            return _DOW_NAMES[u]
        try:
            v = int(u)
        except ValueError:
            return None
        return v if 1 <= v <= 7 else None

    for part in field.upper().split(","):
        if "#" in part:
            day_tok, sep, k_tok = part.partition("#")
            try:
                k = int(k_tok)
            except ValueError:
                return None
            n = _resolve_dow(day_tok)
            if n is None or not 1 <= k <= 5:
                return None
            nth.append((n, k))
        elif part.endswith("L") and part != "L":
            n = _resolve_dow(part[:-1])
            if n is None:
                return None
            last_of.append(n)
        elif part == "L":
            # Bare ``L`` in DoW is "Saturday" per AWS (== 7). Real AWS accepts it.
            last_of.append(7)
        else:
            try:
                days.update(_cron_field(part, 1, 7, _DOW_NAMES))
            except (ValueError, KeyError):
                return None
    return {"days": frozenset(days), "last_of": last_of, "nth": nth}


def _parse_cron_fields(expr: str):
    """Parse AWS cron(Min Hr DoM Mon DoW Year) into expanded field sets, or return None.

    Returns an 8-tuple:
      (min_set, hr_set, dom_struct, mon_set, dow_struct, yr_set_or_none, dom_raw, dow_raw)
    ``dom_struct`` / ``dow_struct`` are dicts (see ``_parse_dom_field`` / ``_parse_dow_field``)
    so ``L`` / ``W`` / ``#`` operators that depend on the actual date can be evaluated at
    match time. ``dom_raw`` / ``dow_raw`` preserve the original token for the AWS ``?``
    mutual-exclusion rule.
    """
    m = re.match(r"^cron\((.+)\)$", expr.strip())
    if not m:
        return None
    parts = m.group(1).split()
    if len(parts) != 6:
        return None
    min_f, hr_f, dom_f, mon_f, dow_f, yr_f = parts
    # AWS rule: exactly one of DoM and DoW must be '?'. Both non-'?' is invalid.
    if dom_f != "?" and dow_f != "?":
        return None
    try:
        dom_struct = _parse_dom_field(dom_f)
        dow_struct = _parse_dow_field(dow_f)
        if dom_struct is None or dow_struct is None:
            return None
        return (
            _cron_field(min_f, 0, 59),
            _cron_field(hr_f, 0, 23),
            dom_struct,
            _cron_field(mon_f, 1, 12, _MONTH_NAMES),
            dow_struct,
            _cron_field(yr_f, 1970, 2199) if yr_f not in ("*", "?") else None,
            dom_f,
            dow_f,
        )
    except (ValueError, KeyError):
        return None


def _dom_matches(dom_struct: dict, dt: datetime) -> bool:
    if dt.day in dom_struct["days"]:
        return True
    if dom_struct["last"] and dt.day == _last_day_of_month(dt.year, dt.month):
        return True
    if dom_struct["last_weekday"] and dt.day == _last_weekday_of_month(dt.year, dt.month):
        return True
    for n in dom_struct["weekday_of"]:
        if dt.day == _nearest_weekday(dt.year, dt.month, n):
            return True
    return False


def _dow_matches(dow_struct: dict, dt: datetime) -> bool:
    aws_dow = (dt.isoweekday() % 7) + 1
    if aws_dow in dow_struct["days"]:
        return True
    last = _last_day_of_month(dt.year, dt.month)
    for n in dow_struct["last_of"]:
        if aws_dow == n and dt.day + 7 > last:
            return True
    for n, k in dow_struct["nth"]:
        if aws_dow == n and (dt.day - 1) // 7 + 1 == k:
            return True
    return False


def _cron_next_fire(fields, after_dt: datetime) -> datetime | None:
    """Return the first datetime >= (after_dt + 1 min) that satisfies the cron fields.

    Uses forward-walking with jumps so sparse schedules (monthly, yearly) don't
    iterate every minute.  Returns None if no match is found within 4 years.
    """
    min_s, hr_s, dom_struct, mon_s, dow_struct, yr_s, dom_raw, dow_raw = fields
    dt = after_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    limit = dt + timedelta(days=4 * 366)
    while dt <= limit:
        if yr_s is not None and dt.year not in yr_s:
            later = sorted(y for y in yr_s if y > dt.year)
            if not later:
                return None
            dt = dt.replace(year=later[0], month=1, day=1, hour=0, minute=0)
            continue
        if dt.month not in mon_s:
            later = sorted(v for v in mon_s if v > dt.month)
            if later:
                dt = dt.replace(month=later[0], day=1, hour=0, minute=0)
            else:
                dt = dt.replace(year=dt.year + 1, month=min(mon_s), day=1, hour=0, minute=0)
            continue
        # AWS '?' rule: exactly one of DoM/DoW is '?'. The non-'?' field gates the day,
        # supporting L (last day), <n>W (nearest weekday), <n>L (last <n> of month),
        # and <n>#<k> (kth <n> of month).
        if dom_raw == "?":
            day_ok = _dow_matches(dow_struct, dt)
        else:
            day_ok = _dom_matches(dom_struct, dt)
        if not day_ok:
            dt = dt.replace(hour=0, minute=0) + timedelta(days=1)
            continue
        if dt.hour not in hr_s:
            later = sorted(v for v in hr_s if v > dt.hour)
            if later:
                dt = dt.replace(hour=later[0], minute=0)
            else:
                dt = dt.replace(hour=0, minute=0) + timedelta(days=1)
            continue
        if dt.minute not in min_s:
            later = sorted(v for v in min_s if v > dt.minute)
            if later:
                dt = dt.replace(minute=later[0])
            else:
                dt = dt.replace(minute=0) + timedelta(hours=1)
            continue
        return dt
    return None


def _tick_scheduled_rules():
    """Fire any enabled scheduled rule whose interval has elapsed."""
    now = _now_ts()
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    # Iterate _rules._data directly so we see every account/region, not just
    # the ContextVar default. Keys are (account_id, region, rule_key) tuples.
    for (account_id, region, rule_key), rule in list(_rules._data.items()):
        if rule.get("State") != "ENABLED":
            continue
        schedule = rule.get("ScheduleExpression", "")
        state_key = (account_id, region, rule_key)

        interval = _parse_rate_seconds(schedule)
        if interval is not None:
            # rate() — fire every `interval` seconds.
            if state_key not in _rule_last_fired:
                # AWS doc: "the countdown begins when you create the rule" — anchor
                # to CreationTime so a rule restored from persistence fires on the
                # first tick if its interval has already elapsed.
                _rule_last_fired[state_key] = rule.get("CreationTime", now)
                if now - _rule_last_fired[state_key] < interval:
                    continue
            if now - _rule_last_fired[state_key] < interval:
                continue
        else:
            fields = _parse_cron_fields(schedule)
            if fields is None:
                continue  # unknown / unsupported expression type
            # cron() — fire once per scheduled occurrence.
            if state_key not in _rule_last_fired:
                _rule_last_fired[state_key] = now
                continue
            last_dt = datetime.fromtimestamp(_rule_last_fired[state_key], tz=timezone.utc)
            next_fire = _cron_next_fire(fields, last_dt)
            if next_fire is None or now_dt < next_fire:
                continue

        _rule_last_fired[state_key] = now
        targets = _targets._data.get((account_id, region, rule_key), [])
        if not targets:
            continue
        try:
            rule_arn = parse_arn(rule.get("Arn", ""))
        except ArnParseError:
            logger.warning(
                "EventBridge scheduler: skipping rule %s with malformed ARN %s",
                rule_key,
                rule.get("Arn", ""),
            )
            continue
        if (
            rule_arn.service != "events"
            or not rule_arn.region
            or rule_arn.account_id != account_id
            or rule_arn.region != region
            or not rule_arn.resource.startswith("rule/")
        ):
            logger.warning(
                "EventBridge scheduler: skipping rule %s with out-of-scope ARN %s",
                rule_key,
                rule.get("Arn", ""),
            )
            continue
        previous_account = get_account_id()
        previous_region = get_region()
        set_request_account_id(account_id)
        set_request_region(region)
        try:
            event = {
                "EventId": new_uuid(),
                "Source": "aws.events",
                "DetailType": "Scheduled Event",
                "Detail": "{}",
                "EventBusName": rule.get("EventBusName", "default"),
                "Time": now,
                "Resources": [rule.get("Arn", "")],
                "Account": account_id,
                "Region": get_region(),
            }
            view = _pattern_event_view(event)
            for target in targets:
                try:
                    _invoke_target(target, event, rule, view)
                except Exception:
                    logger.exception(
                        "EventBridge scheduler: dispatch error for rule %s account %s",
                        rule_key, account_id,
                    )
        finally:
            set_request_account_id(previous_account)
            set_request_region(previous_region)


def _scheduler_loop():
    while True:
        time.sleep(_SCHEDULER_TICK_INTERVAL)
        try:
            _tick_scheduled_rules()
        except Exception:
            logger.exception("EventBridge scheduler tick error")


_scheduler_thread: "threading.Thread | None" = None


def start_scheduler() -> None:
    """Start the eb-scheduler daemon thread (idempotent). Called from the
    gateway lifespan.startup. Kept out of module-import scope so unit tests
    that patch ``_invoke_target`` don't race against a background tick."""
    global _scheduler_thread
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        return
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop, daemon=True, name="eb-scheduler"
    )
    _scheduler_thread.start()


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
    _rule_last_fired.clear()
    with _oauth_tokens_lock:
        _oauth_tokens.clear()
    # These memoize on text a caller sent — a pattern, a wildcard operand, a cidr
    # block — and on the epoch second an event carries, and nothing else evicts an
    # entry below the 1024-entry cap, so a reset that skipped them would keep
    # hundreds of megabytes of compiled regexes and expanded ``$or``s for rules it
    # just deleted. Clearing cannot change an answer: each is a pure function of
    # its key.
    _compiled_pattern.cache_clear()
    _wildcard_regex.cache_clear()
    _cidr_network.cache_clear()
    _iso_time.cache_clear()
    # The "default" bus is lazily recreated per-account on next access via
    # _ensure_default_bus(), so nothing to re-seed here.
