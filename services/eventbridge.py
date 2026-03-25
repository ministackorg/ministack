"""
EventBridge Service Emulator.
JSON-based API via X-Amz-Target (AmazonEventBridge).
Supports: CreateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus,
          PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule,
          PutTargets, RemoveTargets, ListTargetsByRule,
          PutEvents,
          TagResource, UntagResource, ListTagsForResource,
          CreateArchive, DeleteArchive, DescribeArchive, ListArchives,
          PutPermission, RemovePermission,
          CreateConnection, DescribeConnection, DeleteConnection, ListConnections, UpdateConnection,
          CreateApiDestination, DescribeApiDestination, DeleteApiDestination,
          ListApiDestinations, UpdateApiDestination.
"""

import json
import time
import hashlib
import logging
import re
from datetime import datetime, timezone

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("events")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


_event_buses: dict = {
    "default": {
        "Name": "default",
        "Arn": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/default",
        "CreationTime": _now_iso(),
    }
}
_rules: dict = {}
_targets: dict = {}
_events_log: list = []
_tags: dict = {}
_archives: dict = {}
_event_bus_policies: dict = {}  # bus_name -> {Statement: [...]}
_connections: dict = {}         # connection_name -> {...}
_api_destinations: dict = {}    # destination_name -> {...}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateEventBus": _create_event_bus,
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
        "PutEvents": _put_events,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "CreateArchive": _create_archive,
        "DeleteArchive": _delete_archive,
        "DescribeArchive": _describe_archive,
        "ListArchives": _list_archives,
        "PutPermission": _put_permission,
        "RemovePermission": _remove_permission,
        "CreateConnection": _create_connection,
        "DescribeConnection": _describe_connection,
        "DeleteConnection": _delete_connection,
        "ListConnections": _list_connections,
        "UpdateConnection": _update_connection,
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
    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/{name}"
    _event_buses[name] = {
        "Name": name,
        "Arn": arn,
        "CreationTime": _now_iso(),
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
            buses.append({
                "Name": b["Name"],
                "Arn": b["Arn"],
            })
    return json_response({"EventBuses": buses})


def _describe_event_bus(data):
    name = data.get("Name", "default")
    bus = _event_buses.get(name)
    if not bus:
        return error_response_json("ResourceNotFoundException", f"Event bus {name} not found", 400)
    return json_response({
        "Name": bus["Name"],
        "Arn": bus["Arn"],
        "CreationTime": bus["CreationTime"],
    })


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def _rule_arn(rule_name: str, bus_name: str) -> str:
    if bus_name == "default":
        return f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}"
    return f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{bus_name}/{rule_name}"


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
            f"Parameter ScheduleExpression is not valid.",
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
        "CreatedBy": ACCOUNT_ID,
        "CreationTime": existing.get("CreationTime", _now_iso()),
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


# ---------------------------------------------------------------------------
# PutEvents + event pattern matching + target dispatch
# ---------------------------------------------------------------------------

def _put_events(data):
    entries = data.get("Entries", [])
    results = []
    for entry in entries:
        event_id = new_uuid()
        bus_name = entry.get("EventBusName", "default")
        event_time = _now_iso()

        event_record = {
            "EventId": event_id,
            "Source": entry.get("Source", ""),
            "DetailType": entry.get("DetailType", ""),
            "Detail": entry.get("Detail", "{}"),
            "EventBusName": bus_name,
            "Time": event_time,
            "Resources": entry.get("Resources", []),
            "Account": ACCOUNT_ID,
            "Region": REGION,
        }
        _events_log.append(event_record)
        results.append({"EventId": event_id})
        logger.debug(f"EventBridge event: {entry.get('Source')} / {entry.get('DetailType')}")

        _dispatch_event(event_record)

    return json_response({"FailedEntryCount": 0, "Entries": results})


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
        if not _matches_field(event.get("Account", ACCOUNT_ID), pattern["account"]):
            return False

    if "region" in pattern:
        if not _matches_field(event.get("Region", REGION), pattern["region"]):
            return False

    if "resources" in pattern:
        event_resources = event.get("Resources", [])
        for required in pattern["resources"]:
            if required not in event_resources:
                return False

    return True


def _matches_field(value, pattern_values):
    if isinstance(pattern_values, list):
        return value in pattern_values
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
        "account": ACCOUNT_ID,
        "time": event["Time"],
        "region": REGION,
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
            logger.warning(f"EventBridge: unsupported target type for ARN {arn}")
    except Exception as e:
        logger.error(f"EventBridge target dispatch error for {arn}: {e}")


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
        "account": ACCOUNT_ID,
        "region": REGION,
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
    from services import lambda_svc

    parts = arn.split(":")
    func_name = parts[-1].split("/")[-1] if "/" in parts[-1] else parts[-1]
    if func_name.startswith("function:"):
        func_name = func_name[len("function:"):]

    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        event = {"body": payload}

    headers = {"x-amz-invocation-type": "Event"}
    result = lambda_svc._invoke(func_name, event, headers)
    logger.info(f"EventBridge → Lambda {func_name}: status={result[0] if result else 'unknown'}")


def _dispatch_to_sqs(arn, payload):
    from services import sqs as _sqs

    queue_name = arn.split(":")[-1]
    queue_url = _sqs._queue_url(queue_name)
    queue = _sqs._queues.get(queue_url)
    if not queue:
        logger.warning(f"EventBridge → SQS: queue {queue_name} not found")
        return

    msg_id = new_uuid()
    md5 = hashlib.md5(payload.encode()).hexdigest()
    queue["messages"].append({
        "id": msg_id,
        "body": payload,
        "md5": md5,
        "receipt_handle": None,
        "sent_at": time.time(),
        "visible_at": time.time(),
        "receive_count": 0,
        "attributes": {},
    })
    logger.info(f"EventBridge → SQS {queue_name}")


def _dispatch_to_sns(arn, payload):
    from services import sns as _sns

    topic = _sns._topics.get(arn)
    if not topic:
        logger.warning(f"EventBridge → SNS: topic {arn} not found")
        return

    msg_id = new_uuid()
    topic["messages"].append({
        "id": msg_id,
        "message": payload,
        "subject": "EventBridge Notification",
        "timestamp": time.time(),
    })
    _sns._fanout(arn, msg_id, payload, "EventBridge Notification")
    logger.info(f"EventBridge → SNS {arn}")


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
    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:archive/{name}"
    _archives[name] = {
        "ArchiveName": name,
        "ArchiveArn": arn,
        "EventSourceArn": source_arn,
        "Description": data.get("Description", ""),
        "EventPattern": data.get("EventPattern", ""),
        "RetentionDays": data.get("RetentionDays", 0),
        "State": "ENABLED",
        "CreationTime": _now_iso(),
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
# Permissions (resource policies)
# ---------------------------------------------------------------------------

def _put_permission(data):
    bus_name = data.get("EventBusName", "default")
    statement_id = data.get("StatementId")
    if not statement_id:
        return error_response_json("ValidationException", "StatementId is required", 400)

    if bus_name not in _event_bus_policies:
        _event_bus_policies[bus_name] = {"Version": "2012-10-17", "Statement": []}

    policy = _event_bus_policies[bus_name]
    policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != statement_id]

    statement = {
        "Sid": statement_id,
        "Effect": "Allow",
        "Principal": data.get("Principal", "*"),
        "Action": data.get("Action", "events:PutEvents"),
        "Resource": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/{bus_name}",
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

    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:connection/{name}"
    now = _now_iso()
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
        "LastModifiedTime": _now_iso(),
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
    now = _now_iso()
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

    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:api-destination/{name}"
    now = _now_iso()
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
    now = _now_iso()
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
    global _event_buses
    _rules.clear()
    _targets.clear()
    _events_log.clear()
    _tags.clear()
    _archives.clear()
    _event_bus_policies.clear()
    _connections.clear()
    _api_destinations.clear()
    _event_buses = {
        "default": {
            "Name": "default",
            "Arn": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/default",
            "CreationTime": _now_iso(),
        }
    }
