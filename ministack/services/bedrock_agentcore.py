# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Amazon Bedrock AgentCore emulator.

Covers the two AgentCore services, which both sign as ``bedrock-agentcore``:

  * ``bedrock-agentcore-control`` (rest-json) — agent runtime + endpoint control
    plane: CreateAgentRuntime, GetAgentRuntime, ListAgentRuntimes,
    UpdateAgentRuntime, DeleteAgentRuntime, ListAgentRuntimeVersions,
    CreateAgentRuntimeEndpoint, GetAgentRuntimeEndpoint,
    ListAgentRuntimeEndpoints, UpdateAgentRuntimeEndpoint,
    DeleteAgentRuntimeEndpoint, PutResourcePolicy, GetResourcePolicy,
    DeleteResourcePolicy.
  * ``bedrock-agentcore`` (rest-json) — data plane: InvokeAgentRuntime and
    the short-term/long-term Memory data plane: CreateEvent, GetEvent,
    ListEvents, DeleteEvent, ListActors, ListSessions,
    BatchCreateMemoryRecords, BatchUpdateMemoryRecords,
    BatchDeleteMemoryRecords, GetMemoryRecord, ListMemoryRecords,
    DeleteMemoryRecord, RetrieveMemoryRecords, StartMemoryExtractionJob,
    ListMemoryExtractionJobs.

Memory notes: events are recorded verbatim per (actor, session) with branch
and metadata filtering; extraction behind memory strategies does not run (the
control plane records strategies only), so long-term records exist only where
BatchCreateMemoryRecords puts them. RetrieveMemoryRecords has no embeddings
here: it scores records by the fraction of the search query's lowercase word
tokens present in the record text, drops zero-overlap records, and orders by
score.

Deterministic and stateful: resources provision instantly (``READY``) and
InvokeAgentRuntime returns a deterministic echo response, so teams can test
runtime lifecycle, endpoint wiring, resource-policy authorization, and Invoke
request/response contracts locally without live AWS.

Shapes, HTTP methods, URIs, ARN/ID patterns, and status enums are verified
against botocore ``bedrock-agentcore-control`` / ``bedrock-agentcore``
service-2.json.
"""
import asyncio
import base64
import copy
import datetime
import json
import logging
import os
import re
import secrets
import string
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import unquote

from ministack.core.responses import (
    AccountRegionScopedDict,
    StreamingResponse,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
    request_scope,
)

logger = logging.getLogger("bedrock_agentcore")

# ---------------------------------------------------------------------------
# State (account + region scoped)
# ---------------------------------------------------------------------------

_runtimes = AccountRegionScopedDict()    # agentRuntimeId -> runtime record
_endpoints = AccountRegionScopedDict()   # agentRuntimeId -> {endpointName -> endpoint record}
_resource_policies = AccountRegionScopedDict()  # resource ARN -> policy string
_memories = AccountRegionScopedDict()    # memoryId -> memory record
_memory_events = AccountRegionScopedDict()   # memoryId -> {actorId -> {sessionId -> session}}
_memory_records = AccountRegionScopedDict()  # memoryId -> {memoryRecordId -> record}
_extraction_jobs = AccountRegionScopedDict()  # memoryId -> [job metadata]
_containers = {}  # (account, region, runtime id, version) -> Docker container
_container_lock = threading.RLock()

# AgentRuntimeName / EndpointName: start with a letter, then letters/digits/_,
# up to 48 chars total (botocore pattern ^[a-zA-Z][a-zA-Z0-9_]{0,47}$).
_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,47}$")


def get_state():
    return copy.deepcopy({
        "runtimes": _runtimes,
        "endpoints": _endpoints,
        "resourcePolicies": _resource_policies,
        "memories": _memories,
        "memoryEvents": _memory_events,
        "memoryRecords": _memory_records,
        "extractionJobs": _extraction_jobs,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _runtimes.clear()
    _endpoints.clear()
    _resource_policies.clear()
    _memories.clear()
    _memory_events.clear()
    _memory_records.clear()
    _extraction_jobs.clear()
    _runtimes.update(data.get("runtimes", {}))
    _endpoints.update(data.get("endpoints", {}))
    _resource_policies.update(data.get("resourcePolicies", {}))
    _memories.update(data.get("memories", {}))
    _memory_events.update(data.get("memoryEvents", {}))
    _memory_records.update(data.get("memoryRecords", {}))
    _extraction_jobs.update(data.get("extractionJobs", {}))
    _migrate_legacy_arns()
    # Backfill one snapshot for state written before version history existed.
    for runtime in _runtimes._data.values():
        if not runtime.get("_versions"):
            runtime["_versions"] = {
                runtime.get("agentRuntimeVersion", "1"): _version_snapshot(runtime)
            }


def _migrate_legacy_arns():
    """State saved before runtimes had AWS-shaped ARNs (``agent/{uuid}:{version}``)."""
    for key, runtime in list(_runtimes._data.items()):
        old = runtime.get("agentRuntimeArn", "")
        if ":agent/" not in old:
            continue
        account_id, region = key[0], key[1]
        new = f"arn:aws:bedrock-agentcore:{region}:{account_id}:runtime/{runtime['agentRuntimeId']}"
        runtime["agentRuntimeArn"] = new
        runtime.pop("_uuid", None)
        endpoints = _endpoints.get_scoped(account_id, region, runtime["agentRuntimeId"]) or {}
        for name, endpoint in endpoints.items():
            endpoint["agentRuntimeArn"] = new
            endpoint["agentRuntimeEndpointArn"] = _endpoint_arn(new, name)
        if "DEFAULT" not in endpoints:
            with request_scope(account_id, region):
                endpoints["DEFAULT"] = _endpoint_record(runtime, "DEFAULT", runtime["agentRuntimeVersion"])
            _endpoints.set_scoped(account_id, region, runtime["agentRuntimeId"], endpoints)




def reset():
    with _container_lock:
        for container in _containers.values():
            _remove_container(container)
        _containers.clear()
    _runtimes.clear()
    _endpoints.clear()
    _resource_policies.clear()
    _memories.clear()
    _memory_events.clear()
    _memory_records.clear()
    _extraction_jobs.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rand_suffix() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(10))


def _resource_id(name: str) -> str:
    # botocore pattern: [a-zA-Z][a-zA-Z0-9_]{0,99}-[a-zA-Z0-9]{10}
    return f"{name}-{_rand_suffix()}"


def _runtime_arn(runtime_id: str) -> str:
    return f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:runtime/{runtime_id}"


def _endpoint_arn(runtime_arn: str, name: str) -> str:
    return f"{runtime_arn}/runtime-endpoint/{name}"


def _endpoint_record(runtime, name, version, description=""):
    now = now_iso()
    return {
        "name": name,
        "id": _resource_id(name),
        "agentRuntimeEndpointArn": _endpoint_arn(runtime["agentRuntimeArn"], name),
        "agentRuntimeArn": runtime["agentRuntimeArn"],
        "targetVersion": version,
        "liveVersion": version,
        "status": "READY",
        "description": description,
        "createdAt": now,
        "lastUpdatedAt": now,
    }


def _arn_owner(resource_arn: str) -> tuple[str, str] | None:
    """Return the account and region encoded in an ARN."""
    parts = resource_arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or not parts[3] or not parts[4]:
        return None
    return parts[4], parts[3]


def _resource_exists(resource_arn: str, account_id: str, region: str) -> bool:
    """Whether a Runtime or Endpoint with this exact ARN exists."""
    if any(r.get("agentRuntimeArn") == resource_arn
           for r in _runtimes.values_scoped(account_id, region)):
        return True
    for endpoints in _endpoints.values_scoped(account_id, region):
        if any(e.get("agentRuntimeEndpointArn") == resource_arn
               for e in endpoints.values()):
            return True
    return False


def _policy_validation(resource_arn: str, policy: str) -> str | None:
    """Validate AgentCore's resource-policy-specific constraints."""
    from ministack.core.iam_evaluator import validate_policy_document

    if len(policy) > 20_480:
        return "policy exceeds the maximum length of 20480 characters"
    error = validate_policy_document(policy)
    if error:
        return error
    try:
        document = json.loads(policy)
    except json.JSONDecodeError:
        return "Policy document is not valid JSON"
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for index, statement in enumerate(statements):
        if "Principal" not in statement:
            return f"Statement {index} must contain a Principal element"
        if "NotPrincipal" in statement:
            return "NotPrincipal is not supported by MiniStack AgentCore policies"
        resources = statement.get("Resource")
        if isinstance(resources, str):
            resources = [resources]
        if not isinstance(resources, list) or resources != [resource_arn]:
            return (
                f"Statement {index} Resource must contain exactly "
                f"{resource_arn}"
            )
    return None


def _put_resource_policy(resource_arn: str, body):
    data = _parse_body(body)
    policy = data.get("policy")
    owner = _arn_owner(resource_arn)
    if not isinstance(policy, str) or not policy:
        return _validation("policy is required")
    if owner is None:
        return _validation("resourceArn must be a valid ARN")
    account_id, region = owner
    if not _resource_exists(resource_arn, account_id, region):
        return _not_found(f"Resource {resource_arn} not found")
    error = _policy_validation(resource_arn, policy)
    if error:
        return _validation(error)
    _resource_policies.set_scoped(account_id, region, resource_arn, policy)
    return json_response({"policy": policy}, status=201)


def _get_resource_policy(resource_arn: str):
    owner = _arn_owner(resource_arn)
    if owner is None:
        return _validation("resourceArn must be a valid ARN")
    if not _resource_exists(resource_arn, *owner):
        return _not_found(f"Resource {resource_arn} not found")
    policy = _resource_policies.get_scoped(*owner, resource_arn)
    if policy is None:
        return _not_found(f"Resource policy for {resource_arn} not found")
    return json_response({"policy": policy})


def _delete_resource_policy(resource_arn: str):
    owner = _arn_owner(resource_arn)
    if owner is None:
        return _validation("resourceArn must be a valid ARN")
    if not _resource_exists(resource_arn, *owner):
        return _not_found(f"Resource {resource_arn} not found")
    deleted = _resource_policies.pop_scoped(*owner, resource_arn, None)
    if deleted is None:
        return _not_found(f"Resource policy for {resource_arn} not found")
    return 204, {"Content-Type": "application/json"}, b""


def _workload_identity_arn(name: str) -> str:
    return (f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:"
            f"workload-identity-directory/default/workload-identity/{name}")


def _iso(value):
    """Every timestamp this service answers is the model's DateTimestamp, which
    carries timestampFormat iso8601 -- an RFC 3339 string, not an epoch number.
    Records persisted before this took epoch floats, so those are converted on
    the way out."""
    if isinstance(value, (int, float)):
        return (datetime.datetime.fromtimestamp(value, datetime.timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
    return value


def _validation(message: str):
    return error_response_json("ValidationException", message, 400)


def _runtime_not_found(runtime_id):
    return _not_found(f"Agent '{runtime_id}' was not found. Please check the agent ID and try again.")


def _not_found(message: str):
    return error_response_json("ResourceNotFoundException", message, 404)


def _conflict(message: str):
    return error_response_json("ConflictException", message, 409)


def _parse_body(body) -> dict:
    if not body:
        return {}
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return {}


def _agentcore_query_value(query_params, key, default=None):
    value = (query_params or {}).get(key, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _paginate_agentcore_results(items, query_params):
    raw_limit = _agentcore_query_value(query_params, "maxResults", "10")
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return None, _validation("maxResults must be an integer from 1 to 100")
    if not 1 <= limit <= 100:
        return None, _validation("maxResults must be an integer from 1 to 100")

    token = _agentcore_query_value(query_params, "nextToken")
    offset = 0
    if token is not None:
        if not isinstance(token, str) or not token or len(token) > 2048:
            return None, _validation("nextToken is invalid")
        try:
            padded = token + "=" * (-len(token) % 4)
            token_bytes = base64.b64decode(
                padded.encode(), altchars=b"-_", validate=True
            )
            offset = int(token_bytes.decode())
            if offset < 0:
                raise ValueError
        except (ValueError, TypeError, base64.binascii.Error):
            return None, _validation("nextToken is invalid")

    page = {"items": items[offset:offset + limit]}
    if offset + limit < len(items):
        page["nextToken"] = base64.urlsafe_b64encode(
            str(offset + limit).encode()
        ).decode().rstrip("=")
    return page, None


def _memory_arn(memory_id):
    return (f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:"
            f"memory/{memory_id}")


def _memory_public_record(record):
    return {key: copy.deepcopy(value) for key, value in record.items()
            if not key.startswith("_")}


_MEMORY_ID_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9-_]{0,99}-[a-zA-Z0-9]{10}")

# MemoryStrategyInput union member -> MemoryStrategyType.
_MEMORY_STRATEGY_TYPES = {
    "semanticMemoryStrategy": "SEMANTIC",
    "summaryMemoryStrategy": "SUMMARIZATION",
    "userPreferenceMemoryStrategy": "USER_PREFERENCE",
    "customMemoryStrategy": "CUSTOM",
    "episodicMemoryStrategy": "EPISODIC",
}


def _memory_strategies(inputs, now):
    """MemoryStrategy records for a MemoryStrategyInputList, or an error response."""
    if not isinstance(inputs, list):
        return None, _validation("memoryStrategies must be a list")
    strategies = []
    for item in inputs:
        members = [key for key in item if key in _MEMORY_STRATEGY_TYPES] if isinstance(item, dict) else []
        if len(members) != 1 or len(item) != 1:
            return None, _validation("Each memory strategy must set exactly one strategy type")
        spec = item[members[0]]
        name = spec.get("name") if isinstance(spec, dict) else None
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            return None, _validation("Memory strategy name is invalid")
        strategy = {
            "strategyId": _resource_id(name),
            "name": name,
            "type": _MEMORY_STRATEGY_TYPES[members[0]],
            "namespaces": copy.deepcopy(spec.get("namespaces", [])),
            "namespaceTemplates": copy.deepcopy(spec.get("namespaceTemplates", [])),
            "status": "ACTIVE",
            "createdAt": now,
            "updatedAt": now,
        }
        for field in ("description", "memoryRecordSchema"):
            if field in spec:
                strategy[field] = copy.deepcopy(spec[field])
        strategies.append(strategy)
    return strategies, None


def _modify_memory_strategies(record, changes, now):
    """Apply ModifyMemoryStrategies to a copy of the record's strategies."""
    if not isinstance(changes, dict):
        return None, _validation("memoryStrategies must be an object")
    strategies = copy.deepcopy(record.get("strategies", []))
    by_id = {strategy["strategyId"]: strategy for strategy in strategies}
    for item in changes.get("deleteMemoryStrategies") or []:
        strategy_id = (item or {}).get("memoryStrategyId")
        if strategy_id not in by_id:
            return None, _not_found(f"Memory strategy '{strategy_id}' not found")
        strategies.remove(by_id.pop(strategy_id))
    for item in changes.get("modifyMemoryStrategies") or []:
        strategy_id = (item or {}).get("memoryStrategyId")
        if strategy_id not in by_id:
            return None, _not_found(f"Memory strategy '{strategy_id}' not found")
        for field in ("description", "namespaces", "namespaceTemplates", "memoryRecordSchema"):
            if field in item:
                by_id[strategy_id][field] = copy.deepcopy(item[field])
        by_id[strategy_id]["updatedAt"] = now
    added, error = _memory_strategies(changes.get("addMemoryStrategies") or [], now)
    if error:
        return None, error
    return strategies + added, None


def _memory_page(items, data, default=20):
    query = {"maxResults": data.get("maxResults", default)}
    if "nextToken" in data:
        query["nextToken"] = data["nextToken"]
    return _paginate_agentcore_results(items, query)


def _create_memory(body):
    data = _parse_body(body)
    name = data.get("name")
    duration = data.get("eventExpiryDuration")
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        return _validation("name must start with a letter and contain only letters, digits, and underscores")
    if any(memory.get("name") == name for memory in _memories.values()):
        return _conflict(f"A memory with name '{name}' already exists")
    if not isinstance(duration, int) or isinstance(duration, bool) or not 3 <= duration <= 365:
        return _validation("eventExpiryDuration must be an integer from 3 to 365")
    tags = data.get("tags") or {}
    if not isinstance(tags, dict) or len(tags) > 50:
        return _validation("tags must be a map of at most 50 entries")
    now = int(time.time())
    # Strategies are recorded; no extraction runs behind them.
    strategies, error = _memory_strategies(data.get("memoryStrategies") or [], now)
    if error:
        return error

    memory_id = _resource_id(name)
    record = {
        "id": memory_id,
        "arn": _memory_arn(memory_id),
        "name": name,
        "eventExpiryDuration": duration,
        "status": "ACTIVE",
        "createdAt": now,
        "updatedAt": now,
        "strategies": strategies,
        "_tags": copy.deepcopy(tags),
    }
    for field in ("description", "encryptionKeyArn", "memoryExecutionRoleArn",
                  "indexedKeys", "namespaceKeys", "streamDeliveryResources"):
        if field in data:
            record[field] = copy.deepcopy(data[field])
    _memories[memory_id] = record

    response = _memory_public_record(record)
    response["status"] = "CREATING"
    return json_response({"memory": response}, 202)


def _get_memory(memory_id, query_params):
    record = _memories.get(memory_id)
    if record is None:
        return _not_found(f"Memory '{memory_id}' not found")
    view = _agentcore_query_value(query_params, "view", "full")
    if view not in ("full", "without_decryption"):
        return _validation("view must be 'full' or 'without_decryption'")
    return json_response({"memory": _memory_public_record(record)})


def _list_memories(body):
    data = _parse_body(body)
    items = [{key: record[key] for key in (
        "arn", "createdAt", "id", "status", "updatedAt"
    ) if key in record} for record in _memories.values()]
    page, error = _memory_page(items, data, default=10)
    if error:
        return error
    return json_response({
        "memories": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _update_memory(memory_id, body):
    record = _memories.get(memory_id)
    if record is None:
        return _not_found(f"Memory '{memory_id}' not found")
    data = _parse_body(body)
    now = int(time.time())
    if "eventExpiryDuration" in data:
        duration = data["eventExpiryDuration"]
        if not isinstance(duration, int) or isinstance(duration, bool) or not 3 <= duration <= 365:
            return _validation("eventExpiryDuration must be an integer from 3 to 365")
    if "memoryStrategies" in data:
        strategies, error = _modify_memory_strategies(record, data["memoryStrategies"], now)
        if error:
            return error
        record["strategies"] = strategies
    if "eventExpiryDuration" in data:
        record["eventExpiryDuration"] = data["eventExpiryDuration"]
    if "description" in data:
        record["description"] = data["description"]
    for field in ("memoryExecutionRoleArn", "namespaceKeys", "streamDeliveryResources"):
        if field in data:
            record[field] = copy.deepcopy(data[field])
    if data.get("addIndexedKeys"):
        existing = {item.get("key") for item in record.get("indexedKeys", [])}
        record.setdefault("indexedKeys", []).extend(
            copy.deepcopy(item) for item in data["addIndexedKeys"]
            if item.get("key") not in existing
        )
    record["updatedAt"] = now
    response = _memory_public_record(record)
    response["status"] = "UPDATING"
    return json_response({"memory": response}, 202)


def _delete_memory(memory_id):
    record = _memories.pop(memory_id, None)
    if record is None:
        return _not_found(f"Memory '{memory_id}' not found")
    _memory_events.pop(memory_id, None)
    _memory_records.pop(memory_id, None)
    _extraction_jobs.pop(memory_id, None)
    return json_response({"memoryId": memory_id, "status": "DELETING"}, 202)


# ---------------------------------------------------------------------------
# Memory data plane (bedrock-agentcore)
# ---------------------------------------------------------------------------

_ACTOR_ID_RE = re.compile(
    r"[a-zA-Z0-9][a-zA-Z0-9-_/]*(?::[a-zA-Z0-9-_/]+)*[a-zA-Z0-9-_/]*")
_SESSION_ID_RE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9-_]*")
_EVENT_ID_RE = re.compile(r"[0-9]+#[a-fA-F0-9]+")
_NAMESPACE_RE = re.compile(
    r"[a-zA-Z0-9/*][a-zA-Z0-9-_/*]*(?::[a-zA-Z0-9-_/*]+)*[a-zA-Z0-9-_/*]*")
_RECORD_ID_RE = re.compile(r"mem-[a-zA-Z0-9-_]*")
_REQUEST_ID_RE = re.compile(r"[a-zA-Z0-9_-]+")

_PAYLOAD_UNION_KEYS = ("conversational", "blob", "json")


def _mem_get(store, key, scope, default=None):
    return store.get_scoped(*scope, key, default) if scope else store.get(key, default)


def _mem_set(store, key, scope, value):
    if scope:
        store.set_scoped(*scope, key, value)
    else:
        store[key] = value


def _resolve_memory(raw_id):
    """(memory_id, owner_scope, error) for a path ``memoryId``.

    The path label accepts the bare id or the full resource ARN; with an ARN
    the memory and its data live under the ARN owner's account and region."""
    memory_id = unquote(raw_id)
    scope = None
    if ":memory/" in memory_id:
        scope = _arn_owner(memory_id)
        memory_id = memory_id.rsplit(":memory/", 1)[1]
    if not _MEMORY_ID_RE.fullmatch(memory_id):
        return None, None, _validation(
            f"1 validation error detected: Value '{memory_id}' at 'memoryId' "
            f"failed to satisfy constraint: Member must satisfy regular "
            f"expression pattern: {_MEMORY_ID_RE.pattern}")
    record = (_memories.get_scoped(*scope, memory_id) if scope
              else _memories.get(memory_id))
    if record is None:
        return None, None, _not_found(f"Memory '{memory_id}' not found")
    return memory_id, scope, None


def _public_event(event):
    return {key: copy.deepcopy(value) for key, value in event.items()
            if not key.startswith("_")}


def _events_bucket(memory_id, scope):
    return _mem_get(_memory_events, memory_id, scope, {})


def _create_event(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    actor_id = data.get("actorId")
    if not isinstance(actor_id, str) or not _ACTOR_ID_RE.fullmatch(actor_id):
        return _validation(
            f"1 validation error detected: Value '{actor_id}' at 'actorId' "
            f"failed to satisfy constraint: Member must satisfy regular "
            f"expression pattern: {_ACTOR_ID_RE.pattern}")
    session_id = data.get("sessionId")
    if session_id is None:
        session_id = f"session-{_rand_suffix()}"
    if (not isinstance(session_id, str)
            or not _SESSION_ID_RE.fullmatch(session_id)):
        return _validation(
            f"1 validation error detected: Value '{session_id}' at "
            f"'sessionId' failed to satisfy constraint: Member must satisfy "
            f"regular expression pattern: {_SESSION_ID_RE.pattern}")
    if "eventTimestamp" not in data:
        return _validation("eventTimestamp is required")
    payload = data.get("payload")
    if not isinstance(payload, list) or not payload:
        return _validation("payload must be a non-empty list")
    for item in payload:
        if (not isinstance(item, dict) or len(item) != 1
                or next(iter(item)) not in _PAYLOAD_UNION_KEYS):
            return _validation(
                "payload entries must set exactly one of conversational, "
                "blob, or json")

    bucket = _events_bucket(memory_id, scope)
    token = data.get("clientToken")
    if isinstance(token, str) and token:
        for sessions in bucket.values():
            for session in sessions.values():
                for existing in session["events"]:
                    if existing.get("_clientToken") == token:
                        return json_response({"event": _public_event(existing)})

    session = bucket.setdefault(actor_id, {}).get(session_id)
    if session is None:
        session = {"createdAt": time.time(), "nextSeq": 0, "events": []}
        bucket[actor_id][session_id] = session
    session["nextSeq"] += 1
    event = {
        "memoryId": memory_id,
        "actorId": actor_id,
        "sessionId": session_id,
        "eventId": f"{session['nextSeq']}#{secrets.token_hex(8)}",
        "eventTimestamp": data["eventTimestamp"],
        "payload": copy.deepcopy(payload),
        "_createdAt": time.time(),
        "_seq": session["nextSeq"],
    }
    for field in ("branch", "metadata"):
        if field in data:
            event[field] = copy.deepcopy(data[field])
    if isinstance(token, str) and token:
        event["_clientToken"] = token
    session["events"].append(event)
    _mem_set(_memory_events, memory_id, scope, bucket)
    return json_response({"event": _public_event(event)})


def _find_event(memory_id, scope, actor_id, session_id, event_id):
    bucket = _events_bucket(memory_id, scope)
    session = bucket.get(actor_id, {}).get(session_id)
    if session is None:
        return None
    for event in session["events"]:
        if event["eventId"] == event_id:
            return event
    return None


def _get_event(raw_memory_id, actor_id, session_id, event_id):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    event = _find_event(memory_id, scope, unquote(actor_id),
                        unquote(session_id), unquote(event_id))
    if event is None:
        return _not_found(f"Event '{unquote(event_id)}' not found")
    return json_response({"event": _public_event(event)})


def _delete_event(raw_memory_id, actor_id, session_id, event_id):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    actor_id, session_id, event_id = (
        unquote(actor_id), unquote(session_id), unquote(event_id))
    bucket = _events_bucket(memory_id, scope)
    session = bucket.get(actor_id, {}).get(session_id)
    events = [] if session is None else session["events"]
    remaining = [event for event in events if event["eventId"] != event_id]
    if len(remaining) == len(events):
        return _not_found(f"Event '{event_id}' not found")
    session["events"] = remaining
    _mem_set(_memory_events, memory_id, scope, bucket)
    return json_response({"eventId": event_id})


def _event_metadata_matches(metadata, expressions):
    for expression in expressions:
        key = (expression.get("left") or {}).get("metadataKey")
        operator = expression.get("operator")
        present = key in metadata
        if operator == "EXISTS":
            if not present:
                return False
        elif operator == "NOT_EXISTS":
            if present:
                return False
        elif operator == "EQUALS_TO":
            right = ((expression.get("right") or {}).get("metadataValue")
                     or {}).get("stringValue")
            if not present or (metadata[key] or {}).get("stringValue") != right:
                return False
        else:
            return False
    return True


def _list_events(raw_memory_id, actor_id, session_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    bucket = _events_bucket(memory_id, scope)
    session = bucket.get(unquote(actor_id), {}).get(unquote(session_id))
    events = list(session["events"]) if session else []

    branch = (data.get("filter") or {}).get("branch")
    if branch and branch.get("name"):
        name = branch["name"]
        if branch.get("includeParentBranches"):
            by_id = {event["eventId"]: event for event in events}
            keep = set()
            for event in events:
                if (event.get("branch") or {}).get("name") != name:
                    continue
                keep.add(event["eventId"])
                root = (event.get("branch") or {}).get("rootEventId")
                while root in by_id and root not in keep:
                    keep.add(root)
                    root = (by_id[root].get("branch") or {}).get("rootEventId")
            events = [event for event in events if event["eventId"] in keep]
        else:
            events = [event for event in events
                      if (event.get("branch") or {}).get("name") == name]
    expressions = (data.get("filter") or {}).get("eventMetadata") or []
    if expressions:
        events = [event for event in events
                  if _event_metadata_matches(event.get("metadata") or {},
                                             expressions)]

    include_payloads = data.get("includePayloads") is not False
    items = []
    for event in events:
        public = _public_event(event)
        if not include_payloads:
            public.pop("payload", None)
        items.append(public)
    page, error = _memory_page(items, data)
    if error:
        return error
    return json_response({
        "events": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _list_actors(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    items = [{"actorId": actor_id}
             for actor_id in _events_bucket(memory_id, scope)]
    page, error = _memory_page(items, data)
    if error:
        return error
    return json_response({
        "actorSummaries": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _list_sessions(raw_memory_id, actor_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    actor_id = unquote(actor_id)
    sessions = _events_bucket(memory_id, scope).get(actor_id, {})
    only_with_events = (
        (data.get("filter") or {}).get("eventFilter") == "HAS_EVENTS")
    items = [
        {"sessionId": session_id, "actorId": actor_id,
         "createdAt": session["createdAt"]}
        for session_id, session in sessions.items()
        if session["events"] or not only_with_events
    ]
    page, error = _memory_page(items, data)
    if error:
        return error
    return json_response({
        "sessionSummaries": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


# --- long-term memory records ----------------------------------------------

def _record_id():
    return "mem-" + "".join(
        secrets.choice(string.ascii_letters + string.digits + "-_")
        for _ in range(40))


def _records_bucket(memory_id, scope):
    return _mem_get(_memory_records, memory_id, scope, {})


def _record_summary(record, score=None):
    summary = {key: copy.deepcopy(value) for key, value in record.items()
               if not key.startswith("_")}
    if score is not None:
        summary["score"] = score
    return summary


def _namespace_matches(pattern, namespaces):
    if not pattern:
        return True
    regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
    return any(re.match(regex, namespace or "")
               for namespace in namespaces)


def _ts_value(value):
    """Comparable epoch float for a metadata timestamp, or None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(
                value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _meta_scalar(value):
    """A MemoryRecordMetadataValue union member reduced to a python value."""
    if not isinstance(value, dict):
        return None
    if "stringValue" in value:
        return value["stringValue"]
    if "stringListValue" in value:
        return value["stringListValue"]
    if "numberValue" in value:
        return value["numberValue"]
    if "dateTimeValue" in value:
        return _ts_value(value["dateTimeValue"])
    return None


def _compare_meta(left, right, operator):
    if left is None or right is None:
        return False
    if operator == "EQUALS_TO":
        if isinstance(left, list):
            return right in left or left == right
        return left == right
    if operator == "CONTAINS":
        if isinstance(left, str) and isinstance(right, str):
            return right in left
        if isinstance(left, list):
            return right in left
        return False
    left_ts, right_ts = _ts_value(left), _ts_value(right)
    if isinstance(left, (int, float)) and not isinstance(left, bool) \
            and isinstance(right, (int, float)) and not isinstance(right, bool):
        left_ts, right_ts = float(left), float(right)
    if left_ts is None or right_ts is None:
        return False
    if operator == "BEFORE":
        return left_ts < right_ts
    if operator == "AFTER":
        return left_ts > right_ts
    if operator == "GREATER_THAN":
        return left_ts > right_ts
    if operator == "GREATER_THAN_OR_EQUALS":
        return left_ts >= right_ts
    if operator == "LESS_THAN":
        return left_ts < right_ts
    if operator == "LESS_THAN_OR_EQUALS":
        return left_ts <= right_ts
    return False


def _metadata_filters_match(metadata, expressions):
    for expression in expressions:
        if not isinstance(expression, dict):
            return False
        key = (expression.get("left") or {}).get("metadataKey")
        operator = expression.get("operator")
        present = key in metadata
        if operator == "EXISTS":
            if not present:
                return False
            continue
        if operator == "NOT_EXISTS":
            if present:
                return False
            continue
        right = _meta_scalar(
            (expression.get("right") or {}).get("metadataValue"))
        if not present or not _compare_meta(
                _meta_scalar(metadata.get(key)), right, operator):
            return False
    return True


def _valid_namespaces(namespaces):
    return (isinstance(namespaces, list) and len(namespaces) <= 1
            and all(isinstance(ns, str) and _NAMESPACE_RE.fullmatch(ns)
                    for ns in namespaces))


def _batch_result(record_id, status, request_id=None, error=None):
    result = {"memoryRecordId": record_id, "status": status}
    if request_id is not None:
        result["requestIdentifier"] = request_id
    if error:
        result["errorCode"] = 400
        result["errorMessage"] = error
    return result


def _batch_create_memory_records(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    entries = data.get("records")
    if not isinstance(entries, list) or not entries:
        return _validation("records must be a non-empty list")
    bucket = dict(_records_bucket(memory_id, scope))
    succeeded, failed = [], []
    for entry in entries:
        request_id = entry.get("requestIdentifier") if isinstance(entry, dict) else None
        record_id = _record_id()
        problem = None
        if not isinstance(entry, dict):
            problem = "record must be an object"
        elif not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
            problem = "requestIdentifier must match pattern [a-zA-Z0-9_-]+"
        elif not _valid_namespaces(entry.get("namespaces")):
            problem = "namespaces must be a list of at most one valid namespace"
        elif not isinstance(entry.get("content"), dict) \
                or not isinstance(entry["content"].get("text"), str) \
                or not 1 <= len(entry["content"]["text"]) <= 16000:
            problem = "content must be an object with a 'text' string of 1 to 16000 characters"
        elif "timestamp" not in entry:
            problem = "timestamp is required"
        elif "memoryStrategyId" in entry and (
                not isinstance(entry["memoryStrategyId"], str)
                or not 1 <= len(entry["memoryStrategyId"]) <= 100):
            problem = "memoryStrategyId must be a string of 1 to 100 characters"
        if problem:
            failed.append(_batch_result(
                record_id, "FAILED", request_id, problem))
            continue
        record = {
            "memoryRecordId": record_id,
            "content": copy.deepcopy(entry["content"]),
            "memoryStrategyId": entry.get("memoryStrategyId", ""),
            "namespaces": copy.deepcopy(entry["namespaces"]),
            "createdAt": entry["timestamp"],
        }
        if "metadata" in entry:
            record["metadata"] = copy.deepcopy(entry["metadata"])
        bucket[record_id] = record
        succeeded.append(_batch_result(record_id, "SUCCEEDED", request_id))
    _mem_set(_memory_records, memory_id, scope, bucket)
    return json_response(
        {"successfulRecords": succeeded, "failedRecords": failed})


def _batch_update_memory_records(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    entries = data.get("records")
    if not isinstance(entries, list) or not entries:
        return _validation("records must be a non-empty list")
    bucket = dict(_records_bucket(memory_id, scope))
    succeeded, failed = [], []
    for entry in entries:
        record_id = entry.get("memoryRecordId") if isinstance(entry, dict) else None
        problem = None
        if not isinstance(entry, dict):
            problem = "record must be an object"
        elif not isinstance(record_id, str) or not record_id:
            problem = "memoryRecordId is required"
        elif "timestamp" not in entry:
            problem = "timestamp is required"
        elif record_id not in bucket:
            problem = f"Memory record '{record_id}' not found"
        elif "namespaces" in entry and not _valid_namespaces(entry["namespaces"]):
            problem = "namespaces must be a list of at most one valid namespace"
        if problem:
            failed.append(_batch_result(
                record_id or "", "FAILED",
                entry.get("requestIdentifier") if isinstance(entry, dict) else None,
                problem))
            continue
        record = dict(bucket[record_id])
        for field in ("content", "namespaces", "memoryStrategyId", "metadata"):
            if field in entry:
                record[field] = copy.deepcopy(entry[field])
        record["_updatedAt"] = entry["timestamp"]
        bucket[record_id] = record
        succeeded.append(_batch_result(
            record_id, "SUCCEEDED", entry.get("requestIdentifier")))
    _mem_set(_memory_records, memory_id, scope, bucket)
    return json_response(
        {"successfulRecords": succeeded, "failedRecords": failed})


def _batch_delete_memory_records(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    entries = data.get("records")
    if not isinstance(entries, list) or not entries:
        return _validation("records must be a non-empty list")
    bucket = dict(_records_bucket(memory_id, scope))
    succeeded, failed = [], []
    for entry in entries:
        record_id = entry.get("memoryRecordId") if isinstance(entry, dict) else None
        if not isinstance(record_id, str) or not record_id:
            failed.append(_batch_result("", "FAILED", None,
                                        "memoryRecordId is required"))
        elif record_id not in bucket:
            failed.append(_batch_result(
                record_id, "FAILED", entry.get("requestIdentifier"),
                f"Memory record '{record_id}' not found"))
        else:
            del bucket[record_id]
            succeeded.append(_batch_result(
                record_id, "SUCCEEDED", entry.get("requestIdentifier")))
    _mem_set(_memory_records, memory_id, scope, bucket)
    return json_response(
        {"successfulRecords": succeeded, "failedRecords": failed})


def _find_memory_record(memory_id, scope, record_id, namespace=None):
    record = _records_bucket(memory_id, scope).get(record_id)
    if record is None:
        return None
    if namespace and namespace not in (record.get("namespaces") or []):
        return None
    return record


def _get_memory_record(raw_memory_id, record_id, query_params):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    record_id = unquote(record_id)
    namespace = _agentcore_query_value(query_params, "namespace")
    record = _find_memory_record(memory_id, scope, record_id, namespace)
    if record is None:
        return _not_found(f"Memory record '{record_id}' not found")
    return json_response({"memoryRecord": _record_summary(record)})


def _delete_memory_record(raw_memory_id, record_id, query_params):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    record_id = unquote(record_id)
    namespace = _agentcore_query_value(query_params, "namespace")
    record = _find_memory_record(memory_id, scope, record_id, namespace)
    if record is None:
        return _not_found(f"Memory record '{record_id}' not found")
    bucket = dict(_records_bucket(memory_id, scope))
    del bucket[record_id]
    _mem_set(_memory_records, memory_id, scope, bucket)
    return json_response({"memoryRecordId": record_id})


def _list_memory_records(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    namespace = data.get("namespace") or data.get("namespacePath")
    items = []
    for record in _records_bucket(memory_id, scope).values():
        if data.get("memoryStrategyId") and \
                record.get("memoryStrategyId") != data["memoryStrategyId"]:
            continue
        if not _namespace_matches(namespace, record.get("namespaces") or []):
            continue
        if data.get("metadataFilters") and not _metadata_filters_match(
                record.get("metadata") or {}, data["metadataFilters"]):
            continue
        items.append(_record_summary(record))
    page, error = _memory_page(items, data)
    if error:
        return error
    return json_response({
        "memoryRecordSummaries": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _retrieve_memory_records(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    criteria = data.get("searchCriteria")
    if not isinstance(criteria, dict) \
            or not isinstance(criteria.get("searchQuery"), str) \
            or not criteria["searchQuery"]:
        return _validation("searchCriteria.searchQuery is required")
    namespace = data.get("namespace") or data.get("namespacePath")
    terms = set(re.findall(r"[a-z0-9]+", criteria["searchQuery"].lower()))
    scored = []
    for record in _records_bucket(memory_id, scope).values():
        if criteria.get("memoryStrategyId") and \
                record.get("memoryStrategyId") != criteria["memoryStrategyId"]:
            continue
        if not _namespace_matches(namespace, record.get("namespaces") or []):
            continue
        if criteria.get("metadataFilters") and not _metadata_filters_match(
                record.get("metadata") or {}, criteria["metadataFilters"]):
            continue
        text = (record.get("content") or {}).get("text") or ""
        if not isinstance(text, str):
            text = str(text)
        hits = terms & set(re.findall(r"[a-z0-9]+", text.lower()))
        score = len(hits) / len(terms) if terms else 0.0
        if score <= 0:
            continue
        scored.append(_record_summary(record, score))
    scored.sort(key=lambda item: -item["score"])
    top_k = criteria.get("topK")
    if isinstance(top_k, int) and not isinstance(top_k, bool) and top_k >= 1:
        scored = scored[:top_k]
    page, error = _memory_page(scored, data)
    if error:
        return error
    return json_response({
        "memoryRecordSummaries": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


# --- extraction jobs (registry only; extraction itself does not run) --------

def _jobs_bucket(memory_id, scope):
    return _mem_get(_extraction_jobs, memory_id, scope, [])


def _start_extraction_job(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    job = data.get("extractionJob")
    job_id = (job or {}).get("jobId") if isinstance(job, dict) else None
    if not isinstance(job_id, str) or not job_id:
        return _validation("extractionJob.jobId is required")
    jobs = list(_jobs_bucket(memory_id, scope))
    if not any(existing["jobID"] == job_id for existing in jobs):
        jobs.append({"jobID": job_id, "status": "COMPLETED",
                     "messages": {"messagesList": []}})
        _mem_set(_extraction_jobs, memory_id, scope, jobs)
    return json_response({"jobId": job_id})


def _list_extraction_jobs(raw_memory_id, body):
    memory_id, scope, error = _resolve_memory(raw_memory_id)
    if error:
        return error
    data = _parse_body(body)
    filters = data.get("filter") or {}
    items = []
    for job in _jobs_bucket(memory_id, scope):
        if filters.get("status") and job.get("status") != filters["status"]:
            continue
        if filters.get("strategyId") and \
                job.get("strategyId") != filters["strategyId"]:
            continue
        if filters.get("sessionId") and \
                job.get("sessionId") != filters["sessionId"]:
            continue
        if filters.get("actorId") and job.get("actorId") != filters["actorId"]:
            continue
        items.append(copy.deepcopy(job))
    page, error = _memory_page(items, data)
    if error:
        return error
    return json_response({
        "jobs": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _memory_data_plane(method, rest, body, query_params):
    """Dispatch ``/memories/...`` data-plane paths; None when not a match.

    ``memoryId`` and ``actorId`` may carry literal ``/`` (an ARN and the
    ActorId pattern allow it), so every split anchors on a fixed suffix."""
    if "/actor/" in rest:
        memory_id, _, actor_rest = rest.partition("/actor/")
        if actor_rest.endswith("/sessions"):
            if method == "POST":
                return _list_sessions(
                    memory_id, actor_rest[:-len("/sessions")], body)
            return None
        actor_id, sep, tail = actor_rest.rpartition("/sessions/")
        if sep:
            if "/events/" in tail:
                session_id, _, event_id = tail.partition("/events/")
                if "/" in event_id:
                    return None
                if method == "GET":
                    return _get_event(memory_id, actor_id, session_id, event_id)
                if method == "DELETE":
                    return _delete_event(
                        memory_id, actor_id, session_id, event_id)
                return None
            if "/" not in tail and method == "POST":
                return _list_events(memory_id, actor_id, tail, body)
        return None
    if rest.endswith("/extractionJobs/start"):
        if method == "POST":
            return _start_extraction_job(
                rest[:-len("/extractionJobs/start")], body)
        return None
    if rest.endswith("/extractionJobs"):
        if method == "POST":
            return _list_extraction_jobs(rest[:-len("/extractionJobs")], body)
        return None
    if rest.endswith("/events"):
        if method == "POST":
            return _create_event(rest[:-len("/events")], body)
        return None
    if rest.endswith("/actors"):
        if method == "POST":
            return _list_actors(rest[:-len("/actors")], body)
        return None
    if rest.endswith("/retrieve"):
        if method == "POST":
            return _retrieve_memory_records(rest[:-len("/retrieve")], body)
        return None
    for suffix, handler in (
            ("/memoryRecords/batchCreate", _batch_create_memory_records),
            ("/memoryRecords/batchUpdate", _batch_update_memory_records),
            ("/memoryRecords/batchDelete", _batch_delete_memory_records)):
        if rest.endswith(suffix):
            if method == "POST":
                return handler(rest[:-len(suffix)], body)
            return None
    if rest.endswith("/memoryRecords"):
        if method == "POST":
            return _list_memory_records(rest[:-len("/memoryRecords")], body)
        return None
    if "/memoryRecord/" in rest:
        memory_id, _, record_id = rest.partition("/memoryRecord/")
        if "/" not in record_id and method == "GET":
            return _get_memory_record(memory_id, record_id, query_params)
        return None
    if "/memoryRecords/" in rest:
        memory_id, _, record_id = rest.partition("/memoryRecords/")
        if "/" not in record_id and method == "DELETE":
            return _delete_memory_record(memory_id, record_id, query_params)
        return None
    return None


def _version_snapshot(runtime):
    return {key: copy.deepcopy(value) for key, value in runtime.items()
            if not key.startswith("_")}


def _runtime_versions(runtime):
    versions = runtime.get("_versions")
    if versions:
        return versions
    current = runtime.get("agentRuntimeVersion", "1")
    return {current: _version_snapshot(runtime)}


def _version_summary(runtime, version):
    snapshot = _runtime_versions(runtime)[version]
    return {
        "agentRuntimeArn": runtime["agentRuntimeArn"],
        "agentRuntimeId": runtime["agentRuntimeId"],
        "agentRuntimeVersion": version,
        "agentRuntimeName": snapshot["agentRuntimeName"],
        "description": snapshot.get("description", ""),
        "lastUpdatedAt": _iso(snapshot["lastUpdatedAt"]),
        "status": snapshot["status"],
    }


# ---------------------------------------------------------------------------
# Control plane — AgentRuntime
# ---------------------------------------------------------------------------

def _create_agent_runtime(body):
    data = _parse_body(body)
    name = data.get("agentRuntimeName")
    if not name or not _NAME_RE.match(name):
        return _validation("agentRuntimeName must match ^[a-zA-Z][a-zA-Z0-9_]{0,47}$")
    for field in ("agentRuntimeArtifact", "roleArn", "networkConfiguration"):
        if not data.get(field):
            return _validation(f"{field} is required")
    if any(r.get("agentRuntimeName") == name for r in _runtimes.values()):
        return _conflict(f"Agent runtime with name {name} already exists")

    runtime_id = _resource_id(name)
    version = "1"
    now = now_iso()
    arn = _runtime_arn(runtime_id)
    workload = {"workloadIdentityArn": _workload_identity_arn(name)}
    record = {
        "agentRuntimeArn": arn,
        "agentRuntimeName": name,
        "agentRuntimeId": runtime_id,
        "agentRuntimeVersion": version,
        "createdAt": now,
        "lastUpdatedAt": now,
        "roleArn": data["roleArn"],
        "networkConfiguration": data["networkConfiguration"],
        "status": "READY",
        "agentRuntimeArtifact": data["agentRuntimeArtifact"],
        "workloadIdentityDetails": workload,
    }
    for opt in ("description", "protocolConfiguration", "environmentVariables",
                "authorizerConfiguration", "requestHeaderConfiguration",
                "lifecycleConfiguration", "metadataConfiguration",
                "filesystemConfigurations"):
        if opt in data:
            record[opt] = data[opt]
    record["_versions"] = {version: _version_snapshot(record)}
    _runtimes[runtime_id] = record
    # AWS creates the DEFAULT endpoint with the runtime, pointing at the latest version.
    _endpoints[runtime_id] = {"DEFAULT": _endpoint_record(record, "DEFAULT", version)}

    # AWS returns CREATING at create time; the runtime settles to READY.
    return json_response({
        "agentRuntimeArn": arn,
        "workloadIdentityDetails": workload,
        "agentRuntimeId": runtime_id,
        "agentRuntimeVersion": version,
        "createdAt": now,
        "status": "CREATING",
    })


def _get_agent_runtime(runtime_id, query_params=None):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _runtime_not_found(runtime_id)
    version = _agentcore_query_value(query_params, "version")
    if version is None:
        selected = record
    else:
        selected = _runtime_versions(record).get(str(version))
        if selected is None:
            return _not_found(
                f"Agent runtime version {version} for runtime {runtime_id} not found"
            )
    out = {key: value for key, value in selected.items()
           if not key.startswith("_")}
    return json_response(out)


def _list_agent_runtimes(query_params):
    summaries = []
    for r in _runtimes.values():
        summaries.append({
            "agentRuntimeArn": r["agentRuntimeArn"],
            "agentRuntimeId": r["agentRuntimeId"],
            "agentRuntimeVersion": r["agentRuntimeVersion"],
            "agentRuntimeName": r["agentRuntimeName"],
            "description": r.get("description", ""),
            "lastUpdatedAt": _iso(r["lastUpdatedAt"]),
            "status": r["status"],
        })
    page, error = _paginate_agentcore_results(summaries, query_params)
    if error:
        return error
    return json_response({
        "agentRuntimes": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _list_agent_runtime_versions(runtime_id, query_params):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _runtime_not_found(runtime_id)
    items = [
        _version_summary(record, version)
        for version in sorted(_runtime_versions(record), key=int, reverse=True)
    ]
    page, error = _paginate_agentcore_results(items, query_params)
    if error:
        return error
    return json_response({
        "agentRuntimes": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _update_agent_runtime(runtime_id, body):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _runtime_not_found(runtime_id)
    data = _parse_body(body)
    for field in ("agentRuntimeArtifact", "roleArn", "networkConfiguration"):
        if not data.get(field):
            return _validation(f"{field} is required")

    versions = record.get("_versions")
    if not versions:
        versions = {
            record.get("agentRuntimeVersion", "1"): _version_snapshot(record)
        }
        record["_versions"] = versions

    _stop_container(runtime_id)
    now = now_iso()
    new_version = str(int(record["agentRuntimeVersion"]) + 1)
    record["agentRuntimeVersion"] = new_version
    record["lastUpdatedAt"] = now
    record["status"] = "READY"
    for field in ("agentRuntimeArtifact", "roleArn", "networkConfiguration"):
        record[field] = data[field]
    for opt in ("description", "protocolConfiguration", "environmentVariables",
                "authorizerConfiguration", "requestHeaderConfiguration",
                "lifecycleConfiguration", "metadataConfiguration",
                "filesystemConfigurations"):
        if opt in data:
            record[opt] = data[opt]

    versions[new_version] = _version_snapshot(record)
    default = (_endpoints.get(runtime_id) or {}).get("DEFAULT")
    if default:
        default.update(targetVersion=new_version, liveVersion=new_version, lastUpdatedAt=now)
    return json_response({
        "agentRuntimeArn": record["agentRuntimeArn"],
        "agentRuntimeId": runtime_id,
        "workloadIdentityDetails": record.get("workloadIdentityDetails"),
        "agentRuntimeVersion": new_version,
        "createdAt": _iso(record["createdAt"]),
        "lastUpdatedAt": now,
        "status": "UPDATING",
    })


def _delete_agent_runtime(runtime_id):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _runtime_not_found(runtime_id)
    _stop_container(runtime_id)
    _resource_policies.pop(record["agentRuntimeArn"], None)
    for endpoint in (_endpoints.get(runtime_id) or {}).values():
        _resource_policies.pop(endpoint.get("agentRuntimeEndpointArn"), None)
    _runtimes.pop(runtime_id, None)
    _endpoints.pop(runtime_id, None)
    return json_response({"status": "DELETING", "agentRuntimeId": runtime_id})


# ---------------------------------------------------------------------------
# Control plane — AgentRuntimeEndpoint
# ---------------------------------------------------------------------------

def _create_agent_runtime_endpoint(runtime_id, body):
    runtime = _runtimes.get(runtime_id)
    if runtime is None:
        return _runtime_not_found(runtime_id)
    data = _parse_body(body)
    name = data.get("name")
    if not name or not _NAME_RE.match(name):
        return _validation("name must match ^[a-zA-Z][a-zA-Z0-9_]{0,47}$")
    endpoints = _endpoints.get(runtime_id) or {}
    if name in endpoints:
        return _conflict(f"Endpoint {name} already exists")
    target_version = str(data.get("agentRuntimeVersion") or runtime["agentRuntimeVersion"])
    if target_version not in _runtime_versions(runtime):
        return _validation(f"Agent runtime version {target_version} does not exist")
    endpoints = _endpoints.setdefault(runtime_id, {})
    record = _endpoint_record(runtime, name, target_version, data.get("description", ""))
    now = record["createdAt"]
    endpoints[name] = record
    return json_response({
        "targetVersion": target_version,
        "agentRuntimeEndpointArn": record["agentRuntimeEndpointArn"],
        "agentRuntimeArn": runtime["agentRuntimeArn"],
        "agentRuntimeId": runtime_id,
        "endpointName": name,
        "status": "CREATING",
        "createdAt": now,
    })


def _get_agent_runtime_endpoint(runtime_id, endpoint_name):
    record = (_endpoints.get(runtime_id) or {}).get(endpoint_name)
    if record is None:
        return _not_found(f"Endpoint {endpoint_name} not found")
    return json_response({
        "liveVersion": record.get("liveVersion"),
        "targetVersion": record.get("targetVersion"),
        "agentRuntimeEndpointArn": record["agentRuntimeEndpointArn"],
        "agentRuntimeArn": record["agentRuntimeArn"],
        "description": record.get("description", ""),
        "status": record["status"],
        "createdAt": _iso(record["createdAt"]),
        "lastUpdatedAt": _iso(record["lastUpdatedAt"]),
        "name": record["name"],
        "id": record["id"],
    })


def _list_agent_runtime_endpoints(runtime_id, query_params):
    if _runtimes.get(runtime_id) is None:
        return _runtime_not_found(runtime_id)
    endpoints = _endpoints.get(runtime_id) or {}
    items = []
    for record in endpoints.values():
        items.append({
            "name": record["name"],
            "liveVersion": record.get("liveVersion"),
            "targetVersion": record.get("targetVersion"),
            "agentRuntimeEndpointArn": record["agentRuntimeEndpointArn"],
            "agentRuntimeArn": record["agentRuntimeArn"],
            "status": record["status"],
            "id": record["id"],
            "description": record.get("description", ""),
            "createdAt": _iso(record["createdAt"]),
            "lastUpdatedAt": _iso(record["lastUpdatedAt"]),
        })
    page, error = _paginate_agentcore_results(items, query_params)
    if error:
        return error
    return json_response({
        "runtimeEndpoints": page["items"],
        **({"nextToken": page["nextToken"]} if "nextToken" in page else {}),
    })


def _update_agent_runtime_endpoint(runtime_id, endpoint_name, body):
    runtime = _runtimes.get(runtime_id)
    if runtime is None:
        return _runtime_not_found(runtime_id)
    record = (_endpoints.get(runtime_id) or {}).get(endpoint_name)
    if record is None:
        return _not_found(f"Endpoint {endpoint_name} not found")
    data = _parse_body(body)
    if data.get("agentRuntimeVersion") is not None:
        version = str(data["agentRuntimeVersion"])
        if version not in _runtime_versions(runtime):
            return _validation(f"Agent runtime version {version} does not exist")
        record["targetVersion"] = version
        record["liveVersion"] = version
    if "description" in data:
        record["description"] = data["description"]
    now = now_iso()
    record["lastUpdatedAt"] = now
    record["status"] = "READY"
    return json_response({
        "liveVersion": record.get("liveVersion"),
        "targetVersion": record.get("targetVersion"),
        "agentRuntimeEndpointArn": record["agentRuntimeEndpointArn"],
        "agentRuntimeArn": record["agentRuntimeArn"],
        "status": "UPDATING",
        "createdAt": _iso(record["createdAt"]),
        "lastUpdatedAt": now,
    })


def _delete_agent_runtime_endpoint(runtime_id, endpoint_name):
    endpoints = _endpoints.get(runtime_id) or {}
    if endpoint_name not in endpoints:
        return _not_found(f"Endpoint {endpoint_name} not found")
    endpoint = endpoints.pop(endpoint_name, None)
    if endpoint:
        _resource_policies.pop(endpoint.get("agentRuntimeEndpointArn"), None)
    return json_response({
        "status": "DELETING",
        "agentRuntimeId": runtime_id,
        "endpointName": endpoint_name,
    })


# ---------------------------------------------------------------------------
# Data plane — InvokeAgentRuntime
# ---------------------------------------------------------------------------

def _find_runtime(runtime_arn):
    """Resolve a runtime by the owner account encoded in its ARN."""
    owner = _arn_owner(runtime_arn)
    if owner is None:
        return None, None, None
    account_id, region = owner
    runtime = next(
        (
            record
            for record in _runtimes.values_scoped(account_id, region)
            if record["agentRuntimeArn"] == runtime_arn
        ),
        None,
    )
    return runtime, account_id, region


def _endpoint_for_qualifier(runtime, account_id, region, qualifier):
    if runtime is None:
        return None
    qualifier = qualifier or "DEFAULT"
    endpoints = _endpoints.get_scoped(account_id, region, runtime["agentRuntimeId"], {})
    return endpoints.get(qualifier)


def _principal_context(action, resource_arn, region):
    from ministack.core.iam_evaluator import caller_arn

    principal = caller_arn()
    parts = principal.split(":")
    account_id = parts[4] if len(parts) > 4 else get_account_id()
    if ":assumed-role/" in principal:
        principal_type = "AssumedRole"
    elif ":user/" in principal:
        principal_type = "User"
    else:
        principal_type = "Root"
    from ministack.core.iam_evaluator import EvalContext

    return EvalContext(
        principal_arn=principal,
        principal_type=principal_type,
        principal_account=account_id,
        action=action,
        resource_arn=resource_arn,
        region=region,
    )


def _resource_policy_decision(resource_arn, account_id, region, action):
    from ministack.core.iam_evaluator import evaluate_resource_policy

    policy = _resource_policies.get_scoped(account_id, region, resource_arn)
    if policy is None:
        return None
    return evaluate_resource_policy(
        policy, _principal_context(action, resource_arn, region)
    )


def _resource_policy_allows_invocation(
    runtime, account_id, region, qualifier, headers, query_params
):
    from ministack.core.iam_evaluator import resource_policy_allows

    action = "bedrock-agentcore:InvokeAgentRuntime"
    runtime_arn = runtime["agentRuntimeArn"]
    principal = _principal_context(action, runtime_arn, region)
    same_account = principal.principal_account == account_id
    runtime_policy = _resource_policies.get_scoped(account_id, region, runtime_arn)
    if not resource_policy_allows(runtime_policy, principal, same_account):
        return False

    # A named endpoint is an additional policy resource. AWS requires both the
    # runtime and endpoint policies for cross-account invocation.
    endpoint = _endpoint_for_qualifier(runtime, account_id, region, qualifier)
    if qualifier and endpoint is None:
        return False
    if not same_account and endpoint is None:
        return False
    if endpoint is not None:
        endpoint_arn = endpoint["agentRuntimeEndpointArn"]
        endpoint_policy = _resource_policies.get_scoped(
            account_id, region, endpoint_arn
        )
        endpoint_context = _principal_context(action, endpoint_arn, region)
        if not resource_policy_allows(
            endpoint_policy, endpoint_context, same_account
        ):
            return False
        if not same_account:
            from ministack.core.iam_evaluator import enforce
            from ministack.core.router import extract_access_key_id

            identity_result = enforce(
                extract_access_key_id(headers, query_params or {}),
                action,
                "bedrock-agentcore",
                region,
                resource_arn=endpoint_arn,
            )
            if identity_result is not None:
                return False
    return True


def resource_policy_allows_without_identity(path, query_params):
    """Return whether a same-account resource policy can replace an identity Allow."""
    from ministack.app import AUTH
    if not AUTH:
        return False
    inner = path.strip("/")
    if not (inner.startswith("runtimes/") and inner.endswith("/invocations")):
        return False
    runtime_arn = unquote(inner[len("runtimes/"):-len("/invocations")])
    runtime, account_id, region = _find_runtime(runtime_arn)
    if runtime is None:
        return False
    principal = _principal_context(
        "bedrock-agentcore:InvokeAgentRuntime", runtime_arn, region
    )
    if principal.principal_account != account_id:
        return False
    runtime_decision = _resource_policy_decision(
        runtime_arn, account_id, region, "bedrock-agentcore:InvokeAgentRuntime"
    )
    if runtime_decision is None or runtime_decision.decision != "Allow":
        return False
    qualifier = query_params.get("qualifier") if query_params else None
    if isinstance(qualifier, list):
        qualifier = qualifier[0] if qualifier else None
    endpoint = _endpoint_for_qualifier(runtime, account_id, region, qualifier)
    if endpoint is None:
        return True
    endpoint_decision = _resource_policy_decision(
        endpoint["agentRuntimeEndpointArn"], account_id, region,
        "bedrock-agentcore:InvokeAgentRuntime",
    )
    return endpoint_decision is not None and endpoint_decision.decision == "Allow"


def _invoke_agent_runtime(runtime_arn, headers, body, query_params=None):
    runtime, owner_account, owner_region = _find_runtime(runtime_arn)
    if runtime is None:
        return _not_found(f"Agent runtime {runtime_arn} not found")

    qualifier = _agentcore_query_value(query_params, "qualifier")
    endpoint = _endpoint_for_qualifier(runtime, owner_account, owner_region, qualifier)
    if qualifier and endpoint is None:
        return _not_found(f"Agent runtime endpoint {qualifier} not found")
    version = (endpoint or {}).get("liveVersion") or runtime["agentRuntimeVersion"]
    selected = _runtime_versions(runtime).get(str(version))
    if selected is None:
        return _not_found(f"Agent runtime version {version} for runtime {runtime['agentRuntimeId']} not found")

    from ministack.app import AUTH
    from ministack.core.iam_evaluator import caller_arn, pin_request_caller
    pin_request_caller(headers, query_params or {})
    if AUTH and not _resource_policy_allows_invocation(
        runtime, owner_account, owner_region, qualifier, headers, query_params
    ):
        return error_response_json(
            "AccessDeniedException",
            f"User: {caller_arn()} is not authorized to perform: "
            f"bedrock-agentcore:InvokeAgentRuntime on resource: {runtime_arn}",
            403,
        )

    with request_scope(owner_account, owner_region):
        return _invoke_agent_runtime_in_owner(selected, headers, body)


def _invoke_agent_runtime_in_owner(runtime, headers, body):

    session_id = (headers.get("x-amzn-bedrock-agentcore-runtime-session-id")
                  or new_uuid())
    content_type = headers.get("content-type", "application/json")
    artifact = runtime.get("agentRuntimeArtifact", {}).get("containerConfiguration", {})
    if artifact.get("containerUri") and _docker_client() is not None:
        try:
            url = _container_invocations_url(runtime)
        except (RuntimeError, ValueError) as error:
            logger.warning("AgentCore runtime container failed: %s", error)
            return error_response_json("RuntimeClientError", str(error), 424)
        return _invoke_container(url, body, headers, content_type, session_id)

    # Deterministic echo: return the request payload back under a stable shape
    # so contract tests can assert Invoke request/response handling without a
    # real model. No inference is performed.
    try:
        payload = json.loads(body) if body else {}
    except (ValueError, TypeError):
        payload = None
    response = {
        "agentRuntimeArn": runtime["agentRuntimeArn"],
        "input": payload,
    }
    out_body = json.dumps(response).encode("utf-8")
    out_headers = {
        "Content-Type": content_type,
        "x-amzn-bedrock-agentcore-runtime-session-id": session_id,
    }
    return 200, out_headers, out_body


_WORKER_REQUEST_HEADERS = (
    "accept", "x-amzn-trace-id", "traceparent", "tracestate", "baggage",
)
_WORKER_RESPONSE_HEADERS = (
    "x-amzn-trace-id", "traceparent", "tracestate", "baggage",
)

_LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _local_open(request, *, timeout):
    """Reach the local Docker endpoint without inheriting HTTP proxy settings."""
    return _LOCAL_HTTP.open(request, timeout=timeout)


_docker = None


def _docker_client():
    """A Docker client when the SDK is installed and the daemon answers, else None."""
    global _docker
    if _docker is None:
        try:
            import docker
            client = docker.from_env(timeout=60)
            client.ping()
            _docker = client
        except Exception:
            return None
    return _docker


def _container_key(runtime_id, version):
    return get_account_id(), get_region(), runtime_id, str(version)


def _remove_container(container):
    try:
        container.remove(force=True)
    except Exception:
        logger.exception("Could not remove AgentCore runtime container")


def _remove_orphan_containers(client, labels):
    """Recover a container created just before a Docker API timeout."""
    try:
        matches = client.containers.list(
            all=True,
            filters={"label": [f"{name}={value}" for name, value in labels.items()]},
        )
        for match in matches:
            _remove_container(match)
    except Exception:
        logger.exception("Could not inspect orphaned AgentCore runtime containers")


def _stop_container(runtime_id):
    with _container_lock:
        prefix = _container_key(runtime_id, "")[:3]
        for key in [key for key in _containers if key[:3] == prefix]:
            _remove_container(_containers.pop(key))


def _container_invocations_url(runtime):
    """Start the declared image once per runtime version, then use its port 8080."""
    artifact = runtime.get("agentRuntimeArtifact", {}).get("containerConfiguration", {})
    image = artifact.get("containerUri")
    if not isinstance(image, str) or not image:
        raise ValueError("Agent runtime has no containerConfiguration.containerUri")
    key = _container_key(runtime["agentRuntimeId"], runtime["agentRuntimeVersion"])
    with _container_lock:
        container = _containers.get(key)
        if container is not None:
            try:
                container.reload()
                if container.status != "running":
                    _remove_container(container)
                    _containers.pop(key, None)
                    container = None
            except Exception as error:
                raise RuntimeError(f"Could not inspect runtime container: {error}") from error
        if container is None:
            client = None
            from ministack.core.container_reaper import own_labels

            labels = own_labels("agentcore", **{
                "ministack.agentcore.runtime": runtime["agentRuntimeId"],
                "ministack.agentcore.runtime-version": runtime["agentRuntimeVersion"],
                "ministack.agentcore.account": key[0],
                "ministack.agentcore.region": key[1]})
            try:
                import docker
                client = _docker_client() or docker.from_env(timeout=60)
                run_kwargs = {
                    "environment": runtime.get("environmentVariables", {}),
                    "labels": labels,
                }
                network = None
                try:
                    self_container = client.containers.get(os.environ.get("HOSTNAME", ""))
                    self_container.reload()
                    networks = self_container.attrs["NetworkSettings"]["Networks"]
                    network = next(iter(networks), None)
                except Exception:
                    pass  # MiniStack is running directly on the host.
                if network:
                    run_kwargs["network"] = network
                else:
                    run_kwargs["ports"] = {"8080/tcp": ("127.0.0.1", None)}
                try:
                    container = client.containers.create(image, **run_kwargs)
                except docker.errors.ImageNotFound:
                    client.images.pull(image)
                    container = client.containers.create(image, **run_kwargs)
                container.start()
                container.reload()
                if network:
                    address = container.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
                    if not address:
                        raise RuntimeError("Container has no address on MiniStack's network")
                    url = f"http://{address}:8080/invocations"
                else:
                    bindings = container.attrs["NetworkSettings"]["Ports"]["8080/tcp"]
                    if not bindings:
                        raise RuntimeError("Container port 8080 was not published")
                    url = f"http://127.0.0.1:{bindings[0]['HostPort']}/invocations"
                deadline = time.monotonic() + 30
                while True:
                    try:
                        with _local_open(url.removesuffix("/invocations") + "/ping", timeout=1):
                            break
                    except (urllib.error.URLError, TimeoutError, ConnectionError):
                        container.reload()
                        if container.status == "exited" or time.monotonic() >= deadline:
                            raise RuntimeError("Container did not become ready on port 8080")
                        time.sleep(0.1)
                container._ministack_invocations_url = url
                _containers[key] = container
            except Exception as error:
                if container is not None:
                    _remove_container(container)
                elif client is not None:
                    _remove_orphan_containers(client, labels)
                raise RuntimeError(f"Could not start runtime image {image}: {error}") from error
        return container._ministack_invocations_url


def _invoke_container(url, body, headers, content_type, session_id):
    """Forward only invocation data, never the caller's AWS credentials."""
    forwarded_headers = {
        "Content-Type": content_type,
        "x-amzn-bedrock-agentcore-runtime-session-id": session_id,
    }
    forwarded_headers.update({name: headers[name] for name in _WORKER_REQUEST_HEADERS if name in headers})
    request = urllib.request.Request(
        url, data=body or b"", method="POST",
        headers=forwarded_headers,
    )
    try:
        response = _local_open(request, timeout=30)
    except urllib.error.HTTPError as error:
        error.close()
        return error_response_json("RuntimeClientError",
                                   f"Received error ({error.code}) from runtime.", 424)
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        logger.warning("AgentCore container unavailable: %s", error)
        return error_response_json("RuntimeClientError",
                                   "AgentCore runtime container is unavailable", 424)

    out_headers = {
        "Content-Type": response.headers.get("Content-Type", "application/octet-stream"),
        "x-amzn-bedrock-agentcore-runtime-session-id": session_id,
    }
    out_headers.update({name: response.headers[name] for name in _WORKER_RESPONSE_HEADERS
                        if name in response.headers})
    if "Content-Length" in response.headers:
        out_headers["Content-Length"] = response.headers["Content-Length"]

    async def _stream(send, receive):
        try:
            while chunk := await asyncio.to_thread(response.read1, 64 * 1024):
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
        except Exception:
            logger.exception("AgentCore container response stream failed")
        else:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            response.close()

    return response.status, out_headers, StreamingResponse(_stream)


# ---------------------------------------------------------------------------
# Router — dispatch by rest-json HTTP method + path
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    inner = path.strip("/")
    if inner == "memories" and method == "POST":
        return _list_memories(body)
    if inner == "memories/create" and method == "POST":
        return _create_memory(body)
    if inner.startswith("memories/"):
        rest = inner[len("memories/"):]
        response = _memory_data_plane(method, rest, body, query_params)
        if response is not None:
            return response
        memory_id, _, op = unquote(rest).rpartition("/")
        handler = {("GET", "details"): lambda: _get_memory(memory_id, query_params),
                   ("PUT", "update"): lambda: _update_memory(memory_id, body),
                   ("DELETE", "delete"): lambda: _delete_memory(memory_id)}.get((method, op))
        if handler:
            if not _MEMORY_ID_RE.fullmatch(memory_id):
                return _validation(
                    f"1 validation error detected: Value '{memory_id}' at 'memoryId' failed to "
                    f"satisfy constraint: Member must satisfy regular expression pattern: "
                    f"{_MEMORY_ID_RE.pattern}")
            return handler()
    if inner.startswith("resourcepolicy/"):
        resource_arn = unquote(inner[len("resourcepolicy/"):])
        if method == "PUT":
            return _put_resource_policy(resource_arn, body)
        if method == "GET":
            return _get_resource_policy(resource_arn)
        if method == "DELETE":
            return _delete_resource_policy(resource_arn)
        return error_response_json(
            "InvalidAction", f"Unsupported AgentCore request: {method} {path}", 400
        )

    # InvokeAgentRuntime: POST /runtimes/{agentRuntimeArn}/invocations. The ARN
    # is a single path label but carries literal '/' and ':' (agent/{uuid}:{ver}),
    # so match the suffix before splitting on '/'.
    if (method == "POST" and inner.startswith("runtimes/")
            and inner.endswith("/invocations")):
        arn = inner[len("runtimes/"):-len("/invocations")]
        return await asyncio.to_thread(
            _invoke_agent_runtime, unquote(arn), headers, body, query_params
        )

    parts = [p for p in inner.split("/") if p]
    # All remaining AgentCore paths are rooted at /runtimes.
    if not parts or parts[0] != "runtimes":
        return error_response_json("InvalidAction",
                                   f"Unsupported AgentCore path: {path}", 400)

    n = len(parts)
    if n == 1:
        if method == "PUT":
            return _create_agent_runtime(body)
        if method == "POST":
            return _list_agent_runtimes(query_params)
    elif n == 2:
        runtime_id = unquote(parts[1])
        if method == "GET":
            return _get_agent_runtime(runtime_id, query_params)
        if method == "PUT":
            return _update_agent_runtime(runtime_id, body)
        if method == "DELETE":
            return _delete_agent_runtime(runtime_id)
    elif n == 3:
        seg = parts[2]
        if seg == "invocations" and method == "POST":
            return await asyncio.to_thread(
                _invoke_agent_runtime, unquote(parts[1]), headers, body, query_params
            )
        runtime_id = unquote(parts[1])
        if seg == "versions" and method == "POST":
            return _list_agent_runtime_versions(runtime_id, query_params)
        if seg == "runtime-endpoints":
            if method == "PUT":
                return _create_agent_runtime_endpoint(runtime_id, body)
            if method == "POST":
                return _list_agent_runtime_endpoints(runtime_id, query_params)
    elif n == 4 and parts[2] == "runtime-endpoints":
        runtime_id = unquote(parts[1])
        endpoint_name = unquote(parts[3])
        if method == "GET":
            return _get_agent_runtime_endpoint(runtime_id, endpoint_name)
        if method == "PUT":
            return _update_agent_runtime_endpoint(runtime_id, endpoint_name, body)
        if method == "DELETE":
            return _delete_agent_runtime_endpoint(runtime_id, endpoint_name)

    return error_response_json("InvalidAction",
                               f"Unsupported AgentCore request: {method} {path}", 400)
