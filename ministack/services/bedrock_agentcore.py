"""Amazon Bedrock AgentCore emulator.

Covers the two AgentCore services, which both sign as ``bedrock-agentcore``:

  * ``bedrock-agentcore-control`` (rest-json) — agent runtime + endpoint control
    plane: CreateAgentRuntime, GetAgentRuntime, ListAgentRuntimes,
    UpdateAgentRuntime, DeleteAgentRuntime, ListAgentRuntimeVersions,
    CreateAgentRuntimeEndpoint, GetAgentRuntimeEndpoint,
    ListAgentRuntimeEndpoints, UpdateAgentRuntimeEndpoint,
    DeleteAgentRuntimeEndpoint.
  * ``bedrock-agentcore`` (rest-json) — data plane: InvokeAgentRuntime.

Deterministic and stateful: resources provision instantly (``READY``) and
InvokeAgentRuntime returns a deterministic echo response, so teams can test
runtime lifecycle, endpoint wiring, and Invoke request/response contracts
locally without live AWS.

Shapes, HTTP methods, URIs, ARN/ID patterns, and status enums are verified
against botocore ``bedrock-agentcore-control`` / ``bedrock-agentcore``
service-2.json.
"""
import copy
import json
import logging
import re
import secrets
import string
import time
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("bedrock_agentcore")

# ---------------------------------------------------------------------------
# State (account + region scoped)
# ---------------------------------------------------------------------------

_runtimes = AccountRegionScopedDict()    # agentRuntimeId -> runtime record
_endpoints = AccountRegionScopedDict()   # agentRuntimeId -> {endpointName -> endpoint record}

# AgentRuntimeName / EndpointName: start with a letter, then letters/digits/_,
# up to 48 chars total (botocore pattern ^[a-zA-Z][a-zA-Z0-9_]{0,47}$).
_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,47}$")


def get_state():
    return copy.deepcopy({"runtimes": _runtimes, "endpoints": _endpoints})


def restore_state(data):
    if not data:
        return
    _runtimes.clear()
    _endpoints.clear()
    _runtimes.update(data.get("runtimes", {}))
    _endpoints.update(data.get("endpoints", {}))


try:
    _persisted = load_state("bedrock_agentcore")
    if _persisted:
        restore_state(_persisted)
except Exception:  # pragma: no cover - best-effort restore
    pass


def reset():
    _runtimes.clear()
    _endpoints.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rand_suffix() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(10))


def _resource_id(name: str) -> str:
    # botocore pattern: [a-zA-Z][a-zA-Z0-9_]{0,99}-[a-zA-Z0-9]{10}
    return f"{name}-{_rand_suffix()}"


def _runtime_arn(runtime_uuid: str, version: str) -> str:
    return (f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:"
            f"agent/{runtime_uuid}:{version}")


def _endpoint_arn(endpoint_uuid: str) -> str:
    return (f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:"
            f"agentEndpoint/{endpoint_uuid}")


def _workload_identity_arn(name: str) -> str:
    return (f"arn:aws:bedrock-agentcore:{get_region()}:{get_account_id()}:"
            f"workload-identity-directory/default/workload-identity/{name}")


def _validation(message: str):
    return error_response_json("ValidationException", message, 400)


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

    runtime_uuid = new_uuid()
    runtime_id = _resource_id(name)
    version = "1"
    now = time.time()
    arn = _runtime_arn(runtime_uuid, version)
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
        "_uuid": runtime_uuid,
    }
    for opt in ("description", "protocolConfiguration", "environmentVariables",
                "authorizerConfiguration", "requestHeaderConfiguration",
                "lifecycleConfiguration", "metadataConfiguration",
                "filesystemConfigurations"):
        if opt in data:
            record[opt] = data[opt]
    _runtimes[runtime_id] = record
    _endpoints[runtime_id] = {}

    # AWS returns CREATING at create time; the runtime settles to READY.
    return json_response({
        "agentRuntimeArn": arn,
        "workloadIdentityDetails": workload,
        "agentRuntimeId": runtime_id,
        "agentRuntimeVersion": version,
        "createdAt": now,
        "status": "CREATING",
    })


def _get_agent_runtime(runtime_id):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    out = {k: v for k, v in record.items() if not k.startswith("_")}
    return json_response(out)


def _list_agent_runtimes(body):
    summaries = []
    for r in _runtimes.values():
        summaries.append({
            "agentRuntimeArn": r["agentRuntimeArn"],
            "agentRuntimeId": r["agentRuntimeId"],
            "agentRuntimeVersion": r["agentRuntimeVersion"],
            "agentRuntimeName": r["agentRuntimeName"],
            "description": r.get("description", ""),
            "lastUpdatedAt": r["lastUpdatedAt"],
            "status": r["status"],
        })
    return json_response({"agentRuntimes": summaries})


def _list_agent_runtime_versions(runtime_id, body):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    summary = {
        "agentRuntimeArn": record["agentRuntimeArn"],
        "agentRuntimeId": record["agentRuntimeId"],
        "agentRuntimeVersion": record["agentRuntimeVersion"],
        "agentRuntimeName": record["agentRuntimeName"],
        "description": record.get("description", ""),
        "lastUpdatedAt": record["lastUpdatedAt"],
        "status": record["status"],
    }
    return json_response({"agentRuntimes": [summary]})


def _update_agent_runtime(runtime_id, body):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    data = _parse_body(body)
    for field in ("agentRuntimeArtifact", "roleArn", "networkConfiguration"):
        if not data.get(field):
            return _validation(f"{field} is required")
    now = time.time()
    new_version = str(int(record["agentRuntimeVersion"]) + 1)
    record["agentRuntimeVersion"] = new_version
    record["agentRuntimeArn"] = _runtime_arn(record["_uuid"], new_version)
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
    return json_response({
        "agentRuntimeArn": record["agentRuntimeArn"],
        "agentRuntimeId": runtime_id,
        "workloadIdentityDetails": record.get("workloadIdentityDetails"),
        "agentRuntimeVersion": new_version,
        "createdAt": record["createdAt"],
        "lastUpdatedAt": now,
        "status": "UPDATING",
    })


def _delete_agent_runtime(runtime_id):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    _runtimes.pop(runtime_id, None)
    _endpoints.pop(runtime_id, None)
    return json_response({"status": "DELETING", "agentRuntimeId": runtime_id})


# ---------------------------------------------------------------------------
# Control plane — AgentRuntimeEndpoint
# ---------------------------------------------------------------------------

def _create_agent_runtime_endpoint(runtime_id, body):
    runtime = _runtimes.get(runtime_id)
    if runtime is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    data = _parse_body(body)
    name = data.get("name")
    if not name or not _NAME_RE.match(name):
        return _validation("name must match ^[a-zA-Z][a-zA-Z0-9_]{0,47}$")
    endpoints = _endpoints.setdefault(runtime_id, {})
    if name in endpoints:
        return _conflict(f"Endpoint {name} already exists")
    target_version = data.get("agentRuntimeVersion") or runtime["agentRuntimeVersion"]
    endpoint_uuid = new_uuid()
    now = time.time()
    record = {
        "name": name,
        "id": _resource_id(name),
        "agentRuntimeEndpointArn": _endpoint_arn(endpoint_uuid),
        "agentRuntimeArn": runtime["agentRuntimeArn"],
        "targetVersion": target_version,
        "liveVersion": target_version,
        "status": "READY",
        "description": data.get("description", ""),
        "createdAt": now,
        "lastUpdatedAt": now,
    }
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
        "createdAt": record["createdAt"],
        "lastUpdatedAt": record["lastUpdatedAt"],
        "name": record["name"],
        "id": record["id"],
    })


def _list_agent_runtime_endpoints(runtime_id, body):
    if _runtimes.get(runtime_id) is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
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
            "createdAt": record["createdAt"],
            "lastUpdatedAt": record["lastUpdatedAt"],
        })
    return json_response({"runtimeEndpoints": items})


def _update_agent_runtime_endpoint(runtime_id, endpoint_name, body):
    runtime = _runtimes.get(runtime_id)
    if runtime is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
    record = (_endpoints.get(runtime_id) or {}).get(endpoint_name)
    if record is None:
        return _not_found(f"Endpoint {endpoint_name} not found")
    data = _parse_body(body)
    now = time.time()
    if data.get("agentRuntimeVersion"):
        record["targetVersion"] = data["agentRuntimeVersion"]
        record["liveVersion"] = data["agentRuntimeVersion"]
    if "description" in data:
        record["description"] = data["description"]
    record["lastUpdatedAt"] = now
    record["status"] = "READY"
    return json_response({
        "liveVersion": record.get("liveVersion"),
        "targetVersion": record.get("targetVersion"),
        "agentRuntimeEndpointArn": record["agentRuntimeEndpointArn"],
        "agentRuntimeArn": record["agentRuntimeArn"],
        "status": "UPDATING",
        "createdAt": record["createdAt"],
        "lastUpdatedAt": now,
    })


def _delete_agent_runtime_endpoint(runtime_id, endpoint_name):
    endpoints = _endpoints.get(runtime_id) or {}
    if endpoint_name not in endpoints:
        return _not_found(f"Endpoint {endpoint_name} not found")
    endpoints.pop(endpoint_name, None)
    return json_response({
        "status": "DELETING",
        "agentRuntimeId": runtime_id,
        "endpointName": endpoint_name,
    })


# ---------------------------------------------------------------------------
# Data plane — InvokeAgentRuntime
# ---------------------------------------------------------------------------

def _invoke_agent_runtime(runtime_arn, headers, body):
    # The runtime is addressed by ARN; resolve it in this account+region.
    runtime = next((r for r in _runtimes.values()
                    if r["agentRuntimeArn"] == runtime_arn
                    or r["agentRuntimeArn"].rsplit(":", 1)[0] == runtime_arn.rsplit(":", 1)[0]),
                   None)
    if runtime is None:
        return _not_found(f"Agent runtime {runtime_arn} not found")

    session_id = (headers.get("x-amzn-bedrock-agentcore-runtime-session-id")
                  or new_uuid())
    content_type = headers.get("content-type", "application/json")
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


# ---------------------------------------------------------------------------
# Router — dispatch by rest-json HTTP method + path
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    inner = path.strip("/")
    # InvokeAgentRuntime: POST /runtimes/{agentRuntimeArn}/invocations. The ARN
    # is a single path label but carries literal '/' and ':' (agent/{uuid}:{ver}),
    # so match the suffix before splitting on '/'.
    if (method == "POST" and inner.startswith("runtimes/")
            and inner.endswith("/invocations")):
        arn = inner[len("runtimes/"):-len("/invocations")]
        return _invoke_agent_runtime(unquote(arn), headers, body)

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
            return _list_agent_runtimes(body)
    elif n == 2:
        runtime_id = unquote(parts[1])
        if method == "GET":
            return _get_agent_runtime(runtime_id)
        if method == "PUT":
            return _update_agent_runtime(runtime_id, body)
        if method == "DELETE":
            return _delete_agent_runtime(runtime_id)
    elif n == 3:
        seg = parts[2]
        if seg == "invocations" and method == "POST":
            return _invoke_agent_runtime(unquote(parts[1]), headers, body)
        runtime_id = unquote(parts[1])
        if seg == "versions" and method == "POST":
            return _list_agent_runtime_versions(runtime_id, body)
        if seg == "runtime-endpoints":
            if method == "PUT":
                return _create_agent_runtime_endpoint(runtime_id, body)
            if method == "POST":
                return _list_agent_runtime_endpoints(runtime_id, body)
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
