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
  * ``bedrock-agentcore`` (rest-json) — data plane: InvokeAgentRuntime.

Deterministic and stateful: resources provision instantly (``READY``) and
InvokeAgentRuntime returns a deterministic echo response, so teams can test
runtime lifecycle, endpoint wiring, resource-policy authorization, and Invoke
request/response contracts locally without live AWS.

Shapes, HTTP methods, URIs, ARN/ID patterns, and status enums are verified
against botocore ``bedrock-agentcore-control`` / ``bedrock-agentcore``
service-2.json.
"""
import asyncio
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
_containers = {}  # (account, region, runtime id) -> Docker container
_container_lock = threading.RLock()

# AgentRuntimeName / EndpointName: start with a letter, then letters/digits/_,
# up to 48 chars total (botocore pattern ^[a-zA-Z][a-zA-Z0-9_]{0,47}$).
_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,47}$")


def get_state():
    return copy.deepcopy({
        "runtimes": _runtimes,
        "endpoints": _endpoints,
        "resourcePolicies": _resource_policies,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _runtimes.clear()
    _endpoints.clear()
    _resource_policies.clear()
    _runtimes.update(data.get("runtimes", {}))
    _endpoints.update(data.get("endpoints", {}))
    _resource_policies.update(data.get("resourcePolicies", {}))




def reset():
    with _container_lock:
        for container in _containers.values():
            _remove_container(container)
        _containers.clear()
    _runtimes.clear()
    _endpoints.clear()
    _resource_policies.clear()


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
    now = now_iso()
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
            "lastUpdatedAt": _iso(r["lastUpdatedAt"]),
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
        "lastUpdatedAt": _iso(record["lastUpdatedAt"]),
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
    _stop_container(runtime_id)
    old_arn = record["agentRuntimeArn"]
    now = now_iso()
    new_version = str(int(record["agentRuntimeVersion"]) + 1)
    record["agentRuntimeVersion"] = new_version
    record["agentRuntimeArn"] = _runtime_arn(record["_uuid"], new_version)
    _resource_policies.pop(old_arn, None)
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
        "createdAt": _iso(record["createdAt"]),
        "lastUpdatedAt": now,
        "status": "UPDATING",
    })


def _delete_agent_runtime(runtime_id):
    record = _runtimes.get(runtime_id)
    if record is None:
        return _not_found(f"Agent runtime {runtime_id} not found")
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
    now = now_iso()
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
        "createdAt": _iso(record["createdAt"]),
        "lastUpdatedAt": _iso(record["lastUpdatedAt"]),
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
            "createdAt": _iso(record["createdAt"]),
            "lastUpdatedAt": _iso(record["lastUpdatedAt"]),
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
    now = now_iso()
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
            or record["agentRuntimeArn"].rsplit(":", 1)[0]
            == runtime_arn.rsplit(":", 1)[0]
        ),
        None,
    )
    return runtime, account_id, region


def _endpoint_for_qualifier(runtime, account_id, region, qualifier):
    if not qualifier:
        return None
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

    qualifier = query_params.get("qualifier") if query_params else None
    if isinstance(qualifier, list):
        qualifier = qualifier[0] if qualifier else None
    endpoint = _endpoint_for_qualifier(runtime, owner_account, owner_region, qualifier)
    if qualifier and endpoint is None:
        return _not_found(f"Agent runtime endpoint {qualifier} not found")

    from ministack.app import AUTH
    from ministack.core.iam_evaluator import pin_request_caller
    pin_request_caller(headers, query_params or {})
    if AUTH and not _resource_policy_allows_invocation(
        runtime, owner_account, owner_region, qualifier, headers, query_params
    ):
        return error_response_json(
            "AccessDeniedException",
            f"User: {get_account_id()} is not authorized to perform: "
            f"bedrock-agentcore:InvokeAgentRuntime on resource: {runtime_arn}",
            403,
        )

    with request_scope(owner_account, owner_region):
        return _invoke_agent_runtime_in_owner(runtime, headers, body)


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


def _container_key(runtime_id):
    return get_account_id(), get_region(), runtime_id


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
        container = _containers.pop(_container_key(runtime_id), None)
        if container is not None:
            _remove_container(container)


def _container_invocations_url(runtime):
    """Start the declared image once per runtime version, then use its port 8080."""
    artifact = runtime.get("agentRuntimeArtifact", {}).get("containerConfiguration", {})
    image = artifact.get("containerUri")
    if not isinstance(image, str) or not image:
        raise ValueError("Agent runtime has no containerConfiguration.containerUri")
    key = _container_key(runtime["agentRuntimeId"])
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
            return await asyncio.to_thread(
                _invoke_agent_runtime, unquote(parts[1]), headers, body, query_params
            )
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
