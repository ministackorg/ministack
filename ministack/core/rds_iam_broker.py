# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Internal stage-6 broker (#1744). Capability provisioning is stage 8.

Capabilities are process-local, never persisted, and never issued over HTTP.
AUTH controls IAM enforcement, not capability protection or resource enablement.
No plugin/container calls this endpoint yet.
"""

import asyncio
import hashlib
import json
import secrets
import threading
from dataclasses import dataclass

from ministack.core.rds_iam import AuthorizedRdsConnection, authorize_rds_iam_token

PATH = "/_ministack/rds/iam-auth"
MAX_BODY = 70 * 1024
BODY_TIMEOUT = 5
_lock = threading.RLock()
_bindings = {}


@dataclass(frozen=True, repr=False)
class _Binding:
    account_id: str
    region: str
    resource_kind: str
    resource_identifier: str
    reader_endpoint: bool
    target: dict
    owner: dict
    resource_id: str


def _resources(account_id, region, resource_kind, resource_identifier):
    from ministack.services import rds

    store = rds._instances if resource_kind == "instance" else rds._clusters
    target = store.get_scoped(account_id, region, resource_identifier)
    if not target or target.get("Engine") not in ("mysql", "aurora", "aurora-mysql"):
        return None, None
    owner = target
    if resource_kind == "instance" and target.get("DBClusterIdentifier"):
        owner = rds._clusters.get_scoped(account_id, region, target["DBClusterIdentifier"])
        if not owner or owner.get("Engine") not in ("aurora", "aurora-mysql"):
            return None, None
    elif target.get("Engine") != "mysql" and resource_kind == "instance":
        return None, None
    if resource_kind == "cluster" and target.get("Engine") == "mysql":
        return None, None
    return target, owner


def _resource_id(target, owner, kind):
    return owner.get("DbClusterResourceId" if kind == "cluster" or owner is not target else "DbiResourceId")


def issue_capability(*, account_id, region, resource_kind, resource_identifier, reader_endpoint=False):
    """Trusted in-process API; rotates the capability for this endpoint binding.

    No production callers until container wiring. Call again after restart or
    resource replacement; old capabilities cannot authorize recreated resources.
    """
    if (resource_kind not in ("instance", "cluster") or not account_id or not region
            or type(reader_endpoint) is not bool or (reader_endpoint and resource_kind != "cluster")):
        raise ValueError("Invalid RDS target")
    with _lock:
        target, owner = _resources(account_id, region, resource_kind, resource_identifier)
        resource_id = _resource_id(target, owner, resource_kind) if owner is not None else None
        if not resource_id:
            raise ValueError("Invalid RDS target")
        for digest, binding in list(_bindings.items()):
            if (binding.account_id, binding.region, binding.resource_kind, binding.resource_identifier,
                binding.reader_endpoint) == (account_id, region, resource_kind, resource_identifier, reader_endpoint):
                del _bindings[digest]
        capability = secrets.token_hex(32)
        _bindings[hashlib.sha256(capability.encode()).digest()] = _Binding(
            account_id, region, resource_kind, resource_identifier, reader_endpoint, target, owner, resource_id,
        )
        return capability


def revoke_capability(capability):
    with _lock:
        _bindings.pop(hashlib.sha256(capability.encode()).digest(), None)


def reset():
    with _lock:
        _bindings.clear()


def _binding_current(binding):
    target, owner = _resources(binding.account_id, binding.region, binding.resource_kind,
                               binding.resource_identifier)
    return (target is binding.target and owner is binding.owner
            and _resource_id(target, owner, binding.resource_kind) == binding.resource_id
            and owner.get("IAMDatabaseAuthenticationEnabled") is True)


def _decision(capability, payload, auth_enabled):
    with _lock:
        binding = _bindings.get(hashlib.sha256(capability).digest())
        if binding is None:
            return False
        if not _binding_current(binding):
            return False
        if not auth_enabled:
            return True
        result = authorize_rds_iam_token(
            payload["token"], account_id=binding.account_id, region=binding.region,
            resource_kind=binding.resource_kind, resource_identifier=binding.resource_identifier,
            reader_endpoint=binding.reader_endpoint, db_user=payload["username"],
        )
        # RDS lifecycle workers do not hold the broker lock. Authorization
        # looks up state independently, so discard its result if the resource
        # or its owner was replaced or disabled while that lookup ran.
        return isinstance(result, AuthorizedRdsConnection) and _binding_current(binding)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


async def _read_body(receive):
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] != "http.request":
            raise ValueError("Disconnected")
        chunk = message.get("body", b"")
        if len(body) + len(chunk) > MAX_BODY:
            raise ValueError("Oversized request")
        body.extend(chunk)
        if not message.get("more_body", False):
            return bytes(body)


async def handle(scope, receive, send, *, auth_enabled):
    """POST {username, token}, with X-Ministack-RDS-Capability header.

    Transport/schema failures never grant access, even in permissive mode.
    Do not log request data or exception details on this credential-bearing path.
    """
    status, allowed = 403, False
    try:
        headers = {}
        for name, value in scope.get("headers", []):
            name = name.lower()
            if name in headers:
                raise ValueError("Duplicate header")
            headers[name] = value
        capability = headers.get(b"x-ministack-rds-capability", b"")
        if (scope["method"] != "POST" or scope.get("query_string")
                or len(capability) != 64 or any(c not in b"0123456789abcdef" for c in capability)
                or headers.get(b"content-type", b"").split(b";", 1)[0].strip().lower() != b"application/json"
                or b"content-encoding" in headers):
            raise ValueError("Invalid request")
        with _lock:
            if hashlib.sha256(capability).digest() not in _bindings:
                raise ValueError("Invalid capability")
        body = await asyncio.wait_for(_read_body(receive), timeout=BODY_TIMEOUT)
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object)
        if (not isinstance(payload, dict) or payload.keys() != {"username", "token"}
                or not isinstance(payload["username"], str) or not 1 <= len(payload["username"]) <= 256
                or "\x00" in payload["username"] or not isinstance(payload["token"], str)
                or len(payload["token"]) > 65536):
            raise ValueError("Invalid payload")
        allowed = _decision(capability, payload, auth_enabled)
        status = 200 if allowed else 403
    except Exception:
        # Even unexpected verifier failures must not leak tokens through logs
        # or turn a failed request into an allow response.
        pass
    await send({"type": "http.response.start", "status": status, "headers": [
        (b"content-type", b"application/json"), (b"cache-control", b"no-store"),
    ]})
    await send({"type": "http.response.body", "body": b'{"allowed":true}' if allowed else b'{"allowed":false}'})
