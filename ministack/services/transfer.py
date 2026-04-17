"""
AWS Transfer Family Service Emulator.
JSON-based API via X-Amz-Target: TransferService.<Operation>.

Supports:
  Servers:  CreateServer, DescribeServer, DeleteServer, ListServers
  Users:    CreateUser, DescribeUser, DeleteUser, ListUsers
  SSH Keys: ImportSshPublicKey, DeleteSshPublicKey
"""

import copy
import json
import logging
import os
import re

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("transfer")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_servers = AccountScopedDict()  # server_id -> server record
_users = AccountScopedDict()    # "{server_id}/{user_name}" -> user record


def reset():
    _servers.clear()
    _users.clear()


def get_state():
    return copy.deepcopy({
        "servers": _servers,
        "users": _users,
    })


def restore_state(data):
    _servers.update(data.get("servers", {}))
    _users.update(data.get("users", {}))


_restored = load_state("transfer")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _server_id():
    return "s-" + new_uuid().replace("-", "")[:17]


def _key_id():
    return "key-" + new_uuid().replace("-", "")[:17]


def _server_arn(server_id):
    return f"arn:aws:transfer:{REGION}:{get_account_id()}:server/{server_id}"


def _user_arn(server_id, user_name):
    return f"arn:aws:transfer:{REGION}:{get_account_id()}:user/{server_id}/{user_name}"


def _user_key(server_id, user_name):
    return f"{server_id}/{user_name}"


_SSH_KEY_PREFIXES = ("ssh-rsa", "ssh-ed25519", "ssh-dss", "ecdsa-sha2-")


def _validate_ssh_key(key_body):
    """Basic SSH public key format validation."""
    if not key_body or not isinstance(key_body, str):
        return False
    parts = key_body.strip().split()
    if len(parts) < 2:
        return False
    return any(parts[0].startswith(p) for p in _SSH_KEY_PREFIXES)


def _error(code, message, status=400):
    return error_response_json(code, message, status)


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateServer": _create_server,
        "DescribeServer": _describe_server,
        "DeleteServer": _delete_server,
        "ListServers": _list_servers,
        "CreateUser": _create_user,
        "DescribeUser": _describe_user,
        "DeleteUser": _delete_user,
        "ListUsers": _list_users,
        "ImportSshPublicKey": _import_ssh_public_key,
        "DeleteSshPublicKey": _delete_ssh_public_key,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Server handlers
# ---------------------------------------------------------------------------

def _create_server(data):
    sid = _server_id()
    now = now_iso()

    server = {
        "Arn": _server_arn(sid),
        "ServerId": sid,
        "State": "ONLINE",
        "EndpointType": data.get("EndpointType", "PUBLIC"),
        "IdentityProviderType": data.get("IdentityProviderType", "SERVICE_MANAGED"),
        "Protocols": data.get("Protocols", ["SFTP"]),
        "Domain": data.get("Domain", "S3"),
        "Tags": data.get("Tags", []),
        "UserCount": 0,
        "DateCreated": now,
    }

    _servers[sid] = server
    return json_response({"ServerId": sid})


def _describe_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException",
                       f"Unknown server: {sid}", 404)

    server = _servers[sid]
    described = {
        "Arn": server["Arn"],
        "Domain": server.get("Domain", "S3"),
        "EndpointType": server["EndpointType"],
        "IdentityProviderType": server["IdentityProviderType"],
        "Protocols": server["Protocols"],
        "ServerId": sid,
        "State": server["State"],
        "Tags": server.get("Tags", []),
        "UserCount": server.get("UserCount", 0),
    }
    return json_response({"Server": described})


def _delete_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException",
                       f"Unknown server: {sid}", 404)

    # Cascade-delete all users on this server
    to_delete = [k for k in _users if k.startswith(sid + "/")]
    for k in to_delete:
        del _users[k]

    del _servers[sid]
    return json_response({})


def _list_servers(data):
    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")

    all_servers = sorted(_servers.values(), key=lambda s: s["ServerId"])

    start = 0
    if next_token:
        for i, s in enumerate(all_servers):
            if s["ServerId"] == next_token:
                start = i + 1
                break

    page = all_servers[start:start + max_results]
    result = {
        "Servers": [{
            "Arn": s["Arn"],
            "Domain": s.get("Domain", "S3"),
            "EndpointType": s["EndpointType"],
            "IdentityProviderType": s["IdentityProviderType"],
            "Protocols": s["Protocols"],
            "ServerId": s["ServerId"],
            "State": s["State"],
            "UserCount": s.get("UserCount", 0),
        } for s in page],
    }

    if start + max_results < len(all_servers):
        result["NextToken"] = all_servers[start + max_results]["ServerId"]

    return json_response(result)


# ---------------------------------------------------------------------------
# User handlers
# ---------------------------------------------------------------------------

def _create_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")

    if sid not in _servers:
        return _error("ResourceNotFoundException",
                       f"Unknown server: {sid}", 404)

    uk = _user_key(sid, user_name)
    if uk in _users:
        return _error("ResourceExistsException",
                       f"User already exists: {user_name}", 409)

    ssh_keys = []
    ssh_body = data.get("SshPublicKeyBody")
    if ssh_body:
        if not _validate_ssh_key(ssh_body):
            return _error("InvalidRequestException",
                           "Unsupported or invalid SSH public key format", 400)
        now = now_iso()
        ssh_keys.append({
            "SshPublicKeyId": _key_id(),
            "SshPublicKeyBody": ssh_body.strip(),
            "DateImported": now,
        })

    user = {
        "Arn": _user_arn(sid, user_name),
        "UserName": user_name,
        "ServerId": sid,
        "HomeDirectoryType": data.get("HomeDirectoryType", "PATH"),
        "HomeDirectoryMappings": data.get("HomeDirectoryMappings", []),
        "HomeDirectory": data.get("HomeDirectory"),
        "Role": data.get("Role", ""),
        "SshPublicKeys": ssh_keys,
        "Tags": data.get("Tags", []),
    }

    _users[uk] = user
    _servers[sid]["UserCount"] = _servers[sid].get("UserCount", 0) + 1

    return json_response({"ServerId": sid, "UserName": user_name})


def _describe_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")

    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException",
                       f"Unknown user: {user_name}", 404)

    user = _users[uk]
    described = {
        "Arn": user["Arn"],
        "HomeDirectoryType": user.get("HomeDirectoryType", "PATH"),
        "HomeDirectoryMappings": user.get("HomeDirectoryMappings", []),
        "Role": user.get("Role", ""),
        "SshPublicKeys": user.get("SshPublicKeys", []),
        "Tags": user.get("Tags", []),
        "UserName": user["UserName"],
    }
    if user.get("HomeDirectory"):
        described["HomeDirectory"] = user["HomeDirectory"]

    return json_response({"ServerId": sid, "User": described})


def _delete_user(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")

    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException",
                       f"Unknown user: {user_name}", 404)

    del _users[uk]
    if sid in _servers:
        _servers[sid]["UserCount"] = max(0, _servers[sid].get("UserCount", 1) - 1)

    return json_response({})


def _list_users(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException",
                       f"Unknown server: {sid}", 404)

    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")

    prefix = sid + "/"
    server_users = sorted(
        [u for k, u in _users.items() if k.startswith(prefix)],
        key=lambda u: u["UserName"],
    )

    start = 0
    if next_token:
        for i, u in enumerate(server_users):
            if u["UserName"] == next_token:
                start = i + 1
                break

    page = server_users[start:start + max_results]
    result = {
        "ServerId": sid,
        "Users": [{
            "Arn": u["Arn"],
            "HomeDirectoryType": u.get("HomeDirectoryType", "PATH"),
            "Role": u.get("Role", ""),
            "SshPublicKeyCount": len(u.get("SshPublicKeys", [])),
            "UserName": u["UserName"],
        } for u in page],
    }

    if start + max_results < len(server_users):
        result["NextToken"] = server_users[start + max_results]["UserName"]

    return json_response(result)


# ---------------------------------------------------------------------------
# SSH key handlers
# ---------------------------------------------------------------------------

def _import_ssh_public_key(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    ssh_body = data.get("SshPublicKeyBody", "")

    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException",
                       f"Unknown user: {user_name}", 404)

    if not _validate_ssh_key(ssh_body):
        return _error("InvalidRequestException",
                       "Unsupported or invalid SSH public key format", 400)

    kid = _key_id()
    now = now_iso()
    _users[uk]["SshPublicKeys"].append({
        "SshPublicKeyId": kid,
        "SshPublicKeyBody": ssh_body.strip(),
        "DateImported": now,
    })

    return json_response({
        "ServerId": sid,
        "SshPublicKeyId": kid,
        "UserName": user_name,
    })


def _delete_ssh_public_key(data):
    sid = data.get("ServerId", "")
    user_name = data.get("UserName", "")
    key_id = data.get("SshPublicKeyId", "")

    uk = _user_key(sid, user_name)
    if uk not in _users:
        return _error("ResourceNotFoundException",
                       f"Unknown user: {user_name}", 404)

    keys = _users[uk]["SshPublicKeys"]
    _users[uk]["SshPublicKeys"] = [k for k in keys if k["SshPublicKeyId"] != key_id]

    return json_response({})
