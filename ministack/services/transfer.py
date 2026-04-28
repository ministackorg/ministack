"""
AWS Transfer Family Service Emulator.

Two protocols backing one AWS service:

- **Control plane** — JSON API via X-Amz-Target: TransferService.<Operation>.
  CreateServer / DescribeServer / DeleteServer / ListServers / StartServer /
  StopServer, CreateUser / DescribeUser / DeleteUser / ListUsers,
  ImportSshPublicKey / DeleteSshPublicKey.

- **Data plane** — real SFTP listener on :2222 by default
  (`SFTP_PORT`), key-authenticated, S3-backed virtual filesystem.
  `SFTP_PORT_PER_SERVER=1` opts into AWS-style per-server endpoints,
  allocating ports from `SFTP_BASE_PORT` (default 2300, incrementing).
  Authentication matches the presented SSH public key against every
  user's `SshPublicKeys` across every Transfer server (and across every
  account); the matching user determines the server identity, account,
  and home-directory mapping — no `username$serverid` decoration
  (that's a LocalStack-ism, not AWS behavior).

S3 semantics under SFTP
-----------------------
- `mkdir <prefix>` writes a zero-byte object at `<prefix>/`.
- `rename` is implemented as copy + delete (S3 has no atomic rename).
- Writes buffer in memory until close, then PUT the entire body at once.
- `setstat` / `chmod` / `chown` / `utime` are no-ops; S3 has no POSIX
  permissions or mtime.
- The host key persists at `${STATE_DIR}/transfer-host-key` when
  `PERSIST_STATE=1` (otherwise regenerated each run) so SSH clients
  don't see "host identification has changed" warnings between restarts.
"""

from __future__ import annotations

import asyncio
import copy
import io
import json
import logging
import os
import re
import time
from typing import AsyncIterator, Optional

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("transfer")

# asyncssh is an optional extra (pip install ministack[full]). We import
# guarded so that a base install with the transfer service still works for
# the control plane (CreateServer / DescribeServer / etc.) even when the
# SFTP listener isn't available.
try:
    import asyncssh
    from asyncssh.sftp import SFTPAttrs, SFTPName

    _ASYNCSSH_AVAILABLE = True
except Exception:  # noqa: BLE001 — any import failure means no SFTP
    asyncssh = None  # type: ignore[assignment]
    SFTPAttrs = None  # type: ignore[assignment]
    SFTPName = None  # type: ignore[assignment]
    _ASYNCSSH_AVAILABLE = False

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


try:
    _restored = load_state("transfer")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _server_id():
    return "s-" + new_uuid().replace("-", "")[:17]


def _key_id():
    return "key-" + new_uuid().replace("-", "")[:17]


def _server_arn(server_id):
    return f"arn:aws:transfer:{get_region()}:{get_account_id()}:server/{server_id}"


def _user_arn(server_id, user_name):
    return f"arn:aws:transfer:{get_region()}:{get_account_id()}:user/{server_id}/{user_name}"


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
        "StartServer": _start_server,
        "StopServer": _stop_server,
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
    result = handler(data)
    # Some handlers need to await SFTP listener setup; the dispatcher
    # transparently handles both sync and async return shapes so the
    # per-handler signature stays minimal.
    if asyncio.iscoroutine(result):
        result = await result
    return result


# ---------------------------------------------------------------------------
# Server handlers
# ---------------------------------------------------------------------------

async def _create_server(data):
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

    # If SFTP_PORT_PER_SERVER=1, allocate a dedicated SFTP listener for
    # this server (matches AWS's per-server endpoint model). Otherwise
    # the shared :2222 listener handles every server, disambiguating by
    # the user's SSH public key. Discovery of the port is via the admin
    # endpoint /_ministack/transfer/sftp-ports.
    try:
        await sftp_start_server_listener(sid)
    except Exception as e:
        logger.warning("SFTP per-server listener failed for %s: %s", sid, e)

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
    # SFTP port discovery is via GET /_ministack/transfer/sftp-ports —
    # boto3's model-driven deserializer drops fields not in the AWS spec
    # (so DescribeServer can't carry an SftpPort field meaningfully).
    return json_response({"Server": described})


def _start_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)
    _servers[sid]["State"] = "ONLINE"
    return json_response({})


def _stop_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException", f"Unknown server: {sid}", 404)
    _servers[sid]["State"] = "OFFLINE"
    return json_response({})


async def _delete_server(data):
    sid = data.get("ServerId", "")
    if sid not in _servers:
        return _error("ResourceNotFoundException",
                       f"Unknown server: {sid}", 404)

    # Cascade-delete all users on this server
    to_delete = [k for k in _users if k.startswith(sid + "/")]
    for k in to_delete:
        del _users[k]

    del _servers[sid]

    # Tear down any per-server SFTP listener bound for this server (no-op
    # in shared-port mode).
    try:
        await sftp_stop_server_listener(sid)
    except Exception as e:
        logger.debug("SFTP per-server stop error for %s: %s", sid, e)

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


# ===========================================================================
# SFTP data plane
# ===========================================================================
#
# Lifecycle (called from app.py lifespan):
#   - sftp_start()                     — bind shared listener on SFTP_PORT
#   - sftp_stop()                      — close shared + per-server listeners
#   - sftp_start_server_listener(sid)  — per-server-port mode
#   - sftp_stop_server_listener(sid)   — per-server-port mode
#   - sftp_per_server_port(sid)        — discovery for the admin endpoint
#
# Everything else in this section is internal to the SFTP listener.
# ===========================================================================


def _is_truthy(v):
    return (v or "").lower() in ("1", "true", "yes", "on")


def _sftp_enabled() -> bool:
    if not _ASYNCSSH_AVAILABLE:
        return False
    raw = os.environ.get("SFTP_ENABLED")
    if raw is None:
        return True  # default-on when asyncssh is installed
    return _is_truthy(raw)


def _port_per_server() -> bool:
    return _is_truthy(os.environ.get("SFTP_PORT_PER_SERVER"))


def _shared_port() -> int:
    return int(os.environ.get("SFTP_PORT", "2222"))


def _per_server_base_port() -> int:
    return int(os.environ.get("SFTP_BASE_PORT", "2300"))


def _bind_host() -> str:
    return os.environ.get("SFTP_HOST", "0.0.0.0")


# Module-level state — listeners + host key
_sftp_shared_acceptor = None  # type: Optional[object]
_sftp_per_server_acceptors: dict = {}
_sftp_per_server_ports: dict = {}
_sftp_next_per_server_port: Optional[int] = None
_sftp_host_key = None  # type: Optional[object]
_sftp_lock = asyncio.Lock()


def _sftp_host_key_path() -> str:
    from ministack.core.persistence import STATE_DIR
    return os.path.join(STATE_DIR, "transfer-host-key")


def _sftp_load_or_generate_host_key():
    """Return an asyncssh-compatible host key.

    PERSIST_STATE=1 + key file exists  → load it.
    PERSIST_STATE=1 + key file missing → generate, save, return.
    PERSIST_STATE=0                    → generate ephemerally, no save.

    Persisting matters because OpenSSH clients cache the host key
    fingerprint in known_hosts; a fresh key on every container restart
    triggers the "REMOTE HOST IDENTIFICATION HAS CHANGED" warning that
    breaks automated SFTP clients.
    """
    global _sftp_host_key
    if _sftp_host_key is not None:
        return _sftp_host_key

    from ministack.core.persistence import PERSIST_STATE

    path = _sftp_host_key_path()
    if PERSIST_STATE and os.path.exists(path):
        try:
            _sftp_host_key = asyncssh.read_private_key(path)
            logger.info("SFTP: loaded persisted host key from %s", path)
            return _sftp_host_key
        except Exception as e:
            logger.warning("SFTP: failed to load persisted host key %s (%s); regenerating", path, e)

    _sftp_host_key = asyncssh.generate_private_key("ssh-ed25519")
    if PERSIST_STATE:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            _sftp_host_key.write_private_key(path)
            logger.info("SFTP: persisted new host key at %s", path)
        except Exception as e:
            logger.warning("SFTP: failed to persist host key to %s: %s", path, e)
    else:
        logger.info("SFTP: generated ephemeral host key (PERSIST_STATE=0)")
    return _sftp_host_key


def _sftp_normalize_key(key_body: str) -> str:
    """Strip comments + whitespace for stable comparison.

    SSH keys in `authorized_keys` look like ``ssh-ed25519 AAAAC3...== comment``.
    The comment is informational; the key payload is the second field.
    """
    parts = key_body.strip().split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return key_body.strip()


def _sftp_resolve_user_by_key(username: str, presented_key_body: str):
    """Scan every Transfer user across every account for a matching
    (UserName, SshPublicKey) pair. Returns ``(account_id, server_id, user)``
    or ``None``.

    AccountScopedDict stores entries under tuple keys ``(account_id, key)``,
    so we iterate ``_data.items()`` directly to scan across accounts —
    `__iter__` would only yield the current request's account, which isn't
    set during SFTP auth.
    """
    presented = _sftp_normalize_key(presented_key_body)
    for scoped_key, user in _users._data.items():
        account_id, _ = scoped_key
        if user.get("UserName") != username:
            continue
        for key_record in user.get("SshPublicKeys", []):
            if _sftp_normalize_key(key_record.get("SshPublicKeyBody", "")) == presented:
                return account_id, user["ServerId"], user
    return None


def _sftp_server_state(account_id: str, server_id: str):
    """Return the State of a Transfer server (ONLINE / OFFLINE) or None
    if the server doesn't exist for the given account."""
    server = _servers._data.get((account_id, server_id))
    return server["State"] if server else None


# ── S3 path resolution (HomeDirectory + HomeDirectoryMappings) ─────────────


def _sftp_normalize_virtual_path(virtual: str) -> str:
    """Make `virtual` an absolute, slash-rooted path with no trailing slash
    (except for the root). Resolves `..` to prevent chroot escape."""
    if not virtual:
        virtual = "/"
    if not virtual.startswith("/"):
        virtual = "/" + virtual
    parts: list = []
    for seg in virtual.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            if parts:
                parts.pop()
            continue
        parts.append(seg)
    if not parts:
        return "/"
    return "/" + "/".join(parts)


def _sftp_resolve_s3_target(user: dict, virtual_path: str):
    """Translate an SFTP-side virtual path to an S3 ``(bucket, key)`` pair.

    PATH mode  — `HomeDirectory` is e.g. `/my-bucket/some/prefix`.
    LOGICAL    — `HomeDirectoryMappings` list, longest prefix wins.
    """
    virtual_path = _sftp_normalize_virtual_path(virtual_path)
    home_type = user.get("HomeDirectoryType", "PATH")

    if home_type == "LOGICAL":
        mappings = user.get("HomeDirectoryMappings") or []
        sorted_maps = sorted(mappings, key=lambda m: len(m.get("Entry", "")), reverse=True)
        for mapping in sorted_maps:
            entry = _sftp_normalize_virtual_path(mapping.get("Entry", "/"))
            target = mapping.get("Target", "")
            if virtual_path == entry or virtual_path.startswith(entry + "/"):
                rest = virtual_path[len(entry):].lstrip("/")
                full = target.rstrip("/") + ("/" + rest if rest else "")
                return _sftp_split_bucket_key(full)
        return "", virtual_path.lstrip("/")

    # PATH mode
    home = user.get("HomeDirectory") or "/"
    base = _sftp_normalize_virtual_path(home)
    if virtual_path == "/":
        full = base
    elif virtual_path.startswith(base + "/") or virtual_path == base:
        full = virtual_path
    else:
        full = base.rstrip("/") + virtual_path
    return _sftp_split_bucket_key(full)


def _sftp_split_bucket_key(full: str):
    """Split ``/bucket/key/segments`` → ``("bucket", "key/segments")``."""
    full = full.lstrip("/")
    if not full:
        return "", ""
    if "/" in full:
        bucket, key = full.split("/", 1)
        return bucket, key
    return full, ""


# ── S3 helpers — read / write / list against the in-memory S3 service ──────


def _sftp_s3_bucket(account_id: str, bucket: str):
    """Return the bucket dict for the given account, or None.

    S3 lives in a different module so we lazy-import inside the helper to
    avoid an import cycle at module load.
    """
    from ministack.services import s3 as s3_mod
    return s3_mod._buckets._data.get((account_id, bucket))


def _sftp_s3_list_keys(account_id: str, bucket: str, prefix: str):
    b = _sftp_s3_bucket(account_id, bucket)
    if not b:
        return []
    return [k for k in b.get("objects", {}).keys() if k.startswith(prefix)]


def _sftp_s3_get_object(account_id: str, bucket: str, key: str):
    b = _sftp_s3_bucket(account_id, bucket)
    if not b:
        return None
    return b.get("objects", {}).get(key)


def _sftp_s3_put_object(account_id: str, bucket: str, key: str, body: bytes) -> None:
    """Write a new object body using the canonical S3 record schema so
    subsequent GetObject / HeadObject calls via the standard S3 API
    return the bytes unchanged. Mirrors ``s3._build_object_record``."""
    import hashlib

    b = _sftp_s3_bucket(account_id, bucket)
    if not b:
        raise FileNotFoundError(f"bucket {bucket} not found")
    objects = b.setdefault("objects", {})
    objects[key] = {
        "body": body,
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"',
        "last_modified": now_iso(),
        "size": len(body),
        "metadata": {},
        "preserved_headers": {},
    }


def _sftp_s3_delete_object(account_id: str, bucket: str, key: str) -> bool:
    b = _sftp_s3_bucket(account_id, bucket)
    if not b:
        return False
    objects = b.get("objects", {})
    if key in objects:
        del objects[key]
        return True
    return False


# ── SFTP / SSH server classes ──────────────────────────────────────────────


def _sftp_build_sftp_factory(restrict_to_server_id: Optional[str] = None):
    """asyncssh sftp_factory closure. When ``restrict_to_server_id`` is set
    (per-server-port mode), the SFTP layer rejects connections whose
    authenticated user belongs to a different Transfer server."""
    def _factory(chan):
        return _MiniStackSFTPServer(chan, restrict_to_server_id=restrict_to_server_id)
    return _factory


def _sftp_build_server_factory(restrict_to_server_id: Optional[str] = None):
    """asyncssh `SSHServer` factory closure."""
    def _factory():
        return _MiniStackSSHServer(restrict_to_server_id=restrict_to_server_id)
    return _factory


class _MiniStackSSHServer(asyncssh.SSHServer if _ASYNCSSH_AVAILABLE else object):
    """SSH-side auth callback. Resolves the (account, server, user) tuple
    from the presented public key and stashes it on the connection so the
    SFTP layer can read it back.

    Subclassing :class:`asyncssh.SSHServer` (rather than just duck-typing
    the relevant methods) is required because asyncssh probes a wide set
    of optional auth methods (host-based, kbdint, GSSAPI) on the server
    instance during userauth negotiation; the default base-class
    implementations return ``False`` for everything we don't support.
    """

    def __init__(self, restrict_to_server_id: Optional[str] = None):
        super().__init__() if _ASYNCSSH_AVAILABLE else None
        self._restrict_to_server_id = restrict_to_server_id
        self._resolved = None
        self._username = None
        self._conn = None

    def connection_made(self, conn):
        self._conn = conn

    def connection_lost(self, exc):
        pass

    def begin_auth(self, username):
        # True → continue to public-key validation. False would mean "no
        # auth needed", which we never want.
        self._username = username
        return True

    def password_auth_supported(self):
        return False

    def public_key_auth_supported(self):
        return True

    def validate_public_key(self, username, key):
        """Called by asyncssh for each presented public key. Returns True
        if the key is registered for this username on some Transfer server.

        We export the key in OpenSSH format and string-compare against the
        body stored via `ImportSshPublicKey`. This is deliberately stricter
        than fingerprint comparison — same key body, exact match.
        """
        try:
            presented_body = key.export_public_key("openssh").decode("utf-8")
        except Exception as e:
            logger.debug("SFTP: failed to export presented key: %s", e)
            return False

        match = _sftp_resolve_user_by_key(username, presented_body)
        if not match:
            return False

        account_id, server_id, user = match
        if self._restrict_to_server_id and server_id != self._restrict_to_server_id:
            return False
        if _sftp_server_state(account_id, server_id) != "ONLINE":
            return False

        self._resolved = (account_id, server_id, user)
        if self._conn is not None:
            self._conn.set_extra_info(ministack_user=user)
            self._conn.set_extra_info(ministack_server_id=server_id)
            self._conn.set_extra_info(ministack_account_id=account_id)
        return True


class _MiniStackSFTPServer(asyncssh.SFTPServer if _ASYNCSSH_AVAILABLE else object):
    """SFTP filesystem backed by MiniStack's in-memory S3 state.

    asyncssh instantiates this per session, passing the SSH channel so we
    can recover the auth metadata stashed by `_MiniStackSSHServer`.

    Subclassing :class:`asyncssh.SFTPServer` (rather than duck-typing) is
    required so unsupported file ops (statvfs, link, etc.) inherit the
    base implementations that politely return SSH_FX_OP_UNSUPPORTED to
    the client instead of crashing the session.
    """

    def __init__(self, chan, restrict_to_server_id: Optional[str] = None):
        if _ASYNCSSH_AVAILABLE:
            super().__init__(chan)
        self._chan = chan
        self._restrict_to_server_id = restrict_to_server_id
        # asyncssh's SSHServerChannel exposes the parent connection via
        # ``get_connection``; older versions expose it as ``_conn``.
        conn = None
        if hasattr(chan, "get_connection"):
            conn = chan.get_connection()
        elif hasattr(chan, "_conn"):
            conn = chan._conn
        self._user = conn.get_extra_info("ministack_user") if conn else None
        self._account_id = conn.get_extra_info("ministack_account_id") if conn else None
        self._server_id = conn.get_extra_info("ministack_server_id") if conn else None
        # Open-file map. Reads use BytesIO over the existing object body;
        # writes accumulate into a BytesIO that we PUT on close.
        self._open_files: dict = {}
        self._next_handle = 1

    def _resolve(self, path):
        """bytes path → (bucket, key) for the currently authenticated user,
        within their account context."""
        from ministack.core.responses import set_request_account_id

        if self._account_id:
            set_request_account_id(self._account_id)
        virtual = path.decode("utf-8", errors="replace") if isinstance(path, bytes) else path
        return _sftp_resolve_s3_target(self._user or {}, virtual)

    @staticmethod
    def _attrs_for_file(size, mtime):
        a = SFTPAttrs()
        a.type = 1  # SSH_FILEXFER_TYPE_REGULAR
        a.size = size
        a.uid = 0
        a.gid = 0
        a.permissions = 0o100644
        a.atime = mtime
        a.mtime = mtime
        return a

    @staticmethod
    def _attrs_for_dir(mtime):
        a = SFTPAttrs()
        a.type = 2  # SSH_FILEXFER_TYPE_DIRECTORY
        a.size = 0
        a.uid = 0
        a.gid = 0
        a.permissions = 0o040755
        a.atime = mtime
        a.mtime = mtime
        return a

    # ---- path / metadata ops --------------------------------------------

    def realpath(self, path):
        virtual = path.decode("utf-8", errors="replace")
        return _sftp_normalize_virtual_path(virtual).encode("utf-8")

    def stat(self, path):
        bucket, key = self._resolve(path)
        if not bucket:
            return self._attrs_for_dir(int(time.time()))
        obj = _sftp_s3_get_object(self._account_id, bucket, key)
        if obj:
            return self._attrs_for_file(obj.get("size", 0), int(time.time()))
        prefix = key.rstrip("/") + "/" if key else ""
        for _ in _sftp_s3_list_keys(self._account_id, bucket, prefix):
            return self._attrs_for_dir(int(time.time()))
        if _sftp_s3_bucket(self._account_id, bucket) is None:
            raise asyncssh.SFTPNoSuchFile(f"No such bucket: {bucket}")
        raise asyncssh.SFTPNoSuchFile(f"No such file: {path!r}")

    def lstat(self, path):
        return self.stat(path)

    def fstat(self, file_obj):
        h = self._open_files.get(file_obj)
        if not h:
            raise asyncssh.SFTPFailure("Bad file handle")
        size = h["size"] if h["mode"] == "read" else len(h["buf"].getvalue())
        return self._attrs_for_file(size, h.get("mtime", int(time.time())))

    def setstat(self, path, attrs):
        # No-op so `sftp -p` and rsync don't fail on the mode/time copy.
        return None

    def fsetstat(self, file_obj, attrs):
        return None

    def lsetstat(self, path, attrs):
        return None

    # ---- directory ops ---------------------------------------------------

    async def scandir(self, path):
        bucket, key = self._resolve(path)
        seen_names: set = set()

        for special in (".", ".."):
            attrs = self._attrs_for_dir(int(time.time()))
            yield SFTPName(
                filename=special.encode("utf-8"),
                longname=self._format_longname(special, attrs).encode("utf-8"),
                attrs=attrs,
            )

        if not bucket:
            virtual = path.decode("utf-8", errors="replace") if isinstance(path, bytes) else path
            virtual = _sftp_normalize_virtual_path(virtual)
            sub_entries: set = set()
            for mapping in (self._user or {}).get("HomeDirectoryMappings", []) or []:
                entry = _sftp_normalize_virtual_path(mapping.get("Entry", "/"))
                if entry == virtual:
                    continue
                if entry.startswith(virtual.rstrip("/") + "/") or virtual == "/":
                    suffix = entry[len(virtual.rstrip("/")) + 1:].split("/")[0]
                    if suffix:
                        sub_entries.add(suffix)
            for name in sorted(sub_entries):
                if name in seen_names:
                    continue
                seen_names.add(name)
                attrs = self._attrs_for_dir(int(time.time()))
                yield SFTPName(
                    filename=name.encode("utf-8"),
                    longname=self._format_longname(name, attrs).encode("utf-8"),
                    attrs=attrs,
                )
            return

        prefix = key.rstrip("/") + "/" if key else ""
        b = _sftp_s3_bucket(self._account_id, bucket)
        if b is None:
            return

        for full_key in sorted(b.get("objects", {}).keys()):
            if not full_key.startswith(prefix):
                continue
            remainder = full_key[len(prefix):]
            if not remainder:
                continue
            head, sep, _ = remainder.partition("/")
            if sep:
                if head in seen_names:
                    continue
                seen_names.add(head)
                attrs = self._attrs_for_dir(int(time.time()))
                yield SFTPName(
                    filename=head.encode("utf-8"),
                    longname=self._format_longname(head, attrs).encode("utf-8"),
                    attrs=attrs,
                )
            else:
                if head in seen_names:
                    continue
                seen_names.add(head)
                obj = b["objects"][full_key]
                attrs = self._attrs_for_file(obj.get("size", 0), int(time.time()))
                yield SFTPName(
                    filename=head.encode("utf-8"),
                    longname=self._format_longname(head, attrs).encode("utf-8"),
                    attrs=attrs,
                )

    def _format_longname(self, name, attrs) -> str:
        """ls-l-style line for asyncssh's longname field."""
        is_dir = attrs.type == 2
        mode_str = "drwxr-xr-x" if is_dir else "-rw-r--r--"
        size = attrs.size or 0
        mtime = attrs.mtime or int(time.time())
        ts = time.strftime("%b %d %H:%M", time.localtime(mtime))
        return f"{mode_str} 1 ministack ministack {size:>10d} {ts} {name}"

    def mkdir(self, path, attrs):
        bucket, key = self._resolve(path)
        if not bucket:
            raise asyncssh.SFTPFailure("Cannot create directory above bucket level")
        if not key:
            return None
        placeholder_key = key.rstrip("/") + "/"
        _sftp_s3_put_object(self._account_id, bucket, placeholder_key, b"")
        return None

    def rmdir(self, path):
        bucket, key = self._resolve(path)
        if not bucket or not key:
            raise asyncssh.SFTPFailure("Cannot remove bucket root via SFTP")
        prefix = key.rstrip("/") + "/"
        children = [k for k in _sftp_s3_list_keys(self._account_id, bucket, prefix) if k != prefix]
        if children:
            raise asyncssh.SFTPFailure(f"Directory not empty: {path!r}")
        _sftp_s3_delete_object(self._account_id, bucket, prefix)
        return None

    def remove(self, path):
        bucket, key = self._resolve(path)
        if not bucket or not key:
            raise asyncssh.SFTPNoSuchFile(f"Cannot remove {path!r}")
        if not _sftp_s3_delete_object(self._account_id, bucket, key):
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path!r}")
        return None

    def rename(self, oldpath, newpath):
        return self._do_rename(oldpath, newpath, allow_overwrite=False)

    def posix_rename(self, oldpath, newpath):
        return self._do_rename(oldpath, newpath, allow_overwrite=True)

    def _do_rename(self, oldpath, newpath, *, allow_overwrite: bool):
        src_bucket, src_key = self._resolve(oldpath)
        dst_bucket, dst_key = self._resolve(newpath)
        if not src_bucket or not src_key:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {oldpath!r}")
        src_obj = _sftp_s3_get_object(self._account_id, src_bucket, src_key)
        if src_obj is None:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {oldpath!r}")
        if not dst_bucket:
            raise asyncssh.SFTPFailure("Cannot rename above bucket level")
        if not allow_overwrite and _sftp_s3_get_object(self._account_id, dst_bucket, dst_key):
            raise asyncssh.SFTPFailure(f"Destination exists: {newpath!r}")
        # S3 has no atomic rename — copy + delete.
        _sftp_s3_put_object(self._account_id, dst_bucket, dst_key, src_obj.get("body", b""))
        _sftp_s3_delete_object(self._account_id, src_bucket, src_key)
        return None

    # ---- file ops --------------------------------------------------------

    def open(self, path, pflags, attrs):
        # asyncssh masks: SSH_FXF_READ=0x01, SSH_FXF_WRITE=0x02,
        # SSH_FXF_APPEND=0x04, SSH_FXF_CREAT=0x08, SSH_FXF_TRUNC=0x10,
        # SSH_FXF_EXCL=0x20.
        bucket, key = self._resolve(path)
        if not bucket or not key:
            raise asyncssh.SFTPFailure("Open requires bucket+key")

        wants_write = bool(pflags & 0x02)
        wants_create = bool(pflags & 0x08)
        wants_truncate = bool(pflags & 0x10)
        wants_excl = bool(pflags & 0x20)

        existing = _sftp_s3_get_object(self._account_id, bucket, key)

        if wants_write:
            if existing and wants_excl:
                raise asyncssh.SFTPFailure(f"File exists: {path!r}")
            if not existing and not wants_create:
                raise asyncssh.SFTPNoSuchFile(f"No such file: {path!r}")
            initial = b"" if (wants_truncate or not existing) else existing.get("body", b"")
            handle = self._next_handle
            self._next_handle += 1
            self._open_files[handle] = {
                "mode": "write",
                "bucket": bucket,
                "key": key,
                "buf": io.BytesIO(initial),
                "size": len(initial),
                "mtime": int(time.time()),
            }
            return handle

        # Read mode
        if not existing:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path!r}")
        handle = self._next_handle
        self._next_handle += 1
        body = existing.get("body", b"")
        self._open_files[handle] = {
            "mode": "read",
            "bucket": bucket,
            "key": key,
            "buf": io.BytesIO(body),
            "size": len(body),
            "mtime": int(time.time()),
        }
        return handle

    def read(self, file_obj, offset, size):
        h = self._open_files.get(file_obj)
        if not h:
            raise asyncssh.SFTPFailure("Bad file handle")
        h["buf"].seek(offset)
        data = h["buf"].read(size)
        return data if data else b""

    def write(self, file_obj, offset, data):
        h = self._open_files.get(file_obj)
        if not h:
            raise asyncssh.SFTPFailure("Bad file handle")
        if h["mode"] != "write":
            raise asyncssh.SFTPFailure("File not open for writing")
        h["buf"].seek(offset)
        h["buf"].write(data)
        return len(data)

    def close(self, file_obj):
        h = self._open_files.pop(file_obj, None)
        if not h:
            return None
        if h["mode"] == "write":
            _sftp_s3_put_object(self._account_id, h["bucket"], h["key"], h["buf"].getvalue())
        return None

    def fsync(self, file_obj):
        return None

    # ---- explicitly unsupported -----------------------------------------

    def link(self, oldpath, newpath):
        raise asyncssh.SFTPOpUnsupported("S3-backed SFTP does not support hardlinks")

    def symlink(self, oldpath, newpath):
        raise asyncssh.SFTPOpUnsupported("S3-backed SFTP does not support symlinks")

    def readlink(self, path):
        raise asyncssh.SFTPOpUnsupported("S3-backed SFTP does not support symlinks")

    def lock(self, file_obj, offset, length, flags):
        return None

    def unlock(self, file_obj, offset, length):
        return None

    def exit(self):
        # Flush handles the client forgot to close.
        for handle in list(self._open_files):
            self.close(handle)


# ── SFTP lifecycle — called from app.py lifespan + Create/Delete handlers ──


async def sftp_start() -> None:
    """Idempotent: bind the shared SFTP listener on `SFTP_PORT` if enabled."""
    if not _sftp_enabled():
        logger.info("SFTP: disabled (asyncssh missing or SFTP_ENABLED=0)")
        return

    global _sftp_shared_acceptor, _sftp_next_per_server_port
    _sftp_next_per_server_port = _per_server_base_port()

    async with _sftp_lock:
        if _sftp_shared_acceptor is not None:
            return
        host_key = _sftp_load_or_generate_host_key()
        port = _shared_port()
        try:
            _sftp_shared_acceptor = await asyncssh.listen(
                host=_bind_host(),
                port=port,
                server_factory=_sftp_build_server_factory(),
                server_host_keys=[host_key],
                sftp_factory=_sftp_build_sftp_factory(),
                allow_scp=False,
            )
            logger.info("SFTP: listening on %s:%d (shared)", _bind_host(), port)
        except OSError as e:
            logger.warning("SFTP: failed to bind %s:%d (%s); SFTP unavailable", _bind_host(), port, e)
            _sftp_shared_acceptor = None


async def sftp_stop() -> None:
    """Stop the shared listener and any per-server listeners."""
    global _sftp_shared_acceptor
    async with _sftp_lock:
        if _sftp_shared_acceptor is not None:
            try:
                _sftp_shared_acceptor.close()
                await _sftp_shared_acceptor.wait_closed()
            except Exception as e:
                logger.debug("SFTP: error closing shared acceptor: %s", e)
            _sftp_shared_acceptor = None
        for sid, acceptor in list(_sftp_per_server_acceptors.items()):
            try:
                acceptor.close()
                await acceptor.wait_closed()
            except Exception as e:
                logger.debug("SFTP: error closing per-server acceptor for %s: %s", sid, e)
        _sftp_per_server_acceptors.clear()
        _sftp_per_server_ports.clear()
        logger.info("SFTP: stopped")


async def sftp_start_server_listener(server_id: str) -> Optional[int]:
    """Bind a per-server SFTP port for `server_id`. Returns the port (or
    None when SFTP_PORT_PER_SERVER is off or asyncssh isn't available).

    Called from `_create_server` when per-server-port mode is on.
    """
    if not (_sftp_enabled() and _port_per_server()):
        return None
    global _sftp_next_per_server_port
    if _sftp_next_per_server_port is None:
        _sftp_next_per_server_port = _per_server_base_port()
    async with _sftp_lock:
        if server_id in _sftp_per_server_acceptors:
            return _sftp_per_server_ports.get(server_id)
        host_key = _sftp_load_or_generate_host_key()
        port = _sftp_next_per_server_port
        _sftp_next_per_server_port += 1
        try:
            acceptor = await asyncssh.listen(
                host=_bind_host(),
                port=port,
                server_factory=_sftp_build_server_factory(server_id),
                server_host_keys=[host_key],
                sftp_factory=_sftp_build_sftp_factory(server_id),
                allow_scp=False,
            )
            _sftp_per_server_acceptors[server_id] = acceptor
            _sftp_per_server_ports[server_id] = port
            logger.info("SFTP: per-server listener for %s on %s:%d", server_id, _bind_host(), port)
            return port
        except OSError as e:
            logger.warning("SFTP: failed to bind per-server port %d for %s: %s", port, server_id, e)
            return None


async def sftp_stop_server_listener(server_id: str) -> None:
    async with _sftp_lock:
        acceptor = _sftp_per_server_acceptors.pop(server_id, None)
        _sftp_per_server_ports.pop(server_id, None)
        if acceptor is None:
            return
        try:
            acceptor.close()
            await acceptor.wait_closed()
        except Exception as e:
            logger.debug("SFTP: error closing per-server acceptor for %s: %s", server_id, e)


def sftp_per_server_port(server_id: str) -> Optional[int]:
    """Return the bound port for `server_id` in per-server-port mode, if any.
    Used by the `/_ministack/transfer/sftp-ports` admin endpoint."""
    return _sftp_per_server_ports.get(server_id)
