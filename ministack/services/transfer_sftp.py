"""
AWS Transfer Family — SFTP server.

Backs the Transfer service control plane (transfer.py) with an actual SFTP
listener so clients can connect over SSH and transfer files end-to-end.

Architecture
------------
- Single asyncssh listener on `SFTP_PORT` (default 2222) by default.
- Authentication is by SSH public key. The presented key is matched against
  every user's `SshPublicKeys` across every Transfer server (and across
  every account, since `_users` is account-scoped). The matching user
  determines the server identity, account, and home-directory mapping —
  no `username$serverid` decoration needed (that's a LocalStack-ism, not
  AWS behavior).
- `SFTP_PORT_PER_SERVER=1` opts into AWS-style per-server endpoints. Each
  Transfer server gets its own listener allocated from `SFTP_BASE_PORT`
  (default 2300, incrementing). The single shared listener is also kept
  active for backwards compatibility.
- The SFTP virtual filesystem is backed by MiniStack's in-memory S3 state.
  HomeDirectoryMappings (LOGICAL) translates virtual paths to S3 bucket+key
  prefixes; HomeDirectory (PATH) interprets `/bucket/key` paths directly.

S3 semantics under SFTP
-----------------------
- `mkdir <prefix>` writes a zero-byte object at `<prefix>/`. Listing then
  shows the directory because the object exists; removing the prefix
  (rmdir) deletes that placeholder if no other objects share the prefix.
- `rename` is implemented as copy + delete (S3 has no atomic rename).
- Writes buffer in memory until close, then PUT the entire body at once
  (no S3 multipart). That's fine for tests — production-scale transfers
  aren't the local-emulator use case.
- `setstat` / `chmod` / `chown` / `utime` are no-ops; S3 has no POSIX
  permissions or mtime. SFTP clients that try to preserve attributes on
  upload (e.g. `sftp -p`, rsync) get a successful no-op.

Host key
--------
Persistent in `${STATE_DIR}/transfer-host-key` when `PERSIST_STATE=1`,
ephemeral (regenerated each run) otherwise. Persisting prevents the
"WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED" stderr noise on every
container restart.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import time
from typing import AsyncIterator, Optional

logger = logging.getLogger("transfer.sftp")

# asyncssh is an optional extra (pip install ministack[full]). We import
# lazily so that a base install with the transfer service still works for
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


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _is_truthy(v: str | None) -> bool:
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


# ---------------------------------------------------------------------------
# Module-level state — listeners + host key
# ---------------------------------------------------------------------------

_shared_acceptor = None  # type: Optional[object]
_per_server_acceptors: dict[str, object] = {}
_per_server_ports: dict[str, int] = {}
_next_per_server_port: Optional[int] = None
_host_key = None  # type: Optional[object]
_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Host key load / generate / persist
# ---------------------------------------------------------------------------


def _host_key_path() -> str:
    from ministack.core.persistence import STATE_DIR

    return os.path.join(STATE_DIR, "transfer-host-key")


def _load_or_generate_host_key():
    """Return an asyncssh-compatible host key.

    Persistence rules:
    - PERSIST_STATE=1 + key file exists  → load it.
    - PERSIST_STATE=1 + key file missing → generate, save, return.
    - PERSIST_STATE=0                    → generate ephemerally, no save.

    Persisting matters because OpenSSH clients cache the host key fingerprint
    in known_hosts, and a fresh key on every container restart triggers the
    big "REMOTE HOST IDENTIFICATION HAS CHANGED" warning that breaks
    automated SFTP clients.
    """
    global _host_key
    if _host_key is not None:
        return _host_key

    from ministack.core.persistence import PERSIST_STATE

    path = _host_key_path()
    if PERSIST_STATE and os.path.exists(path):
        try:
            _host_key = asyncssh.read_private_key(path)
            logger.info("SFTP: loaded persisted host key from %s", path)
            return _host_key
        except Exception as e:
            logger.warning(
                "SFTP: failed to load persisted host key %s (%s); regenerating",
                path,
                e,
            )

    _host_key = asyncssh.generate_private_key("ssh-ed25519")
    if PERSIST_STATE:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            _host_key.write_private_key(path)
            logger.info("SFTP: persisted new host key at %s", path)
        except Exception as e:
            logger.warning("SFTP: failed to persist host key to %s: %s", path, e)
    else:
        logger.info("SFTP: generated ephemeral host key (PERSIST_STATE=0)")
    return _host_key


# ---------------------------------------------------------------------------
# User lookup — find (account_id, server_id, user_record) by SSH key
# ---------------------------------------------------------------------------


def _normalize_key(key_body: str) -> str:
    """Strip comments + whitespace for stable comparison.

    SSH keys in `authorized_keys` look like ``ssh-ed25519 AAAAC3...== comment``.
    The comment is informational; the key payload is the second field.
    """
    parts = key_body.strip().split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return key_body.strip()


def _resolve_user_by_key(username: str, presented_key_body: str):
    """Scan every Transfer user across every account for a matching
    (UserName, SshPublicKey) pair. Returns ``(account_id, server_id, user)``
    or ``None`` if no match.

    AccountScopedDict stores entries under tuple keys ``(account_id, key)``,
    so we iterate ``_data.items()`` directly to scan across accounts —
    `__iter__` would only yield the current request's account, which isn't
    set during SFTP auth.
    """
    from ministack.services import transfer

    presented = _normalize_key(presented_key_body)
    for scoped_key, user in transfer._users._data.items():
        account_id, _ = scoped_key
        if user.get("UserName") != username:
            continue
        for key_record in user.get("SshPublicKeys", []):
            if _normalize_key(key_record.get("SshPublicKeyBody", "")) == presented:
                return account_id, user["ServerId"], user
    return None


def _server_state(account_id: str, server_id: str) -> Optional[str]:
    """Return the State of a Transfer server (ONLINE / OFFLINE) or None
    if the server doesn't exist for the given account."""
    from ministack.services import transfer

    server = transfer._servers._data.get((account_id, server_id))
    return server["State"] if server else None


# ---------------------------------------------------------------------------
# S3 path resolution (HomeDirectory + HomeDirectoryMappings)
# ---------------------------------------------------------------------------


def _normalize_virtual_path(virtual: str) -> str:
    """Make `virtual` an absolute, slash-rooted path with no trailing slash
    (except for the root itself, which stays ``/``).

    Resolves ``..`` segments to prevent escaping the chroot."""
    if not virtual:
        virtual = "/"
    if not virtual.startswith("/"):
        virtual = "/" + virtual
    parts: list[str] = []
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


def _resolve_s3_target(user: dict, virtual_path: str) -> tuple[str, str]:
    """Translate an SFTP-side virtual path to an S3 ``(bucket, key)`` pair.

    Two AWS-supported modes:
    - **PATH** (`HomeDirectoryType=PATH`): the user's `HomeDirectory` is a
      string like ``/my-bucket/some/prefix``. Virtual paths are resolved
      relative to that root, then split on the first slash to get bucket
      vs key.
    - **LOGICAL** (`HomeDirectoryType=LOGICAL`): the user has a list of
      `HomeDirectoryMappings` of shape ``{Entry: "/foo", Target: "/my-bucket/some/prefix"}``.
      The longest prefix match against the virtual path wins.

    Returns ``(bucket, key)`` where ``key`` may be empty (root of bucket).
    """
    virtual_path = _normalize_virtual_path(virtual_path)
    home_type = user.get("HomeDirectoryType", "PATH")

    if home_type == "LOGICAL":
        mappings = user.get("HomeDirectoryMappings") or []
        # Longest prefix wins so a more-specific mapping overrides a
        # parent. AWS resolves the same way.
        sorted_maps = sorted(
            mappings, key=lambda m: len(m.get("Entry", "")), reverse=True
        )
        for mapping in sorted_maps:
            entry = _normalize_virtual_path(mapping.get("Entry", "/"))
            target = mapping.get("Target", "")
            if virtual_path == entry or virtual_path.startswith(entry + "/"):
                rest = virtual_path[len(entry):].lstrip("/")
                full = target.rstrip("/") + ("/" + rest if rest else "")
                return _split_bucket_key(full)
        # No mapping matched — synthesize a virtual root (no bucket).
        return "", virtual_path.lstrip("/")

    # PATH mode
    home = user.get("HomeDirectory") or "/"
    base = _normalize_virtual_path(home)
    if virtual_path == "/":
        full = base
    elif virtual_path.startswith(base + "/") or virtual_path == base:
        full = virtual_path
    else:
        # Treat absolute-looking virtual path as relative to home.
        full = base.rstrip("/") + virtual_path
    return _split_bucket_key(full)


def _split_bucket_key(full: str) -> tuple[str, str]:
    """Split ``/bucket/key/segments`` → ``("bucket", "key/segments")``."""
    full = full.lstrip("/")
    if not full:
        return "", ""
    if "/" in full:
        bucket, key = full.split("/", 1)
        return bucket, key
    return full, ""


# ---------------------------------------------------------------------------
# S3 helpers — read / write / list against the in-memory S3 service
# ---------------------------------------------------------------------------


def _s3_module():
    from ministack.services import s3 as s3_mod

    return s3_mod


def _bucket(account_id: str, bucket: str):
    """Return the bucket dict for the given account, or None."""
    s3_mod = _s3_module()
    return s3_mod._buckets._data.get((account_id, bucket))


def _list_keys(account_id: str, bucket: str, prefix: str) -> list[str]:
    b = _bucket(account_id, bucket)
    if not b:
        return []
    return [k for k in b.get("objects", {}).keys() if k.startswith(prefix)]


def _get_object(account_id: str, bucket: str, key: str) -> Optional[dict]:
    b = _bucket(account_id, bucket)
    if not b:
        return None
    return b.get("objects", {}).get(key)


def _put_object(account_id: str, bucket: str, key: str, body: bytes) -> None:
    """Write a new object body using the canonical S3 record schema so
    subsequent GetObject / HeadObject calls via the standard S3 API
    return the bytes unchanged.

    Schema mirrors :func:`ministack.services.s3._build_object_record` —
    field names are lowercase/snake_case and the body is stored under
    ``body`` (not ``Body``)."""
    from ministack.core.responses import now_iso

    b = _bucket(account_id, bucket)
    if not b:
        raise FileNotFoundError(f"bucket {bucket} not found")
    objects = b.setdefault("objects", {})
    objects[key] = {
        "body": body,
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": '"' + _md5_hex(body) + '"',
        "last_modified": now_iso(),
        "size": len(body),
        "metadata": {},
        "preserved_headers": {},
    }


def _delete_object(account_id: str, bucket: str, key: str) -> bool:
    b = _bucket(account_id, bucket)
    if not b:
        return False
    objects = b.get("objects", {})
    if key in objects:
        del objects[key]
        return True
    return False


def _md5_hex(body: bytes) -> str:
    import hashlib

    return hashlib.md5(body, usedforsecurity=False).hexdigest()


# ---------------------------------------------------------------------------
# SFTP server implementation
# ---------------------------------------------------------------------------


def _build_sftp_factory(restrict_to_server_id: Optional[str] = None):
    """Build the asyncssh sftp_factory closure.

    When ``restrict_to_server_id`` is set (per-server-port mode), the SFTP
    layer rejects connections whose authenticated user belongs to a
    different Transfer server.
    """

    def _factory(chan):
        return _MiniStackSFTPServer(chan, restrict_to_server_id=restrict_to_server_id)

    return _factory


def _build_server_factory(restrict_to_server_id: Optional[str] = None):
    """Build the asyncssh `SSHServer` factory closure."""

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
        self._resolved: Optional[tuple[str, str, dict]] = None
        self._username: Optional[str] = None
        self._conn = None

    def connection_made(self, conn):
        self._conn = conn

    def connection_lost(self, exc):
        pass

    def begin_auth(self, username):
        # Returning True means "auth required" — i.e. continue to public-key
        # validation. Returning False would mean "no auth needed", which we
        # never want.
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

        match = _resolve_user_by_key(username, presented_body)
        if not match:
            return False

        account_id, server_id, user = match
        # Per-server-port mode: the listener was bound for a specific
        # Transfer server, so reject keys that belong to users on a
        # different one.
        if self._restrict_to_server_id and server_id != self._restrict_to_server_id:
            return False
        # Server must be ONLINE (matches AWS — StopServer disables auth).
        if _server_state(account_id, server_id) != "ONLINE":
            return False

        self._resolved = (account_id, server_id, user)
        # Pin the resolved tuple onto the connection so the SFTP factory
        # can find it. asyncssh exposes `set_extra_info`/`get_extra_info`
        # for exactly this kind of per-connection metadata.
        if hasattr(self, "_conn") and self._conn is not None:
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
        # Map open file handles → in-memory buffer. Reads use
        # BytesIO over the existing object body; writes accumulate into a
        # BytesIO that we PUT on close.
        self._open_files: dict[int, dict] = {}
        self._next_handle = 1

    # ---- helpers ---------------------------------------------------------

    def _resolve(self, path: bytes) -> tuple[str, str]:
        """bytes path → (bucket, key) for the currently authenticated user,
        within their account context."""
        from ministack.core.responses import set_request_account_id

        if self._account_id:
            set_request_account_id(self._account_id)
        virtual = path.decode("utf-8", errors="replace") if isinstance(path, bytes) else path
        return _resolve_s3_target(self._user or {}, virtual)

    @staticmethod
    def _ensure_bytes(p) -> bytes:
        if isinstance(p, str):
            return p.encode("utf-8")
        return p

    @staticmethod
    def _attrs_for_file(size: int, mtime: int) -> "SFTPAttrs":
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
    def _attrs_for_dir(mtime: int) -> "SFTPAttrs":
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
        # Canonicalise without hitting S3 — asyncssh expects bytes back.
        virtual = path.decode("utf-8", errors="replace")
        return _normalize_virtual_path(virtual).encode("utf-8")

    def stat(self, path):
        bucket, key = self._resolve(path)
        # Root or a HomeDirectoryMappings virtual node with no bucket
        # backing it → directory, no I/O needed.
        if not bucket:
            return self._attrs_for_dir(int(time.time()))

        # Direct object hit?
        obj = _get_object(self._account_id, bucket, key)
        if obj:
            return self._attrs_for_file(obj.get("size", 0), int(time.time()))

        # Implicit directory? Any object under this prefix counts.
        prefix = key.rstrip("/") + "/" if key else ""
        for k in _list_keys(self._account_id, bucket, prefix):
            return self._attrs_for_dir(int(time.time()))

        # Bucket existed but no object → not found.
        if _bucket(self._account_id, bucket) is None:
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
        # S3 has no POSIX permissions / mtime to preserve. No-op so that
        # `sftp -p` and rsync don't fail on the mode/time copy step.
        return None

    def fsetstat(self, file_obj, attrs):
        return None

    def lsetstat(self, path, attrs):
        return None

    # ---- directory ops ---------------------------------------------------

    async def scandir(self, path) -> AsyncIterator["SFTPName"]:
        bucket, key = self._resolve(path)
        seen_names: set[str] = set()

        # Always emit `.` and `..` first — clients (including OpenSSH's
        # `sftp` CLI) expect them.
        for special in (".", ".."):
            attrs = self._attrs_for_dir(int(time.time()))
            yield SFTPName(
                filename=special.encode("utf-8"),
                longname=self._format_longname(special, attrs).encode("utf-8"),
                attrs=attrs,
            )

        if not bucket:
            # Virtual-only path (LOGICAL with no matching mapping). Surface
            # any LOGICAL mapping `Entry` segments that live one level under
            # this path so listing roots still shows directories.
            virtual = path.decode("utf-8", errors="replace") if isinstance(path, bytes) else path
            virtual = _normalize_virtual_path(virtual)
            sub_entries: set[str] = set()
            for mapping in (self._user or {}).get("HomeDirectoryMappings", []) or []:
                entry = _normalize_virtual_path(mapping.get("Entry", "/"))
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
        b = _bucket(self._account_id, bucket)
        if b is None:
            return

        for full_key in sorted(b.get("objects", {}).keys()):
            if not full_key.startswith(prefix):
                continue
            remainder = full_key[len(prefix):]
            if not remainder:
                continue
            # Subdirectory? Take the first segment.
            head, sep, _ = remainder.partition("/")
            if sep:
                # It's a subdirectory entry — emit once.
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
                # It's a file directly in this dir.
                if head in seen_names:
                    continue
                seen_names.add(head)
                obj = b["objects"][full_key]
                # mkdir placeholder is a zero-byte object whose key ends
                # with `/`. asyncssh's prefix loop strips the trailing
                # slash, so head ends up empty for those — already filtered.
                attrs = self._attrs_for_file(obj.get("size", 0), int(time.time()))
                yield SFTPName(
                    filename=head.encode("utf-8"),
                    longname=self._format_longname(head, attrs).encode("utf-8"),
                    attrs=attrs,
                )

    def _format_longname(self, name: str, attrs) -> str:
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
            # mkdir at bucket root is a no-op — bucket already exists if
            # we resolved to it.
            return None
        placeholder_key = key.rstrip("/") + "/"
        _put_object(self._account_id, bucket, placeholder_key, b"")
        return None

    def rmdir(self, path):
        bucket, key = self._resolve(path)
        if not bucket or not key:
            raise asyncssh.SFTPFailure("Cannot remove bucket root via SFTP")
        prefix = key.rstrip("/") + "/"
        # Refuse to rmdir a non-empty directory (matches POSIX rmdir).
        children = [
            k for k in _list_keys(self._account_id, bucket, prefix)
            if k != prefix
        ]
        if children:
            raise asyncssh.SFTPFailure(f"Directory not empty: {path!r}")
        # Best-effort delete of the placeholder.
        _delete_object(self._account_id, bucket, prefix)
        return None

    def remove(self, path):
        bucket, key = self._resolve(path)
        if not bucket or not key:
            raise asyncssh.SFTPNoSuchFile(f"Cannot remove {path!r}")
        if not _delete_object(self._account_id, bucket, key):
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
        src_obj = _get_object(self._account_id, src_bucket, src_key)
        if src_obj is None:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {oldpath!r}")
        if not dst_bucket:
            raise asyncssh.SFTPFailure("Cannot rename above bucket level")
        if not allow_overwrite and _get_object(self._account_id, dst_bucket, dst_key):
            raise asyncssh.SFTPFailure(f"Destination exists: {newpath!r}")
        # S3 has no atomic rename — copy + delete.
        _put_object(self._account_id, dst_bucket, dst_key, src_obj.get("body", b""))
        _delete_object(self._account_id, src_bucket, src_key)
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

        existing = _get_object(self._account_id, bucket, key)

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
        if not data:
            # asyncssh interprets b"" as EOF.
            return b""
        return data

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
            body = h["buf"].getvalue()
            _put_object(self._account_id, h["bucket"], h["key"], body)
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
        # Flush any handles the client forgot to close.
        for handle in list(self._open_files):
            self.close(handle)


# ---------------------------------------------------------------------------
# Lifecycle — start/stop, called by app.py lifespan + transfer.py CRUD
# ---------------------------------------------------------------------------


async def start() -> None:
    """Idempotent: start the shared SFTP listener on `SFTP_PORT` if enabled."""
    if not _sftp_enabled():
        logger.info("SFTP: disabled (asyncssh missing or SFTP_ENABLED=0)")
        return

    global _shared_acceptor, _next_per_server_port
    _next_per_server_port = _per_server_base_port()

    async with _lock:
        if _shared_acceptor is not None:
            return
        host_key = _load_or_generate_host_key()
        port = _shared_port()
        try:
            _shared_acceptor = await asyncssh.listen(
                host=_bind_host(),
                port=port,
                server_factory=_build_server_factory(),
                server_host_keys=[host_key],
                sftp_factory=_build_sftp_factory(),
                allow_scp=False,
            )
            logger.info("SFTP: listening on %s:%d (shared)", _bind_host(), port)
        except OSError as e:
            logger.warning("SFTP: failed to bind %s:%d (%s); SFTP unavailable", _bind_host(), port, e)
            _shared_acceptor = None


async def stop() -> None:
    """Stop the shared listener and any per-server listeners."""
    global _shared_acceptor
    async with _lock:
        if _shared_acceptor is not None:
            try:
                _shared_acceptor.close()
                await _shared_acceptor.wait_closed()
            except Exception as e:
                logger.debug("SFTP: error closing shared acceptor: %s", e)
            _shared_acceptor = None
        for sid, acceptor in list(_per_server_acceptors.items()):
            try:
                acceptor.close()
                await acceptor.wait_closed()
            except Exception as e:
                logger.debug("SFTP: error closing per-server acceptor for %s: %s", sid, e)
        _per_server_acceptors.clear()
        _per_server_ports.clear()
        logger.info("SFTP: stopped")


async def start_server_listener(server_id: str) -> Optional[int]:
    """Bind a per-server SFTP port for `server_id`. Returns the port (or
    None if SFTP_PORT_PER_SERVER is off or asyncssh isn't available).

    Called from `transfer._create_server` when per-server-port mode is on.
    """
    if not (_sftp_enabled() and _port_per_server()):
        return None
    global _next_per_server_port
    if _next_per_server_port is None:
        _next_per_server_port = _per_server_base_port()
    async with _lock:
        if server_id in _per_server_acceptors:
            return _per_server_ports.get(server_id)
        host_key = _load_or_generate_host_key()
        port = _next_per_server_port
        _next_per_server_port += 1
        try:
            acceptor = await asyncssh.listen(
                host=_bind_host(),
                port=port,
                server_factory=_build_server_factory(server_id),
                server_host_keys=[host_key],
                sftp_factory=_build_sftp_factory(server_id),
                allow_scp=False,
            )
            _per_server_acceptors[server_id] = acceptor
            _per_server_ports[server_id] = port
            logger.info("SFTP: per-server listener for %s on %s:%d", server_id, _bind_host(), port)
            return port
        except OSError as e:
            logger.warning("SFTP: failed to bind per-server port %d for %s: %s", port, server_id, e)
            return None


async def stop_server_listener(server_id: str) -> None:
    async with _lock:
        acceptor = _per_server_acceptors.pop(server_id, None)
        _per_server_ports.pop(server_id, None)
        if acceptor is None:
            return
        try:
            acceptor.close()
            await acceptor.wait_closed()
        except Exception as e:
            logger.debug("SFTP: error closing per-server acceptor for %s: %s", server_id, e)


def per_server_port(server_id: str) -> Optional[int]:
    """Return the bound port for `server_id` in per-server mode, if any.
    Used by the Transfer service handlers to surface the port in
    DescribeServer responses (Endpoint.VpcEndpointId-style metadata)."""
    return _per_server_ports.get(server_id)
