"""
State persistence for MiniStack services.
When PERSIST_STATE=1, service state is saved to STATE_DIR on shutdown
and reloaded on startup.
"""

import ast
import json
import logging
import os
import tempfile

from ministack.core.responses import AccountRegionScopedDict, AccountScopedDict

logger = logging.getLogger("persistence")

PERSIST_STATE = os.environ.get("PERSIST_STATE", "0") == "1"
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/ministack-state")

# On-disk state format versions. Files are wrapped as
#   {"__ministack_format__": N, "payload": <service state>}
# load_state refuses a file whose version is NEWER than this binary understands
# rather than mis-parsing it (the downgrade-corruption guard). Version 2 is the
# default and introduced region-scoped (AccountRegionScopedDict) stores.
# Services regionalized after v2 use version 3 so a v2 rollback refuses their
# incompatible snapshots. Legacy unwrapped files (implicit v1, account-scoped)
# still load and migrate. (U4)
STATE_FORMAT_VERSION = 2
SERVICE_STATE_FORMAT_VERSIONS = {
    "athena": 3,
    "batch": 3,
    "ecs": 3,
    "resource_groups": 3,
    "codebuild": 3,
    "mq": 3,
    "servicediscovery": 3,
    "ses": 3,
    "ses_v2": 3,
}


def _state_format_version(service: str) -> int:
    return SERVICE_STATE_FORMAT_VERSIONS.get(service, STATE_FORMAT_VERSION)


def _json_default(obj):
    """JSON encoder fallback for scoped dicts, tuple keys, and bytes.

    Historically, several S3 (and other service) stores held raw request
    bodies as ``bytes``. ``json.dump`` raised ``TypeError`` and
    ``save_state`` silently swallowed the error, leaving ``${service}.json``
    absent on disk (issue #422). Bytes are now serialized as base64 inside a
    tagged dict so round-trip fidelity is preserved even for non-UTF-8
    payloads."""
    if isinstance(obj, AccountRegionScopedDict):
        # Serialize all accounts' and regions' data with string keys
        result = {}
        for k, v in obj._data.items():
            # k is (account_id, region, original_key) tuple
            result[f"{k[0]}\x00{k[1]}\x00{k[2]!r}"] = v
        return {"__account_region_scoped__": True, "data": result}
    if isinstance(obj, AccountScopedDict):
        # Serialize all accounts' data with string keys
        result = {}
        for k, v in obj._data.items():
            # k is (account_id, original_key) tuple
            result[f"{k[0]}\x00{k[1]!r}"] = v
        return {"__scoped__": True, "data": result}
    if isinstance(obj, (bytes, bytearray)):
        import base64
        return {"__bytes__": base64.b64encode(bytes(obj)).decode("ascii")}
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json_object_hook(obj):
    """JSON decoder hook to restore scoped dicts and bytes from serialized form."""
    if obj.get("__account_region_scoped__"):
        arsd = AccountRegionScopedDict()
        for k, v in obj["data"].items():
            account_id, region, key_repr = k.split("\x00", 2)
            # Restore the original key (was serialized with repr())
            try:
                original_key = ast.literal_eval(key_repr)
            except (ValueError, SyntaxError):
                original_key = key_repr
            arsd._data[(account_id, region, original_key)] = v
        return arsd
    if obj.get("__scoped__"):
        asd = AccountScopedDict()
        for k, v in obj["data"].items():
            account_id, key_repr = k.split("\x00", 1)
            # Restore the original key (was serialized with repr())
            try:
                original_key = ast.literal_eval(key_repr)
            except (ValueError, SyntaxError):
                original_key = key_repr
            asd._data[(account_id, original_key)] = v
        return asd
    if "__bytes__" in obj:
        import base64
        return base64.b64decode(obj["__bytes__"])
    return obj


def save_state(service: str, data: dict) -> None:
    if not PERSIST_STATE:
        return
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = os.path.join(STATE_DIR, f"{service}.json")
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(
                    {"__ministack_format__": _state_format_version(service), "payload": data},
                    f, default=_json_default,
                )
            os.replace(tmp, path)
        except BaseException:
            # Clean up temp file on any failure to avoid stale partial writes
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise
        logger.info("Persistence: saved %s state to %s", service, path)
    except Exception as e:
        logger.error("Persistence: failed to save %s: %s", service, e)


def load_state(service: str) -> dict | None:
    if not PERSIST_STATE:
        return None
    path = os.path.join(STATE_DIR, f"{service}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f, object_hook=_json_object_hook)
        # Versioned wrapper (current format). A legacy file (no wrapper) is an
        # implicit v1 payload, returned as-is for backward-compatible migration.
        # A file from a NEWER binary is refused rather than mis-parsed — loading
        # it would corrupt state on downgrade. (U4)
        if isinstance(data, dict) and "__ministack_format__" in data:
            version = data.get("__ministack_format__")
            supported_version = _state_format_version(service)
            if isinstance(version, int) and version > supported_version:
                logger.error(
                    "Persistence: %s state is format v%s but this MiniStack only "
                    "understands v%s — refusing to load (downgrade not supported)",
                    service, version, supported_version,
                )
                return None
            data = data.get("payload")
        logger.info("Persistence: loaded %s state from %s", service, path)
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Persistence: failed to load %s: %s", service, e)
        return None


def save_all(services: dict) -> None:
    """Save all service states. services = {name: get_state_fn}"""
    for name, get_state in services.items():
        try:
            save_state(name, get_state())
        except Exception as e:
            logger.error("Persistence: error getting state for %s: %s", name, e)
