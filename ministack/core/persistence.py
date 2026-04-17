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

from ministack.core.responses import AccountScopedDict

logger = logging.getLogger("persistence")

PERSIST_STATE = os.environ.get("PERSIST_STATE", "0") == "1"
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/ministack-state")


def _json_default(obj):
    """JSON encoder fallback for AccountScopedDict and tuple keys."""
    if isinstance(obj, AccountScopedDict):
        # Serialize all accounts' data with string keys
        result = {}
        for k, v in obj._data.items():
            # k is (account_id, original_key) tuple
            result[f"{k[0]}\x00{k[1]!r}"] = v
        return {"__scoped__": True, "data": result}
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json_object_hook(obj):
    """JSON decoder hook to restore AccountScopedDict from serialized form."""
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
                json.dump(data, f, default=_json_default)
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
