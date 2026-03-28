"""
State persistence for MiniStack services.
When PERSIST_STATE=1, service state is saved to STATE_DIR on shutdown
and reloaded on startup.
"""

import os
import json
import logging
import tempfile

logger = logging.getLogger("persistence")

PERSIST_STATE = os.environ.get("PERSIST_STATE", "0") == "1"
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/ministack-state")


def save_state(service: str, data: dict) -> None:
    if not PERSIST_STATE:
        return
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = os.path.join(STATE_DIR, f"{service}.json")
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
        logger.info(f"Persistence: saved {service} state to {path}")
    except Exception as e:
        logger.error(f"Persistence: failed to save {service}: {e}")


def load_state(service: str) -> dict | None:
    if not PERSIST_STATE:
        return None
    path = os.path.join(STATE_DIR, f"{service}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        logger.info(f"Persistence: loaded {service} state from {path}")
        return data
    except Exception as e:
        logger.error(f"Persistence: failed to load {service}: {e}")
        return None


def save_all(services: dict) -> None:
    """Save all service states. services = {name: get_state_fn}"""
    for name, get_state in services.items():
        try:
            save_state(name, get_state())
        except Exception as e:
            logger.error(f"Persistence: error getting state for {name}: {e}")
