"""
AWS DocumentDB Emulator.
JSON-based API via X-Amz-Target.
Supports: find, createUser, ...

Mongo APIs: https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html
https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html
Related Discussion: https://github.com/ministackorg/ministack/discussions/303

"""

import json
import logging
from ministack.core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("documentdb")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_state: dict = {"users": [], "collections": {}}  # in-memory storage


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "find": _find,
        "createUser": _create_user,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _find(data):
    collection = data.get("collection", "default")
    filter_query = data.get("filter", {})
    # Simple stub: return empty result set
    return json_response({"result": "ok", "documents": [], "collection": collection})


def _create_user(data):
    username = data.get("username") or data.get("UserName")
    if not username:
        return error_response_json("InvalidParameter", "username required", 400)
    users = _state.setdefault("users", [])
    if any(u.get("username") == username for u in users):
        return error_response_json("UserAlreadyExists", f"User {username} exists", 400)
    users.append({"username": username, "roles": data.get("roles", [])})
    return json_response({"result": "ok"})


def reset():
    _state.clear()
    _state.update({"users": [], "collections": {}})
