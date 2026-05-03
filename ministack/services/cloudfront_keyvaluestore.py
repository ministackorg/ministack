"""
CloudFront KeyValueStore Data-Plane Service Emulator.
JSON REST API — signing name: cloudfront-keyvaluestore.

Paths are under /key-value-stores/{KvsARN}/...

Supports:
  DescribeKeyValueStore, ListKeys, GetKey, PutKey, DeleteKey, UpdateKeys
"""

import base64
import copy
import json
import logging
import re
from datetime import datetime
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, json_response, new_uuid

logger = logging.getLogger("cloudfront-keyvaluestore")

# ---------------------------------------------------------------------------
# Path regexes — ARN contains a slash (e.g. arn:aws:cloudfront::123:key-value-store/name).
# The store-name segment ([a-zA-Z0-9_-]+) never contains a slash, so the
# /keys boundary anchors unambiguously against the trailing name segment.
# ---------------------------------------------------------------------------
_KEY_RE = re.compile(r"^/key-value-stores/(arn:.+?/[a-zA-Z0-9_-]+)/keys/(.+)$")
_KEYS_RE = re.compile(r"^/key-value-stores/(arn:.+?/[a-zA-Z0-9_-]+)/keys/?$")
_STORE_RE = re.compile(r"^/key-value-stores/(arn:.+)$")

# ---------------------------------------------------------------------------
# In-memory state — keyed by KVS ARN
# ---------------------------------------------------------------------------
_stores = AccountScopedDict()  # arn -> {"etag": str, "items": {key: value}}


def reset():
    _stores.clear()


def get_state():
    return copy.deepcopy({"stores": _stores})


def restore_state(data):
    if not data:
        return
    _stores.clear()
    for k, v in (data.get("stores") or {}).items():
        _stores[k] = v


try:
    _restored = load_state("cloudfront_keyvaluestore")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception("Failed to restore persisted state; continuing with fresh store")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({"Message": message, "__type": code}).encode()
    return status, {"Content-Type": "application/json"}, body


def _get_store(arn: str):
    store = _stores.get(arn)
    if store is None:
        from ministack.services.cloudfront import _kvstores

        kvs = None
        for v in _kvstores.values():
            if v["ARN"] == arn:
                kvs = v
                break
        if kvs is None:
            return None
        store = {"etag": new_uuid(), "items": {}}
        _stores[arn] = store
    return store


def _compute_size(items: dict) -> int:
    total = 0
    for k, v in items.items():
        total += len(k.encode("utf-8")) + len(v.encode("utf-8"))
    return total


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    m = _KEY_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        key = unquote(m.group(2))
        if method == "GET":
            return _get_key(arn, key)
        if method == "PUT":
            return _put_key(arn, key, headers, body)
        if method == "DELETE":
            return _delete_key(arn, key, headers)

    m = _KEYS_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "GET":
            return _list_keys(arn, query_params)
        if method == "POST":
            return _update_keys(arn, headers, body)

    m = _STORE_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "GET":
            return _describe_store(arn)

    return _error("InvalidRequestException", f"No route for {method} {path}", 400)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _describe_store(arn: str):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    from ministack.services.cloudfront import _kvstores

    kvs_meta = None
    for v in _kvstores.values():
        if v["ARN"] == arn:
            kvs_meta = v
            break

    items = store["items"]
    epoch = 0
    if kvs_meta and kvs_meta.get("LastModifiedTime"):
        try:
            epoch = int(datetime.fromisoformat(kvs_meta["LastModifiedTime"].replace("Z", "+00:00")).timestamp())
        except (ValueError, AttributeError):
            epoch = 0
    resp = {
        "KvsARN": arn,
        "ItemCount": len(items),
        "TotalSizeInBytes": _compute_size(items),
        "Status": "READY",
        "Created": epoch,
        "LastModified": epoch,
    }
    return 200, {"Content-Type": "application/json", "ETag": store["etag"]}, json.dumps(resp).encode()


_LIST_KEYS_MAX = 50  # AWS spec cap.


def _qp_first(query_params, key):
    v = query_params.get(key)
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _list_keys(arn: str, query_params):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    raw_max = _qp_first(query_params, "MaxResults")
    try:
        max_results = int(raw_max) if raw_max not in (None, "") else 10
    except (TypeError, ValueError):
        max_results = 10
    max_results = max(1, min(max_results, _LIST_KEYS_MAX))  # AWS spec: cap at 50.

    next_token = _qp_first(query_params, "NextToken") or None
    all_keys = sorted(store["items"].keys())
    start_idx = 0
    if next_token:
        try:
            cursor = base64.urlsafe_b64decode(next_token + "=" * (-len(next_token) % 4)).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return _error("ValidationException", "NextToken is not valid.", 400)
        # Resume after the cursor key (exclusive) so successive pages don't
        # re-emit the boundary key.
        for i, k in enumerate(all_keys):
            if k > cursor:
                start_idx = i
                break
        else:
            start_idx = len(all_keys)

    page = all_keys[start_idx : start_idx + max_results]
    items = [{"Key": k, "Value": store["items"][k]} for k in page]

    resp = {"Items": items}
    if start_idx + max_results < len(all_keys):
        last = page[-1]
        resp["NextToken"] = base64.urlsafe_b64encode(last.encode("utf-8")).decode("ascii").rstrip("=")

    return json_response(resp)


def _get_key(arn: str, key: str):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    value = store["items"].get(key)
    if value is None:
        return _error("ResourceNotFoundException", f"Key {key} was not found.", 404)

    resp = {
        "Key": key,
        "Value": value,
        "ItemCount": len(store["items"]),
        "TotalSizeInBytes": _compute_size(store["items"]),
    }
    return json_response(resp)


def _put_key(arn: str, key: str, headers, body):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("ValidationException", "If-Match header is required.", 400)
    if if_match != store["etag"]:
        return _error("ConflictException", "The provided If-Match value does not match the current ETag.", 409)

    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, TypeError):
        return _error("ValidationException", "Invalid JSON body.", 400)

    value = data.get("Value")
    if value is None:
        return _error("ValidationException", "Value is required.", 400)

    store["items"][key] = value
    store["etag"] = new_uuid()

    resp = {
        "ItemCount": len(store["items"]),
        "TotalSizeInBytes": _compute_size(store["items"]),
    }
    return 200, {"Content-Type": "application/json", "ETag": store["etag"]}, json.dumps(resp).encode()


def _delete_key(arn: str, key: str, headers):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("ValidationException", "If-Match header is required.", 400)
    if if_match != store["etag"]:
        return _error("ConflictException", "The provided If-Match value does not match the current ETag.", 409)

    # AWS spec: deleting a non-existent key returns ResourceNotFoundException, not 200.
    if key not in store["items"]:
        return _error("ResourceNotFoundException", f"Key {key} was not found.", 404)

    del store["items"][key]
    store["etag"] = new_uuid()

    resp = {
        "ItemCount": len(store["items"]),
        "TotalSizeInBytes": _compute_size(store["items"]),
    }
    return 200, {"Content-Type": "application/json", "ETag": store["etag"]}, json.dumps(resp).encode()


def _update_keys(arn: str, headers, body):
    store = _get_store(arn)
    if store is None:
        return _error("ResourceNotFoundException", f"Key value store {arn} was not found.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("ValidationException", "If-Match header is required.", 400)
    if if_match != store["etag"]:
        return _error("ConflictException", "The provided If-Match value does not match the current ETag.", 409)

    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, TypeError):
        return _error("ValidationException", "Invalid JSON body.", 400)

    puts = data.get("Puts", []) or []
    deletes = data.get("Deletes", []) or []
    if not isinstance(puts, list) or not isinstance(deletes, list):
        return _error("ValidationException", "Puts and Deletes must be arrays.", 400)

    # AWS UpdateKeys is atomic: validate every entry first, then commit. A
    # single bad item rejects the whole batch so the store never sees a
    # partial write.
    validated_puts: list[tuple[str, str]] = []
    for i, item in enumerate(puts):
        if not isinstance(item, dict):
            return _error("ValidationException", f"Puts[{i}] must be an object.", 400)
        k = item.get("Key")
        v = item.get("Value")
        if not isinstance(k, str) or not k:
            return _error("ValidationException", f"Puts[{i}].Key must be a non-empty string.", 400)
        if not isinstance(v, str):
            return _error("ValidationException", f"Puts[{i}].Value must be a string.", 400)
        validated_puts.append((k, v))
    validated_deletes: list[str] = []
    for i, item in enumerate(deletes):
        if not isinstance(item, dict):
            return _error("ValidationException", f"Deletes[{i}] must be an object.", 400)
        k = item.get("Key")
        if not isinstance(k, str) or not k:
            return _error("ValidationException", f"Deletes[{i}].Key must be a non-empty string.", 400)
        validated_deletes.append(k)

    for k, v in validated_puts:
        store["items"][k] = v
    for k in validated_deletes:
        store["items"].pop(k, None)
    store["etag"] = new_uuid()

    resp = {
        "ItemCount": len(store["items"]),
        "TotalSizeInBytes": _compute_size(store["items"]),
    }
    return 200, {"Content-Type": "application/json", "ETag": store["etag"]}, json.dumps(resp).encode()
