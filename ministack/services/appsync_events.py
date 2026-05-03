"""
AWS AppSync Events emulator.

Event API management lives under /v2/apis and shares the ``appsync``
credential scope, so ``services/appsync.py`` delegates here. API key
operations land on /v1/apis/{apiId}/apikeys (same delegation path).
Data plane: HTTP publish at POST /event on {apiId}.appsync-api.* and a
realtime WebSocket on {apiId}.appsync-realtime-api.* using the
``aws-appsync-event-ws`` subprotocol.

Channel paths follow the 1..5-segment grammar; subscribe patterns accept a
trailing ``*`` single-level wildcard. Connection-scoped authorization is
declared via a second ``header-<base64url(json)>`` Sec-WebSocket-Protocol
entry. Set ``APPSYNC_EVENTS_ENFORCE_AUTH=1`` for strict mode (registered
x-api-key required, or AWS_LAMBDA authorizer Lambda invoked when configured
in the API ``eventConfig``).
"""

import asyncio
import base64
import binascii
import copy
import json
import logging
import os
import re
import time

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("appsync_events")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

# apiId -> api record
_apis = AccountScopedDict()
# apiId -> {name -> channel namespace record}
_channel_namespaces = AccountScopedDict()
# apiId -> {keyId -> api key record}
_api_keys = AccountScopedDict()

# Active realtime connections, keyed by opaque connection id assigned on accept.
# {connection_id -> {"api_id": str, "account_id": str, "outbox": asyncio.Queue,
#                     "subscriptions": {sub_id -> pattern}}}
_connections: dict[str, dict] = {}
_connections_lock: asyncio.Lock | None = None


def _get_connections_lock() -> asyncio.Lock:
    global _connections_lock
    if _connections_lock is None:
        _connections_lock = asyncio.Lock()
    return _connections_lock


# ---------------------------------------------------------------------------
# Persistence / reset
# ---------------------------------------------------------------------------

def get_state():
    # Only persist control-plane records; connections and live subscriptions
    # are in-process only and cannot survive a restart.
    return {
        "apis": copy.deepcopy(_apis),
        "channel_namespaces": copy.deepcopy(_channel_namespaces),
        "api_keys": copy.deepcopy(_api_keys),
    }


def restore_state(data):
    if not data:
        return
    _apis.update(data.get("apis", {}))
    _channel_namespaces.update(data.get("channel_namespaces", {}))
    _api_keys.update(data.get("api_keys", {}))


# Same contract as apigateway.py (used by app.py persistence loader)
load_persisted_state = restore_state


try:
    _restored = load_state("appsync_events")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore AppSync Events state")


def reset():
    _apis.clear()
    _channel_namespaces.clear()
    _api_keys.clear()
    _connections.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_APPSYNC_API_HOST_RE = re.compile(r"^([a-z0-9]+)\.appsync-api\.")


def _now() -> int:
    return int(time.time())


def _api_arn(api_id: str) -> str:
    return f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}"


def _new_api_id() -> str:
    return new_uuid().replace("-", "")[:26]


def _default_dns_for_api(api_id: str) -> dict[str, str]:
    """Return Event API ``dns`` block (HTTP + REALTIME).

    1) If both ``APPSYNC_EVENTS_HTTP_HOST_TEMPLATE`` and
    ``APPSYNC_EVENTS_REALTIME_HOST_TEMPLATE`` are set, they are
    :meth:`str.format`\\ -ed with ``api_id``, ``region``, ``port``, ``GATEWAY_PORT``.
    2) Otherwise default to AWS-shaped localhost vhosts (see module docstring).
    """
    region = get_region()
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    mini_host = os.environ.get("MINISTACK_HOST", "localhost")
    host_t = os.environ.get("APPSYNC_EVENTS_HTTP_HOST_TEMPLATE", "").strip()
    rt_t = os.environ.get("APPSYNC_EVENTS_REALTIME_HOST_TEMPLATE", "").strip()
    if host_t and rt_t:
        return {
            "HTTP": host_t.format(
                api_id=api_id, region=region, port=port, GATEWAY_PORT=port
            ),
            "REALTIME": rt_t.format(
                api_id=api_id, region=region, port=port, GATEWAY_PORT=port
            ),
        }
    return {
        "HTTP": f"{api_id}.appsync-api.{region}.{mini_host}:{port}",
        "REALTIME": (
            f"{api_id}.appsync-realtime-api.{region}.{mini_host}:{port}"
        ),
    }


def _default_event_config(requested: dict | None) -> dict:
    """Normalise an incoming eventConfig into the shape real AWS returns.

    Every field gets a default so boto3 ``get_api``/``list_apis`` responses
    always contain the full set of keys — matches AWS behaviour and makes
    downstream schema-validating SDKs happy.
    """
    requested = requested or {}
    default_auth = [{"authType": "API_KEY"}]
    out = {
        "authProviders": requested.get("authProviders") or default_auth,
        "connectionAuthModes": requested.get("connectionAuthModes") or default_auth,
        "defaultPublishAuthModes": requested.get("defaultPublishAuthModes") or default_auth,
        "defaultSubscribeAuthModes": requested.get("defaultSubscribeAuthModes") or default_auth,
    }
    if requested.get("logConfig") is not None:
        out["logConfig"] = requested["logConfig"]
    return out


def _api_response(api_id: str) -> dict:
    api = _apis.get(api_id)
    if not api:
        return {}
    return {
        "apiId": api_id,
        "name": api["name"],
        "ownerContact": api.get("ownerContact"),
        "tags": api.get("tags", {}),
        "dns": api["dns"],
        "apiArn": api["apiArn"],
        "created": api["created"],
        "xrayEnabled": api.get("xrayEnabled", False),
        "wafWebAclArn": api.get("wafWebAclArn"),
        "eventConfig": api["eventConfig"],
    }


def _channel_namespace_arn(api_id: str, name: str) -> str:
    return f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}/channelNamespace/{name}"


def _channel_response(api_id: str, name: str) -> dict:
    ns = _channel_namespaces.get(api_id, {}).get(name, {})
    if not ns:
        return {}
    return {
        "apiId": api_id,
        "name": name,
        "subscribeAuthModes": ns.get("subscribeAuthModes", []),
        "publishAuthModes": ns.get("publishAuthModes", []),
        "codeHandlers": ns.get("codeHandlers"),
        "handlerConfigs": ns.get("handlerConfigs"),
        "tags": ns.get("tags", {}),
        "channelNamespaceArn": _channel_namespace_arn(api_id, name),
        "created": ns["created"],
        "lastModified": ns["lastModified"],
    }


def _api_key_response(api_id: str, key_id: str) -> dict:
    k = _api_keys.get(api_id, {}).get(key_id, {})
    if not k:
        return {}
    return {
        "id": key_id,
        "description": k.get("description", ""),
        "expires": k["expires"],
        "deletes": k.get("deletes", k["expires"]),
    }


def _parse_json_body(body: bytes) -> tuple[dict, object]:
    if not body:
        return {}, None
    try:
        data = json.loads(body)
        if not isinstance(data, dict):
            return {}, error_response_json(
                "BadRequestException", "Request body must be a JSON object", 400
            )
        return data, None
    except json.JSONDecodeError:
        return {}, error_response_json(
            "BadRequestException", "Invalid JSON in request body", 400
        )


def _not_found(msg: str):
    return error_response_json("NotFoundException", msg, 404)


# ---------------------------------------------------------------------------
# Channel validation + matching (subscribe pattern -> publish channel)
# ---------------------------------------------------------------------------

# Per the AWS AppSync Events spec, a channel segment is 1..50 chars made up of
# alphanumerics and dashes, not starting or ending with a dash. A path is 1..5
# segments separated by ``/`` with optional leading/trailing slash.
_CHANNEL_SEGMENT_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,48}[A-Za-z0-9])?$")


def _split_channel(channel: str) -> list[str] | None:
    """Split an AWS AppSync Events channel path into segments.

    Per AWS spec the leading and trailing ``/`` are optional, so ``foo/bar``
    and ``/foo/bar/`` both yield ``["foo", "bar"]``. 1..5 segments required.
    """
    if not channel:
        return None
    parts = [p for p in channel.split("/") if p != ""]
    if not 1 <= len(parts) <= 5:
        return None
    return parts


def _validate_channel(channel: str, *, allow_wildcard: bool) -> str | None:
    """Return an error message when ``channel`` is invalid, else ``None``.

    When ``allow_wildcard`` is true, the terminal segment may be ``*`` — used
    for subscribe patterns. Publish channels must be fully-qualified paths.
    """
    parts = _split_channel(channel)
    if parts is None:
        return (
            "channel must start with '/' and have 1..5 segments "
            "(alphanumeric + dash, 1..50 chars each)"
        )
    last_idx = len(parts) - 1
    for i, seg in enumerate(parts):
        if seg == "*":
            if not allow_wildcard:
                return "wildcard '*' not allowed in publish channel"
            if i != last_idx:
                return "wildcard '*' is only allowed as the terminal segment"
            continue
        if not _CHANNEL_SEGMENT_RE.match(seg):
            return f"invalid channel segment: '{seg}'"
    return None


def _channel_namespace_for(channel: str) -> str | None:
    """Return the namespace name embedded in ``channel`` (``/ns/...``)."""
    parts = _split_channel(channel)
    return parts[0] if parts else None


def _channel_matches(publish_channel: str, subscribe_pattern: str) -> bool:
    """Match an AppSync Events subscribe pattern against a publish channel.

    Supported (per spec):
      - Exact:        ``/ns/room1`` matches ``/ns/room1``.
      - Single-level: ``/ns/*`` matches any single-segment suffix of ``/ns``
                      (``/ns/room1`` yes, ``/ns/room1/sub`` no).
    """
    pub = _split_channel(publish_channel)
    sub = _split_channel(subscribe_pattern)
    if pub is None or sub is None:
        return False
    if len(pub) != len(sub):
        return False
    for p, s in zip(pub, sub):
        if s == "*":
            continue
        if p != s:
            return False
    return True


# ---------------------------------------------------------------------------
# Authorization — header-<base64url(json)> subprotocol + frame-level fallback
# ---------------------------------------------------------------------------

def _strict_auth_enabled() -> bool:
    return os.environ.get("APPSYNC_EVENTS_ENFORCE_AUTH", "").lower() in ("1", "true", "yes")


def _decode_header_subprotocol(requested: str) -> tuple[dict | None, str | None]:
    """Parse the ``header-<base64url(json)>`` entry from Sec-WebSocket-Protocol.

    Returns ``(auth_dict, None)`` on success, ``(None, None)`` when absent,
    or ``(None, "<error>")`` when the entry exists but is malformed.
    """
    for proto in (p.strip() for p in requested.split(",")):
        if not proto.startswith("header-"):
            continue
        b64 = proto[len("header-"):]
        try:
            padded = b64 + "=" * (-len(b64) % 4)
            raw = base64.urlsafe_b64decode(padded.encode("ascii"))
            obj = json.loads(raw.decode("utf-8"))
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as e:
            return None, f"Invalid header subprotocol: {e}"
        if not isinstance(obj, dict):
            return None, "Header subprotocol must decode to a JSON object"
        return obj, None
    return None, None


def _auth_has_known_api_key(api_id: str, auth: dict | None) -> bool:
    """Return True when ``auth`` carries an ``x-api-key`` registered on the API.

    MiniStack doesn't verify JWTs or SigV4 — in strict mode we only
    enforce structural presence plus registered API keys.
    """
    if not isinstance(auth, dict):
        return False
    provided = auth.get("x-api-key") or auth.get("X-Api-Key")
    if not provided:
        return False
    return provided in _api_keys.get(api_id, {})


def _find_lambda_authorizer_arn(api_id: str) -> str | None:
    ev = (_apis.get(api_id) or {}).get("eventConfig") or {}
    for p in ev.get("authProviders") or ev.get("auth_providers") or []:
        a_type = p.get("authType") or p.get("auth_type")
        if a_type != "AWS_LAMBDA":
            continue
        cfg = p.get("lambdaAuthorizerConfig") or p.get("lambda_authorizer_config") or {}
        if not isinstance(cfg, dict):
            continue
        uri = cfg.get("authorizerUri") or cfg.get("authorizer_uri")
        if isinstance(uri, str) and uri.strip():
            return uri.strip()
    return None


def _bearer_from_auth_dict(auth: dict | None) -> str:
    """Value for the AppSync Events Lambda authorizer ``AuthorizationToken`` field."""
    if not auth or not isinstance(auth, dict):
        return ""
    for key in (
        "Authorization", "authorization",
        "AuthorizationToken", "authorizationToken",
    ):
        v = auth.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _parse_lambda_authorizer_body(body: object) -> tuple[bool, dict | None]:
    """Parse the AppSync Events Lambda authorizer response.

    Per AWS spec the response carries ``isAuthorized`` (required boolean) and
    optionally ``handlerContext`` (key/value strings, exposed to handlers as
    ``$ctx.identity.handlerContext``). ``ttlOverride`` is parsed/ignored —
    ministack doesn't cache authorizer decisions.
    """
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            return False, None
    if not isinstance(body, dict):
        return False, None
    if not bool(body.get("isAuthorized")):
        return False, None
    ctx = body.get("handlerContext")
    return True, ctx if isinstance(ctx, dict) else None


def _events_authorizer_invoke(
    arn: str,
    api_id: str,
    operation: str,
    channel: str | None,
    namespace: str | None,
    authorization_token: str,
    request_headers: dict,
) -> tuple[bool, dict | None]:
    """Invoke the configured authorizer Lambda with the AWS-spec event payload.

    Real AWS sends ``{authorizationToken, requestContext{apiId,accountId,
    requestId,operation,channelNamespaceName,channel}, requestHeaders}`` with
    operations ``EVENT_CONNECT`` / ``EVENT_SUBSCRIBE`` / ``EVENT_PUBLISH``;
    for ``EVENT_CONNECT`` the channelNamespaceName and channel are null.
    """
    if not authorization_token.strip() or not arn:
        return False, None
    payload: dict = {
        "authorizationToken": authorization_token,
        "requestContext": {
            "apiId": api_id,
            "accountId": get_account_id(),
            "requestId": new_uuid(),
            "operation": operation,
            "channelNamespaceName": namespace,
            "channel": channel,
        },
        "requestHeaders": request_headers or {},
    }
    try:
        from ministack.services import lambda_svc
        name, q = lambda_svc._resolve_name_and_qualifier(arn)  # noqa: SLF001
        fn, _cfg = lambda_svc._get_func_record_for_qualifier(name, q)  # noqa: SLF001
    except Exception as e:
        logger.error("AppSync Events authorizer resolve %s: %s", arn, e)
        return False, None
    if not fn:
        logger.error("AppSync Events authorizer Lambda not registered: %s", arn)
        return False, None
    try:
        res = lambda_svc._execute_function(fn, payload)  # noqa: SLF001
    except Exception as e:
        logger.error("AppSync Events authorizer invoke: %s", e)
        return False, None
    if not isinstance(res, dict) or res.get("error"):
        return False, None
    return _parse_lambda_authorizer_body(res.get("body"))


# AWS operation enum — sent verbatim in the Lambda authorizer ``operation`` field.
EVENT_CONNECT = "EVENT_CONNECT"
EVENT_SUBSCRIBE = "EVENT_SUBSCRIBE"
EVENT_PUBLISH = "EVENT_PUBLISH"

# AWS limit: at most 5 events per publish (HTTP body or WebSocket frame).
_MAX_EVENTS_PER_BATCH = 5


async def _authorize_event_op(
    api_id: str,
    channel: str | None,
    operation: str,
    auth: dict | None,
    request_headers: dict | None = None,
) -> tuple[bool, str | None]:
    """Authorize an AppSync Events subscribe/publish/connect operation."""
    lambda_arn = _find_lambda_authorizer_arn(api_id)
    if lambda_arn:
        token = _bearer_from_auth_dict(auth)
        if not token:
            return False, f"{operation} rejected: no Authorization token"
        namespace = _channel_namespace_for(channel) if channel else None
        ok, _ctx = await asyncio.to_thread(
            _events_authorizer_invoke,
            lambda_arn,
            api_id,
            operation,
            channel,
            namespace,
            token,
            request_headers or {},
        )
        if not ok:
            return False, f"{operation} rejected: AppSync authorizer denied"
        return True, None

    if _strict_auth_enabled() and not _auth_has_known_api_key(api_id, auth):
        return False, f"{operation} rejected: no valid x-api-key"

    return True, None


def _auth_from_headers(headers: dict) -> dict:
    auth: dict = {}
    if key := headers.get("x-api-key"):
        auth["x-api-key"] = key
    if value := headers.get("authorization"):
        auth["Authorization"] = value
    return auth


# ---------------------------------------------------------------------------
# Entry point — dispatches management + data plane HTTP requests
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    # HTTP publish lives on a separate host:
    # {apiId}.appsync-api.{region}.{host}, with a fixed "/event" path.
    # Dispatch it before the management router.
    host = headers.get("host", "")
    host_match = _APPSYNC_API_HOST_RE.match(host)
    if host_match and path == "/event" and method == "POST":
        return await _publish(host_match.group(1), headers, body)

    if path.startswith("/v2/apis"):
        return await _handle_mgmt(method, path, headers, body, query_params)

    return error_response_json("BadRequestException", f"Unsupported AppSync Events path: {path}", 400)


# ---------------------------------------------------------------------------
# Management plane (/v2/apis...)
# ---------------------------------------------------------------------------

_PATH_API_ROOT = re.compile(r"^/v2/apis/?$")
_PATH_API_ID = re.compile(r"^/v2/apis/([^/]+)/?$")
_PATH_NS_ROOT = re.compile(r"^/v2/apis/([^/]+)/channelNamespaces/?$")
_PATH_NS_ITEM = re.compile(r"^/v2/apis/([^/]+)/channelNamespaces/([^/]+)/?$")


async def _handle_mgmt(method, path, headers, body, query_params):
    if _PATH_API_ROOT.match(path):
        if method == "POST":
            return _create_api(body)
        if method == "GET":
            return _list_apis(query_params)
        return error_response_json("BadRequestException", f"Method {method} not allowed on {path}", 405)

    if (m := _PATH_API_ID.match(path)):
        api_id = m.group(1)
        if method == "GET":
            return _get_api(api_id)
        # boto3's UpdateApi uses POST; accept PATCH/PUT for raw-HTTP callers too.
        if method in ("POST", "PATCH", "PUT"):
            return _update_api(api_id, body)
        if method == "DELETE":
            return _delete_api(api_id)
        return error_response_json("BadRequestException", f"Method {method} not allowed on {path}", 405)

    if (m := _PATH_NS_ROOT.match(path)):
        api_id = m.group(1)
        if method == "POST":
            return _create_channel_namespace(api_id, body)
        if method == "GET":
            return _list_channel_namespaces(api_id, query_params)
        return error_response_json("BadRequestException", f"Method {method} not allowed on {path}", 405)

    if (m := _PATH_NS_ITEM.match(path)):
        api_id, name = m.group(1), m.group(2)
        if method == "GET":
            return _get_channel_namespace(api_id, name)
        if method in ("POST", "PATCH", "PUT"):
            return _update_channel_namespace(api_id, name, body)
        if method == "DELETE":
            return _delete_channel_namespace(api_id, name)
        return error_response_json("BadRequestException", f"Method {method} not allowed on {path}", 405)

    return error_response_json("NotFoundException", f"Path not found: {path}", 404)


# --- API CRUD --------------------------------------------------------------

def _create_api(body):
    data, err = _parse_json_body(body)
    if err:
        return err
    name = data.get("name")
    if not name:
        return error_response_json("BadRequestException", "'name' is required", 400)

    api_id = _new_api_id()
    record = {
        "name": name,
        "ownerContact": data.get("ownerContact"),
        "tags": data.get("tags", {}),
        "dns": _default_dns_for_api(api_id),
        "apiArn": _api_arn(api_id),
        "created": _now(),
        "xrayEnabled": bool(data.get("xrayEnabled", False)),
        "wafWebAclArn": None,
        "eventConfig": _default_event_config(data.get("eventConfig")),
    }
    _apis[api_id] = record
    _channel_namespaces.setdefault(api_id, {})
    _api_keys.setdefault(api_id, {})
    return json_response({"api": _api_response(api_id)})


def _get_api(api_id):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    return json_response({"api": _api_response(api_id)})


def _qp_str(query_params: dict, key: str) -> str | None:
    v = query_params.get(key)
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _qp_int(query_params: dict, key: str, default: int) -> int:
    v = _qp_str(query_params, key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _paginate(items: list, query_params: dict) -> tuple[list, str | None]:
    """Apply AppSync ``maxResults`` + ``nextToken`` pagination over a sorted list.

    Token is the index of the first item to return — opaque to clients but
    survives a restart since item order is stable (lexical on identifier).
    """
    max_results = max(1, min(_qp_int(query_params, "maxResults", 100), 1000))
    next_token = _qp_str(query_params, "nextToken")
    start = 0
    if next_token:
        try:
            start = max(0, int(next_token))
        except ValueError:
            start = 0
    page = items[start : start + max_results]
    new_token = str(start + max_results) if start + max_results < len(items) else None
    return page, new_token


def _list_apis(query_params):
    items = [_api_response(aid) for aid in sorted(_apis.keys())]
    page, token = _paginate(items, query_params)
    resp: dict = {"apis": page}
    if token is not None:
        resp["nextToken"] = token
    return json_response(resp)


def _update_api(api_id, body):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    data, err = _parse_json_body(body)
    if err:
        return err
    api = _apis[api_id]
    if "name" in data:
        api["name"] = data["name"]
    if "ownerContact" in data:
        api["ownerContact"] = data["ownerContact"]
    if "xrayEnabled" in data:
        api["xrayEnabled"] = bool(data["xrayEnabled"])
    if "eventConfig" in data:
        api["eventConfig"] = _default_event_config(data["eventConfig"])
    return json_response({"api": _api_response(api_id)})


def _delete_api(api_id):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    _apis.pop(api_id, None)
    _channel_namespaces.pop(api_id, None)
    _api_keys.pop(api_id, None)
    return json_response({}, status=200)


# --- Channel namespaces ----------------------------------------------------

def _create_channel_namespace(api_id, body):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    data, err = _parse_json_body(body)
    if err:
        return err
    name = data.get("name")
    if not name:
        return error_response_json("BadRequestException", "'name' is required", 400)

    namespaces = _channel_namespaces.setdefault(api_id, {})
    if name in namespaces:
        return error_response_json(
            "ConflictException",
            f"Channel namespace '{name}' already exists",
            409,
        )

    now = _now()
    default_modes = _apis[api_id]["eventConfig"]["defaultPublishAuthModes"]
    namespaces[name] = {
        "publishAuthModes": data.get("publishAuthModes") or default_modes,
        "subscribeAuthModes": data.get("subscribeAuthModes") or default_modes,
        "codeHandlers": data.get("codeHandlers"),
        "handlerConfigs": data.get("handlerConfigs"),
        "tags": data.get("tags", {}),
        "created": now,
        "lastModified": now,
    }
    return json_response({"channelNamespace": _channel_response(api_id, name)})


def _get_channel_namespace(api_id, name):
    if name not in _channel_namespaces.get(api_id, {}):
        return _not_found(f"Channel namespace '{name}' not found")
    return json_response({"channelNamespace": _channel_response(api_id, name)})


def _list_channel_namespaces(api_id, query_params):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    namespaces = _channel_namespaces.get(api_id, {})
    items = [_channel_response(api_id, n) for n in sorted(namespaces.keys())]
    page, token = _paginate(items, query_params)
    resp: dict = {"channelNamespaces": page}
    if token is not None:
        resp["nextToken"] = token
    return json_response(resp)


def _update_channel_namespace(api_id, name, body):
    if name not in _channel_namespaces.get(api_id, {}):
        return _not_found(f"Channel namespace '{name}' not found")
    data, err = _parse_json_body(body)
    if err:
        return err
    ns = _channel_namespaces[api_id][name]
    for field in ("publishAuthModes", "subscribeAuthModes", "codeHandlers", "handlerConfigs"):
        if field in data:
            ns[field] = data[field]
    ns["lastModified"] = _now()
    return json_response({"channelNamespace": _channel_response(api_id, name)})


def _delete_channel_namespace(api_id, name):
    if name not in _channel_namespaces.get(api_id, {}):
        return _not_found(f"Channel namespace '{name}' not found")
    del _channel_namespaces[api_id][name]
    return json_response({}, status=200)


# --- API keys --------------------------------------------------------------

def _create_api_key(api_id, body):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    data, err = _parse_json_body(body)
    if err:
        return err

    key_id = "da2-" + new_uuid().replace("-", "")[:26]
    # Real AppSync defaults to 7d and caps at 365d; we don't enforce the cap.
    expires = int(data.get("expires") or (_now() + 7 * 24 * 3600))
    _api_keys.setdefault(api_id, {})[key_id] = {
        "description": data.get("description", ""),
        "expires": expires,
        "deletes": expires,
    }
    return json_response({"apiKey": _api_key_response(api_id, key_id)})


def _list_api_keys(api_id, query_params: dict | None = None):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")
    keys = _api_keys.get(api_id, {})
    items = [_api_key_response(api_id, kid) for kid in sorted(keys.keys())]
    page, token = _paginate(items, query_params or {})
    resp: dict = {"apiKeys": page}
    if token is not None:
        resp["nextToken"] = token
    return json_response(resp)


def _delete_api_key(api_id, key_id):
    if key_id not in _api_keys.get(api_id, {}):
        return _not_found(f"ApiKey {key_id} not found")
    del _api_keys[api_id][key_id]
    return json_response({}, status=200)


# ---------------------------------------------------------------------------
# Data plane — HTTP publish
# ---------------------------------------------------------------------------

async def _publish(api_id: str, headers: dict, body: bytes):
    if api_id not in _apis:
        return _not_found(f"Api {api_id} not found")

    data, err = _parse_json_body(body)
    if err:
        return err

    channel = data.get("channel")
    events = data.get("events")
    if not channel or not isinstance(events, list):
        return error_response_json(
            "BadRequestException",
            "Request body must include 'channel' and 'events' (array)",
            400,
        )
    # AWS spec: max 5 events per publish.
    if len(events) > _MAX_EVENTS_PER_BATCH:
        return error_response_json(
            "BadRequestException",
            f"events array may contain at most {_MAX_EVENTS_PER_BATCH} entries",
            400,
        )

    channel_err = _validate_channel(channel, allow_wildcard=False)
    if channel_err:
        return error_response_json("BadRequestException", channel_err, 400)

    ns_name = _channel_namespace_for(channel)
    if ns_name is None or ns_name not in _channel_namespaces.get(api_id, {}):
        return error_response_json(
            "UnauthorizedException",
            f"No channel namespace matches '{channel}'",
            401,
        )

    ok, message = await _authorize_event_op(
        api_id, str(channel), EVENT_PUBLISH, _auth_from_headers(headers), headers
    )
    if not ok:
        return error_response_json("UnauthorizedException", message or "Unauthorized", 401)

    successful, failed = await _fanout_publish(api_id, channel, events)
    return json_response({"successful": successful, "failed": failed})


async def _fanout_publish(api_id: str, channel: str, events: list) -> tuple[list, list]:
    """Deliver each event to every matching subscriber on ``api_id``.

    Shared between the HTTP ``POST /event`` handler and the WebSocket ``publish``
    frame handler so both paths return the same ``(successful, failed)`` shape.
    Each outbound frame uses ``"event": [<json-string>]`` per the spec.
    """
    successful: list = []
    failed: list = []
    async with _get_connections_lock():
        matching_conns = [
            conn for conn in _connections.values()
            if conn["api_id"] == api_id
        ]

    for idx, ev in enumerate(events):
        identifier = new_uuid()
        try:
            if not isinstance(ev, str):
                raise ValueError("event must be a JSON-encoded string")
            for conn in matching_conns:
                for sub_id, pattern in list(conn["subscriptions"].items()):
                    if _channel_matches(channel, pattern):
                        await conn["outbox"].put({
                            "type": "data",
                            "id": sub_id,
                            "event": [ev],
                        })
            successful.append({"identifier": identifier, "index": idx})
        except Exception as e:
            failed.append({
                "identifier": identifier,
                "index": idx,
                "code": 400,
                "errorMessage": str(e),
            })
    return successful, failed


# ---------------------------------------------------------------------------
# Data plane — WebSocket subscribe
# ---------------------------------------------------------------------------

_KA_INTERVAL_DEFAULT = 60.0  # AppSync Events spec: one "ka" every 60s.
_CONN_TIMEOUT_MS = 300000    # connection_ack connectionTimeoutMs per spec.


def _ka_interval_secs() -> float:
    """Keep-alive cadence; overridable for tests via ``APPSYNC_EVENTS_KA_INTERVAL_SECS``."""
    try:
        return float(os.environ.get("APPSYNC_EVENTS_KA_INTERVAL_SECS", _KA_INTERVAL_DEFAULT))
    except ValueError:
        return _KA_INTERVAL_DEFAULT


async def handle_websocket(scope, receive, send, api_id: str):
    """Drive an AppSync Events realtime WebSocket session.

    Wire protocol (``aws-appsync-event-ws``):
      - Handshake: ``Sec-WebSocket-Protocol: header-<b64>, aws-appsync-event-ws``
        The ``header-<b64>`` entry is base64url(JSON) describing connection-scoped
        auth (``x-api-key``, Cognito/OIDC ``Authorization``, Lambda token, or
        SigV4 headers). MiniStack stores the decoded object on the connection and
        uses it as the fallback when ``subscribe``/``publish`` frames omit their
        own ``authorization`` block.
      - ``{"type":"connection_init"}`` -> ``{"type":"connection_ack","connectionTimeoutMs":300000}``.
      - ``{"type":"subscribe","id":..,"channel":..,"authorization":{...}}`` ->
        ``subscribe_success`` / ``subscribe_error``.
      - ``{"type":"publish","id":..,"channel":..,"events":[..],"authorization":{...}}``
        -> ``publish_success`` (fans events out to matching subscribers) / ``publish_error``.
      - ``{"type":"unsubscribe","id":..}`` -> ``unsubscribe_success``.
      - Server pushes ``{"type":"data","id":<sub-id>,"event":[<json-string>]}`` on
        each matching publish and emits ``{"type":"ka"}`` every ~60s.
    """
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return

    if api_id not in _apis:
        await send({"type": "websocket.close", "code": 1008})
        return

    sub_headers = {}
    for name, value in scope.get("headers", []):
        try:
            sub_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            sub_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
    requested = sub_headers.get("sec-websocket-protocol", "")
    chosen = None
    for proto in [p.strip() for p in requested.split(",") if p.strip()]:
        if proto.startswith("aws-appsync-event-ws"):
            chosen = proto
            break

    conn_auth, auth_error = _decode_header_subprotocol(requested)
    strict = _strict_auth_enabled()
    lambda_auth_arn = _find_lambda_authorizer_arn(api_id)

    accept_msg: dict = {"type": "websocket.accept"}
    if chosen:
        accept_msg["subprotocol"] = chosen
    await send(accept_msg)

    async def _deny_websocket(msg: str, *, code: int = 4401) -> bool:
        try:
            await send({
                "type": "websocket.send",
                "text": json.dumps({
                    "type": "connection_error",
                    "errors": [{
                        "errorType": "UnauthorizedException",
                        "message": msg,
                    }],
                }),
            })
        except Exception:
            pass
        try:
            await send({"type": "websocket.close", "code": code})
        except Exception:
            pass
        return True

    # Malformed header payload, strict API-key, or AppSync-Events authorizer.
    connect_rctx: dict | None = None
    if auth_error is not None:
        if await _deny_websocket(
            auth_error or "Invalid header-<b64> subprotocol", code=4400
        ):
            return
    elif lambda_auth_arn:
        if not (tok := _bearer_from_auth_dict(conn_auth)):
            if await _deny_websocket(
                "Unauthorized: Authorization token required in header- subprotocol",
            ):
                return
        else:
            ok, connect_rctx = await asyncio.to_thread(
                _events_authorizer_invoke,
                lambda_auth_arn,
                api_id,
                EVENT_CONNECT,
                None,
                None,
                tok,
                conn_auth or {},
            )
            if not ok and await _deny_websocket(
                "connect rejected: AppSync authorizer returned deny or error",
            ):
                return
    elif strict and (conn_auth is None or not _auth_has_known_api_key(api_id, conn_auth)):
        if await _deny_websocket(
            "Unauthorized: missing or invalid API key in header-<b64> subprotocol",
        ):
            return

    from ministack.core.responses import _request_account_id
    account_id = get_account_id()
    token = _request_account_id.set(account_id)

    connection_id = new_uuid()
    outbox: asyncio.Queue = asyncio.Queue()
    async with _get_connections_lock():
        _connections[connection_id] = {
            "api_id": api_id,
            "account_id": account_id,
            "outbox": outbox,
            "subscriptions": {},
            "auth": conn_auth,
            "resolverContext": connect_rctx,
        }

    close_event = asyncio.Event()

    async def _drain_outbox():
        """Forward server-originated frames (acks, data pushes, ka) to the socket."""
        while not close_event.is_set():
            try:
                item = await asyncio.wait_for(outbox.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if item is None:
                return
            try:
                await send({"type": "websocket.send", "text": json.dumps(item)})
            except Exception:
                return

    async def _heartbeat():
        """Push ``{"type":"ka"}`` every interval until the connection closes."""
        interval = _ka_interval_secs()
        while not close_event.is_set():
            try:
                await asyncio.wait_for(close_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            else:
                return
            await outbox.put({"type": "ka"})

    drain_task = asyncio.create_task(_drain_outbox())
    ka_task = asyncio.create_task(_heartbeat())

    try:
        while True:
            incoming = await receive()
            mtype = incoming.get("type")
            if mtype == "websocket.disconnect":
                break
            if mtype != "websocket.receive":
                continue
            payload = incoming.get("text")
            if payload is None and incoming.get("bytes") is not None:
                payload = incoming["bytes"].decode("utf-8", errors="replace")
            if not payload:
                continue
            try:
                frame = json.loads(payload)
            except json.JSONDecodeError:
                await outbox.put({"type": "error", "errors": [{"errorType": "BadRequestException", "message": "invalid json"}]})
                continue
            await _handle_client_frame(api_id, connection_id, frame, outbox)
    except Exception:
        logger.exception("AppSync Events WebSocket error")
    finally:
        close_event.set()
        drain_task.cancel()
        ka_task.cancel()
        async with _get_connections_lock():
            _connections.pop(connection_id, None)
        _request_account_id.reset(token)
        try:
            await send({"type": "websocket.close", "code": 1000})
        except Exception:
            pass


def _effective_auth(frame: dict, connection_id: str) -> dict | None:
    """Resolve the auth used for a given frame: per-frame if provided, else connection-scoped."""
    per_frame = frame.get("authorization")
    if isinstance(per_frame, dict):
        return per_frame
    conn = _connections.get(connection_id) or {}
    return conn.get("auth")


async def _handle_client_frame(api_id: str, connection_id: str, frame: dict, outbox: asyncio.Queue):
    ftype = frame.get("type")
    if ftype == "connection_init":
        await outbox.put({"type": "connection_ack", "connectionTimeoutMs": _CONN_TIMEOUT_MS})
        return

    if ftype == "ka":
        # Client-side keep-alive; spec says server acks are not required.
        return

    if ftype == "subscribe":
        sub_id = frame.get("id")
        channel = frame.get("channel")
        if not sub_id or not channel:
            await outbox.put({
                "type": "subscribe_error",
                "id": sub_id,
                "errors": [{"errorType": "BadRequestException",
                            "message": "'id' and 'channel' are required"}],
            })
            return
        channel_err = _validate_channel(channel, allow_wildcard=True)
        if channel_err:
            await outbox.put({
                "type": "subscribe_error",
                "id": sub_id,
                "errors": [{"errorType": "BadRequestException", "message": channel_err}],
            })
            return
        ns_name = _channel_namespace_for(channel)
        if ns_name is None or ns_name not in _channel_namespaces.get(api_id, {}):
            await outbox.put({
                "type": "subscribe_error",
                "id": sub_id,
                "errors": [{"errorType": "UnauthorizedException",
                            "message": f"No channel namespace matches '{channel}'"}],
            })
            return
        ok, message = await _authorize_event_op(
            api_id, str(channel), EVENT_SUBSCRIBE, _effective_auth(frame, connection_id),
        )
        if not ok:
            await outbox.put({
                "type": "subscribe_error",
                "id": sub_id,
                "errors": [{"errorType": "UnauthorizedException",
                            "message": message or "subscribe rejected"}],
            })
            return
        # AWS spec: subscription IDs must be unique per connection. Duplicate → subscribe_error.
        async with _get_connections_lock():
            conn = _connections.get(connection_id)
            if conn is None:
                return
            if sub_id in conn["subscriptions"]:
                await outbox.put({
                    "type": "subscribe_error",
                    "id": sub_id,
                    "errors": [{"errorType": "BadRequestException",
                                "message": f"Subscription id '{sub_id}' is already in use on this connection"}],
                })
                return
            conn["subscriptions"][sub_id] = channel
        await outbox.put({"type": "subscribe_success", "id": sub_id})
        return

    if ftype == "publish":
        pub_id = frame.get("id")
        channel = frame.get("channel")
        events = frame.get("events")
        if not pub_id or not channel or not isinstance(events, list):
            await outbox.put({
                "type": "publish_error",
                "id": pub_id,
                "errors": [{"errorType": "BadRequestException",
                            "message": "'id', 'channel' and 'events' (array) are required"}],
            })
            return
        if len(events) > _MAX_EVENTS_PER_BATCH:
            await outbox.put({
                "type": "publish_error",
                "id": pub_id,
                "errors": [{"errorType": "BadRequestException",
                            "message": f"events array may contain at most {_MAX_EVENTS_PER_BATCH} entries"}],
            })
            return
        channel_err = _validate_channel(channel, allow_wildcard=False)
        if channel_err:
            await outbox.put({
                "type": "publish_error",
                "id": pub_id,
                "errors": [{"errorType": "BadRequestException", "message": channel_err}],
            })
            return
        ns_name = _channel_namespace_for(channel)
        if ns_name is None or ns_name not in _channel_namespaces.get(api_id, {}):
            await outbox.put({
                "type": "publish_error",
                "id": pub_id,
                "errors": [{"errorType": "UnauthorizedException",
                            "message": f"No channel namespace matches '{channel}'"}],
            })
            return
        ok, message = await _authorize_event_op(
            api_id, str(channel), EVENT_PUBLISH, _effective_auth(frame, connection_id),
        )
        if not ok:
            await outbox.put({
                "type": "publish_error",
                "id": pub_id,
                "errors": [{"errorType": "UnauthorizedException",
                            "message": message or "publish rejected"}],
            })
            return
        successful, failed = await _fanout_publish(api_id, channel, events)
        await outbox.put({
            "type": "publish_success",
            "id": pub_id,
            "successful": successful,
            "failed": failed,
        })
        return

    if ftype == "unsubscribe":
        sub_id = frame.get("id")
        removed = False
        async with _get_connections_lock():
            conn = _connections.get(connection_id)
            if conn is not None and sub_id in conn["subscriptions"]:
                conn["subscriptions"].pop(sub_id, None)
                removed = True
        if removed:
            await outbox.put({"type": "unsubscribe_success", "id": sub_id})
        else:
            # AWS spec: unknown sub_id → unsubscribe_error with UnknownOperationError.
            await outbox.put({
                "type": "unsubscribe_error",
                "id": sub_id,
                "errors": [{"errorType": "UnknownOperationError",
                            "message": f"Unknown operation id {sub_id}"}],
            })
        return

    await outbox.put({
        "type": "error",
        "errors": [{"errorType": "BadRequestException",
                    "message": f"unknown frame type: {ftype}"}],
    })
