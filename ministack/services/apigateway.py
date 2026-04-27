"""
API Gateway HTTP API v2 Emulator.

Control plane endpoints implemented:
  POST   /v2/apis                                    — CreateApi
  GET    /v2/apis                                    — GetApis
  GET    /v2/apis/{apiId}                            — GetApi
  PATCH  /v2/apis/{apiId}                            — UpdateApi
  DELETE /v2/apis/{apiId}                            — DeleteApi
  POST   /v2/apis/{apiId}/routes                     — CreateRoute
  GET    /v2/apis/{apiId}/routes                     — GetRoutes
  GET    /v2/apis/{apiId}/routes/{routeId}           — GetRoute
  PATCH  /v2/apis/{apiId}/routes/{routeId}           — UpdateRoute
  DELETE /v2/apis/{apiId}/routes/{routeId}           — DeleteRoute
  POST   /v2/apis/{apiId}/integrations               — CreateIntegration
  GET    /v2/apis/{apiId}/integrations               — GetIntegrations
  GET    /v2/apis/{apiId}/integrations/{integId}     — GetIntegration
  PATCH  /v2/apis/{apiId}/integrations/{integId}     — UpdateIntegration
  DELETE /v2/apis/{apiId}/integrations/{integId}     — DeleteIntegration
  POST   /v2/apis/{apiId}/stages                     — CreateStage
  GET    /v2/apis/{apiId}/stages                     — GetStages
  GET    /v2/apis/{apiId}/stages/{stageName}         — GetStage
  PATCH  /v2/apis/{apiId}/stages/{stageName}         — UpdateStage
  DELETE /v2/apis/{apiId}/stages/{stageName}         — DeleteStage
  POST   /v2/apis/{apiId}/deployments                — CreateDeployment
  GET    /v2/apis/{apiId}/deployments                — GetDeployments
  GET    /v2/apis/{apiId}/deployments/{deployId}     — GetDeployment
  DELETE /v2/apis/{apiId}/deployments/{deployId}     — DeleteDeployment
  GET    /v2/tags/{resourceArn}                      — GetTags
  POST   /v2/tags/{resourceArn}                      — TagResource
  DELETE /v2/tags/{resourceArn}                      — UntagResource
  POST   /v2/apis/{apiId}/authorizers               — CreateAuthorizer
  GET    /v2/apis/{apiId}/authorizers               — GetAuthorizers
  GET    /v2/apis/{apiId}/authorizers/{authId}      — GetAuthorizer
  PATCH  /v2/apis/{apiId}/authorizers/{authId}      — UpdateAuthorizer
  DELETE /v2/apis/{apiId}/authorizers/{authId}      — DeleteAuthorizer

Data plane:
  Requests to /{apiId}.execute-api.localhost/{stage}/{path} are forwarded to
  Lambda (AWS_PROXY) or HTTP backends (HTTP_PROXY) via handle_execute().
"""

import asyncio
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, new_uuid, get_region

_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_PORT = os.environ.get("GATEWAY_PORT", "4566")

logger = logging.getLogger("apigateway")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---- Module-level state ----
_apis = AccountScopedDict()          # api_id -> api object
_routes = AccountScopedDict()        # api_id -> {route_id -> route object}
_integrations = AccountScopedDict()  # api_id -> {integration_id -> integration object}
_stages = AccountScopedDict()        # api_id -> {stage_name -> stage object}
_deployments = AccountScopedDict()   # api_id -> {deployment_id -> deployment object}
_authorizers = AccountScopedDict()   # api_id -> {authorizer_id -> authorizer object}
_api_tags = AccountScopedDict()      # resource_arn -> {key -> value}
_route_responses = AccountScopedDict()         # api_id -> {route_id -> {rr_id -> route_response}}
_integration_responses = AccountScopedDict()   # api_id -> {integration_id -> {ir_id -> int_response}}

# WebSocket connection registry — connections are not per-account-scoped at the store level
# because the @connections management API may arrive on any host/account; instead we store
# the owning account id inside each connection record and check on access.
# { connectionId -> {apiId, accountId, stage, connectedAt, sourceIp, outbox (asyncio.Queue),
#                    close_event (asyncio.Event), lastActiveAt, identity} }
_ws_connections: dict = {}


# ---- Response helpers ----

def _apigw_response(data: dict, status: int = 200) -> tuple:
    """API Gateway v2 uses application/json (not application/x-amz-json-1.0)."""
    return status, {"Content-Type": "application/json"}, json.dumps(data, ensure_ascii=False).encode("utf-8")


def _apigw_error(code: str, message: str, status: int) -> tuple:
    return status, {"Content-Type": "application/json"}, json.dumps({"message": message, "__type": code}, ensure_ascii=False).encode("utf-8")


def _api_arn(api_id: str) -> str:
    return f"arn:aws:apigateway:{get_region()}::/apis/{api_id}"


def _extract_lambda_ref_from_integration_uri(uri: str) -> str:
    """Return the inner Lambda ARN / name from an APIGW integrationUri.

    Terraform and real AWS store Lambda proxy URIs as the wrapper form
    ``arn:aws:apigateway:<region>:lambda:path/2015-03-31/functions/<lambda-arn>/invocations``.
    Splitting the whole wrapper on ``:`` would mis-parse the nested ARN
    (taking the wrapper's ``aws``/``lambda`` segments as function name +
    qualifier, issue #409).

    Also accepts already-unwrapped Lambda ARNs and plain function names so
    fixtures that pass the bare ARN continue to work.
    """
    if not uri:
        return ""
    if "/functions/" in uri:
        inner = uri.split("/functions/", 1)[1]
        if "/invocations" in inner:
            inner = inner.split("/invocations", 1)[0]
        return inner
    if uri.endswith("/invocations"):
        return uri[: -len("/invocations")]
    return uri


# ---- Persistence hooks ----

def get_state() -> dict:
    """Return full module state for persistence.

    Deep-copies each dict so a concurrent write during shutdown
    serialisation can't corrupt the persisted JSON. Every other
    persisted service in this codebase already does the same; the
    apigateway pair was an outlier.
    """
    import copy
    return {
        "apis": copy.deepcopy(_apis),
        "routes": copy.deepcopy(_routes),
        "integrations": copy.deepcopy(_integrations),
        "stages": copy.deepcopy(_stages),
        "deployments": copy.deepcopy(_deployments),
        "authorizers": copy.deepcopy(_authorizers),
        "api_tags": copy.deepcopy(_api_tags),
        "route_responses": copy.deepcopy(_route_responses),
        "integration_responses": copy.deepcopy(_integration_responses),
    }


def load_persisted_state(data: dict) -> None:
    """Restore module state from a previously persisted snapshot."""
    _apis.update(data.get("apis", {}))
    _routes.update(data.get("routes", {}))
    _integrations.update(data.get("integrations", {}))
    _stages.update(data.get("stages", {}))
    _deployments.update(data.get("deployments", {}))
    _authorizers.update(data.get("authorizers", {}))
    _api_tags.update(data.get("api_tags", {}))
    _route_responses.update(data.get("route_responses", {}))
    _integration_responses.update(data.get("integration_responses", {}))


# ---- Control plane router ----

async def handle_request(method, path, headers, body, query_params):
    """Route API Gateway v2 control plane requests."""
    # Dispatch v1 REST API requests first
    parts = [p for p in path.strip("/").split("/") if p]
    if parts and parts[0] in ("restapis", "apikeys", "usageplans", "domainnames", "tags", "account"):
        from ministack.services import apigateway_v1
        return await apigateway_v1.handle_request(method, path, headers, body, query_params)

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # Minimum expected: ["v2", <resource>]

    if not parts or parts[0] != "v2":
        return _apigw_error("NotFoundException", f"Unknown path: {path}", 404)

    resource = parts[1] if len(parts) > 1 else ""

    # /v2/tags/{resourceArn} — tags endpoint
    if resource == "tags":
        # resourceArn may contain slashes; rejoin everything after "tags/"
        resource_arn = "/".join(parts[2:]) if len(parts) > 2 else ""
        if method == "GET":
            return _get_tags(resource_arn)
        if method == "POST":
            return _tag_resource(resource_arn, data)
        if method == "DELETE":
            tag_keys = query_params.get("tagKeys", [])
            if isinstance(tag_keys, str):
                tag_keys = [tag_keys]
            return _untag_resource(resource_arn, tag_keys)

    if resource == "apis":
        api_id = parts[2] if len(parts) > 2 else None
        sub = parts[3] if len(parts) > 3 else None
        sub_id = parts[4] if len(parts) > 4 else None

        # /v2/apis
        if not api_id:
            if method == "POST":
                return _create_api(data)
            if method == "GET":
                return _get_apis()

        # /v2/apis/{apiId}
        if api_id and not sub:
            if method == "GET":
                return _get_api(api_id)
            if method == "DELETE":
                return _delete_api(api_id)
            if method == "PATCH":
                return _update_api(api_id, data)

        # /v2/apis/{apiId}/routes[/{routeId}[/routeresponses[/{routeResponseId}]]]
        if api_id and sub == "routes":
            rr_segment = parts[5] if len(parts) > 5 else None
            rr_id = parts[6] if len(parts) > 6 else None
            if not sub_id:
                if method == "POST":
                    return _create_route(api_id, data)
                if method == "GET":
                    return _get_routes(api_id)
            elif rr_segment == "routeresponses":
                if not rr_id:
                    if method == "POST":
                        return _create_route_response(api_id, sub_id, data)
                    if method == "GET":
                        return _get_route_responses(api_id, sub_id)
                else:
                    if method == "GET":
                        return _get_route_response(api_id, sub_id, rr_id)
                    if method == "PATCH":
                        return _update_route_response(api_id, sub_id, rr_id, data)
                    if method == "DELETE":
                        return _delete_route_response(api_id, sub_id, rr_id)
            else:
                if method == "GET":
                    return _get_route(api_id, sub_id)
                if method == "PATCH":
                    return _update_route(api_id, sub_id, data)
                if method == "DELETE":
                    return _delete_route(api_id, sub_id)

        # /v2/apis/{apiId}/integrations[/{integrationId}[/integrationresponses[/{irId}]]]
        if api_id and sub == "integrations":
            ir_segment = parts[5] if len(parts) > 5 else None
            ir_id = parts[6] if len(parts) > 6 else None
            if not sub_id:
                if method == "POST":
                    return _create_integration(api_id, data)
                if method == "GET":
                    return _get_integrations(api_id)
            elif ir_segment == "integrationresponses":
                if not ir_id:
                    if method == "POST":
                        return _create_integration_response(api_id, sub_id, data)
                    if method == "GET":
                        return _get_integration_responses(api_id, sub_id)
                else:
                    if method == "GET":
                        return _get_integration_response(api_id, sub_id, ir_id)
                    if method == "PATCH":
                        return _update_integration_response(api_id, sub_id, ir_id, data)
                    if method == "DELETE":
                        return _delete_integration_response(api_id, sub_id, ir_id)
            else:
                if method == "GET":
                    return _get_integration(api_id, sub_id)
                if method == "PATCH":
                    return _update_integration(api_id, sub_id, data)
                if method == "DELETE":
                    return _delete_integration(api_id, sub_id)

        # /v2/apis/{apiId}/stages[/{stageName}]
        if api_id and sub == "stages":
            if not sub_id:
                if method == "POST":
                    return _create_stage(api_id, data)
                if method == "GET":
                    return _get_stages(api_id)
            else:
                if method == "GET":
                    return _get_stage(api_id, sub_id)
                if method == "PATCH":
                    return _update_stage(api_id, sub_id, data)
                if method == "DELETE":
                    return _delete_stage(api_id, sub_id)

        # /v2/apis/{apiId}/deployments[/{deploymentId}]
        if api_id and sub == "deployments":
            if not sub_id:
                if method == "POST":
                    return _create_deployment(api_id, data)
                if method == "GET":
                    return _get_deployments(api_id)
            else:
                if method == "GET":
                    return _get_deployment(api_id, sub_id)
                if method == "DELETE":
                    return _delete_deployment(api_id, sub_id)

        # /v2/apis/{apiId}/authorizers[/{authorizerId}]
        if api_id and sub == "authorizers":
            if not sub_id:
                if method == "POST":
                    return _create_authorizer(api_id, data)
                if method == "GET":
                    return _get_authorizers(api_id)
            else:
                if method == "GET":
                    return _get_authorizer(api_id, sub_id)
                if method == "PATCH":
                    return _update_authorizer(api_id, sub_id, data)
                if method == "DELETE":
                    return _delete_authorizer(api_id, sub_id)

    return _apigw_error("NotFoundException", f"Unknown API Gateway path: {path}", 404)


# ---- Data plane ----

def _cors_response_headers(cors_cfg: dict, origin: str) -> dict:
    """Build CORS response headers for a non-OPTIONS dispatched response.

    Per AWS (https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-cors.html):
      - If Origin matches allow_origins (or allow_origins contains "*"), echo
        the caller's Origin back (or "*"); else omit CORS headers entirely.
      - allow_credentials is only emitted when true, and requires a concrete
        origin — never paired with "*".
      - expose_headers / max_age / etc. are attached if configured.
    """
    if not cors_cfg:
        return {}
    allowed_origins = [o.lower() for o in cors_cfg.get("allowOrigins", [])]
    origin_lc = (origin or "").lower()
    if allowed_origins == ["*"]:
        allow_origin_value = "*"
    elif origin_lc and origin_lc in allowed_origins:
        allow_origin_value = origin  # echo exact caller-supplied casing
    else:
        return {}

    out: dict = {"Access-Control-Allow-Origin": allow_origin_value}
    if cors_cfg.get("allowCredentials") and allow_origin_value != "*":
        out["Access-Control-Allow-Credentials"] = "true"
    if cors_cfg.get("exposeHeaders"):
        out["Access-Control-Expose-Headers"] = ",".join(cors_cfg["exposeHeaders"])
    if "Origin" not in out.get("Vary", ""):
        out["Vary"] = "Origin"
    return out


def _cors_preflight_response(cors_cfg: dict, origin: str) -> tuple:
    """Build the full OPTIONS preflight response from corsConfiguration."""
    if not cors_cfg:
        # AWS behaviour: API without CORS configured returns 403 on preflight.
        return 403, {"Content-Type": "application/json"}, json.dumps(
            {"message": "CORS not configured"}
        ).encode()

    base = _cors_response_headers(cors_cfg, origin)
    if not base:
        # Origin not in allow_origins — 403, no CORS headers echoed back.
        return 403, {"Content-Type": "application/json"}, json.dumps(
            {"message": "CORS origin denied"}
        ).encode()

    if cors_cfg.get("allowMethods"):
        base["Access-Control-Allow-Methods"] = ",".join(cors_cfg["allowMethods"])
    if cors_cfg.get("allowHeaders"):
        base["Access-Control-Allow-Headers"] = ",".join(cors_cfg["allowHeaders"])
    if cors_cfg.get("maxAge") is not None:
        base["Access-Control-Max-Age"] = str(cors_cfg["maxAge"])
    base["Content-Length"] = "0"
    return 204, base, b""


async def handle_execute(api_id, stage, path, method, headers, body, query_params):
    """Execute an API request through a deployed API (data plane)."""
    api = _apis.get(api_id)
    if not api:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "Not Found"}).encode()

    # CORS preflight: served from the API's corsConfiguration before any route
    # matching, because AWS responds to OPTIONS itself without invoking the
    # integration. (#406)
    cors_cfg = api.get("corsConfiguration") or {}
    if method == "OPTIONS":
        return _cors_preflight_response(cors_cfg, headers.get("origin") or headers.get("Origin", ""))

    api_stages = _stages.get(api_id, {})
    if stage not in api_stages and stage != "$default":
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": f"Stage '{stage}' not found"}).encode()

    route = _match_route(api_id, method, path)
    if not route:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "No route found"}).encode()

    raw_target = route.get("target", "").replace("integrations/", "")
    # Target is "{integrationId}" — the current Ref / CFN physical ID.
    # Legacy stacks created against the broken #480 provisioner (fixed in
    # #487) wrote "{apiId}/{integrationId}"; strip the prefix for compat.
    integration_id = raw_target.split("/")[-1] if "/" in raw_target else raw_target
    integration = _integrations.get(api_id, {}).get(integration_id)
    if not integration:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": "No integration configured"}).encode()

    integration_type = integration.get("integrationType", "")

    if integration_type == "AWS_PROXY":
        route_key = route.get("routeKey", "$default")
        path_params = None
        rk_parts = route_key.split(" ", 1)
        if len(rk_parts) == 2:
            path_params = _extract_path_params(rk_parts[1], path) or None
        response = await _invoke_lambda_proxy(integration, api_id, stage, path, method, headers, body, query_params, route_key, path_params)
    elif integration_type == "HTTP_PROXY":
        response = await _invoke_http_proxy(integration, path, method, headers, body, query_params)
    else:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": f"Unsupported integration type: {integration_type}"}).encode()

    # Decorate dispatched response with per-API CORS headers (#406) — AWS adds
    # these in front of the integration response for non-OPTIONS requests.
    if cors_cfg:
        status, resp_headers, resp_body = response
        resp_headers.update(_cors_response_headers(cors_cfg, headers.get("origin") or headers.get("Origin", "")))
        response = status, resp_headers, resp_body
    return response


def _match_route(api_id, method, path):
    """Find the best matching route for method+path. $default route is the fallback."""
    routes = _routes.get(api_id, {})
    # First pass: look for a specific method+path match (skip $default)
    for route in routes.values():
        key = route.get("routeKey", "")
        if key == "$default":
            continue
        parts = key.split(" ", 1)
        if len(parts) == 2:
            r_method, r_path = parts
            if (r_method == "ANY" or r_method == method) and _path_matches(r_path, path):
                return route
    # Second pass: $default catch-all
    for route in routes.values():
        if route.get("routeKey") == "$default":
            return route
    return None


def _extract_path_params(route_path: str, request_path: str) -> dict | None:
    """
    Extract path parameter values from a request path using the route template.

    Returns a dict of {paramName: value} on match, or None if no match.
    Supports:
      {param}   — single path segment (no slashes)
      {proxy+}  — greedy match (one or more path segments, may include slashes)
    """
    parts = re.split(r"(\{[^}]+\})", route_path)
    pattern_parts = []
    param_names = []
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            inner = part[1:-1]
            if inner.endswith("+"):
                param_names.append(inner[:-1])
                pattern_parts.append("(.+)")
            else:
                param_names.append(inner)
                pattern_parts.append("([^/]+)")
        else:
            pattern_parts.append(re.escape(part))
    m = re.fullmatch("".join(pattern_parts), request_path)
    if not m:
        return None
    return dict(zip(param_names, m.groups())) if param_names else {}


def _path_matches(route_path: str, request_path: str) -> bool:
    """Match a route path against a request path."""
    return _extract_path_params(route_path, request_path) is not None


async def _invoke_lambda_proxy(integration, api_id, stage, path, method, headers, body, query_params, route_key="$default", path_params=None):
    """Invoke a Lambda function using the API Gateway v2 proxy event format."""
    from ministack.services import lambda_svc

    # integrationUri from Terraform / real AWS is wrapped:
    #   arn:aws:apigateway:<region>:lambda:path/2015-03-31/functions/<lambda-arn>/invocations
    # Unwrap to the inner Lambda ARN before parsing name + qualifier (#409).
    # Bare Lambda ARNs and plain function names keep working via the fallback.
    lambda_ref = _extract_lambda_ref_from_integration_uri(integration.get("integrationUri", ""))
    func_name, qualifier = lambda_svc._resolve_name_and_qualifier(lambda_ref)
    func_data, func_config = lambda_svc._get_func_record_for_qualifier(func_name, qualifier)
    if func_data is None:
        return 502, {"Content-Type": "application/json"}, json.dumps({
            "message": f"Lambda function '{func_name}'" +
                       (f" (qualifier '{qualifier}')" if qualifier else "") +
                       " not found"
        }).encode()

    # Build API Gateway v2 proxy event (payload format 2.0)
    # AWS API Gateway v2 joins multi-value query params with commas
    qs = {k: ",".join(v) for k, v in query_params.items()} if query_params else None
    raw_qs = "&".join(f"{k}={val}" for k, vals in query_params.items() for val in vals)
    event = {
        "version": "2.0",
        "routeKey": route_key,
        "rawPath": path,
        "rawQueryString": raw_qs,
        "headers": dict(headers),
        "queryStringParameters": qs,
        "requestContext": {
            "accountId": get_account_id(),
            "apiId": api_id,
            "domainName": f"{api_id}.execute-api.{_HOST}",
            "http": {
                "method": method,
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": headers.get("user-agent", ""),
            },
            "requestId": new_uuid(),
            "routeKey": route_key,
            "stage": stage,
            "time": time.strftime("%d/%b/%Y:%H:%M:%S +0000"),
            "timeEpoch": int(time.time() * 1000),
        },
        "pathParameters": path_params,
        "body": body.decode("utf-8", errors="replace") if body else None,
        "isBase64Encoded": False,
    }

    # Route through the central _execute_function dispatcher so CloudWatch
    # Logs emission and Docker log output work for API Gateway invocations.
    # Response shaping (throttle→429, error→502, body→envelope) goes through
    # the shared helper so v1/v2 stay consistent and we get 429s on
    # ConcurrentInvocationLimitExceeded.
    exec_record = {"config": func_config, "code_zip": func_data.get("code_zip")}
    result = await asyncio.to_thread(lambda_svc._execute_function, exec_record, event)
    lambda_response, _ = lambda_svc.lambda_execute_result_to_api_proxy_response(result)

    status = lambda_response.get("statusCode", 200)
    resp_headers = {"Content-Type": "application/json"}
    resp_headers.update(lambda_response.get("headers", {}))
    resp_body = lambda_response.get("body", "")
    if isinstance(resp_body, str):
        resp_body = resp_body.encode("utf-8")
    elif isinstance(resp_body, dict):
        resp_body = json.dumps(resp_body, ensure_ascii=False).encode("utf-8")

    return status, resp_headers, resp_body


async def _invoke_http_proxy(integration, path, method, headers, body, query_params):
    """Forward a request to an HTTP backend."""
    uri = integration.get("integrationUri", "")
    url = uri.rstrip("/") + path

    req = urllib.request.Request(url, data=body or None, method=method)
    for k, v in headers.items():
        if k.lower() not in ("host", "content-length"):
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_body = resp.read()
            resp_headers = {"Content-Type": resp.headers.get("Content-Type", "application/json")}
            return resp.status, resp_headers, resp_body
    except urllib.error.HTTPError as e:
        return e.code, {"Content-Type": "application/json"}, e.read()
    except Exception as ex:
        return 502, {"Content-Type": "application/json"}, json.dumps({"message": str(ex)}).encode()


# ---- Control plane: APIs ----

def _resolve_custom_api_id(tags: dict, existing: "AccountScopedDict") -> str | None:
    """Return a caller-pinned API id from the ``ms-custom-id`` tag, or None
    if no tag is set (issue #400).

    Raises ``ValueError`` if the requested id is already in use in the caller's
    account, so misconfigs surface immediately instead of silently falling back
    to a random id.

    ``ls-custom-id`` (LocalStack's tag) is intentionally NOT supported — callers
    hitting it get a clear ``BadRequestException`` pointing them at
    ``ms-custom-id`` so the ministack-native key is the only contract."""
    if not isinstance(tags, dict):
        return None
    if "ls-custom-id" in tags and "ms-custom-id" not in tags:
        raise ValueError(
            "ls-custom-id tag is not supported; use 'ms-custom-id' instead"
        )
    custom = tags.get("ms-custom-id")
    if not custom:
        return None
    if custom in existing:
        raise ValueError(
            f"API id '{custom}' (from ms-custom-id tag) is already in use"
        )
    return str(custom)


def _create_api(data):
    tags = data.get("tags", {})
    try:
        api_id = _resolve_custom_api_id(tags, _apis) or new_uuid()[:8]
    except ValueError as exc:
        msg = str(exc)
        if "already in use" in msg:
            return _apigw_error("ConflictException", msg, 409)
        return _apigw_error("BadRequestException", msg, 400)
    protocol = data.get("protocolType", "HTTP")
    # AWS defaults: HTTP → "$request.method $request.path"; WEBSOCKET → "$request.body.action".
    default_rse = "$request.body.action" if protocol == "WEBSOCKET" else "$request.method $request.path"
    api = {
        "apiId": api_id,
        "name": data.get("name", "unnamed"),
        "protocolType": protocol,
        "apiEndpoint": f"http://{api_id}.execute-api.{_HOST}:{_PORT}",
        "createdDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "routeSelectionExpression": data.get("routeSelectionExpression", default_rse),
        "apiKeySelectionExpression": data.get("apiKeySelectionExpression", "$request.header.x-api-key"),
        "tags": data.get("tags", {}),
        "disableSchemaValidation": data.get("disableSchemaValidation", False),
        "disableExecuteApiEndpoint": data.get("disableExecuteApiEndpoint", False),
        "version": data.get("version", ""),
        "description": data.get("description", ""),
    }
    if data.get("corsConfiguration"):
        api["corsConfiguration"] = data["corsConfiguration"]
    _apis[api_id] = api
    _routes[api_id] = {}
    _integrations[api_id] = {}
    _stages[api_id] = {}
    _deployments[api_id] = {}
    _api_tags[_api_arn(api_id)] = dict(data.get("tags", {}))
    return _apigw_response(api, 201)


def _get_api(api_id):
    api = _apis.get(api_id)
    if not api:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    return _apigw_response(api)


def _get_apis():
    return _apigw_response({"items": list(_apis.values())})


def _delete_api(api_id):
    _apis.pop(api_id, None)
    _routes.pop(api_id, None)
    _integrations.pop(api_id, None)
    _stages.pop(api_id, None)
    _deployments.pop(api_id, None)
    _api_tags.pop(_api_arn(api_id), None)
    return 204, {}, b""


def _update_api(api_id, data):
    api = _apis.get(api_id)
    if not api:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    for k in ("name", "corsConfiguration", "routeSelectionExpression",
              "disableSchemaValidation", "disableExecuteApiEndpoint", "version"):
        if k in data:
            api[k] = data[k]
    return _apigw_response(api)


# ---- Control plane: Routes ----

def _create_route(api_id, data):
    if api_id not in _apis:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    route_id = new_uuid()[:8]
    route = {
        "routeId": route_id,
        "routeKey": data.get("routeKey", "$default"),
        "target": data.get("target", ""),
        "authorizationType": data.get("authorizationType", "NONE"),
        "apiKeyRequired": data.get("apiKeyRequired", False),
        "operationName": data.get("operationName", ""),
        "requestModels": data.get("requestModels", {}),
        "requestParameters": data.get("requestParameters", {}),
    }
    _routes.setdefault(api_id, {})[route_id] = route
    return _apigw_response(route, 201)


def _get_routes(api_id):
    return _apigw_response({"items": list(_routes.get(api_id, {}).values())})


def _get_route(api_id, route_id):
    route = _routes.get(api_id, {}).get(route_id)
    if not route:
        return _apigw_error("NotFoundException", f"Route {route_id} not found", 404)
    return _apigw_response(route)


def _update_route(api_id, route_id, data):
    route = _routes.get(api_id, {}).get(route_id)
    if not route:
        return _apigw_error("NotFoundException", f"Route {route_id} not found", 404)
    for k in ("routeKey", "target", "authorizationType", "apiKeyRequired", "operationName"):
        if k in data:
            route[k] = data[k]
    return _apigw_response(route)


def _delete_route(api_id, route_id):
    _routes.get(api_id, {}).pop(route_id, None)
    return 204, {}, b""


# ---- Control plane: Integrations ----

def _create_integration(api_id, data):
    if api_id not in _apis:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    int_id = new_uuid()[:8]
    integration = {
        "integrationId": int_id,
        "integrationType": data.get("integrationType", "AWS_PROXY"),
        "integrationUri": data.get("integrationUri", ""),
        "integrationMethod": data.get("integrationMethod", "POST"),
        "payloadFormatVersion": data.get("payloadFormatVersion", "2.0"),
        "timeoutInMillis": data.get("timeoutInMillis", 30000),
        "connectionType": data.get("connectionType", "INTERNET"),
        "description": data.get("description", ""),
        "requestParameters": data.get("requestParameters", {}),
        "requestTemplates": data.get("requestTemplates", {}),
        "responseParameters": data.get("responseParameters", {}),
        # #439: contentHandlingStrategy (CONVERT_TO_TEXT | CONVERT_TO_BINARY)
        # is accepted on Create/Update and echoed back by Get; Terraform's
        # aws_apigatewayv2_integration otherwise plans to re-add it on every apply.
        "contentHandlingStrategy": data.get("contentHandlingStrategy"),
    }
    _integrations.setdefault(api_id, {})[int_id] = integration
    return _apigw_response(integration, 201)


def _get_integrations(api_id):
    return _apigw_response({"items": list(_integrations.get(api_id, {}).values())})


def _get_integration(api_id, int_id):
    integration = _integrations.get(api_id, {}).get(int_id)
    if not integration:
        return _apigw_error("NotFoundException", f"Integration {int_id} not found", 404)
    return _apigw_response(integration)


def _update_integration(api_id, int_id, data):
    integration = _integrations.get(api_id, {}).get(int_id)
    if not integration:
        return _apigw_error("NotFoundException", f"Integration {int_id} not found", 404)
    for k in ("integrationType", "integrationUri", "integrationMethod",
              "payloadFormatVersion", "timeoutInMillis", "connectionType",
              "description", "requestParameters", "requestTemplates", "responseParameters",
              "contentHandlingStrategy"):
        if k in data:
            integration[k] = data[k]
    return _apigw_response(integration)


def _delete_integration(api_id, int_id):
    _integrations.get(api_id, {}).pop(int_id, None)
    return 204, {}, b""


# ---- Control plane: Stages ----

def _create_stage(api_id, data):
    if api_id not in _apis:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    stage_name = data.get("stageName", "$default")
    stage = {
        "stageName": stage_name,
        "autoDeploy": data.get("autoDeploy", False),
        "createdDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lastUpdatedDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stageVariables": data.get("stageVariables", {}),
        "description": data.get("description", ""),
        "defaultRouteSettings": data.get("defaultRouteSettings", {}),
        "routeSettings": data.get("routeSettings", {}),
        "tags": data.get("tags", {}),
    }
    _stages.setdefault(api_id, {})[stage_name] = stage
    return _apigw_response(stage, 201)


def _get_stages(api_id):
    return _apigw_response({"items": list(_stages.get(api_id, {}).values())})


def _get_stage(api_id, stage_name):
    stage = _stages.get(api_id, {}).get(stage_name)
    if not stage:
        return _apigw_error("NotFoundException", f"Stage '{stage_name}' not found", 404)
    return _apigw_response(stage)


def _update_stage(api_id, stage_name, data):
    stage = _stages.get(api_id, {}).get(stage_name)
    if not stage:
        return _apigw_error("NotFoundException", f"Stage '{stage_name}' not found", 404)
    for k in ("autoDeploy", "stageVariables", "description",
              "defaultRouteSettings", "routeSettings"):
        if k in data:
            stage[k] = data[k]
    stage["lastUpdatedDate"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return _apigw_response(stage)


def _delete_stage(api_id, stage_name):
    _stages.get(api_id, {}).pop(stage_name, None)
    return 204, {}, b""


# ---- Control plane: Deployments ----

def _create_deployment(api_id, data):
    if api_id not in _apis:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    deployment_id = new_uuid()[:8]
    deployment = {
        "deploymentId": deployment_id,
        "deploymentStatus": "DEPLOYED",
        "createdDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "description": data.get("description", ""),
    }
    _deployments.setdefault(api_id, {})[deployment_id] = deployment
    return _apigw_response(deployment, 201)


def _get_deployments(api_id):
    return _apigw_response({"items": list(_deployments.get(api_id, {}).values())})


def _get_deployment(api_id, deployment_id):
    deployment = _deployments.get(api_id, {}).get(deployment_id)
    if not deployment:
        return _apigw_error("NotFoundException", f"Deployment {deployment_id} not found", 404)
    return _apigw_response(deployment)


def _delete_deployment(api_id, deployment_id):
    _deployments.get(api_id, {}).pop(deployment_id, None)
    return 204, {}, b""


# ---- Control plane: Tags ----

def _get_tags(resource_arn: str):
    tags = _api_tags.get(resource_arn, {})
    return _apigw_response({"tags": tags})


def _tag_resource(resource_arn: str, data: dict):
    tags = data.get("tags", {})
    _api_tags.setdefault(resource_arn, {}).update(tags)
    return 201, {}, b""


def _untag_resource(resource_arn: str, tag_keys: list):
    existing = _api_tags.get(resource_arn, {})
    for key in tag_keys:
        existing.pop(key, None)
    return 204, {}, b""


# ---- Control plane: Authorizers ----

def _create_authorizer(api_id, data):
    if api_id not in _apis:
        return _apigw_error("NotFoundException", f"API {api_id} not found", 404)
    auth_id = new_uuid()[:8]
    authorizer = {
        "authorizerId": auth_id,
        "authorizerType": data.get("authorizerType", "JWT"),
        "name": data.get("name", ""),
        "identitySource": data.get("identitySource", ["$request.header.Authorization"]),
        "jwtConfiguration": data.get("jwtConfiguration", {}),
        "authorizerUri": data.get("authorizerUri", ""),
        "authorizerPayloadFormatVersion": data.get("authorizerPayloadFormatVersion", "2.0"),
        "authorizerResultTtlInSeconds": data.get("authorizerResultTtlInSeconds", 300),
        "enableSimpleResponses": data.get("enableSimpleResponses", False),
        "authorizerCredentialsArn": data.get("authorizerCredentialsArn", ""),
    }
    _authorizers.setdefault(api_id, {})[auth_id] = authorizer
    return _apigw_response(authorizer, 201)


def _get_authorizers(api_id):
    return _apigw_response({"items": list(_authorizers.get(api_id, {}).values())})


def _get_authorizer(api_id, auth_id):
    authorizer = _authorizers.get(api_id, {}).get(auth_id)
    if not authorizer:
        return _apigw_error("NotFoundException", f"Authorizer {auth_id} not found", 404)
    return _apigw_response(authorizer)


def _update_authorizer(api_id, auth_id, data):
    authorizer = _authorizers.get(api_id, {}).get(auth_id)
    if not authorizer:
        return _apigw_error("NotFoundException", f"Authorizer {auth_id} not found", 404)
    for k in ("name", "identitySource", "jwtConfiguration", "authorizerUri",
              "authorizerPayloadFormatVersion", "authorizerResultTtlInSeconds",
              "enableSimpleResponses", "authorizerCredentialsArn"):
        if k in data:
            authorizer[k] = data[k]
    return _apigw_response(authorizer)


def _delete_authorizer(api_id, auth_id):
    _authorizers.get(api_id, {}).pop(auth_id, None)
    return 204, {}, b""


def reset():
    _apis.clear()
    _routes.clear()
    _integrations.clear()
    _stages.clear()
    _deployments.clear()
    _authorizers.clear()
    _api_tags.clear()
    _route_responses.clear()
    _integration_responses.clear()
    # Signal any live WS connections to shut down, then drop registry.
    for conn in list(_ws_connections.values()):
        ev = conn.get("close_event")
        if ev is not None:
            try:
                ev.set()
            except Exception:
                pass
    _ws_connections.clear()


# ==========================================================================
# Route responses (WebSocket)
# ==========================================================================

def _create_route_response(api_id, route_id, data):
    routes = _routes.get(api_id, {})
    if route_id not in routes:
        return _apigw_error("NotFoundException", f"Route {route_id} not found", 404)
    rr_id = new_uuid()[:8]
    rr = {
        "routeResponseId": rr_id,
        "routeResponseKey": data.get("routeResponseKey", "$default"),
        "modelSelectionExpression": data.get("modelSelectionExpression"),
        "responseModels": data.get("responseModels", {}),
        "responseParameters": data.get("responseParameters", {}),
    }
    by_route = _route_responses.setdefault(api_id, {}).setdefault(route_id, {})
    by_route[rr_id] = rr
    return _apigw_response(rr, 201)


def _get_route_responses(api_id, route_id):
    items = list(_route_responses.get(api_id, {}).get(route_id, {}).values())
    return _apigw_response({"items": items})


def _get_route_response(api_id, route_id, rr_id):
    rr = _route_responses.get(api_id, {}).get(route_id, {}).get(rr_id)
    if not rr:
        return _apigw_error("NotFoundException", f"RouteResponse {rr_id} not found", 404)
    return _apigw_response(rr)


def _update_route_response(api_id, route_id, rr_id, data):
    rr = _route_responses.get(api_id, {}).get(route_id, {}).get(rr_id)
    if not rr:
        return _apigw_error("NotFoundException", f"RouteResponse {rr_id} not found", 404)
    for k in ("routeResponseKey", "modelSelectionExpression", "responseModels", "responseParameters"):
        if k in data:
            rr[k] = data[k]
    return _apigw_response(rr)


def _delete_route_response(api_id, route_id, rr_id):
    _route_responses.get(api_id, {}).get(route_id, {}).pop(rr_id, None)
    return 204, {}, b""


# ==========================================================================
# Integration responses (WebSocket)
# ==========================================================================

def _create_integration_response(api_id, integration_id, data):
    integs = _integrations.get(api_id, {})
    if integration_id not in integs:
        return _apigw_error("NotFoundException", f"Integration {integration_id} not found", 404)
    ir_id = new_uuid()[:8]
    ir = {
        "integrationResponseId": ir_id,
        "integrationResponseKey": data.get("integrationResponseKey", "$default"),
        "contentHandlingStrategy": data.get("contentHandlingStrategy"),
        "templateSelectionExpression": data.get("templateSelectionExpression"),
        "responseParameters": data.get("responseParameters", {}),
        "responseTemplates": data.get("responseTemplates", {}),
    }
    by_int = _integration_responses.setdefault(api_id, {}).setdefault(integration_id, {})
    by_int[ir_id] = ir
    return _apigw_response(ir, 201)


def _get_integration_responses(api_id, integration_id):
    items = list(_integration_responses.get(api_id, {}).get(integration_id, {}).values())
    return _apigw_response({"items": items})


def _get_integration_response(api_id, integration_id, ir_id):
    ir = _integration_responses.get(api_id, {}).get(integration_id, {}).get(ir_id)
    if not ir:
        return _apigw_error("NotFoundException", f"IntegrationResponse {ir_id} not found", 404)
    return _apigw_response(ir)


def _update_integration_response(api_id, integration_id, ir_id, data):
    ir = _integration_responses.get(api_id, {}).get(integration_id, {}).get(ir_id)
    if not ir:
        return _apigw_error("NotFoundException", f"IntegrationResponse {ir_id} not found", 404)
    for k in ("integrationResponseKey", "contentHandlingStrategy", "templateSelectionExpression",
              "responseParameters", "responseTemplates"):
        if k in data:
            ir[k] = data[k]
    return _apigw_response(ir)


def _delete_integration_response(api_id, integration_id, ir_id):
    _integration_responses.get(api_id, {}).get(integration_id, {}).pop(ir_id, None)
    return 204, {}, b""


# ==========================================================================
# WebSocket data plane
# ==========================================================================

def _api_protocol(api_id: str) -> str | None:
    """Return the protocolType for an API id, checking all accounts.

    WebSocket connections arrive on the execute-api host before we've resolved
    which account owns the api. We scan every AccountScopedDict bucket to find
    the owning account, then return (protocol, account_id).
    """
    info = _api_owner(api_id)
    return info[0] if info else None


def _api_owner(api_id: str):
    """Return (protocolType, owner_account_id) for an API or None if unknown."""
    # AccountScopedDict stores keys as (account_id, original_key). Walk internals
    # so we can find the owning account without knowing it up front.
    for (acct, key), api in _apis._data.items():
        if key == api_id:
            return (api.get("protocolType", "HTTP"), acct)
    return None


def _match_ws_route(api_id: str, route_key: str):
    """Find the route for a WS route key (e.g. '$connect', '$disconnect', '$default',
    or a custom action like 'sendMessage'). Fallback to $default."""
    routes = _routes.get(api_id, {})
    for r in routes.values():
        if r.get("routeKey") == route_key:
            return r
    for r in routes.values():
        if r.get("routeKey") == "$default":
            return r
    return None


def _evaluate_route_selection(expr: str, payload_text: str) -> str:
    """Evaluate a WebSocket RouteSelectionExpression against an incoming frame.

    AWS supports '$request.body.<dotted.path>' (the common case) and any plain
    literal that the client includes. Anything we can't parse falls back to
    '$default'.
    """
    if not expr:
        return "$default"
    if expr.startswith("$request.body."):
        path = expr[len("$request.body."):]
        try:
            obj = json.loads(payload_text) if payload_text else {}
        except (ValueError, TypeError):
            return "$default"
        cur = obj
        for segment in path.split("."):
            if isinstance(cur, dict) and segment in cur:
                cur = cur[segment]
            else:
                return "$default"
        return str(cur) if cur is not None else "$default"
    return "$default"


async def _invoke_ws_lambda(api_id: str, account_id: str, route: dict, stage: str,
                            connection_id: str, event_type: str, message_id: str,
                            body_text: str, source_ip: str, headers: dict,
                            query_params: dict | None = None, **kwargs) -> dict | None:
    """Invoke a WS route's integration. Returns the integration's response dict or None.

    The event shape matches AWS WebSocket v2 proxy (see docs: "Set up integration
    request in API Gateway" under WebSocket). Headers include the incoming
    handshake headers for $connect (along with query string params); for
    MESSAGE/DISCONNECT the body is the frame payload.

    Integration type handling:
      - AWS / AWS_PROXY → dispatch to Lambda via the warm worker pool.
      - MOCK            → synthesise a 200 response (no Lambda). Any
                          `responseTemplates.$default` on a matching
                          integration response is returned as the body.
      - anything else   → returns None (caller treats as "no reply").
                          AWS itself only supports AWS/AWS_PROXY/MOCK for
                          WebSocket routes, so this also covers the
                          never-valid HTTP_PROXY case.
    """
    from ministack.core.lambda_runtime import get_or_create_worker
    from ministack.services import lambda_svc

    raw_target = route.get("target", "").replace("integrations/", "")
    # Target is "{integrationId}" — the current Ref / CFN physical ID.
    # Legacy stacks created against the broken #480 provisioner (fixed in
    # #487) wrote "{apiId}/{integrationId}"; strip the prefix for compat.
    integration_id = raw_target.split("/")[-1] if "/" in raw_target else raw_target
    integration = _integrations.get(api_id, {}).get(integration_id)
    if not integration:
        return None

    int_type = integration.get("integrationType", "")
    if int_type == "MOCK":
        ir_map = _integration_responses.get(api_id, {}).get(integration_id, {})
        body = ""
        for ir in ir_map.values():
            templates = ir.get("responseTemplates", {}) or {}
            if "$default" in templates:
                body = templates["$default"]
                break
            if templates:
                body = next(iter(templates.values()))
                break
        return {"statusCode": 200, "body": body}

    if int_type not in ("AWS_PROXY", "AWS"):
        logger.warning(
            "WebSocket route %s has unsupported integrationType %r; "
            "AWS only supports AWS / AWS_PROXY / MOCK for WebSocket APIs",
            route.get("routeKey"), int_type,
        )
        return None

    # Same unwrap path as HTTP (#409): APIGW integrationUri is the wrapper form
    # that nests the Lambda ARN between /functions/ and /invocations.
    lambda_ref = _extract_lambda_ref_from_integration_uri(integration.get("integrationUri", ""))
    func_name, qualifier = lambda_svc._resolve_name_and_qualifier(lambda_ref)
    func_data, func_config = lambda_svc._get_func_record_for_qualifier(func_name, qualifier)
    if func_data is None:
        return None

    request_context = {
        "routeKey": route.get("routeKey", "$default"),
        "eventType": event_type,
        "extendedRequestId": new_uuid(),
        "requestTime": time.strftime("%d/%b/%Y:%H:%M:%S +0000"),
        "stage": stage,
        "connectedAt": int(time.time() * 1000),
        "requestTimeEpoch": int(time.time() * 1000),
        "identity": {"sourceIp": source_ip, "userAgent": headers.get("user-agent", "")},
        "requestId": message_id,
        "domainName": f"{api_id}.execute-api.{_HOST}",
        "connectionId": connection_id,
        "apiId": api_id,
    }
    if event_type == "DISCONNECT":
        # Populated by handle_websocket from the ASGI disconnect message.
        request_context["disconnectReason"] = kwargs.get("disconnect_reason", "")
        request_context["disconnectStatusCode"] = int(kwargs.get("disconnect_code", 1005))
    if event_type == "MESSAGE":
        request_context["messageId"] = message_id

    event = {
        "requestContext": request_context,
        "body": body_text if body_text is not None else "",
        "isBase64Encoded": False,
    }
    if event_type == "CONNECT":
        event["headers"] = dict(headers)
        event["multiValueHeaders"] = {k: [v] for k, v in headers.items()}
        if query_params:
            # AWS flattens single-valued QS params to string, keeps multi-valued as lists.
            event["queryStringParameters"] = {
                k: (v[-1] if isinstance(v, list) else v)
                for k, v in query_params.items()
            }
            event["multiValueQueryStringParameters"] = {
                k: (v if isinstance(v, list) else [v])
                for k, v in query_params.items()
            }
        else:
            event["queryStringParameters"] = None
            event["multiValueQueryStringParameters"] = None

    runtime = func_config.get("Runtime", "")
    code_zip = func_data.get("code_zip")
    if code_zip and runtime.startswith(("python", "nodejs")):
        # get_or_create_worker keys internally as f"{func_name}:{qualifier}",
        # so we must pass name + qualifier separately. Building a synthetic
        # `f"{func_name}:{qualifier}"` and passing it as func_name double-
        # suffixes the key (`fn:qual:$LATEST`), missing the warm pool the
        # SDK invoke path populates and forcing a cold start on every WS
        # message. That's why pre-warming the function via the SDK didn't
        # help WebSocket dispatch — keys didn't match.
        worker = get_or_create_worker(
            func_name, func_config, code_zip,
            qualifier=qualifier or "$LATEST",
        )
        result = await asyncio.to_thread(worker.invoke, event, message_id)
        if result.get("status") == "error":
            return {"statusCode": 500, "body": result.get("error", "")}
        return result.get("result", {})
    # Image/unsupported runtime stub — success without body.
    return {"statusCode": 200, "body": ""}


async def handle_websocket(scope, receive, send, api_id: str, path_override: str | None = None):
    """Drive a WebSocket session for a $WEBSOCKET API.

    Flow:
      1. Receive `websocket.connect` from ASGI.
      2. Invoke `$connect` route Lambda (if any). 2xx → accept; else close.
      3. Loop on `websocket.receive`: evaluate routeSelectionExpression, dispatch
         to the matching route's Lambda. If the Lambda returns a body, forward it
         back on the same socket.
      4. Concurrently drain the per-connection outbox (fed by @connections
         PostToConnection) and forward messages to the socket.
      5. On client disconnect, invoke `$disconnect` route Lambda (fire-and-forget).

    ``path_override`` is used when the caller addressed us via the LocalStack-
    compat path form (``/_aws/execute-api/{apiId}/{stage}``) so we read the
    stage from the rewritten path instead of the raw URL.
    """
    owner = _api_owner(api_id)
    if not owner or owner[0] != "WEBSOCKET":
        # Not a WS API — refuse the upgrade.
        await receive()  # consume websocket.connect
        await send({"type": "websocket.close", "code": 1008})
        return

    protocol, account_id = owner

    # Stage parsing: Host-based URLs look like wss://{apiId}.execute-api.../stage;
    # path-based compat URLs (#401) use path_override with the rewritten path.
    # If the first segment isn't a configured stage name but the API has a
    # ``$default`` stage, route to it (issue #404).
    path = path_override if path_override is not None else scope.get("path", "")
    path_parts = path.lstrip("/").split("/", 1)
    tentative = path_parts[0] if path_parts and path_parts[0] else "$default"
    configured_stages = _stages.get(api_id, {})
    if tentative in configured_stages:
        stage = tentative
    elif "$default" in configured_stages:
        stage = "$default"
    else:
        stage = tentative  # pass through; downstream will handle unknown-stage

    headers = {}
    for name, value in scope.get("headers", []):
        try:
            headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            headers[name.decode("latin-1").lower()] = value.decode("latin-1")

    qs = scope.get("query_string", b"").decode("utf-8")
    from urllib.parse import parse_qs as _pq
    query_params = {k: v for k, v in _pq(qs, keep_blank_values=True).items()}

    client = scope.get("client") or ("127.0.0.1", 0)
    source_ip = client[0] if isinstance(client, (tuple, list)) else "127.0.0.1"

    # Wait for websocket.connect.
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return

    connection_id = new_uuid().replace("-", "")[:16]

    # Set account context so downstream Lambda invocations see the right tenant.
    from ministack.core.responses import _request_account_id
    token = _request_account_id.set(account_id)
    try:
        # $connect hook
        connect_route = _match_ws_route(api_id, "$connect")
        if connect_route is not None:
            resp = await _invoke_ws_lambda(
                api_id, account_id, connect_route, stage, connection_id,
                "CONNECT", new_uuid(), "", source_ip, headers,
                query_params=query_params,
            )
            status = int((resp or {}).get("statusCode", 200))
            if status < 200 or status >= 300:
                await send({"type": "websocket.close", "code": 1008})
                return

        await send({"type": "websocket.accept"})

        outbox: asyncio.Queue = asyncio.Queue()
        close_event = asyncio.Event()
        now_epoch = int(time.time())
        conn_record = {
            "apiId": api_id,
            "accountId": account_id,
            "stage": stage,
            # Int epoch seconds — matches ministack JSON timestamp convention.
            "connectedAt": now_epoch,
            "lastActiveAt": now_epoch,
            "sourceIp": source_ip,
            "identity": {"sourceIp": source_ip, "userAgent": headers.get("user-agent", "")},
            "outbox": outbox,
            "close_event": close_event,
        }
        _ws_connections[connection_id] = conn_record

        selection_expr = None
        api_obj = _apis.get(api_id)
        if api_obj:
            selection_expr = api_obj.get("routeSelectionExpression", "$request.body.action")

        async def _drain_outbox():
            while not close_event.is_set():
                try:
                    item = await asyncio.wait_for(outbox.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                if item is None:
                    return
                if isinstance(item, bytes):
                    await send({"type": "websocket.send", "bytes": item})
                else:
                    await send({"type": "websocket.send", "text": str(item)})

        drain_task = asyncio.create_task(_drain_outbox())

        disconnect_code = 1005  # 1005 = "no status rcvd" per RFC 6455, matches AWS default
        disconnect_reason = ""
        try:
            while True:
                message = await receive()
                mtype = message.get("type")
                if mtype == "websocket.disconnect":
                    disconnect_code = int(message.get("code", 1005) or 1005)
                    # ASGI extension: some servers (incl. modern hypercorn) pass the
                    # close-frame reason; fall back to empty string if not present.
                    disconnect_reason = message.get("reason") or ""
                    break
                if mtype != "websocket.receive":
                    continue
                frame_text = message.get("text")
                frame_bytes = message.get("bytes")
                payload = frame_text if frame_text is not None else (
                    frame_bytes.decode("utf-8", errors="replace") if frame_bytes else ""
                )
                conn_record["lastActiveAt"] = int(time.time())

                route_key = _evaluate_route_selection(selection_expr or "", payload)
                route = _match_ws_route(api_id, route_key)
                if route is None:
                    # No $default — AWS sends GoneException to the client; we log and continue.
                    continue
                msg_id = new_uuid()
                resp = await _invoke_ws_lambda(
                    api_id, account_id, route, stage, connection_id, "MESSAGE",
                    msg_id, payload, source_ip, headers,
                )
                if resp is None:
                    continue
                body = resp.get("body")
                if body:
                    if isinstance(body, (dict, list)):
                        body = json.dumps(body)
                    if isinstance(body, bytes):
                        await send({"type": "websocket.send", "bytes": body})
                    else:
                        await send({"type": "websocket.send", "text": str(body)})
        finally:
            close_event.set()
            try:
                await drain_task
            except Exception:
                pass
            _ws_connections.pop(connection_id, None)
            # Fire $disconnect route best-effort.
            disconnect_route = _match_ws_route(api_id, "$disconnect")
            if disconnect_route is not None:
                try:
                    await _invoke_ws_lambda(
                        api_id, account_id, disconnect_route, stage, connection_id,
                        "DISCONNECT", new_uuid(), "", source_ip, headers,
                        disconnect_code=disconnect_code,
                        disconnect_reason=disconnect_reason,
                    )
                except Exception:
                    logger.exception("error firing $disconnect")
            try:
                await send({"type": "websocket.close", "code": 1000})
            except Exception:
                pass
    finally:
        try:
            _request_account_id.reset(token)
        except Exception:
            pass


# ==========================================================================
# @connections management API
# ==========================================================================

async def handle_connections_api(method: str, api_id: str, stage: str,
                                  connection_id: str, body: bytes, headers: dict):
    """Serve the @connections runtime API.

    Paths (on execute-api host):
      POST   /{stage}/@connections/{connectionId}  → PostToConnection
      GET    /{stage}/@connections/{connectionId}  → GetConnection
      DELETE /{stage}/@connections/{connectionId}  → DeleteConnection

    AWS behaviour:
      - 410 Gone      if the connection is unknown or already closed.
      - 403 Forbidden if the caller does not own the API (not enforced locally).
      - 200           on success; POST returns empty body, GET returns JSON.
    """
    conn = _ws_connections.get(connection_id)
    if not conn or conn.get("apiId") != api_id:
        return 410, {"Content-Type": "application/json"}, json.dumps(
            {"message": "GoneException"}
        ).encode()

    if method == "POST":
        # Push the message into the connection outbox; drain_task will forward it.
        try:
            if body:
                await conn["outbox"].put(body)
        except Exception as exc:
            return 500, {"Content-Type": "application/json"}, json.dumps(
                {"message": str(exc)}
            ).encode()
        return 200, {"Content-Type": "application/json"}, b""

    if method == "GET":
        payload = {
            "ConnectedAt": conn.get("connectedAt"),
            "Identity": conn.get("identity", {}),
            "LastActiveAt": conn.get("lastActiveAt"),
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(payload).encode()

    if method == "DELETE":
        ev = conn.get("close_event")
        if ev is not None:
            try:
                ev.set()
            except Exception:
                pass
        # Flush the outbox with a sentinel so drain_task exits promptly.
        try:
            await conn["outbox"].put(None)
        except Exception:
            pass
        return 204, {}, b""

    return 405, {"Content-Type": "application/json"}, json.dumps(
        {"message": f"Method {method} not allowed on @connections"}
    ).encode()
