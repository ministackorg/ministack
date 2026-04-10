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

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, new_uuid

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


# ---- Response helpers ----

def _apigw_response(data: dict, status: int = 200) -> tuple:
    """API Gateway v2 uses application/json (not application/x-amz-json-1.0)."""
    return status, {"Content-Type": "application/json"}, json.dumps(data, ensure_ascii=False).encode("utf-8")


def _apigw_error(code: str, message: str, status: int) -> tuple:
    return status, {"Content-Type": "application/json"}, json.dumps({"message": message, "__type": code}, ensure_ascii=False).encode("utf-8")


def _api_arn(api_id: str) -> str:
    return f"arn:aws:apigateway:{REGION}::/apis/{api_id}"


# ---- Persistence hooks ----

def get_state() -> dict:
    """Return full module state for persistence."""
    return {
        "apis": _apis,
        "routes": _routes,
        "integrations": _integrations,
        "stages": _stages,
        "deployments": _deployments,
        "authorizers": _authorizers,
        "api_tags": _api_tags,
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


# ---- Control plane router ----

async def handle_request(method, path, headers, body, query_params):
    """Route API Gateway v2 control plane requests."""
    # Dispatch v1 REST API requests first
    parts = [p for p in path.strip("/").split("/") if p]
    if parts and parts[0] in ("restapis", "apikeys", "usageplans", "domainnames", "tags"):
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

        # /v2/apis/{apiId}/routes[/{routeId}]
        if api_id and sub == "routes":
            if not sub_id:
                if method == "POST":
                    return _create_route(api_id, data)
                if method == "GET":
                    return _get_routes(api_id)
            else:
                if method == "GET":
                    return _get_route(api_id, sub_id)
                if method == "PATCH":
                    return _update_route(api_id, sub_id, data)
                if method == "DELETE":
                    return _delete_route(api_id, sub_id)

        # /v2/apis/{apiId}/integrations[/{integrationId}]
        if api_id and sub == "integrations":
            if not sub_id:
                if method == "POST":
                    return _create_integration(api_id, data)
                if method == "GET":
                    return _get_integrations(api_id)
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

async def handle_execute(api_id, stage, path, method, headers, body, query_params):
    """Execute an API request through a deployed API (data plane)."""
    api = _apis.get(api_id)
    if not api:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "Not Found"}).encode()

    api_stages = _stages.get(api_id, {})
    if stage not in api_stages and stage != "$default":
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": f"Stage '{stage}' not found"}).encode()

    route = _match_route(api_id, method, path)
    if not route:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "No route found"}).encode()

    integration_id = route.get("target", "").replace("integrations/", "")
    integration = _integrations.get(api_id, {}).get(integration_id)
    if not integration:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": "No integration configured"}).encode()

    integration_type = integration.get("integrationType", "")

    if integration_type == "AWS_PROXY":
        return await _invoke_lambda_proxy(integration, api_id, stage, path, method, headers, body, query_params, route.get("routeKey", "$default"))
    elif integration_type == "HTTP_PROXY":
        return await _invoke_http_proxy(integration, path, method, headers, body, query_params)
    else:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": f"Unsupported integration type: {integration_type}"}).encode()


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


def _path_matches(route_path: str, request_path: str) -> bool:
    """
    Match a route path against a request path.

    Supports:
      {param}   — single path segment (no slashes)
      {proxy+}  — greedy match (one or more path segments, may include slashes)
    """
    # Build regex by splitting on placeholders
    parts = re.split(r"(\{[^}]+\})", route_path)
    pattern_parts = []
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            inner = part[1:-1]
            if inner.endswith("+"):
                # greedy — matches one or more characters including slashes
                pattern_parts.append(".+")
            else:
                # single segment — no slashes
                pattern_parts.append("[^/]+")
        else:
            pattern_parts.append(re.escape(part))
    return bool(re.fullmatch("".join(pattern_parts), request_path))


async def _invoke_lambda_proxy(integration, api_id, stage, path, method, headers, body, query_params, route_key="$default"):
    """Invoke a Lambda function using the API Gateway v2 proxy event format."""
    from ministack.core.lambda_runtime import get_or_create_worker
    from ministack.services import lambda_svc

    uri = integration.get("integrationUri", "")
    # integrationUri is a Lambda ARN; the function name is the last segment
    func_name = uri.split(":")[-1] if ":" in uri else uri
    # Strip /invocations suffix emitted by some SDKs
    func_name = func_name.replace("/invocations", "")

    if func_name not in lambda_svc._functions:
        return 502, {"Content-Type": "application/json"}, json.dumps({"message": f"Lambda function '{func_name}' not found"}).encode()

    # Build API Gateway v2 proxy event (payload format 2.0)
    qs = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
    event = {
        "version": "2.0",
        "routeKey": route_key,
        "rawPath": path,
        "rawQueryString": "&".join(f"{k}={v}" for k, v in query_params.items()),
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
        "body": body.decode("utf-8", errors="replace") if body else None,
        "isBase64Encoded": False,
    }

    func_data = lambda_svc._functions[func_name]
    code_zip = func_data.get("code_zip")

    runtime = func_data["config"].get("Runtime", "")
    if code_zip and runtime.startswith(("python", "nodejs")):
        worker = get_or_create_worker(func_name, func_data["config"], code_zip)
        result = await asyncio.to_thread(worker.invoke, event, new_uuid())
        if result.get("status") == "error":
            return 502, {"Content-Type": "application/json"}, json.dumps({"message": result.get("error")}).encode()
        lambda_response = result.get("result", {})
    else:
        lambda_response = {"statusCode": 200, "body": "Mock response"}

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

def _create_api(data):
    api_id = new_uuid()[:8]
    api = {
        "apiId": api_id,
        "name": data.get("name", "unnamed"),
        "protocolType": data.get("protocolType", "HTTP"),
        "apiEndpoint": f"http://{api_id}.execute-api.{_HOST}:{_PORT}",
        "createdDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "routeSelectionExpression": data.get("routeSelectionExpression", "$request.method $request.path"),
        "tags": data.get("tags", {}),
        "corsConfiguration": data.get("corsConfiguration", {}),
        "disableSchemaValidation": data.get("disableSchemaValidation", False),
        "disableExecuteApiEndpoint": data.get("disableExecuteApiEndpoint", False),
        "version": data.get("version", ""),
    }
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
    return _apigw_response({"items": list(_apis.values()), "nextToken": None})


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
    return _apigw_response({"items": list(_routes.get(api_id, {}).values()), "nextToken": None})


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
    }
    _integrations.setdefault(api_id, {})[int_id] = integration
    return _apigw_response(integration, 201)


def _get_integrations(api_id):
    return _apigw_response({"items": list(_integrations.get(api_id, {}).values()), "nextToken": None})


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
              "description", "requestParameters", "requestTemplates", "responseParameters"):
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
    return _apigw_response({"items": list(_stages.get(api_id, {}).values()), "nextToken": None})


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
    return _apigw_response({"items": list(_deployments.get(api_id, {}).values()), "nextToken": None})


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
    return _apigw_response({"items": list(_authorizers.get(api_id, {}).values()), "nextToken": None})


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
