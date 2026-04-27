"""
API Gateway REST API v1 Emulator.

Control plane endpoints implemented:
  POST   /restapis                                                         — CreateRestApi
  GET    /restapis                                                         — GetRestApis
  GET    /restapis/{id}                                                    — GetRestApi
  PATCH  /restapis/{id}                                                    — UpdateRestApi
  DELETE /restapis/{id}                                                    — DeleteRestApi
  GET    /restapis/{id}/resources                                          — GetResources
  GET    /restapis/{id}/resources/{resourceId}                             — GetResource
  POST   /restapis/{id}/resources/{parentId}                               — CreateResource
  PATCH  /restapis/{id}/resources/{resourceId}                             — UpdateResource
  DELETE /restapis/{id}/resources/{resourceId}                             — DeleteResource
  PUT    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}        — PutMethod
  GET    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}        — GetMethod
  DELETE /restapis/{id}/resources/{resourceId}/methods/{httpMethod}        — DeleteMethod
  PUT    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/responses/{code}       — PutMethodResponse
  GET    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/responses/{code}       — GetMethodResponse
  DELETE /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/responses/{code}       — DeleteMethodResponse
  PUT    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration            — PutIntegration
  GET    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration            — GetIntegration
  DELETE /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration            — DeleteIntegration
  PUT    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration/responses/{code} — PutIntegrationResponse
  GET    /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration/responses/{code} — GetIntegrationResponse
  DELETE /restapis/{id}/resources/{resourceId}/methods/{httpMethod}/integration/responses/{code} — DeleteIntegrationResponse
  POST   /restapis/{id}/deployments                                        — CreateDeployment
  GET    /restapis/{id}/deployments                                        — GetDeployments
  GET    /restapis/{id}/deployments/{deploymentId}                         — GetDeployment
  PATCH  /restapis/{id}/deployments/{deploymentId}                         — UpdateDeployment
  DELETE /restapis/{id}/deployments/{deploymentId}                         — DeleteDeployment
  POST   /restapis/{id}/stages                                             — CreateStage
  GET    /restapis/{id}/stages                                             — GetStages
  GET    /restapis/{id}/stages/{stageName}                                 — GetStage
  PATCH  /restapis/{id}/stages/{stageName}                                 — UpdateStage
  DELETE /restapis/{id}/stages/{stageName}                                 — DeleteStage
  POST   /restapis/{id}/authorizers                                        — CreateAuthorizer
  GET    /restapis/{id}/authorizers                                        — GetAuthorizers
  GET    /restapis/{id}/authorizers/{authorizerId}                         — GetAuthorizer
  PATCH  /restapis/{id}/authorizers/{authorizerId}                         — UpdateAuthorizer
  DELETE /restapis/{id}/authorizers/{authorizerId}                         — DeleteAuthorizer
  POST   /restapis/{id}/models                                             — CreateModel
  GET    /restapis/{id}/models                                             — GetModels
  GET    /restapis/{id}/models/{modelName}                                 — GetModel
  DELETE /restapis/{id}/models/{modelName}                                 — DeleteModel
  GET    /apikeys                                                          — GetApiKeys
  POST   /apikeys                                                          — CreateApiKey
  GET    /apikeys/{keyId}                                                  — GetApiKey
  DELETE /apikeys/{keyId}                                                  — DeleteApiKey
  GET    /usageplans                                                       — GetUsagePlans
  POST   /usageplans                                                       — CreateUsagePlan
  GET    /usageplans/{planId}                                              — GetUsagePlan
  DELETE /usageplans/{planId}                                              — DeleteUsagePlan
  GET    /usageplans/{planId}/keys                                         — GetUsagePlanKeys
  POST   /usageplans/{planId}/keys                                         — CreateUsagePlanKey
  DELETE /usageplans/{planId}/keys/{keyId}                                 — DeleteUsagePlanKey
  GET    /domainnames                                                      — GetDomainNames
  POST   /domainnames                                                      — CreateDomainName
  GET    /domainnames/{domainName}                                         — GetDomainName
  DELETE /domainnames/{domainName}                                         — DeleteDomainName
  GET    /tags/{resourceArn}                                               — GetTags
  PUT    /tags/{resourceArn}                                               — TagResource
  DELETE /tags/{resourceArn}                                               — UntagResource

Data plane:
  Requests to /{apiId}.execute-api.localhost/{stage}/{path} are dispatched
  when api_id is found in _rest_apis.
"""

import asyncio
import datetime
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request

from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid, get_region


def _now_unix():
    """Return current UTC time as Unix timestamp (float).
    API Gateway v1 createdDate/lastUpdatedDate fields must be numbers, not strings.
    Terraform's AWS provider deserializes them as JSON Number and errors on ISO strings."""
    return int(time.time())

logger = logging.getLogger("apigateway_v1")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---- Module-level state ----
# All per-tenant state uses AccountScopedDict so the same REST API id in two
# different accounts never collides and list operations don't leak cross-account.
_rest_apis = AccountScopedDict()           # rest_api_id -> RestApi
_resources = AccountScopedDict()           # rest_api_id -> {resource_id -> Resource}
_stages_v1 = AccountScopedDict()           # rest_api_id -> {stage_name -> Stage}
_deployments_v1 = AccountScopedDict()      # rest_api_id -> {deployment_id -> Deployment}
_authorizers_v1 = AccountScopedDict()      # rest_api_id -> {authorizer_id -> Authorizer}
_models = AccountScopedDict()              # rest_api_id -> {model_id -> Model}
_api_keys = AccountScopedDict()            # key_id -> ApiKey
_usage_plans = AccountScopedDict()         # plan_id -> UsagePlan
_usage_plan_keys = AccountScopedDict()     # plan_id -> {key_id -> UsagePlanKey}
_domain_names = AccountScopedDict()        # domain_name -> DomainName
_base_path_mappings = AccountScopedDict()  # domain_name -> {base_path -> BasePathMapping}
_v1_tags = AccountScopedDict()             # resource_arn -> {key -> value}
_account_settings = AccountScopedDict()    # singleton per account: stores fields set via UpdateAccount


# ---- Helpers ----

def _new_id():
    """Return a 10-char hex id."""
    return new_uuid().replace("-", "")[:10]


def _v1_response(data, status=200):
    """API Gateway v1 uses application/json."""
    return status, {"Content-Type": "application/json"}, json.dumps(data, ensure_ascii=False).encode("utf-8")


def _encode_rest_api_policy(policy):
    """Match AWS's wire shape for the RestApi.policy field.

    terraform-provider-aws's ``flattenAPIPolicy`` wraps the SDK-decoded policy
    string in outer quotes and re-parses it as JSON
    (``NormalizeJsonString(`"` + policy + `"`)`` then ``strconv.Unquote``).
    For that roundtrip to work, AWS returns the policy already JSON-string
    escape-encoded — e.g. ``{\\"Statement\\":[...]}`` — so the provider's
    wrap-and-reparse recovers the original policy JSON. Emitting the raw
    policy string (what ministack used to do) makes the provider's decoder
    error with ``invalid character 'S' after top-level value`` as soon as the
    policy contains an inner quote.
    """
    if policy is None or policy == "":
        return policy
    if not isinstance(policy, str):
        policy = json.dumps(policy, ensure_ascii=False)
    # json.dumps("abc") -> '"abc"'; strip the outer quotes to get the
    # escape-sequence form the provider expects to see in *Policy.
    return json.dumps(policy, ensure_ascii=False)[1:-1]


def _rest_api_view(api):
    """Return a response-shaped copy with the policy field properly encoded."""
    if api is None:
        return api
    view = dict(api)
    if "policy" in view:
        view["policy"] = _encode_rest_api_policy(view["policy"])
    return view


def _v1_error(code, message, status):
    # AWS API Gateway errors use __type (double underscore), matching every
    # other JSON-protocol AWS service. boto3 reads this to populate
    # ``ClientError.response["Error"]["Code"]``; with plain "type" it falls
    # back to the numeric HTTP status as the code.
    return status, {"Content-Type": "application/json"}, json.dumps({"message": message, "__type": code}, ensure_ascii=False).encode("utf-8")


def _rest_api_arn(api_id):
    return f"arn:aws:apigateway:{get_region()}::/restapis/{api_id}"


def _compute_path(api_id, resource_id):
    """Walk the parent chain to build the full resource path."""
    resources = _resources.get(api_id, {})
    parts = []
    rid = resource_id
    while rid:
        r = resources.get(rid)
        if not r:
            break
        pp = r.get("pathPart", "")
        if pp:
            parts.append(pp)
        rid = r.get("parentId")
    if not parts:
        return "/"
    parts.reverse()
    return "/" + "/".join(parts)


def _apply_patch(obj, patch_ops):
    """Apply JSON Patch operations (replace/add/remove) to a dict in place."""
    for op in patch_ops:
        operation = op.get("op", "replace")
        path = op.get("path", "")
        value = op.get("value")

        # Strip leading slash and split
        keys = path.lstrip("/").split("/")
        if not keys or keys == [""]:
            continue

        if operation in ("replace", "add"):
            if len(keys) == 1:
                obj[keys[0]] = value
            else:
                # Walk into nested dicts, create if needed
                target = obj
                for k in keys[:-1]:
                    if k not in target or not isinstance(target[k], dict):
                        target[k] = {}
                    target = target[k]
                target[keys[-1]] = value
        elif operation == "remove":
            if len(keys) == 1:
                obj.pop(keys[0], None)
            else:
                target = obj
                for k in keys[:-1]:
                    if not isinstance(target.get(k), dict):
                        break
                    target = target[k]
                else:
                    target.pop(keys[-1], None)
    return obj


# UpdateStage patch paths for per-method settings use
# ``/{resourcePath}/{httpMethod}/metrics/enabled`` (JSON Pointer ``~1`` for ``/``
# in ``resourcePath``), not ``/methodSettings/...``. ``_apply_patch`` would split
# ``/*/*/metrics/enabled`` into nested keys under the stage root; we map these
# into ``stage["methodSettings"]["*/*"]`` etc. instead (Terraform
# ``aws_api_gateway_method_settings``).

_STAGE_ROOT_PATH_PREFIXES = frozenset(
    {
        "variables",
        "deploymentId",
        "description",
        "cacheClusterEnabled",
        "cacheClusterSize",
        "tracingEnabled",
        "documentationVersion",
        "accessLogSettings",
        "clientCertificateId",
        "methodSettings",
        "canarySettings",
    }
)

_HTTP_METHOD_TOKENS = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "ANY", "*"}
)

_METHOD_SETTING_CATEGORIES = frozenset({"metrics", "logging", "throttling", "caching"})


def _decode_json_pointer_token(segment):
    """RFC 6901 token decode (~1 -> /, ~0 -> ~)."""
    return segment.replace("~1", "/").replace("~0", "~")


def _default_method_setting_entry():
    """Wire-shaped defaults similar to AWS GetStage for a method setting block."""
    return {
        "metricsEnabled": False,
        "loggingLevel": "OFF",
        "dataTraceEnabled": False,
        "throttlingBurstLimit": 5000,
        "throttlingRateLimit": 10000.0,
        "cachingEnabled": False,
        "cacheTtlInSeconds": 300,
        "cacheDataEncrypted": False,
        "requireAuthorizationForCacheControl": True,
        "unauthorizedCacheControlHeaderStrategy": "SUCCEED_WITH_RESPONSE_HEADER",
    }


def _parse_stage_method_setting_value(field, value_str):
    if value_str is None:
        return None
    if field in (
        "metricsEnabled",
        "dataTraceEnabled",
        "cachingEnabled",
        "cacheDataEncrypted",
        "requireAuthorizationForCacheControl",
    ):
        return str(value_str).lower() == "true"
    if field in ("throttlingBurstLimit", "cacheTtlInSeconds"):
        return int(value_str)
    if field == "throttlingRateLimit":
        return float(value_str)
    return str(value_str)


def _method_setting_field_from_patch(rel_tokens):
    """Map tokens after ``{resourcePath}/{httpMethod}/`` to the methodSettings field name."""
    if len(rel_tokens) < 2:
        return None
    cat, rest0 = rel_tokens[0], rel_tokens[1]
    if cat == "metrics" and rest0 == "enabled":
        return "metricsEnabled"
    if cat == "logging" and rest0 == "loglevel":
        return "loggingLevel"
    if cat == "logging" and rest0 == "dataTrace":
        return "dataTraceEnabled"
    if cat == "throttling" and rest0 == "burstLimit":
        return "throttlingBurstLimit"
    if cat == "throttling" and rest0 == "rateLimit":
        return "throttlingRateLimit"
    if cat == "caching" and rest0 == "enabled":
        return "cachingEnabled"
    if cat == "caching" and rest0 == "ttlInSeconds":
        return "cacheTtlInSeconds"
    if cat == "caching" and rest0 == "dataEncrypted":
        return "cacheDataEncrypted"
    if cat == "caching" and rest0 == "requireAuthorizationForCacheControl":
        return "requireAuthorizationForCacheControl"
    if cat == "caching" and rest0 == "unauthorizedCacheControlHeaderStrategy":
        return "unauthorizedCacheControlHeaderStrategy"
    return None


def _try_apply_method_settings_patch(stage, op):
    """Handle UpdateStage patches documented under ``/{resourcePath}/{httpMethod}/...``."""
    path = (op.get("path") or "").strip()
    if not path.startswith("/"):
        return False
    raw = path[1:]
    if not raw:
        return False

    tokens = [_decode_json_pointer_token(p) for p in raw.split("/")]
    operation = (op.get("op") or "replace").lower()
    value = op.get("value")

    # Remove entire method setting: ``/{resourcePath}/{httpMethod}`` (Terraform delete).
    if operation == "remove" and len(tokens) == 2:
        if tokens[0] in _STAGE_ROOT_PATH_PREFIXES:
            return False
        if tokens[1] not in _HTTP_METHOD_TOKENS:
            return False
        key = f"{tokens[0]}/{tokens[1]}"
        stage.setdefault("methodSettings", {}).pop(key, None)
        return True

    cat_idx = None
    for idx, tok in enumerate(tokens):
        if tok in _METHOD_SETTING_CATEGORIES:
            cat_idx = idx
            break

    if cat_idx is None or cat_idx < 1:
        return False
    if tokens[0] in _STAGE_ROOT_PATH_PREFIXES:
        return False

    http_method = tokens[cat_idx - 1]
    resource_path = "/".join(tokens[: cat_idx - 1])
    setting_key = f"{resource_path}/{http_method}"
    rel = tokens[cat_idx:]

    field_name = _method_setting_field_from_patch(rel)
    if field_name is None:
        return False
    ms = stage.setdefault("methodSettings", {})
    if operation == "remove":
        entry = ms.get(setting_key)
        if isinstance(entry, dict):
            entry.pop(field_name, None)
        return True

    if operation not in ("replace", "add"):
        return False

    entry = ms.setdefault(setting_key, {})
    if len(entry) == 0:
        entry.update(_default_method_setting_entry())
    entry[field_name] = _parse_stage_method_setting_value(field_name, value)
    return True


def _apply_stage_patch(stage, patch_ops):
    """Apply UpdateStage patch operations (method settings + generic JSON patch)."""
    leftover = []
    for op in patch_ops:
        if not _try_apply_method_settings_patch(stage, op):
            leftover.append(op)
    if leftover:
        _apply_patch(stage, leftover)


def _match_resource_tree(api_id, segments):
    """Match path segments against the resource tree. Returns (resource, path_params) or (None, {})."""
    resources = _resources.get(api_id, {})
    root = next((r for r in resources.values() if r.get("path") == "/"), None)
    if not root:
        return None, {}
    if not segments or segments == [""]:
        return root, {}
    return _match_recursive(resources, root["id"], segments, {})


def _match_recursive(resources, parent_id, segments, params):
    if not segments:
        return None, params
    segment = segments[0]
    remaining = segments[1:]
    children = [r for r in resources.values() if r.get("parentId") == parent_id]
    for child in children:
        pp = child.get("pathPart", "")
        if pp.endswith("+}") and pp.startswith("{"):
            # greedy {proxy+}
            param_name = pp[1:-2]
            new_params = dict(params)
            new_params[param_name] = "/".join([segment] + list(remaining))
            return child, new_params
        elif pp.startswith("{") and pp.endswith("}"):
            param_name = pp[1:-1]
            new_params = dict(params)
            new_params[param_name] = segment
            if not remaining:
                return child, new_params
            result, rp = _match_recursive(resources, child["id"], list(remaining), new_params)
            if result:
                return result, rp
        elif pp == segment:
            if not remaining:
                return child, params
            result, rp = _match_recursive(resources, child["id"], list(remaining), dict(params))
            if result:
                return result, rp
    return None, params


async def _call_lambda(func_name, event, qualifier=None):
    """Invoke a Lambda function and return the parsed response dict.

    ``qualifier`` may be a version number or alias name; aliases resolve to
    their target version via ``_get_func_record_for_qualifier`` so aliased
    integration URIs (arn:...:function:<name>:<alias>) invoke correctly (#407)."""
    from ministack.services import lambda_svc

    func_data, func_config = lambda_svc._get_func_record_for_qualifier(func_name, qualifier)
    if func_data is None:
        label = f"{func_name}:{qualifier}" if qualifier else func_name
        return None, f"Lambda function '{label}' not found"

    # Route through the central _execute_function dispatcher so CloudWatch
    # Logs emission and Docker log output work for API Gateway invocations.
    # Response shaping (throttle→429, error→502, body→envelope) goes through
    # the shared helper so v1/v2 stay consistent.
    exec_record = {"config": func_config, "code_zip": func_data.get("code_zip")}
    result = await asyncio.to_thread(lambda_svc._execute_function, exec_record, event)
    lambda_response, _ = lambda_svc.lambda_execute_result_to_api_proxy_response(result)
    # On error the helper returns {statusCode: 502, body: <msg>}; preserve
    # the _call_lambda contract of (None, error_msg) so callers that check
    # for error strings keep working.
    if result.get("error") and lambda_response and lambda_response.get("statusCode") == 502:
        return None, str(lambda_response.get("body") or "Lambda invocation error")
    return lambda_response, None


# ---- Persistence hooks ----

def get_state():
    """Return full module state for persistence."""
    return {
        "rest_apis": _rest_apis,
        "resources": _resources,
        "stages_v1": _stages_v1,
        "deployments_v1": _deployments_v1,
        "authorizers_v1": _authorizers_v1,
        "models": _models,
        "api_keys": _api_keys,
        "usage_plans": _usage_plans,
        "usage_plan_keys": _usage_plan_keys,
        "domain_names": _domain_names,
        "base_path_mappings": _base_path_mappings,
        "v1_tags": _v1_tags,
        "account_settings": _account_settings,
    }


def load_persisted_state(data):
    """Restore module state from a previously persisted snapshot."""
    _rest_apis.update(data.get("rest_apis", {}))
    _resources.update(data.get("resources", {}))
    _stages_v1.update(data.get("stages_v1", {}))
    _deployments_v1.update(data.get("deployments_v1", {}))
    _authorizers_v1.update(data.get("authorizers_v1", {}))
    _models.update(data.get("models", {}))
    _api_keys.update(data.get("api_keys", {}))
    _usage_plans.update(data.get("usage_plans", {}))
    _usage_plan_keys.update(data.get("usage_plan_keys", {}))
    _domain_names.update(data.get("domain_names", {}))
    _base_path_mappings.update(data.get("base_path_mappings", {}))
    _v1_tags.update(data.get("v1_tags", {}))
    _account_settings.update(data.get("account_settings", {}))


def reset():
    """Clear all module state."""
    _rest_apis.clear()
    _resources.clear()
    _stages_v1.clear()
    _deployments_v1.clear()
    _authorizers_v1.clear()
    _models.clear()
    _api_keys.clear()
    _usage_plans.clear()
    _usage_plan_keys.clear()
    _domain_names.clear()
    _base_path_mappings.clear()
    _v1_tags.clear()
    _account_settings.clear()


# ---- Control plane router ----

async def handle_request(method, path, headers, body, query_params):
    """Route API Gateway v1 REST API control plane requests."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    parts = [p for p in path.strip("/").split("/") if p]

    if not parts:
        return _v1_error("NotFoundException", f"Unknown path: {path}", 404)

    top = parts[0]

    if top == "account":
        if method == "GET":
            return _get_account()
        if method == "PATCH":
            return _update_account(data)
        return _v1_error("MethodNotAllowedException", f"Method not allowed: {method} /account", 405)

    if top == "tags":
        # /tags/{resourceArn} — ARN may contain slashes
        resource_arn = "/".join(parts[1:]) if len(parts) > 1 else ""
        if method == "GET":
            return _get_v1_tags(resource_arn)
        if method in ("PUT", "POST"):
            return _tag_v1_resource(resource_arn, data)
        if method == "DELETE":
            tag_keys = query_params.get("tagKeys", [])
            if isinstance(tag_keys, str):
                tag_keys = [tag_keys]
            return _untag_v1_resource(resource_arn, tag_keys)

    if top == "apikeys":
        key_id = parts[1] if len(parts) > 1 else None
        if not key_id:
            if method == "GET":
                return _get_api_keys()
            if method == "POST":
                return _create_api_key(data)
        else:
            if method == "GET":
                return _get_api_key(key_id)
            if method == "DELETE":
                return _delete_api_key(key_id)
            if method == "PATCH":
                return _update_api_key(key_id, data)

    if top == "usageplans":
        plan_id = parts[1] if len(parts) > 1 else None
        sub = parts[2] if len(parts) > 2 else None
        sub_id = parts[3] if len(parts) > 3 else None
        if not plan_id:
            if method == "GET":
                return _get_usage_plans()
            if method == "POST":
                return _create_usage_plan(data)
        elif sub == "keys":
            if not sub_id:
                if method == "GET":
                    return _get_usage_plan_keys(plan_id)
                if method == "POST":
                    return _create_usage_plan_key(plan_id, data)
            else:
                if method == "DELETE":
                    return _delete_usage_plan_key(plan_id, sub_id)
        else:
            if method == "GET":
                return _get_usage_plan(plan_id)
            if method == "DELETE":
                return _delete_usage_plan(plan_id)
            if method == "PATCH":
                return _update_usage_plan(plan_id, data)

    if top == "domainnames":
        domain_name = parts[1] if len(parts) > 1 else None
        sub = parts[2] if len(parts) > 2 else None
        sub_id = parts[3] if len(parts) > 3 else None
        if not domain_name:
            if method == "GET":
                return _get_domain_names()
            if method == "POST":
                return _create_domain_name(data)
        elif sub == "basepathmappings":
            base_path = sub_id
            if not base_path:
                if method == "GET":
                    return _get_base_path_mappings(domain_name)
                if method == "POST":
                    return _create_base_path_mapping(domain_name, data)
            else:
                if method == "GET":
                    return _get_base_path_mapping(domain_name, base_path)
                if method == "DELETE":
                    return _delete_base_path_mapping(domain_name, base_path)
        else:
            if method == "GET":
                return _get_domain_name(domain_name)
            if method == "DELETE":
                return _delete_domain_name(domain_name)

    if top == "restapis":
        # /restapis
        if len(parts) == 1:
            if method == "POST":
                return _create_rest_api(data)
            if method == "GET":
                return _get_rest_apis()

        api_id = parts[1]

        # /restapis/{id}
        if len(parts) == 2:
            if method == "GET":
                return _get_rest_api(api_id)
            if method == "DELETE":
                return _delete_rest_api(api_id)
            if method == "PATCH":
                return _update_rest_api(api_id, data)

        sub = parts[2] if len(parts) > 2 else None

        # /restapis/{id}/resources[/{resourceId}[/...]]
        if sub == "resources":
            resource_id = parts[3] if len(parts) > 3 else None
            method_part = parts[4] if len(parts) > 4 else None
            http_method = parts[5] if len(parts) > 5 else None
            after_method = parts[6] if len(parts) > 6 else None
            after_method_id = parts[7] if len(parts) > 7 else None

            if not resource_id:
                # GET /restapis/{id}/resources
                if method == "GET":
                    return _get_resources(api_id)

            elif method_part is None:
                # /restapis/{id}/resources/{resourceId}
                if method == "GET":
                    return _get_resource(api_id, resource_id)
                if method == "POST":
                    # CreateResource: POST /restapis/{id}/resources/{parentId}
                    return _create_resource(api_id, resource_id, data)
                if method == "PATCH":
                    return _update_resource(api_id, resource_id, data)
                if method == "DELETE":
                    return _delete_resource(api_id, resource_id)

            elif method_part == "methods":
                if http_method is None:
                    return _v1_error("NotFoundException", "Method not specified", 404)

                if after_method is None:
                    # /restapis/{id}/resources/{resourceId}/methods/{httpMethod}
                    if method == "PUT":
                        return _put_method(api_id, resource_id, http_method, data)
                    if method == "GET":
                        return _get_method(api_id, resource_id, http_method)
                    if method == "DELETE":
                        return _delete_method(api_id, resource_id, http_method)
                    if method == "PATCH":
                        return _update_method(api_id, resource_id, http_method, data)

                elif after_method == "responses":
                    status_code = after_method_id
                    if not status_code:
                        return _v1_error("NotFoundException", "Status code not specified", 404)
                    if method == "PUT":
                        return _put_method_response(api_id, resource_id, http_method, status_code, data)
                    if method == "GET":
                        return _get_method_response(api_id, resource_id, http_method, status_code)
                    if method == "DELETE":
                        return _delete_method_response(api_id, resource_id, http_method, status_code)

                elif after_method == "integration":
                    # Check for integration/responses/{statusCode}
                    int_sub = parts[7] if len(parts) > 7 else None
                    int_sub_id = parts[8] if len(parts) > 8 else None

                    if after_method_id is None and int_sub is None:
                        # /.../{httpMethod}/integration
                        if method == "PUT":
                            return _put_integration(api_id, resource_id, http_method, data)
                        if method == "GET":
                            return _get_integration(api_id, resource_id, http_method)
                        if method == "DELETE":
                            return _delete_integration(api_id, resource_id, http_method)
                        if method == "PATCH":
                            return _update_integration(api_id, resource_id, http_method, data)
                    elif after_method_id == "responses":
                        status_code = int_sub_id
                        if not status_code:
                            return _v1_error("NotFoundException", "Status code not specified", 404)
                        if method == "PUT":
                            return _put_integration_response(api_id, resource_id, http_method, status_code, data)
                        if method == "GET":
                            return _get_integration_response(api_id, resource_id, http_method, status_code)
                        if method == "DELETE":
                            return _delete_integration_response(api_id, resource_id, http_method, status_code)

        # /restapis/{id}/deployments[/{deploymentId}]
        elif sub == "deployments":
            deployment_id = parts[3] if len(parts) > 3 else None
            if not deployment_id:
                if method == "POST":
                    return _create_deployment(api_id, data)
                if method == "GET":
                    return _get_deployments(api_id)
            else:
                if method == "GET":
                    return _get_deployment(api_id, deployment_id)
                if method == "PATCH":
                    return _update_deployment(api_id, deployment_id, data)
                if method == "DELETE":
                    return _delete_deployment(api_id, deployment_id)

        # /restapis/{id}/stages[/{stageName}]
        elif sub == "stages":
            stage_name = parts[3] if len(parts) > 3 else None
            if not stage_name:
                if method == "POST":
                    return _create_stage(api_id, data)
                if method == "GET":
                    return _get_stages(api_id)
            else:
                if method == "GET":
                    return _get_stage(api_id, stage_name)
                if method == "PATCH":
                    return _update_stage(api_id, stage_name, data)
                if method == "DELETE":
                    return _delete_stage(api_id, stage_name)

        # /restapis/{id}/authorizers[/{authorizerId}]
        elif sub == "authorizers":
            auth_id = parts[3] if len(parts) > 3 else None
            if not auth_id:
                if method == "POST":
                    return _create_authorizer(api_id, data)
                if method == "GET":
                    return _get_authorizers(api_id)
            else:
                if method == "GET":
                    return _get_authorizer(api_id, auth_id)
                if method == "PATCH":
                    return _update_authorizer(api_id, auth_id, data)
                if method == "DELETE":
                    return _delete_authorizer(api_id, auth_id)

        # /restapis/{id}/models[/{modelName}]
        elif sub == "models":
            model_name = parts[3] if len(parts) > 3 else None
            if not model_name:
                if method == "POST":
                    return _create_model(api_id, data)
                if method == "GET":
                    return _get_models(api_id)
            else:
                if method == "GET":
                    return _get_model(api_id, model_name)
                if method == "DELETE":
                    return _delete_model(api_id, model_name)

    return _v1_error("NotFoundException", f"Unknown API Gateway v1 path: {path}", 404)


# ---- Data plane ----

async def handle_execute(api_id, stage_name, method, path, headers, body, query_params):
    """Execute a v1 REST API request through a deployed stage (data plane)."""
    api = _rest_apis.get(api_id)
    if not api:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "Not Found"}).encode()

    stage = _stages_v1.get(api_id, {}).get(stage_name)
    if not stage:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": f"Stage '{stage_name}' not found"}).encode()

    # Match path against resource tree
    segments = [s for s in path.strip("/").split("/") if s]
    resource, path_params = _match_resource_tree(api_id, segments)

    if not resource:
        return 404, {"Content-Type": "application/json"}, json.dumps({"message": "Missing Authentication Token"}).encode()

    # Look up method
    resource_methods = resource.get("resourceMethods", {})
    method_obj = resource_methods.get(method) or resource_methods.get("ANY")
    if not method_obj:
        return 405, {"Content-Type": "application/json"}, json.dumps({"message": "Method Not Allowed"}).encode()

    integration = method_obj.get("methodIntegration")
    if not integration:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": "No integration configured"}).encode()

    int_type = integration.get("type", "")

    if int_type in ("AWS_PROXY", "AWS"):
        return await _invoke_lambda_proxy_v1(
            integration, api_id, stage_name, stage, resource, path, method,
            headers, body, query_params, path_params
        )
    elif int_type in ("HTTP_PROXY", "HTTP"):
        return await _invoke_http_proxy_v1(integration, path, method, headers, body, query_params)
    elif int_type == "MOCK":
        return _invoke_mock_v1(integration)
    else:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": f"Unsupported integration type: {int_type}"}).encode()


async def _invoke_lambda_proxy_v1(integration, api_id, stage_name, stage, resource, request_path, method, headers, body, query_params, path_params):
    """Invoke Lambda with API Gateway v1 payload format 1.0."""
    uri = integration.get("uri", "")
    # Supported URI formats:
    #   1. arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/arn:aws:lambda:{region}:{acct}:function:{name}[:{qualifier}]/invocations
    #   2. arn:aws:lambda:{region}:{acct}:function:{name}[:{qualifier}]
    #   3. plain function name: MyFunction[:{qualifier}]
    from ministack.services import lambda_svc as _lambda_svc
    if "function:" in uri:
        # Strip wrapper up through 'function:' and any trailing /invocations.
        tail = uri.split("function:")[-1].split("/")[0]
        # tail is now "<name>" or "<name>:<qualifier>".
        func_name, qualifier = _lambda_svc._resolve_name_and_qualifier(tail)
    else:
        func_name, qualifier = _lambda_svc._resolve_name_and_qualifier(uri)

    qs_params = {k: v[0] for k, v in query_params.items()} if query_params else None
    mv_qs_params = {k: list(v) for k, v in query_params.items()} if query_params else None

    # Build single and multi-value header dicts
    single_headers = {k: v if isinstance(v, str) else v[-1] for k, v in headers.items()}
    multi_headers = {k: [v] if isinstance(v, str) else list(v) for k, v in headers.items()}

    now_epoch_ms = int(time.time() * 1000)
    request_time = datetime.datetime.utcnow().strftime("%d/%b/%Y:%H:%M:%S +0000")
    request_id = new_uuid()

    event = {
        "version": "1.0",
        "resource": resource["path"],
        "path": request_path,
        "httpMethod": method,
        "headers": single_headers,
        "multiValueHeaders": multi_headers,
        "queryStringParameters": qs_params or None,
        "multiValueQueryStringParameters": mv_qs_params or None,
        "pathParameters": path_params or None,
        "stageVariables": stage.get("variables") or None,
        "requestContext": {
            "accountId": get_account_id(),
            "resourceId": resource["id"],
            "stage": stage_name,
            "requestId": request_id,
            "extendedRequestId": request_id,
            "requestTime": request_time,
            "requestTimeEpoch": now_epoch_ms,
            "path": f"/{stage_name}{request_path}",
            "protocol": "HTTP/1.1",
            "identity": {
                "sourceIp": headers.get("x-forwarded-for", "127.0.0.1").split(",")[0].strip()
                if isinstance(headers.get("x-forwarded-for", ""), str)
                else "127.0.0.1",
                "userAgent": headers.get("user-agent", ""),
            },
            "resourcePath": resource["path"],
            "httpMethod": method,
            "apiId": api_id,
        },
        "body": body.decode("utf-8", errors="replace") if body else None,
        "isBase64Encoded": False,
    }

    lambda_response, err = await _call_lambda(func_name, event, qualifier=qualifier)
    if err:
        return 502, {"Content-Type": "application/json"}, json.dumps({"message": err}).encode()

    status = lambda_response.get("statusCode", 200)
    resp_headers = {"Content-Type": "application/json"}
    resp_headers.update(lambda_response.get("headers", {}))
    resp_body = lambda_response.get("body", "")
    if isinstance(resp_body, str):
        resp_body = resp_body.encode("utf-8")
    elif isinstance(resp_body, dict):
        resp_body = json.dumps(resp_body, ensure_ascii=False).encode("utf-8")

    return status, resp_headers, resp_body


async def _invoke_http_proxy_v1(integration, path, method, headers, body, query_params):
    """Forward a request to an HTTP backend."""
    uri = integration.get("uri", "")
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


def _invoke_mock_v1(integration):
    """Return a MOCK integration response.

    Selection: iterate integrationResponses in status-code order; the first
    entry whose selectionPattern is empty (default) or matches "200" is used,
    matching AWS behaviour for MOCK where the input is always treated as
    successful (statusCode 200).
    """
    int_responses = integration.get("integrationResponses", {})
    if not int_responses:
        return 200, {"Content-Type": "application/json"}, b"{}"

    # AWS selects the response whose selectionPattern matches the integration
    # status code.  For MOCK the "status" is always 200 (success path).
    selected = None
    # Prefer an explicit "200" entry first
    if "200" in int_responses:
        selected = int_responses["200"]
    else:
        # Fall back to the entry with an empty / catch-all selectionPattern
        for resp in int_responses.values():
            pattern = resp.get("selectionPattern", "")
            if not pattern:
                selected = resp
                break
        if not selected:
            selected = next(iter(int_responses.values()))

    status = int(selected.get("statusCode", 200))
    resp_headers = {"Content-Type": "application/json"}

    # Apply responseParameters: map integration values to method response headers
    for dest, src in selected.get("responseParameters", {}).items():
        # dest: "method.response.header.X-Custom-Header"
        if dest.startswith("method.response.header."):
            header_name = dest[len("method.response.header."):]
            # src is a static string value (quoted) or integration reference
            value = src.strip("'") if src.startswith("'") else src
            resp_headers[header_name] = value

    body_template = selected.get("responseTemplates", {}).get("application/json", "")
    if body_template:
        return status, resp_headers, body_template.encode()
    return status, resp_headers, b"{}"


# ---- Control plane: REST APIs ----

def _resolve_custom_rest_api_id(tags: dict) -> tuple[str | None, tuple | None]:
    """Return (api_id_or_None, error_response_or_None).

    Reads the ministack-native ``ms-custom-id`` tag (issue #400). If the
    LocalStack ``ls-custom-id`` tag is set (and ``ms-custom-id`` is not), the
    caller gets a clear ``BadRequestException`` so the ministack-native key is
    the only supported contract."""
    if not isinstance(tags, dict):
        return None, None
    if "ls-custom-id" in tags and "ms-custom-id" not in tags:
        return None, _v1_error(
            "BadRequestException",
            "ls-custom-id tag is not supported; use 'ms-custom-id' instead",
            400,
        )
    custom = tags.get("ms-custom-id")
    if not custom:
        return None, None
    if custom in _rest_apis:
        return None, _v1_error(
            "ConflictException",
            f"REST API id '{custom}' (from ms-custom-id tag) is already in use",
            409,
        )
    return str(custom), None


def _create_rest_api(data):
    tags = data.get("tags", {})
    custom_id, err = _resolve_custom_rest_api_id(tags)
    if err is not None:
        return err
    api_id = custom_id or _new_id()[:8]
    api = {
        "id": api_id,
        "name": data.get("name", "unnamed"),
        "description": data.get("description", ""),
        "createdDate": _now_unix(),
        "version": data.get("version", ""),
        "binaryMediaTypes": data.get("binaryMediaTypes", []),
        "minimumCompressionSize": data.get("minimumCompressionSize"),
        "apiKeySource": data.get("apiKeySource", "HEADER"),
        "endpointConfiguration": data.get("endpointConfiguration", {"types": ["REGIONAL"]}),
        "policy": data.get("policy"),
        "tags": data.get("tags", {}),
        "disableExecuteApiEndpoint": data.get("disableExecuteApiEndpoint", False),
    }
    _rest_apis[api_id] = api
    _resources[api_id] = {}
    _stages_v1[api_id] = {}
    _deployments_v1[api_id] = {}
    _authorizers_v1[api_id] = {}
    _models[api_id] = {}

    # Create root resource "/"
    root_id = _new_id()[:8]
    root_resource = {
        "id": root_id,
        "parentId": None,
        "pathPart": "",
        "path": "/",
        "resourceMethods": {},
    }
    _resources[api_id][root_id] = root_resource

    _v1_tags[_rest_api_arn(api_id)] = dict(data.get("tags", {}))
    return _v1_response(_rest_api_view(api), 201)


def _get_rest_api(api_id):
    api = _rest_apis.get(api_id)
    if not api:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response(_rest_api_view(api))


def _get_rest_apis():
    return _v1_response({"item": [_rest_api_view(a) for a in _rest_apis.values()], "nextToken": None})


def _update_rest_api(api_id, data):
    api = _rest_apis.get(api_id)
    if not api:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(api, patch_ops)
    return _v1_response(_rest_api_view(api))


def _delete_rest_api(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    _rest_apis.pop(api_id, None)
    _resources.pop(api_id, None)
    _stages_v1.pop(api_id, None)
    _deployments_v1.pop(api_id, None)
    _authorizers_v1.pop(api_id, None)
    _models.pop(api_id, None)
    _v1_tags.pop(_rest_api_arn(api_id), None)
    return 202, {}, b""


# ---- Control plane: Resources ----

def _get_resources(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response({"item": list(_resources.get(api_id, {}).values())})


def _get_resource(api_id, resource_id):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    return _v1_response(resource)


def _create_resource(api_id, parent_id, data):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    if parent_id not in _resources.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    path_part = data.get("pathPart", "")
    # Check for duplicate pathPart under same parent
    for r in _resources.get(api_id, {}).values():
        if r.get("parentId") == parent_id and r.get("pathPart") == path_part:
            return _v1_error("ConflictException",
                             f"Another resource with the same parent already has this name: {path_part}", 409)
    resource_id = _new_id()[:8]
    resource = {
        "id": resource_id,
        "parentId": parent_id,
        "pathPart": path_part,
        "path": "",
        "resourceMethods": {},
    }
    _resources[api_id][resource_id] = resource
    # Compute the full path
    resource["path"] = _compute_path(api_id, resource_id)
    return _v1_response(resource, 201)


def _update_resource(api_id, resource_id, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(resource, patch_ops)
    # Recompute path if pathPart changed
    resource["path"] = _compute_path(api_id, resource_id)
    return _v1_response(resource)


def _delete_resource(api_id, resource_id):
    if resource_id not in _resources.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    _resources[api_id].pop(resource_id, None)
    return 202, {}, b""


# ---- Control plane: Methods ----

def _put_method(api_id, resource_id, http_method, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = {
        "httpMethod": http_method,
        "authorizationType": data.get("authorizationType", "NONE"),
        "authorizerId": data.get("authorizerId"),
        "apiKeyRequired": data.get("apiKeyRequired", False),
        "operationName": data.get("operationName", ""),
        "requestParameters": data.get("requestParameters", {}),
        "requestModels": data.get("requestModels", {}),
        "methodResponses": {},
        "methodIntegration": None,
    }
    resource["resourceMethods"][http_method] = method_obj
    return _v1_response(method_obj, 201)


def _get_method(api_id, resource_id, http_method):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    return _v1_response(method_obj)


def _delete_method(api_id, resource_id, http_method):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    resource["resourceMethods"].pop(http_method, None)
    return 204, {}, b""


def _update_method(api_id, resource_id, http_method, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(method_obj, patch_ops)
    return _v1_response(method_obj)


# ---- Control plane: Method Responses ----

def _put_method_response(api_id, resource_id, http_method, status_code, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    method_response = {
        "statusCode": status_code,
        "responseParameters": data.get("responseParameters", {}),
        "responseModels": data.get("responseModels", {}),
    }
    method_obj["methodResponses"][status_code] = method_response
    return _v1_response(method_response)


def _get_method_response(api_id, resource_id, http_method, status_code):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    resp = method_obj["methodResponses"].get(status_code)
    if not resp:
        return _v1_error("NotFoundException", "Invalid Response status code specified", 404)
    return _v1_response(resp)


def _delete_method_response(api_id, resource_id, http_method, status_code):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if method_obj:
        method_obj["methodResponses"].pop(status_code, None)
    return 204, {}, b""


# ---- Control plane: Integration ----

def _put_integration(api_id, resource_id, http_method, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    integration = {
        "type": data.get("type", "AWS_PROXY"),
        "httpMethod": data.get("httpMethod", "POST"),
        "uri": data.get("uri", ""),
        "connectionType": data.get("connectionType", "INTERNET"),
        "credentials": data.get("credentials"),
        "requestParameters": data.get("requestParameters", {}),
        "requestTemplates": data.get("requestTemplates", {}),
        "passthroughBehavior": data.get("passthroughBehavior", "WHEN_NO_MATCH"),
        "timeoutInMillis": data.get("timeoutInMillis", 29000),
        "cacheNamespace": resource_id,
        "cacheKeyParameters": data.get("cacheKeyParameters", []),
        # contentHandling (CONVERT_TO_TEXT | CONVERT_TO_BINARY) is the v1
        # equivalent of v2's contentHandlingStrategy (#439). Without
        # storing it Terraform's aws_api_gateway_integration plans a
        # perpetual replace on every apply.
        "contentHandling": data.get("contentHandling"),
        "integrationResponses": {},
    }
    method_obj["methodIntegration"] = integration
    return _v1_response(integration)


def _get_integration(api_id, resource_id, http_method):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    integration = method_obj.get("methodIntegration")
    if not integration:
        return _v1_error("NotFoundException", "Invalid Integration identifier specified", 404)
    return _v1_response(integration)


def _delete_integration(api_id, resource_id, http_method):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if method_obj:
        method_obj["methodIntegration"] = None
    return 204, {}, b""


def _update_integration(api_id, resource_id, http_method, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    integration = method_obj.get("methodIntegration")
    if not integration:
        return _v1_error("NotFoundException", "Invalid Integration identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(integration, patch_ops)
    return _v1_response(integration)


# ---- Control plane: Integration Responses ----

def _put_integration_response(api_id, resource_id, http_method, status_code, data):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    integration = method_obj.get("methodIntegration")
    if not integration:
        return _v1_error("NotFoundException", "Invalid Integration identifier specified", 404)
    int_response = {
        "statusCode": status_code,
        "selectionPattern": data.get("selectionPattern", ""),
        "responseParameters": data.get("responseParameters", {}),
        "responseTemplates": data.get("responseTemplates", {}),
        "contentHandling": data.get("contentHandling"),
    }
    integration["integrationResponses"][status_code] = int_response
    return _v1_response(int_response)


def _get_integration_response(api_id, resource_id, http_method, status_code):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if not method_obj:
        return _v1_error("NotFoundException", "Invalid Method identifier specified", 404)
    integration = method_obj.get("methodIntegration")
    if not integration:
        return _v1_error("NotFoundException", "Invalid Integration identifier specified", 404)
    resp = integration["integrationResponses"].get(status_code)
    if not resp:
        return _v1_error("NotFoundException", "Invalid Response status code specified", 404)
    return _v1_response(resp)


def _delete_integration_response(api_id, resource_id, http_method, status_code):
    resource = _resources.get(api_id, {}).get(resource_id)
    if not resource:
        return _v1_error("NotFoundException", "Invalid Resource identifier specified", 404)
    method_obj = resource["resourceMethods"].get(http_method)
    if method_obj and method_obj.get("methodIntegration"):
        method_obj["methodIntegration"]["integrationResponses"].pop(status_code, None)
    return 204, {}, b""


# ---- Helpers ----

def _build_api_summary(api_id):
    """Build the apiSummary structure: {path: {httpMethod: {authorizationScopes, apiKeyRequired}}}."""
    summary = {}
    for resource in _resources.get(api_id, {}).values():
        path = resource.get("path", "/")
        for http_method, method_obj in resource.get("resourceMethods", {}).items():
            if path not in summary:
                summary[path] = {}
            summary[path][http_method] = {
                "authorizationScopes": [],
                "apiKeyRequired": method_obj.get("apiKeyRequired", False),
            }
    return summary


# ---- Control plane: Deployments ----

def _create_deployment(api_id, data):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    deployment_id = _new_id()[:8]
    deployment = {
        "id": deployment_id,
        "description": data.get("description", ""),
        "createdDate": _now_unix(),
        "apiSummary": _build_api_summary(api_id),
    }
    _deployments_v1.setdefault(api_id, {})[deployment_id] = deployment

    # If stageName is provided, create/update the stage automatically
    stage_name = data.get("stageName")
    if stage_name:
        existing_stage = _stages_v1.get(api_id, {}).get(stage_name)
        if existing_stage:
            existing_stage["deploymentId"] = deployment_id
            existing_stage["lastUpdatedDate"] = _now_unix()
        else:
            stage = {
                "stageName": stage_name,
                "deploymentId": deployment_id,
                "description": data.get("stageDescription", ""),
                "createdDate": _now_unix(),
                "lastUpdatedDate": _now_unix(),
                "variables": data.get("variables", {}),
                "methodSettings": {},
                "accessLogSettings": {},
                "cacheClusterEnabled": False,
                "cacheClusterSize": None,
                "tracingEnabled": False,
                "tags": {},
                "documentationVersion": None,
            }
            _stages_v1.setdefault(api_id, {})[stage_name] = stage

    return _v1_response(deployment, 201)


def _get_deployments(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response({"item": list(_deployments_v1.get(api_id, {}).values())})


def _get_deployment(api_id, deployment_id):
    deployment = _deployments_v1.get(api_id, {}).get(deployment_id)
    if not deployment:
        return _v1_error("NotFoundException", "Invalid Deployment identifier specified", 404)
    return _v1_response(deployment)


def _update_deployment(api_id, deployment_id, data):
    deployment = _deployments_v1.get(api_id, {}).get(deployment_id)
    if not deployment:
        return _v1_error("NotFoundException", "Invalid Deployment identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(deployment, patch_ops)
    return _v1_response(deployment)


def _delete_deployment(api_id, deployment_id):
    if deployment_id not in _deployments_v1.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Deployment identifier specified", 404)
    _deployments_v1[api_id].pop(deployment_id, None)
    return 202, {}, b""


# ---- Control plane: Stages ----

def _create_stage(api_id, data):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    stage_name = data.get("stageName", "")
    if not stage_name:
        return _v1_error("BadRequestException", "Stage name is required", 400)
    stage = {
        "stageName": stage_name,
        "deploymentId": data.get("deploymentId", ""),
        "description": data.get("description", ""),
        "createdDate": _now_unix(),
        "lastUpdatedDate": _now_unix(),
        "variables": data.get("variables", {}),
        "methodSettings": data.get("methodSettings", {}),
        "accessLogSettings": data.get("accessLogSettings", {}),
        "cacheClusterEnabled": data.get("cacheClusterEnabled", False),
        "cacheClusterSize": data.get("cacheClusterSize"),
        "tracingEnabled": data.get("tracingEnabled", False),
        "tags": data.get("tags", {}),
        "documentationVersion": data.get("documentationVersion"),
    }
    _stages_v1.setdefault(api_id, {})[stage_name] = stage
    return _v1_response(stage, 201)


def _get_stages(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response({"item": list(_stages_v1.get(api_id, {}).values())})


def _get_stage(api_id, stage_name):
    stage = _stages_v1.get(api_id, {}).get(stage_name)
    if not stage:
        return _v1_error("NotFoundException", "Invalid Stage identifier specified", 404)
    return _v1_response(stage)


def _update_stage(api_id, stage_name, data):
    stage = _stages_v1.get(api_id, {}).get(stage_name)
    if not stage:
        return _v1_error("NotFoundException", "Invalid Stage identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_stage_patch(stage, patch_ops)
    stage["lastUpdatedDate"] = _now_unix()
    return _v1_response(stage)


def _delete_stage(api_id, stage_name):
    if stage_name not in _stages_v1.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Stage identifier specified", 404)
    _stages_v1[api_id].pop(stage_name, None)
    return 202, {}, b""


# ---- Control plane: Authorizers ----

def _create_authorizer(api_id, data):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    auth_id = _new_id()[:8]
    authorizer = {
        "id": auth_id,
        "name": data.get("name", ""),
        "type": data.get("type", "TOKEN"),
        "authorizerUri": data.get("authorizerUri", ""),
        "authorizerCredentials": data.get("authorizerCredentials"),
        "identitySource": data.get("identitySource", "method.request.header.Authorization"),
        "identityValidationExpression": data.get("identityValidationExpression", ""),
        "authorizerResultTtlInSeconds": data.get("authorizerResultTtlInSeconds", 300),
        "providerARNs": data.get("providerARNs", []),
    }
    _authorizers_v1.setdefault(api_id, {})[auth_id] = authorizer
    return _v1_response(authorizer, 201)


def _get_authorizers(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response({"item": list(_authorizers_v1.get(api_id, {}).values())})


def _get_authorizer(api_id, auth_id):
    authorizer = _authorizers_v1.get(api_id, {}).get(auth_id)
    if not authorizer:
        return _v1_error("NotFoundException", "Invalid Authorizer identifier specified", 404)
    return _v1_response(authorizer)


def _update_authorizer(api_id, auth_id, data):
    authorizer = _authorizers_v1.get(api_id, {}).get(auth_id)
    if not authorizer:
        return _v1_error("NotFoundException", "Invalid Authorizer identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(authorizer, patch_ops)
    return _v1_response(authorizer)


def _delete_authorizer(api_id, auth_id):
    if auth_id not in _authorizers_v1.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Authorizer identifier specified", 404)
    _authorizers_v1[api_id].pop(auth_id, None)
    return 202, {}, b""


# ---- Control plane: Models ----

def _create_model(api_id, data):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    model_name = data.get("name", "")
    if not model_name:
        return _v1_error("BadRequestException", "Model name is required", 400)
    model = {
        "id": _new_id()[:8],
        "name": model_name,
        "description": data.get("description", ""),
        "schema": data.get("schema", ""),
        "contentType": data.get("contentType", "application/json"),
    }
    _models.setdefault(api_id, {})[model_name] = model
    return _v1_response(model, 201)


def _get_models(api_id):
    if api_id not in _rest_apis:
        return _v1_error("NotFoundException", "Invalid API identifier specified", 404)
    return _v1_response({"item": list(_models.get(api_id, {}).values())})


def _get_model(api_id, model_name):
    model = _models.get(api_id, {}).get(model_name)
    if not model:
        return _v1_error("NotFoundException", "Invalid Model identifier specified", 404)
    return _v1_response(model)


def _delete_model(api_id, model_name):
    if model_name not in _models.get(api_id, {}):
        return _v1_error("NotFoundException", "Invalid Model identifier specified", 404)
    _models[api_id].pop(model_name, None)
    return 202, {}, b""


# ---- Control plane: API Keys ----

def _create_api_key(data):
    key_id = _new_id()[:8]
    key_value = new_uuid().replace("-", "")
    api_key = {
        "id": key_id,
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "enabled": data.get("enabled", True),
        "createdDate": _now_unix(),
        "lastUpdatedDate": _now_unix(),
        "value": key_value,
        "stageKeys": data.get("stageKeys", []),
        "tags": data.get("tags", {}),
    }
    _api_keys[key_id] = api_key
    return _v1_response(api_key, 201)


def _get_api_keys():
    return _v1_response({"item": list(_api_keys.values())})


def _get_api_key(key_id):
    key = _api_keys.get(key_id)
    if not key:
        return _v1_error("NotFoundException", "Invalid API Key identifier specified", 404)
    return _v1_response(key)


def _update_api_key(key_id, data):
    key = _api_keys.get(key_id)
    if not key:
        return _v1_error("NotFoundException", "Invalid API Key identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(key, patch_ops)
    key["lastUpdatedDate"] = _now_unix()
    return _v1_response(key)


def _delete_api_key(key_id):
    if key_id not in _api_keys:
        return _v1_error("NotFoundException", "Invalid API Key identifier specified", 404)
    _api_keys.pop(key_id, None)
    return 202, {}, b""


# ---- Control plane: Usage Plans ----

def _create_usage_plan(data):
    plan_id = _new_id()[:8]
    plan = {
        "id": plan_id,
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "apiStages": data.get("apiStages", []),
        "throttle": data.get("throttle", {}),
        "quota": data.get("quota", {}),
        "tags": data.get("tags", {}),
    }
    _usage_plans[plan_id] = plan
    _usage_plan_keys[plan_id] = {}
    return _v1_response(plan, 201)


def _get_usage_plans():
    return _v1_response({"item": list(_usage_plans.values())})


def _get_usage_plan(plan_id):
    plan = _usage_plans.get(plan_id)
    if not plan:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    return _v1_response(plan)


def _update_usage_plan(plan_id, data):
    plan = _usage_plans.get(plan_id)
    if not plan:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    patch_ops = data.get("patchOperations", [])
    _apply_patch(plan, patch_ops)
    return _v1_response(plan)


def _delete_usage_plan(plan_id):
    if plan_id not in _usage_plans:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    _usage_plans.pop(plan_id, None)
    _usage_plan_keys.pop(plan_id, None)
    return 202, {}, b""


def _create_usage_plan_key(plan_id, data):
    if plan_id not in _usage_plans:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    key_id = data.get("keyId", "")
    key_type = data.get("keyType", "API_KEY")
    plan_key = {
        "id": key_id,
        "type": key_type,
        "name": _api_keys.get(key_id, {}).get("name", ""),
        "value": _api_keys.get(key_id, {}).get("value", ""),
    }
    _usage_plan_keys.setdefault(plan_id, {})[key_id] = plan_key
    return _v1_response(plan_key, 201)


def _get_usage_plan_keys(plan_id):
    if plan_id not in _usage_plans:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    return _v1_response({"item": list(_usage_plan_keys.get(plan_id, {}).values())})


def _delete_usage_plan_key(plan_id, key_id):
    if plan_id not in _usage_plans:
        return _v1_error("NotFoundException", "Invalid Usage Plan identifier specified", 404)
    _usage_plan_keys.get(plan_id, {}).pop(key_id, None)
    return 202, {}, b""


# ---- Control plane: Domain Names ----

def _create_domain_name(data):
    domain_name = data.get("domainName", "")
    if not domain_name:
        return _v1_error("BadRequestException", "Domain name is required", 400)
    dn = {
        "domainName": domain_name,
        "certificateName": data.get("certificateName", ""),
        "certificateArn": data.get("certificateArn", ""),
        "distributionDomainName": f"{domain_name}.cloudfront.net",
        "regionalDomainName": f"{domain_name}.execute-api.{get_region()}.amazonaws.com",
        "regionalHostedZoneId": "Z1UJRXOUMOOFQ8",
        "endpointConfiguration": data.get("endpointConfiguration", {"types": ["REGIONAL"]}),
        "tags": data.get("tags", {}),
    }
    _domain_names[domain_name] = dn
    _base_path_mappings[domain_name] = {}
    return _v1_response(dn, 201)


def _get_domain_names():
    return _v1_response({"item": list(_domain_names.values())})


def _get_domain_name(domain_name):
    dn = _domain_names.get(domain_name)
    if not dn:
        return _v1_error("NotFoundException", "Invalid domain name identifier specified", 404)
    return _v1_response(dn)


def _delete_domain_name(domain_name):
    if domain_name not in _domain_names:
        return _v1_error("NotFoundException", "Invalid domain name identifier specified", 404)
    _domain_names.pop(domain_name, None)
    _base_path_mappings.pop(domain_name, None)
    return 202, {}, b""


def _create_base_path_mapping(domain_name, data):
    if domain_name not in _domain_names:
        return _v1_error("NotFoundException", "Invalid domain name identifier specified", 404)
    base_path = data.get("basePath", "(none)")
    mapping = {
        "basePath": base_path,
        "restApiId": data.get("restApiId", ""),
        "stage": data.get("stage", ""),
    }
    _base_path_mappings.setdefault(domain_name, {})[base_path] = mapping
    return _v1_response(mapping, 201)


def _get_base_path_mappings(domain_name):
    if domain_name not in _domain_names:
        return _v1_error("NotFoundException", "Invalid domain name identifier specified", 404)
    return _v1_response({"item": list(_base_path_mappings.get(domain_name, {}).values())})


def _get_base_path_mapping(domain_name, base_path):
    mapping = _base_path_mappings.get(domain_name, {}).get(base_path)
    if not mapping:
        return _v1_error("NotFoundException", "Invalid base path mapping identifier specified", 404)
    return _v1_response(mapping)


def _delete_base_path_mapping(domain_name, base_path):
    _base_path_mappings.get(domain_name, {}).pop(base_path, None)
    return 202, {}, b""


# ---- Control plane: Tags ----

def _get_v1_tags(resource_arn):
    tags = _v1_tags.get(resource_arn, {})
    return _v1_response({"tags": tags})


def _tag_v1_resource(resource_arn, data):
    tags = data.get("tags", {})
    _v1_tags.setdefault(resource_arn, {}).update(tags)
    return 204, {}, b""


def _untag_v1_resource(resource_arn, tag_keys):
    existing = _v1_tags.get(resource_arn, {})
    for key in tag_keys:
        existing.pop(key, None)
    return 204, {}, b""


# ---- Control plane: Account ----
# GetAccount / UpdateAccount — singleton per AWS account. Terraform's
# aws_api_gateway_account resource reads and writes /account with a single
# patch op for cloudwatchRoleArn.

_ACCOUNT_DEFAULTS = {
    "cloudwatchRoleArn": None,
    "throttleSettings": {"burstLimit": 5000, "rateLimit": 10000},
    "features": ["UsagePlans"],
    "apiKeyVersion": "4",
}


def _get_account():
    overrides = _account_settings.get("settings") or {}
    merged = {**_ACCOUNT_DEFAULTS, **overrides}
    # throttleSettings is a dict — merge nested so a partial override keeps the other limit
    if "throttleSettings" in overrides:
        merged["throttleSettings"] = {**_ACCOUNT_DEFAULTS["throttleSettings"], **overrides["throttleSettings"]}
    return _v1_response(merged)


def _update_account(data):
    current = dict(_account_settings.get("settings") or {})
    _apply_patch(current, data.get("patchOperations", []))
    _account_settings["settings"] = current
    return _get_account()
