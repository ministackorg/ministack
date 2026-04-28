"""
AWS AppSync Service Emulator.

GraphQL API management service — REST/JSON protocol via /v1/apis/* paths.

Supports:
  GraphQL APIs:  CreateGraphQLApi, GetGraphQLApi, ListGraphQLApis,
                 UpdateGraphQLApi, DeleteGraphQLApi
  API Keys:      CreateApiKey, ListApiKeys, DeleteApiKey
  Data Sources:  CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource
  Resolvers:     CreateResolver, GetResolver, ListResolvers, DeleteResolver
  Types:         CreateType, ListTypes, GetType
  Tags:          TagResource, UntagResource, ListTagsForResource

Wire protocol:
  REST/JSON — path-based routing under /v1/apis.
  Credential scope: appsync
"""

import copy
import json
import logging
import os
import re
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("appsync")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_apis = AccountScopedDict()            # apiId -> api record
_api_keys = AccountScopedDict()        # apiId -> {keyId -> key record}
_data_sources = AccountScopedDict()    # apiId -> {name -> data source record}
_resolvers = AccountScopedDict()       # apiId -> {typeName -> {fieldName -> resolver record}}
_types = AccountScopedDict()           # apiId -> {typeName -> type record}
_tags = AccountScopedDict()            # resource_arn -> {key: value}

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _load_persisted():
    if not PERSIST_STATE:
        return
    data = load_state("appsync")
    if data:
        restore_state(data)
        logger.info("Loaded persisted state for appsync")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return int(time.time())


def _api_arn(api_id):
    return f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}"


def _json(status, body):
    return json_response(body, status)


# ---------------------------------------------------------------------------
# GraphQL APIs
# ---------------------------------------------------------------------------

def _create_graphql_api(body):
    api_id = new_uuid().replace("-", "")[:26]
    name = body.get("name", "")
    auth_type = body.get("authenticationType", "API_KEY")
    additional_auth = body.get("additionalAuthenticationProviders", [])
    log_config = body.get("logConfig")
    user_pool_config = body.get("userPoolConfig")
    openid_config = body.get("openIDConnectConfig")
    xray = body.get("xrayEnabled", False)
    tags = body.get("tags", {})
    lambda_auth = body.get("lambdaAuthorizerConfig")

    arn = _api_arn(api_id)
    now = _now()

    record = {
        "apiId": api_id,
        "name": name,
        "authenticationType": auth_type,
        "arn": arn,
        "uris": {
            "GRAPHQL": f"https://{api_id}.appsync-api.{get_region()}.amazonaws.com/graphql",
            "REALTIME": f"wss://{api_id}.appsync-realtime-api.{get_region()}.amazonaws.com/graphql",
        },
        "additionalAuthenticationProviders": additional_auth,
        "xrayEnabled": xray,
        "wafWebAclArn": body.get("wafWebAclArn"),
        "createdAt": now,
        "lastUpdatedAt": now,
    }
    if log_config:
        record["logConfig"] = log_config
    if user_pool_config:
        record["userPoolConfig"] = user_pool_config
    if openid_config:
        record["openIDConnectConfig"] = openid_config
    if lambda_auth:
        record["lambdaAuthorizerConfig"] = lambda_auth

    _apis[api_id] = record
    _api_keys[api_id] = {}
    _data_sources[api_id] = {}
    _resolvers[api_id] = {}
    _types[api_id] = {}

    if tags:
        _tags[arn] = tags

    return _json(200, {"graphqlApi": record})


def _get_graphql_api(api_id):
    api = _apis.get(api_id)
    if not api:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    return _json(200, {"graphqlApi": api})


def _list_graphql_apis(query_params):
    apis = list(_apis.values())
    return _json(200, {"graphqlApis": apis})


def _update_graphql_api(api_id, body):
    api = _apis.get(api_id)
    if not api:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    if "name" in body:
        api["name"] = body["name"]
    if "authenticationType" in body:
        api["authenticationType"] = body["authenticationType"]
    if "additionalAuthenticationProviders" in body:
        api["additionalAuthenticationProviders"] = body["additionalAuthenticationProviders"]
    if "logConfig" in body:
        api["logConfig"] = body["logConfig"]
    if "userPoolConfig" in body:
        api["userPoolConfig"] = body["userPoolConfig"]
    if "openIDConnectConfig" in body:
        api["openIDConnectConfig"] = body["openIDConnectConfig"]
    if "xrayEnabled" in body:
        api["xrayEnabled"] = body["xrayEnabled"]
    if "lambdaAuthorizerConfig" in body:
        api["lambdaAuthorizerConfig"] = body["lambdaAuthorizerConfig"]

    api["lastUpdatedAt"] = _now()
    return _json(200, {"graphqlApi": api})


def _delete_graphql_api(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    arn = _apis[api_id]["arn"]
    del _apis[api_id]
    _api_keys.pop(api_id, None)
    _data_sources.pop(api_id, None)
    _resolvers.pop(api_id, None)
    _types.pop(api_id, None)
    _tags.pop(arn, None)

    return _json(200, {})


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

def _create_api_key(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    key_id = "da2-" + new_uuid()[:26]
    now = _now()
    expires = body.get("expires", now + 604800)  # default 7 days
    description = body.get("description", "")

    record = {
        "id": key_id,
        "description": description,
        "expires": expires,
        "createdAt": now,
        "lastUpdatedAt": now,
        "deletes": expires + 5184000,  # 60 days after expiry
    }

    _api_keys.setdefault(api_id, {})[key_id] = record
    return _json(200, {"apiKey": record})


def _list_api_keys(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    keys = list(_api_keys.get(api_id, {}).values())
    return _json(200, {"apiKeys": keys})


def _delete_api_key(api_id, key_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    keys = _api_keys.get(api_id, {})
    if key_id not in keys:
        return error_response_json("NotFoundException", f"API key {key_id} not found", 404)

    del keys[key_id]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------

def _create_data_source(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    name = body.get("name", "")
    ds_type = body.get("type", "NONE")
    description = body.get("description", "")
    service_role_arn = body.get("serviceRoleArn", "")

    arn = f"{_apis[api_id]['arn']}/datasources/{name}"

    record = {
        "dataSourceArn": arn,
        "name": name,
        "type": ds_type,
        "description": description,
        "serviceRoleArn": service_role_arn,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }

    if ds_type == "AMAZON_DYNAMODB":
        record["dynamodbConfig"] = body.get("dynamodbConfig", {})
    elif ds_type == "AWS_LAMBDA":
        record["lambdaConfig"] = body.get("lambdaConfig", {})
    elif ds_type == "AMAZON_ELASTICSEARCH" or ds_type == "AMAZON_OPENSEARCH_SERVICE":
        record["elasticsearchConfig"] = body.get("elasticsearchConfig", {})
    elif ds_type == "HTTP":
        record["httpConfig"] = body.get("httpConfig", {})
    elif ds_type == "RELATIONAL_DATABASE":
        record["relationalDatabaseConfig"] = body.get("relationalDatabaseConfig", {})

    _data_sources.setdefault(api_id, {})[name] = record
    return _json(200, {"dataSource": record})


def _get_data_source(api_id, name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    ds = _data_sources.get(api_id, {}).get(name)
    if not ds:
        return error_response_json("NotFoundException", f"Data source {name} not found", 404)

    return _json(200, {"dataSource": ds})


def _list_data_sources(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    sources = list(_data_sources.get(api_id, {}).values())
    return _json(200, {"dataSources": sources})


def _delete_data_source(api_id, name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    sources = _data_sources.get(api_id, {})
    if name not in sources:
        return error_response_json("NotFoundException", f"Data source {name} not found", 404)

    del sources[name]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------

def _create_resolver(api_id, type_name, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    field_name = body.get("fieldName", "")
    data_source_name = body.get("dataSourceName")
    request_template = body.get("requestMappingTemplate", "")
    response_template = body.get("responseMappingTemplate", "")
    kind = body.get("kind", "UNIT")
    pipeline_config = body.get("pipelineConfig")
    caching_config = body.get("cachingConfig")
    runtime = body.get("runtime")
    code = body.get("code")

    arn = f"{_apis[api_id]['arn']}/types/{type_name}/resolvers/{field_name}"

    record = {
        "typeName": type_name,
        "fieldName": field_name,
        "dataSourceName": data_source_name,
        "resolverArn": arn,
        "requestMappingTemplate": request_template,
        "responseMappingTemplate": response_template,
        "kind": kind,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }
    if pipeline_config:
        record["pipelineConfig"] = pipeline_config
    if caching_config:
        record["cachingConfig"] = caching_config
    if runtime:
        record["runtime"] = runtime
    if code:
        record["code"] = code

    _resolvers.setdefault(api_id, {}).setdefault(type_name, {})[field_name] = record
    return _json(200, {"resolver": record})


def _get_resolver(api_id, type_name, field_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    resolver = _resolvers.get(api_id, {}).get(type_name, {}).get(field_name)
    if not resolver:
        return error_response_json("NotFoundException",
                                   f"Resolver {type_name}.{field_name} not found", 404)

    return _json(200, {"resolver": resolver})


def _list_resolvers(api_id, type_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    resolvers = list(_resolvers.get(api_id, {}).get(type_name, {}).values())
    return _json(200, {"resolvers": resolvers})


def _delete_resolver(api_id, type_name, field_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    type_resolvers = _resolvers.get(api_id, {}).get(type_name, {})
    if field_name not in type_resolvers:
        return error_response_json("NotFoundException",
                                   f"Resolver {type_name}.{field_name} not found", 404)

    del type_resolvers[field_name]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

def _create_type(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    definition = body.get("definition", "")
    fmt = body.get("format", "SDL")

    # Extract type name from SDL definition (e.g. "type Query { ... }" -> "Query")
    name_match = re.search(r"(?:type|input|enum|interface|union|scalar)\s+(\w+)", definition)
    type_name = name_match.group(1) if name_match else "Unknown"

    arn = f"{_apis[api_id]['arn']}/types/{type_name}"

    record = {
        "name": type_name,
        "description": body.get("description", ""),
        "arn": arn,
        "definition": definition,
        "format": fmt,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }

    _types.setdefault(api_id, {})[type_name] = record
    return _json(200, {"type": record})


def _get_type(api_id, type_name, query_params):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    fmt = "SDL"
    if query_params.get("format"):
        fmt_val = query_params["format"]
        fmt = fmt_val[0] if isinstance(fmt_val, list) else fmt_val

    t = _types.get(api_id, {}).get(type_name)
    if not t:
        return error_response_json("NotFoundException", f"Type {type_name} not found", 404)

    return _json(200, {"type": t})


def _list_types(api_id, query_params):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    types = list(_types.get(api_id, {}).values())
    return _json(200, {"types": types})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(body):
    arn = body.get("resourceArn", "")
    tags = body.get("tags", {})
    _tags.setdefault(arn, {}).update(tags)
    return _json(200, {})


def _untag_resource(arn, query_params):
    tag_keys = query_params.get("tagKeys", [])
    if isinstance(tag_keys, str):
        tag_keys = [tag_keys]
    existing = _tags.get(arn, {})
    for k in tag_keys:
        existing.pop(k, None)
    return _json(200, {})


def _list_tags_for_resource(arn):
    tags = _tags.get(arn, {})
    return _json(200, {"tags": tags})


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------

# Path patterns for routing
_PATH_RE = re.compile(r"^/v1/apis(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?")
# /v1/apis                          -> groups: (None, None, None, None, None)
# /v1/apis/{apiId}                  -> groups: (apiId, None, None, None, None)
# /v1/apis/{apiId}/apikeys          -> groups: (apiId, "apikeys", None, None, None)
# /v1/apis/{apiId}/apikeys/{id}     -> groups: (apiId, "apikeys", id, None, None)
# /v1/apis/{apiId}/datasources      -> groups: (apiId, "datasources", None, None, None)
# /v1/apis/{apiId}/datasources/{n}  -> groups: (apiId, "datasources", name, None, None)
# /v1/apis/{apiId}/types            -> groups: (apiId, "types", None, None, None)
# /v1/apis/{apiId}/types/{t}/resolvers          -> (apiId, "types", t, "resolvers", None)
# /v1/apis/{apiId}/types/{t}/resolvers/{field}  -> (apiId, "types", t, "resolvers", field)


async def handle_request(method, path, headers, body, query_params):
    """Main entry point — route AppSync REST requests."""

    # Tags endpoint: /v1/tags/{resourceArn}
    if path.startswith("/v1/tags/"):
        from urllib.parse import unquote
        arn = unquote(path[len("/v1/tags/"):])
        if method == "POST":
            data = json.loads(body) if body else {}
            data["resourceArn"] = arn
            return _tag_resource(data)
        elif method == "DELETE":
            return _untag_resource(arn, query_params)
        else:  # GET
            return _list_tags_for_resource(arn)

    # GraphQL data plane: POST /graphql or POST /v1/apis/{apiId}/graphql
    if path == "/graphql" and method == "POST":
        api_key = headers.get("x-api-key", "")
        api_id = _resolve_api_by_key(api_key)
        if not api_id:
            return error_response_json("UnauthorizedException", "Valid API key required", 401)
        data = json.loads(body) if body else {}
        return _execute_graphql(api_id, data)

    if path.startswith("/v1/apis/") and path.endswith("/graphql") and method == "POST":
        parts = path.split("/")
        if len(parts) >= 5:
            api_id = parts[3]
            data = json.loads(body) if body else {}
            return _execute_graphql(api_id, data)

    m = _PATH_RE.match(path)
    if not m:
        return error_response_json("NotFoundException", f"Unknown path: {path}", 404)

    api_id, sub1, sub2, sub3, sub4 = m.groups()

    data = {}
    if body:
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}

    # POST /v1/apis — CreateGraphQLApi
    if api_id is None and sub1 is None:
        if method == "POST":
            return _create_graphql_api(data)
        elif method == "GET":
            return _list_graphql_apis(query_params)

    # /v1/apis/{apiId}
    if api_id and sub1 is None:
        if method == "GET":
            return _get_graphql_api(api_id)
        elif method == "POST":
            return _update_graphql_api(api_id, data)
        elif method == "DELETE":
            return _delete_graphql_api(api_id)

    # /v1/apis/{apiId}/apikeys
    if sub1 == "apikeys":
        if sub2 is None:
            if method == "POST":
                return _create_api_key(api_id, data)
            elif method == "GET":
                return _list_api_keys(api_id)
        else:
            # /v1/apis/{apiId}/apikeys/{keyId}
            if method == "DELETE":
                return _delete_api_key(api_id, sub2)

    # /v1/apis/{apiId}/datasources
    if sub1 == "datasources":
        if sub2 is None:
            if method == "POST":
                return _create_data_source(api_id, data)
            elif method == "GET":
                return _list_data_sources(api_id)
        else:
            # /v1/apis/{apiId}/datasources/{name}
            if method == "GET":
                return _get_data_source(api_id, sub2)
            elif method == "DELETE":
                return _delete_data_source(api_id, sub2)

    # /v1/apis/{apiId}/types
    if sub1 == "types":
        if sub2 is None:
            if method == "POST":
                return _create_type(api_id, data)
            elif method == "GET":
                return _list_types(api_id, query_params)
        elif sub3 == "resolvers":
            # /v1/apis/{apiId}/types/{typeName}/resolvers
            type_name = sub2
            if sub4 is None:
                if method == "POST":
                    return _create_resolver(api_id, type_name, data)
                elif method == "GET":
                    return _list_resolvers(api_id, type_name)
            else:
                # /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}
                field_name = sub4
                if method == "GET":
                    return _get_resolver(api_id, type_name, field_name)
                elif method == "DELETE":
                    return _delete_resolver(api_id, type_name, field_name)
        else:
            # /v1/apis/{apiId}/types/{typeName} — GetType
            if sub3 is None and method == "GET":
                return _get_type(api_id, sub2, query_params)

    return error_response_json("BadRequestException", f"Unsupported route: {method} {path}")


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def reset():
    """Clear all in-memory state."""
    _apis.clear()
    _api_keys.clear()
    _data_sources.clear()
    _resolvers.clear()
    _types.clear()
    _tags.clear()


def get_state():
    """Return a deep copy of all state for persistence."""
    return copy.deepcopy({
        "apis": _apis,
        "api_keys": _api_keys,
        "data_sources": _data_sources,
        "resolvers": _resolvers,
        "types": _types,
        "tags": _tags,
    })


def restore_state(data):
    """Restore state from persisted data."""
    _apis.update(data.get("apis", {}))
    _api_keys.update(data.get("api_keys", {}))
    _data_sources.update(data.get("data_sources", {}))
    _resolvers.update(data.get("resolvers", {}))
    _types.update(data.get("types", {}))
    _tags.update(data.get("tags", {}))


# ---------------------------------------------------------------------------
# GraphQL Data Plane — parse and execute queries against DynamoDB
# ---------------------------------------------------------------------------

import re as _re

# Simple GraphQL parser — handles queries/mutations that Amplify generates
_GQL_OP_RE = _re.compile(
    r'(?:query|mutation|subscription)\s+(\w+)?\s*(?:\(([^)]*)\))?\s*\{(.*)\}',
    _re.DOTALL,
)
_GQL_FIELD_RE = _re.compile(r'(\w+)\s*(?:\(([^)]*)\))?\s*(?:\{([^}]*)\})?')


def _resolve_api_by_key(api_key_value):
    """Find the API ID that owns this API key."""
    for api_id, keys in _api_keys.items():
        for kid, key in keys.items():
            if kid == api_key_value or key.get("id") == api_key_value:
                return api_id
    # Fallback: if only one API exists, use it
    if len(_apis) == 1:
        return next(iter(_apis))
    return None


def _execute_graphql(api_id, data):
    """Execute a GraphQL query/mutation against the configured resolvers."""
    query = data.get("query", "")
    variables = data.get("variables", {})
    operation_name = data.get("operationName")

    if not query.strip():
        return _json(400, {"errors": [{"message": "Query is required"}]})

    if api_id not in _apis:
        return _json(404, {"errors": [{"message": f"API {api_id} not found"}]})

    # Parse the top-level operation
    # Strip __typename fields — Amplify adds these everywhere
    query_clean = _re.sub(r'__typename\s*', '', query)

    m = _GQL_OP_RE.search(query_clean)
    if not m:
        # Try bare field query: { getUser(id: "1") { name } }
        inner = query_clean.strip().strip("{}")
        fields = _parse_fields(inner, variables)
    else:
        op_name, op_args, body = m.groups()
        fields = _parse_fields(body, variables)

    # Determine operation type
    is_mutation = query_clean.strip().startswith("mutation")

    results = {}
    errors = []
    for field_name, args, sub_fields in fields:
        resolver = _find_resolver(api_id, "Mutation" if is_mutation else "Query", field_name)
        if resolver:
            try:
                result = _resolve_field(api_id, resolver, args, sub_fields, variables)
                results[field_name] = result
            except Exception as e:
                errors.append({"message": str(e), "path": [field_name]})
                results[field_name] = None
        else:
            # No resolver — return mock empty result
            results[field_name] = None

    response = {"data": results}
    if errors:
        response["errors"] = errors
    return _json(200, response)


def _parse_fields(body, variables):
    """Parse GraphQL field selections into (name, args_dict, sub_fields) tuples."""
    fields = []
    for m in _GQL_FIELD_RE.finditer(body.strip()):
        name = m.group(1)
        args_str = m.group(2) or ""
        sub = m.group(3) or ""
        args = _parse_args(args_str, variables)
        sub_fields = [s.strip() for s in sub.split() if s.strip() and s.strip() != "__typename"]
        fields.append((name, args, sub_fields))
    return fields


def _parse_args(args_str, variables):
    """Parse GraphQL arguments like (id: "1") or (id: $id) into a dict."""
    args = {}
    if not args_str.strip():
        return args
    # Match key: value pairs
    for pair in _re.finditer(r'(\w+)\s*:\s*("(?:[^"\\]|\\.)*"|\$\w+|\d+(?:\.\d+)?|true|false|null|\{[^}]*\}|\[[^\]]*\])', args_str):
        key = pair.group(1)
        val = pair.group(2)
        if val.startswith("$"):
            val = variables.get(val[1:], val)
        elif val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        elif val == "true":
            val = True
        elif val == "false":
            val = False
        elif val == "null":
            val = None
        elif val.startswith("{") and val.endswith("}"):
            val = _parse_args(val[1:-1], variables)
        elif val.startswith("[") and val.endswith("]"):
            val = val  # Keep as string for now
        elif val.replace(".", "").isdigit():
            val = float(val) if "." in val else int(val)
        args[key] = val
    return args


def _find_resolver(api_id, type_name, field_name):
    """Find a resolver for Query.fieldName or Mutation.fieldName."""
    resolvers = _resolvers.get(api_id, {})
    # Try exact match
    if type_name in resolvers and field_name in resolvers[type_name]:
        return resolvers[type_name][field_name]
    # Try generic match (some setups use "Query" or "Mutation" type)
    for tn in resolvers:
        if field_name in resolvers[tn]:
            return resolvers[tn][field_name]
    return None


def _resolve_field(api_id, resolver, args, sub_fields, variables):
    """Execute a resolver against its data source (DynamoDB)."""
    ds_name = resolver.get("dataSourceName", "")
    data_source = _data_sources.get(api_id, {}).get(ds_name)

    if not data_source:
        # No data source — return args as mock
        return args or {}

    ds_type = data_source.get("type", "NONE")

    if ds_type == "AMAZON_DYNAMODB":
        return _resolve_dynamodb(data_source, resolver, args, sub_fields)
    elif ds_type == "AWS_LAMBDA":
        return _resolve_lambda(data_source, args)
    else:
        return args or {}


def _resolve_dynamodb(data_source, resolver, args, sub_fields):
    """Execute a DynamoDB resolver — auto-detect operation from field name and args."""
    import ministack.services.dynamodb as _ddb

    config = data_source.get("dynamodbConfig", {})
    table_name = config.get("tableName", "")
    if not table_name:
        return None

    table = _ddb._tables.get(table_name)
    if not table:
        return None

    field_name = resolver.get("fieldName", "")

    # Auto-detect: get* → GetItem, list* → Scan, create*/update*/put* → PutItem, delete* ��� DeleteItem
    if field_name.startswith("get") or "id" in args:
        return _ddb_get_item(table, table_name, args, sub_fields)
    elif field_name.startswith("list"):
        return _ddb_scan(table, table_name, args, sub_fields)
    elif field_name.startswith("create") or field_name.startswith("put"):
        return _ddb_put_item(table, table_name, args)
    elif field_name.startswith("update"):
        return _ddb_update_item(table, table_name, args)
    elif field_name.startswith("delete"):
        return _ddb_delete_item(table, table_name, args)
    else:
        # Default: try scan
        return _ddb_scan(table, table_name, args, sub_fields)


def _ddb_get_item(table, table_name, args, sub_fields):
    """Get a single item by primary key."""
    pk_name = table["pk_name"]
    sk_name = table.get("sk_name")

    pk_val = args.get("id") or args.get(pk_name) or next(iter(args.values()), None)
    if pk_val is None:
        return None

    items = table["items"]
    pk_bucket = items.get(str(pk_val), {})

    if sk_name:
        sk_val = args.get(sk_name, "")
        item = pk_bucket.get(str(sk_val))
    else:
        # No sort key — get the single item
        item = next(iter(pk_bucket.values()), None) if pk_bucket else None

    if not item:
        return None

    return _strip_ddb_types(item, sub_fields)


def _ddb_scan(table, table_name, args, sub_fields):
    """Scan/list items, optionally with filters and pagination."""
    items = []
    limit = args.get("limit", 100)
    next_token = args.get("nextToken")

    count = 0
    for pk in sorted(table["items"].keys()):
        for sk in sorted(table["items"][pk].keys()):
            if count >= limit:
                break
            items.append(_strip_ddb_types(table["items"][pk][sk], sub_fields))
            count += 1

    # Filter if filter arg provided
    filter_arg = args.get("filter", {})
    if filter_arg and isinstance(filter_arg, dict):
        filtered = []
        for item in items:
            match = True
            for fk, fv in filter_arg.items():
                if isinstance(fv, dict) and "eq" in fv:
                    if item.get(fk) != fv["eq"]:
                        match = False
                elif item.get(fk) != fv:
                    match = False
            if match:
                filtered.append(item)
        items = filtered

    return {"items": items, "nextToken": None}


def _ddb_put_item(table, table_name, args):
    """Create/put an item."""
    from collections import defaultdict

    import ministack.services.dynamodb as _ddb

    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    sk_name = table.get("sk_name")

    # Build DynamoDB-typed item
    ddb_item = {}
    for k, v in input_data.items():
        if isinstance(v, str):
            ddb_item[k] = {"S": v}
        elif isinstance(v, (int, float)):
            ddb_item[k] = {"N": str(v)}
        elif isinstance(v, bool):
            ddb_item[k] = {"BOOL": v}
        elif isinstance(v, list):
            ddb_item[k] = {"L": [{"S": str(i)} for i in v]}
        elif v is None:
            ddb_item[k] = {"NULL": True}
        else:
            ddb_item[k] = {"S": str(v)}

    # Auto-generate ID if not provided
    if pk_name not in ddb_item and "id" not in ddb_item:
        ddb_item["id" if pk_name == "id" else pk_name] = {"S": new_uuid()}

    pk_val = _ddb._extract_key_val(ddb_item.get(pk_name, {}))
    sk_val = _ddb._extract_key_val(ddb_item.get(sk_name, {})) if sk_name else ""

    if not isinstance(table["items"], defaultdict):
        table["items"] = defaultdict(dict, table["items"])

    table["items"][pk_val][sk_val] = ddb_item
    table["ItemCount"] = sum(len(v) for v in table["items"].values())

    return _strip_ddb_types(ddb_item, [])


def _ddb_update_item(table, table_name, args):
    """Update an existing item — merge input fields."""
    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    pk_val = str(input_data.get("id") or input_data.get(pk_name, ""))

    if pk_val in table["items"]:
        sk = next(iter(table["items"][pk_val]), "")
        existing = table["items"][pk_val].get(sk, {})
        for k, v in input_data.items():
            if isinstance(v, str):
                existing[k] = {"S": v}
            elif isinstance(v, (int, float)):
                existing[k] = {"N": str(v)}
            elif isinstance(v, bool):
                existing[k] = {"BOOL": v}
        return _strip_ddb_types(existing, [])
    return None


def _ddb_delete_item(table, table_name, args):
    """Delete an item and return it."""
    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    pk_val = str(input_data.get("id") or input_data.get(pk_name, ""))

    if pk_val in table["items"]:
        sk = next(iter(table["items"][pk_val]), "")
        item = table["items"][pk_val].pop(sk, None)
        if not table["items"][pk_val]:
            table["items"].pop(pk_val, None)
        if item:
            return _strip_ddb_types(item, [])
    return None


def _strip_ddb_types(item, sub_fields):
    """Convert DynamoDB typed attributes to plain values for GraphQL response."""
    if not item:
        return None
    result = {}
    for k, v in item.items():
        if isinstance(v, dict):
            if "S" in v:
                result[k] = v["S"]
            elif "N" in v:
                val = v["N"]
                result[k] = int(val) if "." not in val else float(val)
            elif "BOOL" in v:
                result[k] = v["BOOL"]
            elif "NULL" in v:
                result[k] = None
            elif "L" in v:
                result[k] = [_strip_ddb_types(i, []) if isinstance(i, dict) and not any(t in i for t in ("S", "N", "BOOL")) else (i.get("S") or i.get("N") or i.get("BOOL")) for i in v["L"]]
            elif "M" in v:
                result[k] = _strip_ddb_types(v["M"], [])
            else:
                result[k] = v
        else:
            result[k] = v
    if sub_fields:
        result = {k: v for k, v in result.items() if k in sub_fields or k == "id" or k == "__typename"}
    return result


def _resolve_lambda(data_source, args):
    """Execute a Lambda resolver."""
    config = data_source.get("lambdaConfig", {})
    func_arn = config.get("lambdaFunctionArn", "")
    if not func_arn:
        return args

    import ministack.services.lambda_svc as _lambda_svc
    func_name = func_arn.rsplit(":", 1)[-1]
    func = _lambda_svc._functions.get(func_name)
    if not func:
        return args

    result = _lambda_svc._execute_function(func, args)
    body = result.get("body")
    if isinstance(body, dict):
        return body
    if isinstance(body, (str, bytes)):
        try:
            return json.loads(body)
        except Exception:
            return {"result": body}
    return args

# Load persisted state (must be after restore_state is defined)
_load_persisted()
