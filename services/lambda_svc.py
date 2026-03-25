"""
Lambda Service Emulator.
Supports: CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration,
          ListFunctions (paginated with Marker/MaxItems), Invoke (RequestResponse / Event / DryRun),
          UpdateFunctionCode, UpdateFunctionConfiguration,
          PublishVersion, ListVersionsByFunction,
          CreateAlias, GetAlias, UpdateAlias, DeleteAlias, ListAliases,
          AddPermission, RemovePermission, GetPolicy,
          ListTags, TagResource, UntagResource,
          PublishLayerVersion, GetLayerVersion, ListLayerVersions,
          DeleteLayerVersion, ListLayers,
          CreateEventSourceMapping, DeleteEventSourceMapping,
          GetEventSourceMapping, ListEventSourceMappings, UpdateEventSourceMapping,
          GetFunctionEventInvokeConfig, PutFunctionEventInvokeConfig (stub),
          PutFunctionConcurrency, GetFunctionConcurrency, DeleteFunctionConcurrency,
          GetFunctionCodeSigningConfig (stub),
          CreateFunctionUrlConfig, GetFunctionUrlConfig, UpdateFunctionUrlConfig,
          DeleteFunctionUrlConfig, ListFunctionUrlConfigs.

Functions are stored in-memory.  Python functions are executed in a subprocess
with the event piped through stdin (safe from injection).
SQS event source mappings poll the queue in a background thread.
"""

import os
import copy
import json
import time
import base64
import hashlib
import zipfile
import tempfile
import subprocess
import threading
import logging
import re
from urllib.parse import unquote

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("lambda")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
LAMBDA_EXECUTOR = os.environ.get("LAMBDA_EXECUTOR", "local").lower()
LAMBDA_DOCKER_VOLUME_MOUNT = os.environ.get("LAMBDA_REMOTE_DOCKER_VOLUME_MOUNT", "")

try:
    import docker as docker_lib
    _docker_available = True
except ImportError:
    _docker_available = False

_functions: dict = {}   # function_name -> FunctionRecord
_layers: dict = {}      # layer_name -> {"versions": [...], "next_version": int}
_esms: dict = {}        # uuid -> esm dict
_function_urls: dict = {}  # function_name -> FunctionUrlConfig dict
_poller_started = False
_poller_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Wrapper script executed inside the subprocess.
# All configuration is passed through env vars; event data arrives on stdin.
# ---------------------------------------------------------------------------
_WRAPPER_SCRIPT = """\
import sys, os, json

sys.path.insert(0, os.environ["_LAMBDA_CODE_DIR"])

for _ld in filter(None, os.environ.get("_LAMBDA_LAYERS_DIRS", "").split(os.pathsep)):
    _py = os.path.join(_ld, "python")
    if os.path.isdir(_py):
        sys.path.insert(0, _py)
    sys.path.insert(0, _ld)

_mod_path = os.environ["_LAMBDA_HANDLER_MODULE"]
_fn_name  = os.environ["_LAMBDA_HANDLER_FUNC"]

event = json.loads(sys.stdin.read())

class LambdaContext:
    function_name        = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
    memory_limit_in_mb   = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128"))
    invoked_function_arn = os.environ.get("_LAMBDA_FUNCTION_ARN", "")
    aws_request_id       = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "")
    log_group_name       = "/aws/lambda/" + function_name
    log_stream_name      = aws_request_id

    @staticmethod
    def get_remaining_time_in_millis():
        return int(float(os.environ.get("_LAMBDA_TIMEOUT", "3")) * 1000)

_mod = __import__(_mod_path)
for _part in _mod_path.split(".")[1:]:
    _mod = getattr(_mod, _part)
_result = getattr(_mod, _fn_name)(event, LambdaContext())
if _result is not None:
    print(json.dumps(_result))
"""

# Docker variant: paths fixed to /var/task (code) and /opt (layers).
_DOCKER_WRAPPER_SCRIPT = """\
import sys, os, json

sys.path.insert(0, "/var/task")

for _ld in filter(None, os.environ.get("_LAMBDA_LAYERS_DIRS", "").split(":")):
    _py = os.path.join(_ld, "python")
    if os.path.isdir(_py):
        sys.path.insert(0, _py)
    sys.path.insert(0, _ld)

_mod_path = os.environ["_LAMBDA_HANDLER_MODULE"]
_fn_name  = os.environ["_LAMBDA_HANDLER_FUNC"]

event = json.loads(sys.stdin.read())

class LambdaContext:
    function_name        = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
    memory_limit_in_mb   = int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128"))
    invoked_function_arn = os.environ.get("_LAMBDA_FUNCTION_ARN", "")
    aws_request_id       = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "")
    log_group_name       = "/aws/lambda/" + function_name
    log_stream_name      = aws_request_id

    @staticmethod
    def get_remaining_time_in_millis():
        return int(float(os.environ.get("_LAMBDA_TIMEOUT", "3")) * 1000)

_mod = __import__(_mod_path)
for _part in _mod_path.split(".")[1:]:
    _mod = getattr(_mod, _part)
_result = getattr(_mod, _fn_name)(event, LambdaContext())
if _result is not None:
    print(json.dumps(_result))
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_name(name_or_arn: str) -> str:
    """Extract plain function name from a name, partial ARN, or full ARN."""
    if not name_or_arn:
        return ""
    if name_or_arn.startswith("arn:"):
        segs = name_or_arn.split(":")
        return segs[6] if len(segs) >= 7 else name_or_arn
    if ":" in name_or_arn:
        return name_or_arn.split(":")[0]
    return name_or_arn


def _resolve_name_and_qualifier(name_or_arn: str) -> tuple[str, str | None]:
    """Extract (function_name, qualifier) from a name, partial ARN, or full ARN.

    Handles:
      my-function                -> ("my-function", None)
      my-function:v1             -> ("my-function", "v1")
      arn:...:function:my-func   -> ("my-func", None)
      arn:...:function:my-func:3 -> ("my-func", "3")
    """
    if not name_or_arn:
        return "", None
    if name_or_arn.startswith("arn:"):
        segs = name_or_arn.split(":")
        name = segs[6] if len(segs) >= 7 else name_or_arn
        qualifier = segs[7] if len(segs) >= 8 and segs[7] else None
        return name, qualifier
    if ":" in name_or_arn:
        name, qualifier = name_or_arn.split(":", 1)
        return name, qualifier or None
    return name_or_arn, None


def _func_arn(name: str) -> str:
    return f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"


def _layer_arn(name: str) -> str:
    return f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:layer:{name}"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def _build_config(name: str, data: dict, code_zip: bytes | None = None) -> dict:
    code_size = len(code_zip) if code_zip else 0
    code_sha = base64.b64encode(hashlib.sha256(code_zip).digest()).decode() if code_zip else ""

    layers_cfg = []
    for layer in data.get("Layers", []):
        if isinstance(layer, str):
            layers_cfg.append({"Arn": layer, "CodeSize": 0})
        elif isinstance(layer, dict):
            layers_cfg.append(layer)

    env = data.get("Environment")
    if env is None:
        env = {"Variables": {}}
    elif "Variables" not in env:
        env["Variables"] = {}

    return {
        "FunctionName": name,
        "FunctionArn": _func_arn(name),
        "Runtime": data.get("Runtime", "python3.9"),
        "Role": data.get("Role", f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-role"),
        "Handler": data.get("Handler", "index.handler"),
        "CodeSize": code_size,
        "CodeSha256": code_sha,
        "Description": data.get("Description", ""),
        "Timeout": data.get("Timeout", 3),
        "MemorySize": data.get("MemorySize", 128),
        "LastModified": _now_iso(),
        "Version": "$LATEST",
        "State": "Active",
        "StateReason": "",
        "StateReasonCode": "",
        "LastUpdateStatus": "Successful",
        "LastUpdateStatusReason": "",
        "LastUpdateStatusReasonCode": "",
        "PackageType": data.get("PackageType", "Zip"),
        "Architectures": data.get("Architectures", ["x86_64"]),
        "Environment": env,
        "Layers": layers_cfg,
        "TracingConfig": data.get("TracingConfig", {"Mode": "PassThrough"}),
        "VpcConfig": data.get("VpcConfig", {
            "SubnetIds": [],
            "SecurityGroupIds": [],
            "VpcId": "",
        }),
        "DeadLetterConfig": data.get("DeadLetterConfig", {"TargetArn": ""}),
        "KMSKeyArn": data.get("KMSKeyArn", ""),
        "RevisionId": new_uuid(),
        "EphemeralStorage": data.get("EphemeralStorage", {"Size": 512}),
        "SnapStart": {"ApplyOn": "None", "OptimizationStatus": "Off"},
        "LoggingConfig": data.get("LoggingConfig", {
            "LogFormat": "Text",
            "LogGroup": f"/aws/lambda/{name}",
        }),
        "RuntimeVersionConfig": {
            "RuntimeVersionArn": "",
        },
    }


def _qp_first(query_params: dict, key: str, default: str = "") -> str:
    """Return the first value for *key* from raw query_params (list or str)."""
    val = query_params.get(key, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _get_func_record_for_qualifier(name: str, qualifier: str | None) -> tuple[dict | None, dict | None]:
    """Return (func_record, effective_config) for a given name + qualifier.

    For $LATEST or None, returns the primary record/config.
    For a version number, returns the versioned snapshot.
    For an alias, resolves to the alias target version.
    """
    func = _functions.get(name)
    if func is None:
        return None, None

    if qualifier is None or qualifier == "$LATEST":
        return func, func["config"]

    if qualifier in func.get("aliases", {}):
        target_ver = func["aliases"][qualifier].get("FunctionVersion", "$LATEST")
        if target_ver == "$LATEST":
            return func, func["config"]
        ver = func["versions"].get(target_ver)
        if ver:
            return ver, ver["config"]
        return func, func["config"]

    ver = func["versions"].get(qualifier)
    if ver:
        return ver, ver["config"]

    return func, func["config"]


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------

async def handle_request(method: str, path: str, headers: dict,
                         body: bytes, query_params: dict) -> tuple:
    """Route Lambda REST API requests."""

    path = unquote(path)
    parts = path.rstrip("/").split("/")

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # --- Event Source Mappings: /2015-03-31/event-source-mappings[/{uuid}] ---
    if len(parts) >= 3 and parts[2] == "event-source-mappings":
        esm_id = parts[3] if len(parts) > 3 else None
        if method == "POST" and not esm_id:
            return _create_esm(data)
        if method == "GET" and not esm_id:
            return _list_esms(query_params)
        if method == "GET" and esm_id:
            return _get_esm(esm_id)
        if method == "PUT" and esm_id:
            return _update_esm(esm_id, data)
        if method == "DELETE" and esm_id:
            return _delete_esm(esm_id)

    # --- Tags: /2015-03-31/tags/{arn+} ---
    if len(parts) >= 3 and parts[2] == "tags":
        resource_arn = "/".join(parts[3:]) if len(parts) > 3 else ""
        if method == "GET":
            return _list_tags(resource_arn)
        if method == "POST":
            return _tag_resource(resource_arn, data)
        if method == "DELETE":
            return _untag_resource(resource_arn, query_params)

    # --- Layers: /2015-03-31/layers[/{name}[/versions[/{num}]]] ---
    if len(parts) >= 3 and parts[2] == "layers":
        if len(parts) == 3 and method == "GET":
            return _list_layers(query_params)
        layer_name = parts[3] if len(parts) > 3 else None
        if layer_name and len(parts) >= 5 and parts[4] == "versions":
            ver_str = parts[5] if len(parts) > 5 else None
            ver_num = int(ver_str) if ver_str and ver_str.isdigit() else None
            if method == "POST" and ver_num is None:
                return _publish_layer_version(layer_name, data)
            if method == "GET" and ver_num is None:
                return _list_layer_versions(layer_name, query_params)
            if method == "GET" and ver_num is not None:
                return _get_layer_version(layer_name, ver_num)
            if method == "DELETE" and ver_num is not None:
                return _delete_layer_version(layer_name, ver_num)

    # --- Event Invoke Config: /2019-09-25/functions/{name}/event-invoke-config ---
    if "event-invoke-config" in path:
        m = re.search(r"/functions/([^/]+)/event-invoke-config", path)
        fname = _resolve_name(m.group(1)) if m else ""
        if method == "GET":
            return _get_event_invoke_config(fname)
        if method == "PUT":
            return _put_event_invoke_config(fname, data)
        if method == "DELETE":
            return _delete_event_invoke_config(fname)

    # --- Provisioned Concurrency: /2019-09-30/functions/{name}/provisioned-concurrency ---
    if "provisioned-concurrency" in path:
        m = re.search(r"/functions/([^/]+)/provisioned-concurrency", path)
        fname = _resolve_name(m.group(1)) if m else ""
        qualifier = _qp_first(query_params, "Qualifier")
        if method == "GET":
            return _get_provisioned_concurrency(fname, qualifier)
        if method == "PUT":
            return _put_provisioned_concurrency(fname, qualifier, data)
        if method == "DELETE":
            return _delete_provisioned_concurrency(fname, qualifier)

    # --- Code Signing Config (stub) ---
    if "code-signing-config" in path:
        m = re.search(r"/functions/([^/]+)/code-signing-config", path)
        fname = _resolve_name(m.group(1)) if m else ""
        return json_response({"CodeSigningConfigArn": "", "FunctionName": fname})

    # --- Function URL Config ---
    if "/urls" in path and "/functions/" in path:
        m = re.search(r"/functions/([^/]+)/urls", path)
        fname = _resolve_name(m.group(1)) if m else ""
        if method == "GET":
            return _list_function_url_configs(fname, query_params)
    if "/url" in path and "/functions/" in path:
        m = re.search(r"/functions/([^/]+)/url", path)
        fname = _resolve_name(m.group(1)) if m else ""
        qualifier = _qp_first(query_params, "Qualifier") or None
        if method == "POST":
            return _create_function_url_config(fname, data, qualifier)
        if method == "GET":
            return _get_function_url_config(fname, qualifier)
        if method == "PUT":
            return _update_function_url_config(fname, data, qualifier)
        if method == "DELETE":
            return _delete_function_url_config(fname, qualifier)

    # --- Functions: /...date.../functions[/{name}[/{sub}[/{sub2}]]] ---
    if len(parts) >= 3 and parts[2] == "functions":
        if method == "POST" and len(parts) == 3:
            return _create_function(data)

        if method == "GET" and len(parts) == 3:
            return _list_functions(query_params)

        raw_name = parts[3] if len(parts) > 3 else None
        if not raw_name:
            return error_response_json("InvalidRequest", "Missing function name", 400)

        func_name, path_qualifier = _resolve_name_and_qualifier(raw_name)
        sub = parts[4] if len(parts) > 4 else None
        sub2 = parts[5] if len(parts) > 5 else None

        # Invoke
        if method == "POST" and sub == "invocations":
            return _invoke(func_name, data, headers, path_qualifier)

        # PublishVersion
        if method == "POST" and sub == "versions":
            return _publish_version(func_name, data)

        # ListVersionsByFunction: GET .../functions/{name}/versions
        if method == "GET" and sub == "versions" and sub2 is None:
            return _list_versions(func_name, query_params)

        # --- Aliases ---
        if sub == "aliases":
            alias_name = sub2
            if method == "POST" and not alias_name:
                return _create_alias(func_name, data)
            if method == "GET" and not alias_name:
                return _list_aliases(func_name, query_params)
            if method == "GET" and alias_name:
                return _get_alias(func_name, alias_name)
            if method == "PUT" and alias_name:
                return _update_alias(func_name, alias_name, data)
            if method == "DELETE" and alias_name:
                return _delete_alias(func_name, alias_name)

        # --- Policy / Permissions ---
        if sub == "policy":
            sid = sub2
            if method == "GET" and not sid:
                return _get_policy(func_name, query_params)
            if method == "POST" and not sid:
                return _add_permission(func_name, data, query_params)
            if method == "DELETE" and sid:
                return _remove_permission(func_name, sid, query_params)

        # --- Concurrency ---
        if sub == "concurrency":
            if method == "GET":
                return _get_function_concurrency(func_name)
            if method == "PUT":
                return _put_function_concurrency(func_name, data)
            if method == "DELETE":
                return _delete_function_concurrency(func_name)

        # GetFunction
        if method == "GET" and not sub:
            qualifier = path_qualifier or _qp_first(query_params, "Qualifier") or None
            return _get_function(func_name, qualifier)

        # GetFunctionConfiguration
        if method == "GET" and sub == "configuration":
            qualifier = path_qualifier or _qp_first(query_params, "Qualifier") or None
            return _get_function_config(func_name, qualifier)

        # DeleteFunction
        if method == "DELETE" and not sub:
            return _delete_function(func_name, query_params)

        # UpdateFunctionCode
        if method == "PUT" and sub == "code":
            return _update_code(func_name, data)

        # UpdateFunctionConfiguration
        if method == "PUT" and sub == "configuration":
            return _update_config(func_name, data)

        if method == "GET":
            return json_response({})

    if method == "GET":
        return json_response({})

    return error_response_json("InvalidRequest", f"Unhandled Lambda path: {path}", 400)


# ---------------------------------------------------------------------------
# Function CRUD
# ---------------------------------------------------------------------------

def _create_function(data: dict):
    name = data.get("FunctionName")
    if not name:
        return error_response_json(
            "InvalidParameterValueException", "FunctionName is required", 400,
        )
    if name in _functions:
        return error_response_json(
            "ResourceConflictException",
            f"Function already exist: {name}", 409,
        )

    code_zip = None
    code_data = data.get("Code", {})
    if "ZipFile" in code_data:
        code_zip = base64.b64decode(code_data["ZipFile"])

    config = _build_config(name, data, code_zip)

    _functions[name] = {
        "config": config,
        "code_zip": code_zip,
        "versions": {},
        "next_version": 1,
        "tags": data.get("Tags", {}),
        "policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
        "event_invoke_config": None,
        "aliases": {},
        "concurrency": None,
        "provisioned_concurrency": {},
    }

    return json_response(config, 201)


def _get_function(name: str, qualifier: str | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    func = _functions[name]
    _, effective_config = _get_func_record_for_qualifier(name, qualifier)
    if effective_config is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )

    result: dict = {
        "Configuration": effective_config,
        "Code": {"RepositoryType": "S3", "Location": ""},
        "Tags": func.get("tags", {}),
    }
    if func.get("concurrency") is not None:
        result["Concurrency"] = {
            "ReservedConcurrentExecutions": func["concurrency"],
        }
    return json_response(result)


def _get_function_config(name: str, qualifier: str | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    _, effective_config = _get_func_record_for_qualifier(name, qualifier)
    if effective_config is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    return json_response(effective_config)


def _list_functions(query_params: dict):
    all_names = sorted(_functions.keys())
    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "50"))

    start = 0
    if marker:
        for i, n in enumerate(all_names):
            if n == marker:
                start = i + 1
                break

    page = all_names[start:start + max_items]
    configs = [_functions[n]["config"] for n in page]
    result: dict = {"Functions": configs}
    if start + max_items < len(all_names):
        result["NextMarker"] = page[-1] if page else ""

    return json_response(result)


def _delete_function(name: str, query_params: dict):
    qualifier = _qp_first(query_params, "Qualifier")
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    if qualifier and qualifier != "$LATEST":
        _functions[name]["versions"].pop(qualifier, None)
    else:
        del _functions[name]
    return 204, {}, b""


def _update_code(name: str, data: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    func = _functions[name]
    if "ZipFile" in data:
        code_zip = base64.b64decode(data["ZipFile"])
        func["code_zip"] = code_zip
        func["config"]["CodeSize"] = len(code_zip)
        func["config"]["CodeSha256"] = base64.b64encode(
            hashlib.sha256(code_zip).digest(),
        ).decode()
    func["config"]["LastModified"] = _now_iso()
    func["config"]["LastUpdateStatus"] = "Successful"
    func["config"]["RevisionId"] = new_uuid()
    return json_response(func["config"])


def _update_config(name: str, data: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    config = _functions[name]["config"]
    for key in ("Runtime", "Handler", "Description", "Timeout", "MemorySize",
                "Role", "Environment", "Layers", "TracingConfig",
                "DeadLetterConfig", "KMSKeyArn", "EphemeralStorage",
                "LoggingConfig", "VpcConfig", "Architectures",
                "FileSystemConfigs", "ImageConfig"):
        if key in data:
            config[key] = data[key]
    config["LastModified"] = _now_iso()
    config["LastUpdateStatus"] = "Successful"
    config["RevisionId"] = new_uuid()
    return json_response(config)


# ---------------------------------------------------------------------------
# Invoke
# ---------------------------------------------------------------------------

def _invoke(name: str, event: dict, headers: dict,
            path_qualifier: str | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )

    func = _functions[name]
    invocation_type = (
        headers.get("x-amz-invocation-type")
        or headers.get("X-Amz-Invocation-Type")
        or "RequestResponse"
    )
    qualifier = path_qualifier or _qp_first(headers, "x-amz-qualifier") or None
    executed_version = "$LATEST"

    exec_record = func
    if qualifier and qualifier != "$LATEST":
        if qualifier in func.get("aliases", {}):
            target_ver = func["aliases"][qualifier].get("FunctionVersion", "$LATEST")
            executed_version = target_ver
            if target_ver != "$LATEST" and target_ver in func["versions"]:
                exec_record = func["versions"][target_ver]
        elif qualifier in func["versions"]:
            exec_record = func["versions"][qualifier]
            executed_version = qualifier
        else:
            return error_response_json(
                "ResourceNotFoundException",
                f"Function not found: {_func_arn(name)}:{qualifier}",
                404,
            )

    if invocation_type == "DryRun":
        return 204, {"X-Amz-Executed-Version": executed_version}, b""

    if invocation_type == "Event":
        threading.Thread(
            target=_execute_function, args=(exec_record, event), daemon=True,
        ).start()
        return 202, {"X-Amz-Executed-Version": executed_version}, b""

    # RequestResponse — synchronous execution
    result = _execute_function(exec_record, event)

    resp_headers: dict = {
        "Content-Type": "application/json",
        "X-Amz-Executed-Version": executed_version,
    }

    log_output = result.get("log", "")
    if log_output:
        resp_headers["X-Amz-Log-Result"] = base64.b64encode(
            log_output.encode("utf-8"),
        ).decode()

    if result.get("error"):
        resp_headers["X-Amz-Function-Error"] = "Unhandled"

    payload = result.get("body")
    if payload is None:
        return 200, resp_headers, b"null"
    if isinstance(payload, (str, bytes)):
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        return 200, resp_headers, raw
    return 200, resp_headers, json.dumps(payload).encode("utf-8")


# ---------------------------------------------------------------------------
# Runtime → Docker image mapping
# ---------------------------------------------------------------------------

_RUNTIME_IMAGE_MAP: dict[str, str] = {
    "python3.8": "python:3.8-slim",
    "python3.9": "python:3.9-slim",
    "python3.10": "python:3.10-slim",
    "python3.11": "python:3.11-slim",
    "python3.12": "python:3.12-slim",
    "python3.13": "python:3.13-slim",
    "nodejs14.x": "node:14-slim",
    "nodejs16.x": "node:16-slim",
    "nodejs18.x": "node:18-slim",
    "nodejs20.x": "node:20-slim",
}


def _docker_image_for_runtime(runtime: str) -> str | None:
    if runtime in _RUNTIME_IMAGE_MAP:
        return _RUNTIME_IMAGE_MAP[runtime]
    if runtime.startswith("python"):
        ver = runtime.replace("python", "")
        return f"python:{ver}-slim"
    if runtime.startswith("nodejs"):
        ver = runtime.replace("nodejs", "").rstrip(".x")
        return f"node:{ver}-slim"
    return None


# ---------------------------------------------------------------------------
# Function execution – Docker mode
# ---------------------------------------------------------------------------

def _execute_function_docker(func: dict, event: dict) -> dict:
    """Execute a Lambda function inside a Docker container.

    Mirrors the subprocess executor's interface: returns
    ``{"body": ..., "log": ..., "error": ...}``.
    """
    if not _docker_available:
        logger.warning("docker SDK unavailable – falling back to local subprocess executor")
        return _execute_function_local(func, event)

    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    handler = config["Handler"]
    runtime = config["Runtime"]
    timeout = config.get("Timeout", 3)
    env_vars = config.get("Environment", {}).get("Variables", {})

    image = _docker_image_for_runtime(runtime)
    if image is None:
        return {
            "body": {
                "statusCode": 200,
                "body": f"Mock response - {runtime} not supported for docker execution",
            },
        }

    if not runtime.startswith("python"):
        return {
            "body": {
                "statusCode": 200,
                "body": f"Mock response - {runtime} docker wrapper only supports Python runtimes",
            },
        }

    try:
        client = docker_lib.from_env()
    except Exception as exc:
        logger.warning("Cannot connect to Docker daemon (%s) – falling back to local subprocess", exc)
        return _execute_function_local(func, event)

    container = None
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Extract code zip
            zip_path = os.path.join(tmpdir, "code.zip")
            with open(zip_path, "wb") as f:
                f.write(code_zip)
            code_dir = os.path.join(tmpdir, "code")
            os.makedirs(code_dir)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(code_dir)

            # Write the docker wrapper into the code dir
            wrapper_path = os.path.join(code_dir, "_wrapper.py")
            with open(wrapper_path, "w") as wf:
                wf.write(_DOCKER_WRAPPER_SCRIPT)

            # Extract layers
            layers_dirs: list[str] = []
            for layer_ref in config.get("Layers", []):
                layer_arn_str = layer_ref if isinstance(layer_ref, str) else layer_ref.get("Arn", "")
                layer_zip = _resolve_layer_zip(layer_arn_str)
                if layer_zip:
                    layer_dir = os.path.join(tmpdir, f"layer_{len(layers_dirs)}")
                    os.makedirs(layer_dir)
                    lzip_path = os.path.join(tmpdir, f"layer_{len(layers_dirs)}.zip")
                    with open(lzip_path, "wb") as lf:
                        lf.write(layer_zip)
                    with zipfile.ZipFile(lzip_path) as lzf:
                        lzf.extractall(layer_dir)
                    layers_dirs.append(layer_dir)

            module_name, func_name = handler.rsplit(".", 1)

            # Build volume mounts
            volumes: dict = {}
            if LAMBDA_DOCKER_VOLUME_MOUNT:
                volumes[LAMBDA_DOCKER_VOLUME_MOUNT] = {"bind": "/var/task", "mode": "ro"}
            else:
                volumes[code_dir] = {"bind": "/var/task", "mode": "ro"}

            container_layer_dirs: list[str] = []
            for idx, ld in enumerate(layers_dirs):
                container_path = f"/opt/layer_{idx}"
                volumes[ld] = {"bind": container_path, "mode": "ro"}
                container_layer_dirs.append(container_path)

            # Build environment
            container_env: dict[str, str] = {
                "AWS_DEFAULT_REGION": REGION,
                "AWS_REGION": REGION,
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
                "AWS_LAMBDA_FUNCTION_NAME": config["FunctionName"],
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(config["MemorySize"]),
                "AWS_LAMBDA_FUNCTION_VERSION": config.get("Version", "$LATEST"),
                "AWS_LAMBDA_LOG_STREAM_NAME": new_uuid(),
                "_LAMBDA_HANDLER_MODULE": module_name,
                "_LAMBDA_HANDLER_FUNC": func_name,
                "_LAMBDA_FUNCTION_ARN": config["FunctionArn"],
                "_LAMBDA_TIMEOUT": str(timeout),
                "_LAMBDA_LAYERS_DIRS": ":".join(container_layer_dirs),
            }
            endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
            if endpoint:
                container_env["AWS_ENDPOINT_URL"] = endpoint
            container_env.update(env_vars)

            event_file = os.path.join(code_dir, "_event.json")
            with open(event_file, "w") as ef:
                ef.write(json.dumps(event))

            container = client.containers.run(
                image,
                command=["sh", "-c", "python3 /var/task/_wrapper.py < /var/task/_event.json"],
                environment=container_env,
                volumes=volumes,
                network_mode="host",
                detach=True,
                stdin_open=False,
            )

            result = container.wait(timeout=timeout)
            exit_code = result.get("StatusCode", -1)
            stdout = container.logs(stdout=True, stderr=False).decode("utf-8", errors="replace").strip()
            stderr = container.logs(stdout=False, stderr=True).decode("utf-8", errors="replace").strip()

            if exit_code == 0:
                if not stdout:
                    return {"body": None, "log": stderr}
                try:
                    return {"body": json.loads(stdout), "log": stderr}
                except json.JSONDecodeError:
                    return {"body": stdout, "log": stderr}
            else:
                return {
                    "body": {
                        "errorMessage": stderr or "Unknown error",
                        "errorType": "Runtime.HandlerError",
                    },
                    "error": True,
                    "log": stderr,
                }

    except Exception as exc:
        if "timed out" in str(exc).lower() or "read timed out" in str(exc).lower():
            return {
                "body": {
                    "errorMessage": f"Task timed out after {timeout}.00 seconds",
                    "errorType": "Runtime.ExitError",
                },
                "error": True,
                "log": "",
            }
        logger.error("Lambda docker execution error: %s", exc)
        return {
            "body": {"errorMessage": str(exc), "errorType": type(exc).__name__},
            "error": True,
            "log": "",
        }
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Function execution (subprocess, stdin-piped, no string interpolation)
# ---------------------------------------------------------------------------

def _execute_function(func: dict, event: dict) -> dict:
    """Dispatch to Docker or local subprocess executor."""
    if LAMBDA_EXECUTOR == "docker":
        return _execute_function_docker(func, event)
    return _execute_function_local(func, event)


def _execute_function_local(func: dict, event: dict) -> dict:
    """Execute a Python Lambda function in a subprocess.

    The event is serialised to JSON and piped through stdin.  Handler module,
    function name, and all configuration are passed via environment variables so
    there is zero string interpolation of user data into code.
    """
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    handler = config["Handler"]
    runtime = config["Runtime"]
    timeout = config.get("Timeout", 3)
    env_vars = config.get("Environment", {}).get("Variables", {})

    if not runtime.startswith("python"):
        return {
            "body": {
                "statusCode": 200,
                "body": f"Mock response - {runtime} not supported for local execution",
            },
        }

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "code.zip")
            with open(zip_path, "wb") as f:
                f.write(code_zip)
            code_dir = os.path.join(tmpdir, "code")
            os.makedirs(code_dir)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(code_dir)

            layers_dirs: list[str] = []
            for layer_ref in config.get("Layers", []):
                layer_arn_str = layer_ref if isinstance(layer_ref, str) else layer_ref.get("Arn", "")
                layer_zip = _resolve_layer_zip(layer_arn_str)
                if layer_zip:
                    layer_dir = os.path.join(tmpdir, f"layer_{len(layers_dirs)}")
                    os.makedirs(layer_dir)
                    lzip_path = os.path.join(tmpdir, f"layer_{len(layers_dirs)}.zip")
                    with open(lzip_path, "wb") as lf:
                        lf.write(layer_zip)
                    with zipfile.ZipFile(lzip_path) as lzf:
                        lzf.extractall(layer_dir)
                    layers_dirs.append(layer_dir)

            module_name, func_name = handler.rsplit(".", 1)

            wrapper_path = os.path.join(tmpdir, "_wrapper.py")
            with open(wrapper_path, "w") as wf:
                wf.write(_WRAPPER_SCRIPT)

            env = dict(os.environ)
            env.update({
                "AWS_DEFAULT_REGION": REGION,
                "AWS_REGION": REGION,
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
                "AWS_LAMBDA_FUNCTION_NAME": config["FunctionName"],
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(config["MemorySize"]),
                "AWS_LAMBDA_FUNCTION_VERSION": config.get("Version", "$LATEST"),
                "AWS_LAMBDA_LOG_STREAM_NAME": new_uuid(),
                "_LAMBDA_CODE_DIR": code_dir,
                "_LAMBDA_HANDLER_MODULE": module_name,
                "_LAMBDA_HANDLER_FUNC": func_name,
                "_LAMBDA_FUNCTION_ARN": config["FunctionArn"],
                "_LAMBDA_TIMEOUT": str(timeout),
                "_LAMBDA_LAYERS_DIRS": os.pathsep.join(layers_dirs),
            })
            endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
            if endpoint:
                env["AWS_ENDPOINT_URL"] = endpoint
            env.update(env_vars)

            proc = subprocess.run(
                ["python3", wrapper_path],
                input=json.dumps(event),
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )

            log_tail = proc.stderr.strip()

            if proc.returncode == 0:
                stdout = proc.stdout.strip()
                if not stdout:
                    return {"body": None, "log": log_tail}
                try:
                    return {"body": json.loads(stdout), "log": log_tail}
                except json.JSONDecodeError:
                    return {"body": stdout, "log": log_tail}
            else:
                return {
                    "body": {
                        "errorMessage": log_tail or "Unknown error",
                        "errorType": "Runtime.HandlerError",
                    },
                    "error": True,
                    "log": log_tail,
                }

    except subprocess.TimeoutExpired:
        return {
            "body": {
                "errorMessage": f"Task timed out after {timeout}.00 seconds",
                "errorType": "Runtime.ExitError",
            },
            "error": True,
            "log": "",
        }
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        return {
            "body": {"errorMessage": str(e), "errorType": type(e).__name__},
            "error": True,
            "log": "",
        }


def _resolve_layer_zip(layer_arn_str: str) -> bytes | None:
    """Given a layer version ARN return the stored zip bytes, or None."""
    segs = layer_arn_str.split(":")
    if len(segs) < 8:
        return None
    layer_name = segs[6]
    try:
        version = int(segs[7])
    except (ValueError, IndexError):
        return None
    layer = _layers.get(layer_name)
    if not layer:
        return None
    for v in layer["versions"]:
        if v["Version"] == version:
            return v.get("_zip_data")
    return None


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

def _publish_version(name: str, data: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    func = _functions[name]
    ver_num = func["next_version"]
    func["next_version"] = ver_num + 1

    ver_config = copy.deepcopy(func["config"])
    ver_config["Version"] = str(ver_num)
    ver_config["FunctionArn"] = f"{_func_arn(name)}:{ver_num}"
    ver_config["RevisionId"] = new_uuid()
    if data.get("Description"):
        ver_config["Description"] = data["Description"]

    func["versions"][str(ver_num)] = {
        "config": ver_config,
        "code_zip": func.get("code_zip"),
    }
    return json_response(ver_config, 201)


def _list_versions(name: str, query_params: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}", 404,
        )
    func = _functions[name]
    versions = [func["config"]]
    for vnum in sorted(func["versions"].keys(), key=int):
        versions.append(func["versions"][vnum]["config"])

    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "50"))
    start = 0
    if marker:
        for i, v in enumerate(versions):
            if v["Version"] == marker:
                start = i + 1
                break

    page = versions[start:start + max_items]
    result: dict = {"Versions": page}
    if start + max_items < len(versions):
        result["NextMarker"] = page[-1]["Version"] if page else ""
    return json_response(result)


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------

def _create_alias(func_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    alias_name = data.get("Name", "")
    if not alias_name:
        return error_response_json(
            "InvalidParameterValueException", "Alias name is required", 400,
        )
    func = _functions[func_name]
    if alias_name in func["aliases"]:
        return error_response_json(
            "ResourceConflictException",
            f"Alias already exists: {alias_name}", 409,
        )

    alias: dict = {
        "AliasArn": f"{_func_arn(func_name)}:{alias_name}",
        "Name": alias_name,
        "FunctionVersion": data.get("FunctionVersion", "$LATEST"),
        "Description": data.get("Description", ""),
        "RevisionId": new_uuid(),
    }
    rc = data.get("RoutingConfig")
    if rc:
        alias["RoutingConfig"] = rc
    func["aliases"][alias_name] = alias
    return json_response(alias, 201)


def _get_alias(func_name: str, alias_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    alias = _functions[func_name]["aliases"].get(alias_name)
    if not alias:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}", 404,
        )
    return json_response(alias)


def _update_alias(func_name: str, alias_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    alias = _functions[func_name]["aliases"].get(alias_name)
    if not alias:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}", 404,
        )
    for key in ("FunctionVersion", "Description", "RoutingConfig"):
        if key in data:
            alias[key] = data[key]
    alias["RevisionId"] = new_uuid()
    return json_response(alias)


def _delete_alias(func_name: str, alias_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    if alias_name not in _functions[func_name]["aliases"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}", 404,
        )
    del _functions[func_name]["aliases"][alias_name]
    return 204, {}, b""


def _list_aliases(func_name: str, query_params: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    aliases = list(_functions[func_name]["aliases"].values())

    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "50"))
    start = 0
    if marker:
        for i, a in enumerate(aliases):
            if a["Name"] == marker:
                start = i + 1
                break
    page = aliases[start:start + max_items]
    result: dict = {"Aliases": page}
    if start + max_items < len(aliases):
        result["NextMarker"] = page[-1]["Name"] if page else ""
    return json_response(result)


# ---------------------------------------------------------------------------
# Permissions / Policy  (required by Terraform aws_lambda_permission)
# ---------------------------------------------------------------------------

def _add_permission(func_name: str, data: dict, query_params: dict | None = None):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    func = _functions[func_name]
    sid = data.get("StatementId", new_uuid())

    for stmt in func["policy"]["Statement"]:
        if stmt.get("Sid") == sid:
            return error_response_json(
                "ResourceConflictException",
                f"The statement id ({sid}) provided already exists. "
                "Please provide a new statement id, or remove the existing statement.",
                409,
            )

    principal_raw = data.get("Principal", "")
    if "amazonaws.com" in principal_raw:
        principal = {"Service": principal_raw}
    elif principal_raw == "*":
        principal = "*"
    else:
        principal = {"AWS": principal_raw}

    qualifier = (query_params or {}).get("Qualifier") if query_params else None
    if isinstance(qualifier, list):
        qualifier = qualifier[0] if qualifier else None
    resource_arn = _func_arn(func_name)
    if qualifier:
        resource_arn = f"{resource_arn}:{qualifier}"

    statement: dict = {
        "Sid": sid,
        "Effect": "Allow",
        "Principal": principal,
        "Action": data.get("Action", "lambda:InvokeFunction"),
        "Resource": resource_arn,
    }
    condition: dict = {}
    if "SourceArn" in data:
        condition["ArnLike"] = {"AWS:SourceArn": data["SourceArn"]}
    if "SourceAccount" in data:
        condition["StringEquals"] = {"AWS:SourceAccount": data["SourceAccount"]}
    if "PrincipalOrgID" in data:
        condition.setdefault("StringEquals", {})["aws:PrincipalOrgID"] = data["PrincipalOrgID"]
    if "FunctionUrlAuthType" in data:
        condition.setdefault("StringEquals", {})["lambda:FunctionUrlAuthType"] = data["FunctionUrlAuthType"]
    if condition:
        statement["Condition"] = condition

    func["policy"]["Statement"].append(statement)
    return json_response({"Statement": json.dumps(statement)}, 201)


def _remove_permission(func_name: str, sid: str, query_params: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    func = _functions[func_name]
    before = len(func["policy"]["Statement"])
    func["policy"]["Statement"] = [
        s for s in func["policy"]["Statement"] if s.get("Sid") != sid
    ]
    if len(func["policy"]["Statement"]) == before:
        return error_response_json(
            "ResourceNotFoundException",
            "No policy is associated with the given resource.", 404,
        )
    return 204, {}, b""


def _get_policy(func_name: str, query_params: dict | None = None):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    func = _functions[func_name]
    if not func["policy"]["Statement"]:
        return error_response_json(
            "ResourceNotFoundException",
            "The resource you requested does not exist.", 404,
        )
    return json_response({
        "Policy": json.dumps(func["policy"]),
        "RevisionId": func["config"]["RevisionId"],
    })


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _list_tags(resource_arn: str):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}", 404,
        )
    return json_response({"Tags": _functions[func_name].get("tags", {})})


def _tag_resource(resource_arn: str, data: dict):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}", 404,
        )
    _functions[func_name].setdefault("tags", {}).update(data.get("Tags", {}))
    return 204, {}, b""


def _untag_resource(resource_arn: str, query_params: dict):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}", 404,
        )
    raw = query_params.get("tagKeys", query_params.get("TagKeys", []))
    if isinstance(raw, list):
        tag_keys = raw
    elif isinstance(raw, str):
        tag_keys = [raw]
    else:
        tag_keys = []
    tags = _functions[func_name].setdefault("tags", {})
    for k in tag_keys:
        tags.pop(k.strip(), None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Layers
# ---------------------------------------------------------------------------

def _publish_layer_version(layer_name: str, data: dict):
    if layer_name not in _layers:
        _layers[layer_name] = {"versions": [], "next_version": 1}
    layer = _layers[layer_name]
    ver = layer["next_version"]
    layer["next_version"] = ver + 1

    zip_data = None
    content = data.get("Content", {})
    if "ZipFile" in content:
        zip_data = base64.b64decode(content["ZipFile"])

    ver_config: dict = {
        "LayerArn": _layer_arn(layer_name),
        "LayerVersionArn": f"{_layer_arn(layer_name)}:{ver}",
        "Version": ver,
        "Description": data.get("Description", ""),
        "CompatibleRuntimes": data.get("CompatibleRuntimes", []),
        "CompatibleArchitectures": data.get("CompatibleArchitectures", []),
        "LicenseInfo": data.get("LicenseInfo", ""),
        "CreatedDate": _now_iso(),
        "Content": {
            "Location": "",
            "CodeSha256": (
                base64.b64encode(hashlib.sha256(zip_data).digest()).decode()
                if zip_data else ""
            ),
            "CodeSize": len(zip_data) if zip_data else 0,
        },
        "_zip_data": zip_data,
    }
    layer["versions"].append(ver_config)
    out = {k: v for k, v in ver_config.items() if not k.startswith("_")}
    return json_response(out, 201)


def _list_layer_versions(layer_name: str, query_params: dict):
    layer = _layers.get(layer_name)
    if not layer:
        return json_response({"LayerVersions": []})
    versions = [
        {k: v for k, v in vc.items() if not k.startswith("_")}
        for vc in layer["versions"]
    ]
    return json_response({"LayerVersions": versions})


def _get_layer_version(layer_name: str, version: int):
    layer = _layers.get(layer_name)
    if not layer:
        return error_response_json(
            "ResourceNotFoundException", f"Layer {layer_name} not found", 404,
        )
    for vc in layer["versions"]:
        if vc["Version"] == version:
            out = {k: v for k, v in vc.items() if not k.startswith("_")}
            return json_response(out)
    return error_response_json(
        "ResourceNotFoundException",
        f"Layer version {version} not found for {layer_name}", 404,
    )


def _delete_layer_version(layer_name: str, version: int):
    layer = _layers.get(layer_name)
    if not layer:
        return 204, {}, b""
    layer["versions"] = [vc for vc in layer["versions"] if vc["Version"] != version]
    return 204, {}, b""


def _list_layers(query_params: dict):
    result = []
    for name, layer in _layers.items():
        if layer["versions"]:
            latest = layer["versions"][-1]
            result.append({
                "LayerName": name,
                "LayerArn": _layer_arn(name),
                "LatestMatchingVersion": {
                    k: v for k, v in latest.items() if not k.startswith("_")
                },
            })
    return json_response({"Layers": result})


# ---------------------------------------------------------------------------
# Event Invoke Config (stubs — enough for Terraform to not error)
# ---------------------------------------------------------------------------

def _get_event_invoke_config(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    eic = _functions[func_name].get("event_invoke_config")
    if not eic:
        return error_response_json(
            "ResourceNotFoundException",
            f"The function {func_name} doesn't have an EventInvokeConfig", 404,
        )
    return json_response(eic)


def _put_event_invoke_config(func_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    eic = {
        "FunctionArn": _func_arn(func_name),
        "MaximumRetryAttempts": data.get("MaximumRetryAttempts", 2),
        "MaximumEventAgeInSeconds": data.get("MaximumEventAgeInSeconds", 21600),
        "LastModified": time.time(),
        "DestinationConfig": data.get("DestinationConfig", {
            "OnSuccess": {},
            "OnFailure": {},
        }),
    }
    _functions[func_name]["event_invoke_config"] = eic
    return json_response(eic)


def _delete_event_invoke_config(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    _functions[func_name]["event_invoke_config"] = None
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Concurrency (reserved)
# ---------------------------------------------------------------------------

def _get_function_concurrency(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    conc = _functions[func_name].get("concurrency")
    if conc is None:
        return json_response({"ReservedConcurrentExecutions": 0})
    return json_response({"ReservedConcurrentExecutions": conc})


def _put_function_concurrency(func_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    value = data.get("ReservedConcurrentExecutions", 0)
    _functions[func_name]["concurrency"] = value
    return json_response({"ReservedConcurrentExecutions": value})


def _delete_function_concurrency(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    _functions[func_name]["concurrency"] = None
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Provisioned Concurrency (stubs)
# ---------------------------------------------------------------------------

def _get_provisioned_concurrency(func_name: str, qualifier: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    key = qualifier or "$LATEST"
    pc = _functions[func_name].get("provisioned_concurrency", {}).get(key)
    if not pc:
        return error_response_json(
            "ProvisionedConcurrencyConfigNotFoundException",
            f"No Provisioned Concurrency Config found for function: {_func_arn(func_name)}", 404,
        )
    return json_response(pc)


def _put_provisioned_concurrency(func_name: str, qualifier: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    key = qualifier or "$LATEST"
    requested = data.get("ProvisionedConcurrentExecutions", 0)
    pc = {
        "RequestedProvisionedConcurrentExecutions": requested,
        "AvailableProvisionedConcurrentExecutions": requested,
        "AllocatedProvisionedConcurrentExecutions": requested,
        "Status": "READY",
        "LastModified": _now_iso(),
    }
    _functions[func_name].setdefault("provisioned_concurrency", {})[key] = pc
    return json_response(pc, 202)


def _delete_provisioned_concurrency(func_name: str, qualifier: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    key = qualifier or "$LATEST"
    _functions[func_name].get("provisioned_concurrency", {}).pop(key, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Event Source Mappings
# ---------------------------------------------------------------------------

def _create_esm(data: dict):
    esm_id = new_uuid()
    func_name = _resolve_name(data.get("FunctionName", ""))

    esm = {
        "UUID": esm_id,
        "EventSourceArn": data.get("EventSourceArn", ""),
        "FunctionArn": _func_arn(func_name),
        "FunctionName": func_name,
        "State": "Enabled",
        "StateTransitionReason": "USER_INITIATED",
        "BatchSize": data.get("BatchSize", 10),
        "MaximumBatchingWindowInSeconds": data.get("MaximumBatchingWindowInSeconds", 0),
        "LastModified": time.time(),
        "LastProcessingResult": "No records processed",
        "StartingPosition": data.get("StartingPosition", "LATEST"),
        "Enabled": True,
        "FunctionResponseTypes": data.get("FunctionResponseTypes", []),
    }
    _esms[esm_id] = esm
    _ensure_poller()
    return json_response(esm, 202)


def _get_esm(esm_id: str):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"The resource you requested does not exist. (Service: Lambda, "
            f"Status Code: 404, Request ID: {new_uuid()})",
            404,
        )
    return json_response(esm)


def _list_esms(query_params: dict):
    func = _resolve_name(_qp_first(query_params, "FunctionName"))
    source_arn = _qp_first(query_params, "EventSourceArn")
    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "100"))

    result = list(_esms.values())
    if func:
        result = [e for e in result if e["FunctionName"] == func]
    if source_arn:
        result = [e for e in result if e["EventSourceArn"] == source_arn]

    start = 0
    if marker:
        for i, e in enumerate(result):
            if e["UUID"] == marker:
                start = i + 1
                break

    page = result[start:start + max_items]
    resp: dict = {"EventSourceMappings": page}
    if start + max_items < len(result):
        resp["NextMarker"] = page[-1]["UUID"] if page else ""
    return json_response(resp)


def _update_esm(esm_id: str, data: dict):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"Event source mapping not found: {esm_id}", 404,
        )
    for key in ("BatchSize", "MaximumBatchingWindowInSeconds",
                "FunctionResponseTypes", "MaximumRetryAttempts",
                "MaximumRecordAgeInSeconds", "BisectBatchOnFunctionError",
                "ParallelizationFactor", "DestinationConfig",
                "FilterCriteria"):
        if key in data:
            esm[key] = data[key]
    if "Enabled" in data:
        esm["Enabled"] = data["Enabled"]
        esm["State"] = "Enabled" if data["Enabled"] else "Disabled"
    if "FunctionName" in data:
        new_name = _resolve_name(data["FunctionName"])
        esm["FunctionName"] = new_name
        esm["FunctionArn"] = _func_arn(new_name)
    esm["LastModified"] = time.time()
    return json_response(esm)


def _delete_esm(esm_id: str):
    esm = _esms.pop(esm_id, None)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"Event source mapping not found: {esm_id}", 404,
        )
    esm["State"] = "Deleting"
    return json_response(esm, 202)


# ---------------------------------------------------------------------------
# SQS ESM Poller (kept as-is — works well)
# ---------------------------------------------------------------------------

def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    """Background thread: polls SQS queues for active ESMs and invokes Lambda."""
    while True:
        try:
            _poll_once()
        except Exception as e:
            logger.error(f"ESM poller error: {e}")
        time.sleep(1)


def _poll_once():
    from services import sqs as _sqs

    for esm in list(_esms.values()):
        if not esm.get("Enabled", True):
            continue

        source_arn = esm.get("EventSourceArn", "")
        if "sqs" not in source_arn:
            continue

        func_name = esm["FunctionName"]
        if func_name not in _functions:
            continue

        queue_name = source_arn.split(":")[-1]
        queue_url = _sqs._queue_url(queue_name)
        queue = _sqs._queues.get(queue_url)
        if not queue:
            continue

        batch_size = esm.get("BatchSize", 10)
        now = time.time()

        batch = []
        for msg in list(queue["messages"]):
            if len(batch) >= batch_size:
                break
            if msg["visible_at"] <= now and msg.get("receipt_handle") is None:
                batch.append(msg)

        if not batch:
            continue

        for msg in batch:
            msg["receipt_handle"] = new_uuid()
            msg["visible_at"] = now + 30

        records = []
        for msg in batch:
            records.append({
                "messageId": msg["id"],
                "receiptHandle": msg["receipt_handle"],
                "body": msg["body"],
                "attributes": {
                    "ApproximateReceiveCount": str(msg.get("receive_count", 1)),
                    "SentTimestamp": str(int(msg["sent_at"] * 1000)),
                    "SenderId": ACCOUNT_ID,
                    "ApproximateFirstReceiveTimestamp": str(int(now * 1000)),
                },
                "messageAttributes": msg.get("attributes", {}),
                "md5OfBody": msg["md5"],
                "eventSource": "aws:sqs",
                "eventSourceARN": source_arn,
                "awsRegion": REGION,
            })

        event = {"Records": records}
        result = _execute_function(_functions[func_name], event)

        if result.get("error"):
            for msg in batch:
                msg["receipt_handle"] = None
                msg["visible_at"] = now
            esm["LastProcessingResult"] = "FAILED"
            logger.warning(
                f"ESM: Lambda {func_name} failed processing batch from {queue_name}",
            )
        else:
            receipt_handles = {msg["receipt_handle"] for msg in batch}
            queue["messages"] = [
                m for m in queue["messages"]
                if m.get("receipt_handle") not in receipt_handles
            ]
            esm["LastProcessingResult"] = f"OK - {len(batch)} records"
            logger.info(
                f"ESM: Lambda {func_name} processed {len(batch)} messages from {queue_name}",
            )


# ---------------------------------------------------------------------------
# Function URL Config
# ---------------------------------------------------------------------------

def _url_config_key(func_name: str, qualifier: str | None) -> str:
    return f"{func_name}:{qualifier}" if qualifier else func_name


def _create_function_url_config(func_name: str, data: dict, qualifier: str | None):
    if func_name not in _functions:
        return error_response_json("ResourceNotFoundException",
                                   f"Function not found: {_func_arn(func_name)}", 404)
    key = _url_config_key(func_name, qualifier)
    if key in _function_urls:
        return error_response_json("ResourceConflictException",
                                   f"Function URL config already exists for {func_name}", 409)
    cfg = {
        "FunctionUrl": f"https://{new_uuid()}.lambda-url.us-east-1.on.aws/",
        "FunctionArn": _func_arn(func_name),
        "AuthType": data.get("AuthType", "NONE"),
        "Cors": data.get("Cors", {}),
        "CreationTime": _now_iso(),
        "LastModifiedTime": _now_iso(),
    }
    _function_urls[key] = cfg
    return json_response(cfg, status=201)


def _get_function_url_config(func_name: str, qualifier: str | None):
    key = _url_config_key(func_name, qualifier)
    cfg = _function_urls.get(key)
    if not cfg:
        return error_response_json("ResourceNotFoundException",
                                   f"Function URL config not found for {func_name}", 404)
    return json_response(cfg)


def _update_function_url_config(func_name: str, data: dict, qualifier: str | None):
    key = _url_config_key(func_name, qualifier)
    cfg = _function_urls.get(key)
    if not cfg:
        return error_response_json("ResourceNotFoundException",
                                   f"Function URL config not found for {func_name}", 404)
    if "AuthType" in data:
        cfg["AuthType"] = data["AuthType"]
    if "Cors" in data:
        cfg["Cors"] = data["Cors"]
    cfg["LastModifiedTime"] = _now_iso()
    return json_response(cfg)


def _delete_function_url_config(func_name: str, qualifier: str | None):
    key = _url_config_key(func_name, qualifier)
    if key not in _function_urls:
        return error_response_json("ResourceNotFoundException",
                                   f"Function URL config not found for {func_name}", 404)
    del _function_urls[key]
    return json_response({}, status=204)


def _list_function_url_configs(func_name: str, query_params: dict):
    configs = [v for k, v in _function_urls.items()
               if k == func_name or k.startswith(f"{func_name}:")]
    return json_response({"FunctionUrlConfigs": configs})


def reset():
    from core import lambda_runtime
    _functions.clear()
    _layers.clear()
    _esms.clear()
    _function_urls.clear()
    lambda_runtime.reset()
