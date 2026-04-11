"""
Lambda Service Emulator.
Supports: CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration,
          ListFunctions (paginated with Marker/MaxItems), Invoke (RequestResponse / Event / DryRun),
          UpdateFunctionCode, UpdateFunctionConfiguration,
          PublishVersion, ListVersionsByFunction,
          CreateAlias, GetAlias, UpdateAlias, DeleteAlias, ListAliases,
          AddPermission, RemovePermission, GetPolicy,
          ListTags, TagResource, UntagResource,
          PublishLayerVersion, GetLayerVersion, GetLayerVersionByArn,
          ListLayerVersions, DeleteLayerVersion, ListLayers,
          AddLayerVersionPermission, RemoveLayerVersionPermission,
          GetLayerVersionPolicy,
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

import asyncio
import base64
import copy
import hashlib
import importlib
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
import zipfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid
from ministack.core.lambda_runtime import get_or_create_worker, invalidate_worker

logger = logging.getLogger("lambda")

REGION = os.environ.get("MINISTACK_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
LAMBDA_EXECUTOR = os.environ.get("LAMBDA_EXECUTOR", "local").lower()
LAMBDA_DOCKER_VOLUME_MOUNT = os.environ.get("LAMBDA_REMOTE_DOCKER_VOLUME_MOUNT", "")
LAMBDA_DOCKER_NETWORK = os.environ.get("LAMBDA_DOCKER_NETWORK", "")

try:
    docker_lib: Any = importlib.import_module("docker")
    _docker_available = True
except ImportError:
    docker_lib = None
    _docker_available = False

_functions = AccountScopedDict()  # function_name -> FunctionRecord
_layers = AccountScopedDict()  # layer_name -> {"versions": [...], "next_version": int}
_esms = AccountScopedDict()  # uuid -> esm dict
_function_urls = AccountScopedDict()  # function_name -> FunctionUrlConfig dict
_poller_started = False
_poller_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    """Return JSON-serializable state. code_zip bytes are base64-encoded."""
    from ministack.core.responses import AccountScopedDict
    funcs = AccountScopedDict()
    # Iterate _data directly to capture ALL accounts, not just current request context
    for scoped_key, func in _functions._data.items():
        f = copy.deepcopy(func)
        if f.get("code_zip") and isinstance(f["code_zip"], bytes):
            f["code_zip"] = base64.b64encode(f["code_zip"]).decode()
        for ver in f.get("versions", {}).values():
            if ver.get("code_zip") and isinstance(ver["code_zip"], bytes):
                ver["code_zip"] = base64.b64encode(ver["code_zip"]).decode()
        funcs._data[scoped_key] = f
    return {
        "functions": funcs,
        "layers": copy.deepcopy(_layers),
        "esms": copy.deepcopy(_esms),
        "function_urls": copy.deepcopy(_function_urls),
    }


def restore_state(data):
    if data:
        from ministack.core.responses import AccountScopedDict
        funcs = data.get("functions", {})
        if isinstance(funcs, AccountScopedDict):
            for scoped_key, func in funcs._data.items():
                if func.get("code_zip") and isinstance(func["code_zip"], str):
                    func["code_zip"] = base64.b64decode(func["code_zip"])
                for ver in func.get("versions", {}).values():
                    if ver.get("code_zip") and isinstance(ver["code_zip"], str):
                        ver["code_zip"] = base64.b64decode(ver["code_zip"])
                _functions._data[scoped_key] = func
        else:
            for name, func in funcs.items():
                if func.get("code_zip") and isinstance(func["code_zip"], str):
                    func["code_zip"] = base64.b64decode(func["code_zip"])
                for ver in func.get("versions", {}).values():
                    if ver.get("code_zip") and isinstance(ver["code_zip"], str):
                        ver["code_zip"] = base64.b64decode(ver["code_zip"])
                _functions[name] = func
        _layers.update(data.get("layers", {}))
        _esms.update(data.get("esms", {}))
        _function_urls.update(data.get("function_urls", {}))


_restored = load_state("lambda")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Wrapper script executed inside the subprocess.
# All configuration is passed through env vars; event data arrives on stdin.
# ---------------------------------------------------------------------------
_WRAPPER_SCRIPT = """\
import sys, os, json

sys.path.insert(0, os.environ["_LAMBDA_CODE_DIR"])

_REAL_STDOUT = sys.__stdout__
# Match AWS Lambda semantics: logs go to CloudWatch (stderr here),
# while the Invoke response payload must be clean JSON on stdout.
sys.stdout = sys.stderr

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
    _REAL_STDOUT.write(json.dumps(_result))
    _REAL_STDOUT.flush()
"""

# Docker variant: paths fixed to /var/task (code) and /opt (layers).
_DOCKER_WRAPPER_SCRIPT = """\
import sys, os, json

sys.path.insert(0, "/var/task")

_REAL_STDOUT = sys.__stdout__
# Match AWS Lambda semantics: logs go to CloudWatch (stderr here),
# while the Invoke response payload must be clean JSON on stdout.
sys.stdout = sys.stderr

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
    _REAL_STDOUT.write(json.dumps(_result))
    _REAL_STDOUT.flush()
"""


# Node.js wrapper — written to the code dir and executed with `node`.
# Reads event from stdin, calls handler, writes JSON result to stdout.
_NODE_WRAPPER_SCRIPT = """\
const fs = require('fs');
const path = require('path');

const codeDir = process.env._LAMBDA_CODE_DIR || '/var/task';
const modPath  = process.env._LAMBDA_HANDLER_MODULE;
const fnName   = process.env._LAMBDA_HANDLER_FUNC;

// Prepend layer dirs to NODE_PATH
const layerDirs = (process.env._LAMBDA_LAYERS_DIRS || '').split(path.delimiter).filter(Boolean);
const nodePaths = layerDirs.map(d => path.join(d, 'nodejs', 'node_modules'))
                           .concat(layerDirs)
                           .concat([path.join(codeDir, 'node_modules'), codeDir]);
module.paths.unshift(...nodePaths);

const context = {
  functionName:       process.env.AWS_LAMBDA_FUNCTION_NAME || '',
  memoryLimitInMB:    process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || '128',
  invokedFunctionArn: process.env._LAMBDA_FUNCTION_ARN || '',
  awsRequestId:       process.env.AWS_LAMBDA_LOG_STREAM_NAME || '',
  logGroupName:       '/aws/lambda/' + (process.env.AWS_LAMBDA_FUNCTION_NAME || ''),
  logStreamName:      process.env.AWS_LAMBDA_LOG_STREAM_NAME || '',
  getRemainingTimeInMillis: () => parseFloat(process.env._LAMBDA_TIMEOUT || '3') * 1000,
};

let input = '';
process.stdin.on('data', d => input += d);
process.stdin.on('end', () => {
  const event = JSON.parse(input);
  const mod = require(path.resolve(codeDir, modPath));
  const handler = mod[fnName];
  Promise.resolve(handler(event, context)).then(result => {
    if (result !== undefined) process.stdout.write(JSON.stringify(result));
  }).catch(err => {
    process.stderr.write(String(err.stack || err));
    process.exit(1);
  });
});
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
    return f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{name}"


def _layer_arn(name: str) -> str:
    return f"arn:aws:lambda:{REGION}:{get_account_id()}:layer:{name}"


def _now_iso() -> str:
    now = datetime.now(timezone.utc)
    ms = now.microsecond // 1000
    return now.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}+0000")


def _normalize_endpoint_url(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("http://") or v.startswith("https://"):
        return v
    host = v.rstrip("/")
    if ":" not in host:
        host = f"{host}:4566"
    return f"http://{host}"


def _fetch_code_from_s3(bucket: str, key: str) -> bytes | None:
    """Fetch Lambda code zip from the in-memory S3 service."""
    try:
        from ministack.services import s3 as s3_svc
        obj = s3_svc._get_object_data(bucket, key)
        if obj is not None:
            return obj
    except Exception as e:
        logger.warning("Failed to fetch Lambda code from s3://%s/%s: %s", bucket, key, e)
    return None


def _build_config(name: str, data: dict, code_zip: bytes | None = None) -> dict:
    code_size = len(code_zip) if code_zip else 0
    code_sha = base64.b64encode(hashlib.sha256(code_zip).digest()).decode() if code_zip else ""
    is_image = data.get("PackageType", "Zip") == "Image"

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
        "Runtime": data.get("Runtime", "" if is_image else "python3.9"),
        "Role": data.get("Role", f"arn:aws:iam::{get_account_id()}:role/lambda-role"),
        "Handler": data.get("Handler", "" if is_image else "index.handler"),
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
        "VpcConfig": data.get(
            "VpcConfig",
            {
                "SubnetIds": [],
                "SecurityGroupIds": [],
                "VpcId": "",
            },
        ),
        "DeadLetterConfig": data.get("DeadLetterConfig", {}),
        "KMSKeyArn": data.get("KMSKeyArn", ""),
        "RevisionId": new_uuid(),
        "EphemeralStorage": data.get("EphemeralStorage", {"Size": 512}),
        "SnapStart": {"ApplyOn": "None", "OptimizationStatus": "Off"},
        "LoggingConfig": data.get(
            "LoggingConfig",
            {
                "LogFormat": "Text",
                "LogGroup": f"/aws/lambda/{name}",
            },
        ),
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


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
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

    # --- Layers: /2015-03-31/layers[/{name}[/versions[/{num}[/policy[/{sid}]]]]] ---
    if len(parts) >= 3 and parts[2] == "layers":
        if len(parts) == 3 and method == "GET":
            # GetLayerVersionByArn: GET /layers?find=LayerVersion&Arn=...
            find = _qp_first(query_params, "find")
            if find == "LayerVersion":
                arn = _qp_first(query_params, "Arn")
                return _get_layer_version_by_arn(arn)
            return _list_layers(query_params)
        layer_name = parts[3] if len(parts) > 3 else None
        if layer_name and len(parts) >= 5 and parts[4] == "versions":
            ver_str = parts[5] if len(parts) > 5 else None
            ver_num = int(ver_str) if ver_str and ver_str.isdigit() else None
            if method == "POST" and ver_num is None:
                return _publish_layer_version(layer_name, data)
            if method == "GET" and ver_num is None:
                return _list_layer_versions(layer_name, query_params)
            if ver_num is not None:
                # Check for policy sub-resource: .../versions/{num}/policy[/{sid}]
                policy_sub = parts[6] if len(parts) > 6 else None
                if policy_sub == "policy":
                    policy_sid = parts[7] if len(parts) > 7 else None
                    if method == "POST" and not policy_sid:
                        return _add_layer_version_permission(layer_name, ver_num, data)
                    if method == "GET" and not policy_sid:
                        return _get_layer_version_policy(layer_name, ver_num)
                    if method == "DELETE" and policy_sid:
                        return _remove_layer_version_permission(layer_name, ver_num, policy_sid)
                if method == "GET":
                    return _get_layer_version(layer_name, ver_num)
                if method == "DELETE":
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
            return error_response_json("InvalidParameterValueException", "Missing function name", 400)

        func_name, path_qualifier = _resolve_name_and_qualifier(raw_name)
        sub = parts[4] if len(parts) > 4 else None
        sub2 = parts[5] if len(parts) > 5 else None

        # Invoke
        if method == "POST" and sub == "invocations":
            return await _invoke(func_name, data, headers, path_qualifier)

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

    return error_response_json("ResourceNotFoundException", f"Function not found: {path}", 404)


# ---------------------------------------------------------------------------
# Function CRUD
# ---------------------------------------------------------------------------


def _create_function(data: dict):
    name = data.get("FunctionName")
    if not name:
        return error_response_json(
            "InvalidParameterValueException",
            "FunctionName is required",
            400,
        )
    if name in _functions:
        return error_response_json(
            "ResourceConflictException",
            f"Function already exist: {name}",
            409,
        )

    code_zip = None
    image_uri = None
    code_data = data.get("Code", {})
    if "ImageUri" in code_data:
        image_uri = code_data["ImageUri"]
    elif "ZipFile" in code_data:
        code_zip = base64.b64decode(code_data["ZipFile"])
    elif "S3Bucket" in code_data and "S3Key" in code_data:
        code_zip = _fetch_code_from_s3(code_data["S3Bucket"], code_data["S3Key"])

    if image_uri:
        data.setdefault("PackageType", "Image")

    config = _build_config(name, data, code_zip)
    if image_uri:
        config["ImageUri"] = image_uri
        config["PackageType"] = "Image"
        if "ImageConfig" in data:
            config["ImageConfigResponse"] = {"ImageConfig": data["ImageConfig"]}

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

    if data.get("Publish"):
        ver_num = _functions[name]["next_version"]
        _functions[name]["next_version"] = ver_num + 1
        ver_config = copy.deepcopy(config)
        ver_config["Version"] = str(ver_num)
        _functions[name]["versions"][str(ver_num)] = {
            "config": ver_config,
            "code_zip": code_zip,
        }
        config["Version"] = str(ver_num)

    return json_response(config, 201)


def _get_function(name: str, qualifier: str | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )
    func = _functions[name]
    _, effective_config = _get_func_record_for_qualifier(name, qualifier)
    if effective_config is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )

    if effective_config.get("PackageType") == "Image" and effective_config.get("ImageUri"):
        code_info = {"RepositoryType": "ECR", "ImageUri": effective_config["ImageUri"]}
    else:
        code_info = {"RepositoryType": "S3", "Location": ""}
    result: dict = {
        "Configuration": effective_config,
        "Code": code_info,
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
            f"Function not found: {_func_arn(name)}",
            404,
        )
    _, effective_config = _get_func_record_for_qualifier(name, qualifier)
    if effective_config is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
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

    page = all_names[start : start + max_items]
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
            f"Function not found: {_func_arn(name)}",
            404,
        )
    if qualifier and qualifier != "$LATEST":
        _functions[name]["versions"].pop(qualifier, None)
    else:
        del _functions[name]
        invalidate_worker(name)
    return 204, {}, b""


def _update_code(name: str, data: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )
    func = _functions[name]
    code_zip = None
    if "ImageUri" in data:
        func["config"]["ImageUri"] = data["ImageUri"]
        func["config"]["PackageType"] = "Image"
    elif "ZipFile" in data:
        code_zip = base64.b64decode(data["ZipFile"])
    elif "S3Bucket" in data and "S3Key" in data:
        code_zip = _fetch_code_from_s3(data["S3Bucket"], data["S3Key"])
        if code_zip is None:
            return error_response_json(
                "InvalidParameterValueException",
                f"Failed to fetch code from s3://{data['S3Bucket']}/{data['S3Key']}",
                400,
            )
    if code_zip:
        func["code_zip"] = code_zip
        func["config"]["CodeSize"] = len(code_zip)
        func["config"]["CodeSha256"] = base64.b64encode(
            hashlib.sha256(code_zip).digest(),
        ).decode()
    func["config"]["LastModified"] = _now_iso()
    func["config"]["LastUpdateStatus"] = "Successful"
    func["config"]["RevisionId"] = new_uuid()

    # Invalidate warm worker so next invocation picks up the new code
    invalidate_worker(name)

    if data.get("Publish"):
        ver_num = func["next_version"]
        func["next_version"] = ver_num + 1
        ver_config = copy.deepcopy(func["config"])
        ver_config["Version"] = str(ver_num)
        func["versions"][str(ver_num)] = {
            "config": ver_config,
            "code_zip": func.get("code_zip"),
        }
        func["config"]["Version"] = str(ver_num)

    return json_response(func["config"])


def _update_config(name: str, data: dict):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )
    config = _functions[name]["config"]
    for key in (
        "Runtime",
        "Handler",
        "Description",
        "Timeout",
        "MemorySize",
        "Role",
        "Environment",
        "Layers",
        "TracingConfig",
        "DeadLetterConfig",
        "KMSKeyArn",
        "EphemeralStorage",
        "LoggingConfig",
        "VpcConfig",
        "Architectures",
        "FileSystemConfigs",
    ):
        if key in data:
            config[key] = data[key]
    if "ImageConfig" in data:
        config["ImageConfigResponse"] = {"ImageConfig": data["ImageConfig"]}
    config["LastModified"] = _now_iso()
    config["LastUpdateStatus"] = "Successful"
    config["RevisionId"] = new_uuid()
    return json_response(config)


# ---------------------------------------------------------------------------
# Invoke
# ---------------------------------------------------------------------------


async def _invoke(name: str, event: dict, headers: dict, path_qualifier: str | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )

    func = _functions[name]
    invocation_type = headers.get("x-amz-invocation-type") or headers.get("X-Amz-Invocation-Type") or "RequestResponse"
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
            target=_execute_function,
            args=(exec_record, event),
            daemon=True,
        ).start()
        return 202, {"X-Amz-Executed-Version": executed_version}, b""

    # RequestResponse — execute in worker thread so nested SDK calls
    # from the Lambda process can still reach this ASGI server.
    result = await asyncio.to_thread(_execute_function, exec_record, event)

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
    return 200, resp_headers, json.dumps(payload, ensure_ascii=False).encode("utf-8")


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
    "provided.al2023": "public.ecr.aws/lambda/provided:al2023",
    "provided.al2": "public.ecr.aws/lambda/provided:al2",
    "provided": "public.ecr.aws/lambda/provided:al2023",
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
    if runtime.startswith("provided"):
        return "public.ecr.aws/lambda/provided:al2023"
    return None


# ---------------------------------------------------------------------------
# Function execution – Docker mode
# ---------------------------------------------------------------------------


def _execute_function_docker(func: dict, event: dict) -> dict:
    """Execute a Lambda function inside a Docker container.

    Mirrors the subprocess executor's interface: returns
    ``{"body": ..., "log": ..., "error": ...}``.
    """
    config = func.get("config") or func
    runtime = config.get("Runtime", "python3.9")

    if not _docker_available:
        
        if runtime.startswith("python") or runtime.startswith("nodejs"):
            logger.warning("docker SDK unavailable - falling back to warm executor (node/py detected)")
            return _execute_function_warm(func, event)

        logger.warning("docker SDK unavailable - falling back to local subprocess executor")
        return _execute_function_local(func, event)

    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    handler = config["Handler"]
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

    is_provided = runtime.startswith("provided")
    is_node = runtime.startswith("nodejs")
    if not runtime.startswith("python") and not is_node and not is_provided:
        return {
            "body": {
                "statusCode": 200,
                "body": f"Mock response - {runtime} docker wrapper only supports Python, Node.js, and provided runtimes",
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

            if not is_provided:
                if "." not in handler:
                    return {"body": {"errorMessage": f"Invalid handler format: {handler}", "errorType": "Runtime.InvalidEntrypoint"}, "error": True}
                module_name, func_name = handler.rsplit(".", 1)

            # Build volume mounts
            volumes: dict = {}
            host_code_dir = LAMBDA_DOCKER_VOLUME_MOUNT or code_dir
            volumes[host_code_dir] = {"bind": "/var/task", "mode": "ro"}

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
                "_LAMBDA_FUNCTION_ARN": config["FunctionArn"],
                "_LAMBDA_TIMEOUT": str(timeout),
                "_LAMBDA_LAYERS_DIRS": ":".join(container_layer_dirs),
            }
            if not is_provided:
                container_env["_LAMBDA_HANDLER_MODULE"] = module_name
                container_env["_LAMBDA_HANDLER_FUNC"] = func_name
            else:
                container_env["LAMBDA_TASK_ROOT"] = "/var/task"
                container_env["_HANDLER"] = handler
            endpoint = _normalize_endpoint_url(os.environ.get("AWS_ENDPOINT_URL", ""))
            if not endpoint:
                endpoint = _normalize_endpoint_url(env_vars.get("AWS_ENDPOINT_URL", ""))
            if not endpoint:
                endpoint = _normalize_endpoint_url(env_vars.get("LOCALSTACK_HOSTNAME", ""))
            if endpoint:
                container_env["AWS_ENDPOINT_URL"] = endpoint
            container_env.update(env_vars)

            event_file = os.path.join(code_dir, "_event.json")
            with open(event_file, "w") as ef:
                ef.write(json.dumps(event))

            if is_provided:
                # provided runtimes: use AWS Lambda RIE built into the provided image.
                # The RIE entrypoint runs /var/runtime/bootstrap, while user
                # code is expected at /var/task.  Mount the code dir to both
                # paths using docker.types.Mount (the volumes dict can't bind
                # the same host path to two container paths).
                import urllib.request
                bootstrap_path = os.path.join(code_dir, "bootstrap")
                if os.path.exists(bootstrap_path):
                    os.chmod(bootstrap_path, 0o755)
                mounts = [
                    docker_lib.types.Mount("/var/task", host_code_dir, type="bind", read_only=True),
                    docker_lib.types.Mount("/var/runtime", host_code_dir, type="bind", read_only=True),
                ]
                for idx, ld in enumerate(layers_dirs):
                    mounts.append(docker_lib.types.Mount(f"/opt/layer_{idx}", ld, type="bind", read_only=True))
                run_kwargs: dict = {
                    "image": image, "command": ["bootstrap"],
                    "environment": container_env, "mounts": mounts,
                    "ports": {"8080/tcp": None}, "detach": True, "stdin_open": False,
                }
                if LAMBDA_DOCKER_NETWORK:
                    run_kwargs["network"] = LAMBDA_DOCKER_NETWORK
                container = client.containers.run(**run_kwargs)
                try:
                    import time as _time
                    for _attempt in range(30):
                        _time.sleep(0.5)
                        container.reload()
                        if container.status != "running":
                            break
                        try:
                            if LAMBDA_DOCKER_NETWORK:
                                networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                                container_ip = networks.get(LAMBDA_DOCKER_NETWORK, {}).get("IPAddress", "")
                                if container_ip:
                                    rie_url = f"http://{container_ip}:8080/2015-03-31/functions/function/invocations"
                                else:
                                    continue
                            else:
                                ports = container.ports.get("8080/tcp") or []
                                if not ports:
                                    continue
                                rie_url = f"http://127.0.0.1:{ports[0]['HostPort']}/2015-03-31/functions/function/invocations"
                            req = urllib.request.Request(rie_url, data=json.dumps(event).encode(),
                                                        headers={"Content-Type": "application/json"})
                            resp = urllib.request.urlopen(req, timeout=timeout)
                            body = resp.read().decode("utf-8", errors="replace")
                            try:
                                parsed = json.loads(body)
                            except json.JSONDecodeError:
                                parsed = body
                            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace").strip()
                            return {"body": parsed, "log": logs}
                        except (urllib.error.URLError, ConnectionRefusedError, OSError):
                            continue
                    stdout = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace").strip()
                    return {"body": {"errorMessage": f"provided runtime failed: {stdout[:500]}", "errorType": "Runtime.ExitError"}, "error": True, "log": stdout}
                finally:
                    try:
                        container.stop(timeout=2)
                    except Exception:
                        pass
                    try:
                        container.remove(force=True)
                    except Exception:
                        pass
            elif is_node:
                wrapper_path = os.path.join(code_dir, "_wrapper.js")
                with open(wrapper_path, "w") as wf:
                    wf.write(_NODE_WRAPPER_SCRIPT)
                run_cmd = ["sh", "-c", "node /var/task/_wrapper.js < /var/task/_event.json"]
            else:
                wrapper_path = os.path.join(code_dir, "_wrapper.py")
                with open(wrapper_path, "w") as wf:
                    wf.write(_DOCKER_WRAPPER_SCRIPT)
                run_cmd = ["sh", "-c", "python3 /var/task/_wrapper.py < /var/task/_event.json"]

            container = client.containers.run(
                image,
                command=run_cmd,
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
    """Dispatch to warm worker pool (Python + Node.js) or Docker executor."""
    config = func.get("config") or func

    # Image-based Lambda — always Docker
    if config.get("PackageType") == "Image" and config.get("ImageUri"):
        return _execute_function_image(func, event)

    runtime = config.get("Runtime", "python3.9")

    if LAMBDA_EXECUTOR == "docker":
        return _execute_function_docker(func, event)

    # provided runtimes (Go, Rust, etc.) — use native binary execution
    if runtime.startswith("provided"):
        return _execute_function_provided(func, event)

    if runtime.startswith("python") or runtime.startswith("nodejs"):
        return _execute_function_warm(func, event)

    return _execute_function_local(func, event)


def _execute_function_warm(func: dict, event: dict) -> dict:
    """Execute a Lambda function using the warm worker pool (Python + Node.js)."""
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    func_name = config.get("FunctionName", "unknown")
    try:
        worker = get_or_create_worker(func_name, config, code_zip)
        result = worker.invoke(event, new_uuid())
        if result.get("status") == "ok":
            return {"body": result.get("result"), "log": result.get("log", "")}
        else:
            return {
                "body": {
                    "errorMessage": result.get("error", "Unknown error"),
                    "errorType": "Runtime.HandlerError",
                },
                "error": True,
                "log": result.get("trace", result.get("error", "")),
            }
    except Exception as e:
        logger.error("Warm worker execution error for %s: %s", func_name, e)
        return {
            "body": {"errorMessage": str(e), "errorType": type(e).__name__},
            "error": True,
            "log": "",
        }


# ---------------------------------------------------------------------------
# Function execution – Docker Image mode (PackageType: Image)
# ---------------------------------------------------------------------------


def _execute_function_image(func: dict, event: dict) -> dict:
    """Execute a Lambda function from a user-provided Docker image.

    The image must contain the Lambda Runtime Interface Client (RIC) or
    Runtime Interface Emulator (RIE) listening on port 8080.
    """
    if not _docker_available:
        return {"body": {"errorMessage": "Docker is required for Image-based Lambda functions", "errorType": "Runtime.DockerUnavailable"}, "error": True}

    config = func.get("config") or func
    image_uri = config.get("ImageUri", "")
    timeout = config.get("Timeout", 30)
    env_vars = config.get("Environment", {}).get("Variables", {})

    try:
        client = docker_lib.from_env()
    except Exception as exc:
        return {"body": {"errorMessage": f"Cannot connect to Docker: {exc}", "errorType": "Runtime.DockerError"}, "error": True}

    # Use image locally if available, otherwise pull
    try:
        client.images.get(image_uri)
        logger.info("Using local Lambda image: %s", image_uri)
    except docker_lib.errors.ImageNotFound:
        try:
            logger.info("Pulling Lambda image: %s", image_uri)
            client.images.pull(image_uri)
        except Exception as exc:
            return {"body": {"errorMessage": f"Failed to pull image {image_uri}: {exc}", "errorType": "Runtime.ImagePullError"}, "error": True}

    container_env = {
        "AWS_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "AWS_LAMBDA_FUNCTION_NAME": config["FunctionName"],
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(config.get("MemorySize", 128)),
        "AWS_LAMBDA_FUNCTION_VERSION": config.get("Version", "$LATEST"),
        "AWS_LAMBDA_LOG_STREAM_NAME": new_uuid(),
    }
    endpoint = _normalize_endpoint_url(os.environ.get("AWS_ENDPOINT_URL", ""))
    if not endpoint:
        endpoint = _normalize_endpoint_url(env_vars.get("AWS_ENDPOINT_URL", ""))
    if endpoint:
        container_env["AWS_ENDPOINT_URL"] = endpoint
    container_env.update(env_vars)

    run_kwargs = {
        "image": image_uri,
        "environment": container_env,
        "ports": {"8080/tcp": None},
        "detach": True,
    }
    if LAMBDA_DOCKER_NETWORK:
        run_kwargs["network"] = LAMBDA_DOCKER_NETWORK

    container = client.containers.run(**run_kwargs)
    try:
        import time as _time
        import urllib.request
        for _attempt in range(int(timeout * 2) + 10):
            _time.sleep(0.5)
            container.reload()
            if container.status != "running":
                break
            try:
                if LAMBDA_DOCKER_NETWORK:
                    networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                    container_ip = networks.get(LAMBDA_DOCKER_NETWORK, {}).get("IPAddress", "")
                    if container_ip:
                        rie_url = f"http://{container_ip}:8080/2015-03-31/functions/function/invocations"
                    else:
                        continue
                else:
                    ports = container.ports.get("8080/tcp") or []
                    if not ports:
                        continue
                    rie_url = f"http://127.0.0.1:{ports[0]['HostPort']}/2015-03-31/functions/function/invocations"
                req = urllib.request.Request(rie_url, data=json.dumps(event).encode(),
                                            headers={"Content-Type": "application/json"})
                resp = urllib.request.urlopen(req, timeout=timeout)
                body = resp.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError:
                    parsed = body
                logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace").strip()
                return {"body": parsed, "log": logs}
            except (urllib.error.URLError, ConnectionRefusedError, OSError):
                continue
        stdout = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace").strip()
        return {"body": {"errorMessage": f"Image Lambda failed: {stdout[:500]}", "errorType": "Runtime.ExitError"}, "error": True, "log": stdout}
    finally:
        try:
            container.stop(timeout=2)
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass


def _execute_function_provided(func: dict, event: dict) -> dict:
    """Execute a provided-runtime Lambda (Go/Rust binary) via a minimal Lambda Runtime API."""
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    timeout = config.get("Timeout", 30)
    env_vars = config.get("Environment", {}).get("Variables", {})

    try:
        import http.server
        import socketserver
        with tempfile.TemporaryDirectory() as tmpdir:
            # Extract bootstrap binary
            zip_path = os.path.join(tmpdir, "code.zip")
            with open(zip_path, "wb") as f:
                f.write(code_zip)
            code_dir = os.path.join(tmpdir, "code")
            os.makedirs(code_dir)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(code_dir)

            bootstrap_path = os.path.join(code_dir, "bootstrap")
            if not os.path.exists(bootstrap_path):
                return {"body": {"statusCode": 200, "body": "Mock response - no bootstrap binary found"}}
            os.chmod(bootstrap_path, 0o755)

            # Shared state for the Runtime API
            result_holder = {"response": None, "error": None}
            event_json = json.dumps(event)
            request_id = new_uuid()
            event_served = threading.Event()
            response_received = threading.Event()
            server_ready = threading.Event()

            class RuntimeAPIHandler(http.server.BaseHTTPRequestHandler):
                def log_message(self, format, *args):
                    pass  # Suppress logs

                def _read_body(self):
                    """Read request body, handling both Content-Length and chunked transfer encoding."""
                    transfer_encoding = self.headers.get("Transfer-Encoding", "")
                    if "chunked" in transfer_encoding.lower():
                        chunks = []
                        while True:
                            line = self.rfile.readline().strip()
                            chunk_size = int(line, 16)
                            if chunk_size == 0:
                                self.rfile.readline()  # trailing CRLF
                                break
                            chunks.append(self.rfile.read(chunk_size))
                            self.rfile.readline()  # trailing CRLF
                        return b"".join(chunks)
                    content_length = int(self.headers.get("Content-Length", 0))
                    return self.rfile.read(content_length) if content_length else b""

                def do_GET(self):
                    # GET /2018-06-01/runtime/invocation/next
                    if "/runtime/invocation/next" in self.path:
                        self.send_response(200)
                        self.send_header("Lambda-Runtime-Aws-Request-Id", request_id)
                        self.send_header("Lambda-Runtime-Deadline-Ms",
                                         str(int((time.time() + timeout) * 1000)))
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(event_json.encode())
                        event_served.set()
                    else:
                        self.send_response(404)
                        self.end_headers()

                def do_POST(self):
                    body = self._read_body()
                    if f"/runtime/invocation/{request_id}/response" in self.path:
                        try:
                            result_holder["response"] = json.loads(body)
                        except json.JSONDecodeError:
                            result_holder["response"] = body.decode("utf-8", errors="replace")
                        self.send_response(202)
                        self.end_headers()
                        response_received.set()
                    elif f"/runtime/invocation/{request_id}/error" in self.path:
                        try:
                            result_holder["error"] = json.loads(body)
                        except json.JSONDecodeError:
                            result_holder["error"] = body.decode("utf-8", errors="replace")
                        self.send_response(202)
                        self.end_headers()
                        response_received.set()
                    elif "/runtime/init/error" in self.path:
                        try:
                            result_holder["error"] = json.loads(body)
                        except json.JSONDecodeError:
                            result_holder["error"] = body.decode("utf-8", errors="replace")
                        self.send_response(202)
                        self.end_headers()
                        response_received.set()
                    else:
                        self.send_response(404)
                        self.end_headers()

            # Bind to port 0 — OS assigns a free port atomically, no race window
            server = socketserver.TCPServer(("127.0.0.1", 0), RuntimeAPIHandler)
            port = server.server_address[1]

            def _serve():
                server_ready.set()
                server.serve_forever()

            server_thread = threading.Thread(target=_serve, daemon=True)
            server_thread.start()
            server_ready.wait(timeout=5)

            try:
                # Build environment for the Lambda binary
                proc_env = dict(os.environ)
                proc_env.update({
                    "AWS_LAMBDA_RUNTIME_API": f"127.0.0.1:{port}",
                    "AWS_DEFAULT_REGION": REGION,
                    "AWS_REGION": REGION,
                    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                    "AWS_LAMBDA_FUNCTION_NAME": config.get("FunctionName", "unknown"),
                    "LAMBDA_TASK_ROOT": code_dir,
                    "_HANDLER": config.get("Handler", "bootstrap"),
                })
                # Pass through AWS_ENDPOINT_URL for SDK calls
                endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
                if not endpoint:
                    hostname = os.environ.get("LOCALSTACK_HOSTNAME", "")
                    if hostname:
                        endpoint = _normalize_endpoint_url(hostname)
                if endpoint:
                    proc_env["AWS_ENDPOINT_URL"] = endpoint
                proc_env.update(env_vars)

                proc = subprocess.Popen(
                    [bootstrap_path],
                    cwd=code_dir,
                    env=proc_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                if response_received.wait(timeout=timeout):
                    proc.terminate()
                    try:
                        _, stderr_out = proc.communicate(timeout=5)
                        if stderr_out:
                            logger.info("Lambda %s stderr: %s", config.get("FunctionName", "?"), stderr_out.decode("utf-8", errors="replace")[:500])
                    except Exception:
                        pass
                    if result_holder["error"]:
                        err = result_holder["error"]
                        if isinstance(err, dict):
                            return {"body": err, "error": True}
                        return {"body": {"errorMessage": str(err), "errorType": "Runtime.HandlerError"}, "error": True}
                    return {"body": result_holder["response"]}
                else:
                    proc.kill()
                    stdout, stderr = proc.communicate(timeout=5)
                    logs = (stdout.decode("utf-8", errors="replace") + stderr.decode("utf-8", errors="replace")).strip()
                    return {"body": {"errorMessage": f"Lambda timed out after {timeout}s: {logs[:500]}", "errorType": "Runtime.ExitError"}, "error": True}
            finally:
                server.shutdown()

    except Exception as e:
        logger.error("provided runtime execution error: %s", e)
        return {"body": {"errorMessage": str(e), "errorType": type(e).__name__}, "error": True}


def _execute_function_local(func: dict, event: dict) -> dict:
    """Execute a Lambda function in a one-shot subprocess (fallback for unsupported runtimes)."""
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    handler = config["Handler"]
    runtime = config["Runtime"]
    timeout = config.get("Timeout", 3)
    env_vars = config.get("Environment", {}).get("Variables", {})

    is_node = runtime.startswith("nodejs")
    if not runtime.startswith("python") and not is_node:
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

            if "." not in handler:
                return {"body": {"errorMessage": f"Invalid handler format: {handler}", "errorType": "Runtime.InvalidEntrypoint"}, "error": True}
            module_name, func_name = handler.rsplit(".", 1)

            if is_node:
                wrapper_path = os.path.join(tmpdir, "_wrapper.js")
                with open(wrapper_path, "w") as wf:
                    wf.write(_NODE_WRAPPER_SCRIPT)
            else:
                wrapper_path = os.path.join(tmpdir, "_wrapper.py")
                with open(wrapper_path, "w") as wf:
                    wf.write(_WRAPPER_SCRIPT)

            env = dict(os.environ)
            env.update(
                {
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
                }
            )
            endpoint = _normalize_endpoint_url(os.environ.get("AWS_ENDPOINT_URL", ""))
            if not endpoint:
                endpoint = _normalize_endpoint_url(env_vars.get("AWS_ENDPOINT_URL", ""))
            if not endpoint:
                endpoint = _normalize_endpoint_url(env_vars.get("LOCALSTACK_HOSTNAME", ""))
            if endpoint:
                env["AWS_ENDPOINT_URL"] = endpoint
            env.update(env_vars)

            cmd = ["node", wrapper_path] if is_node else ["python3", wrapper_path]
            proc = subprocess.run(
                cmd,
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

    except subprocess.TimeoutExpired as exc:
        try:
            _stdout = getattr(exc, "stdout", None) or ""
            if isinstance(_stdout, bytes):
                _stdout = _stdout.decode("utf-8", errors="replace")
            _stdout = str(_stdout).strip()
        except Exception:
            _stdout = ""
        try:
            _stderr = getattr(exc, "stderr", None) or ""
            if isinstance(_stderr, bytes):
                _stderr = _stderr.decode("utf-8", errors="replace")
            _stderr = str(_stderr).strip()
        except Exception:
            _stderr = ""
        _log = "\n".join([p for p in (_stderr, _stdout) if p])
        if not _log:
            _log = "Lambda timed out (no stderr/stdout captured)."
        return {
            "body": {
                "errorMessage": f"Task timed out after {timeout}.00 seconds",
                "errorType": "Runtime.ExitError",
            },
            "error": True,
            "log": _log,
        }
    except Exception as e:
        logger.error("Lambda execution error: %s", e)
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
            f"Function not found: {_func_arn(name)}",
            404,
        )
    func = _functions[name]
    ver_num = func["next_version"]
    func["next_version"] = ver_num + 1

    ver_config = copy.deepcopy(func["config"])
    ver_config["Version"] = str(ver_num)
    ver_config["FunctionArn"] = _func_arn(name)
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
            f"Function not found: {_func_arn(name)}",
            404,
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

    page = versions[start : start + max_items]
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    alias_name = data.get("Name", "")
    if not alias_name:
        return error_response_json(
            "InvalidParameterValueException",
            "Alias name is required",
            400,
        )
    func = _functions[func_name]
    if alias_name in func["aliases"]:
        return error_response_json(
            "ResourceConflictException",
            f"Alias already exists: {alias_name}",
            409,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    alias = _functions[func_name]["aliases"].get(alias_name)
    if not alias:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}",
            404,
        )
    return json_response(alias)


def _update_alias(func_name: str, alias_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    alias = _functions[func_name]["aliases"].get(alias_name)
    if not alias:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}",
            404,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    if alias_name not in _functions[func_name]["aliases"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"Alias not found: {_func_arn(func_name)}:{alias_name}",
            404,
        )
    del _functions[func_name]["aliases"][alias_name]
    return 204, {}, b""


def _list_aliases(func_name: str, query_params: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
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
    page = aliases[start : start + max_items]
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
            f"Function not found: {_func_arn(func_name)}",
            404,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    func = _functions[func_name]
    before = len(func["policy"]["Statement"])
    func["policy"]["Statement"] = [s for s in func["policy"]["Statement"] if s.get("Sid") != sid]
    if len(func["policy"]["Statement"]) == before:
        return error_response_json(
            "ResourceNotFoundException",
            "No policy is associated with the given resource.",
            404,
        )
    return 204, {}, b""


def _get_policy(func_name: str, query_params: dict | None = None):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    func = _functions[func_name]
    return json_response(
        {
            "Policy": json.dumps(func["policy"]),
            "RevisionId": func["config"]["RevisionId"],
        }
    )


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _list_tags(resource_arn: str):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
        )
    return json_response({"Tags": _functions[func_name].get("tags", {})})


def _tag_resource(resource_arn: str, data: dict):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
        )
    _functions[func_name].setdefault("tags", {}).update(data.get("Tags", {}))
    return 204, {}, b""


def _untag_resource(resource_arn: str, query_params: dict):
    func_name = _resolve_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
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


def _layer_content_url(layer_name: str, version: int) -> str:
    host = os.environ.get("MINISTACK_HOST", "localhost")
    port = os.environ.get("GATEWAY_PORT", "4566")
    return f"http://{host}:{port}/_ministack/lambda-layers/{layer_name}/{version}/content"


def _publish_layer_version(layer_name: str, data: dict):
    runtimes = data.get("CompatibleRuntimes", [])
    architectures = data.get("CompatibleArchitectures", [])
    if len(runtimes) > 15:
        return error_response_json(
            "InvalidParameterValueException",
            "CompatibleRuntimes list length exceeds maximum allowed length of 15.",
            400,
        )
    if len(architectures) > 2:
        return error_response_json(
            "InvalidParameterValueException",
            "CompatibleArchitectures list length exceeds maximum allowed length of 2.",
            400,
        )

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
        "CompatibleRuntimes": runtimes,
        "CompatibleArchitectures": architectures,
        "LicenseInfo": data.get("LicenseInfo", ""),
        "CreatedDate": _now_iso(),
        "Content": {
            "Location": _layer_content_url(layer_name, ver),
            "CodeSha256": (base64.b64encode(hashlib.sha256(zip_data).digest()).decode() if zip_data else ""),
            "CodeSize": len(zip_data) if zip_data else 0,
        },
        "_zip_data": zip_data,
        "_policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
    }
    layer["versions"].append(ver_config)
    out = {k: v for k, v in ver_config.items() if not k.startswith("_")}
    return json_response(out, 201)


def _match_layer_version(vc: dict, runtime: str, arch: str) -> bool:
    if runtime and runtime not in vc.get("CompatibleRuntimes", []):
        return False
    if arch and arch not in vc.get("CompatibleArchitectures", []):
        return False
    return True


def _list_layer_versions(layer_name: str, query_params: dict):
    layer = _layers.get(layer_name)
    if not layer:
        return json_response({"LayerVersions": []})

    runtime = _qp_first(query_params, "CompatibleRuntime")
    arch = _qp_first(query_params, "CompatibleArchitecture")

    all_versions = [
        {k: v for k, v in vc.items() if not k.startswith("_")}
        for vc in layer["versions"]
        if _match_layer_version(vc, runtime, arch)
    ]
    all_versions.sort(key=lambda v: v["Version"], reverse=True)

    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "50"))
    start = 0
    if marker:
        for i, v in enumerate(all_versions):
            if str(v["Version"]) == marker:
                start = i + 1
                break

    page = all_versions[start : start + max_items]
    result: dict = {"LayerVersions": page}
    if start + max_items < len(all_versions):
        result["NextMarker"] = str(page[-1]["Version"]) if page else ""
    return json_response(result)


def _get_layer_version(layer_name: str, version: int):
    if version < 1:
        return error_response_json(
            "InvalidParameterValueException",
            "Layer Version Cannot be less than 1.",
            400,
        )
    layer = _layers.get(layer_name)
    if not layer:
        return error_response_json(
            "ResourceNotFoundException",
            "The resource you requested does not exist.",
            404,
        )
    for vc in layer["versions"]:
        if vc["Version"] == version:
            out = {k: v for k, v in vc.items() if not k.startswith("_")}
            return json_response(out)
    return error_response_json(
        "ResourceNotFoundException",
        "The resource you requested does not exist.",
        404,
    )


def _get_layer_version_by_arn(arn: str):
    segs = arn.split(":")
    if len(segs) < 8 or not segs[7].isdigit():
        return error_response_json(
            "ValidationException",
            f"Value '{arn}' at 'arn' failed to satisfy constraint: "
            "Member must satisfy regular expression pattern: "
            "arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{{1}}:\\d{{12}}:layer:[a-zA-Z0-9-_]+:[0-9]+",
            400,
        )
    layer_name = segs[6]
    version = int(segs[7])
    return _get_layer_version(layer_name, version)


def _delete_layer_version(layer_name: str, version: int):
    if version < 1:
        return error_response_json(
            "InvalidParameterValueException",
            "Layer Version Cannot be less than 1.",
            400,
        )
    layer = _layers.get(layer_name)
    if not layer:
        return 204, {}, b""
    layer["versions"] = [vc for vc in layer["versions"] if vc["Version"] != version]
    return 204, {}, b""


def _list_layers(query_params: dict):
    runtime = _qp_first(query_params, "CompatibleRuntime")
    arch = _qp_first(query_params, "CompatibleArchitecture")

    result = []
    for name, layer in _layers.items():
        matching = [vc for vc in layer["versions"] if _match_layer_version(vc, runtime, arch)]
        if matching:
            latest = matching[-1]
            result.append(
                {
                    "LayerName": name,
                    "LayerArn": _layer_arn(name),
                    "LatestMatchingVersion": {k: v for k, v in latest.items() if not k.startswith("_")},
                }
            )

    marker = _qp_first(query_params, "Marker")
    max_items = int(_qp_first(query_params, "MaxItems", "50"))
    start = 0
    if marker:
        for i, item in enumerate(result):
            if item["LayerName"] == marker:
                start = i + 1
                break

    page = result[start : start + max_items]
    resp: dict = {"Layers": page}
    if start + max_items < len(result):
        resp["NextMarker"] = page[-1]["LayerName"] if page else ""
    return json_response(resp)


# ---------------------------------------------------------------------------
# Layer Version Permissions
# ---------------------------------------------------------------------------


def _find_layer_version(layer_name: str, version: int):
    """Return (layer_version_config, error_response) — one will be None."""
    layer = _layers.get(layer_name)
    lv_arn = f"{_layer_arn(layer_name)}:{version}"
    if not layer:
        return None, error_response_json(
            "ResourceNotFoundException",
            f"Layer version {lv_arn} does not exist.",
            404,
        )
    for vc in layer["versions"]:
        if vc["Version"] == version:
            return vc, None
    return None, error_response_json(
        "ResourceNotFoundException",
        f"Layer version {lv_arn} does not exist.",
        404,
    )


def _add_layer_version_permission(layer_name: str, version: int, data: dict):
    vc, err = _find_layer_version(layer_name, version)
    if err:
        return err

    action = data.get("Action", "")
    if action != "lambda:GetLayerVersion":
        return error_response_json(
            "ValidationException",
            f"1 validation error detected: Value '{action}' at 'action' failed to satisfy "
            "constraint: Member must satisfy regular expression pattern: lambda:GetLayerVersion",
            400,
        )

    sid = data.get("StatementId", "")
    policy = vc.setdefault("_policy", {"Version": "2012-10-17", "Id": "default", "Statement": []})
    for s in policy["Statement"]:
        if s.get("Sid") == sid:
            return error_response_json(
                "ResourceConflictException",
                f"The statement id ({sid}) provided already exists. "
                "Please provide a new statement id, or remove the existing statement.",
                409,
            )

    statement = {
        "Sid": sid,
        "Effect": "Allow",
        "Principal": data.get("Principal", "*"),
        "Action": action,
        "Resource": vc["LayerVersionArn"],
    }
    org_id = data.get("OrganizationId")
    if org_id:
        statement["Condition"] = {"StringEquals": {"aws:PrincipalOrgID": org_id}}

    policy["Statement"].append(statement)
    return json_response(
        {
            "Statement": json.dumps(statement),
            "RevisionId": new_uuid(),
        },
        201,
    )


def _remove_layer_version_permission(layer_name: str, version: int, sid: str):
    vc, err = _find_layer_version(layer_name, version)
    if err:
        return err

    policy = vc.get("_policy", {"Statement": []})
    before = len(policy["Statement"])
    policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != sid]
    if len(policy["Statement"]) == before:
        return error_response_json(
            "ResourceNotFoundException",
            f"Statement {sid} is not found in resource policy.",
            404,
        )
    return 204, {}, b""


def _get_layer_version_policy(layer_name: str, version: int):
    vc, err = _find_layer_version(layer_name, version)
    if err:
        return err

    policy = vc.get("_policy", {"Statement": []})
    if not policy.get("Statement"):
        return error_response_json(
            "ResourceNotFoundException",
            "No policy is associated with the given resource.",
            404,
        )
    return json_response(
        {
            "Policy": json.dumps(policy),
            "RevisionId": new_uuid(),
        }
    )


def serve_layer_content(layer_name: str, version: int):
    """Serve raw zip bytes for a layer version (called from app.py)."""
    vc, err = _find_layer_version(layer_name, version)
    if err:
        return err
    zip_data = vc.get("_zip_data")
    if not zip_data:
        return 404, {}, b""
    return 200, {"Content-Type": "application/zip"}, zip_data


# ---------------------------------------------------------------------------
# Event Invoke Config (stubs — enough for Terraform to not error)
# ---------------------------------------------------------------------------


def _get_event_invoke_config(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    eic = _functions[func_name].get("event_invoke_config")
    if not eic:
        return error_response_json(
            "ResourceNotFoundException",
            f"The function {func_name} doesn't have an EventInvokeConfig",
            404,
        )
    return json_response(eic)


def _put_event_invoke_config(func_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    eic = {
        "FunctionArn": _func_arn(func_name),
        "MaximumRetryAttempts": data.get("MaximumRetryAttempts", 2),
        "MaximumEventAgeInSeconds": data.get("MaximumEventAgeInSeconds", 21600),
        "LastModified": time.time(),
        "DestinationConfig": data.get(
            "DestinationConfig",
            {
                "OnSuccess": {},
                "OnFailure": {},
            },
        ),
    }
    _functions[func_name]["event_invoke_config"] = eic
    return json_response(eic)


def _delete_event_invoke_config(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    conc = _functions[func_name].get("concurrency")
    if conc is None:
        return json_response({})
    return json_response({"ReservedConcurrentExecutions": conc})


def _put_function_concurrency(func_name: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    value = data.get("ReservedConcurrentExecutions", 0)
    _functions[func_name]["concurrency"] = value
    return json_response({"ReservedConcurrentExecutions": value})


def _delete_function_concurrency(func_name: str):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    key = qualifier or "$LATEST"
    pc = _functions[func_name].get("provisioned_concurrency", {}).get(key)
    if not pc:
        return error_response_json(
            "ProvisionedConcurrencyConfigNotFoundException",
            f"No Provisioned Concurrency Config found for function: {_func_arn(func_name)}",
            404,
        )
    return json_response(pc)


def _put_provisioned_concurrency(func_name: str, qualifier: str, data: dict):
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
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
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    key = qualifier or "$LATEST"
    _functions[func_name].get("provisioned_concurrency", {}).pop(key, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Event Source Mappings
# ---------------------------------------------------------------------------


def _esm_response(esm: dict) -> dict:
    """Return ESM dict without internal-only fields."""
    return {k: v for k, v in esm.items() if k != "FunctionName"}


def _create_esm(data: dict):
    esm_id = new_uuid()
    func_name = _resolve_name(data.get("FunctionName", ""))
    event_source_arn = data.get("EventSourceArn", "")

    esm = {
        "UUID": esm_id,
        "EventSourceArn": event_source_arn,
        "FunctionArn": _func_arn(func_name),
        "FunctionName": func_name,
        "State": "Enabled",
        "StateTransitionReason": "USER_INITIATED",
        "BatchSize": data.get("BatchSize", 10),
        "MaximumBatchingWindowInSeconds": data.get("MaximumBatchingWindowInSeconds", 0),
        "LastModified": time.time(),
        "LastProcessingResult": "No records processed",
        "Enabled": True,
        "FunctionResponseTypes": data.get("FunctionResponseTypes", []),
    }
    if ":sqs:" not in event_source_arn:
        esm["StartingPosition"] = data.get("StartingPosition", "LATEST")
    _esms[esm_id] = esm
    _ensure_poller()
    return json_response(_esm_response(esm), 202)


def _get_esm(esm_id: str):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {new_uuid()})",
            404,
        )
    return json_response(_esm_response(esm))


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

    page = result[start : start + max_items]
    resp: dict = {"EventSourceMappings": [_esm_response(e) for e in page]}
    if start + max_items < len(result):
        resp["NextMarker"] = page[-1]["UUID"] if page else ""
    return json_response(resp)


def _update_esm(esm_id: str, data: dict):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"Event source mapping not found: {esm_id}",
            404,
        )
    for key in (
        "BatchSize",
        "MaximumBatchingWindowInSeconds",
        "FunctionResponseTypes",
        "MaximumRetryAttempts",
        "MaximumRecordAgeInSeconds",
        "BisectBatchOnFunctionError",
        "ParallelizationFactor",
        "DestinationConfig",
        "FilterCriteria",
    ):
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
    return json_response(_esm_response(esm))


def _delete_esm(esm_id: str):
    esm = _esms.pop(esm_id, None)
    if not esm:
        return error_response_json(
            "ResourceNotFoundException",
            f"Event source mapping not found: {esm_id}",
            404,
        )
    esm["State"] = "Deleting"
    return json_response(_esm_response(esm), 202)


# ---------------------------------------------------------------------------
# ESM Poller (SQS + Kinesis + DynamoDB Streams)
# ---------------------------------------------------------------------------

# Per-ESM Kinesis iterator tracking: esm_uuid -> {shard_id: position}
_kinesis_positions = AccountScopedDict()
# Per-ESM DynamoDB stream tracking: esm_uuid -> {shard_id: position}
_dynamodb_stream_positions = AccountScopedDict()
_dynamodb_stream_positions_lock = threading.Lock()


def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    """Background thread: polls SQS/Kinesis/DynamoDB for active ESMs and invokes Lambda."""
    while True:
        try:
            _poll_sqs()
        except Exception as e:
            logger.error("ESM SQS poller error: %s", e)
        try:
            _poll_kinesis()
        except Exception as e:
            logger.error("ESM Kinesis poller error: %s", e)
        try:
            _poll_dynamodb_streams()
        except Exception as e:
            logger.error("ESM DynamoDB streams poller error: %s", e)
        time.sleep(1 if _esms else 5)


def _poll_sqs():
    from ministack.services import sqs as _sqs

    for esm in list(_esms.values()):
        if not esm.get("Enabled", True):
            continue
        source_arn = esm.get("EventSourceArn", "")
        if ":sqs:" not in source_arn:
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

        batch = _sqs._receive_messages_for_esm(queue_url, batch_size)
        if not batch:
            continue

        records = []
        for msg in batch:
            first_recv = msg.get("first_receive_at") or now
            records.append({
                "messageId": msg["id"],
                "receiptHandle": msg["receipt_handle"],
                "body": msg["body"],
                "attributes": {
                    "ApproximateReceiveCount": str(msg.get("receive_count", 1)),
                    "SentTimestamp": str(int(msg["sent_at"] * 1000)),
                    "SenderId": get_account_id(),
                    "ApproximateFirstReceiveTimestamp": str(int(first_recv * 1000)),
                },
                "messageAttributes": msg.get("message_attributes", {}),
                "md5OfBody": msg.get("md5_body") or msg.get("md5") or "",
                "eventSource": "aws:sqs",
                "eventSourceARN": source_arn,
                "awsRegion": REGION,
            })

        event = {"Records": records}
        result = _execute_function(_functions[func_name], event)

        if result.get("error"):
            err_body = result.get("body") or {}
            err_type = err_body.get("errorType") if isinstance(err_body, dict) else None
            err_msg = err_body.get("errorMessage") if isinstance(err_body, dict) else None
            esm["LastProcessingResult"] = "FAILED"
            logger.warning(
                "ESM: Lambda %s failed processing SQS batch from %s (errorType=%s errorMessage=%s)\n%s",
                func_name, queue_name, err_type, err_msg, result.get("log", ""),
            )
        else:
            receipt_handles = {msg["receipt_handle"] for msg in batch if msg.get("receipt_handle")}
            _sqs._delete_messages_for_esm(queue_url, receipt_handles)
            esm["LastProcessingResult"] = f"OK - {len(batch)} records"
            log_output = result.get("log", "")
            if log_output:
                logger.info("ESM: Lambda %s output:\n%s", func_name, log_output)
            logger.info("ESM: Lambda %s processed %d SQS messages from %s", func_name, len(batch), queue_name)


def _poll_kinesis():
    from ministack.services import kinesis as _kin

    for esm in list(_esms.values()):
        if not esm.get("Enabled", True):
            continue
        source_arn = esm.get("EventSourceArn", "")
        if ":kinesis:" not in source_arn:
            continue

        func_name = esm["FunctionName"]
        if func_name not in _functions:
            continue

        stream_name = source_arn.split("/")[-1]
        stream = _kin._streams.get(stream_name)
        if not stream or stream["StreamStatus"] != "ACTIVE":
            continue

        esm_id = esm["UUID"]
        if esm_id not in _kinesis_positions:
            starting = esm.get("StartingPosition", "LATEST")
            _kinesis_positions[esm_id] = {}
            for shard_id, shard in stream["shards"].items():
                if starting == "TRIM_HORIZON":
                    _kinesis_positions[esm_id][shard_id] = 0
                else:
                    _kinesis_positions[esm_id][shard_id] = len(shard["records"])

        batch_size = esm.get("BatchSize", 100)
        positions = _kinesis_positions[esm_id]

        for shard_id, shard in stream["shards"].items():
            if shard_id not in positions:
                positions[shard_id] = len(shard["records"])
                continue

            pos = positions[shard_id]
            raw_records = shard["records"][pos:pos + batch_size]
            if not raw_records:
                continue

            records = []
            for r in raw_records:
                data_val = r["Data"]
                if isinstance(data_val, bytes):
                    data_b64 = base64.b64encode(data_val).decode("ascii")
                elif isinstance(data_val, str):
                    try:
                        base64.b64decode(data_val, validate=True)
                        data_b64 = data_val
                    except Exception:
                        data_b64 = base64.b64encode(data_val.encode("utf-8")).decode("ascii")
                else:
                    data_b64 = base64.b64encode(str(data_val).encode("utf-8")).decode("ascii")

                records.append({
                    "kinesis": {
                        "kinesisSchemaVersion": "1.0",
                        "partitionKey": r["PartitionKey"],
                        "sequenceNumber": r["SequenceNumber"],
                        "data": data_b64,
                        "approximateArrivalTimestamp": r["ApproximateArrivalTimestamp"],
                    },
                    "eventSource": "aws:kinesis",
                    "eventVersion": "1.0",
                    "eventID": f"{shard_id}:{r['SequenceNumber']}",
                    "eventName": "aws:kinesis:record",
                    "invokeIdentityArn": f"arn:aws:iam::{get_account_id()}:role/lambda-role",
                    "awsRegion": REGION,
                    "eventSourceARN": source_arn,
                })

            event = {"Records": records}
            result = _execute_function(_functions[func_name], event)

            if result.get("error"):
                err_body = result.get("body") or {}
                err_type = err_body.get("errorType") if isinstance(err_body, dict) else None
                err_msg = err_body.get("errorMessage") if isinstance(err_body, dict) else None
                esm["LastProcessingResult"] = "FAILED"
                logger.warning(
                    "ESM: Lambda %s failed processing Kinesis batch from %s/%s (errorType=%s errorMessage=%s)\n%s",
                    func_name, stream_name, shard_id, err_type, err_msg, result.get("log", ""),
                )
            else:
                positions[shard_id] = pos + len(raw_records)
                esm["LastProcessingResult"] = f"OK - {len(raw_records)} records"
                log_output = result.get("log", "")
                if log_output:
                    logger.info("ESM: Lambda %s output:\n%s", func_name, log_output)
                logger.info(
                    "ESM: Lambda %s processed %d Kinesis records from %s/%s",
                    func_name, len(raw_records), stream_name, shard_id,
                )


def _poll_dynamodb_streams():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if not stream_records:
        return

    for esm in list(_esms.values()):
        if not esm.get("Enabled", True):
            continue
        source_arn = esm.get("EventSourceArn", "")
        if ":dynamodb:" not in source_arn or "/stream/" not in source_arn:
            continue

        func_name = esm["FunctionName"]
        if func_name not in _functions:
            continue

        table_arn = source_arn.split("/stream/")[0]
        table_name = table_arn.split("/")[-1]
        table_records = stream_records.get(table_name, [])
        if not table_records:
            continue

        esm_id = esm["UUID"]
        with _dynamodb_stream_positions_lock:
            if esm_id not in _dynamodb_stream_positions:
                starting = esm.get("StartingPosition", "LATEST")
                if starting == "TRIM_HORIZON":
                    _dynamodb_stream_positions[esm_id] = 0
                else:
                    _dynamodb_stream_positions[esm_id] = len(table_records)
            pos = _dynamodb_stream_positions[esm_id]

        batch_size = esm.get("BatchSize", 100)
        batch = table_records[pos:pos + batch_size]
        if not batch:
            continue

        event = {"Records": batch}
        result = _execute_function(_functions[func_name], event)

        if result.get("error"):
            err_body = result.get("body") or {}
            err_type = err_body.get("errorType") if isinstance(err_body, dict) else None
            err_msg = err_body.get("errorMessage") if isinstance(err_body, dict) else None
            esm["LastProcessingResult"] = "FAILED"
            logger.warning(
                "ESM: Lambda %s failed processing DynamoDB stream batch from %s (errorType=%s errorMessage=%s)\n%s",
                func_name, table_name, err_type, err_msg, result.get("log", ""),
            )
        else:
            with _dynamodb_stream_positions_lock:
                _dynamodb_stream_positions[esm_id] = pos + len(batch)
            esm["LastProcessingResult"] = f"OK - {len(batch)} records"
            log_output = result.get("log", "")
            if log_output:
                logger.info("ESM: Lambda %s output:\n%s", func_name, log_output)
            logger.info(
                "ESM: Lambda %s processed %d DynamoDB stream records from %s",
                func_name, len(batch), table_name,
            )


# ---------------------------------------------------------------------------
# Function URL Config
# ---------------------------------------------------------------------------


def _url_config_key(func_name: str, qualifier: str | None) -> str:
    return f"{func_name}:{qualifier}" if qualifier else func_name


def _create_function_url_config(func_name: str, data: dict, qualifier: str | None):
    if func_name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function not found: {_func_arn(func_name)}", 404)
    key = _url_config_key(func_name, qualifier)
    if key in _function_urls:
        return error_response_json(
            "ResourceConflictException", f"Function URL config already exists for {func_name}", 409
        )
    cfg = {
        "FunctionUrl": f"https://{new_uuid()}.lambda-url.{REGION}.on.aws/",
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
        return error_response_json("ResourceNotFoundException", f"Function URL config not found for {func_name}", 404)
    return json_response(cfg)


def _update_function_url_config(func_name: str, data: dict, qualifier: str | None):
    key = _url_config_key(func_name, qualifier)
    cfg = _function_urls.get(key)
    if not cfg:
        return error_response_json("ResourceNotFoundException", f"Function URL config not found for {func_name}", 404)
    if "AuthType" in data:
        cfg["AuthType"] = data["AuthType"]
    if "Cors" in data:
        cfg["Cors"] = data["Cors"]
    cfg["LastModifiedTime"] = _now_iso()
    return json_response(cfg)


def _delete_function_url_config(func_name: str, qualifier: str | None):
    key = _url_config_key(func_name, qualifier)
    if key not in _function_urls:
        return error_response_json("ResourceNotFoundException", f"Function URL config not found for {func_name}", 404)
    del _function_urls[key]
    return 204, {}, b""


def _list_function_url_configs(func_name: str, query_params: dict):
    configs = [v for k, v in _function_urls.items() if k == func_name or k.startswith(f"{func_name}:")]
    return json_response({"FunctionUrlConfigs": configs})


def reset():
    from ministack.core import lambda_runtime

    _functions.clear()
    _layers.clear()
    _esms.clear()
    _function_urls.clear()
    _kinesis_positions.clear()
    _dynamodb_stream_positions.clear()
    lambda_runtime.reset()
