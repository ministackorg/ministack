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
import contextvars
import copy
import hashlib
import importlib
import io
import json
import logging
import os
import re
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.lambda_runtime import get_or_create_worker, invalidate_worker
from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    _12_DIGIT_RE,
    AccountRegionScopedDict,
    AccountScopedDict,
    _request_account_id,
    _request_region,
    apply_image_prefix,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("lambda")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")


def _emit_lambda_metrics(function_name: str, duration_ms: float,
                         error: bool, throttle: bool) -> None:
    """Publish ``AWS/Lambda`` metrics for a single invocation.

    Mirrors the four headline CloudWatch metrics every Lambda function emits:
    Invocations (count), Errors (count), Duration (ms), Throttles (count).
    Errors are swallowed; instrumentation must never break the primary call.
    """
    try:
        from ministack.services import cloudwatch as _cw
    except Exception:
        return
    dims = {"FunctionName": function_name}
    try:
        _cw.record_metric("AWS/Lambda", "Invocations", 1, "Count", dims)
        _cw.record_metric(
            "AWS/Lambda", "Errors", 1 if error else 0, "Count", dims,
        )
        _cw.record_metric(
            "AWS/Lambda", "Throttles", 1 if throttle else 0, "Count", dims,
        )
        if duration_ms > 0:
            _cw.record_metric(
                "AWS/Lambda", "Duration", duration_ms, "Milliseconds", dims,
            )
    except Exception:
        logger.debug("emit lambda metrics failed", exc_info=True)


def _xray_trace_id_for_invocation(config: dict, inbound_trace_header: str | None = None) -> str | None:
    """Return the value to set as ``_X_AMZN_TRACE_ID`` for an invocation.

    AWS Lambda exposes this env var to the runtime when ``TracingConfig.Mode``
    is ``Active``. Format per AWS X-Ray docs:
    ``Root=1-<8hex_epoch>-<24hex_random>;Parent=<16hex_random>;Sampled=1``.
    If the inbound request already carries an ``X-Amzn-Trace-Id`` header (a
    chained Lambda → Lambda invocation), prefer it so traces stitch across
    hops; otherwise synthesize a fresh root segment. Returns ``None`` when
    tracing is not Active and no inbound header is present, so the caller can
    skip the env-var entirely.
    """
    if inbound_trace_header:
        return inbound_trace_header
    mode = (config.get("TracingConfig") or {}).get("Mode", "PassThrough")
    if mode != "Active":
        return None
    epoch_hex = format(int(time.time()), "08x")
    root_random = secrets.token_hex(12)   # 24 hex chars
    parent = secrets.token_hex(8)          # 16 hex chars
    return f"Root=1-{epoch_hex}-{root_random};Parent={parent};Sampled=1"


def _account_from_arn(arn: str) -> str:
    """Extract the 12-digit account ID from a Lambda function ARN.

    Falls back to the host's AWS_ACCESS_KEY_ID if the ARN is malformed."""
    try:
        parts = arn.split(":")
        if len(parts) >= 5 and _12_DIGIT_RE.match(parts[4]):
            return parts[4]
    except (AttributeError, TypeError):
        pass
    return os.environ.get("AWS_ACCESS_KEY_ID", "test")


REGION = os.environ.get("MINISTACK_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
LAMBDA_EXECUTOR = os.environ.get("LAMBDA_EXECUTOR", "local").lower()
LAMBDA_DOCKER_VOLUME_MOUNT = os.environ.get("LAMBDA_REMOTE_DOCKER_VOLUME_MOUNT", "")
LAMBDA_DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "") or os.environ.get("LAMBDA_DOCKER_NETWORK", "")
LAMBDA_DOCKER_FLAGS = os.environ.get("LAMBDA_DOCKER_FLAGS", "")
# LAMBDA_STRICT=1 → AWS-fidelity mode: every invocation must run in Docker via
# the AWS RIE image (matching fzonneveld's "docker = docker, no fallbacks"
# rule). When set, the warm-worker / local-subprocess fallbacks are disabled
# and missing Docker is surfaced as a clean Runtime.DockerUnavailable error
# instead of silently degrading to an in-process execution that diverges from
# real AWS semantics.
LAMBDA_STRICT = os.environ.get("LAMBDA_STRICT", "0").lower() in ("1", "true", "yes")

# Lambda proxy mode: invocations are forwarded to a user-managed HTTP
# container instead of running in ministack's worker pool. The container
# receives the Lambda event JSON as the request body and replies with the
# response JSON. Lets users back languages AWS doesn't ship a runtime for
# (PHP, Deno, etc.) with their existing dev container, without bloating
# ministack's image with per-language runtimes.
#
# Configured per-function via the standard CreateFunction API by setting
# Environment.Variables.MINISTACK_LAMBDA_PROXY_URL, or globally per name via
# the env var MINISTACK_LAMBDA_PROXY_<func_name>.
_PROXY_ENV_VAR = "MINISTACK_LAMBDA_PROXY_URL"
_PROXY_PREFIX = "MINISTACK_LAMBDA_PROXY_"


def _proxy_url_for(config: dict) -> str | None:
    env = (config.get("Environment") or {}).get("Variables") or {}
    url = env.get(_PROXY_ENV_VAR)
    if url:
        return url
    name = config.get("FunctionName") or ""
    return os.environ.get(_PROXY_PREFIX + name) or None


try:
    docker_lib: Any = importlib.import_module("docker")
    _docker_available = True
except ImportError:
    docker_lib = None
    _docker_available = False

_cached_docker_client = None
_is_in_container: bool | None = None


def _running_in_container() -> bool:
    """Detect if we're running inside a Docker/Podman container."""
    global _is_in_container
    if _is_in_container is not None:
        return _is_in_container
    # /.dockerenv is created by Docker; /run/.containerenv by Podman
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        _is_in_container = True
        return True
    # Fall back to checking cgroup (works on most Linux container runtimes)
    try:
        with open("/proc/1/cgroup", "r") as f:
            content = f.read()
            if "docker" in content or "containerd" in content or "lxc" in content:
                _is_in_container = True
                return True
    except (OSError, IOError):
        pass
    _is_in_container = False
    return False


def _get_docker_client():
    """Return a cached Docker client, or create one on first call."""
    global _cached_docker_client
    if _cached_docker_client is not None:
        return _cached_docker_client
    if not _docker_available:
        return None
    try:
        _cached_docker_client = docker_lib.from_env()
        return _cached_docker_client
    except Exception:
        return None

_functions = AccountRegionScopedDict()  # function_name -> FunctionRecord
_layers = AccountRegionScopedDict()  # layer_name -> {"versions": [...], "next_version": int}
_esms = AccountRegionScopedDict()  # uuid -> esm dict
_function_urls = AccountRegionScopedDict()  # function_name -> FunctionUrlConfig dict
_poller_started = False
_poller_lock = threading.Lock()

_RESERVED_RUNTIME_ENV_VARS = {
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_LAMBDA_FUNCTION_NAME",
    "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
    "AWS_LAMBDA_FUNCTION_VERSION",
    "AWS_LAMBDA_LOG_STREAM_NAME",
    "AWS_LAMBDA_RUNTIME_API",
    "_HANDLER",
    "_LAMBDA_CODE_DIR",
    "_LAMBDA_HANDLER_MODULE",
    "_LAMBDA_HANDLER_FUNC",
    "_LAMBDA_FUNCTION_ARN",
    "_LAMBDA_LAYERS_DIRS",
    "_LAMBDA_TIMEOUT",
    "LAMBDA_TASK_ROOT",
}


def _runtime_env_vars(config: dict) -> dict:
    env = (config.get("Environment") or {}).get("Variables") or {}
    return {key: value for key, value in env.items() if key not in _RESERVED_RUNTIME_ENV_VARS}


# ── Persistence ────────────────────────────────────────────

def get_state():
    """Return JSON-serializable state. code_zip bytes are base64-encoded."""
    funcs = AccountRegionScopedDict()
    # Iterate _data directly to capture ALL accounts and regions, not just
    # current request context.
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
        # Stream-poll offsets must persist with the ESM record they
        # belong to — without this, every warm-boot replays from the
        # configured StartingPosition (TRIM_HORIZON ⇒ full backlog),
        # violating at-least-once delivery semantics.
        "kinesis_positions": copy.deepcopy(_kinesis_positions),
        "dynamodb_stream_positions": copy.deepcopy(_dynamodb_stream_positions),
    }


def restore_state(data):
    if data:
        funcs = data.get("functions", {})
        if isinstance(funcs, AccountRegionScopedDict):
            for scoped_key, func in funcs._data.items():
                if func.get("code_zip") and isinstance(func["code_zip"], str):
                    func["code_zip"] = base64.b64decode(func["code_zip"])
                for ver in func.get("versions", {}).values():
                    if ver.get("code_zip") and isinstance(ver["code_zip"], str):
                        ver["code_zip"] = base64.b64decode(ver["code_zip"])
                _functions._data[scoped_key] = func
        elif isinstance(funcs, AccountScopedDict):
            for (account_id, name), func in funcs._data.items():
                if func.get("code_zip") and isinstance(func["code_zip"], str):
                    func["code_zip"] = base64.b64decode(func["code_zip"])
                for ver in func.get("versions", {}).values():
                    if ver.get("code_zip") and isinstance(ver["code_zip"], str):
                        ver["code_zip"] = base64.b64decode(ver["code_zip"])
                region = _region_from_function_record(func)
                _functions._data[(account_id, region, name)] = func
        else:
            for name, func in funcs.items():
                if func.get("code_zip") and isinstance(func["code_zip"], str):
                    func["code_zip"] = base64.b64decode(func["code_zip"])
                for ver in func.get("versions", {}).values():
                    if ver.get("code_zip") and isinstance(ver["code_zip"], str):
                        ver["code_zip"] = base64.b64decode(ver["code_zip"])
                region = _region_from_function_record(func)
                _functions._data[(get_account_id(), region, name)] = func
        _layers.update(data.get("layers", {}))
        _esms.update(data.get("esms", {}))
        _function_urls.update(data.get("function_urls", {}))
        _restore_esm_positions(_kinesis_positions, data.get("kinesis_positions", {}))
        _restore_esm_positions(_dynamodb_stream_positions, data.get("dynamodb_stream_positions", {}))
        if _esms.has_any():
            _ensure_poller()


def _region_from_function_record(func: dict) -> str:
    arn = (func.get("config") or {}).get("FunctionArn") or func.get("FunctionArn") or ""
    if arn:
        try:
            _account_id, region = _lambda_function_account_region_from_arn(arn)
            return region
        except ArnParseError:
            pass
    return get_region()


def _restore_esm_positions(store: AccountRegionScopedDict, positions) -> None:
    if isinstance(positions, AccountRegionScopedDict):
        store.update(positions)
        return

    esm_scopes = {
        (account_id, esm_id): region
        for (account_id, region, esm_id) in _esms._data
    }

    if isinstance(positions, AccountScopedDict):
        for (account_id, esm_id), value in positions._data.items():
            region = esm_scopes.get((account_id, esm_id), get_region())
            store._data[(account_id, region, esm_id)] = value
        return

    if isinstance(positions, dict):
        for key, value in positions.items():
            if isinstance(key, tuple) and len(key) == 3:
                store._data[key] = value
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, esm_id = key
                region = esm_scopes.get((account_id, esm_id), get_region())
                store._data[(account_id, region, esm_id)] = value
            else:
                account_id = get_account_id()
                region = esm_scopes.get((account_id, key), get_region())
                store._data[(account_id, region, key)] = value


# NOTE: the persisted-state load used to run here, but ``restore_state`` calls
# ``_ensure_poller()`` when the restored data contains event source mappings,
# and that helper is defined much later in this module. Restoring at import
# time raised ``NameError: _ensure_poller`` on warm starts with a populated
# ``lambda.json`` (issue #412). The load now lives at the bottom of the file,
# after ``_ensure_poller`` is defined.


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
const { pathToFileURL } = require('url');

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
process.stdin.on('end', async () => {
  const event = JSON.parse(input);
  const fullPath = path.resolve(codeDir, modPath);
  let mod;
  let resolvedPath;
  try {
    resolvedPath = require.resolve(fullPath);
    mod = require(resolvedPath);
  } catch (reqErr) {
    if (reqErr.code === 'ERR_REQUIRE_ESM' && resolvedPath) {
      mod = await import(pathToFileURL(resolvedPath).href);
    } else if (reqErr.code === 'MODULE_NOT_FOUND') {
      const mjsPath = fullPath + '.mjs';
      const missingHandlerEntry =
        (reqErr.message && reqErr.message.includes("'" + fullPath + "'")) ||
        (resolvedPath && reqErr.message && reqErr.message.includes("'" + resolvedPath + "'"));
      if (missingHandlerEntry && fs.existsSync(mjsPath)) {
        mod = await import(pathToFileURL(mjsPath).href);
      } else {
        throw reqErr;
      }
    } else {
      throw reqErr;
    }
  }
  const handler = mod[fnName] || (mod.default && mod.default[fnName]) || mod.default;
  if (typeof handler !== 'function') {
    process.stderr.write(
      "Handler '" + fnName + "' in module '" + modPath + "' is undefined or not a function"
    );
    process.exit(1);
  }
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
        name, _qualifier = _lambda_function_name_and_qualifier_from_arn(name_or_arn)
        return name or name_or_arn
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
        name, qualifier = _lambda_function_name_and_qualifier_from_arn(name_or_arn)
        return (name, qualifier) if name else (name_or_arn, None)
    if ":" in name_or_arn:
        name, qualifier = name_or_arn.split(":", 1)
        return name, qualifier or None
    return name_or_arn, None


def _lambda_function_name_and_qualifier_from_arn(value: str) -> tuple[str, str | None]:
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return "", None
    if spec.service != "lambda":
        return "", None
    parts = spec.resource.split(":", 2)
    if len(parts) < 2 or parts[0] != "function" or not parts[1]:
        return "", None
    qualifier = parts[2] if len(parts) == 3 and parts[2] else None
    return parts[1], qualifier


def _resolve_request_scoped_name(name_or_arn: str) -> str:
    """Resolve a Lambda API target without crossing account/region scope."""
    name, _qualifier = _resolve_request_scoped_name_and_qualifier(name_or_arn)
    return name


def _resolve_request_scoped_name_and_qualifier(name_or_arn: str) -> tuple[str, str | None]:
    if not name_or_arn:
        return "", None
    if not name_or_arn.startswith("arn:"):
        return _resolve_name_and_qualifier(name_or_arn)
    try:
        spec = parse_arn(name_or_arn)
    except ArnParseError:
        return name_or_arn, None
    if (
        spec.service != "lambda"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return name_or_arn, None
    name, qualifier = _lambda_function_name_and_qualifier_from_arn(name_or_arn)
    return (name, qualifier) if name else (name_or_arn, None)


def _lambda_function_account_region_from_arn(value: str) -> tuple[str, str]:
    spec = parse_arn(value)
    if spec.service != "lambda":
        raise ArnParseError("arn: expected lambda service")
    if not _12_DIGIT_RE.match(spec.account_id):
        raise ArnParseError("arn: expected 12-digit account id")
    if not spec.region:
        raise ArnParseError("arn: expected region")
    name, _qualifier = _lambda_function_name_and_qualifier_from_arn(value)
    if not name:
        raise ArnParseError("arn: expected lambda function resource")
    return spec.account_id, spec.region


def _account_region_from_function_config(config: dict) -> tuple[str, str]:
    arn = config.get("FunctionArn", "")
    if not arn:
        raise ArnParseError("arn: missing lambda function arn")
    return _lambda_function_account_region_from_arn(arn)


def _parse_required_arn(value: str, param_name: str):
    try:
        return parse_arn(value), None
    except ArnParseError:
        return None, error_response_json(
            "ValidationException",
            f"Value '{value}' at '{param_name}' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:",
            400,
        )


def _invalid_parameter_value(message: str):
    return error_response_json("InvalidParameterValueException", message, 400)


def _sqs_queue_name_from_arn_spec(spec) -> str | None:
    if spec.service != "sqs" or not spec.resource or ":" in spec.resource:
        return None
    return spec.resource


def _sns_topic_name_from_arn_spec(spec) -> str | None:
    if spec.service != "sns" or not spec.resource or ":" in spec.resource:
        return None
    return spec.resource


def _kinesis_stream_name_from_arn_spec(spec) -> str | None:
    prefix = "stream/"
    if spec.service != "kinesis" or not spec.resource.startswith(prefix):
        return None
    name = spec.resource[len(prefix):]
    if not name or "/" in name:
        return None
    return name


def _dynamodb_stream_table_from_arn_spec(spec) -> tuple[str, str] | None:
    prefix = "table/"
    marker = "/stream/"
    if spec.service != "dynamodb" or not spec.resource.startswith(prefix) or marker not in spec.resource:
        return None
    table_name, stream_label = spec.resource[len(prefix):].split(marker, 1)
    if not table_name or not stream_label or "/" in table_name:
        return None
    return table_name, stream_label


def _lambda_function_name_from_arn_spec(spec) -> str | None:
    if spec.service != "lambda":
        return None
    parts = spec.resource.split(":", 2)
    if len(parts) < 2 or parts[0] != "function" or not parts[1]:
        return None
    return parts[1]


def _require_same_account_region_arn(spec, target_arn: str, target_kind: str):
    if spec.account_id != get_account_id():
        return _invalid_parameter_value(f"The {target_kind} ARN {target_arn} is invalid.")
    if spec.region != get_region():
        return _invalid_parameter_value(
            f"The {target_kind} ARN {target_arn} must be in the same region as the Lambda function."
        )
    return None


def _validate_event_source_arn(event_source_arn: str):
    spec, err = _parse_required_arn(event_source_arn, "eventSourceArn")
    if err:
        return None, err
    if spec.service not in {"sqs", "kinesis", "dynamodb"}:
        return None, _invalid_parameter_value(f"Unsupported event source ARN: {event_source_arn}")
    scope_err = _require_same_account_region_arn(spec, event_source_arn, "event source")
    if scope_err:
        return None, scope_err
    if spec.service == "sqs" and not _sqs_queue_name_from_arn_spec(spec):
        return None, _invalid_parameter_value(f"Invalid event source ARN: {event_source_arn}")
    if spec.service == "kinesis" and not _kinesis_stream_name_from_arn_spec(spec):
        return None, _invalid_parameter_value(f"Invalid event source ARN: {event_source_arn}")
    if spec.service == "dynamodb" and not _dynamodb_stream_table_from_arn_spec(spec):
        return None, _invalid_parameter_value(f"Invalid event source ARN: {event_source_arn}")
    return spec, None


def _validate_async_destination_arn(destination_arn: str):
    spec, err = _parse_required_arn(destination_arn, "destinationConfig.destination")
    if err:
        return err
    if spec.service not in {"sqs", "sns", "lambda", "events", "s3"}:
        return _invalid_parameter_value(f"The destination ARN {destination_arn} is invalid.")
    if spec.service in {"sqs", "sns", "lambda"}:
        scope_err = _require_same_account_region_arn(spec, destination_arn, "destination")
        if scope_err:
            return scope_err
    if spec.service == "sqs" and not _sqs_queue_name_from_arn_spec(spec):
        return _invalid_parameter_value(f"The destination ARN {destination_arn} is invalid.")
    if spec.service == "sns" and not _sns_topic_name_from_arn_spec(spec):
        return _invalid_parameter_value(f"The destination ARN {destination_arn} is invalid.")
    if spec.service == "lambda" and not _lambda_function_name_from_arn_spec(spec):
        return _invalid_parameter_value(f"The destination ARN {destination_arn} is invalid.")
    return None


def _validate_destination_config(destination_config: dict | None):
    if not destination_config:
        return None
    for key in ("OnSuccess", "OnFailure"):
        destination_arn = (destination_config.get(key) or {}).get("Destination")
        if not destination_arn:
            continue
        err = _validate_async_destination_arn(destination_arn)
        if err:
            return err
    return None


def _validate_dead_letter_config(dead_letter_config: dict | None):
    target_arn = (dead_letter_config or {}).get("TargetArn")
    if not target_arn:
        return None
    spec, err = _parse_required_arn(target_arn, "deadLetterConfig.targetArn")
    if err:
        return err
    if spec.service not in {"sqs", "sns"}:
        return _invalid_parameter_value(
            "Invalid dead letter queue ARN: The service specified by the TargetArn is not supported."
        )
    scope_err = _require_same_account_region_arn(spec, target_arn, "dead letter queue")
    if scope_err:
        return scope_err
    if spec.service == "sqs" and not _sqs_queue_name_from_arn_spec(spec):
        return _invalid_parameter_value(f"Invalid dead letter queue ARN: {target_arn}")
    if spec.service == "sns" and not _sns_topic_name_from_arn_spec(spec):
        return _invalid_parameter_value(f"Invalid dead letter queue ARN: {target_arn}")
    return None


def _func_arn(name: str) -> str:
    return f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{name}"


def _layer_arn(name: str) -> str:
    return f"arn:aws:lambda:{get_region()}:{get_account_id()}:layer:{name}"


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


def _fetch_code_from_s3(bucket: str, key: str, version_id: str | None = None) -> bytes | None:
    """Fetch Lambda code zip from the in-memory S3 service.

    `version_id` matches AWS Lambda's `Code.S3ObjectVersion` — when set,
    fetches that specific S3 object version instead of the latest."""
    try:
        from ministack.services import s3 as s3_svc
        obj = s3_svc._get_object_data(bucket, key, version_id=version_id)
        if obj is not None:
            return obj
    except Exception as e:
        logger.warning(
            "Failed to fetch Lambda code from s3://%s/%s%s: %s",
            bucket, key, f"?versionId={version_id}" if version_id else "", e,
        )
    return None


_UNZIPPED_LIMIT_BYTES = 262144000  # 250 MiB — AWS hard limit


def _validate_unzipped_size(zip_data: bytes | None):
    if not zip_data:
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            unzipped_size = sum(info.file_size for info in zf.infolist())
    except zipfile.BadZipFile:
        return None
    if unzipped_size > _UNZIPPED_LIMIT_BYTES:
        return error_response_json(
            "InvalidParameterValueException",
            f"Unzipped size must be smaller than {_UNZIPPED_LIMIT_BYTES} bytes",
            400,
        )
    return None


import contextvars

# Per-invocation durable-execution context, set by `_invoke` and read by the
# executor functions to inject env vars into the Lambda container. Keeps
# the data flow explicit and concurrency-safe (each request has its own
# context — ASGI + asyncio.to_thread both propagate ContextVars).
_durable_ctx: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "lambda_durable_ctx", default=None,
)


def _durable_env_overlay() -> dict[str, str]:
    """Return env vars to layer onto the Lambda container when the current
    invocation belongs to a durable execution. Empty dict otherwise."""
    ctx = _durable_ctx.get()
    if not ctx:
        return {}
    return {
        "AWS_LAMBDA_DURABLE_EXECUTION_ARN": ctx.get("arn", ""),
        "AWS_LAMBDA_DURABLE_CHECKPOINT_TOKEN": ctx.get("token", ""),
        "AWS_LAMBDA_DURABLE_EXECUTION_NAME": ctx.get("name", ""),
    }


def invoke_durable_resume(function_name: str, durable_arn: str, original_event: dict) -> None:
    """Re-invoke a paused durable function with the existing execution ARN
    and the now-populated operations log. Called by the resume scheduler
    when a WAIT expires."""
    from ministack.services import lambda_durable
    rec = lambda_durable._executions.get(durable_arn)
    if not rec:
        return
    canonical = _resolve_name(function_name)
    func = _functions.get(canonical)
    if not func:
        return
    config = func.get("config") or func
    # Set the durable context to the SAME ARN/token so the SDK reads the
    # accumulated operations as InitialExecutionState (replay path).
    _durable_ctx.set({
        "arn": durable_arn,
        "token": rec["CheckpointToken"],
        "name": rec["DurableExecutionName"],
    })
    resume_event = {
        "DurableExecutionArn": durable_arn,
        "CheckpointToken": rec["CheckpointToken"],
        "InitialExecutionState": {
            "Operations": lambda_durable._serialize_operations(rec["Operations"], for_event=True),
            "NextMarker": "",
        },
    }
    try:
        result = _execute_function(func, resume_event)
        # If the resume still returns PENDING, schedule the next wakeup.
        try:
            payload = result.get("body")
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8", errors="replace")
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict) and payload.get("Status") == "PENDING":
                lambda_durable.schedule_resume(durable_arn)
            elif isinstance(payload, dict) and payload.get("Status") == "SUCCEEDED":
                lambda_durable.mark_execution_completed(
                    durable_arn,
                    result_payload=payload.get("Result"),
                    error=None,
                )
            elif isinstance(payload, dict) and payload.get("Status") == "FAILED":
                lambda_durable.mark_execution_completed(
                    durable_arn,
                    result_payload=None,
                    error=payload.get("Error"),
                )
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    finally:
        _durable_ctx.set(None)


def _durable_arn_lookup(name_or_arn: str) -> str | None:
    """Resolve a function name or ARN to the canonical function ARN, or None
    if the function doesn't exist. Used by lambda_durable.handle_list_by_function."""
    try:
        canonical = _resolve_name(name_or_arn)
    except Exception:
        return None
    func = _functions.get(canonical)
    if not func:
        return None
    return func.get("FunctionArn") or _func_arn(canonical)


def _build_config(name: str, data: dict, code_zip: bytes | None = None) -> dict:
    code_size = len(code_zip) if code_zip else 0
    code_sha = base64.b64encode(hashlib.sha256(code_zip).digest()).decode() if code_zip else ""
    is_image = data.get("PackageType", "Zip") == "Image"

    layers_cfg = []
    for layer in data.get("Layers", []):
        if isinstance(layer, str):
            layers_cfg.append({"Arn": layer, "CodeSize": _layer_codesize_for_arn(layer)})
        elif isinstance(layer, dict):
            layer = dict(layer)
            if "CodeSize" not in layer and "Arn" in layer:
                layer["CodeSize"] = _layer_codesize_for_arn(layer["Arn"])
            layers_cfg.append(layer)

    env = data.get("Environment")
    if env is not None and "Variables" not in env:
        env["Variables"] = {}

    config = {
        "FunctionName": name,
        "FunctionArn": _func_arn(name),
        "Runtime": data.get("Runtime", "" if is_image else "python3.12"),
        "Role": data.get("Role", f"arn:aws:iam::{get_account_id()}:role/lambda-role"),
        "Handler": data.get("Handler", "" if is_image else "index.handler"),
        "CodeSize": code_size,
        "CodeSha256": code_sha,
        "Description": data.get("Description", ""),
        "Timeout": data.get("Timeout", 3),
        "MemorySize": data.get("MemorySize", 128),
        "LastModified": _now_iso(),
        "Version": "$LATEST",
        # AWS-match: CreateFunction returns State=Pending, transitions to Active
        # asynchronously once the runtime is ready. Terraform's FunctionActive
        # waiter polls for State=Active before invoking.
        "State": "Pending",
        "StateReason": "The function is being created.",
        "StateReasonCode": "Creating",
        "LastUpdateStatus": "InProgress",
        "LastUpdateStatusReason": "",
        "LastUpdateStatusReasonCode": "",
        "PackageType": data.get("PackageType", "Zip"),
        "Architectures": data.get("Architectures", ["x86_64"]),
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
    if env is not None:
        config["Environment"] = env
    dlc = data.get("DeadLetterConfig")
    if dlc and dlc.get("TargetArn"):
        config["DeadLetterConfig"] = dlc
    # 2026-era optional config blocks — stored when provided so DescribeFunction
    # round-trips correctly. Only emitted when explicitly set, matching AWS.
    if "DurableConfig" in data:
        config["DurableConfig"] = data["DurableConfig"]
    if "TenancyConfig" in data:
        config["TenancyConfig"] = data["TenancyConfig"]
    if "CapacityProviderConfig" in data:
        config["CapacityProviderConfig"] = data["CapacityProviderConfig"]
    # FileSystemConfigs is opaque at the wire level — historically EFS access
    # points (fsap-*), and as of 2026-04 also S3 bucket ARNs for the S3 mount
    # feature. The emulator doesn't actually mount; it just round-trips the
    # config so SDK/CFN/Terraform reads see what was set.
    if "FileSystemConfigs" in data:
        config["FileSystemConfigs"] = data["FileSystemConfigs"]
    return config


def _lambda_layer_version_name_and_number_from_arn_spec(spec) -> tuple[str, int] | None:
    if spec.service != "lambda":
        return None
    parts = spec.resource.split(":", 2)
    if len(parts) != 3 or parts[0] != "layer" or not parts[1] or not parts[2].isdigit():
        return None
    return parts[1], int(parts[2])


def _invalid_layer_version_arn(layer_arn: str):
    return error_response_json(
        "InvalidParameterValueException",
        f"Layer version ARN {layer_arn} is invalid.",
        400,
    )


def _resolve_layer_version_for_attachment(layer_arn: str):
    try:
        spec = parse_arn(layer_arn)
    except ArnParseError:
        return None, _invalid_layer_version_arn(layer_arn)

    layer_ref = _lambda_layer_version_name_and_number_from_arn_spec(spec)
    if layer_ref is None:
        return None, _invalid_layer_version_arn(layer_arn)

    if spec.account_id != get_account_id():
        return None, error_response_json(
            "AccessDeniedException",
            "User is not authorized to access this resource.",
            403,
        )
    if spec.region != get_region():
        return None, error_response_json(
            "InvalidParameterValueException",
            f"Layer version ARN {layer_arn} is in region {spec.region}; "
            f"function region is {get_region()}",
            400,
        )

    layer_name, version = layer_ref
    layer = _layers.get_scoped(spec.account_id, spec.region, layer_name)
    if not layer:
        return None, error_response_json(
            "InvalidParameterValueException",
            f"Layer version {layer_arn} does not exist.",
            400,
        )
    for version_config in layer["versions"]:
        if version_config["Version"] == version:
            return version_config, None
    return None, error_response_json(
        "InvalidParameterValueException",
        f"Layer version {layer_arn} does not exist.",
        400,
    )


def _layer_config_from_version(layer_arn: str, version_config: dict) -> dict:
    return {
        "Arn": layer_arn,
        "CodeSize": version_config.get("Content", {}).get("CodeSize", 0),
    }


def _normalize_layer_attachments(layers: list | None):
    layers_cfg = []
    for layer in layers or []:
        layer_arn = layer if isinstance(layer, str) else (layer or {}).get("Arn")
        if not layer_arn:
            return None, _invalid_layer_version_arn(str(layer_arn or ""))
        version_config, err = _resolve_layer_version_for_attachment(layer_arn)
        if err:
            return None, err
        normalized = _layer_config_from_version(layer_arn, version_config)
        if isinstance(layer, dict):
            normalized.update({k: v for k, v in layer.items() if k not in normalized})
        layers_cfg.append(normalized)
    return layers_cfg, None




def _qp_first(query_params: dict, key: str, default: str = "") -> str:
    """Return the first value for *key* from raw query_params (list or str)."""
    val = query_params.get(key, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _qualifier_from_path_or_query(path_qualifier: str | None, query_params: dict | None) -> str | None:
    return path_qualifier or _qp_first(query_params or {}, "Qualifier") or None


def _get_func_record_for_qualifier(name: str, qualifier: str | None) -> tuple[dict | None, dict | None]:
    """Return (func_record, effective_config) for a given name + qualifier.

    For $LATEST or None, returns the primary record/config.
    For a version number, returns the versioned snapshot.
    For an alias, resolves to the alias target version.
    """
    func = _functions.get(name)
    return _effective_func_record_for_qualifier(func, qualifier)


def _effective_func_record_for_qualifier(func: dict | None, qualifier: str | None) -> tuple[dict | None, dict | None]:
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
        return None, None

    ver = func["versions"].get(qualifier)
    if ver:
        return ver, ver["config"]

    return None, None


def _function_qualifier_exists(func: dict | None, qualifier: str | None) -> bool:
    if func is None:
        return False
    if qualifier is None or qualifier == "$LATEST":
        return True
    return qualifier in func.get("aliases", {}) or qualifier in func.get("versions", {})


def _function_qualifier_not_found(func_name: str, qualifier: str | None):
    suffix = f":{qualifier}" if qualifier else ""
    return error_response_json(
        "ResourceNotFoundException",
        f"Function not found: {_func_arn(func_name)}{suffix}",
        404,
    )


def _validate_function_qualifier_exists(func_name: str, qualifier: str | None):
    if not _function_qualifier_exists(_functions.get(func_name), qualifier):
        return _function_qualifier_not_found(func_name, qualifier)
    return None


def _validate_latest_mutation_qualifier(func_name: str, qualifier: str | None):
    if not qualifier or qualifier == "$LATEST":
        return None
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
    return error_response_json(
        "InvalidParameterValueException",
        "Function updates can only target the unqualified function or $LATEST.",
        400,
    )


def _get_func_record_for_ref(function_ref: str) -> tuple[dict | None, dict | None, str]:
    if isinstance(function_ref, str) and function_ref.startswith("arn:"):
        try:
            spec = parse_arn(function_ref)
        except ArnParseError:
            spec = None
        if spec and spec.service == "lambda":
            name, qualifier = _lambda_function_name_and_qualifier_from_arn(function_ref)
            if name:
                if spec.account_id != get_account_id():
                    return None, None, name
                func = _functions.get_scoped(get_account_id(), spec.region, name)
                if not _function_qualifier_exists(func, qualifier):
                    return None, None, name
                record, config = _effective_func_record_for_qualifier(func, qualifier)
                return record, config, name

    name, qualifier = _resolve_name_and_qualifier(function_ref)
    func = _functions.get(name)
    if not _function_qualifier_exists(func, qualifier):
        return None, None, name
    record, config = _effective_func_record_for_qualifier(func, qualifier)
    return record, config, name


def _get_base_func_record_for_ref(function_ref: str) -> tuple[dict | None, dict | None, str]:
    if isinstance(function_ref, str) and function_ref.startswith("arn:"):
        try:
            spec = parse_arn(function_ref)
        except ArnParseError:
            spec = None
        if spec and spec.service == "lambda":
            name, _qualifier = _lambda_function_name_and_qualifier_from_arn(function_ref)
            if name:
                if spec.account_id != get_account_id():
                    return None, None, name
                func = _functions.get_scoped(get_account_id(), spec.region, name)
                return func, (func or {}).get("config"), name

    name, _qualifier = _resolve_name_and_qualifier(function_ref)
    func = _functions.get(name)
    return func, (func or {}).get("config"), name


def _get_func_record_for_ref_in_scope(
    function_ref: str,
    *,
    account_id: str | None = None,
    region: str | None = None,
) -> tuple[dict | None, dict | None, str]:
    account_token = _request_account_id.set(account_id) if account_id else None
    region_token = _request_region.set(region) if region else None
    try:
        return _get_func_record_for_ref(function_ref)
    finally:
        if region_token is not None:
            _request_region.reset(region_token)
        if account_token is not None:
            _request_account_id.reset(account_token)


def _run_with_function_config_scope(func_or_config: dict, callback, *args, **kwargs):
    def _run():
        config = func_or_config.get("config") or func_or_config
        account_id, region = _account_region_from_function_config(config)
        _request_account_id.set(account_id)
        _request_region.set(region)
        return callback(*args, **kwargs)

    return contextvars.copy_context().run(_run)


def _execution_record_for_config(func: dict, config: dict) -> dict:
    record = dict(func or {})
    record["config"] = config
    return record


def _execute_function_with_config_scope(func: dict, event: dict) -> dict:
    return _run_with_function_config_scope(func, _execute_function, func, event)


def _function_config_scope_mismatch(config: dict) -> bool:
    try:
        account_id, region = _account_region_from_function_config(config)
    except ArnParseError:
        return False
    return account_id != get_account_id() or region != get_region()


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Route Lambda REST API requests."""

    path = unquote(path)
    parts = path.rstrip("/").split("/")

    # --- Durable Execution surface (preview, API version 2025-12-01) ---
    # Routed first because some paths embed the function ARN as a path segment
    # which can otherwise be misclassified.
    from ministack.services import lambda_durable
    durable_resp = lambda_durable.try_route(method, path, body, query_params,
                                             function_arn_lookup=_durable_arn_lookup)
    if durable_resp is not None:
        return durable_resp

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # --- Account Settings: GET /2016-08-19/account-settings ---
    if "/account-settings" in path and method == "GET":
        return _get_account_settings()

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

    # --- Event Invoke Config list: GET /2019-09-25/functions/{name}/event-invoke-config/list ---
    if "/event-invoke-config/list" in path:
        m = re.search(r"/functions/([^/]+)/event-invoke-config/list", path)
        fname = _resolve_request_scoped_name(m.group(1)) if m else ""
        if method == "GET":
            return _list_function_event_invoke_configs(fname, query_params)

    # --- Event Invoke Config: /2019-09-25/functions/{name}/event-invoke-config ---
    if "event-invoke-config" in path:
        m = re.search(r"/functions/([^/]+)/event-invoke-config", path)
        fname = _resolve_request_scoped_name(m.group(1)) if m else ""
        if method == "GET":
            return _get_event_invoke_config(fname)
        if method == "PUT":
            return _put_event_invoke_config(fname, data)
        if method == "DELETE":
            return _delete_event_invoke_config(fname)

    # --- Provisioned Concurrency: /2019-09-30/functions/{name}/provisioned-concurrency ---
    if "provisioned-concurrency" in path:
        m = re.search(r"/functions/([^/]+)/provisioned-concurrency", path)
        fname, path_qualifier = (
            _resolve_request_scoped_name_and_qualifier(m.group(1)) if m else ("", None)
        )
        qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
        if method == "GET":
            return _get_provisioned_concurrency(fname, qualifier)
        if method == "PUT":
            return _put_provisioned_concurrency(fname, qualifier, data)
        if method == "DELETE":
            return _delete_provisioned_concurrency(fname, qualifier)

    # --- Code Signing Config ---
    # Matches real AWS shape: response carries both the function name and the
    # CSC ARN (empty when no config is attached).
    if "code-signing-config" in path:
        m = re.search(r"/functions/([^/]+)/code-signing-config", path)
        fname = _resolve_request_scoped_name(m.group(1)) if m else ""
        if fname and fname in _functions:
            csc_arn = _functions[fname].get("code_signing_config_arn", "") or ""
            if method == "GET":
                return json_response({
                    "FunctionName": fname,
                    "CodeSigningConfigArn": csc_arn,
                })
            if method == "PUT":
                _functions[fname]["code_signing_config_arn"] = data.get("CodeSigningConfigArn", "")
                return json_response({
                    "FunctionName": fname,
                    "CodeSigningConfigArn": _functions[fname]["code_signing_config_arn"],
                })
            if method == "DELETE":
                _functions[fname]["code_signing_config_arn"] = ""
                return 204, {}, b""
        return json_response({"FunctionName": fname, "CodeSigningConfigArn": ""})

    # --- Function URL Config ---
    if "/urls" in path and "/functions/" in path:
        m = re.search(r"/functions/([^/]+)/urls", path)
        fname = _resolve_request_scoped_name(m.group(1)) if m else ""
        if method == "GET":
            return _list_function_url_configs(fname, query_params)
    if "/url" in path and "/functions/" in path:
        m = re.search(r"/functions/([^/]+)/url", path)
        fname, path_qualifier = (
            _resolve_request_scoped_name_and_qualifier(m.group(1)) if m else ("", None)
        )
        qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
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

        func_name, path_qualifier = _resolve_request_scoped_name_and_qualifier(raw_name)
        sub = parts[4] if len(parts) > 4 else None
        sub2 = parts[5] if len(parts) > 5 else None

        # Invoke
        if method == "POST" and sub == "invocations":
            return await _invoke(func_name, data, headers, path_qualifier, query_params)

        # InvokeWithResponseStream: POST .../functions/{name}/response-streaming-invocations
        if method == "POST" and sub == "response-streaming-invocations":
            return await _invoke_with_response_stream(func_name, data, headers, path_qualifier, query_params)

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
                return _get_policy(func_name, query_params, path_qualifier)
            if method == "POST" and not sid:
                return _add_permission(func_name, data, query_params, path_qualifier)
            if method == "DELETE" and sid:
                return _remove_permission(func_name, sid, query_params, path_qualifier)

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
            qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
            return _get_function(func_name, qualifier)

        # GetFunctionConfiguration
        if method == "GET" and sub == "configuration":
            qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
            return _get_function_config(func_name, qualifier)

        # DeleteFunction
        if method == "DELETE" and not sub:
            return _delete_function(func_name, query_params, path_qualifier)

        # UpdateFunctionCode
        if method == "PUT" and sub == "code":
            qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
            err = _validate_latest_mutation_qualifier(func_name, qualifier)
            if err:
                return err
            return _update_code(func_name, data)

        # UpdateFunctionConfiguration
        if method == "PUT" and sub == "configuration":
            qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
            err = _validate_latest_mutation_qualifier(func_name, qualifier)
            if err:
                return err
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
        code_zip = _fetch_code_from_s3(
            code_data["S3Bucket"],
            code_data["S3Key"],
            version_id=code_data.get("S3ObjectVersion"),
        )

    err = _validate_unzipped_size(code_zip)
    if err is not None:
        return err

    if image_uri:
        data.setdefault("PackageType", "Image")

    is_image = data.get("PackageType", "Zip") == "Image"
    if not is_image and not data.get("Runtime"):
        return error_response_json(
            "InvalidParameterValueException",
            "Runtime is required for .zip deployment packages.",
            400,
        )

    err = _validate_dead_letter_config(data.get("DeadLetterConfig"))
    if err:
        return err
    layers_cfg, err = _normalize_layer_attachments(data.get("Layers"))
    if err:
        return err
    if data.get("Layers") is not None:
        data = dict(data)
        data["Layers"] = layers_cfg

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

    _schedule_state_transition(name, _LAMBDA_STATE_TRANSITION_DELAY)
    runtime_or_image = config.get("Runtime") or ("image=" + image_uri if image_uri else "?")
    logger.info("Lambda function created: %s (%s)", name, runtime_or_image)
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
        # AWS resolves ImageUri to a digest for ResolvedImageUri; we echo the
        # configured URI since we don't track image digests.
        code_info = {
            "RepositoryType": "ECR",
            "ImageUri": effective_config["ImageUri"],
            "ResolvedImageUri": effective_config["ImageUri"],
        }
    else:
        # AWS returns a pre-signed S3 URL (expiry ~10 min). We return a URL to
        # a ministack-internal endpoint dressed up with the AWS query params
        # so SDKs + pip-style fetch-and-extract tooling work unchanged.
        code_info = {
            "RepositoryType": "S3",
            "Location": _presigned_code_url(name),
        }
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


def _list_function_event_invoke_configs(func_name: str, query_params: dict):
    """AWS `ListFunctionEventInvokeConfigs` — returns the set of per-qualifier
    event-invoke configs for a function. We store one per function on the
    primary record (no per-qualifier split), so the result is 0 or 1 items."""
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}", 404,
        )
    eic = _functions[func_name].get("event_invoke_config")
    items = []
    if eic:
        arn = _func_arn(func_name)
        items.append({
            "FunctionArn": arn,
            "LastModified": int(time.time()),
            "MaximumRetryAttempts": eic.get("MaximumRetryAttempts", 2),
            "MaximumEventAgeInSeconds": eic.get("MaximumEventAgeInSeconds", 21600),
            "DestinationConfig": eic.get("DestinationConfig", {}),
        })
    return json_response({"FunctionEventInvokeConfigs": items})


def _presigned_code_url(func_name: str) -> str:
    """AWS returns a pre-signed S3 URL for `Code.Location`. We can't sign a
    real S3 object (the zip lives in memory), but we can serve it from a
    ministack endpoint and dress the URL up with the query params SDKs and
    scripts expect, so `pip-style` pull-and-extract code works unchanged.
    """
    host = _MINISTACK_HOST
    port = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))
    qs = (
        f"?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        f"&X-Amz-Expires=600"
        f"&X-Amz-Date={datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        f"&X-Amz-SignedHeaders=host"
        f"&X-Amz-Signature=ministack-local-presigned"
    )
    return (
        f"http://{host}:{port}/_ministack/lambda-code/"
        f"{get_account_id()}/{get_region()}/{func_name}{qs}"
    )


def serve_function_code(func_name: str, account_id: str | None = None, region: str | None = None):
    """Serve the stored zip bytes for a Lambda function. Called by app.py
    when a client follows the pre-signed `Code.Location` URL."""
    if account_id and region:
        func = _functions.get_scoped(account_id, region, func_name)
    else:
        func = _functions.get(func_name)
    if not func:
        return 404, {"Content-Type": "text/plain"}, b"Function not found"
    zip_bytes = func.get("code_zip") or b""
    return 200, {"Content-Type": "application/zip"}, zip_bytes


def _get_account_settings():
    """AWS `GetAccountSettings` — Terraform and some CI tools call this to
    discover the account-level concurrency limits and code size quotas.
    AWS returns:
      { "AccountLimit": {...}, "AccountUsage": {...} }
    """
    # Count across the current request's account scope only.
    total_fns = len(list(_functions.keys()))
    total_code_size = 0
    for _name in _functions.keys():
        fn = _functions[_name]
        cz = fn.get("code_zip")
        if cz:
            total_code_size += len(cz)
    reserved_sum = 0
    for _name in _functions.keys():
        c = _functions[_name].get("concurrency")
        if c:
            reserved_sum += int(c)
    account_cap = _ACCOUNT_CONCURRENCY_CAP or 1000
    return json_response({
        "AccountLimit": {
            "TotalCodeSize": 80530636800,          # AWS default: 75 GiB
            "CodeSizeUnzipped": 262144000,         # 250 MiB
            "CodeSizeZipped": 52428800,            # 50 MiB
            "ConcurrentExecutions": account_cap,
            "UnreservedConcurrentExecutions": max(account_cap - reserved_sum, 0),
        },
        "AccountUsage": {
            "TotalCodeSize": total_code_size,
            "FunctionCount": total_fns,
        },
    })


def _es_encode_message(headers: dict[str, str], payload: bytes) -> bytes:
    """Encode a single AWS vnd.amazon.eventstream message.

    Format (all big-endian):
      prelude  (12 bytes): total_length | headers_length | prelude_crc32
      headers  (variable): repeated (name_len:1 | name | type:1 | value...)
      payload  (variable)
      trailer  (4 bytes):  full-message CRC32

    Header value type 7 = string:  value_len:2 | value_bytes
    Lambda response streams use this with `:message-type=event` plus
    `:event-type=PayloadChunk` or `InvokeComplete`.
    """
    import zlib
    # Encode headers
    hdr_bytes = bytearray()
    for name, value in headers.items():
        name_b = name.encode("utf-8")
        val_b = value.encode("utf-8")
        hdr_bytes.append(len(name_b))
        hdr_bytes.extend(name_b)
        hdr_bytes.append(7)  # type 7 = string
        hdr_bytes.extend(len(val_b).to_bytes(2, "big"))
        hdr_bytes.extend(val_b)

    headers_length = len(hdr_bytes)
    total_length = 12 + headers_length + len(payload) + 4  # prelude + headers + payload + crc

    # Prelude (first 8 bytes) + its CRC
    prelude = total_length.to_bytes(4, "big") + headers_length.to_bytes(4, "big")
    prelude_crc = zlib.crc32(prelude).to_bytes(4, "big")

    # Assemble message without the trailing CRC
    msg_head = prelude + prelude_crc + bytes(hdr_bytes) + payload
    # Full-message CRC covers everything from the start up to (but not including) this CRC
    message_crc = zlib.crc32(msg_head).to_bytes(4, "big")
    return msg_head + message_crc


def _build_response_stream(payload: bytes, is_error: bool, function_error: str | None) -> bytes:
    """Build the full vnd.amazon.eventstream body for InvokeWithResponseStream.

    Real AWS emits one or more PayloadChunk events followed by an InvokeComplete
    (success) or InvokeError (handler failure) event. We always emit:
      PayloadChunk(payload) + InvokeComplete({})
    (or InvokeError if the handler raised), because our execution model is
    atomic — we don't see chunks mid-flight.
    """
    stream = b""
    if payload:
        stream += _es_encode_message({
            ":message-type": "event",
            ":event-type": "PayloadChunk",
            ":content-type": "application/octet-stream",
        }, payload)
    if is_error:
        err_payload = json.dumps({
            "errorCode": 500,
            "errorDetails": function_error or "Unhandled",
        }).encode()
        stream += _es_encode_message({
            ":message-type": "event",
            ":event-type": "InvokeComplete",
            ":content-type": "application/json",
        }, err_payload)
    else:
        # InvokeComplete payload is an empty JSON object per AWS wire traces.
        stream += _es_encode_message({
            ":message-type": "event",
            ":event-type": "InvokeComplete",
            ":content-type": "application/json",
        }, b"{}")
    return stream


async def _invoke_with_response_stream(name: str, event: dict, headers: dict,
                                        path_qualifier: str | None = None,
                                        query_params: dict | None = None):
    """AWS `InvokeWithResponseStream` — streaming invocation.

    Real AWS frames the response using vnd.amazon.eventstream: a sequence of
    binary messages (`PayloadChunk`, then `InvokeComplete`), each with a
    prelude CRC and a message CRC. SDK clients (boto3's EventStream parser,
    AWS SDK for Java v2, etc.) validate both CRCs and will raise on a single
    flipped bit. We emit a single PayloadChunk containing the handler's
    response payload followed by an InvokeComplete — wire-valid framing,
    functionally equivalent to atomic-response handlers.

    Handlers that genuinely stream chunks mid-execution would need a
    streaming RIE, which AWS's public Runtime Interface Emulator does not
    provide — so we cannot do any better without a custom RIE fork.
    """
    status, resp_headers, resp_body = await _invoke(name, event, headers, path_qualifier, query_params)
    # Detect handler-level errors from the standard invoke path so we can flip
    # to the InvokeError event type in the stream.
    is_error = bool(resp_headers and resp_headers.get("X-Amz-Function-Error"))
    function_error = (resp_headers or {}).get("X-Amz-Function-Error")
    stream_bytes = _build_response_stream(resp_body or b"", is_error, function_error)
    out_headers = {
        "Content-Type": "application/vnd.amazon.eventstream",
        "X-Amzn-Lambda-Response-Streamed": "true",
    }
    if is_error and function_error:
        out_headers["X-Amz-Function-Error"] = function_error
    return status, out_headers, stream_bytes


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


def _delete_function(name: str, query_params: dict, path_qualifier: str | None = None):
    qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )
    if qualifier and qualifier != "$LATEST":
        if qualifier not in _functions[name].get("versions", {}):
            return _function_qualifier_not_found(name, qualifier)
        alias_names = [
            alias_name
            for alias_name, alias in _functions[name].get("aliases", {}).items()
            if (
                alias.get("FunctionVersion") == qualifier
                or qualifier in (alias.get("RoutingConfig", {}).get("AdditionalVersionWeights") or {})
            )
        ]
        if alias_names:
            return error_response_json(
                "ResourceConflictException",
                f"Function version {qualifier} cannot be deleted because it is referenced by an alias.",
                409,
            )
        _functions[name]["versions"].pop(qualifier, None)
    else:
        del _functions[name]
        invalidate_worker(name, account=get_account_id(), region=get_region())
        # Docker pool too — otherwise the function's pooled containers leak
        # until _WARM_CONTAINER_TTL eviction.
        _pool_kill_function(get_account_id(), name)
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
        code_zip = _fetch_code_from_s3(
            data["S3Bucket"],
            data["S3Key"],
            version_id=data.get("S3ObjectVersion"),
        )
        if code_zip is None:
            return error_response_json(
                "InvalidParameterValueException",
                f"Failed to fetch code from s3://{data['S3Bucket']}/{data['S3Key']}",
                400,
            )
    err = _validate_unzipped_size(code_zip)
    if err is not None:
        return err
    if code_zip:
        func["code_zip"] = code_zip
        func["config"]["CodeSize"] = len(code_zip)
        func["config"]["CodeSha256"] = base64.b64encode(
            hashlib.sha256(code_zip).digest(),
        ).decode()
    func["config"]["LastModified"] = _now_iso()
    # AWS-match: UpdateFunctionCode marks status InProgress while the runtime
    # re-initialises, then flips to Successful. Terraform's FunctionUpdated
    # waiter polls for LastUpdateStatus=Successful.
    func["config"]["LastUpdateStatus"] = "InProgress"
    func["config"]["LastUpdateStatusReason"] = ""
    func["config"]["LastUpdateStatusReasonCode"] = ""
    func["config"]["State"] = "Pending"
    func["config"]["StateReason"] = "The function is being updated."
    func["config"]["StateReasonCode"] = "Updating"
    func["config"]["RevisionId"] = new_uuid()

    # Invalidate only the old $LATEST worker — published version workers stay alive
    invalidate_worker(name, qualifier="$LATEST", account=get_account_id(), region=get_region())
    # Docker pool: the new CodeSha256 changes the pool key so new invokes
    # spawn fresh containers anyway, but the old containers under the old key
    # would linger until _WARM_CONTAINER_TTL. Reap them now.
    _pool_kill_function(get_account_id(), name)
    _schedule_state_transition(name, _LAMBDA_STATE_TRANSITION_DELAY)

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
    err = _validate_dead_letter_config(data.get("DeadLetterConfig"))
    if err:
        return err
    if "Layers" in data:
        layers_cfg, err = _normalize_layer_attachments(data.get("Layers"))
        if err:
            return err
        data = dict(data)
        data["Layers"] = layers_cfg
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
        "DurableConfig",
        "TenancyConfig",
        "CapacityProviderConfig",
    ):
        if key in data:
            if key == "Layers":
                layers_cfg = []
                for layer in data["Layers"]:
                    if isinstance(layer, str):
                        layers_cfg.append({"Arn": layer, "CodeSize": _layer_codesize_for_arn(layer)})
                    elif isinstance(layer, dict):
                        layer = dict(layer)
                        if "CodeSize" not in layer and "Arn" in layer:
                            layer["CodeSize"] = _layer_codesize_for_arn(layer["Arn"])
                        layers_cfg.append(layer)
                config["Layers"] = layers_cfg
            else:
                config[key] = data[key]
    if "ImageConfig" in data:
        config["ImageConfigResponse"] = {"ImageConfig": data["ImageConfig"]}
    config["LastModified"] = _now_iso()
    config["LastUpdateStatus"] = "InProgress"
    config["LastUpdateStatusReason"] = ""
    config["LastUpdateStatusReasonCode"] = ""
    config["State"] = "Pending"
    config["StateReason"] = "The function is being updated."
    config["StateReasonCode"] = "Updating"
    config["RevisionId"] = new_uuid()
    # AWS-match: UpdateFunctionConfiguration recycles the init container when
    # spawn-time inputs change (Runtime/Handler/Layers/Env/MemorySize/Arch/
    # VpcConfig/FileSystemConfigs). The ministack warm-pool key is just
    # account:func:qualifier, so a stale worker would keep serving with the
    # pre-update layers/env. Invalidate to force a fresh worker on next invoke,
    # mirroring what _update_code already does. Otherwise PublishLayerVersion +
    # UpdateFunctionConfiguration(Layers=[...]) leaves the previously-warm
    # worker without the new layer extracted on disk (issue #816).
    _WORKER_AFFECTING = {
        "Runtime", "Handler", "Layers", "Environment", "MemorySize",
        "Architectures", "VpcConfig", "FileSystemConfigs",
    }
    if any(k in data for k in _WORKER_AFFECTING):
        invalidate_worker(name, qualifier="$LATEST")
        # Also invalidate the docker warm-container pool: its key is
        # account:func:zip:CodeSha256, so config-only changes (Layers,
        # Environment, MemorySize, etc.) wouldn't otherwise displace a stale
        # pooled container. Without this, LAMBDA_EXECUTOR=docker users hit the
        # same "old container, no new layer mounted" failure mode that the
        # in-process warm worker recycle solved for Python/Node.
        _pool_kill_function(get_account_id(), name)
    _schedule_state_transition(name, _LAMBDA_STATE_TRANSITION_DELAY)
    return json_response(config)


# ---------------------------------------------------------------------------
# Invoke
# ---------------------------------------------------------------------------


async def _invoke(name: str, event: dict, headers: dict, path_qualifier: str | None = None,
                  query_params: dict | None = None):
    if name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(name)}",
            404,
        )

    func = _functions[name]
    invocation_type = headers.get("x-amz-invocation-type") or headers.get("X-Amz-Invocation-Type") or "RequestResponse"
    qualifier = path_qualifier or _qp_first(query_params or {}, "Qualifier") or _qp_first(headers, "x-amz-qualifier") or None
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

    # If the function has DurableConfig.Enabled, spin up a durable execution
    # record so the SDK calls (Checkpoint / GetState) inside the function
    # have a target. The ARN is surfaced back via the X-Amz-Durable-Execution-
    # Arn response header so callers can wire it to follow-up management ops.
    durable_arn = None
    if (func.get("config", {}) or {}).get("DurableConfig", {}).get("Enabled"):
        from ministack.services import lambda_durable
        try:
            event_payload = json.dumps(event) if not isinstance(event, str) else event
        except (TypeError, ValueError):
            event_payload = ""
        rec = lambda_durable.create_execution_for_invoke(
            function_arn=_func_arn(name),
            version=executed_version,
            input_payload=event_payload,
        )
        durable_arn = rec["DurableExecutionArn"]
        _durable_ctx.set({
            "arn": durable_arn,
            "token": rec["CheckpointToken"],
            "name": rec["DurableExecutionName"],
        })
        # AWS sends a durable Lambda an event containing ONLY the durable
        # context fields the SDK reads via `from_json_dict`: DurableExecutionArn,
        # CheckpointToken, InitialExecutionState. The user's actual payload
        # lives inside the InitialExecutionState as a synthetic EXECUTION-type
        # operation (see lambda_durable.create_execution_for_invoke); the SDK
        # reads it via execution_state.get_input_payload().
        # We pass back the operations list (with the seeded EXECUTION op) so
        # the SDK has the input on every invocation, including replays.
        # Operations are serialized via lambda_durable._serialize_operations.
        from ministack.services import lambda_durable as _ld
        event = {
            "DurableExecutionArn": durable_arn,
            "CheckpointToken": rec["CheckpointToken"],
            "InitialExecutionState": {
                "Operations": _ld._serialize_operations(rec["Operations"], for_event=True),
                "NextMarker": "",
            },
        }

    if invocation_type == "Event":
        # AWS async invocation: retry + DLQ routing handled by the shared
        # helper so event-source fan-out (S3, EventBridge, SNS → Lambda, etc.)
        # gets identical semantics.
        invoke_async_with_retry(exec_record, event)
        _emit_lambda_metrics(name, duration_ms=0.0, error=False, throttle=False)
        return 202, {"X-Amz-Executed-Version": executed_version}, b""

    # RequestResponse — execute in worker thread so nested SDK calls
    # from the Lambda process can still reach this ASGI server.
    _t_start = time.time()
    result = await asyncio.to_thread(_execute_function_with_config_scope, exec_record, event)
    _duration_ms = (time.time() - _t_start) * 1000.0
    _emit_lambda_metrics(
        name,
        duration_ms=_duration_ms,
        error=bool(result.get("error")),
        throttle=bool(result.get("throttle")),
    )

    resp_headers: dict = {
        "Content-Type": "application/json",
        "X-Amz-Executed-Version": executed_version,
    }
    if durable_arn:
        resp_headers["X-Amz-Durable-Execution-Arn"] = durable_arn
        # Real AWS hands the initial CheckpointToken to the runtime via Lambda
        # context; ministack surfaces it on the response header so test clients
        # and SDK-less callers can drive the management ops directly.
        _de_rec = lambda_durable._executions.get(durable_arn)
        if _de_rec:
            resp_headers["X-Amz-Durable-Checkpoint-Token"] = _de_rec["CheckpointToken"]
        # Inspect the SDK's return value: PENDING → schedule the next wakeup
        # from the latest WAIT timestamp; SUCCEEDED/FAILED → mark terminal.
        try:
            payload_obj = result.get("body")
            if isinstance(payload_obj, (bytes, bytearray)):
                payload_obj = payload_obj.decode("utf-8", errors="replace")
            if isinstance(payload_obj, str):
                payload_obj = json.loads(payload_obj)
            if isinstance(payload_obj, dict):
                status = payload_obj.get("Status")
                if status == "PENDING":
                    lambda_durable.schedule_resume(durable_arn)
                elif status == "SUCCEEDED":
                    lambda_durable.mark_execution_completed(
                        durable_arn,
                        result_payload=payload_obj.get("Result"),
                        error=None,
                    )
                elif status == "FAILED":
                    lambda_durable.mark_execution_completed(
                        durable_arn,
                        result_payload=None,
                        error=payload_obj.get("Error"),
                    )
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    log_output = result.get("log", "")
    if log_output:
        logger.info("Lambda %s output:\n%s", name, log_output)
        resp_headers["X-Amz-Log-Result"] = base64.b64encode(
            log_output.encode("utf-8"),
        ).decode()

    # Throttling takes a separate status path: HTTP 429 with the error body
    # shaped as a service-level exception, NOT the 200+X-Amz-Function-Error
    # used for in-function failures. AWS also sets a Retry-After HTTP header.
    if result.get("throttle"):
        body = result.get("body") or {}
        throttle_headers = {"Content-Type": "application/json"}
        retry_after = body.get("retryAfterSeconds") if isinstance(body, dict) else None
        if retry_after:
            throttle_headers["Retry-After"] = str(retry_after)
        return 429, throttle_headers, json.dumps(body).encode()

    if result.get("error"):
        # AWS distinguishes Handled (user returned error-shaped payload) from
        # Unhandled (raised uncaught exception). Default to Unhandled when the
        # executor didn't classify.
        resp_headers["X-Amz-Function-Error"] = result.get("function_error") or "Unhandled"

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
    "python3.8": "public.ecr.aws/lambda/python:3.8",
    "python3.9": "public.ecr.aws/lambda/python:3.9",
    "python3.10": "public.ecr.aws/lambda/python:3.10",
    "python3.11": "public.ecr.aws/lambda/python:3.11",
    "python3.12": "public.ecr.aws/lambda/python:3.12",
    "python3.13": "public.ecr.aws/lambda/python:3.13",
    "python3.14": "public.ecr.aws/lambda/python:3.14",
    "nodejs14.x": "public.ecr.aws/lambda/nodejs:14",
    "nodejs16.x": "public.ecr.aws/lambda/nodejs:16",
    "nodejs18.x": "public.ecr.aws/lambda/nodejs:18",
    "nodejs20.x": "public.ecr.aws/lambda/nodejs:20",
    "nodejs22.x": "public.ecr.aws/lambda/nodejs:22",
    "nodejs24.x": "public.ecr.aws/lambda/nodejs:24",
    "java25": "public.ecr.aws/lambda/java:25",
    "java21": "public.ecr.aws/lambda/java:21",
    "java17": "public.ecr.aws/lambda/java:17",
    "java11": "public.ecr.aws/lambda/java:11",
    "java8.al2": "public.ecr.aws/lambda/java:8.al2",
    "dotnet10": "public.ecr.aws/lambda/dotnet:10",
    "dotnet8": "public.ecr.aws/lambda/dotnet:8",
    "dotnet6": "public.ecr.aws/lambda/dotnet:6",
    "ruby4.0": "public.ecr.aws/lambda/ruby:4.0",
    "ruby3.4": "public.ecr.aws/lambda/ruby:3.4",
    "ruby3.3": "public.ecr.aws/lambda/ruby:3.3",
    "ruby3.2": "public.ecr.aws/lambda/ruby:3.2",
    "provided.al2023": "public.ecr.aws/lambda/provided:al2023",
    "provided.al2": "public.ecr.aws/lambda/provided:al2",
    "provided": "public.ecr.aws/lambda/provided:latest",
}


def _docker_image_for_runtime(runtime: str) -> str | None:
    if runtime in _RUNTIME_IMAGE_MAP:
        return apply_image_prefix(_RUNTIME_IMAGE_MAP[runtime])
    if runtime.startswith("python"):
        ver = runtime.replace("python", "")
        return apply_image_prefix(f"public.ecr.aws/lambda/python:{ver}")
    if runtime.startswith("nodejs"):
        ver = runtime.replace("nodejs", "").rstrip(".x")
        return apply_image_prefix(f"public.ecr.aws/lambda/nodejs:{ver}")
    if runtime.startswith("java"):
        ver = runtime.replace("java", "")
        return apply_image_prefix(f"public.ecr.aws/lambda/java:{ver}")
    if runtime.startswith("dotnet"):
        ver = runtime.replace("dotnet", "")
        return apply_image_prefix(f"public.ecr.aws/lambda/dotnet:{ver}")
    if runtime.startswith("ruby"):
        ver = runtime.replace("ruby", "")
        return apply_image_prefix(f"public.ecr.aws/lambda/ruby:{ver}")
    if runtime.startswith("provided"):
        return apply_image_prefix("public.ecr.aws/lambda/provided:al2023")
    return None


# ---------------------------------------------------------------------------
# Function execution – Docker mode (RIE with warm container pool)
# ---------------------------------------------------------------------------
#
# AWS Lambda model: each function version has a pool of execution environments.
# Concurrent invocations take separate environments from the pool, up to
# ReservedConcurrentExecutions (or the account-level cap — we default to 10).
# Idle environments stay warm ~5-15 minutes before eviction.
# Both Zip and Image package types follow the same lifecycle; only the image
# source and CMD differ.
#
# _warm_pool structure:
#   {cache_key: [ {container, tmpdir, in_use, last_used, created}, ... ]}
#
# Cache key format keeps multi-tenancy isolation + forces cold start on
# redeploy:
#   "{account}:{fn_name}:zip:{CodeSha256}"
#   "{account}:{fn_name}:image:{ImageUri}"
# ---------------------------------------------------------------------------

_warm_pool: dict[str, list[dict]] = {}
_warm_pool_lock = threading.Lock()
_WARM_CONTAINER_TTL = 300  # seconds idle before eviction. AWS doesn't publish
                           # the exact idle TTL; 5 min matches community
                           # observations. Can be overridden via env var.
_WARM_CONTAINER_TTL = int(os.environ.get("LAMBDA_WARM_TTL_SECONDS", _WARM_CONTAINER_TTL))

# Per-function concurrency: only applied when ReservedConcurrentExecutions is
# explicitly set on the function. Otherwise the function can consume the
# full account pool, matching AWS.
# Account-level concurrency cap: AWS default is 1000. We default to unbounded
# locally (a laptop can't actually run 1000 Lambda containers); users who
# want AWS-exact throttling behaviour can set LAMBDA_ACCOUNT_CONCURRENCY.
_ACCOUNT_CONCURRENCY_CAP = int(os.environ.get("LAMBDA_ACCOUNT_CONCURRENCY", "0"))  # 0 = unbounded
_reaper_started = False
_reaper_lock = threading.Lock()


def _warm_pool_key(func_name: str, config: dict) -> str:
    acct, region = _account_region_from_function_config(config)
    if config.get("PackageType") == "Image":
        return f"{acct}:{region}:{func_name}:image:{config.get('ImageUri', '')}"
    return f"{acct}:{region}:{func_name}:zip:{config.get('CodeSha256', 'nosha')}"


def _is_container_running(container) -> bool:
    try:
        container.reload()
        return container.status == "running"
    except Exception:
        return False


def _kill_pool_entry(entry: dict) -> None:
    """Stop + remove the container, clean its tmpdir."""
    container = entry.get("container")
    if container is not None:
        try:
            container.stop(timeout=2)
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass
    tmpdir = entry.get("tmpdir")
    if tmpdir and os.path.exists(tmpdir):
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


def _pool_acquire(key: str, max_concurrency: int | None):
    """Try to reserve a container from the pool.

    `max_concurrency` semantics:
      - int > 0   : per-function cap (ReservedConcurrentExecutions). At cap → (None, False).
      - None / 0  : no per-function cap. Always spawn a fresh container if no free one.

    Account-level cap (if `_ACCOUNT_CONCURRENCY_CAP > 0`) is enforced globally across all keys.

    Returns (entry, reason):
      - (entry,  "reused")     : free live container reused; marked in_use.
      - (None,   "spawn")      : caller should spawn a new container and _pool_register it.
      - (None,   "func_cap")   : function-level ReservedConcurrentExecutions hit → throttle.
      - (None,   "acct_cap")   : account-level cap hit → throttle.
    """
    with _warm_pool_lock:
        entries = _warm_pool.setdefault(key, [])
        # Prune dead containers inline. container.reload() is a cheap Docker API call.
        alive = [e for e in entries if _is_container_running(e["container"])]
        if len(alive) != len(entries):
            _warm_pool[key] = alive
            entries = alive
        # Reuse a free live container
        for e in entries:
            if not e["in_use"]:
                e["in_use"] = True
                e["last_used"] = time.time()
                return e, "reused"
        # Function-level cap
        if max_concurrency and len(entries) >= max_concurrency:
            return None, "func_cap"
        # Account-level cap (count in-use entries across all pools)
        if _ACCOUNT_CONCURRENCY_CAP > 0:
            total_in_use = sum(1 for lst in _warm_pool.values() for e in lst if e["in_use"])
            if total_in_use >= _ACCOUNT_CONCURRENCY_CAP:
                return None, "acct_cap"
        return None, "spawn"


def _pool_register(key: str, container, tmpdir) -> dict:
    entry = {
        "container": container,
        "tmpdir": tmpdir,
        "in_use": True,
        "last_used": time.time(),
        "created": time.time(),
    }
    with _warm_pool_lock:
        _warm_pool.setdefault(key, []).append(entry)
    return entry


def _pool_release(entry: dict) -> None:
    with _warm_pool_lock:
        entry["in_use"] = False
        entry["last_used"] = time.time()


def _pool_remove(entry: dict) -> None:
    """Force-remove an entry (container died or invocation exploded)."""
    with _warm_pool_lock:
        for entries in _warm_pool.values():
            if entry in entries:
                entries.remove(entry)
                break
    _kill_pool_entry(entry)


def _pool_evict_idle() -> None:
    """Reap idle+not-in-use containers past TTL."""
    cutoff = time.time() - _WARM_CONTAINER_TTL
    to_kill = []
    with _warm_pool_lock:
        for key, entries in list(_warm_pool.items()):
            keep = []
            for e in entries:
                if not e["in_use"] and e["last_used"] < cutoff:
                    to_kill.append(e)
                else:
                    keep.append(e)
            if keep:
                _warm_pool[key] = keep
            else:
                _warm_pool.pop(key, None)
    for e in to_kill:
        _kill_pool_entry(e)


def _pool_clear_all() -> None:
    """reset()/shutdown — kill every pooled container across all accounts."""
    with _warm_pool_lock:
        all_entries = [e for lst in _warm_pool.values() for e in lst]
        _warm_pool.clear()
    for e in all_entries:
        _kill_pool_entry(e)


def _pool_kill_function(account: str, func_name: str) -> None:
    """Kill every pooled docker container for a function across all qualifiers.

    The pool key is ``{account}:{func_name}:zip:{CodeSha256}`` (or
    ``:image:{ImageUri}``). UpdateFunctionConfiguration changes attributes that
    don't show up in the key (Layers / Environment / MemorySize / VpcConfig /
    Architectures / FileSystemConfigs / Runtime / Handler), so the same key
    would otherwise hand back a stale container that was spawned before the
    config change. Issue #816 docker-executor follow-up: a layer attached
    after the first invoke was never mounted on the reused warm container,
    so handler imports from the layer kept failing even after the layer's
    extracted dir was correct.
    """
    prefix = f"{account}:{func_name}:"
    to_kill = []
    with _warm_pool_lock:
        for key in list(_warm_pool.keys()):
            if key.startswith(prefix):
                to_kill.extend(_warm_pool.pop(key))
    for e in to_kill:
        _kill_pool_entry(e)


def _ensure_reaper_thread() -> None:
    global _reaper_started
    with _reaper_lock:
        if _reaper_started:
            return
        def _loop():
            while True:
                time.sleep(30)
                try:
                    _pool_evict_idle()
                except Exception as exc:
                    logger.debug("Lambda pool reaper iteration error: %s", exc)
        threading.Thread(target=_loop, daemon=True, name="ministack-lambda-reaper").start()
        _reaper_started = True


# AWS-match: CreateFunction / UpdateFunctionCode / UpdateFunctionConfiguration
# set State=Pending and LastUpdateStatus=InProgress, then transition to
# Active / Successful asynchronously when the runtime is ready. Real AWS takes
# seconds to tens of seconds (image pull time for Image type); we use a short
# delay so local integration tests see the transition without spinning.
_LAMBDA_STATE_TRANSITION_DELAY = float(os.environ.get("LAMBDA_STATE_TRANSITION_SECONDS", "0.5"))


def _schedule_state_transition(func_name: str, delay: float) -> None:
    """Flip State and LastUpdateStatus to the post-ready values after `delay`."""
    def _flip():
        time.sleep(delay)
        # Re-fetch under the request's account and region context so
        # multi-tenant, multi-region cases update the function that was just
        # created or modified.
        fn = _functions.get(func_name)
        if not fn:
            return
        configs = [fn.get("config", {})]
        configs.extend(ver.get("config", {}) for ver in fn.get("versions", {}).values())
        for cfg in configs:
            if not cfg:
                continue
            if (
                cfg.get("State") not in (None, "", "Pending")
                and cfg.get("LastUpdateStatus") not in (None, "", "InProgress")
            ):
                continue
            cfg["State"] = "Active"
            cfg["StateReason"] = ""
            cfg["StateReasonCode"] = ""
            cfg["LastUpdateStatus"] = "Successful"
            cfg["LastUpdateStatusReason"] = ""
            cfg["LastUpdateStatusReasonCode"] = ""

    ctx_snapshot = contextvars.copy_context()
    threading.Thread(target=ctx_snapshot.run, args=(_flip,), daemon=True).start()


# AWS-match: real Lambda async retry spacing is exponential backoff
# starting at 1 minute between attempts, capped at ~5 minutes. For local
# iteration we scale way down; the shape is right, the wall-clock is not.
_LAMBDA_ASYNC_RETRY_BASE_SECONDS = float(os.environ.get("LAMBDA_ASYNC_RETRY_BASE_SECONDS", "1"))
_LAMBDA_ASYNC_RETRY_MAX_SECONDS = float(os.environ.get("LAMBDA_ASYNC_RETRY_MAX_SECONDS", "30"))


def invoke_async_with_retry(func: dict, event: dict) -> None:
    """Fire-and-forget async Lambda invocation matching AWS's Event semantics:
    retries on failure up to `MaximumRetryAttempts` (default 2), with
    exponential backoff between attempts, then routes the final failure to the
    DLQ or `DestinationConfig.OnFailure` target.

    Entry point for internal event-source fan-out (S3 notifications,
    EventBridge rule targets, SNS → Lambda, etc.) — anything that matches
    real AWS's async-invocation path. Runs the retry loop in a background
    thread so callers stay non-blocking.
    """
    def _run():
        config = func.get("config") or func
        account_id, region = _account_region_from_function_config(config)
        _request_account_id.set(account_id)
        _request_region.set(region)
        fn_name = config.get("FunctionName", "unknown")
        eic = func.get("event_invoke_config") or {}
        max_retries = eic.get("MaximumRetryAttempts")
        if max_retries is None:
            max_retries = 2
        # `MaximumEventAgeInSeconds` bounds the total time a failed event can
        # hang around across retries. AWS default = 21600 (6h). We honour it
        # as a ceiling on total retry wall-clock.
        max_event_age = int(eic.get("MaximumEventAgeInSeconds", 21600))
        on_failure_arn = (
            (eic.get("DestinationConfig") or {}).get("OnFailure", {}).get("Destination")
            or (config.get("DeadLetterConfig") or {}).get("TargetArn")
            or ""
        )
        started = time.time()
        last_result = None
        for attempt in range(int(max_retries) + 1):
            if attempt > 0:
                # Exponential backoff: base, base*2, base*4, …, capped.
                delay = min(_LAMBDA_ASYNC_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                            _LAMBDA_ASYNC_RETRY_MAX_SECONDS)
                # Age-gate: if retrying would push us past MaximumEventAgeInSeconds,
                # give up now and route to DLQ (matches AWS's event-age expiry).
                if (time.time() - started) + delay > max_event_age:
                    break
                time.sleep(delay)
            last_result = _execute_function(func, event)
            log_output = last_result.get("log", "")
            if log_output:
                logger.info("Lambda %s async output (attempt %d):\n%s", fn_name, attempt + 1, log_output)
            if not last_result.get("error"):
                return
        if on_failure_arn and last_result is not None:
            _route_async_failure(on_failure_arn, fn_name, event, last_result)

    ctx_snapshot = contextvars.copy_context()
    threading.Thread(target=ctx_snapshot.run, args=(_run,), daemon=True).start()


def _match_esm_filter(record: dict, pattern: dict) -> bool:
    """Recursive match of a pattern dict against a record, mirroring AWS's
    EventBridge-style content-filter semantics used by Lambda ESM FilterCriteria.
    Pattern leaves are always lists of allowed values. Nested dicts recurse."""
    if not isinstance(pattern, dict):
        return False
    for key, pat in pattern.items():
        rec_val = record.get(key)
        if isinstance(pat, dict):
            if not isinstance(rec_val, dict) or not _match_esm_filter(rec_val, pat):
                return False
        elif isinstance(pat, list):
            # Each entry can be a scalar equality check or a content-filter dict
            # (e.g. {"exists": True}, {"prefix": "foo"}, {"anything-but": [...]}).
            matched_any = False
            for p in pat:
                if isinstance(p, dict):
                    if "exists" in p:
                        if p["exists"] and key in record:
                            matched_any = True; break
                        if not p["exists"] and key not in record:
                            matched_any = True; break
                    elif "prefix" in p:
                        if isinstance(rec_val, str) and rec_val.startswith(p["prefix"]):
                            matched_any = True; break
                    elif "suffix" in p:
                        if isinstance(rec_val, str) and rec_val.endswith(p["suffix"]):
                            matched_any = True; break
                    elif "anything-but" in p:
                        banned = p["anything-but"]
                        if not isinstance(banned, list):
                            banned = [banned]
                        if rec_val not in banned:
                            matched_any = True; break
                    elif "numeric" in p:
                        ops = p["numeric"]
                        try:
                            v = float(rec_val)
                            ok = True
                            for i in range(0, len(ops), 2):
                                op, cmp_val = ops[i], float(ops[i + 1])
                                if op == "=" and v != cmp_val: ok = False
                                elif op == ">" and not v > cmp_val: ok = False
                                elif op == ">=" and not v >= cmp_val: ok = False
                                elif op == "<" and not v < cmp_val: ok = False
                                elif op == "<=" and not v <= cmp_val: ok = False
                            if ok:
                                matched_any = True; break
                        except (TypeError, ValueError):
                            pass
                elif p == rec_val:
                    matched_any = True; break
            if not matched_any:
                return False
        else:
            if rec_val != pat:
                return False
    return True


def _apply_filter_criteria(records: list[dict], esm: dict) -> list[dict]:
    """Apply an ESM's FilterCriteria.Filters (OR across patterns) to a batch
    of records, returning only those matching at least one filter. If no
    FilterCriteria configured, pass through unchanged — matches AWS behaviour.
    Filter patterns are JSON strings matched against each record's body JSON."""
    fc = esm.get("FilterCriteria") or {}
    filters = fc.get("Filters") or []
    if not filters:
        return records
    compiled: list[dict] = []
    for f in filters:
        pat = f.get("Pattern") if isinstance(f, dict) else None
        if not pat:
            continue
        try:
            compiled.append(json.loads(pat))
        except (json.JSONDecodeError, TypeError):
            continue
    if not compiled:
        return records
    kept = []
    for rec in records:
        # For SQS records the body is a string; AWS parses JSON bodies
        # automatically when the filter pattern targets `body.*` fields.
        rec_for_match = dict(rec)
        body = rec.get("body")
        if isinstance(body, str):
            try:
                rec_for_match["body"] = json.loads(body)
            except json.JSONDecodeError:
                pass
        if any(_match_esm_filter(rec_for_match, pat) for pat in compiled):
            kept.append(rec)
    return kept


def _route_async_failure(target_arn: str, func_name: str, event: dict, result: dict) -> None:
    """Send the original event + error metadata to DLQ (SQS/SNS) or OnFailure
    destination after all async retries are exhausted — matching AWS behaviour.
    """
    err_body = result.get("body") if isinstance(result.get("body"), dict) else {}
    envelope = {
        "requestContext": {
            "functionArn": _functions.get(func_name, {}).get("config", {}).get("FunctionArn", ""),
            "condition": "RetriesExhausted",
            "approximateInvokeCount": 3,
        },
        "requestPayload": event,
        "responseContext": {
            "statusCode": 200,
            "functionError": result.get("function_error") or "Unhandled",
        },
        "responsePayload": err_body,
    }
    try:
        body = json.dumps(envelope)
        try:
            spec = parse_arn(target_arn)
        except ArnParseError:
            logger.warning("Lambda %s DLQ target is not a valid ARN: %s", func_name, target_arn)
            return
        if spec.account_id != get_account_id() or spec.region != get_region():
            logger.warning("Lambda %s DLQ target not in function scope: %s", func_name, target_arn)
            return

        if spec.service == "sqs":
            import ministack.services.sqs as _sqs
            qname = _sqs_queue_name_from_arn_spec(spec)
            if not qname:
                logger.warning("Lambda %s DLQ target is not a valid SQS ARN: %s", func_name, target_arn)
                return
            target_q = None
            for url, q in _sqs._queues.items():
                if q.get("attributes", {}).get("QueueArn") == target_arn:
                    target_q = q
                    break
            if target_q is not None:
                now = time.time()
                target_q["messages"].append({
                    "id": new_uuid(),
                    "body": body,
                    "md5_body": hashlib.md5(body.encode()).hexdigest(),
                    "md5_attrs": "",
                    "receipt_handle": None,
                    "sent_at": now,
                    "visible_at": now,
                    "receive_count": 0,
                    "first_receive_at": None,
                    "message_attributes": {},
                    "sys": {
                        "SenderId": get_account_id(),
                        "SentTimestamp": str(int(now * 1000)),
                    },
                    "group_id": None, "dedup_id": None,
                    "dedup_cache_key": None, "seq": None,
                })
                return
        elif spec.service == "sns":
            import ministack.services.sns as _sns
            if _sns_topic_name_from_arn_spec(spec) and target_arn in _sns._topics:
                _sns._fanout(target_arn, new_uuid(), body, "Lambda async failure", "", {})
                return
        elif spec.service == "lambda":
            if not _lambda_function_name_from_arn_spec(spec):
                logger.warning("Lambda %s destination target is not a valid Lambda ARN: %s", func_name, target_arn)
                return
            dest_func, _dest_config, dest_name = _get_func_record_for_ref(target_arn)
            if dest_func:
                threading.Thread(
                    target=_execute_function_with_config_scope,
                    args=(dest_func, envelope),
                    daemon=True,
                ).start()
                return
        logger.warning("Lambda %s DLQ target not found: %s", func_name, target_arn)
    except Exception as exc:
        logger.error("Lambda %s DLQ/OnFailure dispatch failed: %s", func_name, exc)


def _throttle_response(reason_code: str, msg: str, retry_after: int = 1) -> dict:
    """Shape a throttle result for the Invoke handler to translate into HTTP 429.

    AWS returns TooManyRequestsException with a `Reason` field distinguishing
    function-level from account-level limits, and a `retryAfterSeconds` hint.
    """
    return {
        "throttle": True,
        "body": {
            "__type": "TooManyRequestsException",
            "message": msg,
            "Reason": reason_code,
            "retryAfterSeconds": retry_after,
        },
        "error": True,
        "log": "",
    }


def _docker_cp_dir(container, src_dir: str, dest_dir: str, arcname: str = "."):
    """Copy a local directory into a Docker container using a tar archive.

    Docker's ``put_archive`` requires ``dest_dir`` to already exist in the
    container. For paths the base image owns (``/var/task``, ``/var/runtime``,
    ``/opt``) this is fine. For paths we want to create (``/opt/layer_N``),
    extract into the existing parent and pass the new subdir via ``arcname``
    so the tar carries ``./layer_N/...`` entries — the put_archive call
    materialises the subdir as part of extraction.
    """
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(src_dir, arcname=arcname)
    buf.seek(0)
    container.put_archive(dest_dir, buf)


def _invoke_rie(container, event: dict, timeout: int) -> dict:
    """POST event to a running RIE container's HTTP endpoint."""
    import urllib.request
    max_attempts = int(timeout * 10) + 20
    for _attempt in range(max_attempts):
        container.reload()
        if container.status != "running":
            break
        try:
            networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
            # Try Docker network first (container-to-container)
            container_ip = None
            if LAMBDA_DOCKER_NETWORK:
                container_ip = networks.get(LAMBDA_DOCKER_NETWORK, {}).get("IPAddress", "")
            if not container_ip and _running_in_container():
                # DinD: host-mapped ports aren't reachable from inside this container.
                # Use the Lambda container's IP on any available Docker network.
                for net_info in networks.values():
                    ip = net_info.get("IPAddress", "")
                    if ip:
                        container_ip = ip
                        break
            if container_ip:
                rie_url = f"http://{container_ip}:8080/2015-03-31/functions/function/invocations"
            else:
                ports = container.ports.get("8080/tcp") or []
                if not ports:
                    continue
                rie_url = f"http://127.0.0.1:{ports[0]['HostPort']}/2015-03-31/functions/function/invocations"
            invoke_time = time.time()
            req = urllib.request.Request(
                rie_url, data=json.dumps(event).encode(),
                headers={"Content-Type": "application/json"},
            )
            resp = urllib.request.urlopen(req, timeout=timeout)
            body = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = body
            logs = container.logs(stdout=True, stderr=True, since=invoke_time).decode("utf-8", errors="replace").strip()
            # RIE sets 'Lambda-Runtime-Function-Error-Type' (or bare
            # 'X-Amz-Function-Error') when the handler raised an unhandled
            # exception. If it's set we surface the error flag + propagate the
            # exact AWS-style marker so _invoke can emit the right header.
            err_header = (resp.headers.get("X-Amz-Function-Error")
                          or resp.headers.get("Lambda-Runtime-Function-Error-Type") or "")
            result = {"body": parsed, "log": logs}
            if err_header or (isinstance(parsed, dict) and parsed.get("errorType")):
                # errorType without an X-Amz header means the handler returned
                # an error-shaped payload itself — AWS signals this as Handled.
                result["error"] = True
                result["function_error"] = "Unhandled" if err_header else "Handled"
            return result
        except (urllib.error.URLError, ConnectionRefusedError, OSError):
            time.sleep(0.1)
            continue
    # Timed out
    stdout = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace").strip()
    return {
        "body": {"errorMessage": f"Lambda RIE failed: {stdout[:500]}", "errorType": "Runtime.ExitError"},
        "error": True, "log": stdout,
    }


def _parse_docker_flags(flags: str) -> dict:
    """Parse a LAMBDA_DOCKER_FLAGS string into docker-py ``containers.run()`` kwargs.

    Supports the same flags as ``docker run`` by mapping CLI flags to their
    docker-py keyword equivalents.  Uses argparse so both ``--flag value`` and
    ``--flag=value`` work.  Unknown flags are silently ignored.
    """
    import argparse
    import shlex

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-e", "--env", action="append", default=[])
    parser.add_argument("-v", "--volume", action="append", default=[])
    parser.add_argument("--dns", action="append", default=[])
    parser.add_argument("--network")
    parser.add_argument("--cap-add", action="append", default=[])
    parser.add_argument("-m", "--memory")
    parser.add_argument("--shm-size")
    parser.add_argument("--tmpfs", action="append", default=[])
    parser.add_argument("--add-host", action="append", default=[])
    parser.add_argument("--privileged", action="store_true")
    parser.add_argument("--read-only", action="store_true")
    args, _ = parser.parse_known_args(shlex.split(flags))

    kwargs: dict = {}

    if args.env:
        env = {}
        for entry in args.env:
            k, _, v = entry.partition("=")
            env[k] = v
        kwargs["environment"] = env

    if args.volume:
        mounts = []
        for vol in args.volume:
            parts = vol.split(":")
            host = parts[0]
            container = parts[1] if len(parts) > 1 else parts[0]
            ro = len(parts) > 2 and parts[2] == "ro"
            mounts.append(docker_lib.types.Mount(container, host, type="bind", read_only=ro))
        kwargs["mounts"] = mounts

    if args.dns:
        kwargs["dns"] = args.dns
    if args.network:
        kwargs["network"] = args.network
    if args.cap_add:
        kwargs["cap_add"] = args.cap_add
    if args.memory:
        kwargs["mem_limit"] = args.memory
    if args.shm_size:
        kwargs["shm_size"] = args.shm_size
    if args.privileged:
        kwargs["privileged"] = True
    if args.read_only:
        kwargs["read_only"] = True

    if args.tmpfs:
        tmpfs = {}
        for entry in args.tmpfs:
            path, _, opts = entry.partition(":")
            tmpfs[path] = opts
        kwargs["tmpfs"] = tmpfs

    if args.add_host:
        extra_hosts = {}
        for entry in args.add_host:
            name, _, ip = entry.partition(":")
            extra_hosts[name] = ip
        kwargs["extra_hosts"] = extra_hosts

    return kwargs


def _spawn_lambda_container(config: dict, code_zip: bytes | None):
    """Create and start a Lambda container for the given config.

    Returns (container, tmpdir). The caller is responsible for pool registration
    and for `_kill_pool_entry` on cleanup (tmpdir is None for Image-type).

    Handles both Zip and Image PackageType, provided runtimes (bootstrap), Lambda
    Layers (Zip only), DinD (docker cp), LAMBDA_DOCKER_NETWORK, ImageConfig
    overrides (EntryPoint/Command/WorkingDirectory), and AWS_ENDPOINT_URL.
    """
    client = _get_docker_client()
    if client is None:
        raise RuntimeError("Docker daemon unreachable")

    package_type = config.get("PackageType", "Zip")
    runtime = config.get("Runtime", "python3.12")
    handler = config.get("Handler", "index.handler")
    timeout = int(config.get("Timeout", 30 if package_type == "Image" else 3))
    env_vars = _runtime_env_vars(config)
    image_config = ((config.get("ImageConfigResponse") or {}).get("ImageConfig")
                    or config.get("ImageConfig") or {})

    if package_type == "Image":
        image = config.get("ImageUri", "")
        if not image:
            raise ValueError("Image PackageType requires ImageUri")
        is_provided = False
        layers_list = []
    else:
        image = _docker_image_for_runtime(runtime)
        if image is None:
            raise ValueError(f"No Docker image available for runtime '{runtime}'")
        is_provided = runtime.startswith("provided")
        layers_list = config.get("Layers", []) or []

    tmpdir = None
    code_dir = None
    layers_dirs: list[str] = []

    if package_type == "Zip":
        if not code_zip:
            raise ValueError("Zip PackageType requires code_zip bytes")
        tmpdir = tempfile.mkdtemp(prefix="ministack-lambda-docker-")
        code_dir = os.path.join(tmpdir, "code")
        os.makedirs(code_dir)
        code_zip_path = os.path.join(tmpdir, "code.zip")
        with open(code_zip_path, "wb") as f:
            f.write(code_zip)
        with zipfile.ZipFile(code_zip_path) as zf:
            zf.extractall(code_dir)
        if is_provided:
            bootstrap = os.path.join(code_dir, "bootstrap")
            if os.path.exists(bootstrap):
                os.chmod(bootstrap, 0o755)
        for layer_ref in layers_list:
            layer_arn_str = layer_ref if isinstance(layer_ref, str) else layer_ref.get("Arn", "")
            layer_zip = _resolve_layer_zip(layer_arn_str)
            if not layer_zip:
                continue
            idx = len(layers_dirs)
            layer_dir = os.path.join(tmpdir, f"layer_{idx}")
            os.makedirs(layer_dir)
            layer_zip_path = os.path.join(tmpdir, f"layer_{idx}.zip")
            with open(layer_zip_path, "wb") as lf:
                lf.write(layer_zip)
            with zipfile.ZipFile(layer_zip_path) as lzf:
                lzf.extractall(layer_dir)
            layers_dirs.append(layer_dir)

    # Shared environment
    container_env: dict[str, str] = {
        "AWS_DEFAULT_REGION": get_region(),
        "AWS_REGION": get_region(),
        "AWS_ACCESS_KEY_ID": _account_region_from_function_config(config)[0],
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
        "AWS_LAMBDA_FUNCTION_NAME": config["FunctionName"],
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(config.get("MemorySize", 128)),
        "AWS_LAMBDA_FUNCTION_VERSION": config.get("Version", "$LATEST"),
        "AWS_LAMBDA_LOG_STREAM_NAME": new_uuid(),
        "_LAMBDA_FUNCTION_ARN": config.get("FunctionArn", ""),
        "_LAMBDA_TIMEOUT": str(timeout),
    }
    if is_provided:
        container_env["LAMBDA_TASK_ROOT"] = "/var/task"
    container_env["_HANDLER"] = handler
    if layers_dirs:
        container_env["_LAMBDA_LAYERS_DIRS"] = ":".join(
            f"/opt/layer_{i}" for i in range(len(layers_dirs))
        )
    container_env.update(env_vars)
    # Per-invocation durable-execution overlay (no-op when the call isn't
    # inside a durable function).
    container_env.update(_durable_env_overlay())
    # NOTE: X-Ray active tracing is NOT supported in the docker RIE
    # executor. AWS RIE explicitly does not implement X-Ray
    # (https://github.com/aws/aws-lambda-runtime-interface-emulator —
    # "The component does not support X-ray and other Lambda integrations
    # locally") and the RIE container is pooled and reused, so baking
    # ``_X_AMZN_TRACE_ID`` into ``container_env`` here would be stale on
    # every reuse anyway. Functions that need X-Ray must use the warm
    # Python/Node executor (default for those runtimes), the provided
    # runtime, or the local subprocess executor.
    if (config.get("TracingConfig") or {}).get("Mode") == "Active":
        logger.warning(
            "Lambda %s: TracingConfig.Mode=Active is not supported in the "
            "docker RIE executor (AWS RIE limitation). _X_AMZN_TRACE_ID will "
            "not be set in the runtime. Set LAMBDA_EXECUTOR= (empty) to use "
            "the warm worker, which supports X-Ray.",
            config.get("FunctionName", "?"),
        )
    # AWS_ENDPOINT_URL set *after* function env so it always points at ministack.
    # Replace localhost/127.0.0.1 with host.docker.internal so the container
    # can reach the host where ministack is running.
    endpoint = _normalize_endpoint_url(os.environ.get("AWS_ENDPOINT_URL", ""))
    if not endpoint:
        endpoint = _normalize_endpoint_url(env_vars.get("AWS_ENDPOINT_URL", ""))
    if not endpoint:
        endpoint = _normalize_endpoint_url(env_vars.get("LOCALSTACK_HOSTNAME", ""))
    if not endpoint:
        port = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))
        endpoint = f"http://host.docker.internal:{port}"
    else:
        # Rewrite localhost/127.0.0.1 → host.docker.internal for container access
        endpoint = endpoint.replace("://localhost:", "://host.docker.internal:")
        endpoint = endpoint.replace("://localhost/", "://host.docker.internal/")
        endpoint = endpoint.replace("://127.0.0.1:", "://host.docker.internal:")
        endpoint = endpoint.replace("://127.0.0.1/", "://host.docker.internal/")
    container_env["AWS_ENDPOINT_URL"] = endpoint

    # Mounts (Zip only — Image bakes code in)
    _use_docker_cp = False
    mounts: list = []
    if package_type == "Zip":
        host_code_dir = LAMBDA_DOCKER_VOLUME_MOUNT or code_dir
        if LAMBDA_DOCKER_VOLUME_MOUNT:
            mounts.append(docker_lib.types.Mount("/var/task", host_code_dir, type="bind", read_only=True))
            if is_provided:
                mounts.append(docker_lib.types.Mount("/var/runtime", host_code_dir, type="bind", read_only=True))
            for idx, ld in enumerate(layers_dirs):
                mounts.append(docker_lib.types.Mount(f"/opt/layer_{idx}", ld, type="bind", read_only=True))
        elif _running_in_container():
            # DinD: host daemon can't see our tmpfs — populate via docker cp after create
            _use_docker_cp = True
        else:
            mounts.append(docker_lib.types.Mount("/var/task", host_code_dir, type="bind", read_only=True))
            if is_provided:
                mounts.append(docker_lib.types.Mount("/var/runtime", host_code_dir, type="bind", read_only=True))
            for idx, ld in enumerate(layers_dirs):
                mounts.append(docker_lib.types.Mount(f"/opt/layer_{idx}", ld, type="bind", read_only=True))

    # CMD / EntryPoint
    run_kwargs: dict = {
        "image": image,
        "environment": container_env,
        "ports": {"8080/tcp": None},
        "detach": True,
        "stdin_open": False,
        "labels": {"ministack": "lambda"},
    }
    if package_type == "Image":
        # User image brings its own entrypoint. ImageConfig can override.
        if image_config.get("EntryPoint"):
            run_kwargs["entrypoint"] = image_config["EntryPoint"]
        if image_config.get("Command"):
            run_kwargs["command"] = image_config["Command"]
        if image_config.get("WorkingDirectory"):
            run_kwargs["working_dir"] = image_config["WorkingDirectory"]
    else:
        # Zip: RIE expects handler as CMD (or "bootstrap" for provided)
        run_kwargs["command"] = ["bootstrap"] if is_provided else [handler]

    if mounts:
        run_kwargs["mounts"] = mounts
    if LAMBDA_DOCKER_NETWORK:
        run_kwargs["network"] = LAMBDA_DOCKER_NETWORK

    # Apply LAMBDA_DOCKER_FLAGS — merge parsed kwargs into run_kwargs
    if LAMBDA_DOCKER_FLAGS:
        df_kwargs = _parse_docker_flags(LAMBDA_DOCKER_FLAGS)
        df_env = df_kwargs.pop("environment", {})
        if df_env:
            container_env.update(df_env)
        df_mounts = df_kwargs.pop("mounts", [])
        if df_mounts:
            run_kwargs.setdefault("mounts", []).extend(df_mounts)
        run_kwargs.update(df_kwargs)

    # Pull the image on first use (both Zip RIE images and user Image types)
    try:
        client.images.get(image)
    except docker_lib.errors.ImageNotFound:
        logger.info("Pulling Lambda image: %s", image)
        try:
            client.images.pull(image)
        except Exception as exc:
            if tmpdir and os.path.exists(tmpdir):
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)
            raise RuntimeError(f"Failed to pull image {image}: {exc}")

    try:
        if _use_docker_cp:
            create_kwargs = {k: v for k, v in run_kwargs.items()
                             if k not in ("detach", "stdin_open")}
            container = client.containers.create(**create_kwargs)
            _docker_cp_dir(container, code_dir, "/var/task")
            if is_provided:
                _docker_cp_dir(container, code_dir, "/var/runtime")
            for idx, ld in enumerate(layers_dirs):
                # /opt/layer_{idx} doesn't exist in the base RIE image — extract
                # into /opt with the layer dir baked into the arcname (issue #816
                # follow-up: docker executor 404 on archive path).
                _docker_cp_dir(container, ld, "/opt", arcname=f"layer_{idx}")
            container.start()
        else:
            container = client.containers.run(**run_kwargs)
    except Exception:
        if tmpdir and os.path.exists(tmpdir):
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        raise

    return container, tmpdir


def _execute_function_docker(func: dict, event: dict) -> dict:
    """Execute a Lambda function inside a Docker container using AWS RIE.

    Unifies Zip and Image PackageType through a single warm container pool.
    Concurrent invocations use separate pooled containers up to
    `ReservedConcurrentExecutions` (or `_DEFAULT_MAX_CONCURRENCY`).
    Idle containers are reaped after `_WARM_CONTAINER_TTL` seconds.
    Reset and shutdown kill every pooled container.
    """
    config = func.get("config") or func
    package_type = config.get("PackageType", "Zip")
    runtime = config.get("Runtime", "python3.12")

    # Docker availability. Image-type always hard-fails when Docker is absent
    # (there's no meaningful fallback). For Zip, strict mode hard-fails too;
    # permissive mode falls back to the in-process executors.
    if not _docker_available:
        if package_type == "Image" or LAMBDA_STRICT:
            return {"body": {"errorMessage": "Docker is required to invoke Lambda functions",
                             "errorType": "Runtime.DockerUnavailable"}, "error": True}
        if runtime.startswith(("python", "nodejs")):
            logger.warning("docker SDK unavailable - falling back to warm executor")
            return _execute_function_warm(func, event)
        logger.warning("docker SDK unavailable - falling back to local subprocess")
        return _execute_function_local(func, event)

    if _get_docker_client() is None:
        if package_type == "Image" or LAMBDA_STRICT:
            return {"body": {"errorMessage": "Cannot connect to Docker",
                             "errorType": "Runtime.DockerError"}, "error": True}
        logger.warning("Docker daemon unreachable – falling back")
        if runtime.startswith(("python", "nodejs")):
            return _execute_function_warm(func, event)
        return _execute_function_local(func, event)

    # Zip needs code (Image brings it in the image)
    code_zip = func.get("code_zip")
    if package_type == "Zip" and not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    # Early validation of runtime → image mapping to return a clean mock
    if package_type != "Image" and _docker_image_for_runtime(runtime) is None:
        return {"body": {"statusCode": 200,
                         "body": f"Mock response - {runtime} not supported for docker execution"}}

    fn_name = config["FunctionName"]
    timeout = int(config.get("Timeout", 30 if package_type == "Image" else 3))
    # ReservedConcurrentExecutions — explicit cap, else unbounded (matching AWS
    # "function can use the full account pool" default).
    reserved = func.get("concurrency")
    if isinstance(reserved, dict):
        reserved = reserved.get("ReservedConcurrentExecutions")
    max_conc = int(reserved) if reserved else None  # None = unbounded per-function

    _ensure_reaper_thread()
    key = _warm_pool_key(fn_name, config)

    # Acquire or spawn. The only wait loop is for account-cap contention; a
    # function-cap rejection throttles immediately (matching real AWS behaviour
    # — AWS returns 429 on burst, doesn't queue).
    entry = None
    wait_deadline = time.time() + 5  # only used if blocked by account cap
    while True:
        entry, reason = _pool_acquire(key, max_conc)
        if entry is not None:
            break
        if reason == "spawn":
            try:
                container, tmpdir = _spawn_lambda_container(config, code_zip)
            except ValueError as exc:
                return {"body": {"statusCode": 200, "body": f"Mock response - {exc}"}}
            except Exception as exc:
                logger.error("Lambda %s spawn error: %s", fn_name, exc)
                return {"body": {"errorMessage": str(exc),
                                 "errorType": type(exc).__name__}, "error": True, "log": ""}
            entry = _pool_register(key, container, tmpdir)
            logger.info("Lambda %s: cold-start container added to pool", fn_name)
            break
        if reason == "func_cap":
            return _throttle_response(
                reason_code="ReservedFunctionConcurrentInvocationLimitExceeded",
                msg=f"Rate Exceeded: function {fn_name} at ReservedConcurrentExecutions={max_conc}",
            )
        # acct_cap: the account-level soft limit may free up as other invocations
        # complete, so wait briefly before throttling.
        if time.time() >= wait_deadline:
            return _throttle_response(
                reason_code="ConcurrentInvocationLimitExceeded",
                msg=f"Rate Exceeded: account concurrency cap {_ACCOUNT_CONCURRENCY_CAP} reached",
            )
        time.sleep(0.05)

    try:
        result = _invoke_rie(entry["container"], event, timeout)
        if result.get("error") and not _is_container_running(entry["container"]):
            # Container died during invocation — evict so next caller doesn't pick a corpse
            _pool_remove(entry)
            entry = None
        return result
    except Exception as exc:
        msg = str(exc).lower()
        if "timed out" in msg or "read timed out" in msg:
            err_body = {"errorMessage": f"Task timed out after {timeout}.00 seconds",
                        "errorType": "Runtime.ExitError"}
        else:
            err_body = {"errorMessage": str(exc), "errorType": type(exc).__name__}
        logger.error("Lambda %s invocation error: %s", fn_name, exc)
        _pool_remove(entry)
        entry = None
        return {"body": err_body, "error": True, "log": ""}
    finally:
        if entry is not None:
            _pool_release(entry)


# ---------------------------------------------------------------------------
# Function execution (subprocess, stdin-piped, no string interpolation)
# ---------------------------------------------------------------------------


def _probe_peak_memory_mb(func: dict) -> int:
    """Best-effort peak RSS in MB for the last invocation.

    - Docker path: read the most-recently-cached pool entry's container stats
      (`memory_stats.max_usage` on Linux; `memory_stats.usage` as fallback).
    - Non-docker paths: use `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`.
      On Linux ru_maxrss is in KB, on macOS in bytes — normalise.
    - Returns 0 if nothing available (matches AWS's fallback when metrics
      aren't collected).
    """
    try:
        # Docker path — try to fetch stats from the warm-pool container that
        # just served this invocation. The pool key is derived the same way
        # as in _warm_pool_key so we don't need to track per-invocation state.
        config = func.get("config") or func
        fn_name = config.get("FunctionName", "")
        key = _warm_pool_key(fn_name, config)
        entries = _warm_pool.get(key) or []
        if entries:
            container = entries[-1].get("container")
            if container is not None and _docker_available:
                try:
                    stats = container.stats(stream=False, one_shot=True)
                    mem = stats.get("memory_stats", {}) or {}
                    peak = mem.get("max_usage") or mem.get("usage") or 0
                    if peak:
                        return int(peak / (1024 * 1024))
                except Exception:
                    pass
    except Exception:
        pass
    # Non-docker fallback
    try:
        import resource
        ru = resource.getrusage(resource.RUSAGE_CHILDREN)
        # macOS: bytes; Linux: KB. Normalise by sniffing sys.platform.
        import sys
        if sys.platform == "darwin":
            return int(ru.ru_maxrss / (1024 * 1024))
        return int(ru.ru_maxrss / 1024)
    except Exception:
        return 0


def _emit_lambda_logs(func: dict, request_id: str, log_text: str,
                      error: bool, duration_ms: int) -> None:
    """Write handler output to CloudWatch Logs under /aws/lambda/{name}, matching AWS.

    - Log group `/aws/lambda/{FunctionName}` is auto-created on first write.
    - Stream name follows AWS's format: `{yyyy}/{mm}/{dd}/[{qualifier}]{uuid}`.
    - Each invocation emits START / body / END / REPORT lines like real Lambda.
    Best-effort: a failure to write logs must never break the invocation.
    """
    try:
        from ministack.services import cloudwatch_logs as _cwl
        config = func.get("config") or func
        fn_name = config.get("FunctionName", "unknown")
        qualifier = config.get("Version", "$LATEST")
        group_name = f"/aws/lambda/{fn_name}"
        now = datetime.now(timezone.utc)
        stream_name = f"{now.year:04d}/{now.month:02d}/{now.day:02d}/[{qualifier}]{new_uuid().replace('-', '')}"
        now_ms = int(time.time() * 1000)

        if group_name not in _cwl._log_groups:
            _cwl._log_groups[group_name] = {
                "arn": _cwl._make_group_arn(group_name),
                "creationTime": now_ms,
                "retentionInDays": None,
                "tags": {},
                "subscriptionFilters": {},
                "streams": {},
            }
        group = _cwl._log_groups[group_name]
        if stream_name not in group["streams"]:
            group["streams"][stream_name] = {
                "events": [],
                "uploadSequenceToken": "1",
                "creationTime": now_ms,
                "firstEventTimestamp": None,
                "lastEventTimestamp": None,
                "lastIngestionTime": None,
            }
        stream = group["streams"][stream_name]

        lines: list[str] = [f"START RequestId: {request_id} Version: {qualifier}"]
        if log_text:
            lines.extend(log_text.splitlines())
        lines.append(f"END RequestId: {request_id}")
        peak_mb = _probe_peak_memory_mb(func)
        lines.append(
            f"REPORT RequestId: {request_id}\tDuration: {duration_ms} ms\t"
            f"Billed Duration: {duration_ms} ms\tMemory Size: "
            f"{config.get('MemorySize', 128)} MB\tMax Memory Used: {peak_mb} MB"
        )
        for line in lines:
            stream["events"].append({"timestamp": now_ms, "message": line, "ingestionTime": now_ms})
        if stream["firstEventTimestamp"] is None:
            stream["firstEventTimestamp"] = now_ms
        stream["lastEventTimestamp"] = now_ms
        stream["lastIngestionTime"] = now_ms
    except Exception as exc:
        logger.debug("CW Logs emit failed for %s: %s", func.get("config", {}).get("FunctionName"), exc)


def _execute_function(func: dict, event: dict) -> dict:
    """Dispatch an invocation to the right executor and emit CloudWatch Logs.

    - Image PackageType always uses the unified Docker RIE pool.
    - LAMBDA_EXECUTOR=docker routes Zip functions through the same pool.
    - provided runtimes use the in-process Runtime API (Go/Rust binaries).
    - python/nodejs use the subprocess warm worker pool.
    - Anything else falls back to a one-off subprocess.

    Every invocation — regardless of executor — writes a START/body/END/REPORT
    sequence to `/aws/lambda/{FunctionName}`, matching AWS's log shape so
    Metric Filters, subscription filters, and CloudWatch alarms all work.
    """
    config = func.get("config") or func
    if _function_config_scope_mismatch(config):
        return _run_with_function_config_scope(config, _execute_function, func, event)

    request_id = new_uuid()
    started = time.time()

    # Proxy mode wins over every other executor: the function is bound to a
    # user-managed container, ministack only forwards the event.
    proxy_url = _proxy_url_for(config)
    if proxy_url:
        result = _execute_function_proxy(func, event, proxy_url, request_id)
    elif LAMBDA_STRICT:
        result = _execute_function_docker(func, event)
    elif config.get("PackageType") == "Image" and config.get("ImageUri"):
        result = _execute_function_docker(func, event)
    elif LAMBDA_EXECUTOR == "docker":
        result = _execute_function_docker(func, event)
    else:
        runtime = config.get("Runtime", "python3.12")
        if runtime.startswith("provided"):
            result = _execute_function_provided(func, event)
        elif (runtime.startswith("python") or runtime.startswith("nodejs")) \
                and not _durable_ctx.get():
            # Warm pool reuses worker subprocesses whose env was fixed at
            # spawn time. Durable invocations need per-call env (the
            # DurableExecutionArn + CheckpointToken change every invoke),
            # so route them through the per-call local executor.
            result = _execute_function_warm(func, event)
        elif runtime.startswith(("python", "nodejs")):
            # Durable python/nodejs falls through to local subprocess (per
            # the elif above we already filtered durable out of warm).
            result = _execute_function_local(func, event)
        else:
            # java*/dotnet*/ruby* need the real RIE image — there's no
            # in-process executor that can run JVM bytecode or .NET IL.
            result = _execute_function_docker(func, event)

    duration_ms = int((time.time() - started) * 1000)
    _emit_lambda_logs(
        func, request_id,
        result.get("log", "") if isinstance(result, dict) else "",
        bool(result.get("error")) if isinstance(result, dict) else False,
        duration_ms,
    )
    return result


def lambda_execute_result_to_api_proxy_response(exec_result: dict) -> tuple[dict | None, str | None]:
    """Convert :func:`_execute_function` output into an API Gateway AWS_PROXY-shaped dict.

    Shared by REST (v1) and HTTP API (v2) execute paths so ``provided.*`` / Image
    Lambdas match ``lambda invoke`` instead of returning a canned mock.
    """
    if exec_result.get("throttle"):
        tb = exec_result.get("body") or {}
        body_str = json.dumps(tb) if isinstance(tb, dict) else str(tb)
        return {
            "statusCode": 429,
            "headers": {"Content-Type": "application/json"},
            "body": body_str,
        }, None

    if exec_result.get("error"):
        err_body = exec_result.get("body")
        if isinstance(err_body, dict) and "statusCode" in err_body:
            return err_body, None
        payload = json.dumps(err_body) if isinstance(err_body, dict) else str(err_body)
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": payload,
        }, None

    payload = exec_result.get("body")
    if payload is None:
        return {"statusCode": 200, "body": ""}, None
    if isinstance(payload, dict) and "statusCode" in payload:
        return payload, None
    if isinstance(payload, (str, bytes)):
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        stripped = payload.strip()
        if stripped.startswith("{"):
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict) and "statusCode" in obj:
                    return obj, None
            except json.JSONDecodeError:
                pass
        return {"statusCode": 200, "body": payload}, None
    return {"statusCode": 200, "body": json.dumps(payload, ensure_ascii=False)}, None


def _execute_function_proxy(func: dict, event: dict, url: str, request_id: str) -> dict:
    """Forward a Lambda invocation to a user-managed HTTP container.

    The container receives the Lambda event JSON as the request body and is
    expected to reply with the response JSON. Errors (timeout, connection
    refused, non-2xx, malformed JSON) are returned in Lambda's standard
    {"errorMessage", "errorType"} shape so async-invoke retry, DLQ,
    destinations, and CloudWatch error metrics behave the same as for any
    other executor.
    """
    config = func.get("config") or func
    func_name = config.get("FunctionName", "")
    timeout = int(config.get("Timeout") or 3)

    payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-Amzn-Lambda-Function-Name": func_name,
        "X-Amzn-Lambda-Function-Version": config.get("Version", "$LATEST"),
        "X-Amzn-Lambda-Function-Arn": config.get("FunctionArn", ""),
        "X-Amzn-Lambda-Request-Id": request_id,
        "X-Amzn-Lambda-Deadline-Ms": str(int((time.time() + timeout) * 1000)),
    }
    # X-Ray active tracing — pass the trace header to the proxy. The user's
    # container can translate it to ``_X_AMZN_TRACE_ID`` if their code reads
    # X-Ray traces. Proxy mode is by definition not a Lambda runtime
    # emulation, so this is best-effort header forwarding only.
    _xray_trace_id = _xray_trace_id_for_invocation(config)
    if _xray_trace_id:
        headers["X-Amzn-Trace-Id"] = _xray_trace_id
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body_bytes = resp.read()
            status = resp.getcode()
    except urllib.error.HTTPError as e:
        body_bytes = e.read() or b""
        status = e.code
    except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
        is_timeout = isinstance(e, (TimeoutError, socket.timeout)) or (
            isinstance(e, urllib.error.URLError) and isinstance(e.reason, (TimeoutError, socket.timeout))
        )
        return {
            "body": {
                "errorMessage": f"Task timed out after {timeout}.00 seconds" if is_timeout
                else f"Proxy target {url} unreachable: {e}",
                "errorType": "Sandbox.Timedout" if is_timeout else "Runtime.HandlerError",
            },
            "error": True,
        }
    except Exception as e:  # pragma: no cover - defensive
        return {
            "body": {"errorMessage": str(e), "errorType": "Runtime.HandlerError"},
            "error": True,
        }

    if status < 200 or status >= 300:
        return {
            "body": {
                "errorMessage": f"Proxy target returned HTTP {status}: "
                                f"{body_bytes[:512].decode('utf-8', 'replace')}",
                "errorType": "Runtime.HandlerError",
            },
            "error": True,
        }

    text = body_bytes.decode("utf-8", "replace")
    if not text:
        return {"body": None}
    try:
        return {"body": json.loads(text)}
    except json.JSONDecodeError:
        return {"body": text}


def _execute_function_warm(func: dict, event: dict) -> dict:
    """Execute a Lambda function using the warm worker pool (Python + Node.js)."""
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    func_name = config.get("FunctionName", "unknown")
    qualifier = config.get("Version", "$LATEST")
    try:
        worker = get_or_create_worker(func_name, config, code_zip, qualifier=qualifier)
        # Inject X-Ray trace header into the event so the worker bootstrap
        # can set ``_X_AMZN_TRACE_ID`` in os.environ before calling the
        # handler. Per-invocation, not bake-time, so it can't live in the
        # worker's spawn env.
        _xray = _xray_trace_id_for_invocation(config)
        if _xray:
            event["_x_amzn_trace_id"] = _xray
        result = worker.invoke(event, new_uuid())
        if result.get("status") == "ok":
            return {"body": result.get("result"), "log": result.get("log", "")}
        else:
            error_msg = result.get("error", "Unknown error")
            error_type = "Runtime.HandlerError"
            if "timed out" in error_msg.lower():
                error_type = "Runtime.ExitError"
            return {
                "body": {
                    "errorMessage": error_msg,
                    "errorType": error_type,
                },
                "error": True,
                "log": "\n".join(filter(None, [result.get("log", ""), result.get("trace", result.get("error", ""))])),
            }
    except Exception as e:
        logger.error("Warm worker execution error for %s: %s", func_name, e)
        invalidate_worker(func_name, qualifier=qualifier, account=get_account_id(), region=get_region())
        return {
            "body": {"errorMessage": str(e), "errorType": type(e).__name__},
            "error": True,
            "log": "",
        }


def _execute_function_provided(func: dict, event: dict) -> dict:
    """Execute a provided-runtime Lambda (Go/Rust binary) via a minimal Lambda Runtime API."""
    config = func.get("config") or func
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response - no code deployed"}}

    timeout = config.get("Timeout", 30)
    env_vars = _runtime_env_vars(config)

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
            class _QuietTCPServer(socketserver.TCPServer):
                def handle_error(self, request, client_address):
                    import sys
                    _, exc, _ = sys.exc_info()
                    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
                        return
                    super().handle_error(request, client_address)

            server = _QuietTCPServer(("127.0.0.1", 0), RuntimeAPIHandler)
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
                    "AWS_DEFAULT_REGION": get_region(),
                    "AWS_REGION": get_region(),
                    "AWS_ACCESS_KEY_ID": _account_region_from_function_config(config)[0],
                    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                    "AWS_LAMBDA_FUNCTION_NAME": config.get("FunctionName", "unknown"),
                    "LAMBDA_TASK_ROOT": code_dir,
                    "_HANDLER": config.get("Handler", "bootstrap"),
                })
                proc_env.update(env_vars)
                proc_env.update(_durable_env_overlay())
                # X-Ray active tracing. ``_execute_function_provided`` builds
                # ``proc_env`` per-invocation, so a per-call trace ID is safe
                # here (unlike the RIE pool). aws-xray-sdk reads this env var
                # per-segment via ``os.getenv``, so the runtime sees it.
                _xray_trace_id = _xray_trace_id_for_invocation(config)
                if _xray_trace_id:
                    proc_env["_X_AMZN_TRACE_ID"] = _xray_trace_id
                # Override AWS_ENDPOINT_URL *after* function env vars so
                # Lambda binaries always call back to this MiniStack
                # instance.  Function-level env vars may carry the
                # host-mapped URL which is unreachable from inside the
                # container.
                endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
                if not endpoint:
                    hostname = os.environ.get("LOCALSTACK_HOSTNAME", "")
                    if hostname:
                        endpoint = _normalize_endpoint_url(hostname)
                if endpoint:
                    proc_env["AWS_ENDPOINT_URL"] = endpoint

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
    env_vars = _runtime_env_vars(config)

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

            # Symlink layer node_modules packages into the code directory so that
            # Node.js ESM import() can resolve them via ancestor-tree lookup.
            if layers_dirs and is_node:
                code_nm = os.path.join(code_dir, "node_modules")
                os.makedirs(code_nm, exist_ok=True)
                for ld in layers_dirs:
                    layer_nm = os.path.join(ld, "nodejs", "node_modules")
                    if os.path.isdir(layer_nm):
                        for pkg in os.listdir(layer_nm):
                            src = os.path.join(layer_nm, pkg)
                            dst = os.path.join(code_nm, pkg)
                            if not os.path.exists(dst):
                                os.symlink(src, dst)

            if "." not in handler:
                return {"body": {"errorMessage": f"Invalid handler format: {handler}", "errorType": "Runtime.InvalidEntrypoint"}, "error": True}
            module_name, func_name = handler.rsplit(".", 1)
            # AWS Python Lambda accepts both dot (``pkg.mod.fn``) and slash
            # (``pkg/mod.fn``) in nested handler paths; ``__import__`` only
            # takes dot. Other runtimes (Node.js, etc.) keep the raw string
            # because they don't use Python module resolution.
            if runtime.startswith("python"):
                module_name = module_name.replace("/", ".")

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
                    "AWS_DEFAULT_REGION": get_region(),
                    "AWS_REGION": get_region(),
                    "AWS_ACCESS_KEY_ID": _account_region_from_function_config(config)[0],
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
            if not endpoint:
                # Subprocess runs on the same host as ministack — point it at
                # ourselves so boto3 calls land back here, not at real AWS.
                gateway_port = os.environ.get("GATEWAY_PORT", "4566")
                endpoint = f"http://{_MINISTACK_HOST}:{gateway_port}"
            if endpoint:
                env["AWS_ENDPOINT_URL"] = endpoint
            env.update(env_vars)
            env.update(_durable_env_overlay())
            # X-Ray active tracing — one-shot subprocess, env is per-invocation
            # so a fresh trace ID per call is safe (unlike the pooled RIE).
            _xray_trace_id = _xray_trace_id_for_invocation(config)
            if _xray_trace_id:
                env["_X_AMZN_TRACE_ID"] = _xray_trace_id

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
    try:
        spec = parse_arn(layer_arn_str)
    except ArnParseError:
        return None
    if spec.service != "lambda":
        return None
    if spec.account_id != get_account_id():
        return None
    if spec.region != get_region():
        return None
    parts = spec.resource.split(":", 2)
    if len(parts) != 3 or parts[0] != "layer":
        return None
    layer_name = parts[1]
    try:
        version = int(parts[2])
    except ValueError:
        return None
    layer = _layers.get_scoped(spec.account_id, spec.region, layer_name)
    if not layer:
        return None
    for v in layer["versions"]:
        if v["Version"] == version:
            return v.get("_zip_data")
    return None


def _layer_codesize_for_arn(layer_arn_str: str) -> int:
    """Look up the stored layer version's CodeSize, or 0 if the layer
    version can't be resolved. Real AWS surfaces the actual layer code size on
    `GetFunctionConfiguration.Layers[*].CodeSize` so callers can sanity-check
    against quotas (250 MB unzipped function + layers)."""
    version_config, err = _resolve_layer_version_for_attachment(layer_arn_str)
    if err:
        return 0
    return version_config.get("Content", {}).get("CodeSize", 0)


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
    # #440: Terraform sends RoutingConfig={"AdditionalVersionWeights": {}} even
    # when no weighted routing is declared. Only store (and echo back) when
    # the weights map is non-empty — real AWS omits RoutingConfig from
    # responses when no weights are configured.
    rc = data.get("RoutingConfig")
    if rc and rc.get("AdditionalVersionWeights"):
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
    for key in ("FunctionVersion", "Description"):
        if key in data:
            alias[key] = data[key]
    # #440: same rule as CreateAlias — empty AdditionalVersionWeights means
    # "no weighted routing"; store only when non-empty, and remove an existing
    # RoutingConfig if the caller clears it.
    if "RoutingConfig" in data:
        rc = data["RoutingConfig"]
        if rc and rc.get("AdditionalVersionWeights"):
            alias["RoutingConfig"] = rc
        else:
            alias.pop("RoutingConfig", None)
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


def _policy_resource_arn(func_name: str, qualifier: str | None) -> str:
    resource_arn = _func_arn(func_name)
    if qualifier:
        resource_arn = f"{resource_arn}:{qualifier}"
    return resource_arn


def _add_permission(
    func_name: str,
    data: dict,
    query_params: dict | None = None,
    path_qualifier: str | None = None,
):
    qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
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

    resource_arn = _policy_resource_arn(func_name, qualifier)

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


def _remove_permission(
    func_name: str,
    sid: str,
    query_params: dict,
    path_qualifier: str | None = None,
):
    qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
    resource_arn = _policy_resource_arn(func_name, qualifier) if qualifier else None
    func = _functions[func_name]
    before = len(func["policy"]["Statement"])
    func["policy"]["Statement"] = [
        s
        for s in func["policy"]["Statement"]
        if s.get("Sid") != sid or (resource_arn is not None and s.get("Resource") != resource_arn)
    ]
    if len(func["policy"]["Statement"]) == before:
        return error_response_json(
            "ResourceNotFoundException",
            "No policy is associated with the given resource.",
            404,
        )
    return 204, {}, b""


def _get_policy(
    func_name: str,
    query_params: dict | None = None,
    path_qualifier: str | None = None,
):
    qualifier = _qualifier_from_path_or_query(path_qualifier, query_params)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {_func_arn(func_name)}",
            404,
        )
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
    func = _functions[func_name]
    policy = copy.deepcopy(func["policy"])
    if qualifier:
        resource_arn = _policy_resource_arn(func_name, qualifier)
        policy["Statement"] = [s for s in policy["Statement"] if s.get("Resource") == resource_arn]
        if not policy["Statement"]:
            return error_response_json(
                "ResourceNotFoundException",
                "No policy is associated with the given resource.",
                404,
            )
    return json_response(
        {
            "Policy": json.dumps(policy),
            "RevisionId": func["config"]["RevisionId"],
        }
    )


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _esm_id_from_arn(resource_arn: str) -> str | None:
    """Return the ESM UUID if the ARN points to an event-source-mapping, else None."""
    try:
        spec = parse_arn(resource_arn)
    except ArnParseError:
        return None
    if (
        spec.service != "lambda"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None
    prefix = "event-source-mapping:"
    if not spec.resource.startswith(prefix):
        return None
    esm_id = spec.resource[len(prefix):]
    return esm_id or None


def _list_tags(resource_arn: str):
    esm_id = _esm_id_from_arn(resource_arn)
    if esm_id is not None:
        esm = _esms.get(esm_id)
        if not esm:
            return error_response_json(
                "ResourceNotFoundException",
                "The resource you requested does not exist.",
                404,
            )
        return json_response({"Tags": esm.get("Tags", {})})
    func_name = _resolve_request_scoped_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
        )
    return json_response({"Tags": _functions[func_name].get("tags", {})})


def _tag_resource(resource_arn: str, data: dict):
    esm_id = _esm_id_from_arn(resource_arn)
    if esm_id is not None:
        esm = _esms.get(esm_id)
        if not esm:
            return error_response_json(
                "ResourceNotFoundException",
                "The resource you requested does not exist.",
                404,
            )
        esm.setdefault("Tags", {}).update(data.get("Tags", {}))
        return 204, {}, b""
    func_name = _resolve_request_scoped_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
        )
    _functions[func_name].setdefault("tags", {}).update(data.get("Tags", {}))
    return 204, {}, b""


def _untag_resource(resource_arn: str, query_params: dict):
    raw = query_params.get("tagKeys", query_params.get("TagKeys", []))
    if isinstance(raw, list):
        tag_keys = raw
    elif isinstance(raw, str):
        tag_keys = [raw]
    else:
        tag_keys = []

    esm_id = _esm_id_from_arn(resource_arn)
    if esm_id is not None:
        esm = _esms.get(esm_id)
        if not esm:
            return error_response_json(
                "ResourceNotFoundException",
                "The resource you requested does not exist.",
                404,
            )
        tags = esm.setdefault("Tags", {})
        for k in tag_keys:
            tags.pop(k.strip(), None)
        return 204, {}, b""

    func_name = _resolve_request_scoped_name(resource_arn)
    if func_name not in _functions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {resource_arn}",
            404,
        )
    tags = _functions[func_name].setdefault("tags", {})
    for k in tag_keys:
        tags.pop(k.strip(), None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Layers
# ---------------------------------------------------------------------------

def _layer_content_url(layer_name: str, version: int) -> str:
    port = os.environ.get("GATEWAY_PORT", "4566")
    return (
        f"http://{_MINISTACK_HOST}:{port}/_ministack/lambda-layers/"
        f"{get_account_id()}/{get_region()}/{layer_name}/{version}/content"
    )   

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
    elif "S3Bucket" in content and "S3Key" in content:
        zip_data = _fetch_code_from_s3(
            content["S3Bucket"],
            content["S3Key"],
            version_id=content.get("S3ObjectVersion"),
        )

    err = _validate_unzipped_size(zip_data)
    if err is not None:
        return err

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
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        spec = None
    if spec and spec.service == "lambda":
        parts = spec.resource.split(":", 2)
    else:
        parts = []
    if len(parts) != 3 or parts[0] != "layer" or not parts[2].isdigit():
        return error_response_json(
            "ValidationException",
            f"Value '{arn}' at 'arn' failed to satisfy constraint: "
            "Member must satisfy regular expression pattern: "
            "arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\\d{{1}}:\\d{{12}}:layer:[a-zA-Z0-9-_]+:[0-9]+",
            400,
        )
    if spec.account_id != get_account_id():
        return error_response_json(
            "AccessDeniedException",
            "User is not authorized to access this resource.",
            403,
        )
    if spec.region != get_region():
        return error_response_json(
            "ResourceNotFoundException",
            "The resource you requested does not exist.",
            404,
        )
    layer_name = parts[1]
    version = int(parts[2])
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


def _find_layer_version(
    layer_name: str,
    version: int,
    account_id: str | None = None,
    region: str | None = None,
):
    """Return (layer_version_config, error_response) — one will be None."""
    if account_id and region:
        layer = _layers.get_scoped(account_id, region, layer_name)
    else:
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


def serve_layer_content(
    layer_name: str,
    version: int,
    account_id: str | None = None,
    region: str | None = None,
):
    """Serve raw zip bytes for a layer version (called from app.py)."""
    vc, err = _find_layer_version(layer_name, version, account_id=account_id, region=region)
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
    destination_config = data.get(
        "DestinationConfig",
        {
            "OnSuccess": {},
            "OnFailure": {},
        },
    )
    err = _validate_destination_config(destination_config)
    if err:
        return err
    eic = {
        "FunctionArn": _func_arn(func_name),
        "MaximumRetryAttempts": data.get("MaximumRetryAttempts", 2),
        "MaximumEventAgeInSeconds": data.get("MaximumEventAgeInSeconds", 21600),
        "LastModified": int(time.time()),
        "DestinationConfig": destination_config,
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
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
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
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
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
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
    key = qualifier or "$LATEST"
    _functions[func_name].get("provisioned_concurrency", {}).pop(key, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Event Source Mappings
# ---------------------------------------------------------------------------


def _esm_response(esm: dict) -> dict:
    """Return ESM dict without internal-only fields."""
    return {k: v for k, v in esm.items() if k not in ("FunctionName", "Enabled")}


def _resolve_existing_esm_function(function_ref: str):
    func_name, qualifier = _resolve_request_scoped_name_and_qualifier(function_ref)
    if not func_name or func_name not in _functions:
        return None, None, error_response_json(
            "ResourceNotFoundException",
            f"Function not found: {function_ref}",
            404,
        )
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return None, None, err
    return func_name, qualifier, None


def _create_esm(data: dict):
    esm_id = new_uuid()
    # Preserve the alias/version qualifier if the caller supplied one so
    # poller invocations route to the correct target (#407).
    func_name, qualifier, err = _resolve_existing_esm_function(
        data.get("FunctionName", ""),
    )
    if err:
        return err
    event_source_arn = data.get("EventSourceArn", "")
    event_source_spec, err = _validate_event_source_arn(event_source_arn)
    if err:
        return err

    enabled = data.get("Enabled", True)
    if isinstance(enabled, str):
        enabled = enabled.lower() != "false"
    esm = {
        "UUID": esm_id,
        "EventSourceArn": event_source_arn,
        "FunctionArn": _func_arn(func_name) + (f":{qualifier}" if qualifier else ""),
        "FunctionName": func_name,
        "Qualifier": qualifier,
        "State": "Enabled" if enabled else "Disabled",
        "StateTransitionReason": "USER_INITIATED",
        "BatchSize": data.get("BatchSize", 10),
        "MaximumBatchingWindowInSeconds": data.get("MaximumBatchingWindowInSeconds", 0),
        "LastModified": int(time.time()),
        "LastProcessingResult": "No records processed",
        "Enabled": enabled,
        "FunctionResponseTypes": data.get("FunctionResponseTypes", []),
    }
    if event_source_spec.service != "sqs":
        esm["StartingPosition"] = data.get("StartingPosition", "LATEST")
    # #442: Tags are accepted on CreateEventSourceMapping. Stored inline on
    # the ESM record; surfaced via _list_tags for the ESM ARN.
    tags = data.get("Tags") or {}
    if tags:
        esm["Tags"] = dict(tags)
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
    func = _resolve_request_scoped_name(_qp_first(query_params, "FunctionName"))
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
        new_name, qualifier, err = _resolve_existing_esm_function(data["FunctionName"])
        if err:
            return err
        esm["FunctionName"] = new_name
        esm["Qualifier"] = qualifier
        esm["FunctionArn"] = _func_arn(new_name) + (f":{qualifier}" if qualifier else "")
    esm["LastModified"] = int(time.time())
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
_kinesis_positions = AccountRegionScopedDict()
# Per-ESM DynamoDB stream tracking: esm_uuid -> {shard_id: position}
_dynamodb_stream_positions = AccountRegionScopedDict()
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
        time.sleep(1 if _esms.has_any() else 5)


def _iter_all_esms():
    for scoped_key, esm in list(_esms._data.items()):
        if len(scoped_key) == 3:
            acct_id, region, _esm_key = scoped_key
        else:
            acct_id, _esm_key = scoped_key
            region = REGION
        yield acct_id, region, esm


def _poll_sqs():
    from ministack.services import sqs as _sqs

    for acct_id, region, esm in _iter_all_esms():
        account_token = _request_account_id.set(acct_id)
        region_token = _request_region.set(region)
        try:
            if not esm.get("Enabled", True):
                continue
            source_arn = esm.get("EventSourceArn", "")
            try:
                source_spec = parse_arn(source_arn)
            except ArnParseError:
                continue
            if (
                source_spec.service != "sqs"
                or source_spec.account_id != get_account_id()
                or source_spec.region != get_region()
            ):
                continue
            queue_name = _sqs_queue_name_from_arn_spec(source_spec)
            if not queue_name:
                continue

            func_name = esm["FunctionName"]
            qualifier = esm.get("Qualifier")
            func_rec, _cfg = _get_func_record_for_qualifier(func_name, qualifier)
            if func_rec is None:
                continue

            queue_url = _sqs._queue_url(queue_name)
            queue = _sqs._queues.get(queue_url)
            if not queue or queue.get("attributes", {}).get("QueueArn") != source_arn:
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
                    "awsRegion": get_region(),
                })

            # Apply FilterCriteria before invoking — AWS filters records out
            # *before* the handler runs, so non-matching records are silently
            # dropped (and immediately deleted from the queue like successful ones).
            records = _apply_filter_criteria(records, esm)
            if not records:
                # All records filtered out — treat the batch as processed.
                for msg in batch:
                    queue["messages"].remove(msg)
                continue

            event = {"Records": records}
            result = _execute_function(func_rec, event)

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
                # Check for ReportBatchItemFailures — partial batch response
                failed_ids = set()
                if "ReportBatchItemFailures" in esm.get("FunctionResponseTypes", []):
                    body = result.get("body")
                    if isinstance(body, dict):
                        for failure in body.get("batchItemFailures", []):
                            fid = failure.get("itemIdentifier", "")
                            if fid:
                                failed_ids.add(fid)
                    elif isinstance(body, str):
                        try:
                            parsed = json.loads(body)
                            for failure in parsed.get("batchItemFailures", []):
                                fid = failure.get("itemIdentifier", "")
                                if fid:
                                    failed_ids.add(fid)
                        except (json.JSONDecodeError, AttributeError):
                            pass

                # Delete only the messages that succeeded (not in failed_ids)
                succeeded = [msg for msg in batch if msg["id"] not in failed_ids]
                receipt_handles = {msg["receipt_handle"] for msg in succeeded if msg.get("receipt_handle")}
                if receipt_handles:
                    _sqs._delete_messages_for_esm(queue_url, receipt_handles)

                n_failed = len(batch) - len(succeeded)
                if n_failed:
                    esm["LastProcessingResult"] = f"OK - {len(succeeded)} records, {n_failed} partial failures"
                    logger.info("ESM: Lambda %s processed %d SQS messages from %s (%d partial failures)",
                                func_name, len(succeeded), queue_name, n_failed)
                else:
                    esm["LastProcessingResult"] = f"OK - {len(batch)} records"
                    logger.info("ESM: Lambda %s processed %d SQS messages from %s", func_name, len(batch), queue_name)
                log_output = result.get("log", "")
                if log_output:
                    logger.info("ESM: Lambda %s output:\n%s", func_name, log_output)
        finally:
            _request_account_id.reset(account_token)
            _request_region.reset(region_token)


def _poll_kinesis():
    from ministack.services import kinesis as _kin

    for acct_id, region, esm in _iter_all_esms():
        account_token = _request_account_id.set(acct_id)
        region_token = _request_region.set(region)
        try:
            if not esm.get("Enabled", True):
                continue
            source_arn = esm.get("EventSourceArn", "")
            try:
                source_spec = parse_arn(source_arn)
            except ArnParseError:
                continue
            if (
                source_spec.service != "kinesis"
                or source_spec.account_id != get_account_id()
                or source_spec.region != get_region()
            ):
                continue
            stream_name = _kinesis_stream_name_from_arn_spec(source_spec)
            if not stream_name:
                continue

            func_name = esm["FunctionName"]
            qualifier = esm.get("Qualifier")
            func_rec, _cfg = _get_func_record_for_qualifier(func_name, qualifier)
            if func_rec is None:
                continue

            stream = _kin._streams.get(stream_name)
            if not stream or stream.get("StreamARN") != source_arn or stream["StreamStatus"] != "ACTIVE":
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
                        "awsRegion": get_region(),
                        "eventSourceARN": source_arn,
                    })

                # AWS drops records that don't match FilterCriteria before invoke.
                # Advance past the raw batch we consumed — filtered records are
                # treated as "successfully processed" (same semantics as the
                # normal success path below, which adds len(raw_records)).
                records = _apply_filter_criteria(records, esm)
                if not records:
                    _kinesis_positions[esm_id][shard_id] = pos + len(raw_records)
                    continue

                event = {"Records": records}
                result = _execute_function(func_rec, event)

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
        finally:
            _request_account_id.reset(account_token)
            _request_region.reset(region_token)


def _poll_dynamodb_streams():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if not stream_records:
        return

    for acct_id, region, esm in _iter_all_esms():
        account_token = _request_account_id.set(acct_id)
        region_token = _request_region.set(region)
        try:
            if not esm.get("Enabled", True):
                continue
            source_arn = esm.get("EventSourceArn", "")
            try:
                source_spec = parse_arn(source_arn)
            except ArnParseError:
                continue
            if (
                source_spec.service != "dynamodb"
                or source_spec.account_id != get_account_id()
                or source_spec.region != get_region()
            ):
                continue
            table_stream = _dynamodb_stream_table_from_arn_spec(source_spec)
            if table_stream is None:
                continue
            table_name, _stream_label = table_stream

            func_name = esm["FunctionName"]
            qualifier = esm.get("Qualifier")
            func_rec, _cfg = _get_func_record_for_qualifier(func_name, qualifier)
            if func_rec is None:
                continue

            table = _ddb._tables.get(table_name)
            if not table or table.get("LatestStreamArn") != source_arn:
                continue
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

            batch = _apply_filter_criteria(batch, esm)
            if not batch:
                # All records filtered — advance position so we don't re-evaluate.
                with _dynamodb_stream_positions_lock:
                    _dynamodb_stream_positions[esm_id] = pos + batch_size
                continue

            event = {"Records": batch}
            result = _execute_function(func_rec, event)

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
        finally:
            _request_account_id.reset(account_token)
            _request_region.reset(region_token)


# ---------------------------------------------------------------------------
# Function URL Config
# ---------------------------------------------------------------------------


def _url_config_key(func_name: str, qualifier: str | None) -> str:
    return f"{func_name}:{qualifier}" if qualifier and qualifier != "$LATEST" else func_name


def _validate_function_url_qualifier(func_name: str, qualifier: str | None):
    err = _validate_function_qualifier_exists(func_name, qualifier)
    if err:
        return err
    if qualifier and qualifier != "$LATEST" and qualifier not in _functions[func_name].get("aliases", {}):
        return error_response_json(
            "InvalidParameterValueException",
            "Function URLs can only be configured for an unqualified function or an alias.",
            400,
        )
    return None


def _create_function_url_config(func_name: str, data: dict, qualifier: str | None):
    if func_name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function not found: {_func_arn(func_name)}", 404)
    err = _validate_function_url_qualifier(func_name, qualifier)
    if err:
        return err
    key = _url_config_key(func_name, qualifier)
    if key in _function_urls:
        return error_response_json(
            "ResourceConflictException", f"Function URL config already exists for {func_name}", 409
        )
    cfg = {
        "FunctionUrl": f"https://{new_uuid()}.lambda-url.{get_region()}.on.aws/",
        "FunctionArn": _func_arn(func_name),
        "AuthType": data.get("AuthType", "NONE"),
        "InvokeMode": data.get("InvokeMode", "BUFFERED"),
        "CreationTime": _now_iso(),
        "LastModifiedTime": _now_iso(),
    }
    if data.get("Cors"):
        cfg["Cors"] = data["Cors"]
    _function_urls[key] = cfg
    return json_response(cfg, status=201)


def _get_function_url_config(func_name: str, qualifier: str | None):
    err = _validate_function_url_qualifier(func_name, qualifier)
    if err:
        return err
    key = _url_config_key(func_name, qualifier)
    cfg = _function_urls.get(key)
    if not cfg:
        return error_response_json("ResourceNotFoundException", f"Function URL config not found for {func_name}", 404)
    return json_response(cfg)


def _update_function_url_config(func_name: str, data: dict, qualifier: str | None):
    err = _validate_function_url_qualifier(func_name, qualifier)
    if err:
        return err
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
    if key in _function_urls:
        del _function_urls[key]
        return 204, {}, b""
    err = _validate_function_url_qualifier(func_name, qualifier)
    if err:
        return err
    return error_response_json("ResourceNotFoundException", f"Function URL config not found for {func_name}", 404)


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
    _pool_clear_all()
    lambda_runtime.reset()


# ---------------------------------------------------------------------------
# Persisted-state restore — runs at module import time but deferred to the
# very bottom of the file so forward references to helpers (e.g.
# ``_ensure_poller``) resolve at call time (issue #412). A corrupt or
# incompatible ``lambda.json`` logs and continues instead of breaking the
# whole service.
# ---------------------------------------------------------------------------
try:
    _restored = load_state("lambda")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted Lambda state; continuing with a fresh store")
