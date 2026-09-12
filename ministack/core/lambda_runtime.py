# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Lambda warm/cold start worker pool.
Each function gets a persistent worker process (Python or Node.js) that imports
the handler once (cold start) and then handles subsequent invocations without
re-importing (warm).
"""

import hashlib
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import _12_DIGIT_RE

logger = logging.getLogger("lambda_runtime")

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
    "_LAMBDA_FUNCTION_ARN",
    "_LAMBDA_TIMEOUT",
    "LAMBDA_TASK_ROOT",
}


def _account_from_arn(arn: str) -> str:
    """Extract the 12-digit account ID from a Lambda function ARN.

    Falls back to the host's AWS_ACCESS_KEY_ID if the ARN is malformed.
    Defined locally to avoid circular imports with lambda_svc."""
    try:
        parts = arn.split(":")
        if len(parts) >= 5 and _12_DIGIT_RE.match(parts[4]):
            return parts[4]
    except (AttributeError, TypeError):
        pass
    return os.environ.get("AWS_ACCESS_KEY_ID", "test")


def _lambda_function_account_region_from_arn(arn: str) -> tuple[str, str]:
    spec = parse_arn(arn)
    if spec.service != "lambda":
        raise ArnParseError("arn: expected lambda service")
    if not _12_DIGIT_RE.match(spec.account_id):
        raise ArnParseError("arn: expected 12-digit account id")
    if not spec.region:
        raise ArnParseError("arn: expected region")
    parts = spec.resource.split(":", 2)
    if len(parts) < 2 or parts[0] != "function" or not parts[1]:
        raise ArnParseError("arn: expected lambda function resource")
    return spec.account_id, spec.region


def _account_region_from_function_config(config: dict) -> tuple[str, str]:
    arn = config.get("FunctionArn", "")
    if not arn:
        raise ArnParseError("arn: missing lambda function arn")
    return _lambda_function_account_region_from_arn(arn)


def execution_credentials(config: dict) -> dict[str, str]:
    """Return account credentials or an AUTH execution-role session."""
    account_id, _region = _account_region_from_function_config(config)
    from ministack import app

    if not app.AUTH:
        return {
            "AWS_ACCESS_KEY_ID": account_id,
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
        }

    role_arn = config.get("Role", "")
    role_name = role_arn.rsplit("/", 1)[-1]
    function_arn = config.get("FunctionArn", "")
    digest = hashlib.sha256(f"{function_arn}:{role_arn}".encode()).hexdigest()
    access_key = f"ASIA{digest[:16].upper()}"
    secret_key = hashlib.sha256(f"secret:{digest}".encode()).hexdigest()
    session_token = hashlib.sha256(f"token:{digest}".encode()).hexdigest()
    session_name = config.get("FunctionName", "ministack-lambda")[:64]

    from ministack.services import sts as sts_svc

    sts_svc.register_session(access_key, {
        "Arn": f"arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}",
        "UserId": f"{role_name}:{session_name}",
        "SecretAccessKey": secret_key,
        "SessionToken": session_token,
    })
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_SESSION_TOKEN": session_token,
    }


_workers: dict = {}
_lock = threading.Lock()

# No per-function ceiling on warm subprocesses: AWS lets an unreserved function
# scale into the account pool, and capping width here would invent a throttle
# AWS never sends and diverge from the docker executor, which spawns freely.
# Concurrency is bounded once, at lambda_svc._acquire_execution_slot, via
# ReservedConcurrentExecutions and the existing LAMBDA_ACCOUNT_CONCURRENCY.
# The reaper reclaims the surplus subprocesses a burst leaves behind.
_LOCAL_MAX_WORKERS = 0
# Seconds a surplus worker may sit idle before it is reaped (the first worker
# per function is never reaped, so warm starts are unaffected). Shares the knob
# that already governs warm-container eviction rather than adding a second one.
_LOCAL_WORKER_TTL = float(os.environ.get("LAMBDA_WARM_TTL_SECONDS", "300"))

# ---------------------------------------------------------------------------
# Recursive-loop lineage (runs inside every ministack-owned Python child)
# ---------------------------------------------------------------------------

# Name of the env var each executor sets to the current invocation's loop
# depth, and of the header the child stamps onto outgoing SDK calls so the
# nested Invoke can be attributed to its caller. Kept here (not in lambda_svc)
# because both the worker script and lambda_svc's one-shot wrappers embed it.
INVOKE_DEPTH_ENV = "_MINISTACK_INVOKE_DEPTH"
INVOKE_DEPTH_HEADER = "X-Ministack-Invoke-Depth"
# The warm worker's env is fixed at spawn time, so its depth rides in the
# event and is popped off before the handler sees it — same trick the X-Ray
# trace header uses.
INVOKE_DEPTH_EVENT_KEY = "_ministack_invoke_depth"


def _sub_depth_tokens(script: str) -> str:
    """Substitute the invoke-depth carrier names into a bootstrap script."""
    return (
        script.replace("__DEPTH_ENV__", INVOKE_DEPTH_ENV)
        .replace("__DEPTH_HEADER__", INVOKE_DEPTH_HEADER)
        .replace("__DEPTH_EVENT_KEY__", INVOKE_DEPTH_EVENT_KEY)
    )


# Prepended to the Python bootstraps we own (warm worker + one-shot wrappers),
# which call `_ms_install_invoke_depth()` once sys.path is set up and before
# they import the handler module — botocore copies BUILTIN_HANDLERS into every
# Session it builds, and handlers routinely build their clients at import time.
#
# botocore forwards `_X_AMZN_TRACE_ID` as `X-Amzn-Trace-Id` on every outgoing
# call through its own `add_recursion_detection_header`; this registers a
# handler alongside it for ministack's depth counter.
#
# Importing botocore is what a cold start pays for the counter (~0.15s here),
# once per worker — or once per invocation on the one-shot executor, which
# only durable invocations use. A function whose environment has no botocore
# is not instrumented: it would have to be calling Invoke over raw urllib,
# which never carried lineage anyway.
INVOKE_DEPTH_BOOTSTRAP = _sub_depth_tokens('''
def _ms_install_invoke_depth():
    import os
    try:
        import botocore.handlers
    except ImportError:
        return

    def _ms_add_invoke_depth(params, **kwargs):
        depth = os.environ.get("__DEPTH_ENV__")
        if depth and "__DEPTH_HEADER__" not in params["headers"]:
            params["headers"]["__DEPTH_HEADER__"] = depth

    botocore.handlers.BUILTIN_HANDLERS.append(("before-call", _ms_add_invoke_depth))
''')

# ---------------------------------------------------------------------------
# Python worker script (runs inside a persistent subprocess)
# ---------------------------------------------------------------------------

_PYTHON_WORKER_SCRIPT = INVOKE_DEPTH_BOOTSTRAP + _sub_depth_tokens('''
import sys, json, importlib, traceback, os, time

def run():
    # Redirect print() to stderr so stdout stays clean for JSON-line protocol
    _real_stdout = sys.stdout
    sys.stdout = sys.stderr

    init = json.loads(sys.stdin.readline())
    code_dir = init["code_dir"]
    module_name = init["module"]
    handler_name = init["handler"]
    env = init.get("env", {})
    os.environ.update(env)
    sys.path.insert(0, code_dir)
    for _ld in filter(None, os.environ.get("_LAMBDA_LAYERS_DIRS", "").split(os.pathsep)):
        _py = os.path.join(_ld, "python")
        if os.path.isdir(_py):
            sys.path.insert(0, _py)
            # AWS exposes <layer>/python/lib/python<ver>/site-packages as a
            # site directory (processes .pth files / namespace packages), where
            # `pip install -t` dependency layers land (#888).
            _lib = os.path.join(_py, "lib")
            if os.path.isdir(_lib):
                import site as _site
                for _v in os.listdir(_lib):
                    _sp = os.path.join(_lib, _v, "site-packages")
                    if os.path.isdir(_sp):
                        _site.addsitedir(_sp)
        sys.path.insert(0, _ld)
    # After sys.path is complete (so a function that bundles its own botocore
    # gets that one) and before the handler module builds any client.
    _ms_install_invoke_depth()
    try:
        mod = importlib.import_module(module_name)
        handler_fn = getattr(mod, handler_name)
        if init.get("snapstart"):
            # SnapStart runtime hooks (snapshot-restore-py, bundled in AWS's
            # python3.12+ runtimes). Each environment of a published SnapStart
            # version runs one snapshot/restore cycle: before-snapshot hooks
            # in reverse registration order, then after-restore hooks in
            # registration order, before the first invocation. A hook that
            # raises fails the init, which fails the publish, as on AWS.
            try:
                from snapshot_restore_py import (
                    get_after_restore,
                    get_before_snapshot,
                )
            except ImportError:
                pass
            else:
                for _fn, _args, _kwargs in reversed(get_before_snapshot()):
                    _fn(*_args, **_kwargs)
                for _fn, _args, _kwargs in get_after_restore():
                    _fn(*_args, **_kwargs)
        _real_stdout.write(json.dumps({"status": "ready", "cold": True}) + "\\n")
        _real_stdout.flush()
    except Exception as e:
        _real_stdout.write(json.dumps({"status": "error", "error": str(e)}) + "\\n")
        _real_stdout.flush()
        return

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        event = json.loads(line)
        # X-Ray active tracing: ministack injects the per-invocation trace
        # header into the event; pop it into os.environ so the AWS X-Ray SDK
        # can read _X_AMZN_TRACE_ID on import.
        _xray_tid = event.pop("_x_amzn_trace_id", None)
        if _xray_tid:
            os.environ["_X_AMZN_TRACE_ID"] = _xray_tid
        elif "_X_AMZN_TRACE_ID" in os.environ:
            del os.environ["_X_AMZN_TRACE_ID"]
        # Recursive-loop depth: same per-invocation channel as the trace
        # header, read by the before-call handler when this function calls out.
        _ms_depth = event.pop("__DEPTH_EVENT_KEY__", None)
        if _ms_depth is not None:
            os.environ["__DEPTH_ENV__"] = str(_ms_depth)
        elif "__DEPTH_ENV__" in os.environ:
            del os.environ["__DEPTH_ENV__"]
        _function_name = init.get("function_name", "")
        _deadline = time.time() + float(os.environ.get("_LAMBDA_TIMEOUT", "3"))
        context = type("Context", (), {
            "function_name": init.get("function_name", ""),
            "function_version": os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST"),
            "memory_limit_in_mb": init.get("memory", 128),
            "invoked_function_arn": init.get("arn", ""),
            "aws_request_id": event.pop("_request_id", ""),
            "log_group_name": os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME", "/aws/lambda/" + _function_name),
            "log_stream_name": os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", ""),
            "identity": None,
            "client_context": None,
            "get_remaining_time_in_millis": lambda self: max(0, int((_deadline - time.time()) * 1000)),
        })()
        try:
            result = handler_fn(event, context)
            _real_stdout.write(json.dumps({"status": "ok", "result": result}) + "\\n")
        except Exception as e:
            _real_stdout.write(json.dumps({"status": "error", "error": str(e), "trace": traceback.format_exc()}) + "\\n")
        _real_stdout.flush()

run()
''')

# ---------------------------------------------------------------------------
# Node.js worker script (runs inside a persistent subprocess)
# ---------------------------------------------------------------------------

_NODEJS_WORKER_SCRIPT = _sub_depth_tokens(r'''
const readline = require("readline");
const path = require("path");
const http = require("http");
const https = require("https");
const url = require("url");
const Module = require("module");

// Redirect stdout to stderr so stdout stays clean for JSON-line protocol
const fs = require("fs");
const _realStdoutWrite = process.stdout.write.bind(process.stdout);
const _stderrWrite = process.stderr.write.bind(process.stderr);
process.stdout.write = function(chunk, encoding, callback) {
  return _stderrWrite(chunk, encoding, callback);
};
const _stdoutFd = process.stdout.fd;
const _realWriteSync = fs.writeSync.bind(fs);
const _realWrite = fs.write.bind(fs);
function _isStdoutFd(fd) {
  return fd === 1 || fd === _stdoutFd;
}
fs.writeSync = function(fd, ...args) {
  if (_isStdoutFd(fd)) fd = 2;
  return _realWriteSync(fd, ...args);
};
fs.write = function(fd, ...args) {
  if (_isStdoutFd(fd)) fd = 2;
  return _realWrite(fd, ...args);
};

// Synthetic AWS SDK v3 stubs — real AWS Lambda (Node.js 18+) ships these
// built-in, but the host runtime does not.  Try the real package first so
// a Lambda Layer with the actual SDK takes precedence; fall back to a stub
// that routes through AWS_ENDPOINT_URL (Ministack).
(function _installAwsSdkV3Stubs() {
  // ── Lambda stub (REST-based, not JSON-RPC) ─────────────────────────────
  function _lambdaInvoke(params) {
    const ep = new URL(process.env.AWS_ENDPOINT_URL || "http://127.0.0.1:4566");
    const fn = encodeURIComponent(params.FunctionName || "");
    const qs = params.Qualifier
      ? "?Qualifier=" + encodeURIComponent(params.Qualifier)
      : "";
    const body = params.Payload instanceof Uint8Array
      ? Buffer.from(params.Payload)
      : (params.Payload || "");
    const headers = { "Content-Type": "application/json" };
    // Carry the caller's recursive-loop depth so ministack can attribute this
    // nested Invoke to it (the counterpart of the botocore before-call handler
    // on the Python side).
    if (process.env.__DEPTH_ENV__) {
      headers["__DEPTH_HEADER__"] = process.env.__DEPTH_ENV__;
    }
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: ep.hostname,
          port: parseInt(ep.port || "4566", 10),
          method: "POST",
          path: "/2015-03-31/functions/" + fn + "/invocations" + qs,
          headers: headers,
        },
        (res) => {
          const chunks = [];
          res.on("data", (c) => chunks.push(c));
          res.on("end", () =>
            resolve({
              StatusCode: res.statusCode,
              Payload: Buffer.concat(chunks),
              FunctionError: res.headers["x-amz-function-error"],
            })
          );
        }
      );
      req.on("error", reject);
      if (body) req.write(body);
      req.end();
    });
  }

  function _makeLambdaClientModule() {
    class Lambda {
      constructor(_cfg) {}
      invoke(params) { return _lambdaInvoke(params); }
    }
    class LambdaClient {
      constructor(_cfg) {}
      send(cmd) { return cmd._run(); }
    }
    class InvokeCommand {
      constructor(params) { this._p = params; }
      _run() { return _lambdaInvoke(this._p); }
    }
    async function waitUntilFunctionActiveV2() { return { state: "SUCCESS" }; }
    return { Lambda, LambdaClient, InvokeCommand, waitUntilFunctionActiveV2 };
  }

  // ── OpenSearch stub (REST-JSON, not JSON-RPC) ─────────────────────────
  function _openSearchRequest(opName, params) {
    const ep = new URL(process.env.AWS_ENDPOINT_URL || "http://127.0.0.1:4566");
    const input = Object.assign({}, params || {});
    const domainName = input.DomainName;
    const encodedName = encodeURIComponent(domainName || "");
    let method = "GET";
    let requestPath;
    let body;

    switch (opName) {
      case "CreateDomain":
        method = "POST";
        requestPath = "/2021-01-01/opensearch/domain";
        body = input;
        break;
      case "DescribeDomain":
        requestPath = "/2021-01-01/opensearch/domain/" + encodedName;
        break;
      case "DescribeDomains":
        method = "POST";
        requestPath = "/2021-01-01/opensearch/domain-info";
        body = input;
        break;
      case "DeleteDomain":
        method = "DELETE";
        requestPath = "/2021-01-01/opensearch/domain/" + encodedName;
        break;
      case "ListDomainNames":
        requestPath = "/2021-01-01/opensearch/domain";
        if (input.EngineType) {
          requestPath += "?engineType=" + encodeURIComponent(input.EngineType);
        }
        break;
      case "UpdateDomainConfig":
        method = "POST";
        requestPath = "/2021-01-01/opensearch/domain/" + encodedName + "/config";
        delete input.DomainName;
        body = input;
        break;
      case "DescribeDomainConfig":
        requestPath = "/2021-01-01/opensearch/domain/" + encodedName + "/config";
        break;
      case "DescribeDomainChangeProgress":
        requestPath = "/2021-01-01/opensearch/domain/" + encodedName + "/progress";
        if (input.ChangeId) {
          requestPath += "?changeId=" + encodeURIComponent(input.ChangeId);
        }
        break;
      case "ListVersions": {
        requestPath = "/2021-01-01/opensearch/versions";
        const query = new URLSearchParams();
        if (input.MaxResults !== undefined) query.set("maxResults", input.MaxResults);
        if (input.NextToken) query.set("nextToken", input.NextToken);
        const suffix = query.toString();
        if (suffix) requestPath += "?" + suffix;
        break;
      }
      case "GetCompatibleVersions":
        requestPath = "/2021-01-01/opensearch/compatibleVersions";
        if (input.DomainName) {
          requestPath += "?domainName=" + encodeURIComponent(input.DomainName);
        }
        break;
      case "AddTags":
        method = "POST";
        requestPath = "/2021-01-01/tags";
        body = input;
        break;
      case "ListTags":
        requestPath = "/2021-01-01/tags?arn=" + encodeURIComponent(input.ARN || "");
        break;
      case "RemoveTags":
        method = "POST";
        requestPath = "/2021-01-01/tags-removal";
        body = input;
        break;
      default:
        return Promise.reject(new Error("Unsupported OpenSearch operation: " + opName));
    }

    const encodedBody = body === undefined ? "" : JSON.stringify(body);
    return new Promise((resolve, reject) => {
      // REST-JSON requests do not carry X-Amz-Target. The lightweight shim
      // does not cryptographically sign them, but supplies the credential
      // scope MiniStack's router uses to select OpenSearch. Let Node set Host
      // from the endpoint: a synthetic ``opensearch.localhost`` host is
      // indistinguishable from a virtual-hosted S3 bucket at the edge.
      const headers = {
        "Accept": "application/json",
      };
      const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-1";
      const accessKey = process.env.AWS_ACCESS_KEY_ID || "test";
      headers["Authorization"] =
        "AWS4-HMAC-SHA256 Credential=" + accessKey + "/19700101/" + region
        + "/es/aws4_request, SignedHeaders=host, Signature=ministack";
      if (encodedBody) {
        headers["Content-Type"] = "application/json";
        headers["Content-Length"] = Buffer.byteLength(encodedBody);
      }
      const req = http.request(
        {
          hostname: ep.hostname,
          port: parseInt(ep.port || "4566", 10),
          method: method,
          path: requestPath,
          headers: headers,
        },
        (res) => {
          const chunks = [];
          res.on("data", (c) => chunks.push(c));
          res.on("end", () => {
            const text = Buffer.concat(chunks).toString();
            let parsed;
            try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = {}; }
            if (res.statusCode >= 400) {
              const type = parsed.__type || parsed.Code || "OpenSearchServiceError";
              const code = String(type).split("#").pop();
              const err = new Error(
                parsed.Message || parsed.message || text || "OpenSearch service error"
              );
              err.statusCode = res.statusCode;
              err.code = code;
              err.name = code;
              reject(err);
            } else {
              resolve(parsed);
            }
          });
        }
      );
      req.on("error", reject);
      if (encodedBody) req.write(encodedBody);
      req.end();
    });
  }

  function _makeOpenSearchClientModule() {
    class OpenSearchClient {
      constructor(cfg) {
        const clientConfig = cfg || {};
        this.config = {
          apiVersion: clientConfig.apiVersion,
          region: async () => clientConfig.region
            || process.env.AWS_REGION
            || process.env.AWS_DEFAULT_REGION
            || "us-east-1",
        };
      }
      send(cmd) { return cmd._run(); }
    }

    class OpenSearch {
      constructor(_cfg) {}
    }

    const operations = [
      "CreateDomain",
      "DescribeDomain",
      "DescribeDomains",
      "DeleteDomain",
      "ListDomainNames",
      "UpdateDomainConfig",
      "DescribeDomainConfig",
      "DescribeDomainChangeProgress",
      "ListVersions",
      "GetCompatibleVersions",
      "AddTags",
      "ListTags",
      "RemoveTags",
    ];
    const exports = { OpenSearch, OpenSearchClient };
    for (const opName of operations) {
      OpenSearch.prototype[opName[0].toLowerCase() + opName.slice(1)] = function(params) {
        return _openSearchRequest(opName, params);
      };
      exports[opName + "Command"] = class {
        constructor(params) { this._params = params; }
        _run() { return _openSearchRequest(opName, this._params); }
      };
    }
    return exports;
  }

  // ── Generic JSON-RPC stub (covers SSM, SFN, STS, CloudWatch, Logs, etc.) ─
  // Most AWS SDK v3 packages use awsJson1.x: POST / with X-Amz-Target header.
  // Ministack's router maps target prefixes to service modules.
  const _JSON_RPC_TARGETS = {
    // JSON-RPC (awsJson1.x) services — keyed by full module specifier.
    // Target prefixes match Ministack's router.py SERVICE_PATTERNS target_prefixes.
    "@aws-sdk/client-ssm":                         "AmazonSSM",
    "@aws-sdk/client-sfn":                         "AWSStepFunctions",
    // sts, sns: query protocol — @aws-sdk/client-{sts,sns} sends Action= form-encoded POST
    // cloudwatch: smithy-rpc-v2-cbor — @aws-sdk/client-cloudwatch sends path-based requests
    // All three are handled by Ministack's native query/path routing when the real SDK is present
    "@aws-sdk/client-cloudwatch-logs":             "Logs_20140328",
    "@aws-sdk/client-logs":                        "Logs_20140328",
    "@aws-sdk/client-secrets-manager":             "secretsmanager",
    "@aws-sdk/client-events":                      "AmazonEventBridge",
    "@aws-sdk/client-eventbridge":                 "AmazonEventBridge",
    "@aws-sdk/client-kinesis":                     "Kinesis_20131202",
    "@aws-sdk/client-ecs":                         "AmazonEC2ContainerServiceV20141113",
    "@aws-sdk/client-dynamodb":                    "DynamoDB_20120810",
    "@aws-sdk/client-dynamodb-streams":            "DynamoDBStreams_20120810",
    "@aws-sdk/client-sqs":                         "AmazonSQS",
    "@aws-sdk/client-glue":                        "AWSGlue",
    "@aws-sdk/client-athena":                      "AmazonAthena",
    "@aws-sdk/client-firehose":                    "Firehose_20150804",
    "@aws-sdk/client-cognito-identity-provider":   "AWSCognitoIdentityProviderService",
    "@aws-sdk/client-cognito-identity":            "AWSCognitoIdentityService",
    "@aws-sdk/client-emr":                         "ElasticMapReduce",
    "@aws-sdk/client-ecr":                         "AmazonEC2ContainerRegistry_V20150921",
    "@aws-sdk/client-acm":                         "CertificateManager",
    "@aws-sdk/client-wafv2":                       "AWSWAF_20190729",
    "@aws-sdk/client-waf":                         "AWSWAF_20150824",
    "@aws-sdk/client-waf-regional":                "AWSWAF_Regional_20161128",
    "@aws-sdk/client-organizations":               "AWSOrganizationsV20161128",
    "@aws-sdk/client-kms":                         "TrentService",
    "@aws-sdk/client-codebuild":                   "CodeBuild_20161006",
    "@aws-sdk/client-transfer":                    "TransferService",
    "@aws-sdk/client-servicediscovery":            "Route53AutoNaming_v20170314",
    "@aws-sdk/client-resource-groups-tagging-api": "ResourceGroupsTaggingAPI_20170126",
    "@aws-sdk/client-cloudtrail":                  "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101",
    "@aws-sdk/client-translate":                   "AWSShineFrontendService_20170701",
  };

  function _jsonRpcRequest(targetPrefix, opName, params) {
    const ep = new URL(process.env.AWS_ENDPOINT_URL || "http://127.0.0.1:4566");
    const body = JSON.stringify(params || {});
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: ep.hostname,
          port: parseInt(ep.port || "4566", 10),
          method: "POST",
          path: "/",
          headers: {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": targetPrefix + "." + opName,
            "Content-Length": Buffer.byteLength(body),
          },
        },
        (res) => {
          const chunks = [];
          res.on("data", (c) => chunks.push(c));
          res.on("end", () => {
            const text = Buffer.concat(chunks).toString();
            let parsed;
            try { parsed = JSON.parse(text); } catch (_) { parsed = {}; }
            if (res.statusCode >= 400) {
              const err = new Error(
                parsed.Message || parsed.message || text || "Service error"
              );
              err.statusCode = res.statusCode;
              err.code = parsed.__type || parsed.Code || "ServiceError";
              err.name = err.code;
              reject(err);
            } else {
              resolve(parsed);
            }
          });
        }
      );
      req.on("error", reject);
      req.write(body);
      req.end();
    });
  }

  function _makeGenericJsonServiceModule(targetPrefix) {
    // Command class factory: new PutParameterCommand(params) → has _run()
    function _cmdClass(opName) {
      return class {
        constructor(params) { this._params = params; }
        _run() { return _jsonRpcRequest(targetPrefix, opName, this._params); }
      };
    }

    // v3-style client: new SSMClient({}).send(new PutParameterCommand({}))
    class GenericClient {
      constructor(_cfg) {}
      send(cmd) { return cmd._run(); }
    }

    // Bare client: new SSM({}).putParameter(params)  (any method → operation)
    const BareClient = new Proxy(function() {}, {
      construct(_target, _args) {
        return new Proxy({}, {
          get(_, prop) {
            if (typeof prop !== "string") return undefined;
            const opName = prop[0].toUpperCase() + prop.slice(1);
            return (params) => _jsonRpcRequest(targetPrefix, opName, params);
          },
        });
      },
    });

    // Module proxy: any named export resolves on demand.
    //   *Client  → GenericClient (v3 style)
    //   *Command → command class  (strip "Command" suffix → op name)
    //   other uppercase name → BareClient (bare/convenience style)
    return new Proxy(
      {},
      {
        get(_, prop) {
          if (typeof prop !== "string") return undefined;
          if (prop.endsWith("Client")) return GenericClient;
          if (prop.endsWith("Command")) {
            return _cmdClass(prop.slice(0, -7));
          }
          if (/^[A-Z]/.test(prop)) return BareClient;
          return undefined;
        },
      }
    );
  }

  // ── require() intercept ────────────────────────────────────────────────
  const _SPECIFIC_STUBS = {
    "@aws-sdk/client-lambda": _makeLambdaClientModule(),
    "@aws-sdk/client-opensearch": _makeOpenSearchClientModule(),
  };
  const _SDK_CLIENT_RE = /^@aws-sdk\/client-(.+)$/;

  const _origRequire = Module.prototype.require;
  Module.prototype.require = function (id) {
    // 1. Specific stubs (Lambda uses REST, not JSON-RPC)
    const specific = _SPECIFIC_STUBS[id];
    if (specific) {
      try { return _origRequire.apply(this, arguments); } catch (_) {}
      return specific;
    }
    // 2. Generic JSON-RPC stubs for known @aws-sdk/client-* packages
    if (_SDK_CLIENT_RE.test(id)) {
      let requireError;
      try {
        return _origRequire.apply(this, arguments);
      } catch (err) {
        requireError = err;
      }
      const prefix = _JSON_RPC_TARGETS[id];
      if (prefix) return _makeGenericJsonServiceModule(prefix);
      if (
        requireError.code !== "MODULE_NOT_FOUND"
        || !requireError.message.includes("'" + id + "'")
      ) {
        throw requireError;
      }
      throw new Error(
        "MiniStack local executor has no stub for '" + id
        + "'; bundle the module with your function or use LAMBDA_EXECUTOR=docker."
      );
    }
    return _origRequire.apply(this, arguments);
  };
}());

function patchAwsSdk() {
  const endpoint = process.env.AWS_ENDPOINT_URL
    || process.env.LOCALSTACK_ENDPOINT
    || process.env.MINISTACK_ENDPOINT;
  if (!endpoint) return;

  const parsed = url.parse(endpoint);
  const msHost = parsed.hostname;
  const msPort = parseInt(parsed.port || "4566", 10);

  // Patch aws-sdk v2 global config
  try {
    const AWS = require("aws-sdk");
    AWS.config.update({
      endpoint: endpoint,
      region: process.env.AWS_REGION || process.env.FBT_AWS_REGION || "us-east-1",
      s3ForcePathStyle: true,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "test",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "test",
    });
    const origHandle = AWS.NodeHttpClient.prototype.handleRequest;
    AWS.NodeHttpClient.prototype.handleRequest = function(req, opts, cb, errCb) {
      if (req.endpoint && req.endpoint.protocol === "http:") {
        if (opts && opts.agent instanceof https.Agent) {
          opts = Object.assign({}, opts, { agent: new http.Agent({ keepAlive: true }) });
        }
      }
      return origHandle.call(this, req, opts, cb, errCb);
    };
  } catch (_) {}

  // Patch https.request for bundled SDK
  const origHttpsReq = https.request;
  https.request = function(options, callback) {
    if (typeof options === "string") options = url.parse(options);
    else if (options instanceof url.URL) options = url.parse(options.toString());
    else options = Object.assign({}, options);

    const host = options.hostname || options.host || "";
    if (host.endsWith(".amazonaws.com") || host.endsWith(".amazonaws.com.cn")) {
      options.protocol = "http:";
      options.hostname = msHost;
      options.host = msHost + ":" + msPort;
      options.port = msPort;
      options.path = options.path || "/";
      if (options.agent instanceof https.Agent) {
        options.agent = new http.Agent({ keepAlive: true });
      } else if (options.agent === undefined) {
        options.agent = new http.Agent({ keepAlive: true });
      }
      delete options._defaultAgent;
      return http.request(options, callback);
    }

    // Downgrade HTTPS to HTTP for localhost — CDK Provider Framework's
    // cfn-response.js calls https.request unconditionally for the ResponseURL
    // PUT, and also drops the port when constructing options.  Intercept here
    // so the PUT reaches Ministack's HTTP server on msPort, not port 443.
    if (host === "127.0.0.1" || host === "localhost" || host === msHost) {
      options.protocol = "http:";
      options.port = options.port || msPort;
      options.host = host + ":" + options.port;
      options.agent = new http.Agent({ keepAlive: true });
      delete options._defaultAgent;
      return http.request(options, callback);
    }

    // Downgrade ES HTTPS to HTTP for local Elasticsearch
    var esHost = process.env.ES_ENDPOINT ? process.env.ES_ENDPOINT.split(":")[0] : null;
    if (esHost && (host === esHost || host.startsWith(esHost + ":"))) {
      var esPort = process.env.ES_ENDPOINT ? parseInt(process.env.ES_ENDPOINT.split(":")[1] || "9200", 10) : 9200;
      options.protocol = "http:";
      options.hostname = esHost;
      options.host = esHost + ":" + esPort;
      options.port = esPort;
      options.rejectUnauthorized = false;
      options.agent = new http.Agent({ keepAlive: true });
      delete options._defaultAgent;
      return http.request(options, callback);
    }

    return origHttpsReq.call(https, options, callback);
  };
  https.get = function(options, callback) {
    var req = https.request(options, callback);
    req.end();
    return req;
  };
}

let handlerFn = null;

const rl = readline.createInterface({ input: process.stdin, terminal: false });
let lineNum = 0;

rl.on("line", async (line) => {
  lineNum++;
  try {
    const msg = JSON.parse(line);

    // First line is the init payload
    if (lineNum === 1) {
      const { code_dir, module: modPath, handler: handlerName, env } = msg;
      Object.assign(process.env, env || {});
      process.env.LAMBDA_TASK_ROOT = code_dir;
      process.env.AWS_LAMBDA_FUNCTION_NAME = msg.function_name || process.env.AWS_LAMBDA_FUNCTION_NAME || "";
      process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE = String(msg.memory || process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || "128");
      process.env._LAMBDA_FUNCTION_ARN = msg.arn || process.env._LAMBDA_FUNCTION_ARN || "";
      patchAwsSdk();
      try {
        const fullPath = path.resolve(code_dir, modPath);
        let mod;
        let resolvedPath;
        try {
          resolvedPath = require.resolve(fullPath);
        } catch (resolveErr) {
          if (resolveErr.code === "MODULE_NOT_FOUND") {
            const fs = require("fs");
            const mjsPath = fullPath + ".mjs";
            if (fs.existsSync(mjsPath)) {
              resolvedPath = mjsPath;
            } else {
              throw resolveErr;
            }
          } else {
            throw resolveErr;
          }
        }
        try {
          mod = require(resolvedPath);
        } catch (reqErr) {
          if (reqErr.code === "ERR_REQUIRE_ESM" || reqErr.code === "ERR_REQUIRE_ASYNC_MODULE") {
            const { pathToFileURL } = require("url");
            mod = await import(pathToFileURL(resolvedPath).href);
          } else {
            throw reqErr;
          }
        }
        handlerFn = mod[handlerName] || (mod.default && mod.default[handlerName]) || mod.default;
        if (typeof handlerFn !== "function") {
          _realStdoutWrite(JSON.stringify({
            status: "error",
            error: `Handler ${handlerName} is not a function in ${modPath}`
          }) + "\n");
          return;
        }
        _realStdoutWrite(JSON.stringify({ status: "ready", cold: true }) + "\n");
      } catch (e) {
        _realStdoutWrite(JSON.stringify({
          status: "error", error: e.message
        }) + "\n");
      }
      return;
    }

    // Subsequent lines are event invocations
    const event = msg;
    const context = {
      functionName: event._function_name || "",
      memoryLimitInMB: event._memory || "128",
      invokedFunctionArn: event._arn || "",
      awsRequestId: event._request_id || "",
      getRemainingTimeInMillis: () => 300000,
      done: () => {},
      succeed: () => {},
      fail: () => {},
    };
    // X-Ray active tracing: ministack injects the per-invocation trace
    // header into the event; promote it to process.env so the AWS X-Ray SDK
    // can read _X_AMZN_TRACE_ID on require().
    if (event._x_amzn_trace_id) {
      process.env._X_AMZN_TRACE_ID = event._x_amzn_trace_id;
    } else if ("_X_AMZN_TRACE_ID" in process.env) {
      delete process.env._X_AMZN_TRACE_ID;
    }
    // Recursive-loop depth rides the same per-invocation channel, and the
    // bundled Lambda stub reads it back off process.env when the handler
    // invokes another function.
    if (event.__DEPTH_EVENT_KEY__ !== undefined) {
      process.env.__DEPTH_ENV__ = String(event.__DEPTH_EVENT_KEY__);
    } else if ("__DEPTH_ENV__" in process.env) {
      delete process.env.__DEPTH_ENV__;
    }
    delete event.__DEPTH_EVENT_KEY__;
    delete event._x_amzn_trace_id;
    delete event._request_id;
    delete event._function_name;
    delete event._memory;
    delete event._arn;

    try {
      let settled = false;
      const settle = (err, res) => {
        if (settled) return;
        settled = true;
        if (err) {
          _realStdoutWrite(JSON.stringify({
            status: "error", error: String(err.message || err), trace: err.stack || ""
          }) + "\n");
        } else {
          _realStdoutWrite(JSON.stringify({ status: "ok", result: res }) + "\n");
        }
      };
      const callback = (err, res) => settle(err, res);
      context.done = (err, res) => settle(err, res);
      context.succeed = (res) => settle(null, res);
      context.fail = (err) => settle(err || new Error("fail"));

      const result = handlerFn(event, context, callback);
      if (result && typeof result.then === "function") {
        // Async/Promise handler
        result.then(res => settle(null, res), err => settle(err));
      } else if (handlerFn.length < 3 && result !== undefined) {
        // Sync handler that doesn't accept callback and returned a value
        settle(null, result);
      }
      // If handler accepts callback (arity >= 3) or returned undefined,
      // we wait for callback/context.done/context.succeed/context.fail
    } catch (e) {
      _realStdoutWrite(JSON.stringify({
        status: "error", error: e.message, trace: e.stack
      }) + "\n");
    }
  } catch (e) {
    _realStdoutWrite(JSON.stringify({
      status: "error", error: "JSON parse error: " + e.message
    }) + "\n");
  }
});
''')


def _detect_runtime_binary(runtime: str) -> tuple[str, str]:
    """Return (binary, worker_script_content) for the given Lambda runtime string."""
    if runtime.startswith("python"):
        return sys.executable, _PYTHON_WORKER_SCRIPT
    if runtime.startswith("nodejs"):
        return "node", _NODEJS_WORKER_SCRIPT
    return "", ""


def _worker_script_extension(runtime: str) -> str:
    if runtime.startswith("python"):
        return ".py"
    if runtime.startswith("nodejs"):
        return ".js"
    return ".py"


class Worker:
    def __init__(self, func_name: str, config: dict, code_zip: bytes):
        self.func_name = func_name
        self.config = config
        self.code_zip = code_zip
        self._proc = None
        self._tmpdir = None
        self._lock = threading.Lock()
        # Leased out to an in-flight invocation. Guarded by the module ``_lock``
        # (see acquire_worker/release_worker), not by ``self._lock``.
        self.in_use = False
        self.last_used = time.time()
        self._cold = True
        self._start_time = None
        self._stderr_queue: queue.Queue = queue.Queue()
        self._stderr_thread: threading.Thread | None = None

    def _read_stderr(self):
        """Background daemon thread: continuously drain stderr into queue."""
        try:
            for line in self._proc.stderr:
                self._stderr_queue.put(line.rstrip("\n"))
        except Exception:
            pass

    def _spawn(self):
        """Extract zip and start worker process."""
        # Clean up any previous tmpdir before creating a new one (respawn scenario)
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir = tempfile.mkdtemp(prefix=f"ministack-lambda-{self.func_name}-")
        runtime = self.config.get("Runtime", "python3.12")
        binary, worker_script = _detect_runtime_binary(runtime)
        if not binary:
            raise RuntimeError(f"Unsupported runtime: {runtime}")

        ext = _worker_script_extension(runtime)
        worker_path = os.path.join(self._tmpdir, f"_worker{ext}")
        with open(worker_path, "w") as f:
            f.write(worker_script)

        code_dir = os.path.join(self._tmpdir, "code")
        os.makedirs(code_dir)
        with open(os.path.join(self._tmpdir, "code.zip"), "wb") as f:
            f.write(self.code_zip)
        with zipfile.ZipFile(os.path.join(self._tmpdir, "code.zip")) as zf:
            zf.extractall(code_dir)

        # Extract Lambda Layers and build search paths for the worker process.
        # This mirrors the layer handling in lambda_svc._execute_function_local().
        layers_dirs: list[str] = []
        layer_refs = self.config.get("Layers", [])
        if layer_refs:
            from ministack.services.lambda_svc import _resolve_layer_zip
        for layer_ref in layer_refs:
            layer_arn = layer_ref if isinstance(layer_ref, str) else layer_ref.get("Arn", "")
            if not layer_arn:
                continue
            try:
                layer_data = _resolve_layer_zip(layer_arn)
                if layer_data:
                    layer_dir = os.path.join(self._tmpdir, f"layer_{len(layers_dirs)}")
                    os.makedirs(layer_dir)
                    lzip = os.path.join(self._tmpdir, f"layer_{len(layers_dirs)}.zip")
                    try:
                        with open(lzip, "wb") as lf:
                            lf.write(layer_data)
                        with zipfile.ZipFile(lzip) as lzf:
                            # Validate paths to prevent zip-slip attacks
                            for member in lzf.namelist():
                                resolved = os.path.realpath(os.path.join(layer_dir, member))
                                if not resolved.startswith(os.path.realpath(layer_dir) + os.sep) and resolved != os.path.realpath(layer_dir):
                                    raise RuntimeError(f"Zip entry escapes target dir: {member}")
                            lzf.extractall(layer_dir)
                    except (OSError, zipfile.BadZipFile, zipfile.LargeFileError) as e:
                        logger.error("Failed to extract layer %s", layer_arn, exc_info=True)
                        raise RuntimeError(f"Failed to extract layer {layer_arn}") from e
                    layers_dirs.append(layer_dir)
            except RuntimeError:
                raise
            except Exception as e:
                logger.error("Unexpected error resolving layer %s: %s", layer_arn, e)
                raise RuntimeError(f"Failed to resolve layer {layer_arn}") from e

        # Symlink layer node_modules packages into the code directory so that
        # Node.js ESM import() can resolve them via ancestor-tree lookup.
        # ESM does not use NODE_PATH, so packages must be physically reachable
        # from the handler file's directory tree.
        if layers_dirs and runtime.startswith("nodejs"):
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

        handler = self.config.get("Handler", "index.handler")
        module_name, handler_name = handler.rsplit(".", 1)
        # AWS Python Lambda accepts both dot (``pkg.mod.fn``) and slash
        # (``pkg/mod.fn``) in nested handler paths; ``__import__`` only
        # takes dot. Other runtimes (Node.js, etc.) keep the raw string
        # because they don't use Python module resolution.
        if runtime.startswith("python"):
            module_name = module_name.replace("/", ".")
        env_vars = {
            key: value
            for key, value in self.config.get("Environment", {}).get("Variables", {}).items()
            if key not in _RESERVED_RUNTIME_ENV_VARS
        }
        spawn_env = {**os.environ, **env_vars}
        # Inject standard Lambda runtime env vars to match the Docker and
        # provided-runtime execution paths in lambda_svc.py.  Real AWS
        # Lambda always injects these; the warm-worker path was missing them.
        # Per AWS docs:
        #   https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html
        from ministack.core.responses import new_uuid
        account_id, region = _account_region_from_function_config(self.config)
        spawn_env["AWS_REGION"] = region
        spawn_env["AWS_DEFAULT_REGION"] = region
        spawn_env.update(execution_credentials(self.config))
        # AWS_ENDPOINT_URL precedence matches real AWS: function
        # Environment.Variables wins, then host env, then the internal
        # default that points at this MiniStack instance.  Real AWS Lambda
        # does not inject AWS_ENDPOINT_URL — it is an SDK/testing convention
        # — so function-level values must be respected.  spawn_env was built
        # as {**os.environ, **env_vars}, so any function-level value is
        # already present; setdefault only fills in the default when neither
        # the function nor the host set one.
        port = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))
        spawn_env.setdefault("AWS_ENDPOINT_URL", f"http://127.0.0.1:{port}")
        if "LOCALSTACK_HOSTNAME" in os.environ:
            spawn_env["LOCALSTACK_HOSTNAME"] = os.environ["LOCALSTACK_HOSTNAME"]
        spawn_env.setdefault("LAMBDA_TASK_ROOT", code_dir)
        spawn_env.setdefault("AWS_LAMBDA_FUNCTION_NAME", self.config.get("FunctionName", ""))
        spawn_env.setdefault("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", str(self.config.get("MemorySize", 128)))
        spawn_env.setdefault("AWS_LAMBDA_FUNCTION_VERSION", self.config.get("Version", "$LATEST"))
        spawn_env.setdefault("AWS_LAMBDA_LOG_STREAM_NAME", new_uuid())
        spawn_env.setdefault("_LAMBDA_FUNCTION_ARN", self.config.get("FunctionArn", ""))
        spawn_env.setdefault("_LAMBDA_TIMEOUT", str(self.config.get("Timeout", 30)))

        # Set layer paths so worker runtimes can find packages from extracted layers.
        # _LAMBDA_LAYERS_DIRS is consumed by the Python worker; Node.js layer resolution
        # is handled via NODE_PATH populated from each layer's nodejs paths below.
        if layers_dirs:
            spawn_env["_LAMBDA_LAYERS_DIRS"] = os.pathsep.join(layers_dirs)
            # NODE_PATH is used by the CJS require() resolver in Node.js workers.
            # ESM import() does not use NODE_PATH — layer packages are instead
            # symlinked into code/node_modules/ above for ancestor-tree resolution.
            node_paths = []
            for ld in layers_dirs:
                nm = os.path.join(ld, "nodejs", "node_modules")
                if os.path.isdir(nm):
                    node_paths.append(nm)
                nj = os.path.join(ld, "nodejs")
                if os.path.isdir(nj):
                    node_paths.append(nj)
            if node_paths:
                existing = spawn_env.get("NODE_PATH")
                if existing:
                    spawn_env["NODE_PATH"] = os.pathsep.join(node_paths + [existing])
                else:
                    spawn_env["NODE_PATH"] = os.pathsep.join(node_paths)

        self._proc = subprocess.Popen(
            [binary, worker_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=spawn_env,
        )

        self._stderr_queue = queue.Queue()
        self._stderr_thread = threading.Thread(
            target=self._read_stderr, daemon=True, name=f"stderr-{self.func_name}"
        )
        self._stderr_thread.start()

        init = {
            "code_dir": code_dir,
            "module": module_name,
            "handler": handler_name,
            "env": env_vars,
            "function_name": self.config.get("FunctionName", ""),
            "memory": self.config.get("MemorySize", 128),
            "arn": self.config.get("FunctionArn", ""),
            # A published SnapStart version runs its snapshot/restore runtime
            # hooks during init (see the worker script). $LATEST never does —
            # it has no snapshot on AWS either.
            "snapstart": (self.config.get("SnapStart") or {}).get(
                "OptimizationStatus") == "On",
        }
        self._proc.stdin.write(json.dumps(init) + "\n")
        self._proc.stdin.flush()

        # Read init response, skipping non-JSON lines (stray console output from modules)
        response = None
        for _ in range(200):
            response_line = self._proc.stdout.readline()
            if not response_line:
                stderr_out = ""
                try:
                    stderr_out = self._proc.stderr.read(4096)
                except Exception:
                    pass
                raise RuntimeError(f"Worker process exited immediately. stderr: {stderr_out}")
            response_line = response_line.strip()
            if not response_line or not response_line.startswith("{"):
                continue
            try:
                response = json.loads(response_line)
                break
            except json.JSONDecodeError:
                continue
        if response is None:
            raise RuntimeError("No JSON init response from worker")
        if response.get("status") != "ready":
            raise RuntimeError(f"Worker init failed: {response.get('error')}")

        self._start_time = time.time()
        logger.info("Lambda worker spawned for %s (%s, cold start)", self.func_name, runtime)

    def _drain_stderr(self) -> str:
        """Collect all currently available stderr lines (non-blocking)."""
        lines = []
        try:
            while True:
                lines.append(self._stderr_queue.get_nowait())
        except queue.Empty:
            pass
        return "\n".join(lines)

    def _drain_stderr_bounded(
        self,
        first_line_wait: float = 0.050,
        idle_confirm: float = 0.005,
        hard_cap: float = 0.250,
    ) -> str:
        """Drain stderr with bounded waits — replaces a blanket ``time.sleep(0.05)``
        that penalised every warm-pool invocation regardless of whether the
        handler emitted any log output.

        Three exit conditions, in order of likelihood:
          1. **Quiescence after a line**: after the first line arrives, exit
             once the queue has been empty for ``idle_confirm`` seconds
             (default 5ms). Typical completion is 1–10ms per invocation.
          2. **Nothing emitted**: if no line has arrived within
             ``first_line_wait`` (default 50ms), assume the handler didn't
             log and bail. Matches the pre-existing worst-case budget.
          3. **Hard cap**: 250ms absolute ceiling in case of a pathologically
             slow/contended pipe; protects against unbounded blocking.

        The polling interval is 1ms, keeping CPU overhead trivial."""
        lines = []
        start = time.time()
        last_received_at = None
        while True:
            elapsed = time.time() - start
            if elapsed >= hard_cap:
                break
            try:
                lines.append(self._stderr_queue.get_nowait())
                last_received_at = time.time()
            except queue.Empty:
                if last_received_at is not None:
                    if time.time() - last_received_at >= idle_confirm:
                        break
                elif elapsed >= first_line_wait:
                    break
                time.sleep(0.001)
        return "\n".join(lines)

    def invoke(self, event: dict, request_id: str) -> dict:
        with self._lock:
            cold = self._cold

            if self._proc is None or self._proc.poll() is not None:
                self._spawn()
                cold = True
                self._cold = False
            else:
                cold = False

            timeout = self.config.get("Timeout", 30)
            event["_request_id"] = request_id
            result_box: list = []

            def _read_response():
                try:
                    self._proc.stdin.write(json.dumps(event) + "\n")
                    self._proc.stdin.flush()
                    for _ in range(200):
                        response_line = self._proc.stdout.readline()
                        if not response_line:
                            result_box.append({"status": "error", "error": "Worker process died"})
                            return
                        response_line = response_line.strip()
                        if not response_line:
                            continue
                        if response_line.startswith("{"):
                            try:
                                response = json.loads(response_line)
                                if response.get("status") in ("ok", "error"):
                                    result_box.append(response)
                                    return
                            except json.JSONDecodeError:
                                continue
                    result_box.append({"status": "error", "error": "No JSON response from worker after 200 lines"})
                except Exception as e:
                    result_box.append({"status": "error", "error": str(e)})

            reader = threading.Thread(target=_read_response, daemon=True)
            reader.start()
            reader.join(timeout=timeout)

            if reader.is_alive():
                # Timeout — kill the worker process
                logger.warning("Lambda %s timed out after %ds — killing worker", self.func_name, timeout)
                proc, self._proc = self._proc, None
                _signal(proc)
                _collect(proc, time.monotonic() + _REAP_GRACE)
                return {
                    "status": "error",
                    "error": f"Task timed out after {timeout}.00 seconds",
                    "cold_start": cold,
                    "log": self._drain_stderr(),
                }

            if not result_box:
                self._proc = None
                return {"status": "error", "error": "Worker returned no response", "cold_start": cold}

            response = result_box[0]
            if response.get("status") == "error":
                proc, self._proc = self._proc, None
                _signal(proc)
                _collect(proc, time.monotonic() + _REAP_GRACE)
            response["cold_start"] = cold
            # Bounded drain — replaces the fixed 50ms sleep that was paid
            # by every warm invocation. Typical completion is 1–10ms.
            response["log"] = self._drain_stderr_bounded()
            return response

    def _discard_tmpdir(self):
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def kill(self):
        kill_workers([self])


# Seconds a worker gets to exit on SIGTERM before it is SIGKILLed. Paid once per
# *batch*, never once per worker — see kill_workers.
_REAP_GRACE = 2.0


def _signal(proc) -> None:
    """Ask a worker subprocess to exit. Does not wait."""
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
    except Exception:
        pass


def _collect(proc, deadline: float) -> None:
    """Wait for a signalled subprocess and reap it, escalating if it ignores SIGTERM.

    Collecting is the point. Signalling alone leaves a zombie: ``subprocess``
    only reaps an abandoned Popen lazily, when the *next* Popen is constructed,
    so an **idle** server keeps every zombie it made. On a host that is invisible
    — the count is small and something else is always spawning — but in a
    container MiniStack is PID 1: the zombies are its own children, nothing else
    will ever collect them, and they hold PID-table slots against the container's
    pid limit. Measured: a 320-invocation burst left 244 zombies that survived
    indefinitely while the server sat idle.

    Warm-worker pooling is what made this reachable. One worker per function was
    killed rarely; a pool of them is reaped by the hundred.
    """
    if proc is None:
        return
    try:
        proc.wait(timeout=max(0.0, deadline - time.monotonic()))
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=0.5)
        except Exception:
            pass                # unkillable: leave it rather than block the caller


def kill_workers(workers) -> None:
    """Terminate and reap a batch of workers, paying the grace period once.

    Two-phase on purpose. Signalling every worker first and only then waiting
    makes a mass teardown cost ``_REAP_GRACE`` in total; doing it worker by
    worker would cost ``n * _REAP_GRACE``, which on a pool built by a burst is
    minutes — and ``reset()`` used to pay it holding the global worker lock.
    """
    workers = [w for w in workers if w is not None]
    if not workers:
        return
    procs = []
    for worker in workers:
        proc, worker._proc = worker._proc, None
        procs.append(proc)
        _signal(proc)
    deadline = time.monotonic() + _REAP_GRACE
    for proc in procs:
        _collect(proc, deadline)
    for worker in workers:
        try:
            worker._discard_tmpdir()
        except Exception:
            pass


def _worker_key(func_name: str, config: dict, qualifier: str) -> str:
    # Include account ID and region in the key to isolate workers across
    # accounts and regions. Two regions deploying the same function name must
    # not share a worker.
    account, region = _account_region_from_function_config(config)
    return f"{account}:{region}:{func_name}:{qualifier}"


def _worker_class_for(config: dict):
    """Pick the execution environment implementation for a function's runtime.

    ``provided.*`` speaks the HTTP Lambda Runtime API; python/nodejs speak the
    JSON-line stdio protocol of the bundled worker scripts.
    """
    if str(config.get("Runtime", "")).startswith("provided"):
        return ProvidedWorker
    return Worker


def acquire_worker(func_name: str, config: dict, code_zip: bytes,
                   qualifier: str = "$LATEST", max_concurrency: int | None = None):
    """Lease a free execution environment for this function.

    Each key holds a *pool* of workers, mirroring how AWS gives concurrent
    invocations separate execution environments (and how the Docker executor's
    ``_warm_pool`` already behaves). A single shared worker per function could
    not: ``Worker.invoke`` holds the worker's lock for the whole handler, so a
    handler that invokes its own function — which real AWS supports, up to the
    ~16-hop recursive-loop limit — waited on the lock its own caller held and
    deadlocked. The thread the invocation runs on is irrelevant to that; the
    lock is the bottleneck.

    ``max_concurrency`` is an optional *resource* ceiling on how many warm
    subprocesses may exist for one function, not a concurrency limit — AWS
    concurrency is enforced upstream in ``lambda_svc._acquire_execution_slot``,
    once, for every executor. It defaults to unbounded, matching the docker
    executor. At a ceiling the caller gets ``(None, "func_cap")`` rather than a
    blocking wait, because waiting for a worker held by your own caller is the
    deadlock this exists to remove.

    Returns ``(worker, "reused"|"spawn")`` or ``(None, "func_cap")``. Release
    with :func:`release_worker`.
    """
    key = _worker_key(func_name, config, qualifier)
    cap = max_concurrency or _LOCAL_MAX_WORKERS
    with _lock:
        entries = _workers.setdefault(key, [])
        for worker in entries:
            if not worker.in_use:
                worker.in_use = True
                worker.last_used = time.time()
                return worker, "reused"
        if cap and len(entries) >= cap:
            return None, "func_cap"
        worker = _worker_class_for(config)(func_name, config, code_zip)
        worker.in_use = True
        worker.last_used = time.time()
        entries.append(worker)
        return worker, "spawn"


def reap_idle_workers(ttl: float = None) -> int:
    """Kill workers idle longer than ``ttl`` and drop them from their pool.

    Without this the pools only ever grow: a burst of concurrency leaves its
    extra subprocesses alive forever. The first worker of each key is kept so
    the common warm-start path is unaffected.
    """
    ttl = _LOCAL_WORKER_TTL if ttl is None else ttl
    now = time.time()
    killed = []
    with _lock:
        for key, entries in list(_workers.items()):
            keep = []
            for i, worker in enumerate(entries):
                idle = now - getattr(worker, "last_used", now)
                # The first worker per key is kept warm indefinitely — except
                # for a published SnapStart version's worker, which exists
                # because PublishVersion pre-initialized it: without the
                # exception every publish would pin one subprocess forever.
                # Reaping it costs a re-init on the next invoke, the same as
                # the docker pool's TTL eviction.
                snapstart_version = (
                    (worker.config.get("SnapStart") or {}).get(
                        "OptimizationStatus") == "On"
                    and worker.config.get("Version", "$LATEST") != "$LATEST"
                )
                if worker.in_use or idle < ttl or (i == 0 and not snapstart_version):
                    keep.append(worker)
                else:
                    killed.append(worker)
            if keep:
                _workers[key] = keep
            else:
                _workers.pop(key, None)
    kill_workers(killed)
    return len(killed)


def ensure_spawned(worker: Worker) -> None:
    """Spawn the worker's subprocess now if it isn't running.

    SnapStart moves initialization to PublishVersion: the publish path leases
    a worker for the new version and forces the cold start here, so an init
    failure fails the publish (version State=Failed, as on AWS) and the first
    invoke finds the environment already warm.
    """
    with worker._lock:
        if worker._proc is None or worker._proc.poll() is not None:
            worker._spawn()
            worker._cold = False


def release_worker(worker: Worker) -> None:
    """Return a leased worker to its pool."""
    if worker is None:
        return
    with _lock:
        worker.in_use = False


def workers_in_use() -> int:
    """Count of leased workers across every function, for concurrency limits."""
    with _lock:
        return sum(1 for lst in _workers.values() for w in lst if w.in_use)


def get_or_create_worker(func_name: str, config: dict, code_zip: bytes,
                         qualifier: str = "$LATEST") -> Worker:
    """Back-compat lease that never reports a cap and is released immediately.

    Retained for callers that hold a worker only for the duration of a single
    ``invoke`` on the calling thread. The lease is dropped before returning, so
    such callers still serialize on ``Worker._lock`` exactly as before rather
    than silently retaining a pool slot they never release.
    """
    worker, _reason = acquire_worker(func_name, config, code_zip, qualifier=qualifier)
    release_worker(worker)
    return worker


def invalidate_worker(func_name: str, qualifier: str = None,
                      account: str = None, region: str = None):
    """Kill and remove workers for a function.

    If qualifier is provided, only kill that specific version/alias worker.
    Otherwise kill all workers for the function (used on delete).
    If account or region is provided, scope the invalidation to that account
    and/or region.
    """
    # Worker keys are "{account}:{region}:{func_name}:{qualifier}". Older
    # in-process keys may be "{account}:{func_name}:{qualifier}" during local
    # development, so tolerate both shapes.
    def _matches(k: str) -> bool:
        parts = k.split(":")
        if len(parts) == 4:
            k_account, k_region, k_func, k_qualifier = parts
        elif len(parts) == 3:
            k_account, k_func, k_qualifier = parts
            k_region = None
        else:
            return False
        if k_func != func_name:
            return False
        if account is not None and k_account != account:
            return False
        if region is not None and k_region is not None and k_region != region:
            return False
        if qualifier is not None and k_qualifier != qualifier:
            return False
        return True

    with _lock:
        doomed = [w for k in [k for k in _workers if _matches(k)]
                  for w in (_workers.pop(k, []) or [])]
    # Outside the lock: terminating a batch takes as long as the slowest worker
    # takes to exit, and holding the pool lock for that blocks every invocation.
    kill_workers(doomed)


def reset():
    """Terminate all warm workers, clean up temp dirs, and clear the pool."""
    with _lock:
        doomed = [w for entries in _workers.values() for w in entries]
        _workers.clear()
    kill_workers(doomed)


# ---------------------------------------------------------------------------
# provided.* (Go / Rust / custom runtime) warm worker
# ---------------------------------------------------------------------------

# Seconds the bootstrap binary gets to complete its cold start and issue its
# first GET /runtime/invocation/next. AWS fixes this: "The Init phase is
# limited to 10 seconds. If all three tasks do not complete within 10 seconds,
# Lambda retries the Init phase at the time of the first function invocation
# with the configured function timeout" (lambda-runtime-environment.html), so
# the number is not ours to tune — and blowing it is not a failed invocation,
# it re-runs init under the function's own Timeout (see _spawn).
_PROVIDED_INIT_TIMEOUT = 10.0

# How often init/invocation waits check whether the bootstrap is still alive.
_PROVIDED_POLL = 0.05

# Log lines buffered per environment between drains; the oldest are dropped
# rather than letting a chatty handler grow the buffer without bound.
_PROVIDED_LOG_MAX_LINES = 10000

# /2018-06-01/runtime/invocation/<request-id>/{response,error}
_INVOCATION_RESULT_RE = re.compile(r"/runtime/invocation/([^/]+)/(response|error)/?$")


class ProvidedInitTimeout(RuntimeError):
    """The bootstrap started but did not reach the Runtime API in time."""


class ProvidedRuntimeError(RuntimeError):
    """A provided-runtime failure that carries the error type AWS reports."""

    def __init__(self, message: str, error_type: str):
        super().__init__(message)
        self.error_type = error_type


class ProvidedWorker(Worker):
    """A reusable execution environment for a ``provided.*`` Lambda.

    ``lambda_svc._execute_function_provided`` starts the bootstrap binary,
    serves exactly one ``/runtime/invocation/next``, and kills the process —
    so every invocation pays a full cold start. The Lambda Runtime API is
    already a long-poll loop, which is precisely how real Lambda reuses an
    environment: hold the HTTP server and the process open and the same binary
    picks up the next event off ``/next``.

    Reuses ``Worker``'s pool bookkeeping (lease/release, idle reaping,
    invalidation on code update); only the spawn and invoke mechanics differ,
    because a custom runtime speaks HTTP rather than the JSON-line stdio
    protocol the Python/Node worker scripts use.

    Each running environment is a *generation*: ``_spawn`` creates one (server,
    process, log pump, event queue), ``_teardown`` destroys it. A generation's
    queues are captured by its own handler threads, and results are accepted
    only for the current generation's in-flight request ID, so a timed-out or
    crashed environment cannot feed the invocation that replaces it.
    """

    def __init__(self, func_name: str, config: dict, code_zip: bytes):
        super().__init__(func_name, config, code_zip)
        self._server = None
        self._server_thread = None
        self._log_thread = None
        # One in-flight invocation at a time: the pool leases a worker
        # exclusively (``in_use``), so a second event can never be queued
        # behind a first on the same environment — matching AWS, where
        # concurrent invocations get separate environments.
        self._pending: queue.Queue = queue.Queue(maxsize=1)
        self._result: dict = {}
        self._response_ready = threading.Event()
        self._init_error = None
        self._first_poll = threading.Event()
        # Set by the first /next poll or by an /init/error POST, so the init
        # wait is not a fixed sleep.
        self._init_settled = threading.Event()
        self._stopping = True
        # Guards state shared with the Runtime API handler threads. Distinct
        # from ``self._lock``, which is held across a whole invocation.
        self._state_lock = threading.Lock()
        self._generation = 0
        self._current_request_id = None

    # -- State transitions driven by the Runtime API handler threads ------

    def _mark_first_poll(self, generation: int) -> bool:
        with self._state_lock:
            if generation != self._generation or self._stopping:
                return False
            self._first_poll.set()
            self._init_settled.set()
            return True

    def _record_init_error(self, generation: int, payload) -> None:
        with self._state_lock:
            if generation != self._generation:
                return
            self._init_error = payload
            self._init_settled.set()
            if self._current_request_id is not None:
                # Init can fail on a respawn an invocation is waiting on: fail
                # it now instead of at the timeout.
                self._current_request_id = None
                self._result = {"error": payload}
                self._response_ready.set()

    def _record_result(self, generation: int, request_id: str, kind: str,
                       payload) -> bool:
        """Accept a ``/response`` or ``/error`` POST for the in-flight request.

        False means the ID is not the one we handed out: a duplicate, or a late
        POST from an environment already torn down, whose result must not reach
        the invocation that replaced it.
        """
        with self._state_lock:
            if generation != self._generation:
                return False
            if not self._current_request_id or request_id != self._current_request_id:
                return False
            self._current_request_id = None
            self._result = ({"error": payload} if kind == "error"
                            else {"response": payload})
            self._response_ready.set()
            return True

    def _build_handler_class(self, generation: int, pending: queue.Queue):
        worker = self

        import http.server

        class RuntimeAPIHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                pass

            def _read_body(self):
                encoding = self.headers.get("Transfer-Encoding", "")
                if "chunked" in encoding.lower():
                    chunks = []
                    while True:
                        size = int(self.rfile.readline().strip(), 16)
                        if size == 0:
                            self.rfile.readline()
                            break
                        chunks.append(self.rfile.read(size))
                        self.rfile.readline()
                    return b"".join(chunks)
                length = int(self.headers.get("Content-Length", 0))
                return self.rfile.read(length) if length else b""

            def _respond(self, code: int, body: bytes = b""):
                self.send_response(code)
                if body:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def _stopped(self):
                self._respond(500, b'{"errorMessage":"execution environment stopped"}')

            def do_GET(self):
                if "/runtime/invocation/next" not in self.path:
                    self._respond(404)
                    return
                # Reaching /next means init finished; the binary long-polls
                # here between invocations exactly as on AWS.
                if not worker._mark_first_poll(generation):
                    self._stopped()
                    return
                while True:
                    if worker._stopping or generation != worker._generation:
                        # Tell a runtime left over from a dead generation to
                        # exit rather than serve the live environment's events.
                        self._stopped()
                        return
                    try:
                        request_id, event, deadline_ms, trace_id = pending.get(
                            timeout=0.1)
                        break
                    except queue.Empty:
                        continue
                payload = json.dumps(event).encode()
                self.send_response(200)
                self.send_header("Lambda-Runtime-Aws-Request-Id", request_id)
                self.send_header("Lambda-Runtime-Deadline-Ms", str(deadline_ms))
                self.send_header(
                    "Lambda-Runtime-Invoked-Function-Arn",
                    worker.config.get("FunctionArn", ""),
                )
                if trace_id:
                    # The Runtime API header is how AWS hands X-Ray context to
                    # a custom runtime per invocation. The one-shot executor
                    # used a spawn-time env var, which a reused environment
                    # cannot do — this is both warm-safe and closer to AWS.
                    self.send_header("Lambda-Runtime-Trace-Id", trace_id)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                body = self._read_body()
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError:
                    parsed = body.decode("utf-8", errors="replace")

                if "/runtime/init/error" in self.path:
                    worker._record_init_error(generation, parsed)
                    self._respond(202)
                    return

                match = _INVOCATION_RESULT_RE.search(self.path)
                if not match:
                    self._respond(404)
                    return
                request_id, kind = match.group(1), match.group(2)
                if not worker._record_result(generation, request_id, kind, parsed):
                    # The shape AWS's own Runtime Interface Emulator renders:
                    # 400 with {"errorMessage": "Invalid request ID",
                    # "errorType": "InvalidRequestID"} (RIE
                    # internal/lambda/rapi/rendering/render_error.go).
                    self._respond(400, json.dumps({
                        "errorMessage": "Invalid request ID",
                        "errorType": "InvalidRequestID",
                    }).encode())
                    return
                self._respond(202)

        return RuntimeAPIHandler

    # -- Logs -------------------------------------------------------------

    def _pump_output(self, stream, log_queue: queue.Queue) -> None:
        """Drain the bootstrap's output into ``log_queue`` for its whole life.

        stdout and stderr are merged into one pipe (a custom runtime logs to
        either) and read continuously: an undrained pipe fills at ~64KiB and
        blocks the bootstrap on its next write. The queue belongs to one
        generation, so a dying environment's tail never mixes into the next.
        """
        try:
            for raw in iter(stream.readline, b""):
                if not raw:
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                line = raw.rstrip("\r\n")
                try:
                    log_queue.put_nowait(line)
                except queue.Full:
                    # Drop the oldest, never block: blocking here would stall
                    # the pipe this thread exists to drain.
                    try:
                        log_queue.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        log_queue.put_nowait(line)
                    except queue.Full:
                        pass
        except Exception:
            pass
        finally:
            try:
                stream.close()
            except Exception:
                pass

    # -- Lifecycle --------------------------------------------------------

    def _spawn(self, init_timeout: float | None = None):
        import socketserver

        from ministack.services.lambda_svc import (
            _provided_code_lock,
            _provided_runtime_code_dir,
            _provided_worker_env,
        )

        # A respawn used to abandon the previous generation's HTTP server,
        # thread and listening socket — one leaked set per bootstrap crash.
        self._teardown()

        code_dir = _provided_runtime_code_dir(self.code_zip)
        bootstrap_path = os.path.join(code_dir, "bootstrap")
        if not os.path.exists(bootstrap_path) or not os.access(bootstrap_path, os.X_OK):
            # "If the bootstrap file doesn't exist or isn't executable, your
            # function returns a Runtime.InvalidEntrypoint error upon
            # invocation" (runtimes-custom.html).
            raise ProvidedRuntimeError(
                "No bootstrap binary found in the deployment package.",
                "Runtime.InvalidEntrypoint",
            )

        pending: queue.Queue = queue.Queue(maxsize=1)
        log_queue: queue.Queue = queue.Queue(maxsize=_PROVIDED_LOG_MAX_LINES)
        with self._state_lock:
            self._generation += 1
            generation = self._generation
            self._stopping = False
            self._first_poll.clear()
            self._init_settled.clear()
            self._init_error = None
            self._result = {}
            self._response_ready.clear()
            self._current_request_id = None
            self._pending = pending
            self._stderr_queue = log_queue

        class _QuietThreadingServer(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

            def handle_error(self, request, client_address):
                _, exc, _ = sys.exc_info()
                if isinstance(exc, (BrokenPipeError, ConnectionResetError,
                                    ConnectionAbortedError)):
                    return
                super().handle_error(request, client_address)

        try:
            # Threading server: /next long-polls, so a single-threaded server
            # would wedge the response POST behind the next poll.
            self._server = _QuietThreadingServer(
                ("127.0.0.1", 0), self._build_handler_class(generation, pending))
            port = self._server.server_address[1]
            self._server_thread = threading.Thread(
                target=self._server.serve_forever, kwargs={"poll_interval": 0.1},
                daemon=True, name=f"provided-api-{self.func_name}")
            self._server_thread.start()

            proc_env = _provided_worker_env(self.config, code_dir, port)

            # Spawn under the code lock: no fork may overlap an extraction write
            # elsewhere, or the child inherits the open write fd and execve fails
            # with ETXTBSY (#1051).
            with _provided_code_lock:
                try:
                    self._proc = subprocess.Popen(
                        [bootstrap_path],
                        cwd=code_dir,
                        env=proc_env,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                    )
                except OSError as exc:
                    # A bootstrap the host cannot execute is the same class of
                    # failure as a missing one, and AWS names it the same way.
                    raise ProvidedRuntimeError(
                        f"Couldn't execute the bootstrap binary: {exc}",
                        "Runtime.InvalidEntrypoint",
                    ) from exc

            self._log_thread = threading.Thread(
                target=self._pump_output, args=(self._proc.stdout, log_queue),
                daemon=True, name=f"provided-log-{self.func_name}")
            self._log_thread.start()
            self._stderr_thread = self._log_thread

            self._await_init(init_timeout)
        except BaseException:
            # Never leave a half-built environment behind.
            self._teardown()
            raise

        self._start_time = time.time()
        logger.info("Lambda provided-runtime environment spawned for %s (cold start)",
                    self.func_name)

    def _await_init(self, init_timeout: float | None = None) -> None:
        """Wait for the bootstrap to reach /next, bounded and interruptible.

        Returns as soon as init settles: a first poll, an /init/error POST, or
        the process exiting. Only a live but silent binary waits out the
        deadline, and that raises ``ProvidedInitTimeout`` so the caller can do
        what AWS does — run init again under the function's own timeout.
        """
        budget = _PROVIDED_INIT_TIMEOUT if init_timeout is None else init_timeout
        deadline = time.monotonic() + budget
        while not self._init_settled.wait(_PROVIDED_POLL):
            exit_code = self._proc.poll() if self._proc is not None else -1
            if exit_code is not None:
                raise RuntimeError(
                    f"bootstrap exited during init with code {exit_code}"
                    f"{self._init_log_suffix()}")
            if time.monotonic() >= deadline:
                raise ProvidedInitTimeout(
                    f"bootstrap did not reach the Runtime API within "
                    f"{budget:g}s{self._init_log_suffix()}")
        if self._init_error is not None:
            raise RuntimeError(f"init error: {self._init_error}")

    def _init_log_suffix(self) -> str:
        log = self._drain_stderr_bounded(first_line_wait=0.1)
        return f": {log}" if log else ""

    def invoke(self, event: dict, request_id: str, *, trace_id: str = None) -> dict:
        """Run one invocation on this environment.

        ``trace_id`` is per-invocation X-Ray context, passed separately rather
        than through a reserved key in ``event``: the payload belongs to the
        caller. It leaves as the ``Lambda-Runtime-Trace-Id`` header, where a
        custom runtime expects it.
        """
        with self._lock:
            cold = False
            timeout = self.config.get("Timeout", 30)
            if self._proc is None or self._proc.poll() is not None:
                try:
                    self._spawn()
                except ProvidedInitTimeout:
                    # AWS: an Init phase that overruns its 10 seconds is not a
                    # failed invocation — "Lambda retries the Init phase at the
                    # time of the first function invocation with the configured
                    # function timeout" (lambda-runtime-environment.html).
                    logger.info(
                        "Lambda %s: init exceeded %gs; re-running it under the "
                        "function timeout (%ss)",
                        self.func_name, _PROVIDED_INIT_TIMEOUT, timeout)
                    self._spawn(init_timeout=timeout)
                cold = True
                self._cold = False

            generation = self._generation

            with self._state_lock:
                self._result = {}
                self._response_ready.clear()
                self._current_request_id = request_id
                pending = self._pending
            deadline_ms = int((time.time() + timeout) * 1000)
            pending.put((request_id, event, deadline_ms, trace_id))

            deadline = time.monotonic() + timeout
            failure = None
            while not self._response_ready.wait(
                    min(_PROVIDED_POLL, max(0.0, deadline - time.monotonic()))):
                proc = self._proc
                if proc is None or proc.poll() is not None:
                    # The process can exit after sending its response but
                    # before the HTTP handler thread records it.
                    if self._response_ready.wait(_PROVIDED_POLL):
                        break
                    failure = "Runtime exited before returning a response"
                    break
                if time.monotonic() >= deadline:
                    failure = f"Task timed out after {timeout}.00 seconds"
                    break
            if failure is not None:
                logger.warning("Lambda %s: %s", self.func_name, failure)
                self._teardown()
                return {
                    "status": "error",
                    "error": failure,
                    "error_payload": {"errorMessage": failure, "errorType": "Runtime.ExitError"},
                    "cold_start": cold,
                    "log": self._drain_stderr(),
                }

            with self._state_lock:
                result, self._result = self._result, {}
                self._current_request_id = None
                init_failed = self._init_error is not None
            log = self._drain_stderr_bounded()
            # An environment whose process is gone cannot serve the next
            # invocation; drop it now so its server and threads go with it.
            died = (self._proc is None or self._proc.poll() is not None
                    or generation != self._generation)

            if "error" in result:
                # A handler error does not poison the environment on AWS — the
                # runtime reports it and goes back to polling /next. Only an
                # init failure (or a dead process) means it is unusable.
                if init_failed or died:
                    self._teardown()
                err = result["error"]
                return {
                    "status": "error",
                    "error": (err.get("errorMessage") if isinstance(err, dict)
                              else str(err)),
                    "error_payload": err,
                    "cold_start": cold,
                    "log": log,
                }

            if died:
                self._teardown()

            return {
                "status": "ok",
                "result": result.get("response"),
                "cold_start": cold,
                "log": log,
            }

    def _teardown(self):
        """Destroy the current generation: process, server, threads.

        Idempotent. Buffered log lines are kept so the invocation that
        triggered the teardown can still report them.
        """
        with self._state_lock:
            self._stopping = True
            self._current_request_id = None
        proc, self._proc = self._proc, None
        _signal(proc)
        _collect(proc, time.monotonic() + _REAP_GRACE)
        self._shutdown_server()
        self._join_log_thread()

    def _join_log_thread(self) -> None:
        thread, self._log_thread = self._log_thread, None
        self._stderr_thread = None
        # The pipe closes with the process, so the pump ends on its own; the
        # join only keeps it from outliving the generation.
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def _shutdown_server(self):
        server, self._server = self._server, None
        thread, self._server_thread = self._server_thread, None
        if server is not None:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)

    def _discard_tmpdir(self):
        # kill_workers() reaps the process and then calls this; piggyback the
        # server teardown so a reaped worker leaves no listening socket or
        # thread behind. The code dir is the shared content-addressed cache
        # from _provided_runtime_code_dir — nothing to remove here.
        with self._state_lock:
            self._stopping = True
        self._shutdown_server()
        self._join_log_thread()
