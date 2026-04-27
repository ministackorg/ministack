"""
MiniStack — Local AWS Service Emulator.
Single-port ASGI application on port 4566 (configurable via GATEWAY_PORT).
Routes requests to service handlers based on AWS headers, paths, and query parameters.
Compatible with AWS CLI, boto3, and any AWS SDK via --endpoint-url.
"""

import argparse
import asyncio
import base64
import json
import logging
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import uuid
from urllib.parse import parse_qs, unquote

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_MINISTACK_PORT = os.environ.get("GATEWAY_PORT", "4566")

_VERSION = os.environ.get("MINISTACK_VERSION") or "dev"
if _VERSION == "dev":
    try:
        from importlib.metadata import version as _pkg_version
        _VERSION = _pkg_version("ministack")
    except Exception:
        pass

# Matches host headers like "{apiId}.execute-api.<host>" or "{apiId}.execute-api.<host>:4566"
_EXECUTE_API_RE = re.compile(
    r"^([a-f0-9]{8})\.execute-api\." + re.escape(_MINISTACK_HOST) + r"(?::\d+)?$"
)
# Matches virtual-hosted S3:
#   "{bucket}.<host>" or "{bucket}.<host>:4566"          (boto3/SDK default)
#   "{bucket}.s3.<host>" or "{bucket}.s3.<host>:4566"    (Terraform AWS provider v4+)
# Does NOT match execute-api, alb, or other sub-service hostnames.
_S3_VHOST_RE = re.compile(
    r"^([^.]+)(?:\.s3)?\." + re.escape(_MINISTACK_HOST) + r"(?::\d+)?$"
)
_S3_VHOST_EXCLUDE_RE = re.compile(r"\.(execute-api|alb|emr|efs|elasticache|s3-control)\.")
_HEALTH_PATHS = ("/_ministack/health", "/_localstack/health", "/health")
_BODY_METHODS = ("POST", "PUT", "PATCH")
_COGNITO_USERINFO_PATHS = ("/oauth2/userInfo", "/oauth2/userinfo")
_RDS_DATA_PATHS = ("/Execute", "/BeginTransaction", "/CommitTransaction", "/RollbackTransaction", "/BatchExecute")
_S3_CONTROL_PREFIX = "/v20180820/"
_SES_V2_PREFIX = "/v2/email"
_ALB_PATH_PREFIX = "/_alb/"
_NON_S3_VHOST_NAMES = frozenset({
    "s3", "s3-control", "sqs", "sns", "dynamodb", "lambda", "iam", "sts",
    "secretsmanager", "logs", "ssm", "events", "kinesis", "monitoring", "ses",
    "states", "ecs", "rds", "rds-data", "elasticache", "glue", "athena",
    "apigateway", "cloudformation", "autoscaling", "codebuild", "transfer",
})

from ministack.core.hypercorn_compat import install as _install_hypercorn_compat
from ministack.core.persistence import PERSIST_STATE, load_state, save_all
from ministack.core.responses import _12_DIGIT_RE, set_request_account_id, set_request_region
from ministack.core.router import detect_service, extract_access_key_id, extract_region

# Must run before hypercorn emits its first Expect: 100-continue reply.
# See ministack/core/hypercorn_compat.py for the rationale (issue #389).
_install_hypercorn_compat()

# ---------------------------------------------------------------------------
# Lazy service loader — modules are imported on first request, not at startup.
# This saves ~20 MB of idle RAM and speeds up boot.
# ---------------------------------------------------------------------------
_loaded_modules: dict = {}

# Execution state of ready.d scripts — surfaced via /_ministack/health and /_ministack/ready.
# status: "pending" (not started) | "running" | "completed" (all scripts finished, errors included)
_ready_scripts_state: dict = {
    "status": "pending",
    "total": 0,
    "completed": 0,
    "failed": 0,
}


class _ErrorModule:
    """Stub returned when a service module fails to import."""
    def __init__(self, name: str, error: str):
        self._name = name
        self._error = error

    async def handle_request(self, method, path, headers, body, query_params):
        return 500, {"Content-Type": "application/json"}, \
            json.dumps({"__type": "ServiceUnavailable",
                        "message": f"Service module '{self._name}' failed to load: {self._error}"}).encode()

    def get_state(self):
        return {}

    def restore_state(self, data):
        pass

    def load_persisted_state(self, data):
        pass

    def reset(self):
        pass


def _get_module(name: str):
    """Import and cache a service module by short name (e.g. 's3', 'lambda_svc')."""
    mod = _loaded_modules.get(name)
    if mod is None:
        try:
            mod = __import__(f"ministack.services.{name}", fromlist=["handle_request"])
        except (ModuleNotFoundError, ImportError) as e:
            logger.warning("Service module failed to load: %s - %s", name, e)
            mod = _ErrorModule(name, str(e))
        _loaded_modules[name] = mod
    return mod


def _lazy_handler(module_name: str):
    """Return a callable that lazily imports module_name and delegates to handle_request."""
    async def _handler(method, path, headers, body, query_params):
        mod = _get_module(module_name)
        return await mod.handle_request(method, path, headers, body, query_params)
    return _handler

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ministack")

# Single source of truth for routable services, their backing modules, and aliases.
SERVICE_REGISTRY = {
    "acm": {"module": "acm"},
    "backup": {"module": "backup"},
    "apigateway": {"module": "apigateway", "aliases": ("execute-api", "apigatewayv2")},
    "appconfig": {"module": "appconfig"},
    "appconfigdata": {"module": "appconfig"},
    "appsync": {"module": "appsync"},
    "athena": {"module": "athena"},
    "autoscaling": {"module": "autoscaling"},
    "cloudformation": {"module": "cloudformation"},
    "cloudfront": {"module": "cloudfront"},
    "codebuild": {"module": "codebuild"},
    "cognito-identity": {"module": "cognito"},
    "cognito-idp": {"module": "cognito"},
    "dynamodb": {"module": "dynamodb"},
    "ec2": {"module": "ec2"},
    "ecr": {"module": "ecr"},
    "ecs": {"module": "ecs"},
    "eks": {"module": "eks"},
    "elasticache": {"module": "elasticache"},
    "elasticfilesystem": {"module": "efs"},
    "elasticloadbalancing": {"module": "alb", "aliases": ("elbv2", "elb")},
    "elasticmapreduce": {"module": "emr"},
    "events": {"module": "eventbridge", "aliases": ("eventbridge",)},
    "firehose": {"module": "firehose", "aliases": ("kinesis-firehose",)},
    "glue": {"module": "glue"},
    "iam": {"module": "iam"},
    "kinesis": {"module": "kinesis"},
    "kms": {"module": "kms"},
    "lambda": {"module": "lambda_svc"},
    "logs": {"module": "cloudwatch_logs", "aliases": ("cloudwatch-logs",)},
    "monitoring": {"module": "cloudwatch", "aliases": ("cloudwatch",)},
    "rds-data": {"module": "rds_data"},
    "rds": {"module": "rds"},
    "route53": {"module": "route53"},
    "s3": {"module": "s3"},
    "s3files": {"module": "s3files"},
    "scheduler": {"module": "scheduler"},
    "secretsmanager": {"module": "secretsmanager"},
    "servicediscovery": {"module": "servicediscovery"},
    "ses": {"module": "ses"},
    "sns": {"module": "sns"},
    "sqs": {"module": "sqs"},
    "ssm": {"module": "ssm"},
    "states": {"module": "stepfunctions", "aliases": ("step-functions", "stepfunctions")},
    "sts": {"module": "sts"},
    "tagging": {"module": "tagging"},
    "transfer": {"module": "transfer"},
    "wafv2": {"module": "waf"},
}

SERVICE_HANDLERS = {
    service_name: _lazy_handler(service_config["module"])
    for service_name, service_config in SERVICE_REGISTRY.items()
}

# Maps the on-disk persistence key to the service module name. `save_all`
# (lifespan.shutdown) consumes this. Restore happens at module import time
# in each service via its own `load_state()` call (see e.g. services/sqs.py);
# a small allow-list is also restored centrally by `_load_persisted_state`
# below. Symmetry between save and restore is enforced by
# tests/test_persistence_symmetry.py.
_state_map = {
    "apigateway": "apigateway", "apigateway_v1": "apigateway_v1",
    "sqs": "sqs", "sns": "sns", "ssm": "ssm",
    "secretsmanager": "secretsmanager", "iam": "iam",
    "dynamodb": "dynamodb", "kms": "kms", "eventbridge": "eventbridge",
    "cloudwatch_logs": "cloudwatch_logs", "kinesis": "kinesis",
    "ec2": "ec2", "route53": "route53", "cognito": "cognito",
    "ecr": "ecr", "cloudwatch": "cloudwatch", "s3": "s3",
    "lambda": "lambda_svc", "rds": "rds", "ecs": "ecs",
    "elasticache": "elasticache", "appsync": "appsync",
    "stepfunctions": "stepfunctions", "alb": "alb",
    "glue": "glue", "efs": "efs", "waf": "waf",
    "athena": "athena", "emr": "emr", "cloudfront": "cloudfront",
    "codebuild": "codebuild", "acm": "acm", "firehose": "firehose",
    "ses": "ses", "ses_v2": "ses_v2",
    "servicediscovery": "servicediscovery", "s3files": "s3files",
    "appconfig": "appconfig", "transfer": "transfer",
    "scheduler": "scheduler", "autoscaling": "autoscaling",
    "eks": "eks", "backup": "backup", "pipes": "pipes",
}

SERVICE_NAME_ALIASES = {
    alias: service_name
    for service_name, service_config in SERVICE_REGISTRY.items()
    for alias in service_config.get("aliases", ())
}


def _resolve_port():
    """Resolve gateway port: GATEWAY_PORT > EDGE_PORT > 4566."""
    return os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"


if os.environ.get("LOCALSTACK_PERSISTENCE") == "1" and os.environ.get("S3_PERSIST") != "1":
    os.environ["S3_PERSIST"] = "1"
    logger.info("LOCALSTACK_PERSISTENCE=1 detected — enabling S3_PERSIST")

_services_env = os.environ.get("SERVICES", "").strip()
if _services_env:
    _requested = {s.strip() for s in _services_env.split(",") if s.strip()}
    _resolved = set()
    for _name in _requested:
        _key = SERVICE_NAME_ALIASES.get(_name, _name)
        if _key in SERVICE_HANDLERS:
            _resolved.add(_key)
        else:
            logger.warning("SERVICES: unknown service '%s' (resolved as '%s') — skipping", _name, _key)
    SERVICE_HANDLERS = {k: v for k, v in SERVICE_HANDLERS.items() if k in _resolved}
    logger.info("SERVICES filter active — enabled: %s", sorted(SERVICE_HANDLERS.keys()))

BANNER = r"""
  __  __ _       _ ____  _             _
 |  \/  (_)_ __ (_) ___|| |_ __ _  ___| | __
 | |\/| | | '_ \| \___ \| __/ _` |/ __| |/ /
 | |  | | | | | | |___) | || (_| | (__|   <
 |_|  |_|_|_| |_|_|____/ \__\__,_|\___|_|\_\

 Local AWS Service Emulator — Port {port}
 Services: S3, SQS, SNS, DynamoDB, Lambda, IAM, STS, SecretsManager, CloudWatch Logs,
          SSM, EventBridge, Kinesis, CloudWatch, SES, SES v2, ACM, WAF v2, Step Functions,
          ECS, RDS, ElastiCache, Glue, Athena, API Gateway, Firehose, Route53,
          Cognito, EC2, EMR, EBS, EFS, ALB/ELBv2, CloudFormation, KMS, ECR, CloudFront,
          AppSync, Cloud Map, S3 Files, RDS Data API, CodeBuild, AppConfig, Transfer, EKS
"""


_reset_lock: "asyncio.Lock | None" = None


def _get_reset_lock() -> asyncio.Lock:
    global _reset_lock
    if _reset_lock is None:
        _reset_lock = asyncio.Lock()
    return _reset_lock


# ---------------------------------------------------------------------------
# Request I/O helpers
# ---------------------------------------------------------------------------

def _decode_aws_chunked_body(body: bytes, headers: dict) -> bytes:
    """Decode AWS chunked request bodies and normalize content-encoding headers."""
    sha256_header = headers.get("x-amz-content-sha256", "")
    content_encoding = headers.get("content-encoding", "")
    if not (
        sha256_header.startswith("STREAMING-")
        or "aws-chunked" in content_encoding
        or headers.get("x-amz-decoded-content-length")
    ):
        return body

    decoded = b""
    remaining = body
    while remaining:
        crlf = remaining.find(b"\r\n")
        if crlf == -1:
            break
        chunk_header = remaining[:crlf].decode("ascii", errors="replace")
        size_hex = chunk_header.split(";")[0].strip()
        try:
            chunk_size = int(size_hex, 16)
        except ValueError:
            break
        if chunk_size == 0:
            break
        data_start = crlf + 2
        decoded += remaining[data_start:data_start + chunk_size]
        remaining = remaining[data_start + chunk_size + 2:]  # skip trailing \r\n

    body = decoded
    if "aws-chunked" in content_encoding:
        encodings = [p.strip() for p in content_encoding.split(",") if p.strip() != "aws-chunked"]
        if encodings:
            headers["content-encoding"] = ", ".join(encodings)
        else:
            headers.pop("content-encoding", None)
    return body


async def _read_request_body(receive, method: str, headers: dict) -> bytes:
    """Read and decode the request body only for methods or headers that can carry one."""
    body = b""
    if headers.get("content-length") or headers.get("transfer-encoding") or method in _BODY_METHODS:
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break
    return _decode_aws_chunked_body(body, headers)


async def _send_response(send, status, headers, body):
    """Send ASGI HTTP response."""
    def _encode_header_value(v: str) -> bytes:
        try:
            return v.encode("latin-1")
        except UnicodeEncodeError:
            return v.encode("utf-8")

    body_bytes = body if isinstance(body, bytes) else body.encode("utf-8")
    if "content-length" not in {k.lower() for k in headers}:
        headers["Content-Length"] = str(len(body_bytes))
    header_list = [(k.encode("latin-1"), _encode_header_value(str(v))) for k, v in headers.items()]
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": header_list,
    })
    await send({
        "type": "http.response.body",
        "body": body_bytes,
        "more_body": False,
    })


async def _send_if_handled(send, response) -> bool:
    """Send a response tuple and report whether the request was handled."""
    if response is None:
        return False
    await _send_response(send, *response)
    return True


# ---------------------------------------------------------------------------
# Tier 1 — Pre-body handlers (no request body needed)
# ---------------------------------------------------------------------------

def _handle_options_request(method: str, request_id: str):
    """Return the standard CORS preflight response when applicable."""
    if method != "OPTIONS":
        return None
    return 200, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "*",
        "Access-Control-Max-Age": "86400",
        "Content-Length": "0",
        "x-amzn-requestid": request_id,
    }, b""


def _handle_health_request(path: str, request_id: str):
    """Return health responses for MiniStack and LocalStack-compatible endpoints."""
    if path not in _HEALTH_PATHS:
        return None
    return 200, {
        "Content-Type": "application/json",
        "x-amzn-requestid": request_id,
    }, json.dumps({
        "services": {s: "available" for s in SERVICE_HANDLERS},
        "edition": "light",
        "version": _VERSION,
        "ready_scripts": dict(_ready_scripts_state),
    }).encode()


def _handle_ready_request(path: str, request_id: str):
    """Return readiness state once ready.d scripts have completed."""
    if path != "/_ministack/ready":
        return None
    ready = _ready_scripts_state["status"] == "completed"
    status = 200 if ready else 503
    return status, {
        "Content-Type": "application/json",
        "x-amzn-requestid": request_id,
    }, json.dumps(dict(_ready_scripts_state)).encode()


def _handle_unknown_localstack_request(path: str, request_id: str):
    """Return a clear 404 JSON for unrecognised /_localstack/* paths.

    /_localstack/health is already matched by _handle_health_request (included in
    _HEALTH_PATHS), so only unknown paths reach here. This prevents them from
    falling through to the S3 handler and returning confusing NoSuchBucket XML.
    """
    if not path.startswith("/_localstack/"):
        return None
    return 404, {
        "Content-Type": "application/json",
        "x-amzn-requestid": request_id,
    }, json.dumps({
        "error": (
            f"Unknown LocalStack endpoint: {path}. "
            "Ministack exposes /_ministack/health, /_ministack/ready, and /_ministack/reset. "
            "See https://github.com/ministackorg/ministack for the full API."
        )
    }).encode()


def _handle_lambda_download_request(path: str, method: str):
    """Serve MiniStack's Lambda layer and function-code download endpoints."""
    if path.startswith("/_ministack/lambda-layers/") and method == "GET":
        path_parts = path.split("/")
        if len(path_parts) >= 6 and path_parts[5] == "content" and path_parts[4].isdigit():
            return _get_module("lambda_svc").serve_layer_content(path_parts[3], int(path_parts[4]))

    if path.startswith("/_ministack/lambda-code/") and method == "GET":
        path_parts = path.split("/")
        if len(path_parts) >= 4:
            return _get_module("lambda_svc").serve_function_code(path_parts[3])
    return None


async def _handle_cognito_get_request(method: str, path: str, headers: dict, query_params: dict):
    """Handle Cognito GET endpoints that do not require request body parsing."""
    if "/.well-known/" in path and method == "GET":
        if path.endswith("/.well-known/jwks.json"):
            pool_id = path.rsplit("/.well-known/jwks.json", 1)[0].lstrip("/")
            if pool_id:
                return _get_module("cognito").well_known_jwks(pool_id)
        elif path.endswith("/.well-known/openid-configuration"):
            pool_id = path.rsplit("/.well-known/openid-configuration", 1)[0].lstrip("/")
            if pool_id:
                region = extract_region(headers) or "us-east-1"
                return _get_module("cognito").well_known_openid_configuration(pool_id, region)

    if path == "/oauth2/authorize" and method == "GET":
        return _get_module("cognito").handle_oauth2_authorize(method, path, headers, query_params)
    if path in _COGNITO_USERINFO_PATHS and method == "GET":
        return _get_module("cognito").handle_oauth2_userinfo(method, path, headers, b"", query_params)
    if path == "/logout" and method == "GET":
        return _get_module("cognito").handle_logout(method, path, headers, query_params)
    return None


async def _handle_admin_reset(path: str, method: str, query_params: dict):
    """Handle reset requests before request body parsing."""
    if path != "/_ministack/reset" or method != "POST":
        return None

    async with _get_reset_lock():
        await asyncio.to_thread(_reset_all_state)

    run_init = query_params.get("init", [""])[0] == "1"
    if run_init:
        _run_init_scripts()
        _ready_scripts_state.update({"status": "pending", "total": 0, "completed": 0, "failed": 0})
        asyncio.create_task(_run_ready_scripts())
    return 200, {"Content-Type": "application/json"}, json.dumps({"reset": "ok"}).encode()


async def _handle_ses_messages_request(method: str, path: str, headers: dict, query_params: dict):
    """Handle SES messages inspection endpoint.

    Supports filtering by account via the 'account' query parameter. When provided,
    sets the request context to that account so emails are retrieved from the correct
    AccountScopedDict._sent_emails_list.
    """
    if path != "/_ministack/ses/messages" or method != "GET":
        return None

    account_id = None
    if "account" in query_params:
        raw_account = query_params["account"]
        account_id = raw_account[0] if isinstance(raw_account, (list, tuple)) else raw_account
        if not _12_DIGIT_RE.match(account_id):
            return 400, {"Content-Type": "application/json"}, json.dumps({
                "__type": "InvalidAccountID",
                "message": f"Account ID must be 12 digits, got: {account_id}",
            }).encode()

    try:
        mod = _get_module("ses")
        sent_emails_dict = {}
        try:
            all_data = mod._sent_emails.to_dict()
            for (acct, key), val in all_data.items():
                if key == "entries" and isinstance(val, list):
                    sent_emails_dict[acct] = val
        except Exception:
            # Fallback: empty dict on any unexpected shape
            sent_emails_dict = {}

        response = {
            "messages": {
                acct: [
                    {
                        "MessageId": rec["MessageId"],
                        "Source": rec["Source"],
                        "To": rec.get("To", []),
                        "CC": rec.get("CC", []),
                        "BCC": rec.get("BCC", []),
                        "Subject": rec.get("RenderedSubject") or rec.get("Subject", ""),
                        "BodyText": rec.get("RenderedBodyText") or rec.get("BodyText", ""),
                        "BodyHtml": rec.get("RenderedBodyHtml") or rec.get("BodyHtml"),
                        "Timestamp": rec["Timestamp"],
                        "Type": rec["Type"],
                    }
                    for rec in (recs if isinstance(recs, list) else [])
                ]
                for acct, recs in sent_emails_dict.items()
                if account_id is None or acct == account_id
            }
        }
    except Exception as e:
        logger.exception("Error retrieving SES messages: %s", e)
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": str(e)}).encode()

    return 200, {"Content-Type": "application/json"}, json.dumps(response).encode()


async def _handle_pre_body_request(method: str, path: str, headers: dict, query_params: dict, request_id: str):
    """Handle fast-path routes that do not require request body parsing."""
    # OPTIONS on an execute-api host / path MUST flow through apigateway.handle_execute
    # so the API's own corsConfiguration is applied (#406). Skip the generic wildcard
    # preflight in that case.
    host = headers.get("host", "")
    is_execute_api = _parse_execute_api_url(host, path) is not None
    for response in (
        None if is_execute_api else _handle_options_request(method, request_id),
        _handle_health_request(path, request_id),
        _handle_ready_request(path, request_id),
        _handle_unknown_localstack_request(path, request_id),
        _handle_lambda_download_request(path, method),
    ):
        if response is not None:
            return response

    response = await _handle_cognito_get_request(method, path, headers, query_params)
    if response is not None:
        return response
    
    response = await _handle_ses_messages_request(method, path, headers, query_params)
    if response is not None:
        return response

    response = _handle_transfer_sftp_ports_request(method, path)
    if response is not None:
        return response

    return await _handle_admin_reset(path, method, query_params)


def _handle_transfer_sftp_ports_request(method: str, path: str):
    """`GET /_ministack/transfer/sftp-ports` returns ``{shared, per_server}``.

    boto3's DescribeServer drops fields not in the AWS spec, so this
    admin endpoint is how tests (and humans) discover which ports
    MiniStack's SFTP listeners ended up on — particularly relevant
    when ``SFTP_PORT_PER_SERVER=1`` allocates ports dynamically from
    ``SFTP_BASE_PORT``.
    """
    if path != "/_ministack/transfer/sftp-ports" or method != "GET":
        return None
    try:
        from ministack.services import transfer
        body = {
            "enabled": transfer._sftp_enabled(),
            "port_per_server": transfer._port_per_server(),
            "shared_port": transfer._shared_port() if transfer._sftp_enabled() else None,
            "per_server": dict(transfer._sftp_per_server_ports),
        }
    except Exception as e:
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": str(e)}).encode()
    return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()


# ---------------------------------------------------------------------------
# Tier 2 — Post-body shortcuts (body required, before generic routing)
# ---------------------------------------------------------------------------

async def _handle_cognito_body_request(method: str, path: str, headers: dict, body: bytes, query_params: dict):
    """Handle Cognito routes that require the parsed request body."""
    if path in ("/oauth2/login", "/login") and method == "POST":
        return _get_module("cognito").handle_login_submit(method, path, headers, body, query_params)
    if path == "/oauth2/token" and method == "POST":
        return _get_module("cognito").handle_oauth2_token(method, path, headers, body, query_params)
    if path in _COGNITO_USERINFO_PATHS and method == "POST":
        return _get_module("cognito").handle_oauth2_userinfo(method, path, headers, body, query_params)
    return None


async def _handle_admin_config_request(path: str, method: str, body: bytes):
    """Apply whitelisted runtime config changes through the admin endpoint."""
    if path != "/_ministack/config" or method != "POST":
        return None

    allowed_config_keys = {
        "athena.ATHENA_ENGINE", "athena.ATHENA_DATA_DIR",
        "stepfunctions._sfn_mock_config",
        "stepfunctions._SFN_WAIT_SCALE",
        "lambda_svc.LAMBDA_EXECUTOR",
    }
    try:
        config = json.loads(body) if body else {}
    except json.JSONDecodeError:
        config = {}

    applied = {}
    for key, value in config.items():
        if key not in allowed_config_keys:
            logger.warning("/_ministack/config: rejected key %s (not in whitelist)", key)
            continue
        if "." not in key:
            continue

        mod_name, var_name = key.rsplit(".", 1)
        try:
            mod = __import__(f"ministack.services.{mod_name}", fromlist=[var_name])
            if key == "stepfunctions._SFN_WAIT_SCALE":
                try:
                    float_value = float(value)
                except (ValueError, TypeError):
                    logger.warning("/_ministack/config: invalid SFN_WAIT_SCALE=%r", value)
                    continue
                if not math.isfinite(float_value) or float_value < 0:
                    logger.warning("/_ministack/config: invalid SFN_WAIT_SCALE=%r", value)
                    continue
                value = float_value
            setattr(mod, var_name, value)
            applied[key] = value
        except (ImportError, AttributeError) as e:
            logger.warning("/_ministack/config: failed to set %s: %s", key, e)
    return 200, {"Content-Type": "application/json"}, json.dumps({"applied": applied}).encode()


async def _handle_post_body_shortcuts(method: str, path: str, headers: dict, body: bytes, query_params: dict):
    """Handle body-dependent routes before the generic service router."""
    response = await _handle_cognito_body_request(method, path, headers, body, query_params)
    if response is not None:
        return response
    return await _handle_admin_config_request(path, method, body)


# ---------------------------------------------------------------------------
# Tier 3 — Special data-plane handlers (host/path-based routing)
# ---------------------------------------------------------------------------

async def _handle_s3_control_request(path: str, method: str, body: bytes, query_params: dict, request_id: str):
    """Handle S3 Control operations addressed via the /v20180820 path prefix."""
    if not path.startswith(_S3_CONTROL_PREFIX):
        return None

    if path.startswith("/v20180820/tags/"):
        raw_arn = path[len("/v20180820/tags/"):]
        arn = unquote(raw_arn)
        bucket_name = arn.split(":::")[-1].split("/")[0] if ":::" in arn else arn.split("/")[0]

        if method == "GET":
            tags = _get_module("s3")._bucket_tags.get(bucket_name, {})
            tag_members = "".join(
                f"<member><Key>{k}</Key><Value>{v}</Value></member>"
                for k, v in tags.items()
            )
            xml_body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListTagsForResourceResult xmlns="https://awss3control.amazonaws.com/doc/2018-08-20/">'
                f"<Tags>{tag_members}</Tags>"
                "</ListTagsForResourceResult>"
            ).encode()
            return 200, {
                "Content-Type": "application/xml",
                "x-amzn-requestid": request_id,
            }, xml_body

        if method in ("POST", "PUT"):
            # AWS SDK Go v2 (used by terraform-aws-provider v6+) sends
            # TagResource as POST with an XML TagResourceRequest body. Older
            # SDKs used PUT with JSON. Accept both methods + both body shapes
            # so we don't silently drop tags (#447).
            new_tags: dict = {}
            try:
                if body:
                    raw = body if isinstance(body, str) else body.decode("utf-8", errors="replace")
                    stripped = raw.lstrip()
                    if stripped.startswith("<"):
                        # XML: <TagResourceRequest><Tags><Tag><Key>..</Key><Value>..</Value></Tag>...</Tags></TagResourceRequest>
                        from xml.etree.ElementTree import fromstring
                        root = fromstring(raw)
                        def _local(el):
                            t = el.tag
                            return t.split("}")[-1] if "}" in t else t
                        for child in root.iter():
                            if _local(child) != "Tag":
                                continue
                            key_el = next((c for c in child if _local(c) == "Key"), None)
                            val_el = next((c for c in child if _local(c) == "Value"), None)
                            if key_el is not None and key_el.text:
                                new_tags[key_el.text] = (val_el.text or "") if val_el is not None else ""
                    elif stripped.startswith("{"):
                        payload = json.loads(stripped)
                        new_tags = {t["Key"]: t["Value"] for t in payload.get("Tags", [])}
            except Exception as e:
                logger.warning("S3 Control TagResource parse error: %s", e)
            if new_tags:
                existing = _get_module("s3")._bucket_tags.get(bucket_name, {})
                existing.update(new_tags)
                _get_module("s3")._bucket_tags[bucket_name] = existing
            return 204, {"x-amzn-requestid": request_id}, b""

        if method == "DELETE":
            keys_to_remove = query_params.get("tagKeys", [])
            if isinstance(keys_to_remove, str):
                keys_to_remove = [keys_to_remove]
            tags = _get_module("s3")._bucket_tags.get(bucket_name, {})
            for key in keys_to_remove:
                tags.pop(key, None)
            _get_module("s3")._bucket_tags[bucket_name] = tags
            return 204, {"x-amzn-requestid": request_id}, b""

        return 200, {
            "Content-Type": "application/json",
            "x-amzn-requestid": request_id,
        }, b"{}"

    return 200, {
        "Content-Type": "application/json",
        "x-amzn-requestid": request_id,
    }, b"{}"


async def _handle_rds_data_request(method: str, path: str, headers: dict, body: bytes, query_params: dict):
    """Handle RDS Data API operations before generic routing."""
    if path not in _RDS_DATA_PATHS:
        return None
    return await _get_module("rds_data").handle_request(method, path, headers, body, query_params)


async def _handle_ses_v2_request(method: str, path: str, headers: dict, body: bytes, query_params: dict):
    """Handle SES v2 REST API operations before generic routing."""
    if not path.startswith(_SES_V2_PREFIX):
        return None
    return await _get_module("ses_v2").handle_request(method, path, headers, body, query_params)


def _parse_execute_api_url(host: str, path: str) -> tuple[str, str, str] | None:
    """Resolve an execute-api request into (api_id, stage, execute_path).

    Supports three addressing modes, in priority order:
      1. Host-based (AWS-native):   {apiId}.execute-api.<host>[:port]/{stage}/{path}
      2. LocalStack-compat (new):   <host>[:port]/_aws/execute-api/{apiId}/{stage}/{path}
      3. LocalStack-compat (v1):    <host>[:port]/restapis/{apiId}/{stage}/_user_request_/{path}

    The path-based forms exist because (a) browsers on macOS don't resolve
    `*.localhost` and (b) many HTTP clients can't override the `Host` header
    (issue #401). Returns ``None`` if none of the three patterns match."""
    m = _EXECUTE_API_RE.match(host)
    if m:
        api_id = m.group(1)
        parts = path.lstrip("/").split("/", 1)
        stage = parts[0] if parts and parts[0] else "$default"
        execute_path = "/" + parts[1] if len(parts) > 1 else "/"
        return api_id, stage, execute_path

    # LocalStack-compat: /_aws/execute-api/{apiId}/{stage}/{path...}
    if path.startswith("/_aws/execute-api/"):
        rest = path[len("/_aws/execute-api/"):]
        parts = rest.split("/", 2)
        if len(parts) >= 2 and parts[0]:
            api_id = parts[0]
            stage = parts[1] if parts[1] else "$default"
            execute_path = "/" + parts[2] if len(parts) > 2 else "/"
            return api_id, stage, execute_path

    # LocalStack v1 legacy: /restapis/{apiId}/{stage}/_user_request_/{path...}
    if path.startswith("/restapis/"):
        rest = path[len("/restapis/"):]
        parts = rest.split("/", 3)
        if len(parts) >= 3 and parts[2] == "_user_request_":
            api_id = parts[0]
            stage = parts[1] if parts[1] else "$default"
            execute_path = "/" + parts[3] if len(parts) > 3 else "/"
            return api_id, stage, execute_path

    return None


def _resolve_stage_and_path(api_id: str, tentative_stage: str, execute_path: str) -> tuple[str, str]:
    """Pick (stage, execute_path) based on the API's configured stages.

    AWS v2 HTTP / WebSocket APIs configured with the ``$default`` stage serve
    from the root of the execute-api URL — no stage segment in the path. v1
    REST APIs always carry the stage as the first path segment. We can't tell
    from the URL alone which pattern applies, so we check the API's configured
    stages and route accordingly (issue #404).

    Rules:
      - If the tentative first segment IS a configured stage name, strip it.
      - Else if the API has a ``$default`` stage, use that and treat the
        whole original path (including ``tentative_stage``) as ``execute_path``.
      - Else fall through (``handle_execute`` will return "Stage not found").
    """
    apigw_v1 = _get_module("apigateway_v1")
    if api_id in apigw_v1._rest_apis:
        stages_map = apigw_v1._stages_v1.get(api_id, {})
    else:
        stages_map = _get_module("apigateway")._stages.get(api_id, {})

    if tentative_stage in stages_map:
        return tentative_stage, execute_path
    if "$default" in stages_map:
        if execute_path == "/":
            resolved_path = "/" + tentative_stage if tentative_stage else "/"
        else:
            resolved_path = "/" + tentative_stage + execute_path
        return "$default", resolved_path
    # No match — let handle_execute report the stage miss verbatim.
    return tentative_stage, execute_path


async def _handle_execute_api_request(host: str, path: str, method: str, headers: dict, body: bytes, query_params: dict):
    """Handle API Gateway execute-api data plane requests (Host-based + path-based)."""
    parsed = _parse_execute_api_url(host, path)
    if parsed is None:
        return None
    api_id, tentative_stage, execute_path = parsed
    try:
        # WebSocket @connections management API — /{stage}/@connections/{id}.
        # The @connections prefix is authoritative; skip $default resolution.
        if execute_path.startswith("/@connections/"):
            connection_id = execute_path[len("/@connections/"):].split("/", 1)[0]
            return await _get_module("apigateway").handle_connections_api(
                method, api_id, tentative_stage, connection_id, body, headers
            )
        stage, execute_path = _resolve_stage_and_path(api_id, tentative_stage, execute_path)
        if api_id in _get_module("apigateway_v1")._rest_apis:
            return await _get_module("apigateway_v1").handle_execute(
                api_id, stage, method, execute_path, headers, body, query_params
            )
        return await _get_module("apigateway").handle_execute(
            api_id, stage, execute_path, method, headers, body, query_params
        )
    except Exception as e:
        logger.exception("Error in execute-api dispatch: %s", e)
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": str(e)}).encode()


def _is_potential_alb_request(host: str, path: str) -> bool:
    """Cheap ALB gate so ordinary requests avoid loading the ALB module."""
    hostname = host.split(":")[0].lower()
    return path.startswith(_ALB_PATH_PREFIX) or hostname.endswith(".elb.amazonaws.com") or hostname.endswith(".alb.localhost")


async def _handle_alb_request(host: str, path: str, method: str, headers: dict, body: bytes, query_params: dict):
    """Handle ALB data-plane requests for host-based and /_alb-prefixed addressing."""
    if not _is_potential_alb_request(host, path):
        return None

    alb_module = _get_module("alb")
    load_balancer = alb_module.find_lb_for_host(host)
    dispatch_path = path

    if load_balancer is None and path.startswith(_ALB_PATH_PREFIX):
        path_parts = path[len(_ALB_PATH_PREFIX):].split("/", 1)
        load_balancer = alb_module._find_lb_by_name(path_parts[0])
        if load_balancer:
            dispatch_path = "/" + path_parts[1] if len(path_parts) > 1 else "/"

    if load_balancer is None:
        return None

    alb_port = 80
    if ":" in host:
        try:
            alb_port = int(host.rsplit(":", 1)[-1])
        except ValueError:
            pass

    try:
        return await alb_module.dispatch_request(
            load_balancer, method, dispatch_path, headers, body, query_params, alb_port
        )
    except Exception as e:
        logger.exception("Error in ALB data-plane dispatch: %s", e)
        return 500, {"Content-Type": "application/json"}, json.dumps({"message": str(e)}).encode()


async def _handle_s3_vhost_request(host: str, path: str, method: str, headers: dict, body: bytes, query_params: dict):
    """Handle virtual-hosted S3 requests before generic routing."""
    s3_vhost = _S3_VHOST_RE.match(host)
    if not s3_vhost or _S3_VHOST_EXCLUDE_RE.search(host):
        return None

    bucket = s3_vhost.group(1)
    if bucket in _NON_S3_VHOST_NAMES:
        return None

    vhost_path = "/" + bucket + path if path != "/" else "/" + bucket + "/"
    try:
        return await _get_module("s3").handle_request(method, vhost_path, headers, body, query_params)
    except Exception as e:
        logger.exception("Error handling virtual-hosted S3 request: %s", e)
        from xml.sax.saxutils import escape as _xml_esc

        return 500, {"Content-Type": "application/xml"}, (
            f"<Error><Code>InternalError</Code><Message>{_xml_esc(str(e))}</Message></Error>".encode()
        )


def _with_data_plane_headers(response, request_id: str, include_s3_id: bool = False, wildcard_cors: bool = True):
    """Attach common data-plane request-id headers to a response tuple.

    ``wildcard_cors`` controls whether a wildcard ``Access-Control-Allow-Origin: *``
    is added. API Gateway owns its own CORS (per-API ``corsConfiguration``,
    issue #406) so the caller passes ``wildcard_cors=False`` there to avoid
    clobbering the per-config value. Respects any ``Access-Control-Allow-Origin``
    already set by the upstream handler."""
    if response is None:
        return None
    status, headers, body = response
    if wildcard_cors and "Access-Control-Allow-Origin" not in headers:
        headers["Access-Control-Allow-Origin"] = "*"
    headers["x-amzn-requestid"] = request_id
    headers["x-amz-request-id"] = request_id
    if include_s3_id:
        headers["x-amz-id-2"] = base64.b64encode(os.urandom(48)).decode()
    return status, headers, body


async def _handle_special_data_plane_request(
    method: str,
    path: str,
    headers: dict,
    body: bytes,
    query_params: dict,
    request_id: str,
):
    """Handle special-case service entrypoints before the generic router."""
    if response := await _handle_s3_control_request(path, method, body, query_params, request_id):
        return response
    if response := await _handle_rds_data_request(method, path, headers, body, query_params):
        return response
    if response := await _handle_ses_v2_request(method, path, headers, body, query_params):
        return response

    host = headers.get("host", "")
    if response := await _handle_execute_api_request(host, path, method, headers, body, query_params):
        return _with_data_plane_headers(response, request_id, wildcard_cors=False)
    if response := await _handle_s3_vhost_request(host, path, method, headers, body, query_params):
        return _with_data_plane_headers(response, request_id, include_s3_id=True)
    if response := await _handle_alb_request(host, path, method, headers, body, query_params):
        return _with_data_plane_headers(response, request_id)
    return None


# ---------------------------------------------------------------------------
# Tier 4 — Generic service dispatch
# ---------------------------------------------------------------------------

def _routing_params(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> dict:
    """Augment routing params for unsigned form-encoded requests whose Action lives in the body."""
    routing_params = query_params
    if not query_params.get("Action") and headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
        body_params = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        if body_params.get("Action"):
            routing_params = {**query_params, "Action": body_params["Action"]}
    return routing_params


async def _dispatch_service_request(method: str, path: str, headers: dict, body: bytes, query_params: dict, request_id: str):
    """Dispatch a request through the generic service router."""
    routing_params = _routing_params(method, path, headers, body, query_params)
    service = detect_service(method, path, headers, routing_params)
    region = extract_region(headers)

    logger.debug("%s %s -> service=%s region=%s", method, path, service, region)

    handler = SERVICE_HANDLERS.get(service)
    if not handler:
        return 400, {"Content-Type": "application/json"}, json.dumps({"error": f"Unsupported service: {service}"}).encode()

    try:
        status, resp_headers, resp_body = await handler(method, path, headers, body, query_params)
    except Exception as e:
        logger.exception("Error handling %s request: %s", service, e)
        return 500, {"Content-Type": "application/json"}, json.dumps({"__type": "InternalError", "message": str(e)}).encode()

    resp_headers.update({
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "*",
        "x-amzn-requestid": request_id,
        "x-amz-request-id": request_id,
        "x-amz-id-2": base64.b64encode(os.urandom(48)).decode(),
    })
    return status, resp_headers, resp_body


# ---------------------------------------------------------------------------
# ASGI entry point
# ---------------------------------------------------------------------------

async def app(scope, receive, send):
    """ASGI application entry point."""
    if scope["type"] == "lifespan":
        await _handle_lifespan(scope, receive, send)
        return

    if scope["type"] == "websocket":
        # WebSocket APIs are reachable two ways:
        #   ws://{apiId}.execute-api.{host}[:port]/{stage}[/...]           (Host-based)
        #   ws://<host>[:port]/_aws/execute-api/{apiId}/{stage}[/...]      (LocalStack-compat path)
        ws_headers = {}
        for name, value in scope.get("headers", []):
            try:
                ws_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
            except UnicodeDecodeError:
                ws_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
        ws_host = ws_headers.get("host", "")
        ws_path = scope.get("path", "")
        parsed = _parse_execute_api_url(ws_host, ws_path)
        if not parsed:
            msg = await receive()
            if msg.get("type") == "websocket.connect":
                await send({"type": "websocket.close", "code": 1008})
            return
        ws_api_id, _stage, _execute_path = parsed
        try:
            await _get_module("apigateway").handle_websocket(
                scope, receive, send, ws_api_id, path_override=_execute_path,
            )
        except Exception:
            logger.exception("Error in WebSocket dispatch")
            try:
                await send({"type": "websocket.close", "code": 1011})
            except Exception:
                pass
        return

    if scope["type"] != "http":
        return

    method = scope["method"]
    path = scope["path"]
    query_string = scope.get("query_string", b"").decode("utf-8")
    query_params = parse_qs(query_string, keep_blank_values=True)

    headers = {}
    for name, value in scope.get("headers", []):
        try:
            headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            headers[name.decode("latin-1").lower()] = value.decode("latin-1")

    request_id = str(uuid.uuid4())

    # If a /_ministack/reset is in flight, wait for it to finish before
    # serving this request. The lock is uncontended in steady state
    # (acquire/release is near-free); during a reset, new requests block
    # until state-wipe completes so no test can observe a half-reset server.
    if path != "/_ministack/reset":
        async with _get_reset_lock():
            pass

    # Set per-request account ID from credentials (multi-tenancy support).
    # If the access key is a 12-digit number, it becomes the account ID.
    _access_key = extract_access_key_id(headers)
    if _access_key:
        set_request_account_id(_access_key)

    # Set per-request region from SigV4 Credential scope so CFN's AWS::Region
    # pseudo-param and ARN-building use the caller's region, not MINISTACK_REGION
    # (issue #398). Falls back to MINISTACK_REGION env.
    set_request_region(extract_region(headers))

    if await _send_if_handled(send, await _handle_pre_body_request(method, path, headers, query_params, request_id)):
        return

    body = await _read_request_body(receive, method, headers)

    if await _send_if_handled(send, await _handle_post_body_shortcuts(method, path, headers, body, query_params)):
        return

    if await _send_if_handled(send, await _handle_special_data_plane_request(
        method, path, headers, body, query_params, request_id
    )):
        return

    await _send_response(send, *await _dispatch_service_request(method, path, headers, body, query_params, request_id))


# ---------------------------------------------------------------------------
# Lifecycle, init scripts, and server administration
# ---------------------------------------------------------------------------

async def _handle_lifespan(scope, receive, send):
    """Handle ASGI lifespan events."""
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            port = _resolve_port()
            logger.info(BANNER.format(port=port))
            # Install a larger default thread-pool executor. Lambda invocations
            # (warm pool subprocess spawn, RIE HTTP, provided-runtime) all ride
            # on asyncio.to_thread; Python's default is min(32, cpu+4) which
            # is only 6 on a 2-core CI runner. Under xdist that queues cold
            # starts behind other blocking work and test urlopen timeouts fire
            # before the handler ever runs. 64 is plenty — threads are cheap
            # and idle. Override with MINISTACK_WORKER_THREADS.
            import concurrent.futures
            _max_workers = int(os.environ.get("MINISTACK_WORKER_THREADS", "64"))
            asyncio.get_running_loop().set_default_executor(
                concurrent.futures.ThreadPoolExecutor(
                    max_workers=_max_workers,
                    thread_name_prefix="ministack-worker",
                )
            )
            logger.info("Worker thread pool: %d threads", _max_workers)
            _run_init_scripts()
            if PERSIST_STATE:
                _load_persisted_state()
            # Start the Transfer Family SFTP listener after persistence is
            # loaded (so any restored Transfer servers/users are visible to
            # the SSH auth callback). Cheap no-op if asyncssh isn't
            # installed or SFTP_ENABLED=0.
            try:
                from ministack.services import transfer
                await transfer.sftp_start()
            except Exception as e:
                logger.warning("Transfer SFTP startup failed: %s", e)
            await send({"type": "lifespan.startup.complete"})
            logger.info("Ready.")
            for svc in SERVICE_HANDLERS:
                logger.info("%s init completed.", svc.capitalize())
            asyncio.create_task(_run_ready_scripts())
        elif message["type"] == "lifespan.shutdown":
            logger.info("MiniStack shutting down...")
            if PERSIST_STATE:
                # Only save state for modules that were actually loaded
                save_dict = {}
                for key, mod_name in _state_map.items():
                    if mod_name in _loaded_modules:
                        save_dict[key] = _loaded_modules[mod_name].get_state
                save_all(save_dict)
            try:
                from ministack.services import transfer
                await transfer.sftp_stop()
            except Exception as e:
                logger.debug("Transfer SFTP shutdown error: %s", e)
            _stop_docker_containers()
            await send({"type": "lifespan.shutdown.complete"})
            return


def _stop_docker_containers():
    """Stop all Docker containers managed by MiniStack (RDS, ECS, ElastiCache).
    Uses container labels to find them — does not touch service state."""
    try:
        import docker
        client = docker.from_env()
    except Exception:
        return
    for label in ("ministack=rds", "ministack=ecs", "ministack=elasticache", "ministack=eks", "ministack=lambda"):
        try:
            for c in client.containers.list(filters={"label": label}):
                try:
                    c.stop(timeout=5)
                    c.remove(v=True)
                except Exception:
                    pass
        except Exception:
            pass


def _load_persisted_state():
    """Load persisted state for services that support it."""
    for svc_key in ("apigateway", "apigateway_v1", "servicediscovery"):
        data = load_state(svc_key)
        if data:
            _get_module(svc_key).load_persisted_state(data)
            logger.info("Loaded persisted state for %s", svc_key)

    # Eagerly import persisted services whose restore path depends on
    # a module-level `load_state()` side-effect, but which would not
    # otherwise be imported during startup. These are NOT covered by
    # the explicit central-restore loop above (no
    # `load_persisted_state` method), and the lazy router will not
    # pull them in early enough — for example, `ses_v2` is reached
    # via the `/v2/email/*` path-prefix shortcut and `pipes` via
    # CloudFormation, neither of which fires at lifespan startup.
    # Importing here triggers the restore (and, for `pipes`, also
    # restarts the background poller for any RUNNING pipe). Keep this
    # list narrow — every entry costs a cold-start import. Enforced
    # by `tests/test_persistence_symmetry.py::test_state_map_
    # services_without_endpoint_are_eagerly_imported`.
    for svc_key in ("pipes", "ses_v2"):
        _get_module(svc_key)


async def _wait_for_port(port, timeout=30):
    """Wait until the server is accepting TCP connections."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.1)
    logger.warning('Server did not become ready within %ds — skipping ready.d scripts', timeout)


async def _run_ready_scripts():
    """Execute .sh/.py scripts from ready.d directories after the server is ready."""
    scripts = _collect_scripts('/docker-entrypoint-initaws.d/ready.d', '/etc/localstack/init/ready.d')
    if not scripts:
        _ready_scripts_state.update({"status": "completed", "total": 0, "completed": 0, "failed": 0})
        return
    _ready_scripts_state.update({"status": "running", "total": len(scripts), "completed": 0, "failed": 0})
    port = int(_resolve_port())
    await _wait_for_port(port)
    logger.info('Found %d ready script(s)', len(scripts))
    # Provide sensible defaults so init scripts can use aws cli / boto3
    # without requiring manual credential configuration.  Skip credential
    # defaults when the user has mounted ~/.aws/credentials so the CLI
    # respects their configured profile.
    script_env = {**os.environ}
    _creds_paths = [os.path.expanduser("~/.aws"), "/root/.aws"]
    _custom_creds = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")
    _has_creds_file = (_custom_creds and os.path.isfile(_custom_creds)) or any(
        os.path.isfile(os.path.join(d, "credentials")) for d in _creds_paths
    )
    if not _has_creds_file:
        script_env.setdefault("AWS_ACCESS_KEY_ID", "test")
        script_env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    script_env.setdefault("AWS_DEFAULT_REGION", os.environ.get("MINISTACK_REGION", "us-east-1"))
    script_env.setdefault("AWS_ENDPOINT_URL", f"http://{_MINISTACK_HOST}:{port}")
    for script_path in scripts:
        logger.info('Running ready script: %s', script_path)
        script_failed = False
        try:
            cmd = [sys.executable, script_path] if script_path.endswith('.py') else ['sh', script_path]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=script_env,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if stdout:
                logger.info('  stdout: %s', stdout.decode('utf-8', errors='replace').rstrip())
            if proc.returncode != 0:
                script_failed = True
                logger.error('Ready script %s failed (exit %d): %s', script_path, proc.returncode,
                             stderr.decode('utf-8', errors='replace'))
            else:
                logger.info('Ready script %s completed successfully', script_path)
        except asyncio.TimeoutError:
            script_failed = True
            logger.error('Ready script %s timed out after 300s', script_path)
            proc.kill()
        except Exception as e:
            script_failed = True
            logger.error('Failed to execute ready script %s: %s', script_path, e)
        _ready_scripts_state["completed"] += 1
        if script_failed:
            _ready_scripts_state["failed"] += 1
    _ready_scripts_state["status"] = "completed"


def _collect_scripts(*dirs):
    """Collect .sh/.py scripts from multiple directories, deduped by filename."""
    seen = {}
    for d in dirs:
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.endswith(('.sh', '.py')) and f not in seen:
                seen[f] = os.path.join(d, f)
    return [seen[f] for f in sorted(seen)]


def _run_init_scripts():
    """Execute .sh/.py scripts from init directories in alphabetical order."""
    scripts = _collect_scripts('/docker-entrypoint-initaws.d', '/etc/localstack/init/boot.d')
    if not scripts:
        return
    logger.info("Found %d init script(s)", len(scripts))
    for script_path in scripts:
        logger.info("Running init script: %s", script_path)
        try:
            cmd = [sys.executable, script_path] if script_path.endswith('.py') else ["sh", script_path]
            result = subprocess.run(
                cmd, env=os.environ,
                capture_output=True, text=True, timeout=300,
            )
            if result.stdout:
                logger.info("  stdout: %s", result.stdout.rstrip())
            if result.returncode != 0:
                logger.error("Init script %s failed (exit %d): %s", script_path, result.returncode, result.stderr)
            else:
                logger.info("Init script %s completed successfully", script_path)
        except subprocess.TimeoutExpired:
            logger.error("Init script %s timed out after 300s", script_path)
        except Exception as e:
            logger.error("Failed to execute init script %s: %s", script_path, e)


def _reset_all_state():
    """Wipe all in-memory state across every service module, and persisted files if enabled."""

    from ministack.core.persistence import PERSIST_STATE, STATE_DIR

    # Stateful modules that don't have a routing entry in SERVICE_REGISTRY but
    # still need reset() — REST API v1 (served via the apigateway module),
    # SES v2 (served via the ses module), and EventBridge Pipes (CFN-only
    # provisioner with a background poller thread that reset() must stop).
    _extra_reset_modules = ("apigateway_v1", "ses_v2", "pipes")

    module_names = {cfg["module"] for cfg in SERVICE_REGISTRY.values()}
    module_names.update(_extra_reset_modules)

    for mod_name in module_names:
        if mod_name in _loaded_modules:
            mod = _loaded_modules[mod_name]
            try:
                mod.reset()
            except Exception as e:
                logger.warning("reset() failed for %s: %s", mod_name, e)

    S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/ministack-data/s3")
    S3_PERSIST = os.environ.get("S3_PERSIST", "0") == "1"

    # Wipe persisted files so a subsequent restart doesn't reload old state
    if PERSIST_STATE and os.path.isdir(STATE_DIR):
        for fname in os.listdir(STATE_DIR):
            if fname.endswith(".json"):
                try:
                    os.remove(os.path.join(STATE_DIR, fname))
                except Exception as e:
                    logger.warning("reset: failed to remove %s: %s", fname, e)
        logger.info("Wiped persisted state files in %s", STATE_DIR)

    if S3_PERSIST and os.path.isdir(S3_DATA_DIR):
        for entry in os.listdir(S3_DATA_DIR):
            entry_path = os.path.join(S3_DATA_DIR, entry)
            try:
                if os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)
                else:
                    os.remove(entry_path)
            except Exception as e:
                logger.warning("reset: failed to remove S3 data %s: %s", entry, e)
        logger.info("Wiped S3 persisted data in %s", S3_DATA_DIR)

    logger.info("State reset complete")


def _pid_file(port: int) -> str:
    return os.path.join(tempfile.gettempdir(), f"ministack-{port}.pid")


def main():
    from hypercorn.config import Config as HypercornConfig
    from hypercorn.asyncio import serve as hypercorn_serve

    parser = argparse.ArgumentParser(description="MiniStack — Local AWS Service Emulator")
    parser.add_argument("-d", "--detach", action="store_true", help="Run in the background (detached mode)")
    parser.add_argument("--stop", action="store_true", help="Stop a detached MiniStack server")
    args = parser.parse_args()

    port = int(_resolve_port())
    # BIND_HOST controls the bind interface; defaults to 0.0.0.0 (existing
    # behaviour). Distinct from MINISTACK_HOST, which is the virtual hostname
    # used for S3 virtual-host / execute-api URL matching.
    bind_host = os.environ.get("BIND_HOST", "0.0.0.0")

    if args.stop:
        pf = _pid_file(port)
        if not os.path.exists(pf):
            print(f"No MiniStack PID file found for port {port}. Is it running?")
            raise SystemExit(1)
        with open(pf) as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"MiniStack (PID {pid}) on port {port} stopped.")
        except ProcessLookupError:
            print(f"MiniStack (PID {pid}) was not running. Cleaning up PID file.")
        os.remove(pf)
        return

    # 0.0.0.0 binds every interface so 127.0.0.1 always works as a probe;
    # for an explicit BIND_HOST, probe that host directly.
    probe_host = "127.0.0.1" if bind_host == "0.0.0.0" else bind_host
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex((probe_host, port)) == 0:
            print(f"ERROR: {probe_host}:{port} is already in use. Is MiniStack already running?\n"
                  f"  Stop it with: ministack --stop\n"
                  f"  Or use a different port: GATEWAY_PORT=4567 ministack")
            raise SystemExit(1)

    if args.detach:
        log_file = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"ministack-{port}.log")
        # Keep a reference to the log file handle — Popen inherits the fd so
        # closing it here would break child process logging.  The handle is
        # intentionally kept open for the lifetime of this (short-lived) parent
        # process; the OS reclaims it when the parent exits.
        log_fh = open(log_file, "w")
        proc = subprocess.Popen(
            [sys.executable, "-m", "hypercorn", "ministack.app:app",
             "--bind", f"{bind_host}:{port}",
             "--log-level", LOG_LEVEL.upper(),
             "--keep-alive", "75"],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        pf = _pid_file(port)
        with open(pf, "w") as f:
            f.write(str(proc.pid))
        print(f"MiniStack started in background (PID {proc.pid}) on port {port}.")
        print(f"  Logs: {log_file}")
        print(f"  Stop: ministack --stop")
        return

    # Foreground — write PID file and clean up on exit
    pf = _pid_file(port)
    with open(pf, "w") as f:
        f.write(str(os.getpid()))

    def _cleanup(*_):
        try:
            os.remove(pf)
        except OSError:
            pass

    signal.signal(signal.SIGTERM, lambda *_: (_cleanup(), sys.exit(0)))
    try:
        # Suppress health-check access logs at INFO level (reported by @McDoit).
        # Visible when LOG_LEVEL=DEBUG.
        class _HealthLogFilter(logging.Filter):
            def filter(self, record):
                if LOG_LEVEL == "DEBUG":
                    return True
                return not any(p in record.getMessage() for p in _HEALTH_PATHS)

        logging.getLogger("hypercorn.access").addFilter(_HealthLogFilter())

        config = HypercornConfig()
        config.bind = [f"{bind_host}:{port}"]
        config.keep_alive_timeout = 75
        config.loglevel = LOG_LEVEL.upper()

        asyncio.run(hypercorn_serve(app, config))
    finally:
        _cleanup()


if __name__ == "__main__":
    main()
