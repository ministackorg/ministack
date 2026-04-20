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
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import uuid
from urllib.parse import parse_qs, urlparse

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_MINISTACK_PORT = os.environ.get("GATEWAY_PORT", "4566")

try:
    from importlib.metadata import version as _pkg_version
    _VERSION = _pkg_version("ministack")
except Exception:
    _VERSION = "dev"

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

from ministack.core.hypercorn_compat import install as _install_hypercorn_compat
from ministack.core.persistence import PERSIST_STATE, load_state, save_all
from ministack.core.responses import set_request_account_id
from ministack.core.router import detect_service, extract_access_key_id, extract_account_id, extract_region

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


async def app(scope, receive, send):
    """ASGI application entry point."""
    if scope["type"] == "lifespan":
        await _handle_lifespan(scope, receive, send)
        return

    if scope["type"] == "websocket":
        # WebSocket APIs only reach us via the execute-api host pattern:
        #   ws://{apiId}.execute-api.{host}[:port]/{stage}[/...]
        ws_headers = {}
        for name, value in scope.get("headers", []):
            try:
                ws_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
            except UnicodeDecodeError:
                ws_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
        ws_host = ws_headers.get("host", "")
        m = _EXECUTE_API_RE.match(ws_host)
        if not m:
            # Not an execute-api host — reject the upgrade.
            msg = await receive()
            if msg.get("type") == "websocket.connect":
                await send({"type": "websocket.close", "code": 1008})
            return
        ws_api_id = m.group(1)
        try:
            await _get_module("apigateway").handle_websocket(scope, receive, send, ws_api_id)
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

    body = b""
    has_body = headers.get("content-length") or headers.get("transfer-encoding")
    if has_body or method in ("POST", "PUT", "PATCH"):
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break

    # AWS SDK v2 sends PutObject with Transfer-Encoding: chunked and
    # x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER].
    # Decode the AWS chunked format: each chunk is "<hex>;chunk-signature=...\r\n<data>\r\n"
    # terminated by "0;chunk-signature=...\r\n".
    sha256_header = headers.get("x-amz-content-sha256", "")
    content_encoding = headers.get("content-encoding", "")
    if sha256_header.startswith("STREAMING-") or "aws-chunked" in content_encoding or headers.get("x-amz-decoded-content-length"):
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
        if decoded or not body:
            body = decoded
        if "aws-chunked" in content_encoding:
            ce = [p.strip() for p in content_encoding.split(",") if p.strip() != "aws-chunked"]
            if ce:
                headers["content-encoding"] = ", ".join(ce)
            else:
                headers.pop("content-encoding", None)

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

    # Lambda layer content download: /_ministack/lambda-layers/{name}/{ver}/content
    if path.startswith("/_ministack/lambda-layers/") and method == "GET":
        lp = path.split("/")  # ['', '_ministack', 'lambda-layers', name, ver, 'content']
        if len(lp) >= 6 and lp[5] == "content" and lp[4].isdigit():
            status, resp_headers, resp_body = _get_module("lambda_svc").serve_layer_content(lp[3], int(lp[4]))
            await _send_response(send, status, resp_headers, resp_body)
            return

    # Lambda function code download (pre-signed-style URL target for
    # GetFunction's Code.Location): /_ministack/lambda-code/{fn}
    if path.startswith("/_ministack/lambda-code/") and method == "GET":
        lp = path.split("/")  # ['', '_ministack', 'lambda-code', fn]
        if len(lp) >= 4:
            status, resp_headers, resp_body = _get_module("lambda_svc").serve_function_code(lp[3])
            await _send_response(send, status, resp_headers, resp_body)
            return

    # Cognito JWKS / OpenID Configuration well-known endpoints
    # Path: /{poolId}/.well-known/jwks.json  or  /{poolId}/.well-known/openid-configuration
    if "/.well-known/" in path and method == "GET":
        if path.endswith("/.well-known/jwks.json"):
            _pool_id = path.rsplit("/.well-known/jwks.json", 1)[0].lstrip("/")
            if _pool_id:
                _region = extract_region(headers) or "us-east-1"
                status, resp_headers, resp_body = _get_module("cognito").well_known_jwks(_pool_id)
                await _send_response(send, status, resp_headers, resp_body)
                return
        elif path.endswith("/.well-known/openid-configuration"):
            _pool_id = path.rsplit("/.well-known/openid-configuration", 1)[0].lstrip("/")
            if _pool_id:
                _region = extract_region(headers) or "us-east-1"
                status, resp_headers, resp_body = _get_module("cognito").well_known_openid_configuration(_pool_id, _region)
                await _send_response(send, status, resp_headers, resp_body)
                return

    # Cognito OAuth2 / Managed Login UI endpoints
    if path == "/oauth2/authorize" and method == "GET":
        status, resp_headers, resp_body = _get_module("cognito").handle_oauth2_authorize(method, path, headers, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return
    if path in ("/oauth2/login", "/login") and method == "POST":
        status, resp_headers, resp_body = _get_module("cognito").handle_login_submit(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return
    if path == "/oauth2/token" and method == "POST":
        status, resp_headers, resp_body = _get_module("cognito").handle_oauth2_token(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return
    if path in ("/oauth2/userInfo", "/oauth2/userinfo") and method in ("GET", "POST"):
        status, resp_headers, resp_body = _get_module("cognito").handle_oauth2_userinfo(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return
    if path == "/logout" and method == "GET":
        status, resp_headers, resp_body = _get_module("cognito").handle_logout(method, path, headers, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return

    # Admin endpoints — no wildcard CORS headers (return early, before CORS block)
    if path == "/_ministack/reset" and method == "POST":
        # Hold the reset lock exclusively so in-flight requests drain first
        # and no new requests can interleave with service state wipes.
        async with _get_reset_lock():
            await asyncio.to_thread(_reset_all_state)
        run_init = query_params.get("init", [""])[0] == "1"
        if run_init:
            _run_init_scripts()
        await _send_response(send, 200, {"Content-Type": "application/json"},
                             json.dumps({"reset": "ok"}).encode())
        if run_init:
            _ready_scripts_state.update({"status": "pending", "total": 0, "completed": 0, "failed": 0})
            asyncio.create_task(_run_ready_scripts())
        return

    if path == "/_ministack/config" and method == "POST":
        _ALLOWED_CONFIG_KEYS = {
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
            if key not in _ALLOWED_CONFIG_KEYS:
                logger.warning("/_ministack/config: rejected key %s (not in whitelist)", key)
                continue
            if "." in key:
                mod_name, var_name = key.rsplit(".", 1)
                try:
                    mod = __import__(f"ministack.services.{mod_name}", fromlist=[var_name])
                    # Validate SFN_WAIT_SCALE before applying
                    if key == "stepfunctions._SFN_WAIT_SCALE":
                        try:
                            fv = float(value)
                        except (ValueError, TypeError):
                            logger.warning("/_ministack/config: invalid SFN_WAIT_SCALE=%r", value)
                            continue
                        if not __import__("math").isfinite(fv) or fv < 0:
                            logger.warning("/_ministack/config: invalid SFN_WAIT_SCALE=%r", value)
                            continue
                        value = fv
                    setattr(mod, var_name, value)
                    applied[key] = value
                except (ImportError, AttributeError) as e:
                    logger.warning("/_ministack/config: failed to set %s: %s", key, e)
        await _send_response(send, 200, {"Content-Type": "application/json"},
                             json.dumps({"applied": applied}).encode())
        return

    # S3 Control API — /v20180820/... with x-amz-account-id header
    if path.startswith("/v20180820/"):
        if path.startswith("/v20180820/tags/"):
            from urllib.parse import unquote
            raw_arn = path[len("/v20180820/tags/"):]
            arn = unquote(raw_arn)
            # ARN format: arn:aws:s3:::bucket-name  or  arn:aws:s3:::bucket-name/object-key
            bucket_name = arn.split(":::")[-1].split("/")[0] if ":::" in arn else arn.split("/")[0]

            if method == "GET":
                # ListTagsForResource — return real tags from _get_module("s3")._bucket_tags
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
                await _send_response(send, 200, {
                    "Content-Type": "application/xml",
                    "x-amzn-requestid": request_id,
                }, xml_body)
            elif method == "PUT":
                # TagResource — merge tags into _get_module("s3")._bucket_tags
                try:
                    payload = json.loads(body) if body else {}
                    new_tags = {t["Key"]: t["Value"] for t in payload.get("Tags", [])}
                    existing = _get_module("s3")._bucket_tags.get(bucket_name, {})
                    existing.update(new_tags)
                    _get_module("s3")._bucket_tags[bucket_name] = existing
                except Exception as e:
                    logger.warning("S3 Control TagResource parse error: %s", e)
                await _send_response(send, 204, {
                    "x-amzn-requestid": request_id,
                }, b"")
            elif method == "DELETE":
                # UntagResource — remove specified keys
                keys_to_remove = query_params.get("tagKeys", [])
                if isinstance(keys_to_remove, str):
                    keys_to_remove = [keys_to_remove]
                tags = _get_module("s3")._bucket_tags.get(bucket_name, {})
                for k in keys_to_remove:
                    tags.pop(k, None)
                _get_module("s3")._bucket_tags[bucket_name] = tags
                await _send_response(send, 204, {
                    "x-amzn-requestid": request_id,
                }, b"")
            else:
                await _send_response(send, 200, {
                    "Content-Type": "application/json",
                    "x-amzn-requestid": request_id,
                }, b"{}")
        else:
            # All other S3 Control operations — accept silently
            await _send_response(send, 200, {
                "Content-Type": "application/json",
                "x-amzn-requestid": request_id,
            }, b"{}")
        return

    # RDS Data API — /Execute, /BeginTransaction, /CommitTransaction, /RollbackTransaction, /BatchExecute
    if path in ("/Execute", "/BeginTransaction", "/CommitTransaction", "/RollbackTransaction", "/BatchExecute"):
        status, resp_headers, resp_body = await _get_module("rds_data").handle_request(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return

    # SES v2 REST API — /v2/email/...
    if path.startswith("/v2/email"):
        status, resp_headers, resp_body = await _get_module("ses_v2").handle_request(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return

    if path in ("/_localstack/health", "/health", "/_ministack/health"):
        await _send_response(send, 200, {
            "Content-Type": "application/json",
            "x-amzn-requestid": request_id,
        }, json.dumps({
            "services": {s: "available" for s in SERVICE_HANDLERS},
            "edition": "light",
            "version": _VERSION,
            "ready_scripts": dict(_ready_scripts_state),
        }).encode())
        return

    # Readiness endpoint — returns 503 until all ready.d scripts finish, then 200.
    # Point a compose healthcheck here to gate depends_on: service_healthy on script completion.
    if path == "/_ministack/ready":
        ready = _ready_scripts_state["status"] == "completed"
        status = 200 if ready else 503
        await _send_response(send, status, {
            "Content-Type": "application/json",
            "x-amzn-requestid": request_id,
        }, json.dumps(dict(_ready_scripts_state)).encode())
        return

    if method == "OPTIONS":
        await _send_response(send, 200, {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Expose-Headers": "*",
            "Access-Control-Max-Age": "86400",
            "Content-Length": "0",
            "x-amzn-requestid": request_id,
        }, b"")
        return

    # API Gateway execute-api data plane: host = {apiId}.execute-api.localhost[:{port}]
    host = headers.get("host", "")
    _execute_match = _EXECUTE_API_RE.match(host)
    if _execute_match:
        api_id = _execute_match.group(1)
        # Path format: /{stage}/{proxy+}  or just /{proxy+} if stage is $default
        path_parts = path.lstrip("/").split("/", 1)
        stage = path_parts[0] if path_parts else "$default"
        execute_path = "/" + path_parts[1] if len(path_parts) > 1 else "/"
        try:
            # WebSocket @connections management API — lives on the execute-api host
            # at /{stage}/@connections/{connectionId}. Detect and dispatch before
            # normal REST/HTTP routing.
            conn_prefix = f"/{stage}/@connections/"
            if path.startswith(conn_prefix):
                connection_id = path[len(conn_prefix):].split("/", 1)[0]
                status, resp_headers, resp_body = await _get_module("apigateway").handle_connections_api(
                    method, api_id, stage, connection_id, body, headers
                )
            elif api_id in _get_module("apigateway_v1")._rest_apis:
                status, resp_headers, resp_body = await _get_module("apigateway_v1").handle_execute(
                    api_id, stage, method, execute_path, headers, body, query_params
                )
            else:
                status, resp_headers, resp_body = await _get_module("apigateway").handle_execute(
                    api_id, stage, execute_path, method, headers, body, query_params
                )
        except Exception as e:
            logger.exception("Error in execute-api dispatch: %s", e)
            status, resp_headers, resp_body = 500, {"Content-Type": "application/json"}, json.dumps({"message": str(e)}).encode()
        resp_headers.update({
            "Access-Control-Allow-Origin": "*",
            "x-amzn-requestid": request_id,
            "x-amz-request-id": request_id,
        })
        await _send_response(send, status, resp_headers, resp_body)
        return

    # ALB data-plane — two addressing modes:
    #   1. Host header matches a configured ALB DNS name or {lb-name}.alb.localhost
    #   2. Path prefix /_alb/{lb-name}/...  (no DNS config needed for local testing)
    _alb_lb = _get_module("alb").find_lb_for_host(host)
    if _alb_lb is None and path.startswith("/_alb/"):
        _alb_path_parts = path[6:].split("/", 1)
        _alb_lb = _get_module("alb")._find_lb_by_name(_alb_path_parts[0])
        if _alb_lb:
            path = "/" + _alb_path_parts[1] if len(_alb_path_parts) > 1 else "/"

    if _alb_lb:
        _alb_port = 80
        if ":" in host:
            try:
                _alb_port = int(host.rsplit(":", 1)[-1])
            except ValueError:
                pass
        try:
            status, resp_headers, resp_body = await _get_module("alb").dispatch_request(
                _alb_lb, method, path, headers, body, query_params, _alb_port
            )
        except Exception as e:
            logger.exception("Error in ALB data-plane dispatch: %s", e)
            status, resp_headers, resp_body = (
                500, {"Content-Type": "application/json"},
                json.dumps({"message": str(e)}).encode(),
            )
        resp_headers.update({
            "Access-Control-Allow-Origin": "*",
            "x-amzn-requestid": request_id,
            "x-amz-request-id": request_id,
        })
        await _send_response(send, status, resp_headers, resp_body)
        return

    # Virtual-hosted S3: {bucket}.localhost[:{port}] — rewrite to path-style and forward to S3
    _s3_vhost = _S3_VHOST_RE.match(host)
    if _s3_vhost and not _execute_match and not _S3_VHOST_EXCLUDE_RE.search(host):
        bucket = _s3_vhost.group(1)
        _non_s3_hosts = {"s3", "s3-control", "sqs", "sns", "dynamodb", "lambda", "iam", "sts",
                         "secretsmanager", "logs", "ssm", "events", "kinesis",
                         "monitoring", "ses", "states", "ecs", "rds", "rds-data", "elasticache",
                         "glue", "athena", "apigateway", "cloudformation", "autoscaling", "codebuild", "transfer"}
        if bucket not in _non_s3_hosts:
            vhost_path = "/" + bucket + path if path != "/" else "/" + bucket + "/"
            try:
                status, resp_headers, resp_body = await _get_module("s3").handle_request(
                    method, vhost_path, headers, body, query_params
                )
            except Exception as e:
                logger.exception("Error handling virtual-hosted S3 request: %s", e)
                from xml.sax.saxutils import escape as _xml_esc
                status, resp_headers, resp_body = 500, {"Content-Type": "application/xml"}, (
                    f"<Error><Code>InternalError</Code><Message>{_xml_esc(str(e))}</Message></Error>".encode()
                )
            resp_headers.update({
                "Access-Control-Allow-Origin": "*",
                "x-amzn-requestid": request_id,
                "x-amz-request-id": request_id,
                "x-amz-id-2": base64.b64encode(os.urandom(48)).decode(),
            })
            await _send_response(send, status, resp_headers, resp_body)
            return

    # For unsigned form-encoded requests (e.g. STS AssumeRoleWithWebIdentity),
    # Action is in the body not the query string — merge it in for routing only.
    routing_params = query_params
    if not query_params.get("Action") and headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
        body_params = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        if body_params.get("Action"):
            routing_params = {**query_params, "Action": body_params["Action"]}

    service = detect_service(method, path, headers, routing_params)
    region = extract_region(headers)

    logger.debug("%s %s -> service=%s region=%s", method, path, service, region)

    handler = SERVICE_HANDLERS.get(service)
    if not handler:
        await _send_response(send, 400, {"Content-Type": "application/json"},
            json.dumps({"error": f"Unsupported service: {service}"}).encode())
        return

    try:
        status, resp_headers, resp_body = await handler(method, path, headers, body, query_params)
    except Exception as e:
        logger.exception("Error handling %s request: %s", service, e)
        await _send_response(send, 500, {"Content-Type": "application/json"},
            json.dumps({"__type": "InternalError", "message": str(e)}).encode())
        return

    resp_headers.update({
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "*",
        "x-amzn-requestid": request_id,
        "x-amz-request-id": request_id,
        "x-amz-id-2": base64.b64encode(os.urandom(48)).decode(),
    })

    await _send_response(send, status, resp_headers, resp_body)


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


async def _handle_lifespan(scope, receive, send):
    """Handle ASGI lifespan events."""
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            port = _resolve_port()
            logger.info(BANNER.format(port=port))
            _run_init_scripts()
            if PERSIST_STATE:
                _load_persisted_state()
            await send({"type": "lifespan.startup.complete"})
            logger.info("Ready.")
            for svc in SERVICE_HANDLERS:
                logger.info("%s init completed.", svc.capitalize())
            asyncio.create_task(_run_ready_scripts())
        elif message["type"] == "lifespan.shutdown":
            logger.info("MiniStack shutting down...")
            if PERSIST_STATE:
                # Only save state for modules that were actually loaded
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
                    "eks": "eks",
                }
                save_dict = {}
                for key, mod_name in _state_map.items():
                    if mod_name in _loaded_modules:
                        save_dict[key] = _loaded_modules[mod_name].get_state
                save_all(save_dict)
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

    for service_config in SERVICE_REGISTRY.values():
        mod_name = service_config["module"]
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

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", port)) == 0:
            print(f"ERROR: Port {port} is already in use. Is MiniStack already running?\n"
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
             "--bind", f"0.0.0.0:{port}",
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
        _HEALTH_PATHS = ("/_ministack/health", "/_localstack/health", "/health")

        class _HealthLogFilter(logging.Filter):
            def filter(self, record):
                if LOG_LEVEL == "DEBUG":
                    return True
                return not any(p in record.getMessage() for p in _HEALTH_PATHS)

        logging.getLogger("hypercorn.access").addFilter(_HealthLogFilter())

        config = HypercornConfig()
        config.bind = [f"0.0.0.0:{port}"]
        config.keep_alive_timeout = 75
        config.loglevel = LOG_LEVEL.upper()

        asyncio.run(hypercorn_serve(app, config))
    finally:
        _cleanup()


if __name__ == "__main__":
    main()
