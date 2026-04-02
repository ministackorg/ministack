"""
MiniStack — Local AWS Service Emulator.
Single-port ASGI application on port 4566 (configurable via GATEWAY_PORT).
Routes requests to service handlers based on AWS headers, paths, and query parameters.
Compatible with AWS CLI, boto3, and any AWS SDK via --endpoint-url.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import uuid
from urllib.parse import parse_qs, urlparse

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_MINISTACK_PORT = os.environ.get("GATEWAY_PORT", "4566")

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

from ministack.core.persistence import PERSIST_STATE, load_state, save_all
from ministack.core.router import detect_service, extract_account_id, extract_region
from ministack.services import (
    acm,
    alb,
    apigateway,
    apigateway_v1,
    athena,
    cloudformation,
    cloudwatch,
    cloudwatch_logs,
    cognito,
    dynamodb,
    ec2,
    ecs,
    efs,
    elasticache,
    emr,
    eventbridge,
    firehose,
    glue,
    kinesis,
    lambda_svc,
    rds,
    route53,
    s3,
    secretsmanager,
    ses,
    ses_v2,
    sns,
    sqs,
    ssm,
    stepfunctions,
    waf,
)
from ministack.services.iam_sts import handle_iam_request, handle_sts_request

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ministack")

SERVICE_HANDLERS = {
    "s3": s3.handle_request,
    "sqs": sqs.handle_request,
    "cloudformation": cloudformation.handle_request,
    "sns": sns.handle_request,
    "dynamodb": dynamodb.handle_request,
    "lambda": lambda_svc.handle_request,
    "iam": handle_iam_request,
    "sts": handle_sts_request,
    "secretsmanager": secretsmanager.handle_request,
    "logs": cloudwatch_logs.handle_request,
    "ssm": ssm.handle_request,
    "events": eventbridge.handle_request,
    "kinesis": kinesis.handle_request,
    "monitoring": cloudwatch.handle_request,
    "ses": ses.handle_request,
    "acm": acm.handle_request,
    "wafv2": waf.handle_request,
    "states": stepfunctions.handle_request,
    "ecs": ecs.handle_request,
    "rds": rds.handle_request,
    "elasticache": elasticache.handle_request,
    "glue": glue.handle_request,
    "athena": athena.handle_request,
    "apigateway": apigateway.handle_request,
    "firehose": firehose.handle_request,
    "route53": route53.handle_request,
    "cognito-idp": cognito.handle_request,
    "cognito-identity": cognito.handle_request,
    "ec2": ec2.handle_request,
    "elasticmapreduce": emr.handle_request,
    "elasticloadbalancing": alb.handle_request,
    "elasticfilesystem": efs.handle_request,
}

SERVICE_NAME_ALIASES = {
    "cloudwatch-logs": "logs",
    "cloudwatch": "monitoring",
    "eventbridge": "events",
    "step-functions": "states",
    "stepfunctions": "states",
    "execute-api": "apigateway",
    "apigatewayv2": "apigateway",
    "kinesis-firehose": "firehose",
    "route53": "route53",
    "cognito-idp": "cognito-idp",
    "cognito-identity": "cognito-identity",
    "elbv2": "elasticloadbalancing",
    "elb": "elasticloadbalancing",
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
          Cognito, EC2, EMR, EBS, EFS, ALB/ELBv2, CloudFormation
"""


async def app(scope, receive, send):
    """ASGI application entry point."""
    if scope["type"] == "lifespan":
        await _handle_lifespan(scope, receive, send)
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
    if sha256_header.startswith("STREAMING-"):
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

    request_id = str(uuid.uuid4())

    # Lambda layer content download: /_ministack/lambda-layers/{name}/{ver}/content
    if path.startswith("/_ministack/lambda-layers/") and method == "GET":
        lp = path.split("/")  # ['', '_ministack', 'lambda-layers', name, ver, 'content']
        if len(lp) >= 6 and lp[5] == "content" and lp[4].isdigit():
            from ministack.services import lambda_svc
            status, resp_headers, resp_body = lambda_svc.serve_layer_content(lp[3], int(lp[4]))
            await _send_response(send, status, resp_headers, resp_body)
            return

    if path == "/_ministack/reset" and method == "POST":
        _reset_all_state()
        await _send_response(send, 200, {"Content-Type": "application/json"},
                             json.dumps({"reset": "ok"}).encode())
        return

    if path == "/_ministack/config" and method == "POST":
        try:
            config = json.loads(body) if body else {}
        except json.JSONDecodeError:
            config = {}
        applied = {}
        for key, value in config.items():
            # Set module-level variables on service modules: "athena.ATHENA_ENGINE" -> "sqlite"
            if "." in key:
                mod_name, var_name = key.rsplit(".", 1)
                try:
                    mod = __import__(f"ministack.services.{mod_name}", fromlist=[var_name])
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
                # ListTagsForResource — return real tags from s3._bucket_tags
                tags = s3._bucket_tags.get(bucket_name, {})
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
                # TagResource — merge tags into s3._bucket_tags
                try:
                    payload = json.loads(body) if body else {}
                    new_tags = {t["Key"]: t["Value"] for t in payload.get("Tags", [])}
                    existing = s3._bucket_tags.get(bucket_name, {})
                    existing.update(new_tags)
                    s3._bucket_tags[bucket_name] = existing
                except Exception:
                    pass
                await _send_response(send, 204, {
                    "x-amzn-requestid": request_id,
                }, b"")
            elif method == "DELETE":
                # UntagResource — remove specified keys
                keys_to_remove = query_params.get("tagKeys", [])
                if isinstance(keys_to_remove, str):
                    keys_to_remove = [keys_to_remove]
                tags = s3._bucket_tags.get(bucket_name, {})
                for k in keys_to_remove:
                    tags.pop(k, None)
                s3._bucket_tags[bucket_name] = tags
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

    # SES v2 REST API — /v2/email/...
    if path.startswith("/v2/email"):
        status, resp_headers, resp_body = await ses_v2.handle_request(method, path, headers, body, query_params)
        await _send_response(send, status, resp_headers, resp_body)
        return

    if path in ("/_localstack/health", "/health", "/_ministack/health"):
        await _send_response(send, 200, {
            "Content-Type": "application/json",
            "x-amzn-requestid": request_id,
        }, json.dumps({
            "services": {s: "available" for s in SERVICE_HANDLERS},
            "edition": "light",
            "version": "3.0.0.dev",
        }).encode())
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
            if api_id in apigateway_v1._rest_apis:
                status, resp_headers, resp_body = await apigateway_v1.handle_execute(
                    api_id, stage, method, execute_path, headers, body, query_params
                )
            else:
                status, resp_headers, resp_body = await apigateway.handle_execute(
                    api_id, stage, execute_path, method, headers, body, query_params
                )
        except Exception as e:
            logger.exception(f"Error in execute-api dispatch: {e}")
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
    _alb_lb = alb.find_lb_for_host(host)
    if _alb_lb is None and path.startswith("/_alb/"):
        _alb_path_parts = path[6:].split("/", 1)
        _alb_lb = alb._find_lb_by_name(_alb_path_parts[0])
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
            status, resp_headers, resp_body = await alb.dispatch_request(
                _alb_lb, method, path, headers, body, query_params, _alb_port
            )
        except Exception as e:
            logger.exception(f"Error in ALB data-plane dispatch: {e}")
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
                         "monitoring", "ses", "states", "ecs", "rds", "elasticache",
                         "glue", "athena", "apigateway", "cloudformation"}
        if bucket not in _non_s3_hosts:
            vhost_path = "/" + bucket + path if path != "/" else "/" + bucket + "/"
            try:
                status, resp_headers, resp_body = await s3.handle_request(
                    method, vhost_path, headers, body, query_params
                )
            except Exception as e:
                logger.exception(f"Error handling virtual-hosted S3 request: {e}")
                status, resp_headers, resp_body = 500, {"Content-Type": "application/xml"}, (
                    f"<Error><Code>InternalError</Code><Message>{e}</Message></Error>".encode()
                )
            resp_headers.update({
                "Access-Control-Allow-Origin": "*",
                "x-amzn-requestid": request_id,
                "x-amz-request-id": request_id,
                "x-amz-id-2": request_id,
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

    logger.debug(f"{method} {path} -> service={service} region={region}")

    handler = SERVICE_HANDLERS.get(service)
    if not handler:
        await _send_response(send, 400, {"Content-Type": "application/json"},
            json.dumps({"error": f"Unsupported service: {service}"}).encode())
        return

    try:
        status, resp_headers, resp_body = await handler(method, path, headers, body, query_params)
    except Exception as e:
        logger.exception(f"Error handling {service} request: {e}")
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
        "x-amz-id-2": request_id,
    })

    await _send_response(send, status, resp_headers, resp_body)


async def _send_response(send, status, headers, body):
    """Send ASGI HTTP response."""
    def _encode_header_value(v: str) -> bytes:
        try:
            return v.encode("latin-1")
        except UnicodeEncodeError:
            return v.encode("utf-8")

    header_list = [(k.encode("latin-1"), _encode_header_value(str(v))) for k, v in headers.items()]
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": header_list,
    })
    await send({
        "type": "http.response.body",
        "body": body if isinstance(body, bytes) else body.encode("utf-8"),
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
        elif message["type"] == "lifespan.shutdown":
            logger.info("MiniStack shutting down...")
            if PERSIST_STATE:
                save_all({
                    "apigateway": apigateway.get_state,
                    "apigateway_v1": apigateway_v1.get_state,
                })
            await send({"type": "lifespan.shutdown.complete"})
            return


def _load_persisted_state():
    """Load persisted state for services that support it."""
    data = load_state("apigateway")
    if data:
        apigateway.load_persisted_state(data)
        logger.info("Loaded persisted state for apigateway")
    data_v1 = load_state("apigateway_v1")
    if data_v1:
        apigateway_v1.load_persisted_state(data_v1)
        logger.info("Loaded persisted state for apigateway_v1")


def _run_init_scripts():
    """Execute .sh scripts from /docker-entrypoint-initaws.d/ in alphabetical order."""
    init_dir = "/docker-entrypoint-initaws.d"
    if not os.path.isdir(init_dir):
        return
    scripts = sorted(f for f in os.listdir(init_dir) if f.endswith(".sh"))
    if not scripts:
        return
    logger.info("Found %d init script(s) in %s", len(scripts), init_dir)
    for script in scripts:
        script_path = os.path.join(init_dir, script)
        logger.info("Running init script: %s", script_path)
        try:
            result = subprocess.run(
                ["sh", script_path], env=os.environ,
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
    import shutil

    from ministack.core.persistence import PERSIST_STATE, STATE_DIR
    from ministack.services.iam_sts import reset as _iam_reset
    from ministack.services.s3 import DATA_DIR as S3_DATA_DIR
    from ministack.services.s3 import PERSIST as S3_PERSIST

    for mod, fn in [
        (s3, s3.reset), (sqs, sqs.reset), (sns, sns.reset),
        (dynamodb, dynamodb.reset), (lambda_svc, lambda_svc.reset),
        (secretsmanager, secretsmanager.reset), (cloudwatch_logs, cloudwatch_logs.reset),
        (ssm, ssm.reset), (eventbridge, eventbridge.reset), (kinesis, kinesis.reset),
        (cloudwatch, cloudwatch.reset), (ses, ses.reset),
        (stepfunctions, stepfunctions.reset), (ecs, ecs.reset),
        (rds, rds.reset), (elasticache, elasticache.reset),
        (glue, glue.reset), (athena, athena.reset),
        (apigateway, apigateway.reset),
        (apigateway_v1, apigateway_v1.reset),
        (firehose, firehose.reset),
        (route53, route53.reset),
        (cognito, cognito.reset),
        (ec2, ec2.reset),
        (emr, emr.reset),
        (alb, alb.reset),
        (acm, acm.reset),
        (ses_v2, ses_v2.reset),
        (waf, waf.reset),
        (efs, efs.reset),
        (cloudformation, cloudformation.reset),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning("reset() failed for %s: %s", mod.__name__, e)
    try:
        _iam_reset()
    except Exception as e:
        logger.warning("reset() failed for iam_sts: %s", e)

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


def main():
    import socket

    import uvicorn
    port = int(_resolve_port())
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", port)) == 0:
            print(f"ERROR: Port {port} is already in use. Is MiniStack already running (Docker or another process)?\n"
                  f"  Stop it with: docker compose down\n"
                  f"  Or use a different port: GATEWAY_PORT=4567 ministack")
            raise SystemExit(1)
    uvicorn.run("ministack.app:app", host="0.0.0.0", port=port, log_level=LOG_LEVEL.lower())


if __name__ == "__main__":
    main()
