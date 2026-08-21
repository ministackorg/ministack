"""IAM action extraction and AccessDenied response formatting.

Maps MiniStack's internal service names to IAM namespaces, extracts the
IAM action string (``service:ActionName``) from each request's protocol,
and formats per-protocol AccessDenied error responses.
"""

import json
import logging
import os
import re

logger = logging.getLogger("ministack")

# ---------------------------------------------------------------------------
# Service name → IAM namespace mapping
# ---------------------------------------------------------------------------
# Keys are the service names returned by router.detect_service().
# Values are the IAM authorization namespace (botocore signingName).

SERVICE_TO_IAM_NAMESPACE: dict[str, str] = {
    "account": "account",
    "acm": "acm",
    "airflow": "airflow",
    "apigateway": "apigateway",
    "appconfig": "appconfig",
    "appconfigdata": "appconfig",
    "appsync": "appsync",
    "appsync-events": "appsync",
    "athena": "athena",
    "autoscaling": "autoscaling",
    "backup": "backup",
    "batch": "batch",
    "bedrock": "bedrock",
    "bedrock-agent": "bedrock",
    "bedrock-agent-runtime": "bedrock",
    "bedrock-agentcore": "bedrock",
    "bedrock-runtime": "bedrock",
    "cloudcontrol": "cloudformation",
    "cloudformation": "cloudformation",
    "cloudfront": "cloudfront",
    "cloudfront-keyvaluestore": "cloudfront-keyvaluestore",
    "cloudtrail": "cloudtrail",
    "codebuild": "codebuild",
    "cognito-identity": "cognito-identity",
    "cognito-idp": "cognito-idp",
    "config": "config",
    "cur": "cur",
    "dsql": "dsql",
    "dynamodb": "dynamodb",
    "dynamodbstreams": "dynamodb",
    "ec2": "ec2",
    "ecr": "ecr",
    "ecs": "ecs",
    "ecs-metadata": "ecs",
    "eks": "eks",
    "elasticache": "elasticache",
    "elasticfilesystem": "elasticfilesystem",
    "elasticloadbalancing": "elasticloadbalancing",
    "elasticmapreduce": "elasticmapreduce",
    "events": "events",
    "firehose": "firehose",
    "glue": "glue",
    "iam": "iam",
    "imds": "ec2",
    "inspector2": "inspector2",
    "iot": "iot",
    "iot-data": "iot",
    "iot-jobs-data": "iot",
    "kafka": "kafka",
    "kinesis": "kinesis",
    "kms": "kms",
    "lambda": "lambda",
    "lambda-microvms": "lambda",
    "logs": "logs",
    "mediaconnect": "mediaconnect",
    "monitoring": "cloudwatch",
    "mq": "mq",
    "opensearch": "es",
    "organizations": "organizations",
    "pipes": "pipes",
    "rds": "rds",
    "rds-data": "rds-data",
    "resource-groups": "resource-groups",
    "route53": "route53",
    "s3": "s3",
    "s3files": "s3",
    "s3tables": "s3tables",
    "scheduler": "scheduler",
    "secretsmanager": "secretsmanager",
    "servicediscovery": "servicediscovery",
    "ses": "ses",
    "sns": "sns",
    "sqs": "sqs",
    "ssm": "ssm",
    "states": "states",
    "sts": "sts",
    "tagging": "tag",
    "transfer": "transfer",
    "waf": "waf",
    "waf-regional": "waf-regional",
    "wafv2": "wafv2",
}


# ---------------------------------------------------------------------------
# Action extraction
# ---------------------------------------------------------------------------

def _action_from_query(query_params: dict, body: bytes,
                       content_type: str) -> str | None:
    """Extract Action from query params or form-encoded body."""
    action = query_params.get("Action")
    if isinstance(action, list):
        action = action[0] if action else None
    if action:
        return action
    if body and "x-www-form-urlencoded" in (content_type or ""):
        from urllib.parse import parse_qs
        bp = parse_qs(body.decode("utf-8", "replace"), keep_blank_values=True)
        action = bp.get("Action", [None])[0]
        if action:
            return action
    return None


def _action_from_target(headers: dict) -> str | None:
    """Extract action from X-Amz-Target header (JSON protocol services)."""
    target = headers.get("x-amz-target", "")
    if "." in target:
        return target.rsplit(".", 1)[-1]
    return None


# S3 REST path → IAM action
_S3_ACTIONS: dict[tuple[str, int], str] = {
    ("GET", 0): "ListAllMyBuckets",
    ("PUT", 1): "CreateBucket",
    ("DELETE", 1): "DeleteBucket",
    ("HEAD", 1): "ListBucket",
    ("GET", 1): "ListBucket",
    ("PUT", 2): "PutObject",
    ("GET", 2): "GetObject",
    ("DELETE", 2): "DeleteObject",
    ("HEAD", 2): "GetObject",
    ("POST", 2): "PutObject",
    ("POST", 1): "DeleteObject",  # DeleteObjects (batch)
}

# S3 query-param sub-operations
_S3_QUERY_ACTIONS: dict[str, dict[str, str]] = {
    "tagging": {"GET": "GetBucketTagging", "PUT": "PutBucketTagging", "DELETE": "DeleteBucketTagging"},
    "versioning": {"GET": "GetBucketVersioning", "PUT": "PutBucketVersioning"},
    "policy": {"GET": "GetBucketPolicy", "PUT": "PutBucketPolicy", "DELETE": "DeleteBucketPolicy"},
    "cors": {"GET": "GetBucketCors", "PUT": "PutBucketCors", "DELETE": "DeleteBucketCors"},
    "lifecycle": {"GET": "GetLifecycleConfiguration", "PUT": "PutLifecycleConfiguration",
                  "DELETE": "DeleteLifecycleConfiguration"},
    "encryption": {"GET": "GetEncryptionConfiguration", "PUT": "PutEncryptionConfiguration",
                   "DELETE": "DeleteEncryptionConfiguration"},
    "notification": {"GET": "GetBucketNotification", "PUT": "PutBucketNotification"},
    "acl": {"GET": "GetBucketAcl", "PUT": "PutBucketAcl"},
    "website": {"GET": "GetBucketWebsite", "PUT": "PutBucketWebsite", "DELETE": "DeleteBucketWebsite"},
    "logging": {"GET": "GetBucketLogging", "PUT": "PutBucketLogging"},
    "replication": {"GET": "GetReplicationConfiguration", "PUT": "PutReplicationConfiguration",
                    "DELETE": "DeleteReplicationConfiguration"},
    "location": {"GET": "GetBucketLocation"},
    "uploads": {"GET": "ListMultipartUploads", "POST": "CreateMultipartUpload"},
}


def _s3_action(method: str, path: str, query_params: dict) -> str | None:
    parts = [p for p in path.split("/") if p]
    depth = min(len(parts), 2)

    # Check sub-operation query params first (bucket-level)
    if depth >= 1:
        for qp, action_map in _S3_QUERY_ACTIONS.items():
            if qp in query_params:
                a = action_map.get(method)
                if a:
                    return a

    return _S3_ACTIONS.get((method, depth))


# Lambda REST path → IAM action
def _lambda_action(method: str, path: str) -> str | None:
    parts = [p for p in path.split("/") if p]
    if "functions" not in parts:
        if "layers" in parts:
            if method == "GET":
                return "ListLayers"
            if method == "POST":
                return "PublishLayerVersion"
        if "event-source-mappings" in parts:
            if method == "GET":
                return "ListEventSourceMappings"
            if method == "POST":
                return "CreateEventSourceMapping"
        return None

    fi = parts.index("functions")
    rest = parts[fi + 1:]
    if not rest:
        return "CreateFunction" if method == "POST" else "ListFunctions"

    sub = rest[1] if len(rest) > 1 else None
    sub_map = {
        "invocations": "InvokeFunction",
        "code": "UpdateFunctionCode" if method == "PUT" else "GetFunction",
        "configuration": ("UpdateFunctionConfiguration" if method == "PUT"
                          else "GetFunctionConfiguration"),
        "aliases": "CreateAlias" if method == "POST" else "ListAliases",
        "versions": "PublishVersion" if method == "POST" else "ListVersionsByFunction",
        "policy": ("AddPermission" if method == "POST"
                   else "RemovePermission" if method == "DELETE"
                   else "GetPolicy"),
        "event-source-mappings": "ListEventSourceMappings",
        "concurrency": "PutFunctionConcurrency",
        "code-signing-config": "GetFunctionCodeSigningConfig",
        "url": "GetFunctionUrlConfig" if method == "GET" else "CreateFunctionUrlConfig",
        "tags": "TagResource" if method == "POST" else "UntagResource" if method == "DELETE" else "ListTags",
    }
    if sub in sub_map:
        return sub_map[sub]

    return {
        "GET": "GetFunction",
        "DELETE": "DeleteFunction",
        "PUT": "UpdateFunctionCode",
    }.get(method)


# ---------------------------------------------------------------------------
# Generic botocore route matcher
# ---------------------------------------------------------------------------

# MiniStack service name → botocore data directory name(s)
_BOTOCORE_SERVICE_MAP: dict[str, list[str]] = {
    "apigateway": ["apigateway", "apigatewayv2"],
    "appconfig": ["appconfig"],
    "appconfigdata": ["appconfigdata"],
    "appsync": ["appsync"],
    "appsync-events": ["appsync"],
    "backup": ["backup"],
    "batch": ["batch"],
    "bedrock": ["bedrock"],
    "bedrock-runtime": ["bedrock-runtime"],
    "bedrock-agent": ["bedrock-agent"],
    "bedrock-agent-runtime": ["bedrock-agent-runtime"],
    "bedrock-agentcore": [],  # no botocore model yet
    "cloudfront": ["cloudfront"],
    "cloudfront-keyvaluestore": ["cloudfront-keyvaluestore"],
    "dsql": ["dsql"],
    "eks": ["eks"],
    "elasticfilesystem": ["efs"],
    "inspector2": ["inspector2"],
    "iot": ["iot"],
    "iot-data": ["iot-data"],
    "iot-jobs-data": ["iot-jobs-data"],
    "kafka": ["kafka"],
    "mediaconnect": ["mediaconnect"],
    "mq": ["mq"],
    "airflow": ["mwaa"],
    "opensearch": ["opensearch"],
    "pipes": ["pipes"],
    "resource-groups": ["resource-groups"],
    "route53": ["route53"],
    "s3files": [],  # uses S3 namespace but different paths
    "s3tables": ["s3tables"],
    "scheduler": ["scheduler"],
}

# Compiled route: (http_method, compiled_regex, operation_name, specificity)
# Compiled route: (http_method, compiled_regex, operation_name, specificity, required_query)
_REST_ROUTE_CACHE: dict[str, list[tuple[str, re.Pattern, str, int, dict[str, str]]]] = {}


def _compile_uri(uri_pattern: str) -> tuple[re.Pattern, int, dict[str, str]]:
    """Compile a botocore URI pattern into a regex + specificity score +
    required query params.

    ``/clusters/{name}/addons/{addonName}`` becomes
    ``^/clusters/[^/]+/addons/[^/]+(?:/)?$`` with specificity 2
    (2 literal segments).

    Query params from the pattern (e.g., ``?mode=import``) are returned
    separately for disambiguation.
    """
    required_query: dict[str, str] = {}
    if "?" in uri_pattern:
        uri_path, qs = uri_pattern.split("?", 1)
        for part in qs.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                if not v.startswith("{"):
                    required_query[k] = v
            else:
                required_query[part] = ""
    else:
        uri_path = uri_pattern

    uri_path = uri_path.rstrip("/") or "/"

    specificity = 0
    segments = uri_path.split("/")
    regex_parts = []
    for seg in segments:
        if not seg:
            regex_parts.append("")
            continue
        if seg.startswith("{") and seg.endswith("+}"):
            regex_parts.append(".+")
        elif seg.startswith("{") and seg.endswith("}"):
            regex_parts.append("[^/]+")
        else:
            regex_parts.append(re.escape(seg))
            specificity += 1

    pattern = "/".join(regex_parts) or "/"
    compiled = re.compile(f"^{pattern}(?:/)?$", re.IGNORECASE)
    return compiled, specificity, required_query


def _load_botocore_routes(botocore_service: str) -> list[tuple[str, re.Pattern, str, int, dict[str, str]]]:
    """Load a botocore service model and compile its URI routes."""
    try:
        import gzip
        import botocore as _bc
        data_dir = os.path.join(os.path.dirname(_bc.__file__), "data")
    except ImportError:
        logger.debug("AUTH: botocore not installed — no generic route matching")
        return []

    svc_dir = os.path.join(data_dir, botocore_service)
    if not os.path.isdir(svc_dir):
        return []

    versions = sorted(os.listdir(svc_dir))
    if not versions:
        return []

    model_path = os.path.join(svc_dir, versions[-1], "service-2.json.gz")
    if not os.path.exists(model_path):
        model_path = os.path.join(svc_dir, versions[-1], "service-2.json")
        if not os.path.exists(model_path):
            return []

    try:
        if model_path.endswith(".gz"):
            import gzip
            with gzip.open(model_path, "rt") as f:
                model = json.load(f)
        else:
            with open(model_path) as f:
                model = json.load(f)
    except Exception:
        logger.debug("AUTH: failed to load botocore model for %s", botocore_service)
        return []

    routes = []
    for op_name, op_def in model.get("operations", {}).items():
        http = op_def.get("http", {})
        method = http.get("method", "").upper()
        uri = http.get("requestUri", "")
        if not method or not uri:
            continue
        compiled, specificity, required_query = _compile_uri(uri)
        # Operations with required query params get a specificity boost
        if required_query:
            specificity += len(required_query)
        routes.append((method, compiled, op_name, specificity, required_query))

    # Sort by specificity descending so more specific routes match first
    routes.sort(key=lambda r: -r[3])
    return routes


def _get_routes_for_service(service: str) -> list[tuple[str, re.Pattern, str, int, dict[str, str]]]:
    """Get compiled routes for a MiniStack service, with lazy loading."""
    if service in _REST_ROUTE_CACHE:
        return _REST_ROUTE_CACHE[service]

    botocore_names = _BOTOCORE_SERVICE_MAP.get(service, [])
    all_routes = []
    for bc_name in botocore_names:
        all_routes.extend(_load_botocore_routes(bc_name))

    all_routes.sort(key=lambda r: -r[3])
    _REST_ROUTE_CACHE[service] = all_routes
    return all_routes


def _match_rest_action(service: str, method: str, path: str,
                       query_params: dict | None = None) -> str | None:
    """Match a REST request against botocore route patterns."""
    if service not in _BOTOCORE_SERVICE_MAP:
        return None

    routes = _get_routes_for_service(service)
    if not routes:
        return None

    norm_path = path.rstrip("/") or "/"
    qp = query_params or {}

    best_match = None
    best_specificity = -1

    for route_method, route_re, op_name, specificity, required_query in routes:
        if route_method != method:
            continue
        if not route_re.match(norm_path):
            continue
        # Check required query params for disambiguation
        if required_query:
            match_qp = True
            for k, v in required_query.items():
                if k not in qp:
                    match_qp = False
                    break
                if v:
                    # Check value match (e.g., Operation=Untag)
                    actual = qp[k]
                    if isinstance(actual, list):
                        actual = actual[0] if actual else ""
                    if actual != v:
                        match_qp = False
                        break
            if not match_qp:
                continue
        if specificity > best_specificity:
            best_match = op_name
            best_specificity = specificity

    return best_match


def extract_iam_action(service: str, method: str, path: str,
                       headers: dict, body: bytes,
                       query_params: dict) -> str | None:
    """Return the IAM action string (``namespace:ActionName``) or None."""
    namespace = SERVICE_TO_IAM_NAMESPACE.get(service)
    if namespace is None:
        logger.debug("AUTH: no IAM namespace for service %s — allowing", service)
        return None

    # Tier 1: Action query param (query-protocol services)
    action_name = _action_from_query(query_params, body,
                                     headers.get("content-type", ""))
    if action_name:
        return f"{namespace}:{action_name}"

    # Tier 2: X-Amz-Target header (JSON-protocol services)
    action_name = _action_from_target(headers)
    if action_name:
        return f"{namespace}:{action_name}"

    # Tier 3: REST path-based mapping
    if service == "s3":
        action_name = _s3_action(method, path, query_params)
        if action_name:
            return f"s3:{action_name}"

    if service == "lambda":
        action_name = _lambda_action(method, path)
        if action_name:
            return f"lambda:{action_name}"

    # Tier 4: Generic botocore route matcher (all other REST services)
    action_name = _match_rest_action(service, method, path, query_params)
    if action_name:
        return f"{namespace}:{action_name}"

    logger.debug("AUTH: could not extract action for %s %s %s — allowing",
                 service, method, path)
    return None


# ---------------------------------------------------------------------------
# Per-protocol AccessDenied response
# ---------------------------------------------------------------------------

# Protocol type per service (for error formatting)
_SERVICE_PROTOCOL: dict[str, str] = {
    "s3": "rest-xml",
    "ec2": "ec2-xml",
    "autoscaling": "query-xml",
    "cloudformation": "query-xml",
    "elasticache": "query-xml",
    "elasticloadbalancing": "query-xml",
    "iam": "query-xml",
    "monitoring": "query-xml",
    "rds": "query-xml",
    "route53": "rest-xml",
    "ses": "query-xml",
    "sns": "query-xml",
    "sts": "query-xml",
    # Everything else defaults to JSON
}


def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def access_denied_response(service: str, action: str, principal_arn: str,
                           request_id: str, *, error_code: str = "",
                           message: str = "") -> tuple:
    """Format a 403 error response matching the service's protocol."""
    if not message:
        message = (
            f"User: {principal_arn} is not authorized to perform: {action} "
            f"because no identity-based policy allows the {action} action"
        )
    code = error_code or "AccessDenied"
    protocol = _SERVICE_PROTOCOL.get(service, "json")

    if protocol == "rest-xml":
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Error><Code>{code}</Code>"
            f"<Message>{_xml_escape(message)}</Message>"
            f"<RequestId>{request_id}</RequestId></Error>"
        )
        return 403, {"Content-Type": "application/xml"}, body.encode()

    if protocol == "ec2-xml":
        ec2_code = "UnauthorizedOperation" if code == "AccessDenied" else code
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Response><Errors><Error>"
            f"<Code>{ec2_code}</Code>"
            f"<Message>{_xml_escape(message)}</Message>"
            f"</Error></Errors>"
            f"<RequestID>{request_id}</RequestID></Response>"
        )
        return 403, {"Content-Type": "application/xml"}, body.encode()

    if protocol == "query-xml":
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">'
            f"<Error><Type>Sender</Type><Code>{code}</Code>"
            f"<Message>{_xml_escape(message)}</Message></Error>"
            f"<RequestId>{request_id}</RequestId></ErrorResponse>"
        )
        return 403, {"Content-Type": "text/xml"}, body.encode()

    # JSON protocol (DynamoDB, Lambda, KMS, Logs, Glue, etc.)
    json_type = code if code != "AccessDenied" else "AccessDeniedException"
    body = json.dumps({
        "__type": json_type,
        "message": message,
    })
    return (
        403,
        {
            "Content-Type": "application/x-amz-json-1.0",
            "x-amzn-errortype": json_type,
        },
        body.encode(),
    )
