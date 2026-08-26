"""IAM action extraction and AccessDenied response formatting.

Maps MiniStack's internal service names to IAM namespaces, extracts the
IAM action string (``service:ActionName``) from each request's protocol,
and formats per-protocol AccessDenied error responses.
"""

import base64
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
    "restore": {"POST": "RestoreObject"},
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
    # CloudWatch accepts the legacy Query API, JSON and smithy-rpc-v2-cbor; the
    # reply has to mirror whatever the caller sent, so the responder resolves
    # this one from the request headers (see _cbor_capable below).
    "monitoring": "query-xml",
    "rds": "query-xml",
    "cloudfront": "rest-xml",
    "route53": "rest-xml",
    "ses": "query-xml",
    "sns": "query-xml",
    "sts": "query-xml",
    # Everything else defaults to JSON
}

# Services whose Query API also answers JSON / smithy-rpc-v2-cbor, where the
# error has to come back in the encoding the request arrived in.
_CBOR_CAPABLE = frozenset({"monitoring"})

# xmlNamespace from each service's botocore model. A query-protocol error
# carries its own service's namespace, not IAM's.
_QUERY_XML_NS: dict[str, str] = {
    "autoscaling": "http://autoscaling.amazonaws.com/doc/2011-01-01/",
    "cloudformation": "http://cloudformation.amazonaws.com/doc/2010-05-15/",
    "elasticache": "http://elasticache.amazonaws.com/doc/2015-02-02/",
    "elasticloadbalancing": "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/",
    "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
    "monitoring": "http://monitoring.amazonaws.com/doc/2010-08-01/",
    "rds": "http://rds.amazonaws.com/doc/2014-10-31/",
    "ses": "http://ses.amazonaws.com/doc/2010-12-01/",
    "sns": "http://sns.amazonaws.com/doc/2010-03-31/",
    "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
}


def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ---------------------------------------------------------------------------
# Resource ARN construction
# ---------------------------------------------------------------------------

def _safe_json_field(body: bytes, field: str) -> str:
    """Extract a field from a JSON body, returning '' on any failure."""
    if not body:
        return ""
    try:
        return json.loads(body).get(field, "") or ""
    except (json.JSONDecodeError, TypeError, AttributeError):
        return ""


def _query_param(query_params: dict, key: str) -> str:
    """Extract a single query parameter value."""
    val = query_params.get(key, "")
    if isinstance(val, list):
        return val[0] if val else ""
    return val or ""


def _param(body: bytes, query_params: dict, *fields: str) -> str:
    """Read a request parameter from the JSON body, falling back to the query form.

    Several services moved to the JSON protocol (SQS in 2023, plus ACM, SSM and
    CloudWatch), where the parameters travel in the body and nothing reaches
    ``query_params``, while older clients still send the query form. Reading
    both keeps one branch correct for either wire shape.
    """
    for field in fields:
        val = _safe_json_field(body, field)
        if val:
            return val
    for field in fields:
        val = _query_param(query_params, field)
        if val:
            return val
    return ""


_KMS_KEY_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _kms_key_id_from_ciphertext(ciphertext_b64: str) -> str:
    """Recover the key id our symmetric ciphertext carries in its first 36 bytes.

    Decrypt and ReEncrypt do not require a KeyId: "AWS KMS can get this
    information from metadata that it adds to the symmetric ciphertext blob."
    IAM still evaluates against that key's ARN, so the resource has to be
    resolved the same way the KMS handler resolves it.
    """
    if not ciphertext_b64:
        return ""
    try:
        raw = base64.b64decode(ciphertext_b64)
    except Exception:
        return ""
    if len(raw) <= 68:
        return ""
    candidate = raw[:36].decode("utf-8", errors="ignore")
    return candidate if _KMS_KEY_ID_RE.match(candidate) else ""


def extract_resource_arn(service: str, method: str, path: str,
                         headers: dict, body: bytes,
                         query_params: dict, region: str,
                         account_id: str) -> str:
    """Construct the resource ARN for the request, or '*' if unknown."""

    if service == "s3":
        parts = [p for p in path.split("/") if p]
        if not parts:
            return "*"  # ListBuckets — no specific resource
        bucket = parts[0]
        if len(parts) >= 2:
            key = "/".join(parts[1:])
            return f"arn:aws:s3:::{bucket}/{key}"
        return f"arn:aws:s3:::{bucket}"

    if service == "dynamodb":
        table = _safe_json_field(body, "TableName")
        if table:
            return f"arn:aws:dynamodb:{region}:{account_id}:table/{table}"
        return "*"

    if service == "lambda":
        # Path: /2015-03-31/functions/{name}/...
        path_parts = [p for p in path.split("/") if p]
        if "functions" in path_parts:
            fi = path_parts.index("functions")
            if fi + 1 < len(path_parts):
                func_name = path_parts[fi + 1]
                return f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"
        if "layers" in path_parts:
            li = path_parts.index("layers")
            if li + 1 < len(path_parts):
                layer_name = path_parts[li + 1]
                return f"arn:aws:lambda:{region}:{account_id}:layer:{layer_name}"
        if "event-source-mappings" in path_parts:
            ei = path_parts.index("event-source-mappings")
            if ei + 1 < len(path_parts):
                uuid = path_parts[ei + 1]
                return f"arn:aws:lambda:{region}:{account_id}:event-source-mapping:{uuid}"
        return "*"

    if service == "sqs":
        # SQS speaks the JSON protocol, so QueueUrl arrives in the body for
        # current SDKs and in the query form for older ones.
        queue_url = _param(body, query_params, "QueueUrl")
        if not queue_url:
            # Query-protocol callers address the queue by path instead:
            # POST /{account_id}/{queue_name}
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 2 and parts[-2].isdigit():
                queue_url = parts[-1]
        if queue_url:
            queue_name = queue_url.rstrip("/").split("/")[-1]
            if queue_name:
                return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"
        # QueueName param (CreateQueue, GetQueueUrl)
        queue_name = _param(body, query_params, "QueueName")
        if queue_name:
            return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"
        return "*"

    if service == "sns":
        topic_arn = _query_param(query_params, "TopicArn")
        if topic_arn:
            return topic_arn
        target_arn = _query_param(query_params, "TargetArn")
        if target_arn:
            return target_arn
        # CreateTopic — name in params
        topic_name = _query_param(query_params, "Name")
        if topic_name:
            return f"arn:aws:sns:{region}:{account_id}:{topic_name}"
        return "*"

    if service == "kms":
        key_id = _safe_json_field(body, "KeyId")
        if not key_id:
            # Decrypt, and ReEncrypt's source key, carry no KeyId for symmetric
            # keys — the key is recovered from the ciphertext.
            key_id = _kms_key_id_from_ciphertext(
                _safe_json_field(body, "CiphertextBlob"))
        if key_id:
            # KeyId can be an ARN, alias, or key ID
            if key_id.startswith("arn:"):
                return key_id
            if key_id.startswith("alias/"):
                return f"arn:aws:kms:{region}:{account_id}:{key_id}"
            return f"arn:aws:kms:{region}:{account_id}:key/{key_id}"
        return "*"

    if service == "secretsmanager":
        secret_id = _safe_json_field(body, "SecretId")
        if not secret_id:
            secret_id = _safe_json_field(body, "Name")
        if secret_id:
            if secret_id.startswith("arn:"):
                return secret_id
            return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_id}"
        return "*"

    if service == "iam":
        # IAM actions target users, roles, policies — extract from params
        role_name = _query_param(query_params, "RoleName")
        if role_name:
            return f"arn:aws:iam::{account_id}:role/{role_name}"
        user_name = _query_param(query_params, "UserName")
        if user_name:
            return f"arn:aws:iam::{account_id}:user/{user_name}"
        policy_arn = _query_param(query_params, "PolicyArn")
        if policy_arn:
            return policy_arn
        group_name = _query_param(query_params, "GroupName")
        if group_name:
            return f"arn:aws:iam::{account_id}:group/{group_name}"
        return "*"

    if service == "sts":
        role_arn = _query_param(query_params, "RoleArn")
        if role_arn:
            return role_arn
        return "*"

    # --- Target-based (JSON body) services ---

    if service == "events":
        name = _safe_json_field(body, "Name") or _safe_json_field(body, "RuleName")
        bus = _safe_json_field(body, "EventBusName") or "default"
        if name:
            return f"arn:aws:events:{region}:{account_id}:rule/{bus}/{name}"
        bus_only = _safe_json_field(body, "EventBusName")
        if bus_only:
            if bus_only.startswith("arn:"):
                return bus_only
            return f"arn:aws:events:{region}:{account_id}:event-bus/{bus_only}"
        return "*"

    if service == "states":
        arn = _safe_json_field(body, "stateMachineArn")
        if arn:
            return arn
        arn = _safe_json_field(body, "executionArn")
        if arn:
            return arn
        arn = _safe_json_field(body, "activityArn")
        if arn:
            return arn
        name = _safe_json_field(body, "name")
        if name:
            return f"arn:aws:states:{region}:{account_id}:stateMachine:{name}"
        return "*"

    if service == "kinesis":
        arn = _safe_json_field(body, "StreamARN")
        if arn:
            return arn
        name = _safe_json_field(body, "StreamName")
        if name:
            return f"arn:aws:kinesis:{region}:{account_id}:stream/{name}"
        return "*"

    if service == "logs":
        name = _safe_json_field(body, "logGroupName")
        if name:
            return f"arn:aws:logs:{region}:{account_id}:log-group:{name}"
        return "*"

    if service == "glue":
        db = _safe_json_field(body, "DatabaseName")
        if db:
            return f"arn:aws:glue:{region}:{account_id}:database/{db}"
        name = _safe_json_field(body, "Name")
        if name:
            return f"arn:aws:glue:{region}:{account_id}:table/{name}"
        crawler = _safe_json_field(body, "CrawlerName")
        if crawler:
            return f"arn:aws:glue:{region}:{account_id}:crawler/{crawler}"
        job = _safe_json_field(body, "JobName")
        if job:
            return f"arn:aws:glue:{region}:{account_id}:job/{job}"
        return f"arn:aws:glue:{region}:{account_id}:catalog"

    if service == "codebuild":
        name = _safe_json_field(body, "projectName") or _safe_json_field(body, "name")
        if name:
            return f"arn:aws:codebuild:{region}:{account_id}:project/{name}"
        return "*"

    if service == "ecr":
        name = _safe_json_field(body, "repositoryName")
        if name:
            return f"arn:aws:ecr:{region}:{account_id}:repository/{name}"
        return "*"

    if service == "config":
        name = _safe_json_field(body, "ConfigRuleName")
        if name:
            return f"arn:aws:config:{region}:{account_id}:config-rule/{name}"
        return "*"

    if service == "athena":
        wg = _safe_json_field(body, "WorkGroup")
        if wg:
            return f"arn:aws:athena:{region}:{account_id}:workgroup/{wg}"
        return "*"

    if service == "servicediscovery":
        sid = _safe_json_field(body, "ServiceId") or _safe_json_field(body, "Id")
        if sid:
            return f"arn:aws:servicediscovery:{region}:{account_id}:service/{sid}"
        nid = _safe_json_field(body, "NamespaceId")
        if nid:
            return f"arn:aws:servicediscovery:{region}:{account_id}:namespace/{nid}"
        name = _safe_json_field(body, "Name")
        if name:
            return f"arn:aws:servicediscovery:{region}:{account_id}:namespace/{name}"
        return "*"

    if service == "opensearch":
        name = _safe_json_field(body, "DomainName")
        if name:
            return f"arn:aws:es:{region}:{account_id}:domain/{name}"
        return "*"

    if service == "organizations":
        return f"arn:aws:organizations::{account_id}:organization/*"

    if service == "wafv2":
        arn = _safe_json_field(body, "ARN")
        if arn:
            return arn
        name = _safe_json_field(body, "Name")
        if name:
            scope = _safe_json_field(body, "Scope") or "REGIONAL"
            prefix = "regional" if scope == "REGIONAL" else "global"
            return f"arn:aws:wafv2:{region}:{account_id}:{prefix}/webacl/{name}/*"
        return "*"

    if service in ("elasticmapreduce", "emr"):
        cid = _safe_json_field(body, "ClusterId") or _safe_json_field(body, "JobFlowId")
        if cid:
            return f"arn:aws:elasticmapreduce:{region}:{account_id}:cluster/{cid}"
        name = _safe_json_field(body, "Name")
        if name:
            return f"arn:aws:elasticmapreduce:{region}:{account_id}:cluster/*"
        return "*"

    if service == "transfer":
        sid = _safe_json_field(body, "ServerId")
        if sid:
            return f"arn:aws:transfer:{region}:{account_id}:server/{sid}"
        return "*"

    if service == "firehose":
        name = _safe_json_field(body, "DeliveryStreamName")
        if name:
            return f"arn:aws:firehose:{region}:{account_id}:deliverystream/{name}"
        return "*"

    if service == "backup":
        vault = _safe_json_field(body, "BackupVaultName")
        if vault:
            return f"arn:aws:backup:{region}:{account_id}:backup-vault:{vault}"
        return "*"

    # --- Target-based services using JSON body for ECS/EKS ---

    if service == "ecs":
        cluster = _safe_json_field(body, "cluster")
        if cluster:
            if cluster.startswith("arn:"):
                return cluster
            return f"arn:aws:ecs:{region}:{account_id}:cluster/{cluster}"
        task_def = _safe_json_field(body, "taskDefinition")
        if task_def:
            if task_def.startswith("arn:"):
                return task_def
            return f"arn:aws:ecs:{region}:{account_id}:task-definition/{task_def}"
        service_name = _safe_json_field(body, "serviceName")
        if service_name:
            return f"arn:aws:ecs:{region}:{account_id}:service/*/{service_name}"
        return "*"

    if service == "eks":
        name = _safe_json_field(body, "name")
        if not name:
            # EKS REST: /clusters/{name}
            parts = [p for p in path.split("/") if p]
            if "clusters" in parts:
                ci = parts.index("clusters")
                if ci + 1 < len(parts):
                    name = parts[ci + 1]
        if name:
            return f"arn:aws:eks:{region}:{account_id}:cluster/{name}"
        return "*"

    # --- Query-based services ---

    if service == "acm":
        arn = _param(body, query_params, "CertificateArn")
        if arn:
            return arn
        return "*"

    if service == "cloudformation":
        name = _query_param(query_params, "StackName")
        if name:
            if name.startswith("arn:"):
                return name
            return f"arn:aws:cloudformation:{region}:{account_id}:stack/{name}/*"
        return "*"

    if service == "monitoring":
        # CloudWatch alarms
        name = _param(body, query_params, "AlarmName")
        if name:
            return f"arn:aws:cloudwatch:{region}:{account_id}:alarm:{name}"
        ns = _query_param(query_params, "Namespace")
        if ns:
            return "*"  # Metrics don't have individual ARNs
        return "*"

    if service == "autoscaling":
        name = _query_param(query_params, "AutoScalingGroupName")
        if name:
            return f"arn:aws:autoscaling:{region}:{account_id}:autoScalingGroup:*:autoScalingGroupName/{name}"
        return "*"

    if service == "elasticache":
        cid = _query_param(query_params, "CacheClusterId")
        if cid:
            return f"arn:aws:elasticache:{region}:{account_id}:cluster:{cid}"
        rgid = _query_param(query_params, "ReplicationGroupId")
        if rgid:
            return f"arn:aws:elasticache:{region}:{account_id}:replicationgroup:{rgid}"
        return "*"

    if service == "elasticloadbalancing":
        arn = _query_param(query_params, "LoadBalancerArn")
        if arn:
            return arn
        arn = _query_param(query_params, "TargetGroupArn")
        if arn:
            return arn
        arn = _query_param(query_params, "ListenerArn")
        if arn:
            return arn
        return "*"

    if service == "rds":
        name = _query_param(query_params, "DBInstanceIdentifier")
        if name:
            return f"arn:aws:rds:{region}:{account_id}:db:{name}"
        name = _query_param(query_params, "DBClusterIdentifier")
        if name:
            return f"arn:aws:rds:{region}:{account_id}:cluster:{name}"
        return "*"

    if service == "ses":
        identity = _query_param(query_params, "Identity")
        if identity:
            return f"arn:aws:ses:{region}:{account_id}:identity/{identity}"
        return "*"

    if service == "ssm":
        name = _param(body, query_params, "Name")
        if name:
            return f"arn:aws:ssm:{region}:{account_id}:parameter{name if name.startswith('/') else '/' + name}"
        return "*"

    if service == "route53":
        zone_id = _query_param(query_params, "HostedZoneId")
        if not zone_id:
            # REST: /2013-04-01/hostedzone/{id}
            parts = [p for p in path.split("/") if p]
            if "hostedzone" in parts:
                hi = parts.index("hostedzone")
                if hi + 1 < len(parts):
                    zone_id = parts[hi + 1]
        if zone_id:
            return f"arn:aws:route53:::hostedzone/{zone_id}"
        return "*"

    if service == "cloudfront":
        # REST: /2020-05-31/distribution/{id}
        parts = [p for p in path.split("/") if p]
        if "distribution" in parts:
            di = parts.index("distribution")
            if di + 1 < len(parts):
                return f"arn:aws:cloudfront::{account_id}:distribution/{parts[di + 1]}"
        return "*"

    if service in ("cognito-idp", "cognito_idp"):
        pool_id = _safe_json_field(body, "UserPoolId")
        if pool_id:
            return f"arn:aws:cognito-idp:{region}:{account_id}:userpool/{pool_id}"
        return "*"

    if service in ("cognito-identity", "cognito_identity"):
        pool_id = _safe_json_field(body, "IdentityPoolId")
        if pool_id:
            return f"arn:aws:cognito-identity:{region}:{account_id}:identitypool/{pool_id}"
        return "*"

    # --- Simple REST path services ---

    if service == "scheduler":
        parts = [p for p in path.split("/") if p]
        if "schedules" in parts:
            si = parts.index("schedules")
            if si + 1 < len(parts):
                return f"arn:aws:scheduler:{region}:{account_id}:schedule/default/{parts[si + 1]}"
        if "schedule-groups" in parts:
            gi = parts.index("schedule-groups")
            if gi + 1 < len(parts):
                return f"arn:aws:scheduler:{region}:{account_id}:schedule-group/{parts[gi + 1]}"
        return "*"

    if service == "pipes":
        parts = [p for p in path.split("/") if p]
        if "pipes" in parts:
            pi = parts.index("pipes")
            if pi + 1 < len(parts):
                return f"arn:aws:pipes:{region}:{account_id}:pipe/{parts[pi + 1]}"
        return "*"

    if service == "mq":
        parts = [p for p in path.split("/") if p]
        if "brokers" in parts:
            bi = parts.index("brokers")
            if bi + 1 < len(parts):
                return f"arn:aws:mq:{region}:{account_id}:broker:{parts[bi + 1]}:*"
        return "*"

    if service == "kafka":
        parts = [p for p in path.split("/") if p]
        if "clusters" in parts:
            ci = parts.index("clusters")
            if ci + 1 < len(parts):
                return f"arn:aws:kafka:{region}:{account_id}:cluster/{parts[ci + 1]}/*"
        return "*"

    if service == "dsql":
        parts = [p for p in path.split("/") if p]
        if "clusters" in parts:
            ci = parts.index("clusters")
            if ci + 1 < len(parts):
                return f"arn:aws:dsql:{region}:{account_id}:cluster/{parts[ci + 1]}"
        return "*"

    if service == "mediaconnect":
        parts = [p for p in path.split("/") if p]
        if "flows" in parts:
            fi = parts.index("flows")
            if fi + 1 < len(parts):
                return f"arn:aws:mediaconnect:{region}:{account_id}:flow:{parts[fi + 1]}:*"
        return "*"

    if service == "inspector2":
        return "*"  # Mostly account-level operations, no per-resource ARNs

    if service == "elasticfilesystem":
        parts = [p for p in path.split("/") if p]
        if "file-systems" in parts:
            fi = parts.index("file-systems")
            if fi + 1 < len(parts):
                return f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/{parts[fi + 1]}"
        if "mount-targets" in parts:
            mi = parts.index("mount-targets")
            if mi + 1 < len(parts):
                return f"arn:aws:elasticfilesystem:{region}:{account_id}:file-system/*"
        if "access-points" in parts:
            ai = parts.index("access-points")
            if ai + 1 < len(parts):
                return f"arn:aws:elasticfilesystem:{region}:{account_id}:access-point/{parts[ai + 1]}"
        return "*"

    if service == "cloudtrail":
        name = _safe_json_field(body, "Name") or _safe_json_field(body, "TrailName")
        if name:
            if name.startswith("arn:"):
                return name
            return f"arn:aws:cloudtrail:{region}:{account_id}:trail/{name}"
        return "*"

    if service == "s3tables":
        parts = [p for p in path.split("/") if p]
        if "buckets" in parts:
            bi = parts.index("buckets")
            if bi + 1 < len(parts):
                bucket = parts[bi + 1]
                if "tables" in parts:
                    ti = parts.index("tables")
                    if ti + 1 < len(parts):
                        return f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket}/table/{parts[ti + 1]}"
                return f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket}"
        return "*"

    if service == "s3files":
        parts = [p for p in path.split("/") if p]
        if "file-systems" in parts:
            fi = parts.index("file-systems")
            if fi + 1 < len(parts):
                return f"arn:aws:s3:{region}:{account_id}:file-system/{parts[fi + 1]}"
        return "*"

    if service == "resource-groups":
        parts = [p for p in path.split("/") if p]
        if "groups" in parts:
            gi = parts.index("groups")
            if gi + 1 < len(parts):
                return f"arn:aws:resource-groups:{region}:{account_id}:group/{parts[gi + 1]}"
        return "*"

    if service == "rds-data":
        arn = _safe_json_field(body, "resourceArn")
        if arn:
            return arn
        return "*"

    if service == "appconfig":
        parts = [p for p in path.split("/") if p]
        if "applications" in parts:
            ai = parts.index("applications")
            if ai + 1 < len(parts):
                app_id = parts[ai + 1]
                if "environments" in parts:
                    ei = parts.index("environments")
                    if ei + 1 < len(parts):
                        return f"arn:aws:appconfig:{region}:{account_id}:application/{app_id}/environment/{parts[ei + 1]}"
                if "configurationprofiles" in parts:
                    ci = parts.index("configurationprofiles")
                    if ci + 1 < len(parts):
                        return f"arn:aws:appconfig:{region}:{account_id}:application/{app_id}/configurationprofile/{parts[ci + 1]}"
                return f"arn:aws:appconfig:{region}:{account_id}:application/{app_id}"
        if "deploymentstrategies" in parts:
            di = parts.index("deploymentstrategies")
            if di + 1 < len(parts):
                return f"arn:aws:appconfig:{region}:{account_id}:deploymentstrategy/{parts[di + 1]}"
        return "*"

    if service == "appconfigdata":
        return "*"  # Session-based, no per-resource ARN

    # --- EC2 (query-based, many resource types) ---

    if service == "ec2":
        # EC2 uses query params. Try resource ID fields in priority order.
        _EC2_RESOURCE_FIELDS = [
            ("InstanceId.1", "instance"),
            ("InstanceId", "instance"),
            ("VpcId", "vpc"),
            ("SubnetId", "subnet"),
            ("SecurityGroupId.1", "security-group"),
            ("GroupId", "security-group"),
            ("GroupName", "security-group"),
            ("VolumeId", "volume"),
            ("KeyName", "key-pair"),
            ("ImageId", "image"),
            ("InternetGatewayId", "internet-gateway"),
            ("RouteTableId", "route-table"),
            ("NetworkInterfaceId", "network-interface"),
            ("AllocationId", "elastic-ip"),
            ("SnapshotId", "snapshot"),
            ("VpcEndpointId.1", "vpc-endpoint"),
        ]
        for field, rtype in _EC2_RESOURCE_FIELDS:
            val = _query_param(query_params, field)
            if val:
                return f"arn:aws:ec2:{region}:{account_id}:{rtype}/{val}"
        return "*"

    # --- IoT (REST path-based, multiple resource types) ---

    if service == "iot":
        parts = [p for p in path.split("/") if p]
        _IOT_RESOURCES = {
            "things": "thing",
            "thing-types": "thingtype",
            "thing-groups": "thinggroup",
            "policies": "policy",
            "certificates": "cert",
            "rules": "rule",
        }
        for segment, rtype in _IOT_RESOURCES.items():
            if segment in parts:
                si = parts.index(segment)
                if si + 1 < len(parts):
                    name = parts[si + 1]
                    if rtype == "cert":
                        return f"arn:aws:iot:{region}:{account_id}:{rtype}/{name}"
                    return f"arn:aws:iot:{region}:{account_id}:{rtype}/{name}"
        return "*"

    # --- API Gateway (REST path-based) ---

    if service == "apigateway":
        parts = [p for p in path.split("/") if p]
        # v2: /v2/apis/{apiId}
        if "apis" in parts:
            ai = parts.index("apis")
            if ai + 1 < len(parts):
                api_id = parts[ai + 1]
                return f"arn:aws:apigateway:{region}::/apis/{api_id}"
            return f"arn:aws:apigateway:{region}::/apis/*"
        # v1: /restapis/{restApiId}
        if "restapis" in parts:
            ri = parts.index("restapis")
            if ri + 1 < len(parts):
                api_id = parts[ri + 1]
                return f"arn:aws:apigateway:{region}::/restapis/{api_id}"
            return f"arn:aws:apigateway:{region}::/restapis/*"
        return "*"

    # --- Bedrock (REST path-based, multiple sub-services) ---

    if service == "bedrock":
        parts = [p for p in path.split("/") if p]
        if "foundation-models" in parts:
            fi = parts.index("foundation-models")
            if fi + 1 < len(parts):
                return f"arn:aws:bedrock:{region}::foundation-model/{parts[fi + 1]}"
        if "custom-models" in parts:
            ci = parts.index("custom-models")
            if ci + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:custom-model/{parts[ci + 1]}"
        if "guardrails" in parts:
            gi = parts.index("guardrails")
            if gi + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:guardrail/{parts[gi + 1]}"
        if "inference-profiles" in parts:
            ii = parts.index("inference-profiles")
            if ii + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:inference-profile/{parts[ii + 1]}"
        return "*"

    if service == "bedrock-runtime":
        parts = [p for p in path.split("/") if p]
        if "model" in parts:
            mi = parts.index("model")
            if mi + 1 < len(parts):
                model_id = parts[mi + 1]
                return f"arn:aws:bedrock:{region}::foundation-model/{model_id}"
        return "*"

    if service == "bedrock-agent":
        parts = [p for p in path.split("/") if p]
        if "agents" in parts:
            ai = parts.index("agents")
            if ai + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:agent/{parts[ai + 1]}"
        if "knowledgebases" in parts:
            ki = parts.index("knowledgebases")
            if ki + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:knowledge-base/{parts[ki + 1]}"
        if "flows" in parts:
            fi = parts.index("flows")
            if fi + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:flow/{parts[fi + 1]}"
        if "prompts" in parts:
            pi = parts.index("prompts")
            if pi + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:prompt/{parts[pi + 1]}"
        return "*"

    if service == "bedrock-agent-runtime":
        parts = [p for p in path.split("/") if p]
        if "knowledgebases" in parts:
            ki = parts.index("knowledgebases")
            if ki + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:knowledge-base/{parts[ki + 1]}"
        if "agents" in parts:
            ai = parts.index("agents")
            if ai + 1 < len(parts):
                return f"arn:aws:bedrock:{region}:{account_id}:agent/{parts[ai + 1]}"
        return "*"

    # --- AppSync (REST path-based) ---

    if service == "appsync":
        parts = [p for p in path.split("/") if p]
        if "apis" in parts:
            ai = parts.index("apis")
            if ai + 1 < len(parts):
                api_id = parts[ai + 1]
                # Sub-resources
                if "datasources" in parts:
                    di = parts.index("datasources")
                    if di + 1 < len(parts):
                        return f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/datasources/{parts[di + 1]}"
                if "types" in parts:
                    ti = parts.index("types")
                    if ti + 1 < len(parts):
                        return f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/types/{parts[ti + 1]}"
                return f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}"
        return "*"

    return "*"


def access_denied_response(service: str, action: str, principal_arn: str,
                           request_id: str, *, error_code: str = "",
                           message: str = "", headers: dict | None = None) -> tuple:
    """Format a 403 error response matching the service's protocol.

    A denial the caller's SDK cannot parse is barely better than no denial: it
    surfaces as a bare 403 with the code buried in an unread body, so a client
    catching AccessDenied misses it. `headers` lets the services that accept
    more than one encoding answer in the one the request arrived in.
    """
    if not message:
        message = (
            f"User: {principal_arn} is not authorized to perform: {action} "
            f"because no identity-based policy allows the {action} action"
        )
    code = error_code or "AccessDenied"
    protocol = _SERVICE_PROTOCOL.get(service, "json")
    if service in _CBOR_CAPABLE:
        # CloudWatch answers the legacy Query API in XML but botocore 1.42+
        # speaks smithy-rpc-v2-cbor to it, and an XML error against a CBOR
        # request parses as a bare 403. Mirror the request, exactly as the
        # service's own _error does.
        ct = (headers or {}).get("content-type", "")
        smithy = (headers or {}).get("smithy-protocol", "")
        if "cbor" in ct or "cbor" in smithy:
            protocol = "cbor"
        elif "json" in ct or (headers or {}).get("x-amz-target"):
            protocol = "json"

    if protocol == "cbor":
        json_type = code if code != "AccessDenied" else "AccessDeniedException"
        try:
            import cbor2
            return (403,
                    {"Content-Type": "application/cbor", "smithy-protocol": "rpc-v2-cbor"},
                    cbor2.dumps({"__type": json_type, "message": message}))
        except ImportError:
            protocol = "json"

    _esc_code = _xml_escape(code)
    _esc_rid = _xml_escape(request_id)
    _esc_msg = _xml_escape(message)

    if protocol == "rest-xml":
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Error><Code>{_esc_code}</Code>"
            f"<Message>{_esc_msg}</Message>"
            f"<RequestId>{_esc_rid}</RequestId></Error>"
        )
        return 403, {"Content-Type": "application/xml"}, body.encode()

    if protocol == "ec2-xml":
        ec2_code = "UnauthorizedOperation" if code == "AccessDenied" else _esc_code
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Response><Errors><Error>"
            f"<Code>{ec2_code}</Code>"
            f"<Message>{_esc_msg}</Message>"
            f"</Error></Errors>"
            f"<RequestID>{_esc_rid}</RequestID></Response>"
        )
        return 403, {"Content-Type": "application/xml"}, body.encode()

    if protocol == "query-xml":
        ns = _QUERY_XML_NS.get(service, "https://iam.amazonaws.com/doc/2010-05-08/")
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<ErrorResponse xmlns="{ns}">'
            f"<Error><Type>Sender</Type><Code>{_esc_code}</Code>"
            f"<Message>{_esc_msg}</Message></Error>"
            f"<RequestId>{_esc_rid}</RequestId></ErrorResponse>"
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
