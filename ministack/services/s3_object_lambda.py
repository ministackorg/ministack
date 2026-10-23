"""S3 Object Lambda data plane: requests through an Object Lambda Access Point (records live in s3.py)."""

import asyncio
import datetime as _dt
import json
import logging
import os
import random
import string
from urllib.parse import quote
from xml.etree.ElementTree import Element, SubElement

from ministack.core.iam_evaluator import CredentialResolutionError, resolve_credential
from ministack.core.responses import get_account_id, new_uuid
from ministack.core.router import extract_access_key_id, extract_signed_region
from ministack.core.sigv4 import (
    build_canonical_query_string,
    build_canonical_request,
    build_string_to_sign,
    calculate_signature,
)
from ministack.services import s3

logger = logging.getLogger("s3")

PROTOCOL_VERSION = "1.00"
_PRESIGN_EXPIRES = 3600
_SUBRESOURCES = frozenset({
    "acl", "tagging", "attributes", "legal-hold", "retention", "torrent", "uploadId", "uploads",
    "restore", "select", "versioning", "location", "policy", "cors", "lifecycle", "versions",
})


def _qp(query_params: dict, name: str, default: str = "") -> str:
    value = query_params.get(name, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _error(code: str, message: str, status: int, resource: str = "") -> tuple:
    return s3._error(code, message, status, resource)


# ---------------------------------------------------------------------------
# Which operation, and which transformation
# ---------------------------------------------------------------------------

def _operation(method: str, key: str, query_params: dict) -> str | None:
    """The transformable operation a request is, or None for everything else."""
    if any(name in query_params for name in _SUBRESOURCES):
        return None
    if key:
        return {"GET": "GetObject", "HEAD": "HeadObject"}.get(method)
    if method == "GET":
        return "ListObjectsV2" if _qp(query_params, "list-type") == "2" else "ListObjects"
    return None


def _transformation(record: dict, operation: str) -> dict | None:
    for transformation in record["Configuration"].get("TransformationConfigurations") or []:
        if operation in transformation["Actions"]:
            return transformation
    return None


def _feature_error(record: dict, operation: str, headers: dict, query_params: dict) -> tuple | None:
    """Range and partNumber reach a transformation only when AllowedFeatures
    lists them (S3 user guide); the 501 refusal is inferred."""
    allowed = record["Configuration"].get("AllowedFeatures") or []
    for present, feature in ((bool(headers.get("range")), f"{operation}-Range"),
                             ("partNumber" in query_params, f"{operation}-PartNumber")):
        if present and feature not in allowed:
            return _error("NotImplemented", "A header you provided implies functionality that is not implemented",
                          501)
    return None


# ---------------------------------------------------------------------------
# The presigned inputS3Url
# ---------------------------------------------------------------------------

def _runs_in_container(func_config: dict) -> bool:
    from ministack.services import lambda_svc

    return bool(
        lambda_svc.LAMBDA_STRICT
        or lambda_svc.LAMBDA_EXECUTOR == "docker"
        or (func_config.get("PackageType") == "Image" and func_config.get("ImageUri"))
        or lambda_svc._proxy_url_for(func_config)
    )


def _endpoint_for(func_config: dict) -> str:
    """The MiniStack endpoint as the function's executor reaches it."""
    from ministack.core import tls
    from ministack.services import lambda_svc

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "").rstrip("/")
    if not endpoint:
        port = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))
        endpoint = f"{'https' if tls.use_ssl_enabled() else 'http'}://127.0.0.1:{port}"
    return lambda_svc._rewrite_host_for_container(endpoint) if _runs_in_container(func_config) else endpoint


def _presign(method: str, endpoint: str, path: str, query: dict[str, str], credential, region: str,
             session_token: str = "") -> str:
    """A SigV4 presigned URL MiniStack's own verification accepts."""
    now = _dt.datetime.now(_dt.timezone.utc)
    amz_date, date_stamp = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    params = {
        **query,
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{credential.access_key_id}/{date_stamp}/{region}/s3/aws4_request",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(_PRESIGN_EXPIRES),
        "X-Amz-SignedHeaders": "host",
    }
    if session_token:
        params["X-Amz-Security-Token"] = session_token
    host = endpoint.split("://", 1)[1]
    query_lists = {k: [v] for k, v in params.items()}
    canonical = build_canonical_request(method, path, {"host": host}, query_lists, "host")
    signature = calculate_signature(credential.secret_access_key, date_stamp, region, "s3",
                                    build_string_to_sign(amz_date, date_stamp, region, "s3", canonical))
    return f"{endpoint}{quote(path, safe='/-_.~')}?{build_canonical_query_string(query_lists)}" \
           f"&X-Amz-Signature={signature}"


def _caller_credential(headers: dict, query_params: dict):
    """The caller's credential, which signs the presigned inputS3Url as AWS does."""
    access_key = extract_access_key_id(headers, query_params) or os.environ.get("AWS_ACCESS_KEY_ID", "test")
    token = headers.get("x-amz-security-token") or _qp(query_params, "X-Amz-Security-Token")
    credential = resolve_credential(access_key, get_account_id(), token or None)
    if isinstance(credential, CredentialResolutionError):
        credential = resolve_credential(access_key, get_account_id())
    return credential, token


# ---------------------------------------------------------------------------
# WriteGetObjectResponse
# ---------------------------------------------------------------------------

def _new_route(region: str) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return f"io-{s3._region_code(region)}-{''.join(random.choices(alphabet, k=12))}"


def _register(route: str, token: str) -> dict:
    pending = {"status": 200, "headers": {}, "body": b"", "error": None, "done": False}
    with s3._olap_requests_lock:
        s3._olap_requests[(route, token)] = pending
    return pending


def _response_header_name(name: str) -> str:
    """``x-amz-fwd-header-content-type`` names ``Content-Type``."""
    if name.startswith("x-amz-"):
        return name
    return "-".join(part.capitalize() for part in name.split("-"))


def write_get_object_response(headers: dict, body: bytes) -> tuple:
    """``POST /WriteGetObjectResponse``, matched by its ``x-amz-request-route``
    header on any host. An unknown route gets AWS's 400 InvalidHostHeader
    (observed); a known route with a wrong or spent token the same (inferred)."""
    key = (headers.get("x-amz-request-route", ""), headers.get("x-amz-request-token", ""))
    with s3._olap_requests_lock:
        pending = s3._olap_requests.get(key)
        if pending is None or pending["done"]:
            return _error("InvalidHostHeader", "Invalid Host header.", 400)
        pending["status"] = int(headers.get("x-amz-fwd-status") or 200)
        pending["headers"] = {
            _response_header_name(name[len("x-amz-fwd-header-"):]): value
            for name, value in headers.items() if name.startswith("x-amz-fwd-header-")
        }
        pending["headers"].update({name: value for name, value in headers.items() if name.startswith("x-amz-meta-")})
        if headers.get("x-amz-fwd-error-code"):
            pending["error"] = (headers["x-amz-fwd-error-code"], headers.get("x-amz-fwd-error-message", ""))
        pending["body"] = body or b""
        pending["done"] = True
    return 200, {"Content-Type": "application/xml"}, b""


# ---------------------------------------------------------------------------
# Lambda invocation
# ---------------------------------------------------------------------------

def _function(arn: str):
    from ministack.services import lambda_svc

    try:
        name, qualifier = lambda_svc._resolve_name_and_qualifier(arn)
        record, _ = lambda_svc._get_func_record_for_qualifier(name, qualifier)
    except Exception:
        return None
    return record


def _lambda_failure(result: dict) -> tuple | None:
    """The error for a failed invocation, or None when the function returned."""
    if not result.get("error"):
        return None
    body = result.get("body")
    error_type = body.get("errorType", "") if isinstance(body, dict) else ""
    message = body.get("errorMessage", "") if isinstance(body, dict) else str(body or "")
    if error_type == "Sandbox.Timedout" or message.startswith("Task timed out"):
        return _error("LambdaTimeout", "The Lambda function timed out.", 500)
    if result.get("throttle") or error_type == "TooManyRequestsException":
        return _error("LambdaInvocationFailed", "The Lambda function could not be invoked.", 400)
    return _error("LambdaRuntimeError", message or "The Lambda function failed.", 500)


def _event(record: dict, transformation: dict, context_name: str, context: dict, url: str, headers: dict,
           access_key: str) -> dict:
    account = get_account_id()
    configuration = {
        "accessPointArn": s3.object_lambda_arn(record["Name"]),
        "supportingAccessPointArn": record["Configuration"]["SupportingAccessPoint"],
    }
    if "FunctionPayload" in transformation:
        configuration["payload"] = transformation["FunctionPayload"]
    return {
        "xAmzRequestId": new_uuid(),
        context_name: context,
        "configuration": configuration,
        "userRequest": {
            "url": url,
            "headers": {_response_header_name(k): v for k, v in headers.items() if k != "authorization"},
        },
        "userIdentity": {
            "type": "IAMUser",
            "principalId": access_key,
            "arn": f"arn:aws:iam::{account}:root",
            "accountId": account,
            "accessKeyId": access_key,
        },
        "protocolVersion": PROTOCOL_VERSION,
    }


async def _invoke(func_record: dict, event: dict) -> dict:
    from ministack.services import lambda_svc

    # On a worker thread, so the event loop serves the Lambda's
    # WriteGetObjectResponse call while the GET waits.
    return await asyncio.to_thread(lambda_svc._execute_function, func_record, event)


# ---------------------------------------------------------------------------
# Request flow
# ---------------------------------------------------------------------------

def _list_result_xml(result: dict) -> bytes | None:
    """``listResultXml`` as is, or ``listBucketResult`` JSON as S3's XML. The
    JSON uses camelCase keys (``contents``, ``key``, ``eTag``) and ``null`` for
    absent values, per the S3 user guide."""
    if isinstance(result.get("listResultXml"), str):
        return result["listResultXml"].encode()
    data = result.get("listBucketResult")
    if not isinstance(data, dict):
        return None
    root = Element("ListBucketResult", xmlns=s3.S3_NS)

    def xml_tag(name: str) -> str:
        return "ETag" if name.lower() == "etag" else name[:1].upper() + name[1:]

    def add(parent, name, value):
        if value is None:
            return
        tag = xml_tag(name)
        if isinstance(value, list):
            for item in value:
                add(parent, name, item)
        elif isinstance(value, dict):
            child = SubElement(parent, tag)
            for k, v in value.items():
                add(child, k, v)
        else:
            SubElement(parent, tag).text = str(value).lower() if isinstance(value, bool) else str(value)

    for tag, value in data.items():
        add(root, tag, value)
    return s3._xml_body(root)


def resolve_host(name: str, account_id: str, headers: dict, query_params: dict):
    """``(record, region, error)`` for an Object Lambda host ``<name>-<account>``
    signed ``s3-object-lambda``. An unknown name answers NoSuchAccessPoint (inferred)."""
    regions = [r for (a, r, n) in list(s3._object_lambda_access_points._data) if a == account_id and n == name]
    if not regions:
        return None, None, _error("NoSuchAccessPoint", "The specified accesspoint does not exist", 404)
    if account_id != get_account_id():
        return None, None, _error("AccessDenied", "Access Denied", 403)
    signed = extract_signed_region(headers, query_params)
    region = signed if signed in regions else regions[0]
    return s3._object_lambda_access_points.get_scoped(account_id, region, name), region, None


async def handle_request(record: dict, region: str, method: str, key: str, headers: dict, body: bytes,
                         query_params: dict, url: str) -> tuple:
    """A request through the Object Lambda Access Point ``record`` in ``region``."""
    signed = extract_signed_region(headers, query_params)
    if signed is not None and signed != region:
        return s3.region_mismatch_error(method, record["Name"], key, headers, query_params, signed, region)
    supporting = s3.supporting_access_point(record["Configuration"], region)
    if supporting is None:
        return _error("NoSuchAccessPoint", "The specified accesspoint does not exist", 404)
    _supporting_name, supporting_record = supporting

    operation = _operation(method, key, query_params)
    transformation = _transformation(record, operation) if operation else None
    if transformation is None:
        # No transformation for this operation: the supporting access point serves it.
        path = f"/{supporting_record['Bucket']}/{key}" if key else f"/{supporting_record['Bucket']}"
        return await s3.handle_request(method, path, headers, body, query_params, enforce_bucket_region=False)

    refused = _feature_error(record, operation, headers, query_params)
    if refused:
        return refused
    func_record = _function(transformation["FunctionArn"])
    if func_record is None:
        return _error("LambdaNotFound", "The Lambda function could not be found.", 404)

    credential, token = _caller_credential(headers, query_params)
    if isinstance(credential, CredentialResolutionError):
        return _error("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.", 403)
    endpoint = _endpoint_for(func_record.get("config") or func_record)
    alias = supporting_record["Alias"]
    if operation in ("GetObject", "HeadObject"):
        query = {name: _qp(query_params, name) for name in ("versionId", "partNumber") if name in query_params}
        input_url = _presign("GET" if operation == "GetObject" else "HEAD", endpoint, f"/{alias}/{key}", query,
                             credential, region, token)
    else:
        query = {name: _qp(query_params, name) for name in query_params if not name.lower().startswith("x-amz-")}
        input_url = _presign("GET", endpoint, f"/{alias}", query, credential, region, token)

    if operation != "GetObject":
        context_name = {"HeadObject": "headObjectContext", "ListObjects": "listObjectsContext",
                        "ListObjectsV2": "listObjectsV2Context"}[operation]
        event = _event(record, transformation, context_name, {"inputS3Url": input_url}, url, headers,
                       credential.access_key_id)
        result = await _invoke(func_record, event)
        failed = _lambda_failure(result)
        if failed:
            return failed
        return _structured_response(operation, result.get("body"))

    route, request_token = _new_route(region), new_uuid()
    pending = _register(route, request_token)
    event = _event(record, transformation, "getObjectContext",
                   {"inputS3Url": input_url, "outputRoute": route, "outputToken": request_token},
                   url, headers, credential.access_key_id)
    try:
        result = await _invoke(func_record, event)
    finally:
        with s3._olap_requests_lock:
            s3._olap_requests.pop((route, request_token), None)
    if not pending["done"]:
        return _lambda_failure(result) or _error(
            "LambdaResponseNotReceived", "The Lambda function exited without calling WriteGetObjectResponse.", 500)
    if pending["error"]:
        code, message = pending["error"]
        status = pending["status"] if pending["status"] >= 400 else 500
        return _error(code, message, status)
    return pending["status"], {"Content-Type": "application/octet-stream", **pending["headers"]}, pending["body"]


def _structured_response(operation: str, returned) -> tuple:
    """HeadObject and list transformations return JSON instead of calling
    WriteGetObjectResponse (S3 user guide)."""
    if isinstance(returned, str):
        try:
            returned = json.loads(returned)
        except ValueError:
            returned = None
    if not isinstance(returned, dict):
        return _error("LambdaRuntimeError", "The Lambda function returned a malformed response.", 500)
    status = int(returned.get("statusCode") or 200)
    if operation == "HeadObject":
        headers = {_response_header_name(k.lower()): str(v) for k, v in (returned.get("headers") or {}).items()}
        headers.update({f"x-amz-meta-{k}": str(v) for k, v in (returned.get("metadata") or {}).items()})
        return status, headers, b""
    if status >= 300:
        return _error(returned.get("errorCode") or "LambdaRuntimeError",
                      returned.get("errorMessage") or "The Lambda function failed.", status)
    xml = _list_result_xml(returned)
    if xml is None:
        return _error("LambdaRuntimeError", "The Lambda function returned a malformed response.", 500)
    return status, {"Content-Type": "application/xml"}, xml
