"""
EC2 Instance Metadata Service (IMDS) emulator.

Implements the link-local 169.254.169.254 HTTP API that EC2 instances
(and ECS/EKS workloads) use to retrieve instance identity and temporary
security credentials. Routed by URL prefix (/latest/...).

Real EC2 binds 169.254.169.254 at the hypervisor; in ministack we expose
the same endpoints on the gateway port and let users redirect their SDK
via AWS_EC2_METADATA_SERVICE_ENDPOINT=http://localhost:4566.

Both IMDSv1 (token-less GET) and IMDSv2 (PUT /latest/api/token then GET
with X-aws-ec2-metadata-token) are supported. Set
MINISTACK_IMDS_V2_REQUIRED=1 to reject token-less requests, matching
real AWS hop-limit-1, IMDSv2-only instances.
"""

import json
import os
import secrets
import time

from ministack.core.responses import get_account_id, get_region, new_uuid
from ministack.services.iam import (
    _future,
    _gen_secret,
    _gen_session_access_key,
    _gen_session_token,
)

_DEFAULT_ROLE = "ministack-instance-role"
_INSTANCE_ID = "i-" + new_uuid().replace("-", "")[:17]

_tokens: dict[str, float] = {}


def reset():
    _tokens.clear()


def _text(body: str, status: int = 200):
    return status, {"Content-Type": "text/plain"}, body.encode()


def _json_resp(payload, status: int = 200):
    return status, {"Content-Type": "application/json"}, json.dumps(payload).encode()


def _unauthorized():
    return _text("Unauthorized", 401)


def _not_found():
    return _text("Not Found", 404)


def _v2_required() -> bool:
    return os.environ.get("MINISTACK_IMDS_V2_REQUIRED", "").lower() in ("1", "true", "yes")


def _check_token(headers: dict) -> bool:
    token = headers.get("x-aws-ec2-metadata-token", "")
    if not token:
        return not _v2_required()
    expiry = _tokens.get(token)
    if expiry is None:
        return False
    if expiry < time.time():
        _tokens.pop(token, None)
        return False
    return True


def _issue_token(headers: dict):
    ttl_raw = headers.get("x-aws-ec2-metadata-token-ttl-seconds", "21600")
    try:
        ttl = max(1, min(21600, int(ttl_raw)))
    except (TypeError, ValueError):
        return _text("Invalid TTL", 400)
    token = secrets.token_urlsafe(43)
    _tokens[token] = time.time() + ttl
    headers_out = {
        "Content-Type": "text/plain",
        "X-Aws-Ec2-Metadata-Token-Ttl-Seconds": str(ttl),
    }
    return 200, headers_out, token.encode()


def _credentials_doc() -> dict:
    return {
        "Code": "Success",
        "LastUpdated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "Type": "AWS-HMAC",
        "AccessKeyId": _gen_session_access_key(),
        "SecretAccessKey": _gen_secret(),
        "Token": _gen_session_token(),
        "Expiration": _future(3600),
    }


def _identity_document() -> dict:
    return {
        "accountId": get_account_id(),
        "architecture": "x86_64",
        "availabilityZone": f"{get_region()}a",
        "imageId": "ami-ministack",
        "instanceId": _INSTANCE_ID,
        "instanceType": "t3.micro",
        "pendingTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "privateIp": "127.0.0.1",
        "region": get_region(),
        "version": "2017-09-30",
    }


_META_FLAT: dict[str, str] = {
    "ami-id": "ami-ministack",
    "ami-launch-index": "0",
    "ami-manifest-path": "(unknown)",
    "hostname": "ministack.local",
    "instance-action": "none",
    "instance-life-cycle": "on-demand",
    "instance-type": "t3.micro",
    "local-hostname": "ministack.local",
    "local-ipv4": "127.0.0.1",
    "mac": "02:00:00:00:00:00",
    "profile": "default-hvm",
    "public-hostname": "ministack.local",
    "public-ipv4": "127.0.0.1",
    "reservation-id": "r-" + new_uuid().replace("-", "")[:17],
    "security-groups": "default",
}


async def handle_request(method, path, headers, body, query_params):
    method = method.upper()
    path = path.rstrip("/") or path

    if path.startswith("/v2/credentials/"):
        if method != "GET":
            return _text("Method Not Allowed", 405)
        return _json_resp(_credentials_doc())

    if path.startswith("/latest/api/token"):
        if method != "PUT":
            return _text("Method Not Allowed", 405)
        return _issue_token(headers)

    if not _check_token(headers):
        return _unauthorized()

    if method != "GET":
        return _text("Method Not Allowed", 405)

    if path == "/latest/meta-data" or path == "/latest/meta-data/":
        keys = sorted(set(list(_META_FLAT.keys()) + [
            "instance-id", "placement/", "iam/", "services/",
        ]))
        return _text("\n".join(keys))

    if path == "/latest/meta-data/instance-id":
        return _text(_INSTANCE_ID)

    if path.startswith("/latest/meta-data/placement/"):
        leaf = path[len("/latest/meta-data/placement/"):]
        region = get_region()
        placement = {
            "availability-zone": f"{region}a",
            "availability-zone-id": f"{region[:3]}1-az1",
            "region": region,
        }
        if leaf == "":
            return _text("\n".join(sorted(placement.keys())))
        if leaf in placement:
            return _text(placement[leaf])
        return _not_found()

    if path == "/latest/meta-data/iam/security-credentials" or \
            path == "/latest/meta-data/iam/security-credentials/":
        return _text(_DEFAULT_ROLE)

    if path.startswith("/latest/meta-data/iam/security-credentials/"):
        role = path[len("/latest/meta-data/iam/security-credentials/"):]
        if role != _DEFAULT_ROLE:
            return _not_found()
        return _json_resp(_credentials_doc())

    if path == "/latest/meta-data/iam/info":
        return _json_resp({
            "Code": "Success",
            "LastUpdated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "InstanceProfileArn":
                f"arn:aws:iam::{get_account_id()}:instance-profile/{_DEFAULT_ROLE}",
            "InstanceProfileId": "AIPAMINISTACK0000000",
        })

    if path == "/latest/dynamic/instance-identity/document":
        return _json_resp(_identity_document())

    if path == "/latest/dynamic/instance-identity/pkcs7" or \
            path == "/latest/dynamic/instance-identity/signature":
        return _text("ministack-unsigned")

    if path.startswith("/latest/meta-data/"):
        leaf = path[len("/latest/meta-data/"):]
        if leaf in _META_FLAT:
            return _text(_META_FLAT[leaf])

    return _not_found()
