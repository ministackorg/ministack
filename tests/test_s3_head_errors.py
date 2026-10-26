"""Real AWS HEAD observations (Osaka, 2026-10-23), replayed only against MiniStack.

AWS.Tools.S3 5.0.318 and AWS CLI 2.38.1 used a bucket-scoped GetObject/ListBucket
grant and explicit Osaka/Tokyo endpoints. Assertions describe the first HTTP
response, before SDK region correction. HEAD has no XML error diagnostics.
"""
import base64
import http.client
import os
import re
import uuid
from urllib.parse import quote, urlsplit

import boto3
import pytest
from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
HOME, TOKYO = "ap-northeast-3", "ap-northeast-1"
PAYLOAD = b"head error validation"
pytestmark = pytest.mark.auth_contract


@pytest.mark.parametrize("source", ["region", "authentication", "normal", "iam", "missing-request", "missing-host"])
def test_head_finalizer_preserves_valid_identifiers(source):
    """Pure formatting contract: no AUTH/IAM patch, no live AWS request."""
    from ministack.core.iam_actions import access_denied_response
    from ministack.services import s3

    host_id = base64.b64encode(b"existing-host-id" * 4).decode()
    if source in {"region", "authentication", "missing-host"}:
        code, status = ("PermanentRedirect", 301) if source == "region" else ("InvalidAccessKeyId", 403)
        response = s3._object_error(code, "formatting probe", status)
        if source == "missing-host":
            response[1].pop("x-amz-id-2")
    elif source == "iam":
        response = access_denied_response("s3", "s3:GetObject", "arn:aws:iam::000000000000:user/probe", "legacy-id")
    else:
        response = 200, {"Content-Type": "text/plain", "Content-Length": "4", "x-amz-id-2": host_id}, b""
        if source == "normal":
            response[1]["x-amz-request-id"] = "12345678-1234-1234-1234-123456789abc"
    original_headers = dict(response[1])
    result = s3.format_head_response("/id-format-probe/key", {}, response)
    request_id = result[1]["x-amz-request-id"]
    assert re.fullmatch(r"[A-Z0-9]{16}", request_id)
    if re.fullmatch(r"[A-Z0-9]{16}", original_headers.get("x-amz-request-id", "")):
        assert request_id == original_headers["x-amz-request-id"]
    if original_headers.get("x-amz-id-2"):
        assert result[1]["x-amz-id-2"] == original_headers["x-amz-id-2"]
    assert len(base64.b64decode(result[1]["x-amz-id-2"], validate=True)) >= 48
    assert result[2] == b""
    assert s3.format_head_response("/id-format-probe/key", {}, result) == result
    assert response[1] == original_headers


@pytest.fixture
def head_bucket(s3):
    name = f"head-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=PAYLOAD, ContentType="text/plain")
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def head_client(head_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{head_bucket}/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{head_bucket}"},
    ]}).client("s3", region=HOME)


def _head(bucket, client, *, key="probe.txt", virtual=False, credential="valid",
          endpoint_region=HOME, region=HOME, root=False, query="", extra_headers=None, presigned=False):
    frozen = client._request_signer._credentials.get_frozen_credentials()
    access_key, secret = frozen.access_key, frozen.secret_key
    if credential == "unknown":
        access_key, secret = "AKIAUNKNOWNKEY000001", "wrong"
    elif credential == "wrong-secret":
        secret = "wrong"
    if root:
        access_key, secret = "test", "any-secret"
    target = urlsplit(ENDPOINT)
    host = f"s3.{endpoint_region}.{target.netloc}"
    path = f"/{quote(bucket)}" + (f"/{quote(key, safe='/')}" if key else "")
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="HEAD", url=f"http://{host}{path}{query}", headers=extra_headers)
    if presigned:
        S3SigV4QueryAuth(Credentials(access_key, secret), "s3", region, expires=300).add_auth(request)
        signed_url = urlsplit(request.url)
        path, query = signed_url.path, "?" + signed_url.query
    else:
        S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("HEAD", path + query, headers={**dict(request.headers), "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _assert_wire(response, *, bucket, key, expected_region=None):
    status, headers, body = response
    assert body == b""
    assert headers.get("x-amz-bucket-region") == expected_region
    assert "x-amz-error-code" not in headers
    assert "x-amz-error-message" not in headers
    assert re.fullmatch(r"[A-Z0-9]{16}", headers["x-amz-request-id"])
    assert len(base64.b64decode(headers["x-amz-id-2"], validate=True)) >= 48
    if key and status == 200:
        assert headers["content-length"] == str(len(PAYLOAD))
        assert headers["content-type"] == "text/plain"
    else:
        assert "content-length" not in headers
        assert headers["content-type"] == "application/xml"
    assert headers.get("x-amz-bucket-arn") == (f"arn:aws:s3:::{bucket}" if not key and status == 200 else None)


@pytest.mark.parametrize("key", ["probe.txt", ""], ids=["HeadObject", "HeadBucket"])
@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations, separately for HeadObject and HeadBucket.
    # Unlike GET, HEAD checks the bucket's home before the signing scope.
    (False, "valid", HOME, HOME, 200, 200),
    (False, "unknown", HOME, HOME, 403, 200),
    (False, "wrong-secret", HOME, HOME, 403, 200),
    (True, "valid", HOME, HOME, 404, 404),
    (True, "unknown", HOME, HOME, 403, 404),
    (True, "wrong-secret", HOME, HOME, 403, 404),
    (False, "valid", HOME, TOKYO, 400, 400),
    (False, "unknown", HOME, TOKYO, 400, 400),
    (False, "wrong-secret", HOME, TOKYO, 400, 400),
    (True, "valid", HOME, TOKYO, 400, 404),
    (True, "unknown", HOME, TOKYO, 400, 404),
    (True, "wrong-secret", HOME, TOKYO, 400, 404),
    # Existing-bucket valid/unknown + Tokyo endpoint/Osaka signature live in
    # test_head_home_precedes_signing_region_and_credentials, including wire checks.
    (False, "wrong-secret", TOKYO, HOME, 301, 200),
    (True, "valid", TOKYO, HOME, 400, 404),
    (True, "unknown", TOKYO, HOME, 400, 404),
    (True, "wrong-secret", TOKYO, HOME, 400, 404),
    (False, "valid", TOKYO, TOKYO, 301, 301),
    (False, "unknown", TOKYO, TOKYO, 301, 301),
    (False, "wrong-secret", TOKYO, TOKYO, 301, 301),
    (True, "valid", TOKYO, TOKYO, 404, 404),
    (True, "unknown", TOKYO, TOKYO, 403, 404),
    (True, "wrong-secret", TOKYO, TOKYO, 403, 404),
])
def test_head_combined_errors(head_bucket, head_client, auth_enabled,
                              key, virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{head_bucket}" if missing else head_bucket
    expected = expected if auth_enabled else compat_expected
    response = _head(bucket, head_client, key=key, virtual=virtual, credential=credential,
                     endpoint_region=endpoint_region, region=region)
    assert response[0] == expected
    assert response[2] == b""
    if auth_enabled:
        _assert_wire(response, bucket=bucket, key=key,
                     expected_region=HOME if not missing and (not key or expected == 301) else None)


@pytest.mark.parametrize("key", ["probe.txt", ""], ids=["HeadObject", "HeadBucket"])
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_head_home_precedes_signing_region_and_credentials(head_bucket, head_client, s3,
                                                           auth_enabled, key, virtual, credential):
    # Fixed AWS regression: home-region redirect wins over signing/authentication errors.
    response = _head(head_bucket, head_client, key=key, virtual=virtual, credential=credential,
                     endpoint_region=TOKYO, region=HOME)
    assert response[0] == (301 if auth_enabled else 200)
    assert response[2] == b""
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME)
    with s3.get_object(Bucket=head_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == PAYLOAD


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_head_missing_key(head_bucket, head_client, auth_enabled, virtual, credential, region):
    response = _head(head_bucket, head_client, key="missing.txt", virtual=virtual, credential=credential, region=region)
    expected = 400 if region != HOME else 403 if auth_enabled and credential != "valid" else 404
    assert response[0] == expected
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key="missing.txt")


@pytest.mark.parametrize("key", ["probe.txt", ""])
def test_head_invalid_name_precedes_credentials(head_client, auth_enabled, key):
    response = _head("invalid bucket", head_client, key=key, credential="unknown")
    assert response[0] == (400 if auth_enabled else 404)
    if auth_enabled:
        _assert_wire(response, bucket="invalid bucket", key=key)


@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_head_still_enforces_iam(head_bucket, iam_user, auth_enabled, key, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _head(head_bucket, client, key=key, virtual=virtual)
    assert response[0] == (403 if auth_enabled else 200)
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME if not key else None)


@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
def test_head_reserved_root_compatibility(head_bucket, s3, auth_enabled, key, virtual):
    response = _head(head_bucket, s3, key=key, virtual=virtual, root=True)
    assert response[0] == 200
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME if not key else None)


@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
def test_head_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, key, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-head-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=PAYLOAD)
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}).client("s3")
    try:
        response = _head(bucket, client, key=key, virtual=virtual)
        assert response[0] == 403
        if auth_enabled:
            _assert_wire(response, bucket=bucket, key=key, expected_region=HOME if not key else None)
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()


@pytest.mark.parametrize("virtual", [False, True])
def test_head_version_query_keeps_iam_path(head_bucket, iam_user, auth_enabled, virtual):
    client = iam_user().client("s3", region=HOME)
    response = _head(f"missing-{head_bucket}", client, virtual=virtual, query="?versionId=null")
    assert response[0] == (403 if auth_enabled else 404)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "wrong-secret"])
def test_head_presigned_validation_is_preserved(head_bucket, head_client, s3, virtual, credential):
    response = _head(head_bucket, head_client, virtual=virtual, credential=credential, presigned=True)
    assert response[0] == (200 if credential == "valid" else 403)
    assert response[2] == b""
    if credential == "valid":
        assert response[1]["content-length"] == str(len(PAYLOAD))
        assert response[1]["content-type"] == "text/plain"
    with s3.get_object(Bucket=head_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == PAYLOAD


@pytest.mark.parametrize("virtual", [False, True])
def test_head_conditional_metadata(head_bucket, head_client, auth_enabled, virtual):
    response = _head(head_bucket, head_client, virtual=virtual)
    assert response[0] == 200
    assert response[1]["content-length"] == str(len(PAYLOAD))
    assert response[2] == b""
    etag = response[1]["etag"]
    response = _head(head_bucket, head_client, virtual=virtual, extra_headers={"If-None-Match": etag})
    assert response[0] == 304
    assert response[2] == b""
    if auth_enabled:
        assert "content-length" not in response[1]
        assert "content-type" not in response[1]
    response = _head(head_bucket, head_client, virtual=virtual, extra_headers={"If-Match": '"incorrect"'})
    assert response[0] == 412
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key="probe.txt")
