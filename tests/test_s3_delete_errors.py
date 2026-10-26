"""DeleteObject observations from real AWS, Osaka, 2026-10-23.

AWS.Tools.S3 5.0.318 / AWS CLI 2.38.1 used explicit Osaka/Tokyo endpoints
and bucket-scoped DeleteObject grants. These tests replay the first HTTP
response locally, before SDK correction. No real AWS requests are made.
"""
import base64
import hashlib
import http.client
import os
import re
import uuid
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from xml.etree.ElementTree import fromstring

import boto3
import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
HOME, TOKYO = "ap-northeast-3", "ap-northeast-1"
PAYLOAD = b"delete error validation"
pytestmark = pytest.mark.auth_contract


@pytest.fixture
def delete_bucket(s3):
    name = f"delete-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=PAYLOAD)
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def delete_client(delete_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"arn:aws:s3:::{delete_bucket}/*"},
    ]}).client("s3", region=HOME)


def _send(host, path, headers):
    target = urlsplit(ENDPOINT)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("DELETE", path, headers={**headers, "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _delete(bucket, client, *, credential="valid", endpoint_region=HOME, region=HOME,
            virtual=False, key="probe.txt", query="", root=False, extra_headers=None):
    frozen = client._request_signer._credentials.get_frozen_credentials()
    access_key, secret = frozen.access_key, frozen.secret_key
    if credential == "unknown":
        access_key, secret = "AKIAUNKNOWNKEY000001", "wrong"
    elif credential == "wrong-secret":
        secret = "wrong"
    if root:
        access_key, secret = "test", "any-secret"
    host = f"s3.{endpoint_region}.{urlsplit(ENDPOINT).netloc}"
    path = f"/{quote(bucket)}/{quote(key, safe='/')}"
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="DELETE", url=f"http://{host}{path}{query}", headers=extra_headers)
    S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    return _send(host, path + query, dict(request.headers))


def _assert_state(client, bucket, *, deleted):
    if deleted:
        with pytest.raises(ClientError) as exc:
            client.get_object(Bucket=bucket, Key="probe.txt")
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    else:
        with client.get_object(Bucket=bucket, Key="probe.txt")["Body"] as body:
            assert body.read() == PAYLOAD


def _assert_error(response, code, *, bucket, region=HOME, endpoint_region=HOME, access_key=None):
    _, headers, body = response
    error = fromstring(body)
    messages = {
        "InvalidBucketName": "The specified bucket is not valid.",
        "InvalidAccessKeyId": "The AWS Access Key Id you provided does not exist in our records.",
        "SignatureDoesNotMatch": "The request signature we calculated does not match the signature you provided. "
                                 "Check your key and signing method.",
        "AuthorizationHeaderMalformed": f"The authorization header is malformed; the region '{region}' is wrong; "
                                        f"expecting '{endpoint_region}'",
        "PermanentRedirect": "The bucket you are attempting to access must be addressed using the specified "
                             "endpoint. Please send all future requests to this endpoint.",
        "NoSuchBucket": "The specified bucket does not exist",
    }
    details = {
        "InvalidBucketName": ["BucketName"], "InvalidAccessKeyId": ["AWSAccessKeyId"],
        "AuthorizationHeaderMalformed": ["Region"], "PermanentRedirect": ["Endpoint", "Bucket"],
        "NoSuchBucket": ["BucketName"],
        "SignatureDoesNotMatch": ["AWSAccessKeyId", "StringToSign", "SignatureProvided", "StringToSignBytes",
                                  "CanonicalRequest", "CanonicalRequestBytes"],
    }
    expected = ["Code", "Message", *details[code], "RequestId", "HostId"]
    assert error.findtext("Code") == code
    assert error.findtext("Message") == messages[code]
    if code == "SignatureDoesNotMatch":
        # Diagnostic membership and consistency matter even if AWS varies field order.
        assert sorted(child.tag for child in error) == sorted(expected)
        for name in ("CanonicalRequest", "StringToSign"):
            assert bytes.fromhex(error.findtext(name + "Bytes")).decode() == error.findtext(name)
        assert error.findtext("CanonicalRequest").startswith("DELETE\n")
        document = error.findtext("StringToSign")
        assert document.splitlines()[-1] == hashlib.sha256(error.findtext("CanonicalRequest").encode()).hexdigest()
        signed = AWSRequest()
        signed.context["timestamp"] = document.splitlines()[1]
        signer = S3SigV4Auth(Credentials(error.findtext("AWSAccessKeyId"), "wrong"), "s3", region)
        assert signer.signature(document, signed) == error.findtext("SignatureProvided")
    else:
        assert [child.tag for child in error] == expected
    assert headers["content-type"] == "application/xml"
    assert error.findtext("RequestId") == headers["x-amz-request-id"]
    assert re.fullmatch(r"[A-Z0-9]{16}", error.findtext("RequestId"))
    assert error.findtext("HostId") == headers["x-amz-id-2"]
    assert len(base64.b64decode(error.findtext("HostId"), validate=True)) >= 48
    assert "location" not in headers
    if code == "PermanentRedirect":
        assert error.findtext("Endpoint") == f"{bucket}.s3.{HOME}.{urlsplit(ENDPOINT).netloc}"
        assert error.findtext("Bucket") == bucket
        assert headers["x-amz-bucket-region"] == HOME
    else:
        assert "x-amz-bucket-region" not in headers
    if code in {"InvalidBucketName", "NoSuchBucket"}:
        assert error.findtext("BucketName") == bucket
    if code == "AuthorizationHeaderMalformed":
        assert error.findtext("Region") == endpoint_region
    if access_key is not None:
        assert error.findtext("AWSAccessKeyId") == access_key


@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations; DELETE success must also remove the object.
    (False, "valid", HOME, HOME, (204, None), (204, None)),
    (False, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (204, None)),
    (False, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (204, None)),
    (True, "valid", HOME, HOME, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
    (False, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (True, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    # Existing-bucket valid/unknown + Tokyo endpoint/Osaka signature live in
    # test_delete_home_precedes_signing_region_and_credentials, including state checks.
    (False, "wrong-secret", TOKYO, HOME, (301, "PermanentRedirect"), (204, None)),
    (True, "valid", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (False, "valid", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "unknown", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "wrong-secret", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (True, "valid", TOKYO, TOKYO, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, TOKYO, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, TOKYO, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
])
def test_delete_combined_errors(delete_bucket, delete_client, s3, auth_enabled,
                                virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{delete_bucket}" if missing else delete_bucket
    expected = expected if auth_enabled else compat_expected
    response = _delete(bucket, delete_client, virtual=virtual, credential=credential,
                       endpoint_region=endpoint_region, region=region)
    assert (response[0], fromstring(response[2]).findtext("Code") if response[2] else None) == expected
    _assert_state(s3, delete_bucket, deleted=not missing and response[0] == 204)
    if response[0] == 204:
        assert response[2] == b""
    elif auth_enabled:
        access_key = "AKIAUNKNOWNKEY000001" if credential == "unknown" else delete_client._request_signer._credentials.access_key
        _assert_error(response, expected[1], bucket=bucket, region=region, endpoint_region=endpoint_region,
                      access_key=access_key if expected[1] in {"InvalidAccessKeyId", "SignatureDoesNotMatch"} else None)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_delete_home_precedes_signing_region_and_credentials(delete_bucket, delete_client, s3,
                                                             auth_enabled, virtual, credential):
    # Fixed AWS observation: redirect, including with an unknown access key.
    response = _delete(delete_bucket, delete_client, virtual=virtual, credential=credential,
                       endpoint_region=TOKYO, region=HOME)
    assert response[0] == (301 if auth_enabled else 204)
    if auth_enabled:
        _assert_error(response, "PermanentRedirect", bucket=delete_bucket, endpoint_region=TOKYO)
    else:
        assert response[2] == b""
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_delete_missing_key_is_idempotent(delete_bucket, delete_client, s3, auth_enabled, virtual, credential, region):
    response = _delete(delete_bucket, delete_client, virtual=virtual, credential=credential, region=region, key="missing.txt")
    code = ("AuthorizationHeaderMalformed" if region != HOME else
            "InvalidAccessKeyId" if auth_enabled and credential == "unknown" else
            "SignatureDoesNotMatch" if auth_enabled and credential == "wrong-secret" else None)
    assert (response[0], fromstring(response[2]).findtext("Code") if response[2] else None) == (
        400 if region != HOME else 403 if code else 204, code)
    if code and auth_enabled:
        _assert_error(response, code, bucket=delete_bucket, region=region)
    if not code:
        assert response[2] == b""
        assert _delete(delete_bucket, delete_client, virtual=virtual, key="missing.txt")[0] == 204
    _assert_state(s3, delete_bucket, deleted=False)


def test_delete_invalid_name_precedes_credentials(delete_client, auth_enabled):
    response = _delete("invalid bucket", delete_client, credential="unknown")
    assert response[0] == (400 if auth_enabled else 404)
    if auth_enabled:
        _assert_error(response, "InvalidBucketName", bucket="invalid bucket")


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_delete_still_enforces_iam(delete_bucket, iam_user, s3, auth_enabled, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _delete(delete_bucket, client, virtual=virtual)
    assert response[0] == (403 if auth_enabled else 204)
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)
    assert _delete(delete_bucket, client, virtual=virtual, key="missing.txt")[0] == (403 if auth_enabled else 204)


@pytest.mark.parametrize("virtual", [False, True])
def test_delete_reserved_root_compatibility(delete_bucket, s3, virtual):
    assert _delete(delete_bucket, s3, virtual=virtual, root=True)[0] == 204
    _assert_state(s3, delete_bucket, deleted=True)


@pytest.mark.parametrize("virtual", [False, True])
def test_delete_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-delete-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=PAYLOAD)
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "*"}]}).client("s3")
    try:
        assert _delete(bucket, client, virtual=virtual)[0] == 403
        _assert_state(owner, bucket, deleted=False)
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("query", ["?versionId=null", "?tagging"])
def test_delete_subresources_keep_their_iam_path(delete_bucket, iam_user, auth_enabled, virtual, query):
    client = iam_user().client("s3", region=HOME)
    response = _delete(f"missing-{delete_bucket}", client, virtual=virtual, query=query)
    assert response[0] == (403 if auth_enabled else 404)


@pytest.mark.parametrize("virtual", [False, True])
def test_delete_version_needs_its_own_permission(delete_bucket, delete_client, s3, auth_enabled, virtual):
    response = _delete(delete_bucket, delete_client, virtual=virtual, query="?versionId=null")
    assert response[0] == (403 if auth_enabled else 204)
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)


@pytest.mark.parametrize("virtual", [False, True])
def test_delete_failed_precondition_preserves_object(delete_bucket, delete_client, s3, virtual):
    response = _delete(delete_bucket, delete_client, virtual=virtual, extra_headers={"If-Match": '"incorrect"'})
    assert (response[0], fromstring(response[2]).findtext("Code")) == (412, "PreconditionFailed")
    _assert_state(s3, delete_bucket, deleted=False)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("invalid", [False, True], ids=["valid", "bad-signature"])
def test_delete_presigned_validation_is_preserved(delete_bucket, iam_user, s3, virtual, invalid):
    client = iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"arn:aws:s3:::{delete_bucket}/*"},
    ]}).client("s3", region=HOME, config=Config(signature_version="s3v4", s3={"addressing_style": "virtual" if virtual else "path"}))
    target = urlsplit(client.generate_presigned_url("delete_object", Params={"Bucket": delete_bucket, "Key": "probe.txt"}))
    query = dict(parse_qsl(target.query))
    if invalid:
        signature = query["X-Amz-Signature"]
        query["X-Amz-Signature"] = ("0" if signature[0] != "0" else "1") + signature[1:]
    response = _send(target.netloc, target.path + "?" + urlencode(query), {})
    assert response[0] == (403 if invalid else 204)
    _assert_state(s3, delete_bucket, deleted=not invalid)
