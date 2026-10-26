"""GetObject observations from real AWS, Osaka, 2026-10-18.

AWS CLI 2.38.1 / AWS.Tools.S3 5.0.318 used explicit Osaka/Tokyo endpoints
and a bucket-scoped GetObject/ListBucket grant. These tests replay HTTP
observations locally, before client region correction; they never call AWS.
AUTH=false assertions preserve MiniStack's permissive compatibility.
"""
import base64
import hashlib
import http.client
import os
import re
import uuid
from urllib.parse import quote, urlsplit
from xml.etree.ElementTree import fromstring

import boto3
import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
HOME, TOKYO = "ap-northeast-3", "ap-northeast-1"
PAYLOAD = b"download error validation"
pytestmark = pytest.mark.auth_contract


@pytest.fixture
def download_bucket(s3):
    name = f"download-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=PAYLOAD)
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def download_client(download_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{download_bucket}/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{download_bucket}"},
    ]}).client("s3", region=HOME)


def _download(bucket, client, *, credential="valid", endpoint_region=HOME, region=HOME,
              virtual=False, key="probe.txt", query="", root=False):
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
    path = f"/{quote(bucket)}/{quote(key, safe='/')}"
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="GET", url=f"http://{host}{path}{query}")
    S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("GET", path + query, headers={**dict(request.headers), "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _assert_error(response, code, *, bucket, key="probe.txt", region=HOME, access_key=None):
    _, headers, body = response
    error = fromstring(body)
    messages = {
        "InvalidBucketName": "The specified bucket is not valid.",
        "InvalidAccessKeyId": "The AWS Access Key Id you provided does not exist in our records.",
        "SignatureDoesNotMatch": "The request signature we calculated does not match the signature you provided. "
                                 "Check your key and signing method.",
        "AuthorizationHeaderMalformed": f"The authorization header is malformed; the region '{region}' is wrong; "
                                        f"expecting '{HOME if region == TOKYO else TOKYO}'",
        "PermanentRedirect": "The bucket you are attempting to access must be addressed using the specified "
                             "endpoint. Please send all future requests to this endpoint.",
        "NoSuchBucket": "The specified bucket does not exist",
        "NoSuchKey": "The specified key does not exist.",
    }
    details = {
        "InvalidBucketName": ["BucketName"], "InvalidAccessKeyId": ["AWSAccessKeyId"],
        "AuthorizationHeaderMalformed": ["Region"], "PermanentRedirect": ["Bucket", "Endpoint"],
        "NoSuchBucket": ["BucketName"], "NoSuchKey": ["Key"],
        "SignatureDoesNotMatch": ["AWSAccessKeyId", "StringToSign", "SignatureProvided", "StringToSignBytes",
                                  "CanonicalRequest", "CanonicalRequestBytes"],
    }
    expected = ["Code", "Message", *details[code], "RequestId", "HostId"]
    assert error.findtext("Code") == code
    assert error.findtext("Message") == messages[code]
    if code == "SignatureDoesNotMatch":
        # Real GET responses varied the order of the signature diagnostics.
        assert sorted(child.tag for child in error) == sorted(expected)
        for name in ("CanonicalRequest", "StringToSign"):
            assert bytes.fromhex(error.findtext(name + "Bytes")).decode() == error.findtext(name)
        assert error.findtext("CanonicalRequest").startswith("GET\n")
        assert error.findtext("StringToSign").splitlines()[-1] == hashlib.sha256(
            error.findtext("CanonicalRequest").encode()).hexdigest()
        assert re.fullmatch(r"[a-f0-9]{64}", error.findtext("SignatureProvided"))
        signed = AWSRequest()
        document = error.findtext("StringToSign")
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
    if code == "NoSuchKey":
        assert error.findtext("Key") == key
    if code == "AuthorizationHeaderMalformed":
        assert error.findtext("Region") == (HOME if region == TOKYO else TOKYO)
    if access_key is not None:
        assert error.findtext("AWSAccessKeyId") == access_key


@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations; AUTH=false is a separate local contract.
    (False, "valid", HOME, HOME, (200, None), (200, None)),
    (False, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (200, None)),
    (False, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (200, None)),
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
    # test_download_signing_region_precedes_home_and_credentials, including wire checks.
    (False, "wrong-secret", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (200, None)),
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
def test_download_combined_errors(download_bucket, download_client, auth_enabled,
                                  virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{download_bucket}" if missing else download_bucket
    expected = expected if auth_enabled else compat_expected
    response = _download(bucket, download_client, credential=credential, endpoint_region=endpoint_region,
                         region=region, virtual=virtual)
    code = fromstring(response[2]).findtext("Code") if response[0] != 200 else None
    assert (response[0], code) == expected
    if response[0] == 200:
        assert response[2] == PAYLOAD
    elif auth_enabled:
        access_key = ("AKIAUNKNOWNKEY000001" if credential == "unknown"
                      else download_client._request_signer._credentials.access_key)
        _assert_error(response, code, bucket=bucket, region=region,
                      access_key=access_key if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch"} else None)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_download_signing_region_precedes_home_and_credentials(download_bucket, download_client, s3,
                                                                auth_enabled, virtual, credential):
    # Fixed AWS observation: Osaka bucket + Tokyo endpoint + Osaka signature.
    # Kept separately as the regression for GET's different priority from HEAD/DELETE.
    response = _download(download_bucket, download_client, virtual=virtual, credential=credential,
                         endpoint_region=TOKYO, region=HOME)
    assert response[0] == (400 if auth_enabled else 200)
    if auth_enabled:
        _assert_error(response, "AuthorizationHeaderMalformed", bucket=download_bucket, region=HOME)
    else:
        assert response[2] == PAYLOAD
    with s3.get_object(Bucket=download_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == PAYLOAD


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_download_missing_key(download_bucket, download_client, auth_enabled, virtual, credential, region):
    response = _download(download_bucket, download_client, virtual=virtual, credential=credential,
                         region=region, key="missing.txt")
    code = ("AuthorizationHeaderMalformed" if region != HOME else
            "InvalidAccessKeyId" if credential == "unknown" else
            "SignatureDoesNotMatch" if credential == "wrong-secret" else "NoSuchKey")
    if not auth_enabled:
        code = "AuthorizationHeaderMalformed" if region != HOME else "NoSuchKey"
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        400 if code == "AuthorizationHeaderMalformed" else 404 if code == "NoSuchKey" else 403, code,
    )
    if auth_enabled:
        _assert_error(response, code, bucket=download_bucket, key="missing.txt", region=region)


def test_download_invalid_name_precedes_credentials(download_client, auth_enabled):
    response = _download("invalid bucket", download_client, credential="unknown")
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        (400, "InvalidBucketName") if auth_enabled else (404, "NoSuchBucket")
    )
    if auth_enabled:
        _assert_error(response, "InvalidBucketName", bucket="invalid bucket")


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_download_still_enforces_iam(download_bucket, iam_user, auth_enabled, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": "s3:GetObject", "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _download(download_bucket, client, virtual=virtual)
    if auth_enabled:
        assert (response[0], fromstring(response[2]).findtext("Code")) == (403, "AccessDenied")
    else:
        assert (response[0], response[2]) == (200, PAYLOAD)


@pytest.mark.parametrize("virtual", [False, True])
def test_download_reserved_root_compatibility(download_bucket, s3, virtual):
    response = _download(download_bucket, s3, virtual=virtual, root=True)
    assert (response[0], response[2]) == (200, PAYLOAD)


@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("query", ["?versionId=null", "?tagging"])
def test_download_subresources_keep_their_iam_path(download_bucket, iam_user, auth_enabled, virtual, query):
    client = iam_user().client("s3", region=HOME)
    response = _download(f"missing-{download_bucket}", client, virtual=virtual, query=query)
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        (403, "AccessDenied") if auth_enabled else (404, "NoSuchBucket")
    )


@pytest.mark.parametrize("virtual", [False, True])
def test_download_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-download-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=b"private")
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}).client("s3")
    try:
        response = _download(bucket, client, virtual=virtual)
        assert (response[0], fromstring(response[2]).findtext("Code")) == (403, "AccessDenied")
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()
