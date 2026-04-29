import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_sts_get_caller_identity(sts):
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"

def test_sts_assume_role_returns_credentials(sts):
    resp = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/test-role",
        RoleSessionName="intg-session",
    )
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds
    assert resp["AssumedRoleUser"]["Arn"]

def test_sts_get_access_key_info(sts):
    resp = sts.get_access_key_info(AccessKeyId="AKIAIOSFODNN7EXAMPLE")
    assert "Account" in resp
    assert resp["Account"] == "000000000000"

def test_sts_get_caller_identity_full(sts):
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"
    assert "Arn" in resp
    assert "UserId" in resp

def test_sts_assume_role(sts):
    resp = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/iam-test-role",
        RoleSessionName="test-session",
        DurationSeconds=900,
    )
    creds = resp["Credentials"]
    assert creds["AccessKeyId"].startswith("ASIA")
    assert len(creds["SecretAccessKey"]) > 0
    assert len(creds["SessionToken"]) > 0
    assert "Expiration" in creds

    assumed = resp["AssumedRoleUser"]
    assert "test-session" in assumed["Arn"]
    assert "AssumedRoleId" in assumed


def test_sts_assumed_role_arn_uses_sts_service(sts):
    """Real AWS returns AssumeRole's AssumedRoleUser.Arn under the sts
    service, not iam — e.g. arn:aws:sts::123456789012:assumed-role/demo/Sess.
    Pinning this against future regressions."""
    resp = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/demo",
        RoleSessionName="TestAR",
    )
    arn = resp["AssumedRoleUser"]["Arn"]
    assert arn == "arn:aws:sts::000000000000:assumed-role/demo/TestAR", arn

    resp_wi = sts.assume_role_with_web_identity(
        RoleArn="arn:aws:iam::000000000000:role/demo",
        RoleSessionName="WebSess",
        WebIdentityToken="dummy.jwt.token",
    )
    arn_wi = resp_wi["AssumedRoleUser"]["Arn"]
    assert arn_wi == "arn:aws:sts::000000000000:assumed-role/demo/WebSess", arn_wi

def test_sts_get_session_token(sts):
    resp = sts.get_session_token(DurationSeconds=900)
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds

def test_sts_assume_role_with_web_identity(sts, iam):
    iam.create_role(
        RoleName="test-oidc-role",
        AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    role_arn = "arn:aws:iam::000000000000:role/test-oidc-role"
    resp = sts.assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName="ci-session",
        WebIdentityToken="fake-oidc-token-value",
    )
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds


def _gwit_post(data: bytes):
    """POST raw form-encoded body to STS GetWebIdentityToken (boto3 has no client method)."""
    import urllib.error
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        endpoint,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=fake",
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _b64url_pad(s):
    return s + "=" * (-len(s) % 4)


def test_sts_get_web_identity_token():
    """GetWebIdentityToken returns a JWT with AWS-spec claims (XML protocol)."""
    import base64
    import re
    status, body = _gwit_post(
        b"Action=GetWebIdentityToken&Audience=my-service&SigningAlgorithm=RS256&DurationSeconds=300"
    )
    assert status == 200
    assert "<WebIdentityToken>" in body
    token = re.search(r"<WebIdentityToken>(.+?)</WebIdentityToken>", body).group(1)
    parts = token.split(".")
    assert len(parts) == 3

    header = json.loads(base64.urlsafe_b64decode(_b64url_pad(parts[0])))
    payload = json.loads(base64.urlsafe_b64decode(_b64url_pad(parts[1])))

    assert header["typ"] == "JWT"
    # Header alg honestly reports HS256 (the actual signature algorithm used by
    # the emulator); SigningAlgorithm in the request is validated separately.
    assert header["alg"] == "HS256"
    assert payload["aud"] == "my-service"
    assert payload["iss"] == "https://sts.amazonaws.com"
    assert "sub" in payload
    assert "exp" in payload
    assert payload["exp"] - payload["iat"] == 300


def test_sts_get_web_identity_token_json_protocol():
    """JSON protocol returns int-epoch Expiration per ministack convention."""
    import urllib.error
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        endpoint,
        data=json.dumps({"Audience": "x", "SigningAlgorithm": "RS256"}).encode(),
        method="POST",
        headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSSecurityTokenServiceV20110615.GetWebIdentityToken",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=fake",
        },
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
        data = json.loads(r.read())
    assert "WebIdentityToken" in data
    assert isinstance(data["Expiration"], int)


def test_sts_get_web_identity_token_es384():
    """ES384 is also a valid SigningAlgorithm value."""
    status, body = _gwit_post(b"Action=GetWebIdentityToken&Audience=x&SigningAlgorithm=ES384")
    assert status == 200


def test_sts_get_web_identity_token_multiple_audiences():
    """Audience.member.N produces aud as a list when multiple."""
    import base64
    import re
    status, body = _gwit_post(
        b"Action=GetWebIdentityToken&SigningAlgorithm=RS256"
        b"&Audience.member.1=alpha&Audience.member.2=beta"
    )
    assert status == 200
    token = re.search(r"<WebIdentityToken>(.+?)</WebIdentityToken>", body).group(1)
    payload = json.loads(base64.urlsafe_b64decode(_b64url_pad(token.split(".")[1])))
    assert payload["aud"] == ["alpha", "beta"]


def test_sts_get_web_identity_token_missing_signing_algorithm():
    status, body = _gwit_post(b"Action=GetWebIdentityToken&Audience=x")
    assert status == 400
    assert "MissingParameter" in body
    assert "SigningAlgorithm" in body


def test_sts_get_web_identity_token_invalid_signing_algorithm():
    status, body = _gwit_post(b"Action=GetWebIdentityToken&Audience=x&SigningAlgorithm=HS256")
    assert status == 400
    assert "ValidationError" in body


def test_sts_get_web_identity_token_missing_audience():
    status, body = _gwit_post(b"Action=GetWebIdentityToken&SigningAlgorithm=RS256")
    assert status == 400
    assert "MissingParameter" in body
    assert "Audience" in body


def test_sts_get_web_identity_token_too_many_audiences():
    audiences = "&".join(f"Audience.member.{i}=a{i}" for i in range(1, 12))
    data = f"Action=GetWebIdentityToken&SigningAlgorithm=RS256&{audiences}".encode()
    status, body = _gwit_post(data)
    assert status == 400
    assert "ValidationError" in body


def test_sts_get_web_identity_token_duration_too_short():
    status, body = _gwit_post(
        b"Action=GetWebIdentityToken&Audience=x&SigningAlgorithm=RS256&DurationSeconds=10"
    )
    assert status == 400
    assert "ValidationError" in body


def test_sts_get_web_identity_token_duration_too_long():
    status, body = _gwit_post(
        b"Action=GetWebIdentityToken&Audience=x&SigningAlgorithm=RS256&DurationSeconds=99999"
    )
    assert status == 400
    assert "ValidationError" in body
def test_get_caller_identity_reflects_assumed_role(sts_as_role):
    """GetCallerIdentity called with assumed-role creds must return the role ARN, not root."""
    identity = sts_as_role("arn:aws:iam::000000000000:role/MyTestRole", "caller-identity-session").get_caller_identity()

    assert identity["Account"] == "000000000000"
    assert "MyTestRole" in identity["Arn"]
    assert "caller-identity-session" in identity["Arn"]
    assert ":assumed-role/" in identity["Arn"]


def test_get_caller_identity_without_assume_role_returns_root(sts):
    """GetCallerIdentity with root/plain creds must still return root ARN."""
    identity = sts.get_caller_identity()
    assert identity["Arn"] == "arn:aws:iam::000000000000:root"


def test_get_caller_identity_different_roles_return_different_arns(sts_as_role):
    """Two distinct assumed roles must produce distinct caller identities."""
    arn_a = sts_as_role("arn:aws:iam::000000000000:role/RoleA", "session-a").get_caller_identity()["Arn"]
    arn_b = sts_as_role("arn:aws:iam::000000000000:role/RoleB", "session-b").get_caller_identity()["Arn"]

    assert "RoleA" in arn_a
    assert "RoleB" in arn_b
    assert arn_a != arn_b
