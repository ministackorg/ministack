import json
import os

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
    resp = sts.get_access_key_info(AccessKeyId="test-key-do-not-use")
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


def test_sts_assume_role_allows_colon_in_role_path(sts):
    resp = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/team:dev/demo",
        RoleSessionName="PathSess",
    )

    assert resp["AssumedRoleUser"]["Arn"] == (
        "arn:aws:sts::000000000000:assumed-role/demo/PathSess"
    )


@pytest.mark.parametrize(
    "role_arn",
    [
        "not-an-arn-but-long-enough",
        "arn:aws:lambda:us-east-1:000000000000:function:demo",
        "arn:aws:iam::000000000000:user/demo",
        "arn:aws:iam:us-east-1:000000000000:role/demo",
        "arn:aws:iam::not-an-account:role/demo",
        "arn:aws:iam::000000000000:role/demo:bad",
        "arn:aws:iam::000000000000:role/",
    ],
)
def test_sts_assume_role_rejects_invalid_role_arns(sts, role_arn):
    with pytest.raises(ClientError) as exc:
        sts.assume_role(RoleArn=role_arn, RoleSessionName="BadRoleArn")

    assert exc.value.response["Error"]["Code"] == "ValidationError"


@pytest.mark.parametrize(
    "role_arn",
    [
        "not-an-arn-but-long-enough",
        "arn:aws:sts::000000000000:assumed-role/demo/session",
        "arn:aws:iam::not-an-account:role/demo",
        "arn:aws:iam::000000000000:role/demo:bad",
        "arn:aws:iam::000000000000:policy/demo",
    ],
)
def test_sts_assume_role_with_web_identity_rejects_invalid_role_arns(sts, role_arn):
    with pytest.raises(ClientError) as exc:
        sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName="BadRoleArnWebIdentity",
            WebIdentityToken="dummy.jwt.token",
        )

    assert exc.value.response["Error"]["Code"] == "ValidationError"


def test_sts_get_session_token(sts):
    resp = sts.get_session_token(DurationSeconds=900)
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds


def test_sts_get_session_token_retains_iam_user_identity(iam):
    import boto3

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    user_name = "session-token-user"
    iam.create_user(UserName=user_name)
    source = iam.create_access_key(UserName=user_name)["AccessKey"]
    try:
        source_sts = boto3.client(
            "sts",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=source["AccessKeyId"],
            aws_secret_access_key=source["SecretAccessKey"],
        )
        session = source_sts.get_session_token(DurationSeconds=900)["Credentials"]
        session_sts = boto3.client(
            "sts",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=session["AccessKeyId"],
            aws_secret_access_key=session["SecretAccessKey"],
            aws_session_token=session["SessionToken"],
        )

        identity = session_sts.get_caller_identity()

        assert identity["Arn"] == f"arn:aws:iam::000000000000:user/{user_name}"
        assert identity["UserId"]
    finally:
        iam.delete_access_key(
            UserName=user_name,
            AccessKeyId=source["AccessKeyId"],
        )
        iam.delete_user(UserName=user_name)


def test_sts_get_session_token_rejects_unknown_caller_when_auth_enabled(monkeypatch):
    import asyncio

    import ministack.app as app_mod
    from ministack.services import sts as sts_mod

    issued_key = "test-rejected-session-key"
    monkeypatch.setattr(app_mod, "AUTH", True)
    monkeypatch.setattr(sts_mod, "_gen_session_access_key", lambda: issued_key)
    body = b"Action=GetSessionToken&Version=2011-06-15&DurationSeconds=900"
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=unknown-caller/20260908/us-east-1/sts/aws4_request"
        ),
        "content-type": "application/x-www-form-urlencoded",
    }

    status, _headers, payload = asyncio.run(
        sts_mod.handle_request("POST", "/", headers, body, {})
    )

    assert status == 403
    assert b"UnrecognizedClientException" in payload
    assert issued_key not in sts_mod._sessions


def test_sts_get_session_token_resolves_query_signed_iam_caller(monkeypatch):
    import asyncio

    import ministack.app as app_mod
    from ministack.core.responses import get_account_id, set_request_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_mod

    account_id = "123456789012"
    user_name = "query-user"
    source_key = "test-query-user-key"
    issued_key = "test-issued-session-key"
    original_account = get_account_id()
    monkeypatch.setattr(app_mod, "AUTH", True)
    monkeypatch.setattr(sts_mod, "_gen_session_access_key", lambda: issued_key)
    set_request_account_id(account_id)
    iam_svc._users.set_scoped(account_id, None, user_name, {
        "UserName": user_name,
        "UserId": "test-query-user-id",
        "Arn": f"arn:aws:iam::{account_id}:user/team/{user_name}",
        "Path": "/team/",
        "AttachedPolicies": [],
    })
    iam_svc._access_keys.set_scoped(account_id, None, source_key, {
        "AccessKeyId": source_key,
        "SecretAccessKey": "test-query-user-secret",
        "Status": "Active",
        "UserName": user_name,
    })
    query = {
        "Action": ["GetSessionToken"],
        "DurationSeconds": ["900"],
        "X-Amz-Credential": [
            f"{source_key}/20260908/us-east-1/sts/aws4_request"
        ],
    }
    try:
        status, _headers, _payload = asyncio.run(
            sts_mod.handle_request("GET", "/", {}, b"", query)
        )

        assert status == 200
        assert sts_mod._sessions[issued_key]["Arn"] == (
            f"arn:aws:iam::{account_id}:user/team/{user_name}"
        )
        assert sts_mod._sessions[issued_key]["PrincipalType"] == "User"
        assert sts_mod._sessions[issued_key]["PrincipalName"] == user_name
        assert sts_mod._sessions[issued_key]["SourceAccessKeyId"] == source_key
    finally:
        sts_mod._sessions.pop(issued_key, None)
        iam_svc._access_keys.pop_scoped(account_id, None, source_key, None)
        iam_svc._users.pop_scoped(account_id, None, user_name, None)
        set_request_account_id(original_account)




@pytest.mark.parametrize("auth_enabled", [False, True])
def test_sts_get_caller_identity_rejects_expired_session(monkeypatch, auth_enabled):
    import asyncio
    import time

    import ministack.app as app_mod
    from ministack.core.responses import get_account_id
    from ministack.services import sts as sts_mod

    account_id = get_account_id()
    access_key = "test-expired-identity-session"
    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    sts_mod._sessions[access_key] = {
        "Arn": f"arn:aws:iam::{account_id}:user/expired-user",
        "UserId": "test-expired-user-id",
        "SecretAccessKey": "test-expired-secret",
        "SessionToken": "test-expired-token",
        "Expiration": time.time() - 1,
        "AccountId": account_id,
        "PrincipalType": "User",
        "PrincipalName": "expired-user",
    }
    query = {"Action": ["GetCallerIdentity"]}
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential={access_key}/20260908/us-east-1/sts/aws4_request"
        )
    }
    try:
        status, _headers, payload = asyncio.run(
            sts_mod.handle_request("GET", "/", headers, b"", query)
        )

        assert status == 403
        assert b"<Code>ExpiredToken</Code>" in payload
    finally:
        sts_mod._sessions.pop(access_key, None)


def test_sts_get_session_token_rejects_temporary_caller_when_auth_enabled(monkeypatch):
    import asyncio
    import time

    import ministack.app as app_mod
    from ministack.core.responses import get_account_id
    from ministack.services import sts as sts_mod

    account_id = get_account_id()
    source_key = "test-temporary-source-key"
    issued_key = "test-renewed-session-key"
    monkeypatch.setattr(app_mod, "AUTH", True)
    monkeypatch.setattr(sts_mod, "_gen_session_access_key", lambda: issued_key)
    sts_mod._sessions[source_key] = {
        "Arn": f"arn:aws:iam::{account_id}:user/session-user",
        "UserId": "test-session-user-id",
        "SecretAccessKey": "test-temporary-secret",
        "SessionToken": "test-temporary-token",
        "Expiration": time.time() + 900,
        "AccountId": account_id,
        "PrincipalType": "User",
    }
    body = b"Action=GetSessionToken&Version=2011-06-15&DurationSeconds=900"
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential={source_key}/20260908/us-east-1/sts/aws4_request"
        ),
        "content-type": "application/x-www-form-urlencoded",
    }
    try:
        status, _headers, payload = asyncio.run(
            sts_mod.handle_request("POST", "/", headers, body, {})
        )

        assert status == 403
        assert b"AccessDenied" in payload
        assert issued_key not in sts_mod._sessions
    finally:
        sts_mod._sessions.pop(source_key, None)


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


def test_sts_cross_account_assumed_session_runs_in_role_account():
    """An assumed session's ASIA key resolves to the account of the role it
    assumed, so the session operates in that tenant — not the default one."""
    import boto3

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    caller = boto3.client(
        "sts", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id="111111111111", aws_secret_access_key="test",
    )
    resp = caller.assume_role(
        RoleArn="arn:aws:iam::999888777666:role/CrossReader",
        RoleSessionName="xacct",
    )
    assert resp["AssumedRoleUser"]["Arn"].startswith(
        "arn:aws:sts::999888777666:assumed-role/CrossReader/")
    creds = resp["Credentials"]
    session_sts = boto3.client(
        "sts", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )
    assert session_sts.get_caller_identity()["Account"] == "999888777666"


def test_sts_assume_role_foreign_arn_never_falls_back_to_caller(monkeypatch):
    """Under AUTH, a role ARN naming another account resolves ONLY there: a
    miss is the same AccessDenied a denial produces (AWS never resolves the
    caller's same-named role, and never discloses role existence)."""
    import asyncio

    import ministack.app as app_mod
    from ministack.core.responses import set_request_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    set_request_account_id("111111111111")
    # The caller's own account HAS a role with the same name — the trap the
    # fallback used to fall into.
    iam_svc._roles.set_scoped("111111111111", None, "Trap", {
        "RoleName": "Trap",
        "Arn": "arn:aws:iam::111111111111:role/Trap",
        "AssumeRolePolicyDocument": "{}",
    })
    body = (
        "Action=AssumeRole&Version=2011-06-15"
        "&RoleArn=arn%3Aaws%3Aiam%3A%3A999999999999%3Arole%2FTrap"
        "&RoleSessionName=probe"
    ).encode()
    status, _headers, payload = asyncio.run(sts_mod.handle_request(
        "POST", "/", {"content-type": "application/x-www-form-urlencoded"},
        body, {},
    ))
    assert status == 403
    assert b"AccessDenied" in payload
    iam_svc._roles.pop_scoped("111111111111", None, "Trap", None)
