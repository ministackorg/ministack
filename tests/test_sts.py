import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
    role_arn = f"arn:aws:iam::000000000000:role/test-oidc-role"
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
