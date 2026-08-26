"""Cognito tests — user pools, identity pools, OAuth2/OIDC flows, auth-code persistence."""

import base64
import importlib
import io
import json
import os
import time
import urllib.error
import urllib.request
import uuid as _uuid_mod
import zipfile
import zlib
from urllib.parse import parse_qs as _parse_qs
from urllib.parse import urlencode as _urlencode
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

from ministack.core import persistence

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _advertised_endpoint():
    # Predict the server-side advertised port. Under the matched-port protocol,
    # MINISTACK_ENDPOINT alone implies a direct connection (endpoint port == server port);
    # split-port topologies must export GATEWAY_PORT/EDGE_PORT into the test env.
    host = os.environ.get("MINISTACK_HOST", "localhost")
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or (
        urlparse(ENDPOINT).port or 4566
    )
    return f"http://{host}:{port}"

# ========== from test_cognito.py ==========


def _identity_pool_arn(identity_pool_id, region="us-east-1", account="000000000000"):
    return f"arn:aws:cognito-identity:{region}:{account}:identitypool/{identity_pool_id}"


def _user_pool_arn(user_pool_id, region="us-east-1", account="000000000000"):
    return f"arn:aws:cognito-idp:{region}:{account}:userpool/{user_pool_id}"


def test_cognito_create_and_describe_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="TestPool")
    pool = resp["UserPool"]
    pid = pool["Id"]
    assert pool["Name"] == "TestPool"
    assert pid.startswith("us-east-1_")

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert desc["Id"] == pid
    assert desc["Name"] == "TestPool"

def test_cognito_list_user_pools(cognito_idp):
    cognito_idp.create_user_pool(PoolName="ListPoolA")
    cognito_idp.create_user_pool(PoolName="ListPoolB")
    resp = cognito_idp.list_user_pools(MaxResults=60)
    names = [p["Name"] for p in resp["UserPools"]]
    assert "ListPoolA" in names
    assert "ListPoolB" in names

def test_cognito_update_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="UpdatePool")
    pid = resp["UserPool"]["Id"]
    cognito_idp.update_user_pool(UserPoolId=pid, UserPoolTags={"env": "test"})
    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert desc["UserPoolTags"].get("env") == "test"

def test_cognito_delete_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="DeletePool")
    pid = resp["UserPool"]["Id"]
    cognito_idp.delete_user_pool(UserPoolId=pid)
    pools = cognito_idp.list_user_pools(MaxResults=60)["UserPools"]
    assert not any(p["Id"] == pid for p in pools)

def test_cognito_create_and_describe_user_pool_client(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ClientPool")["UserPool"]["Id"]
    client_resp = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="MyApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )
    client = client_resp["UserPoolClient"]
    cid = client["ClientId"]
    assert client["ClientName"] == "MyApp"

    desc = cognito_idp.describe_user_pool_client(UserPoolId=pid, ClientId=cid)["UserPoolClient"]
    assert desc["ClientId"] == cid
    assert desc["ClientName"] == "MyApp"

def test_cognito_list_user_pool_clients(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="MultiClientPool")["UserPool"]["Id"]
    cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="App1")
    cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="App2")
    clients = cognito_idp.list_user_pool_clients(UserPoolId=pid, MaxResults=60)["UserPoolClients"]
    names = [c["ClientName"] for c in clients]
    assert "App1" in names
    assert "App2" in names

def test_cognito_create_and_describe_resource_server(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerPool")["UserPool"]["Id"]
    resp = cognito_idp.create_resource_server(
        UserPoolId=pid,
        Identifier="API",
        Name="API",
        Scopes=[{"ScopeName": "resource.get", "ScopeDescription": "Read access"}],
    )
    server = resp["ResourceServer"]
    assert server["Identifier"] == "API"
    assert server["Scopes"] == [{"ScopeName": "resource.get", "ScopeDescription": "Read access"}]

    desc = cognito_idp.describe_resource_server(UserPoolId=pid, Identifier="API")["ResourceServer"]
    assert desc["Identifier"] == "API"
    assert desc["Name"] == "API"

def test_cognito_create_resource_server_duplicate_identifier_error(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerDupPool")["UserPool"]["Id"]
    cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API", Name="API")
    with pytest.raises(ClientError) as exc:
        cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API", Name="API")
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"

def test_cognito_list_resource_servers(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerListPool")["UserPool"]["Id"]
    cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API1", Name="API1")
    cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API2", Name="API2")
    identifiers = [
        s["Identifier"]
        for s in cognito_idp.list_resource_servers(UserPoolId=pid, MaxResults=50)["ResourceServers"]
    ]
    assert "API1" in identifiers
    assert "API2" in identifiers

def test_cognito_update_resource_server(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerUpdatePool")["UserPool"]["Id"]
    cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API", Name="Old Name")
    updated = cognito_idp.update_resource_server(
        UserPoolId=pid,
        Identifier="API",
        Name="New Name",
        Scopes=[{"ScopeName": "resource.put", "ScopeDescription": "Write access"}],
    )["ResourceServer"]
    assert updated["Name"] == "New Name"
    assert updated["Scopes"] == [{"ScopeName": "resource.put", "ScopeDescription": "Write access"}]

def test_cognito_delete_resource_server(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerDeletePool")["UserPool"]["Id"]
    cognito_idp.create_resource_server(UserPoolId=pid, Identifier="API", Name="API")
    cognito_idp.delete_resource_server(UserPoolId=pid, Identifier="API")
    identifiers = [
        s["Identifier"]
        for s in cognito_idp.list_resource_servers(UserPoolId=pid, MaxResults=50)["ResourceServers"]
    ]
    assert "API" not in identifiers

def test_cognito_resource_server_not_found_errors(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResourceServerNotFoundPool")["UserPool"]["Id"]
    with pytest.raises(ClientError) as exc:
        cognito_idp.describe_resource_server(UserPoolId=pid, Identifier="Missing")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as exc:
        cognito_idp.update_resource_server(UserPoolId=pid, Identifier="Missing", Name="x")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as exc:
        cognito_idp.delete_resource_server(UserPoolId=pid, Identifier="Missing")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_cognito_admin_create_and_get_user(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AdminUserPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alice",
        UserAttributes=[{"Name": "email", "Value": "alice@example.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="alice")
    assert user["Username"] == "alice"
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "alice@example.com"

def test_cognito_list_users(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ListUsersPool")["UserPool"]["Id"]
    for name in ["user1", "user2", "user3"]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
    users = cognito_idp.list_users(UserPoolId=pid)["Users"]
    usernames = [u["Username"] for u in users]
    assert "user1" in usernames
    assert "user2" in usernames
    assert "user3" in usernames

def test_cognito_list_users_filter(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="FilterUsersPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="bob",
        UserAttributes=[{"Name": "email", "Value": "bob@example.com"}],
    )
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="charlie",
        UserAttributes=[{"Name": "email", "Value": "charlie@example.com"}],
    )
    resp = cognito_idp.list_users(UserPoolId=pid, Filter='username = "bob"')
    users = resp["Users"]
    assert len(users) == 1
    assert users[0]["Username"] == "bob"

def test_cognito_list_users_filter_quoted_attribute_name(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="QuotedFilterUsersPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="bob",
        UserAttributes=[{"Name": "email", "Value": "bob@example.com"}],
    )
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="charlie",
        UserAttributes=[{"Name": "email", "Value": "charlie@example.com"}],
    )
    resp = cognito_idp.list_users(UserPoolId=pid, Filter='"email" = "bob@example.com"')
    users = resp["Users"]
    assert len(users) == 1
    assert users[0]["Username"] == "bob"

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='"email" = "nonexistent@example.com"')
    assert resp["Users"] == []

def test_cognito_list_users_filter_case_sensitivity(cognito_idp):
    """The ListUsers API reference documents "username" and "status" as
    case-sensitive; other searchable attributes match case-insensitively."""
    pid = cognito_idp.create_user_pool(PoolName="CaseFilterUsersPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="Alpha",
        UserAttributes=[
            {"Name": "email", "Value": "Alpha@Example.com"},
            {"Name": "family_name", "Value": "Reddy"},
        ],
    )
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="beta",
        UserAttributes=[{"Name": "email", "Value": "beta@example.com"}],
    )

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='username = "alpha"')
    assert resp["Users"] == []
    resp = cognito_idp.list_users(UserPoolId=pid, Filter='username = "Alpha"')
    assert [u["Username"] for u in resp["Users"]] == ["Alpha"]

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='"email" = "alpha@example.com"')
    assert [u["Username"] for u in resp["Users"]] == ["Alpha"]

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='family_name ^= "redd"')
    assert [u["Username"] for u in resp["Users"]] == ["Alpha"]

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='status = "enabled"')
    assert resp["Users"] == []
    resp = cognito_idp.list_users(UserPoolId=pid, Filter='status = "Enabled"')
    assert sorted(u["Username"] for u in resp["Users"]) == ["Alpha", "beta"]

def test_cognito_list_users_filter_status(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="StatusFilterUsersPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="active-user")
    cognito_idp.admin_create_user(UserPoolId=pid, Username="disabled-user")
    cognito_idp.admin_disable_user(UserPoolId=pid, Username="disabled-user")

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='status = "Enabled"')
    usernames = [u["Username"] for u in resp["Users"]]
    assert usernames == ["active-user"]

    resp = cognito_idp.list_users(UserPoolId=pid, Filter='status = "Disabled"')
    usernames = [u["Username"] for u in resp["Users"]]
    assert usernames == ["disabled-user"]

def test_cognito_admin_set_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="PwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="PwdApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="dave")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="dave", Password="NewPass123!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "dave", "PASSWORD": "NewPass123!"},
    )
    assert "AuthenticationResult" in auth

def test_cognito_admin_initiate_auth_wrong_password(cognito_idp):
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="AuthFailPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="AuthFailApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="eve")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="eve", Password="Correct1!", Permanent=True)
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "eve", "PASSWORD": "Wrong1!"},
        )
    assert exc_info.value.response["Error"]["Code"] == "NotAuthorizedException"

def test_cognito_initiate_auth_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="InitiateAuthPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="InitiateApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="frank")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="frank", Password="FrankPass1!", Permanent=True)
    auth = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "frank", "PASSWORD": "FrankPass1!"},
    )
    assert "AuthenticationResult" in auth
    result = auth["AuthenticationResult"]
    assert "AccessToken" in result
    assert "IdToken" in result
    assert "RefreshToken" in result

def test_cognito_signup_and_confirm(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SignupPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="SignupApp")["UserPoolClient"]["ClientId"]

    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="grace",
        Password="GracePass1!",
        UserAttributes=[{"Name": "email", "Value": "grace@example.com"}],
    )
    assert resp["UserSub"]

    cognito_idp.confirm_sign_up(
        ClientId=cid,
        Username="grace",
        ConfirmationCode="123456",
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="grace")
    assert user["UserStatus"] == "CONFIRMED"

def test_cognito_forgot_password_and_confirm(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ForgotPwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="ForgotApp")["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="henry")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="henry", Password="OldPass1!", Permanent=True)

    cognito_idp.forgot_password(ClientId=cid, Username="henry")

    cognito_idp.confirm_forgot_password(
        ClientId=cid,
        Username="henry",
        ConfirmationCode="654321",
        Password="NewPass2!",
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="henry", Password="NewPass2!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "henry", "PASSWORD": "NewPass2!"},
    )
    assert "AuthenticationResult" in auth

def test_cognito_admin_update_user_attributes(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="UpdateAttrPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="irene",
        UserAttributes=[{"Name": "email", "Value": "irene@example.com"}],
    )
    cognito_idp.admin_update_user_attributes(
        UserPoolId=pid,
        Username="irene",
        UserAttributes=[{"Name": "email", "Value": "irene@updated.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="irene")
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "irene@updated.com"

def test_cognito_admin_disable_enable_user(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="DisablePool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="jack")

    cognito_idp.admin_disable_user(UserPoolId=pid, Username="jack")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="jack")
    assert user["Enabled"] is False

    cognito_idp.admin_enable_user(UserPoolId=pid, Username="jack")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="jack")
    assert user["Enabled"] is True

def test_cognito_admin_delete_user(cognito_idp):
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="DeleteUserPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="kate")
    cognito_idp.admin_delete_user(UserPoolId=pid, Username="kate")
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="kate")
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"

def test_cognito_groups_crud(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GroupPool")["UserPool"]["Id"]

    resp = cognito_idp.create_group(UserPoolId=pid, GroupName="admins", Description="Admins")
    assert resp["Group"]["GroupName"] == "admins"

    group = cognito_idp.get_group(UserPoolId=pid, GroupName="admins")["Group"]
    assert group["Description"] == "Admins"

    groups = cognito_idp.list_groups(UserPoolId=pid)["Groups"]
    assert any(g["GroupName"] == "admins" for g in groups)

    cognito_idp.delete_group(UserPoolId=pid, GroupName="admins")
    groups = cognito_idp.list_groups(UserPoolId=pid)["Groups"]
    assert not any(g["GroupName"] == "admins" for g in groups)

def test_cognito_admin_add_remove_user_from_group(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GroupMemberPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="liam")
    cognito_idp.create_group(UserPoolId=pid, GroupName="editors")

    cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username="liam", GroupName="editors")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="editors")["Users"]
    assert any(u["Username"] == "liam" for u in members)

    groups_for_user = cognito_idp.admin_list_groups_for_user(UserPoolId=pid, Username="liam")["Groups"]
    assert any(g["GroupName"] == "editors" for g in groups_for_user)

    cognito_idp.admin_remove_user_from_group(UserPoolId=pid, Username="liam", GroupName="editors")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="editors")["Users"]
    assert not any(u["Username"] == "liam" for u in members)

def test_cognito_domain_crud(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="DomainPool")["UserPool"]["Id"]
    resp = cognito_idp.create_user_pool_domain(UserPoolId=pid, Domain="my-test-domain")
    # Prefix domain: AWS returns a null CloudFrontDomain (no alias target).
    assert "CloudFrontDomain" not in resp

    desc = cognito_idp.describe_user_pool_domain(Domain="my-test-domain")
    assert desc["DomainDescription"]["UserPoolId"] == pid
    assert desc["DomainDescription"]["Status"] == "ACTIVE"

    cognito_idp.delete_user_pool_domain(UserPoolId=pid, Domain="my-test-domain")
    desc2 = cognito_idp.describe_user_pool_domain(Domain="my-test-domain")
    assert desc2["DomainDescription"] == {}

def test_cognito_mfa_config(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="MfaPool")["UserPool"]["Id"]
    resp = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert resp["MfaConfiguration"] == "OFF"
    # An unconfigured pool reports no MFA blocks at all; inventing a disabled
    # SoftwareTokenMfaConfiguration reads as drift to a config-diffing client.
    assert "SoftwareTokenMfaConfiguration" not in resp
    assert "SmsMfaConfiguration" not in resp

    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="OPTIONAL",
    )
    resp = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert resp["MfaConfiguration"] == "OPTIONAL"
    assert resp["SoftwareTokenMfaConfiguration"]["Enabled"] is True

def test_cognito_user_pool_returns_standard_schema_attributes(cognito_idp):
    pool = cognito_idp.create_user_pool(PoolName="SchemaStandardPool")["UserPool"]
    attrs = {a["Name"]: a for a in pool["SchemaAttributes"]}
    # AWS always returns the full standard OIDC attribute set.
    for name in ("sub", "name", "given_name", "family_name", "email",
                 "email_verified", "phone_number", "address", "updated_at"):
        assert name in attrs
    assert attrs["sub"] == {
        "Name": "sub",
        "AttributeDataType": "String",
        "DeveloperOnlyAttribute": False,
        "Mutable": False,
        "Required": True,
        "StringAttributeConstraints": {"MinLength": "1", "MaxLength": "2048"},
    }
    assert attrs["email_verified"]["AttributeDataType"] == "Boolean"
    assert "StringAttributeConstraints" not in attrs["email_verified"]
    assert attrs["updated_at"]["NumberAttributeConstraints"] == {"MinValue": "0"}
    assert attrs["birthdate"]["StringAttributeConstraints"] == {
        "MinLength": "10", "MaxLength": "10"}


def test_cognito_user_pool_schema_request_is_echoed_by_describe(cognito_idp):
    pool = cognito_idp.create_user_pool(
        PoolName="SchemaEchoPool",
        Schema=[
            {"Name": "email", "AttributeDataType": "String", "Mutable": True,
             "Required": True,
             "StringAttributeConstraints": {"MinLength": "1", "MaxLength": "256"}},
            {"Name": "tenant", "AttributeDataType": "String", "Mutable": True,
             "StringAttributeConstraints": {"MinLength": "1", "MaxLength": "32"}},
        ],
    )["UserPool"]
    described = cognito_idp.describe_user_pool(
        UserPoolId=pool["Id"])["UserPool"]["SchemaAttributes"]
    assert described == pool["SchemaAttributes"]
    attrs = {a["Name"]: a for a in described}
    # A standard attribute the request redefines keeps the caller's values ...
    assert attrs["email"]["Required"] is True
    assert attrs["email"]["StringAttributeConstraints"] == {
        "MinLength": "1", "MaxLength": "256"}
    # ... and a new attribute is stored under its custom: prefix.
    assert "tenant" not in attrs
    assert attrs["custom:tenant"]["Mutable"] is True
    assert attrs["custom:tenant"]["Required"] is False
    assert attrs["custom:tenant"]["DeveloperOnlyAttribute"] is False


def test_cognito_user_pool_developer_only_attribute_gets_dev_prefix(cognito_idp):
    pool = cognito_idp.create_user_pool(
        PoolName="SchemaDevPool",
        Schema=[{"Name": "internal", "AttributeDataType": "String",
                 "DeveloperOnlyAttribute": True}],
    )["UserPool"]
    names = [a["Name"] for a in pool["SchemaAttributes"]]
    assert "dev:internal" in names


def test_cognito_add_custom_attributes(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AddCustomAttrPool")["UserPool"]["Id"]
    cognito_idp.add_custom_attributes(
        UserPoolId=pid,
        CustomAttributes=[
            {"Name": "deliverables", "AttributeDataType": "String", "Mutable": True,
             "StringAttributeConstraints": {"MinLength": "1", "MaxLength": "255"}},
            {"Name": "seats", "AttributeDataType": "Number",
             "NumberAttributeConstraints": {"MinValue": "1"}},
        ],
    )
    attrs = {a["Name"]: a for a in
             cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]["SchemaAttributes"]}
    assert attrs["custom:deliverables"]["StringAttributeConstraints"] == {
        "MinLength": "1", "MaxLength": "255"}
    assert attrs["custom:seats"]["AttributeDataType"] == "Number"
    assert attrs["custom:seats"]["NumberAttributeConstraints"] == {"MinValue": "1"}


def test_cognito_add_custom_attributes_rejects_duplicate(cognito_idp):
    pid = cognito_idp.create_user_pool(
        PoolName="AddCustomAttrDupPool",
        Schema=[{"Name": "tenant", "AttributeDataType": "String"}],
    )["UserPool"]["Id"]
    with pytest.raises(ClientError) as exc:
        cognito_idp.add_custom_attributes(
            UserPoolId=pid,
            CustomAttributes=[{"Name": "tenant", "AttributeDataType": "String"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_cognito_add_custom_attributes_rejects_standard_attribute(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AddCustomAttrStdPool")["UserPool"]["Id"]
    with pytest.raises(ClientError) as exc:
        cognito_idp.add_custom_attributes(
            UserPoolId=pid,
            CustomAttributes=[{"Name": "email", "AttributeDataType": "String"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_cognito_add_custom_attributes_unknown_pool(cognito_idp):
    with pytest.raises(ClientError) as exc:
        cognito_idp.add_custom_attributes(
            UserPoolId="us-east-1_missingpool",
            CustomAttributes=[{"Name": "tenant", "AttributeDataType": "String"}],
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cognito_email_configuration_defaults_to_cognito_default(cognito_idp):
    empty_pool = cognito_idp.create_user_pool(PoolName="EmailCfgEmptyPool")["UserPool"]
    assert empty_pool["EmailConfiguration"]["EmailSendingAccount"] == "COGNITO_DEFAULT"

    pool = cognito_idp.create_user_pool(
        PoolName="EmailCfgPool",
        EmailConfiguration={"ReplyToEmailAddress": "reply@example.com"},
    )["UserPool"]
    assert pool["EmailConfiguration"]["EmailSendingAccount"] == "COGNITO_DEFAULT"
    described = cognito_idp.describe_user_pool(UserPoolId=pool["Id"])["UserPool"]
    assert described["EmailConfiguration"]["EmailSendingAccount"] == "COGNITO_DEFAULT"


def test_cognito_tags(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="TagPool")
    pid = resp["UserPool"]["Id"]
    arn = resp["UserPool"]["Arn"]

    cognito_idp.tag_resource(ResourceArn=arn, Tags={"project": "ministack"})
    tags = cognito_idp.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert tags["project"] == "ministack"

    cognito_idp.untag_resource(ResourceArn=arn, TagKeys=["project"])
    tags = cognito_idp.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert "project" not in tags


def test_cognito_user_pool_tag_apis_reject_invalid_arns(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="InvalidIdpArnTagPool")["UserPool"]["Id"]
    valid_arn = _user_pool_arn(pid)
    invalid_cases = [
        ("not-an-arn-but-long-enough", "InvalidParameterException"),
        ("arn:aws:cognito-idp:us-east-1", "InvalidParameterException"),
        (f"arn:aws:cognito-identity:us-east-1:000000000000:userpool/{pid}", "InvalidParameterException"),
        (f"arn:aws:cognito-idp:us-east-1:000000000000:identitypool/{pid}", "InvalidParameterException"),
        (_user_pool_arn(pid, region="us-west-2"), "ResourceNotFoundException"),
        (_user_pool_arn(pid, account="111111111111"), "ResourceNotFoundException"),
    ]

    for bad_arn, expected_code in invalid_cases:
        with pytest.raises(ClientError) as exc:
            cognito_idp.tag_resource(ResourceArn=bad_arn, Tags={"bad": "value"})
        assert exc.value.response["Error"]["Code"] == expected_code

    assert cognito_idp.list_tags_for_resource(ResourceArn=valid_arn)["Tags"] == {}


def test_cognito_user_pool_list_and_untag_reject_invalid_arns(cognito_idp):
    for operation, kwargs in [
        (cognito_idp.list_tags_for_resource, {}),
        (cognito_idp.untag_resource, {"TagKeys": ["missing"]}),
    ]:
        with pytest.raises(ClientError) as exc:
            operation(ResourceArn="arn:aws:sqs:us-east-1:000000000000:userpool/not-a-pool", **kwargs)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_cognito_get_user_from_token(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GetUserPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="GetUserApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="maya",
        UserAttributes=[{"Name": "email", "Value": "maya@example.com"}],
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="maya", Password="MayaPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "maya", "PASSWORD": "MayaPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=access_token)
    assert user["Username"] == "maya"

def test_cognito_global_sign_out(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SignOutPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="SignOutApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="noah")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="noah", Password="NoahPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "noah", "PASSWORD": "NoahPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    time.sleep(1.1)  # sign-out invalidates tokens issued before now (1s granularity)
    cognito_idp.global_sign_out(AccessToken=access_token)
    # every refresh token issued before the sign-out must be invalidated (#1395)
    with pytest.raises(cognito_idp.exceptions.NotAuthorizedException):
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid, ClientId=cid, AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
        )


def test_cognito_get_tokens_from_refresh_token(cognito_idp):
    """aws-amplify v6.15+ refreshes sessions only via GetTokensFromRefreshToken;
    it returns the same AuthenticationResult shape as REFRESH_TOKEN_AUTH."""
    pid = cognito_idp.create_user_pool(PoolName="GTFRTPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="GTFRTApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="ada")
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="ada", Password="AdaPass1!", Permanent=True)
    auth = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "ada", "PASSWORD": "AdaPass1!"},
    )["AuthenticationResult"]

    result = cognito_idp.get_tokens_from_refresh_token(
        ClientId=cid, RefreshToken=auth["RefreshToken"],
    )["AuthenticationResult"]
    assert result["AccessToken"]
    assert result["IdToken"]
    assert result["TokenType"] == "Bearer"
    assert result["ExpiresIn"] > 0
    # With rotation disabled (the pool default) AWS returns no new refresh token.
    assert "RefreshToken" not in result
    # The refreshed access token is usable.
    assert cognito_idp.get_user(AccessToken=result["AccessToken"])["Username"] == "ada"


def test_cognito_get_tokens_from_refresh_token_unknown_client(cognito_idp):
    with pytest.raises(cognito_idp.exceptions.ResourceNotFoundException):
        cognito_idp.get_tokens_from_refresh_token(ClientId="nonexistent", RefreshToken="dummy")


def test_cognito_get_tokens_from_refresh_token_revoked(cognito_idp):
    """A globally-signed-out refresh token is rejected, matching REFRESH_TOKEN_AUTH."""
    pid = cognito_idp.create_user_pool(PoolName="GTFRTRevokePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="GTFRTRevokeApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="grace")
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="grace", Password="GracePass1!", Permanent=True)
    auth = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "grace", "PASSWORD": "GracePass1!"},
    )["AuthenticationResult"]
    time.sleep(1.1)
    cognito_idp.global_sign_out(AccessToken=auth["AccessToken"])
    with pytest.raises(cognito_idp.exceptions.NotAuthorizedException):
        cognito_idp.get_tokens_from_refresh_token(
            ClientId=cid, RefreshToken=auth["RefreshToken"])


def test_cognito_admin_confirm_signup(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AdminConfirmPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="AdminConfirmApp")["UserPoolClient"][
        "ClientId"
    ]
    cognito_idp.sign_up(
        ClientId=cid,
        Username="olivia",
        Password="OliviaPass1!",
    )
    cognito_idp.admin_confirm_sign_up(UserPoolId=pid, Username="olivia")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="olivia")
    assert user["UserStatus"] == "CONFIRMED"

def test_cognito_identity_pool_crud(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="TestIdPool",
        AllowUnauthenticatedIdentities=False,
    )
    iid = resp["IdentityPoolId"]
    assert resp["IdentityPoolName"] == "TestIdPool"
    assert iid.startswith("us-east-1:")

    desc = cognito_identity.describe_identity_pool(IdentityPoolId=iid)
    assert desc["IdentityPoolId"] == iid
    assert desc["IdentityPoolName"] == "TestIdPool"

    pools = cognito_identity.list_identity_pools(MaxResults=60)["IdentityPools"]
    assert any(p["IdentityPoolId"] == iid for p in pools)

    cognito_identity.update_identity_pool(
        IdentityPoolId=iid,
        IdentityPoolName="TestIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    desc2 = cognito_identity.describe_identity_pool(IdentityPoolId=iid)
    assert desc2["AllowUnauthenticatedIdentities"] is True

    cognito_identity.delete_identity_pool(IdentityPoolId=iid)
    pools2 = cognito_identity.list_identity_pools(MaxResults=60)["IdentityPools"]
    assert not any(p["IdentityPoolId"] == iid for p in pools2)


def test_cognito_identity_pool_tags(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="IdentityTagPool",
        AllowUnauthenticatedIdentities=True,
    )
    arn = _identity_pool_arn(resp["IdentityPoolId"])

    cognito_identity.tag_resource(ResourceArn=arn, Tags={"project": "ministack"})
    tags = cognito_identity.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert tags["project"] == "ministack"

    cognito_identity.untag_resource(ResourceArn=arn, TagKeys=["project"])
    tags = cognito_identity.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert "project" not in tags


def test_cognito_identity_pool_tag_apis_reject_invalid_arns(cognito_identity):
    iid = cognito_identity.create_identity_pool(
        IdentityPoolName="IdentityInvalidArnTagPool",
        AllowUnauthenticatedIdentities=True,
    )["IdentityPoolId"]
    valid_arn = _identity_pool_arn(iid)
    invalid_cases = [
        ("not-an-arn-but-long-enough", "InvalidParameterException"),
        ("arn:aws:cognito-identity:us-east-1", "InvalidParameterException"),
        (f"arn:aws:cognito-idp:us-east-1:000000000000:identitypool/{iid}", "InvalidParameterException"),
        (f"arn:aws:cognito-identity:us-east-1:000000000000:identity/{iid}", "InvalidParameterException"),
        (_identity_pool_arn(iid, region="us-west-2"), "ResourceNotFoundException"),
        (_identity_pool_arn(iid, account="111111111111"), "ResourceNotFoundException"),
    ]

    for bad_arn, expected_code in invalid_cases:
        with pytest.raises(ClientError) as exc:
            cognito_identity.tag_resource(ResourceArn=bad_arn, Tags={"bad": "value"})
        assert exc.value.response["Error"]["Code"] == expected_code

    assert cognito_identity.list_tags_for_resource(ResourceArn=valid_arn)["Tags"] == {}


def test_cognito_identity_pool_list_and_untag_reject_invalid_arns(cognito_identity):
    for operation, kwargs in [
        (cognito_identity.list_tags_for_resource, {}),
        (cognito_identity.untag_resource, {"TagKeys": ["missing"]}),
    ]:
        with pytest.raises(ClientError) as exc:
            operation(ResourceArn="arn:aws:sqs:us-east-1:000000000000:identitypool/not-a-pool", **kwargs)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_cognito_get_id_and_credentials(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="CredsPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    id_resp = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")
    identity_id = id_resp["IdentityId"]
    assert identity_id

    creds = cognito_identity.get_credentials_for_identity(IdentityId=identity_id)
    assert creds["IdentityId"] == identity_id
    assert "AccessKeyId" in creds["Credentials"]
    assert creds["Credentials"]["AccessKeyId"].startswith("ASIA")
    assert "SecretKey" in creds["Credentials"]
    assert "SessionToken" in creds["Credentials"]

def test_cognito_identity_pool_roles(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="RolesPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    cognito_identity.set_identity_pool_roles(
        IdentityPoolId=iid,
        Roles={
            "authenticated": "arn:aws:iam::000000000000:role/AuthRole",
            "unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
        },
    )
    roles = cognito_identity.get_identity_pool_roles(IdentityPoolId=iid)
    assert roles["Roles"]["authenticated"] == "arn:aws:iam::000000000000:role/AuthRole"
    assert roles["Roles"]["unauthenticated"] == "arn:aws:iam::000000000000:role/UnauthRole"

def test_cognito_list_identities(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="ListIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    id1 = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    id2 = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]

    identities = cognito_identity.list_identities(IdentityPoolId=iid, MaxResults=60)["Identities"]
    ids = [i["IdentityId"] for i in identities]
    assert id1 in ids
    assert id2 in ids

def test_cognito_get_open_id_token(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="OidcPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]

    token_resp = cognito_identity.get_open_id_token(IdentityId=identity_id)
    assert token_resp["IdentityId"] == identity_id
    token = token_resp["Token"]
    # Verify stub JWT structure: header.payload.sig
    parts = token.split(".")
    assert len(parts) == 3

def test_cognito_signup_always_unconfirmed(cognito_idp):
    """SignUp always returns UNCONFIRMED regardless of AutoVerifiedAttributes."""
    # Pool with AutoVerifiedAttributes — user still starts UNCONFIRMED
    pid = cognito_idp.create_user_pool(
        PoolName="AutoVerifyPool",
        AutoVerifiedAttributes=["email"],
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="AutoVerifyApp")["UserPoolClient"]["ClientId"]
    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="testuser",
        Password="TestPass1!",
        UserAttributes=[{"Name": "email", "Value": "test@example.com"}],
    )
    assert resp["UserConfirmed"] is False
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="testuser")
    assert user["UserStatus"] == "UNCONFIRMED"

    # Pool with NO AutoVerifiedAttributes — user also starts UNCONFIRMED
    pid2 = cognito_idp.create_user_pool(PoolName="NoAutoVerifyPool")["UserPool"]["Id"]
    cid2 = cognito_idp.create_user_pool_client(UserPoolId=pid2, ClientName="NoAutoVerifyApp")["UserPoolClient"][
        "ClientId"
    ]
    resp2 = cognito_idp.sign_up(ClientId=cid2, Username="testuser2", Password="TestPass1!")
    assert resp2["UserConfirmed"] is False
    user2 = cognito_idp.admin_get_user(UserPoolId=pid2, Username="testuser2")
    assert user2["UserStatus"] == "UNCONFIRMED"

def test_cognito_change_password(cognito_idp):
    """ChangePassword decodes the access token and updates the stored password."""
    pid = cognito_idp.create_user_pool(PoolName="ChangePwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="ChangePwdApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="pwduser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="pwduser", Password="OldPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "pwduser", "PASSWORD": "OldPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    cognito_idp.change_password(
        AccessToken=access_token,
        PreviousPassword="OldPass1!",
        ProposedPassword="NewPass2!",
    )

    # New password must work
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "pwduser", "PASSWORD": "NewPass2!"},
    )
    assert "AuthenticationResult" in auth2

    # Old password must fail
    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "pwduser", "PASSWORD": "OldPass1!"},
        )
    assert exc_info.value.response["Error"]["Code"] == "NotAuthorizedException"

def test_cognito_refresh_token_auth_correct_user(cognito_idp):
    """REFRESH_TOKEN_AUTH returns tokens for the correct user, not the first user in the pool."""
    pid = cognito_idp.create_user_pool(PoolName="RefreshPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RefreshApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    for name, pw in [("first", "FirstPass1!"), ("second", "SecondPass1!")]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
        cognito_idp.admin_set_user_password(UserPoolId=pid, Username=name, Password=pw, Permanent=True)

    # Auth as "second" user and refresh
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "second", "PASSWORD": "SecondPass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]

    refresh = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    assert "AuthenticationResult" in refresh
    # New access token should resolve back to "second" via GetUser
    new_access = refresh["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=new_access)
    assert user["Username"] == "second"

def test_cognito_refresh_token_alias(cognito_idp):
    """REFRESH_TOKEN (without _AUTH suffix) is accepted as an alias."""
    pid = cognito_idp.create_user_pool(PoolName="RefreshAliasPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RefreshAliasApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="aliasuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="aliasuser", Password="AliasPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "aliasuser", "PASSWORD": "AliasPass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    refresh = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    assert "AuthenticationResult" in refresh
    assert "AccessToken" in refresh["AuthenticationResult"]
    assert "RefreshToken" not in refresh["AuthenticationResult"]

def test_cognito_respond_to_auth_challenge_new_password(cognito_idp):
    """RespondToAuthChallenge with NEW_PASSWORD_REQUIRED confirms the user."""
    pid = cognito_idp.create_user_pool(PoolName="ChallengePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="ChallengeApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="newpwduser")
    # Set a temp password — Permanent=False keeps FORCE_CHANGE_PASSWORD status
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="newpwduser", Password="TempPass1!", Permanent=False)
    # Initiate auth — FORCE_CHANGE_PASSWORD triggers NEW_PASSWORD_REQUIRED challenge
    auth = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "newpwduser", "PASSWORD": "TempPass1!"},
    )
    assert auth.get("ChallengeName") == "NEW_PASSWORD_REQUIRED"
    session = auth["Session"]
    result = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="NEW_PASSWORD_REQUIRED",
        Session=session,
        ChallengeResponses={"USERNAME": "newpwduser", "NEW_PASSWORD": "FinalPass1!"},
    )
    assert "AuthenticationResult" in result
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="newpwduser")
    assert user["UserStatus"] == "CONFIRMED"

def test_cognito_update_user_attributes_via_token(cognito_idp):
    """UpdateUserAttributes (self-service) updates attributes using access token."""
    pid = cognito_idp.create_user_pool(PoolName="UpdateAttrTokenPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="UpdateAttrApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="attrupdate",
        UserAttributes=[{"Name": "email", "Value": "old@example.com"}],
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="attrupdate", Password="AttrPass1!", Permanent=True)
    access_token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "attrupdate", "PASSWORD": "AttrPass1!"},
    )["AuthenticationResult"]["AccessToken"]

    cognito_idp.update_user_attributes(
        AccessToken=access_token,
        UserAttributes=[{"Name": "email", "Value": "new@example.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="attrupdate")
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "new@example.com"

def test_cognito_delete_user_via_token(cognito_idp):
    """DeleteUser (self-service) removes the user using access token."""
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="DeleteSelfPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="DeleteSelfApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="selfdelete")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="selfdelete", Password="DelPass1!", Permanent=True)
    access_token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "selfdelete", "PASSWORD": "DelPass1!"},
    )["AuthenticationResult"]["AccessToken"]

    cognito_idp.delete_user(AccessToken=access_token)

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="selfdelete")
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"

def test_cognito_update_user_pool_client(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="UpdateClientPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="OriginalName")["UserPoolClient"]["ClientId"]
    updated = cognito_idp.update_user_pool_client(
        UserPoolId=pid,
        ClientId=cid,
        ClientName="UpdatedName",
        RefreshTokenValidity=14,
    )["UserPoolClient"]
    assert updated["ClientName"] == "UpdatedName"
    assert updated["RefreshTokenValidity"] == 14
    # Verify persisted
    desc = cognito_idp.describe_user_pool_client(UserPoolId=pid, ClientId=cid)["UserPoolClient"]
    assert desc["ClientName"] == "UpdatedName"

def test_cognito_admin_reset_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResetPwdPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="resetuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="resetuser", Password="PassWord1!", Permanent=True)
    cognito_idp.admin_reset_user_password(UserPoolId=pid, Username="resetuser")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="resetuser")
    assert user["UserStatus"] == "RESET_REQUIRED"

def test_cognito_admin_user_global_sign_out(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GlobalSignOutAdminPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="AdminSignOutApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="signoutuser")
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="signoutuser", Password="SignOut1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "signoutuser", "PASSWORD": "SignOut1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    time.sleep(1.1)  # sign-out invalidates tokens issued before now (1s granularity)
    cognito_idp.admin_user_global_sign_out(UserPoolId=pid, Username="signoutuser")
    # the user's refresh tokens must be invalidated (#1395)
    with pytest.raises(cognito_idp.exceptions.NotAuthorizedException):
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid, ClientId=cid, AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
        )

def test_cognito_revoke_token(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="RevokePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RevokeApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="revokeuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="revokeuser", Password="RevokePass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "revokeuser", "PASSWORD": "RevokePass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    # baseline: the refresh token authenticates before revocation
    cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    cognito_idp.revoke_token(Token=refresh_token, ClientId=cid)
    # after RevokeToken it must no longer mint new tokens (#1395)
    with pytest.raises(cognito_idp.exceptions.NotAuthorizedException):
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid, ClientId=cid, AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
        )

def test_cognito_describe_identity(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="DescribeIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    desc = cognito_identity.describe_identity(IdentityId=identity_id)
    assert desc["IdentityId"] == identity_id

def test_cognito_merge_developer_identities(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="MergePool",
        AllowUnauthenticatedIdentities=True,
        DeveloperProviderName="login.myapp",
    )
    iid = resp["IdentityPoolId"]
    result = cognito_identity.merge_developer_identities(
        SourceUserIdentifier="user-a",
        DestinationUserIdentifier="user-b",
        DeveloperProviderName="login.myapp",
        IdentityPoolId=iid,
    )
    assert "IdentityId" in result

def test_cognito_credentials_secret_access_key(cognito_identity):
    """GetCredentialsForIdentity must return SecretKey (boto3 wire name)."""
    iid = cognito_identity.create_identity_pool(
        IdentityPoolName="qa-creds-pool",
        AllowUnauthenticatedIdentities=True,
    )["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    creds = cognito_identity.get_credentials_for_identity(IdentityId=identity_id)
    c = creds["Credentials"]
    assert "SecretKey" in c
    assert c["AccessKeyId"].startswith("ASIA")
    assert "SessionToken" in c
    assert c["Expiration"] is not None

def test_cognito_change_password_actually_changes(cognito_idp):
    """ChangePassword must update the stored password so old one stops working."""
    pid = cognito_idp.create_user_pool(PoolName="qa-changepwd")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-changepwd-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-cpwd-user")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="qa-cpwd-user", Password="OldPwd1!", Permanent=True)
    token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "OldPwd1!"},
    )["AuthenticationResult"]["AccessToken"]
    cognito_idp.change_password(AccessToken=token, PreviousPassword="OldPwd1!", ProposedPassword="NewPwd2!")
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "NewPwd2!"},
    )
    assert "AuthenticationResult" in auth2
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "OldPwd1!"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"

def test_cognito_refresh_token_returns_correct_user(cognito_idp):
    """REFRESH_TOKEN_AUTH must return tokens for the refreshing user, not users[0]."""
    pid = cognito_idp.create_user_pool(PoolName="qa-refresh-pool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-refresh-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    for name, pw in [("qa-first", "FirstPass1!"), ("qa-second", "SecondPass1!")]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
        cognito_idp.admin_set_user_password(UserPoolId=pid, Username=name, Password=pw, Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-second", "PASSWORD": "SecondPass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    refresh = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    new_token = refresh["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=new_token)
    assert user["Username"] == "qa-second", "Refresh must return tokens for qa-second not qa-first"

def test_cognito_signup_unconfirmed_with_auto_verify(cognito_idp):
    """SignUp with AutoVerifiedAttributes must return UserConfirmed=False."""
    pid = cognito_idp.create_user_pool(PoolName="qa-autoverify", AutoVerifiedAttributes=["email"])["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="qa-autoverify-app")["UserPoolClient"][
        "ClientId"
    ]
    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="qa-signup-user",
        Password="SignUp1!",
        UserAttributes=[{"Name": "email", "Value": "qa@example.com"}],
    )
    assert resp["UserConfirmed"] is False
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="qa-signup-user")
    assert user["UserStatus"] == "UNCONFIRMED"

def test_cognito_disabled_user_auth_fails(cognito_idp):
    """Disabled user must get NotAuthorizedException."""
    pid = cognito_idp.create_user_pool(PoolName="qa-disabled-pool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-disabled-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-disabled")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="qa-disabled", Password="DisableP1!", Permanent=True)
    cognito_idp.admin_disable_user(UserPoolId=pid, Username="qa-disabled")
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "qa-disabled", "PASSWORD": "DisableP1!"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"

def test_cognito_list_users_in_group(cognito_idp):
    """ListUsersInGroup must return members added via AdminAddUserToGroup."""
    pid = cognito_idp.create_user_pool(PoolName="qa-group-members")["UserPool"]["Id"]
    cognito_idp.create_group(UserPoolId=pid, GroupName="qa-grp")
    for u in ["qa-u1", "qa-u2", "qa-u3"]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=u)
        cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username=u, GroupName="qa-grp")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="qa-grp")["Users"]
    names = {u["Username"] for u in members}
    assert {"qa-u1", "qa-u2", "qa-u3"} == names

def test_cognito_duplicate_username_error(cognito_idp):
    """AdminCreateUser with duplicate username must raise UsernameExistsException."""
    pid = cognito_idp.create_user_pool(PoolName="qa-dup-user")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-dup")
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-dup")
    assert exc.value.response["Error"]["Code"] == "UsernameExistsException"

def test_cognito_client_secret_generated(cognito_idp):
    """CreateUserPoolClient with GenerateSecret=True must return a ClientSecret."""
    pid = cognito_idp.create_user_pool(PoolName="qa-secret-client")["UserPool"]["Id"]
    client = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="qa-secret-app", GenerateSecret=True)[
        "UserPoolClient"
    ]
    assert "ClientSecret" in client
    assert len(client["ClientSecret"]) > 20

def test_cognito_force_change_password_challenge(cognito_idp):
    """AdminCreateUser with TemporaryPassword triggers NEW_PASSWORD_REQUIRED challenge."""
    pid = cognito_idp.create_user_pool(PoolName="qa-force-change")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-force-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="qa-force-user",
        TemporaryPassword="TempPwd1!",
    )
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-force-user", "PASSWORD": "TempPwd1!"},
    )
    assert auth.get("ChallengeName") == "NEW_PASSWORD_REQUIRED"
    assert "Session" in auth

def test_cognito_totp_full_flow(cognito_idp):
    """Full TOTP MFA flow: SetUserPoolMfaConfig ON → AssociateSoftwareToken →
    VerifySoftwareToken → InitiateAuth returns SOFTWARE_TOKEN_MFA challenge →
    RespondToAuthChallenge with any code returns tokens."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-full")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    # Enable TOTP MFA on the pool
    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="ON",
    )
    cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert cfg["MfaConfiguration"] == "ON"
    assert cfg["SoftwareTokenMfaConfiguration"]["Enabled"] is True

    # Create and confirm user
    cognito_idp.admin_create_user(UserPoolId=pid, Username="totp-user", TemporaryPassword="TmpPass1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="totp-user", Password="PermPass1!", Permanent=True)

    # Enroll TOTP: associate → get tokens first (MFA not yet enrolled, pool is ON but no enrollment)
    # Pool ON with no enrollment → auth succeeds so user can enroll
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "totp-user", "PASSWORD": "PermPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    # Associate software token
    assoc = cognito_idp.associate_software_token(AccessToken=access_token)
    assert "SecretCode" in assoc
    assert len(assoc["SecretCode"]) > 0

    # Verify (accept any code)
    verify = cognito_idp.verify_software_token(AccessToken=access_token, UserCode="123456")
    assert verify["Status"] == "SUCCESS"

    # Now auth should return SOFTWARE_TOKEN_MFA challenge
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "totp-user", "PASSWORD": "PermPass1!"},
    )
    assert auth2.get("ChallengeName") == "SOFTWARE_TOKEN_MFA"
    assert "Session" in auth2

    # Respond with any TOTP code → get tokens
    result = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid,
        ClientId=cid,
        ChallengeName="SOFTWARE_TOKEN_MFA",
        ChallengeResponses={"USERNAME": "totp-user", "SOFTWARE_TOKEN_MFA_CODE": "123456"},
    )
    assert "AuthenticationResult" in result
    assert "AccessToken" in result["AuthenticationResult"]

def test_cognito_totp_optional_mfa(cognito_idp):
    """OPTIONAL MFA: users without TOTP enrolled go straight to tokens;
    users with TOTP enrolled get the challenge."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-optional")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-opt-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="OPTIONAL",
    )

    # User without MFA enrolled
    cognito_idp.admin_create_user(UserPoolId=pid, Username="no-mfa-user", TemporaryPassword="TmpPass1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="no-mfa-user", Password="PermPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "no-mfa-user", "PASSWORD": "PermPass1!"},
    )
    assert "AuthenticationResult" in auth  # no challenge — not enrolled

    # User with MFA enrolled via AdminSetUserMFAPreference
    cognito_idp.admin_create_user(UserPoolId=pid, Username="mfa-user", TemporaryPassword="TmpPass1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="mfa-user", Password="PermPass1!", Permanent=True)
    cognito_idp.admin_set_user_mfa_preference(
        UserPoolId=pid,
        Username="mfa-user",
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "mfa-user", "PASSWORD": "PermPass1!"},
    )
    assert auth2.get("ChallengeName") == "SOFTWARE_TOKEN_MFA"

def test_cognito_admin_get_user_mfa_fields(cognito_idp):
    """AdminGetUser returns correct UserMFASettingList and PreferredMfaSetting."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-getuser")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="mfa-check-user", TemporaryPassword="TmpPass1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="mfa-check-user", Password="PermPass1!", Permanent=True)

    # Before enrollment
    u = cognito_idp.admin_get_user(UserPoolId=pid, Username="mfa-check-user")
    assert u["UserMFASettingList"] == []
    assert u["PreferredMfaSetting"] == ""

    # After enrollment
    cognito_idp.admin_set_user_mfa_preference(
        UserPoolId=pid,
        Username="mfa-check-user",
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )
    u2 = cognito_idp.admin_get_user(UserPoolId=pid, Username="mfa-check-user")
    assert "SOFTWARE_TOKEN_MFA" in u2["UserMFASettingList"]
    assert u2["PreferredMfaSetting"] == "SOFTWARE_TOKEN_MFA"

def test_cognito_set_user_mfa_preference_via_token(cognito_idp):
    """SetUserMFAPreference (public, uses AccessToken) enrolls TOTP on the user."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-selfenroll")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-self-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="self-enroll", TemporaryPassword="TmpPass1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="self-enroll", Password="PermPass1!", Permanent=True)

    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "self-enroll", "PASSWORD": "PermPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    cognito_idp.set_user_mfa_preference(
        AccessToken=access_token,
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )

    u = cognito_idp.admin_get_user(UserPoolId=pid, Username="self-enroll")
    assert "SOFTWARE_TOKEN_MFA" in u["UserMFASettingList"]
    assert u["PreferredMfaSetting"] == "SOFTWARE_TOKEN_MFA"

def test_cognito_jwks_endpoint():
    """/.well-known/jwks.json returns valid JWK set."""
    import json as _json
    import urllib.request

    from conftest import make_client
    cognito = make_client("cognito-idp")
    pool = cognito.create_user_pool(PoolName="jwks-pool")["UserPool"]
    pool_id = pool["Id"]
    req = urllib.request.Request(
        f"{ENDPOINT}/{pool_id}/.well-known/jwks.json",
    )
    with urllib.request.urlopen(req) as r:
        data = _json.loads(r.read())
    assert "keys" in data
    assert len(data["keys"]) >= 1
    assert data["keys"][0]["kty"] == "RSA"
    assert data["keys"][0]["alg"] == "RS256"


def _cognito_key_fingerprint_loader(state_dir, *, block_fcntl=False):
    import subprocess
    import sys
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["STATE_DIR"] = str(state_dir)
    env["PYTHONPATH"] = (
        f"{repo_root}{os.pathsep}{env['PYTHONPATH']}"
        if env.get("PYTHONPATH")
        else str(repo_root)
    )
    fcntl_blocker = (
        "import importlib.abc\n"
        "import sys\n"
        "sys.modules.pop('fcntl', None)\n"
        "class _BlockFcntl(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname == 'fcntl':\n"
        "            raise ImportError('blocked fcntl')\n"
        "        return None\n"
        "sys.meta_path.insert(0, _BlockFcntl())\n"
    ) if block_fcntl else ""
    script = (
        fcntl_blocker +
        "import hashlib; "
        "from cryptography.hazmat.primitives import serialization; "
        "import ministack.services.cognito as c; "
        "pem = c._RSA_PRIVATE_KEY.private_bytes("
        "encoding=serialization.Encoding.PEM, "
        "format=serialization.PrivateFormat.PKCS8, "
        "encryption_algorithm=serialization.NoEncryption()); "
        "print(hashlib.sha256(pem).hexdigest())"
    )

    def _load_key_fingerprint(_idx):
        return subprocess.check_output(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            timeout=10,
        ).strip()

    return _load_key_fingerprint


def _load_concurrent_cognito_key_fingerprints(state_dir, workers=8):
    import concurrent.futures

    _load_key_fingerprint = _cognito_key_fingerprint_loader(state_dir)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(_load_key_fingerprint, range(workers)))


def test_cognito_rsa_key_creation_is_process_safe(tmp_path):
    """Concurrent server/test-worker imports must converge on one JWKS key."""
    fingerprints = _load_concurrent_cognito_key_fingerprints(tmp_path)

    assert len(set(fingerprints)) == 1
    assert (tmp_path / "cognito-rsa-key.pem").exists()


def test_cognito_rsa_key_corruption_repair_is_process_safe(tmp_path):
    """Concurrent imports must converge after replacing an unreadable JWKS key."""
    from cryptography.hazmat.primitives import serialization

    key_path = tmp_path / "cognito-rsa-key.pem"
    key_path.write_text("not a private key")

    fingerprints = _load_concurrent_cognito_key_fingerprints(tmp_path)

    assert len(set(fingerprints)) == 1
    serialization.load_pem_private_key(key_path.read_bytes(), password=None)


def test_cognito_rsa_key_stale_repair_lock_is_recovered(tmp_path):
    """A crashed repairer must not permanently block shared JWKS key recovery."""
    from cryptography.hazmat.primitives import serialization

    key_path = tmp_path / "cognito-rsa-key.pem"
    lock_path = tmp_path / "cognito-rsa-key.pem.lock"
    key_path.write_text("not a private key")
    lock_path.mkdir()

    fingerprints = _load_concurrent_cognito_key_fingerprints(tmp_path)

    assert len(set(fingerprints)) == 1
    assert lock_path.is_file()
    serialization.load_pem_private_key(key_path.read_bytes(), password=None)


def test_cognito_rsa_key_repair_waits_for_live_file_lock(tmp_path):
    """Concurrent importers must wait for a live repair lock, not steal it."""
    import concurrent.futures
    import fcntl

    from cryptography.hazmat.primitives import serialization

    key_path = tmp_path / "cognito-rsa-key.pem"
    lock_path = tmp_path / "cognito-rsa-key.pem.lock"
    key_path.write_text("not a private key")
    lock_path.touch()
    _load_key_fingerprint = _cognito_key_fingerprint_loader(tmp_path)

    with open(lock_path, "a") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_load_key_fingerprint, idx) for idx in range(8)]
            time.sleep(0.1)
            assert not any(future.done() for future in futures)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            fingerprints = [future.result(timeout=10) for future in futures]

    assert len(set(fingerprints)) == 1
    serialization.load_pem_private_key(key_path.read_bytes(), password=None)


def test_cognito_rsa_key_persists_when_fcntl_is_unavailable(tmp_path):
    """Missing fcntl must not disable RSA signing or shared key persistence."""
    loader = _cognito_key_fingerprint_loader(tmp_path, block_fcntl=True)

    first_fingerprint = loader(0)
    second_fingerprint = loader(1)

    assert first_fingerprint == second_fingerprint
    assert (tmp_path / "cognito-rsa-key.pem").exists()


def test_cognito_openid_configuration():
    """/.well-known/openid-configuration returns valid discovery document."""
    import json as _json
    import urllib.request

    from conftest import make_client
    cognito = make_client("cognito-idp")
    pool = cognito.create_user_pool(PoolName="oidc-pool")["UserPool"]
    pool_id = pool["Id"]
    req = urllib.request.Request(
        f"{ENDPOINT}/{pool_id}/.well-known/openid-configuration",
    )
    with urllib.request.urlopen(req) as r:
        data = _json.loads(r.read())
    assert "issuer" in data
    assert pool_id in data["issuer"]
    assert "jwks_uri" in data
    assert "token_endpoint" in data
    # AWS Cognito advertises both code and token grants
    assert "code" in data["response_types_supported"]
    assert "token" in data["response_types_supported"]


def test_cognito_browser_endpoints_send_cors_headers():
    """Cognito's OAuth2/OIDC endpoints must send `Access-Control-Allow-Origin`
    so browser-based OIDC clients can fetch them. Regression for the bug
    where the dispatcher in app.py returned raw response tuples for
    /.well-known/*, /oauth2/*, /login and /logout, bypassing the
    `_with_data_plane_headers` wrapper that every other data-plane response
    goes through."""
    import urllib.request

    from conftest import make_client
    cognito = make_client("cognito-idp")
    pool_id = cognito.create_user_pool(PoolName="cors-pool")["UserPool"]["Id"]

    # Public well-known endpoints — fetched cross-origin during OIDC discovery.
    for path in (
        f"/{pool_id}/.well-known/openid-configuration",
        f"/{pool_id}/.well-known/jwks.json",
    ):
        with urllib.request.urlopen(f"{ENDPOINT}{path}") as r:
            assert r.headers.get("Access-Control-Allow-Origin") == "*", (
                f"missing CORS header on {path}"
            )

    # Token endpoint — POSTed cross-origin by the OIDC client. We don't care
    # about the body (an empty form yields a 4xx) — only that the CORS header
    # is present on the response.
    req = urllib.request.Request(
        f"{ENDPOINT}/oauth2/token",
        data=b"grant_type=authorization_code",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        resp = e
    assert resp.headers.get("Access-Control-Allow-Origin") == "*", (
        "missing CORS header on /oauth2/token"
    )


# ===========================================================================
# Identity Provider CRUD
# ===========================================================================

def test_cognito_create_and_describe_identity_provider(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpCrudPool")["UserPool"]["Id"]
    resp = cognito_idp.create_identity_provider(
        UserPoolId=pid,
        ProviderName="MySAML",
        ProviderType="SAML",
        ProviderDetails={"MetadataURL": "https://idp.example.com/metadata"},
        AttributeMapping={"email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress"},
        IdpIdentifiers=["my-saml"],
    )
    provider = resp["IdentityProvider"]
    assert provider["ProviderName"] == "MySAML"
    assert provider["ProviderType"] == "SAML"
    assert provider["ProviderDetails"]["MetadataURL"] == "https://idp.example.com/metadata"
    assert provider["IdpIdentifiers"] == ["my-saml"]
    assert "CreationDate" in provider
    assert "LastModifiedDate" in provider

    desc = cognito_idp.describe_identity_provider(UserPoolId=pid, ProviderName="MySAML")
    assert desc["IdentityProvider"]["ProviderName"] == "MySAML"
    assert desc["IdentityProvider"]["UserPoolId"] == pid


def test_cognito_create_identity_provider_duplicate(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpDupPool")["UserPool"]["Id"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid, ProviderName="Dup", ProviderType="OIDC",
        ProviderDetails={"client_id": "abc", "authorize_scopes": "openid"},
    )
    with pytest.raises(ClientError) as exc:
        cognito_idp.create_identity_provider(
            UserPoolId=pid, ProviderName="Dup", ProviderType="OIDC",
            ProviderDetails={"client_id": "abc", "authorize_scopes": "openid"},
        )
    assert "DuplicateProviderException" in str(exc.value)


def test_cognito_update_identity_provider(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpUpdatePool")["UserPool"]["Id"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid, ProviderName="UpdateMe", ProviderType="SAML",
        ProviderDetails={"MetadataURL": "https://old.example.com/metadata"},
        AttributeMapping={"email": "old-claim"},
    )
    resp = cognito_idp.update_identity_provider(
        UserPoolId=pid, ProviderName="UpdateMe",
        ProviderDetails={"MetadataURL": "https://new.example.com/metadata"},
        AttributeMapping={"email": "new-claim", "name": "name-claim"},
        IdpIdentifiers=["updated-id"],
    )
    updated = resp["IdentityProvider"]
    assert updated["ProviderDetails"]["MetadataURL"] == "https://new.example.com/metadata"
    assert updated["AttributeMapping"]["email"] == "new-claim"
    assert updated["AttributeMapping"]["name"] == "name-claim"
    assert updated["IdpIdentifiers"] == ["updated-id"]


def test_cognito_delete_identity_provider(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpDeletePool")["UserPool"]["Id"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid, ProviderName="DeleteMe", ProviderType="OIDC",
        ProviderDetails={"client_id": "x", "authorize_scopes": "openid"},
    )
    cognito_idp.delete_identity_provider(UserPoolId=pid, ProviderName="DeleteMe")

    with pytest.raises(ClientError) as exc:
        cognito_idp.describe_identity_provider(UserPoolId=pid, ProviderName="DeleteMe")
    assert "ResourceNotFoundException" in str(exc.value)


def test_cognito_list_identity_providers(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpListPool")["UserPool"]["Id"]
    for i in range(3):
        cognito_idp.create_identity_provider(
            UserPoolId=pid, ProviderName=f"Idp{i}", ProviderType="SAML",
            ProviderDetails={"MetadataURL": f"https://idp{i}.example.com/metadata"},
        )
    resp = cognito_idp.list_identity_providers(UserPoolId=pid, MaxResults=60)
    names = [p["ProviderName"] for p in resp["Providers"]]
    assert "Idp0" in names
    assert "Idp1" in names
    assert "Idp2" in names
    # Each entry should have the summary fields
    for p in resp["Providers"]:
        assert "ProviderType" in p
        assert "CreationDate" in p
        assert "LastModifiedDate" in p


def test_cognito_list_identity_providers_pagination(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpPagePool")["UserPool"]["Id"]
    for i in range(5):
        cognito_idp.create_identity_provider(
            UserPoolId=pid, ProviderName=f"Page{i}", ProviderType="SAML",
            ProviderDetails={"MetadataURL": f"https://page{i}.example.com/metadata"},
        )
    resp = cognito_idp.list_identity_providers(UserPoolId=pid, MaxResults=2)
    assert len(resp["Providers"]) == 2
    assert "NextToken" in resp
    resp2 = cognito_idp.list_identity_providers(UserPoolId=pid, MaxResults=2, NextToken=resp["NextToken"])
    assert len(resp2["Providers"]) == 2
    all_names = [p["ProviderName"] for p in resp["Providers"] + resp2["Providers"]]
    assert len(set(all_names)) == 4  # no duplicates across pages


def test_cognito_get_identity_provider_by_identifier(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpByIdPool")["UserPool"]["Id"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid, ProviderName="ByIdProvider", ProviderType="SAML",
        ProviderDetails={"MetadataURL": "https://byid.example.com/metadata"},
        IdpIdentifiers=["find-me"],
    )
    resp = cognito_idp.get_identity_provider_by_identifier(UserPoolId=pid, IdpIdentifier="find-me")
    assert resp["IdentityProvider"]["ProviderName"] == "ByIdProvider"

    with pytest.raises(ClientError) as exc:
        cognito_idp.get_identity_provider_by_identifier(UserPoolId=pid, IdpIdentifier="not-exist")
    assert "ResourceNotFoundException" in str(exc.value)


def test_cognito_describe_nonexistent_identity_provider(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="IdpNotFoundPool")["UserPool"]["Id"]
    with pytest.raises(ClientError) as exc:
        cognito_idp.describe_identity_provider(UserPoolId=pid, ProviderName="Ghost")
    assert "ResourceNotFoundException" in str(exc.value)


# ===========================================================================
# Federated SAML / OAuth2 flow
# ===========================================================================

class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Capture 302 redirects without following them."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


_no_redirect_opener = urllib.request.build_opener(_NoRedirectHandler)


def _setup_saml_pool(cognito_idp, lambda_config=None):
    """Helper: create a pool + client + SAML provider for federated tests."""
    pid = cognito_idp.create_user_pool(
        PoolName="FedPool", **({"LambdaConfig": lambda_config} if lambda_config else {}),
    )["UserPool"]["Id"]
    client = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="FedApp",
        CallbackURLs=["http://localhost:3000/callback"],
        AllowedOAuthFlows=["code"],
        AllowedOAuthScopes=["openid", "email"],
        SupportedIdentityProviders=["TestSAML"],
    )["UserPoolClient"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid,
        ProviderName="TestSAML",
        ProviderType="SAML",
        ProviderDetails={"IDPSSOEndpoint": "https://idp.example.com/saml/sso"},
        AttributeMapping={
            "email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
            "name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
        },
    )
    return pid, client["ClientId"]


def _build_mock_saml_response(name_id, attributes=None):
    """Build a minimal SAML Response XML for testing, return base64-encoded."""
    attrs_xml = ""
    if attributes:
        attr_statements = []
        for name, value in attributes.items():
            attr_statements.append(
                f'<saml:Attribute Name="{name}">'
                f'<saml:AttributeValue>{value}</saml:AttributeValue>'
                f'</saml:Attribute>'
            )
        attrs_xml = '<saml:AttributeStatement>' + ''.join(attr_statements) + '</saml:AttributeStatement>'

    xml = (
        '<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"'
        ' xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">'
        '<saml:Assertion>'
        '<saml:Subject>'
        f'<saml:NameID>{name_id}</saml:NameID>'
        '</saml:Subject>'
        f'{attrs_xml}'
        '</saml:Assertion>'
        '</samlp:Response>'
    )
    return base64.b64encode(xml.encode("utf-8")).decode()


def test_cognito_oauth2_authorize_saml_redirect(cognito_idp):
    """GET /oauth2/authorize should 302 to the SAML IdP with SAMLRequest."""
    pid, cid = _setup_saml_pool(cognito_idp)
    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=xyz123&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected redirect, got 200"
    except urllib.error.HTTPError as e:
        assert e.code == 302, f"Expected 302, got {e.code}"
        location = e.headers.get("Location", "")
        assert "idp.example.com" in location
        assert "SAMLRequest=" in location
        assert "RelayState=" in location


def test_cognito_oauth2_authorize_invalid_client(cognito_idp):
    """GET /oauth2/authorize with unknown client_id returns 400."""
    url = f"{ENDPOINT}/oauth2/authorize?response_type=code&client_id=nonexistent&redirect_uri=http://x&identity_provider=X"
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected error"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert "ResourceNotFoundException" in body.get("__type", "")


def test_cognito_saml_full_flow(cognito_idp):
    """Full SAML flow: authorize → SAML response → token exchange → user created."""
    pid, cid = _setup_saml_pool(cognito_idp)

    # Step 1: GET /oauth2/authorize → extract RelayState from redirect Location
    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=mystate&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected redirect"
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location", "")
    parsed_loc = urlparse(location)
    relay_state = _parse_qs(parsed_loc.query).get("RelayState", [""])[0]
    assert relay_state, "RelayState should be in redirect URL"

    # Step 2: POST /saml2/idpresponse with mock SAML assertion
    saml_resp = _build_mock_saml_response(
        name_id="john@example.com",
        attributes={
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress": "john@example.com",
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name": "John Doe",
        },
    )
    form_data = _urlencode({"SAMLResponse": saml_resp, "RelayState": relay_state}).encode()
    req2 = urllib.request.Request(
        f"{ENDPOINT}/saml2/idpresponse",
        data=form_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        _no_redirect_opener.open(req2)
        assert False, "Expected redirect"
    except urllib.error.HTTPError as e2:
        callback_location = e2.headers.get("Location", "")
    assert "localhost:3000/callback" in callback_location
    assert "code=" in callback_location
    assert "state=mystate" in callback_location

    # Extract authorization code
    parsed_cb = urlparse(callback_location)
    auth_code = _parse_qs(parsed_cb.query).get("code", [""])[0]
    assert auth_code, "Authorization code should be in callback URL"

    # Step 3: POST /oauth2/token with authorization_code grant
    token_data = (
        f"grant_type=authorization_code&code={auth_code}"
        f"&client_id={cid}&redirect_uri=http://localhost:3000/callback"
    ).encode()
    req3 = urllib.request.Request(
        f"{ENDPOINT}/oauth2/token",
        data=token_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req3) as resp:
        tokens = json.loads(resp.read())
    assert "access_token" in tokens
    assert "id_token" in tokens
    assert "refresh_token" in tokens
    assert tokens["token_type"] == "Bearer"

    # Step 3b: Verify id_token contains email claim
    id_payload_b64 = tokens["id_token"].split(".")[1]
    id_payload_b64 += "=" * (4 - len(id_payload_b64) % 4)
    id_claims = json.loads(base64.urlsafe_b64decode(id_payload_b64))
    assert id_claims.get("email") == "john@example.com", f"Missing email in id_token: {id_claims}"
    assert id_claims.get("token_use") == "id"
    assert "cognito:username" in id_claims

    # Step 4: Verify user was created via AdminGetUser
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="TestSAML_john@example.com")
    assert user["Username"] == "TestSAML_john@example.com"
    assert user["UserStatus"] == "EXTERNAL_PROVIDER"
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs.get("email") == "john@example.com"
    assert attrs.get("name") == "John Doe"


def test_cognito_saml_presignup_lambda_rejects_unauthorized_user(cognito_idp, lam):
    """PreSignUp_ExternalProvider trigger fails closed: an uninvited federated
    user is rejected and never persisted."""
    handler = "def handler(event, ctx):\n    raise Exception('not invited')\n"
    fn_name = "ministack-presignup-reject"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
    pid, cid = _setup_saml_pool(cognito_idp, lambda_config={"PreSignUp": fn_arn})

    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=mystate&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected redirect"
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location", "")
    relay_state = _parse_qs(urlparse(location).query).get("RelayState", [""])[0]

    saml_resp = _build_mock_saml_response(name_id="uninvited@example.com")
    form_data = _urlencode({"SAMLResponse": saml_resp, "RelayState": relay_state}).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/saml2/idpresponse", data=form_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        _no_redirect_opener.open(req)
        assert False, "Expected 400"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert "UserLambdaValidationException" in body.get("__type", "")

    with pytest.raises(ClientError):
        cognito_idp.admin_get_user(UserPoolId=pid, Username="TestSAML_uninvited@example.com")


def test_cognito_saml_presignup_lambda_autoconfirms_invited_user(cognito_idp, lam):
    """PreSignUp_ExternalProvider trigger's `autoConfirmUser`/`autoVerifyEmail`
    overrides are reflected on the persisted federated user."""
    handler = (
        "def handler(event, ctx):\n"
        "    event['response']['autoConfirmUser'] = True\n"
        "    event['response']['autoVerifyEmail'] = True\n"
        "    return event\n"
    )
    fn_name = "ministack-presignup-autoconfirm"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
    pid, cid = _setup_saml_pool(cognito_idp, lambda_config={"PreSignUp": fn_arn})

    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=mystate&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected redirect"
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location", "")
    relay_state = _parse_qs(urlparse(location).query).get("RelayState", [""])[0]

    saml_resp = _build_mock_saml_response(
        name_id="invited@example.com",
        attributes={
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress": "invited@example.com",
        },
    )
    form_data = _urlencode({"SAMLResponse": saml_resp, "RelayState": relay_state}).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/saml2/idpresponse", data=form_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        _no_redirect_opener.open(req)
        assert False, "Expected redirect"
    except urllib.error.HTTPError:
        pass

    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="TestSAML_invited@example.com")
    # A federated (external-IdP) user is always EXTERNAL_PROVIDER — autoConfirmUser
    # does not flip that status. autoVerifyEmail, however, still applies.
    assert user["UserStatus"] == "EXTERNAL_PROVIDER"
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs.get("email_verified") == "true"


# ---------------------------------------------------------------------------
# OIDC federation (external OIDC IdP — e.g. Keycloak in front of Cognito)
# ---------------------------------------------------------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unsigned_id_token(claims: dict) -> str:
    """Build a JWS-Compact id_token with header.payload.signature.
    Signature is a dummy `notsigned` string — MiniStack doesn't verify."""
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps(claims).encode())
    return f"{header}.{payload}.notsigned"


def _start_fake_oidc_idp(claims):
    """Spin up a local HTTP server acting as an OIDC IdP's token endpoint.

    Returns (token_url, recorded_request, stop_fn). recorded_request is mutated
    in place when the IdP receives the token exchange POST so tests can assert
    on what MiniStack actually sent on the wire.
    """
    import http.server
    import threading

    recorded = {}

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body_bytes = self.rfile.read(length)
            recorded["path"] = self.path
            recorded["headers"] = dict(self.headers)
            recorded["body"] = body_bytes.decode("utf-8")
            recorded["form"] = {
                k: v[0] for k, v in _parse_qs(recorded["body"]).items()
            }
            payload = json.dumps({
                "access_token": "fake-access-token",
                "id_token": _unsigned_id_token(claims),
                "token_type": "Bearer",
                "expires_in": 3600,
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args, **kwargs):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    token_url = f"http://127.0.0.1:{port}/token"

    def stop():
        srv.shutdown()
        srv.server_close()
        thread.join(timeout=2)

    return token_url, recorded, stop


def _setup_oidc_pool(cognito_idp, token_url, lambda_config=None):
    pid = cognito_idp.create_user_pool(
        PoolName="OIDCFedPool", **({"LambdaConfig": lambda_config} if lambda_config else {}),
    )["UserPool"]["Id"]
    client = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="OIDCApp",
        CallbackURLs=["http://localhost:3000/callback"],
        AllowedOAuthFlows=["code"],
        AllowedOAuthScopes=["openid", "email"],
        SupportedIdentityProviders=["TestOIDC"],
    )["UserPoolClient"]
    cognito_idp.create_identity_provider(
        UserPoolId=pid,
        ProviderName="TestOIDC",
        ProviderType="OIDC",
        ProviderDetails={
            "oidc_issuer": "https://idp.example.com",
            "authorize_url": "https://idp.example.com/authorize",
            "token_url": token_url,
            "client_id": "oidc-client-id",
            "client_secret": "oidc-client-secret",
            "authorize_scopes": "openid email profile",
        },
        AttributeMapping={"email": "email", "name": "name"},
    )
    return pid, client["ClientId"]


def test_cognito_oauth2_authorize_oidc_redirect(cognito_idp):
    """GET /oauth2/authorize with an OIDC identity_provider 302s to the IdP
    with redirect_uri pointing at MiniStack's /oauth2/idpresponse."""
    token_url, _recorded, stop = _start_fake_oidc_idp({"sub": "ignored"})
    try:
        _pid, cid = _setup_oidc_pool(cognito_idp, token_url)
        url = (
            f"{ENDPOINT}/oauth2/authorize?"
            f"response_type=code&client_id={cid}"
            f"&redirect_uri=http://localhost:3000/callback"
            f"&identity_provider=TestOIDC&state=mystate&scope=openid"
        )
        try:
            _no_redirect_opener.open(url)
            assert False, "Expected 302"
        except urllib.error.HTTPError as e:
            assert e.code == 302
            location = e.headers.get("Location", "")
        assert "idp.example.com/authorize" in location
        qs = _parse_qs(urlparse(location).query)
        assert qs["client_id"] == ["oidc-client-id"]
        # The redirect_uri MS hands to the IdP must point at MS's OIDC
        # callback, not the SAML one (regression guard for the original bug).
        assert qs["redirect_uri"] == [f"{_advertised_endpoint()}/oauth2/idpresponse"]
        assert qs["state"]  # relay key present
    finally:
        stop()


def test_cognito_oidc_full_flow(cognito_idp):
    """End-to-end OIDC federation: authorize → IdP callback → token exchange
    against fake IdP → user provisioned → app redirect with MS auth code."""
    claims = {
        "sub": "user-9001",
        "email": "alice@example.com",
        "name": "Alice External",
    }
    token_url, recorded, stop = _start_fake_oidc_idp(claims)
    try:
        pid, cid = _setup_oidc_pool(cognito_idp, token_url)

        # Step 1: kick off authorize, grab the relay state from the IdP redirect.
        authorize_url = (
            f"{ENDPOINT}/oauth2/authorize?"
            f"response_type=code&client_id={cid}"
            f"&redirect_uri=http://localhost:3000/callback"
            f"&identity_provider=TestOIDC&state=appstate&scope=openid"
        )
        try:
            _no_redirect_opener.open(authorize_url)
            assert False, "Expected 302"
        except urllib.error.HTTPError as e:
            idp_redirect = e.headers.get("Location", "")
        relay_state = _parse_qs(urlparse(idp_redirect).query)["state"][0]

        # Step 2: simulate the OIDC IdP calling back with code+state.
        cb_url = (
            f"{ENDPOINT}/oauth2/idpresponse?"
            f"code=idp-issued-code&state={relay_state}"
        )
        try:
            _no_redirect_opener.open(cb_url)
            assert False, "Expected 302 back to the app"
        except urllib.error.HTTPError as e:
            callback_location = e.headers.get("Location", "")

        # Step 3: MS must have called the IdP's token endpoint with the right
        # grant_type / code / redirect_uri / client credentials.
        assert recorded.get("path") == "/token"
        form = recorded["form"]
        assert form["grant_type"] == "authorization_code"
        assert form["code"] == "idp-issued-code"
        assert form["redirect_uri"] == f"{_advertised_endpoint()}/oauth2/idpresponse"
        assert form["client_id"] == "oidc-client-id"
        assert form["client_secret"] == "oidc-client-secret"

        # Step 4: MS must redirect to the app callback with a MS-issued code
        # and the original app state.
        assert "localhost:3000/callback" in callback_location
        cb_qs = _parse_qs(urlparse(callback_location).query)
        assert cb_qs["state"] == ["appstate"]
        ms_code = cb_qs["code"][0]
        assert ms_code

        # Step 5: the federated user was provisioned under the OIDC provider
        # namespace, with the id_token claims mapped through AttributeMapping.
        federated_username = "TestOIDC_user-9001"
        user = cognito_idp.admin_get_user(UserPoolId=pid, Username=federated_username)
        assert user["UserStatus"] == "EXTERNAL_PROVIDER"
        attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
        assert attrs["email"] == "alice@example.com"
        assert attrs["name"] == "Alice External"
    finally:
        stop()


def test_cognito_oidc_presignup_lambda_rejects_unauthorized_user(cognito_idp, lam):
    """`/oauth2/idpresponse` also honours the PreSignUp_ExternalProvider
    trigger: a rejected federated user is never persisted."""
    handler = "def handler(event, ctx):\n    raise Exception('not invited')\n"
    fn_name = "ministack-presignup-oidc-reject"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    claims = {"sub": "user-uninvited", "email": "uninvited@example.com"}
    token_url, _recorded, stop = _start_fake_oidc_idp(claims)
    try:
        pid, cid = _setup_oidc_pool(cognito_idp, token_url, lambda_config={"PreSignUp": fn_arn})

        authorize_url = (
            f"{ENDPOINT}/oauth2/authorize?"
            f"response_type=code&client_id={cid}"
            f"&redirect_uri=http://localhost:3000/callback"
            f"&identity_provider=TestOIDC&state=appstate&scope=openid"
        )
        try:
            _no_redirect_opener.open(authorize_url)
            assert False, "Expected 302"
        except urllib.error.HTTPError as e:
            idp_redirect = e.headers.get("Location", "")
        relay_state = _parse_qs(urlparse(idp_redirect).query)["state"][0]

        cb_url = f"{ENDPOINT}/oauth2/idpresponse?code=idp-issued-code&state={relay_state}"
        try:
            _no_redirect_opener.open(cb_url)
            assert False, "Expected 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            body = json.loads(e.read())
            assert "UserLambdaValidationException" in body.get("__type", "")

        with pytest.raises(ClientError):
            cognito_idp.admin_get_user(UserPoolId=pid, Username="TestOIDC_user-uninvited")
    finally:
        stop()


def test_cognito_oidc_callback_invalid_state(cognito_idp):
    """`/oauth2/idpresponse` with an unknown state returns 400 — relay codes
    are single-use, expired, and tied to a prior /oauth2/authorize call."""
    token_url, _recorded, stop = _start_fake_oidc_idp({"sub": "ignored"})
    try:
        _setup_oidc_pool(cognito_idp, token_url)
        cb_url = f"{ENDPOINT}/oauth2/idpresponse?code=x&state=nonexistent"
        try:
            _no_redirect_opener.open(cb_url)
            assert False, "Expected 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            body = json.loads(e.read())
            assert "InvalidParameterException" in body.get("__type", "")
    finally:
        stop()


def test_cognito_oauth2_token_invalid_code():
    """POST /oauth2/token with invalid code returns 400."""
    data = b"grant_type=authorization_code&code=invalid_code&client_id=x"
    req = urllib.request.Request(
        f"{ENDPOINT}/oauth2/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Expected error"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert body.get("error") == "invalid_grant"


def test_cognito_federated_user_idempotent(cognito_idp):
    """Running SAML flow twice with same NameID updates user, doesn't duplicate."""
    pid, cid = _setup_saml_pool(cognito_idp)

    def _do_saml_flow(name_value):
        # Authorize
        url = (
            f"{ENDPOINT}/oauth2/authorize?response_type=code&client_id={cid}"
            f"&redirect_uri=http://localhost:3000/callback"
            f"&identity_provider=TestSAML&state=s&scope=openid"
        )
        try:
            _no_redirect_opener.open(url)
        except urllib.error.HTTPError as e:
            location = e.headers.get("Location", "")
        relay = _parse_qs(urlparse(location).query).get("RelayState", [""])[0]

        # SAML response
        saml = _build_mock_saml_response(
            name_id="repeat@example.com",
            attributes={
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name": name_value,
            },
        )
        form = _urlencode({"SAMLResponse": saml, "RelayState": relay}).encode()
        try:
            _no_redirect_opener.open(urllib.request.Request(
                f"{ENDPOINT}/saml2/idpresponse", data=form,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ))
        except urllib.error.HTTPError:
            pass

    _do_saml_flow("First Name")
    _do_saml_flow("Updated Name")

    # Should be one user, not two
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="TestSAML_repeat@example.com")
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs.get("name") == "Updated Name"

    # Count users with this username pattern
    all_users = cognito_idp.list_users(UserPoolId=pid)["Users"]
    repeat_users = [u for u in all_users if u["Username"] == "TestSAML_repeat@example.com"]
    assert len(repeat_users) == 1


def test_cognito_groups_in_auth_tokens(cognito_idp):
    """cognito:groups claim must appear in both access and ID tokens."""
    pid = cognito_idp.create_user_pool(PoolName="GroupTokenPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="GroupTokenApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    cognito_idp.create_group(UserPoolId=pid, GroupName="admin")
    cognito_idp.create_group(UserPoolId=pid, GroupName="readers")
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="groupuser",
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="groupuser", Password="Group1234!", Permanent=True,
    )
    cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username="groupuser", GroupName="admin")
    cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username="groupuser", GroupName="readers")

    auth = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "groupuser", "PASSWORD": "Group1234!"},
    )
    result = auth["AuthenticationResult"]

    def _decode_jwt_payload(token):
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))

    access_claims = _decode_jwt_payload(result["AccessToken"])
    assert "cognito:groups" in access_claims, "cognito:groups missing from access token"
    assert sorted(access_claims["cognito:groups"]) == ["admin", "readers"]
    assert "scope" in access_claims, "scope missing from access token"
    assert access_claims["scope"] == "aws.cognito.signin.user.admin"

    id_claims = _decode_jwt_payload(result["IdToken"])
    assert "cognito:groups" in id_claims, "cognito:groups missing from id token"
    assert sorted(id_claims["cognito:groups"]) == ["admin", "readers"]


def test_cognito_access_token_scope_no_groups(cognito_idp):
    """AccessToken includes scope claim even when user has no groups."""
    import base64
    pid = cognito_idp.create_user_pool(PoolName="ScopePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="scope-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="scopeuser",
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="scopeuser", Password="Scope1234!", Permanent=True,
    )
    auth = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "scopeuser", "PASSWORD": "Scope1234!"},
    )
    payload = auth["AuthenticationResult"]["AccessToken"].split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    assert claims["scope"] == "aws.cognito.signin.user.admin"
    assert "cognito:groups" not in claims  # no groups = no claim


def test_cognito_admin_set_password_by_sub(cognito_idp):
    """AdminSetUserPassword works with sub UUID, not just username."""
    pid = cognito_idp.create_user_pool(PoolName="SubPassPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="subpassuser",
        UserAttributes=[{"Name": "email", "Value": "subpass@test.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="subpassuser")
    sub = next(a["Value"] for a in user["UserAttributes"] if a["Name"] == "sub")
    # Set password using sub UUID
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username=sub, Password="NewPass1234!", Permanent=True,
    )
    # Verify user still accessible
    user2 = cognito_idp.admin_get_user(UserPoolId=pid, Username=sub)
    assert user2["Username"] == "subpassuser"


def test_cognito_admin_disable_by_sub(cognito_idp):
    """AdminDisableUser works with sub UUID."""
    pid = cognito_idp.create_user_pool(PoolName="SubDisPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="subdisuser",
        UserAttributes=[{"Name": "email", "Value": "subdis@test.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="subdisuser")
    sub = next(a["Value"] for a in user["UserAttributes"] if a["Name"] == "sub")
    cognito_idp.admin_disable_user(UserPoolId=pid, Username=sub)
    user2 = cognito_idp.admin_get_user(UserPoolId=pid, Username=sub)
    assert user2["Enabled"] is False

# ========== from test_cognito_oauth2.py ==========

"""
Integration tests for Cognito OAuth2/OIDC IdP endpoints.

Tests the full OAuth2 authorization code flow including:
  /oauth2/authorize, /login, /oauth2/token, /oauth2/userInfo, /logout
"""
import base64
import hashlib
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request

from conftest import ENDPOINT as _CONFTEST_ENDPOINT
from conftest import make_client

ENDPOINT = _CONFTEST_ENDPOINT.rstrip("/")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_pool_with_user(
    cognito_idp,
    generate_secret=True,
    force_change_password=False,
    lambda_config=None,
    explicit_auth_flows=None,
    mfa_configuration=None,
    allowed_first_auth_factors=None,
    user_email='test@example.com',
):
    """Create a user pool with a confirmed (or FORCE_CHANGE_PASSWORD) user and an
    OAuth-enabled client."""
    policies = None
    if allowed_first_auth_factors is not None:
        # Passing Policies replaces it wholesale, so keep the default
        # PasswordPolicy alongside the SignInPolicy override.
        policies = {
            "PasswordPolicy": {
                "MinimumLength": 8,
                "RequireUppercase": True,
                "RequireLowercase": True,
                "RequireNumbers": True,
                "RequireSymbols": True,
                "TemporaryPasswordValidityDays": 7,
            },
            "SignInPolicy": {"AllowedFirstAuthFactors": allowed_first_auth_factors},
        }

    pool = cognito_idp.create_user_pool(
        PoolName='OAuth2TestPool',
        **({"LambdaConfig": lambda_config} if lambda_config else {}),
        **({"MfaConfiguration": mfa_configuration} if mfa_configuration else {}),
        **({"Policies": policies} if policies else {}),
    )
    pool_id = pool['UserPool']['Id']

    client_kwargs = {
        'UserPoolId': pool_id,
        'ClientName': 'oauth2-test-client',
        'GenerateSecret': generate_secret,
        'AllowedOAuthFlows': ['code'],
        'AllowedOAuthScopes': ['openid', 'email', 'profile'],
        'AllowedOAuthFlowsUserPoolClient': True,
        'CallbackURLs': ['http://localhost:3000/callback'],
        'LogoutURLs': ['http://localhost:3000/logout'],
        'DefaultRedirectURI': 'http://localhost:3000/callback',
        'ExplicitAuthFlows': explicit_auth_flows or ['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
    }
    client_resp = cognito_idp.create_user_pool_client(**client_kwargs)
    client = client_resp['UserPoolClient']

    cognito_idp.admin_create_user(
        UserPoolId=pool_id,
        Username='testuser',
        TemporaryPassword='TempPass1!',
        UserAttributes=[
            {'Name': 'email', 'Value': user_email},
            {'Name': 'email_verified', 'Value': 'true'},
            {'Name': 'name', 'Value': 'Test User'},
        ],
    )
    if not force_change_password:
        cognito_idp.admin_set_user_password(
            UserPoolId=pool_id, Username='testuser', Password='TestPass1!', Permanent=True,
        )

    return pool_id, client


def _lower_headers(h):
    """Return a plain dict with all header names lowercased."""
    return {k.lower(): v for k, v in h.items()}


def _extract_np_token(html):
    """Pull the `np_token` hidden-field value out of the new-password form HTML."""
    marker = 'name="np_token" value="'
    start = html.index(marker) + len(marker)
    end = html.index('"', start)
    return html[start:end]


def _extract_ua_token(html):
    """Pull the `ua_token` hidden-field value out of a choice-based sign-in form HTML."""
    marker = 'name="ua_token" value="'
    start = html.index(marker) + len(marker)
    end = html.index('"', start)
    return html[start:end]


def _get(url, follow_redirects=True):
    """GET request, optionally not following redirects."""
    req = urllib.request.Request(url, method='GET')
    if not follow_redirects:
        opener = urllib.request.build_opener(_NoRedirectHandler)
    else:
        opener = urllib.request.build_opener()
    try:
        resp = opener.open(req, timeout=10)
        return resp.status, _lower_headers(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, _lower_headers(e.headers), e.read()


def _post_form(url, data, headers=None, follow_redirects=True):
    """POST form-encoded data."""
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    if not follow_redirects:
        opener = urllib.request.build_opener(_NoRedirectHandler)
    else:
        opener = urllib.request.build_opener()
    try:
        resp = opener.open(req, timeout=10)
        return resp.status, _lower_headers(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, _lower_headers(e.headers), e.read()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(newurl, code, msg, headers, fp)


# ---------------------------------------------------------------------------
# Tests — /oauth2/authorize
# ---------------------------------------------------------------------------

def test_oauth2_authorize_shows_login_form():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback'
           f'&scope=openid+email'
           f'&state=abc123')
    status, headers, body = _get(url)
    html = body.decode('utf-8')

    assert status == 200
    assert 'text/html' in headers.get('content-type', '')
    assert '<form' in html
    assert 'username' in html
    assert 'password' in html
    assert client_id in html


def test_oauth2_authorize_invalid_client():
    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id=nonexistent'
           f'&redirect_uri=http://localhost:3000/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_client'


def test_oauth2_authorize_invalid_redirect_uri():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://evil.com/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_request'


def test_oauth2_authorize_unsupported_response_type():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=token'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'unsupported_response_type'


# ---------------------------------------------------------------------------
# Tests — /login
# ---------------------------------------------------------------------------

def test_oauth2_login_success_redirects_with_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'mystate',
            'response_type': 'code',
        },
        follow_redirects=False,
    )

    assert status == 302
    location = headers.get('location', '')
    assert location.startswith('http://localhost:3000/callback')
    parsed = urllib.parse.urlparse(location)
    qs = urllib.parse.parse_qs(parsed.query)
    assert 'code' in qs
    assert qs['state'] == ['mystate']


def test_oauth2_login_failure_shows_error():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'WrongPass!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid',
            'state': 'xyz',
            'response_type': 'code',
        },
    )

    assert status == 200
    html = body.decode('utf-8')
    assert 'Incorrect username or password' in html


def test_oauth2_login_force_change_password_shows_new_password_form():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, force_change_password=True)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TempPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'mystate',
            'response_type': 'code',
        },
    )

    assert status == 200
    html = body.decode('utf-8')
    assert '<form' in html
    assert 'new_password' in html
    assert 'confirm_password' in html
    assert 'np_token' in html


def test_oauth2_new_password_submit_success_redirects_with_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, force_change_password=True)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TempPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'mystate',
            'response_type': 'code',
        },
    )
    np_token = _extract_np_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'np_token': np_token,
            'new_password': 'NewPass1!',
            'confirm_password': 'NewPass1!',
        },
        follow_redirects=False,
    )

    assert status == 302
    location = headers.get('location', '')
    assert location.startswith('http://localhost:3000/callback')
    parsed = urllib.parse.urlparse(location)
    qs = urllib.parse.parse_qs(parsed.query)
    assert 'code' in qs
    assert qs['state'] == ['mystate']

    user = cognito_idp.admin_get_user(UserPoolId=pool_id, Username='testuser')
    assert user['UserStatus'] == 'CONFIRMED'


def test_oauth2_new_password_submit_mismatch_shows_error():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, force_change_password=True)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TempPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid',
            'state': 'xyz',
            'response_type': 'code',
        },
    )
    np_token = _extract_np_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'np_token': np_token,
            'new_password': 'NewPass1!',
            'confirm_password': 'Different1!',
        },
    )

    assert status == 200
    html = body.decode('utf-8')
    assert 'Passwords do not match' in html


def test_oauth2_new_password_submit_policy_violation_shows_error():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, force_change_password=True)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TempPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid',
            'state': 'xyz',
            'response_type': 'code',
        },
    )
    np_token = _extract_np_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'np_token': np_token,
            'new_password': 'short',
            'confirm_password': 'short',
        },
    )

    assert status == 200
    html = body.decode('utf-8')
    assert 'did not conform with policy' in html


def test_oauth2_new_password_submit_invalid_token():
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'np_token': 'nonexistent-token',
            'new_password': 'NewPass1!',
            'confirm_password': 'NewPass1!',
        },
    )
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Tests — /login (choice-based sign-in, ALLOW_USER_AUTH)
# ---------------------------------------------------------------------------

def _authorize_url(client_id):
    return (f'{ENDPOINT}/oauth2/authorize?response_type=code'
            f'&client_id={client_id}'
            f'&redirect_uri=http://localhost:3000/callback'
            f'&scope=openid+email'
            f'&state=abc123')


def test_oauth2_authorize_without_allow_user_auth_shows_single_page_form():
    """Regression: clients that do NOT opt into ALLOW_USER_AUTH keep the
    existing single-page username+password form."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    status, headers, body = _get(_authorize_url(client_id))
    html = body.decode('utf-8')

    assert status == 200
    assert 'name="username"' in html
    assert 'name="password"' in html
    assert 'auth_flow' not in html


def test_oauth2_authorize_with_allow_user_auth_shows_username_only_form():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
    )
    client_id = client['ClientId']

    status, headers, body = _get(_authorize_url(client_id))
    html = body.decode('utf-8')

    assert status == 200
    assert 'name="username"' in html
    assert 'name="password"' not in html
    assert 'value="USER_AUTH"' in html


def test_user_auth_two_step_flow_with_mfa_off_redirects_with_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        mfa_configuration='OFF',
    )
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')

    # Step 1: username only.
    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid email',
        'state': 'mystate',
        'response_type': 'code',
    })
    assert status == 200
    html = body.decode('utf-8')
    # MfaConfiguration=OFF skips the challenge-selection step.
    assert 'name="challenge"' not in html
    assert 'name="password"' in html
    ua_token = _extract_ua_token(html)

    # Step 2: password.
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {'ua_token': ua_token, 'password': 'TestPass1!'},
        follow_redirects=False,
    )
    assert status == 302
    location = headers.get('location', '')
    assert location.startswith('http://localhost:3000/callback')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    assert 'code' in qs
    assert qs['state'] == ['mystate']

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': qs['code'][0],
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp


def test_user_auth_three_step_flow_with_multiple_factors_shows_challenge_selection():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid email',
        'state': 'mystate',
        'response_type': 'code',
    })
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="challenge"' in html
    assert 'value="PASSWORD"' in html
    ua_token = _extract_ua_token(html)

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'challenge': 'PASSWORD'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="password"' in html


def test_user_auth_unknown_username_does_not_reveal_at_step_one():
    """Step 1 must succeed regardless of whether the username exists, to avoid
    user enumeration (matches real Cognito USER_AUTH behavior)."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        mfa_configuration='OFF',
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'no-such-user',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    assert status == 200
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'password': 'whatever'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'Incorrect username or password' in html


def test_user_auth_wrong_password_shows_error_and_allows_retry():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        mfa_configuration='OFF',
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'password': 'WrongPass!'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'Incorrect username or password' in html
    retry_ua_token = _extract_ua_token(html)
    assert retry_ua_token == ua_token

    # Same ua_token can be retried with the correct password.
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {'ua_token': ua_token, 'password': 'TestPass1!'},
        follow_redirects=False,
    )
    assert status == 302
    assert 'code' in urllib.parse.parse_qs(
        urllib.parse.urlparse(headers.get('location', '')).query)


def test_user_auth_invalid_challenge_is_rejected():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'challenge': 'WEB_AUTHN'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'Invalid authentication method' in html
    assert 'name="password"' not in html


def test_user_auth_expired_session_returns_error():
    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': 'nonexistent-token', 'password': 'x'},
    )
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_request'


def test_user_auth_force_change_password_reaches_new_password_form():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        force_change_password=True,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        mfa_configuration='OFF',
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'password': 'TempPass1!'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="new_password"' in html
    assert 'name="confirm_password"' in html
    assert 'np_token' in html


def test_user_auth_single_email_otp_factor_skips_challenge_selection():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="challenge"' not in html
    assert 'name="code"' in html
    assert 'name="password"' not in html


def test_user_auth_multiple_factors_shows_both_challenge_options():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    assert status == 200
    html = body.decode('utf-8')
    assert 'value="PASSWORD"' in html
    assert 'value="EMAIL_OTP"' in html


def test_user_auth_email_otp_flow_sends_email_and_signs_in():
    cognito_idp = make_client('cognito-idp')
    email = f'otp-{_uuid_mod.uuid4().hex[:8]}@example.com'
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
        user_email=email,
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid email',
        'state': 'mystate',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'challenge': 'EMAIL_OTP'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="code"' in html
    ua_token = _extract_ua_token(html)

    msgs = _messages_to(email, 'CognitoEmailOtpMessage')
    assert len(msgs) == 1, f'expected 1 OTP email, got {len(msgs)}'

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {'ua_token': ua_token, 'code': '123456'},
        follow_redirects=False,
    )
    assert status == 302
    location = headers.get('location', '')
    assert location.startswith('http://localhost:3000/callback')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    assert 'code' in qs
    assert qs['state'] == ['mystate']


def test_user_auth_email_otp_wrong_code_shows_error_and_allows_retry():
    cognito_idp = make_client('cognito-idp')
    email = f'otp-{_uuid_mod.uuid4().hex[:8]}@example.com'
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
        user_email=email,
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'challenge': 'EMAIL_OTP'},
    )
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'code': '000000'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'Incorrect code' in html
    retry_ua_token = _extract_ua_token(html)
    assert retry_ua_token == ua_token

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {'ua_token': ua_token, 'code': '123456'},
        follow_redirects=False,
    )
    assert status == 302
    assert 'code' in urllib.parse.parse_qs(
        urllib.parse.urlparse(headers.get('location', '')).query)


def test_user_auth_email_otp_unknown_username_shows_same_page_but_never_succeeds():
    """EMAIL_OTP must not reveal whether the username exists: the code-entry
    page renders identically, but no code can ever succeed for it."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'no-such-user',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    ua_token = _extract_ua_token(body.decode('utf-8'))

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'challenge': 'EMAIL_OTP'},
    )
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="code"' in html
    ua_token = _extract_ua_token(html)

    status, headers, body = _post_form(
        f'{ENDPOINT}/login', {'ua_token': ua_token, 'code': '123456'},
    )
    assert status == 200
    assert 'Incorrect code' in body.decode('utf-8')


def test_user_auth_mfa_on_excludes_email_otp_from_choices():
    """MfaConfiguration is the second-factor setting and is independent of
    AllowedFirstAuthFactors, except that AWS forbids OTP first factors when
    MFA is required — so EMAIL_OTP drops out and only PASSWORD remains."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        explicit_auth_flows=['ALLOW_USER_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
        mfa_configuration='ON',
        allowed_first_auth_factors=['PASSWORD', 'EMAIL_OTP'],
    )
    client_id = client['ClientId']

    status, headers, body = _post_form(f'{ENDPOINT}/login', {
        'auth_flow': 'USER_AUTH',
        'username': 'testuser',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid',
        'state': 'xyz',
        'response_type': 'code',
    })
    assert status == 200
    html = body.decode('utf-8')
    assert 'name="challenge"' not in html
    assert 'name="password"' in html


# ---------------------------------------------------------------------------
# Tests — /oauth2/token
# ---------------------------------------------------------------------------

def _do_login_and_get_code(cognito_idp, client_id, extra_form=None):
    """Helper: submit login form, return the authorization code."""
    form = {
        'username': 'testuser',
        'password': 'TestPass1!',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid email',
        'state': 'test',
        'response_type': 'code',
    }
    if extra_form:
        form.update(extra_form)
    status, headers, body = _post_form(f'{ENDPOINT}/login', form, follow_redirects=False)
    assert status == 302
    location = headers.get('location', '')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    return qs['code'][0]


def test_oauth2_token_authorization_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert 'id_token' in resp
    assert 'refresh_token' in resp
    assert resp['token_type'] == 'Bearer'
    assert resp['expires_in'] == 3600


def test_oauth2_token_failed_client_auth_does_not_consume_code():
    """A token request that fails client authentication must NOT consume the
    single-use authorization code (#932). AWS rejects bad client auth without
    invalidating the code, so a client that retries — HTTP Basic first, then
    client_secret_post, as Vault/Go's oauth2 does — succeeds on the retry.
    Consuming the code on the failed first attempt turned the retry into
    invalid_grant 'Invalid or expired authorization code'."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client['ClientSecret']
    code = _do_login_and_get_code(cognito_idp, client_id)

    # First exchange fails client auth (wrong secret) -> invalid_client.
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': 'wrong-secret',
    })
    assert status == 400, body
    assert json.loads(body)['error'] == 'invalid_client'

    # The code must survive the failed attempt: retry with the correct secret
    # on the SAME code succeeds (would be invalid_grant before the fix).
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200, body
    assert 'access_token' in json.loads(body)


def test_oauth2_basic_auth_does_not_url_decode_client_secret():
    """AWS Cognito compares the HTTP Basic client_id/client_secret EXACTLY as
    sent — it does NOT url-decode them. Real clients (incl. Go/Vault) base64 the
    raw "id:secret" without form-urlencoding, so a secret containing '+' or '/'
    must be matched verbatim. Decoding here would corrupt any secret containing
    '+' (→ space) and break valid client_secret_basic auth (#932)."""
    import base64 as _b64

    from ministack.services.cognito import _authenticate_client

    cid = "VxSsBWVIKMZK29W0IN6TKJN8EF"
    for secret in ("ab/cd+ef/gh", "no-specials-here"):
        basic = _b64.b64encode(f"{cid}:{secret}".encode()).decode()
        got_cid, got_secret = _authenticate_client({"authorization": f"Basic {basic}"}, {})
        assert got_cid == cid
        assert got_secret == secret, f"Basic auth must NOT decode; expected {secret!r}, got {got_secret!r}"


def test_oauth2_id_token_echoes_nonce():
    """OIDC requires the id_token to echo the nonce from the authorize request
    so clients can mitigate replay. Strict OIDC clients (oidc-client-ts,
    Auth0/MS libs) silently discard tokens that omit an expected nonce.
    Regression for the bug where the nonce was stored on the auth code but
    never propagated into the id_token."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')

    expected_nonce = 'a-nonce-the-client-sent-' + secrets.token_hex(8)
    code = _do_login_and_get_code(cognito_idp, client_id, {'nonce': expected_nonce})

    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    id_token = json.loads(body)['id_token']
    payload_b64 = id_token.split('.')[1]
    payload_b64 += '=' * (-len(payload_b64) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload_b64))
    assert claims.get('nonce') == expected_nonce


def test_oauth2_token_authorization_code_with_pkce():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=False)
    client_id = client['ClientId']

    # Generate PKCE pair
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')

    code = _do_login_and_get_code(cognito_idp, client_id, {
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    })

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'code_verifier': code_verifier,
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert 'id_token' in resp


def test_oauth2_token_invalid_pkce_verifier():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=False)
    client_id = client['ClientId']

    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')

    code = _do_login_and_get_code(cognito_idp, client_id, {
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    })

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'code_verifier': 'wrong-verifier',
    })

    assert status == 400
    resp = json.loads(body)
    assert resp['error'] == 'invalid_grant'


def test_oauth2_token_code_reuse():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # First use — should succeed
    status1, _, body1 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status1 == 200

    # Second use — should fail
    status2, _, body2 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status2 == 400
    resp2 = json.loads(body2)
    assert resp2['error'] == 'invalid_grant'


def test_oauth2_token_refresh_token():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # Get initial tokens
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    tokens = json.loads(body)
    refresh_token = tokens['refresh_token']

    # Refresh
    status2, _, body2 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status2 == 200
    resp2 = json.loads(body2)
    assert 'access_token' in resp2
    assert 'id_token' in resp2


def test_oauth2_token_client_credentials():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=True)
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'openid',
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert resp['token_type'] == 'Bearer'
    # client_credentials should NOT return id_token or refresh_token
    assert 'id_token' not in resp
    assert 'refresh_token' not in resp


def test_oauth2_token_client_auth_basic():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=True)
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    basic = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()

    status, _, body = _post_form(
        f'{ENDPOINT}/oauth2/token',
        {
            'grant_type': 'client_credentials',
            'scope': 'openid',
        },
        headers={'Authorization': f'Basic {basic}'},
    )

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp


def test_oauth2_login_username_attributes_email_alias_completes_token_exchange():
    """With UsernameAttributes=["email"], logging in via the Hosted UI with
    an email alias that differs from the real Username must still complete
    the authorization code exchange. Logging in with the real Username must
    also keep working."""
    cognito_idp = make_client('cognito-idp')
    pool = cognito_idp.create_user_pool(
        PoolName='UsernameAttrsOAuthPool',
        UsernameAttributes=['email'],
    )
    pool_id = pool['UserPool']['Id']

    client_resp = cognito_idp.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName='oauth2-test-client',
        GenerateSecret=True,
        AllowedOAuthFlows=['code'],
        AllowedOAuthScopes=['openid', 'email', 'profile'],
        AllowedOAuthFlowsUserPoolClient=True,
        CallbackURLs=['http://localhost:3000/callback'],
        LogoutURLs=['http://localhost:3000/logout'],
        DefaultRedirectURI='http://localhost:3000/callback',
        ExplicitAuthFlows=['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
    )
    client = client_resp['UserPoolClient']
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    create_resp = cognito_idp.admin_create_user(
        UserPoolId=pool_id,
        Username='alice@example.com',
    )
    real_username = create_resp['User']['Username']
    assert real_username != 'alice@example.com'
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username=real_username, Password='TestPass1!', Permanent=True,
    )

    def _login_and_exchange(username, state):
        status, headers, body = _post_form(
            f'{ENDPOINT}/login',
            {
                'username': username,
                'password': 'TestPass1!',
                'client_id': client_id,
                'redirect_uri': 'http://localhost:3000/callback',
                'scope': 'openid email',
                'state': state,
                'response_type': 'code',
            },
            follow_redirects=False,
        )
        assert status == 302
        location = headers.get('location', '')
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
        code = qs['code'][0]

        status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': 'http://localhost:3000/callback',
            'client_id': client_id,
            'client_secret': client_secret,
        })
        assert status == 200
        resp = json.loads(body)
        assert 'access_token' in resp

    # Login via the email alias (differs from the real Username).
    _login_and_exchange('alice@example.com', 'alias-state')
    # Login via the real Username.
    _login_and_exchange(real_username, 'real-state')


def test_oauth2_login_after_email_change_completes_token_exchange():
    """Regression for #006 + #007 combined: with UsernameAttributes=["email"],
    the real Username is an immutable UUID and email is just a mutable alias.
    After AdminUpdateUserAttributes changes the email, logging in via the
    Hosted UI with the *new* email must still complete the authorization
    code exchange, and the old email must no longer resolve."""
    cognito_idp = make_client('cognito-idp')
    pool = cognito_idp.create_user_pool(
        PoolName='UsernameAttrsEmailChangePool',
        UsernameAttributes=['email'],
    )
    pool_id = pool['UserPool']['Id']

    client_resp = cognito_idp.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName='oauth2-test-client',
        GenerateSecret=True,
        AllowedOAuthFlows=['code'],
        AllowedOAuthScopes=['openid', 'email', 'profile'],
        AllowedOAuthFlowsUserPoolClient=True,
        CallbackURLs=['http://localhost:3000/callback'],
        LogoutURLs=['http://localhost:3000/logout'],
        DefaultRedirectURI='http://localhost:3000/callback',
        ExplicitAuthFlows=['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
    )
    client = client_resp['UserPoolClient']
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    create_resp = cognito_idp.admin_create_user(
        UserPoolId=pool_id,
        Username='test@example.com',
    )
    real_username = create_resp['User']['Username']
    assert _uuid_mod.UUID(real_username)
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username=real_username, Password='TestPass1!', Permanent=True,
    )

    # Changing the email must not change the real Username (it stays UUID).
    cognito_idp.admin_update_user_attributes(
        UserPoolId=pool_id,
        Username=real_username,
        UserAttributes=[{'Name': 'email', 'Value': 'alice@example.com'}],
    )
    unchanged = cognito_idp.admin_get_user(UserPoolId=pool_id, Username=real_username)
    assert unchanged['Username'] == real_username

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'alice@example.com',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'new-email-state',
            'response_type': 'code',
        },
        follow_redirects=False,
    )
    assert status == 302
    location = headers.get('location', '')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    code = qs['code'][0]

    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp

    # The old email must no longer resolve to the user.
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'test@example.com',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'old-email-state',
            'response_type': 'code',
        },
        follow_redirects=False,
    )
    assert status == 200
    assert b'Incorrect username or password' in body


# ---------------------------------------------------------------------------
# Tests — /oauth2/userInfo
# ---------------------------------------------------------------------------

def test_oauth2_userinfo():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # Get tokens
    _, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    tokens = json.loads(body)
    access_token = tokens['access_token']

    # Call userInfo
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': f'Bearer {access_token}'},
    )
    resp = urllib.request.urlopen(req, timeout=10)
    assert resp.status == 200
    claims = json.loads(resp.read())

    assert 'sub' in claims
    assert claims.get('email') == 'test@example.com'
    assert claims.get('username') == 'testuser'
    assert claims.get('cognito:username') == 'testuser'
    assert claims.get('name') == 'Test User'


def test_oauth2_userinfo_returns_custom_attributes():
    """Custom attributes are part of the userInfo response, as on real Cognito."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    cognito_idp.admin_update_user_attributes(
        UserPoolId=pool_id,
        Username='testuser',
        UserAttributes=[
            {'Name': 'custom:tenant_id', 'Value': 'tenant-42'},
            {'Name': 'custom:employee_guid', 'Value': 'a1000033-0000-0000-0000-000000000033'},
        ],
    )
    code = _do_login_and_get_code(cognito_idp, client_id)

    _, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    access_token = json.loads(body)['access_token']

    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': f'Bearer {access_token}'},
    )
    resp = urllib.request.urlopen(req, timeout=10)
    assert resp.status == 200
    claims = json.loads(resp.read())

    assert claims.get('custom:tenant_id') == 'tenant-42'
    assert claims.get('custom:employee_guid') == 'a1000033-0000-0000-0000-000000000033'
    # Standard claims still come back alongside them
    assert claims.get('email') == 'test@example.com'


def test_oauth2_userinfo_invalid_token():
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': 'Bearer invalid-token'},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        assert False, 'Expected 401'
    except urllib.error.HTTPError as e:
        assert e.code == 401
        resp = json.loads(e.read())
        assert resp['error'] == 'invalid_token'


def test_oauth2_userinfo_missing_token():
    req = urllib.request.Request(f'{ENDPOINT}/oauth2/userInfo')
    try:
        urllib.request.urlopen(req, timeout=10)
        assert False, 'Expected 401'
    except urllib.error.HTTPError as e:
        assert e.code == 401


# ---------------------------------------------------------------------------
# Tests — /logout
# ---------------------------------------------------------------------------

def test_oauth2_logout_redirects():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/logout'
           f'?client_id={client_id}'
           f'&logout_uri=http://localhost:3000/logout')
    status, headers, body = _get(url, follow_redirects=False)

    assert status == 302
    assert headers.get('location', '') == 'http://localhost:3000/logout'


def test_oauth2_logout_invalid_uri():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/logout'
           f'?client_id={client_id}'
           f'&logout_uri=http://evil.com/logout')
    status, headers, body = _get(url, follow_redirects=False)

    assert status == 400
    resp = json.loads(body)
    assert resp['error'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Tests — E2E flow
# ---------------------------------------------------------------------------

def test_oauth2_full_flow():
    """End-to-end: authorize -> login -> token -> userInfo."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')

    # 1. GET /oauth2/authorize — get login form
    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback'
           f'&scope=openid+email'
           f'&state=e2e-state')
    status, headers, body = _get(url)
    assert status == 200
    assert '<form' in body.decode('utf-8')

    # 2. POST /login — submit credentials
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'e2e-state',
            'response_type': 'code',
        },
        follow_redirects=False,
    )
    assert status == 302
    location = headers.get('location', '')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    code = qs['code'][0]
    assert qs['state'] == ['e2e-state']

    # 3. POST /oauth2/token — exchange code for tokens
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    tokens = json.loads(body)
    assert 'access_token' in tokens
    assert 'id_token' in tokens
    assert 'refresh_token' in tokens

    # 4. GET /oauth2/userInfo — verify user claims
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': f'Bearer {tokens["access_token"]}'},
    )
    resp = urllib.request.urlopen(req, timeout=10)
    claims = json.loads(resp.read())
    assert claims['email'] == 'test@example.com'
    assert claims['cognito:username'] == 'testuser'

    # 5. GET /logout — redirect to logout URI
    logout_url = (f'{ENDPOINT}/logout'
                  f'?client_id={client_id}'
                  f'&logout_uri=http://localhost:3000/logout')
    status, headers, _ = _get(logout_url, follow_redirects=False)
    assert status == 302
    assert headers.get('location', '') == 'http://localhost:3000/logout'


def test_oauth2_full_flow_for_non_boot_region_pool():
    """Public discovery and hosted UI remain unscoped for regional pools."""
    cognito_idp = _regional_cognito_client("cognito-idp", "eu-central-1")
    lam = _regional_cognito_client("lambda", "eu-central-1")
    function_name = f"regional-hosted-pretoken-{_uuid_mod.uuid4().hex[:10]}"
    handler = (
        "def handler(event, ctx):\n"
        "    event['response']['claimsAndScopeOverrideDetails'] = {\n"
        "        'accessTokenGeneration': {\n"
        "            'claimsToAddOrOverride': {'hosted_region': 'eu-central-1'},\n"
        "        },\n"
        "    }\n"
        "    return event\n"
    )
    lam.create_function(
        FunctionName=function_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    function_arn = lam.get_function(FunctionName=function_name)["Configuration"][
        "FunctionArn"
    ]
    pool_id, client = _setup_pool_with_user(
        cognito_idp,
        lambda_config={
            "PreTokenGenerationConfig": {
                "LambdaArn": function_arn,
                "LambdaVersion": "V2_0",
            }
        },
    )
    client_id = client["ClientId"]

    with urllib.request.urlopen(
        f"{ENDPOINT}/{pool_id}/.well-known/jwks.json", timeout=10
    ) as response:
        assert json.loads(response.read())["keys"]
    with urllib.request.urlopen(
        f"{ENDPOINT}/{pool_id}/.well-known/openid-configuration", timeout=10
    ) as response:
        discovery = json.loads(response.read())
    assert discovery["issuer"] == (
        f"https://cognito-idp.eu-central-1.amazonaws.com/{pool_id}"
    )

    authorize_url = (
        f"{ENDPOINT}/oauth2/authorize?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&scope=openid+email&state=regional-state"
    )
    status, _, body = _get(authorize_url)
    assert status == 200
    assert "<form" in body.decode("utf-8")

    status, headers, _ = _post_form(
        f"{ENDPOINT}/login",
        {
            "username": "testuser",
            "password": "TestPass1!",
            "client_id": client_id,
            "redirect_uri": "http://localhost:3000/callback",
            "scope": "openid email",
            "state": "regional-state",
            "response_type": "code",
        },
        follow_redirects=False,
    )
    assert status == 302
    code = urllib.parse.parse_qs(
        urllib.parse.urlparse(headers["location"]).query
    )["code"][0]

    status, _, body = _post_form(
        f"{ENDPOINT}/oauth2/token",
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": "http://localhost:3000/callback",
            "client_id": client_id,
            "client_secret": client["ClientSecret"],
        },
    )
    assert status == 200
    tokens = json.loads(body)
    claims = _decode_jwt_claims(tokens["access_token"])
    assert claims["iss"] == f"https://cognito-idp.eu-central-1.amazonaws.com/{pool_id}"
    assert claims["hosted_region"] == "eu-central-1"

    request = urllib.request.Request(
        f"{ENDPOINT}/oauth2/userInfo",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        user_info = json.loads(response.read())
    assert user_info["cognito:username"] == "testuser"


# ========== from test_cognito_auth_codes_persistence.py ==========
# Two distinct OAuth2 code stores exist in services/cognito.py:
#   - _authorization_codes — managed-login PKCE flow (already persisted)
#   - _auth_codes — hosted-UI / SAML-OIDC federation relay flow (5-minute TTL)
# Both must survive warm-boot. Both stay PLAIN dicts (not AccountScopedDict)
# because the OAuth2 token endpoint has no AWS auth context — lookup is by
# random unguessable token, so wrapping them in AccountScopedDict would
# silently break the flow under any non-default tenant.


def _cognito_module():
    return importlib.import_module("ministack.services.cognito")


def _decode_jwt_claims(jwt: str) -> dict:
    p = jwt.split(".")[1]
    p += "=" * (-len(p) % 4)
    return json.loads(base64.urlsafe_b64decode(p))


def _make_pretoken_lambda_zip(handler_body: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_body)
    return buf.getvalue()


def test_cognito_user_pool_lambda_config_round_trip(cognito_idp):
    """LambdaConfig.PreTokenGenerationConfig persists on Create/Update (#533)."""
    pid = cognito_idp.create_user_pool(
        PoolName="lc-rt",
        LambdaConfig={
            "PreTokenGenerationConfig": {
                "LambdaArn": "arn:aws:lambda:us-east-1:000000000000:function:none",
                "LambdaVersion": "V2_0",
            }
        },
    )["UserPool"]["Id"]
    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    cfg = desc["LambdaConfig"]["PreTokenGenerationConfig"]
    assert cfg["LambdaArn"].endswith(":function:none")
    assert cfg["LambdaVersion"] == "V2_0"

    cognito_idp.update_user_pool(
        UserPoolId=pid,
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": "arn:aws:lambda:us-east-1:000000000000:function:other",
            "LambdaVersion": "V2_0",
        }},
    )
    desc2 = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert desc2["LambdaConfig"]["PreTokenGenerationConfig"]["LambdaArn"].endswith(":function:other")


def test_cognito_pretoken_v2_adds_custom_claim_to_access_token(cognito_idp, lam):
    """V2_0 PreTokenGeneration trigger injects custom claims into the access token (#533)."""
    handler = (
        "import json\n"
        "def handler(event, ctx):\n"
        "    attrs = event['request']['userAttributes']\n"
        "    event['response']['claimsAndScopeOverrideDetails'] = {\n"
        "        'accessTokenGeneration': {\n"
        "            'claimsToAddOrOverride': {\n"
        "                'userIDs': attrs.get('custom:userIDs', ''),\n"
        "                'tier': 'platinum',\n"
        "            },\n"
        "            'claimsToSuppress': ['origin_jti'],\n"
        "        }\n"
        "    }\n"
        "    return event\n"
    )
    fn_name = "ministack-pretoken-v2"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    pid = cognito_idp.create_user_pool(
        PoolName="pretoken-v2",
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": fn_arn, "LambdaVersion": "V2_0",
        }},
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="u",
        UserAttributes=[{"Name": "custom:userIDs", "Value": "abc,def"}],
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="u", Password="Pwd1234!", Permanent=True,
    )
    tok = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "u", "PASSWORD": "Pwd1234!"},
    )["AuthenticationResult"]
    access = _decode_jwt_claims(tok["AccessToken"])
    assert access.get("userIDs") == "abc,def"
    assert access.get("tier") == "platinum"
    assert "origin_jti" not in access


def test_cognito_pretoken_v2_id_token_section(cognito_idp, lam):
    """V2_0 idTokenGeneration / accessTokenGeneration sections target their own token (#533)."""
    handler = (
        "def handler(event, ctx):\n"
        "    event['response']['claimsAndScopeOverrideDetails'] = {\n"
        "        'idTokenGeneration': {'claimsToAddOrOverride': {'id_only_marker': 'yes'}},\n"
        "        'accessTokenGeneration': {'claimsToAddOrOverride': {'access_only_marker': 'yes'}},\n"
        "    }\n"
        "    return event\n"
    )
    fn_name = "ministack-pretoken-v2-split"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    pid = cognito_idp.create_user_pool(
        PoolName="pretoken-split",
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": fn_arn, "LambdaVersion": "V2_0",
        }},
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="u",
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="u", Password="Pwd1234!", Permanent=True,
    )
    tok = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "u", "PASSWORD": "Pwd1234!"},
    )["AuthenticationResult"]
    access = _decode_jwt_claims(tok["AccessToken"])
    id_tok = _decode_jwt_claims(tok["IdToken"])
    assert access.get("access_only_marker") == "yes"
    assert "id_only_marker" not in access
    assert id_tok.get("id_only_marker") == "yes"
    assert "access_only_marker" not in id_tok


def test_cognito_pretoken_lambda_failure_fail_open(cognito_idp, lam):
    """A broken PreTokenGeneration Lambda fails open: token issued without overrides (#533)."""
    handler = "def handler(event, ctx):\n    raise RuntimeError('boom')\n"
    fn_name = "ministack-pretoken-broken"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    pid = cognito_idp.create_user_pool(
        PoolName="pretoken-broken",
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": fn_arn, "LambdaVersion": "V2_0",
        }},
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="u",
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="u", Password="Pwd1234!", Permanent=True,
    )
    tok = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "u", "PASSWORD": "Pwd1234!"},
    )["AuthenticationResult"]
    access = _decode_jwt_claims(tok["AccessToken"])
    assert access["client_id"] == cid  # token still issued


def test_cognito_pretoken_trigger_source_by_auth_flow(cognito_idp, lam):
    """PreTokenGeneration's triggerSource should differ by call path, matching
    real AWS: InitiateAuth(USER_PASSWORD_AUTH) -> TokenGeneration_Authentication,
    REFRESH_TOKEN_AUTH/refresh_token grant -> TokenGeneration_RefreshTokens,
    Hosted UI authorization_code grant -> TokenGeneration_HostedAuth. Before the
    fix, every path fell through to _fake_token()'s default
    TokenGeneration_Authentication (#003)."""
    handler = (
        "def handler(event, ctx):\n"
        "    src = event['triggerSource']\n"
        "    event['response']['claimsAndScopeOverrideDetails'] = {\n"
        "        'accessTokenGeneration': {'claimsToAddOrOverride': {'seen_trigger_source': src}},\n"
        "    }\n"
        "    return event\n"
    )
    fn_name = "ministack-pretoken-trigger-source"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    pool_id, client = _setup_pool_with_user(cognito_idp)
    cognito_idp.update_user_pool(
        UserPoolId=pool_id,
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": fn_arn, "LambdaVersion": "V2_0",
        }},
    )
    client_id = client["ClientId"]
    client_secret = client.get("ClientSecret", "")

    # InitiateAuth (USER_PASSWORD_AUTH) -> TokenGeneration_Authentication
    auth = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "testuser", "PASSWORD": "TestPass1!"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(auth["AccessToken"])["seen_trigger_source"] == "TokenGeneration_Authentication"

    # REFRESH_TOKEN_AUTH (InitiateAuth) -> TokenGeneration_RefreshTokens
    refreshed = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": auth["RefreshToken"]},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(refreshed["AccessToken"])["seen_trigger_source"] == "TokenGeneration_RefreshTokens"

    # Hosted UI (/login -> /oauth2/token authorization_code grant) -> TokenGeneration_HostedAuth
    code = _do_login_and_get_code(cognito_idp, client_id)
    status, _, body = _post_form(f"{ENDPOINT}/oauth2/token", {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost:3000/callback",
        "client_id": client_id,
        "client_secret": client_secret,
    })
    assert status == 200, body
    tokens = json.loads(body)
    assert _decode_jwt_claims(tokens["access_token"])["seen_trigger_source"] == "TokenGeneration_HostedAuth"

    # /oauth2/token refresh_token grant -> TokenGeneration_RefreshTokens
    status2, _, body2 = _post_form(f"{ENDPOINT}/oauth2/token", {
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client_id,
        "client_secret": client_secret,
    })
    assert status2 == 200, body2
    resp2 = json.loads(body2)
    assert _decode_jwt_claims(resp2["access_token"])["seen_trigger_source"] == "TokenGeneration_RefreshTokens"


def test_cognito_pretoken_receives_client_metadata(cognito_idp, lam):
    """PreTokenGeneration's event.request.clientMetadata should carry
    ClientMetadata from RespondToAuthChallenge / AdminRespondToAuthChallenge,
    and aws_client_metadata from the M2M client_credentials grant — matching
    real AWS's documented behavior
    (https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-lambda-pre-token-generation.html):
    "Amazon Cognito doesn't include data from the ClientMetadata parameter in
    AdminInitiateAuth and InitiateAuth API operations in the request that it
    passes to the pre token generation function." That exclusion covers
    REFRESH_TOKEN_AUTH (both InitiateAuth and AdminInitiateAuth) and
    GetTokensFromRefreshToken too, since none of those are
    RespondToAuthChallenge. Before this fix, _build_pretoken_event never
    included the key for any operation; an earlier version of this fix
    forwarded it from exactly the operations real AWS excludes, and dropped
    it from the ones that actually carry it."""
    handler = (
        "def handler(event, ctx):\n"
        "    cm = event['request'].get('clientMetadata') or {}\n"
        "    event['response']['claimsAndScopeOverrideDetails'] = {\n"
        "        'accessTokenGeneration': {'claimsToAddOrOverride': {'seen_org_id': cm.get('orgId', '')}},\n"
        "    }\n"
        "    return event\n"
    )
    fn_name = "ministack-pretoken-client-metadata"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_pretoken_lambda_zip(handler)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=True)
    cognito_idp.update_user_pool(
        UserPoolId=pool_id,
        LambdaConfig={"PreTokenGenerationConfig": {
            "LambdaArn": fn_arn, "LambdaVersion": "V2_0",
        }},
    )
    client_id = client["ClientId"]
    client_secret = client["ClientSecret"]

    # InitiateAuth(USER_PASSWORD_AUTH) — real AWS never forwards ClientMetadata
    # here, so the trigger must see none, even though the caller passed one.
    auth = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "testuser", "PASSWORD": "TestPass1!"},
        ClientMetadata={"orgId": "org-password"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(auth["AccessToken"])["seen_org_id"] == ""

    # InitiateAuth(REFRESH_TOKEN_AUTH) — same exclusion.
    refreshed = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": auth["RefreshToken"]},
        ClientMetadata={"orgId": "org-refresh"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(refreshed["AccessToken"])["seen_org_id"] == ""

    # AdminInitiateAuth(ADMIN_USER_PASSWORD_AUTH) — same exclusion.
    admin_auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pool_id, ClientId=client_id, AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "testuser", "PASSWORD": "TestPass1!"},
        ClientMetadata={"orgId": "org-admin-password"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(admin_auth["AccessToken"])["seen_org_id"] == ""

    # AdminInitiateAuth(REFRESH_TOKEN_AUTH) — same exclusion.
    admin_refreshed = cognito_idp.admin_initiate_auth(
        UserPoolId=pool_id, ClientId=client_id, AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": auth["RefreshToken"]},
        ClientMetadata={"orgId": "org-admin-refresh"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(admin_refreshed["AccessToken"])["seen_org_id"] == ""

    # GetTokensFromRefreshToken — same exclusion.
    gtfrt = cognito_idp.get_tokens_from_refresh_token(
        ClientId=client_id, RefreshToken=auth["RefreshToken"],
        ClientMetadata={"orgId": "org-gtfrt"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(gtfrt["AccessToken"])["seen_org_id"] == ""

    # RespondToAuthChallenge(NEW_PASSWORD_REQUIRED) — one of the two operations
    # real AWS's docs say actually carries ClientMetadata to this trigger.
    cognito_idp.admin_create_user(UserPoolId=pool_id, Username="newpwduser")
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username="newpwduser", Password="TempPass1!", Permanent=False)
    challenge = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "newpwduser", "PASSWORD": "TempPass1!"},
    )
    assert challenge["ChallengeName"] == "NEW_PASSWORD_REQUIRED"
    respond_result = cognito_idp.respond_to_auth_challenge(
        ClientId=client_id, ChallengeName="NEW_PASSWORD_REQUIRED", Session=challenge["Session"],
        ChallengeResponses={"USERNAME": "newpwduser", "NEW_PASSWORD": "FinalPass1!"},
        ClientMetadata={"orgId": "org-new-password"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(respond_result["AccessToken"])["seen_org_id"] == "org-new-password"

    # AdminRespondToAuthChallenge(NEW_PASSWORD_REQUIRED) — the admin counterpart.
    cognito_idp.admin_create_user(UserPoolId=pool_id, Username="adminnewpwduser")
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username="adminnewpwduser", Password="TempPass1!", Permanent=False)
    admin_challenge = cognito_idp.admin_initiate_auth(
        UserPoolId=pool_id, ClientId=client_id, AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "adminnewpwduser", "PASSWORD": "TempPass1!"},
    )
    assert admin_challenge["ChallengeName"] == "NEW_PASSWORD_REQUIRED"
    admin_respond_result = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pool_id, ClientId=client_id, ChallengeName="NEW_PASSWORD_REQUIRED",
        Session=admin_challenge["Session"],
        ChallengeResponses={"USERNAME": "adminnewpwduser", "NEW_PASSWORD": "FinalPass1!"},
        ClientMetadata={"orgId": "org-admin-new-password"},
    )["AuthenticationResult"]
    assert _decode_jwt_claims(admin_respond_result["AccessToken"])["seen_org_id"] == "org-admin-new-password"

    # M2M client_credentials — carries client metadata via the separate
    # aws_client_metadata POST field, not a ClientMetadata request parameter.
    status, _, body = _post_form(f"{ENDPOINT}/oauth2/token", {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "aws_client_metadata": json.dumps({"orgId": "org-m2m"}),
    })
    assert status == 200
    m2m_token = json.loads(body)["access_token"]
    assert _decode_jwt_claims(m2m_token)["seen_org_id"] == "org-m2m"


@pytest.fixture
def _enable_persistence(monkeypatch, tmp_path):
    """Force PERSIST_STATE on and point STATE_DIR at a tmp dir so
    save_state / load_state actually write and read JSON files."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def _cognito_round_trip(mod, svc_key="cognito"):
    """Simulate a full warm-boot via the on-disk JSON path."""
    persistence.save_state(svc_key, mod.get_state())
    mod.reset()
    loaded = persistence.load_state(svc_key)
    assert loaded is not None, "load_state returned None — get_state may be wrong"
    mod.restore_state(loaded)


def test_auth_codes_survive_warm_boot(_enable_persistence):
    """`_auth_codes` populated by the hosted-UI / federation flow must
    survive a warm-boot through the on-disk JSON path. Without the fix
    `_auth_codes` was missing from get_state/restore_state, so any
    in-flight hosted-UI sign-in within the 5-minute code TTL was
    silently invalidated by a restart."""
    mod = _cognito_module()
    mod.reset()

    relay_state = "test-relay-12345"
    mod._auth_codes[relay_state] = {
        "type": "code",
        "pool_id": "us-east-1_TestPool",
        "client_id": "client-id-abc",
        "username": "user@example.com",
        "sub": "user-sub-12345",
        "redirect_uri": "https://app.example.com/callback",
        "scopes": "openid email",
        "created_at": 1700000000.0,
    }

    _cognito_round_trip(mod)

    assert relay_state in mod._auth_codes, (
        "Hosted-UI relay code lost across warm-boot — _auth_codes must "
        "be in both get_state() and restore_state()."
    )
    assert mod._auth_codes[relay_state]["pool_id"] == "us-east-1_TestPool"
    assert mod._auth_codes[relay_state]["client_id"] == "client-id-abc"
    mod.reset()


def test_cognito_alias_attributes_lookup_by_email(cognito_idp):
    """AliasAttributes=['email'] lets users be looked up by email as Username."""
    pid = cognito_idp.create_user_pool(
        PoolName="AliasEmailPool",
        AliasAttributes=["email"],
        AutoVerifiedAttributes=["email"],
    )["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alias-email-user",
        UserAttributes=[
            {"Name": "email", "Value": "alice@example.com"},
            {"Name": "email_verified", "Value": "true"},
        ],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="alice@example.com")
    assert user["Username"] == "alias-email-user"


def test_cognito_alias_attributes_lookup_by_preferred_username(cognito_idp):
    """preferred_username alias doesn't require verification."""
    pid = cognito_idp.create_user_pool(
        PoolName="AliasPreferredPool",
        AliasAttributes=["preferred_username"],
    )["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alias-pref-user",
        UserAttributes=[{"Name": "preferred_username", "Value": "alice_pref"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="alice_pref")
    assert user["Username"] == "alias-pref-user"


def test_cognito_alias_attributes_unverified_email_not_found(cognito_idp):
    """Unverified email aliases should NOT resolve when email_verified=false."""
    pid = cognito_idp.create_user_pool(
        PoolName="AliasUnverifiedPool",
        AliasAttributes=["email"],
    )["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alias-unverified",
        UserAttributes=[
            {"Name": "email", "Value": "bob@example.com"},
            {"Name": "email_verified", "Value": "false"},
        ],
    )
    with pytest.raises(ClientError) as ex:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="bob@example.com")
    assert ex.value.response["Error"]["Code"] == "UserNotFoundException"


def test_cognito_alias_attributes_email_without_verified_flag_not_found(cognito_idp):
    """Absent email_verified means alias is not resolvable (matches Cognito docs)."""
    pid = cognito_idp.create_user_pool(
        PoolName="AliasMissingVerifiedPool",
        AliasAttributes=["email"],
    )["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alias-missing-verified",
        UserAttributes=[{"Name": "email", "Value": "noverify@example.com"}],
    )
    with pytest.raises(ClientError) as ex:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="noverify@example.com")
    assert ex.value.response["Error"]["Code"] == "UserNotFoundException"


def test_cognito_alias_attributes_auth_with_email(cognito_idp):
    """InitiateAuth USER_PASSWORD_AUTH accepts the email alias as USERNAME."""
    pid = cognito_idp.create_user_pool(
        PoolName="AliasAuthPool",
        AliasAttributes=["email"],
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="AliasAuthClient",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alias-auth-user",
        UserAttributes=[
            {"Name": "email", "Value": "carol@example.com"},
            {"Name": "email_verified", "Value": "true"},
        ],
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="alias-auth-user",
        Password="StrongPass1!", Permanent=True,
    )
    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "carol@example.com", "PASSWORD": "StrongPass1!"},
    )
    assert "AuthenticationResult" in resp


def test_cognito_username_attributes_lookup_by_email(cognito_idp):
    """UsernameAttributes also enables email-based lookup. Real Cognito
    never uses the supplied value as the actual Username in this mode: it
    auto-generates a UUID Username and stores the value as the email
    attribute instead."""
    pid = cognito_idp.create_user_pool(
        PoolName="UsernameAttrsPool",
        UsernameAttributes=["email"],
    )["UserPool"]["Id"]
    create_resp = cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="dave@example.com",
    )
    real_username = create_resp["User"]["Username"]
    assert real_username != "dave@example.com"
    assert _uuid_mod.UUID(real_username)

    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="dave@example.com")
    assert user["Username"] == real_username


def test_cognito_admin_delete_user_by_email_alias(cognito_idp):
    """AdminDeleteUser must delete the user that the alias resolves to. In a
    UsernameAttributes pool the "_users" key is a generated UUID, so a delete
    keyed on the caller-supplied email raised KeyError and the user survived.
    A delete-then-create seed then failed with UsernameExistsException."""
    pid = cognito_idp.create_user_pool(
        PoolName="DeleteAliasPool",
        UsernameAttributes=["email"],
    )["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="erin@example.com")

    cognito_idp.admin_delete_user(UserPoolId=pid, Username="erin@example.com")

    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="erin@example.com")
    assert exc.value.response["Error"]["Code"] == "UserNotFoundException"
    assert cognito_idp.list_users(UserPoolId=pid)["Users"] == []

    # The pool accepts the same email again.
    cognito_idp.admin_create_user(UserPoolId=pid, Username="erin@example.com")


def test_cognito_admin_group_membership_by_email_alias(cognito_idp):
    """Group membership must record the resolved Username. Storing the
    caller-supplied alias made ListUsersInGroup drop the member, because it
    looks members up as "_users" keys."""
    pid = cognito_idp.create_user_pool(
        PoolName="GroupAliasPool",
        UsernameAttributes=["email"],
    )["UserPool"]["Id"]
    real_username = cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="frank@example.com",
    )["User"]["Username"]
    cognito_idp.create_group(UserPoolId=pid, GroupName="admins")

    cognito_idp.admin_add_user_to_group(
        UserPoolId=pid, Username="frank@example.com", GroupName="admins",
    )
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="admins")["Users"]
    assert [u["Username"] for u in members] == [real_username]

    cognito_idp.admin_remove_user_from_group(
        UserPoolId=pid, Username="frank@example.com", GroupName="admins",
    )
    assert cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="admins")["Users"] == []


def test_auth_codes_dict_types_are_plain_builtin_dict():
    """`_auth_codes` and `_authorization_codes` must remain plain `dict`
    instances. They're looked up by random unguessable token from a public
    OAuth2 callback with no AWS auth context — wrapping in AccountScopedDict
    would make the lookup happen under a default account, invisible to codes
    issued under any other tenant."""
    mod = _cognito_module()
    assert type(mod._auth_codes) is dict
    assert type(mod._authorization_codes) is dict


# ---------------------------------------------------------------------------
# Invitation / verification email delivery via SES
# ---------------------------------------------------------------------------

def _fetch_ses_messages():
    """Pull SES outbox via the public inspection endpoint (account 000000000000)."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    url = f"{endpoint}/_ministack/ses/messages"
    with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=5) as r:
        data = json.loads(r.read().decode())
    return data.get("messages", {}).get("000000000000", [])


def _messages_to(addr: str, type_name: str | None = None):
    msgs = _fetch_ses_messages()
    return [
        m for m in msgs
        if addr in (m.get("To") or [])
        and (type_name is None or m.get("Type") == type_name)
    ]


def test_cognito_admin_create_user_sends_invitation_email(cognito_idp):
    pid = cognito_idp.create_user_pool(
        PoolName="InvitePool",
        AdminCreateUserConfig={
            "AllowAdminCreateUserOnly": True,
            "InviteMessageTemplate": {
                "EmailSubject": "Welcome {username}!",
                "EmailMessage": "Hi {username}, your temp password is {####}.",
            },
        },
    )["UserPool"]["Id"]

    email = f"invite-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="invitee",
        UserAttributes=[{"Name": "email", "Value": email}],
        TemporaryPassword="TempPw1!aa",
        DesiredDeliveryMediums=["EMAIL"],
    )

    msgs = _messages_to(email, "CognitoInvitationMessage")
    assert len(msgs) == 1, f"expected 1 invitation message, got {len(msgs)}"
    msg = msgs[0]
    assert msg["Subject"] == "Welcome invitee!"
    body = msg["BodyText"] or msg["BodyHtml"]
    assert "TempPw1!aa" in body
    assert "invitee" in body
    # Default sender when EmailConfiguration is unset matches AWS's COGNITO_DEFAULT.
    assert msg["Source"] == "no-reply@verificationemail.com"


def test_cognito_admin_create_user_suppress_skips_email(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SuppressPool")["UserPool"]["Id"]

    email = f"suppress-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="quiet",
        UserAttributes=[{"Name": "email", "Value": email}],
        TemporaryPassword="TempPw1!bb",
        MessageAction="SUPPRESS",
        DesiredDeliveryMediums=["EMAIL"],
    )

    assert _messages_to(email) == []


def test_cognito_admin_create_user_resend_sends_again(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResendPool")["UserPool"]["Id"]

    email = f"resend-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="reinv",
        UserAttributes=[{"Name": "email", "Value": email}],
        TemporaryPassword="TempPw1!cc",
        DesiredDeliveryMediums=["EMAIL"],
    )
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="reinv",
        MessageAction="RESEND",
        DesiredDeliveryMediums=["EMAIL"],
    )

    msgs = _messages_to(email, "CognitoInvitationMessage")
    assert len(msgs) == 2


def test_cognito_admin_create_user_sms_only_no_email(cognito_idp):
    """If only SMS is requested, MiniStack must not push an EMAIL invitation."""
    pid = cognito_idp.create_user_pool(PoolName="SmsOnlyPool")["UserPool"]["Id"]

    email = f"smsonly-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="smsuser",
        UserAttributes=[
            {"Name": "email", "Value": email},
            {"Name": "phone_number", "Value": "+15555550100"},
        ],
        TemporaryPassword="TempPw1!dd",
        DesiredDeliveryMediums=["SMS"],
    )

    assert _messages_to(email) == []


def test_cognito_admin_create_user_uses_pool_email_configuration(cognito_idp):
    custom_from = f"custom-{_uuid_mod.uuid4().hex[:8]}@example.com"
    pid = cognito_idp.create_user_pool(
        PoolName="CustomFromPool",
        EmailConfiguration={
            "From": custom_from,
            "ReplyToEmailAddress": "support@example.com",
            "EmailSendingAccount": "DEVELOPER",
        },
    )["UserPool"]["Id"]

    email = f"custom-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="branded",
        UserAttributes=[{"Name": "email", "Value": email}],
        TemporaryPassword="TempPw1!ee",
        DesiredDeliveryMediums=["EMAIL"],
    )

    msgs = _messages_to(email, "CognitoInvitationMessage")
    assert len(msgs) == 1
    assert msgs[0]["Source"] == custom_from


def test_cognito_signup_sends_verification_email(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SignupVerifyPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="VerifyApp",
    )["UserPoolClient"]["ClientId"]

    email = f"signup-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.sign_up(
        ClientId=cid,
        Username="signer",
        Password="Sup3rSecret!",
        UserAttributes=[{"Name": "email", "Value": email}],
    )

    msgs = _messages_to(email, "CognitoVerificationMessage")
    assert len(msgs) == 1
    assert "123456" in (msgs[0]["BodyText"] or msgs[0]["BodyHtml"])


def test_cognito_forgot_password_sends_verification_email(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ForgotVerifyPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="ForgotMailApp",
    )["UserPoolClient"]["ClientId"]
    email = f"forgot-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="forgetter",
        UserAttributes=[{"Name": "email", "Value": email}],
        TemporaryPassword="TempPw1!ff",
        MessageAction="SUPPRESS",
    )

    cognito_idp.forgot_password(ClientId=cid, Username="forgetter")

    msgs = _messages_to(email, "CognitoVerificationMessage")
    assert len(msgs) == 1
    assert "654321" in (msgs[0]["BodyText"] or msgs[0]["BodyHtml"])


def test_cognito_resend_confirmation_code_sends_email(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResendCodePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="ResendCodeApp",
    )["UserPoolClient"]["ClientId"]

    email = f"resendcode-{_uuid_mod.uuid4().hex[:8]}@example.com"
    cognito_idp.sign_up(
        ClientId=cid,
        Username="resender",
        Password="Sup3rSecret!",
        UserAttributes=[{"Name": "email", "Value": email}],
    )
    cognito_idp.resend_confirmation_code(ClientId=cid, Username="resender")

    msgs = _messages_to(email, "CognitoVerificationMessage")
    assert len(msgs) == 2


def test_cognito_iss_claim_uses_pool_region():
    """Regression test for #678: JWT iss claim must reflect the pool's region.

    Creates a user pool via a client configured with eu-central-1 and verifies
    that both IdToken and AccessToken carry eu-central-1 in their iss claim,
    not the server's default us-east-1.
    """
    import boto3

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    region = "eu-central-1"
    client = boto3.client(
        "cognito-idp",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    )

    pid = client.create_user_pool(PoolName="IssRegionPool")["UserPool"]["Id"]
    assert pid.startswith("eu-central-1_"), f"pool_id should encode region: {pid}"

    cid = client.create_user_pool_client(
        UserPoolId=pid,
        ClientName="IssRegionApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    client.admin_create_user(UserPoolId=pid, Username="isstest")
    client.admin_set_user_password(
        UserPoolId=pid, Username="isstest", Password="IssTest1!", Permanent=True
    )

    auth = client.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "isstest", "PASSWORD": "IssTest1!"},
    )
    result = auth["AuthenticationResult"]

    def _decode_payload(token: str) -> dict:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64))

    id_claims = _decode_payload(result["IdToken"])
    access_claims = _decode_payload(result["AccessToken"])

    expected_iss = f"https://cognito-idp.{region}.amazonaws.com/{pid}"
    assert id_claims["iss"] == expected_iss, (
        f"IdToken iss should be {expected_iss!r}, got {id_claims['iss']!r}"
    )
    assert access_claims["iss"] == expected_iss, (
        f"AccessToken iss should be {expected_iss!r}, got {access_claims['iss']!r}"
    )


def test_cognito_pool_region_parser_govcloud():
    """`_pool_region` must accept 4-segment GovCloud / ISO region prefixes.

    Cognito is available in GovCloud (us-gov-east-1, us-gov-west-1) and the ISO
    regions. The original `_pool_region` regex (`^[a-z]+-[a-z]+-\\d+$`) only
    matched 3-segment commercial regions, so GovCloud pools silently fell back
    to `get_region()` and reproduced the iss-claim bug there.
    """
    mod = _cognito_module()
    cases = [
        ("us-east-1_abc123def", "us-east-1"),
        ("eu-central-1_abc123def", "eu-central-1"),
        ("us-gov-east-1_abc123def", "us-gov-east-1"),
        ("us-gov-west-1_abc123def", "us-gov-west-1"),
        ("us-iso-east-1_abc123def", "us-iso-east-1"),
        ("us-isob-east-1_abc123def", "us-isob-east-1"),
        ("eu-isoe-west-1_abc123def", "eu-isoe-west-1"),
        ("cn-northwest-1_abc123def", "cn-northwest-1"),
        ("ap-southeast-4_abc123def", "ap-southeast-4"),
    ]
    for pool_id, expected in cases:
        assert mod._pool_region(pool_id) == expected, (
            f"_pool_region({pool_id!r}) should return {expected!r}, got {mod._pool_region(pool_id)!r}"
        )


def _regional_cognito_client(service: str, region: str):
    import boto3

    return boto3.client(
        service,
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    )


def test_cognito_user_pools_and_domains_are_region_scoped():
    east = _regional_cognito_client("cognito-idp", "us-east-1")
    west = _regional_cognito_client("cognito-idp", "us-west-2")
    suffix = _uuid_mod.uuid4().hex[:10]

    east_pool = east.create_user_pool(PoolName=f"shared-{suffix}")["UserPool"]["Id"]
    west_pool = west.create_user_pool(PoolName=f"shared-{suffix}")["UserPool"]["Id"]

    east_pool_ids = {
        pool["Id"]
        for page in east.get_paginator("list_user_pools").paginate(MaxResults=60)
        for pool in page["UserPools"]
    }
    west_pool_ids = {
        pool["Id"]
        for page in west.get_paginator("list_user_pools").paginate(MaxResults=60)
        for pool in page["UserPools"]
    }
    assert east_pool in east_pool_ids
    assert west_pool not in east_pool_ids
    assert west_pool in west_pool_ids
    assert east_pool not in west_pool_ids
    with pytest.raises(ClientError) as exc:
        east.describe_user_pool(UserPoolId=west_pool)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    for operation, kwargs in (
        (east.create_user_pool_client, {"UserPoolId": west_pool, "ClientName": "wrong-region"}),
        (east.admin_create_user, {"UserPoolId": west_pool, "Username": "wrong-region"}),
        (east.create_group, {"UserPoolId": west_pool, "GroupName": "wrong-region"}),
    ):
        with pytest.raises(ClientError) as exc:
            operation(**kwargs)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    domain = f"shared-{suffix}"
    east.create_user_pool_domain(UserPoolId=east_pool, Domain=domain)
    west.create_user_pool_domain(UserPoolId=west_pool, Domain=domain)
    assert east.describe_user_pool_domain(Domain=domain)["DomainDescription"]["UserPoolId"] == east_pool
    assert west.describe_user_pool_domain(Domain=domain)["DomainDescription"]["UserPoolId"] == west_pool

    second_east_pool = east.create_user_pool(PoolName=f"duplicate-{suffix}")["UserPool"]["Id"]
    with pytest.raises(ClientError) as exc:
        east.create_user_pool_domain(UserPoolId=second_east_pool, Domain=domain)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_cognito_unsigned_user_pool_operations_pin_the_owning_region():
    west = _regional_cognito_client("cognito-idp", "eu-central-1")
    suffix = _uuid_mod.uuid4().hex[:10]
    pool_id = west.create_user_pool(PoolName=f"unsigned-{suffix}")["UserPool"]["Id"]
    client_id = west.create_user_pool_client(
        UserPoolId=pool_id,
        ClientName=f"unsigned-{suffix}",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    west.admin_create_user(
        UserPoolId=pool_id,
        Username="challenge-user",
        TemporaryPassword="TempPass1!",
    )
    challenge = west.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "challenge-user", "PASSWORD": "TempPass1!"},
    )
    assert challenge["ChallengeName"] == "NEW_PASSWORD_REQUIRED"
    west.respond_to_auth_challenge(
        ClientId=client_id,
        ChallengeName="NEW_PASSWORD_REQUIRED",
        Session=challenge["Session"],
        ChallengeResponses={"USERNAME": "challenge-user", "NEW_PASSWORD": "ChallengePass1!"},
    )

    west.sign_up(
        ClientId=client_id,
        Username="public-user",
        Password="PublicPass1!",
        UserAttributes=[{"Name": "email", "Value": f"{suffix}@example.com"}],
    )
    west.resend_confirmation_code(ClientId=client_id, Username="public-user")
    west.confirm_sign_up(ClientId=client_id, Username="public-user", ConfirmationCode="123456")
    west.forgot_password(ClientId=client_id, Username="public-user")
    west.confirm_forgot_password(
        ClientId=client_id,
        Username="public-user",
        ConfirmationCode="654321",
        Password="ResetPass1!",
    )

    auth = west.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "public-user", "PASSWORD": "ResetPass1!"},
    )["AuthenticationResult"]
    access_token = auth["AccessToken"]
    refresh_token = auth["RefreshToken"]

    assert west.get_user(AccessToken=access_token)["Username"] == "public-user"
    west.update_user_attributes(
        AccessToken=access_token,
        UserAttributes=[{"Name": "name", "Value": "Regional User"}],
    )
    west.change_password(
        AccessToken=access_token,
        PreviousPassword="ResetPass1!",
        ProposedPassword="ChangedPass1!",
    )
    software_token = west.associate_software_token(AccessToken=access_token)
    assert software_token["SecretCode"]
    assert west.verify_software_token(
        AccessToken=access_token,
        UserCode="123456",
    )["Status"] == "SUCCESS"
    west.set_user_mfa_preference(
        AccessToken=access_token,
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )
    refreshed = west.get_tokens_from_refresh_token(
        ClientId=client_id,
        RefreshToken=refresh_token,
    )
    assert refreshed["AuthenticationResult"]["AccessToken"]
    west.revoke_token(Token=refresh_token, ClientId=client_id)
    west.global_sign_out(AccessToken=access_token)

    west.admin_create_user(UserPoolId=pool_id, Username="delete-user")
    west.admin_set_user_password(
        UserPoolId=pool_id,
        Username="delete-user",
        Password="DeletePass1!",
        Permanent=True,
    )
    delete_access_token = west.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "delete-user", "PASSWORD": "DeletePass1!"},
    )["AuthenticationResult"]["AccessToken"]
    west.delete_user(AccessToken=delete_access_token)
    with pytest.raises(ClientError) as exc:
        west.admin_get_user(UserPoolId=pool_id, Username="delete-user")
    assert exc.value.response["Error"]["Code"] == "UserNotFoundException"


def test_cognito_client_and_hosted_scans_preserve_account_boundaries():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _cognito_module()
    original_account = get_account_id()
    original_region = get_region()
    owner_account = "111111111111"
    foreign_account = "222222222222"
    pool_id = "us-west-2_accountscope"
    client_id = "account-scoped-client"

    try:
        mod.reset()
        set_request_account_id(owner_account)
        set_request_region("us-west-2")
        mod._user_pools[pool_id] = {"_clients": {client_id: {}}}

        set_request_account_id(foreign_account)
        set_request_region("us-east-1")
        assert mod._pool_scope_for_client(client_id) is None

        set_request_account_id(owner_account)
        assert mod._pool_scope_for_client(client_id) == (pool_id, "us-west-2")

        set_request_account_id(foreign_account)
        set_request_region("us-east-1")
        assert mod._get_pool_unscoped(pool_id) is mod._user_pools.get_scoped(
            owner_account, "us-west-2", pool_id
        )
        assert get_account_id() == owner_account
        assert get_region() == "us-west-2"

        set_request_account_id(foreign_account)
        set_request_region("us-east-1")
        found_pool_id, found_pool, found_client = mod._find_pool_by_client_id(
            client_id
        )
        assert found_pool_id == pool_id
        assert found_pool is mod._user_pools.get_scoped(
            owner_account, "us-west-2", pool_id
        )
        assert found_client == {}
        assert get_account_id() == owner_account
        assert get_region() == "us-west-2"
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


@pytest.mark.parametrize(
    ("action", "payload_kind"),
    (
        ("GetUser", "user_pool_id"),
        ("RespondToAuthChallenge", "session"),
        ("GetUser", "access_token"),
        ("GetTokensFromRefreshToken", "refresh_token"),
        ("RevokeToken", "token"),
        ("SignUp", "client_id"),
    ),
)
def test_cognito_unsigned_idp_operations_pin_each_payload_path(
    action, payload_kind
):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _cognito_module()
    original_account = get_account_id()
    original_region = get_region()
    owner_account = "111111111111"
    owner_region = "eu-central-1"
    pool_id = f"{owner_region}_unsignedpin"
    client_id = "unsigned-pin-client"
    session = "unsigned-pin-session"
    opaque_token = "unsigned-pin-opaque-token"
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    claims = base64.urlsafe_b64encode(
        json.dumps(
            {"iss": f"https://cognito-idp.{owner_region}.amazonaws.com/{pool_id}"}
        ).encode()
    ).rstrip(b"=").decode()
    access_token = f"{header}.{claims}.signature"
    payloads = {
        "user_pool_id": {"UserPoolId": pool_id},
        "session": {"Session": session},
        "access_token": {"AccessToken": access_token},
        "refresh_token": {"RefreshToken": opaque_token},
        "token": {"Token": opaque_token},
        "client_id": {"ClientId": client_id},
    }

    try:
        mod.reset()
        set_request_account_id(owner_account)
        set_request_region(owner_region)
        mod._user_pools[pool_id] = {"_clients": {client_id: {}}}
        mod._challenge_sessions[session] = {"pool_id": pool_id}
        mod._refresh_tokens[opaque_token] = {"pool_id": pool_id}

        set_request_region("us-east-1")
        assert mod._run_idp_handler(
            lambda _data: (get_account_id(), get_region()),
            action,
            payloads[payload_kind],
        ) == (owner_account, owner_region)
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_cognito_unsigned_identity_operations_pin_the_owning_region():
    west = _regional_cognito_client("cognito-identity", "us-west-2")
    east = _regional_cognito_client("cognito-identity", "us-east-1")
    pool_id = west.create_identity_pool(
        IdentityPoolName=f"regional-{_uuid_mod.uuid4().hex[:10]}",
        AllowUnauthenticatedIdentities=True,
    )["IdentityPoolId"]
    pool_arn = _identity_pool_arn(pool_id, region="us-west-2")
    west.tag_resource(ResourceArn=pool_arn, Tags={"scope": "west"})
    assert west.list_tags_for_resource(ResourceArn=pool_arn)["Tags"] == {
        "scope": "west"
    }
    with pytest.raises(ClientError) as exc:
        east.list_tags_for_resource(ResourceArn=pool_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    identity_id = west.get_id(
        AccountId="000000000000",
        IdentityPoolId=pool_id,
    )["IdentityId"]
    assert identity_id.startswith("us-west-2:")
    assert west.get_credentials_for_identity(IdentityId=identity_id)["Credentials"]["AccessKeyId"]
    assert west.get_open_id_token(IdentityId=identity_id)["Token"]
    west.unlink_identity(IdentityId=identity_id, Logins={}, LoginsToRemove=[])

    from ministack.core.responses import get_region, set_request_region

    mod = _cognito_module()
    original_region = get_region()
    try:
        for action, data in (
            ("GetId", {"IdentityPoolId": pool_id}),
            ("GetCredentialsForIdentity", {"IdentityId": identity_id}),
            ("GetOpenIdToken", {"IdentityId": identity_id}),
            ("UnlinkIdentity", {"IdentityId": identity_id}),
        ):
            set_request_region("us-east-1")
            assert mod._run_identity_handler(
                lambda _data: get_region(), action, data
            ) == "us-west-2"
    finally:
        set_request_region(original_region)

    assert pool_id not in {
        pool["IdentityPoolId"]
        for pool in east.list_identity_pools(MaxResults=60)["IdentityPools"]
    }
    with pytest.raises(ClientError) as exc:
        east.describe_identity_pool(IdentityPoolId=pool_id)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cognito_unsigned_token_pinning_rejects_non_object_claims():
    cognito_idp = _regional_cognito_client("cognito-idp", "us-east-1")

    def _token(payload):
        header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
        body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
        return f"{header}.{body}.signature"

    for payload in ([], {"iss": 123}):
        with pytest.raises(ClientError) as exc:
            cognito_idp.get_user(AccessToken=_token(payload))
        assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"

    for token in ([], {}, 123, True):
        request = urllib.request.Request(
            ENDPOINT,
            data=json.dumps({"AccessToken": token}).encode(),
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.GetUser",
            },
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(request, timeout=10)
        assert exc.value.code == 400
        assert json.loads(exc.value.read())["__type"].endswith(
            "NotAuthorizedException"
        )


def test_cognito_legacy_state_self_places_by_pool_ids():
    from ministack.core.responses import (
        AccountRegionScopedDict,
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _cognito_module()
    original_account = get_account_id()
    original_region = get_region()
    account = "111111111111"
    user_pool_id = "us-west-2_legacy123"
    identity_pool_id = "us-west-2:00000000-0000-4000-8000-000000000001"

    legacy_user_pools = AccountScopedDict()
    legacy_user_pools._data[(account, user_pool_id)] = {"Id": user_pool_id, "Name": "legacy"}
    legacy_domains = AccountScopedDict()
    legacy_domains._data[(account, "legacy-domain")] = user_pool_id
    legacy_identity_pools = AccountScopedDict()
    legacy_identity_pools._data[(account, identity_pool_id)] = {
        "IdentityPoolId": identity_pool_id,
        "IdentityPoolName": "legacy",
    }
    legacy_identity_tags = AccountScopedDict()
    legacy_identity_tags._data[(account, identity_pool_id)] = {"env": "legacy"}

    try:
        mod.reset()
        set_request_account_id(account)
        set_request_region("us-east-1")
        mod.restore_state({
            "user_pools": legacy_user_pools,
            "pool_domain_map": legacy_domains,
            "identity_pools": legacy_identity_pools,
            "identity_tags": legacy_identity_tags,
        })

        assert mod._user_pools.get_scoped(account, "us-west-2", user_pool_id)["Name"] == "legacy"
        assert mod._user_pools.get_scoped(account, "us-east-1", user_pool_id) is None
        assert mod._pool_domain_map.get_scoped(account, "us-west-2", "legacy-domain") == user_pool_id
        assert mod._identity_pools.get_scoped(account, "us-west-2", identity_pool_id)[
            "IdentityPoolName"
        ] == "legacy"
        assert mod._identity_tags.get_scoped(account, "us-west-2", identity_pool_id) == {
            "env": "legacy"
        }

        v3 = AccountRegionScopedDict()
        v3.set_scoped(account, "eu-central-1", "eu-central-1_v3", {"Name": "v3"})
        mod.reset()
        mod.restore_state({"user_pools": v3})
        assert mod._user_pools.get_scoped(account, "eu-central-1", "eu-central-1_v3") == {
            "Name": "v3"
        }
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_cognito_pool_arn_uses_pool_region():
    """`_pool_arn` must encode the pool's region, not the request region.

    A pool created in eu-central-1 and described from any other request context
    must still return an ARN whose region segment is `eu-central-1` — that is
    real-AWS behavior (the pool is a regional resource) and CloudFormation /
    cross-account-trust policies depend on a stable ARN.
    """
    mod = _cognito_module()
    arn = mod._pool_arn("eu-central-1_abcdef")
    assert ":cognito-idp:eu-central-1:" in arn, arn
    arn_gov = mod._pool_arn("us-gov-east-1_abcdef")
    assert ":cognito-idp:us-gov-east-1:" in arn_gov, arn_gov


def test_cognito_openid_discovery_issuer_uses_pool_region():
    """OIDC discovery's `issuer` must match the JWT `iss` — both derived from pool region.

    OIDC clients verify `iss == discovery.issuer`. If discovery returns the
    request-scope region but the JWT carries the pool-scope region, every
    standards-compliant validator rejects the token. Regression coverage for
    the same root-cause class as #678.
    """
    mod = _cognito_module()
    pool_id = "eu-central-1_abcdef123"
    # Even if the request scope says us-east-1, discovery must return eu-central-1
    # because that's what the JWT iss will encode.
    _status, _headers, body = mod.well_known_openid_configuration(pool_id, region="us-east-1", host="localhost:4566")
    doc = json.loads(body)
    assert doc["issuer"] == f"https://cognito-idp.eu-central-1.amazonaws.com/{pool_id}", doc["issuer"]


def test_cognito_user_pool_domain_cloudfront_uses_pool_region():
    """CreateUserPoolDomain / DescribeUserPoolDomain CloudFront URL must reflect the pool's region.

    Real AWS user pool domains are regional — the hosted-UI CloudFront URL is
    `{domain}.auth.{pool-region}.amazoncognito.com`. Using the request region
    would point clients at the wrong region's domain.
    """
    import boto3

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    region = "eu-central-1"
    client = boto3.client(
        "cognito-idp",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    )
    pid = client.create_user_pool(PoolName="DomainRegionPool")["UserPool"]["Id"]
    assert pid.startswith("eu-central-1_")

    # Pool ARN should encode eu-central-1.
    desc = client.describe_user_pool(UserPoolId=pid)
    assert ":cognito-idp:eu-central-1:" in desc["UserPool"]["Arn"], desc["UserPool"]["Arn"]

    domain = f"domain-region-{pid.split('_', 1)[1].lower()}"
    create_resp = client.create_user_pool_domain(Domain=domain, UserPoolId=pid)
    # A prefix domain has no CloudFront alias target — AWS returns a null
    # CloudFrontDomain (and an empty CloudFrontDistribution on describe).
    assert not create_resp.get("CloudFrontDomain"), create_resp.get("CloudFrontDomain")
    desc_resp = client.describe_user_pool_domain(Domain=domain)
    assert desc_resp["DomainDescription"]["CloudFrontDistribution"] == "", (
        desc_resp["DomainDescription"]["CloudFrontDistribution"]
    )


def test_cognito_email_disabled_env_skips_send(cognito_idp, monkeypatch):
    """COGNITO_EMAIL_ENABLED=false must short-circuit delivery in-process."""
    mod = _cognito_module()
    ses_mod = importlib.import_module("ministack.services.ses")
    monkeypatch.setenv("COGNITO_EMAIL_ENABLED", "false")
    before = len(ses_mod._sent_emails_list())

    pool_resp = mod._create_user_pool({"PoolName": "DisabledMailPool"})
    pid = json.loads(pool_resp[2])["UserPool"]["Id"]
    email = f"disabled-{_uuid_mod.uuid4().hex[:8]}@example.com"
    mod._admin_create_user({
        "UserPoolId": pid,
        "Username": "muted",
        "UserAttributes": [{"Name": "email", "Value": email}],
        "TemporaryPassword": "TempPw1!gg",
        "DesiredDeliveryMediums": ["EMAIL"],
    })

    assert len(ses_mod._sent_emails_list()) == before


# ---------------------------------------------------------------------------
# CUSTOM_AUTH flow (DefineAuthChallenge / CreateAuthChallenge /
# VerifyAuthChallenge). Folded from test_cognito_custom_auth.py.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_challenge_sessions():
    """Reset the TEST-PROCESS _challenge_sessions after each in-process unit test.

    This only clears the test process's module instance (used by the in-process
    unit tests below). It does NOT touch the server's session store; server-side
    sessions are keyed by random tokens, so they never collide across API tests.
    """
    import ministack.services.cognito as cognito_mod
    yield
    cognito_mod._challenge_sessions.clear()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_zip(handler_code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_code)
    return buf.getvalue()


def _setup_pool(cognito_idp, pool_name, lambda_config=None):
    """Create pool + client with ALLOW_CUSTOM_AUTH + enabled user. Returns (pool_id, client_id)."""
    kwargs = {"PoolName": pool_name}
    if lambda_config:
        kwargs["LambdaConfig"] = lambda_config
    pid = cognito_idp.create_user_pool(**kwargs)["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="user@example.com",
        MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="user@example.com", Password="Pass1234!", Permanent=True
    )
    return pid, cid


def _create_lambda(lam, fn_name, handler_code):
    """Deploy a Python Lambda function and return its ARN."""
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/service-role/lambda-role",
        Handler="index.handler",
        Code={"ZipFile": _make_zip(handler_code)},
    )
    return lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]


# ── Test 1: InitiateAuth CUSTOM_AUTH, no Lambda triggers configured ────────────

def test_custom_auth_initiate_no_trigger(cognito_idp):
    """When no CreateAuthChallenge Lambda is configured, return the default PROVIDE_AUTH_PARAMETERS challenge."""
    pid, cid = _setup_pool(cognito_idp, "NoTriggerPool")

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )

    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert "Session" in resp
    assert len(resp["Session"]) > 10  # non-empty, real-looking token
    assert resp["ChallengeParameters"].get("challenge") == "PROVIDE_AUTH_PARAMETERS"


# ── Test 2: InitiateAuth with CreateAuthChallenge Lambda ─────────────────────

def test_custom_auth_initiate_with_create_trigger(cognito_idp, lam):
    """CreateAuthChallenge Lambda is invoked; its publicChallengeParameters are returned."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK', 'emailIdentifier': 'A1'}\n"
        "    return event\n"
    )
    fn_arn = _create_lambda(lam, "create-auth-basic", create_handler)
    pid, cid = _setup_pool(cognito_idp, "CreateTriggerPool", {"CreateAuthChallenge": fn_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )

    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert resp["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    assert resp["ChallengeParameters"]["emailIdentifier"] == "A1"


# ── Test 3: InitiateAuth — user not found ────────────────────────────────────

def test_custom_auth_user_not_found(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="NfPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "notfound@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "UserNotFoundException"


# ── Test 4: InitiateAuth — user disabled ─────────────────────────────────────

def test_custom_auth_user_disabled(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "DisabledPool")
    cognito_idp.admin_disable_user(UserPoolId=pid, Username="user@example.com")

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"


# ── Test 5: InitiateAuth — client missing ALLOW_CUSTOM_AUTH ──────────────────

def test_custom_auth_client_missing_explicit_flow(cognito_idp):
    """Client without ALLOW_CUSTOM_AUTH in ExplicitAuthFlows is rejected."""
    pid = cognito_idp.create_user_pool(PoolName="WrongFlowPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],  # no ALLOW_CUSTOM_AUTH
    )["UserPoolClient"]["ClientId"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 6: RespondToAuthChallenge — missing Session ─────────────────────────

def test_custom_auth_respond_missing_session(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "MissingSessionPool")

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 7: RespondToAuthChallenge — invalid session token ───────────────────

def test_custom_auth_respond_invalid_session(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "InvalidSessionPool")

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session="invalid-session-token-1234567890",
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 8: Session expiry — in-process unit test ────────────────────────────
# Cannot be driven over HTTP: the test process can't fast-forward the server's
# session clock, and there's no API to do so. Test the helper directly.

def test_custom_auth_session_expiry_in_process():
    import ministack.services.cognito as cognito_mod

    token, session = cognito_mod._create_challenge_session(
        "us-east-1_pool", "client123", "user@example.com"
    )
    # Live session resolves cleanly.
    got, err = cognito_mod._get_challenge_session(token)
    assert got is session and err is None

    # Expire it, then confirm _get_challenge_session rejects and evicts it.
    session["expires_at"] = time.time() - 1  # in the past
    got, err = cognito_mod._get_challenge_session(token)
    assert got is None
    assert "expired" in err.lower()
    assert cognito_mod._challenge_sessions.get(token) is None


# ── Test 9: Full flow — correct answer, tokens issued ────────────────────────

def test_custom_auth_full_flow_issue_tokens(cognito_idp, lam):
    """InitiateAuth → RespondToAuthChallenge with correct answer → AuthenticationResult."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-full-flow", create_handler)
    verify_arn = _create_lambda(lam, "verify-full-flow", verify_handler)
    define_arn = _create_lambda(lam, "define-full-flow", define_handler)

    pid, cid = _setup_pool(cognito_idp, "FullFlowPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeName"] == "CUSTOM_CHALLENGE"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "SECRETCODE", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2
    result = step2["AuthenticationResult"]
    assert "AccessToken" in result
    assert "IdToken" in result
    assert "RefreshToken" in result


# ── Test 10: Wrong answer → failAuthentication, session cleared ───────────────

def test_custom_auth_wrong_answer_fail_auth(cognito_idp, lam):
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = False\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-fail-auth", create_handler)
    verify_arn = _create_lambda(lam, "verify-fail-auth", verify_handler)
    define_arn = _create_lambda(lam, "define-fail-auth", define_handler)

    pid, cid = _setup_pool(cognito_idp, "FailAuthPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "WRONGCODE", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"

    # Prove the session was cleared server-side: retrying with the same token
    # is now rejected as a non-existent session (verified via the API).
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "STILLWRONG", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 11: Multi-round — magic link then SMS OTP ────────────────────────────

def test_custom_auth_multi_round(cognito_idp, lam):
    """Three steps: InitiateAuth → Respond(magic link) → Respond(SMS OTP) → tokens."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    answered_count = len([c for c in event['request']['session'] if c.get('challengeResult')])\n"
        "    if answered_count == 0:\n"
        "        event['response']['publicChallengeParameters'] = {'round': '1', 'challenge': 'MAGIC_LINK'}\n"
        "    else:\n"
        "        event['response']['publicChallengeParameters'] = {'round': str(answered_count + 1), 'challenge': 'SMS_OTP'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    answered = [s for s in session if s.get('challengeResult') is not None]\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif len(answered) >= 2:\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-multi", create_handler)
    verify_arn = _create_lambda(lam, "verify-multi", verify_handler)
    define_arn = _create_lambda(lam, "define-multi", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MultiRoundPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-code", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"
    assert step2["Session"] == session  # SAME token — never re-generated

    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3


# ── Test 12: Lambda not found — session preserved for retry ──────────────────

def test_custom_auth_lambda_not_found_session_preserved(cognito_idp):
    # Only VerifyAuthChallengeResponse points at a non-existent Lambda.
    pid, cid = _setup_pool(cognito_idp, "LambdaNotFoundPool", {
        "VerifyAuthChallengeResponse": "arn:aws:lambda:us-east-1:000000000000:function:does-not-exist",
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session preserved — a retry with the same token reaches the trigger again
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidLambdaResponseException"


# ── Test 13: Lambda crashes — session preserved ───────────────────────────────

def test_custom_auth_lambda_crash_session_preserved(cognito_idp, lam):
    broken = "def handler(event, ctx):\n    raise RuntimeError('boom')\n"
    verify_arn = _create_lambda(lam, "verify-crash", broken)

    pid, cid = _setup_pool(cognito_idp, "CrashPool", {
        "VerifyAuthChallengeResponse": verify_arn,
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session preserved — retry reaches the crashing trigger again
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidLambdaResponseException"


# ── Test 14: AdminInitiateAuth CUSTOM_AUTH ────────────────────────────────────

def test_custom_auth_admin_initiate(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "AdminInitPool")

    resp = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert "Session" in resp


# ── Test 15: AdminRespondToAuthChallenge CUSTOM_CHALLENGE ─────────────────────

def test_custom_auth_admin_respond(cognito_idp, lam):
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-admin", verify_handler)
    define_arn = _create_lambda(lam, "define-admin", define_handler)

    pid, cid = _setup_pool(cognito_idp, "AdminRespondPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 16: ClientMetadata is top-level, propagated to Lambda ────────────────

def test_custom_auth_client_metadata_propagated(cognito_idp, lam):
    """ClientMetadata is a top-level InitiateAuth field, not inside AuthParameters."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    meta = event['request'].get('clientMetadata', {})\n"
        "    event['response']['publicChallengeParameters'] = {'signInMethod': meta.get('signInMethod', 'unknown')}\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-meta", create_handler)
    pid, cid = _setup_pool(cognito_idp, "MetaPool", {"CreateAuthChallenge": create_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
        ClientMetadata={"signInMethod": "MAGIC_LINK"},
    )
    assert resp["ChallengeParameters"]["signInMethod"] == "MAGIC_LINK"


# ── Test 17: Session persists across get_state/restore_state — in-process ────

def test_custom_auth_session_persistence():
    import ministack.services.cognito as cognito_mod

    token, _session = cognito_mod._create_challenge_session(
        "us-east-1_pool", "client123", "user@example.com"
    )
    assert cognito_mod._challenge_sessions.get(token) is not None

    # Save, clear, restore.
    state = cognito_mod.get_state()
    cognito_mod._challenge_sessions.clear()
    assert cognito_mod._challenge_sessions.get(token) is None
    cognito_mod.restore_state(state)
    assert cognito_mod._challenge_sessions.get(token) is not None


# ── Test 18: Concurrent sessions for same user ───────────────────────────────

def test_custom_auth_concurrent_sessions(cognito_idp, lam):
    """Two parallel auth flows for the same user get independent, usable tokens."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-concurrent", define_handler)
    verify_arn = _create_lambda(lam, "verify-concurrent", verify_handler)

    pid, cid = _setup_pool(cognito_idp, "ConcurrentPool", {
        "DefineAuthChallenge": define_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })

    s1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )["Session"]
    s2 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )["Session"]

    assert s1 != s2

    # Both tokens are live and independent — complete each to tokens
    r1 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid, ChallengeName="CUSTOM_CHALLENGE", Session=s1,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid, ChallengeName="CUSTOM_CHALLENGE", Session=s2,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    # Each session should issue tokens independently (DefineAuthChallenge sees a completed challenge)
    assert "AuthenticationResult" in r1
    assert "AuthenticationResult" in r2


# ── Test 19: LambdaConfig keys stored correctly ───────────────────────────────

def test_custom_auth_lambda_config_stored(cognito_idp):
    """Pool LambdaConfig stores DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse."""
    define_arn = "arn:aws:lambda:us-east-1:000000000000:function:define"
    create_arn = "arn:aws:lambda:us-east-1:000000000000:function:create"
    verify_arn = "arn:aws:lambda:us-east-1:000000000000:function:verify"

    pid = cognito_idp.create_user_pool(
        PoolName="LambdaConfigPool",
        LambdaConfig={
            "DefineAuthChallenge": define_arn,
            "CreateAuthChallenge": create_arn,
            "VerifyAuthChallengeResponse": verify_arn,
        },
    )["UserPool"]["Id"]

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    cfg = desc["LambdaConfig"]
    assert cfg["DefineAuthChallenge"] == define_arn
    assert cfg["CreateAuthChallenge"] == create_arn
    assert cfg["VerifyAuthChallengeResponse"] == verify_arn


# ── Test 20: DefineAuth unexpected response — session cleared ─────────────────

def test_custom_auth_define_unexpected_response_clears_session(cognito_idp, lam):
    """DefineAuthChallenge with all-false response and no challengeName clears session."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    else:\n"
        "        pass\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-unexpected", verify_handler)
    define_arn = _create_lambda(lam, "define-unexpected", define_handler)

    pid, cid = _setup_pool(cognito_idp, "UnexpectedPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session cleared
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 21: Empty ANSWER is forwarded to Lambda ────────────────────────────

def test_custom_auth_empty_answer_passed_to_lambda(cognito_idp, lam):
    """Empty ANSWER must not be rejected by the emulator — Lambda handles it."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    answer = event['request'].get('challengeAnswer', '')\n"
        "    event['response']['answerCorrect'] = len(answer) == 0\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-empty-answer", verify_handler)
    define_arn = _create_lambda(lam, "define-empty-answer", define_handler)

    pid, cid = _setup_pool(cognito_idp, "EmptyAnswerPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 22: DefineAuth issues tokens at InitiateAuth (zero-round bypass) ────

def test_custom_auth_define_issues_tokens_at_initiate(cognito_idp, lam):
    """DefineAuthChallenge returning issueTokens=True at InitiateAuth bypasses the challenge."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['issueTokens'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-bypass", define_handler)
    pid, cid = _setup_pool(cognito_idp, "BypassPool", {"DefineAuthChallenge": define_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    # Tokens issued directly from InitiateAuth
    assert "AuthenticationResult" in resp
    assert "AccessToken" in resp["AuthenticationResult"]


# ── Test 23: Session cleared after issueTokens=True ──────────────────────────

def test_custom_auth_session_cleared_after_tokens_issued(cognito_idp, lam):
    """Session is deleted after AuthenticationResult — verified via the API."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-cleanup", verify_handler)
    define_arn = _create_lambda(lam, "define-cleanup", define_handler)

    pid, cid = _setup_pool(cognito_idp, "CleanupPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session_token = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session_token,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2

    # Session cleaned up after tokens issued
    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session_token,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 24: ClientMetadata propagated to VerifyAuthChallenge ────────────────

def test_custom_auth_client_metadata_propagated_to_verify(cognito_idp, lam):
    """ClientMetadata passed in RespondToAuthChallenge reaches VerifyAuthChallenge Lambda."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    meta = event['request'].get('clientMetadata', {})\n"
        "    if meta.get('signInMethod') == 'MAGIC_LINK':\n"
        "        event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-meta-respond", verify_handler)
    define_arn = _create_lambda(lam, "define-meta-respond", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MetaRespondPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
        ClientMetadata={"signInMethod": "MAGIC_LINK"},
    )
    assert "AuthenticationResult" in step2




# ── Test 24b: CUSTOM_WITH_SRP — DefineAuth PASSWORD_VERIFIER is built-in ───────

def test_custom_auth_with_srp_returns_password_verifier_params(cognito_idp, lam):
    """Amplify CUSTOM_WITH_SRP: InitiateAuth+SRP_A yields PASSWORD_VERIFIER with SRP params."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if (len(session) == 1\n"
        "            and session[0].get('challengeName') == 'SRP_A'\n"
        "            and session[0].get('challengeResult')):\n"
        "        event['response']['challengeName'] = 'PASSWORD_VERIFIER'\n"
        "    elif (len(session) >= 2\n"
        "            and session[1].get('challengeName') == 'PASSWORD_VERIFIER'\n"
        "            and session[1].get('challengeResult')):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-custom-srp-init", define_handler)
    pid, cid = _setup_pool(cognito_idp, "CustomSrpInitPool", {
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={
            "USERNAME": "user@example.com",
            "CHALLENGE_NAME": "SRP_A",
            "SRP_A": "ab" * 128,
        },
        ClientMetadata={"deviceKeys": "[]"},
    )
    assert step1["ChallengeName"] == "PASSWORD_VERIFIER"
    params = step1["ChallengeParameters"]
    assert params.get("USER_ID_FOR_SRP") == "user@example.com"
    assert "SALT" in params and params["SALT"]
    assert "SRP_B" in params and params["SRP_B"]
    assert "SECRET_BLOCK" in params and params["SECRET_BLOCK"]


def test_custom_auth_with_srp_full_flow_issues_tokens(cognito_idp, lam):
    """CUSTOM_AUTH SRP_A → PASSWORD_VERIFIER respond → tokens (no CreateAuth in between)."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if (len(session) == 1\n"
        "            and session[0].get('challengeName') == 'SRP_A'\n"
        "            and session[0].get('challengeResult')):\n"
        "        event['response']['challengeName'] = 'PASSWORD_VERIFIER'\n"
        "    elif (len(session) >= 2\n"
        "            and session[1].get('challengeName') == 'PASSWORD_VERIFIER'\n"
        "            and session[1].get('challengeResult')):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-custom-srp-full", define_handler)
    pid, cid = _setup_pool(cognito_idp, "CustomSrpFullPool", {
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={
            "USERNAME": "user@example.com",
            "CHALLENGE_NAME": "SRP_A",
            "SRP_A": "ab" * 128,
        },
    )
    assert step1["ChallengeName"] == "PASSWORD_VERIFIER"
    secret = step1["ChallengeParameters"]["SECRET_BLOCK"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="PASSWORD_VERIFIER",
        Session=step1["Session"],
        ChallengeResponses={
            "USERNAME": "user@example.com",
            "PASSWORD_CLAIM_SECRET_BLOCK": secret,
            "PASSWORD_CLAIM_SIGNATURE": "deadbeef",
            "TIMESTAMP": "Tue Jul 28 17:40:00 UTC 2026",
        },
    )
    assert "AuthenticationResult" in step2
    assert "AccessToken" in step2["AuthenticationResult"]


def test_custom_auth_define_receives_client_metadata(cognito_idp, lam):
    """ClientMetadata on RespondToAuthChallenge is passed into DefineAuthChallenge."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'OTP'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    meta = event['request'].get('clientMetadata') or {}\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult') and meta.get('customChallengeName') == 'OTP_MFA':\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-define-meta", create_handler)
    verify_arn = _create_lambda(lam, "verify-define-meta", verify_handler)
    define_arn = _create_lambda(lam, "define-define-meta", define_handler)

    pid, cid = _setup_pool(cognito_idp, "DefineMetaPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
        ClientMetadata={"customChallengeName": "OTP_MFA"},
    )
    assert "AuthenticationResult" in step2


# ── Test 25: UpdateUserPool stores CUSTOM_AUTH LambdaConfig keys ──────────────

def test_custom_auth_lambda_config_stored_via_update(cognito_idp):
    """UpdateUserPool also stores DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse."""
    pid = cognito_idp.create_user_pool(PoolName="UpdateLambdaPool")["UserPool"]["Id"]

    define_arn = "arn:aws:lambda:us-east-1:000000000000:function:define-update"
    create_arn = "arn:aws:lambda:us-east-1:000000000000:function:create-update"
    verify_arn = "arn:aws:lambda:us-east-1:000000000000:function:verify-update"

    cognito_idp.update_user_pool(
        UserPoolId=pid,
        LambdaConfig={
            "DefineAuthChallenge": define_arn,
            "CreateAuthChallenge": create_arn,
            "VerifyAuthChallengeResponse": verify_arn,
        },
    )

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    cfg = desc["LambdaConfig"]
    assert cfg["DefineAuthChallenge"] == define_arn
    assert cfg["CreateAuthChallenge"] == create_arn
    assert cfg["VerifyAuthChallengeResponse"] == verify_arn


# ── Test 26: User with empty Attributes list ─────────────────────────────────

def test_custom_auth_user_no_attributes(cognito_idp):
    """User created with no UserAttributes still completes CUSTOM_AUTH initiate."""
    pid = cognito_idp.create_user_pool(PoolName="NoAttrsPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="bare@example.com",
        MessageAction="SUPPRESS",
    )

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "bare@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"


# ── Test 27: DefineAuthChallenge present, CreateAuthChallenge absent ──────────

def test_custom_auth_define_present_create_absent(cognito_idp, lam):
    """DefineAuthChallenge configured but no CreateAuthChallenge — default challenge used."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-no-create", define_handler)
    pid, cid = _setup_pool(cognito_idp, "DefineOnlyPool", {"DefineAuthChallenge": define_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert resp["ChallengeParameters"].get("challenge") == "PROVIDE_AUTH_PARAMETERS"


# ── Test 28: Session list grows correctly across rounds ───────────────────────

def test_custom_auth_session_list_grows_across_rounds(cognito_idp, lam):
    """Each round appends one entry to session['challenges']; DefineAuth sees the correct history.
    The first CreateAuthChallenge sees an empty session (AWS parity), so round == "0"."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK', 'round': str(len(session))}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-session-growth", define_handler)
    create_arn = _create_lambda(lam, "create-session-growth", create_handler)
    verify_arn = _create_lambda(lam, "verify-session-growth", verify_handler)

    pid, cid = _setup_pool(cognito_idp, "SessionGrowthPool", {
        "DefineAuthChallenge": define_arn,
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["round"] == "0"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 29: Max challenge attempts exceeded ─────────────────────────────────

def test_custom_auth_max_attempts_exceeded(cognito_idp, lam):
    """Exceeded max attempts terminates with NotAuthorizedException."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'TEST'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = False\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-max-attempts", create_handler)
    verify_arn = _create_lambda(lam, "verify-max-attempts", verify_handler)
    define_arn = _create_lambda(lam, "define-max-attempts", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MaxAttemptsPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    # Keep answering until max attempts exceeded
    last_exc = None
    for i in range(5):
        try:
            cognito_idp.respond_to_auth_challenge(
                ClientId=cid,
                ChallengeName="CUSTOM_CHALLENGE",
                Session=session,
                ChallengeResponses={"ANSWER": f"attempt{i}", "USERNAME": "user@example.com"},
            )
        except ClientError as e:
            last_exc = e
            if e.response["Error"]["Code"] == "NotAuthorizedException":
                break

    assert last_exc is not None
    assert last_exc.response["Error"]["Code"] == "NotAuthorizedException"


# ── Test: issueTokens on cap-boundary attempt must win over MaxAttempts ──────

def test_custom_auth_issue_tokens_on_third_attempt_boundary(cognito_idp, lam):
    """A correct answer on attempt N == MAX_CHALLENGE_ATTEMPTS must issue
    tokens, not be rejected for hitting the cap.

    Regression for the order-of-checks bug: the cap is meant to prevent a
    NEXT (4th) round, not penalize success on the boundary. Define returns
    issueTokens=True only after the 3rd answer; the prior buggy ordering
    rejected with `Max authentication attempts exceeded` before reaching the
    issueTokens branch.
    """
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'TEST'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    answered = sum(1 for c in event['request']['session']"
        " if c.get('challengeResult') is not None)\n"
        # Issue tokens exactly on the 3rd answered attempt (cap boundary).
        "    if answered >= 3:\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-boundary", create_handler)
    verify_arn = _create_lambda(lam, "verify-boundary", verify_handler)
    define_arn = _create_lambda(lam, "define-boundary", define_handler)

    pid, cid = _setup_pool(cognito_idp, "BoundaryPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step["Session"]
    # Answer 3 times — the 3rd must issue tokens, not hit the cap.
    last = None
    for i in range(3):
        last = cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": f"attempt{i}", "USERNAME": "user@example.com"},
        )
        session = last.get("Session", session)
    assert "AuthenticationResult" in last, last
    assert last["AuthenticationResult"].get("AccessToken")


# ── Test 30: Issue #725 reproduction ─────────────────────────────────────────

def test_custom_auth_issue_725_repro(cognito_idp, lam):
    """Exact reproduction from ministackorg/ministack#725 — exercises session[] and private-param carry-through."""
    define = (
        "def handler(event, ctx):\n"
        "    s = event['request']['session']\n"
        "    if not s:\n"
        "        event['response'].update(challengeName='CUSTOM_CHALLENGE', issueTokens=False, failAuthentication=False)\n"
        "    elif s[-1].get('challengeResult'):\n"
        "        event['response'].update(issueTokens=True, failAuthentication=False)\n"
        "    else:\n"
        "        event['response'].update(issueTokens=False, failAuthentication=True)\n"
        "    return event\n"
    )
    create = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'type': 'MAGIC_LINK'}\n"
        "    event['response']['privateChallengeParameters'] = {'answer': 'expected-token'}\n"
        "    event['response']['challengeMetadata'] = 'MAGIC_LINK'\n"
        "    return event\n"
    )
    verify = (
        "def handler(event, ctx):\n"
        "    expected = event['request']['privateChallengeParameters']['answer']\n"
        "    event['response']['answerCorrect'] = (event['request']['challengeAnswer'] == expected)\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-725", define)
    create_arn = _create_lambda(lam, "create-725", create)
    verify_arn = _create_lambda(lam, "verify-725", verify)

    pid = cognito_idp.create_user_pool(PoolName="repro-725", LambdaConfig={
        "DefineAuthChallenge": define_arn,
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="c",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="alice", MessageAction="SUPPRESS")

    r1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "alice"},
    )
    assert r1["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert r1["ChallengeParameters"]["type"] == "MAGIC_LINK"

    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=r1["Session"],
        ChallengeResponses={"USERNAME": "alice", "ANSWER": "expected-token"},
    )
    assert "AccessToken" in r2["AuthenticationResult"]


def test_custom_auth_issue_725_private_params_carry_through(cognito_idp, lam):
    """Verify that privateChallengeParameters round-trip from CreateAuthChallenge to VerifyAuthChallenge."""
    create = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'msg': 'send this to user'}\n"
        "    event['response']['privateChallengeParameters'] = {'secret': 'only-server-knows'}\n"
        "    return event\n"
    )
    verify = (
        "def handler(event, ctx):\n"
        "    # Verify handler can read privateChallengeParameters set by create\n"
        "    secret = event['request']['privateChallengeParameters'].get('secret', '')\n"
        "    answer = event['request']['challengeAnswer']\n"
        "    event['response']['answerCorrect'] = (secret == answer)\n"
        "    return event\n"
    )
    define = (
        "def handler(event, ctx):\n"
        "    if event['request']['session'] and event['request']['session'][-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-private-params", create)
    verify_arn = _create_lambda(lam, "verify-private-params", verify)
    define_arn = _create_lambda(lam, "define-private-params", define)

    pid, cid = _setup_pool(cognito_idp, "PrivateParamsPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    r1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert r1["ChallengeParameters"]["msg"] == "send this to user"

    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=r1["Session"],
        ChallengeResponses={"ANSWER": "only-server-knows", "USERNAME": "user@example.com"},
    )
    assert "AccessToken" in r2["AuthenticationResult"]


# ── Regression: verify result is merged into the round it belongs to ──────────
#
# AWS records ONE ChallengeResult per CUSTOM_AUTH round, carrying BOTH the
# challengeMetadata (set by CreateAuthChallenge) AND the challengeResult (from
# VerifyAuthChallengeResponse). The triggers below model a real consumer
# (magic-link -> SMS-OTP) that identifies the completed step by reading BOTH
# fields from the SAME session element. With the prior split-entry behaviour the
# verify result landed in a second, metadata-less record, so the magic-link
# round was never recognised as complete and the SMS-OTP step never ran.

# CreateAuthChallenge: round 1 = MAGIC_LINK, round 2+ = SMS_OTP, with the step
# name carried in challengeMetadata (not just publicChallengeParameters).
_MERGE_CREATE_HANDLER = (
    "def handler(event, ctx):\n"
    "    answered = [c for c in event['request']['session']"
    " if c.get('challengeResult') is not None]\n"
    "    step = 'MAGIC_LINK' if len(answered) == 0 else 'SMS_OTP'\n"
    "    event['response']['publicChallengeParameters'] = {'challenge': step}\n"
    "    event['response']['challengeMetadata'] = step\n"
    "    return event\n"
)
_MERGE_VERIFY_HANDLER = (
    "def handler(event, ctx):\n"
    "    event['response']['answerCorrect'] = True\n"
    "    return event\n"
)
# DefineAuthChallenge advances ONLY when a round is a single merged entry:
# challengeResult truthy AND the expected challengeMetadata on the SAME element.
# Under the split-entry bug the MAGIC_LINK round is never 'done', so this falls
# through to failAuthentication and never reaches the SMS_OTP step.
_MERGE_DEFINE_HANDLER = (
    "def handler(event, ctx):\n"
    "    session = event['request']['session']\n"
    "    def done(meta):\n"
    "        return any(c.get('challengeResult') and c.get('challengeMetadata') == meta\n"
    "                   for c in session)\n"
    "    if not session:\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    elif done('MAGIC_LINK') and done('SMS_OTP'):\n"
    "        event['response']['issueTokens'] = True\n"
    "    elif done('MAGIC_LINK'):\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    else:\n"
    "        event['response']['failAuthentication'] = True\n"
    "    return event\n"
)


def test_custom_auth_merged_result_metadata_advances_steps(cognito_idp, lam):
    """A round's metadata and result live on one session entry, so a multi-step
    magic-link -> SMS-OTP flow completes (RespondToAuthChallenge path)."""
    create_arn = _create_lambda(lam, "create-merge", _MERGE_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-merge", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-merge", _MERGE_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "MergeResultPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    # Round 1 (magic link). On the buggy split-entry shape this raises
    # NotAuthorizedException because the round is never recognised as complete.
    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    # Round 2 (SMS OTP) -> tokens.
    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3
    assert step3["AuthenticationResult"].get("IdToken")


def test_custom_auth_admin_merged_result_metadata_advances_steps(cognito_idp, lam):
    """Same merged-round contract on the AdminRespondToAuthChallenge path."""
    create_arn = _create_lambda(lam, "create-merge-admin", _MERGE_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-merge-admin", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-merge-admin", _MERGE_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "MergeResultAdminPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3
    assert step3["AuthenticationResult"].get("IdToken")


def test_update_pending_challenge_result_merge_and_fallback():
    """Unit-pin _update_pending_challenge_result's full contract: merge the
    result into the pending round in place (True or False, without growing the
    history), and fall back to appending only when there is no pending round."""
    import ministack.services.cognito as cognito_mod

    def _pending(metadata):
        return {
            "challengeName": "CUSTOM_CHALLENGE",
            "challengeResult": None,
            "challengeMetadata": metadata,
            "publicChallengeParameters": {},
            "privateChallengeParameters": {},
            "timestamp": 0,
        }

    # Correct answer merges into the pending round — no new entry, metadata kept.
    session = {"challenges": [_pending("MAGIC_LINK")], "last_challenge_metadata": "MAGIC_LINK"}
    cognito_mod._update_pending_challenge_result(session, True)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] == "MAGIC_LINK"

    # Wrong answer is recorded in place as False — not None, not dropped.
    session = {"challenges": [_pending("SMS_OTP")], "last_challenge_metadata": "SMS_OTP"}
    cognito_mod._update_pending_challenge_result(session, False)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeResult"] is False

    # No pending round (empty history) — fall back to appending one entry.
    session = {"challenges": [], "last_challenge_metadata": None}
    cognito_mod._update_pending_challenge_result(session, True)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeName"] == "CUSTOM_CHALLENGE"
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] is None

    # Last round already resolved — append a new entry, leave the prior intact.
    resolved = dict(_pending("MAGIC_LINK"), challengeResult=True)
    session = {"challenges": [resolved], "last_challenge_metadata": "MAGIC_LINK"}
    cognito_mod._update_pending_challenge_result(session, False)
    assert len(session["challenges"]) == 2
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] == "MAGIC_LINK"
    assert session["challenges"][1]["challengeResult"] is False


# ── Regression: CreateAuthChallenge sees the AWS-faithful session length ───────
#
# AWS passes an EMPTY session array to the FIRST CreateAuthChallenge (the round
# being created is not itself a session entry), then only COMPLETED rounds on
# later invocations. AWS's own trigger examples branch on session.length, so the
# observed length must match. The handler echoes the raw len it sees each round.
_LEN_CREATE_HANDLER = (
    "def handler(event, ctx):\n"
    "    n = len(event['request']['session'])\n"
    "    step = 'MAGIC_LINK' if n == 0 else 'SMS_OTP'\n"
    "    event['response']['publicChallengeParameters'] = {'challenge': step, 'rawlen': str(n)}\n"
    "    return event\n"
)
_LEN_DEFINE_HANDLER = (
    "def handler(event, ctx):\n"
    "    answered = [c for c in event['request']['session'] if c.get('challengeResult') is not None]\n"
    "    if len(answered) >= 2:\n"
    "        event['response']['issueTokens'] = True\n"
    "    else:\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    return event\n"
)


def test_custom_auth_first_create_receives_empty_session(cognito_idp, lam):
    """Round-1 CreateAuthChallenge sees an empty session (len 0); round-2 sees
    one completed round (len 1) — RespondToAuthChallenge path."""
    create_arn = _create_lambda(lam, "create-len", _LEN_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-len", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-len", _LEN_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "SessionLenPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["rawlen"] == "0"
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2["ChallengeParameters"]["rawlen"] == "1"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3


def test_custom_auth_admin_first_create_receives_empty_session(cognito_idp, lam):
    """Same empty-first-session contract on the AdminInitiateAuth path."""
    create_arn = _create_lambda(lam, "create-len-admin", _LEN_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-len-admin", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-len-admin", _LEN_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "SessionLenAdminPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["rawlen"] == "0"
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2["ChallengeParameters"]["rawlen"] == "1"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3


# ===========================================================================
# CUSTOM USER POOL DOMAINS
#
# The SAML ACS URL and the OIDC federation callback are built from the pool's
# configured domain, the way AWS does it — not from an env var and not from the
# request's Host header. A custom domain is `https://` because TLS for it is
# terminated externally (nginx, an ALB, CloudFront) and proxied to the gateway;
# MiniStack records the certificate but never serves it.
# ===========================================================================

_CUSTOM_CERT_ARN = (
    "arn:aws:acm:us-east-1:000000000000:certificate/"
    "11111111-2222-3333-4444-555555555555"
)


def _pool_unique_domain(pid, label="auth"):
    """A domain unique to this pool — _pool_domain_map outlives a single test."""
    return f"{label}.{pid.split('_', 1)[1].lower()}.example.com"


def test_cognito_custom_domain_create_and_describe(cognito_idp):
    """CustomDomainConfig marks the domain custom: served as-is, fronted by CloudFront."""
    pid = cognito_idp.create_user_pool(PoolName="CustomDomainPool")["UserPool"]["Id"]
    domain = _pool_unique_domain(pid)
    resp = cognito_idp.create_user_pool_domain(
        UserPoolId=pid, Domain=domain,
        CustomDomainConfig={"CertificateArn": _CUSTOM_CERT_ARN},
    )
    # A custom domain gets a CloudFront distribution, never the regional prefix host.
    assert resp["CloudFrontDomain"].endswith(".cloudfront.net"), resp["CloudFrontDomain"]
    assert ".amazoncognito.com" not in resp["CloudFrontDomain"]

    dd = cognito_idp.describe_user_pool_domain(Domain=domain)["DomainDescription"]
    assert dd["UserPoolId"] == pid
    assert dd["Status"] == "ACTIVE"
    assert dd["CustomDomainConfig"] == {"CertificateArn": _CUSTOM_CERT_ARN}
    assert dd["CloudFrontDistribution"] == resp["CloudFrontDomain"]

    # AWS surfaces a custom domain as `CustomDomain`; `Domain` stays the prefix slot.
    pool = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert pool.get("CustomDomain") == domain
    assert not pool.get("Domain")


def test_cognito_prefix_domain_unchanged_by_custom_domain_support(cognito_idp):
    """Regression: no CustomDomainConfig still means a prefix domain that expands."""
    pid = cognito_idp.create_user_pool(PoolName="PrefixDomainPool")["UserPool"]["Id"]
    domain = f"prefix-{pid.split('_', 1)[1].lower()}"
    resp = cognito_idp.create_user_pool_domain(UserPoolId=pid, Domain=domain)
    # Prefix domain: AWS returns a null CloudFrontDomain (no alias target).
    assert not resp.get("CloudFrontDomain")

    dd = cognito_idp.describe_user_pool_domain(Domain=domain)["DomainDescription"]
    assert dd["CloudFrontDistribution"] == ""
    assert dd["CustomDomainConfig"] == {}

    pool = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert pool.get("Domain") == domain
    assert not pool.get("CustomDomain")


def test_cognito_custom_domain_requires_certificate_arn(cognito_idp):
    """A CustomDomainConfig carrying no CertificateArn is rejected, as on AWS.

    Sent raw: botocore enforces the member's 20-char minimum client-side, so this
    only reaches the server from a hand-rolled caller or another SDK. Rejecting it
    keeps a malformed body from being stored as a usable custom domain.
    """
    pid = cognito_idp.create_user_pool(PoolName="NoCertPool")["UserPool"]["Id"]
    for bad_config in ({}, {"CertificateArn": ""}, "not-a-struct"):
        request = urllib.request.Request(
            ENDPOINT,
            data=json.dumps({
                "UserPoolId": pid,
                "Domain": _pool_unique_domain(pid, "nocert"),
                "CustomDomainConfig": bad_config,
            }).encode(),
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.CreateUserPoolDomain",
            },
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(request, timeout=10)
        assert exc.value.code == 400
        assert json.loads(exc.value.read())["__type"].endswith("InvalidParameterException")

    # Nothing was registered, so the domain is still free.
    assert cognito_idp.describe_user_pool_domain(
        Domain=_pool_unique_domain(pid, "nocert"))["DomainDescription"] == {}


def test_cognito_pool_holds_prefix_and_custom_domain_independently(cognito_idp):
    """A pool may have both; deleting one must not clear the other."""
    pid = cognito_idp.create_user_pool(PoolName="BothDomainsPool")["UserPool"]["Id"]
    prefix = f"both-{pid.split('_', 1)[1].lower()}"
    custom = _pool_unique_domain(pid, "both")
    cognito_idp.create_user_pool_domain(UserPoolId=pid, Domain=prefix)
    cognito_idp.create_user_pool_domain(
        UserPoolId=pid, Domain=custom,
        CustomDomainConfig={"CertificateArn": _CUSTOM_CERT_ARN},
    )

    cognito_idp.delete_user_pool_domain(UserPoolId=pid, Domain=custom)
    pool = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert not pool.get("CustomDomain")
    assert pool.get("Domain") == prefix, "deleting the custom domain dropped the prefix one"
    assert cognito_idp.describe_user_pool_domain(Domain=custom)["DomainDescription"] == {}
    assert cognito_idp.describe_user_pool_domain(
        Domain=prefix)["DomainDescription"]["UserPoolId"] == pid


def test_cognito_federation_urls_follow_pool_domain():
    """The ACS / OIDC-callback base is the pool's domain, with the gateway as fallback."""
    mod = _cognito_module()

    # No domain configured — unchanged behaviour, the local gateway.
    bare = {"Id": "us-east-1_abc123"}
    assert mod._acs_url(bare) == f"{_advertised_endpoint()}/saml2/idpresponse"
    assert mod._oidc_callback_url(bare) == f"{_advertised_endpoint()}/oauth2/idpresponse"

    # Prefix domain — expands to the regional host, region read from the pool id.
    prefix = {"Id": "eu-central-1_abc123", "Domain": "my-prefix"}
    assert mod._oidc_callback_url(prefix) == (
        "https://my-prefix.auth.eu-central-1.amazoncognito.com/oauth2/idpresponse")

    # Custom domain wins over the prefix, and carries no port.
    custom = {"Id": "us-east-1_abc123", "Domain": "my-prefix",
              "CustomDomain": "auth.example.com"}
    assert mod._acs_url(custom) == "https://auth.example.com/saml2/idpresponse"
    assert mod._oidc_callback_url(custom) == "https://auth.example.com/oauth2/idpresponse"


def test_cognito_oidc_authorize_redirect_uri_uses_custom_domain(cognito_idp):
    """The redirect_uri handed to an external IdP is the pool's custom domain.

    This is the flow that fails behind an external TLS terminator when the URL is
    built from the internal host: a public IdP rejects `http://localhost:4566/...`.
    """
    token_url, _recorded, stop = _start_fake_oidc_idp({"sub": "ignored"})
    try:
        pid, cid = _setup_oidc_pool(cognito_idp, token_url)
        domain = _pool_unique_domain(pid, "oidc")
        cognito_idp.create_user_pool_domain(
            UserPoolId=pid, Domain=domain,
            CustomDomainConfig={"CertificateArn": _CUSTOM_CERT_ARN},
        )
        url = (
            f"{ENDPOINT}/oauth2/authorize?"
            f"response_type=code&client_id={cid}"
            f"&redirect_uri=http://localhost:3000/callback"
            f"&identity_provider=TestOIDC&state=mystate&scope=openid"
        )
        try:
            _no_redirect_opener.open(url)
            assert False, "Expected 302"
        except urllib.error.HTTPError as e:
            assert e.code == 302
            location = e.headers.get("Location", "")
        qs = _parse_qs(urlparse(location).query)
        assert qs["redirect_uri"] == [f"https://{domain}/oauth2/idpresponse"]
    finally:
        stop()


def test_cognito_saml_authn_request_acs_uses_custom_domain(cognito_idp):
    """The AuthnRequest's AssertionConsumerServiceURL follows the custom domain."""
    pid, cid = _setup_saml_pool(cognito_idp)
    domain = _pool_unique_domain(pid, "saml")
    cognito_idp.create_user_pool_domain(
        UserPoolId=pid, Domain=domain,
        CustomDomainConfig={"CertificateArn": _CUSTOM_CERT_ARN},
    )
    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=mystate&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        assert False, "Expected 302"
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location", "")
    saml_request = _parse_qs(urlparse(location).query)["SAMLRequest"][0]
    # Raw deflate per the SAML HTTP-Redirect binding.
    xml = zlib.decompress(base64.b64decode(saml_request), -15).decode()
    assert f'AssertionConsumerServiceURL="https://{domain}/saml2/idpresponse"' in xml
    assert f'urn:amazon:cognito:sp:{pid}' in xml


# ---------------------------------------------------------------------------
# Linking federated users to an existing profile (#1499)
# ---------------------------------------------------------------------------

def _saml_sign_in(cid, name_id, attributes):
    """Drive /oauth2/authorize -> /saml2/idpresponse -> /oauth2/token."""
    url = (
        f"{ENDPOINT}/oauth2/authorize?"
        f"response_type=code&client_id={cid}"
        f"&redirect_uri=http://localhost:3000/callback"
        f"&identity_provider=TestSAML&state=mystate&scope=openid"
    )
    try:
        _no_redirect_opener.open(url)
        raise AssertionError("Expected redirect")
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location", "")
    relay_state = _parse_qs(urlparse(location).query).get("RelayState", [""])[0]

    form = _urlencode({
        "SAMLResponse": _build_mock_saml_response(name_id, attributes),
        "RelayState": relay_state,
    }).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/saml2/idpresponse", data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        _no_redirect_opener.open(req)
        raise AssertionError("Expected redirect")
    except urllib.error.HTTPError as e:
        callback = e.headers.get("Location", "")
    code = _parse_qs(urlparse(callback).query).get("code", [""])[0]

    token_req = urllib.request.Request(
        f"{ENDPOINT}/oauth2/token",
        data=(f"grant_type=authorization_code&code={code}&client_id={cid}"
              f"&redirect_uri=http://localhost:3000/callback").encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(token_req) as resp:
        tokens = json.loads(resp.read())
    payload = tokens["id_token"].split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def test_cognito_admin_link_provider_writes_identities_attribute(cognito_idp):
    pid, _cid = _setup_saml_pool(cognito_idp)
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="carlos", MessageAction="SUPPRESS",
        UserAttributes=[{"Name": "email", "Value": "msp_carlos@example.com"}],
    )

    assert cognito_idp.admin_link_provider_for_user(
        UserPoolId=pid,
        DestinationUser={"ProviderName": "Cognito", "ProviderAttributeValue": "carlos"},
        SourceUser={"ProviderName": "TestSAML", "ProviderAttributeName": "email",
                    "ProviderAttributeValue": "msp_carlos@example.com"},
    )["ResponseMetadata"]["HTTPStatusCode"] == 200

    attrs = {a["Name"]: a["Value"] for a in cognito_idp.admin_get_user(
        UserPoolId=pid, Username="carlos")["UserAttributes"]}
    identities = json.loads(attrs["identities"])
    assert len(identities) == 1
    identity = identities[0]
    assert identity["userId"] == "msp_carlos@example.com"
    assert identity["providerName"] == "TestSAML"
    assert identity["providerType"] == "SAML"
    # A linked identity is never the profile's primary one.
    assert identity["primary"] is False
    assert isinstance(identity["dateCreated"], int)
    assert set(identity) == {
        "userId", "providerName", "providerType", "issuer", "primary",
        "dateCreated",
    }


def test_cognito_linked_federated_sign_in_resolves_to_the_local_profile(
    cognito_idp,
):
    """A linked user signs in to the profile they were linked to, not a new one."""
    pid, cid = _setup_saml_pool(cognito_idp)
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="carlos", MessageAction="SUPPRESS",
        UserAttributes=[{"Name": "email", "Value": "msp_carlos@example.com"}],
    )
    native_sub = {a["Name"]: a["Value"] for a in cognito_idp.admin_get_user(
        UserPoolId=pid, Username="carlos")["UserAttributes"]}["sub"]

    cognito_idp.admin_link_provider_for_user(
        UserPoolId=pid,
        DestinationUser={"ProviderName": "Cognito", "ProviderAttributeValue": "carlos"},
        SourceUser={"ProviderName": "TestSAML",
                    "ProviderAttributeName": "Cognito_Subject",
                    "ProviderAttributeValue": "msp_carlos@example.com"},
    )

    claims = _saml_sign_in(cid, "msp_carlos@example.com", {
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress":
            "msp_carlos@example.com",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name": "Carlos",
    })

    # "After Amazon Cognito matches your federated user to a linked profile,
    # they always sign in to that profile."
    assert claims["sub"] == native_sub
    assert claims["cognito:username"] == "carlos"
    # "Your user's ID token contains all of their associated providers in the
    # identities claim."
    assert isinstance(claims["identities"], list)
    assert claims["identities"][0]["providerName"] == "TestSAML"

    # No second `{Provider}_id` profile was minted.
    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_get_user(
            UserPoolId=pid, Username="TestSAML_msp_carlos@example.com")
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"

    # "Your user pool then updates the linked local profile with the claims
    # mapped from their sign-in."
    attrs = {a["Name"]: a["Value"] for a in cognito_idp.admin_get_user(
        UserPoolId=pid, Username="carlos")["UserAttributes"]}
    assert attrs["name"] == "Carlos"


def test_cognito_unlinked_federated_sign_in_still_creates_its_own_profile(
    cognito_idp,
):
    pid, cid = _setup_saml_pool(cognito_idp)
    claims = _saml_sign_in(cid, "stranger@example.com", {
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress":
            "stranger@example.com",
    })
    user = cognito_idp.admin_get_user(
        UserPoolId=pid, Username="TestSAML_stranger@example.com")
    assert user["UserStatus"] == "EXTERNAL_PROVIDER"
    assert claims["cognito:username"] == "TestSAML_stranger@example.com"


def test_cognito_admin_disable_provider_unlinks_and_restores_new_profiles(
    cognito_idp,
):
    pid, cid = _setup_saml_pool(cognito_idp)
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="carlos", MessageAction="SUPPRESS",
        UserAttributes=[{"Name": "email", "Value": "msp_carlos@example.com"}],
    )
    source = {"ProviderName": "TestSAML",
              "ProviderAttributeName": "Cognito_Subject",
              "ProviderAttributeValue": "msp_carlos@example.com"}
    cognito_idp.admin_link_provider_for_user(
        UserPoolId=pid,
        DestinationUser={"ProviderName": "Cognito", "ProviderAttributeValue": "carlos"},
        SourceUser=source,
    )
    cognito_idp.admin_disable_provider_for_user(UserPoolId=pid, User=source)

    attrs = {a["Name"]: a["Value"] for a in cognito_idp.admin_get_user(
        UserPoolId=pid, Username="carlos")["UserAttributes"]}
    assert "identities" not in attrs

    # Unlinked again, the same sign-in mints its own federated profile.
    _saml_sign_in(cid, "msp_carlos@example.com", {
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress":
            "msp_carlos@example.com",
    })
    assert cognito_idp.admin_get_user(
        UserPoolId=pid, Username="TestSAML_msp_carlos@example.com",
    )["UserStatus"] == "EXTERNAL_PROVIDER"

    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_disable_provider_for_user(UserPoolId=pid, User=source)
    assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cognito_admin_link_provider_errors(cognito_idp):
    pid, _cid = _setup_saml_pool(cognito_idp)
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="carlos", MessageAction="SUPPRESS",
        UserAttributes=[{"Name": "email", "Value": "msp_carlos@example.com"}],
    )
    dest = {"ProviderName": "Cognito", "ProviderAttributeValue": "carlos"}

    def source(value, provider="TestSAML", attribute="Cognito_Subject"):
        return {"ProviderName": provider, "ProviderAttributeName": attribute,
                "ProviderAttributeValue": value}

    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid,
            DestinationUser={"ProviderName": "Cognito",
                             "ProviderAttributeValue": "nobody"},
            SourceUser=source("a"))
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"

    # "This user must be a federated user ... not another native user."
    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid, DestinationUser=dest,
            SourceUser=source("carlos", provider="Cognito"))
    assert exc_info.value.response["Error"]["Code"] == "InvalidParameterException"

    # Relinking the same pair is a no-op; relinking to another profile is not.
    cognito_idp.admin_link_provider_for_user(
        UserPoolId=pid, DestinationUser=dest, SourceUser=source("dup"))
    cognito_idp.admin_link_provider_for_user(
        UserPoolId=pid, DestinationUser=dest, SourceUser=source("dup"))
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="other", MessageAction="SUPPRESS")
    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid,
            DestinationUser={"ProviderName": "Cognito",
                             "ProviderAttributeValue": "other"},
            SourceUser=source("dup"))
    assert exc_info.value.response["Error"]["Code"] == "AliasExistsException"

    # "You can link up to five federated users to each user profile."
    for i in range(4):
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid, DestinationUser=dest, SourceUser=source(f"extra-{i}"))
    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid, DestinationUser=dest, SourceUser=source("one-too-many"))
    assert exc_info.value.response["Error"]["Code"] == "LimitExceededException"


def test_cognito_admin_link_provider_rejects_an_already_signed_in_user(
    cognito_idp,
):
    """"To link a federated user who has previously signed in, you must first
    delete their existing profile."""
    pid, cid = _setup_saml_pool(cognito_idp)
    _saml_sign_in(cid, "early@example.com", {
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress":
            "early@example.com",
    })
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="carlos", MessageAction="SUPPRESS")

    with pytest.raises(ClientError) as exc_info:
        cognito_idp.admin_link_provider_for_user(
            UserPoolId=pid,
            DestinationUser={"ProviderName": "Cognito",
                             "ProviderAttributeValue": "carlos"},
            SourceUser={"ProviderName": "TestSAML",
                        "ProviderAttributeName": "Cognito_Subject",
                        "ProviderAttributeValue": "early@example.com"})
    assert exc_info.value.response["Error"]["Code"] == "AliasExistsException"
