import base64
import io
import json
import os
import time
import urllib.request
import urllib.error
import zipfile
from urllib.parse import urlparse, parse_qs as _parse_qs, urlencode as _urlencode
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
    assert "CloudFrontDomain" in resp

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

    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="OPTIONAL",
    )
    resp = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert resp["MfaConfiguration"] == "OPTIONAL"
    assert resp["SoftwareTokenMfaConfiguration"]["Enabled"] is True

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
    cognito_idp.global_sign_out(AccessToken=access_token)  # must not raise

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
    cognito_idp.admin_create_user(UserPoolId=pid, Username="signoutuser")
    cognito_idp.admin_user_global_sign_out(UserPoolId=pid, Username="signoutuser")

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
    cognito_idp.revoke_token(Token=refresh_token, ClientId=cid)

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
    import urllib.request, json as _json
    from conftest import make_client
    cognito = make_client("cognito-idp")
    pool = cognito.create_user_pool(PoolName="jwks-pool")["UserPool"]
    pool_id = pool["Id"]
    req = urllib.request.Request(
        f"http://localhost:4566/{pool_id}/.well-known/jwks.json",
    )
    with urllib.request.urlopen(req) as r:
        data = _json.loads(r.read())
    assert "keys" in data
    assert len(data["keys"]) >= 1
    assert data["keys"][0]["kty"] == "RSA"
    assert data["keys"][0]["alg"] == "RS256"

def test_cognito_openid_configuration():
    """/.well-known/openid-configuration returns valid discovery document."""
    import urllib.request, json as _json
    from conftest import make_client
    cognito = make_client("cognito-idp")
    pool = cognito.create_user_pool(PoolName="oidc-pool")["UserPool"]
    pool_id = pool["Id"]
    req = urllib.request.Request(
        f"http://localhost:4566/{pool_id}/.well-known/openid-configuration",
    )
    with urllib.request.urlopen(req) as r:
        data = _json.loads(r.read())
    assert "issuer" in data
    assert pool_id in data["issuer"]
    assert "jwks_uri" in data
    assert "token_endpoint" in data


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

ENDPOINT = "http://localhost:4566"


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Capture 302 redirects without following them."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


_no_redirect_opener = urllib.request.build_opener(_NoRedirectHandler)


def _setup_saml_pool(cognito_idp):
    """Helper: create a pool + client + SAML provider for federated tests."""
    pid = cognito_idp.create_user_pool(PoolName="FedPool")["UserPool"]["Id"]
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
