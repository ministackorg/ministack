"""
Amazon Cognito Service Emulator.

Covers two boto3 clients:
  cognito-idp  — User Pools (X-Amz-Target: AWSCognitoIdentityProviderService.*)
  cognito-identity — Identity Pools (X-Amz-Target: AWSCognitoIdentityService.*)

User Pools operations:
  CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, UpdateUserPool,
  CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient,
  ListUserPoolClients, UpdateUserPoolClient,
  AdminCreateUser, AdminDeleteUser, AdminGetUser, ListUsers,
  AdminSetUserPassword, AdminUpdateUserAttributes,
  AdminInitiateAuth, AdminRespondToAuthChallenge,
  InitiateAuth, RespondToAuthChallenge, SignUp, ConfirmSignUp,
  ForgotPassword, ConfirmForgotPassword, ChangePassword,
  GetUser, UpdateUserAttributes, DeleteUser,
  AdminAddUserToGroup, AdminRemoveUserFromGroup,
  AdminListGroupsForUser, AdminListUserAuthEvents,
  CreateGroup, DeleteGroup, GetGroup, ListGroups,
  AdminConfirmSignUp, AdminDisableUser, AdminEnableUser,
  AdminResetUserPassword, AdminUserGlobalSignOut,
  GlobalSignOut, RevokeToken,
  CreateUserPoolDomain, DeleteUserPoolDomain, DescribeUserPoolDomain,
  GetUserPoolMfaConfig, SetUserPoolMfaConfig,
  AssociateSoftwareToken, VerifySoftwareToken,
  TagResource, UntagResource, ListTagsForResource.

Identity Pools operations:
  CreateIdentityPool, DeleteIdentityPool, DescribeIdentityPool,
  ListIdentityPools, UpdateIdentityPool,
  GetId, GetCredentialsForIdentity, GetOpenIdToken,
  SetIdentityPoolRoles, GetIdentityPoolRoles,
  ListIdentities, DescribeIdentity, MergeDeveloperIdentities,
  UnlinkDeveloperIdentity, UnlinkIdentity,
  TagResource, UntagResource, ListTagsForResource.

Wire protocol:
  Both services use JSON with X-Amz-Target header.
  cognito-idp  credential scope: cognito-idp
  cognito-identity credential scope: cognito-identity
  Routing is handled in app.py via two separate SERVICE_HANDLERS entries.
"""

import base64
import copy
import os
import json
import logging
import re
import secrets
import string
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import error_response_json, json_response, new_uuid

logger = logging.getLogger("cognito")

# ---------------------------------------------------------------------------
# RSA key pair for JWKS / token signing
# ---------------------------------------------------------------------------

_RSA_PRIVATE_KEY = None
_JWKS_KEY: dict = {}

try:
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    _rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    _RSA_PRIVATE_KEY = _rsa_key

    _pub = _rsa_key.public_key()
    _pub_numbers = _pub.public_numbers()

    def _int_to_base64url(n: int, length: int) -> str:
        data = n.to_bytes(length, byteorder="big")
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    _JWKS_KEY = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": "ministack-key-1",
        "n": _int_to_base64url(_pub_numbers.n, 256),
        "e": _int_to_base64url(_pub_numbers.e, 3),
    }
except ImportError:
    # Fallback: static dummy key when cryptography is not installed
    _JWKS_KEY = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": "ministack-key-1",
        "n": (
            "wJUEhGbAmcKEHp7EaNBYYEmign_bbWUBnfQGTCZ0h4ViqHC_KQQ7A"
            "3E9X3OJ1P1E5VWZqvMfVN3l_0ljPBiA0XG4D4GBJzFJBmXq48Sk-"
            "G38q5LHxzH-ajLz7TrEMqSF3XTkmJ_7y3p3BdML2oFGm4F0DUUEU"
            "P3xmILPH2uo9g-5xRjYMh8i7V0xXyTAQS5Tw"
        ),
        "e": "AQAB",
    }


def well_known_jwks(pool_id: str):
    """Return JWKS JSON for /{poolId}/.well-known/jwks.json."""
    return 200, {"Content-Type": "application/json"}, json.dumps({"keys": [_JWKS_KEY]}).encode()


def well_known_openid_configuration(pool_id: str, region: str | None = None):
    """Return OpenID Connect discovery document."""
    r = region or REGION
    issuer = f"https://cognito-idp.{r}.amazonaws.com/{pool_id}"
    doc = {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/.well-known/jwks.json",
        "authorization_endpoint": f"{issuer}/oauth2/authorize",
        "token_endpoint": f"{issuer}/oauth2/token",
        "response_types_supported": ["code", "token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    return 200, {"Content-Type": "application/json"}, json.dumps(doc).encode()

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state — User Pools (cognito-idp)
# ---------------------------------------------------------------------------

_user_pools: dict = {}
# pool_id -> {
#   Id, Name, Arn, CreationDate, LastModifiedDate, Status,
#   Policies, Schema, AutoVerifiedAttributes, UsernameAttributes,
#   MfaConfiguration, EstimatedNumberOfUsers,
#   AdminCreateUserConfig, UserPoolTags,
#   Domain (str|None),
#   _clients: {client_id -> client_dict},
#   _users:   {username -> user_dict},
#   _groups:  {group_name -> group_dict},
# }

_pool_domain_map: dict = {}   # domain -> pool_id

# ---------------------------------------------------------------------------
# In-memory state — Identity Pools (cognito-identity)
# ---------------------------------------------------------------------------

_identity_pools: dict = {}
# identity_pool_id -> {
#   IdentityPoolId, IdentityPoolName, AllowUnauthenticatedIdentities,
#   SupportedLoginProviders, DeveloperProviderName,
#   OpenIdConnectProviderARNs, CognitoIdentityProviders,
#   SamlProviderARNs, IdentityPoolTags,
#   _roles: {authenticated: arn, unauthenticated: arn},
#   _identities: {identity_id -> identity_dict},
# }

_identity_tags: dict = {}   # identity_pool_id -> {key: value}


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "user_pools": copy.deepcopy(_user_pools),
        "pool_domain_map": copy.deepcopy(_pool_domain_map),
        "identity_pools": copy.deepcopy(_identity_pools),
        "identity_tags": copy.deepcopy(_identity_tags),
    }


def restore_state(data):
    if data:
        _user_pools.update(data.get("user_pools", {}))
        _pool_domain_map.update(data.get("pool_domain_map", {}))
        _identity_pools.update(data.get("identity_pools", {}))
        _identity_tags.update(data.get("identity_tags", {}))


_restored = load_state("cognito")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def _pool_arn(pool_id: str) -> str:
    return f"arn:aws:cognito-idp:{REGION}:{ACCOUNT_ID}:userpool/{pool_id}"


def _pool_id() -> str:
    suffix = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(26))
    return f"{REGION}_{suffix[:9]}"


def _client_id() -> str:
    return "".join(secrets.choice(string.digits + string.ascii_letters) for _ in range(26))


def _client_secret() -> str:
    return base64.b64encode(secrets.token_bytes(48)).decode()


def _identity_pool_id() -> str:
    return f"{REGION}:{new_uuid()}"


def _identity_id(pool_id: str) -> str:
    return f"{REGION}:{new_uuid()}"


def _fake_token(sub: str, pool_id: str, client_id: str, token_type: str = "access") -> str:
    """Return a plausible-looking but non-cryptographic JWT stub."""
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "kid": "ministack-key-1"}).encode()
    ).rstrip(b"=").decode()
    now = int(time.time())
    payload = base64.urlsafe_b64encode(
        json.dumps({
            "sub": sub,
            "iss": f"https://cognito-idp.{REGION}.amazonaws.com/{pool_id}",
            "client_id": client_id,
            "token_use": token_type,
            "iat": now,
            "exp": now + 3600,
            "jti": new_uuid(),
        }).encode()
    ).rstrip(b"=").decode()
    sig = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    return f"{header}.{payload}.{sig}"


def _user_from_token(token: str, pool: dict):
    """Decode a stub JWT and return the matching user from pool, or None."""
    try:
        payload_b64 = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=="))
        sub = payload.get("sub", "")
        for user in pool["_users"].values():
            if _attr_list_to_dict(user.get("Attributes", [])).get("sub") == sub:
                return user
    except Exception:
        pass
    return None


def _resolve_pool(pool_id: str):
    pool = _user_pools.get(pool_id)
    if not pool:
        return None, error_response_json(
            "ResourceNotFoundException",
            f"User pool {pool_id} does not exist.", 400,
        )
    return pool, None


def _resolve_user(pool: dict, username: str):
    user = pool["_users"].get(username)
    if not user:
        return None, error_response_json(
            "UserNotFoundException",
            f"User {username} does not exist.", 400,
        )
    return user, None


def _user_out(user: dict) -> dict:
    """Serialise a user dict for API responses."""
    return {
        "Username": user["Username"],
        "Attributes": user.get("Attributes", []),
        "UserCreateDate": user.get("UserCreateDate", _now_epoch()),
        "UserLastModifiedDate": user.get("UserLastModifiedDate", _now_epoch()),
        "Enabled": user.get("Enabled", True),
        "UserStatus": user.get("UserStatus", "CONFIRMED"),
        "MFAOptions": user.get("MFAOptions", []),
    }


def _attr_list_to_dict(attrs: list) -> dict:
    return {a["Name"]: a["Value"] for a in attrs if "Name" in a}


def _dict_to_attr_list(d: dict) -> list:
    return [{"Name": k, "Value": v} for k, v in d.items()]


def _merge_attributes(existing: list, updates: list) -> list:
    d = _attr_list_to_dict(existing)
    d.update(_attr_list_to_dict(updates))
    return _dict_to_attr_list(d)


# ---------------------------------------------------------------------------
# Entry points — two separate handle_request functions
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    """Unified entry point — dispatches to IDP or Identity based on target prefix."""
    target = headers.get("x-amz-target", "")

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    if target.startswith("AWSCognitoIdentityService."):
        action = target.split(".")[-1]
        return _dispatch_identity(action, data)

    if target.startswith("AWSCognitoIdentityProviderService."):
        action = target.split(".")[-1]
        return _dispatch_idp(action, data)

    # Path-based fallback for REST-style calls (e.g. /oauth2/token)
    if path.startswith("/oauth2/token"):
        return _oauth2_token(data, query_params, body)

    return error_response_json("InvalidAction", f"Unknown Cognito target: {target}", 400)


# ---------------------------------------------------------------------------
# IDP dispatcher
# ---------------------------------------------------------------------------

def _dispatch_idp(action: str, data: dict):
    handlers = {
        # User Pool CRUD
        "CreateUserPool": _create_user_pool,
        "DeleteUserPool": _delete_user_pool,
        "DescribeUserPool": _describe_user_pool,
        "ListUserPools": _list_user_pools,
        "UpdateUserPool": _update_user_pool,
        # User Pool Client CRUD
        "CreateUserPoolClient": _create_user_pool_client,
        "DeleteUserPoolClient": _delete_user_pool_client,
        "DescribeUserPoolClient": _describe_user_pool_client,
        "ListUserPoolClients": _list_user_pool_clients,
        "UpdateUserPoolClient": _update_user_pool_client,
        # User management
        "AdminCreateUser": _admin_create_user,
        "AdminDeleteUser": _admin_delete_user,
        "AdminGetUser": _admin_get_user,
        "ListUsers": _list_users,
        "AdminSetUserPassword": _admin_set_user_password,
        "AdminUpdateUserAttributes": _admin_update_user_attributes,
        "AdminConfirmSignUp": _admin_confirm_sign_up,
        "AdminDisableUser": _admin_disable_user,
        "AdminEnableUser": _admin_enable_user,
        "AdminResetUserPassword": _admin_reset_user_password,
        "AdminUserGlobalSignOut": _admin_user_global_sign_out,
        "AdminListGroupsForUser": _admin_list_groups_for_user,
        "AdminListUserAuthEvents": _admin_list_user_auth_events,
        "AdminAddUserToGroup": _admin_add_user_to_group,
        "AdminRemoveUserFromGroup": _admin_remove_user_from_group,
        # Auth flows
        "AdminInitiateAuth": _admin_initiate_auth,
        "AdminRespondToAuthChallenge": _admin_respond_to_auth_challenge,
        "InitiateAuth": _initiate_auth,
        "RespondToAuthChallenge": _respond_to_auth_challenge,
        "GlobalSignOut": _global_sign_out,
        "RevokeToken": _revoke_token,
        # Self-service
        "SignUp": _sign_up,
        "ConfirmSignUp": _confirm_sign_up,
        "ForgotPassword": _forgot_password,
        "ConfirmForgotPassword": _confirm_forgot_password,
        "ChangePassword": _change_password,
        "GetUser": _get_user,
        "UpdateUserAttributes": _update_user_attributes,
        "DeleteUser": _delete_user,
        # Groups
        "CreateGroup": _create_group,
        "DeleteGroup": _delete_group,
        "GetGroup": _get_group,
        "ListGroups": _list_groups,
        "ListUsersInGroup": _list_users_in_group,
        # Domain
        "CreateUserPoolDomain": _create_user_pool_domain,
        "DeleteUserPoolDomain": _delete_user_pool_domain,
        "DescribeUserPoolDomain": _describe_user_pool_domain,
        # MFA
        "GetUserPoolMfaConfig": _get_user_pool_mfa_config,
        "SetUserPoolMfaConfig": _set_user_pool_mfa_config,
        "AssociateSoftwareToken": _associate_software_token,
        "VerifySoftwareToken": _verify_software_token,
        "AdminSetUserMFAPreference": _admin_set_user_mfa_preference,
        "SetUserMFAPreference": _set_user_mfa_preference,
        # Tags
        "TagResource": _idp_tag_resource,
        "UntagResource": _idp_untag_resource,
        "ListTagsForResource": _idp_list_tags_for_resource,
    }
    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Cognito IDP action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Identity Pool dispatcher
# ---------------------------------------------------------------------------

def _dispatch_identity(action: str, data: dict):
    handlers = {
        "CreateIdentityPool": _create_identity_pool,
        "DeleteIdentityPool": _delete_identity_pool,
        "DescribeIdentityPool": _describe_identity_pool,
        "ListIdentityPools": _list_identity_pools,
        "UpdateIdentityPool": _update_identity_pool,
        "GetId": _get_id,
        "GetCredentialsForIdentity": _get_credentials_for_identity,
        "GetOpenIdToken": _get_open_id_token,
        "SetIdentityPoolRoles": _set_identity_pool_roles,
        "GetIdentityPoolRoles": _get_identity_pool_roles,
        "ListIdentities": _list_identities,
        "DescribeIdentity": _describe_identity,
        "MergeDeveloperIdentities": _merge_developer_identities,
        "UnlinkDeveloperIdentity": _unlink_developer_identity,
        "UnlinkIdentity": _unlink_identity,
        "TagResource": _identity_tag_resource,
        "UntagResource": _identity_untag_resource,
        "ListTagsForResource": _identity_list_tags_for_resource,
    }
    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Cognito Identity action: {action}", 400)
    return handler(data)


# ===========================================================================
# USER POOL CRUD
# ===========================================================================

def _create_user_pool(data):
    name = data.get("PoolName")
    if not name:
        return error_response_json("InvalidParameterException", "PoolName is required.", 400)

    pid = _pool_id()
    now = _now_epoch()
    pool = {
        "Id": pid,
        "Name": name,
        "Arn": _pool_arn(pid),
        "CreationDate": now,
        "LastModifiedDate": now,
        "Policies": data.get("Policies", {
            "PasswordPolicy": {
                "MinimumLength": 8,
                "RequireUppercase": True,
                "RequireLowercase": True,
                "RequireNumbers": True,
                "RequireSymbols": True,
                "TemporaryPasswordValidityDays": 7,
            }
        }),
        "Schema": data.get("Schema", []),
        "AutoVerifiedAttributes": data.get("AutoVerifiedAttributes", []),
        "AliasAttributes": data.get("AliasAttributes", []),
        "UsernameAttributes": data.get("UsernameAttributes", []),
        "SmsVerificationMessage": data.get("SmsVerificationMessage", ""),
        "EmailVerificationMessage": data.get("EmailVerificationMessage", ""),
        "EmailVerificationSubject": data.get("EmailVerificationSubject", ""),
        "SmsAuthenticationMessage": data.get("SmsAuthenticationMessage", ""),
        "MfaConfiguration": data.get("MfaConfiguration", "OFF"),
        "DeviceConfiguration": data.get("DeviceConfiguration", {}),
        "EstimatedNumberOfUsers": 0,
        "EmailConfiguration": data.get("EmailConfiguration", {}),
        "SmsConfiguration": data.get("SmsConfiguration", {}),
        "UserPoolTags": data.get("UserPoolTags", {}),
        "AdminCreateUserConfig": data.get("AdminCreateUserConfig", {
            "AllowAdminCreateUserOnly": False,
            "UnusedAccountValidityDays": 7,
        }),
        "UsernameConfiguration": data.get("UsernameConfiguration", {"CaseSensitive": False}),
        "AccountRecoverySetting": data.get("AccountRecoverySetting", {}),
        "UserPoolAddOns": data.get("UserPoolAddOns", {}),
        "VerificationMessageTemplate": data.get("VerificationMessageTemplate", {}),
        "Domain": None,
        "_clients": {},
        "_users": {},
        "_groups": {},
    }
    _user_pools[pid] = pool
    logger.info("Cognito: CreateUserPool %s (%s)", name, pid)
    return json_response({"UserPool": _pool_out(pool)})


def _delete_user_pool(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    if pool.get("Domain"):
        _pool_domain_map.pop(pool["Domain"], None)
    del _user_pools[pid]
    return json_response({})


def _describe_user_pool(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    return json_response({"UserPool": _pool_out(pool)})


def _list_user_pools(data):
    max_results = min(data.get("MaxResults", 60), 60)
    next_token = data.get("NextToken")
    pools = sorted(_user_pools.values(), key=lambda p: p["CreationDate"])
    start = int(next_token) if next_token else 0
    page = pools[start:start + max_results]
    resp = {
        "UserPools": [
            {"Id": p["Id"], "Name": p["Name"],
             "LastModifiedDate": p["LastModifiedDate"], "CreationDate": p["CreationDate"]}
            for p in page
        ]
    }
    if start + max_results < len(pools):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


def _update_user_pool(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    updatable = {
        "Policies", "AutoVerifiedAttributes", "SmsVerificationMessage",
        "EmailVerificationMessage", "EmailVerificationSubject",
        "SmsAuthenticationMessage", "MfaConfiguration", "DeviceConfiguration",
        "EmailConfiguration", "SmsConfiguration", "UserPoolTags",
        "AdminCreateUserConfig", "UserPoolAddOns", "VerificationMessageTemplate",
        "AccountRecoverySetting",
    }
    for k in updatable:
        if k in data:
            pool[k] = data[k]
    pool["LastModifiedDate"] = _now_epoch()
    return json_response({})


def _pool_out(pool: dict) -> dict:
    return {k: v for k, v in pool.items() if not k.startswith("_")}


# ===========================================================================
# USER POOL CLIENT CRUD
# ===========================================================================

def _create_user_pool_client(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err

    cid = _client_id()
    now = _now_epoch()
    generate_secret = data.get("GenerateSecret", False)
    client = {
        "UserPoolId": pid,
        "ClientName": data.get("ClientName", ""),
        "ClientId": cid,
        "ClientSecret": _client_secret() if generate_secret else None,
        "CreationDate": now,
        "LastModifiedDate": now,
        "RefreshTokenValidity": data.get("RefreshTokenValidity", 30),
        "AccessTokenValidity": data.get("AccessTokenValidity", 60),
        "IdTokenValidity": data.get("IdTokenValidity", 60),
        "TokenValidityUnits": data.get("TokenValidityUnits", {}),
        "ReadAttributes": data.get("ReadAttributes", []),
        "WriteAttributes": data.get("WriteAttributes", []),
        "ExplicitAuthFlows": data.get("ExplicitAuthFlows", []),
        "SupportedIdentityProviders": data.get("SupportedIdentityProviders", []),
        "CallbackURLs": data.get("CallbackURLs", []),
        "LogoutURLs": data.get("LogoutURLs", []),
        "DefaultRedirectURI": data.get("DefaultRedirectURI", ""),
        "AllowedOAuthFlows": data.get("AllowedOAuthFlows", []),
        "AllowedOAuthScopes": data.get("AllowedOAuthScopes", []),
        "AllowedOAuthFlowsUserPoolClient": data.get("AllowedOAuthFlowsUserPoolClient", False),
        "AnalyticsConfiguration": data.get("AnalyticsConfiguration", {}),
        "PreventUserExistenceErrors": data.get("PreventUserExistenceErrors", "ENABLED"),
        "EnableTokenRevocation": data.get("EnableTokenRevocation", True),
        "EnablePropagateAdditionalUserContextData": data.get("EnablePropagateAdditionalUserContextData", False),
        "AuthSessionValidity": data.get("AuthSessionValidity", 3),
    }
    pool["_clients"][cid] = client
    out = {k: v for k, v in client.items() if v is not None}
    return json_response({"UserPoolClient": out})


def _delete_user_pool_client(data):
    pid = data.get("UserPoolId")
    cid = data.get("ClientId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    if cid not in pool["_clients"]:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)
    del pool["_clients"][cid]
    return json_response({})


def _describe_user_pool_client(data):
    pid = data.get("UserPoolId")
    cid = data.get("ClientId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    client = pool["_clients"].get(cid)
    if not client:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)
    return json_response({"UserPoolClient": {k: v for k, v in client.items() if v is not None}})


def _list_user_pool_clients(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    max_results = min(data.get("MaxResults", 60), 60)
    next_token = data.get("NextToken")
    clients = sorted(pool["_clients"].values(), key=lambda c: c["CreationDate"])
    start = int(next_token) if next_token else 0
    page = clients[start:start + max_results]
    resp = {
        "UserPoolClients": [
            {"ClientId": c["ClientId"], "UserPoolId": pid, "ClientName": c["ClientName"]}
            for c in page
        ]
    }
    if start + max_results < len(clients):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


def _update_user_pool_client(data):
    pid = data.get("UserPoolId")
    cid = data.get("ClientId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    client = pool["_clients"].get(cid)
    if not client:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)
    updatable = {
        "ClientName", "RefreshTokenValidity", "AccessTokenValidity", "IdTokenValidity",
        "TokenValidityUnits", "ReadAttributes", "WriteAttributes", "ExplicitAuthFlows",
        "SupportedIdentityProviders", "CallbackURLs", "LogoutURLs", "DefaultRedirectURI",
        "AllowedOAuthFlows", "AllowedOAuthScopes", "AllowedOAuthFlowsUserPoolClient",
        "AnalyticsConfiguration", "PreventUserExistenceErrors", "EnableTokenRevocation",
        "EnablePropagateAdditionalUserContextData", "AuthSessionValidity",
    }
    for k in updatable:
        if k in data:
            client[k] = data[k]
    client["LastModifiedDate"] = _now_epoch()
    return json_response({"UserPoolClient": {k: v for k, v in client.items() if v is not None}})


# ===========================================================================
# USER MANAGEMENT
# ===========================================================================

def _admin_create_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err

    username = data.get("Username")
    if not username:
        return error_response_json("InvalidParameterException", "Username is required.", 400)
    if username in pool["_users"]:
        return error_response_json(
            "UsernameExistsException",
            "User account already exists.", 400,
        )

    now = _now_epoch()
    temp_password = data.get("TemporaryPassword") or _generate_temp_password()
    attrs = data.get("UserAttributes", [])
    # Ensure sub attribute
    attr_dict = _attr_list_to_dict(attrs)
    if "sub" not in attr_dict:
        attr_dict["sub"] = new_uuid()
    attrs = _dict_to_attr_list(attr_dict)

    user = {
        "Username": username,
        "Attributes": attrs,
        "UserCreateDate": now,
        "UserLastModifiedDate": now,
        "Enabled": True,
        "UserStatus": "FORCE_CHANGE_PASSWORD",
        "MFAOptions": [],
        "_password": temp_password,
        "_groups": [],
        "_tokens": [],
    }
    pool["_users"][username] = user
    pool["EstimatedNumberOfUsers"] = len(pool["_users"])
    logger.info("Cognito: AdminCreateUser %s in pool %s", username, pid)
    return json_response({"User": _user_out(user)})


def _admin_delete_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    del pool["_users"][username]
    pool["EstimatedNumberOfUsers"] = len(pool["_users"])
    return json_response({})


def _admin_get_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    out = _user_out(user)
    # AdminGetUser uses UserAttributes, not Attributes (per AWS API shape)
    out["UserAttributes"] = out.pop("Attributes", [])
    out["UserMFASettingList"] = user.get("_mfa_enabled", [])
    out["PreferredMfaSetting"] = user.get("_preferred_mfa", "")
    return json_response(out)


def _list_users(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err

    limit = min(data.get("Limit", 60), 60)
    pagination_token = data.get("PaginationToken")
    filter_str = data.get("Filter", "")

    users = list(pool["_users"].values())

    # Simple filter: "attribute_name = \"value\"" or "attribute_name ^= \"value\""
    if filter_str:
        users = _apply_user_filter(users, filter_str)

    start = 0
    try:
        start = int(pagination_token) if pagination_token else 0
    except (ValueError, TypeError):
        start = 0
    page = users[start:start + limit]
    resp = {"Users": [_user_out(u) for u in page]}
    if start + limit < len(users):
        resp["PaginationToken"] = str(start + limit)
    return json_response(resp)


def _admin_set_user_password(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["_password"] = data.get("Password", "")
    permanent = data.get("Permanent", False)
    if permanent:
        user["UserStatus"] = "CONFIRMED"
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_update_user_attributes(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["Attributes"] = _merge_attributes(
        user.get("Attributes", []),
        data.get("UserAttributes", []),
    )
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_confirm_sign_up(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["UserStatus"] = "CONFIRMED"
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_disable_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["Enabled"] = False
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_enable_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["Enabled"] = True
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_reset_user_password(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["UserStatus"] = "RESET_REQUIRED"
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _admin_user_global_sign_out(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    user["_tokens"] = []
    return json_response({})


def _admin_list_groups_for_user(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    groups = [
        pool["_groups"][g] for g in user.get("_groups", [])
        if g in pool["_groups"]
    ]
    return json_response({"Groups": [_group_out(g) for g in groups]})


def _admin_list_user_auth_events(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    _, err = _resolve_user(pool, username)
    if err:
        return err
    return json_response({"AuthEvents": []})


def _admin_add_user_to_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    group_name = data.get("GroupName")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    if group_name not in pool["_groups"]:
        return error_response_json("ResourceNotFoundException", f"Group {group_name} not found.", 400)
    if group_name not in user.get("_groups", []):
        user.setdefault("_groups", []).append(group_name)
        pool["_groups"][group_name].setdefault("_members", []).append(username)
    return json_response({})


def _admin_remove_user_from_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    group_name = data.get("GroupName")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    if group_name in user.get("_groups", []):
        user["_groups"].remove(group_name)
    if group_name in pool["_groups"]:
        members = pool["_groups"][group_name].get("_members", [])
        if username in members:
            members.remove(username)
    return json_response({})


# ===========================================================================
# AUTH FLOWS
# ===========================================================================

def _mfa_challenge_for_user(pool: dict, user: dict, pid: str, username: str) -> dict | None:
    """Return a SOFTWARE_TOKEN_MFA challenge dict if the pool+user require it, else None."""
    mfa_config = pool.get("MfaConfiguration", "OFF")
    if mfa_config == "OFF":
        return None
    preferred = user.get("_preferred_mfa", "")
    enabled_mfa = user.get("_mfa_enabled", [])
    # OPTIONAL: only challenge if user has TOTP set up
    if mfa_config == "OPTIONAL" and "SOFTWARE_TOKEN_MFA" not in enabled_mfa:
        return None
    # ON: challenge if TOTP is set up; if not set up yet, skip (let them enroll)
    if mfa_config == "ON" and "SOFTWARE_TOKEN_MFA" not in enabled_mfa:
        return None
    session = base64.b64encode(secrets.token_bytes(32)).decode()
    return {
        "ChallengeName": "SOFTWARE_TOKEN_MFA",
        "Session": session,
        "ChallengeParameters": {
            "USER_ID_FOR_SRP": username,
            "FRIENDLY_DEVICE_NAME": "TOTP device",
        },
    }


def _build_auth_result(pool_id: str, client_id: str, user: dict) -> dict:
    sub = _attr_list_to_dict(user.get("Attributes", [])).get("sub", user["Username"])
    return {
        "AccessToken": _fake_token(sub, pool_id, client_id, "access"),
        "IdToken": _fake_token(sub, pool_id, client_id, "id"),
        "RefreshToken": _fake_token(sub, pool_id, client_id, "refresh"),
        "TokenType": "Bearer",
        "ExpiresIn": 3600,
    }


def _admin_initiate_auth(data):
    pid = data.get("UserPoolId")
    cid = data.get("ClientId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    if cid not in pool["_clients"]:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    auth_flow = data.get("AuthFlow", "")
    auth_params = data.get("AuthParameters", {})

    if auth_flow in ("ADMIN_USER_PASSWORD_AUTH", "ADMIN_NO_SRP_AUTH"):
        username = auth_params.get("USERNAME")
        password = auth_params.get("PASSWORD")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        if not user.get("Enabled", True):
            return error_response_json("NotAuthorizedException", "User is disabled.", 400)
        if user.get("_password") and user["_password"] != password:
            return error_response_json("NotAuthorizedException", "Incorrect username or password.", 400)
        if user.get("UserStatus") == "FORCE_CHANGE_PASSWORD":
            session = base64.b64encode(secrets.token_bytes(32)).decode()
            return json_response({
                "ChallengeName": "NEW_PASSWORD_REQUIRED",
                "Session": session,
                "ChallengeParameters": {
                    "USER_ID_FOR_SRP": username,
                    "requiredAttributes": "[]",
                    "userAttributes": json.dumps(_attr_list_to_dict(user.get("Attributes", []))),
                },
            })
        mfa_challenge = _mfa_challenge_for_user(pool, user, pid, username)
        if mfa_challenge:
            return json_response(mfa_challenge)
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if auth_flow in ("REFRESH_TOKEN_AUTH", "REFRESH_TOKEN"):
        refresh_token = auth_params.get("REFRESH_TOKEN", "")
        if not refresh_token:
            return error_response_json("NotAuthorizedException", "Refresh token is missing.", 400)
        # Decode stub token to find the correct user by sub
        user = _user_from_token(refresh_token, pool)
        if not user:
            # Fall back to first user if token can't be decoded (e.g. externally issued token)
            users = list(pool["_users"].values())
            if not users:
                return error_response_json("NotAuthorizedException", "No users in pool.", 400)
            user = users[0]
        result = _build_auth_result(pid, cid, user)
        result.pop("RefreshToken", None)  # AWS doesn't return a new refresh token here
        return json_response({"AuthenticationResult": result})

    return error_response_json("InvalidParameterException", f"Unsupported AuthFlow: {auth_flow}", 400)


def _admin_respond_to_auth_challenge(data):
    pid = data.get("UserPoolId")
    cid = data.get("ClientId")
    pool, err = _resolve_pool(pid)
    if err:
        return err

    challenge_name = data.get("ChallengeName", "")
    responses = data.get("ChallengeResponses", {})

    if challenge_name == "NEW_PASSWORD_REQUIRED":
        username = responses.get("USERNAME")
        new_password = responses.get("NEW_PASSWORD")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        if new_password:
            user["_password"] = new_password
        user["UserStatus"] = "CONFIRMED"
        user["UserLastModifiedDate"] = _now_epoch()
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if challenge_name == "SMS_MFA":
        username = responses.get("USERNAME")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if challenge_name == "SOFTWARE_TOKEN_MFA":
        username = responses.get("USERNAME")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        # Accept any TOTP code in emulator — no real TOTP validation
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if challenge_name == "MFA_SETUP":
        # Triggered when pool MFA=ON but user hasn't enrolled yet
        username = responses.get("USERNAME")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    return error_response_json("InvalidParameterException", f"Unsupported challenge: {challenge_name}", 400)


def _initiate_auth(data):
    """Public InitiateAuth — same logic as AdminInitiateAuth but no UserPoolId required."""
    cid = data.get("ClientId")
    auth_flow = data.get("AuthFlow", "")
    auth_params = data.get("AuthParameters", {})

    # Find pool by client id
    pool = None
    pid = None
    for p_id, p in _user_pools.items():
        if cid in p["_clients"]:
            pool = p
            pid = p_id
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    if auth_flow in ("USER_PASSWORD_AUTH",):
        username = auth_params.get("USERNAME")
        password = auth_params.get("PASSWORD")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        if not user.get("Enabled", True):
            return error_response_json("NotAuthorizedException", "User is disabled.", 400)
        if user.get("_password") and user["_password"] != password:
            return error_response_json("NotAuthorizedException", "Incorrect username or password.", 400)
        if user.get("UserStatus") == "FORCE_CHANGE_PASSWORD":
            session = base64.b64encode(secrets.token_bytes(32)).decode()
            return json_response({
                "ChallengeName": "NEW_PASSWORD_REQUIRED",
                "Session": session,
                "ChallengeParameters": {
                    "USER_ID_FOR_SRP": username,
                    "requiredAttributes": "[]",
                    "userAttributes": json.dumps(_attr_list_to_dict(user.get("Attributes", []))),
                },
            })
        mfa_challenge = _mfa_challenge_for_user(pool, user, pid, username)
        if mfa_challenge:
            return json_response(mfa_challenge)
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if auth_flow in ("REFRESH_TOKEN_AUTH", "REFRESH_TOKEN"):
        refresh_token = auth_params.get("REFRESH_TOKEN", "")
        if not refresh_token:
            return error_response_json("NotAuthorizedException", "Refresh token is missing.", 400)
        # Decode stub token to find the correct user by sub
        user = _user_from_token(refresh_token, pool)
        if not user:
            users = list(pool["_users"].values())
            if not users:
                return error_response_json("NotAuthorizedException", "No users in pool.", 400)
            user = users[0]
        result = _build_auth_result(pid, cid, user)
        result.pop("RefreshToken", None)  # AWS doesn't return a new refresh token here
        return json_response({"AuthenticationResult": result})

    # USER_SRP_AUTH — return SRP challenge stub
    if auth_flow == "USER_SRP_AUTH":
        username = auth_params.get("USERNAME", "")
        return json_response({
            "ChallengeName": "PASSWORD_VERIFIER",
            "Session": base64.b64encode(secrets.token_bytes(32)).decode(),
            "ChallengeParameters": {
                "USER_ID_FOR_SRP": username,
                "SRP_B": base64.b64encode(secrets.token_bytes(128)).hex(),
                "SALT": base64.b64encode(secrets.token_bytes(16)).hex(),
                "SECRET_BLOCK": base64.b64encode(secrets.token_bytes(32)).decode(),
            },
        })

    return error_response_json("InvalidParameterException", f"Unsupported AuthFlow: {auth_flow}", 400)


def _respond_to_auth_challenge(data):
    cid = data.get("ClientId")
    challenge_name = data.get("ChallengeName", "")
    responses = data.get("ChallengeResponses", {})

    pool = None
    pid = None
    for p_id, p in _user_pools.items():
        if cid in p["_clients"]:
            pool = p
            pid = p_id
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    if challenge_name in ("NEW_PASSWORD_REQUIRED", "PASSWORD_VERIFIER"):
        username = responses.get("USERNAME")
        new_password = responses.get("NEW_PASSWORD") or responses.get("PASSWORD")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        if new_password:
            user["_password"] = new_password
        user["UserStatus"] = "CONFIRMED"
        user["UserLastModifiedDate"] = _now_epoch()
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    if challenge_name in ("SOFTWARE_TOKEN_MFA", "MFA_SETUP"):
        username = responses.get("USERNAME")
        user = pool["_users"].get(username)
        if not user:
            return error_response_json("UserNotFoundException", "User does not exist.", 400)
        # Accept any TOTP code in emulator
        return json_response({"AuthenticationResult": _build_auth_result(pid, cid, user)})

    return error_response_json("InvalidParameterException", f"Unsupported challenge: {challenge_name}", 400)


def _global_sign_out(data):
    # Access token is opaque to us — accept and succeed
    return json_response({})


def _revoke_token(data):
    return json_response({})


# ===========================================================================
# SELF-SERVICE (public-facing)
# ===========================================================================

def _sign_up(data):
    cid = data.get("ClientId")
    username = data.get("Username")
    password = data.get("Password", "")

    pool = None
    pid = None
    for p_id, p in _user_pools.items():
        if cid in p["_clients"]:
            pool = p
            pid = p_id
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)
    if username in pool["_users"]:
        return error_response_json("UsernameExistsException", "User already exists.", 400)

    now = _now_epoch()
    attrs = data.get("UserAttributes", [])
    attr_dict = _attr_list_to_dict(attrs)
    if "sub" not in attr_dict:
        attr_dict["sub"] = new_uuid()
    attrs = _dict_to_attr_list(attr_dict)

    # SignUp always creates UNCONFIRMED — ConfirmSignUp (or AdminConfirmSignUp) confirms the account.
    # AutoVerifiedAttributes only auto-verifies those attributes (e.g. email), not the account itself.
    # Auto-confirming accounts requires a pre-signup Lambda trigger, which we don't emulate.
    status = "UNCONFIRMED"

    user = {
        "Username": username,
        "Attributes": attrs,
        "UserCreateDate": now,
        "UserLastModifiedDate": now,
        "Enabled": True,
        "UserStatus": status,
        "MFAOptions": [],
        "_password": password,
        "_groups": [],
        "_tokens": [],
        "_confirmation_code": "123456",
    }
    pool["_users"][username] = user
    pool["EstimatedNumberOfUsers"] = len(pool["_users"])

    resp = {
        "UserConfirmed": status == "CONFIRMED",
        "UserSub": attr_dict["sub"],
    }
    if "email" in attr_dict:
        resp["CodeDeliveryDetails"] = {
            "Destination": attr_dict["email"],
            "DeliveryMedium": "EMAIL",
            "AttributeName": "email",
        }
    return json_response(resp)


def _confirm_sign_up(data):
    cid = data.get("ClientId")
    username = data.get("Username")
    code = data.get("ConfirmationCode", "")

    pool = None
    for p in _user_pools.values():
        if cid in p["_clients"]:
            pool = p
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    user = pool["_users"].get(username)
    if not user:
        return error_response_json("UserNotFoundException", "User does not exist.", 400)

    # Accept any code in emulation
    user["UserStatus"] = "CONFIRMED"
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _forgot_password(data):
    cid = data.get("ClientId")
    username = data.get("Username")

    pool = None
    for p in _user_pools.values():
        if cid in p["_clients"]:
            pool = p
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    user = pool["_users"].get(username)
    if not user:
        return error_response_json("UserNotFoundException", "User does not exist.", 400)

    user["_reset_code"] = "654321"
    attrs = _attr_list_to_dict(user.get("Attributes", []))
    return json_response({
        "CodeDeliveryDetails": {
            "Destination": attrs.get("email", ""),
            "DeliveryMedium": "EMAIL",
            "AttributeName": "email",
        }
    })


def _confirm_forgot_password(data):
    cid = data.get("ClientId")
    username = data.get("Username")
    new_password = data.get("Password", "")

    pool = None
    for p in _user_pools.values():
        if cid in p["_clients"]:
            pool = p
            break
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {cid} not found.", 400)

    user = pool["_users"].get(username)
    if not user:
        return error_response_json("UserNotFoundException", "User does not exist.", 400)

    # Accept any confirmation code in emulation (real AWS validates against issued code)
    user["_password"] = new_password
    user["UserStatus"] = "CONFIRMED"
    user["UserLastModifiedDate"] = _now_epoch()
    return json_response({})


def _change_password(data):
    access_token = data.get("AccessToken", "")
    if not access_token:
        return error_response_json("NotAuthorizedException", "Access token is missing.", 400)
    proposed = data.get("ProposedPassword", "")
    # Decode token to find user and update password
    for pool in _user_pools.values():
        user = _user_from_token(access_token, pool)
        if user:
            user["_password"] = proposed
            user["UserLastModifiedDate"] = _now_epoch()
            return json_response({})
    return error_response_json("NotAuthorizedException", "Invalid access token.", 400)


def _get_user(data):
    access_token = data.get("AccessToken", "")
    if not access_token:
        return error_response_json("NotAuthorizedException", "Access token is missing.", 400)
    for pool in _user_pools.values():
        user = _user_from_token(access_token, pool)
        if user:
            out = _user_out(user)
            # GetUser uses UserAttributes, not Attributes (per AWS API shape)
            out["UserAttributes"] = out.pop("Attributes", [])
            out["UserMFASettingList"] = user.get("_mfa_enabled", [])
            out["PreferredMfaSetting"] = user.get("_preferred_mfa", "")
            return json_response(out)
    return error_response_json("NotAuthorizedException", "Invalid access token.", 400)


def _update_user_attributes(data):
    access_token = data.get("AccessToken", "")
    if not access_token:
        return error_response_json("NotAuthorizedException", "Access token is missing.", 400)
    for pool in _user_pools.values():
        user = _user_from_token(access_token, pool)
        if user:
            user["Attributes"] = _merge_attributes(
                user.get("Attributes", []),
                data.get("UserAttributes", []),
            )
            user["UserLastModifiedDate"] = _now_epoch()
            return json_response({"CodeDeliveryDetailsList": []})
    return error_response_json("NotAuthorizedException", "Invalid access token.", 400)


def _delete_user(data):
    access_token = data.get("AccessToken", "")
    if not access_token:
        return error_response_json("NotAuthorizedException", "Access token is missing.", 400)
    for pool in _user_pools.values():
        user = _user_from_token(access_token, pool)
        if user:
            username = user["Username"]
            del pool["_users"][username]
            pool["EstimatedNumberOfUsers"] = len(pool["_users"])
            return json_response({})
    return error_response_json("NotAuthorizedException", "Invalid access token.", 400)


# ===========================================================================
# GROUPS
# ===========================================================================

def _create_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    name = data.get("GroupName")
    if not name:
        return error_response_json("InvalidParameterException", "GroupName is required.", 400)
    if name in pool["_groups"]:
        return error_response_json("GroupExistsException", f"Group {name} already exists.", 400)
    now = _now_epoch()
    group = {
        "GroupName": name,
        "UserPoolId": pid,
        "Description": data.get("Description", ""),
        "RoleArn": data.get("RoleArn", ""),
        "Precedence": data.get("Precedence", 0),
        "CreationDate": now,
        "LastModifiedDate": now,
        "_members": [],
    }
    pool["_groups"][name] = group
    return json_response({"Group": _group_out(group)})


def _delete_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    name = data.get("GroupName")
    if name not in pool["_groups"]:
        return error_response_json("ResourceNotFoundException", f"Group {name} not found.", 400)
    # Remove group from all member users
    for username in pool["_groups"][name].get("_members", []):
        user = pool["_users"].get(username)
        if user and name in user.get("_groups", []):
            user["_groups"].remove(name)
    del pool["_groups"][name]
    return json_response({})


def _get_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    name = data.get("GroupName")
    group = pool["_groups"].get(name)
    if not group:
        return error_response_json("ResourceNotFoundException", f"Group {name} not found.", 400)
    return json_response({"Group": _group_out(group)})


def _list_groups(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    limit = min(data.get("Limit", 60), 60)
    next_token = data.get("NextToken")
    groups = sorted(pool["_groups"].values(), key=lambda g: g["GroupName"])
    start = int(next_token) if next_token else 0
    page = groups[start:start + limit]
    resp = {"Groups": [_group_out(g) for g in page]}
    if start + limit < len(groups):
        resp["NextToken"] = str(start + limit)
    return json_response(resp)


def _group_out(group: dict) -> dict:
    return {k: v for k, v in group.items() if not k.startswith("_")}


def _list_users_in_group(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    name = data.get("GroupName")
    group = pool["_groups"].get(name)
    if not group:
        return error_response_json("ResourceNotFoundException", f"Group {name} not found.", 400)
    limit = min(data.get("Limit", 60), 60)
    next_token = data.get("NextToken")
    members = group.get("_members", [])
    start = int(next_token) if next_token else 0
    page = members[start:start + limit]
    users = [_user_out(pool["_users"][u]) for u in page if u in pool["_users"]]
    resp = {"Users": users}
    if start + limit < len(members):
        resp["NextToken"] = str(start + limit)
    return json_response(resp)


# ===========================================================================
# DOMAIN
# ===========================================================================

def _create_user_pool_domain(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    domain = data.get("Domain")
    if not domain:
        return error_response_json("InvalidParameterException", "Domain is required.", 400)
    if domain in _pool_domain_map:
        return error_response_json("InvalidParameterException", f"Domain {domain} already exists.", 400)
    pool["Domain"] = domain
    _pool_domain_map[domain] = pid
    return json_response({"CloudFrontDomain": f"{domain}.auth.{REGION}.amazoncognito.com"})


def _delete_user_pool_domain(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    domain = data.get("Domain")
    _pool_domain_map.pop(domain, None)
    pool["Domain"] = None
    return json_response({})


def _describe_user_pool_domain(data):
    domain = data.get("Domain")
    pid = _pool_domain_map.get(domain)
    if not pid:
        return json_response({"DomainDescription": {}})
    pool = _user_pools.get(pid, {})
    return json_response({
        "DomainDescription": {
            "UserPoolId": pid,
            "AWSAccountId": ACCOUNT_ID,
            "Domain": domain,
            "S3Bucket": "",
            "CloudFrontDistribution": f"{domain}.auth.{REGION}.amazoncognito.com",
            "Version": "1",
            "Status": "ACTIVE",
            "CustomDomainConfig": {},
        }
    })


# ===========================================================================
# MFA CONFIG
# ===========================================================================

def _get_user_pool_mfa_config(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    return json_response({
        "SmsMfaConfiguration": pool.get("SmsMfaConfiguration", {}),
        "SoftwareTokenMfaConfiguration": pool.get("SoftwareTokenMfaConfiguration", {"Enabled": False}),
        "MfaConfiguration": pool.get("MfaConfiguration", "OFF"),
    })


def _admin_set_user_mfa_preference(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    username = data.get("Username")
    user, err = _resolve_user(pool, username)
    if err:
        return err
    _apply_mfa_preference(user, data)
    return json_response({})


def _set_user_mfa_preference(data):
    """Public (user-facing) version — resolves user from AccessToken."""
    access_token = data.get("AccessToken")
    if not access_token:
        return error_response_json("NotAuthorizedException", "Missing access token.", 400)
    for pool in _user_pools.values():
        user = _user_from_token(access_token, pool)
        if user:
            _apply_mfa_preference(user, data)
            return json_response({})
    return error_response_json("NotAuthorizedException", "Invalid access token.", 400)


def _apply_mfa_preference(user: dict, data: dict):
    """Shared logic for Admin and user-facing SetUserMFAPreference."""
    totp_settings = data.get("SoftwareTokenMfaSettings", {})
    sms_settings = data.get("SMSMfaSettings", {})
    enabled_mfa = user.setdefault("_mfa_enabled", [])

    if totp_settings.get("Enabled"):
        if "SOFTWARE_TOKEN_MFA" not in enabled_mfa:
            enabled_mfa.append("SOFTWARE_TOKEN_MFA")
        if totp_settings.get("PreferredMfa"):
            user["_preferred_mfa"] = "SOFTWARE_TOKEN_MFA"
    elif "Enabled" in totp_settings and not totp_settings["Enabled"]:
        enabled_mfa[:] = [m for m in enabled_mfa if m != "SOFTWARE_TOKEN_MFA"]
        if user.get("_preferred_mfa") == "SOFTWARE_TOKEN_MFA":
            user["_preferred_mfa"] = ""

    if sms_settings.get("Enabled"):
        if "SMS_MFA" not in enabled_mfa:
            enabled_mfa.append("SMS_MFA")
        if sms_settings.get("PreferredMfa"):
            user["_preferred_mfa"] = "SMS_MFA"
    elif "Enabled" in sms_settings and not sms_settings["Enabled"]:
        enabled_mfa[:] = [m for m in enabled_mfa if m != "SMS_MFA"]
        if user.get("_preferred_mfa") == "SMS_MFA":
            user["_preferred_mfa"] = ""


def _set_user_pool_mfa_config(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    if "SmsMfaConfiguration" in data:
        pool["SmsMfaConfiguration"] = data["SmsMfaConfiguration"]
    if "SoftwareTokenMfaConfiguration" in data:
        pool["SoftwareTokenMfaConfiguration"] = data["SoftwareTokenMfaConfiguration"]
    if "MfaConfiguration" in data:
        pool["MfaConfiguration"] = data["MfaConfiguration"]
    pool["LastModifiedDate"] = _now_epoch()
    return json_response({
        "SmsMfaConfiguration": pool.get("SmsMfaConfiguration", {}),
        "SoftwareTokenMfaConfiguration": pool.get("SoftwareTokenMfaConfiguration", {}),
        "MfaConfiguration": pool.get("MfaConfiguration", "OFF"),
    })


def _associate_software_token(data):
    """Issue a stub TOTP secret. Works with both AccessToken and Session."""
    secret = base64.b32encode(secrets.token_bytes(20)).decode()
    session = base64.b64encode(secrets.token_bytes(32)).decode()
    return json_response({"SecretCode": secret, "Session": session})


def _verify_software_token(data):
    """Accept any TOTP code. Mark the user as TOTP-enrolled so auth flow issues the challenge."""
    access_token = data.get("AccessToken")
    user_code = data.get("UserCode", "")  # accepted regardless of value in emulator
    friendly_name = data.get("FriendlyDeviceName", "TOTP device")

    if access_token:
        # Find the user by token across all pools
        for pool in _user_pools.values():
            user = _user_from_token(access_token, pool)
            if user:
                user.setdefault("_mfa_enabled", [])
                if "SOFTWARE_TOKEN_MFA" not in user["_mfa_enabled"]:
                    user["_mfa_enabled"].append("SOFTWARE_TOKEN_MFA")
                user["_preferred_mfa"] = "SOFTWARE_TOKEN_MFA"
                break

    return json_response({"Status": "SUCCESS"})


# ===========================================================================
# IDP TAGS
# ===========================================================================

def _idp_tag_resource(data):
    arn = data.get("ResourceArn", "")
    tags = data.get("Tags", {})
    # Find pool by ARN
    for pool in _user_pools.values():
        if pool["Arn"] == arn:
            pool["UserPoolTags"].update(tags)
            return json_response({})
    return error_response_json("ResourceNotFoundException", f"Resource {arn} not found.", 400)


def _idp_untag_resource(data):
    arn = data.get("ResourceArn", "")
    tag_keys = data.get("TagKeys", [])
    for pool in _user_pools.values():
        if pool["Arn"] == arn:
            for k in tag_keys:
                pool["UserPoolTags"].pop(k, None)
            return json_response({})
    return error_response_json("ResourceNotFoundException", f"Resource {arn} not found.", 400)


def _idp_list_tags_for_resource(data):
    arn = data.get("ResourceArn", "")
    for pool in _user_pools.values():
        if pool["Arn"] == arn:
            return json_response({"Tags": pool.get("UserPoolTags", {})})
    return error_response_json("ResourceNotFoundException", f"Resource {arn} not found.", 400)


# ===========================================================================
# OAUTH2 TOKEN ENDPOINT (data plane)
# ===========================================================================

def _oauth2_token(data, query_params, raw_body: bytes = b""):
    """Stub /oauth2/token endpoint — returns fake tokens for client_credentials flow.
    Body is application/x-www-form-urlencoded, not JSON.
    """
    # Parse form-encoded body first, fall back to query_params
    form: dict = {}
    if raw_body:
        try:
            parsed = parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
            form = {k: v[0] for k, v in parsed.items()}
        except Exception:
            pass

    def _get(key):
        if key in form:
            return form[key]
        v = query_params.get(key, "")
        return v[0] if isinstance(v, list) else v

    client_id = _get("client_id")
    now = int(time.time())
    pool_id = ""
    for pid, pool in _user_pools.items():
        if client_id in pool["_clients"]:
            pool_id = pid
            break
    access_token = _fake_token(client_id or new_uuid(), pool_id, client_id or "", "access")
    return json_response({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
    })


# ===========================================================================
# IDENTITY POOLS (cognito-identity)
# ===========================================================================

def _create_identity_pool(data):
    name = data.get("IdentityPoolName")
    if not name:
        return error_response_json("InvalidParameterException", "IdentityPoolName is required.", 400)
    iid = _identity_pool_id()
    pool = {
        "IdentityPoolId": iid,
        "IdentityPoolName": name,
        "AllowUnauthenticatedIdentities": data.get("AllowUnauthenticatedIdentities", False),
        "AllowClassicFlow": data.get("AllowClassicFlow", False),
        "SupportedLoginProviders": data.get("SupportedLoginProviders", {}),
        "DeveloperProviderName": data.get("DeveloperProviderName", ""),
        "OpenIdConnectProviderARNs": data.get("OpenIdConnectProviderARNs", []),
        "CognitoIdentityProviders": data.get("CognitoIdentityProviders", []),
        "SamlProviderARNs": data.get("SamlProviderARNs", []),
        "IdentityPoolTags": data.get("IdentityPoolTags", {}),
        "_roles": {},
        "_identities": {},
    }
    _identity_pools[iid] = pool
    return json_response(_identity_pool_out(pool))


def _delete_identity_pool(data):
    iid = data.get("IdentityPoolId")
    if iid not in _identity_pools:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    del _identity_pools[iid]
    _identity_tags.pop(iid, None)
    return json_response({})


def _describe_identity_pool(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    return json_response(_identity_pool_out(pool))


def _list_identity_pools(data):
    max_results = min(data.get("MaxResults", 60), 60)
    next_token = data.get("NextToken")
    pools = sorted(_identity_pools.values(), key=lambda p: p["IdentityPoolId"])
    start = int(next_token) if next_token else 0
    page = pools[start:start + max_results]
    resp = {
        "IdentityPools": [
            {"IdentityPoolId": p["IdentityPoolId"], "IdentityPoolName": p["IdentityPoolName"]}
            for p in page
        ]
    }
    if start + max_results < len(pools):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


def _update_identity_pool(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    updatable = {
        "IdentityPoolName", "AllowUnauthenticatedIdentities", "AllowClassicFlow",
        "SupportedLoginProviders", "DeveloperProviderName", "OpenIdConnectProviderARNs",
        "CognitoIdentityProviders", "SamlProviderARNs", "IdentityPoolTags",
    }
    for k in updatable:
        if k in data:
            pool[k] = data[k]
    return json_response(_identity_pool_out(pool))


def _get_id(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    identity_id = _identity_id(iid)
    pool["_identities"][identity_id] = {
        "IdentityId": identity_id,
        "Logins": data.get("Logins", {}),
        "CreationDate": _now_epoch(),
        "LastModifiedDate": _now_epoch(),
    }
    return json_response({"IdentityId": identity_id})


def _get_credentials_for_identity(data):
    identity_id = data.get("IdentityId", new_uuid())
    now = int(time.time())
    return json_response({
        "IdentityId": identity_id,
        "Credentials": {
            "AccessKeyId": f"ASIA{''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(16))}",
            "SecretKey": base64.b64encode(secrets.token_bytes(30)).decode(),
            "SessionToken": base64.b64encode(secrets.token_bytes(64)).decode(),
            "Expiration": now + 3600,
        },
    })


def _get_open_id_token(data):
    identity_id = data.get("IdentityId", new_uuid())
    # Find pool containing this identity
    pool_id = ""
    for iid, pool in _identity_pools.items():
        if identity_id in pool["_identities"]:
            pool_id = iid
            break
    token = _fake_token(identity_id, pool_id, "", "id")
    return json_response({"IdentityId": identity_id, "Token": token})


def _set_identity_pool_roles(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    pool["_roles"] = data.get("Roles", {})
    return json_response({})


def _get_identity_pool_roles(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    return json_response({
        "IdentityPoolId": iid,
        "Roles": pool.get("_roles", {}),
        "RoleMappings": {},
    })


def _list_identities(data):
    iid = data.get("IdentityPoolId")
    pool = _identity_pools.get(iid)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Identity pool {iid} not found.", 400)
    max_results = min(data.get("MaxResults", 60), 60)
    identities = list(pool["_identities"].values())[:max_results]
    return json_response({
        "IdentityPoolId": iid,
        "Identities": [
            {"IdentityId": i["IdentityId"], "Logins": list(i.get("Logins", {}).keys()),
             "CreationDate": i["CreationDate"], "LastModifiedDate": i["LastModifiedDate"]}
            for i in identities
        ],
    })


def _describe_identity(data):
    identity_id = data.get("IdentityId")
    for pool in _identity_pools.values():
        identity = pool["_identities"].get(identity_id)
        if identity:
            return json_response({
                "IdentityId": identity_id,
                "Logins": list(identity.get("Logins", {}).keys()),
                "CreationDate": identity["CreationDate"],
                "LastModifiedDate": identity["LastModifiedDate"],
            })
    return error_response_json("ResourceNotFoundException", f"Identity {identity_id} not found.", 400)


def _merge_developer_identities(data):
    # Stub — return a new identity id
    return json_response({"IdentityId": _identity_id(data.get("IdentityPoolId", ""))})


def _unlink_developer_identity(data):
    return json_response({})


def _unlink_identity(data):
    return json_response({})


def _identity_tag_resource(data):
    arn = data.get("ResourceArn", "")
    tags = data.get("Tags", {})
    # ARN format: arn:aws:cognito-identity:region:account:identitypool/id
    iid = arn.split("/")[-1] if "/" in arn else arn
    _identity_tags.setdefault(iid, {}).update(tags)
    return json_response({})


def _identity_untag_resource(data):
    arn = data.get("ResourceArn", "")
    tag_keys = data.get("TagKeys", [])
    iid = arn.split("/")[-1] if "/" in arn else arn
    for k in tag_keys:
        _identity_tags.get(iid, {}).pop(k, None)
    return json_response({})


def _identity_list_tags_for_resource(data):
    arn = data.get("ResourceArn", "")
    iid = arn.split("/")[-1] if "/" in arn else arn
    return json_response({"Tags": _identity_tags.get(iid, {})})


def _identity_pool_out(pool: dict) -> dict:
    return {k: v for k, v in pool.items() if not k.startswith("_")}


# ===========================================================================
# MISC HELPERS
# ===========================================================================

def _generate_temp_password() -> str:
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits + "!@#$%"
    return "".join(secrets.choice(chars) for _ in range(12))


def _apply_user_filter(users: list, filter_str: str) -> list:
    """
    Supports simple Cognito filter syntax:
      attribute_name = "value"
      attribute_name ^= "value"   (starts with)
      attribute_name != "value"
    """
    m = re.match(r'(\w+)\s*(=|\^=|!=)\s*"([^"]*)"', filter_str.strip())
    if not m:
        return users
    attr_name, op, value = m.group(1), m.group(2), m.group(3)
    result = []
    for user in users:
        attr_dict = _attr_list_to_dict(user.get("Attributes", []))
        # Also check top-level fields like username, status
        field_val = attr_dict.get(attr_name, "")
        if attr_name == "username":
            field_val = user.get("Username", "")
        elif attr_name == "status":
            field_val = user.get("UserStatus", "")
        elif attr_name == "email_verified":
            field_val = attr_dict.get("email_verified", "")
        if op == "=" and field_val == value:
            result.append(user)
        elif op == "^=" and field_val.startswith(value):
            result.append(user)
        elif op == "!=" and field_val != value:
            result.append(user)
    return result


# ===========================================================================
# RESET
# ===========================================================================

def reset():
    _user_pools.clear()
    _pool_domain_map.clear()
    _identity_pools.clear()
    _identity_tags.clear()
