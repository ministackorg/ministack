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
  CreateIdentityProvider, DescribeIdentityProvider, UpdateIdentityProvider,
  DeleteIdentityProvider, ListIdentityProviders, GetIdentityProviderByIdentifier,
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

Data-plane endpoints (path-based, form-encoded):
  GET  /oauth2/authorize   — redirect to external SAML/OIDC IdP
  POST /saml2/idpresponse  — receive SAML assertion, create user, issue auth code
  POST /oauth2/token       — exchange authorization_code or client_credentials for tokens

Wire protocol:
  Both services use JSON with X-Amz-Target header.
  cognito-idp  credential scope: cognito-idp
  cognito-identity credential scope: cognito-identity
  Routing is handled in app.py via two separate SERVICE_HANDLERS entries.
"""

import base64
import copy
import json
import logging
import os
import re
import secrets
import string
import time
import zlib
from datetime import datetime, timezone
import hashlib
import html as html_mod
from urllib.parse import parse_qs, urlencode, quote
from xml.etree.ElementTree import Element, SubElement, tostring as xml_tostring

from defusedxml.ElementTree import fromstring as safe_xml_parse

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, get_region

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
    r = region or get_region()
    issuer = f"https://cognito-idp.{r}.amazonaws.com/{pool_id}"
    doc = {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/.well-known/jwks.json",
        "authorization_endpoint": f"{issuer}/oauth2/authorize",
        "token_endpoint": f"{issuer}/oauth2/token",
        "userinfo_endpoint": f"{issuer}/oauth2/userInfo",
        "end_session_endpoint": f"{issuer}/logout",
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "scopes_supported": ["openid", "email", "phone", "profile"],
        "token_endpoint_auth_methods_supported": ["client_secret_basic", "client_secret_post"],
        "claims_supported": [
            "sub", "iss", "aud", "exp", "iat", "auth_time",
            "email", "email_verified", "name", "phone_number",
            "phone_number_verified", "cognito:username", "cognito:groups",
        ],
    }
    return 200, {"Content-Type": "application/json"}, json.dumps(doc).encode()

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_MINISTACK_PORT = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))

# SAML XML namespaces
_SAML_NS = {
    "samlp": "urn:oasis:names:tc:SAML:2.0:protocol",
    "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
}

# ---------------------------------------------------------------------------
# In-memory state — User Pools (cognito-idp)
# ---------------------------------------------------------------------------

_user_pools = AccountScopedDict()
# pool_id -> {
#   Id, Name, Arn, CreationDate, LastModifiedDate, Status,
#   Policies, Schema, AutoVerifiedAttributes, UsernameAttributes,
#   MfaConfiguration, EstimatedNumberOfUsers,
#   AdminCreateUserConfig, UserPoolTags,
#   Domain (str|None),
#   _clients: {client_id -> client_dict},
#   _users:   {username -> user_dict},
#   _groups:  {group_name -> group_dict},
#   _identity_providers: {provider_name -> provider_dict},
# }

_pool_domain_map = AccountScopedDict()   # domain -> pool_id

# ---------------------------------------------------------------------------
# In-memory state — OAuth2 Authorization Codes & Refresh Tokens
# ---------------------------------------------------------------------------

_authorization_codes: dict[str, dict] = {}   # code -> {client_id, pool_id, redirect_uri, scope, username, nonce, expires_at, code_challenge, code_challenge_method}
_refresh_tokens: dict[str, dict] = {}        # refresh_token_value -> {pool_id, client_id, username, scope}

# ---------------------------------------------------------------------------
# In-memory state — Identity Pools (cognito-identity)
# ---------------------------------------------------------------------------

_identity_pools = AccountScopedDict()
# identity_pool_id -> {
#   IdentityPoolId, IdentityPoolName, AllowUnauthenticatedIdentities,
#   SupportedLoginProviders, DeveloperProviderName,
#   OpenIdConnectProviderARNs, CognitoIdentityProviders,
#   SamlProviderARNs, IdentityPoolTags,
#   _roles: {authenticated: arn, unauthenticated: arn},
#   _identities: {identity_id -> identity_dict},
# }

_identity_tags = AccountScopedDict()   # identity_pool_id -> {key: value}

# ---------------------------------------------------------------------------
# In-memory state — OAuth2 hosted-UI / federation codes
# ---------------------------------------------------------------------------
# `_auth_codes` and `_authorization_codes` are intentionally plain dicts
# (NOT AccountScopedDict). The OAuth2 token endpoint is a public callback
# with no AWS authentication context — it identifies the caller via the
# random code + client_id + client_secret, not via a SigV4 access key.
# Wrapping these in AccountScopedDict would make the callback lookup
# happen under a default account, invisible to codes issued under any
# other tenant. Effective tenant isolation is provided by the random
# unguessable token namespace. See
# tests/test_cognito_auth_codes_persistence.py for a regression pin.

_auth_codes = {}   # code -> {pool_id, client_id, username, redirect_uri, scopes, state, created_at}
_AUTH_CODE_TTL = 300  # 5 minutes


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "user_pools": copy.deepcopy(_user_pools),
        "pool_domain_map": copy.deepcopy(_pool_domain_map),
        "identity_pools": copy.deepcopy(_identity_pools),
        "identity_tags": copy.deepcopy(_identity_tags),
        "authorization_codes": copy.deepcopy(_authorization_codes),
        "refresh_tokens": copy.deepcopy(_refresh_tokens),
        "auth_codes": copy.deepcopy(_auth_codes),
    }


def restore_state(data):
    if data:
        _user_pools.update(data.get("user_pools", {}))
        _pool_domain_map.update(data.get("pool_domain_map", {}))
        _identity_pools.update(data.get("identity_pools", {}))
        _identity_tags.update(data.get("identity_tags", {}))
        _authorization_codes.update(data.get("authorization_codes", {}))
        _refresh_tokens.update(data.get("refresh_tokens", {}))
        _auth_codes.update(data.get("auth_codes", {}))


try:
    _restored = load_state("cognito")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def _pool_arn(pool_id: str) -> str:
    return f"arn:aws:cognito-idp:{get_region()}:{get_account_id()}:userpool/{pool_id}"


def _pool_id() -> str:
    suffix = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(26))
    return f"{get_region()}_{suffix[:9]}"


def _client_id() -> str:
    return "".join(secrets.choice(string.digits + string.ascii_letters) for _ in range(26))


def _client_secret() -> str:
    return base64.b64encode(secrets.token_bytes(48)).decode()


def _identity_pool_id() -> str:
    return f"{get_region()}:{new_uuid()}"


def _identity_id(pool_id: str) -> str:
    return f"{get_region()}:{new_uuid()}"


def _fake_token(sub: str, pool_id: str, client_id: str, token_type: str = "access",
                 username: str = "", user_attrs: dict | None = None,
                 groups: list[str] | None = None) -> str:
    """Return a JWT signed with the RSA key when cryptography is available."""
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "kid": "ministack-key-1"}).encode()
    ).rstrip(b"=").decode()
    now = int(time.time())
    origin_jti = new_uuid()
    claims = {
        "sub": sub,
        "iss": f"https://cognito-idp.{get_region()}.amazonaws.com/{pool_id}",
        "token_use": token_type,
        "iat": now,
        "exp": now + 3600,
        "jti": new_uuid(),
    }
    if token_type == "id":
        # IdToken uses 'aud' (not 'client_id') per OIDC spec
        claims["aud"] = client_id
        claims["auth_time"] = now
        claims["origin_jti"] = origin_jti
        if username:
            claims["cognito:username"] = username
        if groups:
            claims["cognito:groups"] = groups
        # Include user attributes in IdToken
        if user_attrs:
            for k, v in user_attrs.items():
                if k == "sub":
                    continue
                claims[k] = v
            if "email" in user_attrs:
                claims.setdefault("email_verified", True)
    elif token_type == "access":
        claims["client_id"] = client_id
        claims["auth_time"] = now
        claims["origin_jti"] = origin_jti
        claims["scope"] = "aws.cognito.signin.user.admin"
        if username:
            claims["username"] = username
        if groups:
            claims["cognito:groups"] = groups
    else:
        # RefreshToken — opaque in real AWS, but we use a JWT stub for simplicity
        claims["client_id"] = client_id
    payload = base64.urlsafe_b64encode(
        json.dumps(claims).encode()
    ).rstrip(b"=").decode()
    signing_input = f"{header}.{payload}".encode()
    if _RSA_PRIVATE_KEY is not None:
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import hashes
        sig_bytes = _RSA_PRIVATE_KEY.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        sig = base64.urlsafe_b64encode(sig_bytes).rstrip(b"=").decode()
    else:
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
        # Real AWS also accepts the user's 'sub' UUID as Username.
        for u in pool["_users"].values():
            attrs = _attr_list_to_dict(u.get("Attributes", []))
            if attrs.get("sub") == username:
                user = u
                break
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
# SAML / OAuth2 helpers
# ---------------------------------------------------------------------------

def _acs_url() -> str:
    """Assertion Consumer Service URL for SAML responses."""
    return f"http://{_MINISTACK_HOST}:{_MINISTACK_PORT}/saml2/idpresponse"


def _build_saml_authn_request(pool_id: str, destination: str) -> str:
    """Build a minimal SAML AuthnRequest, deflate + base64-encode for HTTP-Redirect binding."""
    req = Element("{urn:oasis:names:tc:SAML:2.0:protocol}AuthnRequest")
    req.set("ID", "_" + new_uuid())
    req.set("Version", "2.0")
    req.set("IssueInstant", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    req.set("AssertionConsumerServiceURL", _acs_url())
    req.set("Destination", destination)
    req.set("ProtocolBinding", "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST")
    issuer = SubElement(req, "{urn:oasis:names:tc:SAML:2.0:assertion}Issuer")
    issuer.text = f"urn:amazon:cognito:sp:{pool_id}"
    xml_bytes = xml_tostring(req, encoding="unicode").encode("utf-8")
    # Raw deflate (strip zlib header/checksum) per SAML HTTP-Redirect binding
    deflated = zlib.compress(xml_bytes)[2:-4]
    return base64.b64encode(deflated).decode()


def _parse_saml_response(saml_response_b64: str) -> dict:
    """Decode and parse a SAML Response, extract NameID and attributes."""
    xml_bytes = base64.b64decode(saml_response_b64)
    root = safe_xml_parse(xml_bytes)
    name_id_el = root.find(".//{urn:oasis:names:tc:SAML:2.0:assertion}Subject/"
                           "{urn:oasis:names:tc:SAML:2.0:assertion}NameID")
    name_id = name_id_el.text if name_id_el is not None else None
    attrs = {}
    for attr_el in root.findall(".//{urn:oasis:names:tc:SAML:2.0:assertion}AttributeStatement/"
                                "{urn:oasis:names:tc:SAML:2.0:assertion}Attribute"):
        attr_name = attr_el.get("Name", "")
        val_el = attr_el.find("{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue")
        if val_el is not None and val_el.text:
            attrs[attr_name] = val_el.text
    return {"name_id": name_id, "attributes": attrs}


def _all_pools():
    """Iterate ALL user pools across ALL accounts.

    OAuth2 endpoints are accessed by browsers without AWS credentials, so the
    normal account-scoped iteration would miss pools created under a specific
    account.  Yields (pool_id, pool_dict) pairs.
    """
    # _user_pools._data stores {(account_id, pool_id): pool_dict}
    for (_, pid), pool in _user_pools._data.items():
        yield pid, pool


def _get_pool_unscoped(pool_id: str):
    """Look up a pool by ID across ALL accounts."""
    for pid, pool in _all_pools():
        if pid == pool_id:
            return pool
    return None


def _find_pool_by_client_id(client_id: str):
    """Return (pool_id, pool, client) or (None, None, None).

    Searches across ALL accounts because OAuth2 endpoints are accessed by
    browsers without AWS credentials, so the normal account-scoped lookup
    would miss pools created under a specific account.
    """
    for pid, pool in _all_pools():
        client = pool["_clients"].get(client_id)
        if client is not None:
            return pid, pool, client
    return None, None, None


def _cleanup_expired_relay_codes():
    """Remove SAML/OIDC relay auth codes older than _AUTH_CODE_TTL."""
    now = time.time()
    expired = [k for k, v in _auth_codes.items() if now - v.get("created_at", 0) > _AUTH_CODE_TTL]
    for k in expired:
        del _auth_codes[k]


def _authenticate_client(headers: dict, form: dict):
    """Extract client_id / client_secret from Basic auth header or form body."""
    auth = headers.get("authorization", "") if headers else ""
    if auth.lower().startswith("basic "):
        try:
            decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
            cid, csec = decoded.split(":", 1)
            return cid, csec
        except Exception:
            pass
    return form.get("client_id", ""), form.get("client_secret", "")


def _generate_auth_code() -> str:
    return secrets.token_urlsafe(32)


def _cleanup_expired_codes():
    now = time.time()
    expired = [code for code, entry in _authorization_codes.items() if entry["expires_at"] < now]
    for code in expired:
        del _authorization_codes[code]


def _verify_pkce(code_verifier: str, code_challenge: str, method: str) -> bool:
    if method == "S256":
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return computed == code_challenge
    # plain
    return code_verifier == code_challenge


def _qp(query_params: dict, key: str, default: str = "") -> str:
    """Extract a single value from query_params (which may have list values)."""
    v = query_params.get(key, default)
    return v[0] if isinstance(v, list) else v


# ---------------------------------------------------------------------------
# Entry points — two separate handle_request functions
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    """Unified entry point — dispatches to IDP or Identity based on target prefix."""
    target = headers.get("x-amz-target", "")

    # Path-based endpoints (form-encoded or no body — must run before JSON parse)
    if path.startswith("/oauth2/authorize"):
        return handle_oauth2_authorize(method, path, headers, query_params)
    if path.startswith("/saml2/idpresponse"):
        return _saml2_idp_response(body, query_params)
    if path.startswith("/oauth2/token"):
        return _oauth2_token({}, query_params, body, headers)

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
        # Identity Providers
        "CreateIdentityProvider": _create_identity_provider,
        "DescribeIdentityProvider": _describe_identity_provider,
        "UpdateIdentityProvider": _update_identity_provider,
        "DeleteIdentityProvider": _delete_identity_provider,
        "ListIdentityProviders": _list_identity_providers,
        "GetIdentityProviderByIdentifier": _get_identity_provider_by_identifier,
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
        "EstimatedNumberOfUsers": 0,
        "EmailConfiguration": data.get("EmailConfiguration", {}),
        "SmsConfiguration": data.get("SmsConfiguration", {}),
        "UserPoolTags": data.get("UserPoolTags", {}),
        "AdminCreateUserConfig": data.get("AdminCreateUserConfig", {
            "AllowAdminCreateUserOnly": False,
            "UnusedAccountValidityDays": 7,
        }),
        "AccountRecoverySetting": data.get("AccountRecoverySetting", {}),
        "DeletionProtection": data.get("DeletionProtection", "INACTIVE"),
        "Domain": None,
        "_clients": {},
        "_users": {},
        "_groups": {},
        "_identity_providers": {},
    }
    if data.get("DeviceConfiguration"):
        pool["DeviceConfiguration"] = data["DeviceConfiguration"]
    if data.get("UsernameConfiguration"):
        pool["UsernameConfiguration"] = data["UsernameConfiguration"]
    if data.get("UserPoolAddOns"):
        pool["UserPoolAddOns"] = data["UserPoolAddOns"]
    if data.get("VerificationMessageTemplate"):
        pool["VerificationMessageTemplate"] = data["VerificationMessageTemplate"]
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
        "AnalyticsConfiguration": data.get("AnalyticsConfiguration"),
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


def _validate_password(pool, password):
    """Validate password against the pool's PasswordPolicy. Returns error response or None."""
    policy = pool.get("Policies", {}).get("PasswordPolicy", {})
    min_len = policy.get("MinimumLength", 8)
    errors = []
    if len(password) < min_len:
        errors.append(f"Password must have length greater than or equal to {min_len}")
    if policy.get("RequireUppercase", True) and not any(c.isupper() for c in password):
        errors.append("Password must have uppercase characters")
    if policy.get("RequireLowercase", True) and not any(c.islower() for c in password):
        errors.append("Password must have lowercase characters")
    if policy.get("RequireNumbers", True) and not any(c.isdigit() for c in password):
        errors.append("Password must have numeric characters")
    if policy.get("RequireSymbols", True) and not any(c in "^$*.[]{}()?-\"!@#%&/\\,><':;|_~`+=" for c in password):
        errors.append("Password must have symbol characters")
    if errors:
        return error_response_json(
            "InvalidPasswordException",
            "Password did not conform with policy: " + "; ".join(errors),
            400,
        )
    return None


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
    pw_err = _validate_password(pool, temp_password)
    if pw_err:
        return pw_err
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
    new_pw = data.get("Password", "")
    pw_err = _validate_password(pool, new_pw)
    if pw_err:
        return pw_err
    user["_password"] = new_pw
    permanent = data.get("Permanent", False)
    if permanent:
        user["UserStatus"] = "CONFIRMED"
    else:
        user["UserStatus"] = "FORCE_CHANGE_PASSWORD"
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
    attrs = _attr_list_to_dict(user.get("Attributes", []))
    sub = attrs.get("sub", user["Username"])
    username = user.get("Username", "")
    groups = user.get("_groups", [])
    return {
        "AccessToken": _fake_token(sub, pool_id, client_id, "access", username=username, groups=groups),
        "IdToken": _fake_token(sub, pool_id, client_id, "id", username=username,
                               user_attrs=attrs, groups=groups),
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

    pw_err = _validate_password(pool, password)
    if pw_err:
        return pw_err

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
    pw_err = _validate_password(pool, new_password)
    if pw_err:
        return pw_err
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
            pw_err = _validate_password(pool, proposed)
            if pw_err:
                return pw_err
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
    return json_response({"CloudFrontDomain": f"{domain}.auth.{get_region()}.amazoncognito.com"})


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
            "AWSAccountId": get_account_id(),
            "Domain": domain,
            "S3Bucket": "",
            "CloudFrontDistribution": f"{domain}.auth.{get_region()}.amazoncognito.com",
            "Version": "1",
            "Status": "ACTIVE",
            "CustomDomainConfig": {},
        }
    })


# ===========================================================================
# IDENTITY PROVIDERS
# ===========================================================================

VALID_PROVIDER_TYPES = {"SAML", "Facebook", "Google", "LoginWithAmazon", "SignInWithApple", "OIDC"}


def _create_identity_provider(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    provider_name = data.get("ProviderName")
    if not provider_name:
        return error_response_json("InvalidParameterException", "ProviderName is required.", 400)
    provider_type = data.get("ProviderType")
    if not provider_type:
        return error_response_json("InvalidParameterException", "ProviderType is required.", 400)
    if provider_type not in VALID_PROVIDER_TYPES:
        return error_response_json("InvalidParameterException", f"Invalid ProviderType: {provider_type}.", 400)
    providers = pool.setdefault("_identity_providers", {})
    if provider_name in providers:
        return error_response_json("DuplicateProviderException",
                                   f"A provider with name {provider_name} already exists.", 400)
    idp_identifiers = data.get("IdpIdentifiers", [])
    # Check identifier uniqueness across all providers in this pool
    existing_ids = set()
    for p in providers.values():
        existing_ids.update(p.get("IdpIdentifiers", []))
    for ident in idp_identifiers:
        if ident in existing_ids:
            return error_response_json("DuplicateProviderException",
                                       f"IdpIdentifier {ident} is already in use.", 400)
    now = _now_epoch()
    provider = {
        "UserPoolId": pid,
        "ProviderName": provider_name,
        "ProviderType": provider_type,
        "ProviderDetails": data.get("ProviderDetails", {}),
        "AttributeMapping": data.get("AttributeMapping", {}),
        "IdpIdentifiers": idp_identifiers,
        "CreationDate": now,
        "LastModifiedDate": now,
    }
    providers[provider_name] = provider
    logger.info("Cognito: CreateIdentityProvider %s in pool %s", provider_name, pid)
    return json_response({"IdentityProvider": provider})


def _describe_identity_provider(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    provider_name = data.get("ProviderName")
    providers = pool.get("_identity_providers", {})
    provider = providers.get(provider_name)
    if not provider:
        return error_response_json("ResourceNotFoundException",
                                   f"Identity provider {provider_name} does not exist.", 400)
    return json_response({"IdentityProvider": provider})


def _update_identity_provider(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    provider_name = data.get("ProviderName")
    providers = pool.get("_identity_providers", {})
    provider = providers.get(provider_name)
    if not provider:
        return error_response_json("ResourceNotFoundException",
                                   f"Identity provider {provider_name} does not exist.", 400)
    if "ProviderDetails" in data:
        provider["ProviderDetails"] = data["ProviderDetails"]
    if "AttributeMapping" in data:
        provider["AttributeMapping"] = data["AttributeMapping"]
    if "IdpIdentifiers" in data:
        new_ids = data["IdpIdentifiers"]
        # Check uniqueness against other providers in the pool
        existing_ids = set()
        for name, p in providers.items():
            if name != provider_name:
                existing_ids.update(p.get("IdpIdentifiers", []))
        for ident in new_ids:
            if ident in existing_ids:
                return error_response_json("DuplicateProviderException",
                                           f"IdpIdentifier {ident} is already in use.", 400)
        provider["IdpIdentifiers"] = new_ids
    provider["LastModifiedDate"] = _now_epoch()
    return json_response({"IdentityProvider": provider})


def _delete_identity_provider(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    provider_name = data.get("ProviderName")
    providers = pool.get("_identity_providers", {})
    if provider_name not in providers:
        return error_response_json("ResourceNotFoundException",
                                   f"Identity provider {provider_name} does not exist.", 400)
    del providers[provider_name]
    logger.info("Cognito: DeleteIdentityProvider %s from pool %s", provider_name, pid)
    return json_response({})


def _list_identity_providers(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    providers = pool.get("_identity_providers", {})
    max_results = min(data.get("MaxResults", 60), 60)
    next_token = data.get("NextToken")
    sorted_providers = sorted(providers.values(), key=lambda p: p["CreationDate"])
    start = int(next_token) if next_token else 0
    page = sorted_providers[start:start + max_results]
    resp = {
        "Providers": [
            {
                "ProviderName": p["ProviderName"],
                "ProviderType": p["ProviderType"],
                "LastModifiedDate": p["LastModifiedDate"],
                "CreationDate": p["CreationDate"],
            }
            for p in page
        ]
    }
    if start + max_results < len(sorted_providers):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


def _get_identity_provider_by_identifier(data):
    pid = data.get("UserPoolId")
    pool, err = _resolve_pool(pid)
    if err:
        return err
    identifier = data.get("IdpIdentifier")
    if not identifier:
        return error_response_json("InvalidParameterException", "IdpIdentifier is required.", 400)
    for provider in pool.get("_identity_providers", {}).values():
        if identifier in provider.get("IdpIdentifiers", []):
            return json_response({"IdentityProvider": provider})
    return error_response_json("ResourceNotFoundException",
                               f"Identity provider with identifier {identifier} does not exist.", 400)


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
# OAUTH2 / OIDC / SAML ENDPOINTS (data plane)
# ===========================================================================

def _oauth2_error(error: str, description: str, status: int = 400):
    body = json.dumps({"error": error, "error_description": description}).encode()
    return status, {"Content-Type": "application/json"}, body


def _login_page_html(client_id, redirect_uri, scope, state, response_type,
                     code_challenge="", code_challenge_method="", nonce="",
                     error_message=""):
    esc = html_mod.escape
    err_block = ""
    if error_message:
        err_block = f'<div class="error">{esc(error_message)}</div>'
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sign in</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  background:#f0f2f5;display:flex;justify-content:center;align-items:center;min-height:100vh}}
.card{{background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);
  padding:40px;width:400px;max-width:90vw}}
h1{{font-size:24px;font-weight:600;color:#232f3e;margin-bottom:24px;text-align:center}}
label{{display:block;font-size:14px;font-weight:500;color:#545b64;margin-bottom:4px}}
input[type=text],input[type=password]{{width:100%;padding:10px 12px;border:1px solid #aab7b8;
  border-radius:4px;font-size:14px;margin-bottom:16px;outline:none;transition:border-color .15s}}
input[type=text]:focus,input[type=password]:focus{{border-color:#0073bb;box-shadow:0 0 0 2px rgba(0,115,187,.2)}}
button{{width:100%;padding:10px;background:#0073bb;color:#fff;border:none;border-radius:4px;
  font-size:16px;font-weight:500;cursor:pointer;transition:background .15s}}
button:hover{{background:#005a94}}
.error{{background:#fce8e6;color:#d13212;border:1px solid #d13212;border-radius:4px;
  padding:10px;margin-bottom:16px;font-size:13px;text-align:center}}
.footer{{text-align:center;margin-top:20px;font-size:12px;color:#879596}}
</style>
</head>
<body>
<div class="card">
<h1>Sign in</h1>
{err_block}
<form method="POST" action="/login">
<input type="hidden" name="client_id" value="{esc(client_id)}">
<input type="hidden" name="redirect_uri" value="{esc(redirect_uri)}">
<input type="hidden" name="scope" value="{esc(scope)}">
<input type="hidden" name="state" value="{esc(state)}">
<input type="hidden" name="response_type" value="{esc(response_type)}">
<input type="hidden" name="code_challenge" value="{esc(code_challenge)}">
<input type="hidden" name="code_challenge_method" value="{esc(code_challenge_method)}">
<input type="hidden" name="nonce" value="{esc(nonce)}">
<label for="username">Username</label>
<input type="text" id="username" name="username" autocomplete="username" required autofocus>
<label for="password">Password</label>
<input type="password" id="password" name="password" autocomplete="current-password" required>
<button type="submit">Sign in</button>
</form>
<div class="footer">Powered by ministack</div>
</div>
</body>
</html>"""


# -- /oauth2/authorize (GET) ------------------------------------------------

def _oauth2_authorize_federation(query_params):
    """Redirect to external IdP (SAML or OIDC) when identity_provider is specified."""
    response_type = _qp(query_params, "response_type")
    client_id = _qp(query_params, "client_id")
    redirect_uri = _qp(query_params, "redirect_uri")
    identity_provider = _qp(query_params, "identity_provider")
    state = _qp(query_params, "state")
    scope = _qp(query_params, "scope", "openid")

    if not client_id:
        return error_response_json("InvalidParameterException", "client_id is required.", 400)

    pool_id, pool, client = _find_pool_by_client_id(client_id)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"Client {client_id} not found.", 400)

    # Validate redirect_uri against CallbackURLs (skip if empty for dev convenience)
    callback_urls = client.get("CallbackURLs", [])
    if callback_urls and redirect_uri not in callback_urls:
        return error_response_json("InvalidParameterException",
                                   f"redirect_uri {redirect_uri} is not in CallbackURLs.", 400)

    if not identity_provider:
        return error_response_json("InvalidParameterException", "identity_provider is required.", 400)

    provider = pool.get("_identity_providers", {}).get(identity_provider)
    if not provider:
        return error_response_json("ResourceNotFoundException",
                                   f"Identity provider {identity_provider} not found.", 400)

    # Store relay context for the callback
    _cleanup_expired_relay_codes()
    relay_key = secrets.token_urlsafe(24)
    _auth_codes[relay_key] = {
        "type": "relay",
        "pool_id": pool_id,
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": scope,
        "provider_name": identity_provider,
        "created_at": time.time(),
    }

    provider_type = provider.get("ProviderType", "")
    details = provider.get("ProviderDetails", {})

    if provider_type == "SAML":
        # Resolve IdP SSO URL: IDPSSOEndpoint > MetadataURL
        sso_url = details.get("IDPSSOEndpoint") or details.get("MetadataURL", "")
        if not sso_url:
            return error_response_json("InvalidParameterException",
                                       "SAML provider has no IDPSSOEndpoint or MetadataURL.", 400)
        saml_request = _build_saml_authn_request(pool_id, sso_url)
        redirect_url = sso_url + ("&" if "?" in sso_url else "?") + urlencode({
            "SAMLRequest": saml_request,
            "RelayState": relay_key,
        })
    elif provider_type == "OIDC":
        # OIDC authorize redirect
        oidc_issuer = details.get("oidc_issuer", "")
        authorize_url = details.get("authorize_url", f"{oidc_issuer}/authorize" if oidc_issuer else "")
        if not authorize_url:
            return error_response_json("InvalidParameterException",
                                       "OIDC provider has no oidc_issuer or authorize_url.", 400)
        oidc_client_id = details.get("client_id", "")
        redirect_url = authorize_url + ("&" if "?" in authorize_url else "?") + urlencode({
            "response_type": response_type or "code",
            "client_id": oidc_client_id,
            "redirect_uri": _acs_url(),
            "scope": details.get("authorize_scopes", scope),
            "state": relay_key,
        })
    else:
        return error_response_json("InvalidParameterException",
                                   f"Federated sign-in not supported for {provider_type}.", 400)

    logger.info("Cognito: OAuth2 authorize redirect to %s for provider %s", provider_type, identity_provider)
    return 302, {"Location": redirect_url, "Content-Type": "text/html"}, b""


def handle_oauth2_authorize(method, path, headers, query_params):
    """GET /oauth2/authorize — if identity_provider is given, redirect to external IdP;
    otherwise show managed login form."""
    # Federation redirect (SAML / OIDC external IdP)
    identity_provider = _qp(query_params, "identity_provider")
    if identity_provider:
        return _oauth2_authorize_federation(query_params)

    # Managed login UI
    client_id = _qp(query_params, "client_id")
    redirect_uri = _qp(query_params, "redirect_uri")
    response_type = _qp(query_params, "response_type")
    scope = _qp(query_params, "scope")
    state = _qp(query_params, "state")
    code_challenge = _qp(query_params, "code_challenge")
    code_challenge_method = _qp(query_params, "code_challenge_method")
    nonce = _qp(query_params, "nonce")

    if response_type != "code":
        return _oauth2_error("unsupported_response_type", "Only response_type=code is supported.")

    if not client_id:
        return _oauth2_error("invalid_request", "client_id is required.")

    pool_id, pool, client = _find_pool_by_client_id(client_id)
    if not pool:
        return _oauth2_error("invalid_client", f"Client {client_id} not found.")

    if "code" not in client.get("AllowedOAuthFlows", []):
        return _oauth2_error("unauthorized_client", "Client is not allowed to use the code flow.")

    # Validate redirect_uri
    callback_urls = client.get("CallbackURLs", [])
    if not redirect_uri:
        redirect_uri = client.get("DefaultRedirectURI", "")
    if not redirect_uri:
        return _oauth2_error("invalid_request", "redirect_uri is required.")
    if callback_urls and redirect_uri not in callback_urls:
        return _oauth2_error("invalid_request", f"redirect_uri is not allowed: {redirect_uri}")

    html_body = _login_page_html(
        client_id, redirect_uri, scope, state, response_type,
        code_challenge, code_challenge_method, nonce,
    )
    return 200, {"Content-Type": "text/html; charset=utf-8"}, html_body.encode("utf-8")


# -- /saml2/idpresponse (POST) ---------------------------------------------

def _saml2_idp_response(body: bytes, query_params):
    """POST /saml2/idpresponse — receive SAML assertion, create user, redirect with auth code."""
    # Parse form-encoded body
    form = {}
    if body:
        try:
            parsed = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
            form = {k: v[0] for k, v in parsed.items()}
        except Exception:
            pass

    saml_response_b64 = form.get("SAMLResponse", "")
    relay_state = form.get("RelayState", "")

    if not saml_response_b64:
        return error_response_json("InvalidParameterException", "SAMLResponse is required.", 400)

    # Look up relay context
    relay = _auth_codes.pop(relay_state, None)
    if not relay or relay.get("type") != "relay":
        return error_response_json("InvalidParameterException", "Invalid or expired RelayState.", 400)

    pool_id = relay["pool_id"]
    client_id = relay["client_id"]
    redirect_uri = relay["redirect_uri"]
    state = relay.get("state", "")
    provider_name = relay["provider_name"]

    pool = _get_pool_unscoped(pool_id)
    if not pool:
        return error_response_json("ResourceNotFoundException", f"User pool {pool_id} not found.", 400)

    provider = pool.get("_identity_providers", {}).get(provider_name)
    if not provider:
        return error_response_json("ResourceNotFoundException",
                                   f"Identity provider {provider_name} not found.", 400)

    # Parse SAML assertion
    try:
        saml_data = _parse_saml_response(saml_response_b64)
    except Exception as e:
        logger.warning("Cognito: failed to parse SAML response: %s", e)
        return error_response_json("InvalidParameterException", f"Failed to parse SAML response: {e}", 400)

    name_id = saml_data.get("name_id")
    if not name_id:
        return error_response_json("InvalidParameterException", "SAML assertion missing NameID.", 400)

    # Apply attribute mapping: IdP claim name → Cognito attribute name
    attr_mapping = provider.get("AttributeMapping", {})
    reverse_mapping = {v: k for k, v in attr_mapping.items()}  # IdP claim → Cognito attr
    user_attrs = {}
    for idp_claim, value in saml_data.get("attributes", {}).items():
        cognito_attr = reverse_mapping.get(idp_claim, idp_claim)
        user_attrs[cognito_attr] = value

    # Create or update federated user
    username = f"{provider_name}_{name_id}"
    existing_user = pool["_users"].get(username)
    now = _now_epoch()

    if existing_user:
        # Update attributes
        existing_dict = _attr_list_to_dict(existing_user.get("Attributes", []))
        existing_dict.update(user_attrs)
        existing_user["Attributes"] = _dict_to_attr_list(existing_dict)
        existing_user["UserLastModifiedDate"] = now
        sub = existing_dict.get("sub", new_uuid())
    else:
        sub = new_uuid()
        user_attrs["sub"] = sub
        if "email" not in user_attrs:
            user_attrs["email"] = name_id if "@" in name_id else ""
        user = {
            "Username": username,
            "Attributes": _dict_to_attr_list(user_attrs),
            "UserCreateDate": now,
            "UserLastModifiedDate": now,
            "Enabled": True,
            "UserStatus": "EXTERNAL_PROVIDER",
            "MFAOptions": [],
            "_password": "",
            "_groups": [],
            "_tokens": [],
        }
        pool["_users"][username] = user
        pool["EstimatedNumberOfUsers"] = len(pool["_users"])

    # Generate authorization code
    _cleanup_expired_relay_codes()
    code = secrets.token_urlsafe(32)
    _auth_codes[code] = {
        "type": "code",
        "pool_id": pool_id,
        "client_id": client_id,
        "username": username,
        "sub": sub,
        "redirect_uri": redirect_uri,
        "scopes": relay.get("scope", "openid"),
        "created_at": time.time(),
    }

    # Redirect to app callback
    params = {"code": code}
    if state:
        params["state"] = state
    location = redirect_uri + ("&" if "?" in redirect_uri else "?") + urlencode(params)
    logger.info("Cognito: SAML IdP response — user %s created/updated, redirecting to app", username)
    return 302, {"Location": location, "Content-Type": "text/html"}, b""


# -- /login (POST) ----------------------------------------------------------

def handle_login_submit(method, path, headers, body, query_params):
    """POST /login — process the login form and redirect with auth code."""
    form: dict = {}
    if body:
        try:
            parsed = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
            form = {k: v[0] for k, v in parsed.items()}
        except Exception:
            pass

    username = form.get("username", "")
    password = form.get("password", "")
    client_id = form.get("client_id", "")
    redirect_uri = form.get("redirect_uri", "")
    scope = form.get("scope", "")
    state = form.get("state", "")
    response_type = form.get("response_type", "code")
    code_challenge = form.get("code_challenge", "")
    code_challenge_method = form.get("code_challenge_method", "")
    nonce = form.get("nonce", "")

    pool_id, pool, client = _find_pool_by_client_id(client_id)
    if not pool:
        return _oauth2_error("invalid_client", f"Client {client_id} not found.")

    # Authenticate user
    error_msg = ""
    user = pool["_users"].get(username)
    if not user:
        error_msg = "Incorrect username or password."
    elif not user.get("Enabled", True):
        error_msg = "User is disabled."
    elif user.get("UserStatus") not in ("CONFIRMED", "FORCE_CHANGE_PASSWORD"):
        error_msg = "User is not confirmed."
    elif user.get("_password") != password:
        error_msg = "Incorrect username or password."

    if error_msg:
        html_body = _login_page_html(
            client_id, redirect_uri, scope, state, response_type,
            code_challenge, code_challenge_method, nonce,
            error_message=error_msg,
        )
        return 200, {"Content-Type": "text/html; charset=utf-8"}, html_body.encode("utf-8")

    # Generate authorization code
    code = _generate_auth_code()
    _authorization_codes[code] = {
        "client_id": client_id,
        "pool_id": pool_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "username": username,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "expires_at": time.time() + 300,
    }

    # Redirect with code
    sep = "&" if "?" in redirect_uri else "?"
    location = f"{redirect_uri}{sep}code={quote(code)}"
    if state:
        location += f"&state={quote(state)}"

    return 302, {"Location": location, "Cache-Control": "no-store"}, b""


# -- /oauth2/token (POST) ---------------------------------------------------

def _oauth2_token(data, query_params, raw_body: bytes = b"", headers: dict | None = None):
    """/oauth2/token endpoint — supports authorization_code, refresh_token, client_credentials."""
    # Parse form-encoded body
    form: dict = {}
    if raw_body:
        try:
            parsed = parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
            form = {k: v[0] for k, v in parsed.items()}
        except Exception:
            pass

    grant_type = form.get("grant_type", "")

    # Client authentication
    cid, csec = _authenticate_client(headers or {}, form)

    # ── authorization_code ──
    if grant_type == "authorization_code":
        code = form.get("code", "")
        redirect_uri = form.get("redirect_uri", "")
        code_verifier = form.get("code_verifier", "")

        # Try managed-login authorization codes first
        _cleanup_expired_codes()
        entry = _authorization_codes.get(code)
        if entry:
            # Validate
            if entry["client_id"] != cid:
                return _oauth2_error("invalid_grant", "client_id mismatch.")
            if entry["redirect_uri"] and entry["redirect_uri"] != redirect_uri:
                return _oauth2_error("invalid_grant", "redirect_uri mismatch.")

            # PKCE verification
            if entry.get("code_challenge"):
                if not code_verifier:
                    return _oauth2_error("invalid_grant", "code_verifier is required.")
                method = entry.get("code_challenge_method", "plain")
                if not _verify_pkce(code_verifier, entry["code_challenge"], method):
                    return _oauth2_error("invalid_grant", "PKCE verification failed.")

            # Consume code (one-time use)
            del _authorization_codes[code]

            pool_id = entry["pool_id"]
            pool = _get_pool_unscoped(pool_id)
            if not pool:
                return _oauth2_error("server_error", "User pool not found.")

            user = pool["_users"].get(entry["username"])
            if not user:
                return _oauth2_error("server_error", "User not found.")

            # Validate client secret if client has one
            _, _, client = _find_pool_by_client_id(cid)
            if client and client.get("ClientSecret") and csec != client["ClientSecret"]:
                return _oauth2_error("invalid_client", "Invalid client credentials.")

            result = _build_auth_result(pool_id, cid, user)
            refresh_val = result["RefreshToken"]
            _refresh_tokens[refresh_val] = {
                "pool_id": pool_id,
                "client_id": cid,
                "username": entry["username"],
                "scope": entry.get("scope", ""),
            }

            resp = {
                "access_token": result["AccessToken"],
                "id_token": result["IdToken"],
                "refresh_token": refresh_val,
                "token_type": "Bearer",
                "expires_in": 3600,
            }
            return 200, {"Content-Type": "application/json"}, json.dumps(resp).encode()

        # Try SAML/OIDC federation auth codes
        _cleanup_expired_relay_codes()
        code_data = _auth_codes.pop(code, None)
        if code_data and code_data.get("type") == "code":
            if cid and code_data["client_id"] != cid:
                return _oauth2_error("invalid_grant", "client_id mismatch.")
            if redirect_uri and code_data["redirect_uri"] != redirect_uri:
                return _oauth2_error("invalid_grant", "redirect_uri mismatch.")

            pool_id = code_data["pool_id"]
            username = code_data["username"]
            sub = code_data["sub"]
            effective_client_id = code_data["client_id"]

            user_attrs = {}
            pool = _get_pool_unscoped(pool_id)
            if pool:
                user = pool["_users"].get(username)
                if user:
                    user_attrs = _attr_list_to_dict(user.get("Attributes", []))

            access_token = _fake_token(sub, pool_id, effective_client_id, "access", username)
            id_token = _fake_token(sub, pool_id, effective_client_id, "id", username, user_attrs=user_attrs)
            refresh_token = secrets.token_urlsafe(48)

            return json_response({
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": 3600,
            })

        return _oauth2_error("invalid_grant", "Invalid or expired authorization code.")

    # ── refresh_token ──
    if grant_type == "refresh_token":
        refresh_val = form.get("refresh_token", "")
        entry = _refresh_tokens.get(refresh_val)
        if not entry:
            return _oauth2_error("invalid_grant", "Invalid refresh token.")

        pool_id = entry["pool_id"]
        pool = _get_pool_unscoped(pool_id)
        if not pool:
            return _oauth2_error("server_error", "User pool not found.")
        user = pool["_users"].get(entry["username"])
        if not user:
            return _oauth2_error("server_error", "User not found.")

        # Validate client secret if client has one
        _, _, client = _find_pool_by_client_id(cid or entry["client_id"])
        if client and client.get("ClientSecret") and csec and csec != client["ClientSecret"]:
            return _oauth2_error("invalid_client", "Invalid client credentials.")

        client_id = cid or entry["client_id"]
        attrs = _attr_list_to_dict(user.get("Attributes", []))
        sub = attrs.get("sub", user["Username"])
        username = user.get("Username", "")
        resp = {
            "access_token": _fake_token(sub, pool_id, client_id, "access", username=username),
            "id_token": _fake_token(sub, pool_id, client_id, "id", username=username, user_attrs=attrs),
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(resp).encode()

    # ── client_credentials ──
    if grant_type == "client_credentials":
        pool_id, pool, client = _find_pool_by_client_id(cid)
        if not pool or not client:
            return _oauth2_error("invalid_client", "Client not found.")
        if not client.get("ClientSecret"):
            return _oauth2_error("invalid_client", "client_credentials requires a confidential client.")
        if csec != client["ClientSecret"]:
            return _oauth2_error("invalid_client", "Invalid client credentials.")

        access_token = _fake_token(cid, pool_id, cid, "access")
        resp = {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        return 200, {"Content-Type": "application/json"}, json.dumps(resp).encode()

    # ── fallback (legacy behaviour for unrecognised grant_type) ──
    pool_id, pool, client = _find_pool_by_client_id(cid)
    access_token = _fake_token(cid or new_uuid(), pool_id or "", cid or "", "access")
    return json_response({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
    })


def handle_oauth2_token(method, path, headers, body, query_params):
    """Public entry point called from app.py for POST /oauth2/token."""
    return _oauth2_token({}, query_params, body, headers)


# -- /oauth2/userInfo (GET/POST) ---------------------------------------------

def handle_oauth2_userinfo(method, path, headers, body, query_params):
    """GET or POST /oauth2/userInfo — return user claims from the access token."""
    auth = headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return 401, {"Content-Type": "application/json", "WWW-Authenticate": "Bearer"}, \
            json.dumps({"error": "invalid_token", "error_description": "Missing Bearer token."}).encode()

    token = auth.split(" ", 1)[1].strip()
    # Decode JWT payload
    try:
        parts = token.split(".")
        payload_b64 = parts[1]
        # Add padding
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return 401, {"Content-Type": "application/json", "WWW-Authenticate": "Bearer"}, \
            json.dumps({"error": "invalid_token", "error_description": "Invalid token."}).encode()

    sub = payload.get("sub", "")
    username = payload.get("username", "") or payload.get("cognito:username", "")

    # Extract pool_id from the issuer claim to scope the lookup
    iss = payload.get("iss", "")
    token_pool_id = iss.rsplit("/", 1)[-1] if "/" in iss else ""

    # Find user — prefer the specific pool from the token
    user = None
    if token_pool_id:
        pool = _get_pool_unscoped(token_pool_id)
        if pool:
            if username and username in pool["_users"]:
                user = pool["_users"][username]
            else:
                for u in pool["_users"].values():
                    attrs = _attr_list_to_dict(u.get("Attributes", []))
                    if attrs.get("sub") == sub:
                        user = u
                        break

    # Fallback: search all pools (no AWS auth in browser)
    if not user:
        for _, pool in _all_pools():
            if username and username in pool["_users"]:
                user = pool["_users"][username]
                break
            for u in pool["_users"].values():
                attrs = _attr_list_to_dict(u.get("Attributes", []))
                if attrs.get("sub") == sub:
                    user = u
                    break
            if user:
                break

    if not user:
        return 401, {"Content-Type": "application/json", "WWW-Authenticate": "Bearer"}, \
            json.dumps({"error": "invalid_token", "error_description": "User not found."}).encode()

    attrs = _attr_list_to_dict(user.get("Attributes", []))
    claims = {"sub": attrs.get("sub", user["Username"])}
    # Standard OIDC claims
    for key in ("email", "email_verified", "name", "family_name", "given_name",
                "phone_number", "phone_number_verified", "preferred_username",
                "nickname", "picture", "profile", "website", "gender",
                "birthdate", "zoneinfo", "locale", "address", "updated_at"):
        if key in attrs:
            claims[key] = attrs[key]
    claims["cognito:username"] = user.get("Username", "")
    groups = user.get("_groups", [])
    if groups:
        claims["cognito:groups"] = groups

    return 200, {"Content-Type": "application/json"}, json.dumps(claims).encode()


# -- /logout (GET) -----------------------------------------------------------

def handle_logout(method, path, headers, query_params):
    """GET /logout — redirect to the logout URI."""
    client_id = _qp(query_params, "client_id")
    logout_uri = _qp(query_params, "logout_uri")

    if not client_id:
        return _oauth2_error("invalid_request", "client_id is required.")
    if not logout_uri:
        return _oauth2_error("invalid_request", "logout_uri is required.")

    _, _, client = _find_pool_by_client_id(client_id)
    if not client:
        return _oauth2_error("invalid_client", f"Client {client_id} not found.")

    allowed = client.get("LogoutURLs", [])
    if allowed and logout_uri not in allowed:
        return _oauth2_error("invalid_request", f"logout_uri is not allowed: {logout_uri}")

    return 302, {"Location": logout_uri, "Cache-Control": "no-store"}, b""


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
    # Ensure at least one char from each required class to satisfy default policy
    required = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
        secrets.choice("!@#$%^&*"),
    ]
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits + "!@#$%^&*"
    remaining = [secrets.choice(chars) for _ in range(8)]
    password = required + remaining
    secrets.SystemRandom().shuffle(password)
    return "".join(password)


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
    _auth_codes.clear()
    _authorization_codes.clear()
    _refresh_tokens.clear()
