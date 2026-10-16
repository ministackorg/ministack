# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
STS Service Emulator (AWS-compatible).

Actions:
  GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
  GetSessionToken, GetFederationToken, GetAccessKeyInfo, GetWebIdentityToken.
"""

import base64
import dataclasses
import hashlib
import hmac
import json
import os
import re
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.iam_evaluator import (
    AmbiguousAccessKeyError,
    CredentialResolutionError,
    _principal_keys,
    find_iam_access_key_account,
    is_root_access_key,
    resolve_credential,
    validate_policy_document,
)
from ministack.core.responses import (
    _account_from_sts_session,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)
from ministack.core.router import extract_access_key_id

# Shared helpers — IAM and STS are a natural pair; STS is stateless
# and reuses IAM's XML builders and credential generators.
from ministack.services.iam import (
    _error,
    _extract_tags,
    _future,
    _gen_secret,
    _gen_session_access_key,
    _gen_session_token,
    _p,
    _xml,
    policy_known,
)

_sessions: dict[str, dict] = {}


def _evict_expired_sessions():
    """Drop sessions past their Expiration — the store otherwise grows with
    every AssumeRole and every vended identity-pool credential, forever.
    Called on each insert; entries without an Expiration are kept."""
    now = time.time()
    for key in [k for k, s in _sessions.items()
                if isinstance(s.get("Expiration"), (int, float))
                and now > s["Expiration"]]:
        _sessions.pop(key, None)


def _session_expired(session) -> bool:
    exp = session.get("Expiration")
    return isinstance(exp, (int, float)) and time.time() > exp


def register_session(access_key: str, session: dict) -> None:
    """Register a temporary-credential session (AssumeRole, identity pools)."""
    _evict_expired_sessions()
    session.setdefault("IssuedAt", time.time())
    _sessions[access_key] = session


def _caller_identity(
    headers: dict, query_params=None
) -> dict | CredentialResolutionError:
    """Return the best-known originating principal for a new STS session."""
    from ministack.app import AUTH

    access_key = extract_access_key_id(headers, query_params) or "test"
    if not AUTH:
        # Session metadata is descriptive in permissive mode; issuing a
        # session must not introduce credential rejection checks.
        from ministack.core.iam_evaluator import resolve_caller_identity

        identity = resolve_caller_identity(access_key)
        account_id = get_account_id()
        arn = identity["userArn"] if identity else f"arn:aws:iam::{account_id}:root"
        principal_type = (
            "AssumedRole" if ":assumed-role/" in arn
            else "User" if ":user/" in arn else "Root"
        )
        return {
            "accessKey": access_key,
            "accountId": account_id,
            "userArn": arn,
            "userId": identity["userId"] if identity else account_id,
            "principalType": principal_type,
            "principalName": arn.rsplit("/", 1)[-1] if principal_type == "User" else "",
            "isTemporary": bool(identity and identity.get("session")),
        }
    credential = resolve_credential(access_key, get_account_id())
    if isinstance(credential, CredentialResolutionError):
        return credential
    return {
        "accessKey": access_key,
        "accountId": credential.account_id,
        "userArn": credential.principal_arn,
        "userId": credential.principal_id,
        "principalType": credential.principal_type,
        "principalName": credential.principal_name,
        "isTemporary": credential.session_token is not None,
        "principalKeys": _principal_keys(credential),
    }


def _credential_error_response(error: CredentialResolutionError):
    code = {"ExpiredTokenException": "ExpiredToken",
            "UnrecognizedClientException": "InvalidClientTokenId"}.get(error.code, error.code)
    return _error(403, code, error.message, ns="sts")


def _access_key_id_problems(key_id: str | None) -> list[str]:
    """The constraint violations AWS reports for a GetAccessKeyInfo AccessKeyId."""
    if key_id is None:
        return ["Value null at 'accessKeyId' failed to satisfy constraint: Member must not be null"]
    constraints = [
        ("Member must satisfy regular expression pattern: [\\w]*", not re.fullmatch(r"\w*", key_id, re.ASCII)),
        ("Member must have length greater than or equal to 16", len(key_id) < 16),
        ("Member must have length less than or equal to 128", len(key_id) > 128),
    ]
    return [f"Value '{escape(key_id)}' at 'accessKeyId' failed to satisfy constraint: {c}"
            for c, failed in constraints if failed]


def _access_key_account(key_id: str) -> str | None:
    """The account owning a known key, else the account AWS decodes from an AKIA/ASIA id, else None."""
    if is_root_access_key(key_id):
        return os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
    try:
        account = _account_from_sts_session(key_id) or find_iam_access_key_account(key_id)
    except AmbiguousAccessKeyError:
        account = None
    if account:
        return account
    if key_id[:4] not in ("AKIA", "ASIA") or len(key_id) > 25:
        return None
    bits = 0
    for char in key_id[4:14]:
        bits = bits << 5 | max("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".find(char), 0)
    return f"{bits >> 9 & (1 << 40) - 1:012d}" if bits >> 49 else None


def reset():
    _sessions.clear()


def _assumed_role_arn(role_arn: str, session_name: str):
    try:
        spec = parse_arn(role_arn)
    except ArnParseError:
        return None, _invalid_role_arn(role_arn)

    if (
        spec.service != "iam"
        or spec.region
        or len(spec.account_id) != 12
        or not spec.account_id.isdigit()
        or not spec.resource.startswith("role/")
    ):
        return None, _invalid_role_arn(role_arn)

    role_resource = spec.resource[len("role/"):]
    role_name = role_resource.rsplit("/", 1)[-1]
    if not role_resource or not role_name or ":" in role_name:
        return None, _invalid_role_arn(role_arn)

    return f"arn:{spec.partition}:sts::{spec.account_id}:assumed-role/{role_name}/{session_name}", None


def _role_id(role_arn: str) -> str:
    """The RoleId of the role *role_arn* names, or a new one for a role that is not stored."""
    from ministack.services import iam as iam_svc

    try:
        spec = parse_arn(role_arn)
        role = iam_svc._roles.get_scoped(spec.account_id, None, spec.resource.rsplit("/", 1)[-1])
    except ArnParseError:
        role = None
    return (role or {}).get("RoleId") or "AROA" + new_uuid().replace("-", "")[:17].upper()


def _session_tags(params: dict, check=True):
    """The session tags of a request, from the query or the JSON form, or, if *check*, their validation error."""
    tags = params.get("Tags")
    if isinstance(tags, list) and tags and isinstance(tags[0], dict):
        tags = [{"Key": str(t.get("Key")), "Value": str(t.get("Value", ""))} for t in tags]
    else:
        tags = _extract_tags(params)
    if check and len({t["Key"].lower() for t in tags}) < len(tags):
        return None, _error(400, "InvalidParameterValue",
                            "Duplicate tag keys found. Please note that Tag keys are case insensitive.", ns="sts")
    return tags, None


def _session_policies(params, account_id=None, check=True):
    """Policy and PolicyArns of a request, or with *check* its validation error (ARNs must be in ``account_id``)."""
    policy = _p(params, "Policy")
    arns = [a.get("arn", "") for a in params.get("PolicyArns", []) if isinstance(a, dict)]
    while arn := _p(params, f"PolicyArns.member.{len(arns) + 1}.arn"):
        arns.append(arn)
    if not check:
        return {"Policy": policy, "PolicyArns": arns}, None
    if len(policy) > 2048:
        return None, _error(400, "ValidationError",
                            f"1 validation error detected: Value '{escape(policy)}' at 'policy' failed to satisfy "
                            "constraint: Member must have length less than or equal to 2048", ns="sts")
    malformed = policy and validate_policy_document(policy)
    if malformed:
        return None, _error(400, "MalformedPolicyDocument", escape(malformed), ns="sts")
    if not all(policy_known(arn, account_id) for arn in arns):
        return None, _error(400, "ValidationError",
                            "At least one Policy ARN in the PolicyArns parameter does not match "
                            "an existing IAM Managed Policy ARN.", ns="sts")
    return {"Policy": policy, "PolicyArns": arns}, None


def _federated_name_error(name):
    """ValidationError unless ``name`` is a valid GetFederationToken Name."""
    if name is None:
        constraint = "Member must not be null"
    elif len(name) < 2:
        constraint = "Member must have length greater than or equal to 2"
    elif len(name) > 32:
        constraint = "Member must have length less than or equal to 32"
    elif not re.fullmatch(r"[\w+=,.@-]*", name, re.ASCII):
        constraint = "Member must satisfy regular expression pattern: [\\w+=,.@-]*"
    else:
        return None
    value = "null" if name is None else f"'{escape(name)}'"
    return _error(400, "ValidationError",
                  f"1 validation error detected: Value {value} at 'name' failed to satisfy constraint: {constraint}",
                  ns="sts")


def _session_caller_refused(caller, action):
    """AccessDenied for temporary credentials under AUTH, or None."""
    from ministack.app import AUTH
    if AUTH and caller["isTemporary"]:
        return _error(403, "AccessDenied", f"Cannot call {action} with session credentials", ns="sts")
    return None


def _invalid_role_arn(role_arn: str):
    return _error(400, "ValidationError", f"Invalid RoleArn: {role_arn}", ns="sts")


_WELL_KNOWN_WEB_IDENTITY_PROVIDERS = {
    "accounts.google.com", "cognito-identity.amazonaws.com", "graph.facebook.com", "www.amazon.com",
}


def _web_identity_principal(token: str, provider_id: str, role_arn: str):
    """The Federated principal a web identity token names, with its condition keys."""
    from ministack.services.cognito import _decode_id_token_unverified

    try:
        claims = _decode_id_token_unverified(token)
    except (ValueError, TypeError):
        claims = {}
    if not isinstance(claims, dict):
        claims = {}
    host = str(claims.get("iss") or provider_id or "").split("://", 1)[-1].rstrip("/")
    principal = host
    if host and host not in _WELL_KNOWN_WEB_IDENTITY_PROVIDERS:
        parts = role_arn.split(":")
        principal = f"arn:{parts[1]}:iam::{parts[4]}:oidc-provider/{host}"
    return principal, {f"{host}:{k}".lower(): claims[k] for k in ("aud", "sub", "amr") if k in claims}


def _web_identity_trust_denied(role_arn: str, params: dict, use_json: bool):
    """AccessDenied unless the role's trust policy lets the token's provider assume it."""
    from ministack.core.iam_evaluator import EvalContext, evaluate_trust_policy
    from ministack.services import iam as iam_svc

    account = role_arn.split(":")[4]
    role = iam_svc._roles.get_scoped(account, None, role_arn.rsplit("/", 1)[-1])
    provider, keys = _web_identity_principal(
        _p(params, "WebIdentityToken"), _p(params, "ProviderId"), role_arn)
    trust_context = EvalContext(
        principal_arn=provider, principal_type="WebIdentityUser", principal_account=account,
        action="sts:AssumeRoleWithWebIdentity", resource_arn=role_arn, region=get_region(),
        service_context=keys,
    )
    if role is not None and evaluate_trust_policy(
            role.get("AssumeRolePolicyDocument", "{}"), provider, trust_context):
        return None
    message = "Not authorized to perform sts:AssumeRoleWithWebIdentity"
    if use_json:
        return error_response_json("AccessDenied", message, 403)
    return _error(403, "AccessDenied", message, ns="sts")


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")

    # JSON protocol (newer SDKs): X-Amz-Target: AWSSecurityTokenServiceV20110615.ActionName
    if "amz-json" in content_type and target.startswith("AWSSecurityTokenServiceV20110615."):
        action_name = target.split(".")[-1]
        params["Action"] = [action_name]
        if body:
            try:
                json_body = json.loads(body)
                for k, v in json_body.items():
                    params[k] = [str(v)] if not isinstance(v, list) else v
            except (json.JSONDecodeError, TypeError):
                pass
    elif method == "POST" and body:
        for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
            params[k] = v

    action = _p(params, "Action")
    use_json = "amz-json" in content_type

    if action == "GetCallerIdentity":
        access_key = extract_access_key_id(headers, query_params)
        if access_key in _sessions and _session_expired(_sessions[access_key]):
            return _error(403, "ExpiredToken",
                          "The security token included in the request is expired",
                          ns="sts")
        caller = _caller_identity(headers, query_params)
        if isinstance(caller, CredentialResolutionError):
            return _credential_error_response(caller)
        caller_arn = caller["userArn"]
        caller_user_id = caller["userId"]
        caller_account = caller["accountId"]
        if use_json:
            return json_response({"Account": caller_account, "Arn": caller_arn, "UserId": caller_user_id})
        return _xml(200, "GetCallerIdentityResponse",
                    f"<GetCallerIdentityResult>"
                    f"<Arn>{caller_arn}</Arn>"
                    f"<UserId>{caller_user_id}</UserId>"
                    f"<Account>{caller_account}</Account>"
                    f"</GetCallerIdentityResult>",
                    ns="sts")

    if action == "AssumeRole":
        role_arn = _p(params, "RoleArn")
        session_name = _p(params, "RoleSessionName")
        assumed_arn, validation_error = _assumed_role_arn(role_arn, session_name)
        if validation_error:
            return validation_error
        from ministack.app import AUTH
        session_tags, validation_error = _session_tags(params, check=AUTH)
        if validation_error:
            return validation_error
        session_policies, validation_error = _session_policies(params, role_arn.split(":")[4], check=AUTH)
        if validation_error:
            return validation_error
        caller = _caller_identity(headers, query_params)
        if isinstance(caller, CredentialResolutionError):
            return _credential_error_response(caller)

        # When AUTH=true, validate role exists and trust policy permits the caller
        if AUTH:
            from ministack.core.iam_evaluator import EvalContext, evaluate_trust_policy
            from ministack.services import iam as iam_svc
            # Extract role name from ARN
            role_name = role_arn.split("/")[-1] if "/" in role_arn else ""
            # Resolve the role in the account the ARN names, not the caller's. `_roles` is
            # account-scoped, so a plain `.get()` looks in the CALLER's account — and a
            # cross-account AssumeRole, where the role by definition lives somewhere else, then
            # finds nothing and is refused with the role-missing message below (which reports an
            # empty `User:`, since it never got as far as resolving the caller).
            role_account = role_arn.split(":")[4] if role_arn.count(":") > 4 else ""
            role = None
            if role_account and role_account != get_account_id():
                # The ARN's account is authoritative: a role that isn't found
                # there does NOT fall back to a same-named role in the caller's
                # account — that resolution exists nowhere in AWS, and the miss
                # must surface as the same AccessDenied a denial produces (AWS
                # deliberately doesn't disclose role existence).
                try:
                    role = iam_svc._roles.get_scoped(role_account, None, role_name)
                except Exception:
                    role = None
            else:
                role = iam_svc._roles.get(role_name)
            if role is None:
                return _error(403, "AccessDenied",
                              f"User: is not authorized to perform: sts:AssumeRole on resource: {role_arn}",
                              ns="sts")
            # Evaluate trust policy
            trust_doc = role.get("AssumeRolePolicyDocument", "{}")
            caller_arn = caller["userArn"]
            trust_context = EvalContext(
                principal_arn=caller_arn,
                principal_type=caller["principalType"],
                principal_account=caller["accountId"],
                action="sts:AssumeRole", resource_arn=role_arn, region=get_region(),
                service_context={
                    "sts:externalid": _p(params, "ExternalId", None),
                    "sts:rolesessionname": _p(params, "RoleSessionName", None),
                },
                principal_keys=caller["principalKeys"],
                request_tags={t["Key"]: t["Value"] for t in session_tags},
                tag_keys=[t["Key"] for t in session_tags],
            )
            denied_action = None
            if not evaluate_trust_policy(trust_doc, caller_arn, trust_context):
                denied_action = "sts:AssumeRole"
            elif session_tags and not evaluate_trust_policy(
                    trust_doc, caller_arn, dataclasses.replace(trust_context, action="sts:TagSession")):
                denied_action = "sts:TagSession"
            if denied_action:
                message = (f"User: {caller_arn} is not authorized to perform: "
                           f"{denied_action} on resource: {role_arn}")
                if use_json:
                    return error_response_json("AccessDenied", message, 403)
                return _error(403, "AccessDenied", message, ns="sts")

        duration = int(_p(params, "DurationSeconds") or 3600)
        expiration = _future(duration)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        role_id = _role_id(role_arn)
        register_session(access_key, {
            "Arn": assumed_arn,
            "UserId": f"{role_id}:{session_name}",
            "SecretAccessKey": secret_key,
            "SessionToken": session_token,
            "Expiration": time.time() + duration,
            "AccountId": role_arn.split(":")[4],
            "PrincipalType": "AssumedRole",
            "SourceAccessKeyId": caller["accessKey"],
            "SourcePrincipalArn": caller["userArn"],
            "SessionTags": session_tags,
            "SessionPolicies": session_policies if any(session_policies.values()) else None,
        })
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": int(time.time() + duration)},
                "AssumedRoleUser": {"AssumedRoleId": f"{role_id}:{session_name}", "Arn": assumed_arn},
                "PackedPolicySize": 0,
            })
        return _xml(200, "AssumeRoleResponse",
                    f"<AssumeRoleResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{expiration}</Expiration>"
                    f"</Credentials>"
                    f"<AssumedRoleUser>"
                    f"<AssumedRoleId>{role_id}:{session_name}</AssumedRoleId>"
                    f"<Arn>{assumed_arn}</Arn>"
                    f"</AssumedRoleUser>"
                    f"<PackedPolicySize>0</PackedPolicySize>"
                    f"</AssumeRoleResult>",
                    ns="sts")

    if action == "AssumeRoleWithWebIdentity":
        role_arn = _p(params, "RoleArn")
        session = _p(params, "RoleSessionName", "session")
        assumed_arn, validation_error = _assumed_role_arn(role_arn, session)
        if validation_error:
            return validation_error
        from ministack.app import AUTH
        if AUTH:
            denied = _web_identity_trust_denied(role_arn, params, use_json)
            if denied:
                return denied
        duration = int(_p(params, "DurationSeconds") or 3600)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        role_id = _role_id(role_arn)
        register_session(access_key, {
            "Arn": assumed_arn,
            "UserId": f"{role_id}:{session}",
            "SecretAccessKey": secret_key,
            "SessionToken": session_token,
            "Expiration": time.time() + duration,
            "AccountId": role_arn.split(":")[4],
            "PrincipalType": "AssumedRole",
        })
        provider = _p(params, "ProviderId") or "sts.amazonaws.com"
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": int(time.time() + duration)},
                "AssumedRoleUser": {"AssumedRoleId": f"{role_id}:{session}", "Arn": assumed_arn},
                "SubjectFromWebIdentityToken": "test-subject",
                "Audience": "sts.amazonaws.com",
                "Provider": provider,
            })
        return _xml(200, "AssumeRoleWithWebIdentityResponse",
                    f"<AssumeRoleWithWebIdentityResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{_future(duration)}</Expiration>"
                    f"</Credentials>"
                    f"<AssumedRoleUser>"
                    f"<AssumedRoleId>{role_id}:{session}</AssumedRoleId>"
                    f"<Arn>{assumed_arn}</Arn>"
                    f"</AssumedRoleUser>"
                    f"<SubjectFromWebIdentityToken>test-subject</SubjectFromWebIdentityToken>"
                    f"<Audience>sts.amazonaws.com</Audience>"
                    f"<Provider>{provider}</Provider>"
                    f"</AssumeRoleWithWebIdentityResult>",
                    ns="sts")

    if action == "GetSessionToken":
        duration = int(_p(params, "DurationSeconds") or 43200)
        expiration = _future(duration)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        caller = _caller_identity(headers, query_params)
        if isinstance(caller, CredentialResolutionError):
            return _credential_error_response(caller)
        refused = _session_caller_refused(caller, action)
        if refused:
            return refused
        register_session(access_key, {
            "Arn": caller["userArn"],
            "UserId": caller["userId"],
            "SecretAccessKey": secret_key,
            "SessionToken": session_token,
            "Expiration": time.time() + duration,
            "AccountId": caller["accountId"],
            "PrincipalType": caller["principalType"],
            "PrincipalName": caller["principalName"],
            "SourceAccessKeyId": caller["accessKey"],
            "SourcePrincipalArn": caller["userArn"],
        })
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": int(time.time() + duration)},
            })
        return _xml(200, "GetSessionTokenResponse",
                    f"<GetSessionTokenResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{expiration}</Expiration>"
                    f"</Credentials>"
                    f"</GetSessionTokenResult>",
                    ns="sts")

    if action == "GetFederationToken":
        name = _p(params, "Name", None)
        validation_error = _federated_name_error(name)
        if validation_error:
            return validation_error
        session_tags, validation_error = _session_tags(params)
        if validation_error:
            return validation_error
        session_policies, validation_error = _session_policies(params)
        if validation_error:
            return validation_error
        caller = _caller_identity(headers, query_params)
        if isinstance(caller, CredentialResolutionError):
            return _credential_error_response(caller)
        refused = _session_caller_refused(caller, action)
        if refused:
            return refused
        duration = int(_p(params, "DurationSeconds") or 43200)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        federated_arn = f"arn:aws:sts::{caller['accountId']}:federated-user/{name}"
        federated_id = f"{caller['accountId']}:{name}"
        register_session(access_key, {
            "Arn": federated_arn,
            "UserId": federated_id,
            "SecretAccessKey": secret_key,
            "SessionToken": session_token,
            "Expiration": time.time() + duration,
            "AccountId": caller["accountId"],
            "PrincipalType": "FederatedUser",
            "PrincipalName": caller["principalName"],
            "SourceAccessKeyId": caller["accessKey"],
            "SourcePrincipalArn": caller["userArn"],
            "SessionTags": session_tags,
            "SessionPolicies": session_policies,
        })
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key,
                                "SessionToken": session_token, "Expiration": int(time.time() + duration)},
                "FederatedUser": {"FederatedUserId": federated_id, "Arn": federated_arn},
                "PackedPolicySize": 0,
            })
        return _xml(200, "GetFederationTokenResponse",
                    f"<GetFederationTokenResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{_future(duration)}</Expiration>"
                    f"</Credentials>"
                    f"<FederatedUser>"
                    f"<FederatedUserId>{federated_id}</FederatedUserId>"
                    f"<Arn>{federated_arn}</Arn>"
                    f"</FederatedUser>"
                    f"<PackedPolicySize>0</PackedPolicySize>"
                    f"</GetFederationTokenResult>",
                    ns="sts")

    if action == "GetAccessKeyInfo":
        key_id = _p(params, "AccessKeyId", None)
        problems = _access_key_id_problems(key_id)
        if problems:
            count = f"{len(problems)} validation error{'s' if len(problems) > 1 else ''} detected"
            return _error(400, "ValidationError", f"{count}: " + "; ".join(problems), ns="sts")
        account = _access_key_account(key_id)
        if account is None:
            return _error(400, "ValidationError", "Access key ID is not valid.", ns="sts")
        if use_json:
            return json_response({"Account": account})
        return _xml(200, "GetAccessKeyInfoResponse",
                    f"<GetAccessKeyInfoResult>"
                    f"<Account>{account}</Account>"
                    f"</GetAccessKeyInfoResult>",
                    ns="sts")

    if action == "GetWebIdentityToken":
        # --- AWS-spec validation ---
        # SigningAlgorithm: REQUIRED, must be RS256 or ES384.
        signing_alg = _p(params, "SigningAlgorithm")
        if not signing_alg:
            return _error(400, "MissingParameter",
                          "The request must contain the parameter SigningAlgorithm.",
                          ns="sts")
        if signing_alg not in ("RS256", "ES384"):
            return _error(400, "ValidationError",
                          f"Value '{signing_alg}' at 'signingAlgorithm' failed to satisfy constraint: "
                          f"Member must satisfy enum value set: [RS256, ES384]",
                          ns="sts")

        # Audience: REQUIRED, 1-10 items, each 1-1000 chars.
        audiences = []
        for k, v in params.items():
            if k == "Audience" or k.startswith("Audience.member."):
                vals = v if isinstance(v, list) else [v]
                audiences.extend(vals)
        if not audiences:
            return _error(400, "MissingParameter",
                          "The request must contain the parameter Audience.",
                          ns="sts")
        if len(audiences) > 10:
            return _error(400, "ValidationError",
                          "1 validation error detected: Value at 'audience' failed to satisfy constraint: "
                          "Member must have length less than or equal to 10",
                          ns="sts")
        for a in audiences:
            if not (1 <= len(a) <= 1000):
                return _error(400, "ValidationError",
                              "1 validation error detected: Value at 'audience' failed to satisfy constraint: "
                              "Member must have length between 1 and 1000",
                              ns="sts")

        # DurationSeconds: optional, 60-3600, default 300.
        try:
            duration = int(_p(params, "DurationSeconds") or 300)
        except (TypeError, ValueError):
            return _error(400, "ValidationError",
                          "Value for DurationSeconds must be an integer.",
                          ns="sts")
        if not (60 <= duration <= 3600):
            return _error(400, "ValidationError",
                          f"1 validation error detected: Value '{duration}' at 'durationSeconds' "
                          f"failed to satisfy constraint: Member must have value between 60 and 3600",
                          ns="sts")

        now = int(time.time())
        exp = now + duration
        expiration = _future(duration)

        # Emulator stub: real STS signs RS256/ES384 via JWKS-published keys; we
        # HMAC-sign so the token is self-contained and parseable but not
        # publicly verifiable. Header reports HS256 to stay honest about the
        # signature. Workloads that only inspect claims work; workloads that
        # verify against AWS JWKS are not in scope for the emulator.
        header = {"alg": "HS256", "typ": "JWT"}
        payload = {
            "sub": f"arn:aws:iam::{get_account_id()}:root",
            "aud": audiences if len(audiences) > 1 else audiences[0],
            "iss": "https://sts.amazonaws.com",
            "iat": now,
            "exp": exp,
        }

        def _b64url(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

        h = _b64url(json.dumps(header, separators=(",", ":")).encode())
        p = _b64url(json.dumps(payload, separators=(",", ":")).encode())
        sig = _b64url(hmac.new(b"ministack-fake-key", f"{h}.{p}".encode(), hashlib.sha256).digest())
        token = f"{h}.{p}.{sig}"

        if use_json:
            return json_response({
                "WebIdentityToken": token,
                "Expiration": exp,
            })
        return _xml(200, "GetWebIdentityTokenResponse",
                    f"<GetWebIdentityTokenResult>"
                    f"<WebIdentityToken>{token}</WebIdentityToken>"
                    f"<Expiration>{expiration}</Expiration>"
                    f"</GetWebIdentityTokenResult>",
                    ns="sts")

    return _error(400, "InvalidAction", f"Unknown STS action: {action}", ns="sts")


def get_state() -> dict:
    return {}


def load_persisted_state(data: dict) -> None:
    pass
