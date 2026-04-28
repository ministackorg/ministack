"""
STS Service Emulator (AWS-compatible).

Actions:
  GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
  GetSessionToken, GetAccessKeyInfo, GetWebIdentityToken.
"""

import base64
import hashlib
import hmac
import json
import time
from urllib.parse import parse_qs

from ministack.core.responses import get_account_id, json_response, new_uuid
# Shared helpers — IAM and STS are a natural pair; STS is stateless
# and reuses IAM's XML builders and credential generators.
from ministack.services.iam import _p, _xml, _error, _future, \
    _gen_session_access_key, _gen_secret, _gen_session_token


_sessions: dict[str, dict] = {}


def reset():
    _sessions.clear()


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
        auth = headers.get("authorization", "")
        caller_arn = f"arn:aws:iam::{get_account_id()}:root"
        caller_user_id = get_account_id()
        if "Credential=" in auth:
            try:
                access_key = auth.split("Credential=")[1].split("/")[0]
                if access_key in _sessions:
                    caller_arn = _sessions[access_key]["Arn"]
                    caller_user_id = _sessions[access_key]["UserId"]
            except Exception:
                pass
        if use_json:
            return json_response({"Account": get_account_id(), "Arn": caller_arn, "UserId": caller_user_id})
        return _xml(200, "GetCallerIdentityResponse",
                    f"<GetCallerIdentityResult>"
                    f"<Arn>{caller_arn}</Arn>"
                    f"<UserId>{caller_user_id}</UserId>"
                    f"<Account>{get_account_id()}</Account>"
                    f"</GetCallerIdentityResult>",
                    ns="sts")

    if action == "AssumeRole":
        role_arn = _p(params, "RoleArn")
        session_name = _p(params, "RoleSessionName")
        duration = int(_p(params, "DurationSeconds") or 3600)
        expiration = _future(duration)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
        # Real AWS returns the assumed-role ARN under the `sts` service,
        # not `iam` — e.g. arn:aws:sts::123456789012:assumed-role/demo/TestAR.
        assumed_arn = role_arn.replace(":iam:", ":sts:", 1).replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session_name}"):
            assumed_arn = f"{assumed_arn}/{session_name}"
        _sessions[access_key] = {"Arn": assumed_arn, "UserId": f"{role_id}:{session_name}"}
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": time.time() + duration},
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
        duration = int(_p(params, "DurationSeconds") or 3600)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        assumed_arn = role_arn.replace(":iam:", ":sts:", 1).replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session}"):
            assumed_arn = f"{assumed_arn}/{session}"
        role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
        _sessions[access_key] = {"Arn": assumed_arn, "UserId": f"{role_id}:{session}"}
        provider = _p(params, "ProviderId") or "sts.amazonaws.com"
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": time.time() + duration},
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
        if use_json:
            return json_response({
                "Credentials": {"AccessKeyId": access_key, "SecretAccessKey": secret_key, "SessionToken": session_token, "Expiration": time.time() + duration},
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

    if action == "GetAccessKeyInfo":
        if use_json:
            return json_response({"Account": get_account_id()})
        return _xml(200, "GetAccessKeyInfoResponse",
                    f"<GetAccessKeyInfoResult>"
                    f"<Account>{get_account_id()}</Account>"
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
