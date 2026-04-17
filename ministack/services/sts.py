"""
STS Service Emulator (AWS-compatible).

Actions:
  GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
  GetSessionToken, GetAccessKeyInfo.
"""

import json
import time
from urllib.parse import parse_qs

from ministack.core.responses import get_account_id, json_response, new_uuid
# Shared helpers — IAM and STS are a natural pair; STS is stateless
# and reuses IAM's XML builders and credential generators.
from ministack.services.iam import _p, _xml, _error, _future, \
    _gen_session_access_key, _gen_secret, _gen_session_token


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
        if use_json:
            return json_response({"Account": get_account_id(), "Arn": f"arn:aws:iam::{get_account_id()}:root", "UserId": get_account_id()})
        return _xml(200, "GetCallerIdentityResponse",
                    f"<GetCallerIdentityResult>"
                    f"<Arn>arn:aws:iam::{get_account_id()}:root</Arn>"
                    f"<UserId>{get_account_id()}</UserId>"
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
        assumed_arn = role_arn.replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session_name}"):
            assumed_arn = f"{assumed_arn}/{session_name}"
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
        assumed_arn = role_arn.replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session}"):
            assumed_arn = f"{assumed_arn}/{session}"
        role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
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

    return _error(400, "InvalidAction", f"Unknown STS action: {action}", ns="sts")
