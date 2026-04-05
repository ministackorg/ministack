"""
SES v2 Service Emulator.
REST/JSON API via path /v2/email/...
Supports: SendEmail, CreateEmailIdentity, GetEmailIdentity, DeleteEmailIdentity,
          ListEmailIdentities, CreateConfigurationSet, GetConfigurationSet,
          DeleteConfigurationSet, ListConfigurationSets, GetAccount,
          ListSuppressedDestinations, PutAccountSuppressionAttributes,
          TagResource, UntagResource, ListTagsForResource.
"""

import json
import logging
import os
import re

from ministack.core.responses import json_response, new_uuid, now_iso
from ministack.services.ses import _build_mime_message, _smtp_relay

logger = logging.getLogger("ses-v2")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_identities: dict = {}        # identity -> dict
_config_sets: dict = {}       # name -> dict
_ses_tags: dict = {}          # resource_arn -> [tags]


def _json_err(code, message, status=400):
    body = json.dumps({"message": message, "name": code}).encode("utf-8")
    return status, {"Content-Type": "application/json"}, body


async def handle_request(method, path, headers, body, query_params):
    # Strip /v2/email prefix
    sub = path[len("/v2/email"):]

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # GET /v2/email/account
    if sub == "/account" and method == "GET":
        return json_response({
            "DedicatedIpAutoWarmupEnabled": False,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": True,
            "SendQuota": {"Max24HourSend": 50000.0, "MaxSendRate": 14.0, "SentLast24Hours": 0.0},
            "SendingEnabled": True,
            "SuppressionAttributes": {"SuppressedReasons": []},
        })

    # PUT /v2/email/account/suppression
    if sub == "/account/suppression" and method == "PUT":
        return json_response({})

    # GET /v2/email/suppression/addresses
    if sub == "/suppression/addresses" and method == "GET":
        return json_response({"SuppressedDestinationSummaries": [], "NextToken": None})

    # POST /v2/email/outbound-emails  (SendEmail)
    if sub == "/outbound-emails" and method == "POST":
        msg_id = f"ministack-{new_uuid()}"
        logger.info("SESv2 SendEmail: MessageId=%s", msg_id)
        # SMTP relay
        source = data.get("FromEmailAddress", "")
        dest = data.get("Destination", {})
        to_addrs = dest.get("ToAddresses", [])
        cc_addrs = dest.get("CcAddresses", [])
        bcc_addrs = dest.get("BccAddresses", [])
        all_addrs = to_addrs + cc_addrs + bcc_addrs
        if source and all_addrs:
            content = data.get("Content", {})
            simple = content.get("Simple", {})
            raw = content.get("Raw", {})
            if simple:
                subj = simple.get("Subject", {}).get("Data", "")
                body_text = simple.get("Body", {}).get("Text", {}).get("Data", "")
                body_html = simple.get("Body", {}).get("Html", {}).get("Data", "")
                mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                               subj, body_text, body_html, msg_id)
                _smtp_relay(source, all_addrs, mime_str)
            elif raw:
                import base64
                raw_data = raw.get("Data", "")
                try:
                    decoded = base64.b64decode(raw_data).decode('utf-8', errors='replace')
                except Exception:
                    decoded = raw_data
                raw_str = f'Message-ID: <{msg_id}>\r\n' + decoded
                _smtp_relay(source, all_addrs, raw_str)
        return json_response({"MessageId": msg_id})

    # POST /v2/email/identities  (CreateEmailIdentity)
    if sub == "/identities" and method == "POST":
        identity = data.get("EmailIdentity", "")
        if not identity:
            return _json_err("BadRequestException", "EmailIdentity is required")
        identity_type = "DOMAIN" if "." in identity and "@" not in identity else "EMAIL_ADDRESS"
        _identities[identity] = {
            "EmailIdentity": identity,
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": True,
            "DkimAttributes": {"SigningEnabled": False, "Status": "NOT_STARTED", "Tokens": []},
            "MailFromAttributes": {"BehaviorOnMxFailure": "USE_DEFAULT_VALUE"},
            "Tags": data.get("Tags", []),
            "CreatedTimestamp": now_iso(),
        }
        return json_response({
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": True,
            "DkimAttributes": {"SigningEnabled": False, "Status": "NOT_STARTED", "Tokens": []},
        })

    # GET /v2/email/identities  (ListEmailIdentities)
    if sub == "/identities" and method == "GET":
        return json_response({
            "EmailIdentities": [
                {"IdentityType": v["IdentityType"], "IdentityName": k, "SendingEnabled": True}
                for k, v in _identities.items()
            ],
            "NextToken": None,
        })

    # GET /v2/email/identities/{identity}
    m = re.match(r"^/identities/(.+)$", sub)
    if m:
        identity = m.group(1)
        if method == "GET":
            rec = _identities.get(identity)
            if not rec:
                return _json_err("NotFoundException", f"Identity {identity} not found", 404)
            return json_response(rec)
        if method == "DELETE":
            _identities.pop(identity, None)
            return json_response({})

    # POST /v2/email/configuration-sets  (CreateConfigurationSet)
    if sub == "/configuration-sets" and method == "POST":
        name = data.get("ConfigurationSetName", "")
        if not name:
            return _json_err("BadRequestException", "ConfigurationSetName is required")
        _config_sets[name] = {"ConfigurationSetName": name, "Tags": data.get("Tags", [])}
        return json_response({})

    # GET /v2/email/configuration-sets  (ListConfigurationSets)
    if sub == "/configuration-sets" and method == "GET":
        return json_response({"ConfigurationSets": list(_config_sets.keys()), "NextToken": None})

    # GET/DELETE /v2/email/configuration-sets/{name}
    m = re.match(r"^/configuration-sets/([^/]+)$", sub)
    if m:
        name = m.group(1)
        if method == "GET":
            rec = _config_sets.get(name)
            if not rec:
                return _json_err("NotFoundException", f"ConfigurationSet {name} not found", 404)
            return json_response(rec)
        if method == "DELETE":
            _config_sets.pop(name, None)
            return json_response({})

    # GET/POST/DELETE /v2/email/tags  (ListTagsForResource / TagResource / UntagResource)
    if sub == "/tags" and method == "GET":
        arn = query_params.get("ResourceArn", [""])[0] if isinstance(query_params.get("ResourceArn"), list) else query_params.get("ResourceArn", "")
        return json_response({"Tags": _ses_tags.get(arn, [])})

    m = re.match(r"^/tags$", sub)
    if m and method == "POST":
        arn = data.get("ResourceArn", "")
        existing = {t["Key"]: t for t in _ses_tags.get(arn, [])}
        for tag in data.get("Tags", []):
            existing[tag["Key"]] = tag
        _ses_tags[arn] = list(existing.values())
        return json_response({})

    if sub == "/tags" and method == "DELETE":
        arn = query_params.get("ResourceArn", [""])[0] if isinstance(query_params.get("ResourceArn"), list) else query_params.get("ResourceArn", "")
        remove_keys = set(query_params.get("TagKeys", []))
        _ses_tags[arn] = [t for t in _ses_tags.get(arn, []) if t["Key"] not in remove_keys]
        return json_response({})

    return _json_err("NotFoundException", f"Unknown SES v2 path: {method} {path}", 404)


def reset():
    _identities.clear()
    _config_sets.clear()
    _ses_tags.clear()
