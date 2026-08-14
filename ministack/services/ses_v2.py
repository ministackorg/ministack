"""
SES v2 Service Emulator.
REST/JSON API via path /v2/email/...
Supports: SendEmail, CreateEmailIdentity, GetEmailIdentity, DeleteEmailIdentity,
          ListEmailIdentities, CreateConfigurationSet, GetConfigurationSet,
          DeleteConfigurationSet, ListConfigurationSets, CreateEmailTemplate,
          GetEmailTemplate, UpdateEmailTemplate, DeleteEmailTemplate,
          ListEmailTemplates, GetAccount, ListSuppressedDestinations,
          PutAccountSuppressionAttributes, TagResource, UntagResource,
          ListTagsForResource.
Email templates live in the v1 store, so either API version sees the other's.
"""

import base64
import copy
import json
import logging
import os
import re
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)
from ministack.services.ses import (
    _build_mime_message,
    _parse_raw_mime,
    _render_template,
    _restore_regional_store,
    _sent_emails_list,
    _smtp_relay,
    _templates,
)

logger = logging.getLogger("ses-v2")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
TEMPLATE_PAGE_SIZE = 10  # ListEmailTemplates default per the AWS API reference

_identities = AccountRegionScopedDict()  # identity -> dict
_config_sets = AccountRegionScopedDict()  # name -> dict
_ses_tags = AccountRegionScopedDict()  # resource_arn -> [tags]


def get_state() -> dict:
    return copy.deepcopy({
        "_identities": _identities,
        "_config_sets": _config_sets,
        "_ses_tags": _ses_tags,
    })


def restore_state(data: dict):
    _restore_regional_store(_identities, data.get("_identities", {}))
    _restore_regional_store(_config_sets, data.get("_config_sets", {}))
    _restore_tag_store(data.get("_ses_tags", {}))


def _restore_tag_store(restored):
    """Move legacy ARN-keyed tags with their boot-region resource."""
    if isinstance(restored, AccountRegionScopedDict):
        _ses_tags.update(restored)
        return
    if isinstance(restored, AccountScopedDict):
        region = get_region()
        for (account_id, resource_arn), tags in restored._data.items():
            normalized_arn = _legacy_resource_arn_for_region(
                resource_arn, account_id, region
            )
            _ses_tags.set_scoped(account_id, region, normalized_arn, tags)
        return
    for resource_arn, tags in restored.items():
        account_id = get_account_id()
        region = get_region()
        normalized_arn = _legacy_resource_arn_for_region(
            resource_arn, account_id, region
        )
        _ses_tags[normalized_arn] = tags


def _legacy_resource_arn_for_region(resource_arn, account_id, region):
    try:
        spec = parse_arn(resource_arn)
    except (ArnParseError, TypeError):
        return resource_arn
    if spec.partition != "aws" or spec.service != "ses":
        return resource_arn
    return f"arn:aws:ses:{region}:{account_id}:{spec.resource}"


try:
    _restored = load_state("ses_v2")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _json_err(code, message, status=400):
    body = json.dumps({"message": message, "name": code}).encode("utf-8")
    headers = {"Content-Type": "application/json", "x-amzn-errortype": code}
    return status, headers, body


def _resource_arn(kind, name):
    return f"arn:aws:ses:{get_region()}:{get_account_id()}:{kind}/{name}"


def _invalid_resource_arn(arn):
    return _json_err("BadRequestException", f"Invalid ResourceArn: {arn}")


def _not_found_resource_arn(arn):
    return _json_err("NotFoundException", f"Resource {arn} not found", 404)


def _first_query_value(query_params, key, default=""):
    value = query_params.get(key, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _query_values(query_params, key):
    value = query_params.get(key, [])
    if isinstance(value, list):
        return value
    if value:
        return [value]
    return []


def _encode_page_token(offset):
    return base64.urlsafe_b64encode(str(offset).encode("ascii")).decode("ascii").rstrip("=")


def _decode_page_token(token):
    if not token:
        return 0, None
    try:
        padded = token + "=" * (-len(token) % 4)
        offset = int(base64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii"))
    except (ValueError, UnicodeDecodeError):
        return 0, _json_err("BadRequestException", f"Invalid NextToken: {token}")
    if offset < 0:
        return 0, _json_err("BadRequestException", f"Invalid NextToken: {token}")
    return offset, None


def _page_size(query_params, default, maximum=100):
    raw = _first_query_value(query_params, "PageSize")
    if not raw:
        return default, None
    try:
        size = int(raw)
    except (TypeError, ValueError):
        return default, _json_err("BadRequestException", f"Invalid PageSize: {raw}")
    if size < 1 or size > maximum:
        return default, _json_err(
            "BadRequestException", f"PageSize must be between 1 and {maximum}"
        )
    return size, None


def _paginate(items, query_params, default_size):
    size, err = _page_size(query_params, default_size)
    if err:
        return [], None, err
    start, err = _decode_page_token(_first_query_value(query_params, "NextToken"))
    if err:
        return [], None, err
    end = start + size
    nxt = _encode_page_token(end) if end < len(items) else None
    return items[start:end], nxt, None


def _template_parts(template_content):
    """Map a v2 EmailTemplateContent onto the v1 template record fields."""
    return {
        "SubjectPart": template_content.get("Subject") or "",
        "TextPart": template_content.get("Text") or "",
        "HtmlPart": template_content.get("Html") or "",
    }


def _template_content(stored):
    """Map a stored v1 template record back onto a v2 EmailTemplateContent."""
    return {
        "Subject": stored.get("SubjectPart", ""),
        "Text": stored.get("TextPart", ""),
        "Html": stored.get("HtmlPart", ""),
    }


def _template_name_from_arn(arn):
    """Extract the template name from a `template/<name>` ARN, or None if it isn't one."""
    try:
        spec = parse_arn(arn)
    except (ArnParseError, TypeError):
        return None
    kind, sep, name = spec.resource.partition("/")
    if sep != "/" or kind != "template" or not name:
        return None
    return name


def _resolve_send_template(tpl):
    """Resolve a SendEmail Content.Template to (v1-shaped template record, name, error).

    Inline TemplateContent renders without being stored, hence the empty name.
    """
    arn = tpl.get("TemplateArn") or ""
    name = tpl.get("TemplateName") or ""
    if not name and arn:
        name = _template_name_from_arn(arn) or ""
        if not name:
            return None, "", _json_err("BadRequestException", f"Invalid TemplateArn: {arn}")

    if name:
        stored = _templates.get(name)
        if stored is None:
            return None, "", _json_err(
                "NotFoundException", f"Template {name} does not exist", 404
            )
        return stored, name, None

    inline = tpl.get("TemplateContent")
    if isinstance(inline, dict):
        return _template_parts(inline), "", None

    return None, "", _json_err(
        "BadRequestException",
        "Content.Template requires one of TemplateName, TemplateArn, or TemplateContent",
    )


def _local_ses_v2_resource_arn(arn):
    if not arn:
        return None, _json_err("BadRequestException", "ResourceArn is required")
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _invalid_resource_arn(arn)

    if (
        spec.partition != "aws"
        or spec.service != "ses"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None, _invalid_resource_arn(arn)

    kind, sep, name = spec.resource.partition("/")
    if sep != "/" or not name or "/" in name:
        return None, _invalid_resource_arn(arn)

    if kind == "identity":
        if name not in _identities:
            return None, _not_found_resource_arn(arn)
    elif kind == "configuration-set":
        if name not in _config_sets:
            return None, _not_found_resource_arn(arn)
    else:
        return None, _invalid_resource_arn(arn)

    return str(spec), None


async def handle_request(method, path, headers, body, query_params):
    # Strip /v2/email prefix
    sub = path[len("/v2/email"):]

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # GET /v2/email/account
    if sub == "/account" and method == "GET":
        cutoff = time.time() - 86400
        sent_list = _sent_emails_list()
        sent_24h = sum(1 for e in sent_list if e["Timestamp"] >= cutoff)
        return json_response({
            "DedicatedIpAutoWarmupEnabled": False,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": True,
            "SendQuota": {"Max24HourSend": 50000.0, "MaxSendRate": 14.0, "SentLast24Hours": float(sent_24h)},
            "SendingEnabled": True,
            "SuppressionAttributes": {"SuppressedReasons": []},
        })

    # PUT /v2/email/account/suppression
    if sub == "/account/suppression" and method == "PUT":
        return json_response({})

    # GET /v2/email/suppression/addresses
    if sub == "/suppression/addresses" and method == "GET":
        return json_response({"SuppressedDestinationSummaries": []})

    # POST /v2/email/outbound-emails  (SendEmail)
    if sub == "/outbound-emails" and method == "POST":
        msg_id = f"ministack-{new_uuid()}"
        source = data.get("FromEmailAddress", "")
        dest = data.get("Destination", {})
        to_addrs = dest.get("ToAddresses", [])
        cc_addrs = dest.get("CcAddresses", [])
        bcc_addrs = dest.get("BccAddresses", [])
        content = data.get("Content", {})
        simple = content.get("Simple", {})
        raw = content.get("Raw", {})
        tpl = content.get("Template", {})
        subj = ""
        body_text = ""
        body_html = None
        template_name = ""
        if simple:
            subj = simple.get("Subject", {}).get("Data", "")
            body_text = simple.get("Body", {}).get("Text", {}).get("Data", "")
            body_html = simple.get("Body", {}).get("Html", {}).get("Data", "")
        elif raw:
            raw_data = raw.get("Data", "")
            parsed = _parse_raw_mime(raw_data)
            subj = parsed.get("Subject", "") or ""
            for part_info in parsed.get("BodyParts", []):
                if isinstance(part_info, dict):
                    ct = part_info.get("ContentType", "")
                    data = part_info.get("Data", "")
                    if "text/plain" in ct:
                        body_text = data
                    elif "text/html" in ct:
                        body_html = data
            # Extract Cc/Bcc from raw MIME headers when not provided via Destination
            if not cc_addrs:
                cc_addrs = [e.strip() for e in parsed.get("Cc", "").split(",") if e.strip()]
            if not bcc_addrs:
                bcc_addrs = [e.strip() for e in parsed.get("Bcc", "").split(",") if e.strip()]
        elif tpl:
            stored, template_name, err = _resolve_send_template(tpl)
            if err:
                return err
            rendered = _render_template(stored, tpl.get("TemplateData", ""))
            subj = rendered.get("Subject", "")
            body_text = rendered.get("Text", "")
            body_html = rendered.get("Html", "")

        all_addrs = to_addrs + cc_addrs + bcc_addrs
        if source and all_addrs:
            mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                           subj, body_text, body_html, msg_id)
            _smtp_relay(source, all_addrs, mime_str)

        # Append to shared sent_emails list for inspection endpoint visibility
        record = {
            "MessageId": msg_id,
            "Source": source,
            "To": to_addrs,
            "CC": cc_addrs,
            "BCC": bcc_addrs,
            "Subject": subj,
            "BodyText": body_text,
            "BodyHtml": body_html,
            "Timestamp": time.time(),
            "Type": "v2.SendEmail",
        }
        if tpl:
            record["TemplateData"] = tpl.get("TemplateData", "")
            if template_name:
                record["Template"] = template_name
        _sent_emails_list().append(record)

        logger.info("SESv2 SendEmail: MessageId=%s | %s -> %s%s", msg_id, source, to_addrs,
                    f" | template={template_name}" if template_name else "")
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
        _ses_tags[_resource_arn("identity", identity)] = list(data.get("Tags", []))
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
        _ses_tags[_resource_arn("configuration-set", name)] = list(data.get("Tags", []))
        return json_response({})

    # GET /v2/email/configuration-sets  (ListConfigurationSets)
    if sub == "/configuration-sets" and method == "GET":
        return json_response({"ConfigurationSets": list(_config_sets.keys())})

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

    # POST /v2/email/templates  (CreateEmailTemplate)
    if sub == "/templates" and method == "POST":
        name = data.get("TemplateName", "")
        template_content = data.get("TemplateContent")
        if not name:
            return _json_err("BadRequestException", "TemplateName is required")
        if not isinstance(template_content, dict):
            return _json_err("BadRequestException", "TemplateContent is required")
        if name in _templates:
            return _json_err("AlreadyExistsException", f"Template {name} already exists")
        _templates[name] = {
            "TemplateName": name,
            **_template_parts(template_content),
            "CreatedTimestamp": now_iso(),
            # Not taggable via TagResource on AWS, so these surface via GetEmailTemplate only
            "Tags": list(data.get("Tags", [])),
        }
        return json_response({})

    # GET /v2/email/templates  (ListEmailTemplates)
    if sub == "/templates" and method == "GET":
        page, next_token, err = _paginate(
            list(_templates.values()), query_params, TEMPLATE_PAGE_SIZE
        )
        if err:
            return err
        body_out = {
            "TemplatesMetadata": [
                {
                    "TemplateName": t["TemplateName"],
                    "CreatedTimestamp": t.get("CreatedTimestamp", ""),
                }
                for t in page
            ],
        }
        if next_token:
            body_out["NextToken"] = next_token
        return json_response(body_out)

    # GET/PUT/DELETE /v2/email/templates/{TemplateName}
    m = re.match(r"^/templates/([^/]+)$", sub)
    if m:
        name = m.group(1)
        stored = _templates.get(name)
        if stored is None:
            return _json_err("NotFoundException", f"Template {name} does not exist", 404)
        if method == "GET":
            body_out = {"TemplateName": name, "TemplateContent": _template_content(stored)}
            if stored.get("Tags"):
                body_out["Tags"] = stored["Tags"]
            return json_response(body_out)
        if method == "PUT":
            template_content = data.get("TemplateContent")
            if not isinstance(template_content, dict):
                return _json_err("BadRequestException", "TemplateContent is required")
            stored.update(_template_parts(template_content))
            return json_response({})
        if method == "DELETE":
            _templates.pop(name, None)
            return json_response({})

    # GET/POST/DELETE /v2/email/tags  (ListTagsForResource / TagResource / UntagResource)
    if sub == "/tags" and method == "GET":
        arn = _first_query_value(query_params, "ResourceArn")
        canonical_arn, err = _local_ses_v2_resource_arn(arn)
        if err:
            return err
        return json_response({"Tags": _ses_tags.get(canonical_arn, [])})

    m = re.match(r"^/tags$", sub)
    if m and method == "POST":
        arn = data.get("ResourceArn", "")
        canonical_arn, err = _local_ses_v2_resource_arn(arn)
        if err:
            return err
        existing = {t["Key"]: t for t in _ses_tags.get(canonical_arn, [])}
        for tag in data.get("Tags", []):
            existing[tag["Key"]] = tag
        _ses_tags[canonical_arn] = list(existing.values())
        return json_response({})

    if sub == "/tags" and method == "DELETE":
        arn = _first_query_value(query_params, "ResourceArn")
        canonical_arn, err = _local_ses_v2_resource_arn(arn)
        if err:
            return err
        remove_keys = set(_query_values(query_params, "TagKeys"))
        _ses_tags[canonical_arn] = [t for t in _ses_tags.get(canonical_arn, []) if t["Key"] not in remove_keys]
        return json_response({})

    return _json_err("NotFoundException", f"Unknown SES v2 path: {method} {path}", 404)


def reset():
    _identities.clear()
    _config_sets.clear()
    _ses_tags.clear()
