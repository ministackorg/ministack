"""
SES (Simple Email Service) Emulator — v1 Query API + v2 REST/JSON API.

v1 Query API (Action=...) via POST form body.
v2 JSON API detected via path prefix /v2/ or X-Amz-Target containing "sesv2".

v1 actions: SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail,
            VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity,
            VerifyDomainDkim, ListIdentities, GetIdentityVerificationAttributes,
            DeleteIdentity, GetSendQuota, GetSendStatistics,
            ListVerifiedEmailAddresses, CreateConfigurationSet,
            DeleteConfigurationSet, DescribeConfigurationSet,
            ListConfigurationSets, CreateTemplate, GetTemplate, DeleteTemplate,
            ListTemplates, UpdateTemplate, GetIdentityDkimAttributes,
            SetIdentityNotificationTopic, SetIdentityFeedbackForwardingEnabled.

v2 REST endpoints under /v2/email/:
            outbound-emails, outbound-bulk-emails, identities, configuration-sets,
            templates, account.

All emails stored in-memory for test inspection.
Send statistics aggregated into 15-minute buckets per AWS spec.
"""

import base64
import hashlib
import json
import logging
import os
import re
import smtplib
import time
from datetime import datetime, timezone
from email import message_from_bytes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.policy import default as default_policy
from urllib.parse import parse_qs, unquote

from ministack.core.responses import new_uuid

logger = logging.getLogger("ses")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_identities: dict = {}
_sent_emails: list = []
_templates: dict = {}
_configuration_sets: dict = {}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    is_v2 = path.startswith("/v2/") or "sesv2" in target.lower()

    if is_v2:
        return _handle_v2(method, path, headers, body)

    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "SendEmail": _send_email,
        "SendRawEmail": _send_raw_email,
        "SendTemplatedEmail": _send_templated_email,
        "SendBulkTemplatedEmail": _send_bulk_templated_email,
        "VerifyEmailIdentity": _verify_email_identity,
        "VerifyEmailAddress": _verify_email_identity,
        "VerifyDomainIdentity": _verify_domain_identity,
        "VerifyDomainDkim": _verify_domain_dkim,
        "ListIdentities": _list_identities,
        "GetIdentityVerificationAttributes": _get_identity_verification_attributes,
        "DeleteIdentity": _delete_identity,
        "GetSendQuota": _get_send_quota,
        "GetSendStatistics": _get_send_statistics,
        "ListVerifiedEmailAddresses": _list_verified_emails,
        "CreateConfigurationSet": _create_configuration_set,
        "DeleteConfigurationSet": _delete_configuration_set,
        "DescribeConfigurationSet": _describe_configuration_set,
        "ListConfigurationSets": _list_configuration_sets,
        "CreateTemplate": _create_template,
        "GetTemplate": _get_template,
        "DeleteTemplate": _delete_template,
        "ListTemplates": _list_templates,
        "UpdateTemplate": _update_template,
        "GetIdentityDkimAttributes": _get_identity_dkim_attributes,
        "SetIdentityNotificationTopic": _set_identity_notification_topic,
        "SetIdentityFeedbackForwardingEnabled": _set_identity_feedback_forwarding,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# v1 — Send operations
# ---------------------------------------------------------------------------

def _send_email(params):
    source = _p(params, "Source")
    subject = _p(params, "Message.Subject.Data")
    body_text = _p(params, "Message.Body.Text.Data")
    body_html = _p(params, "Message.Body.Html.Data")
    config_set = _p(params, "ConfigurationSetName")

    to_addrs = _collect_list(params, "Destination.ToAddresses.member")
    cc_addrs = _collect_list(params, "Destination.CcAddresses.member")
    bcc_addrs = _collect_list(params, "Destination.BccAddresses.member")

    msg_id = f"{new_uuid()}@email.amazonses.com"
    record = {
        "MessageId": msg_id,
        "Source": source,
        "To": to_addrs,
        "CC": cc_addrs,
        "BCC": bcc_addrs,
        "Subject": subject,
        "BodyText": body_text,
        "BodyHtml": body_html,
        "Timestamp": time.time(),
        "Type": "SendEmail",
    }
    if config_set:
        record["ConfigurationSetName"] = config_set
    _sent_emails.append(record)
    logger.info("SES SendEmail: %s -> %s | %s", source, to_addrs, subject)
    all_addrs = to_addrs + cc_addrs + bcc_addrs
    if all_addrs:
        mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                       subject, body_text, body_html, msg_id)
        _smtp_relay(source, all_addrs, mime_str)
    return _xml(200, "SendEmailResponse",
                f"<SendEmailResult><MessageId>{msg_id}</MessageId></SendEmailResult>")


def _send_raw_email(params):
    raw_b64 = _p(params, "RawMessage.Data")
    source = _p(params, "Source")
    msg_id = f"{new_uuid()}@email.amazonses.com"

    parsed = _parse_raw_mime(raw_b64)

    record = {
        "MessageId": msg_id,
        "Source": source or parsed.get("From", ""),
        "RawMessage": raw_b64,
        "Parsed": parsed,
        "Timestamp": time.time(),
        "Type": "SendRawEmail",
    }
    _sent_emails.append(record)
    logger.info("SES SendRawEmail: %s", msg_id)
    # Relay raw message via SMTP
    actual_source = source or parsed.get("From", "")
    raw_destinations = _collect_list(params, "Destinations.member")
    to_from_parsed = [a.strip() for a in parsed.get("To", "").split(",") if a.strip()]
    relay_addrs = raw_destinations or to_from_parsed
    if actual_source and relay_addrs:
        try:
            raw_bytes = raw_b64.encode('utf-8') if isinstance(raw_b64, str) else raw_b64
            try:
                decoded = base64.b64decode(raw_bytes)
            except Exception:
                decoded = raw_bytes
            raw_str = f'Message-ID: <{msg_id}>\r\n' + decoded.decode('utf-8', errors='replace')
            _smtp_relay(actual_source, relay_addrs, raw_str)
        except Exception:
            logger.warning('SMTP relay failed for SendRawEmail: %s', msg_id, exc_info=True)
    return _xml(200, "SendRawEmailResponse",
                f"<SendRawEmailResult><MessageId>{msg_id}</MessageId></SendRawEmailResult>")


def _send_templated_email(params):
    source = _p(params, "Source")
    template_name = _p(params, "Template")
    template_data = _p(params, "TemplateData")
    config_set = _p(params, "ConfigurationSetName")

    to_addrs = _collect_list(params, "Destination.ToAddresses.member")
    cc_addrs = _collect_list(params, "Destination.CcAddresses.member")
    bcc_addrs = _collect_list(params, "Destination.BccAddresses.member")

    if template_name not in _templates:
        return _error("TemplateDoesNotExist",
                       f"Template {template_name} does not exist", 400)

    rendered = _render_template(_templates[template_name], template_data)
    msg_id = f"{new_uuid()}@email.amazonses.com"
    record = {
        "MessageId": msg_id,
        "Source": source,
        "To": to_addrs,
        "CC": cc_addrs,
        "BCC": bcc_addrs,
        "Template": template_name,
        "TemplateData": template_data,
        "RenderedSubject": rendered.get("Subject", ""),
        "RenderedBodyText": rendered.get("Text", ""),
        "RenderedBodyHtml": rendered.get("Html", ""),
        "Timestamp": time.time(),
        "Type": "SendTemplatedEmail",
    }
    if config_set:
        record["ConfigurationSetName"] = config_set
    _sent_emails.append(record)
    logger.info("SES SendTemplatedEmail: %s -> %s | template=%s", source, to_addrs, template_name)
    all_addrs = to_addrs + cc_addrs + bcc_addrs
    if all_addrs:
        mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                       rendered.get("Subject", ""),
                                       rendered.get("Text", ""),
                                       rendered.get("Html", ""), msg_id)
        _smtp_relay(source, all_addrs, mime_str)
    return _xml(200, "SendTemplatedEmailResponse",
                f"<SendTemplatedEmailResult><MessageId>{msg_id}</MessageId></SendTemplatedEmailResult>")


def _send_bulk_templated_email(params):
    source = _p(params, "Source")
    template_name = _p(params, "Template")
    default_template_data = _p(params, "DefaultTemplateData")
    config_set = _p(params, "ConfigurationSetName")

    if template_name not in _templates:
        return _error("TemplateDoesNotExist",
                       f"Template {template_name} does not exist", 400)

    template = _templates[template_name]
    destinations = []
    i = 1
    while _p(params, f"Destinations.member.{i}.Destination.ToAddresses.member.1"):
        to_addrs = _collect_list(
            params, f"Destinations.member.{i}.Destination.ToAddresses.member")
        replacement = (_p(params, f"Destinations.member.{i}.ReplacementTemplateData")
                       or default_template_data)
        destinations.append({"To": to_addrs, "TemplateData": replacement})
        i += 1

    statuses = []
    for dest in destinations:
        msg_id = f"{new_uuid()}@email.amazonses.com"
        rendered = _render_template(template, dest["TemplateData"])
        record = {
            "MessageId": msg_id,
            "Source": source,
            "To": dest["To"],
            "Template": template_name,
            "TemplateData": dest["TemplateData"],
            "RenderedSubject": rendered.get("Subject", ""),
            "Timestamp": time.time(),
            "Type": "SendBulkTemplatedEmail",
        }
        if config_set:
            record["ConfigurationSetName"] = config_set
        _sent_emails.append(record)
        if dest["To"]:
            mime_str = _build_mime_message(source, dest["To"], [], [],
                                           rendered.get("Subject", ""),
                                           rendered.get("Text", ""),
                                           rendered.get("Html", ""), msg_id)
            _smtp_relay(source, dest["To"], mime_str)
        statuses.append(
            f"<member><Status>Success</Status>"
            f"<MessageId>{msg_id}</MessageId></member>")

    logger.info("SES SendBulkTemplatedEmail: %s | template=%s | %s destinations",
                source, template_name, len(destinations))
    return _xml(200, "SendBulkTemplatedEmailResponse",
                f"<SendBulkTemplatedEmailResult>"
                f"<Status>{''.join(statuses)}</Status>"
                f"</SendBulkTemplatedEmailResult>")


# ---------------------------------------------------------------------------
# v1 — Identity operations
# ---------------------------------------------------------------------------

def _verify_email_identity(params):
    email = _p(params, "EmailAddress")
    _identities[email] = _make_identity(email, "EmailAddress")
    return _xml(200, "VerifyEmailIdentityResponse",
                "<VerifyEmailIdentityResult/>")


def _verify_domain_identity(params):
    domain = _p(params, "Domain")
    _identities[domain] = _make_identity(domain, "Domain")
    token = hashlib.md5(domain.encode()).hexdigest()[:32]
    return _xml(200, "VerifyDomainIdentityResponse",
                f"<VerifyDomainIdentityResult>"
                f"<VerificationToken>{token}</VerificationToken>"
                f"</VerifyDomainIdentityResult>")


def _verify_domain_dkim(params):
    domain = _p(params, "Domain")
    if domain not in _identities:
        _identities[domain] = _make_identity(domain, "Domain")

    tokens = [
        hashlib.md5(f"{domain}-dkim-{i}".encode()).hexdigest()[:32]
        for i in range(3)
    ]
    _identities[domain]["DkimEnabled"] = True
    _identities[domain]["DkimTokens"] = tokens
    _identities[domain]["DkimVerificationStatus"] = "Success"

    members = "".join(f"<member>{t}</member>" for t in tokens)
    return _xml(200, "VerifyDomainDkimResponse",
                f"<VerifyDomainDkimResult>"
                f"<DkimTokens>{members}</DkimTokens>"
                f"</VerifyDomainDkimResult>")


def _list_identities(params):
    identity_type = _p(params, "IdentityType")
    members = ""
    for identity, info in _identities.items():
        if not identity_type or info["Type"] == identity_type:
            members += f"<member>{identity}</member>"
    return _xml(200, "ListIdentitiesResponse",
                f"<ListIdentitiesResult>"
                f"<Identities>{members}</Identities>"
                f"</ListIdentitiesResult>")


def _get_identity_verification_attributes(params):
    identities = _collect_list(params, "Identities.member")
    entries = ""
    for identity in identities:
        info = _identities.get(identity)
        status = info["VerificationStatus"] if info else "Pending"
        entries += (f"<entry><key>{identity}</key>"
                    f"<value><VerificationStatus>{status}"
                    f"</VerificationStatus></value></entry>")
    return _xml(200, "GetIdentityVerificationAttributesResponse",
                f"<GetIdentityVerificationAttributesResult>"
                f"<VerificationAttributes>{entries}</VerificationAttributes>"
                f"</GetIdentityVerificationAttributesResult>")


def _delete_identity(params):
    identity = _p(params, "Identity")
    _identities.pop(identity, None)
    return _xml(200, "DeleteIdentityResponse", "")


def _list_verified_emails(params):
    members = "".join(
        f"<member>{e}</member>"
        for e, info in _identities.items()
        if info["VerificationStatus"] == "Success" and info["Type"] == "EmailAddress"
    )
    return _xml(200, "ListVerifiedEmailAddressesResponse",
                f"<ListVerifiedEmailAddressesResult>"
                f"<VerifiedEmailAddresses>{members}</VerifiedEmailAddresses>"
                f"</ListVerifiedEmailAddressesResult>")


def _get_identity_dkim_attributes(params):
    identities = _collect_list(params, "Identities.member")
    entries = ""
    for identity in identities:
        info = _identities.get(identity, {})
        enabled = "true" if info.get("DkimEnabled") else "false"
        status = info.get("DkimVerificationStatus", "NotStarted")
        tokens_xml = "".join(
            f"<member>{t}</member>" for t in info.get("DkimTokens", []))
        entries += (f"<entry><key>{identity}</key><value>"
                    f"<DkimEnabled>{enabled}</DkimEnabled>"
                    f"<DkimVerificationStatus>{status}</DkimVerificationStatus>"
                    f"<DkimTokens>{tokens_xml}</DkimTokens>"
                    f"</value></entry>")
    return _xml(200, "GetIdentityDkimAttributesResponse",
                f"<GetIdentityDkimAttributesResult>"
                f"<DkimAttributes>{entries}</DkimAttributes>"
                f"</GetIdentityDkimAttributesResult>")


def _set_identity_notification_topic(params):
    identity = _p(params, "Identity")
    notification_type = _p(params, "NotificationType")
    sns_topic = _p(params, "SnsTopic")
    if identity in _identities:
        _identities[identity]["NotificationTopics"][notification_type] = sns_topic
    return _xml(200, "SetIdentityNotificationTopicResponse", "")


def _set_identity_feedback_forwarding(params):
    identity = _p(params, "Identity")
    enabled = _p(params, "ForwardingEnabled").lower() == "true"
    if identity in _identities:
        _identities[identity]["FeedbackForwardingEnabled"] = enabled
    return _xml(200, "SetIdentityFeedbackForwardingEnabledResponse", "")


# ---------------------------------------------------------------------------
# v1 — Quota / statistics (bugs fixed)
# ---------------------------------------------------------------------------

def _get_send_quota(params):
    cutoff = time.time() - 86400
    sent_24h = sum(1 for e in _sent_emails if e["Timestamp"] >= cutoff)
    return _xml(200, "GetSendQuotaResponse",
                f"<GetSendQuotaResult>"
                f"<Max24HourSend>50000.0</Max24HourSend>"
                f"<MaxSendRate>14.0</MaxSendRate>"
                f"<SentLast24Hours>{float(sent_24h)}</SentLast24Hours>"
                f"</GetSendQuotaResult>")


def _get_send_statistics(params):
    buckets = _aggregate_15min_buckets()
    members = ""
    for bucket in buckets:
        ts = datetime.fromtimestamp(
            bucket["Timestamp"], tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        members += (f"<member>"
                    f"<Timestamp>{ts}</Timestamp>"
                    f"<DeliveryAttempts>{bucket['DeliveryAttempts']}</DeliveryAttempts>"
                    f"<Bounces>{bucket['Bounces']}</Bounces>"
                    f"<Complaints>{bucket['Complaints']}</Complaints>"
                    f"<Rejects>{bucket['Rejects']}</Rejects>"
                    f"</member>")
    return _xml(200, "GetSendStatisticsResponse",
                f"<GetSendStatisticsResult>"
                f"<SendDataPoints>{members}</SendDataPoints>"
                f"</GetSendStatisticsResult>")


# ---------------------------------------------------------------------------
# v1 — Configuration sets
# ---------------------------------------------------------------------------

def _create_configuration_set(params):
    name = _p(params, "ConfigurationSet.Name")
    if not name:
        return _error("ValidationError",
                       "ConfigurationSet.Name is required", 400)
    if name in _configuration_sets:
        return _error("ConfigurationSetAlreadyExists",
                       f"Configuration set {name} already exists", 400)
    _configuration_sets[name] = {
        "Name": name,
        "CreatedTimestamp": _iso_now(),
    }
    return _xml(200, "CreateConfigurationSetResponse", "<CreateConfigurationSetResult/>")


def _delete_configuration_set(params):
    name = _p(params, "ConfigurationSetName")
    if name not in _configuration_sets:
        return _error("ConfigurationSetDoesNotExist",
                       f"Configuration set {name} does not exist", 400)
    del _configuration_sets[name]
    return _xml(200, "DeleteConfigurationSetResponse", "<DeleteConfigurationSetResult/>")


def _describe_configuration_set(params):
    name = _p(params, "ConfigurationSetName")
    cs = _configuration_sets.get(name)
    if not cs:
        return _error("ConfigurationSetDoesNotExist",
                       f"Configuration set {name} does not exist", 400)
    return _xml(200, "DescribeConfigurationSetResponse",
                f"<DescribeConfigurationSetResult>"
                f"<ConfigurationSet><Name>{cs['Name']}</Name></ConfigurationSet>"
                f"</DescribeConfigurationSetResult>")


def _list_configuration_sets(params):
    members = "".join(
        f"<member><Name>{cs['Name']}</Name></member>"
        for cs in _configuration_sets.values()
    )
    return _xml(200, "ListConfigurationSetsResponse",
                f"<ListConfigurationSetsResult>"
                f"<ConfigurationSets>{members}</ConfigurationSets>"
                f"</ListConfigurationSetsResult>")


# ---------------------------------------------------------------------------
# v1 — Templates
# ---------------------------------------------------------------------------

def _create_template(params):
    name = _p(params, "Template.TemplateName")
    if not name:
        return _error("ValidationError",
                       "Template.TemplateName is required", 400)
    if name in _templates:
        return _error("AlreadyExists",
                       f"Template {name} already exists", 400)
    _templates[name] = {
        "TemplateName": name,
        "SubjectPart": _p(params, "Template.SubjectPart"),
        "TextPart": _p(params, "Template.TextPart"),
        "HtmlPart": _p(params, "Template.HtmlPart"),
        "CreatedTimestamp": _iso_now(),
    }
    return _xml(200, "CreateTemplateResponse", "<CreateTemplateResult/>")


def _get_template(params):
    name = _p(params, "TemplateName")
    tpl = _templates.get(name)
    if not tpl:
        return _error("TemplateDoesNotExist",
                       f"Template {name} does not exist", 400)
    return _xml(200, "GetTemplateResponse",
                f"<GetTemplateResult><Template>"
                f"<TemplateName>{_esc(tpl['TemplateName'])}</TemplateName>"
                f"<SubjectPart>{_esc(tpl['SubjectPart'])}</SubjectPart>"
                f"<TextPart>{_esc(tpl['TextPart'])}</TextPart>"
                f"<HtmlPart>{_esc(tpl['HtmlPart'])}</HtmlPart>"
                f"</Template></GetTemplateResult>")


def _delete_template(params):
    name = _p(params, "TemplateName")
    _templates.pop(name, None)
    return _xml(200, "DeleteTemplateResponse", "<DeleteTemplateResult/>")


def _list_templates(params):
    members = "".join(
        f"<member><Name>{_esc(t['TemplateName'])}</Name>"
        f"<CreatedTimestamp>{t['CreatedTimestamp']}</CreatedTimestamp></member>"
        for t in _templates.values()
    )
    return _xml(200, "ListTemplatesResponse",
                f"<ListTemplatesResult>"
                f"<TemplatesMetadata>{members}</TemplatesMetadata>"
                f"</ListTemplatesResult>")


def _update_template(params):
    name = _p(params, "Template.TemplateName")
    if name not in _templates:
        return _error("TemplateDoesNotExist",
                       f"Template {name} does not exist", 400)
    tpl = _templates[name]
    for field, param in [("SubjectPart", "Template.SubjectPart"),
                         ("TextPart", "Template.TextPart"),
                         ("HtmlPart", "Template.HtmlPart")]:
        val = _p(params, param)
        if val:
            tpl[field] = val
    return _xml(200, "UpdateTemplateResponse", "<UpdateTemplateResult/>")


# ---------------------------------------------------------------------------
# v2 — REST / JSON dispatcher
# ---------------------------------------------------------------------------

def _handle_v2(method, path, headers, body):
    try:
        raw = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else (body or "")
        data = json.loads(raw) if raw else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        data = {}

    route = path.rstrip("/")
    if route.startswith("/v2/email"):
        route = route[len("/v2/email"):]

    if method == "POST" and route == "/outbound-emails":
        return _v2_send_email(data)
    if method == "POST" and route == "/outbound-bulk-emails":
        return _v2_send_bulk_email(data)
    if method == "POST" and route == "/identities":
        return _v2_create_identity(data)
    if method == "GET" and route == "/identities":
        return _v2_list_identities()
    if method == "POST" and route == "/configuration-sets":
        return _v2_create_configuration_set(data)
    if method == "GET" and route == "/configuration-sets":
        return _v2_list_configuration_sets()
    if method == "POST" and route == "/templates":
        return _v2_create_template(data)
    if method == "GET" and route == "/templates":
        return _v2_list_templates()
    if method == "GET" and route == "/account":
        return _v2_get_account()

    parts = route.split("/")

    if len(parts) == 3 and parts[1] == "identities":
        identity = unquote(parts[2])
        if method == "GET":
            return _v2_get_identity(identity)
        if method == "DELETE":
            return _v2_delete_identity(identity)

    if len(parts) == 3 and parts[1] == "configuration-sets":
        name = unquote(parts[2])
        if method == "GET":
            return _v2_get_configuration_set(name)
        if method == "DELETE":
            return _v2_delete_configuration_set(name)

    if len(parts) == 3 and parts[1] == "templates":
        name = unquote(parts[2])
        if method == "GET":
            return _v2_get_template(name)
        if method == "PUT":
            return _v2_update_template(name, data)
        if method == "DELETE":
            return _v2_delete_template(name)

    return _json_error("NotFoundException", f"Route not found: {method} {path}", 404)


# ---------------------------------------------------------------------------
# v2 — Send
# ---------------------------------------------------------------------------

def _v2_send_email(data):
    from_addr = data.get("FromEmailAddress", "")
    dest = data.get("Destination", {})
    to_addrs = dest.get("ToAddresses", [])
    cc_addrs = dest.get("CcAddresses", [])
    bcc_addrs = dest.get("BccAddresses", [])
    content = data.get("Content", {})
    config_set = data.get("ConfigurationSetName", "")

    subject = ""
    body_text = ""
    body_html = ""
    template_name = ""
    template_data = ""

    simple = content.get("Simple", {})
    if simple:
        subject = simple.get("Subject", {}).get("Data", "")
        body_obj = simple.get("Body", {})
        body_text = body_obj.get("Text", {}).get("Data", "")
        body_html = body_obj.get("Html", {}).get("Data", "")

    tpl = content.get("Template", {})
    if tpl:
        template_name = tpl.get("TemplateName", "")
        template_data = tpl.get("TemplateData", "")

    raw = content.get("Raw", {})
    parsed = {}
    if raw:
        parsed = _parse_raw_mime(raw.get("Data", ""))

    msg_id = f"{new_uuid()}@email.amazonses.com"
    record = {
        "MessageId": msg_id,
        "Source": from_addr,
        "To": to_addrs,
        "CC": cc_addrs,
        "BCC": bcc_addrs,
        "Subject": subject,
        "BodyText": body_text,
        "BodyHtml": body_html,
        "Timestamp": time.time(),
        "Type": "v2.SendEmail",
    }
    if template_name:
        record["Template"] = template_name
        record["TemplateData"] = template_data
    if parsed:
        record["Parsed"] = parsed
    if config_set:
        record["ConfigurationSetName"] = config_set
    _sent_emails.append(record)
    logger.info("SES v2 SendEmail: %s -> %s", from_addr, to_addrs)
    return _json_response(200, {"MessageId": msg_id})


def _v2_send_bulk_email(data):
    from_addr = data.get("FromEmailAddress", "")
    default_content = data.get("DefaultContent", {})
    tpl = default_content.get("Template", {})
    template_name = tpl.get("TemplateName", "")
    default_data = tpl.get("TemplateData", "")
    entries = data.get("BulkEmailEntries", [])
    config_set = data.get("ConfigurationSetName", "")

    results = []
    for entry in entries:
        dest = entry.get("Destination", {})
        to_addrs = dest.get("ToAddresses", [])
        replacement = (
            entry.get("ReplacementEmailContent", {})
                 .get("ReplacementTemplate", {})
                 .get("ReplacementTemplateData", default_data)
        )
        msg_id = f"{new_uuid()}@email.amazonses.com"
        record = {
            "MessageId": msg_id,
            "Source": from_addr,
            "To": to_addrs,
            "Template": template_name,
            "TemplateData": replacement,
            "Timestamp": time.time(),
            "Type": "v2.SendBulkEmail",
        }
        if config_set:
            record["ConfigurationSetName"] = config_set
        _sent_emails.append(record)
        results.append({"Status": "SUCCESS", "MessageId": msg_id})

    logger.info("SES v2 SendBulkEmail: %s | template=%s | %s entries",
                from_addr, template_name, len(entries))
    return _json_response(200, {"BulkEmailEntryResults": results})


# ---------------------------------------------------------------------------
# v2 — Identity
# ---------------------------------------------------------------------------

def _v2_create_identity(data):
    identity = data.get("EmailIdentity", "")
    id_type = "Domain" if ("." in identity and "@" not in identity) else "EmailAddress"
    _identities[identity] = _make_identity(identity, id_type)
    return _json_response(200, {
        "IdentityType": "DOMAIN" if id_type == "Domain" else "EMAIL_ADDRESS",
        "VerifiedForSendingStatus": True,
    })


def _v2_list_identities():
    items = []
    for identity, info in _identities.items():
        items.append({
            "IdentityType": "DOMAIN" if info["Type"] == "Domain" else "EMAIL_ADDRESS",
            "IdentityName": identity,
            "SendingEnabled": info["VerificationStatus"] == "Success",
        })
    return _json_response(200, {"EmailIdentities": items})


def _v2_get_identity(identity):
    info = _identities.get(identity)
    if not info:
        return _json_error("NotFoundException",
                           f"Identity {identity} not found", 404)
    return _json_response(200, {
        "IdentityType": "DOMAIN" if info["Type"] == "Domain" else "EMAIL_ADDRESS",
        "VerifiedForSendingStatus": info["VerificationStatus"] == "Success",
        "FeedbackForwardingStatus": info.get("FeedbackForwardingEnabled", True),
        "DkimAttributes": {
            "SigningEnabled": info.get("DkimEnabled", False),
            "Status": info.get("DkimVerificationStatus", "NOT_STARTED"),
            "Tokens": info.get("DkimTokens", []),
        },
    })


def _v2_delete_identity(identity):
    _identities.pop(identity, None)
    return _json_response(200, {})


# ---------------------------------------------------------------------------
# v2 — Configuration sets
# ---------------------------------------------------------------------------

def _v2_create_configuration_set(data):
    name = data.get("ConfigurationSetName", "")
    if not name:
        return _json_error("BadRequestException",
                           "ConfigurationSetName is required", 400)
    if name in _configuration_sets:
        return _json_error("AlreadyExistsException",
                           f"Configuration set {name} already exists", 409)
    _configuration_sets[name] = {"Name": name, "CreatedTimestamp": _iso_now()}
    return _json_response(200, {})


def _v2_list_configuration_sets():
    items = [{"Name": cs["Name"]} for cs in _configuration_sets.values()]
    return _json_response(200, {"ConfigurationSets": items})


def _v2_get_configuration_set(name):
    cs = _configuration_sets.get(name)
    if not cs:
        return _json_error("NotFoundException",
                           f"Configuration set {name} not found", 404)
    return _json_response(200, {"ConfigurationSetName": cs["Name"]})


def _v2_delete_configuration_set(name):
    if name not in _configuration_sets:
        return _json_error("NotFoundException",
                           f"Configuration set {name} not found", 404)
    del _configuration_sets[name]
    return _json_response(200, {})


# ---------------------------------------------------------------------------
# v2 — Templates
# ---------------------------------------------------------------------------

def _v2_create_template(data):
    name = data.get("TemplateName", "")
    content = data.get("TemplateContent", {})
    if not name:
        return _json_error("BadRequestException",
                           "TemplateName is required", 400)
    if name in _templates:
        return _json_error("AlreadyExistsException",
                           f"Template {name} already exists", 409)
    _templates[name] = {
        "TemplateName": name,
        "SubjectPart": content.get("Subject", ""),
        "TextPart": content.get("Text", ""),
        "HtmlPart": content.get("Html", ""),
        "CreatedTimestamp": _iso_now(),
    }
    return _json_response(200, {})


def _v2_list_templates():
    items = [
        {"TemplateName": t["TemplateName"],
         "CreatedTimestamp": t["CreatedTimestamp"]}
        for t in _templates.values()
    ]
    return _json_response(200, {"TemplatesMetadata": items})


def _v2_get_template(name):
    tpl = _templates.get(name)
    if not tpl:
        return _json_error("NotFoundException",
                           f"Template {name} not found", 404)
    return _json_response(200, {
        "TemplateName": tpl["TemplateName"],
        "TemplateContent": {
            "Subject": tpl["SubjectPart"],
            "Text": tpl["TextPart"],
            "Html": tpl["HtmlPart"],
        },
    })


def _v2_update_template(name, data):
    if name not in _templates:
        return _json_error("NotFoundException",
                           f"Template {name} not found", 404)
    content = data.get("TemplateContent", {})
    tpl = _templates[name]
    if "Subject" in content:
        tpl["SubjectPart"] = content["Subject"]
    if "Text" in content:
        tpl["TextPart"] = content["Text"]
    if "Html" in content:
        tpl["HtmlPart"] = content["Html"]
    return _json_response(200, {})


def _v2_delete_template(name):
    _templates.pop(name, None)
    return _json_response(200, {})


# ---------------------------------------------------------------------------
# v2 — Account
# ---------------------------------------------------------------------------

def _v2_get_account():
    cutoff = time.time() - 86400
    sent_24h = sum(1 for e in _sent_emails if e["Timestamp"] >= cutoff)
    return _json_response(200, {
        "SendQuota": {
            "Max24HourSend": 50000.0,
            "MaxSendRate": 14.0,
            "SentLast24Hours": float(sent_24h),
        },
        "SendingEnabled": True,
    })


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_identity(identity, identity_type):
    return {
        "VerificationStatus": "Success",
        "Type": identity_type,
        "DkimEnabled": False,
        "DkimTokens": [],
        "DkimVerificationStatus": "NotStarted",
        "NotificationTopics": {"Bounce": "", "Complaint": "", "Delivery": ""},
        "FeedbackForwardingEnabled": True,
    }


def _aggregate_15min_buckets():
    """Aggregate sent emails into 15-minute (900 s) buckets per the AWS spec."""
    if not _sent_emails:
        return []
    buckets: dict = {}
    for email in _sent_emails:
        ts = email["Timestamp"]
        bucket_ts = ts - (ts % 900)
        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "Timestamp": bucket_ts,
                "DeliveryAttempts": 0,
                "Bounces": 0,
                "Complaints": 0,
                "Rejects": 0,
            }
        buckets[bucket_ts]["DeliveryAttempts"] += 1
    return sorted(buckets.values(), key=lambda b: b["Timestamp"])


def _render_template(template, template_data_json):
    """Replace {{var}} placeholders with values from template data."""
    try:
        data = (json.loads(template_data_json)
                if isinstance(template_data_json, str)
                else (template_data_json or {}))
    except (json.JSONDecodeError, TypeError):
        data = {}

    result = {}
    for out_key, field in [("Subject", "SubjectPart"),
                           ("Text", "TextPart"),
                           ("Html", "HtmlPart")]:
        text = template.get(field, "")
        for key, val in data.items():
            text = text.replace("{{" + key + "}}", str(val))
        result[out_key] = text
    return result


def _parse_smtp_host():
    """Parse SMTP_HOST env var. Returns (host, port) or None if not set."""
    val = os.environ.get('SMTP_HOST')
    if not val:
        return None
    if ':' in val:
        host, port_str = val.rsplit(':', 1)
        try:
            return host, int(port_str)
        except ValueError:
            return val, 25
    return val, 25


def _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                        subject, body_text, body_html, message_id):
    """Build a MIME message string for SMTP relay."""
    if body_text and body_html:
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(body_text, 'plain', 'utf-8'))
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))
    elif body_html:
        msg = MIMEText(body_html, 'html', 'utf-8')
    else:
        msg = MIMEText(body_text or '', 'plain', 'utf-8')
    msg['Message-ID'] = f'<{message_id}>'
    msg['Subject'] = subject or ''
    msg['From'] = source
    if to_addrs:
        msg['To'] = ', '.join(to_addrs)
    if cc_addrs:
        msg['Cc'] = ', '.join(cc_addrs)
    return msg.as_string()


def _smtp_relay(source, to_addrs, message_str):
    """Relay email via external SMTP if SMTP_HOST is set. Best-effort."""
    endpoint = _parse_smtp_host()
    if not endpoint:
        return
    host, port = endpoint
    try:
        with smtplib.SMTP(host, port) as conn:
            conn.sendmail(source, to_addrs, message_str)
        logger.info('SMTP relay: %s -> %s via %s:%d', source, to_addrs, host, port)
    except Exception:
        logger.warning('SMTP relay failed: %s -> %s via %s:%d',
                       source, to_addrs, host, port, exc_info=True)


def _parse_raw_mime(raw_b64):
    """Best-effort MIME parse of a base64 or raw message."""
    parsed: dict = {}
    try:
        raw_bytes = raw_b64.encode("utf-8") if isinstance(raw_b64, str) else raw_b64
        try:
            decoded = base64.b64decode(raw_bytes)
        except Exception:
            decoded = raw_bytes

        mime_msg = message_from_bytes(decoded, policy=default_policy)
        parsed["From"] = mime_msg.get("From", "")
        parsed["To"] = mime_msg.get("To", "")
        parsed["Subject"] = mime_msg.get("Subject", "")
        parsed["ContentType"] = mime_msg.get_content_type()

        body_parts = []
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                ct = part.get_content_type()
                if ct in ("text/plain", "text/html"):
                    try:
                        payload = part.get_content()
                    except Exception:
                        payload = ""
                    body_parts.append({"ContentType": ct, "Data": payload})
        else:
            try:
                payload = mime_msg.get_content()
            except Exception:
                payload = ""
            body_parts.append({
                "ContentType": mime_msg.get_content_type(),
                "Data": payload,
            })
        parsed["BodyParts"] = body_parts
    except Exception as exc:
        parsed["ParseError"] = str(exc)
    return parsed


def _collect_list(params, prefix):
    result = []
    i = 1
    while _p(params, f"{prefix}.{i}"):
        result.append(_p(params, f"{prefix}.{i}"))
        i += 1
    return result


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _esc(text):
    if not text:
        return ""
    return (text.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace('"', "&quot;"))


def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _xml(status, root_tag, inner):
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="http://ses.amazonaws.com/doc/2010-12-01/">'
            f'{inner}'
            f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<ErrorResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">'
            f'<Error><Code>{code}</Code><Message>{_esc(message)}</Message></Error>'
            f'<RequestId>{new_uuid()}</RequestId>'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _json_response(status, data):
    return status, {"Content-Type": "application/json"}, json.dumps(data).encode("utf-8")


def _json_error(code, message, status):
    return _json_response(status, {"__type": code, "message": message})


def reset():
    _identities.clear()
    _sent_emails.clear()
    _templates.clear()
    _configuration_sets.clear()
