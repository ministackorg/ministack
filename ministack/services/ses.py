# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
SES (Simple Email Service) Emulator — v1 Query API.

v1 Query API (Action=...) via POST form body.
SES v2 requests are delegated to :mod:`ses_v2`.

v1 actions: SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail,
            VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity,
            VerifyDomainDkim, ListIdentities, GetIdentityVerificationAttributes,
            DeleteIdentity, GetSendQuota, GetSendStatistics,
            ListVerifiedEmailAddresses, CreateConfigurationSet,
            DeleteConfigurationSet, DescribeConfigurationSet,
            ListConfigurationSets, CreateTemplate, GetTemplate, DeleteTemplate,
            ListTemplates, UpdateTemplate, GetIdentityDkimAttributes,
            SetIdentityNotificationTopic, SetIdentityFeedbackForwardingEnabled,
            CreateReceiptRuleSet, ListReceiptRuleSets, DescribeReceiptRuleSet,
            DeleteReceiptRuleSet, SetActiveReceiptRuleSet,
            DescribeActiveReceiptRuleSet, CreateReceiptRule, DescribeReceiptRule,
            DeleteReceiptRule.

All emails stored in-memory for test inspection.
Send statistics aggregated into 15-minute buckets per AWS spec.
"""

import base64
import copy
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
from email.utils import parseaddr
from urllib.parse import parse_qs

from ministack.core.concurrency import spawn_background
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("ses")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_identities = AccountRegionScopedDict()
# Per-account-and-region sent-mail record. The scoped dict stays under
# "entries" so list manipulation remains simple while statistics stay regional.
_sent_emails = AccountRegionScopedDict()
_templates = AccountRegionScopedDict()
_configuration_sets = AccountRegionScopedDict()
# SESv2 PutAccountDetails, under "account": ProductionAccessEnabled and the details.
_account_details = AccountRegionScopedDict()
_receipt_rule_sets = AccountRegionScopedDict()
_active_receipt_rule_set = AccountRegionScopedDict()
_SIMULATOR_DOMAIN = "simulator.amazonses.com"


def _sent_emails_list() -> list:
    lst = _sent_emails.get("entries")
    if lst is None:
        lst = []
        _sent_emails["entries"] = lst
    return lst


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def get_state() -> dict:
    return copy.deepcopy({
        "_identities": _identities,
        "_templates": _templates,
        "_configuration_sets": _configuration_sets,
        "_account_details": _account_details,
        "_receipt_rule_sets": _receipt_rule_sets,
        "_active_receipt_rule_set": _active_receipt_rule_set,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data: dict):
    _restore_regional_store(_identities, data.get("_identities", {}))
    _restore_regional_store(_templates, data.get("_templates", {}))
    _restore_regional_store(
        _configuration_sets, data.get("_configuration_sets", {})
    )
    _restore_regional_store(_account_details, data.get("_account_details", {}))
    _restore_regional_store(_receipt_rule_sets, data.get("_receipt_rule_sets", {}))
    _restore_regional_store(
        _active_receipt_rule_set, data.get("_active_receipt_rule_set", {})
    )


def _restore_regional_store(store, restored):
    """Map legacy SES state to the boot region without inspecting content ARNs."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return
    if isinstance(restored, AccountScopedDict):
        region = get_region()
        for (account_id, key), value in restored._data.items():
            store.set_scoped(account_id, region, key, value)
        return
    for key, value in restored.items():
        store[key] = value




# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    is_v2 = path.startswith("/v2/") or "sesv2" in target.lower()

    if is_v2:
        from ministack.services import ses_v2

        return await ses_v2.handle_request(method, path, headers, body, query_params)

    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "CreateReceiptRuleSet": _create_receipt_rule_set,
        "ListReceiptRuleSets": _list_receipt_rule_sets,
        "DescribeReceiptRuleSet": _describe_receipt_rule_set,
        "DeleteReceiptRuleSet": _delete_receipt_rule_set,
        "SetActiveReceiptRuleSet": _set_active_receipt_rule_set,
        "DescribeActiveReceiptRuleSet": _describe_active_receipt_rule_set,
        "CreateReceiptRule": _create_receipt_rule,
        "DescribeReceiptRule": _describe_receipt_rule,
        "DeleteReceiptRule": _delete_receipt_rule,
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
# v1 — Receipt-rule metadata (AddHeader/Stop only; no receiving execution)
# ---------------------------------------------------------------------------

_RECEIPT_ACTION_FIELDS = {
    "S3Action": ("TopicArn", "BucketName", "ObjectKeyPrefix", "KmsKeyArn", "IamRoleArn"),
    "BounceAction": ("TopicArn", "SmtpReplyCode", "StatusCode", "Message", "Sender"),
    "WorkmailAction": ("TopicArn", "OrganizationArn"),
    "LambdaAction": ("TopicArn", "FunctionArn", "InvocationType"),
    "StopAction": ("Scope", "TopicArn"),
    "AddHeaderAction": ("HeaderName", "HeaderValue"),
    "SNSAction": ("TopicArn", "Encoding"),
    "ConnectAction": ("InstanceARN", "IAMRoleARN"),
}


def _receipt_error(code, message):
    return _error(code, message, 400, "Sender")


def _receipt_result(action, data=None):
    inner = _receipt_element(f"{action}Result", data or {})
    # AWS stores headers containing NUL but returns InternalFailure when the
    # metadata cannot be serialized as XML. Do not emit malformed success XML.
    if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]", inner):
        return _error("InternalFailure", None, 500, "Receiver")
    return _xml(200, f"{action}Response", inner)


def _receipt_element(name, value):
    if isinstance(value, dict):
        inner = "".join(_receipt_element(k, v) for k, v in value.items())
    elif isinstance(value, list):
        inner = "".join(_receipt_element("member", v) for v in value)
    elif isinstance(value, bool):
        inner = str(value).lower()
    else:
        inner = _esc(str(value))
    return f"<{name}>{inner}</{name}>"


def _receipt_name_error(params, key, field, business_field=None):
    if key not in params:
        return _receipt_error("ValidationError", (
            f"1 validation error detected: Value at '{field}' failed to satisfy "
            "constraint: Member must not be null"))
    name = _p(params, key)
    if not name:
        return _receipt_error("ValidationError", (
            f"2 validation errors detected: Value at '{field}' failed to satisfy "
            "constraint: Member must have length greater than or equal to 1; "
            f"Value at '{field}' failed to satisfy constraint: Member must satisfy "
            "regular expression pattern: ^[a-zA-Z0-9_.-]+$"))
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", name):
        return _receipt_error("ValidationError", (
            f"1 validation error detected: Value at '{field}' failed to satisfy "
            "constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9_.-]+$"))
    if business_field and (len(name) > 64 or not re.fullmatch(
            r"[a-zA-Z0-9](?:[a-zA-Z0-9_.-]*[a-zA-Z0-9])?", name)):
        return _receipt_error("InvalidParameterValue", f"Not a valid {business_field}: {name}")
    return None


def _receipt_missing_set(name):
    return _receipt_error("RuleSetDoesNotExist", f"Rule set does not exist: {name}")


def _create_receipt_rule_set(params):
    error = _receipt_name_error(params, "RuleSetName", "ruleSetName", "ruleSetName")
    if error:
        return error
    name = _p(params, "RuleSetName")
    if name in _receipt_rule_sets:
        return _receipt_error("AlreadyExists", f"Rule set already exists: {name}")
    _receipt_rule_sets[name] = {
        "Metadata": {"Name": name, "CreatedTimestamp": datetime.now(timezone.utc).isoformat()},
        "Rules": [],
    }
    return _receipt_result("CreateReceiptRuleSet")


def _list_receipt_rule_sets(params):
    names = sorted(_receipt_rule_sets)
    token = _p(params, "NextToken")
    if token:
        try:
            last = base64.b64decode(token, validate=True).decode("utf-8")
            start = names.index(last) + 1
        except (ValueError, UnicodeError):
            return _receipt_error("InvalidParameterValue", f"Invalid token: {token}")
    else:
        start = 0
    page = names[start:start + 100]
    result = {"RuleSets": [_receipt_rule_sets[n]["Metadata"] for n in page]}
    if start + len(page) < len(names):
        result["NextToken"] = base64.b64encode(page[-1].encode()).decode()
    return _receipt_result("ListReceiptRuleSets", result)


def _describe_receipt_rule_set(params):
    name = _p(params, "RuleSetName")
    error = _receipt_name_error(params, "RuleSetName", "ruleSetName", "ruleSetName")
    if error:
        return error
    if name not in _receipt_rule_sets:
        return _receipt_missing_set(name)
    return _receipt_result("DescribeReceiptRuleSet", _receipt_rule_sets[name])


def _delete_receipt_rule_set(params):
    name = _p(params, "RuleSetName")
    error = _receipt_name_error(params, "RuleSetName", "ruleSetName", "ruleSetName")
    if error:
        return error
    if _active_receipt_rule_set.get("name") == name:
        return _receipt_error("CannotDelete", f"Cannot delete active rule set: {name}")
    _receipt_rule_sets.pop(name, None)
    return _receipt_result("DeleteReceiptRuleSet")


def _set_active_receipt_rule_set(params):
    if "RuleSetName" not in params:
        _active_receipt_rule_set.pop("name", None)
    else:
        error = _receipt_name_error(params, "RuleSetName", "ruleSetName", "ruleSetName")
        if error:
            return error
        name = _p(params, "RuleSetName")
        if name not in _receipt_rule_sets:
            return _receipt_missing_set(name)
        _active_receipt_rule_set["name"] = name
    return _receipt_result("SetActiveReceiptRuleSet")


def _describe_active_receipt_rule_set(params):
    name = _active_receipt_rule_set.get("name")
    return _receipt_result("DescribeActiveReceiptRuleSet", _receipt_rule_sets.get(name, {}))


def _receipt_rule_from_params(params):
    rule = {
        "Name": _p(params, "Rule.Name"),
        "Enabled": _p(params, "Rule.Enabled").lower() == "true",
        "TlsPolicy": _p(params, "Rule.TlsPolicy", "Optional"),
        "Actions": [],
        "ScanEnabled": _p(params, "Rule.ScanEnabled").lower() == "true",
    }
    if any(k == "Rule.Recipients" or k.startswith("Rule.Recipients.member.") for k in params):
        recipients = []
        i = 1
        while f"Rule.Recipients.member.{i}" in params:
            recipients.append(_p(params, f"Rule.Recipients.member.{i}"))
            i += 1
        rule["Recipients"] = recipients
    indexes = sorted({int(k.split(".")[3]) for k in params
                      if re.match(r"^Rule\.Actions\.member\.\d+\.", k)})
    for i in indexes:
        action = {}
        for name, fields in _RECEIPT_ACTION_FIELDS.items():
            prefix = f"Rule.Actions.member.{i}.{name}."
            values = {f: _p(params, prefix + f) for f in fields if prefix + f in params}
            if values:
                action[name] = values
        rule["Actions"].append(action)
    return rule


def _receipt_rule_error(rule):
    tls = rule["TlsPolicy"]
    if tls not in ("Optional", "Require"):
        return _receipt_error("ValidationError", (
            "1 validation error detected: Value at 'rule.tlsPolicy' failed to satisfy "
            "constraint: Member must satisfy enum value set: [Optional, Require]"))
    for i, action in enumerate(rule["Actions"], 1):
        if len(action) != 1:
            return _receipt_error("InvalidParameterValue", (
                "Exactly one action type must be specified for each ReceiptAction"))
        if "StopAction" in action:
            if action["StopAction"].get("Scope") != "RuleSet":
                return _receipt_error("ValidationError", (
                    f"1 validation error detected: Value at 'rule.actions.{i}.member.stopAction.scope' "
                    "failed to satisfy constraint: Member must satisfy enum value set: [RuleSet]"))
            if i != len(rule["Actions"]):
                return _receipt_error("InvalidParameterValue", (
                    "Stop action, if any, must be placed at the end of the actions list"))
        header = action.get("AddHeaderAction")
        if header is not None:
            for field in ("HeaderName", "HeaderValue"):
                if field not in header:
                    return _receipt_error("ValidationError", (
                        f"1 validation error detected: Value at 'rule.actions.{i}.member."
                        f"addHeaderAction.{field[0].lower() + field[1:]}' failed to satisfy "
                        "constraint: Member must not be null"))
            name, value = header["HeaderName"], header["HeaderValue"]
            if not re.fullmatch(r"[a-zA-Z0-9-]{1,50}", name):
                return _receipt_error("InvalidParameterValue", f"Invalid header name: {name}")
            if len(value) > 2048 or "\n" in value or "\r" in value:
                printable = value.replace("\n", "0x000a").replace("\r", "0x000d")
                return _receipt_error("InvalidParameterValue", f"Invalid header value: {printable}")
    recipients = rule.get("Recipients", [])
    for i, recipient in enumerate(recipients):
        parts = recipient.rsplit("@", 1)
        domain = parts[-1]
        if (len(parts) == 2 and not parts[0]) or not re.fullmatch(
                r"\.?[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?"
                r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+", domain) or (
                    len(parts) == 2 and ("@" in parts[0] or re.search(r"\s", parts[0]))):
            return _receipt_error("InvalidParameterValue", f"Invalid recipient: {recipient}")
        recipients[i] = recipient.lower()
    if "Recipients" in rule:
        rule["Recipients"] = sorted(set(recipients))
    return None


def _create_receipt_rule(params):
    if not any(key.startswith("Rule.") for key in params):
        return _receipt_error("ValidationError", (
            "1 validation error detected: Value at 'rule' failed to satisfy "
            "constraint: Member must not be null"))
    error = _receipt_name_error(params, "Rule.Name", "rule.name", "ruleName")
    if error:
        return error
    error = _receipt_name_error(params, "RuleSetName", "ruleSetName", "ruleSetName")
    if error:
        return error
    if "After" in params:
        error = _receipt_name_error(params, "After", "after")
        if error:
            return error
    rule = _receipt_rule_from_params(params)
    error = _receipt_rule_error(rule)
    if error:
        return error
    name = _p(params, "RuleSetName")
    if name not in _receipt_rule_sets:
        return _receipt_missing_set(name)
    rules = _receipt_rule_sets[name]["Rules"]
    if any(r["Name"] == rule["Name"] for r in rules):
        return _receipt_error("AlreadyExists", f"Rule already exists: {rule['Name']}")
    after = _p(params, "After")
    position = 0
    if after:
        position = next((i + 1 for i, r in enumerate(rules) if r["Name"] == after), None)
        if position is None:
            return _receipt_error("RuleDoesNotExist", f"Rule does not exist: {after}")
    rules.insert(position, rule)
    return _receipt_result("CreateReceiptRule")


def _describe_receipt_rule(params):
    for key, field in (("RuleSetName", "ruleSetName"), ("RuleName", "ruleName")):
        error = _receipt_name_error(params, key, field, field)
        if error:
            return error
    name, rule_name = _p(params, "RuleSetName"), _p(params, "RuleName")
    if name not in _receipt_rule_sets:
        return _receipt_missing_set(name)
    rule = next((r for r in _receipt_rule_sets[name]["Rules"] if r["Name"] == rule_name), None)
    if rule is None:
        return _receipt_error("RuleDoesNotExist", f"Rule does not exist: {rule_name}")
    return _receipt_result("DescribeReceiptRule", {"Rule": rule})


def _delete_receipt_rule(params):
    for key, field in (("RuleSetName", "ruleSetName"), ("RuleName", "ruleName")):
        error = _receipt_name_error(params, key, field, field)
        if error:
            return error
    name, rule_name = _p(params, "RuleSetName"), _p(params, "RuleName")
    if name not in _receipt_rule_sets:
        return _receipt_missing_set(name)
    rules = _receipt_rule_sets[name]["Rules"]
    rules[:] = [r for r in rules if r["Name"] != rule_name]
    return _receipt_result("DeleteReceiptRule")


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
    recipients = to_addrs + cc_addrs + bcc_addrs
    if any(_missing_domain(address) for address in [source, *recipients] if address):
        return _error("InvalidParameterValue", "Missing final '@domain'", 400)
    if config_set and config_set not in _configuration_sets:
        return _config_set_missing(config_set)
    rejected = _message_rejection(source, recipients)
    if rejected:
        return _error("MessageRejected", rejected, 400)

    msg_id = _record_send(
        source=source,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        type_name="SendEmail",
        config_set=config_set,
    )
    return _xml(200, "SendEmailResponse",
                f"<SendEmailResult><MessageId>{msg_id}</MessageId></SendEmailResult>")


def _missing_domain(value: str) -> bool:
    """SES answers "Missing final '@domain'" for an address without a domain
    and for one with non-ASCII characters ("must be 7-bit ASCII")."""
    if not value.isascii():
        return True
    _, separator, domain = parseaddr(value)[1].rpartition("@")
    return not separator or not domain


def _config_set_missing(name):
    return _error("ConfigurationSetDoesNotExist", f"Configuration set {name} does not exist", 400,
                  "Sender", fields={"ConfigurationSetName": name})


def _record_send(source, to_addrs, cc_addrs=None, bcc_addrs=None,
                 subject="", body_text="", body_html="",
                 type_name="SendEmail", config_set="", extra=None):
    """Record a sent email and best-effort SMTP relay. Returns MessageId.

    Used by both HTTP handlers and other in-process services (e.g. Cognito's
    invitation/verification flows) so all simulated mail flows through SES.
    """
    to_addrs = list(to_addrs or [])
    cc_addrs = list(cc_addrs or [])
    bcc_addrs = list(bcc_addrs or [])
    msg_id = f"{new_uuid()}@email.amazonses.com"
    record = {
        "MessageId": msg_id,
        "Source": source,
        "To": to_addrs,
        "CC": cc_addrs,
        "BCC": bcc_addrs,
        "Subject": subject,
        "BodyText": body_text or "",
        "BodyHtml": body_html or "",
        "Timestamp": time.time(),
        "Type": type_name,
    }
    if config_set:
        record["ConfigurationSetName"] = config_set
    if extra:
        record.update(extra)
    _sent_emails_list().append(record)
    logger.info("SES %s: %s -> %s | %s", type_name, source, to_addrs, subject)
    all_addrs = to_addrs + cc_addrs + bcc_addrs
    if all_addrs:
        mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                       subject, body_text, body_html, msg_id)
        _smtp_relay(source, all_addrs, mime_str)
    _send_notifications(msg_id, source, all_addrs)
    return msg_id


def send_internal_email(source, to_addrs, subject, body_text="", body_html="",
                        type_name="InternalSend", config_set="", extra=None):
    """Public hook for other emulated services to deliver mail through SES.

    Returns the MessageId of the stored record.
    """
    return _record_send(
        source=source,
        to_addrs=to_addrs,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        type_name=type_name,
        config_set=config_set,
        extra=extra,
    )


def _send_raw_email(params):
    raw_b64 = _p(params, "RawMessage.Data")
    source = _p(params, "Source")
    msg_id = f"{new_uuid()}@email.amazonses.com"

    parsed = _parse_raw_mime(raw_b64)
    rejected = _message_rejection(
        source or parsed.get("From", ""),
        _collect_list(params, "Destinations.member")
        or [a.strip() for h in ("To", "Cc", "Bcc") for a in parsed.get(h, "").split(",") if a.strip()])
    if rejected:
        return _error("MessageRejected", rejected, 400)

    # Extract from body parts if available
    subject = ""
    body_text = ""
    body_html = None
    for part_info in parsed.get("BodyParts", []):
        if isinstance(part_info, dict):
            ct = part_info.get("ContentType", "")
            data = part_info.get("Data", "")
            if "text/plain" in ct:
                body_text = data
            elif "text/html" in ct:
                body_html = data
        elif isinstance(part_info, str):
            # Already parsed as string (edge case)
            pass
    
    record = {
        "MessageId": msg_id,
        "Source": source or parsed.get("From", ""),
        "To": [e.strip() for e in parsed.get("To", "").split(",") if e.strip()] or [],
        "CC": [e.strip() for e in parsed.get("Cc", "").split(",") if e.strip()] or [],
        "BCC": [e.strip() for e in parsed.get("Bcc", "").split(",") if e.strip()] or [],
        "Subject": subject or parsed.get("Subject", ""),
        "BodyText": body_text,
        "BodyHtml": body_html,
        "Timestamp": time.time(),
        "Type": "SendRawEmail",
    }
    _sent_emails_list().append(record)
    logger.info("SES SendRawEmail: %s", msg_id)
    # Relay raw message via SMTP
    actual_source = source or parsed.get("From", "")
    raw_destinations = _collect_list(params, "Destinations.member")
    to_from_parsed = [a.strip() for a in parsed.get("To", "").split(",") if a.strip()]
    cc_from_parsed = [a.strip() for a in parsed.get("Cc", "").split(",") if a.strip()]
    bcc_from_parsed = [a.strip() for a in parsed.get("Bcc", "").split(",") if a.strip()]
    relay_addrs = raw_destinations or (to_from_parsed + cc_from_parsed + bcc_from_parsed)
    if actual_source and relay_addrs:
        try:
            raw_bytes = raw_b64.encode('utf-8') if isinstance(raw_b64, str) else raw_b64
            try:
                decoded = base64.b64decode(raw_bytes)
            except Exception:
                decoded = raw_bytes
            raw_str = f'Message-ID: <{msg_id}>\r\n' + _strip_message_id_header(
                decoded.decode('utf-8', errors='replace'))
            _smtp_relay(actual_source, relay_addrs, raw_str)
        except Exception:
            logger.warning('SMTP relay failed for SendRawEmail: %s', msg_id, exc_info=True)
    _send_notifications(msg_id, actual_source, relay_addrs)
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
    rejected = _message_rejection(source, to_addrs + cc_addrs + bcc_addrs)
    if rejected:
        return _error("MessageRejected", rejected, 400)

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
    _sent_emails_list().append(record)
    logger.info("SES SendTemplatedEmail: %s -> %s | template=%s", source, to_addrs, template_name)
    all_addrs = to_addrs + cc_addrs + bcc_addrs
    if all_addrs:
        mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                       rendered.get("Subject", ""),
                                       rendered.get("Text", ""),
                                       rendered.get("Html", ""), msg_id)
        _smtp_relay(source, all_addrs, mime_str)
    _send_notifications(msg_id, source, all_addrs)
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

    rejected = _message_rejection(source)
    if rejected:
        return _error("MessageRejected", rejected, 400)
    statuses = []
    for dest in destinations:
        rejected = _message_rejection(None, dest["To"])
        if rejected:
            statuses.append(f"<member><Status>MessageRejected</Status>"
                            f"<Error>{_esc(rejected)}</Error></member>")
            continue
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
        _sent_emails_list().append(record)
        if dest["To"]:
            mime_str = _build_mime_message(source, dest["To"], [], [],
                                           rendered.get("Subject", ""),
                                           rendered.get("Text", ""),
                                           rendered.get("Html", ""), msg_id)
            _smtp_relay(source, dest["To"], mime_str)
        _send_notifications(msg_id, source, dest["To"])
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


def _dkim_tokens(domain):
    """The three Easy DKIM tokens for a domain identity, stable per domain."""
    return [hashlib.md5(f"{domain}-dkim-{i}".encode()).hexdigest()[:32] for i in range(3)]


def _verify_domain_dkim(params):
    domain = _p(params, "Domain")
    if domain not in _identities:
        _identities[domain] = _make_identity(domain, "Domain")

    tokens = _dkim_tokens(domain)
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


def _tenant_delete_block(kind, name):
    """Refuse v1 deletes of resources with SESv2 tenant associations, as AWS does."""
    from ministack.services import ses_v2

    arn = f"arn:aws:ses:{get_region()}:{get_account_id()}:{kind}/{name}"
    if any(arn in resources for resources in ses_v2._tenant_resources.values()):
        return _error(
            "InvalidParameterValue",
            f"Cannot delete <{arn}> because it has tenant associations. Remove all tenant associations and try again.",
            400,
        )
    return None


def _delete_identity(params):
    identity = _p(params, "Identity")
    blocked = _tenant_delete_block("identity", identity)
    if blocked:
        return blocked
    _identities.pop(identity, None)
    return _xml(200, "DeleteIdentityResponse", "<DeleteIdentityResult/>")


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
    return _xml(200, "SetIdentityNotificationTopicResponse", "<SetIdentityNotificationTopicResult/>")


def _set_identity_feedback_forwarding(params):
    identity = _p(params, "Identity")
    enabled = _p(params, "ForwardingEnabled").lower() == "true"
    if identity in _identities:
        _identities[identity]["FeedbackForwardingEnabled"] = enabled
    return _xml(200, "SetIdentityFeedbackForwardingEnabledResponse", "<SetIdentityFeedbackForwardingEnabledResult/>")


# ---------------------------------------------------------------------------
# v1 — Quota / statistics (bugs fixed)
# ---------------------------------------------------------------------------

def _get_send_quota(params):
    cutoff = time.time() - 86400
    sent_24h = sum(1 for e in _sent_emails_list() if e["Timestamp"] >= cutoff)
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
    blocked = _tenant_delete_block("configuration-set", name)
    if blocked:
        return blocked
    if name not in _configuration_sets:
        return _config_set_missing(name)
    del _configuration_sets[name]
    return _xml(200, "DeleteConfigurationSetResponse", "<DeleteConfigurationSetResult/>")


def _describe_configuration_set(params):
    name = _p(params, "ConfigurationSetName")
    cs = _configuration_sets.get(name)
    if not cs:
        return _config_set_missing(name)
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
    blocked = _tenant_delete_block("template", name)
    if blocked:
        return blocked
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
# Shared helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Sandbox (SESv2 PutAccountDetails ProductionAccessEnabled=false)
# ---------------------------------------------------------------------------

def _production_access_enabled() -> bool:
    return (_account_details.get("account") or {}).get("ProductionAccessEnabled", True)


def _domain_and_parents(domain: str):
    """`a.b.com` -> `a.b.com`, `b.com` (the bare TLD is never an identity)."""
    labels = domain.split(".")
    return (".".join(labels[i:]) for i in range(len(labels) - 1))


def _identity_verified(address: str, *, simulator: bool = False) -> bool:
    """A verified address identity (case-sensitive) or a verified domain identity
    for the address's domain or any parent domain (case-insensitive), in v1 or v2."""
    from ministack.services import ses_v2

    addr = parseaddr(address)[1]
    if not addr:
        return False
    domain = addr.rpartition("@")[2].lower()
    if simulator and domain == _SIMULATOR_DOMAIN:
        return True

    def ok(v1_rec, v2_rec):
        return ((v1_rec or {}).get("VerificationStatus") == "Success"
                or bool((v2_rec or {}).get("VerifiedForSendingStatus")))

    if ok(_identities.get(addr), ses_v2._identities.get(addr)):
        return True
    v1 = {k.lower(): r for k, r in _identities.items() if "@" not in k}
    v2 = {k.lower(): r for k, r in ses_v2._identities.items() if "@" not in k}
    return any(ok(v1.get(d), v2.get(d)) for d in _domain_and_parents(domain))


def _message_rejection(sender, recipients=()) -> str | None:
    """The MessageRejected message for an unverified sender, or, in the sandbox,
    unverified recipients."""
    failed = [sender] if sender and not _identity_verified(sender) else []
    if not _production_access_enabled():
        failed += [r for r in recipients if r and not _identity_verified(r, simulator=True)]
    if not failed:
        return None
    return ("Email address is not verified. The following identities failed the check "
            f"in region {get_region().upper()}: {', '.join(failed)}")


# Mailbox simulator local part -> (notificationType, bounceSubType) it produces;
# any other address (success@, ooto@) is delivered.
_SIMULATOR_OUTCOMES = {
    "bounce": (("Bounce", "General"),),
    "suppressionlist": (("Bounce", "Suppressed"),),
    # Accepted and delivered, then marked as spam.
    "complaint": (("Delivery", None), ("Complaint", None)),
}


def _notification_identity(source: str) -> tuple[str, dict] | None:
    """The (name, record) of the identity whose notification topics apply to a
    sender: the address identity if verified, else the nearest verified parent domain."""
    addr = parseaddr(source or "")[1]
    if not addr:
        return None
    if addr in _identities:
        return addr, _identities[addr]
    domains = {k.lower(): (k, r) for k, r in _identities.items() if "@" not in k}
    for d in _domain_and_parents(addr.rpartition("@")[2].lower()):
        if d in domains:
            return domains[d]
    return None


def _publish_notification(topic_arn: str, payload: dict) -> None:
    from ministack.services import sns

    try:
        sns.publish_internal(topic_arn, json.dumps(payload),
                             "Amazon SES Email Event Notification")
    except Exception:
        logger.warning("SES notification publish to %s failed", topic_arn, exc_info=True)


def _notification_detail(kind: str, rcpt: str, ts: str, bounce_subtype: str | None = None) -> dict:
    if kind == "Delivery":
        return {"delivery": {
            "timestamp": ts, "processingTimeMillis": 0, "recipients": [rcpt],
            "smtpResponse": "250 2.6.0 Message received",
            "reportingMTA": "a0-0.smtp-out.amazonses.com"}}
    if kind == "Bounce" and bounce_subtype == "Suppressed":
        # SES suppressed the send, so no remote MTA returned a DSN.
        return {"bounce": {
            "bounceType": "Permanent", "bounceSubType": "Suppressed",
            "bouncedRecipients": [{"emailAddress": rcpt}],
            "timestamp": ts, "feedbackId": new_uuid()}}
    if kind == "Bounce":
        return {"bounce": {
            "bounceType": "Permanent", "bounceSubType": "General",
            "bouncedRecipients": [{
                "emailAddress": rcpt, "action": "failed", "status": "5.1.1",
                "diagnosticCode": "smtp; 550 5.1.1 user unknown"}],
            "timestamp": ts, "feedbackId": new_uuid(),
            "reportingMTA": "dsn; a0-0.smtp-out.amazonses.com"}}
    return {"complaint": {
        "complainedRecipients": [{"emailAddress": rcpt}],
        "timestamp": ts, "feedbackId": new_uuid(),
        "userAgent": "Amazon SES Mailbox Simulator",
        "complaintFeedbackType": "abuse", "arrivalDate": ts}}


def _send_notifications(msg_id, source, recipients):
    """Publish the Delivery / Bounce / Complaint notification of each recipient
    to the SNS topic set on the sender's identity (SetIdentityNotificationTopic)."""
    found = _notification_identity(source)
    if not found:
        return
    identity_name, identity = found
    topics = identity.get("NotificationTopics", {})
    if not any(topics.values()):
        return
    recipients = [a for a in (parseaddr(r)[1] for r in recipients) if a]
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    mail = {
        "timestamp": ts,
        # The envelope MAIL FROM address and the identity used to send.
        "source": parseaddr(source)[1],
        "sourceArn": f"arn:aws:ses:{get_region()}:{get_account_id()}:identity/{identity_name}",
        "sendingAccountId": get_account_id(),
        "messageId": msg_id,
        "destination": recipients,
    }
    for rcpt in recipients:
        local, _, domain = rcpt.partition("@")
        outcomes = (("Delivery", None),)
        if domain.lower() == _SIMULATOR_DOMAIN:
            outcomes = _SIMULATOR_OUTCOMES.get(local.lower().split("+")[0], outcomes)
        for kind, bounce_subtype in outcomes:
            topic = topics.get(kind)
            if not topic:
                continue
            detail = _notification_detail(kind, rcpt, ts, bounce_subtype)
            _publish_notification(topic, {"notificationType": kind, "mail": mail, **detail})


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
    if not _sent_emails_list():
        return []
    buckets: dict = {}
    for email in _sent_emails_list():
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


_SMTP_TIMEOUT_SECONDS = 10


def _strip_message_id_header(raw_str):
    """Drop client-supplied Message-ID headers (incl. folded lines); real SES replaces it."""
    lines = raw_str.splitlines(keepends=True)
    out = []
    in_headers = True
    skipping = False
    for line in lines:
        if in_headers:
            if line in ('\r\n', '\n', '\r'):
                in_headers = False
            elif line[0] in ' \t':
                if skipping:
                    continue
            else:
                skipping = line.lower().startswith('message-id:')
                if skipping:
                    continue
        out.append(line)
    return ''.join(out)


def _smtp_relay(source, to_addrs, message_str):
    """Relay email via external SMTP if SMTP_HOST is set. Best-effort, off the event loop."""
    endpoint = _parse_smtp_host()
    if not endpoint:
        return
    spawn_background(_smtp_send, source, to_addrs, message_str, *endpoint,
                     thread_name="ministack-ses-smtp")


def _smtp_send(source, to_addrs, message_str, host, port):
    try:
        with smtplib.SMTP(host, port, timeout=_SMTP_TIMEOUT_SECONDS) as conn:
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


def _error(code, message, status, error_type="", fields=None):
    type_xml = f"<Type>{error_type}</Type>" if error_type else ""
    message_xml = f"<Message>{_esc(message)}</Message>" if message is not None else ""
    message_xml += "".join(f"<{k}>{_esc(v)}</{k}>" for k, v in (fields or {}).items())
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<ErrorResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">'
            f'<Error>{type_xml}<Code>{code}</Code>{message_xml}</Error>'
            f'<RequestId>{new_uuid()}</RequestId>'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def reset():
    _identities.clear()
    _sent_emails.clear()
    _templates.clear()
    _configuration_sets.clear()
    _account_details.clear()
    _receipt_rule_sets.clear()
    _active_receipt_rule_set.clear()
