# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
SES v2 Service Emulator.
REST/JSON API via path /v2/email/...
Supports: SendEmail, CreateEmailIdentity, GetEmailIdentity, DeleteEmailIdentity,
          ListEmailIdentities, CreateConfigurationSet, GetConfigurationSet,
          DeleteConfigurationSet, ListConfigurationSets, CreateEmailTemplate,
          GetEmailTemplate, UpdateEmailTemplate, DeleteEmailTemplate,
          ListEmailTemplates, GetAccount, ListSuppressedDestinations,
          PutAccountSuppressionAttributes, TagResource, UntagResource,
          ListTagsForResource, CreateTenant, GetTenant, ListTenants, DeleteTenant,
          CreateTenantResourceAssociation, DeleteTenantResourceAssociation,
          ListTenantResources, ListResourceTenants, PutTenantSuppressionAttributes,
          CreateDedicatedIpPool, GetDedicatedIpPool, ListDedicatedIpPools,
          DeleteDedicatedIpPool.
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
    _account_details,
    _build_mime_message,
    _dkim_tokens,
    _message_rejection,
    _parse_raw_mime,
    _render_template,
    _restore_regional_store,
    _sent_emails_list,
    _smtp_relay,
    _templates,
)
from ministack.services.ses import (
    _configuration_sets as _v1_config_sets,
)
from ministack.services.ses import (
    _identities as _v1_identities,
)

logger = logging.getLogger("ses-v2")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
TEMPLATE_PAGE_SIZE = 10  # ListEmailTemplates default per the AWS API reference

_identities = AccountRegionScopedDict()  # identity -> dict
_config_sets = AccountRegionScopedDict()  # name -> dict
_ses_tags = AccountRegionScopedDict()  # resource_arn -> [tags]
_tenants = AccountRegionScopedDict()
_tenant_resources = AccountRegionScopedDict()  # tenant name -> {ARN: timestamp}
_dedicated_ip_pools = AccountRegionScopedDict()  # pool name -> {PoolName, ScalingMode}


def get_state() -> dict:
    return copy.deepcopy({
        "_identities": _identities,
        "_config_sets": _config_sets,
        "_ses_tags": _ses_tags,
        "_tenants": _tenants,
        "_tenant_resources": _tenant_resources,
        "_dedicated_ip_pools": _dedicated_ip_pools,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data: dict):
    _restore_regional_store(_identities, data.get("_identities", {}))
    _restore_regional_store(_config_sets, data.get("_config_sets", {}))
    _restore_tag_store(data.get("_ses_tags", {}))
    _restore_regional_store(_tenants, data.get("_tenants", {}))
    _restore_regional_store(_tenant_resources, data.get("_tenant_resources", {}))
    _restore_regional_store(_dedicated_ip_pools, data.get("_dedicated_ip_pools", {}))


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




def _json_err(code, message, status=400):
    body = json.dumps({"message": message, "name": code}).encode("utf-8")
    headers = {"Content-Type": "application/json", "x-amzn-errortype": code}
    return status, headers, body


def _resource_arn(kind, name):
    return f"arn:aws:ses:{get_region()}:{get_account_id()}:{kind}/{name}"


def _easy_dkim_attributes(identity, identity_type, signing_attributes):
    """A DOMAIN identity uses Easy DKIM unless DkimSigningAttributes brings its
    own key (BYODKIM): three tokens for its CNAME records, verification
    pending. An EMAIL_ADDRESS identity has no DKIM tokens."""
    byodkim = any(signing_attributes.get(k) for k in ("DomainSigningPrivateKey", "DomainSigningSelector"))
    if identity_type != "DOMAIN" or byodkim:
        return {"SigningEnabled": False, "Status": "NOT_STARTED", "Tokens": []}
    tokens = _dkim_tokens(identity)
    return {
        "SigningEnabled": False,
        "SigningAttributesOrigin": "AWS_SES",
        "Status": "PENDING",
        "Tokens": tokens,
    }


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
    if size < 1:
        return default, _json_err(
            "BadRequestException",
            f"1 validation error detected: Value '{raw}' at 'pageSize' failed to satisfy constraint: Member must have value greater than or equal to 1",
        )
    if size > maximum:
        return default, _json_err(
            "BadRequestException",
            f"1 validation error detected: Value '{raw}' at 'pageSize' failed to satisfy constraint: Member must have value less than or equal to {maximum}",
        )
    return size, None


def _body_paging(data):
    """NextToken / PageSize sent in a JSON body, in the query-parameter form."""
    return {k: str(data[k]) for k in ("NextToken", "PageSize") if data.get(k) is not None}


def _paginate(items, query_params, default_size, maximum=100):
    size, err = _page_size(query_params, default_size, maximum)
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
    canonical = str(spec)

    kind, sep, name = spec.resource.partition("/")
    if sep != "/" or not name or ("/" in name and kind != "tenant"):
        return None, _invalid_resource_arn(arn)

    if kind == "identity":
        if name not in _identities:
            return None, _not_found_resource_arn(arn)
    elif kind == "configuration-set":
        if name not in _config_sets:
            return None, _not_found_resource_arn(arn)
    elif kind == "dedicated-ip-pool":
        if name not in _dedicated_ip_pools:
            return None, _not_found_resource_arn(arn)
    elif kind == "tenant":
        parts = name.split("/")
        if len(parts) == 1:
            return None, _json_err(
                "NotFoundException",
                f"No Tenant present with name: nullwith tenantId: {name}",
                404,
            )
        if len(parts) != 2 or not all(parts):
            return None, _invalid_resource_arn(arn)
        tenant_name, tenant_id = parts
        rec = _tenants.get(tenant_name)
        if not rec or rec["TenantArn"] != canonical:
            return None, _json_err(
                "NotFoundException",
                f"No Tenant present with name: {tenant_name}with tenantId: {tenant_id}",
                404,
            )
    else:
        return None, _invalid_resource_arn(arn)

    return canonical, None


# The ResourceType enum of a tenant's associated resources, by ARN resource kind.
_TENANT_RESOURCE_TYPES = {
    "identity": "EMAIL_IDENTITY", "configuration-set": "CONFIGURATION_SET", "template": "EMAIL_TEMPLATE",
}


def _missing_tenant(name):
    return _json_err("NotFoundException", f"The requested tenant <{name}> does not exist.", 404)


def _association_resource(arn):
    """Validate an association ARN; returns (kind, canonical aws-partition ARN, error)."""
    try:
        spec = parse_arn(arn)
    except (ArnParseError, TypeError):
        return None, None, _json_err("BadRequestException", "Provided resource identifier is not an SES resource")
    if spec.service != "ses":
        return None, None, _json_err("BadRequestException", "Provided ARN is not in SES resource ARN format")
    kind, _, name = spec.resource.partition("/")
    if kind not in ("configuration-set", "identity", "template"):
        return None, None, _json_err("BadRequestException", f"Unsupported resource type: {kind}")
    if spec.region != get_region():
        return None, None, _json_err("BadRequestException", f"Resource <{arn}> must be in the same region")
    if spec.account_id != get_account_id():
        return None, None, _json_err("BadRequestException", f"Resource <{arn}> must be in the same account")
    stores, label = {
        "configuration-set": ((_v1_config_sets, _config_sets), "Configuration set"),
        "identity": ((_v1_identities, _identities), "Identity"),
        "template": ((_templates,), "Template"),
    }[kind]
    if not any(name in store for store in stores):
        return None, None, _json_err("NotFoundException", f"{label} <{name}> does not exist:", 404)
    canonical = f"arn:aws:{spec.service}:{spec.region}:{spec.account_id}:{spec.resource}"
    return kind, canonical, None


_SUPPRESSED_REASONS = ("BOUNCE", "COMPLAINT")
_SUPPRESSION_SCOPES = ("TENANT", "ACCOUNT")


def _suppression_enum_errors(prefix, reasons, reasons_present, scope, scope_present):
    """Model-level enum errors for suppression attributes, in member order."""
    errors = []
    if reasons_present:
        members = reasons if isinstance(reasons, list) else [reasons]
        if any(m not in _SUPPRESSED_REASONS for m in members):
            errors.append(
                f"Value at '{prefix}suppressedReasons' failed to satisfy constraint: Member must satisfy constraint: [Member must satisfy enum value set: [BOUNCE, COMPLAINT]]"
            )
    if scope_present and scope not in _SUPPRESSION_SCOPES:
        errors.append(
            f"Value at '{prefix}suppressionScope' failed to satisfy constraint: Member must satisfy enum value set: [TENANT, ACCOUNT]"
        )
    return errors


def _combined_validation_error(errors):
    noun = "error" if len(errors) == 1 else "errors"
    return _json_err("BadRequestException", f"{len(errors)} validation {noun} detected: {'; '.join(errors)}")


def _create_suppression_pairing(attrs):
    """Pairing/null rules for CreateTenant SuppressionAttributes; returns (value, error)."""
    reasons_present = "SuppressedReasons" in attrs
    scope_present = "SuppressionScope" in attrs
    reasons = attrs.get("SuppressedReasons")
    scope = attrs.get("SuppressionScope")
    if bool(reasons) and not scope_present:
        return None, _json_err("BadRequestException", "SuppressedReasons cannot be specified without SuppressionScope.")
    if scope_present and not reasons_present:
        return None, _json_err("BadRequestException", "SuppressionScope cannot be specified without SuppressedReasons.")
    if not bool(reasons) and not scope_present:
        nulls = []
        if not reasons_present:
            nulls.append("Value null at 'suppressionAttributes.suppressedReasons' failed to satisfy constraint: Member must not be null")
        if not scope_present:
            nulls.append("Value null at 'suppressionAttributes.suppressionScope' failed to satisfy constraint: Member must not be null")
        return None, _combined_validation_error(nulls)
    return {"SuppressedReasons": list(reasons) if isinstance(reasons, list) else reasons, "SuppressionScope": scope}, None


def _put_suppression_pairing(data):
    """Pairing rules for PutTenantSuppressionAttributes; returns (value, clear, error)."""
    reasons_present = "SuppressedReasons" in data
    scope_present = "SuppressionScope" in data
    if not reasons_present and not scope_present:
        return None, True, None
    reasons = data.get("SuppressedReasons")
    scope = data.get("SuppressionScope")
    if bool(reasons) and not scope_present:
        return None, False, _json_err("BadRequestException", "SuppressedReasons cannot be specified without SuppressionScope.")
    if reasons_present and not scope_present:
        return None, False, _json_err("BadRequestException", "SuppressionScope is required when SuppressedReasons are provided. Valid values are: TENANT, ACCOUNT")
    if scope_present and not reasons_present:
        return None, False, _json_err("BadRequestException", "SuppressionScope cannot be specified without SuppressedReasons.")
    return {"SuppressedReasons": list(reasons) if isinstance(reasons, list) else reasons, "SuppressionScope": scope}, False, None


def _tenant_delete_block(arn):
    if any(arn in resources for resources in _tenant_resources.values()):
        return _json_err(
            "BadRequestException",
            f"Cannot delete <{arn}> because it has tenant associations. Remove all tenant associations and try again.",
        )
    return None


def _tenant_request(method, sub, data):
    if method == "POST" and sub == "/resources/tenants/list":
        arn = data.get("ResourceArn", "")
        _, canonical, err = _association_resource(arn)
        if err:
            return err
        items = [
            {
                "TenantName": name,
                "TenantId": _tenants[name]["TenantId"],
                "ResourceArn": canonical,
                "AssociatedTimestamp": resources[canonical],
            }
            for name, resources in _tenant_resources.items()
            if canonical in resources
        ]
        page, token, err = _paginate(items, _body_paging(data), 100)
        return err or json_response({"ResourceTenants": page, **({"NextToken": token} if token else {})})
    if method == "POST" and sub == "/tenant/suppression":
        enum_errors = _suppression_enum_errors(
            "",
            data.get("SuppressedReasons"), "SuppressedReasons" in data,
            data.get("SuppressionScope"), "SuppressionScope" in data,
        )
        if enum_errors:
            return _combined_validation_error(enum_errors)
        value, clear, err = _put_suppression_pairing(data)
        if err:
            return err
        rec = _tenants.get(data.get("TenantName", ""))
        if rec is None:
            return _missing_tenant(data.get("TenantName", ""))
        if clear:
            rec.pop("SuppressionAttributes", None)
        else:
            rec["SuppressionAttributes"] = value
        return json_response({})
    if method != "POST" or not sub.startswith("/tenants"):
        return None
    name = data.get("TenantName", "")
    if sub == "/tenants":
        attrs = data.get("SuppressionAttributes")
        if attrs is not None:
            if not isinstance(attrs, dict):
                attrs = {}
            enum_errors = _suppression_enum_errors(
                "suppressionAttributes.",
                attrs.get("SuppressedReasons"), "SuppressedReasons" in attrs,
                attrs.get("SuppressionScope"), "SuppressionScope" in attrs,
            )
            if enum_errors:
                return _combined_validation_error(enum_errors)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            return _json_err(
                "BadRequestException",
                f"Invalid tenant name <{name}>: only alphanumeric ASCII characters, '_', and '-' are allowed.",
            )
        suppression = None
        if attrs is not None:
            suppression, err = _create_suppression_pairing(attrs)
            if err:
                return err
        if name in _tenants:
            return _json_err(
                "AlreadyExistsException", f"Tenant with name {name} already exists in account {get_account_id()}"
            )
        tenant_id = "tn-" + new_uuid().replace("-", "")[:29]
        arn = _resource_arn("tenant", f"{name}/{tenant_id}")
        rec = {
            "TenantName": name,
            "TenantId": tenant_id,
            "TenantArn": arn,
            "CreatedTimestamp": int(time.time()),
            "Tags": copy.deepcopy(data.get("Tags", [])),
            "SendingStatus": "ENABLED",
        }
        if suppression is not None:
            rec["SuppressionAttributes"] = suppression
        _tenants[name] = rec
        _tenant_resources[name] = {}
        _ses_tags[arn] = copy.deepcopy(rec["Tags"])
        return json_response(rec)
    if sub == "/tenants/list":
        filters = data.get("Filter") or {}
        if filters.keys() - {"SENDING_STATUS", "TENANT_NAME_CONTAINS"}:
            return _json_err(
                "BadRequestException",
                "1 validation error detected: Value at 'filter' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy enum value set: [SENDING_STATUS, TENANT_NAME_CONTAINS]]",
            )
        status = filters.get("SENDING_STATUS")
        if status is not None and status not in ("ENABLED", "REINSTATED", "DISABLED"):
            return _json_err("BadRequestException", f"Invalid sending status <{status}>.")
        items = sorted(
            (
                {k: v for k, v in rec.items() if k not in ("Tags", "SuppressionAttributes")}
                for rec in _tenants.values()
                if filters.get("TENANT_NAME_CONTAINS", "") in rec["TenantName"]
                and (not filters.get("SENDING_STATUS") or filters["SENDING_STATUS"] == rec["SendingStatus"])
            ),
            key=lambda item: item["TenantName"],
        )
        page, token, err = _paginate(items, _body_paging(data), 100)
        return err or json_response({"Tenants": page, **({"NextToken": token} if token else {})})
    if sub not in (
        "/tenants/get",
        "/tenants/delete",
        "/tenants/resources",
        "/tenants/resources/delete",
        "/tenants/resources/list",
    ):
        return None
    rec = _tenants.get(name)
    if rec is None:
        return _missing_tenant(name)
    if sub == "/tenants/get":
        out = copy.deepcopy(rec)
        out["Tags"] = _ses_tags.get(rec["TenantArn"], [])
        return json_response({"Tenant": out})
    if sub == "/tenants/delete":
        _tenants.pop(name)
        _tenant_resources.pop(name, None)
        _ses_tags.pop(rec["TenantArn"], None)
        return json_response({})
    resources = _tenant_resources[name]
    if sub == "/tenants/resources/list":
        items = [{"ResourceType": _TENANT_RESOURCE_TYPES[parse_arn(arn).resource.split("/")[0]], "ResourceArn": arn}
                 for arn in resources]
        filters = data.get("Filter") or {}
        if filters.keys() - {"RESOURCE_TYPE"}:
            return _json_err(
                "BadRequestException",
                "1 validation error detected: Value at 'filter' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy enum value set: [RESOURCE_TYPE]]",
            )
        resource_type = filters.get("RESOURCE_TYPE")
        if resource_type is not None and resource_type not in _TENANT_RESOURCE_TYPES.values():
            return _json_err("BadRequestException", f"Invalid resource type {resource_type} specified.")
        items = sorted(
            (
                item
                for item in items
                if not filters.get("RESOURCE_TYPE") or item["ResourceType"] == filters["RESOURCE_TYPE"]
            ),
            key=lambda item: item["ResourceArn"],
        )
        page, token, err = _paginate(items, _body_paging(data), 100)
        return err or json_response({"TenantResources": page, **({"NextToken": token} if token else {})})
    arn = data.get("ResourceArn", "")
    _, canonical, err = _association_resource(arn)
    if err:
        return err
    if sub.endswith("/delete"):
        resources.pop(canonical, None)
    else:
        if canonical in resources:
            return _json_err(
                "AlreadyExistsException", f"Resources {arn} has already been associated with tenant {name}"
            )
        resources[canonical] = int(time.time())
    return json_response({})


async def handle_request(method, path, headers, body, query_params):
    # The SES dispatcher also accepts unprefixed REST paths when selected by
    # a SESv2 target header. Preserve those paths and trailing-slash handling
    # from its former inline v2 implementation.
    sub = path.rstrip("/")
    if sub.startswith("/v2/email"):
        sub = sub[len("/v2/email"):]

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    result = _tenant_request(method, sub, data)
    if result is not None:
        return result

    # GET /v2/email/account
    if sub == "/account" and method == "GET":
        cutoff = time.time() - 86400
        sent_list = _sent_emails_list()
        sent_24h = sum(1 for e in sent_list if e["Timestamp"] >= cutoff)
        account = _account_details.get("account") or {}
        out = {
            "DedicatedIpAutoWarmupEnabled": False,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": account.get("ProductionAccessEnabled", True),
            "SendQuota": {"Max24HourSend": 50000.0, "MaxSendRate": 14.0, "SentLast24Hours": float(sent_24h)},
            "SendingEnabled": True,
            "SuppressionAttributes": {"SuppressedReasons": []},
        }
        if account.get("Details"):
            out["Details"] = account["Details"]
        return json_response(out)

    # POST /v2/email/account/details  (PutAccountDetails)
    if sub == "/account/details" and method == "POST":
        if data.get("MailType") not in ("MARKETING", "TRANSACTIONAL"):
            return _json_err("BadRequestException", "MailType must be MARKETING or TRANSACTIONAL")
        if not data.get("WebsiteURL"):
            return _json_err("BadRequestException", "WebsiteURL is required")
        account = dict(_account_details.get("account") or {})
        account["Details"] = {k: data[k] for k in (
            "MailType", "WebsiteURL", "ContactLanguage", "UseCaseDescription",
            "AdditionalContactEmailAddresses") if k in data}
        if "ProductionAccessEnabled" in data:
            account["ProductionAccessEnabled"] = bool(data["ProductionAccessEnabled"])
        _account_details["account"] = account
        return json_response({})

    # PUT /v2/email/account/suppression
    if sub == "/account/suppression" and method == "PUT":
        return json_response({})

    # GET /v2/email/suppression/addresses
    if sub == "/suppression/addresses" and method == "GET":
        return json_response({"SuppressedDestinationSummaries": []})

    # POST /v2/email/outbound-emails  (SendEmail)
    if sub == "/outbound-emails" and method == "POST":
        msg_id = f"{new_uuid()}@email.amazonses.com"
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
        rejected = _message_rejection(source or (parsed.get("From", "") if raw else ""), all_addrs)
        if rejected:
            return _json_err("MessageRejected", rejected)
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

    # POST /v2/email/outbound-bulk-emails  (SendBulkEmail)
    if sub == "/outbound-bulk-emails" and method == "POST":
        source = data.get("FromEmailAddress", "")
        config_set = data.get("ConfigurationSetName", "")
        tpl = data.get("DefaultContent", {}).get("Template", {})
        if not tpl:
            return _json_err("BadRequestException", "DefaultContent.Template is required")
        stored, template_name, err = _resolve_send_template(tpl)
        if err:
            return err
        default_data = tpl.get("TemplateData", "")
        entries = data.get("BulkEmailEntries", [])
        if not entries:
            return _json_err("BadRequestException", "BulkEmailEntries is required")

        rejected = _message_rejection(source)
        if rejected:
            return _json_err("MessageRejected", rejected)
        results = []
        for entry in entries:
            dest = entry.get("Destination", {})
            to_addrs = dest.get("ToAddresses", [])
            cc_addrs = dest.get("CcAddresses", [])
            bcc_addrs = dest.get("BccAddresses", [])
            template_data = (
                entry.get("ReplacementEmailContent", {})
                     .get("ReplacementTemplate", {})
                     .get("ReplacementTemplateData", default_data)
            )
            rejected = _message_rejection(None, to_addrs + cc_addrs + bcc_addrs)
            if rejected:
                results.append({"Status": "MESSAGE_REJECTED", "Error": rejected})
                continue
            rendered = _render_template(stored, template_data)
            subj = rendered.get("Subject", "")
            body_text = rendered.get("Text", "")
            body_html = rendered.get("Html", "")
            msg_id = f"{new_uuid()}@email.amazonses.com"

            all_addrs = to_addrs + cc_addrs + bcc_addrs
            if source and all_addrs:
                mime_str = _build_mime_message(source, to_addrs, cc_addrs, bcc_addrs,
                                               subj, body_text, body_html, msg_id)
                _smtp_relay(source, all_addrs, mime_str)

            record = {
                "MessageId": msg_id,
                "Source": source,
                "To": to_addrs,
                "CC": cc_addrs,
                "BCC": bcc_addrs,
                "Subject": subj,
                "BodyText": body_text,
                "BodyHtml": body_html,
                "TemplateData": template_data,
                "Timestamp": time.time(),
                "Type": "v2.SendBulkEmail",
            }
            if template_name:
                record["Template"] = template_name
            if config_set:
                record["ConfigurationSetName"] = config_set
            _sent_emails_list().append(record)
            results.append({"Status": "SUCCESS", "MessageId": msg_id})

        logger.info("SESv2 SendBulkEmail: %s | template=%s | %s entries",
                    source, template_name or "<inline>", len(entries))
        return json_response({"BulkEmailEntryResults": results})

    # POST /v2/email/identities  (CreateEmailIdentity)
    if sub == "/identities" and method == "POST":
        identity = data.get("EmailIdentity", "")
        if not identity:
            return _json_err("BadRequestException", "EmailIdentity is required")
        identity_type = "DOMAIN" if "." in identity and "@" not in identity else "EMAIL_ADDRESS"
        dkim_attributes = _easy_dkim_attributes(
            identity, identity_type, data.get("DkimSigningAttributes") or {})
        _identities[identity] = {
            "EmailIdentity": identity,
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": True,
            "DkimAttributes": dkim_attributes,
            "MailFromAttributes": {"BehaviorOnMxFailure": "USE_DEFAULT_VALUE"},
            "Tags": data.get("Tags", []),
            "CreatedTimestamp": now_iso(),
        }
        _ses_tags[_resource_arn("identity", identity)] = list(data.get("Tags", []))
        return json_response({
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": True,
            "DkimAttributes": dkim_attributes,
        })

    # ListEmailIdentities: GET /v2/email/identities, or POST /v2/email/list-identities
    # with paging and Filter in the body (newer SDKs)
    if (sub == "/identities" and method == "GET") or (sub == "/list-identities" and method == "POST"):
        params = query_params if method == "GET" else _body_paging(data)
        wanted = (data.get("Filter") or {}) if method == "POST" else {}
        items = [
            {"IdentityType": v["IdentityType"], "IdentityName": k, "SendingEnabled": True,
             "VerificationStatus": "SUCCESS"}
            for k, v in _identities.items()
        ]
        items = [i for i in items
                 if wanted.get("IDENTITY_NAME_CONTAINS", "") in i["IdentityName"]
                 and wanted.get("IDENTITY_TYPE", i["IdentityType"]) == i["IdentityType"]
                 and wanted.get("VERIFICATION_STATUS", "SUCCESS") == "SUCCESS"]
        page, next_token, err = _paginate(items, params, 1000, maximum=1000)
        if err:
            return err
        out = {"EmailIdentities": page}
        if next_token:
            out["NextToken"] = next_token
        return json_response(out)

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
            blocked = _tenant_delete_block(_resource_arn("identity", identity))
            if blocked:
                return blocked
            _identities.pop(identity, None)
            return json_response({})

    # /v2/email/dedicated-ip-pools  (Create/Get/List/DeleteDedicatedIpPool)
    if sub == "/dedicated-ip-pools" and method == "POST":
        name = data.get("PoolName", "")
        if not name:
            return _json_err("BadRequestException", "PoolName is required")
        if name in _dedicated_ip_pools:
            return _json_err("AlreadyExistsException", f"Pool {name} already exists")
        _dedicated_ip_pools[name] = {"PoolName": name, "ScalingMode": data.get("ScalingMode") or "STANDARD"}
        _ses_tags[_resource_arn("dedicated-ip-pool", name)] = list(data.get("Tags", []))
        return json_response({})

    if sub == "/dedicated-ip-pools" and method == "GET":
        page, next_token, err = _paginate(list(_dedicated_ip_pools.keys()), query_params, 100, maximum=1000)
        if err:
            return err
        out = {"DedicatedIpPools": page}
        if next_token:
            out["NextToken"] = next_token
        return json_response(out)

    m = re.match(r"^/dedicated-ip-pools/([^/]+)$", sub)
    if m and method in ("GET", "DELETE"):
        name = m.group(1)
        pool = _dedicated_ip_pools.get(name)
        if pool is None:
            return _json_err("NotFoundException", f"Pool {name} does not exist", 404)
        if method == "GET":
            return json_response({"DedicatedIpPool": dict(pool)})
        _dedicated_ip_pools.pop(name, None)
        _ses_tags.pop(_resource_arn("dedicated-ip-pool", name), None)
        return json_response({})

    # POST /v2/email/configuration-sets  (CreateConfigurationSet)
    if sub == "/configuration-sets" and method == "POST":
        name = data.get("ConfigurationSetName", "")
        if not name:
            return _json_err("BadRequestException", "ConfigurationSetName is required")
        _config_sets[name] = {"ConfigurationSetName": name, "Tags": data.get("Tags", [])}
        _ses_tags[_resource_arn("configuration-set", name)] = list(data.get("Tags", []))
        return json_response({})

    # ListConfigurationSets: GET /v2/email/configuration-sets, or
    # POST /v2/email/list-configuration-sets with paging and Filter in the body
    if ((sub == "/configuration-sets" and method == "GET")
            or (sub == "/list-configuration-sets" and method == "POST")):
        params = query_params if method == "GET" else _body_paging(data)
        contains = ((data.get("Filter") or {}).get("CONFIGURATION_SET_NAME_CONTAINS", "")
                    if method == "POST" else "")
        page, next_token, err = _paginate(
            [n for n in _config_sets if contains in n], params, 1000, maximum=1000)
        if err:
            return err
        out = {"ConfigurationSets": page}
        if next_token:
            out["NextToken"] = next_token
        return json_response(out)

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
            arn = _resource_arn("configuration-set", name)
            blocked = _tenant_delete_block(arn)
            if blocked:
                return blocked
            _config_sets.pop(name, None)
            _ses_tags.pop(arn, None)
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
            blocked = _tenant_delete_block(_resource_arn("template", name))
            if blocked:
                return blocked
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
    _tenants.clear()
    _tenant_resources.clear()
    _dedicated_ip_pools.clear()
