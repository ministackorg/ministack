# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
WAF v2 Service Emulator.
JSON-based API via X-Amz-Target: AWSWAF_20190729.
Supports: CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs,
          AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL,
          CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets,
          CreateRegexPatternSet, GetRegexPatternSet, UpdateRegexPatternSet,
          DeleteRegexPatternSet, ListRegexPatternSets,
          CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups,
          TagResource, UntagResource, ListTagsForResource,
          CheckCapacity, DescribeManagedRuleGroup.
Data plane: evaluate_web_acl decides a CloudFront viewer request.
"""

import base64
import collections
import copy
import hashlib
import ipaddress
import json
import logging
import operator
import os
import re
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    new_uuid,
)
from ministack.core.responses import (
    json_response as _core_json_response,
)

logger = logging.getLogger("wafv2")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CLOUDFRONT_HOME_REGION = "us-east-1"

_web_acls = AccountRegionScopedDict()       # id -> webacl
_ip_sets = AccountRegionScopedDict()        # id -> ipset
_regex_pattern_sets = AccountRegionScopedDict()  # id -> regexpatternset
_rule_groups = AccountRegionScopedDict()    # id -> rulegroup
# AssociateWebACL is for regional application resources; CloudFront uses the
# distribution configuration path. Keep this request-region scoped even though
# the mock accepts CLOUDFRONT-scope WebACL ARNs for compatibility.
_associations = AccountRegionScopedDict()   # resource_arn -> webacl_arn
_waf_tags = AccountScopedDict()       # resource_arn -> [tags]

_WAF_RESOURCE_SCOPES = {
    "webacl": {"regional", "cloudfront", "global"},
    "ipset": {"regional", "global"},
    "regexpatternset": {"regional", "global"},
    "rulegroup": {"regional", "global"},
}


def _drop_none(value):
    """AWS omits unset members; it never sends null."""
    if isinstance(value, dict):
        return {k: _drop_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_none(v) for v in value]
    return value


def json_response(data, status=200, **kwargs):
    return _core_json_response(_drop_none(data), status, **kwargs)


def get_state():
    return {
        "_web_acls": copy.deepcopy(_web_acls),
        "_ip_sets": copy.deepcopy(_ip_sets),
        "_regex_pattern_sets": copy.deepcopy(_regex_pattern_sets),
        "_rule_groups": copy.deepcopy(_rule_groups),
        "_associations": copy.deepcopy(_associations),
        "_waf_tags": copy.deepcopy(_waf_tags),
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not isinstance(data, dict):
        return
    arn_rewrites = {}
    _restore_resource_store(_web_acls, data.get("_web_acls", {}), "webacl", arn_rewrites)
    _restore_resource_store(_ip_sets, data.get("_ip_sets", {}), "ipset", arn_rewrites)
    _restore_resource_store(
        _regex_pattern_sets, data.get("_regex_pattern_sets", {}), "regexpatternset", arn_rewrites
    )
    _restore_resource_store(_rule_groups, data.get("_rule_groups", {}), "rulegroup", arn_rewrites)
    _rewrite_nested_resource_arns(arn_rewrites)
    _restore_associations(data.get("_associations", {}), arn_rewrites)
    _restore_waf_tags(data.get("_waf_tags", {}), arn_rewrites)


def _restore_resource_store(store, saved, resource_type, arn_rewrites):
    if isinstance(saved, AccountRegionScopedDict):
        store.update(saved)
        return

    boot_region = get_region()
    for account_id, uid, resource in _legacy_account_items(saved):
        resource_copy = copy.deepcopy(resource)
        scope = _normalize_scope(resource_copy.get("Scope", "REGIONAL"))
        resource_copy["Scope"] = scope
        old_arn = resource_copy.get("ARN")
        if scope == "CLOUDFRONT":
            region = CLOUDFRONT_HOME_REGION
            name = resource_copy.get("Name") or _name_from_waf_arn(old_arn) or uid
            new_arn = _resource_arn(resource_type, name, uid, scope, account_id=account_id)
            if old_arn and old_arn != new_arn:
                arn_rewrites[old_arn] = new_arn
            resource_copy["ARN"] = new_arn
        else:
            region = _region_from_arnish(old_arn, fallback=boot_region)
        store.set_scoped(account_id, region, uid, resource_copy)


def _restore_associations(saved, arn_rewrites):
    boot_region = get_region()
    if isinstance(saved, AccountRegionScopedDict):
        for (account_id, region, resource_arn), web_acl_arn in saved.all_items():
            _associations.set_scoped(
                account_id,
                region,
                resource_arn,
                arn_rewrites.get(web_acl_arn, web_acl_arn),
            )
        return
    for account_id, resource_arn, web_acl_arn in _legacy_account_items(saved):
        region = _region_from_arnish(resource_arn, fallback=boot_region)
        _associations.set_scoped(
            account_id,
            region,
            resource_arn,
            arn_rewrites.get(web_acl_arn, web_acl_arn),
        )


def _restore_waf_tags(saved, arn_rewrites):
    if isinstance(saved, AccountScopedDict):
        for (account_id, arn), tags in saved._data.items():
            _waf_tags.set_scoped(
                account_id,
                None,
                arn_rewrites.get(arn, arn),
                copy.deepcopy(tags),
            )
        return
    if isinstance(saved, dict):
        for arn, tags in saved.items():
            _waf_tags[arn_rewrites.get(arn, arn)] = copy.deepcopy(tags)


def _rewrite_nested_resource_arns(arn_rewrites):
    if not arn_rewrites:
        return
    for store in (_web_acls, _ip_sets, _rule_groups):
        for _scoped_key, resource in store.all_items():
            _rewrite_nested_arn_values(resource, arn_rewrites)


def _rewrite_nested_arn_values(value, arn_rewrites):
    if isinstance(value, dict):
        for key, nested in value.items():
            value[key] = _rewrite_nested_arn_values(nested, arn_rewrites)
        return value
    if isinstance(value, list):
        for index, nested in enumerate(value):
            value[index] = _rewrite_nested_arn_values(nested, arn_rewrites)
        return value
    if isinstance(value, str):
        return arn_rewrites.get(value, value)
    return value


def _legacy_account_items(saved):
    if isinstance(saved, AccountScopedDict):
        for (account_id, key), value in saved._data.items():
            yield account_id, key, value
    elif isinstance(saved, dict):
        account_id = get_account_id()
        for key, value in saved.items():
            yield account_id, key, value


def _region_from_arnish(value, fallback):
    if not isinstance(value, str) or not value.startswith("arn:"):
        return fallback
    try:
        region = parse_arn(value).region
    except ArnParseError:
        return fallback
    return region or fallback


def _name_from_waf_arn(arn):
    if not isinstance(arn, str):
        return None
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    parts = spec.resource.split("/")
    if len(parts) == 4:
        return parts[2] or None
    return None


def _normalize_scope(scope):
    return (scope or "REGIONAL").upper()


def _scope_home_region(scope):
    return CLOUDFRONT_HOME_REGION if _normalize_scope(scope) == "CLOUDFRONT" else get_region()


def _scope_arn_region_and_segment(scope):
    if _normalize_scope(scope) == "CLOUDFRONT":
        return CLOUDFRONT_HOME_REGION, "global"
    return get_region(), "regional"


def _resource_arn(resource_type, name, uid, scope, account_id=None, region=None):
    arn_region, segment = _scope_arn_region_and_segment(scope)
    region = region or arn_region
    account_id = account_id or get_account_id()
    return f"arn:aws:wafv2:{region}:{account_id}:{segment}/{resource_type}/{name}/{uid}"


_WEB_ACL_OPTIONAL = (
    "DataProtectionConfig", "CustomResponseBodies", "CaptchaConfig", "ChallengeConfig", "TokenDomains",
    "AssociationConfig", "OnSourceDDoSProtectionConfig", "ApplicationConfig", "MonetizationConfig",
)
_RULE_GROUP_OPTIONAL = ("CustomResponseBodies", "MonetizationConfig")


def _apply_optional(record, props, keys):
    """Store each optional member as sent; an update replaces them, so an omitted one is cleared."""
    for key in keys:
        if props.get(key) is not None:
            record[key] = props[key]
        else:
            record.pop(key, None)


def create_web_acl_record(name, scope, props):
    scope = _normalize_scope(scope)
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _acl_arn(name, uid, scope)
    record = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": props.get("Description", ""),
        "DefaultAction": props.get("DefaultAction", {"Allow": {}}),
        "Rules": props.get("Rules", []),
        "VisibilityConfig": props.get("VisibilityConfig", {}),
        "Capacity": 0,
        "LockToken": lock_token,
        "Scope": scope,
    }
    _apply_optional(record, props, _WEB_ACL_OPTIONAL)
    _web_acls.set_scoped(get_account_id(), _scope_home_region(scope), uid, record)
    _waf_tags[arn] = props.get("Tags", [])
    return uid, arn, record


def web_acl_record(uid, scope):
    """The stored web ACL, or None. The CloudFormation provisioner reads it
    to update an ACL in place."""
    return _resource_from_scope(_web_acls, uid, scope)


def update_web_acl_record(acl, props):
    """Apply the members ``create_web_acl_record`` stores to an existing web
    ACL and roll its lock token, as ``UpdateWebACL`` does. The ACL keeps its
    id, ARN and tags."""
    acl["Description"] = props.get("Description", "")
    acl["DefaultAction"] = props.get("DefaultAction", {"Allow": {}})
    acl["Rules"] = props.get("Rules", [])
    acl["VisibilityConfig"] = props.get("VisibilityConfig", {})
    _apply_optional(acl, props, _WEB_ACL_OPTIONAL)
    acl["LockToken"] = new_uuid()
    return acl


def delete_web_acl_record(uid, scope):
    acl = _web_acls.pop_scoped(get_account_id(), _scope_home_region(scope), uid, None)
    if acl:
        _waf_tags.pop(acl["ARN"], None)


def _resource_store(resource_type):
    return {
        "webacl": _web_acls,
        "ipset": _ip_sets,
        "regexpatternset": _regex_pattern_sets,
        "rulegroup": _rule_groups,
    }[resource_type]


def _resource_from_scope(store, uid, scope):
    return store.get_scoped(get_account_id(), _scope_home_region(scope), uid)


def _values_for_scope(store, scope):
    return store.values_scoped(get_account_id(), _scope_home_region(scope))




def _waf_err(code, message):
    return error_response_json(code, message, 400)


_WAFV2_ID_PATTERN = r"^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$"
_WAFV2_ID_RE = re.compile(_WAFV2_ID_PATTERN)


def _validate_web_acl_id(uid):
    """AWS validates the WebACL Id shape before checking existence — a non-UUID
    or >36-char value is a ValidationException, never WAFNonexistentItem.
    Evidence: real AWS wire capture observation."""
    errors = []
    if not _WAFV2_ID_RE.match(uid):
        errors.append(
            f"Value '{uid}' at 'id' failed to satisfy constraint: "
            f"Member must satisfy regular expression pattern: {_WAFV2_ID_PATTERN}"
        )
    if len(uid) > 36:
        errors.append(
            f"Value '{uid}' at 'id' failed to satisfy constraint: "
            "Member must have length less than or equal to 36"
        )
    if errors:
        n = len(errors)
        return _waf_err(
            "ValidationException",
            f"{n} validation error{'s' if n > 1 else ''} detected: {'; '.join(errors)}",
        )
    return None


def _waf_invalid_arn(arn):
    return _waf_err("WAFInvalidParameterException", f"Invalid WAFv2 resource ARN: {arn}")


def _waf_not_found(arn):
    return _waf_err("WAFNonexistentItemException", f"WAFv2 resource {arn} not found")


def _acl_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("webacl", name, uid, scope)


def _ipset_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("ipset", name, uid, scope)


def _regex_pattern_set_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("regexpatternset", name, uid, scope)


def _rg_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("rulegroup", name, uid, scope)


def _parse_local_waf_arn(arn, allowed_types):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, None, _waf_invalid_arn(arn)

    if (
        spec.partition != "aws"
        or spec.service != "wafv2"
        or spec.account_id != get_account_id()
    ):
        return None, None, _waf_invalid_arn(arn)

    parts = spec.resource.split("/")
    if len(parts) != 4:
        return None, None, _waf_invalid_arn(arn)
    scope, resource_type, name, uid = parts
    if resource_type not in allowed_types or not name or not uid:
        return None, None, _waf_invalid_arn(arn)
    if scope not in _WAF_RESOURCE_SCOPES.get(resource_type, set()):
        return None, None, _waf_invalid_arn(arn)
    if scope == "regional":
        if spec.region != get_region():
            return None, None, _waf_invalid_arn(arn)
        region = spec.region
    else:
        if spec.region != CLOUDFRONT_HOME_REGION:
            return None, None, _waf_invalid_arn(arn)
        region = CLOUDFRONT_HOME_REGION
    return resource_type, (spec.account_id, region, uid), None


def _resolve_local_waf_resource_arn(arn, allowed_types=("webacl", "ipset", "regexpatternset", "rulegroup")):
    resource_type, scoped_key, err = _parse_local_waf_arn(arn, allowed_types)
    if err:
        return None, err

    account_id, region, uid = scoped_key
    resource = _resource_store(resource_type).get_scoped(account_id, region, uid)
    if not resource or resource.get("ARN") != arn:
        return None, _waf_not_found(arn)
    return arn, None


def _resolve_local_web_acl(web_acl_arn):
    arn, err = _resolve_local_waf_resource_arn(web_acl_arn, ("webacl",))
    if err:
        return None, err
    resource_type, scoped_key, err = _parse_local_waf_arn(arn, ("webacl",))
    if err:
        return None, err
    account_id, region, uid = scoped_key
    acl = _resource_store(resource_type).get_scoped(account_id, region, uid)
    if acl and acl["ARN"] == arn:
        return acl, None
    return None, _waf_not_found(web_acl_arn)


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateWebACL": _create_web_acl,
        "GetWebACL": _get_web_acl,
        "UpdateWebACL": _update_web_acl,
        "DeleteWebACL": _delete_web_acl,
        "ListWebACLs": _list_web_acls,
        "AssociateWebACL": _associate_web_acl,
        "DisassociateWebACL": _disassociate_web_acl,
        "GetWebACLForResource": _get_web_acl_for_resource,
        "ListResourcesForWebACL": _list_resources_for_web_acl,
        "CreateIPSet": _create_ip_set,
        "GetIPSet": _get_ip_set,
        "UpdateIPSet": _update_ip_set,
        "DeleteIPSet": _delete_ip_set,
        "ListIPSets": _list_ip_sets,
        "CreateRegexPatternSet": _create_regex_pattern_set,
        "GetRegexPatternSet": _get_regex_pattern_set,
        "UpdateRegexPatternSet": _update_regex_pattern_set,
        "DeleteRegexPatternSet": _delete_regex_pattern_set,
        "ListRegexPatternSets": _list_regex_pattern_sets,
        "CreateRuleGroup": _create_rule_group,
        "GetRuleGroup": _get_rule_group,
        "UpdateRuleGroup": _update_rule_group,
        "DeleteRuleGroup": _delete_rule_group,
        "ListRuleGroups": _list_rule_groups,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "CheckCapacity": _check_capacity,
        "DescribeManagedRuleGroup": _describe_managed_rule_group,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown WAF action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# WebACL
# ---------------------------------------------------------------------------

_DUPLICATE_ITEM_MESSAGE = ("AWS WAF couldn’t perform the operation because some resource in your "
                          "request is a duplicate of an existing one.")


def _create_web_acl(data):
    name = data.get("Name", "")
    if not name:
        return _waf_err("WAFInvalidParameterException", "Name is required")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    for existing in _values_for_scope(_web_acls, scope):
        if existing["Name"] == name and existing.get("Scope") == scope:
            return _waf_err("WAFDuplicateItemException", _DUPLICATE_ITEM_MESSAGE)
    uid, arn, acl = create_web_acl_record(name, scope, data)
    logger.info("CreateWebACL: %s (%s)", name, uid)
    return json_response({"Summary": {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "LockToken": acl["LockToken"],
    }})



def _shape_view(record, shape):
    """The record's members that wafv2's output shape has; internal keys such as Scope stay out."""
    import botocore.session

    global _WAF_MODEL
    if "_WAF_MODEL" not in globals():
        _WAF_MODEL = botocore.session.get_session().get_service_model("wafv2")
    members = _WAF_MODEL.shape_for(shape).members
    return {k: v for k, v in record.items() if k in members}

def _get_web_acl(data):
    uid = data.get("Id", "")
    if err := _validate_web_acl_id(uid):
        return err
    acl = _resource_from_scope(_web_acls, uid, data.get("Scope", "REGIONAL"))
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    acl_body = _shape_view(acl, "WebACL")
    return json_response({"WebACL": acl_body, "LockToken": acl["LockToken"]})


def _update_web_acl(data):
    uid = data.get("Id", "")
    acl = _resource_from_scope(_web_acls, uid, data.get("Scope", "REGIONAL"))
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    lock_token = data.get("LockToken", "")
    if lock_token != acl.get("LockToken", ""):
        return _waf_err("WAFOptimisticLockException", "The resource you are trying to update has been modified by another request.")
    update_web_acl_record(acl, data)
    return json_response({"NextLockToken": acl["LockToken"]})


def _delete_web_acl(data):
    uid = data.get("Id", "")
    scope = data.get("Scope", "REGIONAL")
    acl = _resource_from_scope(_web_acls, uid, scope)
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    lock_token = data.get("LockToken", "")
    if lock_token != acl["LockToken"]:
        return _waf_err("WAFOptimisticLockException",
                        "The resource you are trying to update has changed. Please retry.")
    delete_web_acl_record(uid, scope)
    return json_response({})


def _marker_page(data, key, items):
    try:
        start = max(int(data.get("NextMarker") or 0), 0)
    except (TypeError, ValueError):
        start = 0
    end = start + (data.get("Limit") or len(items))
    resp = {key: items[start:end]}
    if end < len(items):
        resp["NextMarker"] = str(end)
    return json_response(resp)


def _list_web_acls(data):
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    acls = [
        {"ARN": a["ARN"], "Id": a["Id"], "Name": a["Name"],
         "Description": a.get("Description", ""), "LockToken": a["LockToken"]}
        for a in _values_for_scope(_web_acls, scope) if a.get("Scope", "REGIONAL") == scope
    ]
    return _marker_page(data, "WebACLs", acls)


# ---------------------------------------------------------------------------
# Association
# ---------------------------------------------------------------------------

def _associate_web_acl(data):
    web_acl_arn = data.get("WebACLArn", "")
    _, err = _resolve_local_web_acl(web_acl_arn)
    if err:
        return err
    resource_arn = data.get("ResourceArn", "")
    _associations[resource_arn] = web_acl_arn
    return json_response({})


def _disassociate_web_acl(data):
    resource_arn = data.get("ResourceArn", "")
    _associations.pop(resource_arn, None)
    return json_response({})


def _get_web_acl_for_resource(data):
    resource_arn = data.get("ResourceArn", "")
    web_acl_arn = _associations.get(resource_arn)
    if not web_acl_arn:
        return _waf_err("WAFNonexistentItemException", f"No WebACL associated with {resource_arn}")
    acl, err = _resolve_local_web_acl(web_acl_arn)
    if err:
        return err
    acl_body = _shape_view(acl, "WebACL")
    return json_response({"WebACL": acl_body})


def _list_resources_for_web_acl(data):
    web_acl_arn = data.get("WebACLArn", "")
    _, err = _resolve_local_web_acl(web_acl_arn)
    if err:
        return err
    # "If you don't provide a resource type, the call uses ... APPLICATION_LOAD_BALANCER" (service-2.json).
    resource_type = data.get("ResourceType") or "APPLICATION_LOAD_BALANCER"
    arns = [r for r, a in _associations.items()
            if a == web_acl_arn and _associated_resource_type(r) == resource_type]
    return json_response({"ResourceArns": arns})


def _associated_resource_type(resource_arn):
    parts = resource_arn.split(":", 5)
    service, resource = (parts[2], parts[5]) if len(parts) == 6 else ("", "")
    if service == "elasticloadbalancing":
        return "APPLICATION_LOAD_BALANCER"
    if service == "ec2" and resource.startswith("verified-access-instance/"):
        return "VERIFIED_ACCESS_INSTANCE"
    return {
        "apigateway": "API_GATEWAY",
        "appsync": "APPSYNC",
        "cognito-idp": "COGNITO_USER_POOL",
        "apprunner": "APP_RUNNER_SERVICE",
        "amplify": "AMPLIFY",
        "bedrock-agentcore": "AGENTCORE_GATEWAY",
    }.get(service, "")


# ---------------------------------------------------------------------------
# IPSet
# ---------------------------------------------------------------------------

def _create_ip_set(data):
    name = data.get("Name", "")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    for existing in _values_for_scope(_ip_sets, scope):
        if existing["Name"] == name and existing.get("Scope", scope) == scope:
            return _waf_err("WAFDuplicateItemException", _DUPLICATE_ITEM_MESSAGE)
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _ipset_arn(name, uid, scope)
    _ip_sets.set_scoped(get_account_id(), _scope_home_region(scope), uid, {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "IPAddressVersion": data.get("IPAddressVersion", "IPV4"),
        "Addresses": data.get("Addresses", []),
        "LockToken": lock_token,
        "Scope": scope,
    })
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name, "LockToken": lock_token}})


def _get_ip_set(data):
    uid = data.get("Id", "")
    ipset = _resource_from_scope(_ip_sets, uid, data.get("Scope", "REGIONAL"))
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    ipset_body = _shape_view(ipset, "IPSet")
    return json_response({"IPSet": ipset_body, "LockToken": ipset["LockToken"]})


def _update_ip_set(data):
    uid = data.get("Id", "")
    ipset = _resource_from_scope(_ip_sets, uid, data.get("Scope", "REGIONAL"))
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    ipset["Description"] = data.get("Description", "")
    ipset["Addresses"] = data.get("Addresses", ipset["Addresses"])
    ipset["LockToken"] = new_uuid()
    return json_response({"NextLockToken": ipset["LockToken"]})


def _delete_ip_set(data):
    uid = data.get("Id", "")
    ipset = _resource_from_scope(_ip_sets, uid, data.get("Scope", "REGIONAL"))
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    arn = ipset["ARN"]
    _ip_sets.pop_scoped(get_account_id(), _scope_home_region(data.get("Scope", "REGIONAL")), uid, None)
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_ip_sets(data):
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    sets = [
        {"ARN": s["ARN"], "Id": s["Id"], "Name": s["Name"],
         "Description": s.get("Description", ""), "LockToken": s["LockToken"]}
        for s in _values_for_scope(_ip_sets, scope) if s.get("Scope", "REGIONAL") == scope
    ]
    return _marker_page(data, "IPSets", sets)


# ---------------------------------------------------------------------------
# RegexPatternSet
# ---------------------------------------------------------------------------

def _regex_pattern_set_body(rps):
    return {
        "ARN": rps["ARN"], "Id": rps["Id"], "Name": rps["Name"],
        "Description": rps.get("Description", ""),
        "RegularExpressionList": rps.get("RegularExpressionList", []),
    }


def _create_regex_pattern_set(data):
    name = data.get("Name", "")
    if not name:
        return _waf_err("WAFInvalidParameterException", "Name is required")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    for existing in _values_for_scope(_regex_pattern_sets, scope):
        if existing["Name"] == name and existing.get("Scope") == scope:
            return _waf_err("WAFDuplicateItemException", _DUPLICATE_ITEM_MESSAGE)
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _regex_pattern_set_arn(name, uid, scope)
    _regex_pattern_sets.set_scoped(get_account_id(), _scope_home_region(scope), uid, {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "RegularExpressionList": data.get("RegularExpressionList", []),
        "LockToken": lock_token,
        "Scope": scope,
    })
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name,
                                      "Description": data.get("Description", ""),
                                      "LockToken": lock_token}})


def _get_regex_pattern_set(data):
    uid = data.get("Id", "")
    rps = _resource_from_scope(_regex_pattern_sets, uid, data.get("Scope", "REGIONAL"))
    if not rps:
        return _waf_err("WAFNonexistentItemException", f"RegexPatternSet {uid} not found")
    return json_response({"RegexPatternSet": _regex_pattern_set_body(rps), "LockToken": rps["LockToken"]})


def _update_regex_pattern_set(data):
    uid = data.get("Id", "")
    rps = _resource_from_scope(_regex_pattern_sets, uid, data.get("Scope", "REGIONAL"))
    if not rps:
        return _waf_err("WAFNonexistentItemException", f"RegexPatternSet {uid} not found")
    if data.get("LockToken", "") != rps["LockToken"]:
        return _waf_err("WAFOptimisticLockException",
                        "The resource you are trying to update has been modified by another request.")
    rps["Description"] = data.get("Description", rps.get("Description", ""))
    rps["RegularExpressionList"] = data.get("RegularExpressionList", rps["RegularExpressionList"])
    rps["LockToken"] = new_uuid()
    return json_response({"NextLockToken": rps["LockToken"]})


def _delete_regex_pattern_set(data):
    uid = data.get("Id", "")
    scope = data.get("Scope", "REGIONAL")
    rps = _resource_from_scope(_regex_pattern_sets, uid, scope)
    if not rps:
        return _waf_err("WAFNonexistentItemException", f"RegexPatternSet {uid} not found")
    if data.get("LockToken", "") != rps["LockToken"]:
        return _waf_err("WAFOptimisticLockException",
                        "The resource you are trying to delete has been modified by another request.")
    arn = rps["ARN"]
    _regex_pattern_sets.pop_scoped(get_account_id(), _scope_home_region(scope), uid, None)
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_regex_pattern_sets(data):
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    sets = [
        {"ARN": r["ARN"], "Id": r["Id"], "Name": r["Name"],
         "Description": r.get("Description", ""), "LockToken": r["LockToken"]}
        for r in _values_for_scope(_regex_pattern_sets, scope) if r.get("Scope", "REGIONAL") == scope
    ]
    return _marker_page(data, "RegexPatternSets", sets)


# ---------------------------------------------------------------------------
# RuleGroup
# ---------------------------------------------------------------------------

def _create_rule_group(data):
    name = data.get("Name", "")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    for existing in _values_for_scope(_rule_groups, scope):
        if existing["Name"] == name and existing.get("Scope", scope) == scope:
            return _waf_err("WAFDuplicateItemException", _DUPLICATE_ITEM_MESSAGE)
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _rg_arn(name, uid, scope)
    record = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "Capacity": data.get("Capacity", 0),
        "Rules": data.get("Rules", []),
        "VisibilityConfig": data.get("VisibilityConfig", {}),
        "LockToken": lock_token,
        "Scope": scope,
    }
    _apply_optional(record, data, _RULE_GROUP_OPTIONAL)
    _rule_groups.set_scoped(get_account_id(), _scope_home_region(scope), uid, record)
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name, "LockToken": lock_token}})


def _get_rule_group(data):
    uid = data.get("Id", "")
    rg = _resource_from_scope(_rule_groups, uid, data.get("Scope", "REGIONAL"))
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    rg_body = _shape_view(rg, "RuleGroup")
    return json_response({"RuleGroup": rg_body, "LockToken": rg["LockToken"]})


def _update_rule_group(data):
    uid = data.get("Id", "")
    rg = _resource_from_scope(_rule_groups, uid, data.get("Scope", "REGIONAL"))
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    if data.get("LockToken", "") != rg["LockToken"]:
        return _waf_err("WAFOptimisticLockException", "The resource you are trying to update has been modified by another request.")
    rg["Description"] = data.get("Description", "")
    rg["Rules"] = data.get("Rules", [])
    rg["VisibilityConfig"] = data.get("VisibilityConfig", {})
    _apply_optional(rg, data, _RULE_GROUP_OPTIONAL)
    rg["LockToken"] = new_uuid()
    return json_response({"NextLockToken": rg["LockToken"]})


def _delete_rule_group(data):
    uid = data.get("Id", "")
    rg = _resource_from_scope(_rule_groups, uid, data.get("Scope", "REGIONAL"))
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    arn = rg["ARN"]
    _rule_groups.pop_scoped(get_account_id(), _scope_home_region(data.get("Scope", "REGIONAL")), uid, None)
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_rule_groups(data):
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    groups = [
        {"ARN": r["ARN"], "Id": r["Id"], "Name": r["Name"],
         "Description": r.get("Description", ""), "LockToken": r["LockToken"]}
        for r in _values_for_scope(_rule_groups, scope) if r.get("Scope", "REGIONAL") == scope
    ]
    return _marker_page(data, "RuleGroups", groups)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_local_waf_resource_arn(arn)
    if err:
        return err
    existing = {t["Key"]: t for t in _waf_tags.get(arn, [])}
    for tag in data.get("Tags", []):
        existing[tag["Key"]] = tag
    _waf_tags[arn] = list(existing.values())
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_local_waf_resource_arn(arn)
    if err:
        return err
    remove_keys = set(data.get("TagKeys", []))
    _waf_tags[arn] = [t for t in _waf_tags.get(arn, []) if t["Key"] not in remove_keys]
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    arn, err = _resolve_local_waf_resource_arn(arn)
    if err:
        return err
    return json_response({"TagInfoForResource": {"ResourceARN": arn, "TagList": _waf_tags.get(arn, [])}})


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

def _check_capacity(data):
    return json_response({"Capacity": 1})


def _describe_managed_rule_group(data):
    return json_response({
        "VersionName": "Version_1.0",
        "SnsTopicArn": "",
        "Capacity": 700,
        "Rules": [],
        "LabelNamespace": f"awswaf:managed:{data.get('VendorName', 'AWS')}:{data.get('Name', '')}:",
        "AvailableLabels": [],
        "ConsumedLabels": [],
    })


# ---------------------------------------------------------------------------
# Web ACL evaluation (CloudFront viewer requests)
# ---------------------------------------------------------------------------
# Rules run in ascending Priority: Allow, Block, Captcha and Challenge stop, Count adds the rule's labels
# and goes on, and no match takes the DefaultAction. A rule that MiniStack cannot decide never matches,
# and the first time it is skipped in each web ACL version is logged.


class _Unknown(Exception):
    pass


class _NotApplied(Exception):
    """The forwarded IP header is absent, so AWS WAF does not apply the rule at all."""


def _one(union):
    return next(iter((union or {}).items()), (None, {}))


def _size_gt(field_to_match, size):
    return {"SizeConstraintStatement": {"FieldToMatch": field_to_match, "ComparisonOperator": "GT", "Size": size,
                                        "TextTransformations": [{"Priority": 0, "Type": "NONE"}]}}


# The AWS managed rules whose criterion the Developer Guide states exactly (aws-managed-rule-groups-baseline),
# as (name, label, statement); the other rules of these groups and all other managed groups never match.
_MANAGED_RULES = {
    "AWSManagedRulesCommonRuleSet": ("awswaf:managed:aws:core-rule-set:", [
        ("NoUserAgent_HEADER", "NoUserAgent_Header",
         {"NotStatement": {"Statement": _size_gt({"SingleHeader": {"Name": "user-agent"}}, -1)}}),
        ("SizeRestrictions_QUERYSTRING", "SizeRestrictions_QueryString", _size_gt({"QueryString": {}}, 2048)),
        ("SizeRestrictions_Cookie_HEADER", "SizeRestrictions_Cookie_Header",
         _size_gt({"SingleHeader": {"Name": "cookie"}}, 10240)),
        ("SizeRestrictions_BODY", "SizeRestrictions_Body", _size_gt({"Body": {}}, 8192)),
        ("SizeRestrictions_URIPATH", "SizeRestrictions_URIPath", _size_gt({"UriPath": {}}, 1024)),
    ]),
    "AWSManagedRulesKnownBadInputsRuleSet": ("awswaf:managed:aws:known-bad-inputs:", [
        ("PROPFIND_METHOD", "Propfind_Method", {"ByteMatchStatement": {
            "FieldToMatch": {"Method": {}}, "PositionalConstraint": "EXACTLY",
            "SearchString": base64.b64encode(b"PROPFIND").decode(),
            "TextTransformations": [{"Priority": 0, "Type": "NONE"}]}}),
    ]),
}

# MiniStack issues no CAPTCHA or challenge tokens, so every request gets the response for a missing token.
_TOKEN_STATUS = {"Captcha": 405, "Challenge": 202}
_CONTENT_TYPES = {"TEXT_PLAIN": "text/plain", "TEXT_HTML": "text/html", "APPLICATION_JSON": "application/json"}
_rate_windows = {}  # (web ACL ARN, rule path, aggregate key) -> request times
_reported = {}  # (web ACL ARN, LockToken) -> rule paths already logged as skipped


class _Request:
    def __init__(self, acl, method, path, query, headers, body, client_ip):
        self.acl, self.method, self.path, self.query, self.body, self.ip = acl, method, path, query, body, client_ip
        self.headers = [(k.lower(), v) for k, v in headers.items()]
        self.labels, self.inserted, self.skipped = [], {}, {}
        limit = (((acl.get("AssociationConfig") or {}).get("RequestBody") or {}).get("CLOUDFRONT") or {}).get(
            "DefaultSizeInspectionLimit", "KB_16")
        self.body_limit = int(limit[3:]) * 1024


def resource_by_arn(arn, resource_type):
    """The stored resource of ``resource_type`` that ``arn`` names, in the ARN's own account and Region, or None."""
    try:
        spec = parse_arn(arn or "")
    except ArnParseError:
        return None
    parts = spec.resource.split("/")
    if spec.service != "wafv2" or len(parts) != 4 or parts[1] != resource_type:
        return None
    resource = _resource_store(resource_type).get_scoped(spec.account_id, spec.region, parts[3])
    return resource if resource and resource["ARN"] == arn else None


def _ref(spec, resource_type):
    resource = resource_by_arn(spec.get("ARN"), resource_type)
    if resource is None:
        raise _Unknown(f"{resource_type} {spec.get('ARN')} not found")
    return resource


def evaluate_web_acl(acl, method, path, query, headers, body, client_ip):
    """``("Allow", headers to insert)``, or ``("Block", None)`` for the host's own block page, or the action
    with a ``(status, headers, body)`` response."""
    req = _Request(acl, method, path, query, headers, body or b"", client_ip)
    bodies = acl.get("CustomResponseBodies") or {}
    outcome = _run_rules(req, acl.get("Rules") or [], f"awswaf:{acl['ARN'].split(':')[4]}:webacl:{acl['Name']}:",
                         bodies, {}, acl["Name"]) or (*_one(acl.get("DefaultAction")), bodies)
    seen = _reported.setdefault((acl["ARN"], acl.get("LockToken")), set())
    if new := [f"{p} ({r})" for p, r in req.skipped.items() if p not in seen]:
        seen.update(req.skipped)
        logger.warning("WAFv2 web ACL %s: rules not evaluated, they never match: %s", acl["Name"], "; ".join(new))
    name, action, bodies = outcome
    if name == "Allow":
        _insert_headers(req, action)
        return name, req.inserted
    if name in _TOKEN_STATUS:
        return name, (_TOKEN_STATUS[name], {"x-amzn-waf-action": name.lower()}, b"")
    custom = action.get("CustomResponse")
    if not custom:
        return name, None
    response_headers = {h["Name"]: h["Value"] for h in custom.get("ResponseHeaders") or []}
    content = bodies.get(custom.get("CustomResponseBodyKey"))
    if content:
        response_headers["content-type"] = _CONTENT_TYPES[content["ContentType"]]
    return name, (int(custom["ResponseCode"]), response_headers, content["Content"].encode() if content else b"")


def _insert_headers(req, action):
    for header in ((action or {}).get("CustomRequestHandling") or {}).get("InsertHeaders") or []:
        req.inserted[f"x-amzn-waf-{header['Name']}".lower()] = header["Value"]


def _run_rules(req, rules, prefix, bodies, overrides, path):
    """The terminating ``(action name, action, custom response bodies)``, or None."""
    for rule in sorted(rules, key=lambda r: r.get("Priority", 0)):
        rule_path = f"{path}/{rule.get('Name')}"
        try:
            outcome = _run_rule(req, rule, prefix, bodies, overrides, rule_path)
        except _NotApplied:
            continue
        except Exception as exc:
            req.skipped[rule_path] = str(exc) if isinstance(exc, _Unknown) else repr(exc)
            continue
        if outcome:
            return outcome
    return None


def _run_rule(req, rule, prefix, bodies, overrides, path):
    kind, spec = _one(rule.get("Statement"))
    if spec.get("ScopeDownStatement") and not _eval(req, spec["ScopeDownStatement"], prefix):
        return None
    if kind in ("ManagedRuleGroupStatement", "RuleGroupReferenceStatement"):
        outcome = _run_group(req, kind, spec, bodies, path)
        return None if _one(rule.get("OverrideAction"))[0] == "Count" else outcome
    if not (_rate(req, spec, prefix, path) if kind == "RateBasedStatement" else _eval(req, rule["Statement"], prefix)):
        return None
    name, action, bodies = overrides.get(rule.get("Name")) or (*_one(rule.get("Action")), bodies)
    req.labels += [prefix + label["Name"] for label in rule.get("RuleLabels") or []]
    if name == "Count":
        _insert_headers(req, action)
        return None
    return name, action, bodies


def _run_group(req, kind, spec, bodies, path):
    overrides = {r["Name"]: ("Count", {}, bodies) for r in spec.get("ExcludedRules") or []}
    overrides.update({o["Name"]: (*_one(o["ActionToUse"]), bodies) for o in spec.get("RuleActionOverrides") or []})
    if kind == "RuleGroupReferenceStatement":
        group = _ref(spec, "rulegroup")
        return _run_rules(req, group.get("Rules") or [], f"awswaf:{group['ARN'].split(':')[4]}:rulegroup:{group['Name']}:",
                          group.get("CustomResponseBodies") or {}, overrides, path)
    managed = _MANAGED_RULES.get(spec.get("Name")) if spec.get("VendorName") == "AWS" else None
    if managed is None:
        raise _Unknown("managed rule group not modelled")
    namespace, rules = managed
    req.skipped[path] = f"only {', '.join(r[0] for r in rules)} of this managed rule group are evaluated"
    return _run_rules(req, [{"Name": name, "Priority": i, "Statement": statement, "Action": {"Block": {}},
                             "RuleLabels": [{"Name": label}]} for i, (name, label, statement) in enumerate(rules)],
                      namespace, {}, overrides, path)


_POSITIONS = {
    "EXACTLY": operator.eq, "STARTS_WITH": bytes.startswith, "ENDS_WITH": bytes.endswith,
    "CONTAINS": lambda value, search: search in value,
    "CONTAINS_WORD": lambda value, search: re.search(
        rb"(?<![A-Za-z0-9_])" + re.escape(search) + rb"(?![A-Za-z0-9_])", value) is not None,
}
_COMPARISONS = {"EQ": operator.eq, "NE": operator.ne, "LE": operator.le, "LT": operator.lt,
                "GE": operator.ge, "GT": operator.gt}


def _eval(req, statement, prefix):
    kind, spec = _one(statement)
    if kind in ("AndStatement", "OrStatement"):
        results = [_eval(req, member, prefix) for member in spec["Statements"]]
        return all(results) if kind == "AndStatement" else any(results)
    if kind == "NotStatement":
        return not _eval(req, spec["Statement"], prefix)
    if kind == "ByteMatchStatement":
        search, test = base64.b64decode(spec["SearchString"]), _POSITIONS[spec["PositionalConstraint"]]
        return _field_match(req, spec, lambda value: test(value, search))
    if kind in ("RegexMatchStatement", "RegexPatternSetReferenceStatement"):
        # Python's re stands in for the regex library of AWS WAF.
        patterns = [re.compile(p["RegexString"]) for p in (
            [spec] if kind == "RegexMatchStatement" else _ref(spec, "regexpatternset")["RegularExpressionList"])]
        return _field_match(req, spec, lambda value: any(p.search(value.decode("latin-1")) for p in patterns))
    if kind == "SizeConstraintStatement":
        compare = _COMPARISONS[spec["ComparisonOperator"]]
        return _field_match(req, spec, lambda value: compare(len(value), spec["Size"]))
    if kind == "IPSetReferenceStatement":
        networks = [ipaddress.ip_network(a, strict=False) for a in _ref(spec, "ipset")["Addresses"]]
        config = spec.get("IPSetForwardedIPConfig")
        ips = _forwarded_ips(req, config) if config else [ipaddress.ip_address(req.ip)]
        if isinstance(ips, bool):
            return ips
        return any(ip in network for ip in ips for network in networks)
    if kind == "LabelMatchStatement":
        return any(_label_matches(label, spec["Key"], spec["Scope"], prefix) for label in req.labels)
    raise _Unknown(kind)


def _label_matches(label, key, scope, prefix):
    """Developer Guide, waf-rule-label-match: a key without the ``awswaf:`` prefix is read in the
    context of the rule; LABEL matches the end of the label, NAMESPACE contiguous namespaces."""
    qualified = key.startswith("awswaf:")
    if not qualified:
        if not label.startswith(prefix):
            return False
        label = label[len(prefix):]
    if scope == "LABEL":
        return label == key or not qualified and label.endswith(":" + key)
    namespace = key.rstrip(":") + ":"
    return label.startswith(namespace) if qualified else f":{namespace}" in f":{label.rpartition(':')[0]}:"


def _forwarded_ips(req, config):
    """The valid addresses at the configured position of the forwarded IP header, or the fallback outcome."""
    values = [v for n, v in req.headers if n == config["HeaderName"].lower()]
    if not values:
        raise _NotApplied()
    parts = [p.strip() for p in ",".join(values).split(",")]
    ips = []
    for part in {"FIRST": parts[:1], "LAST": parts[-1:]}.get(config.get("Position", "FIRST"), parts[-10:]):
        try:
            ips.append(ipaddress.ip_address(part))
        except ValueError:
            pass
    return ips or config["FallbackBehavior"] == "MATCH"


def _rate(req, spec, prefix, path):
    key_type = spec.get("AggregateKeyType", "IP")
    if key_type == "FORWARDED_IP":
        ips = _forwarded_ips(req, spec["ForwardedIPConfig"])
        if isinstance(ips, bool):
            return ips
        key = str(ips[0])
    elif key_type in ("IP", "CONSTANT"):
        key = req.ip if key_type == "IP" else ""
    else:
        raise _Unknown(f"aggregate key type {key_type}")
    now = time.monotonic()
    times = _rate_windows.setdefault((req.acl["ARN"], path, key), collections.deque())
    times.append(now)
    while times[0] <= now - spec.get("EvaluationWindowSec", 300):
        times.popleft()
    return len(times) > spec["Limit"]


def _field_match(req, spec, test):
    values = _field_values(req, spec["FieldToMatch"])
    if isinstance(values, bool):
        return values
    return any(test(_transform(value, spec.get("TextTransformations") or [])) for value in values)


def _oversize(spec, over):
    """None to inspect the component, else the outcome its OversizeHandling forces."""
    handling = spec.get("OversizeHandling", "CONTINUE")
    return None if not over or handling == "CONTINUE" else handling == "MATCH"


def _latin1(text):
    return text.encode("latin-1", "replace")


def _field_values(req, field_to_match):
    """The values to inspect, any one of which can match, or the outcome that the component decides.
    A component the request lacks has no values, so it does not match."""
    kind, spec = _one(field_to_match)
    if kind == "UriPath":
        return [_latin1(req.path)]
    if kind == "QueryString":
        return [_latin1(req.query)] if req.query else []
    if kind == "Method":
        return [_latin1(req.method)]
    if kind == "SingleHeader":
        return [_latin1(v) for n, v in req.headers if n == spec["Name"].lower()]
    if kind in ("SingleQueryArgument", "AllQueryArguments"):
        args = [part.partition("=") for part in req.query.split("&") if part]
        return [_latin1(v) for n, _, v in args if kind == "AllQueryArguments" or n.lower() == spec["Name"].lower()]
    if kind == "Body":
        if (forced := _oversize(spec, len(req.body) > req.body_limit)) is not None:
            return forced
        return [req.body[:req.body_limit]] if req.body else []
    if kind == "UriFragment":
        # A request target never carries a fragment (RFC 9112), so the fallback applies.
        return spec.get("FallbackBehavior") == "MATCH"
    raise _Unknown(f"FieldToMatch {kind}")


def _normalize_path(value):
    out = []
    for segment in re.sub(rb"/{2,}", b"/", value).split(b"/"):
        if segment == b"." and out:
            continue
        if segment == b".." and out and out[-1] not in (b"", b".."):
            out.pop()
            continue
        out.append(segment)
    if value.endswith((b"/.", b"/..")) and out and out[-1] != b"..":
        out.append(b"")
    return b"/".join(out)


_SPACE = b" \t\n\r\x0b\x0c"
# Developer Guide, waf-rule-statement-transformation; any other type makes its rule not evaluated.
_TRANSFORMS = {
    "NONE": lambda v: v,
    "LOWERCASE": bytes.lower,
    "UPPERCASE": bytes.upper,
    "COMPRESS_WHITE_SPACE": lambda v: re.sub(rb"[ \t\n\r\x0b\x0c\xa0]+", b" ", v),
    "NORMALIZE_PATH": _normalize_path,
    "NORMALIZE_PATH_WIN": lambda v: _normalize_path(v.replace(b"\\", b"/")),
    "REMOVE_NULLS": lambda v: v.replace(b"\0", b""),
    "REPLACE_NULLS": lambda v: v.replace(b"\0", b" "),
    "REMOVE_WHITESPACE": lambda v: v.translate(None, _SPACE),
    "TRIM": lambda v: v.strip(_SPACE),
    "TRIM_LEFT": lambda v: v.lstrip(_SPACE),
    "TRIM_RIGHT": lambda v: v.rstrip(_SPACE),
    "REPLACE_COMMENTS": lambda v: re.sub(rb"/\*.*?(?:\*/|\Z)", b" ", v, flags=re.S),
    "REMOVE_COMMENTS_CHAR": lambda v: re.sub(rb"/\*|\*/|--|#", b"", v),
    "URL_DECODE": lambda v: re.sub(rb"%([0-9A-Fa-f]{2})", lambda m: bytes.fromhex(m[1].decode()), v),
    "BASE64_DECODE": lambda v: base64.b64decode(v, validate=True),
    "HEX_DECODE": lambda v: bytes.fromhex(v.decode()),
    "SQL_HEX_DECODE": lambda v: re.sub(rb"0x((?:[0-9A-Fa-f]{2})+)", lambda m: bytes.fromhex(m[1].decode()), v),
    "MD5": lambda v: hashlib.md5(v).digest(),
    "SHA256": lambda v: hashlib.sha256(v).digest(),
}


def _transform(value, transformations):
    for transformation in sorted(transformations, key=lambda t: t.get("Priority", 0)):
        if (fn := _TRANSFORMS.get(transformation["Type"])) is None:
            raise _Unknown(f"text transformation {transformation['Type']}")
        value = fn(value)
    return value


def api_rules_from_cfn(rules):
    """CloudFormation rules in the API shape: a ByteMatchStatement's plain SearchString, or its
    SearchStringBase64, becomes the API's base64 blob (aws-properties-wafv2-webacl-bytematchstatement)."""
    if isinstance(rules, list):
        return [api_rules_from_cfn(r) for r in rules]
    if not isinstance(rules, dict):
        return rules
    out = {k: api_rules_from_cfn(v) for k, v in rules.items()}
    if isinstance(match := out.get("ByteMatchStatement"), dict) and "SearchString" in match:
        match["SearchString"] = base64.b64encode(str(match["SearchString"]).encode()).decode()
    if isinstance(match, dict) and "SearchStringBase64" in match:
        match["SearchString"] = match.pop("SearchStringBase64")
    return out


def reset():
    _web_acls.clear()
    _ip_sets.clear()
    _regex_pattern_sets.clear()
    _rule_groups.clear()
    _associations.clear()
    _waf_tags.clear()
    _rate_windows.clear()
    _reported.clear()
