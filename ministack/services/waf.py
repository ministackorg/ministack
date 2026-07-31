"""
WAF v2 Service Emulator.
JSON-based API via X-Amz-Target: AWSWAF_20190729.
Supports: CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs,
          AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL,
          CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets,
          CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups,
          TagResource, UntagResource, ListTagsForResource,
          CheckCapacity, DescribeManagedRuleGroup.
"""

import copy
import json
import logging
import os

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("wafv2")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CLOUDFRONT_HOME_REGION = "us-east-1"

_web_acls = AccountRegionScopedDict()       # id -> webacl
_ip_sets = AccountRegionScopedDict()        # id -> ipset
_rule_groups = AccountRegionScopedDict()    # id -> rulegroup
# AssociateWebACL is for regional application resources; CloudFront uses the
# distribution configuration path. Keep this request-region scoped even though
# the mock accepts CLOUDFRONT-scope WebACL ARNs for compatibility.
_associations = AccountRegionScopedDict()   # resource_arn -> webacl_arn
_waf_tags = AccountScopedDict()       # resource_arn -> [tags]

_WAF_RESOURCE_SCOPES = {
    "webacl": {"regional", "cloudfront", "global"},
    "ipset": {"regional", "global"},
    "rulegroup": {"regional", "global"},
}


def get_state():
    return {
        "_web_acls": copy.deepcopy(_web_acls),
        "_ip_sets": copy.deepcopy(_ip_sets),
        "_rule_groups": copy.deepcopy(_rule_groups),
        "_associations": copy.deepcopy(_associations),
        "_waf_tags": copy.deepcopy(_waf_tags),
    }


def restore_state(data):
    if not isinstance(data, dict):
        return
    arn_rewrites = {}
    _restore_resource_store(_web_acls, data.get("_web_acls", {}), "webacl", arn_rewrites)
    _restore_resource_store(_ip_sets, data.get("_ip_sets", {}), "ipset", arn_rewrites)
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
    _web_acls.set_scoped(get_account_id(), _scope_home_region(scope), uid, record)
    _waf_tags[arn] = props.get("Tags", [])
    return uid, arn, record


def delete_web_acl_record(uid, scope):
    acl = _web_acls.pop_scoped(get_account_id(), _scope_home_region(scope), uid, None)
    if acl:
        _waf_tags.pop(acl["ARN"], None)


def _resource_store(resource_type):
    return {
        "webacl": _web_acls,
        "ipset": _ip_sets,
        "rulegroup": _rule_groups,
    }[resource_type]


def _resource_from_scope(store, uid, scope):
    return store.get_scoped(get_account_id(), _scope_home_region(scope), uid)


def _values_for_scope(store, scope):
    return store.values_scoped(get_account_id(), _scope_home_region(scope))


try:
    _restored = load_state("waf")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _waf_err(code, message):
    return error_response_json(code, message, 400)


def _waf_invalid_arn(arn):
    return _waf_err("WAFInvalidParameterException", f"Invalid WAFv2 resource ARN: {arn}")


def _waf_not_found(arn):
    return _waf_err("WAFNonexistentItemException", f"WAFv2 resource {arn} not found")


def _acl_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("webacl", name, uid, scope)


def _ipset_arn(name, uid, scope="REGIONAL"):
    return _resource_arn("ipset", name, uid, scope)


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


def _resolve_local_waf_resource_arn(arn, allowed_types=("webacl", "ipset", "rulegroup")):
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

def _create_web_acl(data):
    name = data.get("Name", "")
    if not name:
        return _waf_err("WAFInvalidParameterException", "Name is required")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    for existing in _values_for_scope(_web_acls, scope):
        if existing["Name"] == name and existing.get("Scope") == scope:
            return _waf_err("WAFDuplicateItemException", f"A WebACL with name '{name}' already exists.")
    uid, arn, acl = create_web_acl_record(name, scope, data)
    logger.info("CreateWebACL: %s (%s)", name, uid)
    return json_response({"Summary": {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "LockToken": acl["LockToken"],
    }})


def _get_web_acl(data):
    uid = data.get("Id", "")
    acl = _resource_from_scope(_web_acls, uid, data.get("Scope", "REGIONAL"))
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    acl_body = {k: v for k, v in acl.items() if k != "LockToken"}
    return json_response({"WebACL": acl_body, "LockToken": acl["LockToken"]})


def _update_web_acl(data):
    uid = data.get("Id", "")
    acl = _resource_from_scope(_web_acls, uid, data.get("Scope", "REGIONAL"))
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    lock_token = data.get("LockToken", "")
    if lock_token != acl.get("LockToken", ""):
        return _waf_err("WAFOptimisticLockException", "The resource you are trying to update has been modified by another request.")
    acl["Rules"] = data.get("Rules", acl["Rules"])
    acl["DefaultAction"] = data.get("DefaultAction", acl["DefaultAction"])
    acl["VisibilityConfig"] = data.get("VisibilityConfig", acl["VisibilityConfig"])
    acl["LockToken"] = new_uuid()
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


def _list_web_acls(data):
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    acls = [
        {"ARN": a["ARN"], "Id": a["Id"], "Name": a["Name"],
         "Description": a.get("Description", ""), "LockToken": a["LockToken"]}
        for a in _values_for_scope(_web_acls, scope) if a.get("Scope", "REGIONAL") == scope
    ]
    return json_response({"WebACLs": acls, "NextMarker": None})


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
    acl_body = {k: v for k, v in acl.items() if k != "LockToken"}
    return json_response({"WebACL": acl_body})


def _list_resources_for_web_acl(data):
    web_acl_arn = data.get("WebACLArn", "")
    _, err = _resolve_local_web_acl(web_acl_arn)
    if err:
        return err
    arns = [r for r, a in _associations.items() if a == web_acl_arn]
    return json_response({"ResourceArns": arns})


# ---------------------------------------------------------------------------
# IPSet
# ---------------------------------------------------------------------------

def _create_ip_set(data):
    name = data.get("Name", "")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
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
    ipset_body = {k: v for k, v in ipset.items() if k != "LockToken"}
    return json_response({"IPSet": ipset_body, "LockToken": ipset["LockToken"]})


def _update_ip_set(data):
    uid = data.get("Id", "")
    ipset = _resource_from_scope(_ip_sets, uid, data.get("Scope", "REGIONAL"))
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
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
    return json_response({"IPSets": sets, "NextMarker": None})


# ---------------------------------------------------------------------------
# RuleGroup
# ---------------------------------------------------------------------------

def _create_rule_group(data):
    name = data.get("Name", "")
    scope = _normalize_scope(data.get("Scope", "REGIONAL"))
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _rg_arn(name, uid, scope)
    _rule_groups.set_scoped(get_account_id(), _scope_home_region(scope), uid, {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "Capacity": data.get("Capacity", 0),
        "Rules": data.get("Rules", []),
        "VisibilityConfig": data.get("VisibilityConfig", {}),
        "LockToken": lock_token,
        "Scope": scope,
    })
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name, "LockToken": lock_token}})


def _get_rule_group(data):
    uid = data.get("Id", "")
    rg = _resource_from_scope(_rule_groups, uid, data.get("Scope", "REGIONAL"))
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    rg_body = {k: v for k, v in rg.items() if k != "LockToken"}
    return json_response({"RuleGroup": rg_body, "LockToken": rg["LockToken"]})


def _update_rule_group(data):
    uid = data.get("Id", "")
    rg = _resource_from_scope(_rule_groups, uid, data.get("Scope", "REGIONAL"))
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    rg["Rules"] = data.get("Rules", rg["Rules"])
    rg["VisibilityConfig"] = data.get("VisibilityConfig", rg["VisibilityConfig"])
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
    return json_response({"RuleGroups": groups, "NextMarker": None})


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


def reset():
    _web_acls.clear()
    _ip_sets.clear()
    _rule_groups.clear()
    _associations.clear()
    _waf_tags.clear()
