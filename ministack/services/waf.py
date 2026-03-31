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

import json
import logging

from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("wafv2")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_web_acls: dict = {}       # id -> webacl
_ip_sets: dict = {}        # id -> ipset
_rule_groups: dict = {}    # id -> rulegroup
_associations: dict = {}   # resource_arn -> webacl_arn
_waf_tags: dict = {}       # resource_arn -> [tags]


def _waf_err(code, message):
    return error_response_json(code, message, 400)


def _acl_arn(name, uid):
    return f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:regional/webacl/{name}/{uid}"


def _ipset_arn(name, uid):
    return f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:regional/ipset/{name}/{uid}"


def _rg_arn(name, uid):
    return f"arn:aws:wafv2:{REGION}:{ACCOUNT_ID}:regional/rulegroup/{name}/{uid}"


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
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _acl_arn(name, uid)
    _web_acls[uid] = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "DefaultAction": data.get("DefaultAction", {"Allow": {}}),
        "Rules": data.get("Rules", []),
        "VisibilityConfig": data.get("VisibilityConfig", {}),
        "Capacity": 0,
        "LockToken": lock_token,
        "Scope": data.get("Scope", "REGIONAL"),
    }
    _waf_tags[arn] = data.get("Tags", [])
    logger.info("CreateWebACL: %s (%s)", name, uid)
    return json_response({"Summary": {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "LockToken": lock_token,
    }})


def _get_web_acl(data):
    uid = data.get("Id", "")
    acl = _web_acls.get(uid)
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    acl_body = {k: v for k, v in acl.items() if k != "LockToken"}
    return json_response({"WebACL": acl_body, "LockToken": acl["LockToken"]})


def _update_web_acl(data):
    uid = data.get("Id", "")
    acl = _web_acls.get(uid)
    if not acl:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    acl["Rules"] = data.get("Rules", acl["Rules"])
    acl["DefaultAction"] = data.get("DefaultAction", acl["DefaultAction"])
    acl["VisibilityConfig"] = data.get("VisibilityConfig", acl["VisibilityConfig"])
    acl["LockToken"] = new_uuid()
    return json_response({"NextLockToken": acl["LockToken"]})


def _delete_web_acl(data):
    uid = data.get("Id", "")
    if uid not in _web_acls:
        return _waf_err("WAFNonexistentItemException", f"WebACL {uid} not found")
    arn = _web_acls[uid]["ARN"]
    del _web_acls[uid]
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_web_acls(data):
    scope = data.get("Scope", "REGIONAL")
    acls = [
        {"ARN": a["ARN"], "Id": a["Id"], "Name": a["Name"],
         "Description": a.get("Description", ""), "LockToken": a["LockToken"]}
        for a in _web_acls.values() if a.get("Scope", "REGIONAL") == scope
    ]
    return json_response({"WebACLs": acls, "NextMarker": None})


# ---------------------------------------------------------------------------
# Association
# ---------------------------------------------------------------------------

def _associate_web_acl(data):
    web_acl_arn = data.get("WebACLArn", "")
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
    for acl in _web_acls.values():
        if acl["ARN"] == web_acl_arn:
            acl_body = {k: v for k, v in acl.items() if k != "LockToken"}
            return json_response({"WebACL": acl_body})
    return _waf_err("WAFNonexistentItemException", f"WebACL {web_acl_arn} not found")


def _list_resources_for_web_acl(data):
    web_acl_arn = data.get("WebACLArn", "")
    arns = [r for r, a in _associations.items() if a == web_acl_arn]
    return json_response({"ResourceArns": arns})


# ---------------------------------------------------------------------------
# IPSet
# ---------------------------------------------------------------------------

def _create_ip_set(data):
    name = data.get("Name", "")
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _ipset_arn(name, uid)
    _ip_sets[uid] = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "IPAddressVersion": data.get("IPAddressVersion", "IPV4"),
        "Addresses": data.get("Addresses", []),
        "LockToken": lock_token,
        "Scope": data.get("Scope", "REGIONAL"),
    }
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name, "LockToken": lock_token}})


def _get_ip_set(data):
    uid = data.get("Id", "")
    ipset = _ip_sets.get(uid)
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    ipset_body = {k: v for k, v in ipset.items() if k != "LockToken"}
    return json_response({"IPSet": ipset_body, "LockToken": ipset["LockToken"]})


def _update_ip_set(data):
    uid = data.get("Id", "")
    ipset = _ip_sets.get(uid)
    if not ipset:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    ipset["Addresses"] = data.get("Addresses", ipset["Addresses"])
    ipset["LockToken"] = new_uuid()
    return json_response({"NextLockToken": ipset["LockToken"]})


def _delete_ip_set(data):
    uid = data.get("Id", "")
    if uid not in _ip_sets:
        return _waf_err("WAFNonexistentItemException", f"IPSet {uid} not found")
    arn = _ip_sets[uid]["ARN"]
    del _ip_sets[uid]
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_ip_sets(data):
    scope = data.get("Scope", "REGIONAL")
    sets = [
        {"ARN": s["ARN"], "Id": s["Id"], "Name": s["Name"],
         "Description": s.get("Description", ""), "LockToken": s["LockToken"]}
        for s in _ip_sets.values() if s.get("Scope", "REGIONAL") == scope
    ]
    return json_response({"IPSets": sets, "NextMarker": None})


# ---------------------------------------------------------------------------
# RuleGroup
# ---------------------------------------------------------------------------

def _create_rule_group(data):
    name = data.get("Name", "")
    uid = new_uuid()
    lock_token = new_uuid()
    arn = _rg_arn(name, uid)
    _rule_groups[uid] = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": data.get("Description", ""),
        "Capacity": data.get("Capacity", 0),
        "Rules": data.get("Rules", []),
        "VisibilityConfig": data.get("VisibilityConfig", {}),
        "LockToken": lock_token,
        "Scope": data.get("Scope", "REGIONAL"),
    }
    _waf_tags[arn] = data.get("Tags", [])
    return json_response({"Summary": {"ARN": arn, "Id": uid, "Name": name, "LockToken": lock_token}})


def _get_rule_group(data):
    uid = data.get("Id", "")
    rg = _rule_groups.get(uid)
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    rg_body = {k: v for k, v in rg.items() if k != "LockToken"}
    return json_response({"RuleGroup": rg_body, "LockToken": rg["LockToken"]})


def _update_rule_group(data):
    uid = data.get("Id", "")
    rg = _rule_groups.get(uid)
    if not rg:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    rg["Rules"] = data.get("Rules", rg["Rules"])
    rg["VisibilityConfig"] = data.get("VisibilityConfig", rg["VisibilityConfig"])
    rg["LockToken"] = new_uuid()
    return json_response({"NextLockToken": rg["LockToken"]})


def _delete_rule_group(data):
    uid = data.get("Id", "")
    if uid not in _rule_groups:
        return _waf_err("WAFNonexistentItemException", f"RuleGroup {uid} not found")
    arn = _rule_groups[uid]["ARN"]
    del _rule_groups[uid]
    _waf_tags.pop(arn, None)
    return json_response({})


def _list_rule_groups(data):
    scope = data.get("Scope", "REGIONAL")
    groups = [
        {"ARN": r["ARN"], "Id": r["Id"], "Name": r["Name"],
         "Description": r.get("Description", ""), "LockToken": r["LockToken"]}
        for r in _rule_groups.values() if r.get("Scope", "REGIONAL") == scope
    ]
    return json_response({"RuleGroups": groups, "NextMarker": None})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    existing = {t["Key"]: t for t in _waf_tags.get(arn, [])}
    for tag in data.get("Tags", []):
        existing[tag["Key"]] = tag
    _waf_tags[arn] = list(existing.values())
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    remove_keys = set(data.get("TagKeys", []))
    _waf_tags[arn] = [t for t in _waf_tags.get(arn, []) if t["Key"] not in remove_keys]
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
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
