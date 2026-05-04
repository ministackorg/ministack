"""
AWS Resource Groups (resource-groups, apiVersion 2017-11-27) emulator.
REST/JSON. Routes, methods, and shapes match the AWS spec.

Supports:
  Groups:        CreateGroup, GetGroup, DeleteGroup, UpdateGroup, ListGroups
  Group query:   GetGroupQuery, UpdateGroupQuery
  Configuration: GetGroupConfiguration, PutGroupConfiguration
  Membership:    GroupResources, UngroupResources, ListGroupResources,
                 ListGroupingStatuses, SearchResources
  Tags:          Tag, Untag, GetTags
  Account:       GetAccountSettings, UpdateAccountSettings

Tag-sync operations (CancelTagSyncTask / GetTagSyncTask / ListTagSyncTasks /
StartTagSyncTask) are intentionally not exposed here — they aren't reachable
through the AWS CLI or Terraform AWS provider.
"""

import copy
import json
import logging
import re

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
)

logger = logging.getLogger("resource_groups")

_GROUP_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,300}$")
_GROUP_ARN_RE = re.compile(
    r"^arn:aws[-a-z]*:resource-groups:[^:]*:[^:]*:group/(?P<name>[A-Za-z0-9_.\-]+)$"
)
_VALID_QUERY_TYPES = {"TAG_FILTERS_1_0", "CLOUDFORMATION_STACK_1_0"}

_groups = AccountScopedDict()           # name -> Group dict
_group_queries = AccountScopedDict()    # name -> ResourceQuery dict
_group_configs = AccountScopedDict()    # name -> [GroupConfigurationItem]
_group_members = AccountScopedDict()    # name -> [resource_arn]
_group_tags = AccountScopedDict()       # name -> {tag_key: tag_value}
_account_settings = AccountScopedDict()  # "settings" -> AccountSettings dict


def get_state():
    return copy.deepcopy({
        "groups": _groups,
        "group_queries": _group_queries,
        "group_configs": _group_configs,
        "group_members": _group_members,
        "group_tags": _group_tags,
        "account_settings": _account_settings,
    })


def restore_state(data):
    if not data:
        return
    _groups.update(data.get("groups", {}))
    _group_queries.update(data.get("group_queries", {}))
    _group_configs.update(data.get("group_configs", {}))
    _group_members.update(data.get("group_members", {}))
    _group_tags.update(data.get("group_tags", {}))
    _account_settings.update(data.get("account_settings", {}))


try:
    _restored = load_state("resource_groups")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted state; continuing with fresh store")


def reset():
    _groups.clear()
    _group_queries.clear()
    _group_configs.clear()
    _group_members.clear()
    _group_tags.clear()
    _account_settings.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _arn(name):
    return f"arn:aws:resource-groups:{get_region()}:{get_account_id()}:group/{name}"


def _resolve_name(group_name=None, group=None):
    """Accepts either GroupName (bare) or Group (name or full ARN). Returns
    the bare name, or None if both inputs are missing."""
    if group_name:
        return group_name
    if not group:
        return None
    m = _GROUP_ARN_RE.match(group)
    if m:
        return m.group("name")
    return group


def _json(body, status=200):
    return status, {"Content-Type": "application/json"}, json.dumps(body, ensure_ascii=False).encode("utf-8")


def _err(code, message, status):
    body = {"Message": message}
    return status, {
        "Content-Type": "application/json",
        "x-amzn-errortype": code,
    }, json.dumps(body).encode("utf-8")


def _bad_request(msg):
    return _err("BadRequestException", msg, 400)


def _not_found(msg):
    return _err("NotFoundException", msg, 404)


def _validate_resource_query(rq):
    if not isinstance(rq, dict):
        return "ResourceQuery must be an object."
    qtype = rq.get("Type")
    if qtype not in _VALID_QUERY_TYPES:
        return f"ResourceQuery.Type must be one of {sorted(_VALID_QUERY_TYPES)}."
    if not isinstance(rq.get("Query"), str) or not rq["Query"]:
        return "ResourceQuery.Query is required."
    return None


def _group_record(name, description="", criticality=None, owner=None, display_name=None):
    rec = {"GroupArn": _arn(name), "Name": name, "Description": description or ""}
    if criticality is not None:
        rec["Criticality"] = int(criticality)
    if owner is not None:
        rec["Owner"] = owner
    if display_name is not None:
        rec["DisplayName"] = display_name
    return rec


def _group_identifier(rec):
    out = {"GroupName": rec["Name"], "GroupArn": rec["GroupArn"]}
    for k in ("Description", "Criticality", "Owner", "DisplayName"):
        if k in rec:
            out[k] = rec[k]
    return out


def _settings():
    s = _account_settings.get("settings")
    if s is None:
        s = {"GroupLifecycleEventsDesiredStatus": "INACTIVE",
             "GroupLifecycleEventsStatus": "INACTIVE"}
        _account_settings["settings"] = s
    return s


def _paginate(items, max_results, next_token):
    try:
        start = int(next_token) if next_token else 0
    except (TypeError, ValueError):
        start = 0
    if max_results is None:
        max_results = 50
    end = start + int(max_results)
    page = items[start:end]
    nxt = str(end) if end < len(items) else None
    return page, nxt


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------

def _create_group(data):
    name = data.get("Name")
    if not name or not _GROUP_NAME_RE.match(name):
        return _bad_request("Name is required and must match [A-Za-z0-9_.-]{1,300}.")
    rq = data.get("ResourceQuery")
    if rq is not None:
        err = _validate_resource_query(rq)
        if err:
            return _bad_request(err)
    if name in _groups:
        return _bad_request(f"A group named {name!r} already exists.")

    rec = _group_record(
        name,
        description=data.get("Description"),
        criticality=data.get("Criticality"),
        owner=data.get("Owner"),
        display_name=data.get("DisplayName"),
    )
    _groups[name] = rec
    if rq is not None:
        _group_queries[name] = copy.deepcopy(rq)
    cfg = data.get("Configuration")
    if cfg is not None:
        _group_configs[name] = list(cfg)
    tags = dict(data.get("Tags") or {})
    if tags:
        _group_tags[name] = tags

    out = {"Group": rec, "Tags": tags}
    if rq is not None:
        out["ResourceQuery"] = _group_queries[name]
    if cfg is not None:
        out["GroupConfiguration"] = {
            "Configuration": _group_configs[name],
            "Status": "UPDATE_COMPLETE",
        }
    return _json(out)


def _get_group(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    rec = _groups.get(name)
    if not rec:
        return _not_found(f"Group {name!r} not found.")
    return _json({"Group": rec})


def _delete_group(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    rec = _groups.pop(name, None)
    if not rec:
        return _not_found(f"Group {name!r} not found.")
    _group_queries.pop(name, None)
    _group_configs.pop(name, None)
    _group_members.pop(name, None)
    _group_tags.pop(name, None)
    return _json({"Group": rec})


def _update_group(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    rec = _groups.get(name)
    if not rec:
        return _not_found(f"Group {name!r} not found.")
    if "Description" in data:
        rec["Description"] = data["Description"] or ""
    if "Criticality" in data:
        rec["Criticality"] = int(data["Criticality"])
    if "Owner" in data:
        rec["Owner"] = data["Owner"]
    if "DisplayName" in data:
        rec["DisplayName"] = data["DisplayName"]
    return _json({"Group": rec})


def _list_groups(data):
    items = list(_groups.values())
    page, nxt = _paginate(items, data.get("MaxResults"), data.get("NextToken"))
    out = {
        "GroupIdentifiers": [_group_identifier(r) for r in page],
        "Groups": page,
    }
    if nxt:
        out["NextToken"] = nxt
    return _json(out)


def _get_group_query(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    rq = _group_queries.get(name)
    if rq is None:
        return _not_found(f"Group {name!r} has no resource query.")
    return _json({"GroupQuery": {"GroupName": name, "ResourceQuery": rq}})


def _update_group_query(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    rq = data.get("ResourceQuery")
    err = _validate_resource_query(rq)
    if err:
        return _bad_request(err)
    _group_queries[name] = copy.deepcopy(rq)
    return _json({"GroupQuery": {"GroupName": name, "ResourceQuery": _group_queries[name]}})


def _get_group_configuration(data):
    name = _resolve_name(group=data.get("Group"))
    if not name:
        return _bad_request("Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    cfg = _group_configs.get(name) or []
    return _json({"GroupConfiguration": {"Configuration": cfg, "Status": "UPDATE_COMPLETE"}})


def _put_group_configuration(data):
    name = _resolve_name(group=data.get("Group"))
    if not name:
        return _bad_request("Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    _group_configs[name] = list(data.get("Configuration") or [])
    return _json({})


def _group_resources(data):
    name = _resolve_name(group=data.get("Group"))
    if not name:
        return _bad_request("Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    arns = list(data.get("ResourceArns") or [])
    if not arns:
        return _bad_request("ResourceArns is required.")
    members = _group_members.get(name) or []
    seen = set(members)
    succeeded = []
    for arn in arns:
        if arn not in seen:
            members.append(arn)
            seen.add(arn)
        succeeded.append(arn)
    _group_members[name] = members
    return _json({"Succeeded": succeeded, "Failed": [], "Pending": []})


def _ungroup_resources(data):
    name = _resolve_name(group=data.get("Group"))
    if not name:
        return _bad_request("Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    arns = list(data.get("ResourceArns") or [])
    if not arns:
        return _bad_request("ResourceArns is required.")
    members = list(_group_members.get(name) or [])
    target = set(arns)
    _group_members[name] = [a for a in members if a not in target]
    return _json({"Succeeded": list(target), "Failed": [], "Pending": []})


def _list_group_resources(data):
    name = _resolve_name(data.get("GroupName"), data.get("Group"))
    if not name:
        return _bad_request("GroupName or Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    members = list(_group_members.get(name) or [])

    # Filters: list of {Name, Values}; supported filter Name is "resource-type".
    for f in (data.get("Filters") or []):
        if f.get("Name") == "resource-type":
            allowed = set(f.get("Values") or [])
            members = [a for a in members if _resource_type_from_arn(a) in allowed]

    page, nxt = _paginate(members, data.get("MaxResults"), data.get("NextToken"))
    identifiers = [
        {"ResourceArn": a, "ResourceType": _resource_type_from_arn(a)}
        for a in page
    ]
    out = {
        "Resources": [{"Identifier": ident, "Status": {"Name": "ACTIVE"}} for ident in identifiers],
        "ResourceIdentifiers": identifiers,
        "QueryErrors": [],
    }
    if nxt:
        out["NextToken"] = nxt
    return _json(out)


def _list_grouping_statuses(data):
    name = _resolve_name(group=data.get("Group"))
    if not name:
        return _bad_request("Group is required.")
    if name not in _groups:
        return _not_found(f"Group {name!r} not found.")
    members = list(_group_members.get(name) or [])
    page, nxt = _paginate(members, data.get("MaxResults"), data.get("NextToken"))
    statuses = [
        {"ResourceArn": a, "Action": "GROUP", "Status": "SUCCESS"}
        for a in page
    ]
    out = {"Group": _arn(name), "GroupingStatuses": statuses}
    if nxt:
        out["NextToken"] = nxt
    return _json(out)


def _search_resources(data):
    rq = data.get("ResourceQuery")
    err = _validate_resource_query(rq)
    if err:
        return _bad_request(err)
    # Stub: ministack does not maintain a global resource index across services,
    # so SearchResources returns empty results. Round-trips MaxResults / NextToken
    # for paginator compatibility.
    return _json({"ResourceIdentifiers": [], "QueryErrors": []})


def _get_account_settings(_data):
    return _json({"AccountSettings": _settings()})


def _update_account_settings(data):
    desired = data.get("GroupLifecycleEventsDesiredStatus")
    if desired not in ("ACTIVE", "INACTIVE"):
        return _bad_request("GroupLifecycleEventsDesiredStatus must be ACTIVE or INACTIVE.")
    s = _settings()
    s["GroupLifecycleEventsDesiredStatus"] = desired
    s["GroupLifecycleEventsStatus"] = desired
    return _json({"AccountSettings": s})


# --- Tag, Untag, GetTags (operate on group ARN) ----------------------------

def _resolve_arn_group(arn):
    if not arn:
        return None
    m = _GROUP_ARN_RE.match(arn)
    if not m:
        return None
    return m.group("name")


def _tag(arn, data):
    name = _resolve_arn_group(arn)
    if not name or name not in _groups:
        return _not_found(f"Group ARN {arn!r} not found.")
    tags = dict(data.get("Tags") or {})
    if not tags:
        return _bad_request("Tags is required.")
    existing = dict(_group_tags.get(name) or {})
    existing.update(tags)
    _group_tags[name] = existing
    return _json({"Arn": arn, "Tags": tags})


def _untag(arn, data):
    name = _resolve_arn_group(arn)
    if not name or name not in _groups:
        return _not_found(f"Group ARN {arn!r} not found.")
    keys = list(data.get("Keys") or [])
    if not keys:
        return _bad_request("Keys is required.")
    existing = dict(_group_tags.get(name) or {})
    for k in keys:
        existing.pop(k, None)
    _group_tags[name] = existing
    return _json({"Arn": arn, "Keys": keys})


def _get_tags(arn):
    name = _resolve_arn_group(arn)
    if not name or name not in _groups:
        return _not_found(f"Group ARN {arn!r} not found.")
    return _json({"Arn": arn, "Tags": dict(_group_tags.get(name) or {})})


def _resource_type_from_arn(arn):
    """Best-effort extraction of `AWS::Service::ResourceType` from an ARN.
    Returns the raw ARN service slot uppercased when the type can't be inferred."""
    parts = arn.split(":", 5) if isinstance(arn, str) else []
    if len(parts) < 6:
        return ""
    service = parts[2]
    tail = parts[5]
    sub = tail.split("/", 1)[0] if "/" in tail else tail.split(":", 1)[0]
    if not sub:
        sub = "Resource"
    return f"AWS::{service.upper()}::{sub.capitalize()}"


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

_POST_ROUTES = {
    "/groups": _create_group,
    "/groups-list": _list_groups,
    "/get-group": _get_group,
    "/delete-group": _delete_group,
    "/update-group": _update_group,
    "/get-group-query": _get_group_query,
    "/update-group-query": _update_group_query,
    "/get-group-configuration": _get_group_configuration,
    "/put-group-configuration": _put_group_configuration,
    "/group-resources": _group_resources,
    "/ungroup-resources": _ungroup_resources,
    "/list-group-resources": _list_group_resources,
    "/list-grouping-statuses": _list_grouping_statuses,
    "/resources/search": _search_resources,
    "/get-account-settings": _get_account_settings,
    "/update-account-settings": _update_account_settings,
}


_TAG_ARN_PATH_RE = re.compile(r"^/resources/(?P<arn>.+)/tags$")


async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return _bad_request("Invalid JSON body.")

    if method == "POST":
        handler = _POST_ROUTES.get(path)
        if handler:
            return handler(data)

    m = _TAG_ARN_PATH_RE.match(path)
    if m:
        from urllib.parse import unquote
        arn = unquote(m.group("arn"))
        if method == "GET":
            return _get_tags(arn)
        if method == "PUT":
            return _tag(arn, data)
        if method == "PATCH":
            return _untag(arn, data)

    return _bad_request(f"Unknown Resource Groups route: {method} {path}")
