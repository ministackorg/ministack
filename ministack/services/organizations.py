"""
AWS Organizations stub.

JSON 1.1 protocol, target prefix ``AWSOrganizationsV20161128``.

Models a single-master-account organization. The master is whatever account
the request is made under (resolved via ``get_account_id``); the org returns
itself as ALL-features by default. Accounts and OUs are stored in
account-scoped state so each tenant gets its own org.

Includes the ``Path`` field on Account and OrganizationalUnit per the
2026-03-31 AWS additive change.
"""

import copy
import json
import logging
import time

from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    new_uuid,
)

logger = logging.getLogger("organizations")


# Per-master-account state. Each account that calls Organizations gets its
# own org graph; that mirrors how local-emulator multi-tenancy works.
_orgs = AccountScopedDict()       # singleton "self" -> Organization dict
_accounts = AccountScopedDict()   # account_id -> Account dict
_ous = AccountScopedDict()        # ou_id -> OU dict (with ParentId)
_roots = AccountScopedDict()      # root_id -> Root dict (single root)
_tags = AccountScopedDict()       # resource_id (ou-/account/r-/policy) -> {tag_key: tag_value}


def reset():
    _orgs.clear()
    _accounts.clear()
    _ous.clear()
    _roots.clear()
    _tags.clear()


def get_state():
    return {
        "orgs": copy.deepcopy(_orgs),
        "accounts": copy.deepcopy(_accounts),
        "ous": copy.deepcopy(_ous),
        "roots": copy.deepcopy(_roots),
        "tags": copy.deepcopy(_tags),
    }


def restore_state(data):
    if not data:
        return
    for store, key in (
        (_orgs, "orgs"), (_accounts, "accounts"),
        (_ous, "ous"), (_roots, "roots"), (_tags, "tags")
    ):
        store.clear()
        for k, v in (data.get(key) or {}).items():
            store[k] = v


def _json(status, body):
    return status, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps(body).encode()


def _ensure_org():
    """Lazily initialise the org for the current master account."""
    if "self" in _orgs:
        return
    master = get_account_id()
    org_id = "o-" + new_uuid().replace("-", "")[:10]
    root_id = "r-" + new_uuid().replace("-", "")[:6]
    _orgs["self"] = {
        "Id": org_id,
        "Arn": f"arn:aws:organizations::{master}:organization/{org_id}",
        "FeatureSet": "ALL",
        "MasterAccountArn": f"arn:aws:organizations::{master}:account/{org_id}/{master}",
        "MasterAccountId": master,
        "MasterAccountEmail": f"master+{master}@ministack.local",
        "AvailablePolicyTypes": [
            {"Type": "SERVICE_CONTROL_POLICY", "Status": "ENABLED"},
        ],
    }
    _roots[root_id] = {
        "Id": root_id,
        "Arn": f"arn:aws:organizations::{master}:root/{org_id}/{root_id}",
        "Name": "Root",
        "PolicyTypes": [],
    }
    # Master account record
    _accounts[master] = {
        "Id": master,
        "Arn": f"arn:aws:organizations::{master}:account/{org_id}/{master}",
        "Email": f"master+{master}@ministack.local",
        "Name": "Master Account",
        "Status": "ACTIVE",
        "JoinedMethod": "INVITED",
        "JoinedTimestamp": int(time.time()),
        "Path": "/",
        "_ParentId": root_id,
    }


def _public_account(a: dict) -> dict:
    return {k: v for k, v in a.items() if not k.startswith("_")}


def _public_ou(o: dict) -> dict:
    return {k: v for k, v in o.items() if not k.startswith("_")}


def _describe_organization(_payload):
    _ensure_org()
    return _json(200, {"Organization": dict(_orgs["self"])})


def _list_roots(_payload):
    _ensure_org()
    return _json(200, {"Roots": list(_roots.values()), "NextToken": None})


def _list_accounts(_payload):
    _ensure_org()
    return _json(200, {
        "Accounts": [_public_account(a) for a in _accounts.values()],
        "NextToken": None,
    })


def _describe_account(payload):
    _ensure_org()
    aid = payload.get("AccountId")
    if not aid:
        return error_response_json("InvalidInputException", "AccountId is required", 400)
    a = _accounts.get(aid)
    if not a:
        return error_response_json("AccountNotFoundException",
                                   f"Account {aid} not found", 400)
    return _json(200, {"Account": _public_account(a)})


def _list_organizational_units_for_parent(payload):
    _ensure_org()
    parent_id = payload.get("ParentId") or ""
    out = [_public_ou(o) for o in _ous.values() if o.get("_ParentId") == parent_id]
    return _json(200, {"OrganizationalUnits": out, "NextToken": None})


def _list_accounts_for_parent(payload):
    _ensure_org()
    parent_id = payload.get("ParentId") or ""
    out = [_public_account(a) for a in _accounts.values()
           if a.get("_ParentId") == parent_id]
    return _json(200, {"Accounts": out, "NextToken": None})


def _list_parents(payload):
    _ensure_org()
    child_id = payload.get("ChildId")
    if not child_id:
        return error_response_json("InvalidInputException", "ChildId is required", 400)
    # A child is either an OU (ou-*) or an account; both store ``_ParentId``.
    # AWS returns exactly one parent and does not surface it on Describe*, so the
    # provider must ListParents the child to learn it (fires on create + refresh).
    rec = _ous.get(child_id) or _accounts.get(child_id)
    if rec is None:
        return error_response_json(
            "ChildNotFoundException",
            f"We can't find an organizational unit (OU) or account with the ChildId {child_id}",
            400,
        )
    parent_id = rec.get("_ParentId")
    parent_type = "ROOT" if str(parent_id).startswith("r-") else "ORGANIZATIONAL_UNIT"
    return _json(200, {"Parents": [{"Id": parent_id, "Type": parent_type}], "NextToken": None})


def _create_organizational_unit(payload):
    _ensure_org()
    parent_id = payload.get("ParentId")
    name = payload.get("Name")
    if not parent_id or not name:
        return error_response_json("InvalidInputException",
                                   "ParentId and Name are required", 400)
    org_id = _orgs["self"]["Id"]
    master = get_account_id()
    ou_id = f"ou-{parent_id.split('-')[-1][:4]}-{new_uuid().replace('-','')[:10]}"
    parent_ou = _ous.get(parent_id)
    parent_path = (parent_ou or {}).get("Path", "/")
    rec = {
        "Id": ou_id,
        "Arn": f"arn:aws:organizations::{master}:ou/{org_id}/{ou_id}",
        "Name": name,
        "Path": (parent_path.rstrip("/") + "/" + name + "/") if parent_path != "/" else f"/{name}/",
        "_ParentId": parent_id,
    }
    _ous[ou_id] = rec
    inline_tags = payload.get("Tags") or []
    if inline_tags:
        _tags[ou_id] = {t["Key"]: t.get("Value", "") for t in inline_tags if "Key" in t}
    return _json(200, {"OrganizationalUnit": _public_ou(rec)})


def _describe_organizational_unit(payload):
    _ensure_org()
    ou_id = payload.get("OrganizationalUnitId")
    o = _ous.get(ou_id) if ou_id else None
    if not o:
        return error_response_json("OrganizationalUnitNotFoundException",
                                   f"OU {ou_id} not found", 400)
    return _json(200, {"OrganizationalUnit": _public_ou(o)})


def _delete_organizational_unit(payload):
    _ensure_org()
    ou_id = payload.get("OrganizationalUnitId")
    if not ou_id or ou_id not in _ous:
        return error_response_json("OrganizationalUnitNotFoundException",
                                   f"OU {ou_id} not found", 400)
    del _ous[ou_id]
    _tags.pop(ou_id, None)
    return _json(200, {})


def _tag_list(resource_id):
    return [{"Key": k, "Value": v} for k, v in (_tags.get(resource_id) or {}).items()]


def _resource_exists(rid):
    # Taggable org resources tracked today: OUs, accounts, and the root.
    return (
        _ous.get(rid) is not None
        or _accounts.get(rid) is not None
        or _roots.get(rid) is not None
    )


def _require_resource(rid):
    """Shared validation for the tag ops. Returns an error 3-tuple, or None when the
    ResourceId is present and known. AWS errors on an unknown target, so we match it
    rather than return an empty/spurious result."""
    if not rid:
        return error_response_json("InvalidInputException", "ResourceId is required", 400)
    if not _resource_exists(rid):
        return error_response_json(
            "TargetNotFoundException",
            f"We can't find a resource with the ResourceId {rid}", 400,
        )
    return None


def _tag_resource(payload):
    _ensure_org()
    rid = payload.get("ResourceId")
    err = _require_resource(rid)
    if err:
        return err
    current = dict(_tags.get(rid) or {})
    for t in payload.get("Tags") or []:
        if "Key" in t:
            current[t["Key"]] = t.get("Value", "")
    _tags[rid] = current
    return _json(200, {})


def _untag_resource(payload):
    _ensure_org()
    rid = payload.get("ResourceId")
    err = _require_resource(rid)
    if err:
        return err
    current = dict(_tags.get(rid) or {})
    for k in payload.get("TagKeys") or []:
        current.pop(k, None)
    _tags[rid] = current
    return _json(200, {})


def _list_tags_for_resource(payload):
    _ensure_org()
    rid = payload.get("ResourceId")
    err = _require_resource(rid)
    if err:
        return err
    # A consumer's Read of any taggable org resource calls ListTagsForResource on
    # create + refresh; without it the read-back fails and apply can't converge.
    return _json(200, {"Tags": _tag_list(rid), "NextToken": None})


_DISPATCH = {
    "DescribeOrganization": _describe_organization,
    "ListRoots": _list_roots,
    "ListAccounts": _list_accounts,
    "DescribeAccount": _describe_account,
    "ListOrganizationalUnitsForParent": _list_organizational_units_for_parent,
    "ListAccountsForParent": _list_accounts_for_parent,
    "ListParents": _list_parents,
    "CreateOrganizationalUnit": _create_organizational_unit,
    "DescribeOrganizationalUnit": _describe_organizational_unit,
    "DeleteOrganizationalUnit": _delete_organizational_unit,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    op = target.split(".", 1)[1] if "." in target else target
    if not op:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)

    fn = _DISPATCH.get(op)
    if fn is None:
        return error_response_json("InvalidAction",
                                   f"Operation '{op}' not implemented", 400)
    return fn(payload)
