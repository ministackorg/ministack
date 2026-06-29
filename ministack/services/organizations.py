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
import re
import time

from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    new_uuid,
    set_request_account_id,
)

logger = logging.getLogger("organizations")

_12_DIGITS = re.compile(r"^\d{12}$")


# Per-master-account state. Each account that calls Organizations gets its
# own org graph; that mirrors how local-emulator multi-tenancy works.
_orgs = AccountScopedDict()       # singleton "self" -> Organization dict
_accounts = AccountScopedDict()   # account_id -> Account dict
_ous = AccountScopedDict()        # ou_id -> OU dict (with ParentId)
_roots = AccountScopedDict()      # root_id -> Root dict (single root)
_create_status = AccountScopedDict()  # car-id -> CreateAccountStatus dict

# Handshakes are intentionally NOT account-scoped: an invite flow spans two
# accounts (the master creates it, the invited account accepts it), so both
# callers must be able to look up the same record by id. Visibility is enforced
# per-caller in ListHandshakesForAccount / AcceptHandshake instead.
_handshakes: dict = {}            # h-id -> handshake dict (global)


def reset():
    _orgs.clear()
    _accounts.clear()
    _ous.clear()
    _roots.clear()
    _create_status.clear()
    _handshakes.clear()


def get_state():
    return {
        "orgs": copy.deepcopy(_orgs),
        "accounts": copy.deepcopy(_accounts),
        "ous": copy.deepcopy(_ous),
        "roots": copy.deepcopy(_roots),
        "create_status": copy.deepcopy(_create_status),
        "handshakes": copy.deepcopy(_handshakes),
    }


def restore_state(data):
    if not data:
        return
    for store, key in (
        (_orgs, "orgs"), (_accounts, "accounts"),
        (_ous, "ous"), (_roots, "roots"),
        (_create_status, "create_status"),
    ):
        store.clear()
        for k, v in (data.get(key) or {}).items():
            store[k] = v
    _handshakes.clear()
    _handshakes.update(data.get("handshakes") or {})


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


def _public_create_status(s: dict) -> dict:
    return {k: v for k, v in s.items() if not k.startswith("_")}


def _public_handshake(h: dict) -> dict:
    return {k: v for k, v in h.items() if not k.startswith("_")}


def _new_account_id() -> str:
    """Generate an unused 12-digit account id (service-assigned, like real AWS)."""
    aid = str(int(new_uuid().replace("-", ""), 16))[-12:].zfill(12)
    while aid in _accounts:
        aid = str(int(new_uuid().replace("-", ""), 16))[-12:].zfill(12)
    return aid


def _root_id() -> str:
    """Return the single root id for the current org (org is already ensured)."""
    return next(iter(_roots))


def _account_record(aid: str, *, email: str, name: str, joined_method: str,
                    parent_id: str) -> dict:
    """Build an Account record matching the master-account shape in _ensure_org."""
    org_id = _orgs["self"]["Id"]
    master = get_account_id()
    return {
        "Id": aid,
        "Arn": f"arn:aws:organizations::{master}:account/{org_id}/{aid}",
        "Email": email,
        "Name": name,
        "Status": "ACTIVE",
        "JoinedMethod": joined_method,
        "JoinedTimestamp": int(time.time()),
        "Path": "/",
        "_ParentId": parent_id,
    }


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
    return _json(200, {})


def _create_account(payload):
    _ensure_org()
    email = payload.get("Email")
    name = payload.get("AccountName")
    if not email or not name:
        return error_response_json("InvalidInputException",
                                   "Email and AccountName are required", 400)
    aid = _new_account_id()
    car_id = ("car-" + new_uuid().replace("-", ""))[:36]
    rec = {
        "Id": car_id,
        "AccountName": name,
        "State": "IN_PROGRESS",
        "RequestedTimestamp": int(time.time()),
        "_Email": email,
        "_AccountId": aid,
    }
    _create_status[car_id] = rec
    return _json(200, {"CreateAccountStatus": _public_create_status(rec)})


def _describe_create_account_status(payload):
    _ensure_org()
    car_id = payload.get("CreateAccountRequestId")
    rec = _create_status.get(car_id) if car_id else None
    if not rec:
        return error_response_json("CreateAccountStatusNotFoundException",
                                   f"CreateAccountStatus {car_id} not found", 400)
    # Async create completes on first describe: place the account at the root.
    if rec["State"] == "IN_PROGRESS":
        aid = rec["_AccountId"]
        _accounts[aid] = _account_record(
            aid, email=rec["_Email"], name=rec["AccountName"],
            joined_method="CREATED", parent_id=_root_id())
        rec["State"] = "SUCCEEDED"
        rec["AccountId"] = aid
        rec["CompletedTimestamp"] = int(time.time())
    return _json(200, {"CreateAccountStatus": _public_create_status(rec)})


def _move_account(payload):
    _ensure_org()
    aid = payload.get("AccountId")
    source = payload.get("SourceParentId")
    dest = payload.get("DestinationParentId")
    if not aid or not source or not dest:
        return error_response_json(
            "InvalidInputException",
            "AccountId, SourceParentId and DestinationParentId are required", 400)
    account = _accounts.get(aid)
    if not account:
        return error_response_json("AccountNotFoundException",
                                   f"Account {aid} not found", 400)
    if (source not in _roots and source not in _ous) or account.get("_ParentId") != source:
        return error_response_json("SourceParentNotFoundException",
                                   f"Account {aid} is not in source parent {source}", 400)
    if dest not in _roots and dest not in _ous:
        return error_response_json("DestinationParentNotFoundException",
                                   f"Destination parent {dest} not found", 400)
    account["_ParentId"] = dest
    return _json(200, {})


def _list_parents(payload):
    _ensure_org()
    child_id = payload.get("ChildId")
    rec = (_accounts.get(child_id) or _ous.get(child_id)) if child_id else None
    if not rec:
        return error_response_json("ChildNotFoundException",
                                   f"Child {child_id} not found", 400)
    parent_id = rec.get("_ParentId")
    ptype = "ROOT" if parent_id in _roots else "ORGANIZATIONAL_UNIT"
    return _json(200, {"Parents": [{"Id": parent_id, "Type": ptype}], "NextToken": None})


def _list_children(payload):
    _ensure_org()
    parent_id = payload.get("ParentId")
    child_type = payload.get("ChildType")
    if not parent_id or (parent_id not in _roots and parent_id not in _ous):
        return error_response_json("ParentNotFoundException",
                                   f"Parent {parent_id} not found", 400)
    if child_type == "ACCOUNT":
        children = [{"Id": a["Id"], "Type": "ACCOUNT"}
                    for a in _accounts.values() if a.get("_ParentId") == parent_id]
    elif child_type == "ORGANIZATIONAL_UNIT":
        children = [{"Id": o["Id"], "Type": "ORGANIZATIONAL_UNIT"}
                    for o in _ous.values() if o.get("_ParentId") == parent_id]
    else:
        return error_response_json("InvalidInputException",
                                   "ChildType must be ACCOUNT or ORGANIZATIONAL_UNIT", 400)
    return _json(200, {"Children": children, "NextToken": None})


def _invite_account_to_organization(payload):
    _ensure_org()
    target = payload.get("Target") or {}
    target_id = target.get("Id")
    if not target_id:
        return error_response_json("InvalidInputException",
                                   "Target.Id is required", 400)
    if target.get("Type") == "ACCOUNT":
        if not _12_DIGITS.match(target_id):
            return error_response_json("InvalidInputException",
                                       "Target.Id must be a 12-digit account id", 400)
        invited = target_id
    else:
        invited = _new_account_id()
    if invited in _accounts:
        return error_response_json("DuplicateAccountException",
                                   f"Account {invited} is already a member", 400)
    master = get_account_id()
    if any(h["State"] == "OPEN" and h["_InvitedAccountId"] == invited
           and h["_MasterAccountId"] == master for h in _handshakes.values()):
        return error_response_json("DuplicateHandshakeException",
                                   f"An open handshake for {invited} already exists", 400)
    org_id = _orgs["self"]["Id"]
    h_id = "h-" + new_uuid().replace("-", "")[:10]
    now = int(time.time())
    rec = {
        "Id": h_id,
        "Arn": f"arn:aws:organizations::{master}:handshake/{org_id}/invite/{h_id}",
        "State": "OPEN",
        "Action": "INVITE",
        "RequestedTimestamp": now,
        "ExpirationTimestamp": now + 15 * 24 * 3600,
        "Parties": [
            {"Id": org_id, "Type": "ORGANIZATION"},
            {"Id": invited, "Type": "ACCOUNT"},
        ],
        "_MasterAccountId": master,
        "_InvitedAccountId": invited,
    }
    _handshakes[h_id] = rec
    return _json(200, {"Handshake": _public_handshake(rec)})


def _accept_handshake(payload):
    _ensure_org()
    h_id = payload.get("HandshakeId")
    rec = _handshakes.get(h_id) if h_id else None
    if not rec:
        return error_response_json("HandshakeNotFoundException",
                                   f"Handshake {h_id} not found", 400)
    caller = get_account_id()
    if caller != rec["_InvitedAccountId"]:
        return error_response_json("AccountOwnerNotVerifiedException",
                                   "Only the invited account may accept this handshake", 400)
    if rec["State"] != "OPEN":
        return error_response_json("InvalidHandshakeTransitionException",
                                   f"Handshake {h_id} is not OPEN", 400)
    rec["State"] = "ACCEPTED"
    # Materialise the member into the MASTER's org graph. _accounts is scoped to
    # the current caller, so impersonate the master for the write, then restore.
    master = rec["_MasterAccountId"]
    invited = rec["_InvitedAccountId"]
    try:
        set_request_account_id(master)
        _ensure_org()
        _accounts[invited] = _account_record(
            invited, email=f"member+{invited}@ministack.local", name=f"Account {invited}",
            joined_method="INVITED", parent_id=_root_id())
    finally:
        set_request_account_id(caller)
    return _json(200, {"Handshake": _public_handshake(rec)})


def _list_handshakes_for_account(_payload):
    _ensure_org()
    caller = get_account_id()
    out = [_public_handshake(h) for h in _handshakes.values()
           if caller in (h["_MasterAccountId"], h["_InvitedAccountId"])]
    return _json(200, {"Handshakes": out, "NextToken": None})


_DISPATCH = {
    "DescribeOrganization": _describe_organization,
    "ListRoots": _list_roots,
    "ListAccounts": _list_accounts,
    "DescribeAccount": _describe_account,
    "ListOrganizationalUnitsForParent": _list_organizational_units_for_parent,
    "ListAccountsForParent": _list_accounts_for_parent,
    "CreateOrganizationalUnit": _create_organizational_unit,
    "DescribeOrganizationalUnit": _describe_organizational_unit,
    "DeleteOrganizationalUnit": _delete_organizational_unit,
    "CreateAccount": _create_account,
    "DescribeCreateAccountStatus": _describe_create_account_status,
    "MoveAccount": _move_account,
    "ListParents": _list_parents,
    "ListChildren": _list_children,
    "InviteAccountToOrganization": _invite_account_to_organization,
    "AcceptHandshake": _accept_handshake,
    "ListHandshakesForAccount": _list_handshakes_for_account,
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
