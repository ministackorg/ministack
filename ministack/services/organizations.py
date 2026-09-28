# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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
_policies = AccountScopedDict()   # policy_id -> Policy dict (summary fields + Content)
_attachments = AccountScopedDict()  # policy_id -> [target_id, ...]
_create_statuses = AccountScopedDict()  # car-id -> CreateAccountStatus dict

FULL_ACCESS_POLICY_ID = "p-FullAWSAccess"


def reset():
    _orgs.clear()
    _accounts.clear()
    _ous.clear()
    _roots.clear()
    _tags.clear()
    _policies.clear()
    _attachments.clear()
    _create_statuses.clear()


def get_state():
    return {
        "orgs": copy.deepcopy(_orgs),
        "accounts": copy.deepcopy(_accounts),
        "ous": copy.deepcopy(_ous),
        "roots": copy.deepcopy(_roots),
        "tags": copy.deepcopy(_tags),
        "policies": copy.deepcopy(_policies),
        "attachments": copy.deepcopy(_attachments),
        "create_statuses": copy.deepcopy(_create_statuses),
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    for store, key in (
        (_orgs, "orgs"), (_accounts, "accounts"),
        (_ous, "ous"), (_roots, "roots"), (_tags, "tags"),
        (_policies, "policies"), (_attachments, "attachments"),
        (_create_statuses, "create_statuses"),
    ):
        # update() copies every account's entries; iterating an
        # AccountScopedDict yields only the caller's, and the loader runs at
        # boot with no request scope, so a per-key loop would drop every
        # account but the default one. `or {}` is wrong here for the same
        # reason: the truthiness of a scoped dict is account-scoped too.
        restored = data.get(key)
        store.clear()
        if restored is not None:
            store.update(restored)


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
        # An ALL-features org has SCPs enabled on its root.
        "PolicyTypes": [{"Type": "SERVICE_CONTROL_POLICY", "Status": "ENABLED"}],
    }
    _policies[FULL_ACCESS_POLICY_ID] = {
        "Id": FULL_ACCESS_POLICY_ID,
        "Arn": f"arn:aws:organizations::aws:policy/service_control_policy/{FULL_ACCESS_POLICY_ID}",
        "Name": "FullAWSAccess",
        "Description": "Allows access to every operation",
        "Type": "SERVICE_CONTROL_POLICY",
        "AwsManaged": True,
        "_Content": json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}),
    }
    _attachments[FULL_ACCESS_POLICY_ID] = [root_id, master]
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
    return _json(200, {"Roots": list(_roots.values())})


def _list_accounts(_payload):
    _ensure_org()
    return _json(200, {
        "Accounts": [_public_account(a) for a in _accounts.values()],
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
    return _json(200, {"OrganizationalUnits": out})


def _list_accounts_for_parent(payload):
    _ensure_org()
    parent_id = payload.get("ParentId") or ""
    out = [_public_account(a) for a in _accounts.values()
           if a.get("_ParentId") == parent_id]
    return _json(200, {"Accounts": out})


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
    return _json(200, {"Parents": [{"Id": parent_id, "Type": parent_type}]})


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
    return (
        _ous.get(rid) is not None
        or _accounts.get(rid) is not None
        or _roots.get(rid) is not None
        or _policies.get(rid) is not None
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
    return _json(200, {"Tags": _tag_list(rid)})


_POLICY_TYPES = (
    "SERVICE_CONTROL_POLICY", "RESOURCE_CONTROL_POLICY", "TAG_POLICY", "BACKUP_POLICY",
    "AISERVICES_OPT_OUT_POLICY", "CHATBOT_POLICY", "DECLARATIVE_POLICY_EC2",
    "SECURITYHUB_POLICY", "INSPECTOR_POLICY", "UPGRADE_ROLLOUT_POLICY", "BEDROCK_POLICY",
    "S3_POLICY", "NETWORK_SECURITY_DIRECTOR_POLICY",
)


def _policy_summary(p):
    return {k: v for k, v in p.items() if not k.startswith("_")}


def _policy_document(p):
    return {"PolicySummary": _policy_summary(p), "Content": p["_Content"]}


def _require_policy(policy_id):
    if not policy_id:
        return None, error_response_json("InvalidInputException", "PolicyId is required", 400)
    p = _policies.get(policy_id)
    if p is None:
        return None, error_response_json(
            "PolicyNotFoundException",
            f"We can't find a policy with the PolicyId {policy_id}", 400)
    return p, None


def _target(target_id):
    """The record and TargetType for a root, OU or account id."""
    if _roots.get(target_id):
        return _roots[target_id], "ROOT"
    if _ous.get(target_id):
        return _ous[target_id], "ORGANIZATIONAL_UNIT"
    if _accounts.get(target_id):
        return _accounts[target_id], "ACCOUNT"
    return None, None


def _policy_type_enabled(policy_type):
    for root in _roots.values():
        for entry in root.get("PolicyTypes") or []:
            if entry.get("Type") == policy_type and entry.get("Status") == "ENABLED":
                return True
    return False


def _create_account(payload):
    _ensure_org()
    email = payload.get("Email")
    name = payload.get("AccountName")
    if not email or not name:
        return error_response_json("InvalidInputException",
                                   "Email and AccountName are required", 400)
    org_id = _orgs["self"]["Id"]
    master = get_account_id()
    digits = new_uuid().replace("-", "")
    account_id = "".join(c for c in digits if c.isdigit()).ljust(12, "0")[:12]
    request_id = "car-" + new_uuid().replace("-", "")[:8]
    now = int(time.time())
    root_id = next(iter(_roots))
    _accounts[account_id] = {
        "Id": account_id,
        "Arn": f"arn:aws:organizations::{master}:account/{org_id}/{account_id}",
        "Email": email,
        "Name": name,
        "Status": "ACTIVE",
        "JoinedMethod": "CREATED",
        "JoinedTimestamp": now,
        "Path": "/",
        "_ParentId": root_id,
    }
    attached = list(_attachments.get(FULL_ACCESS_POLICY_ID) or [])
    _attachments[FULL_ACCESS_POLICY_ID] = attached + [account_id]
    inline_tags = payload.get("Tags") or []
    if inline_tags:
        _tags[account_id] = {t["Key"]: t.get("Value", "") for t in inline_tags if "Key" in t}
    status = {
        "Id": request_id,
        "AccountName": name,
        "State": "SUCCEEDED",
        "RequestedTimestamp": now,
        "CompletedTimestamp": now,
        "AccountId": account_id,
    }
    _create_statuses[request_id] = status
    return _json(200, {"CreateAccountStatus": dict(status)})


def _describe_create_account_status(payload):
    _ensure_org()
    request_id = payload.get("CreateAccountRequestId")
    if not request_id:
        return error_response_json("InvalidInputException",
                                   "CreateAccountRequestId is required", 400)
    status = _create_statuses.get(request_id)
    if status is None:
        return error_response_json(
            "CreateAccountStatusNotFoundException",
            f"We can't find an create account request with the Id {request_id}", 400)
    return _json(200, {"CreateAccountStatus": dict(status)})


def _move_account(payload):
    _ensure_org()
    account_id = payload.get("AccountId")
    source = payload.get("SourceParentId")
    destination = payload.get("DestinationParentId")
    if not account_id or not source or not destination:
        return error_response_json(
            "InvalidInputException",
            "AccountId, SourceParentId and DestinationParentId are required", 400)
    account = _accounts.get(account_id)
    if account is None:
        return error_response_json("AccountNotFoundException",
                                   f"We can't find an account with the AccountId {account_id}", 400)
    if _target(source)[1] not in ("ROOT", "ORGANIZATIONAL_UNIT"):
        return error_response_json("SourceParentNotFoundException",
                                   f"We can't find a parent with the ParentId {source}", 400)
    if _target(destination)[1] not in ("ROOT", "ORGANIZATIONAL_UNIT"):
        return error_response_json("DestinationParentNotFoundException",
                                   f"We can't find a parent with the ParentId {destination}", 400)
    if account.get("_ParentId") == destination:
        return error_response_json("DuplicateAccountException",
                                   "That account is already present in the specified destination.", 400)
    updated = dict(account)
    updated["_ParentId"] = destination
    _accounts[account_id] = updated
    return _json(200, {})


def _close_account(payload):
    _ensure_org()
    account_id = payload.get("AccountId")
    if not account_id:
        return error_response_json("InvalidInputException", "AccountId is required", 400)
    account = _accounts.get(account_id)
    if account is None:
        return error_response_json("AccountNotFoundException",
                                   f"We can't find an account with the AccountId {account_id}", 400)
    if account.get("Status") == "SUSPENDED":
        return error_response_json("AccountAlreadyClosedException",
                                   f"The account {account_id} is already closed", 400)
    updated = dict(account)
    updated["Status"] = "SUSPENDED"
    _accounts[account_id] = updated
    return _json(200, {})


def _create_policy(payload):
    _ensure_org()
    content = payload.get("Content")
    description = payload.get("Description")
    name = payload.get("Name")
    policy_type = payload.get("Type")
    if not content or description is None or not name or not policy_type:
        return error_response_json(
            "InvalidInputException", "Content, Description, Name and Type are required", 400)
    if policy_type not in _POLICY_TYPES:
        return error_response_json("InvalidInputException",
                                   f"Type {policy_type} is not a supported policy type", 400)
    try:
        json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return error_response_json("MalformedPolicyDocumentException",
                                   "The provided policy document is not valid JSON", 400)
    if any(p["Name"] == name and p["Type"] == policy_type for p in _policies.values()):
        return error_response_json("DuplicatePolicyException",
                                   f"A policy with the name {name} already exists", 400)
    org_id = _orgs["self"]["Id"]
    policy_id = "p-" + new_uuid().replace("-", "")[:8]
    rec = {
        "Id": policy_id,
        "Arn": f"arn:aws:organizations::{get_account_id()}:policy/{org_id}/"
               f"{policy_type.lower()}/{policy_id}",
        "Name": name,
        "Description": description,
        "Type": policy_type,
        "AwsManaged": False,
        "_Content": content,
    }
    _policies[policy_id] = rec
    inline_tags = payload.get("Tags") or []
    if inline_tags:
        _tags[policy_id] = {t["Key"]: t.get("Value", "") for t in inline_tags if "Key" in t}
    return _json(200, {"Policy": _policy_document(rec)})


def _describe_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    return _json(200, {"Policy": _policy_document(policy)})


def _update_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    if policy.get("AwsManaged"):
        return error_response_json("AccessDeniedException",
                                   "You can't modify an Amazon Web Services managed policy", 400)
    name = payload.get("Name")
    if name and any(p["Name"] == name and p["Id"] != policy["Id"]
                    and p["Type"] == policy["Type"] for p in _policies.values()):
        return error_response_json("DuplicatePolicyException",
                                   f"A policy with the name {name} already exists", 400)
    content = payload.get("Content")
    if content is not None:
        try:
            json.loads(content)
        except (TypeError, json.JSONDecodeError):
            return error_response_json("MalformedPolicyDocumentException",
                                       "The provided policy document is not valid JSON", 400)
    updated = dict(policy)
    if name:
        updated["Name"] = name
    if payload.get("Description") is not None:
        updated["Description"] = payload["Description"]
    if content is not None:
        updated["_Content"] = content
    _policies[policy["Id"]] = updated
    return _json(200, {"Policy": _policy_document(updated)})


def _delete_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    if _attachments.get(policy["Id"]):
        return error_response_json(
            "PolicyInUseException",
            f"The policy {policy['Id']} is attached to one or more targets", 400)
    del _policies[policy["Id"]]
    _tags.pop(policy["Id"], None)
    _attachments.pop(policy["Id"], None)
    return _json(200, {})


def _list_policies(payload):
    _ensure_org()
    policy_filter = payload.get("Filter")
    if not policy_filter:
        return error_response_json("InvalidInputException", "Filter is required", 400)
    out = [_policy_summary(p) for p in _policies.values() if p["Type"] == policy_filter]
    return _json(200, {"Policies": out})


def _attach_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    target_id = payload.get("TargetId")
    if not target_id:
        return error_response_json("InvalidInputException", "TargetId is required", 400)
    if _target(target_id)[0] is None:
        return error_response_json("TargetNotFoundException",
                                   f"We can't find a root, OU or account with the TargetId {target_id}",
                                   400)
    if not _policy_type_enabled(policy["Type"]):
        return error_response_json(
            "PolicyTypeNotEnabledException",
            f"The policy type {policy['Type']} is not enabled in the current root", 400)
    attached = list(_attachments.get(policy["Id"]) or [])
    if target_id in attached:
        return error_response_json(
            "DuplicatePolicyAttachmentException",
            f"The policy {policy['Id']} is already attached to {target_id}", 400)
    _attachments[policy["Id"]] = attached + [target_id]
    return _json(200, {})


def _detach_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    target_id = payload.get("TargetId")
    attached = list(_attachments.get(policy["Id"]) or [])
    if target_id not in attached:
        return error_response_json(
            "PolicyNotAttachedException",
            f"The policy {policy['Id']} is not attached to {target_id}", 400)
    attached.remove(target_id)
    _attachments[policy["Id"]] = attached
    return _json(200, {})


def _list_policies_for_target(payload):
    _ensure_org()
    target_id = payload.get("TargetId")
    policy_filter = payload.get("Filter")
    if not target_id or not policy_filter:
        return error_response_json("InvalidInputException",
                                   "TargetId and Filter are required", 400)
    if _target(target_id)[0] is None:
        return error_response_json("TargetNotFoundException",
                                   f"We can't find a root, OU or account with the TargetId {target_id}",
                                   400)
    out = [_policy_summary(p) for pid, p in _policies.items()
           if p["Type"] == policy_filter and target_id in (_attachments.get(pid) or [])]
    return _json(200, {"Policies": out})


def _list_targets_for_policy(payload):
    _ensure_org()
    policy, err = _require_policy(payload.get("PolicyId"))
    if err:
        return err
    targets = []
    for target_id in _attachments.get(policy["Id"]) or []:
        record, target_type = _target(target_id)
        if record is None:
            continue
        targets.append({
            "TargetId": target_id,
            "Arn": record.get("Arn", ""),
            "Name": record.get("Name", ""),
            "Type": target_type,
        })
    return _json(200, {"Targets": targets})


def _set_policy_type(payload, enable):
    _ensure_org()
    root_id = payload.get("RootId")
    policy_type = payload.get("PolicyType")
    if not root_id or not policy_type:
        return error_response_json("InvalidInputException",
                                   "RootId and PolicyType are required", 400)
    root = _roots.get(root_id)
    if root is None:
        return error_response_json("RootNotFoundException",
                                   f"We can't find a root with the RootId {root_id}", 400)
    if policy_type not in _POLICY_TYPES:
        return error_response_json("InvalidInputException",
                                   f"Type {policy_type} is not a supported policy type", 400)
    entries = [dict(e) for e in root.get("PolicyTypes") or []]
    present = next((e for e in entries if e.get("Type") == policy_type), None)
    if enable:
        if present and present.get("Status") == "ENABLED":
            return error_response_json(
                "PolicyTypeAlreadyEnabledException",
                f"The policy type {policy_type} is already enabled", 400)
        if present:
            present["Status"] = "ENABLED"
        else:
            entries.append({"Type": policy_type, "Status": "ENABLED"})
    else:
        if not present or present.get("Status") != "ENABLED":
            return error_response_json(
                "PolicyTypeNotEnabledException",
                f"The policy type {policy_type} is not enabled", 400)
        entries = [e for e in entries if e.get("Type") != policy_type]
    updated = dict(root)
    updated["PolicyTypes"] = entries
    _roots[root_id] = updated
    return _json(200, {"Root": dict(updated)})


def _enable_policy_type(payload):
    return _set_policy_type(payload, True)


def _disable_policy_type(payload):
    return _set_policy_type(payload, False)


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
    "CreateAccount": _create_account,
    "DescribeCreateAccountStatus": _describe_create_account_status,
    "MoveAccount": _move_account,
    "CloseAccount": _close_account,
    "CreatePolicy": _create_policy,
    "DescribePolicy": _describe_policy,
    "UpdatePolicy": _update_policy,
    "DeletePolicy": _delete_policy,
    "ListPolicies": _list_policies,
    "AttachPolicy": _attach_policy,
    "DetachPolicy": _detach_policy,
    "ListPoliciesForTarget": _list_policies_for_target,
    "ListTargetsForPolicy": _list_targets_for_policy,
    "EnablePolicyType": _enable_policy_type,
    "DisablePolicyType": _disable_policy_type,
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
