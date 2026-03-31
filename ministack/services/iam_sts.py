"""
IAM & STS Service Emulators (AWS-compatible).

IAM actions:
  CreateUser, GetUser, ListUsers, DeleteUser,
  CreateRole, GetRole, ListRoles, DeleteRole,
  CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicyVersions, ListPolicies, DeletePolicy,
  CreatePolicyVersion, DeletePolicyVersion,
  AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies,
  PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies,
  AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies,
  PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies,
  CreateAccessKey, ListAccessKeys, DeleteAccessKey,
  CreateInstanceProfile, DeleteInstanceProfile, GetInstanceProfile,
  AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile,
  ListInstanceProfiles, ListInstanceProfilesForRole,
  UpdateAssumeRolePolicy,
  CreateGroup, GetGroup, DeleteGroup, ListGroups,
  AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser,
  CreateServiceLinkedRole,
  CreateOpenIDConnectProvider, GetOpenIDConnectProvider, DeleteOpenIDConnectProvider,
  TagRole, UntagRole, ListRoleTags,
  TagUser, UntagUser, ListUserTags,
  TagPolicy, UntagPolicy, ListPolicyTags,
  SimulatePrincipalPolicy, SimulateCustomPolicy.

STS actions:
  GetCallerIdentity, AssumeRole, GetSessionToken.
"""

import json
import logging
import time
from urllib.parse import parse_qs
from urllib.parse import quote as _url_quote

from ministack.core.responses import new_uuid

logger = logging.getLogger("iam")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_users: dict = {}
_roles: dict = {}
_policies: dict = {}
_access_keys: dict = {}
_instance_profiles: dict = {}
_groups: dict = {}
_user_inline_policies: dict = {}
_oidc_providers: dict = {}


# ===================================================================== IAM
# =====================================================================

async def handle_iam_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
            params[k] = v

    action = _p(params, "Action")
    handler = _IAM_HANDLERS.get(action)
    if not handler:
        return _error(400, "InvalidAction", f"Unknown IAM action: {action}", ns="iam")
    return handler(params)


# -------------------- User management --------------------

def _create_user(p):
    name = _p(p, "UserName")
    if name in _users:
        return _error(409, "EntityAlreadyExists",
                      f"User with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _users[name] = {
        "UserName": name,
        "Arn": f"arn:aws:iam::{ACCOUNT_ID}:user{path}{name}" if path != "/" else f"arn:aws:iam::{ACCOUNT_ID}:user/{name}",
        "UserId": _gen_id("AIDA"),
        "CreateDate": _now(),
        "Path": path,
        "AttachedPolicies": [],
        "Tags": [],
    }
    return _xml(200, "CreateUserResponse",
                f"<CreateUserResult><User>{_user_xml(name)}</User></CreateUserResult>",
                ns="iam")


def _get_user(p):
    name = _p(p, "UserName")
    if not name:
        return _xml(200, "GetUserResponse",
                    "<GetUserResult><User>"
                    f"<UserName>root</UserName>"
                    f"<UserId>{ACCOUNT_ID}</UserId>"
                    f"<Arn>arn:aws:iam::{ACCOUNT_ID}:root</Arn>"
                    "<Path>/</Path>"
                    f"<CreateDate>{_now()}</CreateDate>"
                    "</User></GetUserResult>",
                    ns="iam")
    if name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {name} cannot be found.", ns="iam")
    return _xml(200, "GetUserResponse",
                f"<GetUserResult><User>{_user_xml(name)}</User></GetUserResult>",
                ns="iam")


def _list_users(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_user_xml(n)}</member>"
        for n, u in _users.items()
        if u.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListUsersResponse",
                f"<ListUsersResult><Users>{members}</Users>"
                "<IsTruncated>false</IsTruncated></ListUsersResult>",
                ns="iam")


def _delete_user(p):
    name = _p(p, "UserName")
    user = _users.get(name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {name} cannot be found.", ns="iam")
    if user.get("AttachedPolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must detach all policies first.", ns="iam")
    user_keys = [k for k, v in _access_keys.items() if v["UserName"] == name]
    if user_keys:
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must delete access keys first.", ns="iam")
    _users.pop(name, None)
    return _xml(200, "DeleteUserResponse", "", ns="iam")


# -------------------- Role management --------------------

def _create_role(p):
    name = _p(p, "RoleName")
    if name in _roles:
        return _error(409, "EntityAlreadyExists",
                      f"Role with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _roles[name] = {
        "RoleName": name,
        "Arn": f"arn:aws:iam::{ACCOUNT_ID}:role{path}{name}" if path != "/" else f"arn:aws:iam::{ACCOUNT_ID}:role/{name}",
        "RoleId": _gen_id("AROA"),
        "CreateDate": _now(),
        "Path": path,
        "AssumeRolePolicyDocument": _p(p, "AssumeRolePolicyDocument"),
        "Description": _p(p, "Description"),
        "MaxSessionDuration": int(_p(p, "MaxSessionDuration") or 3600),
        "AttachedPolicies": [],
        "InlinePolicies": {},
        "Tags": [],
    }
    return _xml(200, "CreateRoleResponse",
                f"<CreateRoleResult><Role>{_role_xml(name)}</Role></CreateRoleResult>",
                ns="iam")


def _get_role(p):
    name = _p(p, "RoleName")
    if name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    return _xml(200, "GetRoleResponse",
                f"<GetRoleResult><Role>{_role_xml(name)}</Role></GetRoleResult>",
                ns="iam")


def _list_roles(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_role_xml(n)}</member>"
        for n, r in _roles.items()
        if r.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListRolesResponse",
                f"<ListRolesResult><Roles>{members}</Roles>"
                "<IsTruncated>false</IsTruncated></ListRolesResult>",
                ns="iam")


def _delete_role(p):
    name = _p(p, "RoleName")
    role = _roles.get(name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    if role.get("AttachedPolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must detach all policies first.", ns="iam")
    if role.get("InlinePolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must delete all inline policies first.", ns="iam")
    for ip in _instance_profiles.values():
        if name in ip.get("Roles", []):
            return _error(409, "DeleteConflict",
                          "Cannot delete entity, must remove role from all instance profiles first.", ns="iam")
    _roles.pop(name, None)
    return _xml(200, "DeleteRoleResponse", "", ns="iam")


def _update_role(p):
    name = _p(p, "RoleName")
    role = _roles.get(name)
    if not role:
        return _error(404, "NoSuchEntity", f"Role {name} not found.", ns="iam")
    if "Description" in p:
        role["Description"] = _p(p, "Description")
    if "MaxSessionDuration" in p:
        role["MaxSessionDuration"] = int(_p(p, "MaxSessionDuration", "3600"))
    return _xml(200, "UpdateRoleResponse", "<UpdateRoleResult></UpdateRoleResult>", ns="iam")


def _update_assume_role_policy(p):
    name = _p(p, "RoleName")
    if name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    _roles[name]["AssumeRolePolicyDocument"] = _p(p, "PolicyDocument")
    return _xml(200, "UpdateAssumeRolePolicyResponse", "", ns="iam")


# -------------------- Managed policy management --------------------

def _create_policy(p):
    name = _p(p, "PolicyName")
    path = _p(p, "Path") or "/"
    arn = f"arn:aws:iam::{ACCOUNT_ID}:policy{path}{name}" if path != "/" else f"arn:aws:iam::{ACCOUNT_ID}:policy/{name}"
    if arn in _policies:
        return _error(409, "EntityAlreadyExists",
                      f"A policy called {name} already exists.", ns="iam")
    doc = _p(p, "PolicyDocument")
    policy_id = _gen_id("ANPA")
    version_id = "v1"
    _policies[arn] = {
        "PolicyName": name,
        "Arn": arn,
        "PolicyId": policy_id,
        "CreateDate": _now(),
        "UpdateDate": _now(),
        "DefaultVersionId": version_id,
        "AttachmentCount": 0,
        "IsAttachable": True,
        "Path": path,
        "Tags": [],
        "Versions": {
            version_id: {
                "Document": doc,
                "VersionId": version_id,
                "IsDefaultVersion": True,
                "CreateDate": _now(),
            }
        },
    }
    return _xml(200, "CreatePolicyResponse",
                f"<CreatePolicyResult><Policy>{_managed_policy_xml(arn)}</Policy></CreatePolicyResult>",
                ns="iam")


def _get_policy(p):
    arn = _p(p, "PolicyArn")
    if arn not in _policies:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    return _xml(200, "GetPolicyResponse",
                f"<GetPolicyResult><Policy>{_managed_policy_xml(arn)}</Policy></GetPolicyResult>",
                ns="iam")


def _get_policy_version(p):
    arn = _p(p, "PolicyArn")
    vid = _p(p, "VersionId")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    ver = pol["Versions"].get(vid)
    if not ver:
        return _error(404, "NoSuchEntity",
                      f"Policy version {vid} not found.", ns="iam")
    doc = _url_quote(ver.get("Document") or "{}", safe="")
    is_default = "true" if ver.get("IsDefaultVersion") else "false"
    return _xml(200, "GetPolicyVersionResponse",
                f"<GetPolicyVersionResult><PolicyVersion>"
                f"<Document>{doc}</Document>"
                f"<VersionId>{vid}</VersionId>"
                f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                f"<CreateDate>{ver['CreateDate']}</CreateDate>"
                f"</PolicyVersion></GetPolicyVersionResult>",
                ns="iam")


def _list_policy_versions(p):
    arn = _p(p, "PolicyArn")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    members = ""
    for vid, ver in pol["Versions"].items():
        is_default = "true" if ver.get("IsDefaultVersion") else "false"
        members += (f"<member><VersionId>{vid}</VersionId>"
                    f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                    f"<CreateDate>{ver['CreateDate']}</CreateDate></member>")
    return _xml(200, "ListPolicyVersionsResponse",
                f"<ListPolicyVersionsResult><Versions>{members}</Versions>"
                "<IsTruncated>false</IsTruncated></ListPolicyVersionsResult>",
                ns="iam")


def _create_policy_version(p):
    arn = _p(p, "PolicyArn")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    if len(pol["Versions"]) >= 5:
        return _error(409, "LimitExceeded",
                      "A managed policy can have at most 5 versions.", ns="iam")
    doc = _p(p, "PolicyDocument")
    set_default = _p(p, "SetAsDefault").lower() in ("true", "1") if _p(p, "SetAsDefault") else False
    next_v = max((int(v.lstrip("v")) for v in pol["Versions"]), default=0) + 1
    vid = f"v{next_v}"
    pol["Versions"][vid] = {
        "Document": doc,
        "VersionId": vid,
        "IsDefaultVersion": set_default,
        "CreateDate": _now(),
    }
    if set_default:
        for v in pol["Versions"].values():
            v["IsDefaultVersion"] = (v["VersionId"] == vid)
        pol["DefaultVersionId"] = vid
    pol["UpdateDate"] = _now()
    is_default = "true" if set_default else "false"
    return _xml(200, "CreatePolicyVersionResponse",
                f"<CreatePolicyVersionResult><PolicyVersion>"
                f"<VersionId>{vid}</VersionId>"
                f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                f"<CreateDate>{pol['Versions'][vid]['CreateDate']}</CreateDate>"
                f"</PolicyVersion></CreatePolicyVersionResult>",
                ns="iam")


def _delete_policy_version(p):
    arn = _p(p, "PolicyArn")
    vid = _p(p, "VersionId")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    ver = pol["Versions"].get(vid)
    if not ver:
        return _error(404, "NoSuchEntity",
                      f"Policy version {vid} not found.", ns="iam")
    if ver.get("IsDefaultVersion"):
        return _error(409, "DeleteConflict",
                      "Cannot delete the default version of a policy.", ns="iam")
    del pol["Versions"][vid]
    return _xml(200, "DeletePolicyVersionResponse", "", ns="iam")


def _list_policies(p):
    scope = _p(p, "Scope") or "All"
    prefix = _p(p, "PathPrefix") or "/"
    members = ""
    for arn, pol in _policies.items():
        if not pol.get("Path", "/").startswith(prefix):
            continue
        if scope == "Local" and arn.startswith("arn:aws:iam::aws:"):
            continue
        members += f"<member>{_managed_policy_xml(arn)}</member>"
    return _xml(200, "ListPoliciesResponse",
                f"<ListPoliciesResult><Policies>{members}</Policies>"
                "<IsTruncated>false</IsTruncated></ListPoliciesResult>",
                ns="iam")


def _delete_policy(p):
    arn = _p(p, "PolicyArn")
    if arn not in _policies:
        return _error(404, "NoSuchEntity", f"Policy {arn} not found.", ns="iam")
    pol = _policies[arn]
    if pol.get("AttachmentCount", 0) > 0:
        return _error(409, "DeleteConflict",
                      "Cannot delete a policy attached to entities.", ns="iam")
    del _policies[arn]
    return _xml(200, "DeletePolicyResponse", "", ns="iam")


# -------------------- Attached role policies --------------------

def _attach_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_arn = _p(p, "PolicyArn")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_arn not in role["AttachedPolicies"]:
        role["AttachedPolicies"].append(policy_arn)
        pol = _policies.get(policy_arn)
        if pol:
            pol["AttachmentCount"] = pol.get("AttachmentCount", 0) + 1
    return _xml(200, "AttachRolePolicyResponse", "", ns="iam")


def _detach_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_arn = _p(p, "PolicyArn")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_arn not in role["AttachedPolicies"]:
        return _error(404, "NoSuchEntity",
                      f"Policy {policy_arn} is not attached to role {role_name}.", ns="iam")
    role["AttachedPolicies"].remove(policy_arn)
    pol = _policies.get(policy_arn)
    if pol:
        pol["AttachmentCount"] = max(pol.get("AttachmentCount", 1) - 1, 0)
    return _xml(200, "DetachRolePolicyResponse", "", ns="iam")


def _list_attached_role_policies(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = ""
    for arn in role["AttachedPolicies"]:
        pol = _policies.get(arn)
        pname = pol["PolicyName"] if pol else arn.rsplit("/", 1)[-1]
        members += (f"<member><PolicyName>{pname}</PolicyName>"
                    f"<PolicyArn>{arn}</PolicyArn></member>")
    return _xml(200, "ListAttachedRolePoliciesResponse",
                f"<ListAttachedRolePoliciesResult><AttachedPolicies>{members}</AttachedPolicies>"
                "<IsTruncated>false</IsTruncated></ListAttachedRolePoliciesResult>",
                ns="iam")


# -------------------- Inline role policies --------------------

def _put_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    policy_doc = _p(p, "PolicyDocument")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    role["InlinePolicies"][policy_name] = policy_doc
    return _xml(200, "PutRolePolicyResponse", "", ns="iam")


def _get_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    doc = role["InlinePolicies"].get(policy_name)
    if doc is None:
        return _error(404, "NoSuchEntity",
                      f"The role policy with name {policy_name} cannot be found.", ns="iam")
    if isinstance(doc, (dict, list)):
        doc_str = json.dumps(doc)
    elif isinstance(doc, (bytes, bytearray)):
        doc_str = doc.decode("utf-8")
    else:
        doc_str = doc
    encoded_doc = _url_quote(doc_str, safe="")
    return _xml(200, "GetRolePolicyResponse",
                f"<GetRolePolicyResult>"
                f"<RoleName>{role_name}</RoleName>"
                f"<PolicyName>{policy_name}</PolicyName>"
                f"<PolicyDocument>{encoded_doc}</PolicyDocument>"
                f"</GetRolePolicyResult>",
                ns="iam")


def _delete_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_name not in role["InlinePolicies"]:
        return _error(404, "NoSuchEntity",
                      f"The role policy with name {policy_name} cannot be found.", ns="iam")
    del role["InlinePolicies"][policy_name]
    return _xml(200, "DeleteRolePolicyResponse", "", ns="iam")


def _list_role_policies(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member>{name}</member>"
        for name in role["InlinePolicies"]
    )
    return _xml(200, "ListRolePoliciesResponse",
                f"<ListRolePoliciesResult><PolicyNames>{members}</PolicyNames>"
                "<IsTruncated>false</IsTruncated></ListRolePoliciesResult>",
                ns="iam")


# -------------------- Attached user policies --------------------

def _attach_user_policy(p):
    user_name = _p(p, "UserName")
    policy_arn = _p(p, "PolicyArn")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if policy_arn not in user["AttachedPolicies"]:
        user["AttachedPolicies"].append(policy_arn)
        pol = _policies.get(policy_arn)
        if pol:
            pol["AttachmentCount"] = pol.get("AttachmentCount", 0) + 1
    return _xml(200, "AttachUserPolicyResponse", "", ns="iam")


def _detach_user_policy(p):
    user_name = _p(p, "UserName")
    policy_arn = _p(p, "PolicyArn")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if policy_arn not in user["AttachedPolicies"]:
        return _error(404, "NoSuchEntity",
                      f"Policy {policy_arn} is not attached to user {user_name}.", ns="iam")
    user["AttachedPolicies"].remove(policy_arn)
    pol = _policies.get(policy_arn)
    if pol:
        pol["AttachmentCount"] = max(pol.get("AttachmentCount", 1) - 1, 0)
    return _xml(200, "DetachUserPolicyResponse", "", ns="iam")


def _list_attached_user_policies(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = ""
    for arn in user["AttachedPolicies"]:
        pol = _policies.get(arn)
        pname = pol["PolicyName"] if pol else arn.rsplit("/", 1)[-1]
        members += (f"<member><PolicyName>{pname}</PolicyName>"
                    f"<PolicyArn>{arn}</PolicyArn></member>")
    return _xml(200, "ListAttachedUserPoliciesResponse",
                f"<ListAttachedUserPoliciesResult><AttachedPolicies>{members}</AttachedPolicies>"
                "<IsTruncated>false</IsTruncated></ListAttachedUserPoliciesResult>",
                ns="iam")


# -------------------- Access keys --------------------

def _create_access_key(p):
    user_name = _p(p, "UserName")
    if not user_name:
        user_name = "default"
    if user_name != "default" and user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    key_id = _gen_access_key_id()
    secret = new_uuid().replace("-", "") + new_uuid().replace("-", "")[:8]
    _access_keys[key_id] = {
        "UserName": user_name,
        "AccessKeyId": key_id,
        "SecretAccessKey": secret,
        "Status": "Active",
        "CreateDate": _now(),
    }
    return _xml(200, "CreateAccessKeyResponse",
                f"<CreateAccessKeyResult><AccessKey>"
                f"<UserName>{user_name}</UserName>"
                f"<AccessKeyId>{key_id}</AccessKeyId>"
                f"<SecretAccessKey>{secret}</SecretAccessKey>"
                f"<Status>Active</Status>"
                f"<CreateDate>{_access_keys[key_id]['CreateDate']}</CreateDate>"
                f"</AccessKey></CreateAccessKeyResult>",
                ns="iam")


def _list_access_keys(p):
    user_name = _p(p, "UserName") or "default"
    members = ""
    for kid, v in _access_keys.items():
        if v["UserName"] == user_name:
            members += (f"<member><AccessKeyId>{kid}</AccessKeyId>"
                        f"<Status>{v['Status']}</Status>"
                        f"<UserName>{user_name}</UserName>"
                        f"<CreateDate>{v['CreateDate']}</CreateDate>"
                        f"</member>")
    return _xml(200, "ListAccessKeysResponse",
                f"<ListAccessKeysResult><AccessKeyMetadata>{members}</AccessKeyMetadata>"
                "<IsTruncated>false</IsTruncated></ListAccessKeysResult>",
                ns="iam")


def _delete_access_key(p):
    key_id = _p(p, "AccessKeyId")
    if key_id not in _access_keys:
        return _error(404, "NoSuchEntity",
                      f"The Access Key with id {key_id} cannot be found.", ns="iam")
    del _access_keys[key_id]
    return _xml(200, "DeleteAccessKeyResponse", "", ns="iam")


# -------------------- Instance profiles --------------------

def _create_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name in _instance_profiles:
        return _error(409, "EntityAlreadyExists",
                      f"Instance profile {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    ip_id = _gen_id("AIPA")
    arn = (f"arn:aws:iam::{ACCOUNT_ID}:instance-profile{path}{name}"
           if path != "/" else
           f"arn:aws:iam::{ACCOUNT_ID}:instance-profile/{name}")
    _instance_profiles[name] = {
        "InstanceProfileName": name,
        "InstanceProfileId": ip_id,
        "Arn": arn,
        "Path": path,
        "CreateDate": _now(),
        "Roles": [],
    }
    return _xml(200, "CreateInstanceProfileResponse",
                f"<CreateInstanceProfileResult>"
                f"<InstanceProfile>{_instance_profile_xml(name)}</InstanceProfile>"
                f"</CreateInstanceProfileResult>",
                ns="iam")


def _delete_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name not in _instance_profiles:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {name} not found.", ns="iam")
    ip = _instance_profiles[name]
    if ip["Roles"]:
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must remove all roles first.", ns="iam")
    del _instance_profiles[name]
    return _xml(200, "DeleteInstanceProfileResponse", "", ns="iam")


def _get_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name not in _instance_profiles:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {name} not found.", ns="iam")
    return _xml(200, "GetInstanceProfileResponse",
                f"<GetInstanceProfileResult>"
                f"<InstanceProfile>{_instance_profile_xml(name)}</InstanceProfile>"
                f"</GetInstanceProfileResult>",
                ns="iam")


def _add_role_to_instance_profile(p):
    ip_name = _p(p, "InstanceProfileName")
    role_name = _p(p, "RoleName")
    ip = _instance_profiles.get(ip_name)
    if not ip:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {ip_name} not found.", ns="iam")
    if role_name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if role_name in ip["Roles"]:
        return _error(409, "LimitExceeded",
                      f"Role {role_name} is already associated with instance profile {ip_name}.", ns="iam")
    if len(ip["Roles"]) >= 1:
        return _error(409, "LimitExceeded",
                      "An instance profile can have only one role.", ns="iam")
    ip["Roles"].append(role_name)
    return _xml(200, "AddRoleToInstanceProfileResponse", "", ns="iam")


def _remove_role_from_instance_profile(p):
    ip_name = _p(p, "InstanceProfileName")
    role_name = _p(p, "RoleName")
    ip = _instance_profiles.get(ip_name)
    if not ip:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {ip_name} not found.", ns="iam")
    if role_name not in ip["Roles"]:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} is not associated with instance profile {ip_name}.", ns="iam")
    ip["Roles"].remove(role_name)
    return _xml(200, "RemoveRoleFromInstanceProfileResponse", "", ns="iam")


def _list_instance_profiles(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_instance_profile_xml(name)}</member>"
        for name, ip in _instance_profiles.items()
        if ip["Path"].startswith(prefix)
    )
    return _xml(200, "ListInstanceProfilesResponse",
                f"<ListInstanceProfilesResult><InstanceProfiles>{members}</InstanceProfiles>"
                "<IsTruncated>false</IsTruncated></ListInstanceProfilesResult>",
                ns="iam")


def _list_instance_profiles_for_role(p):
    role_name = _p(p, "RoleName")
    if role_name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member>{_instance_profile_xml(name)}</member>"
        for name, ip in _instance_profiles.items()
        if role_name in ip["Roles"]
    )
    return _xml(200, "ListInstanceProfilesForRoleResponse",
                f"<ListInstanceProfilesForRoleResult><InstanceProfiles>{members}</InstanceProfiles>"
                "<IsTruncated>false</IsTruncated></ListInstanceProfilesForRoleResult>",
                ns="iam")


# -------------------- Tags: roles --------------------

def _tag_role(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in role["Tags"]}
    for t in new_tags:
        existing[t["Key"]] = t
    role["Tags"] = list(existing.values())
    return _xml(200, "TagRoleResponse", "", ns="iam")


def _untag_role(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    role["Tags"] = [t for t in role["Tags"] if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagRoleResponse", "", ns="iam")


def _list_role_tags(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in role["Tags"]
    )
    return _xml(200, "ListRoleTagsResponse",
                f"<ListRoleTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListRoleTagsResult>",
                ns="iam")


# -------------------- Tags: users --------------------

def _tag_user(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in user["Tags"]}
    for t in new_tags:
        existing[t["Key"]] = t
    user["Tags"] = list(existing.values())
    return _xml(200, "TagUserResponse", "", ns="iam")


def _untag_user(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    user["Tags"] = [t for t in user["Tags"] if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagUserResponse", "", ns="iam")


def _list_user_tags(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in user["Tags"]
    )
    return _xml(200, "ListUserTagsResponse",
                f"<ListUserTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListUserTagsResult>",
                ns="iam")


# -------------------- Simulate (stubs) --------------------

def _simulate_principal_policy(p):
    results = _build_simulate_results(p)
    return _xml(200, "SimulatePrincipalPolicyResponse",
                f"<SimulatePrincipalPolicyResult>"
                f"<EvaluationResults>{results}</EvaluationResults>"
                "<IsTruncated>false</IsTruncated>"
                f"</SimulatePrincipalPolicyResult>",
                ns="iam")


def _simulate_custom_policy(p):
    results = _build_simulate_results(p)
    return _xml(200, "SimulateCustomPolicyResponse",
                f"<SimulateCustomPolicyResult>"
                f"<EvaluationResults>{results}</EvaluationResults>"
                "<IsTruncated>false</IsTruncated>"
                f"</SimulateCustomPolicyResult>",
                ns="iam")


def _build_simulate_results(p):
    actions = []
    idx = 1
    while True:
        a = _p(p, f"ActionNames.member.{idx}")
        if not a:
            break
        actions.append(a)
        idx += 1
    if not actions:
        actions = ["sts:AssumeRole"]
    resource_arn = _p(p, "ResourceArns.member.1") or "*"
    members = ""
    for action in actions:
        members += (f"<member>"
                    f"<EvalActionName>{action}</EvalActionName>"
                    f"<EvalResourceName>{resource_arn}</EvalResourceName>"
                    f"<EvalDecision>allowed</EvalDecision>"
                    f"<MatchedStatements></MatchedStatements>"
                    f"<MissingContextValues></MissingContextValues>"
                    f"</member>")
    return members


# -------------------- Group management --------------------

def _create_group(p):
    name = _p(p, "GroupName")
    if name in _groups:
        return _error(409, "EntityAlreadyExists",
                      f"Group with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _groups[name] = {
        "GroupName": name,
        "GroupId": _gen_id("AGPA"),
        "Arn": f"arn:aws:iam::{ACCOUNT_ID}:group{path}{name}" if path != "/" else f"arn:aws:iam::{ACCOUNT_ID}:group/{name}",
        "Path": path,
        "CreateDate": _now(),
        "Users": [],
    }
    return _xml(200, "CreateGroupResponse",
                f"<CreateGroupResult><Group>{_group_xml(name)}</Group></CreateGroupResult>",
                ns="iam")


def _get_group(p):
    name = _p(p, "GroupName")
    if name not in _groups:
        return _error(404, "NoSuchEntity",
                      f"The group with name {name} cannot be found.", ns="iam")
    g = _groups[name]
    user_members = ""
    for uname in g["Users"]:
        if uname in _users:
            user_members += f"<member>{_user_xml(uname)}</member>"
    return _xml(200, "GetGroupResponse",
                f"<GetGroupResult>"
                f"<Group>{_group_xml(name)}</Group>"
                f"<Users>{user_members}</Users>"
                f"<IsTruncated>false</IsTruncated>"
                f"</GetGroupResult>",
                ns="iam")


def _delete_group(p):
    name = _p(p, "GroupName")
    if name not in _groups:
        return _error(404, "NoSuchEntity",
                      f"The group with name {name} cannot be found.", ns="iam")
    _groups.pop(name, None)
    return _xml(200, "DeleteGroupResponse", "", ns="iam")


def _list_groups(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_group_xml(n)}</member>"
        for n, g in _groups.items()
        if g.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListGroupsResponse",
                f"<ListGroupsResult><Groups>{members}</Groups>"
                "<IsTruncated>false</IsTruncated></ListGroupsResult>",
                ns="iam")


def _add_user_to_group(p):
    group_name = _p(p, "GroupName")
    user_name = _p(p, "UserName")
    g = _groups.get(group_name)
    if not g:
        return _error(404, "NoSuchEntity",
                      f"The group with name {group_name} cannot be found.", ns="iam")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if user_name not in g["Users"]:
        g["Users"].append(user_name)
    return _xml(200, "AddUserToGroupResponse", "", ns="iam")


def _remove_user_from_group(p):
    group_name = _p(p, "GroupName")
    user_name = _p(p, "UserName")
    g = _groups.get(group_name)
    if not g:
        return _error(404, "NoSuchEntity",
                      f"The group with name {group_name} cannot be found.", ns="iam")
    if user_name not in g["Users"]:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} is not in group {group_name}.", ns="iam")
    g["Users"].remove(user_name)
    return _xml(200, "RemoveUserFromGroupResponse", "", ns="iam")


def _list_groups_for_user(p):
    user_name = _p(p, "UserName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member>{_group_xml(n)}</member>"
        for n, g in _groups.items()
        if user_name in g["Users"]
    )
    return _xml(200, "ListGroupsForUserResponse",
                f"<ListGroupsForUserResult><Groups>{members}</Groups>"
                "<IsTruncated>false</IsTruncated></ListGroupsForUserResult>",
                ns="iam")


# -------------------- Inline user policies --------------------

def _put_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    policy_doc = _p(p, "PolicyDocument")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    _user_inline_policies[(user_name, policy_name)] = policy_doc
    return _xml(200, "PutUserPolicyResponse", "", ns="iam")


def _get_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    doc = _user_inline_policies.get((user_name, policy_name))
    if doc is None:
        return _error(404, "NoSuchEntity",
                      f"The user policy with name {policy_name} cannot be found.", ns="iam")
    if isinstance(doc, (dict, list)):
        doc_str = json.dumps(doc)
    elif isinstance(doc, (bytes, bytearray)):
        doc_str = doc.decode("utf-8")
    else:
        doc_str = doc
    encoded_doc = _url_quote(doc_str, safe="")
    return _xml(200, "GetUserPolicyResponse",
                f"<GetUserPolicyResult>"
                f"<UserName>{user_name}</UserName>"
                f"<PolicyName>{policy_name}</PolicyName>"
                f"<PolicyDocument>{encoded_doc}</PolicyDocument>"
                f"</GetUserPolicyResult>",
                ns="iam")


def _delete_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if (user_name, policy_name) not in _user_inline_policies:
        return _error(404, "NoSuchEntity",
                      f"The user policy with name {policy_name} cannot be found.", ns="iam")
    del _user_inline_policies[(user_name, policy_name)]
    return _xml(200, "DeleteUserPolicyResponse", "", ns="iam")


def _list_user_policies(p):
    user_name = _p(p, "UserName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member>{pname}</member>"
        for (uname, pname) in _user_inline_policies
        if uname == user_name
    )
    return _xml(200, "ListUserPoliciesResponse",
                f"<ListUserPoliciesResult><PolicyNames>{members}</PolicyNames>"
                "<IsTruncated>false</IsTruncated></ListUserPoliciesResult>",
                ns="iam")


# -------------------- Service-linked roles --------------------

def _create_service_linked_role(p):
    service_name = _p(p, "AWSServiceName")
    suffix = service_name.split(".")[0] if "." in service_name else service_name
    role_name = f"AWSServiceRoleFor{suffix.capitalize()}"
    path = f"/aws-service-role/{service_name}/"

    if role_name in _roles:
        return _error(409, "EntityAlreadyExists",
                      f"Role with name {role_name} already exists.", ns="iam")

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": service_name},
            "Action": "sts:AssumeRole"
        }]
    })

    _roles[role_name] = {
        "RoleName": role_name,
        "Arn": f"arn:aws:iam::{ACCOUNT_ID}:role{path}{role_name}",
        "RoleId": _gen_id("AROA"),
        "CreateDate": _now(),
        "Path": path,
        "AssumeRolePolicyDocument": trust_policy,
        "Description": f"Service-linked role for {service_name}",
        "MaxSessionDuration": 3600,
        "AttachedPolicies": [],
        "InlinePolicies": {},
        "Tags": [],
    }
    return _xml(200, "CreateServiceLinkedRoleResponse",
                f"<CreateServiceLinkedRoleResult><Role>{_role_xml(role_name)}</Role></CreateServiceLinkedRoleResult>",
                ns="iam")


# -------------------- OIDC providers --------------------

def _create_oidc_provider(p):
    url = _p(p, "Url")
    client_ids = []
    idx = 1
    while True:
        cid = _p(p, f"ClientIDList.member.{idx}")
        if not cid:
            break
        client_ids.append(cid)
        idx += 1
    thumbprints = []
    idx = 1
    while True:
        tp = _p(p, f"ThumbprintList.member.{idx}")
        if not tp:
            break
        thumbprints.append(tp)
        idx += 1

    host = url.replace("https://", "").replace("http://", "").rstrip("/")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:oidc-provider/{host}"

    if arn in _oidc_providers:
        return _error(409, "EntityAlreadyExists",
                      f"OIDC provider with url {url} already exists.", ns="iam")

    tags = _extract_tags(p)
    _oidc_providers[arn] = {
        "Url": url,
        "ClientIDList": client_ids,
        "ThumbprintList": thumbprints,
        "Arn": arn,
        "CreateDate": _now(),
        "Tags": tags,
    }
    return _xml(200, "CreateOpenIDConnectProviderResponse",
                f"<CreateOpenIDConnectProviderResult>"
                f"<OpenIDConnectProviderArn>{arn}</OpenIDConnectProviderArn>"
                f"</CreateOpenIDConnectProviderResult>",
                ns="iam")


def _get_oidc_provider(p):
    arn = _p(p, "OpenIDConnectProviderArn")
    prov = _oidc_providers.get(arn)
    if not prov:
        return _error(404, "NoSuchEntity",
                      f"OIDC provider {arn} not found.", ns="iam")
    client_members = "".join(f"<member>{c}</member>" for c in prov["ClientIDList"])
    thumb_members = "".join(f"<member>{t}</member>" for t in prov["ThumbprintList"])
    tag_members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in prov.get("Tags", [])
    )
    return _xml(200, "GetOpenIDConnectProviderResponse",
                f"<GetOpenIDConnectProviderResult>"
                f"<Url>{prov['Url']}</Url>"
                f"<ClientIDList>{client_members}</ClientIDList>"
                f"<ThumbprintList>{thumb_members}</ThumbprintList>"
                f"<CreateDate>{prov['CreateDate']}</CreateDate>"
                f"<Tags>{tag_members}</Tags>"
                f"</GetOpenIDConnectProviderResult>",
                ns="iam")


def _delete_oidc_provider(p):
    arn = _p(p, "OpenIDConnectProviderArn")
    if arn not in _oidc_providers:
        return _error(404, "NoSuchEntity",
                      f"OIDC provider {arn} not found.", ns="iam")
    del _oidc_providers[arn]
    return _xml(200, "DeleteOpenIDConnectProviderResponse", "", ns="iam")


# -------------------- Tags: policies --------------------

def _tag_policy(p):
    arn = _p(p, "PolicyArn")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in pol.get("Tags", [])}
    for t in new_tags:
        existing[t["Key"]] = t
    pol["Tags"] = list(existing.values())
    return _xml(200, "TagPolicyResponse", "", ns="iam")


def _untag_policy(p):
    arn = _p(p, "PolicyArn")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    pol["Tags"] = [t for t in pol.get("Tags", []) if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagPolicyResponse", "", ns="iam")


def _list_policy_tags(p):
    arn = _p(p, "PolicyArn")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in pol.get("Tags", [])
    )
    return _xml(200, "ListPolicyTagsResponse",
                f"<ListPolicyTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListPolicyTagsResult>",
                ns="iam")


# ===================================================================== STS
# =====================================================================

async def handle_sts_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
            params[k] = v

    action = _p(params, "Action")

    if action == "GetCallerIdentity":
        return _xml(200, "GetCallerIdentityResponse",
                    f"<GetCallerIdentityResult>"
                    f"<Arn>arn:aws:iam::{ACCOUNT_ID}:root</Arn>"
                    f"<UserId>{ACCOUNT_ID}</UserId>"
                    f"<Account>{ACCOUNT_ID}</Account>"
                    f"</GetCallerIdentityResult>",
                    ns="sts")

    if action == "AssumeRole":
        role_arn = _p(params, "RoleArn")
        session_name = _p(params, "RoleSessionName")
        duration = int(_p(params, "DurationSeconds") or 3600)
        expiration = _future(duration)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
        assumed_arn = role_arn.replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session_name}"):
            assumed_arn = f"{assumed_arn}/{session_name}"
        return _xml(200, "AssumeRoleResponse",
                    f"<AssumeRoleResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{expiration}</Expiration>"
                    f"</Credentials>"
                    f"<AssumedRoleUser>"
                    f"<AssumedRoleId>{role_id}:{session_name}</AssumedRoleId>"
                    f"<Arn>{assumed_arn}</Arn>"
                    f"</AssumedRoleUser>"
                    f"<PackedPolicySize>0</PackedPolicySize>"
                    f"</AssumeRoleResult>",
                    ns="sts")

    if action == "AssumeRoleWithWebIdentity":
        role_arn = _p(params, "RoleArn")
        session = _p(params, "RoleSessionName", "session")
        duration = int(_p(params, "DurationSeconds") or 3600)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        assumed_arn = role_arn.replace(":role/", ":assumed-role/", 1)
        if not assumed_arn.endswith(f"/{session}"):
            assumed_arn = f"{assumed_arn}/{session}"
        role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
        return _xml(200, "AssumeRoleWithWebIdentityResponse",
                    f"<AssumeRoleWithWebIdentityResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{_future(duration)}</Expiration>"
                    f"</Credentials>"
                    f"<AssumedRoleUser>"
                    f"<AssumedRoleId>{role_id}:{session}</AssumedRoleId>"
                    f"<Arn>{assumed_arn}</Arn>"
                    f"</AssumedRoleUser>"
                    f"<SubjectFromWebIdentityToken>test-subject</SubjectFromWebIdentityToken>"
                    f"<Audience>sts.amazonaws.com</Audience>"
                    f"<Provider>accounts.google.com</Provider>"
                    f"</AssumeRoleWithWebIdentityResult>",
                    ns="sts")

    if action == "GetSessionToken":
        duration = int(_p(params, "DurationSeconds") or 43200)
        expiration = _future(duration)
        access_key = _gen_session_access_key()
        secret_key = _gen_secret()
        session_token = _gen_session_token()
        return _xml(200, "GetSessionTokenResponse",
                    f"<GetSessionTokenResult>"
                    f"<Credentials>"
                    f"<AccessKeyId>{access_key}</AccessKeyId>"
                    f"<SecretAccessKey>{secret_key}</SecretAccessKey>"
                    f"<SessionToken>{session_token}</SessionToken>"
                    f"<Expiration>{expiration}</Expiration>"
                    f"</Credentials>"
                    f"</GetSessionTokenResult>",
                    ns="sts")

    return _error(400, "InvalidAction", f"Unknown STS action: {action}", ns="sts")


# ===================================================================== Shared helpers
# =====================================================================

def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _future(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _gen_id(prefix="AIDA"):
    return prefix + new_uuid().replace("-", "")[:17].upper()


def _gen_access_key_id():
    return "AKIA" + new_uuid().replace("-", "")[:16].upper()


def _gen_session_access_key():
    return "ASIA" + new_uuid().replace("-", "")[:16].upper()


def _gen_secret():
    raw = new_uuid().replace("-", "") + new_uuid().replace("-", "")
    return raw[:40]


def _gen_session_token():
    parts = [new_uuid().replace("-", "") for _ in range(4)]
    return "FwoGZX" + "".join(parts)


def _extract_tags(p):
    tags = []
    idx = 1
    while True:
        key = _p(p, f"Tags.member.{idx}.Key")
        if not key:
            break
        value = _p(p, f"Tags.member.{idx}.Value")
        tags.append({"Key": key, "Value": value})
        idx += 1
    return tags


def _extract_tag_keys(p):
    keys = set()
    idx = 1
    while True:
        key = _p(p, f"TagKeys.member.{idx}")
        if not key:
            break
        keys.add(key)
        idx += 1
    return keys


# -------------------- XML builders --------------------

def _user_xml(name):
    u = _users[name]
    return (f"<UserName>{u['UserName']}</UserName>"
            f"<UserId>{u['UserId']}</UserId>"
            f"<Arn>{u['Arn']}</Arn>"
            f"<Path>{u['Path']}</Path>"
            f"<CreateDate>{u['CreateDate']}</CreateDate>")


def _role_xml(name):
    r = _roles[name]
    assume_doc = _url_quote(r.get("AssumeRolePolicyDocument") or "{}", safe="")
    desc = r.get("Description") or ""
    max_dur = r.get("MaxSessionDuration", 3600)
    tags_xml = ""
    if r.get("Tags"):
        tag_members = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in r["Tags"]
        )
        tags_xml = f"<Tags>{tag_members}</Tags>"
    return (f"<RoleName>{r['RoleName']}</RoleName>"
            f"<RoleId>{r['RoleId']}</RoleId>"
            f"<Arn>{r['Arn']}</Arn>"
            f"<Path>{r['Path']}</Path>"
            f"<CreateDate>{r['CreateDate']}</CreateDate>"
            f"<AssumeRolePolicyDocument>{assume_doc}</AssumeRolePolicyDocument>"
            f"<Description>{desc}</Description>"
            f"<MaxSessionDuration>{max_dur}</MaxSessionDuration>"
            f"{tags_xml}")


def _managed_policy_xml(arn):
    pol = _policies[arn]
    return (f"<PolicyName>{pol['PolicyName']}</PolicyName>"
            f"<Arn>{arn}</Arn>"
            f"<PolicyId>{pol['PolicyId']}</PolicyId>"
            f"<DefaultVersionId>{pol['DefaultVersionId']}</DefaultVersionId>"
            f"<AttachmentCount>{pol.get('AttachmentCount', 0)}</AttachmentCount>"
            f"<IsAttachable>true</IsAttachable>"
            f"<CreateDate>{pol['CreateDate']}</CreateDate>"
            f"<UpdateDate>{pol.get('UpdateDate', pol['CreateDate'])}</UpdateDate>"
            f"<Path>{pol.get('Path', '/')}</Path>")


def _group_xml(name):
    g = _groups[name]
    return (f"<GroupName>{g['GroupName']}</GroupName>"
            f"<GroupId>{g['GroupId']}</GroupId>"
            f"<Arn>{g['Arn']}</Arn>"
            f"<Path>{g['Path']}</Path>"
            f"<CreateDate>{g['CreateDate']}</CreateDate>")


def _instance_profile_xml(name):
    ip = _instance_profiles[name]
    roles_xml = ""
    for rname in ip["Roles"]:
        if rname in _roles:
            roles_xml += f"<member>{_role_xml(rname)}</member>"
    return (f"<InstanceProfileName>{ip['InstanceProfileName']}</InstanceProfileName>"
            f"<InstanceProfileId>{ip['InstanceProfileId']}</InstanceProfileId>"
            f"<Arn>{ip['Arn']}</Arn>"
            f"<Path>{ip['Path']}</Path>"
            f"<CreateDate>{ip['CreateDate']}</CreateDate>"
            f"<Roles>{roles_xml}</Roles>")


def _xml(status, root_tag, inner, ns="iam"):
    ns_url = {
        "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
        "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
    }.get(ns, "")
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="{ns_url}">'
            f'{inner}'
            f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(status, code, message, ns="iam"):
    ns_url = {
        "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
        "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
    }.get(ns, "")
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<ErrorResponse xmlns="{ns_url}">'
            f'<Error><Code>{code}</Code><Message>{message}</Message></Error>'
            f'<RequestId>{new_uuid()}</RequestId>'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


# -------------------- Handler dispatch table --------------------

_IAM_HANDLERS = {
    "CreateUser": _create_user,
    "GetUser": _get_user,
    "ListUsers": _list_users,
    "DeleteUser": _delete_user,
    "CreateRole": _create_role,
    "GetRole": _get_role,
    "ListRoles": _list_roles,
    "DeleteRole": _delete_role,
    "UpdateRole": _update_role,
    "CreatePolicy": _create_policy,
    "GetPolicy": _get_policy,
    "GetPolicyVersion": _get_policy_version,
    "ListPolicyVersions": _list_policy_versions,
    "CreatePolicyVersion": _create_policy_version,
    "DeletePolicyVersion": _delete_policy_version,
    "ListPolicies": _list_policies,
    "DeletePolicy": _delete_policy,
    "AttachRolePolicy": _attach_role_policy,
    "DetachRolePolicy": _detach_role_policy,
    "ListAttachedRolePolicies": _list_attached_role_policies,
    "PutRolePolicy": _put_role_policy,
    "GetRolePolicy": _get_role_policy,
    "DeleteRolePolicy": _delete_role_policy,
    "ListRolePolicies": _list_role_policies,
    "AttachUserPolicy": _attach_user_policy,
    "DetachUserPolicy": _detach_user_policy,
    "ListAttachedUserPolicies": _list_attached_user_policies,
    "CreateAccessKey": _create_access_key,
    "ListAccessKeys": _list_access_keys,
    "DeleteAccessKey": _delete_access_key,
    "CreateInstanceProfile": _create_instance_profile,
    "DeleteInstanceProfile": _delete_instance_profile,
    "GetInstanceProfile": _get_instance_profile,
    "AddRoleToInstanceProfile": _add_role_to_instance_profile,
    "RemoveRoleFromInstanceProfile": _remove_role_from_instance_profile,
    "ListInstanceProfiles": _list_instance_profiles,
    "ListInstanceProfilesForRole": _list_instance_profiles_for_role,
    "UpdateAssumeRolePolicy": _update_assume_role_policy,
    "TagRole": _tag_role,
    "UntagRole": _untag_role,
    "ListRoleTags": _list_role_tags,
    "TagUser": _tag_user,
    "UntagUser": _untag_user,
    "ListUserTags": _list_user_tags,
    "SimulatePrincipalPolicy": _simulate_principal_policy,
    "SimulateCustomPolicy": _simulate_custom_policy,
    "CreateGroup": _create_group,
    "GetGroup": _get_group,
    "DeleteGroup": _delete_group,
    "ListGroups": _list_groups,
    "AddUserToGroup": _add_user_to_group,
    "RemoveUserFromGroup": _remove_user_from_group,
    "ListGroupsForUser": _list_groups_for_user,
    "PutUserPolicy": _put_user_policy,
    "GetUserPolicy": _get_user_policy,
    "DeleteUserPolicy": _delete_user_policy,
    "ListUserPolicies": _list_user_policies,
    "CreateServiceLinkedRole": _create_service_linked_role,
    "CreateOpenIDConnectProvider": _create_oidc_provider,
    "GetOpenIDConnectProvider": _get_oidc_provider,
    "DeleteOpenIDConnectProvider": _delete_oidc_provider,
    "TagPolicy": _tag_policy,
    "UntagPolicy": _untag_policy,
    "ListPolicyTags": _list_policy_tags,
}


def reset():
    _users.clear()
    _roles.clear()
    _policies.clear()
    _access_keys.clear()
    _instance_profiles.clear()
    _groups.clear()
    _user_inline_policies.clear()
    _oidc_providers.clear()
