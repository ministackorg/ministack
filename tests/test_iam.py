import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_iam_role_user(iam):
    iam.create_role(
        RoleName="test-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    roles = iam.list_roles()
    assert any(r["RoleName"] == "test-role" for r in roles.get("Roles", []))
    iam.create_user(UserName="test-user")
    users = iam.list_users()
    assert any(u["UserName"] == "test-user" for u in users.get("Users", []))

def test_iam_create_user(iam):
    resp = iam.create_user(UserName="iam-test-user")
    user = resp["User"]
    assert user["UserName"] == "iam-test-user"
    assert "Arn" in user
    assert "UserId" in user

def test_iam_get_user(iam):
    resp = iam.get_user(UserName="iam-test-user")
    assert resp["User"]["UserName"] == "iam-test-user"

def test_iam_get_user_not_found(iam):
    with pytest.raises(ClientError) as exc:
        iam.get_user(UserName="ghost-user-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

def test_iam_list_users(iam):
    resp = iam.list_users()
    names = [u["UserName"] for u in resp["Users"]]
    assert "iam-test-user" in names

def test_iam_delete_user(iam):
    iam.create_user(UserName="iam-del-user")
    iam.delete_user(UserName="iam-del-user")
    with pytest.raises(ClientError) as exc:
        iam.get_user(UserName="iam-del-user")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

def test_iam_create_role(iam):
    assume = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(
        RoleName="iam-test-role",
        AssumeRolePolicyDocument=assume,
        Description="integration test role",
    )
    role = resp["Role"]
    assert role["RoleName"] == "iam-test-role"
    assert "Arn" in role
    assert "RoleId" in role

def test_iam_get_role(iam):
    resp = iam.get_role(RoleName="iam-test-role")
    assert resp["Role"]["RoleName"] == "iam-test-role"

def test_iam_list_roles(iam):
    resp = iam.list_roles()
    names = [r["RoleName"] for r in resp["Roles"]]
    assert "iam-test-role" in names

def test_iam_delete_role(iam):
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    iam.create_role(RoleName="iam-del-role", AssumeRolePolicyDocument=assume)
    iam.delete_role(RoleName="iam-del-role")
    with pytest.raises(ClientError) as exc:
        iam.get_role(RoleName="iam-del-role")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

def test_iam_create_policy(iam):
    policy_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                }
            ],
        }
    )
    resp = iam.create_policy(
        PolicyName="iam-test-policy",
        PolicyDocument=policy_doc,
    )
    pol = resp["Policy"]
    assert pol["PolicyName"] == "iam-test-policy"
    assert "Arn" in pol
    assert pol["DefaultVersionId"] == "v1"

def test_iam_get_policy(iam):
    arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    resp = iam.get_policy(PolicyArn=arn)
    assert resp["Policy"]["PolicyName"] == "iam-test-policy"


def test_iam_policy_description_roundtrip(iam):
    """Regression for #438: CreatePolicy(Description=...) must survive GetPolicy.
    Without this, Terraform force-replaces every aws_iam_policy with a description
    on every warm boot because `description` is ForceNew in the provider."""
    import uuid as _u
    name = f"desc-policy-{_u.uuid4().hex[:8]}"
    created = iam.create_policy(
        PolicyName=name,
        Description="managed by ministack regression test",
        PolicyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}',
    )
    assert created["Policy"].get("Description") == "managed by ministack regression test"
    fetched = iam.get_policy(PolicyArn=created["Policy"]["Arn"])
    assert fetched["Policy"].get("Description") == "managed by ministack regression test"
    iam.delete_policy(PolicyArn=created["Policy"]["Arn"])


def test_iam_user_tags_serialized_in_get_user(iam):
    """Regression for #441: GetUser must include Tags set via TagUser / CreateUser.
    _user_xml previously omitted <Tags>, so Terraform's refresh saw empty tags
    and re-added default_tags on every apply."""
    import uuid as _u
    name = f"tag-user-{_u.uuid4().hex[:8]}"
    iam.create_user(UserName=name, Tags=[{"Key": "Team", "Value": "core"}])
    resp = iam.get_user(UserName=name)
    tags = {t["Key"]: t["Value"] for t in resp["User"].get("Tags", [])}
    assert tags.get("Team") == "core"
    iam.tag_user(UserName=name, Tags=[{"Key": "Env", "Value": "dev"}])
    resp = iam.get_user(UserName=name)
    tags = {t["Key"]: t["Value"] for t in resp["User"].get("Tags", [])}
    assert tags == {"Team": "core", "Env": "dev"}
    iam.delete_user(UserName=name)

def test_iam_attach_role_policy(iam):
    policy_arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    iam.attach_role_policy(RoleName="iam-test-role", PolicyArn=policy_arn)

def test_iam_list_attached_role_policies(iam):
    resp = iam.list_attached_role_policies(RoleName="iam-test-role")
    arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
    assert "arn:aws:iam::000000000000:policy/iam-test-policy" in arns

def test_iam_detach_role_policy(iam):
    policy_arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    iam.detach_role_policy(RoleName="iam-test-role", PolicyArn=policy_arn)
    resp = iam.list_attached_role_policies(RoleName="iam-test-role")
    arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
    assert policy_arn not in arns

def test_iam_put_role_policy(iam):
    inline_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "logs:*",
                    "Resource": "*",
                }
            ],
        }
    )
    iam.put_role_policy(
        RoleName="iam-test-role",
        PolicyName="inline-logs",
        PolicyDocument=inline_doc,
    )

def test_iam_get_role_policy(iam):
    resp = iam.get_role_policy(RoleName="iam-test-role", PolicyName="inline-logs")
    assert resp["RoleName"] == "iam-test-role"
    assert resp["PolicyName"] == "inline-logs"
    doc = resp["PolicyDocument"]
    if isinstance(doc, str):
        doc = json.loads(doc)
    assert doc["Statement"][0]["Action"] == "logs:*"

def test_iam_list_role_policies(iam):
    resp = iam.list_role_policies(RoleName="iam-test-role")
    assert "inline-logs" in resp["PolicyNames"]

def test_iam_create_access_key(iam):
    username = f"create-key-user-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_user(UserName=username)
    try:
        resp = iam.create_access_key(UserName=username)
        key = resp["AccessKey"]
        assert key["UserName"] == username
        assert key["AccessKeyId"].startswith("AKIA")
        assert len(key["SecretAccessKey"]) > 0
        assert key["Status"] == "Active"
    finally:
        keys = iam.list_access_keys(UserName=username)["AccessKeyMetadata"]
        for k in keys:
            iam.delete_access_key(AccessKeyId=k["AccessKeyId"])
        iam.delete_user(UserName=username)


def test_iam_update_access_key(iam):
    username = f"upd-key-user-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_user(UserName=username)
    resp = iam.create_access_key(UserName=username)
    key_id = resp["AccessKey"]["AccessKeyId"]
    try:
        iam.update_access_key(UserName=username, AccessKeyId=key_id, Status="Inactive")
        keys = iam.list_access_keys(UserName=username)["AccessKeyMetadata"]
        found = next(k for k in keys if k["AccessKeyId"] == key_id)
        assert found["Status"] == "Inactive"

        iam.update_access_key(UserName=username, AccessKeyId=key_id, Status="Active")
        keys = iam.list_access_keys(UserName=username)["AccessKeyMetadata"]
        found = next(k for k in keys if k["AccessKeyId"] == key_id)
        assert found["Status"] == "Active"
    finally:
        iam.delete_access_key(AccessKeyId=key_id)
        iam.delete_user(UserName=username)


def test_iam_update_access_key_not_found(iam):
    with pytest.raises(ClientError) as exc:
        iam.update_access_key(AccessKeyId="AKIAIOSFODNN7EXAMPLE", Status="Inactive")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_update_access_key_invalid_status(iam):
    username = f"inv-status-user-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_user(UserName=username)
    resp = iam.create_access_key(UserName=username)
    key_id = resp["AccessKey"]["AccessKeyId"]
    try:
        with pytest.raises(ClientError) as exc:
            iam.update_access_key(AccessKeyId=key_id, Status="Disabled")
        assert exc.value.response["Error"]["Code"] == "InvalidInput"
    finally:
        iam.delete_access_key(AccessKeyId=key_id)
        iam.delete_user(UserName=username)


def test_iam_get_access_key_last_used(iam):
    username = f"key-last-used-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_user(UserName=username)
    resp = iam.create_access_key(UserName=username)
    key_id = resp["AccessKey"]["AccessKeyId"]
    try:
        result = iam.get_access_key_last_used(AccessKeyId=key_id)
        assert result["UserName"] == username
        last_used = result["AccessKeyLastUsed"]
        assert last_used["Region"] == "N/A"
        assert last_used["ServiceName"] == "N/A"
        assert "LastUsedDate" not in last_used
    finally:
        iam.delete_access_key(AccessKeyId=key_id)
        iam.delete_user(UserName=username)


def test_iam_get_access_key_last_used_not_found(iam):
    with pytest.raises(ClientError) as exc:
        iam.get_access_key_last_used(AccessKeyId="AKIAIOSFODNN7EXAMPLE")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_instance_profile(iam):
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    try:
        iam.create_role(RoleName="ip-role", AssumeRolePolicyDocument=assume)
    except ClientError:
        pass

    resp = iam.create_instance_profile(InstanceProfileName="test-ip")
    ip = resp["InstanceProfile"]
    assert ip["InstanceProfileName"] == "test-ip"
    assert "Arn" in ip

    iam.add_role_to_instance_profile(InstanceProfileName="test-ip", RoleName="ip-role")

    resp = iam.get_instance_profile(InstanceProfileName="test-ip")
    roles = resp["InstanceProfile"]["Roles"]
    assert any(r["RoleName"] == "ip-role" for r in roles)

    resp = iam.list_instance_profiles()
    names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
    assert "test-ip" in names

    iam.remove_role_from_instance_profile(InstanceProfileName="test-ip", RoleName="ip-role")
    iam.delete_instance_profile(InstanceProfileName="test-ip")

def test_iam_groups(iam):
    iam.create_group(GroupName="test-grp")
    resp = iam.get_group(GroupName="test-grp")
    assert resp["Group"]["GroupName"] == "test-grp"

    listed = iam.list_groups()
    assert any(g["GroupName"] == "test-grp" for g in listed["Groups"])

    iam.create_user(UserName="grp-usr")
    iam.add_user_to_group(GroupName="test-grp", UserName="grp-usr")
    members = iam.get_group(GroupName="test-grp")
    assert any(u["UserName"] == "grp-usr" for u in members["Users"])

    user_groups = iam.list_groups_for_user(UserName="grp-usr")
    assert any(g["GroupName"] == "test-grp" for g in user_groups["Groups"])

    iam.remove_user_from_group(GroupName="test-grp", UserName="grp-usr")
    iam.delete_group(GroupName="test-grp")

def test_iam_user_inline_policy(iam):
    iam.create_user(UserName="inl-pol-usr")
    doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }
    )
    iam.put_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc", PolicyDocument=doc)
    resp = iam.get_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc")
    assert resp["PolicyName"] == "s3-acc"
    listed = iam.list_user_policies(UserName="inl-pol-usr")
    assert "s3-acc" in listed["PolicyNames"]
    iam.delete_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc")

def test_iam_service_linked_role(iam):
    resp = iam.create_service_linked_role(AWSServiceName="elasticloadbalancing.amazonaws.com")
    role = resp["Role"]
    assert "AWSServiceRoleFor" in role["RoleName"]
    assert role["Path"].startswith("/aws-service-role/")

    del_resp = iam.delete_service_linked_role(RoleName=role["RoleName"])
    task_id = del_resp["DeletionTaskId"]
    assert task_id

    status = iam.get_service_linked_role_deletion_status(DeletionTaskId=task_id)
    assert status["Status"] == "SUCCEEDED"

    with pytest.raises(ClientError) as exc:
        iam.get_role(RoleName=role["RoleName"])
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

def test_iam_oidc_provider(iam):
    resp = iam.create_open_id_connect_provider(
        Url="https://oidc.example.com",
        ClientIDList=["my-client"],
        ThumbprintList=["a" * 40],
    )
    arn = resp["OpenIDConnectProviderArn"]
    assert "oidc.example.com" in arn
    desc = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
    assert "my-client" in desc["ClientIDList"]
    iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)

def test_iam_policy_tags(iam):
    resp = iam.create_policy(
        PolicyName="tagged-pol",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
            }
        ),
    )
    arn = resp["Policy"]["Arn"]
    iam.tag_policy(PolicyArn=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags = iam.list_policy_tags(PolicyArn=arn)
    assert any(t["Key"] == "env" for t in tags["Tags"])
    iam.untag_policy(PolicyArn=arn, TagKeys=["env"])
    tags2 = iam.list_policy_tags(PolicyArn=arn)
    assert not any(t["Key"] == "env" for t in tags2["Tags"])


def test_iam_policy_tags_serialized_in_get_policy(iam):
    """Regression for #445: _managed_policy_xml must emit Tags so GetPolicy /
    ListPolicies surface them. Without this block, Terraform's aws_iam_policy
    refresh sees tags_all={} and replans default_tags on every apply — same
    bug class as #441 (user tags) and #438 (policy description)."""
    import uuid as _u
    name = f"tagged-serialize-{_u.uuid4().hex[:8]}"
    resp = iam.create_policy(
        PolicyName=name,
        PolicyDocument=json.dumps({"Version": "2012-10-17",
                                    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}),
        Tags=[{"Key": "Team", "Value": "platform"}],
    )
    arn = resp["Policy"]["Arn"]
    # CreatePolicy response must carry Tags.
    create_tags = {t["Key"]: t["Value"] for t in resp["Policy"].get("Tags") or []}
    assert create_tags.get("Team") == "platform", f"CreatePolicy dropped Tags: {resp['Policy']}"
    # GetPolicy (separate endpoint, uses _managed_policy_xml) must too.
    got = iam.get_policy(PolicyArn=arn)
    got_tags = {t["Key"]: t["Value"] for t in got["Policy"].get("Tags") or []}
    assert got_tags.get("Team") == "platform", f"GetPolicy dropped Tags: {got['Policy']}"
    # TagPolicy after-the-fact must also round-trip via GetPolicy.
    iam.tag_policy(PolicyArn=arn, Tags=[{"Key": "Env", "Value": "dev"}])
    got2 = iam.get_policy(PolicyArn=arn)
    got2_tags = {t["Key"]: t["Value"] for t in got2["Policy"].get("Tags") or []}
    assert got2_tags == {"Team": "platform", "Env": "dev"}
    iam.delete_policy(PolicyArn=arn)

def test_iam_update_role(iam):
    iam.create_role(
        RoleName="test-update-role",
        AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    iam.update_role(RoleName="test-update-role", Description="updated desc", MaxSessionDuration=7200)
    resp = iam.get_role(RoleName="test-update-role")
    assert resp["Role"]["Description"] == "updated desc"
    assert resp["Role"]["MaxSessionDuration"] == 7200

def test_iam_policy_version_crud(iam):
    """CreatePolicyVersion, GetPolicyVersion, ListPolicyVersions, DeletePolicyVersion."""
    doc1 = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }
    )
    doc2 = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "sqs:*", "Resource": "*"}],
        }
    )
    arn = iam.create_policy(PolicyName="qa-iam-versions", PolicyDocument=doc1)["Policy"]["Arn"]
    iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc2, SetAsDefault=True)
    versions = iam.list_policy_versions(PolicyArn=arn)["Versions"]
    assert len(versions) == 2
    default = next(v for v in versions if v["IsDefaultVersion"])
    assert default["VersionId"] == "v2"
    v1 = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    assert v1["IsDefaultVersion"] is False
    iam.delete_policy_version(PolicyArn=arn, VersionId="v1")
    versions2 = iam.list_policy_versions(PolicyArn=arn)["Versions"]
    assert len(versions2) == 1

def test_iam_inline_user_policy(iam):
    """PutUserPolicy / GetUserPolicy / ListUserPolicies / DeleteUserPolicy."""
    iam.create_user(UserName="qa-iam-inline-user")
    doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
        }
    )
    iam.put_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline", PolicyDocument=doc)
    policies = iam.list_user_policies(UserName="qa-iam-inline-user")["PolicyNames"]
    assert "qa-inline" in policies
    got = iam.get_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline")
    # boto3 deserialises PolicyDocument as a dict
    assert "s3:GetObject" in json.dumps(got["PolicyDocument"])
    iam.delete_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline")
    policies2 = iam.list_user_policies(UserName="qa-iam-inline-user")["PolicyNames"]
    assert "qa-inline" not in policies2

def test_iam_instance_profile_crud(iam):
    """CreateInstanceProfile, AddRoleToInstanceProfile, GetInstanceProfile, ListInstanceProfiles."""
    iam.create_role(
        RoleName="qa-iam-ip-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    iam.create_instance_profile(InstanceProfileName="qa-iam-ip")
    iam.add_role_to_instance_profile(InstanceProfileName="qa-iam-ip", RoleName="qa-iam-ip-role")
    ip = iam.get_instance_profile(InstanceProfileName="qa-iam-ip")["InstanceProfile"]
    assert ip["InstanceProfileName"] == "qa-iam-ip"
    assert any(r["RoleName"] == "qa-iam-ip-role" for r in ip["Roles"])
    profiles = iam.list_instance_profiles()["InstanceProfiles"]
    assert any(p["InstanceProfileName"] == "qa-iam-ip" for p in profiles)
    iam.remove_role_from_instance_profile(InstanceProfileName="qa-iam-ip", RoleName="qa-iam-ip-role")
    iam.delete_instance_profile(InstanceProfileName="qa-iam-ip")

def test_iam_attach_detach_user_policy(iam):
    """AttachUserPolicy / DetachUserPolicy / ListAttachedUserPolicies."""
    iam.create_user(UserName="qa-iam-attach-user")
    doc = json.dumps({"Version": "2012-10-17", "Statement": []})
    policy_arn = iam.create_policy(PolicyName="qa-iam-attach-pol", PolicyDocument=doc)["Policy"]["Arn"]
    iam.attach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert any(p["PolicyArn"] == policy_arn for p in attached)
    iam.detach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached2 = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert not any(p["PolicyArn"] == policy_arn for p in attached2)

def test_iam_list_entities_for_policy(iam):
    """ListEntitiesForPolicy returns users and roles attached to a policy."""
    doc = json.dumps({"Version": "2012-10-17", "Statement": []})
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    policy_arn = iam.create_policy(PolicyName="qa-entities-pol", PolicyDocument=doc)["Policy"]["Arn"]
    iam.create_user(UserName="qa-entities-user")
    try:
        iam.create_role(RoleName="qa-entities-role", AssumeRolePolicyDocument=assume)
    except ClientError:
        pass
    iam.attach_user_policy(UserName="qa-entities-user", PolicyArn=policy_arn)
    iam.attach_role_policy(RoleName="qa-entities-role", PolicyArn=policy_arn)

    resp = iam.list_entities_for_policy(PolicyArn=policy_arn)
    user_names = [u["UserName"] for u in resp["PolicyUsers"]]
    role_names = [r["RoleName"] for r in resp["PolicyRoles"]]
    assert "qa-entities-user" in user_names
    assert "qa-entities-role" in role_names

    # Detach user and verify it's removed
    iam.detach_user_policy(UserName="qa-entities-user", PolicyArn=policy_arn)
    resp2 = iam.list_entities_for_policy(PolicyArn=policy_arn)
    user_names2 = [u["UserName"] for u in resp2["PolicyUsers"]]
    assert "qa-entities-user" not in user_names2
    assert "qa-entities-role" in [r["RoleName"] for r in resp2["PolicyRoles"]]

    # Test EntityFilter
    resp3 = iam.list_entities_for_policy(PolicyArn=policy_arn, EntityFilter="Role")
    assert len(resp3["PolicyRoles"]) >= 1
    assert len(resp3.get("PolicyUsers", [])) == 0


# -- AWS-managed policies (arn:aws:iam::aws:policy/<Name>) -----------------
#
# AWS-managed policies live under a virtual ``aws`` account that every
# customer can read regardless of their own session account. These
# tests cover the global store and its interaction with role/user
# attachment, ListPolicies scoping, and mutation rejection.

_ADMIN_ARN = "arn:aws:iam::aws:policy/AdministratorAccess"


def test_iam_get_aws_managed_policy_administrator_access(iam):
    """GetPolicy must succeed for a seeded AWS-managed policy even
    though no session can authenticate as the ``aws`` virtual account."""
    resp = iam.get_policy(PolicyArn=_ADMIN_ARN)
    assert resp["Policy"]["PolicyName"] == "AdministratorAccess"
    assert resp["Policy"]["Arn"] == _ADMIN_ARN
    assert resp["Policy"]["DefaultVersionId"] == "v1"


def test_iam_get_aws_managed_policy_version_returns_document(iam):
    resp = iam.get_policy_version(PolicyArn=_ADMIN_ARN, VersionId="v1")
    doc = resp["PolicyVersion"]["Document"]
    # boto3 deserialises PolicyDocument; stringify before checking.
    assert "Allow" in json.dumps(doc)


def test_iam_list_policies_scope_all_includes_aws_managed(iam):
    resp = iam.list_policies(Scope="All")
    arns = [p["Arn"] for p in resp["Policies"]]
    assert _ADMIN_ARN in arns


def test_iam_list_policies_scope_aws_returns_only_aws_managed(iam):
    # Seed a customer-managed policy so we can prove it's filtered out.
    doc = json.dumps({"Version": "2012-10-17",
                      "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
    name = f"awsmgd-filter-cust-{_uuid_mod.uuid4().hex[:8]}"
    cust = iam.create_policy(PolicyName=name, PolicyDocument=doc)["Policy"]["Arn"]
    try:
        resp = iam.list_policies(Scope="AWS")
        arns = [p["Arn"] for p in resp["Policies"]]
        assert _ADMIN_ARN in arns
        assert all(a.startswith("arn:aws:iam::aws:") for a in arns)
        assert cust not in arns
    finally:
        iam.delete_policy(PolicyArn=cust)


def test_iam_list_policies_scope_local_excludes_aws_managed(iam):
    resp = iam.list_policies(Scope="Local")
    arns = [p["Arn"] for p in resp["Policies"]]
    assert all(not a.startswith("arn:aws:iam::aws:") for a in arns)


def test_iam_attach_aws_managed_policy_to_role(iam):
    """The original motivating use case: terraform attaches an
    AWS-managed policy by ARN to a customer role."""
    role_name = f"awsmgd-role-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    try:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=_ADMIN_ARN)
        attached = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
        names = [p["PolicyName"] for p in attached]
        arns = [p["PolicyArn"] for p in attached]
        assert "AdministratorAccess" in names
        assert _ADMIN_ARN in arns
        iam.detach_role_policy(RoleName=role_name, PolicyArn=_ADMIN_ARN)
    finally:
        iam.delete_role(RoleName=role_name)


def test_iam_cannot_create_policy_under_aws_account(iam, monkeypatch):
    """Customer code must never be able to author into the AWS-managed
    namespace. Real AWS rejects this with InvalidInput."""
    # We can't change the session account at request time from boto3,
    # so this test is best-effort: we ensure a normal CreatePolicy
    # response still lands under the session account, not under ``aws``.
    doc = json.dumps({"Version": "2012-10-17",
                      "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
    name = f"awsmgd-bounds-{_uuid_mod.uuid4().hex[:8]}"
    resp = iam.create_policy(PolicyName=name, PolicyDocument=doc)
    try:
        assert not resp["Policy"]["Arn"].startswith("arn:aws:iam::aws:")
    finally:
        iam.delete_policy(PolicyArn=resp["Policy"]["Arn"])


def test_iam_cannot_delete_aws_managed_policy(iam):
    with pytest.raises(ClientError) as exc:
        iam.delete_policy(PolicyArn=_ADMIN_ARN)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"


def test_iam_cannot_tag_aws_managed_policy(iam):
    with pytest.raises(ClientError) as exc:
        iam.tag_policy(PolicyArn=_ADMIN_ARN, Tags=[{"Key": "k", "Value": "v"}])
    assert exc.value.response["Error"]["Code"] == "AccessDenied"


def test_iam_unknown_aws_managed_policy_is_not_found_by_default(iam):
    """Real AWS returns NoSuchEntity for AWS-managed ARNs that don't
    exist (e.g. typos like ``AdminstratorAccess`` — missing ``i``).
    Ministack defaults to the same behaviour so that typos surface
    locally the same way they would in real AWS, instead of silently
    autovivifying a permissive stub and masking the bug until apply
    against real AWS."""
    arn = "arn:aws:iam::aws:policy/AdminstratorAccess"  # missing 'i'
    with pytest.raises(ClientError) as exc:
        iam.get_policy(PolicyArn=arn)
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_aws_managed_attachment_count_is_per_account(iam):
    """AWS-managed AttachmentCount must reflect the calling account's
    own attachments — real AWS scopes the counter per-account. Attach
    to two roles and a user under the same session, then GetPolicy and
    expect AttachmentCount == 3."""
    role_a = f"awsmgd-count-role-a-{_uuid_mod.uuid4().hex[:8]}"
    role_b = f"awsmgd-count-role-b-{_uuid_mod.uuid4().hex[:8]}"
    user = f"awsmgd-count-user-{_uuid_mod.uuid4().hex[:8]}"
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    # ReadOnlyAccess so the test doesn't contend with other tests that
    # attach AdministratorAccess.
    arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
    baseline = iam.get_policy(PolicyArn=arn)["Policy"]["AttachmentCount"]
    iam.create_role(RoleName=role_a, AssumeRolePolicyDocument=assume)
    iam.create_role(RoleName=role_b, AssumeRolePolicyDocument=assume)
    iam.create_user(UserName=user)
    try:
        iam.attach_role_policy(RoleName=role_a, PolicyArn=arn)
        iam.attach_role_policy(RoleName=role_b, PolicyArn=arn)
        iam.attach_user_policy(UserName=user, PolicyArn=arn)
        got = iam.get_policy(PolicyArn=arn)["Policy"]
        assert got["AttachmentCount"] == baseline + 3
        # Detach decrements.
        iam.detach_role_policy(RoleName=role_a, PolicyArn=arn)
        got2 = iam.get_policy(PolicyArn=arn)["Policy"]
        assert got2["AttachmentCount"] == baseline + 2
    finally:
        for r in (role_a, role_b):
            try:
                iam.detach_role_policy(RoleName=r, PolicyArn=arn)
            except ClientError:
                pass
            iam.delete_role(RoleName=r)
        try:
            iam.detach_user_policy(UserName=user, PolicyArn=arn)
        except ClientError:
            pass
        iam.delete_user(UserName=user)


def test_iam_aws_managed_attachment_count_persists_through_state_round_trip():
    """Regression for 1.3.36: the _aws_managed_attachment_counts sidecar
    added with the AWS-managed policy work (1.3.36) was missing from
    get_state/restore_state, so attachment counts on AWS-managed policies
    reset to zero on every warm-boot."""
    from ministack.services import iam as _iam

    arn = "arn:aws:iam::aws:policy/AdministratorAccess"
    _iam._aws_managed_attachment_counts.clear()
    _iam._bump_aws_managed_attachment(arn, +2)
    assert _iam._aws_managed_attachment_counts.get(arn) == 2

    snapshot = _iam.get_state()
    _iam._aws_managed_attachment_counts.clear()
    assert _iam._aws_managed_attachment_counts.get(arn, 0) == 0

    _iam.restore_state(snapshot)
    assert _iam._aws_managed_attachment_counts.get(arn) == 2
