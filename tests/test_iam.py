import json
import uuid as _uuid_mod

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


_TRUST_ALLOW_ROOT = {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
                     "Action": "sts:AssumeRole"}
_TRUST_ALLOW_NOT_PRINCIPAL = {"Effect": "Allow", "NotPrincipal": {"AWS": "arn:aws:iam::111122223333:root"},
                              "Action": "sts:AssumeRole"}
_TRUST_DENY_NOT_PRINCIPAL = {"Effect": "Deny", "NotPrincipal": {"AWS": "arn:aws:iam::000000000000:root"},
                             "Action": "sts:AssumeRole"}


@pytest.mark.parametrize("statement, message", [
    ([_TRUST_ALLOW_NOT_PRINCIPAL], "Allow with NotPrincipal is not allowed."),
    (_TRUST_ALLOW_NOT_PRINCIPAL, "Allow with NotPrincipal is not allowed."),
    ([_TRUST_ALLOW_ROOT, _TRUST_DENY_NOT_PRINCIPAL], "AssumeRole policy must not contain NotPrincipal field."),
    ([_TRUST_DENY_NOT_PRINCIPAL, _TRUST_ALLOW_NOT_PRINCIPAL], "Allow with NotPrincipal is not allowed."),
], ids=["allow", "allow-object", "deny", "deny-then-allow"])
def test_iam_role_trust_policy_not_principal_refused(iam, statement, message):
    name = f"not-principal-role-{_uuid_mod.uuid4().hex[:8]}"
    refused = json.dumps({"Version": "2012-10-17", "Statement": statement})
    plain = {"Version": "2012-10-17", "Statement": [_TRUST_ALLOW_ROOT]}
    with pytest.raises(ClientError) as exc:
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=refused)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"
    assert exc.value.response["Error"]["Message"] == message
    with pytest.raises(ClientError) as exc:
        iam.get_role(RoleName=name)
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

    iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(plain))
    try:
        with pytest.raises(ClientError) as exc:
            iam.update_assume_role_policy(RoleName=name, PolicyDocument=refused)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"
        assert exc.value.response["Error"]["Message"] == message
        assert iam.get_role(RoleName=name)["Role"]["AssumeRolePolicyDocument"] == plain
        # The name and the role's existence are checked before the document.
        with pytest.raises(ClientError) as exc:
            iam.create_role(RoleName=name, AssumeRolePolicyDocument=refused)
        assert exc.value.response["Error"]["Code"] == "EntityAlreadyExists"
        with pytest.raises(ClientError) as exc:
            iam.update_assume_role_policy(RoleName=f"{name}-missing", PolicyDocument=refused)
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"
    finally:
        iam.delete_role(RoleName=name)


def test_iam_update_role_description(iam):
    name = f"description-role-{_uuid_mod.uuid4().hex[:8]}"
    iam.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    updated = iam.update_role_description(RoleName=name, Description="updated description")["Role"]
    assert updated["Description"] == "updated description"
    assert iam.get_role(RoleName=name)["Role"]["Description"] == "updated description"
    iam.delete_role(RoleName=name)


def test_iam_role_permissions_boundary_roundtrip(iam):
    name = f"boundary-role-{_uuid_mod.uuid4().hex[:8]}"
    original_boundary = "arn:aws:iam::000000000000:policy/original-boundary"
    replacement_boundary = "arn:aws:iam::000000000000:policy/replacement-boundary"
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})

    created = iam.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=assume,
        PermissionsBoundary=original_boundary,
    )["Role"]
    assert created["PermissionsBoundary"] == {
        "PermissionsBoundaryType": "PermissionsBoundaryPolicy",
        "PermissionsBoundaryArn": original_boundary,
    }
    assert iam.get_role(RoleName=name)["Role"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == original_boundary

    iam.put_role_permissions_boundary(RoleName=name, PermissionsBoundary=replacement_boundary)
    assert iam.get_role(RoleName=name)["Role"]["PermissionsBoundary"] == {
        "PermissionsBoundaryType": "PermissionsBoundaryPolicy",
        "PermissionsBoundaryArn": replacement_boundary,
    }

    iam.delete_role_permissions_boundary(RoleName=name)
    assert "PermissionsBoundary" not in iam.get_role(RoleName=name)["Role"]
    # AWS treats deletion as idempotent when the role exists without a boundary.
    iam.delete_role_permissions_boundary(RoleName=name)
    iam.delete_role(RoleName=name)


@pytest.mark.parametrize("operation", ["put", "delete"])
def test_iam_role_permissions_boundary_missing_role(iam, operation):
    name = f"missing-boundary-role-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        if operation == "put":
            iam.put_role_permissions_boundary(
                RoleName=name,
                PermissionsBoundary="arn:aws:iam::000000000000:policy/boundary",
            )
        else:
            iam.delete_role_permissions_boundary(RoleName=name)
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"

def test_iam_user_permissions_boundary_roundtrip(iam):
    name = f"boundary-user-{_uuid_mod.uuid4().hex[:8]}"
    original_boundary = "arn:aws:iam::000000000000:policy/original-boundary"
    replacement_boundary = "arn:aws:iam::000000000000:policy/replacement-boundary"

    created = iam.create_user(UserName=name, PermissionsBoundary=original_boundary)["User"]
    assert created["PermissionsBoundary"] == {
        "PermissionsBoundaryType": "PermissionsBoundaryPolicy",
        "PermissionsBoundaryArn": original_boundary,
    }
    iam.put_user_permissions_boundary(UserName=name, PermissionsBoundary=replacement_boundary)
    assert iam.get_user(UserName=name)["User"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == replacement_boundary
    iam.delete_user_permissions_boundary(UserName=name)
    assert "PermissionsBoundary" not in iam.get_user(UserName=name)["User"]
    iam.delete_user(UserName=name)


@pytest.mark.parametrize("operation", ["put", "delete"])
def test_iam_user_permissions_boundary_missing_user(iam, operation):
    name = f"missing-boundary-user-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        if operation == "put":
            iam.put_user_permissions_boundary(
                UserName=name, PermissionsBoundary="arn:aws:iam::000000000000:policy/boundary")
        else:
            iam.delete_user_permissions_boundary(UserName=name)
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def _boundary_policy(iam, name):
    doc = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "sqs:ListQueues", "Resource": "*"}]})
    return iam.create_policy(PolicyName=name, PolicyDocument=doc)["Policy"]["Arn"]


def _iam_action(monkeypatch, auth, action, **params):
    """Run one IAM query action in-process with AUTH set to *auth*; return (status, body)."""
    import asyncio
    from urllib.parse import urlencode

    from ministack import app as app_mod
    from ministack.services import iam as iam_svc

    monkeypatch.setattr(app_mod, "AUTH", auth)
    body = urlencode({"Action": action, "Version": "2010-05-08", **params}).encode()
    status, _headers, payload = asyncio.run(iam_svc.handle_request("POST", "/", {}, body, {}))
    return status, payload.decode()


def _message(body):
    return body.split("<Message>", 1)[1].split("</Message>", 1)[0]


@pytest.mark.parametrize("auth", [True, False])
@pytest.mark.parametrize("kind", ["customer", "aws"])
@pytest.mark.parametrize("operation", [
    "CreateRole", "CreateUser", "PutRolePermissionsBoundary", "PutUserPermissionsBoundary"])
def test_iam_permissions_boundary_must_name_a_known_policy_under_auth(monkeypatch, auth, kind, operation):
    name = f"boundary-missing-{_uuid_mod.uuid4().hex[:8]}"
    missing = f"arn:aws:iam::{'aws' if kind == 'aws' else '000000000000'}:policy/{name}"
    entity = "Role" if "Role" in operation else "User"
    key = {f"{entity}Name": name}
    create = {"AssumeRolePolicyDocument": "{}"} if entity == "Role" else {}
    if operation.startswith("Put"):
        assert _iam_action(monkeypatch, False, f"Create{entity}", **key, **create)[0] == 200
    params = {**key, **(create if operation.startswith("Create") else {})}
    try:
        status, body = _iam_action(monkeypatch, auth, operation, PermissionsBoundary=missing, **params)
        if auth:
            assert (status, f"Scope ARN: {missing} does not exist or is not attachable.") == (404, _message(body))
        else:
            assert status == 200
    finally:
        _iam_action(monkeypatch, False, f"Delete{entity}", **key)


@pytest.mark.parametrize("auth, status", [(True, 400), (False, 200)])
def test_iam_permissions_boundary_must_be_an_arn_under_auth(monkeypatch, auth, status):
    name = f"boundary-malformed-{_uuid_mod.uuid4().hex[:8]}"
    _iam_action(monkeypatch, False, "CreateUser", UserName=name)
    try:
        got, body = _iam_action(monkeypatch, auth, "PutUserPermissionsBoundary", UserName=name,
                                PermissionsBoundary=f"not-an-arn-{name}")
        assert got == status
        if auth:
            assert _message(body) == "Invalid ARN:  Could not be parsed!"
    finally:
        _iam_action(monkeypatch, False, "DeleteUser", UserName=name)


@pytest.mark.parametrize("auth, status", [(True, 404), (False, 200)])
@pytest.mark.parametrize("entity", ["Role", "User"])
def test_iam_delete_absent_permissions_boundary_under_auth(monkeypatch, auth, status, entity):
    name = f"boundary-absent-{_uuid_mod.uuid4().hex[:8]}"
    create = {"AssumeRolePolicyDocument": "{}"} if entity == "Role" else {}
    _, created = _iam_action(monkeypatch, False, f"Create{entity}", **{f"{entity}Name": name}, **create)
    principal_id = created.split(f"<{entity}Id>", 1)[1].split("<", 1)[0]
    try:
        got, body = _iam_action(monkeypatch, auth, f"Delete{entity}PermissionsBoundary", **{f"{entity}Name": name})
        assert got == status
        if auth:
            assert _message(body) == f"Principal {principal_id} does not have scope"
    finally:
        _iam_action(monkeypatch, False, f"Delete{entity}", **{f"{entity}Name": name})


@pytest.mark.parametrize("auth, status", [(True, 409), (False, 200)])
def test_iam_delete_policy_in_use_as_a_boundary_under_auth(monkeypatch, auth, status):
    name = f"boundary-delete-{_uuid_mod.uuid4().hex[:8]}"
    _, created = _iam_action(monkeypatch, False, "CreatePolicy", PolicyName=name, PolicyDocument=json.dumps(
        {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sqs:ListQueues", "Resource": "*"}]}))
    arn = created.split("<Arn>", 1)[1].split("<", 1)[0]
    _iam_action(monkeypatch, False, "CreateUser", UserName=name, PermissionsBoundary=arn)
    try:
        assert _iam_action(monkeypatch, auth, "DeletePolicy", PolicyArn=arn)[0] == status
    finally:
        _iam_action(monkeypatch, False, "DeleteUser", UserName=name)
        _iam_action(monkeypatch, False, "DeletePolicy", PolicyArn=arn)


def test_iam_permissions_boundary_uses_are_listed_as_policy_entities(iam):
    name = f"boundary-entities-{_uuid_mod.uuid4().hex[:8]}"
    boundary = _boundary_policy(iam, name)
    iam.create_user(UserName=name, PermissionsBoundary=boundary)
    iam.create_role(RoleName=name, PermissionsBoundary=boundary,
                    AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}))

    def entities(**kwargs):
        resp = iam.list_entities_for_policy(PolicyArn=boundary, **kwargs)
        return [r["RoleName"] for r in resp["PolicyRoles"]], [u["UserName"] for u in resp["PolicyUsers"]]

    assert entities() == ([name], [name])
    assert entities(PolicyUsageFilter="PermissionsBoundary") == ([name], [name])
    assert entities(PolicyUsageFilter="PermissionsPolicy") == ([], [])
    listed_user = next(u for u in iam.list_users()["Users"] if u["UserName"] == name)
    listed_role = next(r for r in iam.list_roles()["Roles"] if r["RoleName"] == name)
    assert "PermissionsBoundary" not in listed_user and "PermissionsBoundary" not in listed_role
    iam.delete_user(UserName=name)
    iam.delete_role(RoleName=name)
    iam.delete_policy(PolicyArn=boundary)


def test_iam_permissions_boundary_accepts_an_aws_managed_policy(iam):
    name = f"boundary-managed-{_uuid_mod.uuid4().hex[:8]}"
    boundary = "arn:aws:iam::aws:policy/ReadOnlyAccess"
    iam.create_user(UserName=name)
    iam.put_user_permissions_boundary(UserName=name, PermissionsBoundary=boundary)
    assert iam.get_user(UserName=name)["User"]["PermissionsBoundary"]["PermissionsBoundaryArn"] == boundary
    iam.delete_user(UserName=name)


def test_iam_policy_counts_its_permissions_boundary_uses(iam):
    name = f"boundary-count-{_uuid_mod.uuid4().hex[:8]}"
    boundary = _boundary_policy(iam, name)
    assert iam.get_policy(PolicyArn=boundary)["Policy"]["PermissionsBoundaryUsageCount"] == 0
    iam.create_user(UserName=f"{name}-user", PermissionsBoundary=boundary)
    iam.create_role(RoleName=f"{name}-role", PermissionsBoundary=boundary,
                    AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}))
    policy = iam.get_policy(PolicyArn=boundary)["Policy"]
    assert (policy["AttachmentCount"], policy["PermissionsBoundaryUsageCount"]) == (0, 2)
    listed = next(p for p in iam.list_policies(Scope="Local")["Policies"] if p["Arn"] == boundary)
    assert listed["PermissionsBoundaryUsageCount"] == 2
    iam.delete_user(UserName=f"{name}-user")
    iam.delete_role_permissions_boundary(RoleName=f"{name}-role")
    assert iam.get_policy(PolicyArn=boundary)["Policy"]["PermissionsBoundaryUsageCount"] == 0
    iam.delete_role(RoleName=f"{name}-role")
    iam.delete_policy(PolicyArn=boundary)


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


_IDENTITY_TAG_TRUST = {"Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                                 "Action": "sts:AssumeRole"}]}


def _identity_tag_map(tags):
    result = {t["Key"]: t["Value"] for t in tags}
    assert len(result) == len(tags), f"Duplicate tag keys in response: {tags}"
    return result


@pytest.mark.parametrize("resource", ["user", "role"])
def test_iam_identity_tag_keys_ignore_case_and_preserve_spelling(iam, resource):
    name = f"tag-case-{resource}-{_uuid_mod.uuid4().hex[:8]}"
    params = {f"{resource.title()}Name": name}
    create_params = {"AssumeRolePolicyDocument": json.dumps(_IDENTITY_TAG_TRUST)} if resource == "role" else {}
    original = [{"Key": "Department", "Value": "Finance"}, {"Key": "ExternalKey", "Value": "Keep"}]
    getattr(iam, f"create_{resource}")(**params, **create_params, Tags=original)
    try:
        assert _identity_tag_map(getattr(iam, f"get_{resource}")(**params)[resource.title()]["Tags"]) == _identity_tag_map(original)
        # Untag must match both the original spelling and a subsequently changed spelling.
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["DEPARTMENT"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [original[1]]
        getattr(iam, f"tag_{resource}")(**params, Tags=[original[0]])
        # A spelling-only update and a case-sensitive value change must both take effect.
        for key, value in [("department", "Finance"), ("DEPARTMENT", "finance")]:
            getattr(iam, f"tag_{resource}")(**params, Tags=[{"Key": key, "Value": value}])
            expected = {key: value, "ExternalKey": "Keep"}
            assert _identity_tag_map(getattr(iam, f"get_{resource}")(**params)[resource.title()]["Tags"]) == expected
            assert _identity_tag_map(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == expected
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["DePaRtMeNt", "missing"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [original[1]]
    finally:
        getattr(iam, f"delete_{resource}")(**params)


@pytest.mark.parametrize("resource", ["user", "role"])
@pytest.mark.parametrize("operation", ["create", "tag"])
@pytest.mark.parametrize("second_key", ["Department", "department"])
def test_iam_identity_duplicate_tag_keys_reject_entire_request(iam, resource, operation, second_key):
    params = {f"{resource.title()}Name": f"tag-duplicate-{resource}-{_uuid_mod.uuid4().hex[:8]}"}
    create_params = {"AssumeRolePolicyDocument": json.dumps(_IDENTITY_TAG_TRUST)} if resource == "role" else {}
    original = [{"Key": "Department", "Value": "Finance"}]
    if operation == "tag":
        getattr(iam, f"create_{resource}")(**params, **create_params, Tags=original)
    try:
        with pytest.raises(ClientError) as exc:
            getattr(iam, f"{operation}_{resource}")(**params,
                **(create_params if operation == "create" else {}), Tags=[
                    {"Key": "Unrelated", "Value": "MustNotBeAdded"},
                    {"Key": "Department", "Value": "HR"}, {"Key": second_key, "Value": "HR"},
                ])
        assert exc.value.response["Error"]["Code"] == "InvalidInput"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        if operation == "tag":
            assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == original
        else:
            with pytest.raises(iam.exceptions.NoSuchEntityException):
                getattr(iam, f"get_{resource}")(**params)
    finally:
        try:
            getattr(iam, f"delete_{resource}")(**params)
        except iam.exceptions.NoSuchEntityException:
            pass


@pytest.mark.parametrize("resource", ["policy", "instance_profile"])
def test_iam_other_resource_tag_keys_remain_case_sensitive(iam, resource):
    name = f"tag-case-{resource}-{_uuid_mod.uuid4().hex[:8]}"
    tags = [{"Key": "Costcenter", "Value": "1234"}, {"Key": "costcenter", "Value": "5678"}]
    if resource == "policy":
        arn = iam.create_policy(PolicyName=name, PolicyDocument=json.dumps({
            "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "*"}],
        }), Tags=[tags[0]])["Policy"]["Arn"]
        params = {"PolicyArn": arn}
    else:
        iam.create_instance_profile(InstanceProfileName=name, Tags=[tags[0]])
        params = {"InstanceProfileName": name}
    try:
        getattr(iam, f"tag_{resource}")(**params, Tags=[tags[1]])
        assert _identity_tag_map(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == _identity_tag_map(tags)
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["Costcenter"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [tags[1]]
    finally:
        getattr(iam, f"delete_{resource}")(**params)


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
        iam.update_access_key(AccessKeyId="test-key-do-not-use", Status="Inactive")
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
        iam.get_access_key_last_used(AccessKeyId="test-key-do-not-use")
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

def test_iam_group_managed_policy(iam):
    iam.create_group(GroupName="grp-managed")
    pol = iam.create_policy(
        PolicyName="grp-managed-pol",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
            }
        ),
    )
    arn = pol["Policy"]["Arn"]

    iam.attach_group_policy(GroupName="grp-managed", PolicyArn=arn)
    listed = iam.list_attached_group_policies(GroupName="grp-managed")
    assert any(p["PolicyArn"] == arn for p in listed["AttachedPolicies"])

    iam.detach_group_policy(GroupName="grp-managed", PolicyArn=arn)
    listed = iam.list_attached_group_policies(GroupName="grp-managed")
    assert not any(p["PolicyArn"] == arn for p in listed["AttachedPolicies"])


def test_iam_group_managed_policy_unknown_group(iam):
    with pytest.raises(ClientError) as exc:
        iam.attach_group_policy(
            GroupName="no-such-grp",
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_group_inline_policy(iam):
    iam.create_group(GroupName="grp-inline")
    doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }
    )
    iam.put_group_policy(GroupName="grp-inline", PolicyName="s3-acc", PolicyDocument=doc)
    resp = iam.get_group_policy(GroupName="grp-inline", PolicyName="s3-acc")
    assert resp["PolicyName"] == "s3-acc"
    # botocore URL-decodes and parses the policy document into a dict.
    assert resp["PolicyDocument"] == json.loads(doc)
    listed = iam.list_group_policies(GroupName="grp-inline")
    assert "s3-acc" in listed["PolicyNames"]
    iam.delete_group_policy(GroupName="grp-inline", PolicyName="s3-acc")
    listed = iam.list_group_policies(GroupName="grp-inline")
    assert "s3-acc" not in listed["PolicyNames"]


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


def test_iam_instance_profile_tags(iam):
    """TagInstanceProfile, ListInstanceProfileTags, UntagInstanceProfile round-trip."""
    iam.create_instance_profile(InstanceProfileName="qa-iam-ip-tags")
    iam.tag_instance_profile(
        InstanceProfileName="qa-iam-ip-tags",
        Tags=[{"Key": "k", "Value": "v"}],
    )
    tags = iam.list_instance_profile_tags(InstanceProfileName="qa-iam-ip-tags")["Tags"]
    assert any(t["Key"] == "k" and t["Value"] == "v" for t in tags)
    iam.untag_instance_profile(InstanceProfileName="qa-iam-ip-tags", TagKeys=["k"])
    tags2 = iam.list_instance_profile_tags(InstanceProfileName="qa-iam-ip-tags")["Tags"]
    assert not any(t["Key"] == "k" for t in tags2)
    iam.delete_instance_profile(InstanceProfileName="qa-iam-ip-tags")


def test_iam_instance_profile_tags_serialized_in_get(iam):
    """Tags set via TagInstanceProfile (and at create time) must read back from
    GetInstanceProfile / ListInstanceProfiles so Terraform's
    aws_iam_instance_profile does not detect tag drift on re-apply. Same bug
    class as #441 (user tags) / #445 (policy tags)."""
    import uuid as _u
    name = f"qa-iam-ip-ser-{_u.uuid4().hex[:8]}"
    # Tags supplied at create time must round-trip.
    iam.create_instance_profile(
        InstanceProfileName=name,
        Tags=[{"Key": "Team", "Value": "platform"}],
    )
    got = iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]
    got_tags = {t["Key"]: t["Value"] for t in got.get("Tags") or []}
    assert got_tags.get("Team") == "platform", f"GetInstanceProfile dropped Tags: {got}"
    # TagInstanceProfile after-the-fact must also round-trip via GetInstanceProfile.
    iam.tag_instance_profile(InstanceProfileName=name, Tags=[{"Key": "Env", "Value": "dev"}])
    got2 = iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]
    got2_tags = {t["Key"]: t["Value"] for t in got2.get("Tags") or []}
    assert got2_tags == {"Team": "platform", "Env": "dev"}
    # ListInstanceProfiles (separate code path) must surface them too.
    listed = iam.list_instance_profiles()["InstanceProfiles"]
    match = next(p for p in listed if p["InstanceProfileName"] == name)
    listed_tags = {t["Key"]: t["Value"] for t in match.get("Tags") or []}
    assert listed_tags == {"Team": "platform", "Env": "dev"}
    iam.delete_instance_profile(InstanceProfileName=name)


def test_iam_attach_detach_user_policy(iam):
    """AttachUserPolicy / DetachUserPolicy / ListAttachedUserPolicies."""
    iam.create_user(UserName="qa-iam-attach-user")
    doc = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]})
    policy_arn = iam.create_policy(PolicyName="qa-iam-attach-pol", PolicyDocument=doc)["Policy"]["Arn"]
    iam.attach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert any(p["PolicyArn"] == policy_arn for p in attached)
    iam.detach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached2 = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert not any(p["PolicyArn"] == policy_arn for p in attached2)

def test_iam_list_entities_for_policy(iam):
    """ListEntitiesForPolicy returns users and roles attached to a policy."""
    doc = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]})
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


def test_iam_seeded_amazon_eks_cluster_policy(iam):
    """AmazonEKSClusterPolicy must be seeded with the real AWS document,
    not the wildcard fallback (issue #1092)."""
    arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    actions = []
    for stmt in doc["Statement"]:
        a = stmt.get("Action", [])
        actions.extend(a if isinstance(a, list) else [a])
    # Spot-check that real EKS-cluster actions are present and the wildcard fallback isn't.
    assert "elasticloadbalancing:CreateLoadBalancer" in actions
    assert "ec2:CreateSecurityGroup" in actions
    assert "*" not in actions


def test_iam_seeded_amazon_eks_worker_node_policy(iam):
    arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    actions = doc["Statement"][0]["Action"]
    assert "eks:DescribeCluster" in actions
    assert "eks-auth:AssumeRoleForPodIdentity" in actions


def test_iam_seeded_amazon_eks_cni_policy(iam):
    arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    first_actions = doc["Statement"][0]["Action"]
    assert "ec2:AssignPrivateIpAddresses" in first_actions
    assert "ec2:ModifyNetworkInterfaceAttribute" in first_actions


def test_iam_seeded_aws_xray_daemon_write_access(iam):
    """AWSXRayDaemonWriteAccess is heavily referenced by
    terraform-aws-modules/lambda's `attach_tracing_policy = true` path
    via `data "aws_iam_policy" "tracing" { arn = ... }`."""
    arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
    resp = iam.get_policy(PolicyArn=arn)
    assert resp["Policy"]["PolicyName"] == "AWSXRayDaemonWriteAccess"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    actions = doc["Statement"][0]["Action"]
    assert "xray:PutTraceSegments" in actions
    assert "xray:PutTelemetryRecords" in actions
    assert "*" not in actions


def test_iam_seeded_aws_xray_readonly(iam):
    arn = "arn:aws:iam::aws:policy/AWSXrayReadOnlyAccess"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    actions = doc["Statement"][0]["Action"]
    assert "xray:BatchGetTraces" in actions
    assert "xray:GetServiceGraph" in actions


def test_iam_seeded_aws_lambda_role(iam):
    # AWS publishes this policy only under service-role/; the path-less ARN
    # answers NoSuchEntity, as on AWS.
    arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
    pv = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    doc = pv["Document"] if isinstance(pv["Document"], dict) else json.loads(pv["Document"])
    actions = doc["Statement"][0]["Action"]
    assert "lambda:InvokeFunction" in actions


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

    _iam.load_persisted_state(snapshot)
    assert _iam._aws_managed_attachment_counts.get(arn) == 2


# ── Service last accessed (Access Advisor) ────────────────────────────


def test_iam_service_last_accessed_job(iam):
    # Use the default account user
    resp_user = iam.create_user(UserName="sla-test-user")
    user_arn = resp_user["User"]["Arn"]
    try:
        gen_resp = iam.generate_service_last_accessed_details(Arn=user_arn)
        job_id = gen_resp["JobId"]
        assert job_id

        get_resp = iam.get_service_last_accessed_details(JobId=job_id)
        assert get_resp["JobStatus"] == "COMPLETED"
        assert "ServicesLastAccessed" in get_resp
        assert isinstance(get_resp["ServicesLastAccessed"], list)
    finally:
        iam.delete_user(UserName="sla-test-user")
# ── SAML providers + ListOpenIDConnectProviders ──────────────────────


def test_iam_saml_provider_crud(iam):
    name = "saml-test-provider"
    # botocore requires ≥ 1000 chars for SAMLMetadataDocument (client-side validation)
    metadata = "<EntityDescriptor>" + "x" * 990 + "</EntityDescriptor>"
    resp = iam.create_saml_provider(Name=name, SAMLMetadataDocument=metadata)
    arn = resp["SAMLProviderArn"]
    assert f":saml-provider/{name}" in arn

    providers = iam.list_saml_providers()["SAMLProviderList"]
    assert any(p["Arn"] == arn for p in providers)

    get_resp = iam.get_saml_provider(SAMLProviderArn=arn)
    assert get_resp["SAMLMetadataDocument"] == metadata

    iam.delete_saml_provider(SAMLProviderArn=arn)
    with pytest.raises(iam.exceptions.NoSuchEntityException):
        iam.get_saml_provider(SAMLProviderArn=arn)


def test_iam_list_oidc_providers(iam):
    resp = iam.create_open_id_connect_provider(
        Url="https://oidc-list-test.example.com",
        ClientIDList=["aud"],
        ThumbprintList=["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00"],
    )
    arn = resp["OpenIDConnectProviderArn"]
    try:
        providers = iam.list_open_id_connect_providers()["OpenIDConnectProviderList"]
        assert any(p["Arn"] == arn for p in providers)
    finally:
        iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
# ── GetAccountAuthorizationDetails ───────────────────────────────────


def test_iam_account_authorization_details_all(iam):
    policy_doc = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]})
    assume_doc = json.dumps({"Version": "2012-10-17", "Statement": []})

    pol = iam.create_policy(PolicyName="aad-test-policy", PolicyDocument=policy_doc)
    pol_arn = pol["Policy"]["Arn"]
    iam.create_user(UserName="aad-test-user")
    iam.attach_user_policy(UserName="aad-test-user", PolicyArn=pol_arn)
    iam.create_group(GroupName="aad-test-group")
    iam.add_user_to_group(UserName="aad-test-user", GroupName="aad-test-group")
    iam.create_role(RoleName="aad-test-role", AssumeRolePolicyDocument=assume_doc)
    try:
        resp = iam.get_account_authorization_details()

        user_names = [u["UserName"] for u in resp.get("UserDetailList", [])]
        assert "aad-test-user" in user_names

        role_names = [r["RoleName"] for r in resp.get("RoleDetailList", [])]
        assert "aad-test-role" in role_names

        policy_arns = [p["Arn"] for p in resp.get("Policies", [])]
        assert pol_arn in policy_arns
    finally:
        iam.detach_user_policy(UserName="aad-test-user", PolicyArn=pol_arn)
        iam.remove_user_from_group(UserName="aad-test-user", GroupName="aad-test-group")
        iam.delete_user(UserName="aad-test-user")
        iam.delete_group(GroupName="aad-test-group")
        iam.delete_role(RoleName="aad-test-role")
        iam.delete_policy(PolicyArn=pol_arn)


def test_iam_account_authorization_details_filter(iam):
    assume_doc = json.dumps({"Version": "2012-10-17", "Statement": []})
    iam.create_user(UserName="aad-filter-user")
    iam.create_role(RoleName="aad-filter-role", AssumeRolePolicyDocument=assume_doc)
    try:
        resp = iam.get_account_authorization_details(Filter=["Role"])
        # UserDetailList should be empty when filtering for Role only
        assert resp.get("UserDetailList", []) == []
        role_names = [r["RoleName"] for r in resp.get("RoleDetailList", [])]
        assert "aad-filter-role" in role_names
    finally:
        iam.delete_user(UserName="aad-filter-user")
        iam.delete_role(RoleName="aad-filter-role")
# ── Virtual MFA devices ───────────────────────────────────────────────


def test_iam_create_virtual_mfa(iam):
    device_name = "mfa-device-create-test"
    resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=device_name)
    dev = resp["VirtualMFADevice"]
    assert dev["SerialNumber"].endswith(f":mfa/{device_name}")
    assert len(dev["Base32StringSeed"]) > 0
    iam.delete_virtual_mfa_device(SerialNumber=dev["SerialNumber"])


def test_iam_enable_and_list_mfa(iam):
    user_name = "mfa-user-enable-list"
    device_name = "mfa-device-enable-list"
    iam.create_user(UserName=user_name)
    dev_resp = iam.create_virtual_mfa_device(VirtualMFADeviceName=device_name)
    serial = dev_resp["VirtualMFADevice"]["SerialNumber"]
    try:
        iam.enable_mfa_device(
            UserName=user_name,
            SerialNumber=serial,
            AuthenticationCode1="123456",
            AuthenticationCode2="234567",
        )
        resp = iam.list_mfa_devices(UserName=user_name)
        devices = resp["MFADevices"]
        assert any(d["SerialNumber"] == serial for d in devices)
        assert all("EnableDate" in d for d in devices if d["SerialNumber"] == serial)
    finally:
        try:
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
        except Exception:
            pass
        try:
            iam.delete_virtual_mfa_device(SerialNumber=serial)
        except Exception:
            pass
        iam.delete_user(UserName=user_name)


def test_iam_list_virtual_mfa_assignment_filter(iam):
    user_name = "mfa-user-filter"
    iam.create_user(UserName=user_name)
    d1 = iam.create_virtual_mfa_device(VirtualMFADeviceName="mfa-filter-assigned")["VirtualMFADevice"]["SerialNumber"]
    d2 = iam.create_virtual_mfa_device(VirtualMFADeviceName="mfa-filter-unassigned")["VirtualMFADevice"]["SerialNumber"]
    try:
        iam.enable_mfa_device(UserName=user_name, SerialNumber=d1,
                              AuthenticationCode1="111111", AuthenticationCode2="222222")

        # default (Assigned) returns only assigned
        assigned_serials = {d["SerialNumber"] for d in
                            iam.list_virtual_mfa_devices()["VirtualMFADevices"]}
        assert d1 in assigned_serials
        assert d2 not in assigned_serials

        # Unassigned returns only free device
        unassigned_serials = {d["SerialNumber"] for d in
                              iam.list_virtual_mfa_devices(AssignmentStatus="Unassigned")["VirtualMFADevices"]}
        assert d2 in unassigned_serials
        assert d1 not in unassigned_serials

        # Any returns both
        any_serials = {d["SerialNumber"] for d in
                       iam.list_virtual_mfa_devices(AssignmentStatus="Any")["VirtualMFADevices"]}
        assert d1 in any_serials
        assert d2 in any_serials
    finally:
        for serial, uname in [(d1, user_name), (d2, None)]:
            if uname:
                try:
                    iam.deactivate_mfa_device(UserName=uname, SerialNumber=serial)
                except Exception:
                    pass
            try:
                iam.delete_virtual_mfa_device(SerialNumber=serial)
            except Exception:
                pass
        iam.delete_user(UserName=user_name)


def test_iam_deactivate_mfa(iam):
    user_name = "mfa-user-deactivate"
    device_name = "mfa-device-deactivate"
    iam.create_user(UserName=user_name)
    serial = iam.create_virtual_mfa_device(VirtualMFADeviceName=device_name)["VirtualMFADevice"]["SerialNumber"]
    try:
        iam.enable_mfa_device(UserName=user_name, SerialNumber=serial,
                              AuthenticationCode1="111111", AuthenticationCode2="222222")
        iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)

        # no devices for user after deactivate
        devices = iam.list_mfa_devices(UserName=user_name)["MFADevices"]
        assert not any(d["SerialNumber"] == serial for d in devices)

        # device should appear in Unassigned list
        unassigned = {d["SerialNumber"] for d in
                      iam.list_virtual_mfa_devices(AssignmentStatus="Unassigned")["VirtualMFADevices"]}
        assert serial in unassigned
    finally:
        try:
            iam.delete_virtual_mfa_device(SerialNumber=serial)
        except Exception:
            pass
        iam.delete_user(UserName=user_name)


def test_iam_delete_assigned_mfa_conflict(iam):
    user_name = "mfa-user-conflict"
    device_name = "mfa-device-conflict"
    iam.create_user(UserName=user_name)
    serial = iam.create_virtual_mfa_device(VirtualMFADeviceName=device_name)["VirtualMFADevice"]["SerialNumber"]
    try:
        iam.enable_mfa_device(UserName=user_name, SerialNumber=serial,
                              AuthenticationCode1="111111", AuthenticationCode2="222222")
        with pytest.raises(iam.exceptions.DeleteConflictException):
            iam.delete_virtual_mfa_device(SerialNumber=serial)
    finally:
        try:
            iam.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
        except Exception:
            pass
        try:
            iam.delete_virtual_mfa_device(SerialNumber=serial)
        except Exception:
            pass
        iam.delete_user(UserName=user_name)

# ── Login profiles ────────────────────────────────────────────────────


def test_iam_create_get_login_profile(iam):
    name = "lp-user-create-get"
    iam.create_user(UserName=name)
    try:
        resp = iam.create_login_profile(UserName=name, Password="Test1234!", PasswordResetRequired=True)
        profile = resp["LoginProfile"]
        assert profile["UserName"] == name
        assert "CreateDate" in profile
        assert profile["PasswordResetRequired"] is True

        resp2 = iam.get_login_profile(UserName=name)
        assert resp2["LoginProfile"]["UserName"] == name
        assert "CreateDate" in resp2["LoginProfile"]
    finally:
        try:
            iam.delete_login_profile(UserName=name)
        except Exception:
            pass
        iam.delete_user(UserName=name)


def test_iam_get_login_profile_absent(iam):
    name = "lp-user-absent"
    iam.create_user(UserName=name)
    try:
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            iam.get_login_profile(UserName=name)
    finally:
        iam.delete_user(UserName=name)


def test_iam_create_login_profile_no_user(iam):
    with pytest.raises(iam.exceptions.NoSuchEntityException):
        iam.create_login_profile(UserName="lp-ghost-user-xyz", Password="Test1234!")


def test_iam_delete_login_profile(iam):
    name = "lp-user-delete"
    iam.create_user(UserName=name)
    try:
        iam.create_login_profile(UserName=name, Password="Test1234!")
        iam.delete_login_profile(UserName=name)
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            iam.get_login_profile(UserName=name)
    finally:
        try:
            iam.delete_login_profile(UserName=name)
        except Exception:
            pass
        iam.delete_user(UserName=name)


# ── Credential report ─────────────────────────────────────────────────

_CRED_REPORT_COLUMNS = (
    "user,arn,user_creation_time,password_enabled,password_last_used,"
    "password_last_changed,password_next_rotation,mfa_active,"
    "access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date,"
    "access_key_1_last_used_region,access_key_1_last_used_service,"
    "access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date,"
    "access_key_2_last_used_region,access_key_2_last_used_service,"
    "cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"
)


def test_iam_credential_report_get_before_generate(iam):
    with pytest.raises(Exception) as exc_info:
        iam.get_credential_report()
    assert exc_info.value.response["Error"]["Code"] == "ReportNotPresent"


def test_iam_credential_report_mfa_and_password(iam):
    user_a = "cr-user-a-mfapw"
    user_b = "cr-user-b-neither"
    device_name = "cr-mfa-device"

    iam.create_user(UserName=user_a)
    iam.create_user(UserName=user_b)
    iam.create_login_profile(UserName=user_a, Password="Test1234!")
    serial = iam.create_virtual_mfa_device(VirtualMFADeviceName=device_name)["VirtualMFADevice"]["SerialNumber"]
    iam.enable_mfa_device(UserName=user_a, SerialNumber=serial,
                          AuthenticationCode1="111111", AuthenticationCode2="222222")
    try:
        iam.generate_credential_report()
        resp = iam.get_credential_report()
        csv_bytes = resp["Content"]
        csv_text = csv_bytes.decode("utf-8") if isinstance(csv_bytes, (bytes, bytearray)) else csv_bytes
        rows = {r.split(",")[0]: r.split(",") for r in csv_text.strip().splitlines()[1:]}

        # user A: password_enabled=true, mfa_active=true
        assert rows[user_a][3] == "true", f"Expected password_enabled=true for {user_a}"
        assert rows[user_a][7] == "true", f"Expected mfa_active=true for {user_a}"

        # user B: password_enabled=false, mfa_active=false
        assert rows[user_b][3] == "false", f"Expected password_enabled=false for {user_b}"
        assert rows[user_b][7] == "false", f"Expected mfa_active=false for {user_b}"
    finally:
        try:
            iam.deactivate_mfa_device(UserName=user_a, SerialNumber=serial)
        except Exception:
            pass
        try:
            iam.delete_virtual_mfa_device(SerialNumber=serial)
        except Exception:
            pass
        try:
            iam.delete_login_profile(UserName=user_a)
        except Exception:
            pass
        iam.delete_user(UserName=user_a)
        iam.delete_user(UserName=user_b)


def test_iam_credential_report_header(iam):
    iam.generate_credential_report()
    resp = iam.get_credential_report()
    csv_bytes = resp["Content"]
    csv_text = csv_bytes.decode("utf-8") if isinstance(csv_bytes, (bytes, bytearray)) else csv_bytes
    lines = csv_text.strip().splitlines()
    assert lines[0] == _CRED_REPORT_COLUMNS
    user_col = [r.split(",")[0] for r in lines]
    assert "<root_account>" in user_col
# ── Account posture (summary / password policy / aliases) ─────────────


def test_iam_password_policy_absent_then_set(iam):
    # First, delete any existing policy to ensure clean state (serial test)
    try:
        iam.delete_account_password_policy()
    except Exception:
        pass
    with pytest.raises(iam.exceptions.NoSuchEntityException):
        iam.get_account_password_policy()
    iam.update_account_password_policy(MinimumPasswordLength=14)
    resp = iam.get_account_password_policy()
    assert resp["PasswordPolicy"]["MinimumPasswordLength"] == 14
    iam.delete_account_password_policy()


def test_iam_account_summary_counts(iam):
    resp = iam.get_account_summary()
    sm = resp["SummaryMap"]
    assert "Users" in sm
    assert "MFADevices" in sm
    assert "AccountMFAEnabled" in sm
    assert isinstance(sm["Users"], int)


def test_iam_account_alias_crud(iam):
    alias = "my-test-alias-acct"
    iam.create_account_alias(AccountAlias=alias)
    aliases = iam.list_account_aliases()["AccountAliases"]
    assert alias in aliases
    iam.delete_account_alias(AccountAlias=alias)
    aliases_after = iam.list_account_aliases()["AccountAliases"]
    assert alias not in aliases_after


# -------------------- Service-specific credentials --------------------

def test_iam_service_specific_credential_lifecycle(iam):
    iam.create_user(UserName="ssc-user")
    created = iam.create_service_specific_credential(
        UserName="ssc-user", ServiceName="bedrock.amazonaws.com"
    )["ServiceSpecificCredential"]
    assert created["ServiceSpecificCredentialId"].startswith("ACCA")
    assert created["ServiceName"] == "bedrock.amazonaws.com"
    assert created["Status"] == "Active"
    assert created["ServiceCredentialSecret"]
    cred_id = created["ServiceSpecificCredentialId"]

    listed = iam.list_service_specific_credentials(UserName="ssc-user")["ServiceSpecificCredentials"]
    assert len(listed) == 1
    assert listed[0]["ServiceSpecificCredentialId"] == cred_id
    # AWS only exposes the secret on create/reset, never on list
    assert "ServiceCredentialSecret" not in listed[0]

    iam.update_service_specific_credential(
        UserName="ssc-user", ServiceSpecificCredentialId=cred_id, Status="Inactive"
    )
    listed = iam.list_service_specific_credentials(UserName="ssc-user")["ServiceSpecificCredentials"]
    assert listed[0]["Status"] == "Inactive"

    reset = iam.reset_service_specific_credential(
        UserName="ssc-user", ServiceSpecificCredentialId=cred_id
    )["ServiceSpecificCredential"]
    assert reset["ServiceCredentialSecret"] != created["ServiceCredentialSecret"]
    assert reset["Status"] == "Inactive"

    iam.delete_service_specific_credential(UserName="ssc-user", ServiceSpecificCredentialId=cred_id)
    assert iam.list_service_specific_credentials(UserName="ssc-user")["ServiceSpecificCredentials"] == []


def test_iam_service_specific_credential_service_name_filter(iam):
    iam.create_user(UserName="ssc-filter-user")
    iam.create_service_specific_credential(UserName="ssc-filter-user", ServiceName="a.amazonaws.com")
    iam.create_service_specific_credential(UserName="ssc-filter-user", ServiceName="b.amazonaws.com")
    filtered = iam.list_service_specific_credentials(
        UserName="ssc-filter-user", ServiceName="a.amazonaws.com"
    )["ServiceSpecificCredentials"]
    assert len(filtered) == 1
    assert filtered[0]["ServiceName"] == "a.amazonaws.com"


def test_iam_service_specific_credential_age_days(iam):
    iam.create_user(UserName="ssc-age-user")
    cred = iam.create_service_specific_credential(
        UserName="ssc-age-user", ServiceName="bedrock.amazonaws.com", CredentialAgeDays=30
    )["ServiceSpecificCredential"]
    assert "ExpirationDate" in cred


def test_iam_service_specific_credential_unknown_user(iam):
    with pytest.raises(ClientError) as exc:
        iam.list_service_specific_credentials(UserName="ghost-user-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_service_specific_credential_username_required_for_non_user(iam):
    # The default test credentials do not resolve to an IAM user, so AWS
    # requires UserName explicitly (real AWS: InvalidInput, 400).
    with pytest.raises(ClientError) as exc:
        iam.list_service_specific_credentials()
    assert exc.value.response["Error"]["Code"] == "InvalidInput"


def test_iam_service_specific_credential_unknown_id(iam):
    iam.create_user(UserName="ssc-noid-user")
    with pytest.raises(ClientError) as exc:
        iam.update_service_specific_credential(
            UserName="ssc-noid-user", ServiceSpecificCredentialId="ACCAZZZZZZZZZZZZZZZZZ",
            Status="Inactive"
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_service_specific_credential_caller_user_inferred(iam):
    # An IAM user calling with its own access key may omit UserName; AWS takes
    # it from the caller identity (real AWS observation).
    import boto3
    from conftest import ENDPOINT, REGION

    iam.create_user(UserName="ssc-caller")
    key = iam.create_access_key(UserName="ssc-caller")["AccessKey"]
    caller_iam = boto3.client(
        "iam",
        endpoint_url=ENDPOINT,
        aws_access_key_id=key["AccessKeyId"],
        aws_secret_access_key=key["SecretAccessKey"],
        region_name=REGION,
    )
    # botocore requires UserName on create client-side; list sends it bare.
    iam.create_service_specific_credential(UserName="ssc-caller", ServiceName="bedrock.amazonaws.com")
    listed = caller_iam.list_service_specific_credentials()["ServiceSpecificCredentials"]
    assert len(listed) == 1
    assert listed[0]["UserName"] == "ssc-caller"


# -------------------- Server certificates --------------------

def _self_signed_pem():
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ministack-test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30))
        .sign(key, hashes.SHA256())
    )
    return (
        cert.public_bytes(serialization.Encoding.PEM).decode(),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ).decode(),
    )


def test_iam_server_certificate_lifecycle(iam):
    cert_pem, key_pem = _self_signed_pem()
    md = iam.upload_server_certificate(
        ServerCertificateName="svc-cert", Path="/cloudfront/prod/",
        CertificateBody=cert_pem, PrivateKey=key_pem,
        Tags=[{"Key": "env", "Value": "test"}],
    )["ServerCertificateMetadata"]
    assert md["ServerCertificateId"].startswith("ASCA")
    assert md["Path"] == "/cloudfront/prod/"
    assert md["Arn"].endswith(":server-certificate/cloudfront/prod/svc-cert")
    assert "Expiration" in md

    listed = iam.list_server_certificates()["ServerCertificateMetadataList"]
    assert [m["ServerCertificateName"] for m in listed] == ["svc-cert"]

    got = iam.get_server_certificate(ServerCertificateName="svc-cert")["ServerCertificate"]
    assert "BEGIN CERTIFICATE" in got["CertificateBody"]

    tags = iam.list_server_certificate_tags(ServerCertificateName="svc-cert")["Tags"]
    assert {"Key": "env", "Value": "test"} in tags

    iam.update_server_certificate(
        ServerCertificateName="svc-cert", NewPath="/cdn/", NewServerCertificateName="svc-cert-2"
    )
    listed = iam.list_server_certificates()["ServerCertificateMetadataList"]
    assert [(m["ServerCertificateName"], m["Path"]) for m in listed] == [("svc-cert-2", "/cdn/")]

    iam.delete_server_certificate(ServerCertificateName="svc-cert-2")
    assert iam.list_server_certificates()["ServerCertificateMetadataList"] == []


def test_iam_server_certificate_path_normalized(iam):
    cert_pem, key_pem = _self_signed_pem()
    md = iam.upload_server_certificate(
        ServerCertificateName="bare-cert", CertificateBody=cert_pem, PrivateKey=key_pem
    )["ServerCertificateMetadata"]
    assert md["Path"] == "/"


def test_iam_server_certificate_malformed(iam):
    with pytest.raises(ClientError) as exc:
        iam.upload_server_certificate(
            ServerCertificateName="bad-cert",
            CertificateBody="-----BEGIN CERTIFICATE-----\nnot-a-cert\n-----END CERTIFICATE-----",
            PrivateKey="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
        )
    assert exc.value.response["Error"]["Code"] == "MalformedCertificate"


def test_iam_server_certificate_unknown(iam):
    with pytest.raises(ClientError) as exc:
        iam.get_server_certificate(ServerCertificateName="ghost-cert-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"
