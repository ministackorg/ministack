"""IAM tagging guide / API references and MiniStack tests, not real AWS runs."""

import copy
import json
import uuid

import pytest
from botocore.exceptions import ClientError
from test_cfn import _delete_cfn_test_stack, _wait_stack

from ministack.services.cloudformation.provisioners import _reconcile_tag_list, _with_stack_tags

_TRUST = {"Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                         "Action": "sts:AssumeRole"}]}


def _tags(tags):
    result = {t["Key"]: t["Value"] for t in tags}
    assert len(result) == len(tags), f"Duplicate tag keys in response: {tags}"
    return result


def _custom_tags(tags):
    return _tags([t for t in tags if not t["Key"].lower().startswith("aws:")])


@pytest.mark.parametrize("resource", ["user", "role"])
def test_iam_identity_tag_keys_ignore_case_and_preserve_spelling(iam, resource):
    name = f"tag-case-{resource}-{uuid.uuid4().hex[:8]}"
    params = {f"{resource.title()}Name": name}
    create_params = {"AssumeRolePolicyDocument": json.dumps(_TRUST)} if resource == "role" else {}
    original = [{"Key": "Department", "Value": "Finance"}, {"Key": "ExternalKey", "Value": "Keep"}]
    getattr(iam, f"create_{resource}")(**params, **create_params, Tags=original)
    try:
        assert _tags(getattr(iam, f"get_{resource}")(**params)[resource.title()]["Tags"]) == _tags(original)
        # Untag must match both the original spelling and a subsequently changed spelling.
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["DEPARTMENT"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [original[1]]
        getattr(iam, f"tag_{resource}")(**params, Tags=[original[0]])
        # A spelling-only update and a case-sensitive value change must both take effect.
        for key, value in [("department", "Finance"), ("DEPARTMENT", "finance")]:
            getattr(iam, f"tag_{resource}")(**params, Tags=[{"Key": key, "Value": value}])
            expected = {key: value, "ExternalKey": "Keep"}
            assert _tags(getattr(iam, f"get_{resource}")(**params)[resource.title()]["Tags"]) == expected
            assert _tags(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == expected
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["DePaRtMeNt", "missing"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [original[1]]
    finally:
        getattr(iam, f"delete_{resource}")(**params)


@pytest.mark.parametrize("resource", ["user", "role"])
@pytest.mark.parametrize("operation", ["create", "tag"])
@pytest.mark.parametrize("second_key", ["Department", "department"])
def test_iam_identity_duplicate_tag_keys_reject_entire_request(iam, resource, operation, second_key):
    params = {f"{resource.title()}Name": f"tag-duplicate-{resource}-{uuid.uuid4().hex[:8]}"}
    create_params = {"AssumeRolePolicyDocument": json.dumps(_TRUST)} if resource == "role" else {}
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
    name = f"tag-case-{resource}-{uuid.uuid4().hex[:8]}"
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
        assert _tags(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == _tags(tags)
        getattr(iam, f"untag_{resource}")(**params, TagKeys=["Costcenter"])
        assert getattr(iam, f"list_{resource}_tags")(**params)["Tags"] == [tags[1]]
    finally:
        getattr(iam, f"delete_{resource}")(**params)


def _identity_template(resource, name, tags, after=False):
    props = {f"{resource.title()}Name": name, "Tags": tags}
    if resource == "role":
        props.update(AssumeRolePolicyDocument=_TRUST, Description="After" if after else "Before")
    else:
        props["Path"] = "/after/" if after else "/before/"
    return json.dumps({"Resources": {"Identity": {"Type": f"AWS::IAM::{resource.title()}", "Properties": props}}})


@pytest.mark.parametrize("resource", ["user", "role"])
@pytest.mark.parametrize("source", ["template", "stack"])
def test_cfn_iam_identity_tag_case_update_and_removal(cfn, iam, resource, source):
    name = f"cfn-{resource}-tag-case-{uuid.uuid4().hex[:8]}"
    params = {f"{resource.title()}Name": name}
    original_tag = {"Key": "Department", "Value": "Finance"}
    body = _identity_template(resource, name, [original_tag] if source == "template" else [])
    cfn.create_stack(StackName=name, TemplateBody=body, Capabilities=["CAPABILITY_NAMED_IAM"],
                     Tags=[original_tag] if source == "stack" else [])
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        before = getattr(iam, f"get_{resource}")(**params)[resource.title()]
        assert _custom_tags(before["Tags"]) == {"Department": "Finance"}
        # First change only the key's spelling, then change its value too.
        for value in ["Finance", "HR"]:
            desired_tags = [{"Key": "department", "Value": value}, {"Key": "Added", "Value": "New"}]
            body = _identity_template(resource, name, desired_tags if source == "template" else [])
            cfn.update_stack(StackName=name, TemplateBody=body,
                             Tags=desired_tags if source == "stack" else [], Capabilities=["CAPABILITY_NAMED_IAM"])
            assert _wait_stack(cfn, name)["StackStatus"] == "UPDATE_COMPLETE"
            identity = getattr(iam, f"get_{resource}")(**params)[resource.title()]
            assert identity[f"{resource.title()}Id"] == before[f"{resource.title()}Id"]
            assert _custom_tags(identity["Tags"]) == {"department": value, "Added": "New"}
        # Change a managed key's spelling externally; do not add unrelated external tags.
        getattr(iam, f"tag_{resource}")(**params, Tags=[{"Key": "DEPARTMENT", "Value": "ExternalEdit"}])
        assert _custom_tags(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == {
            "DEPARTMENT": "ExternalEdit", "Added": "New"}
        cfn.update_stack(StackName=name, TemplateBody=_identity_template(resource, name, []), Tags=[],
                         Capabilities=["CAPABILITY_NAMED_IAM"])
        assert _wait_stack(cfn, name)["StackStatus"] == "UPDATE_COMPLETE"
        identity = getattr(iam, f"get_{resource}")(**params)[resource.title()]
        assert identity[f"{resource.title()}Id"] == before[f"{resource.title()}Id"]
        assert _custom_tags(getattr(iam, f"list_{resource}_tags")(**params)["Tags"]) == {}
    finally:
        _delete_cfn_test_stack(cfn, name)


@pytest.mark.parametrize("resource", ["user", "role"])
def test_cfn_iam_identity_stack_tag_merge_preserves_resource_spelling(cfn, iam, resource):
    """At 50 custom tags, a case-insensitive merge must not create a 51st key.

    IAM allows 50 user/role tags; aws: tags do not count toward that limit:
    https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateUser.html
    https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html
    https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-resource-tags.html
    """
    name = f"cfn-{resource}-tag-merge-{uuid.uuid4().hex[:8]}"
    own = [{"Key": "department", "Value": "Resource"}, *[
        {"Key": f"Key-{i}", "Value": "Value"} for i in range(48)]]
    cfn.create_stack(StackName=name, TemplateBody=_identity_template(resource, name, own), Capabilities=["CAPABILITY_NAMED_IAM"],
                     Tags=[{"Key": "Department", "Value": "Stack"}, {"Key": "StackOnly", "Value": "Keep"}])
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        actual = getattr(iam, f"list_{resource}_tags")(**{f"{resource.title()}Name": name})["Tags"]
        assert [t for t in actual if t["Key"].lower() == "department"] == [own[0]]
        assert _custom_tags(actual) == {**_tags(own), "StackOnly": "Keep"}
    finally:
        _delete_cfn_test_stack(cfn, name)


@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize("resource", ["user", "role"])
def test_cfn_iam_identity_duplicate_tag_keys_leave_state_unchanged(cfn, iam, operation, resource):
    name = f"cfn-{resource}-tag-duplicate-{uuid.uuid4().hex[:8]}"
    params = {f"{resource.title()}Name": name}
    before = None
    if operation == "update":
        cfn.create_stack(StackName=name, TemplateBody=_identity_template(resource, name, [{"Key": "Department", "Value": "Finance"}]),
                         Capabilities=["CAPABILITY_NAMED_IAM"])
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        before = getattr(iam, f"get_{resource}")(**params)[resource.title()]
    try:
        # Both duplicate forms use the predicate covered by the IAM API tests.
        tags = [{"Key": "Department", "Value": "HR"}, {"Key": "department", "Value": "HR"}]
        getattr(cfn, f"{operation}_stack")(StackName=name, TemplateBody=_identity_template(resource, name, tags, after=True),
                                           Capabilities=["CAPABILITY_NAMED_IAM"])
        assert _wait_stack(cfn, name)["StackStatus"] == (
            "UPDATE_ROLLBACK_COMPLETE" if operation == "update" else "ROLLBACK_COMPLETE")
        if before is not None:
            assert getattr(iam, f"get_{resource}")(**params)[resource.title()] == before
        else:
            with pytest.raises(iam.exceptions.NoSuchEntityException):
                getattr(iam, f"get_{resource}")(**params)
    finally:
        _delete_cfn_test_stack(cfn, name)


@pytest.mark.parametrize("resource_type", ["AWS::SQS::Queue", "AWS::SSM::Parameter"])
def test_cfn_other_resource_stack_tag_merge_remains_case_sensitive(resource_type):
    # Protect the list/map branches of the shared merge without provisioning other services.
    props = {"Tags": [{"Key": "department", "Value": "Resource"}]} if resource_type == "AWS::SQS::Queue" else {
        "Tags": {"department": "Resource"}}
    original = copy.deepcopy(props)
    result = _with_stack_tags(resource_type, props, [{"Key": "Department", "Value": "Stack"}],
                              "stack-name", "stack-id", "Resource")
    tags = result["Tags"]
    actual = tags if isinstance(tags, dict) else _tags(tags)
    assert {k: v for k, v in actual.items() if not k.lower().startswith("aws:")} == {
        "Department": "Stack", "department": "Resource"}
    assert props == original


def test_cfn_other_resource_tag_reconciliation_remains_case_sensitive():
    store = [{"Key": "Department", "Value": "External"}, {"Key": "department", "Value": "Before"}]
    _reconcile_tag_list(store, {"Tags": [{"Key": "department", "Value": "Before"}]},
                        {"Tags": [{"Key": "DEPARTMENT", "Value": "After"}]})
    assert _tags(store) == {"Department": "External", "DEPARTMENT": "After"}
    _reconcile_tag_list(store, {"Tags": [{"Key": "DEPARTMENT", "Value": "After"}]}, {})
    assert _tags(store) == {"Department": "External"}
