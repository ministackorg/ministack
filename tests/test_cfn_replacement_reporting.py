"""Replacement reporting checked against AWS change sets and resource schemas."""

import json
import uuid

import pytest

from ministack.services.cloudformation.provisioners import (
    _CONDITIONALLY_REPLACING_PROPERTIES,
    _REPLACING_PROPERTIES,
    _RESOURCE_HANDLERS,
)
from ministack.services.cloudformation.stacks import _diff_resources


def _template(resource_type, properties):
    return {"Resources": {"R": {"Type": resource_type, "Properties": properties}}}


@pytest.fixture
def stack(cfn):
    names = []

    def create(template):
        name = f"repl-{uuid.uuid4().hex[:10]}"
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
        names.append(name)
        cfn.get_waiter("stack_create_complete").wait(
            StackName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        return name

    yield create
    for name in names:
        cfn.delete_stack(StackName=name)
        cfn.get_waiter("stack_delete_complete").wait(
            StackName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})


def _change(cfn, name, template):
    cfn.create_change_set(StackName=name, ChangeSetName="cs", ChangeSetType="UPDATE",
                          TemplateBody=json.dumps(template))
    cfn.get_waiter("change_set_create_complete").wait(
        StackName=name, ChangeSetName="cs", WaiterConfig={"Delay": 1, "MaxAttempts": 30})
    return cfn.describe_change_set(StackName=name, ChangeSetName="cs")["Changes"][0]["ResourceChange"]


def _requirements(change):
    return {d["Target"]["Name"]: d["Target"]["RequiresRecreation"]
            for d in change.get("Details", []) if d["Target"].get("Name")}


# These are independent AWS observations, including negative cases where
# service API immutability must not be turned into an Always result.
@pytest.mark.parametrize("rtype,old,new,expected", [
    ("AWS::Cognito::UserPool", {"UserPoolName": "before"}, {"UserPoolName": "after"}, "Never"),
    ("AWS::Cognito::UserPool", {}, {"Schema": [{"Name": "review", "AttributeDataType": "String"}]}, "Never"),
    ("AWS::IoT::ThingType",
     {"ThingTypeProperties": {"Mqtt5Configuration": {"PropagatingAttributes": []}}},
     {"ThingTypeProperties": {"Mqtt5Configuration": {"PropagatingAttributes": [{"UserPropertyKey": "k", "ThingAttribute": "v"}]}}}, "Never"),
    ("AWS::DynamoDB::Table",
     {"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]},
     {"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "N"}]}, "Never"),
    ("AWS::DynamoDB::Table", {}, {"ImportSourceSpecification": {"InputFormat": "CSV"}}, "Always"),
    ("AWS::DynamoDB::Table", {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]},
     {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}]},
     "Conditionally"),
    ("AWS::Lambda::Function", {"DurableConfig": {"ExecutionTimeout": 60}},
     {"DurableConfig": {"ExecutionTimeout": 120}}, "Conditionally"),
    ("AWS::Lambda::Function", {"DurableConfig": {"ExecutionTimeout": 60}}, {}, "Conditionally"),
    ("AWS::SQS::Queue", {"QueueName": "before"}, {"QueueName": "after"}, "Always"),
    ("AWS::SQS::Queue", {"FifoQueue": False}, {"FifoQueue": True}, "Always"),
    ("AWS::SQS::Queue", {"DelaySeconds": 0}, {"DelaySeconds": 5}, "Never"),
    ("AWS::SSM::Parameter", {"Value": "before"}, {"Value": "after"}, "Never"),
    ("AWS::SNS::Topic", {"FifoTopic": False}, {"FifoTopic": True}, "Always"),
    # Fixed at CreateUserPool by the service API, yet reported in place.
    ("AWS::Cognito::UserPool", {}, {"AliasAttributes": ["email"]}, "Never"),
    ("AWS::Lambda::LayerVersion", {"Content": {"S3Bucket": "layers", "S3Key": "a.zip"}},
     {"Content": {"S3Bucket": "layers", "S3Key": "b.zip"}}, "Always"),
])
def test_property_reporting(rtype, old, new, expected):
    change = _diff_resources(_template(rtype, old), _template(rtype, new))[0]["ResourceChange"]
    assert set(_requirements(change).values()) == {expected}
    assert change["Replacement"] == {"Never": "False", "Always": "True", "Conditionally": "Conditional"}[expected]


def test_adding_a_dynamodb_index_does_not_report_replacement():
    old = {"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]}
    new = {
        "AttributeDefinitions": [*old["AttributeDefinitions"], {"AttributeName": "gpk", "AttributeType": "S"}],
        "GlobalSecondaryIndexes": [{"IndexName": "review", "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                                    "Projection": {"ProjectionType": "KEYS_ONLY"}}],
    }
    change = _diff_resources(_template("AWS::DynamoDB::Table", old), _template("AWS::DynamoDB::Table", new))[0]["ResourceChange"]
    assert change["Replacement"] == "False"
    assert _requirements(change) == {"AttributeDefinitions": "Never", "GlobalSecondaryIndexes": "Never"}


# Measured AWS change sets: a listed type reports every property its schema
# does not mark create-only or conditionally create-only as Never.
@pytest.mark.parametrize("rtype,old,new", [
    ("AWS::DynamoDB::Table", {}, {"TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": True}}),
    ("AWS::DynamoDB::Table", {}, {"Tags": [{"Key": "review", "Value": "1"}]}),
    ("AWS::Lambda::Function", {"Timeout": 30}, {"Timeout": 45}),
    ("AWS::StepFunctions::StateMachine", {"DefinitionString": "a"}, {"DefinitionString": "b"}),
    ("AWS::Cognito::UserPoolGroup", {"Description": "a"}, {"Description": "b"}),
    ("AWS::Cognito::UserPoolResourceServer", {"Name": "a"}, {"Name": "b"}),
    ("AWS::IoT::ThingGroup", {"ThingGroupProperties": {"ThingGroupDescription": "a"}},
     {"ThingGroupProperties": {"ThingGroupDescription": "b"}}),
    ("AWS::IoT::ProvisioningTemplate", {"Description": "a"}, {"Description": "b"}),
    ("AWS::IAM::InstanceProfile", {"Roles": ["a"]}, {"Roles": ["b"]}),
    ("AWS::CloudFront::Function", {"FunctionCode": "a"}, {"FunctionCode": "b"}),
    ("AWS::Backup::BackupVault", {}, {"AccessPolicy": {"Version": "2012-10-17", "Statement": []}}),
    ("AWS::Location::Tracker", {"Description": "a"}, {"Description": "b"}),
    ("AWS::ElasticLoadBalancingV2::TargetGroup", {},
     {"TargetGroupAttributes": [{"Key": "lambda.multi_value_headers.enabled", "Value": "true"}]}),
])
def test_listed_types_report_other_properties_in_place(rtype, old, new):
    change = _diff_resources(_template(rtype, old), _template(rtype, new))[0]["ResourceChange"]
    assert set(_requirements(change).values()) == {"Never"}
    assert change["Replacement"] == "False"


def test_conditional_and_in_place_properties_report_together():
    # KeySchema is conditionally create-only in the DynamoDB schema, so the
    # change stays Conditional next to an in-place AttributeDefinitions edit.
    rtype = "AWS::DynamoDB::Table"
    attributes = [{"AttributeName": "pk", "AttributeType": "S"}]
    keys = [{"AttributeName": "pk", "KeyType": "HASH"}]
    old = _template(rtype, {"AttributeDefinitions": attributes, "KeySchema": keys})
    new = _template(rtype, {
        "AttributeDefinitions": [*attributes, {"AttributeName": "sk", "AttributeType": "S"}],
        "KeySchema": [*keys, {"AttributeName": "sk", "KeyType": "RANGE"}],
    })
    change = _diff_resources(old, new)[0]["ResourceChange"]
    assert change["Replacement"] == "Conditional"
    assert _requirements(change) == {"AttributeDefinitions": "Never", "KeySchema": "Conditionally"}


def test_unknown_resource_type_keeps_conditional():
    change = _diff_resources(_template("Custom::Example", {"Value": "a"}),
                             _template("Custom::Example", {"Value": "b"}))[0]["ResourceChange"]
    assert change["Replacement"] == "Conditional"


def test_policy_only_edit_does_not_replace():
    old = _template("AWS::SSM::Parameter", {"Type": "String", "Value": "value"})
    new = _template("AWS::SSM::Parameter", {"Type": "String", "Value": "value"})
    new["Resources"]["R"]["UpdateReplacePolicy"] = "Retain"
    change = _diff_resources(old, new)[0]["ResourceChange"]
    assert change["Replacement"] == "False"
    assert change["Scope"] == ["UpdateReplacePolicy"]


def test_reporting_tables_are_consistent():
    assert _REPLACING_PROPERTIES.keys() <= _RESOURCE_HANDLERS.keys()
    # A conditional row for an unlisted type would never be read.
    assert _CONDITIONALLY_REPLACING_PROPERTIES.keys() <= _REPLACING_PROPERTIES.keys()
    for rtype, names in _CONDITIONALLY_REPLACING_PROPERTIES.items():
        assert names
        assert not set(names).intersection(_REPLACING_PROPERTIES[rtype])


@pytest.mark.parametrize("rename,value", [(True, "a"), (False, "b"), (True, "b")])
def test_ssm_change_set_reports_each_property(cfn, stack, rename, value):
    pname = f"/repl/{uuid.uuid4().hex[:10]}"
    old = _template("AWS::SSM::Parameter", {"Name": pname, "Type": "String", "Value": "a"})
    new = _template("AWS::SSM::Parameter", {"Name": pname + "-new" if rename else pname, "Type": "String", "Value": value})
    change = _change(cfn, stack(old), new)
    expected = {}
    if rename:
        expected["Name"] = "Always"
    if value != "a":
        expected["Value"] = "Never"
    assert _requirements(change) == expected
    assert change["Replacement"] == ("True" if rename else "False")


@pytest.mark.parametrize("prop,value,recreation,replacement", [
    ("TopicName", None, "Always", "True"),
    ("DisplayName", "review", "Never", "False"),
])
def test_sns_change_set_reports_name_and_display_name(cfn, stack, prop, value,
                                                      recreation, replacement):
    name = f"repl-{uuid.uuid4().hex[:10]}"
    old = _template("AWS::SNS::Topic", {"TopicName": name})
    new = _template("AWS::SNS::Topic", {"TopicName": name, prop: value or name + "-new"})
    change = _change(cfn, stack(old), new)
    assert _requirements(change) == {prop: recreation}
    assert change["Replacement"] == replacement


def test_cognito_change_set_reports_mfa_configuration_in_place(cfn, stack):
    name = f"repl-{uuid.uuid4().hex[:10]}"
    old = _template("AWS::Cognito::UserPool", {"UserPoolName": name})
    new = _template("AWS::Cognito::UserPool", {"UserPoolName": name, "MfaConfiguration": "OFF"})
    change = _change(cfn, stack(old), new)
    assert _requirements(change) == {"MfaConfiguration": "Never"}
    assert change["Replacement"] == "False"


_PASS = json.dumps({"StartAt": "A", "States": {"A": {"Type": "Pass", "End": True}}})
_ROLE = "arn:aws:iam::000000000000:role/review"


@pytest.mark.parametrize("rtype,props,prop,value", [
    ("AWS::DynamoDB::Table", {"BillingMode": "PAY_PER_REQUEST",
                              "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                              "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]},
     "Tags", [{"Key": "review", "Value": "1"}]),
    ("AWS::Lambda::Function", {"Runtime": "python3.13", "Handler": "index.handler", "Timeout": 30,
                               "Role": _ROLE, "Code": {"ZipFile": "def handler(event, context):\n    return {}\n"}},
     "Timeout", 45),
    ("AWS::StepFunctions::StateMachine", {"RoleArn": _ROLE, "DefinitionString": _PASS},
     "DefinitionString", _PASS.replace('"A"', '"B"')),
], ids=["dynamodb-tags", "lambda-timeout", "sfn-definition"])
def test_change_set_reports_other_properties_in_place(cfn, stack, rtype, props, prop, value):
    old = _template(rtype, props)
    new = _template(rtype, {**props, prop: value})
    change = _change(cfn, stack(old), new)
    assert _requirements(change) == {prop: "Never"}
    assert change["Replacement"] == "False"


def test_sqs_change_set_reports_the_issue_reproduction(cfn, stack):
    name = f"repl-{uuid.uuid4().hex[:10]}"
    old = _template("AWS::SQS::Queue", {"QueueName": name})
    new = _template("AWS::SQS::Queue", {"QueueName": name + "-new", "DelaySeconds": 5})
    change = _change(cfn, stack(old), new)
    assert change["Replacement"] == "True"
    assert _requirements(change) == {"QueueName": "Always", "DelaySeconds": "Never"}
