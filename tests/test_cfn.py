import base64
import io
import json
import os
import re
import time
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

from ministack.services import pipes as _pipes


def _wait_stack(cfn, name, timeout=30):
    """Poll until stack reaches terminal status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")

_E2E_STACK = "e2e-test"

_E2E_TEMPLATE = """
AWSTemplateFormatVersion: '2010-09-09'
Description: E2E test stack — verifies CFN resources are functional

Parameters:
  Env:
    Type: String
    Default: e2etest

Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-${Env}-assets"

  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-${Env}-events"
      VisibilityTimeout: 120

  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${AWS::StackName}-${Env}-alerts"

  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-${Env}-role"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole

  Processor:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub "${AWS::StackName}-${Env}-processor"
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt Role.Arn
      Code:
        ZipFile: |
          def handler(event, context):
              return {"statusCode": 200}

  QueueUrlParam:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub "/${AWS::StackName}/${Env}/queue-url"
      Type: String
      Value: !Ref Queue

Outputs:
  BucketName:
    Value: !Ref Bucket
    Export:
      Name: !Sub "${AWS::StackName}-bucket"
  QueueUrl:
    Value: !Ref Queue
  TopicArn:
    Value: !Ref Topic
  ProcessorArn:
    Value: !GetAtt Processor.Arn
  RoleArn:
    Value: !GetAtt Role.Arn
"""

@pytest.fixture(scope="module")
def cfn_e2e_stack(cfn):
    """Deploy the e2e stack once for all e2e tests in this module."""
    # Clean up from a previous run
    try:
        cfn.delete_stack(StackName=_E2E_STACK)
        _wait_stack(cfn, _E2E_STACK)
    except Exception:
        pass

    cfn.create_stack(StackName=_E2E_STACK, TemplateBody=_E2E_TEMPLATE)
    s = _wait_stack(cfn, _E2E_STACK)
    assert s["StackStatus"] == "CREATE_COMPLETE", f"Stack failed: {s.get('StackStatusReason')}"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in s.get("Outputs", [])}
    yield outputs

    cfn.delete_stack(StackName=_E2E_STACK)
    _wait_stack(cfn, _E2E_STACK)

def test_cfn_create_describe_delete_stack(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t01-bucket"},
            }
        },
    }
    cfn.create_stack(StackName="cfn-t01", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-t01")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    s3.head_bucket(Bucket="cfn-t01-bucket")

    cfn.delete_stack(StackName="cfn-t01")
    _wait_stack(cfn, "cfn-t01")

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t01-bucket")

def test_cfn_stack_with_parameters(cfn, sqs):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "String",
                "Default": "cfn-t02-default",
            }
        },
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            }
        },
    }
    cfn.create_stack(StackName="cfn-t02a", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t02a")

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02-default").get("QueueUrls", [])
    assert any("cfn-t02-default" in u for u in urls)

    cfn.create_stack(
        StackName="cfn-t02b",
        TemplateBody=json.dumps(template),
        Parameters=[{"ParameterKey": "QueueName", "ParameterValue": "cfn-t02-custom"}],
    )
    _wait_stack(cfn, "cfn-t02b")

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02-custom").get("QueueUrls", [])
    assert any("cfn-t02-custom" in u for u in urls)

def test_cfn_change_set_use_previous_value_updates_resource(cfn, ssm):
    """A change set created with UsePreviousValue (the `aws cloudformation deploy`
    no-`--parameter-overrides` path) must resolve the parameter to its stored
    value, so a parameter-driven resource still updates rather than resolving to
    an empty value and missing the real resource (#897)."""
    def template(value):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Parameters": {"Prefix": {"Type": "String", "Default": "demo"}},
            "Resources": {"P": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": {"Fn::Sub": "/${Prefix}/config"},
                    "Type": "String",
                    "Value": value,
                },
            }},
        })

    cfn.create_stack(StackName="cfn-upv", TemplateBody=template("v1"))
    _wait_stack(cfn, "cfn-upv")
    assert ssm.get_parameter(Name="/demo/config")["Parameter"]["Value"] == "v1"

    # Change set re-sends Prefix as UsePreviousValue (what `deploy` does without
    # --parameter-overrides). Prefix must resolve to "demo", not "".
    cfn.create_change_set(
        StackName="cfn-upv", ChangeSetName="cs2", TemplateBody=template("v2"),
        Parameters=[{"ParameterKey": "Prefix", "UsePreviousValue": True}],
    )
    cfn.execute_change_set(StackName="cfn-upv", ChangeSetName="cs2")
    _wait_stack(cfn, "cfn-upv")

    assert ssm.get_parameter(Name="/demo/config")["Parameter"]["Value"] == "v2"

def test_cfn_intrinsic_ref_getatt(cfn, ssm):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cfn-t03-queue"},
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t03-param",
                    "Type": "String",
                    "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t03", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t03")

    val = ssm.get_parameter(Name="cfn-t03-param")["Parameter"]["Value"]
    assert val.startswith("arn:aws:sqs:")

def test_cfn_conditions(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "Create": {"Type": "String", "Default": "yes"},
        },
        "Conditions": {
            "ShouldCreate": {"Fn::Equals": [{"Ref": "Create"}, "yes"]},
        },
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Condition": "ShouldCreate",
                "Properties": {"BucketName": "cfn-t04-cond"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t04a", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t04a")
    s3.head_bucket(Bucket="cfn-t04-cond")

    # Delete first stack so the bucket name is freed
    cfn.delete_stack(StackName="cfn-t04a")
    _wait_stack(cfn, "cfn-t04a")

    cfn.create_stack(
        StackName="cfn-t04b",
        TemplateBody=json.dumps(template),
        Parameters=[{"ParameterKey": "Create", "ParameterValue": "no"}],
    )
    stack = _wait_stack(cfn, "cfn-t04b")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t04-cond")

def test_cfn_outputs_exports(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t05-exports"},
            },
        },
        "Outputs": {
            "BucketOut": {
                "Value": {"Ref": "Bucket"},
                "Export": {"Name": "cfn-t05-bucket-export"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t05", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t05")

    exports = cfn.list_exports()["Exports"]
    assert any(e["Name"] == "cfn-t05-bucket-export" for e in exports)


def test_cfn_kinesis_stream(cfn, kin):
    stream_name = "cfn-kinesis-cfn-test"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DataStream": {
                "Type": "AWS::Kinesis::Stream",
                "Properties": {
                    "Name": stream_name,
                    "ShardCount": 2,
                },
            },
        },
        "Outputs": {
            "StreamArn": {"Value": {"Fn::GetAtt": ["DataStream", "Arn"]}},
        },
    }
    cfn.create_stack(StackName="cfn-t-kinesis", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-t-kinesis")
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    desc = kin.describe_stream(StreamName=stream_name)
    assert desc["StreamDescription"]["StreamStatus"] == "ACTIVE"
    assert len(desc["StreamDescription"]["Shards"]) == 2

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["StreamArn"] == desc["StreamDescription"]["StreamARN"]

    cfn.delete_stack(StackName="cfn-t-kinesis")
    _wait_stack(cfn, "cfn-t-kinesis")

    with pytest.raises(ClientError):
        kin.describe_stream(StreamName=stream_name)


def test_cfn_fn_sub(cfn, ssm):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t06-src"},
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t06-param",
                    "Type": "String",
                    "Value": {"Fn::Sub": "${MyBucket}-replica"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t06", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t06")

    val = ssm.get_parameter(Name="cfn-t06-param")["Parameter"]["Value"]
    assert val == "cfn-t06-src-replica"

def test_cfn_multi_resource_dependencies(cfn, iam, lam):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "cfn-t07-role",
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                },
            },
            "Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "cfn-t07-func",
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                    "Code": {"ZipFile": "def handler(e,c): return {}"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t07", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t07")
    role = iam.get_role(RoleName="cfn-t07-role")["Role"]
    func = lam.get_function(FunctionName="cfn-t07-func")["Configuration"]
    assert func["Role"] == role["Arn"]

def test_cfn_change_set_lifecycle(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t08-cs"},
            },
        },
    }
    cfn.create_change_set(
        StackName="cfn-t08",
        ChangeSetName="cfn-t08-cs1",
        TemplateBody=json.dumps(template),
        ChangeSetType="CREATE",
    )
    time.sleep(1)

    cs = cfn.describe_change_set(StackName="cfn-t08", ChangeSetName="cfn-t08-cs1")
    assert cs["ChangeSetName"] == "cfn-t08-cs1"

    cfn.execute_change_set(StackName="cfn-t08", ChangeSetName="cfn-t08-cs1")
    stack = _wait_stack(cfn, "cfn-t08")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

def test_cfn_change_set_create_emits_review_event(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t08b-cs"},
            },
        },
    }
    cfn.create_change_set(
        StackName="cfn-t08b",
        ChangeSetName="cfn-t08b-cs1",
        TemplateBody=json.dumps(template),
        ChangeSetType="CREATE",
    )
    time.sleep(1)

    stack = cfn.describe_stacks(StackName="cfn-t08b")["Stacks"][0]
    assert stack["StackStatus"] == "REVIEW_IN_PROGRESS"

    events = cfn.describe_stack_events(StackName="cfn-t08b")["StackEvents"]
    assert len(events) > 0
    review = events[0]
    assert review["ResourceStatus"] == "REVIEW_IN_PROGRESS"
    assert review["ResourceType"] == "AWS::CloudFormation::Stack"
    assert review["LogicalResourceId"] == "cfn-t08b"

def test_cfn_update_stack(cfn, s3):
    template_v1 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "BucketA": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-a"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t09", TemplateBody=json.dumps(template_v1))
    _wait_stack(cfn, "cfn-t09")

    template_v2 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "BucketA": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-a"},
            },
            "BucketB": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-b"},
            },
        },
    }
    cfn.update_stack(StackName="cfn-t09", TemplateBody=json.dumps(template_v2))
    stack = _wait_stack(cfn, "cfn-t09")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    s3.head_bucket(Bucket="cfn-t09-a")
    s3.head_bucket(Bucket="cfn-t09-b")

def test_cfn_delete_nonexistent_stack(cfn):
    # AWS returns 200 for deleting non-existent stacks (idempotent)
    cfn.delete_stack(StackName="cfn-nonexistent-xyz")
    # But describing it should fail
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName="cfn-nonexistent-xyz")

def test_cfn_validate_template(cfn):
    valid_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "Env": {"Type": "String", "Default": "dev"},
        },
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t11-validate"},
            },
        },
    }
    result = cfn.validate_template(TemplateBody=json.dumps(valid_template))
    assert any(p["ParameterKey"] == "Env" for p in result["Parameters"])

    invalid_template = {"AWSTemplateFormatVersion": "2010-09-09"}
    with pytest.raises(ClientError):
        cfn.validate_template(TemplateBody=json.dumps(invalid_template))

def test_cfn_get_template_summary(cfn):
    # Basic template: parameters and resource types surfaced, no capabilities
    basic = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "summary test",
        "Parameters": {
            "Env": {"Type": "String", "Default": "dev", "Description": "env"},
        },
        "Resources": {
            "Bucket": {"Type": "AWS::S3::Bucket"},
        },
    }
    result = cfn.get_template_summary(TemplateBody=json.dumps(basic))
    assert result["Description"] == "summary test"
    assert "AWS::S3::Bucket" in result["ResourceTypes"]
    assert any(p["ParameterKey"] == "Env" for p in result["Parameters"])
    assert result.get("Capabilities", []) == []

    # IAM role with explicit RoleName → CAPABILITY_NAMED_IAM
    named_iam = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "my-role",
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                },
            }
        },
    }
    result = cfn.get_template_summary(TemplateBody=json.dumps(named_iam))
    assert "CAPABILITY_NAMED_IAM" in result["Capabilities"]
    assert result.get("CapabilitiesReason") == "The following resource(s) require capabilities: [AWS::IAM::Role]"

    # IAM role without explicit name → CAPABILITY_IAM
    unnamed_iam = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                },
            }
        },
    }
    result = cfn.get_template_summary(TemplateBody=json.dumps(unnamed_iam))
    assert result["Capabilities"] == ["CAPABILITY_IAM"]

    # Template with Transform → CAPABILITY_AUTO_EXPAND
    transform_tpl = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "Fn": {"Type": "AWS::Serverless::Function", "Properties": {}},
        },
    }
    result = cfn.get_template_summary(TemplateBody=json.dumps(transform_tpl))
    assert "CAPABILITY_AUTO_EXPAND" in result["Capabilities"]

def test_cfn_list_stacks(cfn):
    for name in ("cfn-t12-a", "cfn-t12-b"):
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Bucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": f"{name}-bucket"},
                },
            },
        }
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t12-a")
    _wait_stack(cfn, "cfn-t12-b")

    summaries = cfn.list_stacks()["StackSummaries"]
    names = [s["StackName"] for s in summaries]
    assert "cfn-t12-a" in names
    assert "cfn-t12-b" in names

def test_cfn_stack_events(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t13-events"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t13", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t13")

    events = cfn.describe_stack_events(StackName="cfn-t13")["StackEvents"]
    assert len(events) > 0
    assert all("ResourceStatus" in e for e in events)

def test_cfn_describe_stack_resources_logical_id_filter(cfn, s3, sqs):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t10-bucket"},
            },
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cfn-t10-queue"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t10", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t10")

    filtered = cfn.describe_stack_resources(
        StackName="cfn-t10", LogicalResourceId="Bucket"
    )["StackResources"]
    assert len(filtered) == 1
    assert filtered[0]["LogicalResourceId"] == "Bucket"
    assert filtered[0]["ResourceType"] == "AWS::S3::Bucket"

    with pytest.raises(ClientError) as exc_info:
        cfn.describe_stack_resources(
            StackName="cfn-t10", LogicalResourceId="DoesNotExist"
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationError"


def test_cfn_yaml_template(cfn, s3):
    yaml_body = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: cfn-t14-yaml
"""
    cfn.create_stack(StackName="cfn-t14", TemplateBody=yaml_body)
    _wait_stack(cfn, "cfn-t14")

    s3.head_bucket(Bucket="cfn-t14-yaml")

def test_cfn_rollback_on_failure(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t15-rollback"},
            },
            "Bad": {
                "Type": "AWS::Fake::Nope",
                "Properties": {},
            },
        },
    }
    cfn.create_stack(
        StackName="cfn-t15",
        TemplateBody=json.dumps(template),
        DisableRollback=False,
    )
    stack = _wait_stack(cfn, "cfn-t15")
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t15-rollback")

def test_cfn_import_nonexistent_export(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t16-param",
                    "Type": "String",
                    "Value": {"Fn::ImportValue": "NonExistentExport123"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t16", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-t16")
    assert stack["StackStatus"] in ("CREATE_FAILED", "ROLLBACK_COMPLETE")

def test_cfn_delete_stack_with_active_imports(cfn):
    exporter_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t17-exporter"},
            },
        },
        "Outputs": {
            "BucketOut": {
                "Value": {"Ref": "Bucket"},
                "Export": {"Name": "cfn-t17-export"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t17-exp", TemplateBody=json.dumps(exporter_template))
    _wait_stack(cfn, "cfn-t17-exp")

    importer_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t17-param",
                    "Type": "String",
                    "Value": {"Fn::ImportValue": "cfn-t17-export"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t17-imp", TemplateBody=json.dumps(importer_template))
    _wait_stack(cfn, "cfn-t17-imp")

    with pytest.raises(ClientError):
        cfn.delete_stack(StackName="cfn-t17-exp")

def test_cfn_update_rollback_on_failure(cfn, s3):
    template_v1 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t18-orig"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t18", TemplateBody=json.dumps(template_v1))
    _wait_stack(cfn, "cfn-t18")

    template_v2 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t18-orig"},
            },
            "Bad": {
                "Type": "AWS::Fake::Nope",
                "Properties": {},
            },
        },
    }
    cfn.update_stack(StackName="cfn-t18", TemplateBody=json.dumps(template_v2))
    stack = _wait_stack(cfn, "cfn-t18")
    assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"

    s3.head_bucket(Bucket="cfn-t18-orig")

def test_cfn_e2e_s3_put_and_get(cfn_e2e_stack, s3):
    bucket = cfn_e2e_stack["BucketName"]
    body = json.dumps({"id": "001", "total": 99.99})
    s3.put_object(Bucket=bucket, Key="orders/order-001.json", Body=body.encode())
    obj = s3.get_object(Bucket=bucket, Key="orders/order-001.json")
    data = json.loads(obj["Body"].read())
    assert data["id"] == "001"
    assert data["total"] == 99.99

def test_cfn_e2e_s3_list_objects(cfn_e2e_stack, s3):
    bucket = cfn_e2e_stack["BucketName"]
    s3.put_object(Bucket=bucket, Key="docs/readme.txt", Body=b"hello")
    listing = s3.list_objects_v2(Bucket=bucket)
    assert listing["KeyCount"] >= 1
    keys = [o["Key"] for o in listing["Contents"]]
    assert "docs/readme.txt" in keys

def test_cfn_e2e_sqs_send_receive_delete(cfn_e2e_stack, sqs):
    url = cfn_e2e_stack["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"event": "order.created"}))
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"event": "order.shipped"}))
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    received = msgs.get("Messages", [])
    assert len(received) == 2
    events = sorted(json.loads(m["Body"])["event"] for m in received)
    assert events == ["order.created", "order.shipped"]
    for m in received:
        sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    empty = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(empty.get("Messages", [])) == 0

def test_cfn_e2e_sns_publish(cfn_e2e_stack, sns):
    topic_arn = cfn_e2e_stack["TopicArn"]
    resp = sns.publish(TopicArn=topic_arn, Subject="Test Alert",
                       Message=json.dumps({"alert": "test", "severity": "low"}))
    assert "MessageId" in resp

def test_cfn_e2e_ssm_read_cfn_param(cfn_e2e_stack, ssm):
    param = ssm.get_parameter(Name=f"/{_E2E_STACK}/e2etest/queue-url")["Parameter"]
    assert param["Value"] == cfn_e2e_stack["QueueUrl"]

def test_cfn_e2e_ssm_write_and_read(cfn_e2e_stack, ssm):
    ssm.put_parameter(Name=f"/{_E2E_STACK}/e2etest/flags", Type="String",
                      Value=json.dumps({"dark_mode": True}))
    flags = json.loads(ssm.get_parameter(Name=f"/{_E2E_STACK}/e2etest/flags")["Parameter"]["Value"])
    assert flags["dark_mode"] is True

def test_cfn_e2e_lambda_invoke(cfn_e2e_stack, lam):
    resp = lam.invoke(FunctionName=f"{_E2E_STACK}-e2etest-processor",
                      Payload=json.dumps({"action": "test"}).encode())
    assert resp["StatusCode"] == 200

def test_cfn_e2e_lambda_role_matches_iam_role(cfn_e2e_stack, lam, iam):
    fn = lam.get_function(FunctionName=f"{_E2E_STACK}-e2etest-processor")["Configuration"]
    role = iam.get_role(RoleName=f"{_E2E_STACK}-e2etest-role")["Role"]
    assert fn["Role"] == role["Arn"]

def test_cfn_e2e_pipeline(cfn_e2e_stack, s3, sqs, sns):
    """S3 upload → SQS queue → read back from S3 → SNS alert."""
    bucket = cfn_e2e_stack["BucketName"]
    url = cfn_e2e_stack["QueueUrl"]
    topic_arn = cfn_e2e_stack["TopicArn"]

    for i in range(3):
        order = {"id": f"pipe-{i}", "item": f"widget-{i}", "qty": (i + 1) * 5}
        s3.put_object(Bucket=bucket, Key=f"pipeline/order-{i}.json",
                      Body=json.dumps(order).encode())

    for i in range(3):
        sqs.send_message(QueueUrl=url,
                         MessageBody=json.dumps({"event": "process", "key": f"pipeline/order-{i}.json"}))

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 3

    total_qty = 0
    for m in msgs["Messages"]:
        body = json.loads(m["Body"])
        obj = s3.get_object(Bucket=bucket, Key=body["key"])
        order = json.loads(obj["Body"].read())
        total_qty += order["qty"]
        sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])

    assert total_qty == 5 + 10 + 15

    resp = sns.publish(TopicArn=topic_arn, Subject="Pipeline Done",
                       Message=json.dumps({"processed": 3, "total_qty": total_qty}))
    assert "MessageId" in resp

def test_cfn_e2e_exports_available(cfn_e2e_stack, cfn):
    exports = cfn.list_exports()["Exports"]
    names = {e["Name"]: e["Value"] for e in exports}
    assert f"{_E2E_STACK}-bucket" in names
    assert names[f"{_E2E_STACK}-bucket"] == cfn_e2e_stack["BucketName"]

def test_cfn_auto_name_s3_follows_aws_pattern(cfn, s3):
    """S3 bucket auto-name: lowercase, stackName-logicalId-SUFFIX, max 63 chars."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}},
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "MyBucket"}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-s3", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-s3")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bucket_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BucketName")
    assert bucket_name == bucket_name.lower(), "S3 auto-name must be lowercase"
    assert bucket_name.startswith("cfn-autoname-s3-mybucket-"), f"Expected AWS-pattern name, got: {bucket_name}"
    assert len(bucket_name) <= 63, f"S3 name too long: {len(bucket_name)}"
    # Verify bucket actually exists
    s3.head_bucket(Bucket=bucket_name)

    cfn.delete_stack(StackName="cfn-autoname-s3")
    _wait_stack(cfn, "cfn-autoname-s3")

def test_cfn_auto_name_sqs_follows_aws_pattern(cfn, sqs):
    """SQS queue auto-name: stackName-logicalId-SUFFIX, max 80 chars, case preserved."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {"Type": "AWS::SQS::Queue", "Properties": {}},
        },
        "Outputs": {
            "QueueName": {"Value": {"Fn::GetAtt": ["MyQueue", "QueueName"]}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-sqs", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-sqs")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    queue_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "QueueName")
    assert queue_name.startswith("cfn-autoname-sqs-MyQueue-"), f"Expected AWS-pattern name, got: {queue_name}"
    assert len(queue_name) <= 80

    cfn.delete_stack(StackName="cfn-autoname-sqs")
    _wait_stack(cfn, "cfn-autoname-sqs")

def test_cfn_auto_name_dynamodb_follows_aws_pattern(cfn, ddb):
    """DynamoDB table auto-name: stackName-logicalId-SUFFIX, max 255 chars."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            },
        },
        "Outputs": {
            "TableName": {"Value": {"Ref": "MyTable"}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-ddb", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-ddb")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    table_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "TableName")
    assert table_name.startswith("cfn-autoname-ddb-MyTable-"), f"Expected AWS-pattern name, got: {table_name}"
    assert len(table_name) <= 255
    ddb.describe_table(TableName=table_name)

    cfn.delete_stack(StackName="cfn-autoname-ddb")
    _wait_stack(cfn, "cfn-autoname-ddb")


def test_cfn_dynamodb_global_table_pay_per_request(cfn, ddb):
    """AWS::DynamoDB::GlobalTable with PAY_PER_REQUEST billing — the common
    CDK TableV2 default. Replicas is required by CFN; locally it's ignored.
    Regression for issue #596."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyGlobal": {
                "Type": "AWS::DynamoDB::GlobalTable",
                "Properties": {
                    "TableName": "cfn-global-table-1",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    "StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"},
                    "Replicas": [
                        {"Region": "us-east-1"},
                        {"Region": "eu-west-1"},
                    ],
                },
            },
        },
        "Outputs": {"TableName": {"Value": {"Ref": "MyGlobal"}}},
    }
    cfn.create_stack(StackName="cfn-global-table-ppr", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-global-table-ppr")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    table_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "TableName")
    desc = ddb.describe_table(TableName=table_name)["Table"]
    assert desc["TableName"] == "cfn-global-table-1"
    assert desc["LatestStreamArn"]  # StreamSpecification was honoured

    cfn.delete_stack(StackName="cfn-global-table-ppr")
    _wait_stack(cfn, "cfn-global-table-ppr")


def test_cfn_dynamodb_global_table_provisioned_throughput(cfn, ddb):
    """AWS::DynamoDB::GlobalTable with PROVISIONED billing carries capacity
    via WriteProvisionedThroughputSettings / ReadProvisionedThroughputSettings
    (no top-level ProvisionedThroughput on this resource type). The CFN
    provisioner translates them to the engine's expected
    ProvisionedThroughput shape so DescribeTable returns the configured RCU /
    WCU instead of the engine's default 5/5. Mirrors what CDK TableV2 emits
    for a provisioned-billing table."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyGlobal": {
                "Type": "AWS::DynamoDB::GlobalTable",
                "Properties": {
                    "TableName": "cfn-global-table-prov",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PROVISIONED",
                    "Replicas": [{"Region": "us-east-1"}],
                    "WriteProvisionedThroughputSettings": {
                        "WriteCapacityAutoScalingSettings": {
                            "MinCapacity": 7,
                            "MaxCapacity": 100,
                            "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70},
                        }
                    },
                    "ReadProvisionedThroughputSettings": {
                        "ReadCapacityAutoScalingSettings": {
                            "MinCapacity": 13,
                            "MaxCapacity": 200,
                            "TargetTrackingScalingPolicyConfiguration": {"TargetValue": 70},
                        }
                    },
                },
            },
        },
        "Outputs": {"TableName": {"Value": {"Ref": "MyGlobal"}}},
    }
    cfn.create_stack(StackName="cfn-global-table-prov", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-global-table-prov")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    table_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "TableName")
    desc = ddb.describe_table(TableName=table_name)["Table"]
    assert desc["ProvisionedThroughput"]["WriteCapacityUnits"] == 7
    assert desc["ProvisionedThroughput"]["ReadCapacityUnits"] == 13

    cfn.delete_stack(StackName="cfn-global-table-prov")
    _wait_stack(cfn, "cfn-global-table-prov")

def test_cfn_explicit_name_not_overridden(cfn, s3):
    """Explicit BucketName must be used as-is, not overridden by auto-name logic."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-explicit-name-test"},
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "MyBucket"}},
        },
    }
    cfn.create_stack(StackName="cfn-explicit-name", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-explicit-name")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bucket_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BucketName")
    assert bucket_name == "cfn-explicit-name-test"

    cfn.delete_stack(StackName="cfn-explicit-name")
    _wait_stack(cfn, "cfn-explicit-name")

def test_cfn_s3_bucket_policy(cfn, s3):
    """AWS::S3::BucketPolicy provisions and deletes bucket policies."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-policy-test"},
            },
            "Policy": {
                "Type": "AWS::S3::BucketPolicy",
                "Properties": {
                    "Bucket": "cfn-policy-test",
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::cfn-policy-test/*"}],
                    },
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-s3-policy", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-s3-policy")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    policy = s3.get_bucket_policy(Bucket="cfn-policy-test")
    assert "s3:GetObject" in policy["Policy"]
    cfn.delete_stack(StackName="cfn-s3-policy")
    _wait_stack(cfn, "cfn-s3-policy")

def test_cfn_lambda_permission(cfn, lam):
    """AWS::Lambda::Permission provisions invoke permissions."""
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="cfn-perm-fn", Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Perm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": "cfn-perm-fn",
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "SourceArn": "arn:aws:s3:::my-bucket",
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-lambda-perm", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-lambda-perm")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    cfn.delete_stack(StackName="cfn-lambda-perm")
    _wait_stack(cfn, "cfn-lambda-perm")
    lam.delete_function(FunctionName="cfn-perm-fn")


def test_cfn_lambda_url_uses_function_url_state(cfn, lam):
    """CloudFormation Lambda URLs share state with the Lambda Function URL API."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-lambda-url-{suffix}"
    function_name = f"cfn-url-{suffix}"

    def template(auth_type, invoke_mode):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Function": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {
                        "FunctionName": function_name,
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "Role": "arn:aws:iam::000000000000:role/lambda-role",
                        "Code": {
                            "ZipFile": "def handler(event, context):\n    return {'statusCode': 200}\n"
                        },
                    },
                },
                "FunctionUrl": {
                    "Type": "AWS::Lambda::Url",
                    "Properties": {
                        "TargetFunctionArn": {"Ref": "Function"},
                        "AuthType": auth_type,
                        "InvokeMode": invoke_mode,
                        "Cors": {"AllowOrigins": ["https://example.com"]},
                    },
                },
            },
            "Outputs": {
                "UrlRef": {"Value": {"Ref": "FunctionUrl"}},
                "FunctionArn": {
                    "Value": {"Fn::GetAtt": ["FunctionUrl", "FunctionArn"]}
                },
                "FunctionUrl": {
                    "Value": {"Fn::GetAtt": ["FunctionUrl", "FunctionUrl"]}
                },
            },
        }

    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("NONE", "BUFFERED")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    assert outputs["UrlRef"] == function_name
    config = lam.get_function_url_config(FunctionName=function_name)
    assert config["FunctionUrl"] == outputs["FunctionUrl"]
    assert config["FunctionArn"] == outputs["FunctionArn"]
    assert config["AuthType"] == "NONE"
    assert config["InvokeMode"] == "BUFFERED"
    original_url = config["FunctionUrl"]

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("AWS_IAM", "RESPONSE_STREAM")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    config = lam.get_function_url_config(FunctionName=function_name)
    assert config["FunctionUrl"] == original_url
    assert config["AuthType"] == "AWS_IAM"
    assert config["InvokeMode"] == "RESPONSE_STREAM"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert lam.list_function_url_configs(
        FunctionName=function_name
    )["FunctionUrlConfigs"] == []


def test_cfn_lambda_permission_qualified_arn_uses_base_function_policy(cfn, lam):
    """Qualified FunctionName refs should add permission to the base function policy."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-perm-qualified-{suffix}"
    stack_name = f"cfn-lambda-perm-qualified-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    version = lam.publish_version(FunctionName=fn)["Version"]
    alias_arn = lam.create_alias(FunctionName=fn, Name="live", FunctionVersion=version)["AliasArn"]
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Perm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": alias_arn,
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "SourceArn": "arn:aws:s3:::my-bucket",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        policy = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        statements = policy["Statement"]
        assert len(statements) == 1
        assert statements[0]["Resource"] == alias_arn
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


def test_cfn_lambda_version(cfn, lam):
    """AWS::Lambda::Version creates a published version."""
    code = "def handler(e,c): return {'v': 1}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="cfn-ver-fn", Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Ver": {
                "Type": "AWS::Lambda::Version",
                "Properties": {
                    "FunctionName": "cfn-ver-fn",
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-lambda-ver", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-lambda-ver")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    versions = lam.list_versions_by_function(FunctionName="cfn-ver-fn")["Versions"]
    assert len([v for v in versions if v["Version"] != "$LATEST"]) >= 1
    cfn.delete_stack(StackName="cfn-lambda-ver")
    _wait_stack(cfn, "cfn-lambda-ver")
    lam.delete_function(FunctionName="cfn-ver-fn")


def test_cfn_lambda_event_invoke_config_lifecycle(cfn, lam):
    """AWS::Lambda::EventInvokeConfig creates, updates, and deletes cleanly."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-event-invoke-{suffix}"
    stack_name = f"cfn-event-invoke-{suffix}"
    destination = f"arn:aws:sqs:us-east-1:000000000000:failure-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    def template(retries, event_age):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "InvokeConfig": {
                    "Type": "AWS::Lambda::EventInvokeConfig",
                    "Properties": {
                        "FunctionName": fn,
                        "Qualifier": "$LATEST",
                        "MaximumRetryAttempts": retries,
                        "MaximumEventAgeInSeconds": event_age,
                        "DestinationConfig": {
                            "OnFailure": {"Destination": destination},
                        },
                    },
                },
            },
            "Outputs": {
                "InvokeConfigId": {"Value": {"Ref": "InvokeConfig"}},
            },
        }

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template(1, 300)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert stack["Outputs"][0]["OutputValue"] == f"{fn}:$LATEST"

        config = lam.get_function_event_invoke_config(
            FunctionName=fn, Qualifier="$LATEST"
        )
        assert config["FunctionArn"].endswith(f":function:{fn}:$LATEST")
        assert config["MaximumRetryAttempts"] == 1
        assert config["MaximumEventAgeInSeconds"] == 300
        assert config["DestinationConfig"]["OnFailure"]["Destination"] == destination

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template(0, 120)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        updated = lam.get_function_event_invoke_config(
            FunctionName=fn, Qualifier="$LATEST"
        )
        assert updated["MaximumRetryAttempts"] == 0
        assert updated["MaximumEventAgeInSeconds"] == 120

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        with pytest.raises(ClientError) as exc:
            lam.get_function_event_invoke_config(
                FunctionName=fn, Qualifier="$LATEST"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        lam.delete_function(FunctionName=fn)


def test_cfn_esm_filter_criteria_round_trips(cfn, lam, ddb):
    code = "def handler(e,c): return {'ok': True}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="cfn-esm-fc-fn", Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    table = ddb.create_table(
        TableName="cfn-esm-fc-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    stream_arn = table["TableDescription"]["LatestStreamArn"]
    fc = {"Filters": [{"Pattern": '{"eventName":["INSERT"]}'}]}
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Mapping": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": "cfn-esm-fc-fn",
                    "EventSourceArn": stream_arn,
                    "StartingPosition": "LATEST",
                    "FilterCriteria": fc,
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-esm-fc", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-esm-fc")
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    mappings = lam.list_event_source_mappings(FunctionName="cfn-esm-fc-fn")[
        "EventSourceMappings"
    ]
    assert len(mappings) == 1
    assert mappings[0].get("FilterCriteria") == fc, "FilterCriteria must round-trip"

    cfn.delete_stack(StackName="cfn-esm-fc")
    _wait_stack(cfn, "cfn-esm-fc")
    ddb.delete_table(TableName="cfn-esm-fc-table")
    lam.delete_function(FunctionName="cfn-esm-fc-fn")


def test_cfn_esm_extra_props_round_trip_and_in_place_update(cfn, lam, ddb):
    """CFN-created EventSourceMappings must round-trip every optional prop (not just
    FilterCriteria), and a stack update must mutate the mapping in place (same UUID),
    never duplicate it — #1034 follow-up."""
    code = "def handler(e,c): return {'ok': True}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="cfn-esm-xp-fn", Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    table = ddb.create_table(
        TableName="cfn-esm-xp-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    stream_arn = table["TableDescription"]["LatestStreamArn"]
    dlq = "arn:aws:sqs:us-east-1:000000000000:cfn-esm-xp-dlq"

    def template(batch):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"Mapping": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": "cfn-esm-xp-fn",
                    "EventSourceArn": stream_arn,
                    "StartingPosition": "LATEST",
                    "BatchSize": batch,
                    "MaximumRetryAttempts": 3,
                    "BisectBatchOnFunctionError": True,
                    "ParallelizationFactor": 4,
                    "DestinationConfig": {"OnFailure": {"Destination": dlq}},
                },
            }},
        }

    cfn.create_stack(StackName="cfn-esm-xp", TemplateBody=json.dumps(template(5)))
    stack = _wait_stack(cfn, "cfn-esm-xp")
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    maps = lam.list_event_source_mappings(FunctionName="cfn-esm-xp-fn")["EventSourceMappings"]
    assert len(maps) == 1
    m = maps[0]
    uuid_before = m["UUID"]
    assert m["BatchSize"] == 5
    assert m["MaximumRetryAttempts"] == 3
    assert m["BisectBatchOnFunctionError"] is True
    assert m["ParallelizationFactor"] == 4
    assert m["DestinationConfig"]["OnFailure"]["Destination"] == dlq

    # Stack update changing BatchSize must update in place: same UUID, still one mapping.
    cfn.update_stack(StackName="cfn-esm-xp", TemplateBody=json.dumps(template(9)))
    _wait_stack(cfn, "cfn-esm-xp")
    maps2 = lam.list_event_source_mappings(FunctionName="cfn-esm-xp-fn")["EventSourceMappings"]
    assert len(maps2) == 1, "stack update must not duplicate the mapping"
    assert maps2[0]["UUID"] == uuid_before, "update must mutate in place (same UUID)"
    assert maps2[0]["BatchSize"] == 9

    cfn.delete_stack(StackName="cfn-esm-xp")
    _wait_stack(cfn, "cfn-esm-xp")
    ddb.delete_table(TableName="cfn-esm-xp-table")
    lam.delete_function(FunctionName="cfn-esm-xp-fn")


def test_cfn_lambda_alias_and_esm_keep_function_name(cfn, lam):
    """Lambda Alias and ESM provisioners need function names, not permission resource ARNs."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-alias-esm-{suffix}"
    stack_name = f"cfn-alias-esm-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Alias": {
                "Type": "AWS::Lambda::Alias",
                "Properties": {"FunctionName": fn, "Name": "live", "FunctionVersion": "$LATEST"},
            },
            "Esm": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": fn,
                    "EventSourceArn": f"arn:aws:sqs:us-east-1:000000000000:source-{suffix}",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        aliases = lam.list_aliases(FunctionName=fn)["Aliases"]
        assert aliases[0]["AliasArn"].endswith(f":function:{fn}:live")

        mappings = lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"]
        assert len(mappings) == 1
        assert mappings[0]["FunctionArn"].endswith(f":function:{fn}")
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


def test_cfn_lambda_esm_preserves_qualified_function_ref(cfn, lam):
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-esm-qualified-{suffix}"
    stack_name = f"cfn-esm-qualified-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    version = lam.publish_version(FunctionName=fn)["Version"]
    lam.create_alias(FunctionName=fn, Name="live", FunctionVersion=version)
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Esm": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": f"{fn}:live",
                    "EventSourceArn": f"arn:aws:sqs:us-east-1:000000000000:source-{suffix}",
                    "BatchSize": 1,
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        mappings = lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"]
        assert len(mappings) == 1
        assert mappings[0]["FunctionArn"].endswith(f":function:{fn}:live")
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


def test_cfn_lambda_provisioners_do_not_tail_resolve_wrong_service_arns(cfn, lam):
    """Lambda CFN provisioners must not map a non-Lambda ARN tail to a local function."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-lambda-arn-guard-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    wrong_ref = f"arn:aws:sqs:us-east-1:000000000000:function:{fn}"
    stack_name = f"cfn-lambda-arn-guard-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Perm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": wrong_ref,
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "SourceArn": "arn:aws:s3:::my-bucket",
                },
            },
            "Ver": {
                "Type": "AWS::Lambda::Version",
                "Properties": {"FunctionName": wrong_ref},
            },
            "Alias": {
                "Type": "AWS::Lambda::Alias",
                "Properties": {"FunctionName": wrong_ref, "Name": "live", "FunctionVersion": "1"},
            },
            "Esm": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": wrong_ref,
                    "EventSourceArn": f"arn:aws:sqs:us-east-1:000000000000:source-{suffix}",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        policy = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        assert policy["Statement"] == []

        versions = lam.list_versions_by_function(FunctionName=fn)["Versions"]
        assert [v["Version"] for v in versions] == ["$LATEST"]

        assert lam.list_aliases(FunctionName=fn)["Aliases"] == []
        assert lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"] == []
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        lam.delete_function(FunctionName=fn)


def test_cfn_lambda_provisioners_reject_missing_bare_qualifier(cfn, lam):
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-lambda-missing-qual-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    missing_ref = f"{fn}:missing"
    stack_name = f"cfn-lambda-missing-qual-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Perm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": missing_ref,
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "SourceArn": "arn:aws:s3:::my-bucket",
                },
            },
            "Alias": {
                "Type": "AWS::Lambda::Alias",
                "Properties": {"FunctionName": missing_ref, "Name": "live", "FunctionVersion": "1"},
            },
            "Esm": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "FunctionName": missing_ref,
                    "EventSourceArn": f"arn:aws:sqs:us-east-1:000000000000:source-{suffix}",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        policy = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        assert policy["Statement"] == []
        assert lam.list_aliases(FunctionName=fn)["Aliases"] == []
        assert lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"] == []
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        lam.delete_function(FunctionName=fn)


def test_cfn_wait_condition(cfn):
    """AWS::CloudFormation::WaitCondition and WaitConditionHandle are no-ops."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"},
            "Wait": {
                "Type": "AWS::CloudFormation::WaitCondition",
                "Properties": {"Handle": {"Ref": "Handle"}, "Timeout": "10"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-wait", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-wait")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    cfn.delete_stack(StackName="cfn-wait")
    _wait_stack(cfn, "cfn-wait")

def test_cfn_secretsmanager_generate_secret_string(cfn, sm):
    """CFN stack with SecretsManager::Secret + GenerateSecretString produces valid JSON secret."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MySecret": {
                "Type": "AWS::SecretsManager::Secret",
                "Properties": {
                    "Name": "intg-cfn-gensecret",
                    "GenerateSecretString": {
                        "PasswordLength": 20,
                        "SecretStringTemplate": '{"username":"admin"}',
                        "GenerateStringKey": "password",
                    },
                },
            }
        },
    }
    cfn.create_stack(
        StackName="intg-cfn-gensecret-stack",
        TemplateBody=json.dumps(template),
    )
    stack = _wait_stack(cfn, "intg-cfn-gensecret-stack")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resp = sm.get_secret_value(SecretId="intg-cfn-gensecret")
    secret = json.loads(resp["SecretString"])
    assert secret["username"] == "admin"
    assert "password" in secret
    assert len(secret["password"]) >= 20

def test_cfn_stack_with_s3_lambda_dynamodb(cfn, s3, lam, ddb):
    """CloudFormation stack provisions S3 bucket, Lambda function, and DynamoDB table together."""
    stack_name = "intg-cfn-full-stack"
    bucket_name = "intg-cfn-full-bkt"
    fn_name = "intg-cfn-full-fn"
    table_name = "intg-cfn-full-tbl"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": bucket_name},
            },
            "MyTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": table_name,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            },
            "MyFunction": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": fn_name,
                    "Runtime": "python3.11",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/cfn-role",
                    "Code": {
                        "ZipFile": (
                            "import json\n"
                            "def handler(event, context):\n"
                            "    return {'statusCode': 200, 'body': json.dumps(event)}\n"
                        ),
                    },
                },
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify S3 bucket was created
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert bucket_name in buckets

    # Verify DynamoDB table was created and is functional
    tables = ddb.list_tables()["TableNames"]
    assert table_name in tables
    ddb.put_item(TableName=table_name, Item={"pk": {"S": "cfn-test"}, "val": {"S": "works"}})
    item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "cfn-test"}})
    assert item["Item"]["val"]["S"] == "works"

    # Verify Lambda function was created and is invocable
    funcs = [f["FunctionName"] for f in lam.list_functions()["Functions"]]
    assert fn_name in funcs
    resp = lam.invoke(FunctionName=fn_name, Payload=json.dumps({"test": "cfn"}))
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200

    # Verify stack describes all 3 resources
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    resource_types = {r["ResourceType"] for r in resources}
    assert "AWS::S3::Bucket" in resource_types
    assert "AWS::DynamoDB::Table" in resource_types
    assert "AWS::Lambda::Function" in resource_types

    # Delete stack and verify cleanup
    cfn.delete_stack(StackName=stack_name)
    time.sleep(2)
    stacks = cfn.describe_stacks()["Stacks"]
    active = [st for st in stacks if st["StackName"] == stack_name and "DELETE" not in st["StackStatus"]]
    assert len(active) == 0

def test_cfn_cdk_bootstrap_resources(cfn, s3, ecr):
    """CDK bootstrap template resources: S3 + ECR + IAM Role + KMS Key + SSM Parameter."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "StagingBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cdk-bootstrap-v44"},
            },
            "ContainerRepo": {
                "Type": "AWS::ECR::Repository",
                "Properties": {"RepositoryName": "cdk-assets-v44"},
            },
            "DeployRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "cdk-deploy-v44",
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                },
            },
            "FileKey": {
                "Type": "AWS::KMS::Key",
                "Properties": {"Description": "CDK file assets key"},
            },
            "KeyAlias": {
                "Type": "AWS::KMS::Alias",
                "Properties": {"AliasName": "alias/cdk-key-v44", "TargetKeyId": "dummy"},
            },
            "BootstrapVersion": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {"Name": "/cdk-bootstrap/v44/version", "Type": "String", "Value": "27"},
            },
            "DeployPolicy": {
                "Type": "AWS::IAM::ManagedPolicy",
                "Properties": {"ManagedPolicyName": "cdk-policy-v44", "PolicyDocument": {"Version": "2012-10-17", "Statement": []}},
            },
        },
    }
    cfn.create_stack(StackName="CDKToolkit-v44", TemplateBody=json.dumps(template))
    import time as _t

    _t.sleep(2)
    stack = cfn.describe_stacks(StackName="CDKToolkit-v44")["Stacks"][0]
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify resources
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert "cdk-bootstrap-v44" in buckets
    repos = [r["repositoryName"] for r in ecr.describe_repositories()["repositories"]]
    assert "cdk-assets-v44" in repos

    cfn.delete_stack(StackName="CDKToolkit-v44")

def test_cfn_ec2_launch_template(cfn, ec2):
    """CloudFormation should provision and delete an EC2 LaunchTemplate."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyLT": {
                "Type": "AWS::EC2::LaunchTemplate",
                "Properties": {
                    "LaunchTemplateName": "cfn-lt-test",
                    "LaunchTemplateData": {
                        "InstanceType": "t3.medium",
                        "ImageId": "ami-cfn123",
                    },
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-lt-stack", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-lt-stack")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify the launch template exists via EC2 API
    desc = ec2.describe_launch_templates(LaunchTemplateNames=["cfn-lt-test"])
    assert len(desc["LaunchTemplates"]) == 1
    lt_id = desc["LaunchTemplates"][0]["LaunchTemplateId"]

    versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    assert versions["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"] == "t3.medium"

    # Delete and verify cleanup
    cfn.delete_stack(StackName="cfn-lt-stack")
    _wait_stack(cfn, "cfn-lt-stack")

    desc2 = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert len(desc2["LaunchTemplates"]) == 0


def test_cfn_appsync_function_configuration_attributes(cfn, appsync):
    """AppSync pipeline functions expose the identities consumed by resolvers."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-function-{suffix}"
    api_id = appsync.create_graphql_api(
        name=f"pipeline-api-{suffix}",
        authenticationType="API_KEY",
    )["graphqlApi"]["apiId"]
    appsync.create_data_source(
        apiId=api_id,
        name="NoneSource",
        type="NONE",
    )

    def template(function_name):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "PipelineFunction": {
                    "Type": "AWS::AppSync::FunctionConfiguration",
                    "Properties": {
                        "ApiId": api_id,
                        "DataSourceName": "NoneSource",
                        "Name": function_name,
                        "FunctionVersion": "2018-05-29",
                        "RequestMappingTemplate": "{}",
                        "ResponseMappingTemplate": "$util.toJson($ctx.result)",
                    },
                },
            },
            "Outputs": {
                "RefArn": {"Value": {"Ref": "PipelineFunction"}},
                "FunctionArn": {
                    "Value": {"Fn::GetAtt": ["PipelineFunction", "FunctionArn"]}
                },
                "FunctionId": {
                    "Value": {"Fn::GetAtt": ["PipelineFunction", "FunctionId"]}
                },
                "FunctionName": {
                    "Value": {"Fn::GetAtt": ["PipelineFunction", "Name"]}
                },
                "DataSourceName": {
                    "Value": {"Fn::GetAtt": ["PipelineFunction", "DataSourceName"]}
                },
            },
        }

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("ExampleFunction")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        assert outputs["RefArn"] == outputs["FunctionArn"]
        assert outputs["RefArn"].endswith(f"/functions/{outputs['FunctionId']}")
        assert outputs["FunctionName"] == "ExampleFunction"
        assert outputs["DataSourceName"] == "NoneSource"
        original_arn = outputs["FunctionArn"]

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("UpdatedFunction")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        assert outputs["FunctionArn"] == original_arn
        assert outputs["FunctionName"] == "UpdatedFunction"
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass
        appsync.delete_graphql_api(apiId=api_id)


def test_cfn_ec2_vpc_endpoint_uses_ec2_state(cfn, ec2):
    """CloudFormation VPC endpoints share the EC2 API state and expose their ID."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpce-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Vpc": {
                "Type": "AWS::EC2::VPC",
                "Properties": {"CidrBlock": "10.40.0.0/16"},
            },
            "Endpoint": {
                "Type": "AWS::EC2::VPCEndpoint",
                "Properties": {
                    "VpcEndpointType": "Gateway",
                    "VpcId": {"Ref": "Vpc"},
                    "ServiceName": "com.amazonaws.us-east-1.s3",
                    "Tags": [{"Key": "source", "Value": "cloudformation"}],
                },
            },
        },
        "Outputs": {
            "RefId": {"Value": {"Ref": "Endpoint"}},
            "GetAttId": {"Value": {"Fn::GetAtt": ["Endpoint", "Id"]}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    assert outputs["RefId"] == outputs["GetAttId"]
    assert outputs["RefId"].startswith("vpce-")

    endpoints = ec2.describe_vpc_endpoints(
        VpcEndpointIds=[outputs["RefId"]]
    )["VpcEndpoints"]
    assert len(endpoints) == 1
    assert endpoints[0]["VpcEndpointId"] == outputs["RefId"]
    assert endpoints[0]["ServiceName"] == "com.amazonaws.us-east-1.s3"
    assert endpoints[0]["Tags"] == [{"Key": "source", "Value": "cloudformation"}]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert ec2.describe_vpc_endpoints(
        VpcEndpointIds=[outputs["RefId"]]
    )["VpcEndpoints"] == []


def test_cfn_elbv2_load_balancer_and_listener(cfn, elbv2):
    """CloudFormation provisions ELBv2 LoadBalancer + Listener and cleans both on delete."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-elbv2-{uid}"
    lb_name = f"cfn-alb-{uid}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Alb": {
                "Type": "AWS::ElasticLoadBalancingV2::LoadBalancer",
                "Properties": {
                    "Name": lb_name,
                    "Type": "application",
                    "Scheme": "internal",
                    "SecurityGroups": ["sg-cfn12345"],
                    "Subnets": ["subnet-cfn-a", "subnet-cfn-b"],
                    "LoadBalancerAttributes": [
                        {"Key": "idle_timeout.timeout_seconds", "Value": "45"},
                    ],
                },
            },
            "AlbListener": {
                "Type": "AWS::ElasticLoadBalancingV2::Listener",
                "Properties": {
                    "LoadBalancerArn": {"Ref": "Alb"},
                    "Port": 443,
                    "Protocol": "HTTPS",
                    "DefaultActions": [
                        {
                            "Type": "fixed-response",
                            "FixedResponseConfig": {
                                "StatusCode": "404",
                                "ContentType": "application/json",
                                "MessageBody": '{"status":404}',
                            },
                        }
                    ],
                },
            },
        },
        "Outputs": {
            "AlbDnsName": {"Value": {"Fn::GetAtt": ["Alb", "DNSName"]}},
            "AlbFullName": {"Value": {"Fn::GetAtt": ["Alb", "LoadBalancerFullName"]}},
            "AlbListenerArn": {"Value": {"Ref": "AlbListener"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["AlbDnsName"].endswith(".elb.amazonaws.com")
    assert outputs["AlbFullName"].startswith(f"app/{lb_name}/")
    assert ":listener/app/" in outputs["AlbListenerArn"]

    lbs = elbv2.describe_load_balancers(Names=[lb_name])["LoadBalancers"]
    assert len(lbs) == 1
    lb_arn = lbs[0]["LoadBalancerArn"]
    assert lbs[0]["Scheme"] == "internal"
    assert lbs[0]["Type"] == "application"

    listeners = elbv2.describe_listeners(LoadBalancerArn=lb_arn)["Listeners"]
    assert len(listeners) == 1
    listener = listeners[0]
    assert listener["Port"] == 443
    assert listener["Protocol"] == "HTTPS"
    assert listener["DefaultActions"][0]["Type"] == "fixed-response"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc:
        elbv2.describe_load_balancers(Names=[lb_name])
    assert exc.value.response["Error"]["Code"] == "LoadBalancerNotFound"


def test_cfn_cloudwatch_alarm_lifecycle(cfn, cw):
    """CloudFormation creates a metric alarm and removes it on stack delete."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cwal-{uid}"
    alarm_name = f"cfn-cw-alarm-{uid}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CpuAlarm": {
                "Type": "AWS::CloudWatch::Alarm",
                "Properties": {
                    "AlarmName": alarm_name,
                    "AlarmDescription": "CFN integration test",
                    "MetricName": "CPUUtilization",
                    "Namespace": f"CfnCwTest/{uid}",
                    "Statistic": "Average",
                    "Period": 60,
                    "EvaluationPeriods": 1,
                    "Threshold": 80.0,
                    "ComparisonOperator": "GreaterThanThreshold",
                    "TreatMissingData": "notBreaching",
                },
            },
        },
        "Outputs": {
            "AlarmNameOut": {"Value": {"Ref": "CpuAlarm"}},
            "AlarmArnOut": {"Value": {"Fn::GetAtt": ["CpuAlarm", "Arn"]}},
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["AlarmNameOut"] == alarm_name
    assert outputs["AlarmArnOut"].endswith(f":alarm:{alarm_name}")

    resp = cw.describe_alarms(AlarmNames=[alarm_name])
    assert len(resp["MetricAlarms"]) == 1
    a = resp["MetricAlarms"][0]
    assert a["MetricName"] == "CPUUtilization"
    assert a["Namespace"] == f"CfnCwTest/{uid}"
    assert float(a["Threshold"]) == 80.0

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    resp2 = cw.describe_alarms(AlarmNames=[alarm_name])
    assert resp2["MetricAlarms"] == []


def test_cfn_cloudwatch_dashboard_lifecycle(cfn, cw):
    """CloudFormation creates, updates, and removes a CloudWatch dashboard."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cwdash-{uid}"
    dashboard_name = f"cfn-dashboard-{uid}"
    replacement_name = f"cfn-dashboard-replaced-{uid}"
    body = json.dumps({"widgets": [{"type": "text", "properties": {"markdown": "Created"}}]})
    updated_body = json.dumps({"widgets": [{"type": "text", "properties": {"markdown": "Updated"}}]})

    def template(dashboard_body, name=dashboard_name):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Dashboard": {
                    "Type": "AWS::CloudWatch::Dashboard",
                    "Properties": {
                        "DashboardName": name,
                        "DashboardBody": dashboard_body,
                    },
                },
            },
            "Outputs": {
                "DashboardName": {"Value": {"Ref": "Dashboard"}},
            },
        }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template(body)))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    assert stack["Outputs"][0]["OutputValue"] == dashboard_name
    assert cw.get_dashboard(DashboardName=dashboard_name)["DashboardBody"] == body

    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template(updated_body)))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert cw.get_dashboard(DashboardName=dashboard_name)["DashboardBody"] == updated_body

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template(updated_body, replacement_name)),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert stack["Outputs"][0]["OutputValue"] == replacement_name
    with pytest.raises(ClientError) as exc:
        cw.get_dashboard(DashboardName=dashboard_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFound"
    assert cw.get_dashboard(DashboardName=replacement_name)["DashboardBody"] == updated_body

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc:
        cw.get_dashboard(DashboardName=replacement_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFound"


def test_cfn_route53_hosted_zone_and_record_set(cfn, r53):
    """CloudFormation provisions Route53 HostedZone + RecordSet and removes records on delete."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-r53rs-{uid}"
    zone_name = f"cfnrs{uid}.com."
    record_name = f"www.cfnrs{uid}.com"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Zone": {
                "Type": "AWS::Route53::HostedZone",
                "Properties": {"Name": zone_name},
            },
            "WebA": {
                "Type": "AWS::Route53::RecordSet",
                "Properties": {
                    "HostedZoneId": {"Ref": "Zone"},
                    "Name": record_name,
                    "Type": "A",
                    "TTL": 300,
                    "ResourceRecords": [{"Value": "198.51.100.10"}],
                },
            },
        },
        "Outputs": {
            "RecordFqdn": {"Value": {"Ref": "WebA"}},
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["RecordFqdn"].endswith(".")

    resources = {r["LogicalResourceId"]: r for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]}
    zone_id = resources["Zone"]["PhysicalResourceId"]

    rrs = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    a_rrs = [r for r in rrs if r["Type"] == "A" and "cfnrs" in r["Name"]]
    assert len(a_rrs) == 1
    assert a_rrs[0]["ResourceRecords"][0]["Value"] == "198.51.100.10"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)

    with pytest.raises(ClientError) as exc:
        r53.get_hosted_zone(Id=zone_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"


def test_cfn_ssm_parameter_timestamp_is_epoch(cfn, ssm):
    """SSM parameters created via CloudFormation must store LastModifiedDate
    as an epoch float, not an ISO string.  The JS SDK v3 deserializes SSM
    timestamps with parseEpochTimestamp() which throws 'Expected real number,
    got implicit NaN' when the value is an ISO string.  This broke cdk deploy."""
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "/cfn-test/epoch-check",
                    "Type": "String",
                    "Value": "42",
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-ssm-epoch", TemplateBody=template)
    _wait_stack(cfn, "cfn-ssm-epoch")

    try:
        resp = ssm.get_parameter(Name="/cfn-test/epoch-check")
        last_mod = resp["Parameter"]["LastModifiedDate"]
        # boto3 converts epoch floats to datetime objects automatically.
        # If it were an ISO string, boto3 would leave it as a string or error.
        import datetime
        assert isinstance(last_mod, datetime.datetime), (
            f"LastModifiedDate should be datetime (from epoch float), "
            f"got {type(last_mod).__name__}: {last_mod}"
        )
    finally:
        cfn.delete_stack(StackName="cfn-ssm-epoch")
        _wait_stack(cfn, "cfn-ssm-epoch")


def test_cfn_appconfig_application(cfn, appconfig_client):
    """AWS::AppConfig::Application provisions via CFN and is reachable via the
    AppConfig API. Mirrors the CDK template from the reporter."""
    template = json.dumps({
        "Resources": {
            "AppConfig1FDF3617": {
                "Type": "AWS::AppConfig::Application",
                "Properties": {
                    "Name": "digital-cdk-template-test-master-AppConfig",
                    "Tags": [
                        {"Key": "application-id", "Value": "digital-cdk-template"},
                    ],
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-appconfig-app", TemplateBody=template)
    _wait_stack(cfn, "cfn-appconfig-app")

    try:
        apps = appconfig_client.list_applications()["Items"]
        match = [
            a for a in apps
            if a["Name"] == "digital-cdk-template-test-master-AppConfig"
        ]
        assert len(match) == 1
        app_id = match[0]["Id"]

        resources = cfn.describe_stack_resources(StackName="cfn-appconfig-app")
        cfn_res = [
            r for r in resources["StackResources"]
            if r["LogicalResourceId"] == "AppConfig1FDF3617"
        ]
        assert len(cfn_res) == 1
        assert cfn_res[0]["PhysicalResourceId"] == app_id
    finally:
        cfn.delete_stack(StackName="cfn-appconfig-app")
        _wait_stack(cfn, "cfn-appconfig-app")

    apps_after = appconfig_client.list_applications()["Items"]
    assert not any(
        a["Name"] == "digital-cdk-template-test-master-AppConfig"
        for a in apps_after
    )


def test_cfn_appconfig_full_stack(cfn, appconfig_client):
    """Issue #832: end-to-end AppConfig CFN stack — Application + Environment +
    ConfigurationProfile + HostedConfigurationVersion + DeploymentStrategy +
    Deployment, with Ref / Fn::GetAtt cross-references."""
    template = json.dumps({
        "Resources": {
            "App": {
                "Type": "AWS::AppConfig::Application",
                "Properties": {"Name": "cfn-832-app"},
            },
            "Env": {
                "Type": "AWS::AppConfig::Environment",
                "Properties": {
                    "ApplicationId": {"Ref": "App"},
                    "Name": "cfn-832-env",
                    "Description": "from cfn",
                    "Tags": [{"Key": "stage", "Value": "test"}],
                },
            },
            "Profile": {
                "Type": "AWS::AppConfig::ConfigurationProfile",
                "Properties": {
                    "ApplicationId": {"Ref": "App"},
                    "Name": "cfn-832-profile",
                    "LocationUri": "hosted",
                    "Type": "AWS.Freeform",
                },
            },
            "HCV": {
                "Type": "AWS::AppConfig::HostedConfigurationVersion",
                "Properties": {
                    "ApplicationId": {"Ref": "App"},
                    "ConfigurationProfileId": {"Ref": "Profile"},
                    "Content": '{"flag":true}',
                    "ContentType": "application/json",
                },
            },
            "Strategy": {
                "Type": "AWS::AppConfig::DeploymentStrategy",
                "Properties": {
                    "Name": "cfn-832-strategy",
                    "DeploymentDurationInMinutes": 0,
                    "GrowthFactor": 100,
                    "ReplicateTo": "NONE",
                },
            },
            "Deploy": {
                "Type": "AWS::AppConfig::Deployment",
                "Properties": {
                    "ApplicationId": {"Ref": "App"},
                    "EnvironmentId": {"Ref": "Env"},
                    "ConfigurationProfileId": {"Ref": "Profile"},
                    "DeploymentStrategyId": {"Ref": "Strategy"},
                    "ConfigurationVersion": {"Fn::GetAtt": ["HCV", "VersionNumber"]},
                    "Tags": [{"Key": "owner", "Value": "cfn-832"}],
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-832", TemplateBody=template)
    _wait_stack(cfn, "cfn-832")

    try:
        # Application
        app = next(a for a in appconfig_client.list_applications()["Items"]
                   if a["Name"] == "cfn-832-app")
        app_id = app["Id"]

        # Environment
        envs = appconfig_client.list_environments(ApplicationId=app_id)["Items"]
        env = next(e for e in envs if e["Name"] == "cfn-832-env")
        assert env["Description"] == "from cfn"

        # ConfigurationProfile
        profiles = appconfig_client.list_configuration_profiles(ApplicationId=app_id)["Items"]
        profile = next(p for p in profiles if p["Name"] == "cfn-832-profile")
        assert profile["LocationUri"] == "hosted"

        # HostedConfigurationVersion — version number 1 for the first one.
        hcvs = appconfig_client.list_hosted_configuration_versions(
            ApplicationId=app_id, ConfigurationProfileId=profile["Id"],
        )["Items"]
        assert any(h["VersionNumber"] == 1 for h in hcvs)

        # DeploymentStrategy
        strategies = appconfig_client.list_deployment_strategies()["Items"]
        strategy = next(s for s in strategies if s["Name"] == "cfn-832-strategy")
        assert strategy["DeploymentDurationInMinutes"] == 0
        assert strategy["ReplicateTo"] == "NONE"

        # Deployment — uses Fn::GetAtt HCV.VersionNumber as ConfigurationVersion.
        deployments = appconfig_client.list_deployments(
            ApplicationId=app_id, EnvironmentId=env["Id"],
        )["Items"]
        assert len(deployments) == 1
        # Fn::GetAtt HCV.VersionNumber resolves to the int 1; the Deployment
        # stores whatever the engine hands the provisioner, so accept either
        # form when asserting the wiring.
        assert str(deployments[0]["ConfigurationVersion"]) == "1"
        assert deployments[0]["State"] == "COMPLETE"

        # Deployment Tags are stored and resolvable via ListTagsForResource.
        deploy_arn = (
            f"arn:aws:appconfig:us-east-1:000000000000:application/{app_id}/"
            f"environment/{env['Id']}/deployment/{deployments[0]['DeploymentNumber']}"
        )
        tags = appconfig_client.list_tags_for_resource(ResourceArn=deploy_arn)["Tags"]
        assert tags.get("owner") == "cfn-832"

        # CFN-side: every logical resource resolved to a physical id.
        resources = cfn.describe_stack_resources(StackName="cfn-832")["StackResources"]
        by_logical = {r["LogicalResourceId"]: r["PhysicalResourceId"] for r in resources}
        for logical in ("App", "Env", "Profile", "HCV", "Strategy", "Deploy"):
            assert by_logical.get(logical), f"{logical} has no PhysicalResourceId"
    finally:
        cfn.delete_stack(StackName="cfn-832")
        _wait_stack(cfn, "cfn-832")

    # Post-delete: app is gone (cascade also wipes children).
    apps_after = appconfig_client.list_applications()["Items"]
    assert not any(a["Name"] == "cfn-832-app" for a in apps_after)


def test_cfn_lambda_nodejs_inline_zip(cfn, lam):
    """CFN inline ZipFile with Node.js runtime should write index.js, not index.py."""
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "cfn-nodejs-inline",
                    "Runtime": "nodejs20.x",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/r",
                    "Code": {
                        "ZipFile": 'exports.handler = async () => { return "hello"; };',
                    },
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-nodejs-inline", TemplateBody=template)
    stack = _wait_stack(cfn, "cfn-nodejs-inline")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resp = lam.invoke(FunctionName="cfn-nodejs-inline",
                      Payload=b'{}')
    assert resp["StatusCode"] == 200
    payload = resp["Payload"].read().decode()
    assert "hello" in payload

    cfn.delete_stack(StackName="cfn-nodejs-inline")
    _wait_stack(cfn, "cfn-nodejs-inline")

def test_cfn_lambda_s3_code(cfn, lam, s3):
    """CFN Lambda with Code.S3Bucket/S3Key should fetch the zip from S3
    and execute the deployed handler (not return a mock response)."""
    bucket = "cfn-lambda-code-test"
    key = "handler.zip"
    s3.create_bucket(Bucket=bucket)

    # Build a zip with a Node.js handler
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.mjs", """
export async function handler(event) {
    return { statusCode: 200, body: JSON.stringify({ ok: true }) };
}
""")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "cfn-s3-code-test",
                    "Runtime": "nodejs20.x",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/r",
                    "Environment": {"Variables": {"MY_VAR": "hello"}},
                    "Code": {"S3Bucket": bucket, "S3Key": key},
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-s3-code-test", TemplateBody=template)
    stack = _wait_stack(cfn, "cfn-s3-code-test")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resp = lam.invoke(FunctionName="cfn-s3-code-test", Payload=b'{}')
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read().decode())
    # Should execute real code, not return "Mock response"
    assert payload.get("statusCode") == 200
    body = json.loads(payload["body"])
    assert body["ok"] is True

    cfn.delete_stack(StackName="cfn-s3-code-test")
    _wait_stack(cfn, "cfn-s3-code-test")


def test_cfn_dynamodb_stream_spec(cfn, ddb):
    """CloudFormation DynamoDB table with StreamViewType (no StreamEnabled) must
    have streams enabled: LatestStreamArn and StreamSpecification present on
    describe_table, and StreamArn Fn::GetAtt output must be a valid stream ARN."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-ddb-stream-{uid}"
    table_name = f"cfn-stream-tbl-{uid}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "StreamTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": table_name,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    # CFN standard form: StreamViewType only, no StreamEnabled
                    "StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"},
                },
            },
        },
        "Outputs": {
            "StreamArn": {"Value": {"Fn::GetAtt": ["StreamTable", "StreamArn"]}},
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    # StreamArn output must look like a real DynamoDB stream ARN, not the table name
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    stream_arn = outputs.get("StreamArn", "")
    assert ":dynamodb:" in stream_arn and "/stream/" in stream_arn, (
        f"Expected a DynamoDB stream ARN, got: {stream_arn!r}"
    )

    # describe_table must expose stream info
    desc = ddb.describe_table(TableName=table_name)["Table"]
    assert desc.get("LatestStreamArn"), "LatestStreamArn missing from describe_table"
    spec = desc.get("StreamSpecification", {})
    assert spec.get("StreamViewType") == "NEW_AND_OLD_IMAGES", (
        f"StreamViewType mismatch: {spec}"
    )

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_pipes_dynamodb_stream_to_sns(cfn, ddb, sqs):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-pipe-{uid}"
    table_name = f"cfn-pipe-table-{uid}"
    queue_name = f"cfn-pipe-q-{uid}"
    topic_name = f"cfn-pipe-topic-{uid}"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "PipeTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": table_name,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    "StreamSpecification": {"StreamViewType": "NEW_AND_OLD_IMAGES"},
                },
            },
            "PipeTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": topic_name},
            },
            "PipeQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": queue_name},
            },
            "PipeSubscription": {
                "Type": "AWS::SNS::Subscription",
                "Properties": {
                    "Protocol": "sqs",
                    "TopicArn": {"Ref": "PipeTopic"},
                    "Endpoint": {"Fn::GetAtt": ["PipeQueue", "Arn"]},
                },
            },
            "DdbToSnsPipe": {
                "Type": "AWS::Pipes::Pipe",
                "Properties": {
                    "Name": f"{stack_name}-pipe",
                    "RoleArn": "arn:aws:iam::000000000000:role/test-pipe-role",
                    "Source": {"Fn::GetAtt": ["PipeTable", "StreamArn"]},
                    "Target": {"Ref": "PipeTopic"},
                    "SourceParameters": {
                        "DynamoDBStreamParameters": {"StartingPosition": "TRIM_HORIZON"}
                    },
                },
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    ddb.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": "1"},
            "val": {"S": "hello"},
        },
    )

    msg = None
    deadline = time.time() + 8
    while time.time() < deadline:
        out = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        msgs = out.get("Messages", [])
        if msgs:
            msg = msgs[0]
            break

    assert msg is not None, "Expected DynamoDB stream record to reach SNS/SQS via Pipe"

    envelope = json.loads(msg["Body"])
    rec = json.loads(envelope["Message"])
    assert rec.get("eventSource") == "aws:dynamodb"
    assert rec.get("eventName") in ("INSERT", "MODIFY", "REMOVE")

    dynamodb  = rec.get("dynamodb", {})
    assert dynamodb.get("Keys", {}).get("pk", {}).get("S") == "1"
    assert dynamodb.get("NewImage", {}).get("pk", {}).get("S") == "1"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_pipes_rejects_cross_region_target(cfn):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-pipe-xreg-{uid}"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DdbToSnsPipe": {
                "Type": "AWS::Pipes::Pipe",
                "Properties": {
                    "Name": f"{stack_name}-pipe",
                    "RoleArn": "arn:aws:iam::000000000000:role/test-pipe-role",
                    "Source": (
                        "arn:aws:dynamodb:us-east-1:000000000000:"
                        f"table/{stack_name}-table/stream/2026-05-22T00:00:00.000"
                    ),
                    "Target": f"arn:aws:sns:us-west-2:000000000000:{stack_name}-topic",
                    "SourceParameters": {
                        "DynamoDBStreamParameters": {"StartingPosition": "TRIM_HORIZON"}
                    },
                },
            },
        },
    }

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template),
            DisableRollback=True,
        )
        stack = _wait_stack(cfn, stack_name)

        assert stack["StackStatus"] == "CREATE_FAILED"
        assert _pipes.CROSS_REGION_PIPE_ERROR in stack.get("StackStatusReason", "")

        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        pipe_events = [
            event
            for event in events
            if event["LogicalResourceId"] == "DdbToSnsPipe"
        ]
        assert any(
            event["ResourceStatus"] == "CREATE_FAILED"
            and _pipes.CROSS_REGION_PIPE_ERROR in event.get("ResourceStatusReason", "")
            for event in pipe_events
        )
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass


def test_cfn_sns_topic_subscription_filter_policy_scope(cfn, sns, sqs):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sns-filter-{uid}"
    queue_name = f"cfn-sns-filter-q-{uid}"
    topic_name = f"cfn-sns-filter-topic-{uid}"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "FilterQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": queue_name},
            },
            "FilterTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {
                    "TopicName": topic_name,
                },  
            },
            "FilterSubscription": {
                "Type": "AWS::SNS::Subscription",
                "Properties": {
                    "Protocol": "sqs",
                    "TopicArn": {"Ref": "FilterTopic"},
                    "Endpoint": {"Fn::GetAtt": ["FilterQueue", "Arn"]},
                    "FilterPolicy": {"color": ["blue"]},
                },
            },
        },
        "Outputs": {
            "TopicArn": {"Value": {"Ref": "FilterTopic"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    topic_arn = outputs["TopicArn"]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    sns.publish(
        TopicArn=topic_arn,
        Message="red message",
        MessageAttributes={"color": {"DataType": "String", "StringValue": "red"}},
    )
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs.get("Messages", [])) == 0

    sns.publish(
        TopicArn=topic_arn,
        Message="blue message",
        MessageAttributes={"color": {"DataType": "String", "StringValue": "blue"}},
    )
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Message"] == "blue message"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_sns_subscription_raw_message_delivery(cfn, sns, sqs):
    """Regression: AWS::SNS::Subscription must honor RawMessageDelivery=true.
    Without it, MessageAttributes are wrapped inside the SNS envelope JSON
    instead of being delivered as SQS-level MessageAttributes — breaking
    consumers that rely on attribute-based routing or read attrs directly."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sns-raw-{uid}"
    queue_name = f"cfn-sns-raw-q-{uid}"
    topic_name = f"cfn-sns-raw-topic-{uid}"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "RawQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": queue_name},
            },
            "RawTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": topic_name},
            },
            "RawSubscription": {
                "Type": "AWS::SNS::Subscription",
                "Properties": {
                    "Protocol": "sqs",
                    "TopicArn": {"Ref": "RawTopic"},
                    "Endpoint": {"Fn::GetAtt": ["RawQueue", "Arn"]},
                    "RawMessageDelivery": True,
                },
            },
        },
        "Outputs": {
            "TopicArn": {"Value": {"Ref": "RawTopic"}},
            "SubscriptionArn": {"Value": {"Ref": "RawSubscription"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    topic_arn = outputs["TopicArn"]
    sub_arn = outputs["SubscriptionArn"]
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    sub_attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
    assert sub_attrs.get("RawMessageDelivery") == "true"

    sns.publish(
        TopicArn=topic_arn,
        Message="raw-payload",
        MessageAttributes={"ext_props": {"DataType": "String", "StringValue": "k=v"}},
    )
    msgs = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2,
        MessageAttributeNames=["All"],
    )
    assert len(msgs.get("Messages", [])) == 1
    m = msgs["Messages"][0]
    assert m["Body"] == "raw-payload"
    assert m.get("MessageAttributes", {}).get("ext_props", {}).get("StringValue") == "k=v"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


# ===========================================================================
# CodeBuild Project Tests
# ===========================================================================

def test_cfn_codebuild_project_basic(cfn, codebuild):
    """CFN stack with a minimal CodeBuild project deploys successfully."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t01",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-cb-t01", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t01")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify project exists via CodeBuild API
    result = codebuild.batch_get_projects(names=["cfn-cb-t01"])
    assert len(result["projects"]) == 1
    assert result["projects"][0]["name"] == "cfn-cb-t01"

    # Delete stack and verify cleanup
    cfn.delete_stack(StackName="cfn-cb-t01")
    _wait_stack(cfn, "cfn-cb-t01")
    result = codebuild.batch_get_projects(names=["cfn-cb-t01"])
    assert len(result["projects"]) == 0


def test_cfn_codebuild_project_auto_name(cfn, codebuild):
    """When Name is omitted, _physical_name() generates one."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-cb-t02", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t02")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Find the auto-generated project name via stack resources
    resources = cfn.describe_stack_resources(StackName="cfn-cb-t02")["StackResources"]
    project_name = next(r["PhysicalResourceId"] for r in resources if r["ResourceType"] == "AWS::CodeBuild::Project")
    assert project_name.startswith("cfn-cb-t02-Project-")

    # Verify it exists
    result = codebuild.batch_get_projects(names=[project_name])
    assert len(result["projects"]) == 1

    cfn.delete_stack(StackName="cfn-cb-t02")
    _wait_stack(cfn, "cfn-cb-t02")


def test_cfn_codebuild_project_getatt_arn(cfn, codebuild):
    """Fn::GetAtt on Arn attribute resolves correctly."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t03",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            }
        },
        "Outputs": {
            "ProjectArn": {"Value": {"Fn::GetAtt": ["Project", "Arn"]}},
        },
    }
    cfn.create_stack(StackName="cfn-cb-t03", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t03")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["ProjectArn"].startswith("arn:aws:codebuild:")
    assert outputs["ProjectArn"].endswith(":project/cfn-cb-t03")

    cfn.delete_stack(StackName="cfn-cb-t03")
    _wait_stack(cfn, "cfn-cb-t03")


def test_cfn_codebuild_project_tags(cfn, codebuild):
    """CFN Tags (capitalised Key/Value) are translated correctly."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t04",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                    "Tags": [
                        {"Key": "env", "Value": "test"},
                        {"Key": "team", "Value": "platform"},
                    ],
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-cb-t04", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t04")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    result = codebuild.batch_get_projects(names=["cfn-cb-t04"])
    tags = {t["key"]: t["value"] for t in result["projects"][0]["tags"]}
    assert tags["env"] == "test"
    assert tags["team"] == "platform"

    cfn.delete_stack(StackName="cfn-cb-t04")
    _wait_stack(cfn, "cfn-cb-t04")


def test_cfn_codebuild_project_with_iam_role(cfn, codebuild, iam):
    """Project references IAM role via Fn::GetAtt."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "cfn-cb-t05-role",
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect": "Allow",
                            "Principal": {"Service": "codebuild.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }],
                    },
                },
            },
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t05",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": {"Fn::GetAtt": ["Role", "Arn"]},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-cb-t05", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t05")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    role_arn = iam.get_role(RoleName="cfn-cb-t05-role")["Role"]["Arn"]
    result = codebuild.batch_get_projects(names=["cfn-cb-t05"])
    assert result["projects"][0]["serviceRole"] == role_arn

    cfn.delete_stack(StackName="cfn-cb-t05")
    _wait_stack(cfn, "cfn-cb-t05")


def test_cfn_codebuild_project_duplicate_name_fails(cfn, codebuild):
    """Duplicate project name causes CREATE_FAILED."""
    # Pre-create the project directly via CodeBuild API
    codebuild.create_project(
        name="cfn-cb-t06-dup",
        source={"type": "NO_SOURCE"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t06-dup",  # Same name — should fail
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-cb-t06", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cb-t06")
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

    # Cleanup
    cfn.delete_stack(StackName="cfn-cb-t06")
    _wait_stack(cfn, "cfn-cb-t06")
    codebuild.delete_project(name="cfn-cb-t06-dup")


def test_cfn_codebuild_project_idempotent_delete(cfn, codebuild):
    """Delete is idempotent — double delete does not crash."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": "cfn-cb-t07",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-cb-t07", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-cb-t07")

    # First delete
    cfn.delete_stack(StackName="cfn-cb-t07")
    _wait_stack(cfn, "cfn-cb-t07")

    # Second delete — must not raise
    cfn.delete_stack(StackName="cfn-cb-t07")
    stack = _wait_stack(cfn, "cfn-cb-t07")
    assert stack["StackStatus"] in ("DELETE_COMPLETE", "DOES_NOT_EXIST")


def test_cfn_scheduler_schedule(cfn):
    """AWS::Scheduler::Schedule and ScheduleGroup should provision and delete cleanly."""
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Group": {
                "Type": "AWS::Scheduler::ScheduleGroup",
                "Properties": {"Name": "cfn-test-group"},
            },
            "Schedule": {
                "Type": "AWS::Scheduler::Schedule",
                "Properties": {
                    "Name": "cfn-test-schedule",
                    "GroupName": "cfn-test-group",
                    "ScheduleExpression": "rate(5 minutes)",
                    "FlexibleTimeWindow": {"Mode": "OFF"},
                    "Target": {
                        "Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                        "RoleArn": "arn:aws:iam::000000000000:role/test",
                    },
                },
            },
        },
    })
    cfn.create_stack(StackName="cfn-scheduler-test", TemplateBody=template)
    stack = _wait_stack(cfn, "cfn-scheduler-test")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resources = {
        r["ResourceType"]: r
        for r in cfn.list_stack_resources(StackName="cfn-scheduler-test")["StackResourceSummaries"]
    }
    assert "AWS::Scheduler::Schedule" in resources
    assert resources["AWS::Scheduler::Schedule"]["PhysicalResourceId"] == "cfn-test-schedule"
    assert "AWS::Scheduler::ScheduleGroup" in resources
    assert resources["AWS::Scheduler::ScheduleGroup"]["PhysicalResourceId"] == "cfn-test-group"

    cfn.delete_stack(StackName="cfn-scheduler-test")
    stack = _wait_stack(cfn, "cfn-scheduler-test")
    assert stack["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_eventbus_basic(cfn, eb):
    """Test basic EventBus create and delete."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {"Name": "cfn-eb-t01"},
            }
        },
    }
    cfn.create_stack(StackName="cfn-eb-t01", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t01")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bus = eb.describe_event_bus(Name="cfn-eb-t01")
    assert bus["Name"] == "cfn-eb-t01"
    assert "arn:aws:events:" in bus["Arn"]

    cfn.delete_stack(StackName="cfn-eb-t01")
    _wait_stack(cfn, "cfn-eb-t01")
    with pytest.raises(ClientError):
        eb.describe_event_bus(Name="cfn-eb-t01")


def test_cfn_eventbus_auto_name(cfn, eb):
    """Test EventBus with auto-generated name."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {},
            }
        },
    }
    cfn.create_stack(StackName="cfn-eb-t02", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t02")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resources = cfn.describe_stack_resources(StackName="cfn-eb-t02")["StackResources"]
    bus_name = next(r["PhysicalResourceId"] for r in resources if r["ResourceType"] == "AWS::Events::EventBus")
    assert bus_name.startswith("cfn-eb-t02-Bus-")

    bus = eb.describe_event_bus(Name=bus_name)
    assert bus["Name"] == bus_name

    cfn.delete_stack(StackName="cfn-eb-t02")
    _wait_stack(cfn, "cfn-eb-t02")


def test_cfn_eventbus_getatt_arn(cfn, eb):
    """Test Fn::GetAtt for Arn and Name attributes."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {"Name": "cfn-eb-t03"},
            }
        },
        "Outputs": {
            "BusArn": {"Value": {"Fn::GetAtt": ["Bus", "Arn"]}},
            "BusName": {"Value": {"Fn::GetAtt": ["Bus", "Name"]}},
        },
    }
    cfn.create_stack(StackName="cfn-eb-t03", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t03")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["BusArn"].startswith("arn:aws:events:")
    assert outputs["BusArn"].endswith(":event-bus/cfn-eb-t03")
    assert outputs["BusName"] == "cfn-eb-t03"

    cfn.delete_stack(StackName="cfn-eb-t03")
    _wait_stack(cfn, "cfn-eb-t03")


def test_cfn_eventbus_tags(cfn, eb):
    """Test EventBus tags are propagated."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {
                    "Name": "cfn-eb-t04",
                    "Tags": [
                        {"Key": "env", "Value": "test"},
                        {"Key": "team", "Value": "platform"},
                    ],
                },
            }
        },
    }
    cfn.create_stack(StackName="cfn-eb-t04", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t04")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bus = eb.describe_event_bus(Name="cfn-eb-t04")
    tags = eb.list_tags_for_resource(ResourceARN=bus["Arn"])["Tags"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "platform"

    cfn.delete_stack(StackName="cfn-eb-t04")
    _wait_stack(cfn, "cfn-eb-t04")


def test_cfn_eventbus_with_rule(cfn, eb):
    """Test EventBus with EventBridge Rule on custom bus."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {"Name": "cfn-eb-t05"},
            },
            "Rule": {
                "Type": "AWS::Events::Rule",
                "Properties": {
                    "Name": "cfn-eb-t05-rule",
                    "EventBusName": {"Ref": "Bus"},
                    "EventPattern": {"source": ["my.app"]},
                    "State": "ENABLED",
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-eb-t05", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t05")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bus = eb.describe_event_bus(Name="cfn-eb-t05")
    assert bus["Name"] == "cfn-eb-t05"

    rules = eb.list_rules(EventBusName="cfn-eb-t05")["Rules"]
    assert any(r["Name"] == "cfn-eb-t05-rule" for r in rules)

    cfn.delete_stack(StackName="cfn-eb-t05")
    _wait_stack(cfn, "cfn-eb-t05")


def test_cfn_eventbus_duplicate_name_fails(cfn, eb):
    """Test that duplicate EventBus name causes ROLLBACK_COMPLETE."""
    eb.create_event_bus(Name="cfn-eb-t06-dup")

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {"Name": "cfn-eb-t06-dup"},
            }
        },
    }
    cfn.create_stack(StackName="cfn-eb-t06", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t06")
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

    cfn.delete_stack(StackName="cfn-eb-t06")
    _wait_stack(cfn, "cfn-eb-t06")
    eb.delete_event_bus(Name="cfn-eb-t06-dup")


def test_cfn_eventbus_default_name_fails(cfn, eb):
    """Test that 'default' bus name causes ROLLBACK_COMPLETE."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bus": {
                "Type": "AWS::Events::EventBus",
                "Properties": {"Name": "default"},
            }
        },
    }
    cfn.create_stack(StackName="cfn-eb-t07", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-eb-t07")
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

    cfn.delete_stack(StackName="cfn-eb-t07")
    _wait_stack(cfn, "cfn-eb-t07")

    # Default bus must still exist and be unaffected
    bus = eb.describe_event_bus(Name="default")
    assert bus["Name"] == "default"


def test_cfn_aws_region_pseudo_param_uses_caller_region():
    """CFN's AWS::Region pseudo-param must resolve to the caller's request region,
    not MINISTACK_REGION (issue #398 — CDK bootstrap resources inheriting wrong region)."""
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    # Caller explicitly uses us-east-2 via SigV4 Credential scope.
    def _client(svc: str):
        return boto3.client(
            svc, endpoint_url=endpoint, region_name="us-east-2",
            aws_access_key_id="test", aws_secret_access_key="test",
            config=Config(retries={"mode": "standard"}),
        )

    cfn_us2 = _client("cloudformation")
    s3_us2 = _client("s3")

    template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  RegionalBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "rgn-test-${AWS::Region}"
Outputs:
  Region:
    Value: !Ref AWS::Region
  BucketName:
    Value: !Ref RegionalBucket
"""

    stack_name = "cfn-region-398"
    try:
        cfn_us2.delete_stack(StackName=stack_name)
    except Exception:
        pass

    cfn_us2.create_stack(StackName=stack_name, TemplateBody=template)
    _wait_stack(cfn_us2, stack_name)

    stack = cfn_us2.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["Region"] == "us-east-2", \
        f"AWS::Region should resolve to caller's region, got {outputs['Region']!r}"
    assert outputs["BucketName"] == "rgn-test-us-east-2"

    # Stack ARN itself must carry the caller's region, not us-east-1.
    assert ":us-east-2:" in stack["StackId"], f"StackId missing caller region: {stack['StackId']!r}"

    # And the bucket was actually created with that name.
    buckets = [b["Name"] for b in s3_us2.list_buckets()["Buckets"]]
    assert "rgn-test-us-east-2" in buckets


def test_cfn_cognito_user_pool_client_generate_secret(cfn, cognito_idp):
    """CFN AWS::Cognito::UserPoolClient with GenerateSecret=true creates a
    ClientSecret; GenerateSecret=false/absent leaves it None (#403)."""
    template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: cfn-upc-secret-pool
  ClientWithSecret:
    Type: AWS::Cognito::UserPoolClient
    Properties:
      UserPoolId: !Ref Pool
      ClientName: with-secret
      GenerateSecret: true
  ClientWithoutSecret:
    Type: AWS::Cognito::UserPoolClient
    Properties:
      UserPoolId: !Ref Pool
      ClientName: no-secret
      GenerateSecret: false
Outputs:
  PoolId:
    Value: !Ref Pool
  ClientWithSecretId:
    Value: !Ref ClientWithSecret
  ClientWithoutSecretId:
    Value: !Ref ClientWithoutSecret
"""
    stack_name = "cfn-upc-secret"
    try:
        cfn.delete_stack(StackName=stack_name)
    except Exception:
        pass
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    _wait_stack(cfn, stack_name)

    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    pool_id = outputs["PoolId"]

    with_resp = cognito_idp.describe_user_pool_client(
        UserPoolId=pool_id, ClientId=outputs["ClientWithSecretId"],
    )
    without_resp = cognito_idp.describe_user_pool_client(
        UserPoolId=pool_id, ClientId=outputs["ClientWithoutSecretId"],
    )
    assert with_resp["UserPoolClient"].get("ClientSecret"), "GenerateSecret=true should produce a non-empty ClientSecret"
    assert not without_resp["UserPoolClient"].get("ClientSecret"), "GenerateSecret=false should leave ClientSecret empty"


def test_cfn_cognito_user_pool_group(cfn, cognito_idp):
    """CFN AWS::Cognito::UserPoolGroup creates a group whose Ref resolves to
    its GroupName, matching real AWS, and admin_add_user_to_group can then
    reference it."""
    template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: cfn-group-pool
  AdminGroup:
    Type: AWS::Cognito::UserPoolGroup
    Properties:
      UserPoolId: !Ref Pool
      GroupName: admins
      Description: Administrators
      Precedence: 1
Outputs:
  PoolId:
    Value: !Ref Pool
  GroupRef:
    Value: !Ref AdminGroup
"""
    stack_name = "cfn-cognito-group"
    try:
        cfn.delete_stack(StackName=stack_name)
    except Exception:
        pass
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    _wait_stack(cfn, stack_name)

    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["GroupRef"] == "admins"

    group = cognito_idp.get_group(UserPoolId=outputs["PoolId"], GroupName="admins")["Group"]
    assert group["Description"] == "Administrators"
    assert group["Precedence"] == 1

    cognito_idp.admin_create_user(UserPoolId=outputs["PoolId"], Username="alice")
    cognito_idp.admin_add_user_to_group(UserPoolId=outputs["PoolId"], Username="alice", GroupName="admins")
    groups = cognito_idp.admin_list_groups_for_user(UserPoolId=outputs["PoolId"], Username="alice")["Groups"]
    assert any(g["GroupName"] == "admins" for g in groups)

    cfn.delete_stack(StackName=stack_name)
    with pytest.raises(ClientError) as exc:
        cognito_idp.get_group(UserPoolId=outputs["PoolId"], GroupName="admins")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_cognito_user_pool_resource_server(cfn, cognito_idp):
    """CFN AWS::Cognito::UserPoolResourceServer creates a resource server
    whose Ref resolves to its Identifier, matching real AWS."""
    template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: cfn-resource-server-pool
  ApiResourceServer:
    Type: AWS::Cognito::UserPoolResourceServer
    Properties:
      UserPoolId: !Ref Pool
      Identifier: API
      Name: API
      Scopes:
        - ScopeName: resource.get
          ScopeDescription: Read access
Outputs:
  PoolId:
    Value: !Ref Pool
  ResourceServerRef:
    Value: !Ref ApiResourceServer
"""
    stack_name = "cfn-resource-server"
    try:
        cfn.delete_stack(StackName=stack_name)
    except Exception:
        pass
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    _wait_stack(cfn, stack_name)

    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["ResourceServerRef"] == "API"

    server = cognito_idp.describe_resource_server(
        UserPoolId=outputs["PoolId"], Identifier="API",
    )["ResourceServer"]
    assert server["Name"] == "API"
    assert server["Scopes"] == [{"ScopeName": "resource.get", "ScopeDescription": "Read access"}]

    cfn.delete_stack(StackName=stack_name)
    with pytest.raises(ClientError) as exc:
        cognito_idp.describe_resource_server(UserPoolId=outputs["PoolId"], Identifier="API")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_cognito_user_pool_lambda_config(cfn, cognito_idp):
    """AWS::Cognito::UserPool's LambdaConfig property is honored when the pool
    is provisioned via CloudFormation, not just via the raw CreateUserPool API.

    _cognito_user_pool_create previously built the pool's state dict without
    ever reading props["LambdaConfig"], so a CFN-provisioned pool's Lambda
    triggers (PreTokenGeneration here, but the gap applied to all of them)
    were silently dropped — the trigger Lambda was never invoked and tokens
    were issued unmodified, even though the identical raw API call already
    honored LambdaConfig correctly.
    """
    template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  TriggerRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
  TriggerFn:
    Type: AWS::Lambda::Function
    Properties:
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt TriggerRole.Arn
      Code:
        ZipFile: |
          def handler(event, context):
              event['response']['claimsOverrideDetails'] = {
                  'claimsToAddOrOverride': {'injected_claim': 'from-cfn-trigger'},
              }
              return event
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: cfn-pretoken-pool
      LambdaConfig:
        PreTokenGeneration: !GetAtt TriggerFn.Arn
  Client:
    Type: AWS::Cognito::UserPoolClient
    Properties:
      UserPoolId: !Ref Pool
      ExplicitAuthFlows:
        - ALLOW_USER_PASSWORD_AUTH
Outputs:
  PoolId:
    Value: !Ref Pool
  ClientId:
    Value: !Ref Client
"""
    stack_name = "cfn-cognito-pretoken"
    try:
        cfn.delete_stack(StackName=stack_name)
    except Exception:
        pass
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    _wait_stack(cfn, stack_name)

    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    pool_id, client_id = outputs["PoolId"], outputs["ClientId"]

    desc = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
    assert desc["LambdaConfig"]["PreTokenGeneration"]

    cognito_idp.admin_create_user(
        UserPoolId=pool_id, Username="pretoken-user",
        TemporaryPassword="Temp1234!", MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username="pretoken-user", Password="Pwd1234!", Permanent=True,
    )
    tok = cognito_idp.initiate_auth(
        ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "pretoken-user", "PASSWORD": "Pwd1234!"},
    )["AuthenticationResult"]

    id_payload = tok["IdToken"].split(".")[1]
    id_payload += "=" * (-len(id_payload) % 4)
    id_claims = json.loads(base64.urlsafe_b64decode(id_payload))
    assert id_claims.get("injected_claim") == "from-cfn-trigger"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc:
        cognito_idp.describe_user_pool(UserPoolId=pool_id)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# ApiGatewayV2 Integration + Route provisioners
# ---------------------------------------------------------------------------

def test_cfn_apigwv2_integration_basic(cfn, apigw):
    """CFN stack with ApiGatewayV2 Api + Integration deploys successfully."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-int-t01",
                    "ProtocolType": "HTTP",
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                    "PayloadFormatVersion": "2.0",
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-int-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify integration exists via ApiGatewayV2 API
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
    api_id = api_res["PhysicalResourceId"]

    integrations = apigw.get_integrations(ApiId=api_id)["Items"]
    assert len(integrations) == 1
    assert integrations[0]["IntegrationType"] == "AWS_PROXY"
    assert integrations[0]["PayloadFormatVersion"] == "2.0"

    # Delete and verify cleanup
    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert apigw.get_integrations(ApiId=api_id)["Items"] == []


def test_cfn_apigwv2_ms_custom_id(cfn, apigw):
    """CloudFormation ms-custom-id tag pins the ApiGatewayV2 API id (issue #400)."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-custom-id-t01",
                    "ProtocolType": "HTTP",
                    "Tags": {"ms-custom-id": "cfn-pinned-api"},
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-custom-id-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
    assert api_res["PhysicalResourceId"] == "cfn-pinned-api"

    api = apigw.get_api(ApiId="cfn-pinned-api")
    assert api["ApiId"] == "cfn-pinned-api"
    assert api["Name"] == "cfn-apigwv2-custom-id-t01"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigwv2_route_basic(cfn, apigw):
    """CFN stack with ApiGatewayV2 Api + Integration + Route deploys successfully."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-route-t01",
                    "ProtocolType": "HTTP",
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                    "PayloadFormatVersion": "2.0",
                },
            },
            "DefaultRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "ANY /{proxy+}",
                    "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integration"}]]},
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-route-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify route exists via ApiGatewayV2 API
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
    api_id = api_res["PhysicalResourceId"]

    routes = apigw.get_routes(ApiId=api_id)["Items"]
    assert len(routes) == 1
    assert routes[0]["RouteKey"] == "ANY /{proxy+}"
    assert "integrations/" in routes[0].get("Target", "")

    # Delete and verify cleanup
    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert apigw.get_routes(ApiId=api_id)["Items"] == []


def test_cfn_apigwv2_authorizer_jwt(cfn, apigw):
    """CFN stack with an AWS::ApiGatewayV2::Authorizer deploys successfully and
    a Route referencing it via AuthorizerId is enforced at request time.

    Regression test: AWS::ApiGatewayV2::Authorizer previously had no CFN
    provisioner at all ("Unsupported resource type"), even though the
    control-plane CreateAuthorizer API (and Terraform's
    aws_apigatewayv2_authorizer) already worked.
    """
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-authorizer-t01",
                    "ProtocolType": "HTTP",
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                    "PayloadFormatVersion": "2.0",
                },
            },
            "JwtAuthorizer": {
                "Type": "AWS::ApiGatewayV2::Authorizer",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "Name": "cfn-apigwv2-authorizer-t01-jwt",
                    "AuthorizerType": "JWT",
                    "IdentitySource": ["$request.header.Authorization"],
                    "JwtConfiguration": {
                        "Audience": ["client-id"],
                        "Issuer": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_example",
                    },
                },
            },
            "ProtectedRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "GET /protected",
                    "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integration"}]]},
                    "AuthorizationType": "JWT",
                    "AuthorizerId": {"Ref": "JwtAuthorizer"},
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-authorizer-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
    api_id = api_res["PhysicalResourceId"]
    authorizer_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Authorizer"][0]
    authorizer_id = authorizer_res["PhysicalResourceId"]

    authorizers = apigw.get_authorizers(ApiId=api_id)["Items"]
    assert len(authorizers) == 1
    assert authorizers[0]["AuthorizerId"] == authorizer_id
    assert authorizers[0]["AuthorizerType"] == "JWT"
    assert authorizers[0]["JwtConfiguration"]["Audience"] == ["client-id"]
    assert authorizers[0]["JwtConfiguration"]["Issuer"] == "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_example"

    routes = apigw.get_routes(ApiId=api_id)["Items"]
    assert len(routes) == 1
    assert routes[0]["AuthorizationType"] == "JWT"
    assert routes[0]["AuthorizerId"] == authorizer_id

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert apigw.get_authorizers(ApiId=api_id)["Items"] == []


def test_cfn_apigwv2_integration_getatt(cfn, apigw):
    """Fn::GetAtt on IntegrationId resolves correctly."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-int-t02",
                    "ProtocolType": "HTTP",
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                    "PayloadFormatVersion": "2.0",
                },
            },
        },
        "Outputs": {
            "IntegrationId": {"Value": {"Fn::GetAtt": ["Integration", "IntegrationId"]}},
            "ApiId": {"Value": {"Ref": "HttpApi"}},
        },
    }
    stack_name = "cfn-apigwv2-int-t02"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert "IntegrationId" in outputs
    assert len(outputs["IntegrationId"]) == 8  # UUID[:8]

    # Verify the integration ID matches what the API returns
    integrations = apigw.get_integrations(ApiId=outputs["ApiId"])["Items"]
    assert integrations[0]["IntegrationId"] == outputs["IntegrationId"]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigwv2_route_getatt(cfn, apigw):
    """Fn::GetAtt on RouteId resolves correctly."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "cfn-apigwv2-route-t02",
                    "ProtocolType": "HTTP",
                },
            },
            "MyRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "GET /health",
                },
            },
        },
        "Outputs": {
            "RouteId": {"Value": {"Fn::GetAtt": ["MyRoute", "RouteId"]}},
            "ApiId": {"Value": {"Ref": "HttpApi"}},
        },
    }
    stack_name = "cfn-apigwv2-route-t02"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert "RouteId" in outputs
    assert len(outputs["RouteId"]) == 8  # UUID[:8]

    # Verify the route ID matches what the API returns
    routes = apigw.get_routes(ApiId=outputs["ApiId"])["Items"]
    assert routes[0]["RouteId"] == outputs["RouteId"]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigwv2_integration_idempotent_delete(cfn):
    """Deleting a stack with an integration twice does not crash."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {"Name": "cfn-apigwv2-int-t03", "ProtocolType": "HTTP"},
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-int-t03"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    _wait_stack(cfn, stack_name)

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)

    # Second delete should not raise
    cfn.delete_stack(StackName=stack_name)


def test_cfn_apigwv2_full_http_api_stack(cfn, apigw):
    """Full HTTP API stack with Api + Stage + Integration + Route deploys and cleans up."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {"Name": "cfn-apigwv2-full-t01", "ProtocolType": "HTTP"},
            },
            "Stage": {
                "Type": "AWS::ApiGatewayV2::Stage",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "StageName": "$default",
                    "AutoDeploy": True,
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:my-handler",
                    "PayloadFormatVersion": "2.0",
                },
            },
            "ProxyRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "ANY /{proxy+}",
                    "Target": {"Fn::Join": ["/", ["integrations", {"Ref": "Integration"}]]},
                },
            },
        },
    }
    stack_name = "cfn-apigwv2-full-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
    api_id = api_res["PhysicalResourceId"]

    # All four resource types should exist
    assert len(apigw.get_integrations(ApiId=api_id)["Items"]) == 1
    assert len(apigw.get_routes(ApiId=api_id)["Items"]) == 1
    assert len(apigw.get_stages(ApiId=api_id)["Items"]) == 1

    # Delete and verify all resources cleaned up
    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)

    assert apigw.get_integrations(ApiId=api_id)["Items"] == []
    assert apigw.get_routes(ApiId=api_id)["Items"] == []


def test_cfn_apigwv2_integration_ref_returns_integration_id_alone(cfn, apigw):
    """Regression: Ref on AWS::ApiGatewayV2::Integration must return the bare
    integration ID (e.g. "abcd123"), NOT "{apiId}/{integrationId}".

    Per AWS CloudFormation Template Reference:
      "Ref returns the Integration resource ID, such as abcd123."
      https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-apigatewayv2-integration.html#aws-resource-apigatewayv2-integration-return-values

    A Route's Target is built by substituting the Integration's Ref into
    "integrations/${Integration}". If Ref returns "{apiId}/{integrationId}",
    the route target becomes "integrations/{apiId}/{integrationId}", which
    cannot be matched against the integration store at request time.
    """
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {"Name": "cfn-apigwv2-ref-t01", "ProtocolType": "HTTP"},
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": "arn:aws:lambda:us-east-1:000000000000:function:dummy",
                    "PayloadFormatVersion": "2.0",
                },
            },
            "Route": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "GET /hello",
                    "Target": {"Fn::Sub": "integrations/${Integration}"},
                },
            },
        },
        "Outputs": {
            "IntegrationRef": {"Value": {"Ref": "Integration"}},
            "ApiId": {"Value": {"Ref": "HttpApi"}},
        },
    }
    stack_name = "cfn-apigwv2-ref-t01"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    api_id = outputs["ApiId"]
    integration_ref = outputs["IntegrationRef"]

    # The Integration Ref must be the bare integration ID (no slash).
    integrations = apigw.get_integrations(ApiId=api_id)["Items"]
    assert len(integrations) == 1
    actual_int_id = integrations[0]["IntegrationId"]
    assert integration_ref == actual_int_id, (
        f"Ref returned {integration_ref!r}, expected bare integration ID "
        f"{actual_int_id!r}. AWS spec requires Ref to return the integration "
        f"ID alone, not '{{apiId}}/{{integrationId}}'."
    )
    assert "/" not in integration_ref, (
        f"Ref returned {integration_ref!r} containing '/'. AWS returns just "
        f"the integration ID, never a composite identifier."
    )

    # The route target should resolve to integrations/<int_id>, not
    # integrations/<api_id>/<int_id>.
    routes = apigw.get_routes(ApiId=api_id)["Items"]
    assert len(routes) == 1
    target = routes[0].get("Target", "")
    assert target == f"integrations/{actual_int_id}", (
        f"Route target is {target!r}, expected 'integrations/{actual_int_id}'. "
        f"A malformed target prevents handle_execute() from matching the route "
        f"to its integration at request time."
    )

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigwv2_full_http_api_stack_invokes_lambda(cfn, apigw, lam):
    """Regression: an HTTP API deployed via CFN must actually route requests
    through to the Lambda integration. PR #480's tests validated resource
    creation and Fn::GetAtt but never sent a request through the deployed API,
    so a broken physical_id (used by Ref) went undetected — every CFN-deployed
    HTTP API returned 500 'No integration configured' at request time.
    """
    import urllib.request as _urlreq

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    execute_port = urlparse(endpoint).port or 4566

    fname = f"cfn-e2e-fn-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'headers': {'Content-Type': 'application/json'},\n"
        b"        'body': json.dumps({'path': event.get('rawPath', '/'), 'ok': True}),\n"
        b"    }\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {"Name": f"cfn-e2e-{fname}", "ProtocolType": "HTTP"},
            },
            "Stage": {
                "Type": "AWS::ApiGatewayV2::Stage",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "StageName": "$default",
                    "AutoDeploy": True,
                },
            },
            "Integration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
                    "PayloadFormatVersion": "2.0",
                },
            },
            "ProxyRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "HttpApi"},
                    "RouteKey": "ANY /{proxy+}",
                    "Target": {"Fn::Sub": "integrations/${Integration}"},
                },
            },
        },
        "Outputs": {"ApiId": {"Value": {"Ref": "HttpApi"}}},
    }
    stack_name = f"cfn-e2e-{fname}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    api_id = outputs["ApiId"]

    # Send a real HTTP request through the deployed API.
    url = f"http://{api_id}.execute-api.localhost:{execute_port}/$default/hello"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{execute_port}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200, f"Expected 200, got {resp.status}"
    body = json.loads(resp.read())
    assert body["ok"] is True
    assert body["path"] == "/hello"

    # Cleanup
    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    lam.delete_function(FunctionName=fname)


# ---------------------------------------------------------------------------
# AWS::CloudFront::KeyValueStore — covers create, in-place update via
# UpdateStack (Comment change), and stack-delete teardown.
# ---------------------------------------------------------------------------

_KVS_TEMPLATE_V1 = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  EdgeRoutes:
    Type: AWS::CloudFront::KeyValueStore
    Properties:
      Name: %(name)s
      Comment: initial
Outputs:
  KvsArn:
    Value: !GetAtt EdgeRoutes.Arn
  KvsId:
    Value: !GetAtt EdgeRoutes.Id
"""

_KVS_TEMPLATE_V2 = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  EdgeRoutes:
    Type: AWS::CloudFront::KeyValueStore
    Properties:
      Name: %(name)s
      Comment: updated by UpdateStack
Outputs:
  KvsArn:
    Value: !GetAtt EdgeRoutes.Arn
"""


def test_cfn_cloudfront_keyvaluestore_create_update_delete(cfn, cloudfront):
    """AWS::CloudFront::KeyValueStore: create via CFN, update Comment via
    UpdateStack (in-place; AWS spec only allows Comment to change), describe
    through the native CloudFront API to confirm the new Comment, then
    delete via the stack."""
    stack_name = f"e2e-kvs-{_uuid_mod.uuid4().hex[:8]}"
    kvs_name = f"cfnkvs-{_uuid_mod.uuid4().hex[:8]}"

    cfn.create_stack(StackName=stack_name, TemplateBody=_KVS_TEMPLATE_V1 % {"name": kvs_name})
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Outputs carry the ARN + Id from the provisioner.
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    assert outputs["KvsArn"].endswith(f":key-value-store/{kvs_name}")
    assert outputs["KvsId"]

    # Native describe sees the create-time Comment.
    desc = cloudfront.describe_key_value_store(Name=kvs_name)
    assert desc["KeyValueStore"]["Comment"] == "initial"

    # UpdateStack changes the Comment in place — same physical name, no replacement.
    cfn.update_stack(StackName=stack_name, TemplateBody=_KVS_TEMPLATE_V2 % {"name": kvs_name})
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    desc = cloudfront.describe_key_value_store(Name=kvs_name)
    assert desc["KeyValueStore"]["Comment"] == "updated by UpdateStack"

    # Stack delete cleans up the KVS.
    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc:
        cloudfront.describe_key_value_store(Name=kvs_name)
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cfn_cloudfront_origin_access_identity_attributes(cfn):
    """CloudFront OAIs expose stable CFN identities and canonical user IDs."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cloudfront-oai-{suffix}"

    def template(comment):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "OriginAccessIdentity": {
                    "Type": "AWS::CloudFront::CloudFrontOriginAccessIdentity",
                    "Properties": {
                        "CloudFrontOriginAccessIdentityConfig": {
                            "Comment": comment,
                        },
                    },
                },
            },
            "Outputs": {
                "OaiRef": {"Value": {"Ref": "OriginAccessIdentity"}},
                "OaiId": {
                    "Value": {"Fn::GetAtt": ["OriginAccessIdentity", "Id"]},
                },
                "CanonicalUserId": {
                    "Value": {
                        "Fn::GetAtt": [
                            "OriginAccessIdentity",
                            "S3CanonicalUserId",
                        ],
                    },
                },
            },
        }

    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("initial comment")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    assert outputs["OaiRef"] == outputs["OaiId"]
    assert re.fullmatch(r"E[A-Z0-9]{13}", outputs["OaiId"])
    assert re.fullmatch(r"[0-9a-f]{64}", outputs["CanonicalUserId"])

    original_outputs = outputs
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("updated comment")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
    assert outputs == original_outputs

    cfn.delete_stack(StackName=stack_name)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_cloudfront_distribution_supports_invalidations(cfn, cloudfront):
    """A distribution provisioned through CloudFormation must initialize the
    invalidation state used by the native CloudFront API (#1147)."""
    stack_name = f"cfn-cloudfront-invalidation-{_uuid_mod.uuid4().hex[:8]}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Distribution": {
                "Type": "AWS::CloudFront::Distribution",
                "Properties": {
                    "DistributionConfig": {
                        "Enabled": True,
                    },
                },
            },
        },
        "Outputs": {
            "DistributionId": {"Value": {"Ref": "Distribution"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    dist_id = outputs["DistributionId"]
    response = cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/*"]},
            "CallerReference": f"cfn-invalidation-{_uuid_mod.uuid4().hex}",
        },
    )
    assert response["Invalidation"]["Status"] == "Completed"
    assert response["Invalidation"]["InvalidationBatch"]["Paths"]["Items"] == ["/*"]

    cfn.delete_stack(StackName=stack_name)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_auto_named_s3_bucket_stable_across_updates(cfn, s3):
    """Regression: auto-named S3 buckets (no explicit BucketName) must keep
    the same physical resource ID across stack updates.  Before the fix,
    _update_resource fell through to _s3_create which generated a new random
    name on every update, orphaning the original bucket and all its objects."""
    stack_name = f"cfn-s3-stable-{_uuid_mod.uuid4().hex[:8]}"
    template_v1 = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DeployBucket": {
                "Type": "AWS::S3::Bucket",
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "DeployBucket"}},
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template_v1)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    bucket_v1 = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["BucketName"]

    s3.put_object(Bucket=bucket_v1, Key="artifact.zip", Body=b"zipdata")

    template_v2 = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DeployBucket": {
                "Type": "AWS::S3::Bucket",
            },
            "LogGroup": {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {"LogGroupName": f"/test/{stack_name}"},
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "DeployBucket"}},
        },
    })
    cfn.update_stack(StackName=stack_name, TemplateBody=template_v2)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    bucket_v2 = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["BucketName"]

    assert bucket_v1 == bucket_v2, (
        f"Auto-named bucket changed from {bucket_v1!r} to {bucket_v2!r} on update"
    )

    obj = s3.get_object(Bucket=bucket_v2, Key="artifact.zip")
    assert obj["Body"].read() == b"zipdata"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_lambda_s3_ref_bucket_has_code_size(cfn, lam, s3):
    """Regression: Lambda deployed via CFN with Code.S3Bucket using
    {Ref: DeployBucket} must report correct CodeSize and CodeSha256
    (not NaN / 'cfn-deployed'), and the code must be downloadable."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-lam-s3ref-{uid}"
    fn_name = f"cfn-lam-s3ref-fn-{uid}"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.mjs",
            'export async function handler(event) '
            '{ return { statusCode: 200, body: "ok" }; }')
    zip_bytes = buf.getvalue()

    template_create = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DeployBucket": {"Type": "AWS::S3::Bucket"},
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "DeployBucket"}},
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template_create)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    bucket = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["BucketName"]

    s3_key = f"deploy/{uid}/code.zip"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=zip_bytes)

    template_update = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "DeployBucket": {"Type": "AWS::S3::Bucket"},
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": fn_name,
                    "Runtime": "nodejs20.x",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/r",
                    "Code": {"S3Bucket": {"Ref": "DeployBucket"}, "S3Key": s3_key},
                },
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "DeployBucket"}},
        },
    })
    cfn.update_stack(StackName=stack_name, TemplateBody=template_update)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    fn = lam.get_function(FunctionName=fn_name)
    config = fn["Configuration"]
    assert config["CodeSize"] == len(zip_bytes), (
        f"CodeSize mismatch: expected {len(zip_bytes)}, got {config.get('CodeSize')}"
    )
    assert config["CodeSha256"] != "cfn-deployed", "CodeSha256 still hardcoded"

    code_url = fn["Code"]["Location"]
    local_url = code_url.replace("localhost", "127.0.0.1")
    resp = urllib.request.urlopen(local_url, timeout=5)
    downloaded = resp.read()
    assert len(downloaded) == len(zip_bytes)
    assert downloaded == zip_bytes

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


# -- AWS::ApiGateway::Model --------------------------------------------


def test_cfn_apigateway_model_lifecycle(cfn, apigw_v1):
    """A CDK-style API Gateway model provisions, updates, and deletes through
    CloudFormation; Ref resolves to the model name and Schema is normalized
    from CFN's JSON value to the API Gateway string representation."""
    api_id = apigw_v1.create_rest_api(name="cfn-model-api")["id"]
    stack_name = f"intg-cfn-model-{_uuid_mod.uuid4().hex[:8]}"
    model_name = f"AggregatedMetric{_uuid_mod.uuid4().hex[:8]}"
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "AggregatedMetric",
        "type": "object",
        "properties": {
            "metric": {"type": "string"},
            "value": {"type": "number"},
        },
        "required": ["metric", "value"],
    }
    template = {
        "Resources": {
            "SchemasAggregatedMetric": {
                "Type": "AWS::ApiGateway::Model",
                "Properties": {
                    "RestApiId": api_id,
                    "Name": model_name,
                    "ContentType": "application/json",
                    "Description": "Aggregated metric schema",
                    "Schema": schema,
                },
            },
        },
        "Outputs": {"ModelName": {"Value": {"Ref": "SchemasAggregatedMetric"}}},
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["ModelName"] == model_name

    model = apigw_v1.get_model(restApiId=api_id, modelName=model_name)
    assert model["description"] == "Aggregated metric schema"
    assert json.loads(model["schema"]) == schema

    updated = json.loads(json.dumps(template))
    updated_props = updated["Resources"]["SchemasAggregatedMetric"]["Properties"]
    updated_props["Description"] = "Updated schema"
    updated_props["Schema"]["properties"]["timestamp"] = {"type": "string"}
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    model = apigw_v1.get_model(restApiId=api_id, modelName=model_name)
    assert model["description"] == "Updated schema"
    assert "timestamp" in json.loads(model["schema"])["properties"]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_model(restApiId=api_id, modelName=model_name)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
    apigw_v1.delete_rest_api(restApiId=api_id)


# -- AWS::ApiGateway::Authorizer ---------------------------------------


def test_cfn_apigateway_authorizer_provisions(cfn):
    """AWS::ApiGateway::Authorizer was previously not registered in the
    CFN resource handler map, so stacks that declared a custom authorizer
    failed with `Unsupported resource type`. The handler now provisions
    the authorizer against the existing apigateway_v1 store."""
    stack_name = f"intg-cfn-authz-{_uuid_mod.uuid4().hex[:8]}"
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": "intg-authz-api"},
            },
            "Auth": {
                "Type": "AWS::ApiGateway::Authorizer",
                "Properties": {
                    "Name": "intg-token-authz",
                    "Type": "TOKEN",
                    "RestApiId": {"Ref": "Api"},
                    "AuthorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:noop/invocations",
                    "IdentitySource": "method.request.header.Authorization",
                    "AuthorizerResultTtlInSeconds": 300,
                },
            },
        },
        "Outputs": {
            "AuthorizerId": {"Value": {"Ref": "Auth"}},
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs.get("AuthorizerId"), "AuthorizerId output should be populated"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigateway_base_path_mapping_lifecycle(cfn, apigw_v1):
    """AWS::ApiGateway::BasePathMapping creates, updates, replaces, and deletes."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-base-path-mapping-{suffix}"
    domain_name = f"cfn-base-path-{suffix}.example.com"
    first_api_id = apigw_v1.create_rest_api(name=f"base-path-first-{suffix}")["id"]
    second_api_id = apigw_v1.create_rest_api(name=f"base-path-second-{suffix}")["id"]
    stack_deleted = False

    apigw_v1.create_domain_name(domainName=domain_name)

    def template(base_path, rest_api_id, stage):
        properties = {
            "DomainName": domain_name,
            "RestApiId": rest_api_id,
            "Stage": stage,
        }
        if base_path is not None:
            properties["BasePath"] = base_path
        return {
            "Resources": {
                "Mapping": {
                    "Type": "AWS::ApiGateway::BasePathMapping",
                    "Properties": properties,
                },
            },
            "Outputs": {"MappingRef": {"Value": {"Ref": "Mapping"}}},
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="Mapping",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template(None, first_api_id, "prod")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == f"{domain_name}/(none)"
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
        assert outputs["MappingRef"] == f"{domain_name}/(none)"

        mapping = apigw_v1.get_base_path_mapping(domainName=domain_name, basePath="(none)")
        assert mapping["restApiId"] == first_api_id
        assert mapping["stage"] == "prod"

        # RestApiId and Stage update without replacing the physical resource.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template(None, second_api_id, "beta")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == f"{domain_name}/(none)"
        mapping = apigw_v1.get_base_path_mapping(domainName=domain_name, basePath="(none)")
        assert mapping["restApiId"] == second_api_id
        assert mapping["stage"] == "beta"

        # BasePath requires replacement and removes the previous mapping.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("v2", second_api_id, "beta")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == f"{domain_name}/v2"
        with pytest.raises(ClientError) as exc:
            apigw_v1.get_base_path_mapping(domainName=domain_name, basePath="(none)")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        mapping = apigw_v1.get_base_path_mapping(domainName=domain_name, basePath="v2")
        assert mapping["restApiId"] == second_api_id

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        stack_deleted = True
        with pytest.raises(ClientError) as exc:
            apigw_v1.get_base_path_mapping(domainName=domain_name, basePath="v2")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        if not stack_deleted:
            try:
                cfn.delete_stack(StackName=stack_name)
                _wait_stack(cfn, stack_name)
            except ClientError:
                pass
        apigw_v1.delete_rest_api(restApiId=first_api_id)
        apigw_v1.delete_rest_api(restApiId=second_api_id)
        apigw_v1.delete_domain_name(domainName=domain_name)


def test_cfn_apigateway_account_provisions(cfn, apigw_v1):
    """AWS::ApiGateway::Account is the CDK ``cloudWatchRole: true`` resource.
    Without a registered handler, stacks fail with ``Unsupported resource
    type: AWS::ApiGateway::Account``. We persist the CloudWatchRoleArn into
    the same store the runtime GetAccount API reads from, so the value round-
    trips end-to-end. Regression for issue #657.
    """
    stack_name = f"intg-cfn-apigw-account-{_uuid_mod.uuid4().hex[:8]}"
    role_arn = f"arn:aws:iam::000000000000:role/cfn-apigw-cw-{_uuid_mod.uuid4().hex[:6]}"
    template = {
        "Resources": {
            "Account": {
                "Type": "AWS::ApiGateway::Account",
                "Properties": {"CloudWatchRoleArn": role_arn},
            },
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # GetAccount must reflect the role arn the stack just set.
    settings = apigw_v1.get_account()
    assert settings.get("cloudwatchRoleArn") == role_arn

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigateway_stage_ref_returns_stage_name(cfn, apigw_v1):
    """Stage Ref is directly usable as the stageName in API Gateway calls.

    Regression for #1161: MiniStack previously returned ``<api-id>-<stage>``
    from Ref, causing dependent custom resources to fail GetStage with
    ``Invalid Stage identifier specified``.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-apigw-stage-ref-{suffix}"
    stage_name = "prod"
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": f"stage-ref-{suffix}"},
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "Properties": {"RestApiId": {"Ref": "Api"}},
            },
            "Stage": {
                "Type": "AWS::ApiGateway::Stage",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "DeploymentId": {"Ref": "Deployment"},
                    "StageName": stage_name,
                },
            },
        },
        "Outputs": {
            "ApiId": {"Value": {"Ref": "Api"}},
            "StageName": {"Value": {"Ref": "Stage"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
    assert outputs["StageName"] == stage_name

    stage = apigw_v1.get_stage(
        restApiId=outputs["ApiId"],
        stageName=outputs["StageName"],
    )
    assert stage["stageName"] == stage_name

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigateway_domain_name_lifecycle(cfn, apigw_v1):
    """CloudFormation provisions CDK-style regional and edge custom domains."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-apigw-domain-{suffix}"
    regional_name = f"regional-{suffix}.example.local"
    edge_name = f"edge-{suffix}.example.local"
    regional_certificate_arn = (
        f"arn:aws:acm:us-east-1:000000000000:certificate/regional-{suffix}"
    )
    edge_certificate_arn = (
        f"arn:aws:acm:us-east-1:000000000000:certificate/edge-{suffix}"
    )
    template = {
        "Resources": {
            "RegionalDomain": {
                "Type": "AWS::ApiGateway::DomainName",
                "Properties": {
                    "DomainName": regional_name,
                    "EndpointConfiguration": {"Types": ["REGIONAL"]},
                    "RegionalCertificateArn": regional_certificate_arn,
                    "SecurityPolicy": "TLS_1_2",
                    "Tags": [{"Key": "created-by", "Value": "cloudformation"}],
                },
            },
            "EdgeDomain": {
                "Type": "AWS::ApiGateway::DomainName",
                "Properties": {
                    "DomainName": edge_name,
                    "EndpointConfiguration": {"Types": ["EDGE"]},
                    "CertificateArn": edge_certificate_arn,
                    "SecurityPolicy": "TLS_1_2",
                },
            },
        },
        "Outputs": {
            "RegionalRef": {"Value": {"Ref": "RegionalDomain"}},
            "RegionalDomainName": {
                "Value": {"Fn::GetAtt": ["RegionalDomain", "RegionalDomainName"]},
            },
            "RegionalHostedZoneId": {
                "Value": {"Fn::GetAtt": ["RegionalDomain", "RegionalHostedZoneId"]},
            },
            "RegionalDomainNameArn": {
                "Value": {"Fn::GetAtt": ["RegionalDomain", "DomainNameArn"]},
            },
            "DistributionDomainName": {
                "Value": {"Fn::GetAtt": ["EdgeDomain", "DistributionDomainName"]},
            },
            "DistributionHostedZoneId": {
                "Value": {"Fn::GetAtt": ["EdgeDomain", "DistributionHostedZoneId"]},
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
    assert outputs["RegionalRef"] == regional_name
    assert outputs["RegionalDomainName"] == (
        f"{regional_name}.execute-api.us-east-1.amazonaws.com"
    )
    assert outputs["RegionalHostedZoneId"] == "Z1UJRXOUMOOFQ8"
    assert outputs["RegionalDomainNameArn"] == (
        f"arn:aws:apigateway:us-east-1::/domainnames/{regional_name}"
    )
    assert outputs["DistributionDomainName"] == f"{edge_name}.cloudfront.net"
    assert outputs["DistributionHostedZoneId"] == "Z2FDTNDATAQYW2"

    regional = apigw_v1.get_domain_name(domainName=regional_name)
    assert regional["endpointConfiguration"] == {"types": ["REGIONAL"]}
    assert regional["regionalCertificateArn"] == regional_certificate_arn
    assert regional["tags"] == {"created-by": "cloudformation"}
    edge = apigw_v1.get_domain_name(domainName=edge_name)
    assert edge["endpointConfiguration"] == {"types": ["EDGE"]}
    assert edge["certificateArn"] == edge_certificate_arn

    cfn.delete_stack(StackName=stack_name)
    deleted = _wait_stack(cfn, stack_name)
    assert deleted["StackStatus"] == "DELETE_COMPLETE"
    for domain_name in (regional_name, edge_name):
        with pytest.raises(ClientError) as exc:
            apigw_v1.get_domain_name(domainName=domain_name)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_cfn_apigateway_gateway_response_resolves_rest_api_ref(cfn, apigw_v1):
    """The issue #1124 CDK shape provisions a response against a stack REST API."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-gateway-response-ref-{suffix}"
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": f"gateway-response-ref-{suffix}"},
            },
            "BadRequestBody": {
                "Type": "AWS::ApiGateway::GatewayResponse",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "ResponseType": "BAD_REQUEST_BODY",
                    "StatusCode": "400",
                    "ResponseParameters": {
                        "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
                    },
                },
            },
        },
        "Outputs": {"ApiId": {"Value": {"Ref": "Api"}}},
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    api_id = next(
        item["OutputValue"]
        for item in stack.get("Outputs", [])
        if item["OutputKey"] == "ApiId"
    )
    response = apigw_v1.get_gateway_response(
        restApiId=api_id,
        responseType="BAD_REQUEST_BODY",
    )
    assert response["defaultResponse"] is False
    assert response["responseParameters"] == {
        "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
    }

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_apigateway_gateway_response_lifecycle(cfn, apigw_v1):
    """GatewayResponse creates, updates, replaces, and resets through CFN.

    Regression for #1124: the resource type previously failed immediately as
    unsupported, rolling back every CDK stack that declared a gateway response.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-gateway-response-{suffix}"
    api_id = apigw_v1.create_rest_api(name=f"gateway-response-{suffix}")["id"]
    stack_deleted = False

    def template(response_type, status_code, marker):
        return {
            "Resources": {
                "GatewayResponse": {
                    "Type": "AWS::ApiGateway::GatewayResponse",
                    "Properties": {
                        "RestApiId": api_id,
                        "ResponseType": response_type,
                        "StatusCode": status_code,
                        "ResponseParameters": {
                            "gatewayresponse.header.X-Marker": f"'{marker}'",
                        },
                        "ResponseTemplates": {
                            "application/json": f'{{"marker":"{marker}"}}',
                        },
                    },
                },
            },
            "Outputs": {
                "GatewayResponseId": {
                    "Value": {"Fn::GetAtt": ["GatewayResponse", "Id"]},
                },
            },
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="GatewayResponse",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("BAD_REQUEST_BODY", "400", "created")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        created_id = physical_id()
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
        assert outputs["GatewayResponseId"] == created_id

        created = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType="BAD_REQUEST_BODY",
        )
        assert created["defaultResponse"] is False
        assert created["responseTemplates"] == {"application/json": '{"marker":"created"}'}

        # Mutable properties update in place and keep the physical id.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("BAD_REQUEST_BODY", "422", "updated")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == created_id
        updated = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType="BAD_REQUEST_BODY",
        )
        assert updated["statusCode"] == "422"
        assert updated["responseParameters"] == {
            "gatewayresponse.header.X-Marker": "'updated'",
        }

        # ResponseType is immutable: replace the physical resource and reset
        # the previous response type to its generated default.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("BAD_REQUEST_PARAMETERS", "409", "replacement")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        replacement_id = physical_id()
        assert replacement_id != created_id
        assert apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType="BAD_REQUEST_BODY",
        )["defaultResponse"] is True
        assert apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType="BAD_REQUEST_PARAMETERS",
        )["statusCode"] == "409"

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        stack_deleted = True
        reset_response = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType="BAD_REQUEST_PARAMETERS",
        )
        assert reset_response["defaultResponse"] is True
        assert reset_response["statusCode"] == "400"
    finally:
        if not stack_deleted:
            try:
                cfn.delete_stack(StackName=stack_name)
                _wait_stack(cfn, stack_name)
            except ClientError:
                pass
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_cfn_apigateway_documentation_part_lifecycle(cfn, apigw_v1):
    """DocumentationPart supports create, update, replacement, Ref, and delete.

    Regression for #1159: the resource type previously failed stack creation
    with ``Unsupported resource type: AWS::ApiGateway::DocumentationPart``.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-documentation-part-{suffix}"
    api_id = apigw_v1.create_rest_api(name=f"documentation-part-{suffix}")["id"]
    stack_deleted = False

    def template(location, description):
        return {
            "Resources": {
                "DocumentationPart": {
                    "Type": "AWS::ApiGateway::DocumentationPart",
                    "Properties": {
                        "RestApiId": api_id,
                        "Location": location,
                        "Properties": json.dumps({"description": description}),
                    },
                },
            },
            "Outputs": {
                "DocumentationPartId": {"Value": {"Ref": "DocumentationPart"}},
            },
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="DocumentationPart",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template({"Type": "API"}, "Created")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        created_id = physical_id()
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
        assert outputs["DocumentationPartId"] == created_id
        created = apigw_v1.get_documentation_part(
            restApiId=api_id,
            documentationPartId=created_id,
        )
        assert created["location"] == {"type": "API"}
        assert json.loads(created["properties"])["description"] == "Created"

        # Properties updates in place.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template({"Type": "API"}, "Updated")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == created_id
        updated = apigw_v1.get_documentation_part(
            restApiId=api_id,
            documentationPartId=created_id,
        )
        assert json.loads(updated["properties"])["description"] == "Updated"

        # Location is immutable and replaces the documentation part.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(
                template({"Type": "RESOURCE", "Path": "/pets"}, "Replacement")
            ),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        replacement_id = physical_id()
        assert replacement_id != created_id
        with pytest.raises(ClientError):
            apigw_v1.get_documentation_part(
                restApiId=api_id,
                documentationPartId=created_id,
            )
        replacement = apigw_v1.get_documentation_part(
            restApiId=api_id,
            documentationPartId=replacement_id,
        )
        assert replacement["location"] == {"type": "RESOURCE", "path": "/pets"}

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        stack_deleted = True
        with pytest.raises(ClientError):
            apigw_v1.get_documentation_part(
                restApiId=api_id,
                documentationPartId=replacement_id,
            )
    finally:
        if not stack_deleted:
            try:
                cfn.delete_stack(StackName=stack_name)
                _wait_stack(cfn, stack_name)
            except ClientError:
                pass
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_cfn_apigateway_request_validator_identity(cfn, apigw_v1):
    """RequestValidator exposes its ID while local requests remain permissive."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-request-validator-{suffix}"
    api_id = apigw_v1.create_rest_api(name=f"request-validator-{suffix}")["id"]

    def template(name, validate_body):
        return {
            "Resources": {
                "RequestValidator": {
                    "Type": "AWS::ApiGateway::RequestValidator",
                    "Properties": {
                        "RestApiId": api_id,
                        "Name": name,
                        "ValidateRequestBody": validate_body,
                        "ValidateRequestParameters": True,
                    },
                },
            },
            "Outputs": {
                "RefId": {"Value": {"Ref": "RequestValidator"}},
                "GetAttId": {
                    "Value": {
                        "Fn::GetAtt": ["RequestValidator", "RequestValidatorId"]
                    }
                },
            },
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="RequestValidator",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("body-and-parameters", True)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        created_id = physical_id()
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        assert outputs == {"RefId": created_id, "GetAttId": created_id}

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("body-and-parameters", False)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == created_id

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("replacement", False)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() != created_id
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_cfn_apigateway_documentation_version_lifecycle(cfn, apigw_v1):
    """DocumentationVersion has a stable CFN identity and supports replacement."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-documentation-version-{suffix}"
    api_id = apigw_v1.create_rest_api(name=f"documentation-version-{suffix}")["id"]

    def template(version, description):
        return {
            "Resources": {
                "DocumentationVersion": {
                    "Type": "AWS::ApiGateway::DocumentationVersion",
                    "Properties": {
                        "RestApiId": api_id,
                        "DocumentationVersion": version,
                        "Description": description,
                    },
                },
            },
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="DocumentationVersion",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("v1", "Created")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        created_id = physical_id()
        assert created_id == f"{api_id}/v1"

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("v1", "Updated")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == created_id

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("v2", "Replacement")),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == f"{api_id}/v2"
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass
        apigw_v1.delete_rest_api(restApiId=api_id)


# ---------------------------------------------------------------------------
# ApiGatewayV1 Integration with OpenAPI spec parsing
# ---------------------------------------------------------------------------

def test_cfn_restapi_openapi_body_petstore(cfn, apigw_v1):
    stack = "cfn-restapi-body"
    op = {
        "x-amazon-apigateway-integration": {
            "httpMethod": "POST",
            "type": "aws_proxy",
            "uri": {
                "Fn::Sub": "arn:aws:apigateway:${AWS::Region}:lambda:path/"
                           "2015-03-31/functions/${PetStoreFunction.Arn}/invocations"
            },
        },
        "responses": {},
    }
    body = {
        "swagger": "2.0",
        "info": {"version": "1.0", "title": {"Ref": "AWS::StackName"}},
        "paths": {
            "/pets": {"get": dict(op), "post": dict(op)},
            "/pets/featured": {"get": dict(op)},
            "/pets/{petId}": {"get": dict(op), "delete": dict(op)},
        },
    }
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "PetStoreFunction": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": f"{stack}-fn",
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/r",
                    "Code": {"ZipFile": "def handler(e, c):\n    return {}\n"},
                },
            },
            "ServerlessRestApi": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Body": body},
            },
        },
        "Outputs": {"ApiId": {"Value": {"Ref": "ServerlessRestApi"}}},
    })

    cfn.create_stack(StackName=stack, TemplateBody=template)
    s = _wait_stack(cfn, stack)
    assert s["StackStatus"] == "CREATE_COMPLETE"
    api_id = {o["OutputKey"]: o["OutputValue"] for o in s["Outputs"]}["ApiId"]

    api = apigw_v1.get_rest_api(restApiId=api_id)
    assert api["name"] == stack
    assert api["version"] == "1.0"

    rmap = {}
    for r in apigw_v1.get_resources(restApiId=api_id, limit=500)["items"]:
        rmap[r["path"]] = {
            m: apigw_v1.get_integration(restApiId=api_id, resourceId=r["id"],
                                        httpMethod=m)
            for m in (r.get("resourceMethods") or {})
        }

    assert set(rmap) == {"/", "/pets", "/pets/featured", "/pets/{petId}"}
    assert set(rmap["/pets"]) == {"GET", "POST"}
    assert set(rmap["/pets/featured"]) == {"GET"}
    assert set(rmap["/pets/{petId}"]) == {"GET", "DELETE"}

    integ = rmap["/pets"]["GET"]
    assert integ["type"] == "AWS_PROXY"
    assert integ["httpMethod"] == "POST"
    assert integ["uri"].startswith("arn:aws:apigateway:")
    assert "${" not in integ["uri"]
    assert f":function:{stack}-fn/invocations" in integ["uri"]

    cfn.delete_stack(StackName=stack)
    _wait_stack(cfn, stack)
    ids = [a["id"] for a in apigw_v1.get_rest_apis(limit=500)["items"]]
    assert api_id not in ids


# ============================================================================
# Nested Stacks (AWS::CloudFormation::Stack)
# ============================================================================

def test_cfn_nested_stack_basic(cfn, s3):
    """Parent stack provisions a nested stack via TemplateURL. The nested
    stack creates an S3 bucket and exposes its name as an Output, which the
    parent reads back via Fn::GetAtt: [Nested, Outputs.BucketName]."""
    suffix = _uuid_mod.uuid4().hex[:8]
    templates_bucket = f"cfn-nested-templates-{suffix}"
    s3.create_bucket(Bucket=templates_bucket)

    child_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "BucketSuffix": {"Type": "String"},
        },
        "Resources": {
            "ChildBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": {"Fn::Sub": "cfn-nested-child-${BucketSuffix}"},
                },
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "ChildBucket"}},
        },
    }
    s3.put_object(Bucket=templates_bucket, Key="child.json",
                  Body=json.dumps(child_template).encode())

    parent_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {
                    "TemplateURL": f"http://localhost:4566/{templates_bucket}/child.json",
                    "Parameters": {"BucketSuffix": suffix},
                },
            },
            "ParentParam": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": f"/cfn-nested-parent-{suffix}/child-bucket",
                    "Type": "String",
                    "Value": {"Fn::GetAtt": ["Nested", "Outputs.BucketName"]},
                },
            },
        },
        "Outputs": {
            "NestedBucketName": {
                "Value": {"Fn::GetAtt": ["Nested", "Outputs.BucketName"]},
            },
        },
    }

    parent_name = f"cfn-nested-parent-{suffix}"
    cfn.create_stack(StackName=parent_name,
                     TemplateBody=json.dumps(parent_template))
    stack = _wait_stack(cfn, parent_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    expected_bucket = f"cfn-nested-child-{suffix}"
    assert outputs.get("NestedBucketName") == expected_bucket

    # The nested-created bucket really exists
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert expected_bucket in buckets

    # Delete the parent — child resources are cleaned up too
    cfn.delete_stack(StackName=parent_name)
    _wait_stack(cfn, parent_name)
    buckets_after = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert expected_bucket not in buckets_after, \
        "Nested stack delete did not propagate to child resources"

    s3.delete_object(Bucket=templates_bucket, Key="child.json")
    s3.delete_bucket(Bucket=templates_bucket)


def test_cfn_logs_subscription_filter_provisions(cfn, logs):
    """AWS::Logs::SubscriptionFilter provisions via CFN and is removed on stack
    delete (#896). The filter Refs the in-stack log group so it is created
    after the group."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyGroup": {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {"LogGroupName": "/cfn/subfilter-test"},
            },
            "MyFilter": {
                "Type": "AWS::Logs::SubscriptionFilter",
                "Properties": {
                    "LogGroupName": {"Ref": "MyGroup"},
                    "FilterPattern": "[Producer]",
                    "DestinationArn":
                        "arn:aws:lambda:us-east-1:000000000000:function:consumer",
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-subfilter", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-subfilter")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    filters = logs.describe_subscription_filters(
        logGroupName="/cfn/subfilter-test")["subscriptionFilters"]
    assert len(filters) == 1
    assert filters[0]["filterPattern"] == "[Producer]"
    assert filters[0]["destinationArn"].endswith(":function:consumer")

    cfn.delete_stack(StackName="cfn-subfilter")
    _wait_stack(cfn, "cfn-subfilter")
    # The stack delete removes the LogGroup too, so the subscription filter is
    # gone with it — describing it now raises ResourceNotFoundException.
    with pytest.raises(ClientError):
        logs.describe_subscription_filters(logGroupName="/cfn/subfilter-test")


def test_cfn_logs_resource_policy_identity_and_lifecycle(cfn):
    """Logs resource policies expose their policy name without enforcing it."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-logs-policy-{suffix}"
    policy_name = f"logs-policy-{suffix}"

    def template(statement_sid, name=policy_name):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "LogsPolicy": {
                    "Type": "AWS::Logs::ResourcePolicy",
                    "Properties": {
                        "PolicyName": name,
                        "PolicyDocument": json.dumps({
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Sid": statement_sid,
                                "Effect": "Allow",
                                "Principal": {"Service": "route53.amazonaws.com"},
                                "Action": "logs:PutLogEvents",
                                "Resource": "*",
                            }],
                        }),
                    },
                },
            },
            "Outputs": {
                "PolicyName": {"Value": {"Ref": "LogsPolicy"}},
            },
        }

    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("InitialPolicy")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    assert stack["Outputs"][0]["OutputValue"] == policy_name

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("UpdatedPolicy")),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert stack["Outputs"][0]["OutputValue"] == policy_name

    updated_name = f"{policy_name}-updated"
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("UpdatedPolicy", updated_name)),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert stack["Outputs"][0]["OutputValue"] == updated_name

    cfn.delete_stack(StackName=stack_name)
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_change_set_detects_parameter_driven_change(cfn, s3):
    """A change set must detect a parameter-driven property change (e.g. a Lambda
    Code S3Key behind a Ref) so `aws cloudformation deploy` doesn't silently
    no-op while `update-stack` works (#897). Also guards against false positives
    when nothing changed."""
    s3.create_bucket(Bucket="cfn897-code")
    for k in ("a.zip", "b.zip"):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("index.py", "def handler(e, c):\n    return 'ok'\n")
        s3.put_object(Bucket="cfn897-code", Key=k, Body=buf.getvalue())

    tmpl = json.dumps({
        "Parameters": {"CodeKey": {"Type": "String"}},
        "Resources": {"Fn": {"Type": "AWS::Lambda::Function", "Properties": {
            "FunctionName": "cfn897-fn", "Runtime": "python3.12",
            "Handler": "index.handler", "Role": "arn:aws:iam::000000000000:role/r",
            "Code": {"S3Bucket": "cfn897-code", "S3Key": {"Ref": "CodeKey"}}}}}})
    cfn.create_stack(StackName="cfn897", TemplateBody=tmpl,
                     Parameters=[{"ParameterKey": "CodeKey", "ParameterValue": "a.zip"}])
    _wait_stack(cfn, "cfn897")

    def _change_set(name, val):
        cfn.create_change_set(
            StackName="cfn897", ChangeSetName=name, ChangeSetType="UPDATE",
            TemplateBody=tmpl,
            Parameters=[{"ParameterKey": "CodeKey", "ParameterValue": val}])
        deadline = time.time() + 30
        while time.time() < deadline:
            d = cfn.describe_change_set(StackName="cfn897", ChangeSetName=name)
            if d["Status"] in ("CREATE_COMPLETE", "FAILED"):
                return d
            time.sleep(0.5)
        return d

    changed = _change_set("cs-changed", "b.zip")
    assert len(changed.get("Changes", [])) == 1
    assert changed["Changes"][0]["ResourceChange"]["Action"] == "Modify"

    # nothing changed -> empty change set (no false positive)
    noop = _change_set("cs-noop", "a.zip")
    assert len(noop.get("Changes", [])) == 0


def test_cfn_lambda_layer_packages_importable(cfn, s3, lam):
    """A Lambda layer deployed via CloudFormation (CDK pattern: Content from S3)
    must make its packages importable at invoke time.

    Regression: the CFN LayerVersion provisioner fetched the layer zip but never
    stored it as ``_zip_data``, so ``_resolve_layer_zip`` returned None and the
    layer was silently skipped at worker spawn — ``No module named ...`` even
    though ``list-layers`` showed the layer. Reported by @ocr-lasagna."""
    stack_name = "cfn-layer-import"
    bucket_name = "cfn-layer-assets"
    fn_name = "cfn-layer-fn"

    s3.create_bucket(Bucket=bucket_name)

    # Layer zip with a Python module under python/ (the AWS layer convention).
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr("python/cfn_layer_helper.py", "LAYER_VALUE = 'from-cfn-layer'\n")
    s3.put_object(Bucket=bucket_name, Key="layer.zip", Body=layer_buf.getvalue())

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyLayer": {
                "Type": "AWS::Lambda::LayerVersion",
                "Properties": {
                    "LayerName": "cfn-import-layer",
                    "CompatibleRuntimes": ["python3.12"],
                    "Content": {"S3Bucket": bucket_name, "S3Key": "layer.zip"},
                },
            },
            "MyFunction": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": fn_name,
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/cfn-role",
                    "Layers": [{"Ref": "MyLayer"}],
                    "Code": {
                        "ZipFile": (
                            "import cfn_layer_helper\n"
                            "def handler(event, context):\n"
                            "    return {'value': cfn_layer_helper.LAYER_VALUE}\n"
                        ),
                    },
                },
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    try:
        resp = lam.invoke(FunctionName=fn_name, Payload=b"{}")
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, (
            f"Lambda error: {resp['Payload'].read().decode()}"
        )
        payload = json.loads(resp["Payload"].read())
        assert payload["value"] == "from-cfn-layer"
    finally:
        cfn.delete_stack(StackName=stack_name)


def test_cfn_sam_transform_function_and_simple_table(cfn, lam, s3, ddb):
    pytest.importorskip("samtranslator")
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"cfn-sam-code-{suffix}"
    key = "handler.zip"
    s3.create_bucket(Bucket=bucket)

    code = b"def handler(event, context):\n    return {'ok': True}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    stack_name = f"cfn-sam-basic-{suffix}"
    fn_name = f"sam-fn-{suffix}"
    table_name = f"sam-table-{suffix}"

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "MyFunction": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "FunctionName": fn_name,
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "CodeUri": {"Bucket": bucket, "Key": key},
                    "MemorySize": 256,
                    "Timeout": 10,
                    "Environment": {"Variables": {"TABLE": table_name}},
                },
            },
            "MyTable": {
                "Type": "AWS::Serverless::SimpleTable",
                "Properties": {
                    "TableName": table_name,
                    "PrimaryKey": {"Name": "pk", "Type": "String"},
                },
            },
        },
        "Outputs": {
            "FunctionName": {"Value": {"Ref": "MyFunction"}},
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["FunctionName"] == fn_name

    resp = lam.invoke(FunctionName=fn_name, Payload=b"{}")
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload.get("ok") is True

    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    rtypes = {r["ResourceType"] for r in resources}
    assert "AWS::IAM::Role" in rtypes, f"Expected auto-generated IAM role, got {rtypes}"
    assert "AWS::Lambda::Function" in rtypes
    assert "AWS::DynamoDB::Table" in rtypes

    table_desc = ddb.describe_table(TableName=table_name)["Table"]
    ks = {k["AttributeName"]: k["KeyType"] for k in table_desc["KeySchema"]}
    assert ks.get("pk") == "HASH"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    s3.delete_object(Bucket=bucket, Key=key)
    s3.delete_bucket(Bucket=bucket)


def test_cfn_sam_transform_serverless_api(cfn, s3, lam):
    pytest.importorskip("samtranslator")
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"cfn-sam-api-code-{suffix}"
    key = "handler.zip"
    s3.create_bucket(Bucket=bucket)

    code = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    stack_name = f"cfn-sam-api-{suffix}"
    fn_name = f"sam-api-fn-{suffix}"

    openapi_body = {
        "openapi": "3.0.1",
        "info": {"title": "test", "version": "1.0"},
        "paths": {
            "/hello": {
                "get": {
                    "x-amazon-apigateway-integration": {
                        "type": "aws_proxy",
                        "httpMethod": "POST",
                        "uri": {"Fn::Sub": "arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${MyFunction.Arn}/invocations"},
                        "passthroughBehavior": "WHEN_NO_MATCH",
                    }
                }
            }
        },
    }
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "MyApi": {
                "Type": "AWS::Serverless::Api",
                "Properties": {
                    "Name": f"sam-api-{suffix}",
                    "StageName": "v1",
                    "DefinitionBody": openapi_body,
                },
            },
            "MyFunction": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "FunctionName": fn_name,
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "CodeUri": {"Bucket": bucket, "Key": key},
                },
            },
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    rtypes = {r["ResourceType"] for r in resources}
    assert "AWS::ApiGateway::RestApi" in rtypes, f"Missing RestApi in {rtypes}"
    assert "AWS::ApiGateway::Deployment" in rtypes, f"Missing Deployment in {rtypes}"
    assert "AWS::ApiGateway::Stage" in rtypes, f"Missing Stage in {rtypes}"
    assert "AWS::Lambda::Function" in rtypes
    assert "AWS::IAM::Role" in rtypes

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    s3.delete_object(Bucket=bucket, Key=key)
    s3.delete_bucket(Bucket=bucket)


def test_cfn_sam_transform_missing_translator_falls_back(monkeypatch):
    import sys

    from ministack.services.cloudformation.engine import (
        _apply_sam_transform_if_applicable,
    )

    # Simulate the package being absent: a None entry makes `from ... import`
    # raise ImportError even if samtranslator is installed in the test env.
    monkeypatch.setitem(sys.modules, "samtranslator.translator.transform", None)

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "MyFunction": {
                "Type": "AWS::Serverless::Function",
                "Properties": {"Handler": "index.handler", "Runtime": "python3.12"},
            },
        },
    }
    with pytest.raises(ValueError) as exc:
        _apply_sam_transform_if_applicable(template)
    msg = str(exc.value)
    assert "AWS::Serverless-2016-10-31" in msg
    assert "docs/iac#sam" in msg

    # Templates that don't use the SAM transform are unaffected.
    plain = {"Resources": {"B": {"Type": "AWS::S3::Bucket", "Properties": {}}}}
    assert _apply_sam_transform_if_applicable(plain) is plain


# AWS::OpenSearchService::Domain


def _opensearch_stack_template(domain_props):
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "SearchDomain": {
                "Type": "AWS::OpenSearchService::Domain",
                "Properties": domain_props,
            },
        },
        "Outputs": {
            "Ref": {"Value": {"Ref": "SearchDomain"}},
            "Arn": {"Value": {"Fn::GetAtt": ["SearchDomain", "Arn"]}},
            "DomainArn": {
                "Value": {"Fn::GetAtt": ["SearchDomain", "DomainArn"]}
            },
            "Endpoint": {
                "Value": {"Fn::GetAtt": ["SearchDomain", "DomainEndpoint"]}
            },
            "Id": {"Value": {"Fn::GetAtt": ["SearchDomain", "Id"]},},
        },
    }


def _stack_resource(cfn, stack_name, logical_id="SearchDomain"):
    return cfn.describe_stack_resource(
        StackName=stack_name, LogicalResourceId=logical_id
    )["StackResourceDetail"]


def test_cfn_opensearch_domain_create_update_replace_and_idempotent_delete(
        cfn, opensearch):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-{suffix}"
    domain_name = f"search-{suffix}"
    sentinel = f"NeverLog-{suffix}"
    all_properties = {
        "AccessPolicies": {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "es:*", "Resource": "*"}],
        },
        "AdvancedOptions": {"indices.fielddata.cache.size": "20"},
        "AdvancedSecurityOptions": {
            "Enabled": True,
            "InternalUserDatabaseEnabled": True,
            "MasterUserOptions": {
                "MasterUserName": "admin",
                "MasterUserPassword": sentinel,
            },
        },
        "AIMLOptions": {"NaturalLanguageQueryGenerationOptions": {"DesiredState": "ENABLED"}},
        "AutomatedSnapshotPauseOptions": {"Enabled": True},
        "ClusterConfig": {"InstanceCount": 1, "InstanceType": "t3.small.search"},
        "CognitoOptions": {"Enabled": False},
        "DeploymentStrategyOptions": {"DeploymentStrategy": "BLUE_GREEN"},
        "DomainEndpointOptions": {"EnforceHTTPS": False},
        "DomainName": domain_name,
        "EBSOptions": {"EBSEnabled": True, "VolumeSize": 20, "VolumeType": "gp3"},
        "EncryptionAtRestOptions": {"Enabled": True},
        "EngineVersion": "OpenSearch_2.15",
        "IdentityCenterOptions": {"EnabledAPIAccess": False},
        "IPAddressType": "ipv4",
        "LogPublishingOptions": {},
        "NodeToNodeEncryptionOptions": {"Enabled": True},
        "OffPeakWindowOptions": {"Enabled": True},
        "SkipShardMigrationWait": True,
        "SnapshotOptions": {"AutomatedSnapshotStartHour": 7},
        "SoftwareUpdateOptions": {"AutoSoftwareUpdateEnabled": True},
        "Tags": [
            {"Key": "Environment", "Value": "test"},
            {"Key": "Changing", "Value": "old"},
        ],
        "VPCOptions": {},
    }

    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template(all_properties)),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    resource = _stack_resource(cfn, stack_name)
    assert resource["PhysicalResourceId"] == domain_name
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    expected_arn = f"arn:aws:es:us-east-1:000000000000:domain/{domain_name}"
    assert outputs == {
        "Ref": domain_name,
        "Arn": expected_arn,
        "DomainArn": expected_arn,
        "Endpoint": f"{domain_name}.ministack.local:9200",
        "Id": f"000000000000/{domain_name}",
    }

    status = opensearch.describe_domain(DomainName=domain_name)["DomainStatus"]
    assert status["EngineVersion"] == "OpenSearch_2.15"
    assert status["ClusterConfig"]["InstanceType"] == "t3.small.search"
    assert status["SnapshotOptions"]["AutomatedSnapshotStartHour"] == 7
    assert status["OffPeakWindowOptions"]["Enabled"] is True
    assert status["SoftwareUpdateOptions"]["AutoSoftwareUpdateEnabled"] is True
    assert sentinel not in json.dumps(status, default=str)
    tags = opensearch.list_tags(ARN=expected_arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in tags} == {
        "Environment": "test", "Changing": "old"
    }
    assert domain_name in {
        item["DomainName"] for item in opensearch.list_domain_names()["DomainNames"]
    }

    updated_properties = {
        "DomainName": domain_name,
        "EngineVersion": "OpenSearch_2.17",
        "ClusterConfig": {"InstanceCount": 2},
        "AIMLOptions": {"NaturalLanguageQueryGenerationOptions": {"DesiredState": "DISABLED"}},
        "Tags": [
            {"Key": "Changing", "Value": "new"},
            {"Key": "Added", "Value": "yes"},
        ],
    }
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template(updated_properties)),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == domain_name
    status = opensearch.describe_domain(DomainName=domain_name)["DomainStatus"]
    assert status["EngineVersion"] == "OpenSearch_2.17"
    assert status["ClusterConfig"]["InstanceCount"] == 2
    assert status["EBSOptions"]["VolumeSize"] == 10
    assert status["AdvancedOptions"] == {}
    assert status["OffPeakWindowOptions"] == {"Enabled": False}
    tags = opensearch.list_tags(ARN=expected_arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in tags} == {
        "Changing": "new", "Added": "yes"
    }
    progress = opensearch.describe_domain_change_progress(DomainName=domain_name)[
        "ChangeProgressStatus"
    ]
    assert progress["Status"] == "COMPLETED"
    assert progress["ConfigChangeStatus"] == "Completed"

    replacement_name = f"replace-{suffix}"
    replacement_props = {
        "DomainName": replacement_name,
        "Tags": [{"Key": "Replacement", "Value": "true"}],
    }
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template(replacement_props)),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == replacement_name
    with pytest.raises(ClientError) as exc:
        opensearch.describe_domain(DomainName=domain_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    opensearch.describe_domain(DomainName=replacement_name)

    # Removing an explicit DomainName is also a replacement, with a newly
    # generated physical name derived from the stack and logical ID.
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template({"Tags": []})),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    generated_name = _stack_resource(cfn, stack_name)["PhysicalResourceId"]
    assert generated_name != replacement_name
    assert len(generated_name) <= 28
    with pytest.raises(ClientError):
        opensearch.describe_domain(DomainName=replacement_name)

    # Manual removal must not make CloudFormation deletion fail.
    opensearch.delete_domain(DomainName=generated_name)
    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_opensearch_auto_name_is_stable_across_update(cfn, opensearch):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-auto-{suffix}"
    template = _opensearch_stack_template({"EngineVersion": "OpenSearch_2.15"})
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    physical_id = _stack_resource(cfn, stack_name)["PhysicalResourceId"]
    assert len(physical_id) <= 28
    assert len(physical_id) >= 3
    assert physical_id[0].islower()
    assert all(c.islower() or c.isdigit() or c == "-" for c in physical_id)

    template = _opensearch_stack_template({
        "EngineVersion": "OpenSearch_2.17",
        "VPCOptions": {
            "SubnetIds": ["subnet-control-plane"],
            "SecurityGroupIds": ["sg-control-plane"],
        },
    })
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == physical_id
    status = opensearch.describe_domain(DomainName=physical_id)["DomainStatus"]
    assert status["EngineVersion"] == "OpenSearch_2.17"
    assert status["Endpoints"]["vpc"] == f"{physical_id}.ministack.local:9200"

    # Removing VPCOptions is an in-place control-plane update and restores the
    # public endpoint response shape.
    template = _opensearch_stack_template({"EngineVersion": "OpenSearch_2.17"})
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == physical_id
    status = opensearch.describe_domain(DomainName=physical_id)["DomainStatus"]
    assert status["Endpoint"] == f"{physical_id}.ministack.local:9200"
    assert "Endpoints" not in status

    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_opensearch_vpc_refs_use_vpc_endpoint(cfn, opensearch):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-vpc-{suffix}"
    domain_name = f"vpc-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
            "Subnet": {
                "Type": "AWS::EC2::Subnet",
                "Properties": {"VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.0.1.0/24"},
            },
            "SecurityGroup": {
                "Type": "AWS::EC2::SecurityGroup",
                "Properties": {"VpcId": {"Ref": "Vpc"}, "GroupDescription": "search"},
            },
            "SearchDomain": {
                "Type": "AWS::OpenSearchService::Domain",
                "Properties": {
                    "DomainName": domain_name,
                    "VPCOptions": {
                        "SubnetIds": [{"Ref": "Subnet"}],
                        "SecurityGroupIds": [{"Ref": "SecurityGroup"}],
                    },
                },
            },
        },
        "Outputs": {
            "Endpoint": {"Value": {"Fn::GetAtt": ["SearchDomain", "DomainEndpoint"]}}
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    status = opensearch.describe_domain(DomainName=domain_name)["DomainStatus"]
    endpoint = status["Endpoints"]["vpc"]
    assert {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]} == {
        "Endpoint": endpoint
    }
    resources = {
        r["LogicalResourceId"]: r
        for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    }
    assert status["VPCOptions"]["SubnetIds"] == [resources["Subnet"]["PhysicalResourceId"]]
    assert status["VPCOptions"]["SecurityGroupIds"] == [
        resources["SecurityGroup"]["PhysicalResourceId"]
    ]
    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_opensearch_failed_replacement_preserves_original(cfn, opensearch):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-failure-{suffix}"
    original = f"original-{suffix}"
    duplicate = f"duplicate-{suffix}"
    opensearch.create_domain(DomainName=duplicate)
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template({"DomainName": original})),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(_opensearch_stack_template({"DomainName": duplicate})),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
    assert opensearch.describe_domain(DomainName=original)["DomainStatus"]["DomainName"] == original
    assert opensearch.describe_domain(DomainName=duplicate)["DomainStatus"]["DomainName"] == duplicate
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == original

    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    opensearch.delete_domain(DomainName=duplicate)


def test_cfn_opensearch_invalid_create_redacts_secret_and_rolls_back(cfn, opensearch):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-invalid-{suffix}"
    sentinel = f"SentinelPassword-{suffix}"
    invalid_name = f"INVALID-{suffix}"
    template = _opensearch_stack_template({
        "DomainName": invalid_name,
        "AdvancedSecurityOptions": {
            "Enabled": True,
            "MasterUserOptions": {
                "MasterUserName": "admin",
                "MasterUserPassword": sentinel,
            },
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"
    events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
    assert sentinel not in json.dumps(events, default=str)
    assert sentinel not in json.dumps(stack, default=str)
    with pytest.raises(ClientError) as exc:
        opensearch.describe_domain(DomainName=invalid_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_opensearch_private_compatibility_state_is_detached_and_replaced():
    from ministack.services import opensearch as service
    from ministack.services.cloudformation.provisioners import (
        _opensearch_domain_create,
        _opensearch_domain_delete,
        _opensearch_domain_update,
    )

    suffix = _uuid_mod.uuid4().hex[:8]
    name = f"private-{suffix}"
    compatibility = {"AIMLOptions": {"Nested": ["original"]}}
    props = {
        "DomainName": name,
        **compatibility,
        "Tags": [{"Key": "Original", "Value": "yes"}],
    }
    physical_id, _ = _opensearch_domain_create("SearchDomain", props, "unit-stack")
    try:
        props["AIMLOptions"]["Nested"].append("mutated")
        props["Tags"][0]["Value"] = "mutated"
        rec = service._domains[physical_id]
        assert rec["_CloudFormationCompatibility"] == {
            "AIMLOptions": {"Nested": ["original"]}
        }
        assert service._tags[rec["ARN"]] == [{"Key": "Original", "Value": "yes"}]

        same_id, _ = _opensearch_domain_update(
            physical_id,
            {"DomainName": name, "AIMLOptions": compatibility["AIMLOptions"]},
            {"DomainName": name},
            "unit-stack",
            "SearchDomain",
        )
        assert same_id == physical_id
        assert service._domains[physical_id]["_CloudFormationCompatibility"] == {}
        assert service._tags.get(rec["ARN"]) is None
    finally:
        _opensearch_domain_delete(physical_id, {})
        _opensearch_domain_delete(physical_id, {})
        assert service._domains.get(physical_id) is None
        assert service._change_progress.get(physical_id) is None


def test_cfn_cdk_opensearch_access_policy_custom_resource(cfn, opensearch):
    """CDK's provider Lambda can load the OpenSearch SDK v3 package.

    The OpenSearch Domain L2 emits Custom::OpenSearchAccessPolicy with
    InstallLatestAwsSdk=false. Its shared provider dynamically loads
    @aws-sdk/client-opensearch and sends UpdateDomainConfigCommand.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-os-access-{suffix}"
    domain_name = f"access-{suffix}"
    function_name = f"cfn-os-provider-{suffix}"
    access_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "es:*",
            "Resource": "*",
        }],
    }, separators=(",", ":"))
    provider_code = r"""
const http = require("http");

function respond(event, status, reason, physicalId, data) {
  const body = JSON.stringify({
    Status: status,
    Reason: reason,
    PhysicalResourceId: physicalId,
    StackId: event.StackId,
    RequestId: event.RequestId,
    LogicalResourceId: event.LogicalResourceId,
    NoEcho: false,
    Data: data || {},
  });
  const target = new URL(event.ResponseURL);
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: target.hostname,
      port: target.port,
      path: target.pathname + target.search,
      method: "PUT",
      headers: {
        "Content-Type": "",
        "Content-Length": Buffer.byteLength(body),
      },
    }, (res) => {
      res.resume();
      res.on("end", resolve);
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

exports.handler = async (event) => {
  let physicalId = event.PhysicalResourceId || event.LogicalResourceId;
  try {
    const raw = event.ResourceProperties[event.RequestType];
    if (raw) {
      const call = typeof raw === "string" ? JSON.parse(raw) : raw;
      physicalId = call.physicalResourceId?.id || physicalId;
      const sdk = require("@aws-sdk/client-opensearch");
      const clientClass = Object.entries(sdk).find(([name]) => name === "OpenSearchClient")?.[1];
      const commandName = call.action[0].toUpperCase() + call.action.slice(1) + "Command";
      const commandClass = Object.entries(sdk).find(([name]) => name === commandName)?.[1];
      if (!clientClass || !commandClass) throw new Error("OpenSearch SDK exports not found");
      const client = new clientClass({ apiVersion: "2021-01-01" });
      const result = await client.send(new commandClass(call.parameters));
      result.apiVersion = client.config.apiVersion;
      result.region = await client.config.region().catch(() => undefined);
      await respond(event, "SUCCESS", "OK", physicalId, result);
    } else {
      await respond(event, "SUCCESS", "OK", physicalId, {});
    }
  } catch (err) {
    await respond(event, "FAILED", err.message || String(err), physicalId, {});
  }
};
"""
    call_prefix = (
        '{"action":"updateDomainConfig","service":"OpenSearch",'
        f'"parameters":{{"DomainName":"{domain_name}",'
        f'"AccessPolicies":{json.dumps(access_policy)}}},'
        f'"physicalResourceId":{{"id":"{domain_name}AccessPolicy"}}}}'
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "SearchDomain": {
                "Type": "AWS::OpenSearchService::Domain",
                "Properties": {"DomainName": domain_name},
            },
            "Provider": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": function_name,
                    "Runtime": "nodejs18.x",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/custom-resource",
                    "Timeout": 10,
                    "Code": {"ZipFile": provider_code},
                    # CDK local tooling sets this exact hostname. The SDK shim
                    # must not turn it into the S3-shaped
                    # ``opensearch.localhost`` virtual host.
                    "Environment": {
                        "Variables": {"AWS_ENDPOINT_URL": "http://localhost:4566"},
                    },
                },
            },
            "AccessPolicy": {
                "Type": "Custom::OpenSearchAccessPolicy",
                "Properties": {
                    "ServiceToken": {"Fn::GetAtt": ["Provider", "Arn"]},
                    "Create": call_prefix,
                    "Update": call_prefix,
                    "InstallLatestAwsSdk": False,
                    "ServiceTimeout": 5,
                },
                "DependsOn": ["SearchDomain", "Provider"],
            },
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    config = opensearch.describe_domain_config(DomainName=domain_name)["DomainConfig"]
    assert config["AccessPolicies"]["Options"] == access_policy
    resource = _stack_resource(cfn, stack_name, "AccessPolicy")
    assert resource["PhysicalResourceId"] == f"{domain_name}AccessPolicy"

    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_s3tables_resources(cfn, s3tables):
    """CloudFormation can provision AWS::S3Tables::TableBucket, Namespace, and Table."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3Tables::TableBucket",
                "Properties": {"TableBucketName": "cfn-s3tables-test"},
            },
            "Ns": {
                "Type": "AWS::S3Tables::Namespace",
                "Properties": {
                    "TableBucketARN": {"Fn::GetAtt": ["Bucket", "TableBucketARN"]},
                    "Namespace": "myns",
                },
                "DependsOn": "Bucket",
            },
            "Table": {
                "Type": "AWS::S3Tables::Table",
                "Properties": {
                    "TableBucketARN": {"Fn::GetAtt": ["Bucket", "TableBucketARN"]},
                    "Namespace": "myns",
                    "TableName": "mytable",
                    "OpenTableFormat": "ICEBERG",
                },
                "DependsOn": "Ns",
            },
        },
        "Outputs": {
            "BucketArn": {"Value": {"Fn::GetAtt": ["Bucket", "TableBucketARN"]}},
            "TableArn": {"Value": {"Fn::GetAtt": ["Table", "TableARN"]}},
        },
    }

    stack_name = "cfn-s3tables-t01"
    try:
        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
    except Exception:
        pass

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    bucket_arn = outputs["BucketArn"]
    table_arn = outputs["TableArn"]
    assert "cfn-s3tables-test" in bucket_arn
    assert "mytable" in table_arn

    table = s3tables.get_table(tableBucketARN=bucket_arn, namespace="myns", name="mytable")
    assert table["name"] == "mytable"
    assert table["format"] == "ICEBERG"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
