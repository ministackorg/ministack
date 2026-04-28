import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
    import time as _t; _t.sleep(2)
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
