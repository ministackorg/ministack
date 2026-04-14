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
