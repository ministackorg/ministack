import base64
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services import pipes as _pipes


def _cfn_iceberg_json(path):
    """Hit the S3 Tables Iceberg REST catalog directly (LoadTable etc.) — the
    boto3 s3tables client only exposes the control-plane view, not the actual
    Iceberg schema/metadata a query engine like DuckDB reads."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    req = urllib.request.Request(
        f"{endpoint}{path}",
        headers={
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=test/20260604/us-east-1/s3tables/aws4_request, "
                "SignedHeaders=host, Signature=test"
            ),
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


# A resource that is accepted by the template pre-flight but fails when
# provisioned: a custom resource whose Lambda does not exist. Tests that need a
# runtime failure use this; unrecognized types are rejected before any
# resource is touched (see test_cfn_unrecognized_resource_type_rejected_up_front).
_FAILING_RESOURCE = {
    "Type": "AWS::CloudFormation::CustomResource",
    "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-does-not-exist",
    },
}


def _wait_stack(cfn, name, timeout=30):
    """Poll until stack reaches terminal status.

    A deleted stack is addressable only by its stack ID, so once it reaches
    DELETE_COMPLETE describe-by-name returns "does not exist" (real AWS); treat
    that as the terminal deleted state.
    """
    deadline = time.time() + timeout
    status = "UNKNOWN"
    while time.time() < deadline:
        try:
            stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE", "StackName": name}
            raise
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")


def _assert_apigwv2_api_not_found(call):
    with pytest.raises(ClientError) as exc_info:
        call()
    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def _regional_cfn_test_client(service, region):
    import boto3
    from botocore.config import Config

    return boto3.client(
        service,
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


def _delete_cfn_test_stack(cfn, stack_name):
    try:
        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
    except (ClientError, TimeoutError):
        pass


def _all_pages(client, operation, key, **kwargs):
    """Every item of a list action; the service pages at 100 items."""
    return [
        item
        for page in client.get_paginator(operation).paginate(**kwargs)
        for item in page[key]
    ]


def _output(stack, key):
    return next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == key)


def _stack_event_reasons(cfn, stack_name):
    return " ".join(
        e.get("ResourceStatusReason", "")
        for e in _all_pages(cfn, "describe_stack_events", "StackEvents",
                            StackName=stack_name)
    )


def _template_tags(value):
    """`value` without the ``aws:cloudformation:`` tags every stack resource
    carries since stack tags propagate: what the template itself set. Tag
    dicts, ``Key``/``Value`` lists and ``TagKey``/``TagValue`` lists are
    filtered; anything else is returned as is."""
    if isinstance(value, dict):
        return {k: v for k, v in value.items() if not str(k).startswith("aws:")}
    if isinstance(value, list) and value and all(
        isinstance(t, dict) and ("Key" in t or "TagKey" in t) for t in value
    ):
        return [t for t in value if not str(t.get("Key", t.get("TagKey"))).startswith("aws:")]
    return value


def test_cfn_region_scopes_stacks_change_sets_and_events():
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-regional-{suffix}"
    change_set_name = f"regional-update-{suffix}"
    east = _regional_cfn_test_client("cloudformation", "us-east-1")
    west = _regional_cfn_test_client("cloudformation", "us-west-2")
    empty_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {},
    }

    try:
        east.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(empty_template),
        )
        west.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(empty_template),
        )
        east_stack = _wait_stack(east, stack_name)
        west_stack = _wait_stack(west, stack_name)

        assert east_stack["StackStatus"] == "CREATE_COMPLETE"
        assert west_stack["StackStatus"] == "CREATE_COMPLETE"
        assert east_stack["StackId"] != west_stack["StackId"]
        assert ":us-east-1:" in east_stack["StackId"]
        assert ":us-west-2:" in west_stack["StackId"]

        east_described_ids = {
            stack["StackId"]
            for stack in _all_pages(east, "describe_stacks", "Stacks")
        }
        west_described_ids = {
            stack["StackId"]
            for stack in _all_pages(west, "describe_stacks", "Stacks")
        }
        assert east_stack["StackId"] in east_described_ids
        assert west_stack["StackId"] not in east_described_ids
        assert west_stack["StackId"] in west_described_ids
        assert east_stack["StackId"] not in west_described_ids

        east_listed_ids = {
            stack["StackId"]
            for stack in _all_pages(east, "list_stacks", "StackSummaries")
        }
        west_listed_ids = {
            stack["StackId"]
            for stack in _all_pages(west, "list_stacks", "StackSummaries")
        }
        assert east_stack["StackId"] in east_listed_ids
        assert west_stack["StackId"] not in east_listed_ids
        assert west_stack["StackId"] in west_listed_ids
        assert east_stack["StackId"] not in west_listed_ids

        east_events = east.describe_stack_events(
            StackName=east_stack["StackId"]
        )["StackEvents"]
        assert east_events
        assert {event["StackId"] for event in east_events} == {
            east_stack["StackId"]
        }
        with pytest.raises(ClientError) as exc:
            west.describe_stack_events(StackName=east_stack["StackId"])
        assert exc.value.response["Error"]["Code"] == "ValidationError"

        change_template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Handle": {
                    "Type": "AWS::CloudFormation::WaitConditionHandle",
                }
            },
        }
        change_set_id = east.create_change_set(
            StackName=stack_name,
            ChangeSetName=change_set_name,
            ChangeSetType="UPDATE",
            TemplateBody=json.dumps(change_template),
        )["Id"]
        assert east.describe_change_set(ChangeSetName=change_set_id)[
            "ChangeSetId"
        ] == change_set_id
        assert west.list_change_sets(StackName=stack_name)["Summaries"] == []
        with pytest.raises(ClientError) as exc:
            west.describe_change_set(ChangeSetName=change_set_id)
        assert exc.value.response["Error"]["Code"] == "ChangeSetNotFound"

        east.delete_stack(StackName=stack_name)
        assert _wait_stack(east, stack_name)["StackStatus"] == "DELETE_COMPLETE"
        assert west.describe_stacks(StackName=stack_name)["Stacks"][0][
            "StackId"
        ] == west_stack["StackId"]
    finally:
        _delete_cfn_test_stack(east, stack_name)
        _delete_cfn_test_stack(west, stack_name)


def test_cfn_region_scopes_exports_imports_and_delete_checks():
    suffix = _uuid_mod.uuid4().hex[:8]
    export_name = f"cfn-regional-export-{suffix}"
    producer_name = f"cfn-regional-producer-{suffix}"
    consumer_name = f"cfn-regional-consumer-{suffix}"
    decoy_name = f"cfn-regional-decoy-{suffix}"
    east = _regional_cfn_test_client("cloudformation", "us-east-1")
    west = _regional_cfn_test_client("cloudformation", "us-west-2")
    producer_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {},
        "Outputs": {
            "SharedValue": {
                "Value": "east-value",
                "Export": {"Name": export_name},
            }
        },
    }
    consumer_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "ImportedParameter": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": f"/cfn/regional/{suffix}",
                    "Type": "String",
                    "Value": {"Fn::ImportValue": export_name},
                },
            }
        },
    }
    decoy_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Metadata": {"CrossRegionReference": {"Fn::ImportValue": export_name}},
        "Resources": {},
    }

    try:
        east.create_stack(
            StackName=producer_name,
            TemplateBody=json.dumps(producer_template),
        )
        producer = _wait_stack(east, producer_name)
        assert producer["StackStatus"] == "CREATE_COMPLETE"
        assert {
            export["Name"]: export["Value"]
            for export in _all_pages(east, "list_exports", "Exports")
        }[export_name] == "east-value"
        assert export_name not in {
            export["Name"] for export in _all_pages(west, "list_exports", "Exports")
        }
        with pytest.raises(ClientError) as exc:
            west.describe_stacks(StackName=producer_name)
        assert exc.value.response["Error"]["Code"] == "ValidationError"

        west.create_stack(
            StackName=consumer_name,
            TemplateBody=json.dumps(consumer_template),
            DisableRollback=True,
        )
        consumer = _wait_stack(west, consumer_name)
        assert consumer["StackStatus"] == "CREATE_FAILED"
        assert f"Export '{export_name}' not found" in consumer["StackStatusReason"]

        west.create_stack(
            StackName=decoy_name,
            TemplateBody=json.dumps(decoy_template),
        )
        assert _wait_stack(west, decoy_name)["StackStatus"] == "CREATE_COMPLETE"

        east.delete_stack(StackName=producer_name)
        assert _wait_stack(east, producer_name)["StackStatus"] == "DELETE_COMPLETE"
        assert west.describe_stacks(StackName=decoy_name)["Stacks"][0][
            "StackStatus"
        ] == "CREATE_COMPLETE"
    finally:
        _delete_cfn_test_stack(east, producer_name)
        _delete_cfn_test_stack(west, consumer_name)
        _delete_cfn_test_stack(west, decoy_name)


def test_cfn_list_actions_page_at_one_hundred(cfn):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-paging-{suffix}"
    template = {
        "Resources": {
            f"P{i}": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Type": "String",
                    "Name": f"/cfn-paging/{suffix}/{i}",
                    "Value": str(i),
                },
            }
            for i in range(101)
        }
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

        # 101 resources: two pages, the token only on the first one.
        first = cfn.list_stack_resources(StackName=stack_name)
        assert len(first["StackResourceSummaries"]) == 100
        second = cfn.list_stack_resources(
            StackName=stack_name, NextToken=first["NextToken"])
        assert len(second["StackResourceSummaries"]) == 1
        assert "NextToken" not in second
        logical_ids = {r["LogicalResourceId"] for r in first["StackResourceSummaries"]}
        logical_ids |= {r["LogicalResourceId"] for r in second["StackResourceSummaries"]}
        assert logical_ids == set(template["Resources"])
        assert len(_all_pages(cfn, "list_stack_resources", "StackResourceSummaries",
                              StackName=stack_name)) == 101
        assert "NextToken" not in cfn.describe_stacks(StackName=stack_name)

        # Two events per resource plus the stack's own: three pages, newest first
        # across the page boundary.
        pages = list(cfn.get_paginator("describe_stack_events").paginate(
            StackName=stack_name))
        assert len(pages) == 3
        assert [len(page["StackEvents"]) for page in pages[:2]] == [100, 100]
        events = [e for page in pages for e in page["StackEvents"]]
        assert len({e["EventId"] for e in events}) == len(events) > 200
        timestamps = [e["Timestamp"] for e in events]
        assert timestamps == sorted(timestamps, reverse=True)
        # The stack's own CREATE_IN_PROGRESS is the oldest event and lands on
        # the last page (events of one millisecond keep their emission order).
        stack_start = [
            e for e in pages[-1]["StackEvents"]
            if e["ResourceType"] == "AWS::CloudFormation::Stack"
            and e["ResourceStatus"] == "CREATE_IN_PROGRESS"
        ]
        assert len(stack_start) == 1
        assert stack_start[0]["Timestamp"] == min(timestamps)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_list_exports_pages_at_one_hundred(cfn):
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-paging-exports-{suffix}"
    template = {
        "Resources": {
            "P": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Type": "String",
                    "Name": f"/cfn-paging-exports/{suffix}",
                    "Value": "v",
                },
            }
        },
        "Outputs": {
            f"O{i}": {"Value": str(i), "Export": {"Name": f"cfn-paging-{suffix}-{i}"}}
            for i in range(101)
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        pages = list(cfn.get_paginator("list_exports").paginate())
        assert len(pages) >= 2
        assert all(len(page["Exports"]) == 100 for page in pages[:-1])
        assert "NextToken" not in pages[-1]
        names = {e["Name"] for page in pages for e in page["Exports"]}
        assert {f"cfn-paging-{suffix}-{i}" for i in range(101)} <= names
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_list_imports_and_change_sets_take_a_token(cfn):
    suffix = _uuid_mod.uuid4().hex[:8]
    producer = f"cfn-paging-producer-{suffix}"
    consumer = f"cfn-paging-consumer-{suffix}"
    export_name = f"cfn-paging-export-{suffix}"
    producer_template = {
        "Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
        "Outputs": {"Q": {"Value": {"Ref": "Q"}, "Export": {"Name": export_name}}},
    }
    consumer_template = {
        "Resources": {
            "P": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Type": "String",
                    "Name": f"/cfn-paging-consumer/{suffix}",
                    "Value": {"Fn::ImportValue": export_name},
                },
            }
        }
    }
    cfn.create_stack(StackName=producer, TemplateBody=json.dumps(producer_template))
    try:
        assert _wait_stack(cfn, producer)["StackStatus"] == "CREATE_COMPLETE"
        cfn.create_stack(StackName=consumer, TemplateBody=json.dumps(consumer_template))
        try:
            assert _wait_stack(cfn, consumer)["StackStatus"] == "CREATE_COMPLETE"
            assert _all_pages(cfn, "list_imports", "Imports",
                              ExportName=export_name) == [consumer]
            assert "NextToken" not in cfn.list_imports(ExportName=export_name)
            # The token is checked once the export is known to be imported.
            with pytest.raises(ClientError) as exc:
                cfn.list_imports(ExportName=export_name, NextToken="ListStacks:0")
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert "NextToken" in exc.value.response["Error"]["Message"]
            with pytest.raises(ClientError) as exc:
                cfn.list_imports(ExportName=f"{export_name}-unused", NextToken="ListStacks:0")
            assert "not imported" in exc.value.response["Error"]["Message"]

            cfn.create_change_set(
                StackName=producer, ChangeSetName=f"cs-{suffix}",
                ChangeSetType="UPDATE",
                TemplateBody=json.dumps({**producer_template, "Description": "changed"}),
            )
            listed = cfn.list_change_sets(StackName=producer)
            assert [cs["ChangeSetName"] for cs in listed["Summaries"]] == [f"cs-{suffix}"]
            assert "NextToken" not in listed
            assert _all_pages(cfn, "list_change_sets", "Summaries",
                              StackName=producer) == listed["Summaries"]
            with pytest.raises(ClientError) as exc:
                cfn.list_change_sets(StackName=producer, NextToken="ListStacks:0")
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert "NextToken" in exc.value.response["Error"]["Message"]
        finally:
            _delete_cfn_test_stack(cfn, consumer)
    finally:
        _delete_cfn_test_stack(cfn, producer)


def test_cfn_list_actions_offset_past_the_end_is_an_empty_page(cfn):
    listed = cfn.list_stacks(NextToken="ListStacks:999999")
    assert listed["StackSummaries"] == []
    assert "NextToken" not in listed
    exports = cfn.list_exports(NextToken="ListExports:999999")
    assert exports["Exports"] == []
    assert "NextToken" not in exports


@pytest.mark.parametrize(
    ("operation", "token"),
    [
        ("list_stacks", "not-a-token"),
        ("describe_stacks", "not-a-token"),
        ("list_exports", "not-a-token"),
        ("list_exports", "ListStacks:100"),
        ("list_stacks", "ListStacks:hundred"),
        ("describe_stacks", "DescribeStacks:"),
    ],
)
def test_cfn_list_actions_refuse_a_foreign_next_token(cfn, operation, token):
    """A token is ``<Action>:<offset>``; anything else, a token of another
    action included, is refused before a page is built."""
    with pytest.raises(ClientError) as exc:
        getattr(cfn, operation)(NextToken=token)
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert "NextToken" in exc.value.response["Error"]["Message"]


def test_cfn_nested_stack_stays_in_parent_region():
    suffix = _uuid_mod.uuid4().hex[:8]
    parent_name = f"cfn-regional-parent-{suffix}"
    templates_bucket = f"cfn-regional-templates-{suffix}"
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    east = _regional_cfn_test_client("cloudformation", "us-east-1")
    west = _regional_cfn_test_client("cloudformation", "us-west-2")
    west_s3 = _regional_cfn_test_client("s3", "us-west-2")
    child_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {},
        "Outputs": {"ChildRegion": {"Value": {"Ref": "AWS::Region"}}},
    }
    parent_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {
                    "TemplateURL": f"{endpoint}/{templates_bucket}/child.json",
                },
            }
        },
        "Outputs": {
            "ChildRegion": {
                "Value": {"Fn::GetAtt": ["Nested", "Outputs.ChildRegion"]}
            }
        },
    }

    try:
        west_s3.create_bucket(
            Bucket=templates_bucket,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        west_s3.put_object(
            Bucket=templates_bucket,
            Key="child.json",
            Body=json.dumps(child_template).encode(),
        )
        west.create_stack(
            StackName=parent_name,
            TemplateBody=json.dumps(parent_template),
        )
        parent = _wait_stack(west, parent_name)
        assert parent["StackStatus"] == "CREATE_COMPLETE", parent.get(
            "StackStatusReason"
        )
        assert {
            output["OutputKey"]: output["OutputValue"]
            for output in parent["Outputs"]
        }["ChildRegion"] == "us-west-2"

        child = next(
            stack
            for stack in _all_pages(west, "describe_stacks", "Stacks")
            if stack["StackName"].startswith(f"{parent_name}-Nested-")
        )
        assert ":us-west-2:" in child["StackId"]
        with pytest.raises(ClientError) as exc:
            east.describe_stacks(StackName=child["StackId"])
        assert exc.value.response["Error"]["Code"] == "ValidationError"
    finally:
        _delete_cfn_test_stack(west, parent_name)
        try:
            west_s3.delete_object(Bucket=templates_bucket, Key="child.json")
            west_s3.delete_bucket(Bucket=templates_bucket)
        except ClientError:
            pass


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


def test_cfn_s3_bucket_notification_configuration(cfn, s3, sqs):
    """AWS::S3::Bucket NotificationConfiguration is applied, not silently dropped:
    it round-trips through GetBucketNotificationConfiguration (with the
    CloudFormation property names translated to the S3 API's), an upload matching
    the event and key filter is delivered to the target, and removing the property
    on a stack update clears the configuration. (#1359)
    """
    queue_url = sqs.create_queue(QueueName="cfn-notif-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    def template(with_notif):
        props = {"BucketName": "cfn-notif-bucket"}
        if with_notif:
            props["NotificationConfiguration"] = {
                "QueueConfigurations": [{
                    "Queue": queue_arn,
                    "Event": "s3:ObjectCreated:*",
                    "Filter": {"S3Key": {"Rules": [{"Name": "suffix", "Value": ".csv"}]}},
                }],
            }
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"Bucket": {"Type": "AWS::S3::Bucket", "Properties": props}},
        })

    cfn.create_stack(StackName="cfn-notif", TemplateBody=template(True))
    assert _wait_stack(cfn, "cfn-notif")["StackStatus"] == "CREATE_COMPLETE"

    # The property survived: the config is readable back, with the CloudFormation
    # names (Queue/Event/Rules) translated to the S3 API's (QueueArn/Events/Key).
    qcfgs = s3.get_bucket_notification_configuration(
        Bucket="cfn-notif-bucket")["QueueConfigurations"]
    assert len(qcfgs) == 1
    assert qcfgs[0]["QueueArn"] == queue_arn
    assert qcfgs[0]["Events"] == ["s3:ObjectCreated:*"]
    assert qcfgs[0]["Filter"]["Key"]["FilterRules"] == [{"Name": "suffix", "Value": ".csv"}]

    # Delivery works end to end through the CloudFormation path, honouring the filter.
    s3.put_object(Bucket="cfn-notif-bucket", Key="skip.txt", Body=b"no")
    s3.put_object(Bucket="cfn-notif-bucket", Key="take.csv", Body=b"yes")
    time.sleep(0.5)
    msgs = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    keys = [
        json.loads(m["Body"])["Records"][0]["s3"]["object"]["key"]
        for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])
    ]
    assert "take.csv" in keys
    assert "skip.txt" not in keys

    # Removing the property on an update clears the configuration.
    cfn.update_stack(StackName="cfn-notif", TemplateBody=template(False))
    assert _wait_stack(cfn, "cfn-notif")["StackStatus"] == "UPDATE_COMPLETE"
    cleared = s3.get_bucket_notification_configuration(Bucket="cfn-notif-bucket")
    assert not cleared.get("QueueConfigurations")

    cfn.delete_stack(StackName="cfn-notif")
    _wait_stack(cfn, "cfn-notif")


def test_cfn_iot_and_cognito_role_attachment(cfn, iot_client, cognito_identity):
    """AWS::IoT::ThingType, AWS::IoT::Policy, and
    AWS::Cognito::IdentityPoolRoleAttachment provision onto their real services
    instead of rolling the stack back — each is readable through its own API. (#1345, item 5)
    """
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "TT": {"Type": "AWS::IoT::ThingType", "Properties": {
                "ThingTypeName": "cfn-tt",
                "ThingTypeProperties": {"ThingTypeDescription": "d", "SearchableAttributes": ["room"]}}},
            "Pol": {"Type": "AWS::IoT::Policy", "Properties": {
                "PolicyName": "cfn-pol",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"}]}}},
            "Pool": {"Type": "AWS::Cognito::IdentityPool", "Properties": {
                "IdentityPoolName": "cfn-pool", "AllowUnauthenticatedIdentities": True}},
            "Roles": {"Type": "AWS::Cognito::IdentityPoolRoleAttachment", "Properties": {
                "IdentityPoolId": {"Ref": "Pool"},
                "Roles": {"authenticated": "arn:aws:iam::000000000000:role/auth"}}},
        },
        "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
    }
    cfn.create_stack(StackName="cfn-iot-cog", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-iot-cog")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    tt = iot_client.describe_thing_type(thingTypeName="cfn-tt")
    assert tt["thingTypeProperties"]["searchableAttributes"] == ["room"]
    pol = iot_client.get_policy(policyName="cfn-pol")
    assert pol["policyArn"].endswith("policy/cfn-pol")

    pool_id = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "PoolId")
    roles = cognito_identity.get_identity_pool_roles(IdentityPoolId=pool_id)["Roles"]
    assert roles["authenticated"] == "arn:aws:iam::000000000000:role/auth"

    cfn.delete_stack(StackName="cfn-iot-cog")
    _wait_stack(cfn, "cfn-iot-cog")


def test_cfn_cognito_identity_pool_principal_tag(cfn, cognito_identity):
    """AWS::Cognito::IdentityPoolPrincipalTag provisions onto the identity pool,
    is readable through GetPrincipalTagAttributeMap, and is cleared on delete."""
    provider = "cognito-idp.us-east-1.amazonaws.com/us-east-1_example"
    replacement = "cognito-idp.us-east-1.amazonaws.com/us-east-1_replacement"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Pool": {"Type": "AWS::Cognito::IdentityPool", "Properties": {
                "IdentityPoolName": "cfn-tag-pool", "AllowUnauthenticatedIdentities": True}},
            "Tags": {"Type": "AWS::Cognito::IdentityPoolPrincipalTag", "Properties": {
                "IdentityPoolId": {"Ref": "Pool"},
                "IdentityProviderName": provider,
                "UseDefaults": False,
                "PrincipalTags": {"tenant": "custom:tenant"}}},
        },
        "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                    "TagRef": {"Value": {"Ref": "Tags"}}},
    }
    cfn.create_stack(StackName="cfn-cog-tag", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cog-tag")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    pool_id = outputs["PoolId"]
    # Ref is the documented primary identifier: "<pool id>|<provider name>".
    assert outputs["TagRef"] == f"{pool_id}|{provider}"
    mapping = cognito_identity.get_principal_tag_attribute_map(
        IdentityPoolId=pool_id, IdentityProviderName=provider)
    assert mapping["UseDefaults"] is False
    assert mapping["PrincipalTags"] == {"tenant": "custom:tenant"}

    # A changed PrincipalTags applies in place.
    template["Resources"]["Tags"]["Properties"]["PrincipalTags"] = {"email": "email"}
    cfn.update_stack(StackName="cfn-cog-tag", TemplateBody=json.dumps(template))
    assert _wait_stack(cfn, "cfn-cog-tag")["StackStatus"] == "UPDATE_COMPLETE"
    assert cognito_identity.get_principal_tag_attribute_map(
        IdentityPoolId=pool_id, IdentityProviderName=provider,
    )["PrincipalTags"] == {"email": "email"}

    # IdentityProviderName is create-only: the mapping moves to the new provider
    # and the one it left goes inactive.
    template["Resources"]["Tags"]["Properties"]["IdentityProviderName"] = replacement
    cfn.update_stack(StackName="cfn-cog-tag", TemplateBody=json.dumps(template))
    assert _wait_stack(cfn, "cfn-cog-tag")["StackStatus"] == "UPDATE_COMPLETE"
    assert cognito_identity.get_principal_tag_attribute_map(
        IdentityPoolId=pool_id, IdentityProviderName=replacement,
    )["PrincipalTags"] == {"email": "email"}
    with pytest.raises(ClientError) as exc:
        cognito_identity.get_principal_tag_attribute_map(
            IdentityPoolId=pool_id, IdentityProviderName=provider)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    cfn.delete_stack(StackName="cfn-cog-tag")
    _wait_stack(cfn, "cfn-cog-tag")


def test_cfn_iot_policy_document_update_applies_in_place(cfn, iot_client):
    """A changed PolicyDocument updates the policy instead of rolling the stack
    back: IoT stores a new default version and Ref keeps the same name."""
    def template(actions):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IoT::Policy", "Properties": {
                "PolicyName": "cfn-pol-upd",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": actions, "Resource": "*"}]}}}},
            "Outputs": {"Name": {"Value": {"Ref": "Pol"}}},
        })

    cfn.create_stack(StackName="cfn-iot-pol-upd", TemplateBody=template(["iot:Connect"]))
    assert _wait_stack(cfn, "cfn-iot-pol-upd")["StackStatus"] == "CREATE_COMPLETE"

    cfn.update_stack(
        StackName="cfn-iot-pol-upd",
        TemplateBody=template(["iot:Connect", "iot:Publish"]),
    )
    stack = _wait_stack(cfn, "cfn-iot-pol-upd")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    ) == "cfn-pol-upd"

    policy = iot_client.get_policy(policyName="cfn-pol-upd")
    assert json.loads(policy["policyDocument"])["Statement"][0]["Action"] == [
        "iot:Connect", "iot:Publish",
    ]
    assert policy["defaultVersionId"] == "2"
    versions = iot_client.list_policy_versions(policyName="cfn-pol-upd")["policyVersions"]
    assert {v["versionId"] for v in versions} == {"1", "2"}

    cfn.delete_stack(StackName="cfn-iot-pol-upd")
    _wait_stack(cfn, "cfn-iot-pol-upd")


def test_cfn_iot_policy_updates_stay_under_the_version_cap(cfn, iot_client):
    """IoT keeps at most five versions of a policy, so repeated updates prune the
    oldest non-default version rather than growing the list without bound."""
    def template(count):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IoT::Policy", "Properties": {
                "PolicyName": "cfn-pol-cap",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "iot:Connect",
                     "Resource": [f"arn:aws:iot:*:*:client/c{i}" for i in range(count)]}]}}}},
        })

    cfn.create_stack(StackName="cfn-iot-pol-cap", TemplateBody=template(1))
    assert _wait_stack(cfn, "cfn-iot-pol-cap")["StackStatus"] == "CREATE_COMPLETE"

    for count in range(2, 9):
        cfn.update_stack(StackName="cfn-iot-pol-cap", TemplateBody=template(count))
        assert _wait_stack(cfn, "cfn-iot-pol-cap")["StackStatus"] == "UPDATE_COMPLETE"

    versions = iot_client.list_policy_versions(policyName="cfn-pol-cap")["policyVersions"]
    assert len(versions) == 5
    assert {v["versionId"] for v in versions} == {"4", "5", "6", "7", "8"}

    policy = iot_client.get_policy(policyName="cfn-pol-cap")
    assert policy["defaultVersionId"] == "8"
    assert len(json.loads(policy["policyDocument"])["Statement"][0]["Resource"]) == 8

    cfn.delete_stack(StackName="cfn-iot-pol-cap")
    _wait_stack(cfn, "cfn-iot-pol-cap")


def test_cfn_iot_policy_rename_replaces_the_policy(cfn, iot_client):
    """Renaming is a replacement — the new policy exists under the new name and
    the old one does not survive the update."""
    def template(name):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IoT::Policy", "Properties": {
                "PolicyName": name,
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"}]}}}},
            "Outputs": {"Name": {"Value": {"Ref": "Pol"}}},
        })

    cfn.create_stack(StackName="cfn-iot-pol-ren", TemplateBody=template("cfn-pol-before"))
    assert _wait_stack(cfn, "cfn-iot-pol-ren")["StackStatus"] == "CREATE_COMPLETE"

    cfn.update_stack(StackName="cfn-iot-pol-ren", TemplateBody=template("cfn-pol-after"))
    stack = _wait_stack(cfn, "cfn-iot-pol-ren")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    ) == "cfn-pol-after"

    assert iot_client.get_policy(policyName="cfn-pol-after")["policyName"] == "cfn-pol-after"
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.get_policy(policyName="cfn-pol-before")

    cfn.delete_stack(StackName="cfn-iot-pol-ren")
    _wait_stack(cfn, "cfn-iot-pol-ren")


def _cfn_provisioning_template_body(marker="one"):
    # The IoT control plane refuses a body without an AWS::IoT::Certificate
    # resource, so every stack body carries one; `marker` varies the body so a
    # stack update can carry a genuinely changed TemplateBody.
    return json.dumps({
        "Parameters": {"SerialNumber": {"Type": "String"}},
        "Resources": {
            "certificate": {"Type": "AWS::IoT::Certificate",
                            "Properties": {"CertificateId":
                                           {"Ref": "AWS::IoT::Certificate::Id"}}},
            "thing": {"Type": "AWS::IoT::Thing",
                      "Properties": {"ThingName": {"Ref": "SerialNumber"},
                                     "AttributePayload": {"Attributes":
                                                          {"marker": marker}}}},
        },
    })


def test_cfn_iot_provisioning_template(cfn, iot_client):
    """AWS::IoT::ProvisioningTemplate provisions onto the IoT control plane
    instead of rolling the stack back, is readable through the real API, and
    Description + TemplateBody changes apply in place under the same
    physical id."""

    def template(description, body):
        return json.dumps({
            "Resources": {"PT": {"Type": "AWS::IoT::ProvisioningTemplate", "Properties": {
                "TemplateName": "cfn-provtemplate",
                "Description": description,
                "Enabled": True,
                "ProvisioningRoleArn": "arn:aws:iam::000000000000:role/provisioning",
                "TemplateBody": body}}},
            "Outputs": {"Name": {"Value": {"Ref": "PT"}},
                        "Arn": {"Value": {"Fn::GetAtt": ["PT", "TemplateArn"]}}},
        })

    body_before = _cfn_provisioning_template_body("before")
    cfn.create_stack(StackName="cfn-iot-provtmpl",
                     TemplateBody=template("before", body_before))
    stack = _wait_stack(cfn, "cfn-iot-provtmpl")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    assert outputs["Name"] == "cfn-provtemplate"
    assert outputs["Arn"].endswith(":provisioningtemplate/cfn-provtemplate")

    desc = iot_client.describe_provisioning_template(templateName="cfn-provtemplate")
    assert desc["description"] == "before"
    assert desc["enabled"] is True
    assert desc["templateBody"] == body_before
    assert desc["templateArn"] == outputs["Arn"]

    body_after = _cfn_provisioning_template_body("after")
    cfn.update_stack(StackName="cfn-iot-provtmpl",
                     TemplateBody=template("after", body_after))
    stack = _wait_stack(cfn, "cfn-iot-provtmpl")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    ) == "cfn-provtemplate"
    desc = iot_client.describe_provisioning_template(templateName="cfn-provtemplate")
    assert desc["description"] == "after"
    assert desc["enabled"] is True
    assert desc["templateBody"] == body_after
    assert desc["provisioningRoleArn"] == "arn:aws:iam::000000000000:role/provisioning"

    cfn.delete_stack(StackName="cfn-iot-provtmpl")
    _wait_stack(cfn, "cfn-iot-provtmpl")
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.describe_provisioning_template(templateName="cfn-provtemplate")


def test_cfn_iot_provisioning_template_rename_replaces(cfn, iot_client):
    """A TemplateName change replaces the template the way AWS::IoT::Policy's
    rename does: new physical id, old template gone."""
    body = _cfn_provisioning_template_body()

    def template(name):
        return json.dumps({
            "Resources": {"PT": {"Type": "AWS::IoT::ProvisioningTemplate", "Properties": {
                "TemplateName": name,
                "ProvisioningRoleArn": "arn:aws:iam::000000000000:role/provisioning",
                "TemplateBody": body}}},
            "Outputs": {"Name": {"Value": {"Ref": "PT"}}},
        })

    cfn.create_stack(StackName="cfn-iot-provtmpl-ren",
                     TemplateBody=template("cfn-provtemplate-old"))
    assert _wait_stack(cfn, "cfn-iot-provtmpl-ren")["StackStatus"] == "CREATE_COMPLETE"

    cfn.update_stack(StackName="cfn-iot-provtmpl-ren",
                     TemplateBody=template("cfn-provtemplate-new"))
    stack = _wait_stack(cfn, "cfn-iot-provtmpl-ren")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    ) == "cfn-provtemplate-new"
    iot_client.describe_provisioning_template(templateName="cfn-provtemplate-new")
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.describe_provisioning_template(templateName="cfn-provtemplate-old")

    cfn.delete_stack(StackName="cfn-iot-provtmpl-ren")
    _wait_stack(cfn, "cfn-iot-provtmpl-ren")
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.describe_provisioning_template(templateName="cfn-provtemplate-new")


def test_cfn_iot_provisioning_template_generated_name(cfn, iot_client):
    """TemplateName is optional in the CloudFormation schema: an omitted one
    gets a generated physical name that satisfies the 36-char template-name
    limit, and an in-place update keeps that name stable."""

    def template(description):
        return json.dumps({
            "Resources": {"PT": {"Type": "AWS::IoT::ProvisioningTemplate", "Properties": {
                "Description": description,
                "ProvisioningRoleArn": "arn:aws:iam::000000000000:role/provisioning",
                "TemplateBody": _cfn_provisioning_template_body()}}},
            "Outputs": {"Name": {"Value": {"Ref": "PT"}}},
        })

    cfn.create_stack(StackName="cfn-iot-provtmpl-auto",
                     TemplateBody=template("before"))
    stack = _wait_stack(cfn, "cfn-iot-provtmpl-auto")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    name = next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    )
    assert re.fullmatch(r"[0-9A-Za-z_-]{1,36}", name)
    assert iot_client.describe_provisioning_template(
        templateName=name
    )["description"] == "before"

    cfn.update_stack(StackName="cfn-iot-provtmpl-auto",
                     TemplateBody=template("after"))
    stack = _wait_stack(cfn, "cfn-iot-provtmpl-auto")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert next(
        o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "Name"
    ) == name
    assert iot_client.describe_provisioning_template(
        templateName=name
    )["description"] == "after"

    cfn.delete_stack(StackName="cfn-iot-provtmpl-auto")
    _wait_stack(cfn, "cfn-iot-provtmpl-auto")
    with pytest.raises(iot_client.exceptions.ResourceNotFoundException):
        iot_client.describe_provisioning_template(templateName=name)


def test_cfn_iot_ca_certificate_lifecycle(cfn, iot_client):
    """AWS::IoT::CACertificate provisions onto the real CA registry instead of
    rolling the stack back: create registers the PEM (readable back through
    DescribeCACertificate), a status change is applied in place under the same
    PEM-derived physical id, and delete deactivates first — an ACTIVE CA
    refuses deletion — before removing the registration."""
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca

    ca_pem, _ca_key = generate_ca(common_name="cfn-test-ca")

    def template(status):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"CA": {"Type": "AWS::IoT::CACertificate", "Properties": {
                "CACertificatePem": ca_pem,
                "Status": status,
                "AutoRegistrationStatus": "ENABLE",
                "CertificateMode": "SNI_ONLY",
            }}},
            "Outputs": {
                "CaRef": {"Value": {"Ref": "CA"}},
                "CaArn": {"Value": {"Fn::GetAtt": ["CA", "Arn"]}},
                "CaId": {"Value": {"Fn::GetAtt": ["CA", "Id"]}},
            },
        })

    cfn.create_stack(StackName="cfn-iot-ca", TemplateBody=template("ACTIVE"))
    stack = _wait_stack(cfn, "cfn-iot-ca")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    ca_id = outputs["CaId"]
    assert outputs["CaRef"] == ca_id
    assert outputs["CaArn"].endswith(":cacert/" + ca_id)

    # Readable back through the CA registry's own API, with every declared
    # property applied.
    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"]
    assert desc["certificatePem"] == ca_pem
    assert desc["status"] == "ACTIVE"
    assert desc["autoRegistrationStatus"] == "ENABLE"
    assert desc["certificateMode"] == "SNI_ONLY"

    # A status change is applied in place: same physical id, no replacement.
    cfn.update_stack(StackName="cfn-iot-ca", TemplateBody=template("INACTIVE"))
    stack = _wait_stack(cfn, "cfn-iot-ca")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    assert outputs["CaId"] == ca_id
    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"]
    assert desc["status"] == "INACTIVE"

    # Reactivate, then delete the stack: an ACTIVE CA refuses DeleteCACertificate,
    # so the provisioner must deactivate before deleting.
    cfn.update_stack(StackName="cfn-iot-ca", TemplateBody=template("ACTIVE"))
    assert _wait_stack(cfn, "cfn-iot-ca")["StackStatus"] == "UPDATE_COMPLETE"

    cfn.delete_stack(StackName="cfn-iot-ca")
    _wait_stack(cfn, "cfn-iot-ca")
    with pytest.raises(ClientError) as ei:
        iot_client.describe_ca_certificate(certificateId=ca_id)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_iot_ca_certificate_existing_registration_fails_the_stack(cfn, iot_client):
    """A CA already registered out of band answers ResourceAlreadyExists when
    the stack's create re-registers the PEM (the certificate id is derived
    from it). Real CloudFormation fails the create on a resource that already
    exists rather than adopting one the stack never created — and the
    out-of-band registration survives, untouched by the rollback."""
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca

    ca_pem, _ca_key = generate_ca(common_name="cfn-adopt-ca")
    # Registered out of band: INACTIVE, auto-registration DISABLE.
    pre_id = iot_client.register_ca_certificate(caCertificate=ca_pem)["certificateId"]

    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {"CA": {"Type": "AWS::IoT::CACertificate", "Properties": {
            "CACertificatePem": ca_pem,
            "Status": "ACTIVE",
            "AutoRegistrationStatus": "ENABLE",
        }}},
    })
    cfn.create_stack(StackName="cfn-iot-ca-exists", TemplateBody=template)
    stack = _wait_stack(cfn, "cfn-iot-ca-exists")
    assert stack["StackStatus"] in ("CREATE_FAILED", "ROLLBACK_COMPLETE"), stack["StackStatus"]

    # The out-of-band CA is untouched: still registered, still INACTIVE.
    desc = iot_client.describe_ca_certificate(certificateId=pre_id)[
        "certificateDescription"]
    assert desc["status"] == "INACTIVE"
    assert desc["autoRegistrationStatus"] == "DISABLE"

    cfn.delete_stack(StackName="cfn-iot-ca-exists")
    _wait_stack(cfn, "cfn-iot-ca-exists")
    # And it survives the failed stack's deletion too.
    assert iot_client.describe_ca_certificate(certificateId=pre_id)
    iot_client.delete_ca_certificate(certificateId=pre_id)


def test_cfn_iot_ca_certificate_registration_config_and_mode_immutability(cfn, iot_client):
    """RegistrationConfig round-trips through the provisioner into
    DescribeCACertificate (it is the JITR provisioning config — the property a
    JITR CA exists for), RemoveAutoRegistration turns auto-registration off on
    update, and a CertificateMode change is refused the way CloudFormation's
    update-requires-replacement would replace it."""
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca

    ca_pem, _ca_key = generate_ca(common_name="cfn-ca-regcfg")

    def template(mode, remove_auto=False):
        props = {
            "CACertificatePem": ca_pem,
            "Status": "INACTIVE",
            "AutoRegistrationStatus": "ENABLE",
            "CertificateMode": mode,
            "RegistrationConfig": {
                "RoleArn": "arn:aws:iam::123456789012:role/jitr-role",
                "TemplateName": "jitr-template",
            },
        }
        if remove_auto:
            props["RemoveAutoRegistration"] = True
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"CA": {"Type": "AWS::IoT::CACertificate",
                                 "Properties": props}},
            "Outputs": {"CaId": {"Value": {"Fn::GetAtt": ["CA", "Id"]}}},
        })

    cfn.create_stack(StackName="cfn-iot-ca-regcfg", TemplateBody=template("SNI_ONLY"))
    stack = _wait_stack(cfn, "cfn-iot-ca-regcfg")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    ca_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["CaId"]

    got = iot_client.describe_ca_certificate(certificateId=ca_id)
    assert got["registrationConfig"] == {
        "roleArn": "arn:aws:iam::123456789012:role/jitr-role",
        "templateName": "jitr-template",
    }

    # RemoveAutoRegistration on an update turns auto-registration off.
    cfn.update_stack(StackName="cfn-iot-ca-regcfg",
                     TemplateBody=template("SNI_ONLY", remove_auto=True))
    assert _wait_stack(cfn, "cfn-iot-ca-regcfg")["StackStatus"] == "UPDATE_COMPLETE"
    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"]
    assert desc["autoRegistrationStatus"] == "DISABLE"

    # A CertificateMode change cannot be applied in place; the update fails
    # and rolls back, the stored mode untouched.
    cfn.update_stack(StackName="cfn-iot-ca-regcfg", TemplateBody=template("DEFAULT"))
    stack = _wait_stack(cfn, "cfn-iot-ca-regcfg")
    assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"]
    assert desc["certificateMode"] == "SNI_ONLY"

    cfn.delete_stack(StackName="cfn-iot-ca-regcfg")
    _wait_stack(cfn, "cfn-iot-ca-regcfg")


def test_cfn_iot_ca_certificate_pem_change_refused(cfn, iot_client):
    """CACertificatePem is the physical identity — the certificate id derives
    from it — so an update that changes the PEM fails loudly and rolls back
    instead of silently replacing the CA (which would orphan every device
    certificate registered under the old one). The original registration
    survives untouched."""
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca

    pem_a, _key_a = generate_ca(common_name="cfn-ca-pem-a")
    pem_b, _key_b = generate_ca(common_name="cfn-ca-pem-b")

    def template(pem):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"CA": {"Type": "AWS::IoT::CACertificate", "Properties": {
                "CACertificatePem": pem,
                "Status": "INACTIVE",
            }}},
            "Outputs": {"CaId": {"Value": {"Fn::GetAtt": ["CA", "Id"]}}},
        })

    cfn.create_stack(StackName="cfn-iot-ca-pem", TemplateBody=template(pem_a))
    stack = _wait_stack(cfn, "cfn-iot-ca-pem")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    ca_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["CaId"]

    cfn.update_stack(StackName="cfn-iot-ca-pem", TemplateBody=template(pem_b))
    stack = _wait_stack(cfn, "cfn-iot-ca-pem")
    assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"

    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"]
    assert desc["certificatePem"] == pem_a

    cfn.delete_stack(StackName="cfn-iot-ca-pem")
    _wait_stack(cfn, "cfn-iot-ca-pem")


def test_cfn_deleted_stack_name_is_reusable(cfn):
    """A DELETE_COMPLETE stack is addressable only by stack ID; its name is free
    to re-create, and an UpdateStack against the deleted name is "does not
    exist" (which is what lets `aws cloudformation deploy` re-create it). (#1345)
    """
    tpl = json.dumps({
        "Resources": {
            "P": {"Type": "AWS::SSM::Parameter",
                  "Properties": {"Type": "String", "Value": "v1"}},
        },
    })
    name = "cfn-recreate-1345"
    first_id = cfn.create_stack(StackName=name, TemplateBody=tpl)["StackId"]
    assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"

    cfn.delete_stack(StackName=name)
    assert _wait_stack(cfn, name)["StackStatus"] == "DELETE_COMPLETE"

    # By name: gone. By unique stack ID: still visible as DELETE_COMPLETE.
    with pytest.raises(ClientError) as exc:
        cfn.describe_stacks(StackName=name)
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert "does not exist" in exc.value.response["Error"]["Message"]
    by_id = cfn.describe_stacks(StackName=first_id)["Stacks"][0]
    assert by_id["StackStatus"] == "DELETE_COMPLETE"

    # UpdateStack against the deleted name is "does not exist", not "cannot be updated".
    with pytest.raises(ClientError) as uexc:
        cfn.update_stack(StackName=name, TemplateBody=tpl)
    assert "does not exist" in uexc.value.response["Error"]["Message"]

    # The name re-creates cleanly as a brand-new stack (new stack ID).
    second_id = cfn.create_stack(StackName=name, TemplateBody=tpl)["StackId"]
    assert second_id != first_id
    assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
    cfn.delete_stack(StackName=name)
    _wait_stack(cfn, name)


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


def test_cfn_parameter_constraints_are_enforced(cfn):
    """AllowedPattern, MinLength, MaxLength, MinValue and MaxValue are checked
    before a stack exists, with CloudFormation's message (measured:
    ``Parameter 'P' must match pattern ^[a-z]+$``); a ConstraintDescription
    replaces the reason; a CommaDelimitedList is checked per member."""
    uid = _uuid_mod.uuid4().hex[:8]

    def template(params):
        return json.dumps({"Parameters": params, "Resources": {"P": {
            "Type": "AWS::SSM::Parameter",
            "Properties": {"Name": f"/cfn-constraints/{uid}", "Type": "String",
                           "Value": {"Ref": next(iter(params))}}}}})

    def refused(params, value, expected):
        name = f"cfn-constraints-{uid}-{_uuid_mod.uuid4().hex[:4]}"
        key = next(iter(params))
        with pytest.raises(ClientError) as exc:
            cfn.create_stack(StackName=name, TemplateBody=template(params),
                             Parameters=[{"ParameterKey": key, "ParameterValue": value}])
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == expected
        with pytest.raises(ClientError):
            cfn.describe_stacks(StackName=name)

    refused({"P": {"Type": "String", "AllowedPattern": "^[a-z]+$", "MinLength": "3",
                   "MaxLength": "3"}}, "ABC1", "Parameter 'P' must match pattern ^[a-z]+$")
    refused({"P": {"Type": "String", "AllowedPattern": "[a-z]+", "ConstraintDescription":
                   "must be lowercase letters"}}, "abc1", "Parameter 'P' must be lowercase letters")
    refused({"P": {"Type": "String", "MinLength": "3"}}, "ab",
            "Parameter 'P' must contain at least 3 characters")
    refused({"P": {"Type": "String", "MaxLength": "3"}}, "abcd",
            "Parameter 'P' must contain at most 3 characters")
    refused({"N": {"Type": "Number", "MinValue": "1", "MaxValue": "10"}}, "0",
            "Parameter 'N' must be a number not less than 1")
    refused({"N": {"Type": "Number", "MinValue": "1", "MaxValue": "10"}}, "11",
            "Parameter 'N' must be a number not greater than 10")
    refused({"L": {"Type": "CommaDelimitedList", "AllowedPattern": "[a-z]+"}}, "ab, c1",
            "Parameter 'L' must match pattern [a-z]+")
    refused({"L": {"Type": "List<Number>"}}, "1,x",
            "Parameter 'L' value '1,x' is not a valid List<Number>")
    refused({"L": {"Type": "List<Number>", "MinValue": "1"}}, "1,0",
            "Parameter 'L' must be a number not less than 1")
    refused({"P": {"Type": "String", "AllowedPattern": "["}}, "abc",
            "Parameter 'P' has an invalid AllowedPattern: unterminated character set at position 0")

    # A Default that satisfies the constraints and a valid override both pass.
    name = f"cfn-constraints-ok-{uid}"
    params = {"P": {"Type": "String", "Default": "abc", "AllowedPattern": "^[a-z]+$",
                    "MinLength": "1", "MaxLength": "5"}}
    cfn.create_stack(StackName=name, TemplateBody=template(params),
                     Parameters=[{"ParameterKey": "P", "ParameterValue": "hello"}])
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_unnamed_dynamodb_table_survives_unrelated_update(cfn, ddb, ssm):
    """A stack update must not touch an auto-named resource whose own
    properties didn't change — DynamoDB::Table has no update handler, so it
    falls back to calling create again on every update. That create wasn't
    idempotent: with no explicit TableName, it derived a fresh name every
    call, so any update of a stack containing an unnamed table silently
    created a second, empty table under a new name — and an unrelated
    resource referencing the table via Ref (real CloudFormation propagates
    that Ref's resolved value on every update) picked up that new, wrong
    identity the moment it was reprocessed."""
    def template(param_value_source, description="unrelated change forces this resource to be reprocessed"):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Table": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                        "BillingMode": "PAY_PER_REQUEST",
                    },
                },
                "Param": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/cfn-t02f/table-name",
                        "Type": "String",
                        "Value": param_value_source,
                        "Description": description,
                    },
                },
            },
        })

    cfn.create_stack(StackName="cfn-t02f", TemplateBody=template({"Ref": "Table"}))
    _wait_stack(cfn, "cfn-t02f")
    tables_before = set(ddb.list_tables()["TableNames"])
    table_name_before = ssm.get_parameter(Name="/cfn-t02f/table-name")["Parameter"]["Value"]
    assert table_name_before in tables_before

    # Table itself is untouched; only Param's Description changes (forcing
    # Param, not Table, to actually be reprocessed this update). An identical
    # template would be refused with "No updates are to be performed."
    cfn.update_stack(StackName="cfn-t02f", TemplateBody=template(
        {"Ref": "Table"}, description="the unrelated change, second edition"))
    stack = _wait_stack(cfn, "cfn-t02f")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    tables_after = set(ddb.list_tables()["TableNames"])
    table_name_after = ssm.get_parameter(Name="/cfn-t02f/table-name")["Parameter"]["Value"]
    # Assert on this stack's table only. `list_tables()` is global and all xdist
    # workers share one server, so any comparison of the whole set against a
    # snapshot taken before the update is racy in both directions: a concurrent
    # test creating a table breaks equality, and one deleting its own table
    # breaks a subset check. Neither says anything about the behaviour here.
    assert table_name_after == table_name_before, "the table was re-created under a new name"
    assert table_name_after in tables_after, "the stack's table did not survive the update"
    
def test_cfn_custom_named_table_replacement_refused_data_survives(cfn, ddb):
    """A stack update that requires replacing a custom-named DynamoDB table
    (changing a key attribute's type S->N) is refused exactly as real
    CloudFormation refuses it: the stack rolls back to UPDATE_ROLLBACK_COMPLETE
    with the "custom-named resource requires replacing" message, and the table
    and its data survive untouched instead of being silently replaced (#1433)."""
    def template(sk_type):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "T": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "TableName": "cfn_named_probe_1433",
                        "BillingMode": "PAY_PER_REQUEST",
                        "AttributeDefinitions": [
                            {"AttributeName": "pk", "AttributeType": "S"},
                            {"AttributeName": "sk", "AttributeType": sk_type},
                        ],
                        "KeySchema": [
                            {"AttributeName": "pk", "KeyType": "HASH"},
                            {"AttributeName": "sk", "KeyType": "RANGE"},
                        ],
                    },
                },
            },
        })

    cfn.create_stack(StackName="cfn-1433", TemplateBody=template("S"))
    _wait_stack(cfn, "cfn-1433")
    try:
        ddb.put_item(TableName="cfn_named_probe_1433",
                     Item={"pk": {"S": "row"}, "sk": {"S": "keep-me"}})

        # sk type S->N requires replacing the table; AWS refuses because the
        # table carries a custom name.
        cfn.update_stack(StackName="cfn-1433", TemplateBody=template("N"))
        stack = _wait_stack(cfn, "cfn-1433")
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"

        # Schema unchanged: sk is still S.
        attrs = ddb.describe_table(
            TableName="cfn_named_probe_1433")["Table"]["AttributeDefinitions"]
        sk = next(a for a in attrs if a["AttributeName"] == "sk")
        assert sk["AttributeType"] == "S"

        # The seeded row is intact.
        assert ddb.scan(TableName="cfn_named_probe_1433")["Count"] == 1

        # The refusal names the real AWS reason.
        events = cfn.describe_stack_events(
            StackName="cfn-1433")["StackEvents"]
        reasons = " ".join(
            e.get("ResourceStatusReason", "") for e in events)
        assert "custom-named resource requires replacing" in reasons
    finally:
        cfn.delete_stack(StackName="cfn-1433")


def test_cfn_change_set_status_execution_and_lifecycle(cfn):
    """Change set Status stays CREATE_COMPLETE after execution (not
    EXECUTE_COMPLETE, which broke the CDK), a failed execution reports
    EXECUTE_FAILED, a duplicate name is AlreadyExistsException, and a change set
    does not outlive its stack (#1418)."""
    S = "cfn-1418"
    CS = "cdk-deploy-change-set"
    # Unrecognized types are rejected at CreateChangeSet time now (like AWS),
    # so a custom resource whose Lambda does not exist provides the
    # execution-time failure this test is about.
    bad = json.dumps({"Resources": {"X": _FAILING_RESOURCE}})
    ok = json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter",
        "Properties": {"Name": "/cfn-1418/p", "Type": "String",
                       "Value": "v"}}}})

    cfn.create_change_set(StackName=S, ChangeSetName=CS,
                          ChangeSetType="CREATE", TemplateBody=bad)
    d = cfn.describe_change_set(ChangeSetName=CS, StackName=S)
    assert d["Status"] == "CREATE_COMPLETE"
    assert d["ExecutionStatus"] == "AVAILABLE"

    # A duplicate name while the change set exists -> AlreadyExistsException.
    with pytest.raises(ClientError) as exc:
        cfn.create_change_set(StackName=S, ChangeSetName=CS,
                              ChangeSetType="CREATE", TemplateBody=ok)
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"

    # Execute: the bad template fails and rolls back.
    cfn.execute_change_set(StackName=S, ChangeSetName=CS)
    deadline = time.time() + 30
    while time.time() < deadline:
        d = cfn.describe_change_set(ChangeSetName=CS, StackName=S)
        if d["ExecutionStatus"] in ("EXECUTE_COMPLETE", "EXECUTE_FAILED"):
            break
        time.sleep(0.2)
    assert d["Status"] == "CREATE_COMPLETE"          # not EXECUTE_COMPLETE
    assert d["ExecutionStatus"] == "EXECUTE_FAILED"  # execution actually failed

    # Delete the failed stack -> its change set goes with it.
    cfn.delete_stack(StackName=S)
    with pytest.raises(ClientError) as exc:
        cfn.describe_change_set(ChangeSetName=CS, StackName=S)
    assert "ChangeSetNotFound" in exc.value.response["Error"]["Code"]

    # Once the stack is gone, the same name resolves to a fresh change set.
    _wait_stack(cfn, S)
    cfn.create_change_set(StackName=S, ChangeSetName=CS,
                          ChangeSetType="CREATE", TemplateBody=ok)
    d = cfn.describe_change_set(ChangeSetName=CS, StackName=S)
    assert d["Status"] == "CREATE_COMPLETE"
    assert d["ExecutionStatus"] == "AVAILABLE"
    cfn.delete_stack(StackName=S)


def test_cfn_change_set_no_changes_is_failed(cfn, ssm):
    """A change set with no changes ends FAILED with the real-AWS reason, not
    CREATE_COMPLETE/AVAILABLE (#1418)."""
    S = "cfn-1418-nochg"
    tpl = json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter",
        "Properties": {"Name": "/cfn-1418-nochg/p", "Type": "String",
                       "Value": "v"}}}})
    cfn.create_stack(StackName=S, TemplateBody=tpl)
    _wait_stack(cfn, S)
    try:
        cfn.create_change_set(StackName=S, ChangeSetName="noop",
                              ChangeSetType="UPDATE", TemplateBody=tpl)
        d = cfn.describe_change_set(ChangeSetName="noop", StackName=S)
        assert d["Status"] == "FAILED"
        assert d["ExecutionStatus"] == "UNAVAILABLE"
        assert "didn't contain changes" in d["StatusReason"]
    finally:
        cfn.delete_stack(StackName=S)


def test_cfn_change_set_sees_policy_and_metadata_changes(cfn, sqs):
    """A change set lists a resource whose DeletionPolicy, UpdateReplacePolicy
    or Metadata changed and nothing else: Modify, Replacement False, the
    attribute in Scope and Details, and the executed set stores the new
    template. A DependsOn-only edit is not a change, as on AWS."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cs-attrs-{uid}"

    def template(policy=None, depends=False, metadata=None, queue_name=None):
        queue = {"Type": "AWS::SQS::Queue",
                 "Properties": {"QueueName": queue_name or f"cfn-cs-attrs-{uid}"}}
        if policy:
            queue["DeletionPolicy"] = policy
            queue["UpdateReplacePolicy"] = policy
        if metadata:
            queue["Metadata"] = metadata
        param = {"Type": "AWS::SSM::Parameter",
                 "Properties": {"Name": f"/cfn-cs-attrs/{uid}", "Type": "String",
                                "Value": "v"}}
        if depends:
            param["DependsOn"] = "Queue"
        return json.dumps({"Resources": {"Queue": queue, "Param": param}})

    def changes_of(name, body):
        cfn.create_change_set(StackName=stack_name, ChangeSetName=name,
                              ChangeSetType="UPDATE", TemplateBody=body)
        return cfn.describe_change_set(ChangeSetName=name, StackName=stack_name)

    cfn.create_stack(StackName=stack_name, TemplateBody=template())
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        described = changes_of("depends-only", template(depends=True))
        assert described["Status"] == "FAILED"
        assert "didn't contain changes" in described["StatusReason"]

        described = changes_of("retain", template(policy="Retain", depends=True))
        assert described["Status"] == "CREATE_COMPLETE", described.get("StatusReason")
        assert described["ExecutionStatus"] == "AVAILABLE"
        assert [c["ResourceChange"]["LogicalResourceId"] for c in described["Changes"]] == ["Queue"]
        change = described["Changes"][0]["ResourceChange"]
        assert change["Action"] == "Modify"
        assert change["Replacement"] == "False"
        assert sorted(change["Scope"]) == ["DeletionPolicy", "UpdateReplacePolicy"]
        assert sorted(d["Target"]["Attribute"] for d in change["Details"]) == [
            "DeletionPolicy", "UpdateReplacePolicy"]

        cfn.execute_change_set(ChangeSetName="retain", StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        body = cfn.get_template(StackName=stack_name)["TemplateBody"]
        stored = json.loads(body) if isinstance(body, str) else body
        assert stored["Resources"]["Queue"]["DeletionPolicy"] == "Retain"

        described = changes_of("metadata", template(policy="Retain", depends=True,
                                                    metadata={"owner": "fleet"}))
        change = described["Changes"][0]["ResourceChange"]
        assert change["Replacement"] == "False"
        assert change["Scope"] == ["Metadata"]

        described = changes_of("rename", template(policy="Retain", depends=True,
                                                  queue_name=f"cfn-cs-attrs-{uid}-b"))
        change = described["Changes"][0]["ResourceChange"]
        assert change["Scope"] == ["Properties"]
        assert [d["Target"]["Name"] for d in change["Details"]] == ["QueueName"]
        assert change["Details"][0]["Target"]["RequiresRecreation"] == "Conditionally"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_execute_change_set_deletes_sibling_change_sets(cfn, ssm):
    """Executing a change set deletes the stack's other change sets — they are
    no longer valid for the updated stack (#1418)."""
    S = "cfn-1418-sib"
    def tpl(v):
        return json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter",
            "Properties": {"Name": "/cfn-1418-sib/p", "Type": "String",
                           "Value": v}}}})
    cfn.create_stack(StackName=S, TemplateBody=tpl("v0"))
    _wait_stack(cfn, S)
    try:
        cfn.create_change_set(StackName=S, ChangeSetName="cs-a",
                              ChangeSetType="UPDATE", TemplateBody=tpl("v1"))
        cfn.create_change_set(StackName=S, ChangeSetName="cs-b",
                              ChangeSetType="UPDATE", TemplateBody=tpl("v2"))
        cfn.execute_change_set(StackName=S, ChangeSetName="cs-a")
        with pytest.raises(ClientError) as exc:
            cfn.describe_change_set(ChangeSetName="cs-b", StackName=S)
        assert "ChangeSetNotFound" in exc.value.response["Error"]["Code"]
        _wait_stack(cfn, S)
    finally:
        cfn.delete_stack(StackName=S)


def test_cfn_direct_update_marks_pending_change_sets_obsolete(cfn, ssm):
    """A direct UpdateStack supersedes any pending change set — it becomes
    OBSOLETE, not left AVAILABLE (#1418)."""
    S = "cfn-1418-obs"
    def tpl(v):
        return json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter",
            "Properties": {"Name": "/cfn-1418-obs/p", "Type": "String",
                           "Value": v}}}})
    cfn.create_stack(StackName=S, TemplateBody=tpl("v0"))
    _wait_stack(cfn, S)
    try:
        cfn.create_change_set(StackName=S, ChangeSetName="pending",
                              ChangeSetType="UPDATE", TemplateBody=tpl("v1"))
        assert cfn.describe_change_set(
            ChangeSetName="pending", StackName=S)["ExecutionStatus"] == "AVAILABLE"
        # A direct update (not via the change set) supersedes it.
        cfn.update_stack(StackName=S, TemplateBody=tpl("v2"))
        d = cfn.describe_change_set(ChangeSetName="pending", StackName=S)
        assert d["ExecutionStatus"] == "OBSOLETE"
        _wait_stack(cfn, S)
    finally:
        cfn.delete_stack(StackName=S)


def test_cfn_ssm_parameter_value_type_resolves_stored_value(cfn, ssm, sqs):
    """A `AWS::SSM::Parameter::Value<String>` template parameter's Default/
    provided value is an SSM parameter *name*, not the value itself — real
    CloudFormation resolves it against SSM Parameter Store before `Ref` ever
    sees it (the mechanism behind CDK's `StringParameter.valueForStringParameter`).
    Ref must yield the stored value, not the parameter name."""
    ssm.put_parameter(Name="/cfn-t02c/queue-name", Value="cfn-t02c-resolved", Type="String")

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "AWS::SSM::Parameter::Value<String>",
                "Default": "/cfn-t02c/queue-name",
            }
        },
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            }
        },
    }
    cfn.create_stack(StackName="cfn-t02c", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t02c")

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02c-resolved").get("QueueUrls", [])
    assert any("cfn-t02c-resolved" in u for u in urls)
    # Never the literal parameter name — that would mean resolution silently
    # fell back to treating the SSM path as the value itself.
    urls_by_name = sqs.list_queues(QueueNamePrefix="cfn-t02c/queue-name").get("QueueUrls", [])
    assert urls_by_name == []


def test_cfn_ssm_parameter_value_type_use_previous_value_on_update(cfn, ssm, sqs):
    """A change set that re-sends an AWS::SSM::Parameter::Value<String>
    parameter as UsePreviousValue (the `aws cloudformation deploy`
    no-`--parameter-overrides` path, and what CDK sends for parameters it
    isn't touching, e.g. its own BootstrapVersion) must reuse the
    already-resolved value from the prior deployment as-is — not feed that
    resolved value back into another SSM lookup, where it would almost
    never itself be a valid parameter name and the update would fail."""
    ssm.put_parameter(Name="/cfn-t02e/queue-name", Value="cfn-t02e-resolved", Type="String")

    def template(bucket_tag):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Parameters": {
                "QueueName": {
                    "Type": "AWS::SSM::Parameter::Value<String>",
                    "Default": "/cfn-t02e/queue-name",
                },
                "Tag": {"Type": "String", "Default": bucket_tag},
            },
            "Resources": {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Ref": "QueueName"},
                        "Tags": [{"Key": "build", "Value": {"Ref": "Tag"}}],
                    },
                }
            },
        })

    cfn.create_stack(StackName="cfn-t02e", TemplateBody=template("v1"))
    _wait_stack(cfn, "cfn-t02e")

    # Second deploy changes an unrelated parameter (Tag) and re-sends
    # QueueName as UsePreviousValue, exactly as CDK does for parameters it
    # isn't updating this deploy.
    cfn.create_change_set(
        StackName="cfn-t02e", ChangeSetName="cs2", TemplateBody=template("v2"),
        Parameters=[{"ParameterKey": "QueueName", "UsePreviousValue": True}],
    )
    cfn.execute_change_set(StackName="cfn-t02e", ChangeSetName="cs2")
    stack = _wait_stack(cfn, "cfn-t02e")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02e-resolved").get("QueueUrls", [])
    assert any("cfn-t02e-resolved" in u for u in urls)


def test_cfn_ssm_parameter_value_type_missing_parameter_fails_stack(cfn):
    """An `AWS::SSM::Parameter::Value<String>` naming a parameter that was
    never put must fail the same way real CloudFormation does — a
    synchronous ValidationError at CreateStack time (parameter resolution
    happens before the stack is created), not a silent resolve to the
    parameter's own name string."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "AWS::SSM::Parameter::Value<String>",
                "Default": "/cfn-t02d/does-not-exist",
            }
        },
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            }
        },
    }
    with pytest.raises(ClientError) as exc_info:
        cfn.create_stack(StackName="cfn-t02d", TemplateBody=json.dumps(template))
    assert exc_info.value.response["Error"]["Code"] == "ValidationError"


def _ssm_value(ssm, name):
    return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]


def test_cfn_dynamic_references_resolve_and_follow_the_update_rules(cfn, ssm, sm):
    """{{resolve:ssm}}, {{resolve:ssm-secure}} and {{resolve:secretsmanager}}
    resolve at provisioning time; GetTemplate keeps the literal. On update an
    identical template is accepted but changes nothing, UsePreviousTemplate is
    refused, a changed template re-resolves the ssm references (a pinned
    version stays), and a secretsmanager reference re-resolves only when its
    resource changes (measured on AWS)."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-dynref-{uid}"
    src, secure, secret, json_secret = (
        f"/cfn-dynref/{uid}/in", f"/cfn-dynref/{uid}/secure",
        f"cfn-dynref-{uid}-plain", f"cfn-dynref-{uid}-json")
    ssm.put_parameter(Name=src, Value="v1", Type="String")
    ssm.put_parameter(Name=secure, Value="hush", Type="SecureString")
    sm.create_secret(Name=secret, SecretString="s1")
    sm.create_secret(Name=json_secret, SecretString=json.dumps({"password": "p1"}))
    outs = {k: f"/cfn-dynref/{uid}/out-{k}" for k in ("ssm", "pinned", "secure", "secret", "key")}

    def param(name, value, description=None):
        props = {"Name": name, "Type": "String", "Value": value}
        if description:
            props["Description"] = description
        return {"Type": "AWS::SSM::Parameter", "Properties": props}

    def template(description="one", secret_description=None):
        return json.dumps({"Description": description, "Resources": {
            "FromSsm": param(outs["ssm"], f"{{{{resolve:ssm:{src}}}}}"),
            "Pinned": param(outs["pinned"], f"{{{{resolve:ssm:{src}:1}}}}"),
            "Secure": param(outs["secure"], f"{{{{resolve:ssm-secure:{secure}}}}}"),
            "FromSecret": param(outs["secret"], f"prefix-{{{{resolve:secretsmanager:{secret}}}}}",
                                secret_description),
            "FromKey": param(outs["key"],
                             f"{{{{resolve:secretsmanager:{json_secret}:SecretString:password}}}}"),
        }})

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template())
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert _ssm_value(ssm, outs["ssm"]) == "v1"
        assert _ssm_value(ssm, outs["pinned"]) == "v1"
        assert _ssm_value(ssm, outs["secure"]) == "hush"
        assert _ssm_value(ssm, outs["secret"]) == "prefix-s1"
        assert _ssm_value(ssm, outs["key"]) == "p1"
        body = cfn.get_template(StackName=stack_name)["TemplateBody"]
        body = json.dumps(body) if not isinstance(body, str) else body
        assert f"{{{{resolve:ssm:{src}}}}}" in body

        ssm.put_parameter(Name=src, Value="v2", Type="String", Overwrite=True)
        sm.put_secret_value(SecretId=secret, SecretString="s2")

        # identical template: accepted, nothing re-resolved
        cfn.update_stack(StackName=stack_name, TemplateBody=template())
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _ssm_value(ssm, outs["ssm"]) == "v1"
        assert _ssm_value(ssm, outs["secret"]) == "prefix-s1"

        with pytest.raises(ClientError) as exc:
            cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True)
        assert "No updates are to be performed" in exc.value.response["Error"]["Message"]

        # a changed template: ssm re-resolves, the pinned version and the secret stay
        cfn.update_stack(StackName=stack_name, TemplateBody=template(description="two"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _ssm_value(ssm, outs["ssm"]) == "v2"
        assert _ssm_value(ssm, outs["pinned"]) == "v1"
        assert _ssm_value(ssm, outs["secret"]) == "prefix-s1"

        # the resource carrying the secret changes: the secret re-resolves
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=template(description="two", secret_description="touched"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _ssm_value(ssm, outs["secret"]) == "prefix-s2"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for name in (src, secure):
            try:
                ssm.delete_parameter(Name=name)
            except ClientError:
                pass
        for name in (secret, json_secret):
            try:
                sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
            except ClientError:
                pass


def test_cfn_secretsmanager_reference_by_arn_version_id_and_json_number(cfn, ssm, sm):
    """The secret id may be an ARN (its colons do not split the segments), a
    version-stage or a version-id selects an older version, and a JSON key
    whose value is not a string comes back as its JSON text."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-dynref-arn-{uid}"
    secret = f"cfn-dynref-arn-{uid}"
    created = sm.create_secret(Name=secret, SecretString=json.dumps({"port": 5432, "host": "old"}))
    arn, first_version = created["ARN"], created["VersionId"]
    sm.put_secret_value(SecretId=secret, SecretString=json.dumps({"port": 5433, "host": "new"}))
    outs = {k: f"/cfn-dynref-arn/{uid}/{k}" for k in ("arn", "previous", "version")}

    def param(name, value):
        return {"Type": "AWS::SSM::Parameter",
                "Properties": {"Name": name, "Type": "String", "Value": value}}

    template = json.dumps({"Resources": {
        "ByArn": param(outs["arn"], f"{{{{resolve:secretsmanager:{arn}:SecretString:port}}}}"),
        "Previous": param(outs["previous"],
                          f"{{{{resolve:secretsmanager:{secret}:SecretString:host:AWSPREVIOUS}}}}"),
        "ByVersion": param(outs["version"],
                           f"{{{{resolve:secretsmanager:{secret}:SecretString:host::{first_version}}}}}"),
    }})
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert _ssm_value(ssm, outs["arn"]) == "5433"
        assert _ssm_value(ssm, outs["previous"]) == "old"
        assert _ssm_value(ssm, outs["version"]) == "old"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            sm.delete_secret(SecretId=secret, ForceDeleteWithoutRecovery=True)
        except ClientError:
            pass


def test_cfn_dynamic_reference_that_cannot_resolve_fails_the_resource(cfn, ssm):
    """A reference to a missing parameter fails the resource and rolls the
    stack back with the reason."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-dynref-bad-{uid}"
    template = json.dumps({"Resources": {"P": {
        "Type": "AWS::SSM::Parameter",
        "Properties": {"Name": f"/cfn-dynref-bad/{uid}", "Type": "String",
                       "Value": f"{{{{resolve:ssm:/cfn-dynref-bad/{uid}/missing}}}}"}}}})
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert f"/cfn-dynref-bad/{uid}/missing" in _stack_event_reasons(cfn, stack_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


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

    exports = _all_pages(cfn, "list_exports", "Exports")
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


def test_cfn_fn_sub_literal_escape(cfn, ssm):
    """`${!Literal}` is Fn::Sub's escape for emitting `${Literal}` verbatim —
    the way an IoT policy carries `${iot:Connection.Thing.ThingName}` through a
    template. The name must not be looked up, even when it matches a resource,
    and the surrounding substitutions must still resolve."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Plain": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-sub-escape-src"},
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "/cfn-sub-escape/literal",
                    "Type": "String",
                    "Value": {
                        "Fn::Sub": "client/${!iot:Connection.Thing.ThingName} "
                                   "and ${!Plain} in ${AWS::Region}"
                    },
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-sub-escape", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-sub-escape")

    val = ssm.get_parameter(Name="/cfn-sub-escape/literal")["Parameter"]["Value"]
    assert val == ("client/${iot:Connection.Thing.ThingName} "
                   "and ${Plain} in us-east-1")


def test_cfn_intrinsics_cidr_getazs_findinmap_and_condition_functions(cfn, ssm):
    """Fn::Cidr splits the block it is given (measured: 192.168.0.0/16, 2, 8 is
    192.168.0.0/24,192.168.1.0/24), Fn::GetAZs answers the stack's zones for
    its own region and an empty list for another (measured), Fn::FindInMap
    takes a DefaultValue and a missing key without one is a template error
    (measured), a condition function as an Output value is refused up front
    (measured) and in a property position evaluates to a boolean."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-intrinsics-{uid}"
    region = cfn.meta.region_name
    template = json.dumps({
        "Mappings": {"M": {"a": {"b": "ok"}}},
        "Resources": {
            "P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-intrinsics/{uid}", "Type": "String",
                "Value": {"Fn::Join": [",", {"Fn::Cidr": ["192.168.0.0/16", "2", "8"]}]}}},
            "Bools": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-intrinsics/{uid}/bools", "Type": "String",
                "Value": {"Fn::Join": [",", [
                    {"Fn::Not": [{"Fn::Equals": ["a", "b"]}]},
                    {"Fn::Or": [{"Fn::Equals": ["a", "b"]}, {"Fn::Equals": ["b", "b"]}]},
                    {"Fn::And": [{"Fn::Equals": ["a", "a"]}, {"Fn::Equals": ["a", "b"]}]}]]}}},
        },
        "Outputs": {
            "Cidr": {"Value": {"Fn::Join": [",", {"Fn::Cidr": ["10.0.0.0/24", 6, 5]}]}},
            "OwnAzs": {"Value": {"Fn::Join": [",", {"Fn::GetAZs": ""}]}},
            "RefAzs": {"Value": {"Fn::Join": [",", {"Fn::GetAZs": {"Ref": "AWS::Region"}}]}},
            "OtherAzs": {"Value": {"Fn::Join": [",", {"Fn::GetAZs": "eu-north-1"}]}},
            "Map": {"Value": {"Fn::FindInMap": ["M", "a", "b"]}},
            "MapDefault": {"Value": {"Fn::FindInMap": ["M", "x", "y", {"DefaultValue": "dflt"}]}},
            "AndInIf": {"Value": {"Fn::If": ["Yes", "yes", "no"]}},
        },
        "Conditions": {"Yes": {"Fn::And": [{"Fn::Equals": ["a", "a"]}, {"Fn::Not": [{"Fn::Equals": ["a", "b"]}]}]}},
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
        assert outputs["Cidr"] == ",".join(f"10.0.0.{32 * i}/27" for i in range(6))
        assert outputs["OwnAzs"] == f"{region}a,{region}b,{region}c"
        assert outputs["RefAzs"] == outputs["OwnAzs"]
        assert outputs["OtherAzs"] == ""
        assert outputs["Map"] == "ok"
        assert outputs["MapDefault"] == "dflt"
        assert outputs["AndInIf"] == "yes"
        assert ssm.get_parameter(Name=f"/cfn-intrinsics/{uid}")["Parameter"]["Value"] == (
            "192.168.0.0/24,192.168.1.0/24")
        # In a property position the condition functions evaluate to booleans
        # (joined as their string form), never to the Python repr of the call.
        assert ssm.get_parameter(Name=f"/cfn-intrinsics/{uid}/bools")["Parameter"]["Value"] == (
            "True,True,False")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)

    # An impossible Fn::Cidr fails the resource with the reason.
    bad_cidr = f"cfn-intrinsics-cidr-{uid}"
    cfn.create_stack(StackName=bad_cidr, TemplateBody=json.dumps({"Resources": {
        "P": {"Type": "AWS::SSM::Parameter", "Properties": {
            "Name": f"/cfn-intrinsics/{uid}/cidr", "Type": "String",
            "Value": {"Fn::Select": [0, {"Fn::Cidr": ["10.0.0.0/24", 300, 8]}]}}}}}))
    try:
        stack = _wait_stack(cfn, bad_cidr)
        assert stack["StackStatus"] == "ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "Fn::Cidr count must be between 1 and 256" in _stack_event_reasons(cfn, bad_cidr)
    finally:
        _delete_cfn_test_stack(cfn, bad_cidr)

    def refused(body, message):
        name = f"cfn-intrinsics-bad-{_uuid_mod.uuid4().hex[:6]}"
        with pytest.raises(ClientError) as exc:
            cfn.create_stack(StackName=name, TemplateBody=json.dumps(body))
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == message
        with pytest.raises(ClientError):
            cfn.describe_stacks(StackName=name)

    queue = {"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}
    refused({**queue, "Mappings": {"M": {"a": {"b": "ok"}}},
             "Outputs": {"Map": {"Value": {"Fn::FindInMap": ["M", "x", "y"]}}}},
            "Template error: Unable to get mapping for M::x::y")
    refused({**queue, "Outputs": {"And": {"Value": {"Fn::And": [
                {"Fn::Equals": ["a", "a"]}, {"Fn::Equals": ["a", "b"]}]}}}},
            "Template format error: The Value field of every Outputs member must evaluate to a String.")


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


# --- Capabilities are enforced under AUTH=true (in-process, no server) ---
#
# API_CreateStack / API_UpdateStack / API_CreateChangeSet, "Capabilities":
# IAM resources need CAPABILITY_IAM or CAPABILITY_NAMED_IAM, custom-named
# ones CAPABILITY_NAMED_IAM, a template with macros CAPABILITY_AUTO_EXPAND on
# CreateStack / UpdateStack ("doesn't apply to creating change sets");
# otherwise the InsufficientCapabilities error, HTTP 400. Measured on AWS for
# an unnamed role: "Requires capabilities : [CAPABILITY_IAM]", no stack.

_CAPS_ROLE_TEMPLATE = {
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
_CAPS_NAMED_ROLE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "Role": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": "cfn-caps-named-role",
                "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
            },
        }
    },
}
# A SAM template the transform accepts. The transform generates an unnamed
# execution role for the function, so a handler needs CAPABILITY_IAM for the
# transformed template and CAPABILITY_AUTO_EXPAND for the macro. The helper
# tests below pass this template untransformed, where only the macro counts.
_CAPS_TRANSFORM_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Transform": "AWS::Serverless-2016-10-31",
    "Resources": {
        "Fn": {
            "Type": "AWS::Serverless::Function",
            "Properties": {
                "Handler": "index.handler",
                "Runtime": "python3.12",
                "InlineCode": "def handler(event, context): return None",
            },
        },
    },
}
_CAPS_BUCKET_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {"Bucket": {"Type": "AWS::S3::Bucket"}},
}
# PolicyName is a required property of AWS::IAM::Policy, not a custom name
# (aws-resource-iam-policy.html): CAPABILITY_IAM is enough.
_CAPS_POLICY_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "Policy": {
            "Type": "AWS::IAM::Policy",
            "Properties": {
                "PolicyName": "cfn-caps-inline",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": []},
                "Roles": ["cfn-caps-some-role"],
            },
        }
    },
}
# Not one of the eight documented types: no capability needed.
_CAPS_SERVICE_LINKED_ROLE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "Slr": {
            "Type": "AWS::IAM::ServiceLinkedRole",
            "Properties": {"AWSServiceName": "autoscaling.amazonaws.com"},
        }
    },
}
_CAPS_NULL_PROPS_ROLE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {"Role": {"Type": "AWS::IAM::Role", "Properties": None}},
}


def _caps_params(template, *capabilities, **extra):
    """A parsed query-protocol request the way ``handle_request`` builds it."""
    params = {"TemplateBody": [json.dumps(template)]}
    for i, cap in enumerate(capabilities, 1):
        params[f"Capabilities.member.{i}"] = [cap]
    for key, value in extra.items():
        params[key] = [value]
    return params


def _caps_error(response):
    """``(status, code, message)`` of an in-process CloudFormation response."""
    status, _headers, body = response
    text = body.decode("utf-8")
    code = re.search(r"<Code>([^<]*)</Code>", text)
    message = re.search(r"<Message>([^<]*)</Message>", text)
    return status, code.group(1) if code else "", message.group(1) if message else ""


def test_cfn_required_capabilities_helper():
    from ministack.services.cloudformation.handlers import _required_capabilities

    assert _required_capabilities(_CAPS_BUCKET_TEMPLATE) == ([], [])
    assert _required_capabilities(_CAPS_ROLE_TEMPLATE) == (
        ["CAPABILITY_IAM"], ["AWS::IAM::Role"])
    assert _required_capabilities(_CAPS_NAMED_ROLE_TEMPLATE) == (
        ["CAPABILITY_NAMED_IAM"], ["AWS::IAM::Role"])
    assert _required_capabilities(_CAPS_TRANSFORM_TEMPLATE) == (
        ["CAPABILITY_AUTO_EXPAND"], [])
    both = dict(_CAPS_NAMED_ROLE_TEMPLATE, Transform="AWS::LanguageExtensions")
    assert _required_capabilities(both) == (
        ["CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"], ["AWS::IAM::Role"])
    assert _required_capabilities(_CAPS_NULL_PROPS_ROLE_TEMPLATE) == (
        ["CAPABILITY_IAM"], ["AWS::IAM::Role"])

    # The summary's rule stays as it was: PolicyName counts as a name and every
    # AWS::IAM::* type needs a capability. The documented rule the enforcement
    # uses (strict=True) differs on both.
    assert _required_capabilities(_CAPS_POLICY_TEMPLATE) == (
        ["CAPABILITY_NAMED_IAM"], ["AWS::IAM::Policy"])
    assert _required_capabilities(_CAPS_POLICY_TEMPLATE, strict=True) == (
        ["CAPABILITY_IAM"], ["AWS::IAM::Policy"])
    assert _required_capabilities(_CAPS_SERVICE_LINKED_ROLE_TEMPLATE) == (
        ["CAPABILITY_IAM"], ["AWS::IAM::ServiceLinkedRole"])
    assert _required_capabilities(_CAPS_SERVICE_LINKED_ROLE_TEMPLATE, strict=True) == ([], [])
    assert _required_capabilities(_CAPS_NAMED_ROLE_TEMPLATE, strict=True) == (
        ["CAPABILITY_NAMED_IAM"], ["AWS::IAM::Role"])


def test_cfn_capabilities_not_enforced_without_auth(monkeypatch):
    import ministack.app as app_mod
    from ministack.services.cloudformation import _change_sets, _stack_events, _stacks
    from ministack.services.cloudformation.changesets import _create_change_set
    from ministack.services.cloudformation.handlers import _check_capabilities

    monkeypatch.setattr(app_mod, "AUTH", False)
    # The check takes the template as sent and the transformed one; these
    # templates are passed untransformed, so both arguments are the same.
    assert _check_capabilities(
        _CAPS_ROLE_TEMPLATE, _CAPS_ROLE_TEMPLATE, _caps_params(_CAPS_ROLE_TEMPLATE)) is None
    assert _check_capabilities(
        _CAPS_NAMED_ROLE_TEMPLATE, _CAPS_NAMED_ROLE_TEMPLATE,
        _caps_params(_CAPS_NAMED_ROLE_TEMPLATE)) is None
    assert _check_capabilities(
        _CAPS_TRANSFORM_TEMPLATE, _CAPS_TRANSFORM_TEMPLATE,
        _caps_params(_CAPS_TRANSFORM_TEMPLATE)) is None

    # A handler accepts the IAM template without any capability, as before.
    stack_name = "cfn-caps-noauth-cs"
    try:
        response = _create_change_set(_caps_params(
            _CAPS_ROLE_TEMPLATE, StackName=stack_name,
            ChangeSetName="cs1", ChangeSetType="CREATE"))
        status, code, _ = _caps_error(response)
        assert status == 200, code
        assert stack_name in _stacks
    finally:
        stack = _stacks.pop(stack_name, None)
        if stack:
            _stack_events.pop(stack["StackId"], None)
        for cs_id in [k for k, v in _change_sets.items()
                      if v.get("StackName") == stack_name]:
            _change_sets.pop(cs_id, None)


def test_cfn_capabilities_check_under_auth(monkeypatch):
    import ministack.app as app_mod
    from ministack.services.cloudformation.handlers import _check_capabilities

    monkeypatch.setattr(app_mod, "AUTH", True)

    def check(template, *caps, **kw):
        # Untransformed: the template as sent and the transformed one are equal.
        response = _check_capabilities(
            template, template, _caps_params(template, *caps), **kw)
        return None if response is None else _caps_error(response)

    refused = (400, "InsufficientCapabilitiesException",
               "Requires capabilities : [CAPABILITY_IAM]")
    assert check(_CAPS_BUCKET_TEMPLATE) is None
    assert check(_CAPS_ROLE_TEMPLATE) == refused
    assert check(_CAPS_ROLE_TEMPLATE, "CAPABILITY_AUTO_EXPAND") == refused
    # "If you have IAM resources, you can specify either capability."
    assert check(_CAPS_ROLE_TEMPLATE, "CAPABILITY_IAM") is None
    assert check(_CAPS_ROLE_TEMPLATE, "CAPABILITY_NAMED_IAM") is None
    # "If you have IAM resources with custom names, you must specify
    # CAPABILITY_NAMED_IAM."
    assert check(_CAPS_NAMED_ROLE_TEMPLATE, "CAPABILITY_IAM") == (
        400, "InsufficientCapabilitiesException",
        "Requires capabilities : [CAPABILITY_NAMED_IAM]")
    assert check(_CAPS_NAMED_ROLE_TEMPLATE, "CAPABILITY_NAMED_IAM") is None
    assert check(_CAPS_NAMED_ROLE_TEMPLATE, "CAPABILITY_IAM", "CAPABILITY_NAMED_IAM") is None
    # A macro template needs CAPABILITY_AUTO_EXPAND on CreateStack / UpdateStack
    # but not on CreateChangeSet.
    assert check(_CAPS_TRANSFORM_TEMPLATE) == (
        400, "InsufficientCapabilitiesException",
        "Requires capabilities : [CAPABILITY_AUTO_EXPAND]")
    assert check(_CAPS_TRANSFORM_TEMPLATE, "CAPABILITY_AUTO_EXPAND") is None
    assert check(_CAPS_TRANSFORM_TEMPLATE, macros=False) is None
    both = dict(_CAPS_ROLE_TEMPLATE, Transform="AWS::LanguageExtensions")
    assert check(both, "CAPABILITY_IAM") == (
        400, "InsufficientCapabilitiesException",
        "Requires capabilities : [CAPABILITY_AUTO_EXPAND]")
    assert check(both, "CAPABILITY_AUTO_EXPAND") == refused
    assert check(both, "CAPABILITY_AUTO_EXPAND", "CAPABILITY_IAM") is None
    # An AWS::IAM::Policy needs CAPABILITY_IAM only; a type outside the
    # documented eight needs nothing; a null Properties is an unnamed resource.
    assert check(_CAPS_POLICY_TEMPLATE) == refused
    assert check(_CAPS_POLICY_TEMPLATE, "CAPABILITY_IAM") is None
    assert check(_CAPS_SERVICE_LINKED_ROLE_TEMPLATE) is None
    assert check(_CAPS_NULL_PROPS_ROLE_TEMPLATE) == refused


def test_cfn_create_stack_refuses_missing_capabilities_under_auth(monkeypatch):
    import ministack.app as app_mod
    from ministack.services.cloudformation import _stack_events, _stacks
    from ministack.services.cloudformation.handlers import _create_stack

    monkeypatch.setattr(app_mod, "AUTH", True)
    stack_name = "cfn-caps-auth-create"

    try:
        response = _create_stack(_caps_params(_CAPS_ROLE_TEMPLATE, StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        assert stack_name not in _stacks

        response = _create_stack(_caps_params(
            _CAPS_NAMED_ROLE_TEMPLATE, "CAPABILITY_IAM", StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_NAMED_IAM]")
        assert stack_name not in _stacks

        # Two capabilities missing at once. The macro rule reads the template
        # as sent, whose Transform the SAM transform drops; the IAM rule reads
        # the transformed template, which carries the generated function role.
        pytest.importorskip("samtranslator")
        response = _create_stack(_caps_params(_CAPS_TRANSFORM_TEMPLATE, StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM, CAPABILITY_AUTO_EXPAND]")
        assert stack_name not in _stacks

        response = _create_stack(_caps_params(
            _CAPS_TRANSFORM_TEMPLATE, "CAPABILITY_AUTO_EXPAND", StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        assert stack_name not in _stacks

        response = _create_stack(_caps_params(
            _CAPS_NULL_PROPS_ROLE_TEMPLATE, StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        assert stack_name not in _stacks
    finally:
        stack = _stacks.pop(stack_name, None)
        if stack:
            _stack_events.pop(stack["StackId"], None)


def test_cfn_embedded_fn_transform_needs_auto_expand_under_auth(monkeypatch):
    """A macro called on part of the template (``Fn::Transform``, e.g.
    ``AWS::Include``) has no top-level ``Transform`` but is a macro all the
    same, so CreateStack needs CAPABILITY_AUTO_EXPAND for it. The summary's
    rule stays top-level only."""
    import ministack.app as app_mod
    from ministack.services.cloudformation import _stacks
    from ministack.services.cloudformation.handlers import (
        _check_capabilities,
        _create_stack,
        _required_capabilities,
    )

    embedded = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "Fn::Transform": {
                        "Name": "AWS::Include",
                        "Parameters": {"Location": "s3://example.local/bucket-props.yaml"},
                    },
                },
            },
        },
    }
    assert _required_capabilities(embedded) == ([], [])
    assert _required_capabilities(embedded, strict=True) == (["CAPABILITY_AUTO_EXPAND"], [])
    # A parameter default is not walked: a macro name there is not a macro call.
    in_parameters = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {"P": {"Type": "String", "Default": "Fn::Transform"}},
        "Resources": {"Bucket": {"Type": "AWS::S3::Bucket"}},
    }
    assert _required_capabilities(in_parameters, strict=True) == ([], [])

    monkeypatch.setattr(app_mod, "AUTH", True)
    refused = (400, "InsufficientCapabilitiesException",
               "Requires capabilities : [CAPABILITY_AUTO_EXPAND]")
    assert _caps_error(
        _check_capabilities(embedded, embedded, _caps_params(embedded))) == refused
    assert _check_capabilities(
        embedded, embedded, _caps_params(embedded, "CAPABILITY_AUTO_EXPAND")) is None
    # Not on a change set (macros=False).
    assert _check_capabilities(embedded, embedded, _caps_params(embedded), macros=False) is None

    stack_name = "cfn-caps-auth-embedded"
    response = _create_stack(_caps_params(embedded, StackName=stack_name))
    assert _caps_error(response) == refused
    assert stack_name not in _stacks


def test_cfn_create_stack_capabilities_parsed_from_wire_under_auth(monkeypatch):
    import asyncio
    from urllib.parse import urlencode

    import ministack.app as app_mod
    from ministack.services.cloudformation import _stacks, handle_request

    monkeypatch.setattr(app_mod, "AUTH", True)
    stack_name = "cfn-caps-auth-wire"
    body = urlencode({
        "Action": "CreateStack",
        "Version": "2010-05-15",
        "StackName": stack_name,
        "TemplateBody": json.dumps(_CAPS_NAMED_ROLE_TEMPLATE),
        "Capabilities.member.1": "CAPABILITY_IAM",
        "Capabilities.member.2": "CAPABILITY_AUTO_EXPAND",
    }).encode()
    headers = {"content-type": "application/x-www-form-urlencoded"}
    response = asyncio.run(handle_request("POST", "/", headers, body, {}))
    # CAPABILITY_IAM arrived and was considered: only NAMED_IAM is missing.
    assert _caps_error(response) == (
        400, "InsufficientCapabilitiesException",
        "Requires capabilities : [CAPABILITY_NAMED_IAM]")
    assert stack_name not in _stacks


def test_cfn_update_stack_refuses_missing_capabilities_under_auth(monkeypatch):
    import ministack.app as app_mod
    from ministack.services.cloudformation import _stack_events, _stacks
    from ministack.services.cloudformation.handlers import _update_stack

    monkeypatch.setattr(app_mod, "AUTH", True)
    stack_name = "cfn-caps-auth-update"
    stack_id = f"arn:aws:cloudformation:us-east-1:000000000000:stack/{stack_name}/caps-1"
    _stacks[stack_name] = {
        "StackName": stack_name,
        "StackId": stack_id,
        "StackStatus": "CREATE_COMPLETE",
        "Parameters": [],
        "Tags": [],
        "Outputs": [],
        "_region": "us-east-1",
        "_resources": {},
        "_template": _CAPS_BUCKET_TEMPLATE,
        "_template_body": json.dumps(_CAPS_BUCKET_TEMPLATE),
        "_resolved_params": {},
        "_conditions": {},
    }
    _stack_events[stack_id] = []
    try:
        response = _update_stack(_caps_params(_CAPS_ROLE_TEMPLATE, StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        assert _stacks[stack_name]["StackStatus"] == "CREATE_COMPLETE"

        pytest.importorskip("samtranslator")
        response = _update_stack(_caps_params(_CAPS_TRANSFORM_TEMPLATE, StackName=stack_name))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM, CAPABILITY_AUTO_EXPAND]")
        assert _stacks[stack_name]["StackStatus"] == "CREATE_COMPLETE"

        # UsePreviousTemplate re-checks the stored template (unmeasured on AWS).
        _stacks[stack_name]["_template"] = _CAPS_ROLE_TEMPLATE
        _stacks[stack_name]["_template_body"] = json.dumps(_CAPS_ROLE_TEMPLATE)
        response = _update_stack({
            "StackName": [stack_name], "UsePreviousTemplate": ["true"],
            "Parameters.member.1.ParameterKey": ["Unused"],
        })
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        assert _stacks[stack_name]["StackStatus"] == "CREATE_COMPLETE"
    finally:
        _stacks.pop(stack_name, None)
        _stack_events.pop(stack_id, None)


def test_cfn_create_change_set_refuses_missing_iam_capability_under_auth(monkeypatch):
    import ministack.app as app_mod
    from ministack.services.cloudformation import _change_sets, _stack_events, _stacks
    from ministack.services.cloudformation.changesets import _create_change_set

    monkeypatch.setattr(app_mod, "AUTH", True)
    stack_name = "cfn-caps-auth-cs"

    try:
        response = _create_change_set(_caps_params(
            _CAPS_ROLE_TEMPLATE, StackName=stack_name,
            ChangeSetName="cs1", ChangeSetType="CREATE"))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_IAM]")
        # A refused CREATE change set leaves no REVIEW_IN_PROGRESS stack behind.
        assert stack_name not in _stacks

        response = _create_change_set(_caps_params(
            _CAPS_NAMED_ROLE_TEMPLATE, "CAPABILITY_IAM", StackName=stack_name,
            ChangeSetName="cs1", ChangeSetType="CREATE"))
        assert _caps_error(response) == (
            400, "InsufficientCapabilitiesException",
            "Requires capabilities : [CAPABILITY_NAMED_IAM]")
        assert stack_name not in _stacks
    finally:
        stack = _stacks.pop(stack_name, None)
        if stack:
            _stack_events.pop(stack["StackId"], None)
        for cs_id in [k for k, v in _change_sets.items()
                      if v.get("StackName") == stack_name]:
            _change_sets.pop(cs_id, None)

    # With CAPABILITY_IAM the set is created and the REVIEW_IN_PROGRESS
    # placeholder stays.
    try:
        response = _create_change_set(_caps_params(
            _CAPS_ROLE_TEMPLATE, "CAPABILITY_IAM", StackName=stack_name,
            ChangeSetName="cs1", ChangeSetType="CREATE"))
        status, code, _ = _caps_error(response)
        assert status == 200, code
        assert _stacks[stack_name]["StackStatus"] == "REVIEW_IN_PROGRESS"
    finally:
        stack = _stacks.pop(stack_name, None)
        if stack:
            _stack_events.pop(stack["StackId"], None)
        for cs_id in [k for k, v in _change_sets.items()
                      if v.get("StackName") == stack_name]:
            _change_sets.pop(cs_id, None)

    # CAPABILITY_AUTO_EXPAND "doesn't apply to creating change sets": a macro
    # template without it is not refused for capabilities.
    macro = dict(_CAPS_BUCKET_TEMPLATE, Transform="AWS::LanguageExtensions")
    try:
        response = _create_change_set(_caps_params(
            macro, StackName=stack_name, ChangeSetName="cs1", ChangeSetType="CREATE"))
        assert _caps_error(response)[1] != "InsufficientCapabilitiesException"
    finally:
        stack = _stacks.pop(stack_name, None)
        if stack:
            _stack_events.pop(stack["StackId"], None)
        for cs_id in [k for k, v in _change_sets.items()
                      if v.get("StackName") == stack_name]:
            _change_sets.pop(cs_id, None)

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

    summaries = _all_pages(cfn, "list_stacks", "StackSummaries")
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


def test_cfn_describe_stack_resource_returns_the_metadata(cfn):
    """DescribeStackResource carries the resource's Metadata attribute as a
    JSON string with intrinsics interpreted, and follows the template after an
    update; a Metadata that resolves away or cannot be resolved is returned as
    declared; a resource without Metadata has no such field; a healthy resource
    has no ResourceStatusReason."""
    stack_name = f"cfn-resource-metadata-{_uuid_mod.uuid4().hex[:8]}"

    def template(path, rev):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Parameters": {"Owner": {"Type": "String", "Default": "platform"}},
            "Resources": {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Metadata": {
                        "aws:cdk:path": path,
                        "Owner": {"Ref": "Owner"},
                        "Region": {"Ref": "AWS::Region"},
                        "Nested": {"Flag": True, "List": [1, rev]},
                    },
                },
                "Plain": {"Type": "AWS::SQS::Queue"},
                "Gone": {
                    "Type": "AWS::SQS::Queue",
                    "Metadata": {"Ref": "AWS::NoValue"},
                },
                "Broken": {
                    "Type": "AWS::SQS::Queue",
                    "Metadata": {"Fn::GetAtt": ["Plain", "NoSuchAttr"]},
                },
            },
        }

    def detail(logical_id):
        return cfn.describe_stack_resource(
            StackName=stack_name, LogicalResourceId=logical_id)["StackResourceDetail"]

    cfn.create_stack(StackName=stack_name,
                     TemplateBody=json.dumps(template("ExampleStack/Queue/Resource", 2)))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        queue = detail("Queue")
        assert json.loads(queue["Metadata"]) == {
            "aws:cdk:path": "ExampleStack/Queue/Resource",
            "Owner": "platform",
            "Region": "us-east-1",
            "Nested": {"Flag": True, "List": [1, 2]},
        }
        assert "ResourceStatusReason" not in queue
        assert queue["LastUpdatedTimestamp"]
        assert "Metadata" not in detail("Plain")
        # A Metadata that resolves away entirely, or one that cannot be
        # resolved, is returned as declared, not a failed request.
        assert json.loads(detail("Gone")["Metadata"]) == {"Ref": "AWS::NoValue"}
        assert json.loads(detail("Broken")["Metadata"]) == {
            "Fn::GetAtt": ["Plain", "NoSuchAttr"]}

        # After an update the new template's Metadata is what comes back.
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=json.dumps(template("ExampleStack/Queue/Resource/v2", 3)))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        updated = json.loads(detail("Queue")["Metadata"])
        assert updated["aws:cdk:path"] == "ExampleStack/Queue/Resource/v2"
        assert updated["Nested"] == {"Flag": True, "List": [1, 3]}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_stack_id_addresses_every_read_action(cfn, sqs):
    """Every action that takes a StackName accepts the stack id (what the CDK
    sends after its first DescribeStacks), and the responses carry the stack's
    name, not the id that was passed."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-by-id-{uid}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps({"Resources": {
        "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": f"cfn-by-id-{uid}"}}}}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        stack_id = stack["StackId"]

        assert cfn.describe_stacks(StackName=stack_id)["Stacks"][0]["StackName"] == stack_name
        detail = cfn.describe_stack_resource(StackName=stack_id, LogicalResourceId="Queue")
        assert detail["StackResourceDetail"]["StackName"] == stack_name
        resources = cfn.describe_stack_resources(StackName=stack_id)["StackResources"]
        assert [r["StackName"] for r in resources] == [stack_name]
        summaries = cfn.list_stack_resources(StackName=stack_id)["StackResourceSummaries"]
        assert [r["LogicalResourceId"] for r in summaries] == ["Queue"]
        assert cfn.get_template(StackName=stack_id)["TemplateBody"]
        assert cfn.describe_stack_events(StackName=stack_id)["StackEvents"]
        assert cfn.get_template_summary(StackName=stack_id)["ResourceTypes"] == ["AWS::SQS::Queue"]

        cfn.create_change_set(StackName=stack_id, ChangeSetName="by-id", ChangeSetType="UPDATE",
                              TemplateBody=json.dumps({"Resources": {"Queue": {
                                  "Type": "AWS::SQS::Queue", "Properties": {
                                      "QueueName": f"cfn-by-id-{uid}",
                                      "VisibilityTimeout": 45}}}}))
        described = cfn.describe_change_set(ChangeSetName="by-id", StackName=stack_name)
        assert described["StackName"] == stack_name
        assert described["Status"] == "CREATE_COMPLETE", described.get("StatusReason")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


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
            "Bad": _FAILING_RESOURCE,
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

def test_cfn_list_imports_names_the_importing_stacks(cfn):
    """ListImports lists the stacks whose templates import the export through
    Fn::ImportValue; a template that only mentions the name does not count,
    and an export nobody imports is a ValidationError."""
    uid = _uuid_mod.uuid4().hex[:8]
    export_name = f"cfn-imports-{uid}"
    exporter, importer, mention = (f"cfn-imports-exp-{uid}", f"cfn-imports-imp-{uid}",
                                   f"cfn-imports-mention-{uid}")
    cfn.create_stack(StackName=exporter, TemplateBody=json.dumps({
        "Resources": {"P": {"Type": "AWS::SSM::Parameter", "Properties": {
            "Name": f"/cfn-imports/{uid}/exp", "Type": "String", "Value": "v"}}},
        "Outputs": {"Out": {"Value": "v", "Export": {"Name": export_name}}}}))
    try:
        assert _wait_stack(cfn, exporter)["StackStatus"] == "CREATE_COMPLETE"
        with pytest.raises(ClientError) as exc:
            cfn.list_imports(ExportName=export_name)
        assert exc.value.response["Error"]["Message"] == (
            f"Export '{export_name}' is not imported by any stack.")

        cfn.create_stack(StackName=importer, TemplateBody=json.dumps({"Resources": {
            "P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-imports/{uid}/imp", "Type": "String",
                "Value": {"Fn::ImportValue": export_name}}}}}))
        cfn.create_stack(StackName=mention, TemplateBody=json.dumps({"Resources": {
            "P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-imports/{uid}/mention", "Type": "String",
                "Value": export_name}}}}))
        for name in (importer, mention):
            assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        assert cfn.list_imports(ExportName=export_name)["Imports"] == [importer]
    finally:
        for name in (importer, mention, exporter):
            _delete_cfn_test_stack(cfn, name)


def test_cfn_termination_protection_blocks_delete_until_disabled(cfn):
    """A stack created with EnableTerminationProtection refuses DeleteStack
    and stays as it is; UpdateTerminationProtection turns it off, DescribeStacks
    reports the flag either way."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-protected-{uid}"
    template = json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter", "Properties": {
        "Name": f"/cfn-protected/{uid}", "Type": "String", "Value": "v"}}}})
    cfn.create_stack(StackName=stack_name, TemplateBody=template,
                     EnableTerminationProtection=True)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert stack["EnableTerminationProtection"] is True

        with pytest.raises(ClientError) as exc:
            cfn.delete_stack(StackName=stack_name)
        assert "cannot be deleted while TerminationProtection is enabled" in (
            exc.value.response["Error"]["Message"])
        assert cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"] == (
            "CREATE_COMPLETE")

        result = cfn.update_termination_protection(
            StackName=stack_name, EnableTerminationProtection=False)
        assert result["StackId"] == stack["StackId"]
        assert cfn.describe_stacks(StackName=stack_name)["Stacks"][0][
            "EnableTerminationProtection"] is False
        with pytest.raises(ClientError):
            cfn.update_termination_protection(
                StackName=f"{stack_name}-nope", EnableTerminationProtection=True)
        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    finally:
        try:
            cfn.update_termination_protection(
                StackName=stack_name, EnableTerminationProtection=False)
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_stack_policy_is_stored_and_returned(cfn):
    """SetStackPolicy stores the policy and GetStackPolicy returns it; a stack
    without one answers no StackPolicyBody; CreateStack takes StackPolicyBody."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-policy-{uid}"
    template = json.dumps({"Resources": {"P": {"Type": "AWS::SSM::Parameter", "Properties": {
        "Name": f"/cfn-policy/{uid}", "Type": "String", "Value": "v"}}}})
    policy = json.dumps({"Statement": [{"Effect": "Deny", "Action": "Update:*",
                                        "Principal": "*", "Resource": "LogicalResourceId/P"}]})
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        assert "StackPolicyBody" not in cfn.get_stack_policy(StackName=stack_name)
        cfn.set_stack_policy(StackName=stack_name, StackPolicyBody=policy)
        assert json.loads(cfn.get_stack_policy(StackName=stack_name)["StackPolicyBody"]) == (
            json.loads(policy))
        with pytest.raises(ClientError):
            cfn.set_stack_policy(StackName=stack_name, StackPolicyBody="not json")

        cfn.create_stack(StackName=f"{stack_name}-b", TemplateBody=template.replace(
            f"/cfn-policy/{uid}", f"/cfn-policy/{uid}/b"), StackPolicyBody=policy)
        assert _wait_stack(cfn, f"{stack_name}-b")["StackStatus"] == "CREATE_COMPLETE"
        assert cfn.get_stack_policy(StackName=f"{stack_name}-b")["StackPolicyBody"]

        replaced = json.dumps({"Statement": [{"Effect": "Allow", "Action": "Update:*",
                                              "Principal": "*", "Resource": "*"}]})
        cfn.update_stack(StackName=f"{stack_name}-b", TemplateBody=template.replace(
            f"/cfn-policy/{uid}", f"/cfn-policy/{uid}/b").replace('"v"', '"w"'),
            StackPolicyBody=replaced)
        assert _wait_stack(cfn, f"{stack_name}-b")["StackStatus"] == "UPDATE_COMPLETE"
        assert json.loads(cfn.get_stack_policy(StackName=f"{stack_name}-b")["StackPolicyBody"]) == (
            json.loads(replaced))
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _delete_cfn_test_stack(cfn, f"{stack_name}-b")


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
            "Bad": _FAILING_RESOURCE,
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
    exports = _all_pages(cfn, "list_exports", "Exports")
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


def test_cfn_lambda_image_package_type(cfn, lam):
    """AWS::Lambda::Function with PackageType=Image is created as an image, not a Zip."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-lambda-image-{suffix}"
    function_name = f"cfn-image-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Function": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": function_name,
                    "PackageType": "Image",
                    "Role": "arn:aws:iam::000000000000:role/lambda-role",
                    "Code": {"ImageUri": "public.ecr.aws/lambda/python:3.12"},
                    "ImageConfig": {"Command": ["app.handler"]},
                },
            },
        },
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    got = lam.get_function(FunctionName=function_name)
    config = got["Configuration"]
    assert config["PackageType"] == "Image"
    assert got["Code"]["ImageUri"] == "public.ecr.aws/lambda/python:3.12"
    # An image package carries no Runtime/Handler on AWS.
    assert not config.get("Runtime")
    assert not config.get("Handler")
    assert config["ImageConfigResponse"] == {"ImageConfig": {"Command": ["app.handler"]}}

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


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


def _wait_condition_template(timeout="20", count=None):
    """A handle, a wait condition on it, a resource that must not start before
    the wait ends. The 20 s fuse keeps a failing test from parking a worker."""
    props = {"Handle": {"Ref": "Handle"}, "Timeout": timeout}
    if count is not None:
        props["Count"] = count
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"},
            "Wait": {"Type": "AWS::CloudFormation::WaitCondition", "Properties": props},
            "After": {
                "Type": "AWS::SSM::Parameter",
                "DependsOn": "Wait",
                "Properties": {"Type": "String", "Name": {"Fn::Sub": "/${AWS::StackName}/after"}, "Value": "x"},
            },
        },
        "Outputs": {
            "Url": {"Value": {"Ref": "Handle"}},
            "HandleId": {"Value": {"Fn::GetAtt": ["Handle", "Id"]}},
            "Data": {"Value": {"Fn::GetAtt": ["Wait", "Data"]}},
        },
    }


def _creation_policy_wait_template(policy):
    """The form the CreationPolicy attribute reference documents: no handle,
    no Properties, signals through SignalResource only."""
    return {
        "Resources": {
            "Wait": {"Type": "AWS::CloudFormation::WaitCondition", "CreationPolicy": policy},
            "After": {
                "Type": "AWS::SSM::Parameter",
                "DependsOn": "Wait",
                "Properties": {"Type": "String", "Name": {"Fn::Sub": "/${AWS::StackName}/after"}, "Value": "x"},
            },
        },
        "Outputs": {"Data": {"Value": {"Fn::GetAtt": ["Wait", "Data"]}}},
    }


def _wait_condition_is_waiting(cfn, stack_name, logical_id="Wait"):
    events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
    return any(e["LogicalResourceId"] == logical_id and e["ResourceStatus"] == "CREATE_IN_PROGRESS"
               for e in events)


def _wait_for_wait_condition(cfn, stack_name, timeout=15):
    """Poll until the wait condition is CREATE_IN_PROGRESS; the handle URL, if any."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _wait_condition_is_waiting(cfn, stack_name):
            events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
            return next((e["PhysicalResourceId"] for e in events
                         if e["LogicalResourceId"] == "Handle" and e["ResourceStatus"] == "CREATE_COMPLETE"), None)
        time.sleep(0.2)
    raise TimeoutError(f"{stack_name}: the wait condition never started waiting")


def _assert_still_waiting(cfn, stack_name, seconds=1.0):
    """The stack stays CREATE_IN_PROGRESS for the whole window."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"] == "CREATE_IN_PROGRESS"
        time.sleep(0.2)


def _signal_events(cfn, stack_name):
    return [e.get("ResourceStatusReason", "")
            for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
            if "signal with UniqueId" in e.get("ResourceStatusReason", "")]


def _put_wait_condition_signal(url, status="SUCCESS", unique_id="ID1", data="", reason="",
                               content_type="", body=None):
    """PUT the signal JSON to the handle URL, addressed at the test endpoint
    (the minted URL names the server's own host), with an empty Content-Type
    as the user guide asks for."""
    endpoint = urlparse(os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))
    target = urlparse(url)._replace(scheme=endpoint.scheme, netloc=endpoint.netloc).geturl()
    if body is None:
        body = json.dumps({"Status": status, "UniqueId": unique_id, "Data": data, "Reason": reason}).encode()
    req = urllib.request.Request(target, data=body, method="PUT", headers={"Content-Type": content_type})
    with urllib.request.urlopen(req) as resp:
        return resp.status


def _assert_validation_error(exc_info, message):
    assert exc_info.value.response["Error"]["Code"] == "ValidationError"
    assert message in exc_info.value.response["Error"]["Message"]


def test_cfn_wait_condition(cfn):
    """A WaitCondition holds the stack until its handle receives the SUCCESS
    signal; the handle's Ref is the signal URL, Data carries the signal, the
    signal is published to the stack events, a deleted handle answers 404."""
    stack_name = f"cfn-wait-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template()))
    try:
        url = _wait_for_wait_condition(cfn, stack_name)
        assert "/_ministack/cfn-signal/" in url
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert not any(e["LogicalResourceId"] == "After" for e in events)
        _assert_still_waiting(cfn, stack_name, 0.5)
        assert _put_wait_condition_signal(url, "SUCCESS", "ID1234", data="Application has completed configuration.") == 200
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert _output(stack, "Url") == url
        assert url.endswith("/" + _output(stack, "HandleId"))
        assert json.loads(_output(stack, "Data")) == {"ID1234": "Application has completed configuration."}
        resources = {r["LogicalResourceId"]: r for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]}
        assert resources["Handle"]["PhysicalResourceId"] == url
        assert resources["After"]["ResourceStatus"] == "CREATE_COMPLETE"
        assert _signal_events(cfn, stack_name) == ["Received SUCCESS signal with UniqueId ID1234"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _put_wait_condition_signal(url, "SUCCESS", "late")
    assert exc_info.value.code == 404


def test_cfn_wait_condition_failure_signal_rolls_back(cfn):
    stack_name = f"cfn-wait-fail-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template()))
    try:
        url = _wait_for_wait_condition(cfn, stack_name)
        assert _put_wait_condition_signal(url, "FAILURE", "node-1", reason="Setup failed") == 200
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
        reasons = _stack_event_reasons(cfn, stack_name)
        assert "WaitCondition received failed message: 'Setup failed' for uniqueId: node-1" in reasons
        assert "Received FAILURE signal with UniqueId node-1" in reasons
        assert not any(e["LogicalResourceId"] == "After"
                       for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"])
        # The rollback deleted the handle: its URL is gone.
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _put_wait_condition_signal(url, "SUCCESS", "late")
        assert exc_info.value.code == 404
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_wait_condition_timeout_rolls_back(cfn):
    stack_name = f"cfn-wait-timeout-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template(timeout="1")))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
        assert "WaitCondition timed out. Received 0 conditions when expecting 1" in _stack_event_reasons(cfn, stack_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_wait_condition_count_and_signal_endpoint_rules(cfn):
    """Count=2 needs two distinct UniqueIds; a repeated UniqueId is a
    retransmission and does not count. The endpoint refuses a non-empty
    Content-Type (403), a body that is not the signal JSON (400) and an
    unknown token (404)."""
    stack_name = f"cfn-wait-count-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template(count=2)))
    try:
        url = _wait_for_wait_condition(cfn, stack_name)
        for kwargs, code in (
            ({"status": "DONE"}, 400),
            ({"body": b"not json"}, 400),
            ({"content_type": "application/json"}, 403),
            ({"url": url.rsplit("/", 1)[0] + "/no-such-token"}, 404),
        ):
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                _put_wait_condition_signal(**{"url": url, "unique_id": "node-1", **kwargs})
            assert exc_info.value.code == code, kwargs
        assert "must be an empty string or omitted" in str(
            pytest.raises(urllib.error.HTTPError, _put_wait_condition_signal, url,
                          unique_id="node-1", content_type="text/plain").value.read())
        _put_wait_condition_signal(url, "SUCCESS", "node-1", data="first")
        _put_wait_condition_signal(url, "SUCCESS", "node-1", data="again")
        _assert_still_waiting(cfn, stack_name)
        assert _signal_events(cfn, stack_name) == ["Received SUCCESS signal with UniqueId node-1"]
        _put_wait_condition_signal(url, "SUCCESS", "node-2", data="second")
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert json.loads(_output(stack, "Data")) == {"node-1": "first", "node-2": "second"}
        assert sorted(_signal_events(cfn, stack_name)) == [
            "Received SUCCESS signal with UniqueId node-1",
            "Received SUCCESS signal with UniqueId node-2",
        ]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_wait_condition_handle_must_be_a_handle_of_the_stack(cfn):
    """A Handle that is not a handle URL, or the handle of another stack, fails
    the resource; the other stack's handle stays usable."""
    other_name = f"cfn-wait-other-{_uuid_mod.uuid4().hex[:8]}"
    stack_name = f"cfn-wait-badhandle-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=other_name, TemplateBody=json.dumps(_wait_condition_template()))
    try:
        other_url = _wait_for_wait_condition(cfn, other_name)
        for handle, message in (
            (other_url, "Handle must be the Ref of an AWS::CloudFormation::WaitConditionHandle of this stack"),
            ("https://example.local/not-a-handle", "Handle must be the Ref of an AWS::CloudFormation::WaitConditionHandle"),
        ):
            template = _wait_condition_template()
            template["Resources"]["Wait"]["Properties"]["Handle"] = handle
            cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
            try:
                assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
                assert message in _stack_event_reasons(cfn, stack_name)
            finally:
                _delete_cfn_test_stack(cfn, stack_name)
        _put_wait_condition_signal(other_url, "SUCCESS", "still-works")
        assert _wait_stack(cfn, other_name)["StackStatus"] == "CREATE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, other_name)


@pytest.mark.parametrize(
    ("props", "message"),
    [
        ({"Count": 1}, "Timeout is required"),
        ({"Timeout": "30", "Count": "0"}, "Count must be an integer of at least 1"),
    ],
)
def test_cfn_wait_condition_refuses_invalid_properties(cfn, props, message):
    stack_name = f"cfn-wait-invalid-{_uuid_mod.uuid4().hex[:8]}"
    template = _wait_condition_template()
    template["Resources"]["Wait"]["Properties"] = {"Handle": {"Ref": "Handle"}, **props}
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
        assert message in _stack_event_reasons(cfn, stack_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


@pytest.mark.parametrize(
    ("validator", "value", "message"),
    [
        ("validate_timeout_seconds", "0", "Timeout must be a number of seconds between 1 and 43200"),
        ("validate_timeout_seconds", "43201", "Timeout must be a number of seconds between 1 and 43200"),
        ("validate_timeout_seconds", "PT5M", "Timeout must be a number of seconds between 1 and 43200"),
        ("validate_count", "two", "Count must be an integer of at least 1"),
        ("validate_resource_signal_timeout", "5 minutes", "ResourceSignal Timeout must be an ISO 8601 duration"),
        ("validate_resource_signal_timeout", "PT0S", "ResourceSignal Timeout must be at least one second"),
        ("validate_resource_signal_timeout", "PT13H", "ResourceSignal Timeout must be at most 12 hours"),
    ],
)
def test_cfn_wait_condition_validators(validator, value, message):
    from ministack.services.cloudformation import wait_conditions as wc

    with pytest.raises(ValueError, match=re.escape(message)):
        getattr(wc, validator)(value, "Wait")
    assert wc.validate_count("3", "Wait") == 3
    assert wc.validate_count(None, "Wait") == 1
    assert wc.validate_timeout_seconds(" 300 ", "Wait") == 300
    assert wc.validate_resource_signal_timeout(None, "Wait") == 300
    assert wc.validate_resource_signal_timeout("PT1H30M15S", "Wait") == 5415


@pytest.mark.parametrize(
    "policy",
    [
        {"ResourceSignal": {"Timeout": "PT2M", "Count": "2"}},
        {"ResourceSignal": {"Count": 2}},
    ],
)
def test_cfn_wait_condition_creation_policy_waits_for_signal_resource(cfn, policy):
    """The CreationPolicy form: no handle, SignalResource only, Timeout
    defaults to PT5M; an update never waits again."""
    stack_name = f"cfn-wait-cp-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_creation_policy_wait_template(policy)))
    try:
        _wait_for_wait_condition(cfn, stack_name)
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="i-1", Status="SUCCESS")
        _assert_still_waiting(cfn, stack_name)
        assert _signal_events(cfn, stack_name) == ["Received SUCCESS signal with UniqueId i-1"]
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="i-2", Status="SUCCESS")
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert json.loads(_output(stack, "Data")) == {"i-1": "", "i-2": ""}
        assert len(_signal_events(cfn, stack_name)) == 2
        template = _creation_policy_wait_template({"ResourceSignal": {"Count": "5"}})
        template["Resources"]["After"]["Properties"]["Value"] = "y"
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE"
        assert json.loads(_output(stack, "Data")) == {"i-1": "", "i-2": ""}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


@pytest.mark.parametrize("policy", [{}, {"ResourceSignal": {}}])
def test_cfn_wait_condition_creation_policy_defaults(cfn, policy):
    """An empty CreationPolicy or ResourceSignal means one signal, PT5M."""
    stack_name = f"cfn-wait-cpdef-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_creation_policy_wait_template(policy)))
    try:
        _wait_for_wait_condition(cfn, stack_name)
        _assert_still_waiting(cfn, stack_name, 0.5)
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="only", Status="SUCCESS")
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        ({"ResourceSignal": {"Timeout": "PT1S"}}, "WaitCondition timed out. Received 0 conditions when expecting 1"),
        ("PT5M", "CreationPolicy must be an object"),
        ({"ResourceSignal": "PT5M"}, "CreationPolicy ResourceSignal must be an object"),
    ],
)
def test_cfn_wait_condition_creation_policy_timeout_and_shape(cfn, policy, expected):
    stack_name = f"cfn-wait-cpfail-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_creation_policy_wait_template(policy)))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
        assert expected in _stack_event_reasons(cfn, stack_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_wait_condition_store_reset_releases_a_waiter():
    """The service's reset (what POST /_ministack/reset calls) must not leave
    a worker thread blocked in a wait that nothing can signal any more. Run
    in-process against this test's own copy of the stores."""
    import threading

    from ministack.services import cloudformation as cfn_service
    from ministack.services.cloudformation import wait_conditions as wc

    token = wc.register_slot("arn:aws:cloudformation:eu-central-1:000000000000:stack/reset-probe/1")
    outcome = {}

    def waiter():
        try:
            wc.wait_for(token, "arn:aws:cloudformation:eu-central-1:000000000000:stack/reset-probe/1",
                        "reset-probe", "Wait", "AWS::CloudFormation::WaitCondition", 1, 30)
        except Exception as exc:
            outcome["error"] = exc

    thread = threading.Thread(target=waiter, daemon=True)
    thread.start()
    time.sleep(0.2)
    cfn_service.reset()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert "state was reset" in str(outcome.get("error"))


def test_cfn_signal_resource_completes_the_wait_condition(cfn):
    """SignalResource delivers a signal without the handle URL; the stack name
    or its id addresses the stack."""
    stack_name = f"cfn-wait-signal-{_uuid_mod.uuid4().hex[:8]}"
    stack_id = cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template(count=2)))["StackId"]
    try:
        _wait_for_wait_condition(cfn, stack_name)
        with pytest.raises(ClientError) as exc_info:
            cfn.signal_resource(StackName=stack_name, LogicalResourceId="After", UniqueId="s1", Status="SUCCESS")
        _assert_validation_error(exc_info, f"Resource [After] in stack [{stack_name}] is not waiting for signals")
        with pytest.raises(ClientError) as exc_info:
            cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="s1", Status="DONE")
        _assert_validation_error(exc_info, "Member must satisfy enum value set: [FAILURE, SUCCESS]")
        with pytest.raises(ClientError) as exc_info:
            cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="x" * 65, Status="SUCCESS")
        _assert_validation_error(exc_info, "Member must have length less than or equal to 64")
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="s1", Status="SUCCESS")
        cfn.signal_resource(StackName=stack_id, LogicalResourceId="Wait", UniqueId="s2", Status="SUCCESS")
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert json.loads(_output(stack, "Data")) == {"s1": "", "s2": ""}
        assert sorted(_signal_events(cfn, stack_name)) == [
            "Received SUCCESS signal with UniqueId s1",
            "Received SUCCESS signal with UniqueId s2",
        ]
        with pytest.raises(ClientError) as exc_info:
            cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="s3", Status="SUCCESS")
        _assert_validation_error(exc_info, f"Stack [{stack_name}] is in CREATE_COMPLETE state and cannot be signaled")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError) as exc_info:
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="s4", Status="SUCCESS")
    _assert_validation_error(exc_info, f"Stack [{stack_name}] does not exist")


@pytest.mark.parametrize("missing", ["StackName", "LogicalResourceId", "UniqueId", "Status"])
def test_cfn_signal_resource_requires_every_field(missing):
    """boto3 refuses a missing field itself; a client without parameter
    validation shows the service's own answer."""
    client = boto3.client(
        "cloudformation",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(parameter_validation=False, retries={"mode": "standard"}),
    )
    params = {"StackName": "no-such-stack", "LogicalResourceId": "Wait", "UniqueId": "s1", "Status": "SUCCESS"}
    params.pop(missing)
    with pytest.raises(ClientError) as exc_info:
        client.signal_resource(**params)
    _assert_validation_error(exc_info, f"{missing} is required")


def test_cfn_signal_resource_failure_rolls_back(cfn):
    stack_name = f"cfn-wait-signalfail-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_wait_condition_template()))
    try:
        _wait_for_wait_condition(cfn, stack_name)
        cfn.signal_resource(StackName=stack_name, LogicalResourceId="Wait", UniqueId="node-9", Status="FAILURE")
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "ROLLBACK_COMPLETE"
        reasons = _stack_event_reasons(cfn, stack_name)
        assert "for uniqueId: node-9" in reasons
        assert "Received FAILURE signal with UniqueId node-9" in reasons
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_wait_condition_in_a_nested_stack(cfn, s3):
    """A nested stack deploys on a worker thread, so a wait condition inside
    it holds the parent without blocking the server; SignalResource on the
    child stack (its id is the parent's resource physical id) releases it."""
    suffix = _uuid_mod.uuid4().hex[:8]
    templates_bucket = f"cfn-wait-nested-{suffix}"
    s3.create_bucket(Bucket=templates_bucket)
    s3.put_object(Bucket=templates_bucket, Key="child.json",
                  Body=json.dumps(_wait_condition_template()).encode())
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    parent_name = f"cfn-wait-parent-{suffix}"
    parent_template = {
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {"TemplateURL": f"{endpoint}/{templates_bucket}/child.json"},
            },
            "AfterNested": {
                "Type": "AWS::SSM::Parameter",
                "DependsOn": "Nested",
                "Properties": {"Type": "String", "Name": f"/{parent_name}/after", "Value": "x"},
            },
        },
        "Outputs": {"ChildData": {"Value": {"Fn::GetAtt": ["Nested", "Outputs.Data"]}}},
    }
    cfn.create_stack(StackName=parent_name, TemplateBody=json.dumps(parent_template))
    try:
        deadline = time.time() + 15
        child_id = None
        while time.time() < deadline and child_id is None:
            for summary in cfn.list_stacks(StackStatusFilter=["CREATE_IN_PROGRESS"])["StackSummaries"]:
                if summary["StackName"].startswith(f"{parent_name}-Nested-") \
                        and _wait_condition_is_waiting(cfn, summary["StackId"]):
                    child_id = summary["StackId"]
            time.sleep(0.2)
        assert child_id, "the child stack never reached its wait condition"
        # The server answers while the parent waits.
        assert cfn.describe_stacks(StackName=parent_name)["Stacks"][0]["StackStatus"] == "CREATE_IN_PROGRESS"
        cfn.signal_resource(StackName=child_id, LogicalResourceId="Wait", UniqueId="child-1", Status="SUCCESS")
        stack = _wait_stack(cfn, parent_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert json.loads(_output(stack, "ChildData")) == {"child-1": ""}
        nested = next(r for r in cfn.describe_stack_resources(StackName=parent_name)["StackResources"]
                      if r["LogicalResourceId"] == "Nested")
        assert nested["PhysicalResourceId"] == child_id
    finally:
        _delete_cfn_test_stack(cfn, parent_name)
        try:
            s3.delete_object(Bucket=templates_bucket, Key="child.json")
            s3.delete_bucket(Bucket=templates_bucket)
        except ClientError:
            pass


@pytest.mark.parametrize(
    ("scope", "arn_region", "arn_segment"),
    [
        ("REGIONAL", "us-east-1", "regional"),
        ("CLOUDFRONT", "us-east-1", "global"),
    ],
)
def test_cfn_wafv2_web_acl_uses_canonical_arn(
    cfn, wafv2, scope, arn_region, arn_segment
):
    scope_name = scope.lower()
    stack_name = f"cfn-wafv2-{scope_name}"
    acl_name = f"cfn-wafv2-{scope_name}-acl"
    template = {
        "Resources": {
            "Acl": {
                "Type": "AWS::WAFv2::WebACL",
                "Properties": {
                    "Name": acl_name,
                    "Scope": scope,
                    "DefaultAction": {"Allow": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": False,
                        "CloudWatchMetricsEnabled": False,
                        "MetricName": acl_name,
                    },
                    "Tags": [{"Key": "from", "Value": "cfn"}],
                },
            },
        },
        "Outputs": {
            "AclId": {"Value": {"Ref": "Acl"}},
            "AclArn": {"Value": {"Fn::GetAtt": ["Acl", "Arn"]}},
        },
    }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}

        assert outputs["AclArn"] == (
            f"arn:aws:wafv2:{arn_region}:000000000000:"
            f"{arn_segment}/webacl/{acl_name}/{outputs['AclId']}"
        )
        acls = wafv2.list_web_acls(Scope=scope)["WebACLs"]
        assert outputs["AclArn"] in {acl["ARN"] for acl in acls}
        tags = wafv2.list_tags_for_resource(ResourceARN=outputs["AclArn"])
        assert tags["TagInfoForResource"]["TagList"] == [
            {"Key": "from", "Value": "cfn"}
        ]
    finally:
        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)

    acls = wafv2.list_web_acls(Scope=scope)["WebACLs"]
    assert acl_name not in {acl["Name"] for acl in acls}

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
    funcs = [f["FunctionName"] for page in lam.get_paginator("list_functions").paginate() for f in page["Functions"]]
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
    stacks = _all_pages(cfn, "describe_stacks", "Stacks")
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
    assert _template_tags(endpoints[0]["Tags"]) == [{"Key": "source", "Value": "cloudformation"}]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)
    assert ec2.describe_vpc_endpoints(
        VpcEndpointIds=[outputs["RefId"]]
    )["VpcEndpoints"] == []


def test_cfn_ec2_resources_use_caller_region_context():
    """EC2 CFN provisioners must write through the caller's region context."""
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

    def _client(svc, region):
        return boto3.client(
            svc,
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(region_name=region, retries={"mode": "standard"}),
        )

    cfn_west = _client("cloudformation", "us-west-2")
    cfn_east = _client("cloudformation", "us-east-1")
    ec2_west = _client("ec2", "us-west-2")
    ec2_east = _client("ec2", "us-east-1")
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-ec2-region-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Vpc": {
                "Type": "AWS::EC2::VPC",
                "Properties": {"CidrBlock": "10.41.0.0/16"},
            },
            "Subnet": {
                "Type": "AWS::EC2::Subnet",
                "Properties": {
                    "VpcId": {"Ref": "Vpc"},
                    "CidrBlock": "10.41.1.0/24",
                    "AvailabilityZone": "us-west-2a",
                },
            },
            "SecurityGroup": {
                "Type": "AWS::EC2::SecurityGroup",
                "Properties": {
                    "GroupDescription": "regional cfn default-vpc fallback proof",
                },
            },
            "Endpoint": {
                "Type": "AWS::EC2::VPCEndpoint",
                "Properties": {
                    "VpcEndpointType": "Gateway",
                    "ServiceName": "com.amazonaws.us-west-2.s3",
                },
            },
            "RouteTable": {
                "Type": "AWS::EC2::RouteTable",
                "Properties": {},
            },
        },
        "Outputs": {
            "VpcId": {"Value": {"Ref": "Vpc"}},
            "SubnetId": {"Value": {"Ref": "Subnet"}},
            "SecurityGroupId": {"Value": {"Ref": "SecurityGroup"}},
            "EndpointId": {"Value": {"Ref": "Endpoint"}},
            "RouteTableId": {"Value": {"Ref": "RouteTable"}},
        },
    }
    outputs = {}

    try:
        cfn_west.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn_west, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}

        assert ec2_west.describe_vpcs(VpcIds=[outputs["VpcId"]])["Vpcs"][0]["CidrBlock"] == "10.41.0.0/16"
        assert ec2_west.describe_subnets(SubnetIds=[outputs["SubnetId"]])["Subnets"][0]["VpcId"] == outputs["VpcId"]
        assert ec2_west.describe_security_groups(
            GroupIds=[outputs["SecurityGroupId"]]
        )["SecurityGroups"][0]["VpcId"] == "vpc-00000001"
        assert ec2_west.describe_vpc_endpoints(
            VpcEndpointIds=[outputs["EndpointId"]]
        )["VpcEndpoints"][0]["VpcId"] == "vpc-00000001"
        assert ec2_west.describe_route_tables(
            RouteTableIds=[outputs["RouteTableId"]]
        )["RouteTables"][0]["VpcId"] == "vpc-00000001"

        with pytest.raises(ClientError):
            ec2_east.describe_vpcs(VpcIds=[outputs["VpcId"]])
        with pytest.raises(ClientError):
            ec2_east.describe_subnets(SubnetIds=[outputs["SubnetId"]])
        with pytest.raises(ClientError):
            ec2_east.describe_security_groups(GroupIds=[outputs["SecurityGroupId"]])
        assert ec2_east.describe_vpc_endpoints(VpcEndpointIds=[outputs["EndpointId"]])["VpcEndpoints"] == []

        updated_template = json.loads(json.dumps(template))
        updated_template["Resources"]["Endpoint2"] = {
            "Type": "AWS::EC2::VPCEndpoint",
            "Properties": {
                "VpcEndpointType": "Gateway",
                "ServiceName": "com.amazonaws.us-west-2.dynamodb",
            },
        }
        updated_template["Outputs"]["Endpoint2Id"] = {"Value": {"Ref": "Endpoint2"}}

        with pytest.raises(ClientError) as exc:
            cfn_east.update_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(updated_template),
            )
        assert exc.value.response["Error"]["Code"] == "ValidationError"

        cfn_west.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(updated_template),
        )
        stack = _wait_stack(cfn_west, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        assert ec2_west.describe_vpc_endpoints(
            VpcEndpointIds=[outputs["Endpoint2Id"]]
        )["VpcEndpoints"][0]["VpcId"] == "vpc-00000001"
        assert ec2_east.describe_vpc_endpoints(
            VpcEndpointIds=[outputs["Endpoint2Id"]]
        )["VpcEndpoints"] == []

        cfn_west.delete_stack(StackName=stack_name)
        _wait_stack(cfn_west, stack_name)
        assert ec2_west.describe_vpc_endpoints(
            VpcEndpointIds=[outputs["EndpointId"], outputs["Endpoint2Id"]]
        )["VpcEndpoints"] == []
    finally:
        try:
            cfn_west.delete_stack(StackName=stack_name)
            _wait_stack(cfn_west, stack_name)
        except ClientError:
            pass

    if outputs:
        assert ec2_west.describe_vpc_endpoints(VpcEndpointIds=[outputs["EndpointId"]])["VpcEndpoints"] == []


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


def test_cfn_location_tracker(cfn, location):
    """AWS::Location::Tracker provisions through the location service (so it is
    readable back through the real API) and the stack tags reach its Tags;
    Description and a tag change update in place; a KmsKeyId change on the
    custom-named tracker is refused as on AWS; a TrackerName change is a
    replacement, after which the old tracker is gone."""
    stack_name = f"cfn-loc-{_uuid_mod.uuid4().hex[:8]}"
    first, second = f"{stack_name}-a", f"{stack_name}-b"

    def template(name, description, kms_key_id=None):
        props = {
            "TrackerName": name,
            "Description": description,
            "Tags": [{"Key": "env", "Value": "test"}],
        }
        if kms_key_id:
            props["KmsKeyId"] = kms_key_id
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Tracker": {"Type": "AWS::Location::Tracker", "Properties": props},
            },
            "Outputs": {
                "Name": {"Value": {"Ref": "Tracker"}},
                "Arn": {"Value": {"Fn::GetAtt": ["Tracker", "Arn"]}},
                "TrackerArn": {"Value": {"Fn::GetAtt": ["Tracker", "TrackerArn"]}},
                "UpdateTime": {"Value": {"Fn::GetAtt": ["Tracker", "UpdateTime"]}},
            },
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(first, "v1"),
                     Tags=[{"Key": "owner", "Value": "team-a"}])
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == first
        assert _output(stack, "Arn").endswith(f":tracker/{first}")
        # The resource exposes both Arn and TrackerArn, carrying the same value.
        assert _output(stack, "TrackerArn") == _output(stack, "Arn")
        assert _output(stack, "UpdateTime").endswith("Z")

        described = location.describe_tracker(TrackerName=first)
        assert described["Description"] == "v1"
        assert described["TrackerArn"] == _output(stack, "Arn")
        assert described["Tags"] == {
            "env": "test", "owner": "team-a", **_system_tags(stack, "Tracker"),
        }

        cfn.update_stack(StackName=stack_name, TemplateBody=template(first, "v2"),
                         Tags=[{"Key": "owner", "Value": "team-b"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        resources = cfn.list_stack_resources(StackName=stack_name)["StackResourceSummaries"]
        assert resources[0]["PhysicalResourceId"] == first
        described = location.describe_tracker(TrackerName=first)
        assert described["Description"] == "v2"
        assert _template_tags(described["Tags"]) == {"env": "test", "owner": "team-b"}

        # KmsKeyId requires replacement, which AWS refuses for a custom-named
        # resource; the tracker stays as it was.
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=template(first, "v2", kms_key_id="alias/probe"),
                         Tags=[{"Key": "owner", "Value": "team-b"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        assert "custom-named resource requires replacing" in _stack_event_reasons(cfn, stack_name)
        assert "KmsKeyId" not in location.describe_tracker(TrackerName=first)

        # A rename is a replacement: the new tracker is created, the old one removed.
        cfn.update_stack(StackName=stack_name, TemplateBody=template(second, "v2"),
                         Tags=[{"Key": "owner", "Value": "team-b"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == second
        assert location.describe_tracker(TrackerName=second)["Description"] == "v2"
        with pytest.raises(ClientError):
            location.describe_tracker(TrackerName=first)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError):
        location.describe_tracker(TrackerName=second)


def test_cfn_location_tracker_generated_name(cfn, location):
    """Without TrackerName the tracker gets the stack-derived physical name;
    PositionFiltering, EventBridgeEnabled and KmsKeyEnableGeospatialQueries
    update in place and revert to what a fresh create has (the last one
    absent) when the template drops them; a KmsKeyId change replaces the
    auto-named tracker under the same name, so its positions are gone."""
    stack_name = f"cfn-loc-gen-{_uuid_mod.uuid4().hex[:8]}"

    def template(props):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Tracker": {"Type": "AWS::Location::Tracker", "Properties": props},
            },
            "Outputs": {"Name": {"Value": {"Ref": "Tracker"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template({}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        name = _output(stack, "Name")
        assert name.startswith(f"{stack_name}-Tracker-")
        described = location.describe_tracker(TrackerName=name)
        assert described["PositionFiltering"] == "TimeBased"
        assert described["EventBridgeEnabled"] is False
        assert "KmsKeyEnableGeospatialQueries" not in described

        cfn.update_stack(StackName=stack_name, TemplateBody=template({
            "Description": "in place",
            "PositionFiltering": "DistanceBased",
            "EventBridgeEnabled": True,
            "KmsKeyEnableGeospatialQueries": True,
        }))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == name
        described = location.describe_tracker(TrackerName=name)
        assert described["Description"] == "in place"
        assert described["PositionFiltering"] == "DistanceBased"
        assert described["EventBridgeEnabled"] is True
        assert described["KmsKeyEnableGeospatialQueries"] is True

        # Dropping the properties reverts them to what a fresh create has.
        cfn.update_stack(StackName=stack_name, TemplateBody=template({}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        described = location.describe_tracker(TrackerName=name)
        assert described["Description"] == ""
        assert described["PositionFiltering"] == "TimeBased"
        assert described["EventBridgeEnabled"] is False
        assert "KmsKeyEnableGeospatialQueries" not in described

        location.batch_update_device_position(
            TrackerName=name,
            Updates=[{"DeviceId": "veh-1", "Position": [1.0, 1.0],
                      "SampleTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}],
        )
        # KmsKeyId requires replacement: the auto-named tracker is re-created
        # under the same physical name, without its positions.
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=template({"KmsKeyId": "alias/probe"}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == name
        assert location.describe_tracker(TrackerName=name)["KmsKeyId"] == "alias/probe"
        with pytest.raises(ClientError):
            location.get_device_position(TrackerName=name, DeviceId="veh-1")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


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


def test_cfn_eventbus_survives_unrelated_update(cfn, eb, sqs):
    """A stack update must not fail an unchanged AWS::Events::EventBus.

    EventBus has no update handler, so an update falls back to calling
    create again — a name that already exists (its own, from the previous
    deploy — e.g. a name computed client-side and baked into the template,
    like CDK's EventBus construct default-names its bus) previously made
    every update of a stack containing one fail with "already exists", even
    when nothing about the bus itself changed. Fixed generically (see
    _update_resource's no-op short-circuit), not with EventBus-specific
    logic — this exercises that general fix against a real resource type
    known to hit it."""
    def template(queue_name):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Bus": {
                    "Type": "AWS::Events::EventBus",
                    "Properties": {"Name": "cfn-eb-t10"},
                },
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": queue_name},
                },
            },
        })

    cfn.create_stack(StackName="cfn-eb-t10", TemplateBody=template("cfn-eb-t10-q1"))
    stack = _wait_stack(cfn, "cfn-eb-t10")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    bus_before = eb.describe_event_bus(Name="cfn-eb-t10")

    # Only the queue changes — the bus is untouched, exactly like a real
    # redeploy that doesn't touch AuditTrail at all.
    cfn.update_stack(StackName="cfn-eb-t10", TemplateBody=template("cfn-eb-t10-q2"))
    stack = _wait_stack(cfn, "cfn-eb-t10")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    bus_after = eb.describe_event_bus(Name="cfn-eb-t10")
    assert bus_after["Arn"] == bus_before["Arn"]
    urls = sqs.list_queues(QueueNamePrefix="cfn-eb-t10-q2").get("QueueUrls", [])
    assert any("cfn-eb-t10-q2" in u for u in urls)


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


# --- Tags: the Tags property of the common types, stack-level tags ---

def _cfn_tag_template(tags):
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"Tags": tags}},
            "Topic": {"Type": "AWS::SNS::Topic", "Properties": {"Tags": tags}},
            "Table": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    "Tags": tags,
                },
            },
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/cfn-tags-role",
                    "Code": {"ZipFile": "def handler(e, c): return {}"},
                    "Tags": tags,
                },
            },
            "Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"Tags": tags}},
            "Stream": {
                "Type": "AWS::Kinesis::Stream",
                "Properties": {"ShardCount": 1, "Tags": tags},
            },
        },
        "Outputs": {
            "QueueUrl": {"Value": {"Ref": "Queue"}},
            "TopicArn": {"Value": {"Ref": "Topic"}},
            "TableArn": {"Value": {"Fn::GetAtt": ["Table", "Arn"]}},
            "FnArn": {"Value": {"Fn::GetAtt": ["Fn", "Arn"]}},
            "LogGroup": {"Value": {"Ref": "Logs"}},
            "StreamName": {"Value": {"Ref": "Stream"}},
        },
    }


def _cfn_tag_readback(sqs, sns, ddb, lam, logs, kin, stack):
    """The tags each service reports for the resources of `stack`, keyed by
    logical id, as {key: value} maps."""
    return {
        "Queue": sqs.list_queue_tags(QueueUrl=_output(stack, "QueueUrl")).get("Tags", {}),
        "Topic": {
            t["Key"]: t["Value"]
            for t in sns.list_tags_for_resource(ResourceArn=_output(stack, "TopicArn"))["Tags"]
        },
        "Table": {
            t["Key"]: t["Value"]
            for t in ddb.list_tags_of_resource(ResourceArn=_output(stack, "TableArn"))["Tags"]
        },
        "Fn": lam.list_tags(Resource=_output(stack, "FnArn"))["Tags"],
        "Logs": logs.list_tags_log_group(logGroupName=_output(stack, "LogGroup"))["tags"],
        "Stream": {
            t["Key"]: t["Value"]
            for t in kin.list_tags_for_stream(StreamName=_output(stack, "StreamName"))["Tags"]
        },
    }


def test_cfn_resource_tags_reach_the_service(cfn, sqs, sns, ddb, lam, logs, kin):
    """The Tags property of a queue, topic, table, function, log group and
    stream is stored where the service's own tag API reads it, and a template
    change to it is reconciled on update without touching tags added through
    that API."""
    name = f"cfn-res-tags-{_uuid_mod.uuid4().hex[:8]}"
    first = [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}]
    cfn.create_stack(StackName=name, TemplateBody=json.dumps(_cfn_tag_template(first)))
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        seen = _cfn_tag_readback(sqs, sns, ddb, lam, logs, kin, stack)
        for logical_id, tags in seen.items():
            assert _template_tags(tags) == {"env": "test", "team": "platform"}, logical_id

        # A tag set through the service API is not CloudFormation's to remove.
        sqs.tag_queue(QueueUrl=_output(stack, "QueueUrl"), Tags={"manual": "yes"})
        lam.tag_resource(Resource=_output(stack, "FnArn"), Tags={"manual": "yes"})

        second = [{"Key": "env", "Value": "prod"}, {"Key": "owner", "Value": "ops"}]
        cfn.update_stack(StackName=name, TemplateBody=json.dumps(_cfn_tag_template(second)))
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE"
        seen = _cfn_tag_readback(sqs, sns, ddb, lam, logs, kin, stack)
        for logical_id in ("Topic", "Table", "Logs", "Stream"):
            assert _template_tags(seen[logical_id]) == {"env": "prod", "owner": "ops"}, logical_id
        assert _template_tags(seen["Queue"]) == {"env": "prod", "owner": "ops", "manual": "yes"}
        assert _template_tags(seen["Fn"]) == {"env": "prod", "owner": "ops", "manual": "yes"}
    finally:
        _delete_cfn_test_stack(cfn, name)


def _system_tags(stack, logical_id):
    return {
        "aws:cloudformation:stack-name": stack["StackName"],
        "aws:cloudformation:stack-id": stack["StackId"],
        "aws:cloudformation:logical-id": logical_id,
    }


def test_cfn_stack_tags_reach_the_resources(cfn, sqs, ssm):
    """Stack-level tags and the three aws:cloudformation tags land on the
    resources (a list-shaped Tags property and a map-shaped one), the
    template's own tag wins on a shared key, and a stack-tag change on update
    reaches the resources."""
    name = f"cfn-stack-tags-{_uuid_mod.uuid4().hex[:8]}"
    template = {
        "Resources": {
            "Q": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"Tags": [{"Key": "env", "Value": "template"}]},
            },
            "P": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": f"/{name}/p",
                    "Type": "String",
                    "Value": "v",
                    "Tags": {"tier": "gold"},
                },
            },
        },
        "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}},
    }
    cfn.create_stack(
        StackName=name,
        TemplateBody=json.dumps(template),
        Tags=[{"Key": "owner", "Value": "team-a"}, {"Key": "env", "Value": "stack"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = _output(stack, "QueueUrl")
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {
            "env": "template",
            "owner": "team-a",
            **_system_tags(stack, "Q"),
        }
        param_tags = {
            t["Key"]: t["Value"]
            for t in ssm.list_tags_for_resource(
                ResourceType="Parameter", ResourceId=f"/{name}/p"
            )["TagList"]
        }
        assert param_tags == {
            "tier": "gold",
            "owner": "team-a",
            "env": "stack",
            **_system_tags(stack, "P"),
        }
        # An unchanged template with changed stack tags is an update (the tag
        # change reaches the resources); the same tags again are refused.
        with pytest.raises(ClientError, match="No updates are to be performed"):
            cfn.update_stack(
                StackName=name,
                UsePreviousTemplate=True,
                Tags=[{"Key": "owner", "Value": "team-a"}, {"Key": "env", "Value": "stack"}],
            )

        cfn.update_stack(
            StackName=name,
            UsePreviousTemplate=True,
            Tags=[{"Key": "owner", "Value": "team-b"}, {"Key": "cost", "Value": "42"}],
        )
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {
            "env": "template",
            "owner": "team-b",
            "cost": "42",
            **_system_tags(stack, "Q"),
        }
        param_tags = {
            t["Key"]: t["Value"]
            for t in ssm.list_tags_for_resource(
                ResourceType="Parameter", ResourceId=f"/{name}/p"
            )["TagList"]
        }
        assert param_tags == {
            "tier": "gold",
            "owner": "team-b",
            "cost": "42",
            **_system_tags(stack, "P"),
        }
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tags_reach_a_nested_stack(cfn, s3, sqs):
    """The parent's stack tags reach the resources of a nested stack; the
    aws:cloudformation tags carry the nested stack's own name and id."""
    suffix = _uuid_mod.uuid4().hex[:8]
    templates_bucket = f"cfn-tags-templates-{suffix}"
    child_template = {
        "Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
        "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}},
    }
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    parent_template = {
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {"TemplateURL": f"{endpoint}/{templates_bucket}/child.json"},
            },
        },
        "Outputs": {
            "QueueUrl": {"Value": {"Fn::GetAtt": ["Nested", "Outputs.QueueUrl"]}},
            "NestedId": {"Value": {"Ref": "Nested"}},
        },
    }
    parent_name = f"cfn-tags-parent-{suffix}"
    s3.create_bucket(Bucket=templates_bucket)
    try:
        s3.put_object(Bucket=templates_bucket, Key="child.json",
                      Body=json.dumps(child_template).encode())
        cfn.create_stack(
            StackName=parent_name,
            TemplateBody=json.dumps(parent_template),
            Tags=[{"Key": "owner", "Value": "team-a"}],
        )
        stack = _wait_stack(cfn, parent_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        nested = cfn.describe_stacks(StackName=_output(stack, "NestedId"))["Stacks"][0]
        assert nested["Tags"] == [{"Key": "owner", "Value": "team-a"}]
        tags = sqs.list_queue_tags(QueueUrl=_output(stack, "QueueUrl"))["Tags"]
        assert tags == {"owner": "team-a", **_system_tags(nested, "Q")}
    finally:
        _delete_cfn_test_stack(cfn, parent_name)
        s3.delete_object(Bucket=templates_bucket, Key="child.json")
        s3.delete_bucket(Bucket=templates_bucket)


def test_cfn_change_set_keeps_tagged_resources_without_update_handler(cfn, sqs):
    """Executing an UPDATE change set on a tagged stack does not re-create the
    tagged resources: the snapshot the execution compares against carries the
    stack's tags, so a type without an update handler (an HTTP API) keeps its
    physical id while the queue next to it changes."""
    name = f"cfn-cs-tags-{_uuid_mod.uuid4().hex[:8]}"

    def template(visibility):
        return json.dumps({
            "Resources": {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"VisibilityTimeout": visibility},
                },
                "Api": {
                    "Type": "AWS::ApiGatewayV2::Api",
                    "Properties": {"Name": f"{name}-api", "ProtocolType": "HTTP"},
                },
            },
            "Outputs": {
                "ApiId": {"Value": {"Ref": "Api"}},
                "QueueUrl": {"Value": {"Ref": "Q"}},
            },
        })

    cfn.create_stack(
        StackName=name, TemplateBody=template(30),
        Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        api_id = _output(stack, "ApiId")
        queue_url = _output(stack, "QueueUrl")

        cfn.create_change_set(
            StackName=name, ChangeSetName="visibility", TemplateBody=template(60),
        )
        described = cfn.describe_change_set(ChangeSetName="visibility", StackName=name)
        assert [c["ResourceChange"]["LogicalResourceId"] for c in described["Changes"]] == ["Q"]
        cfn.execute_change_set(ChangeSetName="visibility", StackName=name)
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "ApiId") == api_id
        attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]
        assert attributes["VisibilityTimeout"] == "60"
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {
            "owner": "team-a", **_system_tags(stack, "Q")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tag_change_keeps_a_resource_without_update_handler(cfn):
    """A stack-tag change is an update; a tagged type without an update
    handler (a certificate) keeps its ARN instead of being re-created."""
    name = f"cfn-tag-only-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {
            "Cert": {
                "Type": "AWS::CertificateManager::Certificate",
                "Properties": {"DomainName": f"{name}.example.local", "ValidationMethod": "DNS"},
            },
        },
        "Outputs": {"CertArn": {"Value": {"Ref": "Cert"}}},
    })
    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "CertArn")
        cfn.update_stack(
            StackName=name, UsePreviousTemplate=True,
            Tags=[{"Key": "owner", "Value": "team-b"}],
        )
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "CertArn") == arn
        assert stack["Tags"] == [{"Key": "owner", "Value": "team-b"}]
        detail = cfn.describe_stack_resource(StackName=name, LogicalResourceId="Cert")
        assert detail["StackResourceDetail"]["PhysicalResourceId"] == arn
        # Without an update handler the certificate keeps the tags it was
        # created with; the changed stack tag does not reach it.
        acm = _regional_cfn_test_client("acm", cfn.meta.region_name)
        cert_tags = {t["Key"]: t["Value"] for t in acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]}
        assert cert_tags == {"owner": "team-a", **_system_tags(stack, "Cert")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_secret_and_parameter_manual_tags_survive_a_stack_update(cfn, sm, ssm):
    """Tags added through TagResource / AddTagsToResource on a secret and a
    parameter the template never tagged are kept by a stack update, next to
    the stack tags and the aws:cloudformation tags."""
    name = f"cfn-manual-tags-{_uuid_mod.uuid4().hex[:8]}"

    def template(description, value):
        return json.dumps({
            "Resources": {
                "Secret": {
                    "Type": "AWS::SecretsManager::Secret",
                    "Properties": {
                        "Name": f"{name}-secret",
                        "Description": description,
                        "SecretString": "s3cret",
                    },
                },
                "Param": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {"Name": f"/{name}/p", "Type": "String", "Value": value},
                },
            },
            "Outputs": {"SecretArn": {"Value": {"Ref": "Secret"}}},
        })

    cfn.create_stack(
        StackName=name, TemplateBody=template("one", "v1"),
        Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        secret_arn = _output(stack, "SecretArn")
        sm.tag_resource(SecretId=secret_arn, Tags=[{"Key": "manual", "Value": "yes"}])
        ssm.add_tags_to_resource(
            ResourceType="Parameter", ResourceId=f"/{name}/p",
            Tags=[{"Key": "manual", "Value": "yes"}],
        )

        cfn.update_stack(StackName=name, TemplateBody=template("two", "v2"))
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        secret = sm.describe_secret(SecretId=secret_arn)
        assert secret["Description"] == "two"
        assert {t["Key"]: t["Value"] for t in secret["Tags"]} == {
            "manual": "yes", "owner": "team-a", **_system_tags(stack, "Secret")}
        assert ssm.get_parameter(Name=f"/{name}/p")["Parameter"]["Value"] == "v2"
        param_tags = ssm.list_tags_for_resource(
            ResourceType="Parameter", ResourceId=f"/{name}/p")["TagList"]
        assert {t["Key"]: t["Value"] for t in param_tags} == {
            "manual": "yes", "owner": "team-a", **_system_tags(stack, "Param")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tag_change_reaches_an_event_bus_and_an_api_key(cfn, eb, apigw_v1):
    """Types whose update handler used to ignore the tag property (an event
    bus, an API key) receive a stack-tag change on update."""
    name = f"cfn-tag-upd-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {
            "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": f"{name}-bus"}},
            "Key": {
                "Type": "AWS::ApiGateway::ApiKey",
                "Properties": {"Name": f"{name}-key", "Enabled": True},
            },
        },
        "Outputs": {
            "BusArn": {"Value": {"Fn::GetAtt": ["Bus", "Arn"]}},
            "KeyId": {"Value": {"Ref": "Key"}},
        },
    })

    def bus_tags(arn):
        return {t["Key"]: t["Value"] for t in eb.list_tags_for_resource(ResourceARN=arn)["Tags"]}

    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        bus_arn = _output(stack, "BusArn")
        key_id = _output(stack, "KeyId")
        assert bus_tags(bus_arn) == {"owner": "team-a", **_system_tags(stack, "Bus")}
        assert apigw_v1.get_api_key(apiKey=key_id)["tags"] == {
            "owner": "team-a", **_system_tags(stack, "Key")}

        cfn.update_stack(
            StackName=name, UsePreviousTemplate=True,
            Tags=[{"Key": "owner", "Value": "team-b"}, {"Key": "cost", "Value": "42"}],
        )
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "KeyId") == key_id
        expected = {"owner": "team-b", "cost": "42"}
        assert bus_tags(bus_arn) == {**expected, **_system_tags(stack, "Bus")}
        assert apigw_v1.get_api_key(apiKey=key_id)["tags"] == {
            **expected, **_system_tags(stack, "Key")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tags_reach_the_map_shaped_tag_properties(cfn, cognito_idp, cognito_identity):
    """The map-shaped properties receive the stack tags too: UserPoolTags,
    IdentityPoolTags, BackupVaultTags and BackupPlanTags, each read back
    through the service's own tag API."""
    name = f"cfn-map-tags-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {
            "Pool": {
                "Type": "AWS::Cognito::UserPool",
                "Properties": {"UserPoolName": f"{name}-pool", "UserPoolTags": {"tier": "gold"}},
            },
            "IdPool": {
                "Type": "AWS::Cognito::IdentityPool",
                "Properties": {
                    "IdentityPoolName": f"{name}-idpool",
                    "AllowUnauthenticatedIdentities": True,
                    "IdentityPoolTags": {"tier": "silver"},
                },
            },
            "Vault": {
                "Type": "AWS::Backup::BackupVault",
                "Properties": {"BackupVaultName": f"{name}-vault", "BackupVaultTags": {"tier": "bronze"}},
            },
            "Plan": {
                "Type": "AWS::Backup::BackupPlan",
                "Properties": {
                    "BackupPlan": {
                        "BackupPlanName": f"{name}-plan",
                        "BackupPlanRule": [{
                            "RuleName": "daily",
                            "TargetBackupVault": {"Ref": "Vault"},
                            "ScheduleExpression": "cron(0 5 ? * * *)",
                        }],
                    },
                    "BackupPlanTags": {"tier": "iron"},
                },
            },
        },
        "Outputs": {
            "PoolArn": {"Value": {"Fn::GetAtt": ["Pool", "Arn"]}},
            "IdPoolId": {"Value": {"Ref": "IdPool"}},
            "VaultArn": {"Value": {"Fn::GetAtt": ["Vault", "BackupVaultArn"]}},
            "PlanArn": {"Value": {"Fn::GetAtt": ["Plan", "BackupPlanArn"]}},
        },
    })
    backup = _regional_cfn_test_client("backup", cfn.meta.region_name)
    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_tags = cognito_idp.list_tags_for_resource(ResourceArn=_output(stack, "PoolArn"))["Tags"]
        assert pool_tags == {"tier": "gold", "owner": "team-a", **_system_tags(stack, "Pool")}
        idpool_arn = (
            f"arn:aws:cognito-identity:{cfn.meta.region_name}:000000000000:"
            f"identitypool/{_output(stack, 'IdPoolId')}"
        )
        idpool_tags = cognito_identity.list_tags_for_resource(ResourceArn=idpool_arn)["Tags"]
        assert idpool_tags == {"tier": "silver", "owner": "team-a", **_system_tags(stack, "IdPool")}
        vault_tags = backup.list_tags(ResourceArn=_output(stack, "VaultArn"))["Tags"]
        assert vault_tags == {"tier": "bronze", "owner": "team-a", **_system_tags(stack, "Vault")}
        plan_tags = backup.list_tags(ResourceArn=_output(stack, "PlanArn"))["Tags"]
        assert plan_tags == {"tier": "iron", "owner": "team-a", **_system_tags(stack, "Plan")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tags_map_valued_tags_and_eks_nodegroup(cfn, sqs, eks):
    """An EKS node group's Tags is a map on AWS and is stored as one; a
    map-valued Tags on a list-shaped type (a queue) is accepted as well."""
    name = f"cfn-eks-tags-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {
            "Cluster": {
                "Type": "AWS::EKS::Cluster",
                "Properties": {
                    "Name": f"{name}-cluster",
                    "RoleArn": "arn:aws:iam::000000000000:role/eks-role",
                    "ResourcesVpcConfig": {"SubnetIds": ["subnet-1", "subnet-2"]},
                },
            },
            "Nodes": {
                "Type": "AWS::EKS::Nodegroup",
                "Properties": {
                    "ClusterName": {"Ref": "Cluster"},
                    "NodegroupName": f"{name}-nodes",
                    "NodeRole": "arn:aws:iam::000000000000:role/eks-node-role",
                    "Subnets": ["subnet-1"],
                    "Tags": {"tier": "gold"},
                },
            },
            "Q": {"Type": "AWS::SQS::Queue", "Properties": {"Tags": {"env": "map"}}},
        },
        "Outputs": {
            "NodesArn": {"Value": {"Fn::GetAtt": ["Nodes", "Arn"]}},
            "QueueUrl": {"Value": {"Ref": "Q"}},
        },
    })
    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        node_tags = eks.list_tags_for_resource(resourceArn=_output(stack, "NodesArn"))["tags"]
        assert node_tags == {"tier": "gold", "owner": "team-a", **_system_tags(stack, "Nodes")}
        assert sqs.list_queue_tags(QueueUrl=_output(stack, "QueueUrl"))["Tags"] == {
            "env": "map", "owner": "team-a", **_system_tags(stack, "Q")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_change_set_tags_replace_the_previous_stack_tags(cfn, sqs):
    """An UPDATE change set that carries new stack tags: after execute the
    queue carries the new tag and no longer the old one. The execution
    compares against a snapshot that holds the previous tags; without them
    the old tag would survive on the queue."""
    name = f"cfn-cs-retag-{_uuid_mod.uuid4().hex[:8]}"

    def template(visibility):
        return json.dumps({
            "Resources": {
                "Q": {"Type": "AWS::SQS::Queue", "Properties": {"VisibilityTimeout": visibility}},
            },
            "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}},
        })

    cfn.create_stack(
        StackName=name, TemplateBody=template(30), Tags=[{"Key": "phase", "Value": "a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = _output(stack, "QueueUrl")
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"]["phase"] == "a"

        cfn.create_change_set(
            StackName=name, ChangeSetName="retag", TemplateBody=template(60),
            Tags=[{"Key": "owner", "Value": "b"}],
        )
        cfn.execute_change_set(ChangeSetName="retag", StackName=name)
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert stack["Tags"] == [{"Key": "owner", "Value": "b"}]
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {
            "owner": "b", **_system_tags(stack, "Q")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_update_keeps_foreign_tags_on_table_and_project(cfn, ddb, codebuild, apigw_v1):
    """Tags set outside the stack on a table (TagResource) and on a CodeBuild
    project (UpdateProject) survive a stack-tag change, which also reaches a
    usage plan through its update handler."""
    name = f"cfn-foreign-tags-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {
            "Table": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            },
            "Project": {
                "Type": "AWS::CodeBuild::Project",
                "Properties": {
                    "Name": f"{name}-project",
                    "Source": {"Type": "NO_SOURCE"},
                    "Artifacts": {"Type": "NO_ARTIFACTS"},
                    "Environment": {
                        "Type": "LINUX_CONTAINER",
                        "Image": "aws/codebuild/standard:7.0",
                        "ComputeType": "BUILD_GENERAL1_SMALL",
                    },
                    "ServiceRole": "arn:aws:iam::000000000000:role/codebuild-role",
                },
            },
            "Plan": {
                "Type": "AWS::ApiGateway::UsagePlan",
                "Properties": {"UsagePlanName": f"{name}-plan", "Description": "plan"},
            },
        },
        "Outputs": {
            "TableArn": {"Value": {"Fn::GetAtt": ["Table", "Arn"]}},
            "PlanId": {"Value": {"Ref": "Plan"}},
        },
    })
    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        table_arn = _output(stack, "TableArn")
        plan_id = _output(stack, "PlanId")
        ddb.tag_resource(ResourceArn=table_arn, Tags=[{"Key": "manual", "Value": "yes"}])
        project = codebuild.batch_get_projects(names=[f"{name}-project"])["projects"][0]
        codebuild.update_project(
            name=f"{name}-project",
            tags=project["tags"] + [{"key": "manual", "value": "yes"}],
        )
        assert apigw_v1.get_usage_plan(usagePlanId=plan_id)["tags"] == {
            "owner": "team-a", **_system_tags(stack, "Plan")}

        cfn.update_stack(
            StackName=name, UsePreviousTemplate=True,
            Tags=[{"Key": "owner", "Value": "team-b"}],
        )
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        table_tags = {t["Key"]: t["Value"] for t in ddb.list_tags_of_resource(ResourceArn=table_arn)["Tags"]}
        assert table_tags == {"manual": "yes", "owner": "team-b", **_system_tags(stack, "Table")}
        project = codebuild.batch_get_projects(names=[f"{name}-project"])["projects"][0]
        assert {t["key"]: t["value"] for t in project["tags"]} == {
            "manual": "yes", "owner": "team-b", **_system_tags(stack, "Project")}
        assert apigw_v1.get_usage_plan(usagePlanId=plan_id)["tags"] == {
            "owner": "team-b", **_system_tags(stack, "Plan")}
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_stack_tags_update_reaches_a_nested_stack(cfn, s3, sqs):
    """A stack-tag change on the parent reaches the resources of the nested
    stack: the child's queue is updated against the tags it had before, and
    the child's own Tags never carry the parent's aws: keys."""
    suffix = _uuid_mod.uuid4().hex[:8]
    templates_bucket = f"cfn-tags-upd-templates-{suffix}"
    child_template = {
        "Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
        "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}},
    }
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    parent_template = json.dumps({
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {"TemplateURL": f"{endpoint}/{templates_bucket}/child.json"},
            },
        },
        "Outputs": {
            "QueueUrl": {"Value": {"Fn::GetAtt": ["Nested", "Outputs.QueueUrl"]}},
            "NestedId": {"Value": {"Ref": "Nested"}},
        },
    })
    parent_name = f"cfn-tags-upd-parent-{suffix}"
    s3.create_bucket(Bucket=templates_bucket)
    try:
        s3.put_object(Bucket=templates_bucket, Key="child.json",
                      Body=json.dumps(child_template).encode())
        cfn.create_stack(
            StackName=parent_name, TemplateBody=parent_template,
            Tags=[{"Key": "owner", "Value": "team-a"}],
        )
        stack = _wait_stack(cfn, parent_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = _output(stack, "QueueUrl")
        nested_id = _output(stack, "NestedId")

        cfn.update_stack(
            StackName=parent_name, UsePreviousTemplate=True,
            Tags=[{"Key": "owner", "Value": "team-b"}, {"Key": "cost", "Value": "42"}],
        )
        stack = _wait_stack(cfn, parent_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "NestedId") == nested_id
        nested = cfn.describe_stacks(StackName=nested_id)["Stacks"][0]
        assert nested["Tags"] == [{"Key": "owner", "Value": "team-b"}, {"Key": "cost", "Value": "42"}]
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {
            "owner": "team-b", "cost": "42", **_system_tags(nested, "Q")}
    finally:
        _delete_cfn_test_stack(cfn, parent_name)
        s3.delete_object(Bucket=templates_bucket, Key="child.json")
        s3.delete_bucket(Bucket=templates_bucket)


def test_cfn_stack_tags_are_validated_and_an_empty_list_clears_them(cfn, sqs):
    """More than 50 tags and an aws: prefixed key are refused before a stack
    exists; Tags=[] on UpdateStack removes the stack's tags, from the stack
    and from the queue, as the API documents for an empty value."""
    name = f"cfn-tag-rules-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
        "Outputs": {"QueueUrl": {"Value": {"Ref": "Q"}}},
    })
    with pytest.raises(ClientError, match="maximum number of 50 tags"):
        cfn.create_stack(
            StackName=name, TemplateBody=template,
            Tags=[{"Key": f"k{i}", "Value": "v"} for i in range(51)],
        )
    with pytest.raises(ClientError, match="aws:"):
        cfn.create_stack(
            StackName=name, TemplateBody=template,
            Tags=[{"Key": "aws:cloudformation:stack-name", "Value": "spoof"}],
        )
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=name)

    cfn.create_stack(
        StackName=name, TemplateBody=template, Tags=[{"Key": "owner", "Value": "team-a"}],
    )
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = _output(stack, "QueueUrl")
        with pytest.raises(ClientError, match="maximum number of 50 tags"):
            cfn.update_stack(
                StackName=name, UsePreviousTemplate=True,
                Tags=[{"Key": f"k{i}", "Value": "v"} for i in range(51)],
            )
        with pytest.raises(ClientError, match="aws:"):
            cfn.create_change_set(
                StackName=name, ChangeSetName="spoof", UsePreviousTemplate=True,
                Tags=[{"Key": "AWS:reserved", "Value": "spoof"}],
            )

        cfn.update_stack(StackName=name, UsePreviousTemplate=True, Tags=[])
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert stack.get("Tags", []) == []
        assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == _system_tags(stack, "Q")
        # The same empty list again is no update.
        with pytest.raises(ClientError, match="No updates are to be performed"):
            cfn.update_stack(StackName=name, UsePreviousTemplate=True, Tags=[])
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_aws_region_pseudo_param_uses_caller_region():
    """CFN's AWS::Region pseudo-param must resolve to the caller's request region,
    not MINISTACK_REGION (issue #398 — CDK bootstrap resources inheriting wrong region)."""
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

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


def test_cfn_cognito_resources_use_the_stack_region():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    def _client(service, region):
        return boto3.client(
            service,
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(retries={"mode": "standard"}),
        )

    west_cfn = _client("cloudformation", "us-west-2")
    west_cognito = _client("cognito-idp", "us-west-2")
    east_cognito = _client("cognito-idp", "us-east-1")
    suffix = _uuid_mod.uuid4().hex[:10]
    stack_name = f"cfn-cognito-west-{suffix}"
    domain = f"cfn-cognito-west-{suffix}"
    template = f"""
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Pool:
    Type: AWS::Cognito::UserPool
    Properties:
      UserPoolName: {stack_name}
  Client:
    Type: AWS::Cognito::UserPoolClient
    Properties:
      UserPoolId: !Ref Pool
      ClientName: west-client
  Domain:
    Type: AWS::Cognito::UserPoolDomain
    Properties:
      UserPoolId: !Ref Pool
      Domain: {domain}
Outputs:
  PoolId:
    Value: !Ref Pool
  ClientId:
    Value: !Ref Client
"""

    west_cfn.create_stack(StackName=stack_name, TemplateBody=template)
    stack = _wait_stack(west_cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    outputs = {output["OutputKey"]: output["OutputValue"] for output in stack["Outputs"]}

    west_cognito.describe_user_pool(UserPoolId=outputs["PoolId"])
    west_cognito.describe_user_pool_client(
        UserPoolId=outputs["PoolId"], ClientId=outputs["ClientId"]
    )
    assert west_cognito.describe_user_pool_domain(Domain=domain)["DomainDescription"][
        "UserPoolId"
    ] == outputs["PoolId"]

    assert outputs["PoolId"] not in {
        pool["Id"] for pool in east_cognito.list_user_pools(MaxResults=60)["UserPools"]
    }
    with pytest.raises(ClientError) as exc:
        east_cognito.describe_user_pool(UserPoolId=outputs["PoolId"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
    _assert_apigwv2_api_not_found(lambda: apigw.get_integrations(ApiId=api_id))


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
    _assert_apigwv2_api_not_found(lambda: apigw.get_routes(ApiId=api_id))


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
    _assert_apigwv2_api_not_found(lambda: apigw.get_authorizers(ApiId=api_id))


def test_cfn_apigwv2_authorizer_update_trusts_additional_audience(cfn, apigw):
    """Updating an Authorizer's JwtConfiguration (e.g. trusting an
    additional app client's audience — hotshot's multi-app-support Phase B)
    must mutate the same authorizer in place, not mint a second one.

    Regression test: AWS::ApiGatewayV2::Authorizer had no update handler,
    so a property change fell back to create — whose authorizerId is a
    fresh random value every call (unlike name-based resources, there's no
    stable identity to derive) — leaving a second, orphaned authorizer
    while the Route's own (unchanged) AuthorizerId kept pointing at the
    original, now-stale one with the old audience list."""
    def template(audience):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "HttpApi": {
                    "Type": "AWS::ApiGatewayV2::Api",
                    "Properties": {"Name": "cfn-apigwv2-authorizer-t02", "ProtocolType": "HTTP"},
                },
                "JwtAuthorizer": {
                    "Type": "AWS::ApiGatewayV2::Authorizer",
                    "Properties": {
                        "ApiId": {"Ref": "HttpApi"},
                        "Name": "cfn-apigwv2-authorizer-t02-jwt",
                        "AuthorizerType": "JWT",
                        "IdentitySource": ["$request.header.Authorization"],
                        "JwtConfiguration": {
                            "Audience": audience,
                            "Issuer": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_example",
                        },
                    },
                },
            },
        })

    stack_name = "cfn-apigwv2-authorizer-t02"
    cfn.create_stack(StackName=stack_name, TemplateBody=template(["client-a"]))
    _wait_stack(cfn, stack_name)
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    api_id = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]["PhysicalResourceId"]
    authorizer_id_before = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Authorizer"][0]["PhysicalResourceId"]

    cfn.update_stack(StackName=stack_name, TemplateBody=template(["client-a", "client-b"]))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    authorizers = apigw.get_authorizers(ApiId=api_id)["Items"]
    assert len(authorizers) == 1
    assert authorizers[0]["AuthorizerId"] == authorizer_id_before
    assert authorizers[0]["JwtConfiguration"]["Audience"] == ["client-a", "client-b"]


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

    _assert_apigwv2_api_not_found(lambda: apigw.get_integrations(ApiId=api_id))
    _assert_apigwv2_api_not_found(lambda: apigw.get_routes(ApiId=api_id))


def test_cfn_apigwv2_full_http_api_stack_in_non_boot_region():
    """A region-B CloudFormation stack creates ApiGatewayV2 children in region B."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-apigwv2-west-{suffix}"
    west_cfn = _regional_cfn_test_client("cloudformation", "us-west-2")
    west_apigw = _regional_cfn_test_client("apigatewayv2", "us-west-2")
    east_apigw = _regional_cfn_test_client("apigatewayv2", "us-east-1")
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "HttpApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": f"{stack_name}-api",
                    "ProtocolType": "HTTP",
                },
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
                    "IntegrationUri": "arn:aws:lambda:us-west-2:000000000000:function:my-handler",
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
    api_id = None
    try:
        west_cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(west_cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        resources = west_cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        api_res = [r for r in resources if r["ResourceType"] == "AWS::ApiGatewayV2::Api"][0]
        api_id = api_res["PhysicalResourceId"]

        assert len(west_apigw.get_integrations(ApiId=api_id)["Items"]) == 1
        assert len(west_apigw.get_routes(ApiId=api_id)["Items"]) == 1
        assert len(west_apigw.get_stages(ApiId=api_id)["Items"]) == 1
        _assert_apigwv2_api_not_found(lambda: east_apigw.get_routes(ApiId=api_id))
    finally:
        _delete_cfn_test_stack(west_cfn, stack_name)

    if api_id is not None:
        _assert_apigwv2_api_not_found(lambda: west_apigw.get_routes(ApiId=api_id))


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

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
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



_FUNCTION_CODE = """
function handler(event) {
    var request = event.request;
    request.headers['x-provisioned-by'] = {value: 'cloudformation'};
    return request;
}
"""


def _cloudfront_template(suffix):
    return json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CachePolicy": {"Type": "AWS::CloudFront::CachePolicy", "Properties": {
                "CachePolicyConfig": {
                    "Name": f"cfn-cache-{suffix}",
                    "DefaultTTL": 3600, "MaxTTL": 86400, "MinTTL": 1,
                    "ParametersInCacheKeyAndForwardedToOrigin": {
                        "EnableAcceptEncodingGzip": True,
                        "EnableAcceptEncodingBrotli": True,
                        # CDK emits cache-policy lists bare, without an Items wrapper.
                        "HeadersConfig": {"HeaderBehavior": "whitelist",
                                          "Headers": ["X-Service", "X-Query"]},
                        "QueryStringsConfig": {"QueryStringBehavior": "whitelist",
                                               "QueryStrings": ["page"]},
                        "CookiesConfig": {"CookieBehavior": "none"},
                    },
                },
            }},
            "OriginRequestPolicy": {"Type": "AWS::CloudFront::OriginRequestPolicy", "Properties": {
                "OriginRequestPolicyConfig": {
                    "Name": f"cfn-orp-{suffix}",
                    "HeadersConfig": {"HeaderBehavior": "whitelist",
                                      "Headers": ["X-Tenant-Key", "X-Service"]},
                    "QueryStringsConfig": {"QueryStringBehavior": "all"},
                    "CookiesConfig": {"CookieBehavior": "none"},
                },
            }},
            "ResponseHeadersPolicy": {"Type": "AWS::CloudFront::ResponseHeadersPolicy", "Properties": {
                "ResponseHeadersPolicyConfig": {
                    "Name": f"cfn-rhp-{suffix}",
                    # ...while CORS lists arrive wrapped. Both shapes must land.
                    "CorsConfig": {
                        "AccessControlAllowCredentials": False,
                        "AccessControlAllowHeaders": {"Items": ["Authorization"]},
                        "AccessControlAllowMethods": {"Items": ["GET", "HEAD"]},
                        "AccessControlAllowOrigins": {"Items": ["https://example.test"]},
                        "AccessControlMaxAgeSec": 86400,
                        "OriginOverride": True,
                    },
                    "CustomHeadersConfig": {"Items": [
                        {"Header": "X-Env", "Value": "local", "Override": True},
                    ]},
                },
            }},
            "OriginAccessControl": {"Type": "AWS::CloudFront::OriginAccessControl", "Properties": {
                "OriginAccessControlConfig": {
                    "Name": f"cfn-oac-{suffix}",
                    "OriginAccessControlOriginType": "s3",
                    "SigningBehavior": "always",
                    "SigningProtocol": "sigv4",
                },
            }},
            "Function": {"Type": "AWS::CloudFront::Function", "Properties": {
                "Name": f"cfn-fn-{suffix}",
                "AutoPublish": True,
                "FunctionCode": _FUNCTION_CODE,
                "FunctionConfig": {"Comment": "provisioned by cfn",
                                   "Runtime": "cloudfront-js-2.0"},
            }},
        },
        "Outputs": {
            "CachePolicyRef": {"Value": {"Ref": "CachePolicy"}},
            "OrpRef": {"Value": {"Ref": "OriginRequestPolicy"}},
            "RhpRef": {"Value": {"Ref": "ResponseHeadersPolicy"}},
            "OacId": {"Value": {"Fn::GetAtt": ["OriginAccessControl", "Id"]}},
            "FunctionArn": {"Value": {"Fn::GetAtt": ["Function", "FunctionARN"]}},
        },
    })


def test_cfn_cloudfront_policy_oac_and_function_provision(cfn, cloudfront):
    """The five CloudFront types with a complete API and no provisioner used to
    roll the stack back on "Unsupported resource type". Each now provisions onto
    the service's own parser, so every nested list — including the whitelists a
    hand-written mapper most easily loses — reads back through the CloudFront
    API exactly as CreateCachePolicy would have stored it."""
    suffix = "prov1"
    cfn.create_stack(StackName="cfn-cf-policies", TemplateBody=_cloudfront_template(suffix))
    stack = _wait_stack(cfn, "cfn-cf-policies")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    out = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}

    # Ref returns the policy Id for the three policy families (CFN spec), and
    # that Id addresses the object through the service's own API.
    cache_cfg = cloudfront.get_cache_policy(Id=out["CachePolicyRef"])["CachePolicy"]["CachePolicyConfig"]
    assert cache_cfg["Name"] == f"cfn-cache-{suffix}"
    assert cache_cfg["DefaultTTL"] == 3600
    params = cache_cfg["ParametersInCacheKeyAndForwardedToOrigin"]
    assert params["EnableAcceptEncodingGzip"] is True
    assert params["HeadersConfig"]["HeaderBehavior"] == "whitelist"
    assert params["HeadersConfig"]["Headers"]["Items"] == ["X-Service", "X-Query"]
    assert params["QueryStringsConfig"]["QueryStrings"]["Items"] == ["page"]

    orp_cfg = cloudfront.get_origin_request_policy(
        Id=out["OrpRef"])["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
    assert orp_cfg["HeadersConfig"]["Headers"]["Items"] == ["X-Tenant-Key", "X-Service"]
    assert orp_cfg["QueryStringsConfig"]["QueryStringBehavior"] == "all"

    rhp_cfg = cloudfront.get_response_headers_policy(
        Id=out["RhpRef"])["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
    cors = rhp_cfg["CorsConfig"]
    assert cors["AccessControlMaxAgeSec"] == 86400
    assert cors["AccessControlAllowMethods"]["Items"] == ["GET", "HEAD"]
    assert cors["AccessControlAllowOrigins"]["Items"] == ["https://example.test"]
    assert rhp_cfg["CustomHeadersConfig"]["Items"][0]["Header"] == "X-Env"

    oac_cfg = cloudfront.get_origin_access_control(
        Id=out["OacId"])["OriginAccessControl"]["OriginAccessControlConfig"]
    assert oac_cfg["SigningBehavior"] == "always"
    assert oac_cfg["OriginAccessControlOriginType"] == "s3"

    # The template sets AutoPublish: true explicitly (as CDK emits it), so the
    # function is published to LIVE; a created function defaults to the
    # DEVELOPMENT stage per the resource reference.
    summary = cloudfront.describe_function(
        Name=f"cfn-fn-{suffix}", Stage="LIVE")["FunctionSummary"]
    assert summary["FunctionMetadata"]["Stage"] == "LIVE"
    assert summary["FunctionMetadata"]["FunctionARN"] == out["FunctionArn"]

    cfn.delete_stack(StackName="cfn-cf-policies")
    _wait_stack(cfn, "cfn-cf-policies")
    with pytest.raises(ClientError):
        cloudfront.get_cache_policy(Id=out["CachePolicyRef"])


def test_cfn_cloudfront_function_without_autopublish_stays_in_development(cfn, cloudfront):
    """"By default, when you create a function, it's in the DEVELOPMENT stage"
    (AWS::CloudFront::Function reference): a template that omits AutoPublish
    gets an unpublished function — DESCRIBE at LIVE fails, DEVELOPMENT works."""
    name = "cfn-fn-devstage"
    template = json.dumps({"Resources": {"Fn": {
        "Type": "AWS::CloudFront::Function",
        "Properties": {"Name": name, "FunctionCode": _FUNCTION_CODE,
                       "FunctionConfig": {"Comment": "unpublished",
                                          "Runtime": "cloudfront-js-2.0"}},
    }}})
    cfn.create_stack(StackName="cfn-cf-fn-dev", TemplateBody=template)
    try:
        stack = _wait_stack(cfn, "cfn-cf-fn-dev")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        dev = cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")["FunctionSummary"]
        assert dev["FunctionMetadata"]["Stage"] == "DEVELOPMENT"
        with pytest.raises(ClientError):
            cloudfront.describe_function(Name=name, Stage="LIVE")
    finally:
        cfn.delete_stack(StackName="cfn-cf-fn-dev")
        _wait_stack(cfn, "cfn-cf-fn-dev")


# ---------------------------------------------------------------------------
# Updating the five CloudFront types. Every property of the three policy
# families and of the OAC is "Update requires: No interruption" in the resource
# references, so the object keeps its Id across every change; only
# AWS::CloudFront::Function has a Replacement property (Name).
# ---------------------------------------------------------------------------

def _cfn_cf_stack(cfn, stack_name, template, update=False):
    """Create or update one CloudFront stack and return its outputs."""
    body = json.dumps(template)
    if update:
        cfn.update_stack(StackName=stack_name, TemplateBody=body)
    else:
        cfn.create_stack(StackName=stack_name, TemplateBody=body)
    stack = _wait_stack(cfn, stack_name)
    expected = "UPDATE_COMPLETE" if update else "CREATE_COMPLETE"
    assert stack["StackStatus"] == expected, stack.get("StackStatusReason")
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def _cache_policy_template(name, comment=None, default_ttl=3600):
    config = {
        "Name": name,
        "DefaultTTL": default_ttl, "MaxTTL": 86400, "MinTTL": 1,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "EnableAcceptEncodingGzip": True,
            "HeadersConfig": {"HeaderBehavior": "whitelist", "Headers": ["X-Service"]},
            "QueryStringsConfig": {"QueryStringBehavior": "none"},
            "CookiesConfig": {"CookieBehavior": "none"},
        },
    }
    if comment is not None:
        config["Comment"] = comment
    return {
        "Resources": {"CachePolicy": {"Type": "AWS::CloudFront::CachePolicy",
                                      "Properties": {"CachePolicyConfig": config}}},
        "Outputs": {"Id": {"Value": {"Ref": "CachePolicy"}}},
    }


def test_cfn_cloudfront_cache_policy_updates_in_place(cfn, cloudfront):
    """AWS::CloudFront::CachePolicy has no Replacement property — CachePolicyConfig
    and every field under it are "No interruption" — so an UpdateStack keeps the
    policy Id that Ref handed to the distribution, a rename included, and a
    property the template drops falls back to its create default."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-cp-update-{uid}"
    name = f"cfn-cp-update-{uid}"
    renamed = f"cfn-cp-renamed-{uid}"
    policy_id = None
    try:
        out = _cfn_cf_stack(cfn, stack_name, _cache_policy_template(name, comment="v1"))
        policy_id = out["Id"]
        cfg = cloudfront.get_cache_policy(Id=policy_id)["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Comment"] == "v1"
        assert cfg["DefaultTTL"] == 3600

        # In-place change: same Id, new values.
        out = _cfn_cf_stack(cfn, stack_name,
                            _cache_policy_template(name, comment="v2", default_ttl=120),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_cache_policy(Id=policy_id)["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Comment"] == "v2"
        assert cfg["DefaultTTL"] == 120
        # The nested whitelist still round-trips after the update.
        params = cfg["ParametersInCacheKeyAndForwardedToOrigin"]
        assert params["HeadersConfig"]["Headers"]["Items"] == ["X-Service"]

        # A rename is "No interruption" too: same policy, new name, and the
        # account holds one policy rather than a replacement pair.
        out = _cfn_cf_stack(cfn, stack_name,
                            _cache_policy_template(renamed, comment="v2", default_ttl=120),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_cache_policy(Id=policy_id)["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Name"] == renamed
        names = [p["CachePolicy"]["CachePolicyConfig"]["Name"]
                 for p in cloudfront.list_cache_policies()["CachePolicyList"]["Items"]]
        assert names.count(renamed) == 1
        assert name not in names

        # Dropping Comment reverts it to the create default rather than
        # leaving the previous value behind.
        out = _cfn_cf_stack(cfn, stack_name,
                            _cache_policy_template(renamed, default_ttl=120),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_cache_policy(Id=policy_id)["CachePolicy"]["CachePolicyConfig"]
        assert cfg.get("Comment", "") == ""
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    if policy_id:
        with pytest.raises(ClientError):
            cloudfront.get_cache_policy(Id=policy_id)


def test_cfn_cloudfront_cache_policy_deleted_out_of_band_is_recreated(cfn, cloudfront):
    """The policy handlers take a different path than the function on an
    out-of-band delete: no record under the physical id sends the update
    straight to the create handler, which mints a new policy under the
    template's name rather than failing the stack. The new Id is what Ref
    hands out from then on."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-cp-gone-{uid}"
    name = f"cfn-cp-gone-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name, _cache_policy_template(name, comment="v1"))
        old_id = out["Id"]
        etag = cloudfront.get_cache_policy(Id=old_id)["ETag"]
        cloudfront.delete_cache_policy(Id=old_id, IfMatch=etag)
        with pytest.raises(ClientError):
            cloudfront.get_cache_policy(Id=old_id)

        out = _cfn_cf_stack(cfn, stack_name,
                            _cache_policy_template(name, comment="v2"), update=True)
        cfg = cloudfront.get_cache_policy(Id=out["Id"])["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Name"] == name
        assert cfg["Comment"] == "v2"
        names = [p["CachePolicy"]["CachePolicyConfig"]["Name"]
                 for p in cloudfront.list_cache_policies()["CachePolicyList"]["Items"]]
        assert names.count(name) == 1
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudfront_policy_rename_onto_a_taken_name_is_refused(cfn, cloudfront):
    """A cache policy renamed onto a name another policy holds is refused the way
    UpdateCachePolicy refuses it: the update rolls back, the stack's policy keeps
    its name and its Id, and the other policy is untouched."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-cp-taken-{uid}"
    name = f"cfn-cp-mine-{uid}"
    taken = f"cfn-cp-taken-{uid}"
    # The API shape, not the template's: the wire form carries Quantity/Items.
    other = cloudfront.create_cache_policy(CachePolicyConfig={
        "Name": taken, "MinTTL": 1, "DefaultTTL": 60, "MaxTTL": 120,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "EnableAcceptEncodingGzip": True,
            "HeadersConfig": {"HeaderBehavior": "none"},
            "QueryStringsConfig": {"QueryStringBehavior": "none"},
            "CookiesConfig": {"CookieBehavior": "none"},
        },
    })["CachePolicy"]["Id"]
    try:
        out = _cfn_cf_stack(cfn, stack_name, _cache_policy_template(name))
        policy_id = out["Id"]
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=json.dumps(_cache_policy_template(taken)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "already exists" in _stack_event_reasons(cfn, stack_name)
        cfg = cloudfront.get_cache_policy(Id=policy_id)["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Name"] == name
        cfg = cloudfront.get_cache_policy(Id=other)["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Name"] == taken
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        etag = cloudfront.get_cache_policy(Id=other)["ETag"]
        cloudfront.delete_cache_policy(Id=other, IfMatch=etag)


def _origin_request_policy_template(name, comment=None, query_behavior="all",
                                    headers=("X-Tenant-Key",)):
    config = {
        "Name": name,
        "HeadersConfig": {"HeaderBehavior": "whitelist", "Headers": list(headers)},
        "QueryStringsConfig": {"QueryStringBehavior": query_behavior},
        "CookiesConfig": {"CookieBehavior": "none"},
    }
    if comment is not None:
        config["Comment"] = comment
    return {
        "Resources": {"Orp": {"Type": "AWS::CloudFront::OriginRequestPolicy",
                              "Properties": {"OriginRequestPolicyConfig": config}}},
        "Outputs": {"Id": {"Value": {"Ref": "Orp"}}},
    }


def test_cfn_cloudfront_origin_request_policy_updates_in_place(cfn, cloudfront):
    """AWS::CloudFront::OriginRequestPolicy is "No interruption" throughout, so
    an UpdateStack keeps the policy Id, the whitelists follow the template, and
    a dropped Comment goes back to the create default."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-orp-update-{uid}"
    name = f"cfn-orp-update-{uid}"
    renamed = f"cfn-orp-renamed-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name,
                            _origin_request_policy_template(name, comment="v1"))
        policy_id = out["Id"]
        cfg = cloudfront.get_origin_request_policy(
            Id=policy_id)["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
        assert cfg["Comment"] == "v1"
        assert cfg["QueryStringsConfig"]["QueryStringBehavior"] == "all"

        # In-place: the behaviour flips and the header whitelist grows, same Id.
        out = _cfn_cf_stack(cfn, stack_name,
                            _origin_request_policy_template(
                                name, comment="v2", query_behavior="none",
                                headers=("X-Tenant-Key", "X-Service")),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_origin_request_policy(
            Id=policy_id)["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
        assert cfg["Comment"] == "v2"
        assert cfg["QueryStringsConfig"]["QueryStringBehavior"] == "none"
        assert cfg["HeadersConfig"]["Headers"]["Items"] == ["X-Tenant-Key", "X-Service"]

        # A rename keeps the same policy.
        out = _cfn_cf_stack(cfn, stack_name,
                            _origin_request_policy_template(
                                renamed, comment="v2", query_behavior="none",
                                headers=("X-Tenant-Key", "X-Service")),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_origin_request_policy(
            Id=policy_id)["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
        assert cfg["Name"] == renamed
        names = [p["OriginRequestPolicy"]["OriginRequestPolicyConfig"]["Name"] for p in
                 cloudfront.list_origin_request_policies()[
                     "OriginRequestPolicyList"]["Items"]]
        assert names.count(renamed) == 1
        assert name not in names

        # Dropped Comment reverts.
        out = _cfn_cf_stack(cfn, stack_name,
                            _origin_request_policy_template(
                                renamed, query_behavior="none",
                                headers=("X-Tenant-Key", "X-Service")),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_origin_request_policy(
            Id=policy_id)["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
        assert cfg.get("Comment", "") == ""
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _response_headers_policy_template(name, max_age=86400, custom_header=None,
                                      with_cors=True):
    config = {"Name": name}
    if with_cors:
        config["CorsConfig"] = {
            "AccessControlAllowCredentials": False,
            "AccessControlAllowHeaders": {"Items": ["Authorization"]},
            "AccessControlAllowMethods": {"Items": ["GET", "HEAD"]},
            "AccessControlAllowOrigins": {"Items": ["https://example.test"]},
            "AccessControlMaxAgeSec": max_age,
            "OriginOverride": True,
        }
    if custom_header is not None:
        config["CustomHeadersConfig"] = {"Items": [
            {"Header": "X-Env", "Value": custom_header, "Override": True},
        ]}
    return {
        "Resources": {"Rhp": {"Type": "AWS::CloudFront::ResponseHeadersPolicy",
                              "Properties": {"ResponseHeadersPolicyConfig": config}}},
        "Outputs": {"Id": {"Value": {"Ref": "Rhp"}}},
    }


def test_cfn_cloudfront_response_headers_policy_updates_in_place(cfn, cloudfront):
    """AWS::CloudFront::ResponseHeadersPolicy is "No interruption" throughout.
    The CORS max-age and the custom headers follow the template on an
    UpdateStack under the same Id, and dropping CorsConfig removes the block
    rather than leaving the previous one in the policy."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-rhp-update-{uid}"
    name = f"cfn-rhp-update-{uid}"
    renamed = f"cfn-rhp-renamed-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name,
                            _response_headers_policy_template(name, custom_header="local"))
        policy_id = out["Id"]
        cfg = cloudfront.get_response_headers_policy(
            Id=policy_id)["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
        assert cfg["CorsConfig"]["AccessControlMaxAgeSec"] == 86400
        assert cfg["CustomHeadersConfig"]["Items"][0]["Value"] == "local"

        # In-place: max-age and the custom header value change, Id does not.
        out = _cfn_cf_stack(cfn, stack_name,
                            _response_headers_policy_template(
                                name, max_age=600, custom_header="staging"),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_response_headers_policy(
            Id=policy_id)["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
        assert cfg["CorsConfig"]["AccessControlMaxAgeSec"] == 600
        assert cfg["CustomHeadersConfig"]["Items"][0]["Value"] == "staging"

        # A rename keeps the same policy.
        out = _cfn_cf_stack(cfn, stack_name,
                            _response_headers_policy_template(
                                renamed, max_age=600, custom_header="staging"),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_response_headers_policy(
            Id=policy_id)["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
        assert cfg["Name"] == renamed
        names = [p["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]["Name"]
                 for p in cloudfront.list_response_headers_policies()[
                     "ResponseHeadersPolicyList"]["Items"]]
        assert names.count(renamed) == 1
        assert name not in names

        # Dropping CorsConfig removes the block; dropping CustomHeadersConfig
        # leaves an empty one, which is what the parser builds for an absent
        # element.
        out = _cfn_cf_stack(cfn, stack_name,
                            _response_headers_policy_template(renamed, with_cors=False),
                            update=True)
        assert out["Id"] == policy_id
        cfg = cloudfront.get_response_headers_policy(
            Id=policy_id)["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
        assert "CorsConfig" not in cfg
        assert cfg["CustomHeadersConfig"]["Quantity"] == 0
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _oac_template(name, signing_behavior="always", description=None):
    config = {"Name": name, "OriginAccessControlOriginType": "s3",
              "SigningBehavior": signing_behavior, "SigningProtocol": "sigv4"}
    if description is not None:
        config["Description"] = description
    return {
        "Resources": {"Oac": {"Type": "AWS::CloudFront::OriginAccessControl",
                              "Properties": {"OriginAccessControlConfig": config}}},
        "Outputs": {"Id": {"Value": {"Fn::GetAtt": ["Oac", "Id"]}}},
    }


def test_cfn_cloudfront_origin_access_control_updates_in_place(cfn, cloudfront):
    """AWS::CloudFront::OriginAccessControl is "No interruption" throughout, so
    the Id an origin points at survives a changed SigningBehavior, Description
    or Name. Description is the type's one optional field, so it is the one
    whose dropping can be tested with a template AWS accepts."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-oac-update-{uid}"
    name = f"cfn-oac-update-{uid}"
    renamed = f"cfn-oac-renamed-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name,
                            _oac_template(name, signing_behavior="never",
                                          description="first"))
        oac_id = out["Id"]
        cfg = cloudfront.get_origin_access_control(
            Id=oac_id)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["SigningBehavior"] == "never"
        assert cfg["Description"] == "first"

        # In-place: signing behaviour and description change, same Id.
        out = _cfn_cf_stack(cfn, stack_name,
                            _oac_template(name, signing_behavior="no-override",
                                          description="second"),
                            update=True)
        assert out["Id"] == oac_id
        cfg = cloudfront.get_origin_access_control(
            Id=oac_id)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["SigningBehavior"] == "no-override"
        assert cfg["Description"] == "second"

        # A rename keeps the same OAC.
        out = _cfn_cf_stack(cfn, stack_name,
                            _oac_template(renamed, signing_behavior="no-override",
                                          description="second"),
                            update=True)
        assert out["Id"] == oac_id
        cfg = cloudfront.get_origin_access_control(
            Id=oac_id)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["Name"] == renamed
        summaries = cloudfront.list_origin_access_controls()[
            "OriginAccessControlList"]["Items"]
        assert [s["Name"] for s in summaries].count(renamed) == 1
        assert name not in [s["Name"] for s in summaries]

        # Dropping Description reverts it to the create default.
        out = _cfn_cf_stack(cfn, stack_name,
                            _oac_template(renamed, signing_behavior="no-override"),
                            update=True)
        assert out["Id"] == oac_id
        cfg = cloudfront.get_origin_access_control(
            Id=oac_id)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["SigningBehavior"] == "no-override"
        assert cfg.get("Description", "") == ""
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudfront_oac_rename_onto_a_taken_name_is_refused(cfn, cloudfront):
    """An OAC renamed onto a name another OAC holds is refused the way
    UpdateOriginAccessControl refuses it: the update rolls back and both OACs
    keep their names."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-oac-taken-{uid}"
    name = f"cfn-oac-mine-{uid}"
    taken = f"cfn-oac-taken-{uid}"
    other = cloudfront.create_origin_access_control(
        OriginAccessControlConfig=_oac_template(taken)["Resources"]["Oac"][
            "Properties"]["OriginAccessControlConfig"])["OriginAccessControl"]["Id"]
    try:
        out = _cfn_cf_stack(cfn, stack_name, _oac_template(name))
        oac_id = out["Id"]
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(_oac_template(taken)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "already exists" in _stack_event_reasons(cfn, stack_name)
        cfg = cloudfront.get_origin_access_control(
            Id=oac_id)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["Name"] == name
        cfg = cloudfront.get_origin_access_control(
            Id=other)["OriginAccessControl"]["OriginAccessControlConfig"]
        assert cfg["Name"] == taken
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        etag = cloudfront.get_origin_access_control(Id=other)["ETag"]
        cloudfront.delete_origin_access_control(Id=other, IfMatch=etag)


def _cf_function_template(name, comment="v1", code=_FUNCTION_CODE, auto_publish=True):
    props = {"Name": name, "FunctionCode": code,
             "FunctionConfig": {"Comment": comment, "Runtime": "cloudfront-js-2.0"}}
    if auto_publish is not None:
        props["AutoPublish"] = auto_publish
    return {
        "Resources": {"Fn": {"Type": "AWS::CloudFront::Function", "Properties": props}},
        "Outputs": {"Arn": {"Value": {"Fn::GetAtt": ["Fn", "FunctionARN"]}}},
    }


def test_cfn_cloudfront_function_updates_in_place(cfn, cloudfront):
    """AWS::CloudFront::Function: FunctionCode, FunctionConfig and AutoPublish are
    "No interruption", so an UpdateStack edits the function the create made —
    same name, same CreatedTime — and republishes it when AutoPublish is set."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-fn-update-{uid}"
    name = f"cfn-fn-update-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name, _cf_function_template(name, comment="v1"))
        before = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
        created = before["FunctionMetadata"]["CreatedTime"]
        assert before["FunctionConfig"]["Comment"] == "v1"

        new_code = _FUNCTION_CODE.replace("cloudformation", "updated-by-cfn")
        out = _cfn_cf_stack(cfn, stack_name,
                            _cf_function_template(name, comment="v2", code=new_code),
                            update=True)
        assert out["Arn"].endswith(f":function/{name}")
        after = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
        assert after["FunctionConfig"]["Comment"] == "v2"
        # An in-place update is not a re-create: the function keeps the time it
        # was created at, which the fall-through to the create handler reset.
        assert after["FunctionMetadata"]["CreatedTime"] == created
        body = cloudfront.get_function(Name=name, Stage="LIVE")["FunctionCode"].read()
        assert b"updated-by-cfn" in body

        # Dropping AutoPublish leaves the new code unpublished: the update
        # reaches DEVELOPMENT only and LIVE keeps serving what was published,
        # as UpdateFunction does.
        _cfn_cf_stack(cfn, stack_name,
                      _cf_function_template(name, comment="v3", code=new_code,
                                            auto_publish=None),
                      update=True)
        dev = cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")["FunctionSummary"]
        assert dev["FunctionConfig"]["Comment"] == "v3"
        assert dev["FunctionMetadata"]["CreatedTime"] == created
        still_live = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
        assert still_live["FunctionConfig"]["Comment"] == "v2"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudfront_function_rename_replaces(cfn, cloudfront):
    """`Name` is the one "Update requires: Replacement" property of
    AWS::CloudFront::Function: the renamed function is created and the
    predecessor removed, so the ARN moves and only one function is left."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-fn-rename-{uid}"
    before, after = f"cfn-fn-before-{uid}", f"cfn-fn-after-{uid}"
    try:
        out = _cfn_cf_stack(cfn, stack_name, _cf_function_template(before))
        assert out["Arn"].endswith(f":function/{before}")
        out = _cfn_cf_stack(cfn, stack_name, _cf_function_template(after), update=True)
        assert out["Arn"].endswith(f":function/{after}")
        cloudfront.describe_function(Name=after, Stage="LIVE")
        with pytest.raises(ClientError):
            cloudfront.describe_function(Name=before, Stage="DEVELOPMENT")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError):
        cloudfront.describe_function(Name=after, Stage="DEVELOPMENT")


def test_cfn_cloudfront_function_rename_onto_a_taken_name_is_refused(cfn, cloudfront):
    """A function renamed onto a name another function holds is refused the way
    CreateFunction refuses it with FunctionAlreadyExists: the update rolls back,
    the stack's function keeps its name and the other function is untouched
    rather than written over."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-fn-taken-{uid}"
    name = f"cfn-fn-mine-{uid}"
    taken = f"cfn-fn-taken-{uid}"
    cloudfront.create_function(
        Name=taken, FunctionCode=b"function handler(event) { return event.request; }",
        FunctionConfig={"Comment": "not the stack's", "Runtime": "cloudfront-js-2.0"})
    try:
        _cfn_cf_stack(cfn, stack_name, _cf_function_template(name))
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=json.dumps(_cf_function_template(taken)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "already exists" in _stack_event_reasons(cfn, stack_name)
        cloudfront.describe_function(Name=name, Stage="LIVE")
        other = cloudfront.describe_function(Name=taken, Stage="DEVELOPMENT")["FunctionSummary"]
        assert other["FunctionConfig"]["Comment"] == "not the stack's"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        etag = cloudfront.describe_function(Name=taken, Stage="DEVELOPMENT")["ETag"]
        cloudfront.delete_function(Name=taken, IfMatch=etag)


def test_cfn_cloudfront_function_deleted_out_of_band_is_recreated(cfn, cloudfront):
    """A function removed through the API between two stack updates converges:
    the update handler finds no record under the physical id, so the shared
    _rename_replacement prologue treats it as a replacement and creates the
    function the template asks for rather than failing the stack."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-fn-gone-{uid}"
    name = f"cfn-fn-gone-{uid}"
    try:
        _cfn_cf_stack(cfn, stack_name, _cf_function_template(name, comment="v1"))
        etag = cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")["ETag"]
        cloudfront.delete_function(Name=name, IfMatch=etag)
        with pytest.raises(ClientError):
            cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")

        out = _cfn_cf_stack(cfn, stack_name,
                            _cf_function_template(name, comment="v2"), update=True)
        assert out["Arn"].endswith(f":function/{name}")
        summary = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
        assert summary["FunctionConfig"]["Comment"] == "v2"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudfront_distribution_consumes_provisioned_policies(cfn, cloudfront):
    """The payoff: a distribution in the same stack references the policies and
    the function by Ref/GetAtt. This is what a CDK app emits, and it only works
    if Ref yields the value the distribution's own parser expects."""
    template = json.loads(_cloudfront_template("prov2"))
    template["Resources"]["Dist"] = {
        "Type": "AWS::CloudFront::Distribution",
        "Properties": {"DistributionConfig": {
            "Enabled": True,
            "Comment": "consumes provisioned policies",
            "Origins": [{"Id": "origin1", "DomainName": "example.test",
                         "CustomOriginConfig": {"OriginProtocolPolicy": "https-only"}}],
            "DefaultCacheBehavior": {
                "TargetOriginId": "origin1",
                "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": {"Ref": "CachePolicy"},
                "OriginRequestPolicyId": {"Ref": "OriginRequestPolicy"},
                "ResponseHeadersPolicyId": {"Ref": "ResponseHeadersPolicy"},
                "FunctionAssociations": [{
                    "EventType": "viewer-request",
                    "FunctionARN": {"Fn::GetAtt": ["Function", "FunctionARN"]},
                }],
            },
        }},
    }
    template["Outputs"]["DistId"] = {"Value": {"Ref": "Dist"}}

    cfn.create_stack(StackName="cfn-cf-dist-policies", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-cf-dist-policies")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    out = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}

    # The distribution provisioner accepted every Ref without rolling back, and
    # each one addresses a real object. Asserting the stored DefaultCacheBehavior
    # would be the stronger check, but GetDistribution and GetDistributionConfig
    # both 500 on any CloudFormation-provisioned distribution (the record carries
    # an empty config_xml) — that is a separate, pre-existing bug.
    assert cloudfront.get_cache_policy(Id=out["CachePolicyRef"])["CachePolicy"]["Id"]
    assert cloudfront.get_origin_request_policy(
        Id=out["OrpRef"])["OriginRequestPolicy"]["Id"]
    assert cloudfront.get_response_headers_policy(
        Id=out["RhpRef"])["ResponseHeadersPolicy"]["Id"]
    assert out["FunctionArn"].endswith(":function/cfn-fn-prov2")
    assert out["DistId"]

    cfn.delete_stack(StackName="cfn-cf-dist-policies")
    _wait_stack(cfn, "cfn-cf-dist-policies")


def test_cfn_cloudfront_distribution_keeps_its_policy_ref_across_a_rename(cfn, cloudfront):
    """The payoff of updating in place: a distribution that took the cache
    policy's Ref keeps pointing at the same policy after the policy is renamed
    through the stack. Only the Ref and the policy itself can be asserted here;
    GetDistributionConfig on a CloudFormation-provisioned distribution is the
    separate, pre-existing 500."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cf-dist-rename-{uid}"
    template = json.loads(_cloudfront_template(f"ren{uid}"))
    template["Resources"]["Dist"] = {
        "Type": "AWS::CloudFront::Distribution",
        "Properties": {"DistributionConfig": {
            "Enabled": True,
            "Comment": "keeps its policy ref",
            "Origins": [{"Id": "origin1", "DomainName": "example.test",
                         "CustomOriginConfig": {"OriginProtocolPolicy": "https-only"}}],
            "DefaultCacheBehavior": {
                "TargetOriginId": "origin1",
                "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": {"Ref": "CachePolicy"},
            },
        }},
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        before = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}

        renamed = f"cfn-cp-ren{uid}-v2"
        template["Resources"]["CachePolicy"]["Properties"]["CachePolicyConfig"]["Name"] = renamed
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", _stack_event_reasons(cfn, stack_name)
        after = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
        assert after["CachePolicyRef"] == before["CachePolicyRef"]
        cfg = cloudfront.get_cache_policy(Id=after["CachePolicyRef"])["CachePolicy"]["CachePolicyConfig"]
        assert cfg["Name"] == renamed
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _mrap_template(name, buckets):
    return json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            **{f"B{i}": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": b}}
               for i, b in enumerate(buckets)},
            "Mrap": {"Type": "AWS::S3::MultiRegionAccessPoint",
                     "DependsOn": [f"B{i}" for i in range(len(buckets))],
                     "Properties": {"Name": name,
                                    "Regions": [{"Bucket": b} for b in buckets]}},
        },
        "Outputs": {"Alias": {"Value": {"Fn::GetAtt": ["Mrap", "Alias"]}}},
    })


def _mrap_get(alias, key, region=None):
    """GET through the MRAP hostname. The host is sent explicitly rather than
    resolved: <alias>.mrap.accesspoint.s3-global.amazonaws.com is a real public
    suffix, so letting DNS see it would leave the test dependent on egress."""
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    req = urllib.request.Request(f"{endpoint}/{key}")
    # The alias itself ends in ".mrap"; the hostname appends only the suffix.
    req.add_header("Host", f"{alias}.accesspoint.s3-global.amazonaws.com")
    if region:
        req.add_header("Authorization",
                       "AWS4-HMAC-SHA256 "
                       f"Credential=test/20260101/{region}/s3/aws4_request, "
                       "SignedHeaders=host, Signature=unsigned")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read().decode()


def test_cfn_s3_multi_region_access_point(cfn, s3):
    """AWS::S3::MultiRegionAccessPoint used to roll the stack back, so a CDK app
    fronting regional buckets with one could not deploy at all. It now provisions
    and hands back the 13-character alias — the only attribute a template reads,
    and the name the data plane is addressed by."""
    buckets = ["mrap-cfn-us-east-1-app", "mrap-cfn-eu-west-1-app"]
    cfn.create_stack(StackName="cfn-s3-mrap",
                     TemplateBody=_mrap_template("cfn-mrap-app", buckets))
    stack = _wait_stack(cfn, "cfn-s3-mrap")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    alias = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["Alias"]
    # The alias S3 mints ends in ".mrap" (e.g. mfzwi23gnjvgw.mrap; documented
    # pattern ^[a-z][a-z0-9]*[.]mrap$); templates build
    # "<alias>.accesspoint.s3-global.amazonaws.com" from it.
    base, _, suffix = alias.rpartition(".")
    assert suffix == "mrap" and re.fullmatch(r"[a-z][a-z0-9]{12}", base)

    cfn.delete_stack(StackName="cfn-s3-mrap")
    _wait_stack(cfn, "cfn-s3-mrap")
    # The alias stops resolving with the stack.
    import urllib.error
    with pytest.raises(urllib.error.HTTPError) as ei:
        _mrap_get(alias, "anything")
    assert ei.value.code in (403, 404)


def test_s3_mrap_alias_matches_the_documented_pattern():
    # In-process: the generator alone. A digit-only draw was possible before
    # (uuid hex prefix), which is outside ^[a-z][a-z0-9]*[.]mrap$.
    from ministack.services.s3 import new_mrap_alias

    pattern = re.compile(r"^[a-z][a-z0-9]{12}\.mrap$")
    for _ in range(200):
        alias = new_mrap_alias()
        assert pattern.fullmatch(alias), alias


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


def test_cfn_apigateway_rest_api_tracks_stack_region(cfn, apigw_v1):
    """A v1 REST API created by a regional stack is scoped to that region, while
    unsigned execute-api data-plane requests still resolve by API id."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    port = urlparse(endpoint).port or 4566
    west_cfn = boto3.client(
        "cloudformation",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
        config=Config(region_name="us-west-2"),
    )
    west_apigw = boto3.client(
        "apigateway",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
        config=Config(region_name="us-west-2"),
    )

    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-apigw-region-{suffix}"
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": f"region-cfn-{suffix}"},
            },
            "MockResource": {
                "Type": "AWS::ApiGateway::Resource",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "ParentId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
                    "PathPart": "mock",
                },
            },
            "MockMethod": {
                "Type": "AWS::ApiGateway::Method",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "ResourceId": {"Ref": "MockResource"},
                    "HttpMethod": "GET",
                    "AuthorizationType": "NONE",
                    "Integration": {"Type": "MOCK"},
                },
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "DependsOn": "MockMethod",
                "Properties": {"RestApiId": {"Ref": "Api"}, "StageName": "prod"},
            },
        },
        "Outputs": {"ApiId": {"Value": {"Ref": "Api"}}},
    }

    west_cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    try:
        stack = _wait_stack(west_cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        api_id = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}[
            "ApiId"
        ]

        assert west_apigw.get_rest_api(restApiId=api_id)["id"] == api_id
        with pytest.raises(ClientError) as exc:
            apigw_v1.get_rest_api(restApiId=api_id)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/prod/mock",
            method="GET",
            headers={"Host": f"{api_id}.execute-api.localhost:{port}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            assert json.loads(resp.read() or b"{}") == {}
    finally:
        west_cfn.delete_stack(StackName=stack_name)
        _wait_stack(west_cfn, stack_name)


def test_cfn_apigateway_method_responses_survive_provisioning(cfn, apigw_v1):
    """MethodResponses and Integration.IntegrationResponses declared in a template
    reach the deployed method, so a CDK-style defaultCorsPreflightOptions OPTIONS
    method actually answers with the Access-Control-Allow-* headers it maps."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    port = urlparse(endpoint).port or 4566
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"intg-cfn-apigw-cors-{suffix}"
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": f"cors-cfn-{suffix}"},
            },
            "Items": {
                "Type": "AWS::ApiGateway::Resource",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "ParentId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
                    "PathPart": "items",
                },
            },
            "OptionsMethod": {
                "Type": "AWS::ApiGateway::Method",
                "Properties": {
                    "RestApiId": {"Ref": "Api"},
                    "ResourceId": {"Ref": "Items"},
                    "HttpMethod": "OPTIONS",
                    "AuthorizationType": "NONE",
                    "Integration": {
                        "Type": "MOCK",
                        "RequestTemplates": {"application/json": '{ "statusCode": 200 }'},
                        "IntegrationResponses": [
                            {
                                "StatusCode": "204",
                                "ResponseParameters": {
                                    "method.response.header.Access-Control-Allow-Origin": "'*'",
                                    "method.response.header.Access-Control-Allow-Headers": (
                                        "'Content-Type,Authorization'"
                                    ),
                                    "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,GET'",
                                },
                            },
                        ],
                    },
                    "MethodResponses": [
                        {
                            "StatusCode": "204",
                            "ResponseParameters": {
                                "method.response.header.Access-Control-Allow-Origin": True,
                                "method.response.header.Access-Control-Allow-Headers": True,
                                "method.response.header.Access-Control-Allow-Methods": True,
                            },
                        },
                    ],
                },
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "DependsOn": "OptionsMethod",
                "Properties": {"RestApiId": {"Ref": "Api"}, "StageName": "prod"},
            },
        },
        "Outputs": {
            "ApiId": {"Value": {"Ref": "Api"}},
            "ResourceId": {"Value": {"Ref": "Items"}},
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}

        method = apigw_v1.get_method(
            restApiId=outputs["ApiId"],
            resourceId=outputs["ResourceId"],
            httpMethod="OPTIONS",
        )
        assert method["methodResponses"], "MethodResponses were dropped during provisioning"
        assert "204" in method["methodResponses"]
        integration_responses = method["methodIntegration"]["integrationResponses"]
        assert integration_responses, "IntegrationResponses were dropped during provisioning"
        # Quotes are stripped at request time, not at storage time, so the
        # stored mapping must keep them verbatim.
        assert integration_responses["204"]["responseParameters"] == {
            "method.response.header.Access-Control-Allow-Origin": "'*'",
            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,Authorization'",
            "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,GET'",
        }

        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/prod/items",
            method="OPTIONS",
            headers={
                "Host": f"{outputs['ApiId']}.execute-api.localhost:{port}",
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "authorization",
            },
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 204
            assert resp.headers.get("Access-Control-Allow-Origin") == "*"
            assert resp.headers.get("Access-Control-Allow-Methods") == "OPTIONS,GET"
    finally:
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
    assert _template_tags(regional["tags"]) == {"created-by": "cloudformation"}
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


def test_cfn_apigateway_api_key_lifecycle(cfn, apigw_v1):
    """ApiKey supports create, a pinned Value, Ref/GetAtt, in-place update, delete.

    Regression: the resource type previously failed stack creation with
    ``Unsupported resource type: AWS::ApiGateway::ApiKey``.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-api-key-{suffix}"
    pinned_value = f"pinnedkeyvalue{suffix}0000000000"
    stack_deleted = False

    def template(description, enabled):
        return {
            "Resources": {
                "ApiKey": {
                    "Type": "AWS::ApiGateway::ApiKey",
                    "Properties": {
                        "Name": f"key-{suffix}",
                        "Description": description,
                        "Enabled": enabled,
                        "Value": pinned_value,
                    },
                },
            },
            "Outputs": {
                "RefId": {"Value": {"Ref": "ApiKey"}},
                "GetAttId": {"Value": {"Fn::GetAtt": ["ApiKey", "APIKeyId"]}},
            },
        }

    def physical_id():
        detail = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="ApiKey",
        )["StackResourceDetail"]
        return detail["PhysicalResourceId"]

    try:
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("Created", True)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        created_id = physical_id()
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        assert outputs == {"RefId": created_id, "GetAttId": created_id}

        created = apigw_v1.get_api_key(apiKey=created_id, includeValue=True)
        assert created["name"] == f"key-{suffix}"
        assert created["description"] == "Created"
        assert created["enabled"] is True
        assert created["value"] == pinned_value

        # Description and Enabled update in place without replacing the key.
        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(template("Updated", False)),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert physical_id() == created_id
        updated = apigw_v1.get_api_key(apiKey=created_id)
        assert updated["description"] == "Updated"
        assert updated["enabled"] is False

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        stack_deleted = True
        with pytest.raises(ClientError):
            apigw_v1.get_api_key(apiKey=created_id)
    finally:
        if not stack_deleted:
            try:
                cfn.delete_stack(StackName=stack_name)
                _wait_stack(cfn, stack_name)
            except ClientError:
                pass


def test_cfn_apigateway_usage_plan_and_key_lifecycle(cfn, apigw_v1):
    """UsagePlan and UsagePlanKey provision, expose ids, associate a key, and delete.

    Regression: both resource types previously failed stack creation with
    ``Unsupported resource type``.
    """
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-usage-plan-{suffix}"
    stack_deleted = False

    template = {
        "Resources": {
            "ApiKey": {
                "Type": "AWS::ApiGateway::ApiKey",
                "Properties": {"Name": f"plan-key-{suffix}", "Enabled": True},
            },
            "UsagePlan": {
                "Type": "AWS::ApiGateway::UsagePlan",
                "Properties": {
                    "UsagePlanName": f"plan-{suffix}",
                    "Description": "integration plan",
                    "Throttle": {"BurstLimit": 20, "RateLimit": 10},
                    "Quota": {"Limit": 1000, "Period": "MONTH"},
                },
            },
            "UsagePlanKey": {
                "Type": "AWS::ApiGateway::UsagePlanKey",
                "Properties": {
                    "KeyId": {"Ref": "ApiKey"},
                    "KeyType": "API_KEY",
                    "UsagePlanId": {"Ref": "UsagePlan"},
                },
            },
        },
        "Outputs": {
            "PlanRef": {"Value": {"Ref": "UsagePlan"}},
            "PlanGetAtt": {"Value": {"Fn::GetAtt": ["UsagePlan", "Id"]}},
            "KeyRef": {"Value": {"Ref": "ApiKey"}},
            "PlanKeyRef": {"Value": {"Ref": "UsagePlanKey"}},
        },
    }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}

        plan_id = outputs["PlanRef"]
        assert outputs["PlanGetAtt"] == plan_id
        # UsagePlanKey Ref is "{keyId}:{usagePlanId}" (the AWS-documented physical id).
        assert outputs["PlanKeyRef"] == f"{outputs['KeyRef']}:{outputs['PlanRef']}"

        plan = apigw_v1.get_usage_plan(usagePlanId=plan_id)
        assert plan["name"] == f"plan-{suffix}"
        assert plan["throttle"] == {"burstLimit": 20, "rateLimit": 10}
        assert plan["quota"]["limit"] == 1000 and plan["quota"]["period"] == "MONTH"

        keys = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
        assert [k["id"] for k in keys] == [outputs["KeyRef"]]

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        stack_deleted = True
        with pytest.raises(ClientError):
            apigw_v1.get_usage_plan(usagePlanId=plan_id)
    finally:
        if not stack_deleted:
            try:
                cfn.delete_stack(StackName=stack_name)
                _wait_stack(cfn, stack_name)
            except ClientError:
                pass


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
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

    parent_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Nested": {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {
                    "TemplateURL": f"{endpoint}/{templates_bucket}/child.json",
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


def test_cfn_nested_stack_long_name_lambda_functions_get_distinct_physical_names(cfn, s3, lam):
    """Regression test: a nested stack's own auto-generated name (parent name
    + nested-stack logical id + a CloudFormation-assigned suffix — exactly
    what CDK's NestedStack construct produces) can itself already exceed a
    downstream resource's own name-length limit, e.g. Lambda's 64-char
    FunctionName cap. Before this fix, _physical_name() built the full
    "{stack}-{logicalId}-{suffix}" string and only then truncated it to
    max_len from the end — so once stack_name alone was >= max_len, every
    resource in that nested stack (regardless of logical_id) collapsed onto
    the exact same truncated physical name. Two real Lambda functions used to
    become one physical function; only whichever was provisioned last ever
    actually ran, regardless of which one a caller invoked."""
    suffix = _uuid_mod.uuid4().hex[:8]
    templates_bucket = f"cfn-nested-templates-{suffix}"
    s3.create_bucket(Bucket=templates_bucket)

    def _lambda_resource(marker):
        return {
            "Type": "AWS::Lambda::Function",
            "Properties": {
                "Runtime": "python3.12",
                "Handler": "index.handler",
                "Role": "arn:aws:iam::000000000000:role/lambda-role",
                "Code": {"ZipFile": f"def handler(event, context):\n    return {{'marker': '{marker}'}}\n"},
            },
        }

    child_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "FirstFunction": _lambda_resource("first"),
            "SecondFunction": _lambda_resource("second"),
        },
        "Outputs": {
            "FirstArn": {"Value": {"Fn::GetAtt": ["FirstFunction", "Arn"]}},
            "SecondArn": {"Value": {"Fn::GetAtt": ["SecondFunction", "Arn"]}},
        },
    }
    s3.put_object(Bucket=templates_bucket, Key="child.json",
                  Body=json.dumps(child_template).encode())
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

    # Deliberately verbose, mirroring the shape CDK actually generates for a
    # NestedStack construct's own logical id (ParentId + "NestedStack" +
    # ParentId + "NestedStackResource" + a CloudFormation-assigned hash) —
    # long enough that ministack's generated child stack name
    # ("{parent_name}-{nested_logical_id}-{uuid[:12]}") already meets or
    # exceeds 64 characters on its own, before any Lambda logical_id is even
    # appended.
    parent_name = f"cfn-nested-longname-parent-{suffix}"
    nested_logical_id = "ApiStackNestedStackApiStackNestedStackResourceABCDEFG1234"

    parent_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            nested_logical_id: {
                "Type": "AWS::CloudFormation::Stack",
                "Properties": {
                    "TemplateURL": f"{endpoint}/{templates_bucket}/child.json",
                },
            },
        },
        "Outputs": {
            "FirstArn": {"Value": {"Fn::GetAtt": [nested_logical_id, "Outputs.FirstArn"]}},
            "SecondArn": {"Value": {"Fn::GetAtt": [nested_logical_id, "Outputs.SecondArn"]}},
        },
    }

    cfn.create_stack(StackName=parent_name, TemplateBody=json.dumps(parent_template))
    stack = _wait_stack(cfn, parent_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    first_arn = outputs["FirstArn"]
    second_arn = outputs["SecondArn"]

    assert first_arn != second_arn, (
        f"FirstFunction and SecondFunction collapsed onto the same physical Lambda: {first_arn}"
    )

    first_name = first_arn.rsplit(":", 1)[-1]
    second_name = second_arn.rsplit(":", 1)[-1]
    assert len(first_name) <= 64
    assert len(second_name) <= 64

    # Each is independently invocable and runs its own code — not just
    # distinctly named, but genuinely distinct resources.
    first_result = json.loads(lam.invoke(FunctionName=first_name)["Payload"].read())
    second_result = json.loads(lam.invoke(FunctionName=second_name)["Payload"].read())
    assert first_result["marker"] == "first"
    assert second_result["marker"] == "second"

    cfn.delete_stack(StackName=parent_name)
    _wait_stack(cfn, parent_name)

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


def test_cfn_kinesisfirehose_delivery_stream_shares_firehose_state(cfn, fh):
    """AWS::KinesisFirehose::DeliveryStream provisions through CloudFormation and
    shares state with the Firehose API; Ref returns the name, GetAtt Arn the ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-firehose-{suffix}"
    stream_name = f"cfn-fh-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Stream": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": {
                    "DeliveryStreamName": stream_name,
                    "DeliveryStreamType": "DirectPut",
                    "ExtendedS3DestinationConfiguration": {
                        "BucketARN": "arn:aws:s3:::cfn-fh-bucket",
                        "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
                        "Prefix": "raw/",
                    },
                },
            },
        },
        "Outputs": {
            "RefName": {"Value": {"Ref": "Stream"}},
            "StreamArn": {"Value": {"Fn::GetAtt": ["Stream", "Arn"]}},
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
        assert outputs["RefName"] == stream_name
        assert outputs["StreamArn"].endswith(f":deliverystream/{stream_name}")

        desc = fh.describe_delivery_stream(
            DeliveryStreamName=stream_name
        )["DeliveryStreamDescription"]
        assert desc["DeliveryStreamStatus"] == "ACTIVE"
        assert desc["DeliveryStreamARN"] == outputs["StreamArn"]
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass

    with pytest.raises(ClientError) as exc:
        fh.describe_delivery_stream(DeliveryStreamName=stream_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_kinesisfirehose_iceberg_destination_provisions(cfn, fh):
    """Regression for #1206: a stack with an Iceberg-destination Firehose stream
    no longer fails with Unsupported resource type and reaches CREATE_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-fh-iceberg-{suffix}"
    stream_name = f"cfn-fh-ice-{suffix}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Stream": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": {
                    "DeliveryStreamName": stream_name,
                    "DeliveryStreamType": "DirectPut",
                    "IcebergDestinationConfiguration": {
                        "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
                        "CatalogConfiguration": {
                            "CatalogARN": "arn:aws:glue:us-east-1:000000000000:catalog"
                        },
                        "S3Configuration": {
                            "BucketARN": "arn:aws:s3:::cfn-fh-ice-bucket",
                            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
                        },
                        "DestinationTableConfigurationList": [{
                            "DestinationDatabaseName": "analytics",
                            "DestinationTableName": "events",
                            "UniqueKeys": ["id"],
                        }],
                    },
                },
            },
        },
        "Outputs": {"RefName": {"Value": {"Ref": "Stream"}}},
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert fh.describe_delivery_stream(
            DeliveryStreamName=stream_name
        )["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except ClientError:
            pass


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


def test_cfn_lambda_layer_version_permission(cfn, s3, lam):
    """A layer plus the permission resource that grants another account access
    to it — the shape serverless-python-requirements emits for a layer with
    ``allowedAccounts``, and CDK's ``LayerVersion.addPermission``.

    Regression: AWS::Lambda::LayerVersionPermission had no provisioner, so the
    whole stack failed with "Unsupported resource type" and rolled back.
    Reported by @iot-rocket."""
    stack_name = "cfn-layer-permission"
    bucket_name = "cfn-layer-permission-assets"
    layer_name = "cfn-permission-layer"
    account_id = "210987654321"

    s3.create_bucket(Bucket=bucket_name)
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr("python/cfn_permission_helper.py", "VALUE = 1\n")
    s3.put_object(Bucket=bucket_name, Key="layer.zip", Body=layer_buf.getvalue())

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "PythonRequirementsLambdaLayer": {
                "Type": "AWS::Lambda::LayerVersion",
                "Properties": {
                    "LayerName": layer_name,
                    "CompatibleRuntimes": ["python3.12"],
                    "Content": {"S3Bucket": bucket_name, "S3Key": "layer.zip"},
                },
            },
            "PythonRequirementsLambdaLayerPermission": {
                "Type": "AWS::Lambda::LayerVersionPermission",
                "Properties": {
                    "Action": "lambda:GetLayerVersion",
                    "LayerVersionArn": {"Ref": "PythonRequirementsLambdaLayer"},
                    "Principal": account_id,
                },
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    try:
        version = lam.list_layer_versions(LayerName=layer_name)["LayerVersions"][0]["Version"]
        policy = json.loads(
            lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)["Policy"]
        )
        assert len(policy["Statement"]) == 1
        statement = policy["Statement"][0]
        assert statement["Action"] == "lambda:GetLayerVersion"
        assert statement["Principal"] == {"AWS": f"arn:aws:iam::{account_id}:root"}

        # Ref/Id is "<layer version ARN>#<statement id>".
        resource = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="PythonRequirementsLambdaLayerPermission",
        )["StackResourceDetail"]
        version_arn, sep, statement_id = resource["PhysicalResourceId"].rpartition("#")
        assert sep == "#"
        assert version_arn.endswith(f":layer:{layer_name}:{version}")
        assert statement_id == statement["Sid"]
    finally:
        cfn.delete_stack(StackName=stack_name)

    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    with pytest.raises(ClientError) as exc:
        lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_lambda_layer_version_permission_delete_leaves_layer(cfn, lam):
    """Deleting the stack revokes the grant it made and nothing else — the
    layer version it pointed at (published outside the stack, as CDK's
    ``LayerVersion.fromLayerVersionArn`` does) is still there afterwards."""
    stack_name = "cfn-layer-permission-detached"
    layer_name = "cfn-detached-perm-layer"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("python/detached.py", "VALUE = 1\n")
    published = lam.publish_layer_version(
        LayerName=layer_name,
        Content={"ZipFile": buf.getvalue()},
    )
    version = published["Version"]

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "LayerPermission": {
                "Type": "AWS::Lambda::LayerVersionPermission",
                "Properties": {
                    "Action": "lambda:GetLayerVersion",
                    "LayerVersionArn": published["LayerVersionArn"],
                    "Principal": "*",
                },
            },
        },
    }

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    policy = json.loads(
        lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)["Policy"]
    )
    assert policy["Statement"][0]["Principal"] == "*"

    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"

    with pytest.raises(ClientError) as exc:
        lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert lam.get_layer_version(LayerName=layer_name, VersionNumber=version)["Version"] == version


def test_cfn_lambda_layer_version_permission_property_change_replaces(cfn, lam):
    """Every property of this type is create-only, so a changed Principal is a
    replacement. With no update handler the framework re-runs create (#1340),
    which must land on the same statement rather than leaving the old grant
    behind next to the new one."""
    stack_name = "cfn-layer-permission-update"
    layer_name = "cfn-update-perm-layer"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("python/updated.py", "VALUE = 1\n")
    published = lam.publish_layer_version(
        LayerName=layer_name,
        Content={"ZipFile": buf.getvalue()},
    )
    version = published["Version"]

    def template(principal):
        return json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "LayerPermission": {
                    "Type": "AWS::Lambda::LayerVersionPermission",
                    "Properties": {
                        "Action": "lambda:GetLayerVersion",
                        "LayerVersionArn": published["LayerVersionArn"],
                        "Principal": principal,
                    },
                },
            },
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("111111111111"))
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

    cfn.update_stack(StackName=stack_name, TemplateBody=template("222222222222"))
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

    policy = json.loads(
        lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)["Policy"]
    )
    assert len(policy["Statement"]) == 1
    assert policy["Statement"][0]["Principal"] == {"AWS": "arn:aws:iam::222222222222:root"}

    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    with pytest.raises(ClientError) as exc:
        lam.get_layer_version_policy(LayerName=layer_name, VersionNumber=version)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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


def _opensearch_stub_endpoint(domain_name, region="us-east-1"):
    return f"{domain_name}.{region}.ministack.local:9200"


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
        "Endpoint": _opensearch_stub_endpoint(domain_name),
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
    assert _template_tags({t["Key"]: t["Value"] for t in tags}) == {
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
    assert _template_tags({t["Key"]: t["Value"] for t in tags}) == {
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
    assert status["Endpoints"]["vpc"] == _opensearch_stub_endpoint(physical_id)

    # Removing VPCOptions is an in-place control-plane update and restores the
    # public endpoint response shape.
    template = _opensearch_stack_template({"EngineVersion": "OpenSearch_2.17"})
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert _stack_resource(cfn, stack_name)["PhysicalResourceId"] == physical_id
    status = opensearch.describe_domain(DomainName=physical_id)["DomainStatus"]
    assert status["Endpoint"] == _opensearch_stub_endpoint(physical_id)
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
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
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
                    # CDK local tooling sets a root MiniStack gateway endpoint.
                    # The SDK shim must not turn it into the S3-shaped
                    # ``opensearch.<gateway>`` virtual host.
                    "Environment": {
                        "Variables": {"AWS_ENDPOINT_URL": endpoint},
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


def test_cfn_s3tables_table_schema_from_iceberg_metadata(cfn, s3tables):
    """AWS::S3Tables::Table's IcebergMetadata.IcebergSchema.SchemaFieldList must
    populate the table's actual Iceberg schema — not just be accepted and
    discarded. A table created with an empty schema silently breaks any
    consumer relying on the declared columns (e.g. a Firehose Iceberg
    destination fails to insert with a "does not have a column" error even
    though the CFN template clearly declares one)."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3Tables::TableBucket",
                "Properties": {"TableBucketName": "cfn-s3tables-schema-test"},
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
                    "IcebergMetadata": {
                        "IcebergSchema": {
                            "SchemaFieldList": [
                                {"Id": 1, "Name": "id", "Type": "string", "Required": True},
                                {"Id": 2, "Name": "value", "Type": "string"},
                            ],
                        },
                    },
                },
                "DependsOn": "Ns",
            },
        },
    }

    stack_name = "cfn-s3tables-schema-t01"
    try:
        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
    except Exception:
        pass

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    resp = _cfn_iceberg_json("/iceberg/v1/namespaces/myns/tables/mytable")
    fields = resp.get("metadata", {}).get("schemas", [{}])[0].get("fields", [])
    field_names = {f["name"] for f in fields}
    assert field_names == {"id", "value"}, f"expected columns id/value, got {field_names}"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


# ── AWS::KMS::Key ───────────────────────────────────────────────────────────
# Property names, defaults and update behaviour follow the resource reference:
# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-kms-key.html

# The Id is deliberately not "key-default-1" — that is what CreateKey generates
# when no policy is supplied, so a default would satisfy the assertion below even
# if the template's KeyPolicy were dropped on the floor.
_KMS_KEY_POLICY = {
    "Version": "2012-10-17",
    "Id": "cfn-supplied-key-policy",
    "Statement": [
        {
            "Sid": "Enable IAM User Permissions",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
            "Action": "kms:*",
            "Resource": "*",
        }
    ],
}


def _kms_key_template(props):
    return json.dumps(
        {
            "Resources": {"Key": {"Type": "AWS::KMS::Key", "Properties": props}},
            "Outputs": {
                "KeyRef": {"Value": {"Ref": "Key"}},
                "KeyArn": {"Value": {"Fn::GetAtt": ["Key", "Arn"]}},
            },
        }
    )


def _kms_stack_outputs(cfn, stack_name):
    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def test_cfn_kms_key_asymmetric_key_spec_is_honored(cfn, kms_client):
    """An RSA_2048 SIGN_VERIFY key declared in a template must actually sign.

    The provisioner used to hardcode SYMMETRIC_DEFAULT, so the stack reached
    CREATE_COMPLETE and DescribeKey reported KeyUsage=SIGN_VERIFY while the key
    underneath was symmetric — Sign then failed with UnsupportedOperationException.
    """
    stack_name = f"cfn-kms-rsa-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template(
            {
                "KeySpec": "RSA_2048",
                "KeyUsage": "SIGN_VERIFY",
                "Description": "signing key",
                "KeyPolicy": _KMS_KEY_POLICY,
            }
        ),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    out = _kms_stack_outputs(cfn, stack_name)
    meta = kms_client.describe_key(KeyId=out["KeyArn"])["KeyMetadata"]
    assert meta["KeySpec"] == "RSA_2048"
    assert meta["KeyUsage"] == "SIGN_VERIFY"
    assert "RSASSA_PSS_SHA_256" in meta["SigningAlgorithms"]
    # Ref returns the key id; GetAtt exposes Arn and KeyId.
    assert out["KeyRef"] == meta["KeyId"]
    assert out["KeyArn"] == meta["Arn"]

    message = b"cfn-kms-parity"
    signature = kms_client.sign(
        KeyId=out["KeyArn"],
        Message=message,
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PSS_SHA_256",
    )["Signature"]
    assert kms_client.verify(
        KeyId=out["KeyArn"],
        Message=message,
        MessageType="RAW",
        Signature=signature,
        SigningAlgorithm="RSASSA_PSS_SHA_256",
    )["SignatureValid"]
    assert kms_client.get_public_key(KeyId=out["KeyArn"])["PublicKey"]

    # The key policy is CFN's `KeyPolicy`, stored as the API's `Policy`.
    policy = json.loads(
        kms_client.get_key_policy(KeyId=out["KeyArn"], PolicyName="default")["Policy"]
    )
    assert policy["Id"] == "cfn-supplied-key-policy"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_kms_key_tags_rotation_and_enabled(cfn, kms_client):
    """Tags, EnableKeyRotation and Enabled:false are applied at create time."""
    stack_name = f"cfn-kms-props-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template(
            {
                "KeyPolicy": _KMS_KEY_POLICY,
                "Enabled": False,
                "EnableKeyRotation": True,
                "RotationPeriodInDays": 180,
                # CloudFormation tags are Key/Value; KMS stores TagKey/TagValue.
                "Tags": [{"Key": "env", "Value": "test"}],
            }
        ),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    arn = _kms_stack_outputs(cfn, stack_name)["KeyArn"]
    meta = kms_client.describe_key(KeyId=arn)["KeyMetadata"]
    assert meta["KeySpec"] == "SYMMETRIC_DEFAULT"
    assert meta["KeyState"] == "Disabled"
    assert meta["Enabled"] is False

    rotation = kms_client.get_key_rotation_status(KeyId=arn)
    assert rotation["KeyRotationEnabled"] is True
    assert rotation["RotationPeriodInDays"] == 180

    tags = kms_client.list_resource_tags(KeyId=arn)["Tags"]
    assert _template_tags(tags) == [{"TagKey": "env", "TagValue": "test"}]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_kms_key_mutable_property_updates_in_place(cfn, kms_client):
    """Description is 'Update requires: No interruption' — the key is not replaced."""
    stack_name = f"cfn-kms-upd-{_uuid_mod.uuid4().hex[:8]}"
    props = {"KeySpec": "RSA_2048", "KeyUsage": "SIGN_VERIFY", "Description": "v1"}
    cfn.create_stack(StackName=stack_name, TemplateBody=_kms_key_template(props))
    _wait_stack(cfn, stack_name)
    original_id = _kms_stack_outputs(cfn, stack_name)["KeyRef"]

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template({**props, "Description": "v2"}),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

    assert _kms_stack_outputs(cfn, stack_name)["KeyRef"] == original_id
    meta = kms_client.describe_key(KeyId=original_id)["KeyMetadata"]
    assert meta["Description"] == "v2"
    # The key material survived the update — signatures stay verifiable.
    assert meta["KeySpec"] == "RSA_2048"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_kms_key_immutable_property_change_is_rejected(cfn, kms_client):
    """"If you change the value of the KeySpec ... the update request fails."

    Without an update handler the engine falls back to `create`, minting a fresh
    key pair and silently invalidating every signature made with the old one.
    """
    stack_name = f"cfn-kms-immut-{_uuid_mod.uuid4().hex[:8]}"
    props = {"KeySpec": "RSA_2048", "KeyUsage": "SIGN_VERIFY"}
    cfn.create_stack(StackName=stack_name, TemplateBody=_kms_key_template(props))
    _wait_stack(cfn, stack_name)
    original_id = _kms_stack_outputs(cfn, stack_name)["KeyRef"]

    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template({**props, "KeySpec": "RSA_4096"}),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] != "UPDATE_COMPLETE"

    # The original key is untouched: same id, same spec, still usable.
    meta = kms_client.describe_key(KeyId=original_id)["KeyMetadata"]
    assert meta["KeySpec"] == "RSA_2048"
    assert meta["KeyState"] == "Enabled"

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)


def test_cfn_kms_key_unsupported_key_spec_fails_the_stack(cfn):
    """An unimplemented spec must fail the stack, not quietly become symmetric."""
    stack_name = f"cfn-kms-badspec-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template(
            {"KeySpec": "SM2", "KeyUsage": "ENCRYPT_DECRYPT"}
        ),
    )
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] != "CREATE_COMPLETE"

    cfn.delete_stack(StackName=stack_name)


def test_cfn_kms_key_delete_schedules_deletion(cfn, kms_client):
    """Removing a key from a stack schedules deletion; it does not vanish."""
    stack_name = f"cfn-kms-del-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=_kms_key_template({"PendingWindowInDays": 7}),
    )
    _wait_stack(cfn, stack_name)
    key_id = _kms_stack_outputs(cfn, stack_name)["KeyRef"]

    cfn.delete_stack(StackName=stack_name)
    _wait_stack(cfn, stack_name)

    meta = kms_client.describe_key(KeyId=key_id)["KeyMetadata"]
    assert meta["KeyState"] == "PendingDeletion"
    assert meta["Enabled"] is False
    assert "DeletionDate" in meta


# ===========================================================================
# CloudFormation Custom Resource protocol tests
# (merged from the former tests/test_cfn_custom_resource.py — #603)
# Reuses this module's _wait_stack / _regional_cfn_test_client helpers.
# ===========================================================================

_CR_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
_CR_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _cr_make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _cfn_custom_template(func_name, resource_type="Custom::Tester", extra_props=None, outputs=None):
    """Build a CF template with a single custom resource."""
    props = {"ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{func_name}"}
    if extra_props:
        props.update(extra_props)
    tpl = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": resource_type,
                "Properties": props,
            }
        },
    }
    if outputs:
        tpl["Outputs"] = outputs
    return json.dumps(tpl)


# -- token registry smoke test ----------------------------------------------

def test_cfn_response_endpoint_accepts_put(cfn):
    """PUT to /_ministack/cfn-response/{token} returns 200 even for unknown tokens."""
    token = str(_uuid_mod.uuid4())
    payload = json.dumps({"Status": "SUCCESS", "PhysicalResourceId": "x",
                          "RequestId": "r", "StackId": "s", "LogicalResourceId": "l"}).encode()
    req = urllib.request.Request(
        f"{_CR_ENDPOINT}/_ministack/cfn-response/{token}",
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200


# -- Create lifecycle -------------------------------------------------------

_CR_HANDLER_SUCCESS = """\
import json, urllib.request

def handler(event, context):
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": "my-custom-resource-123",
        "Data": {"Endpoint": "https://example.com", "Region": "us-east-1"},
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


_CR_HANDLER_CDK_COMPAT = """\
import json, urllib.request

def handler(event, context):
    props = event["ResourceProperties"]
    managed = props.get("Managed", "true").lower() == "true"
    skip_validation = props.get("SkipDestinationValidation", "false").lower() == "true"
    payload = json.dumps({
        "Status": "SUCCESS",
        "Reason": f"See the details in CloudWatch Log Stream: {context.log_stream_name}",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": "cdk-compatible-resource",
        "Data": {
            "Managed": str(managed),
            "SkipDestinationValidation": str(skip_validation),
            "NestedEnabled": props["Nested"]["Enabled"],
            "NestedCount": props["Nested"]["Count"],
        },
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_create_success(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-success",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
    )
    try:
        cfn.create_stack(
            StackName="cr-t01",
            TemplateBody=_cfn_custom_template("cr-test-success"),
        )
        stack = _wait_stack(cfn, "cr-t01")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t01", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["PhysicalResourceId"] == "my-custom-resource-123"
    finally:
        cfn.delete_stack(StackName="cr-t01")
        _wait_stack(cfn, "cr-t01")
        lam.delete_function(FunctionName="cr-test-success")


def test_custom_resource_cdk_boolean_properties_and_lambda_context(cfn, lam):
    """CDK-style handlers receive string leaves and standard Lambda context."""
    lam.create_function(
        FunctionName="cr-test-cdk-compat",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_CDK_COMPAT)},
    )
    props = {
        "ServiceTimeout": "2",
        "Managed": True,
        "SkipDestinationValidation": False,
        "Nested": {"Enabled": True, "Count": 2},
    }
    outputs = {
        "Managed": {"Value": {"Fn::GetAtt": ["CR", "Managed"]}},
        "SkipDestinationValidation": {
            "Value": {"Fn::GetAtt": ["CR", "SkipDestinationValidation"]}
        },
        "NestedEnabled": {"Value": {"Fn::GetAtt": ["CR", "NestedEnabled"]}},
        "NestedCount": {"Value": {"Fn::GetAtt": ["CR", "NestedCount"]}},
    }
    try:
        cfn.create_stack(
            StackName="cr-t01-cdk-compat",
            TemplateBody=_cfn_custom_template("cr-test-cdk-compat", extra_props=props, outputs=outputs),
        )
        stack = _wait_stack(cfn, "cr-t01-cdk-compat")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        values = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
        assert values == {
            "Managed": "True",
            "SkipDestinationValidation": "False",
            "NestedEnabled": "true",
            "NestedCount": "2",
        }
    finally:
        cfn.delete_stack(StackName="cr-t01-cdk-compat")
        _wait_stack(cfn, "cr-t01-cdk-compat")
        lam.delete_function(FunctionName="cr-test-cdk-compat")


def test_custom_resource_type_prefix(cfn, lam):
    """Custom::Tester and AWS::CloudFormation::CustomResource both work."""
    lam.create_function(
        FunctionName="cr-test-prefix",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
    )
    try:
        cfn.create_stack(
            StackName="cr-t02a",
            TemplateBody=_cfn_custom_template("cr-test-prefix", resource_type="Custom::MyTester"),
        )
        stack = _wait_stack(cfn, "cr-t02a")
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        cfn.create_stack(
            StackName="cr-t02b",
            TemplateBody=_cfn_custom_template("cr-test-prefix", resource_type="AWS::CloudFormation::CustomResource"),
        )
        stack = _wait_stack(cfn, "cr-t02b")
        assert stack["StackStatus"] == "CREATE_COMPLETE"
    finally:
        for name in ("cr-t02a", "cr-t02b"):
            try:
                cfn.delete_stack(StackName=name)
                _wait_stack(cfn, name)
            except Exception:
                pass
        lam.delete_function(FunctionName="cr-test-prefix")


# -- FAILED status -> rollback ----------------------------------------------

_CR_HANDLER_FAILED = """\
import json, urllib.request

def handler(event, context):
    payload = json.dumps({
        "Status": "FAILED",
        "Reason": "Intentional test failure",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": "failed-resource",
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_create_failed_triggers_rollback(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-fail",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_FAILED)},
    )
    try:
        cfn.create_stack(StackName="cr-t03", TemplateBody=_cfn_custom_template("cr-test-fail"))
        stack = _wait_stack(cfn, "cr-t03")
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t03")
            _wait_stack(cfn, "cr-t03")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-fail")


# -- Update lifecycle -------------------------------------------------------

_CR_HANDLER_RECORD = """\
import json, urllib.request

def handler(event, context):
    # Echo what was received so tests can inspect it
    data = {
        "RequestType": event["RequestType"],
        "PhysicalResourceId": event.get("PhysicalResourceId", ""),
        "HasOldProps": str("OldResourceProperties" in event),
        "OldFoo": str(event.get("OldResourceProperties", {}).get("Foo", "")),
        "NewFoo": str(event.get("ResourceProperties", {}).get("Foo", "")),
    }
    pid = event.get("PhysicalResourceId") or "recorded-resource-id"
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": pid,
        "Data": data,
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_update_sends_old_properties(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-record",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_RECORD)},
    )
    try:
        tpl_v1 = _cfn_custom_template("cr-test-record", extra_props={"Foo": "bar-v1"})
        cfn.create_stack(StackName="cr-t04", TemplateBody=tpl_v1)
        _wait_stack(cfn, "cr-t04")

        tpl_v2 = _cfn_custom_template(
            "cr-test-record",
            extra_props={"Foo": "bar-v2"},
            outputs={
                "HasOldPropsOut": {"Value": {"Fn::GetAtt": ["CR", "HasOldProps"]}},
                "OldFooOut":      {"Value": {"Fn::GetAtt": ["CR", "OldFoo"]}},
                "NewFooOut":      {"Value": {"Fn::GetAtt": ["CR", "NewFoo"]}},
            },
        )
        cfn.update_stack(StackName="cr-t04", TemplateBody=tpl_v2)
        stack = _wait_stack(cfn, "cr-t04")
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t04", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["ResourceStatus"] == "UPDATE_COMPLETE"

        # Verify OldResourceProperties were forwarded to the Lambda on Update
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert outputs.get("HasOldPropsOut") == "True", f"OldResourceProperties missing: {outputs}"
        assert outputs.get("OldFooOut") == "bar-v1", f"OldFoo wrong: {outputs}"
        assert outputs.get("NewFooOut") == "bar-v2", f"NewFoo wrong: {outputs}"
    finally:
        cfn.delete_stack(StackName="cr-t04")
        _wait_stack(cfn, "cr-t04")
        lam.delete_function(FunctionName="cr-test-record")


def test_custom_resource_delete_sends_physical_id(cfn, lam):
    """Stack delete must send the PhysicalResourceId from Create to the Lambda."""
    _CR_DELETE_CHECK = """\
import json, urllib.request

def handler(event, context):
    data = {
        "RequestType": event["RequestType"],
        "ReceivedPhysicalId": event.get("PhysicalResourceId", "MISSING"),
    }
    pid = event.get("PhysicalResourceId") or "delete-test-id"
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": pid,
        "Data": data,
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""

    lam.create_function(
        FunctionName="cr-test-delete",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_DELETE_CHECK)},
    )
    try:
        cfn.create_stack(StackName="cr-t05", TemplateBody=_cfn_custom_template("cr-test-delete"))
        _wait_stack(cfn, "cr-t05")

        res = cfn.describe_stack_resource(StackName="cr-t05", LogicalResourceId="CR")
        create_pid = res["StackResourceDetail"]["PhysicalResourceId"]
        assert create_pid  # must be non-empty

        cfn.delete_stack(StackName="cr-t05")
        stack = _wait_stack(cfn, "cr-t05")
        assert stack["StackStatus"] == "DELETE_COMPLETE", stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t05")
            _wait_stack(cfn, "cr-t05")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-delete")


# -- Data accessible via Fn::GetAtt -----------------------------------------

def test_custom_resource_data_via_getatt(cfn, lam, ssm):
    """Data keys returned by the Lambda are accessible via Fn::GetAtt in outputs."""
    lam.create_function(
        FunctionName="cr-test-getatt",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
    )
    tpl = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": "Custom::GetAttTest",
                "Properties": {
                    "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cr-test-getatt",
                },
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cr-t06-endpoint",
                    "Type": "String",
                    "Value": {"Fn::GetAtt": ["CR", "Endpoint"]},
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName="cr-t06", TemplateBody=json.dumps(tpl))
        stack = _wait_stack(cfn, "cr-t06")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        val = ssm.get_parameter(Name="cr-t06-endpoint")["Parameter"]["Value"]
        assert val == "https://example.com"
    finally:
        cfn.delete_stack(StackName="cr-t06")
        _wait_stack(cfn, "cr-t06")
        lam.delete_function(FunctionName="cr-test-getatt")


# -- PhysicalResourceId fallback --------------------------------------------

_CR_HANDLER_NO_PID = """\
import json, urllib.request

def handler(event, context):
    # Deliberately omit PhysicalResourceId - Ministack should use RequestId
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_physical_id_fallback(cfn, lam):
    """When Lambda omits PhysicalResourceId on Create, Ministack falls back to RequestId."""
    lam.create_function(
        FunctionName="cr-test-nopid",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_NO_PID)},
    )
    try:
        cfn.create_stack(StackName="cr-t07", TemplateBody=_cfn_custom_template("cr-test-nopid"))
        stack = _wait_stack(cfn, "cr-t07")
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        res = cfn.describe_stack_resource(StackName="cr-t07", LogicalResourceId="CR")
        pid = res["StackResourceDetail"]["PhysicalResourceId"]
        # Must be a non-empty UUID (the RequestId fallback)
        assert pid and len(pid) > 8
    finally:
        cfn.delete_stack(StackName="cr-t07")
        _wait_stack(cfn, "cr-t07")
        lam.delete_function(FunctionName="cr-test-nopid")


# -- Async response (Lambda returns before PUTting ResponseURL) -------------

_CR_HANDLER_ASYNC = """\
import json, threading, time, urllib.request

def handler(event, context):
    # Return immediately; a background thread delivers the response after a delay.
    captured = dict(event)

    def respond():
        time.sleep(0.5)
        payload = json.dumps({
            "Status": "SUCCESS",
            "RequestId": captured["RequestId"],
            "StackId": captured["StackId"],
            "LogicalResourceId": captured["LogicalResourceId"],
            "PhysicalResourceId": "async-resource-id",
            "Data": {"AsyncResult": "done"},
        }).encode()
        req = urllib.request.Request(
            captured["ResponseURL"],
            data=payload,
            method="PUT",
            headers={"content-type": "", "content-length": str(len(payload))},
        )
        urllib.request.urlopen(req, timeout=10)

    threading.Thread(target=respond, daemon=True).start()
"""


def test_custom_resource_async_response(cfn, lam):
    """Lambda returns without responding; background thread PUTs to ResponseURL later."""
    lam.create_function(
        FunctionName="cr-test-async",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_ASYNC)},
    )
    try:
        cfn.create_stack(StackName="cr-t08", TemplateBody=_cfn_custom_template("cr-test-async"))
        stack = _wait_stack(cfn, "cr-t08", timeout=30)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t08", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["PhysicalResourceId"] == "async-resource-id"
    finally:
        cfn.delete_stack(StackName="cr-t08")
        _wait_stack(cfn, "cr-t08")
        lam.delete_function(FunctionName="cr-test-async")


# -- Timeout ----------------------------------------------------------------

_CR_HANDLER_SILENT = """\
def handler(event, context):
    # Never PUTs to ResponseURL - triggers timeout
    pass
"""


def test_custom_resource_timeout_fails_stack(cfn, lam):
    """ServiceTimeout=2 with a silent Lambda causes the stack to fail."""
    lam.create_function(
        FunctionName="cr-test-timeout",
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SILENT)},
    )
    tpl = _cfn_custom_template("cr-test-timeout", extra_props={"ServiceTimeout": "2"})
    try:
        cfn.create_stack(StackName="cr-t09", TemplateBody=tpl)
        stack = _wait_stack(cfn, "cr-t09", timeout=30)
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t09")
            _wait_stack(cfn, "cr-t09")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-timeout")


# -- Lambda not found -------------------------------------------------------

def test_custom_resource_lambda_not_found(cfn):
    """ServiceToken pointing to a nonexistent Lambda fails the stack immediately."""
    tpl = _cfn_custom_template("cr-does-not-exist-function")
    try:
        cfn.create_stack(StackName="cr-t10", TemplateBody=tpl)
        stack = _wait_stack(cfn, "cr-t10")
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t10")
            _wait_stack(cfn, "cr-t10")
        except Exception:
            pass


def test_custom_resource_rejects_cross_region_lambda_token(cfn):
    west_lam = _regional_cfn_test_client("lambda", "us-west-2")
    fn_name = f"cr-cross-region-{_uuid_mod.uuid4().hex[:8]}"
    stack_name = f"cr-cross-{_uuid_mod.uuid4().hex[:8]}"
    west_arn = west_lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
    )["FunctionArn"]

    tpl = _cfn_custom_template(fn_name, extra_props={"ServiceToken": west_arn})
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=tpl)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        west_lam.delete_function(FunctionName=fn_name)


def test_cfn_ses_configuration_set_and_event_destination(cfn, ses, sesv2):
    cs_name = f"cfn-ses-cs-{_uuid_mod.uuid4().hex[:8]}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "AlarmTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": f"{cs_name}-events"},
            },
            "ConfigSet": {
                "Type": "AWS::SES::ConfigurationSet",
                "Properties": {"Name": cs_name},
            },
            "EventDest": {
                "Type": "AWS::SES::ConfigurationSetEventDestination",
                "Properties": {
                    "ConfigurationSetName": {"Ref": "ConfigSet"},
                    "EventDestination": {
                        "Name": "to-sns",
                        "Enabled": True,
                        "MatchingEventTypes": ["send", "bounce", "complaint"],
                        "SnsDestination": {"TopicARN": {"Ref": "AlarmTopic"}},
                    },
                },
            },
        },
        "Outputs": {
            "ConfigSetName": {"Value": {"Ref": "ConfigSet"}},
        },
    }
    cfn.create_stack(StackName="cfn-ses-cs", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-ses-cs")
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

    # Ref on the configuration set resolves to its name.
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
    assert outputs["ConfigSetName"] == cs_name

    # The set is visible through both the classic and v2 SES APIs.
    described = ses.describe_configuration_set(ConfigurationSetName=cs_name)
    assert described["ConfigurationSet"]["Name"] == cs_name
    listed = ses.list_configuration_sets()["ConfigurationSets"]
    assert any(cs.get("Name") == cs_name for cs in listed)
    assert sesv2.get_configuration_set(
        ConfigurationSetName=cs_name)["ConfigurationSetName"] == cs_name

    cfn.delete_stack(StackName="cfn-ses-cs")
    _wait_stack(cfn, "cfn-ses-cs")

    with pytest.raises(ClientError):
        ses.describe_configuration_set(ConfigurationSetName=cs_name)


# ===========================================================================
# Loud deletes — DELETE_FAILED / ROLLBACK_FAILED stack states, and the
# delete handlers for the types that used to leak silently
# ===========================================================================

_CR_HANDLER_DELETE_FAILS = """\
import json, urllib.request

def handler(event, context):
    status = "FAILED" if event["RequestType"] == "Delete" else "SUCCESS"
    payload = json.dumps({
        "Status": status,
        "Reason": "delete refused for testing",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": event.get("PhysicalResourceId", "delete-fails-cr"),
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_cfn_stack_delete_failure_lands_delete_failed(cfn, lam, sns):
    """A resource delete that fails lands the stack in DELETE_FAILED, not a
    silent DELETE_COMPLETE: the other resources are still deleted, the failed
    resource is retained, and a retried DeleteStack can finish the job."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-del-fail-{suffix}"
    stack_name = f"cfn-delete-failed-{suffix}"
    topic_name = f"cfn-delete-failed-topic-{suffix}"

    def create_handler():
        lam.create_function(
            FunctionName=fn,
            Runtime="python3.12",
            Role=_CR_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
        )

    create_handler()
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Topic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": topic_name},
            },
            "CR": {
                "Type": "Custom::Tester",
                "Properties": {
                    "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        # Remove the custom resource's handler so its Delete cannot be delivered.
        lam.delete_function(FunctionName=fn)

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_FAILED"
        assert "CR" in stack.get("StackStatusReason", "")

        # The deletable resource is gone regardless — CFN keeps deleting.
        topic_arns = {t["TopicArn"] for t in sns.list_topics()["Topics"]}
        assert not any(arn.endswith(f":{topic_name}") for arn in topic_arns)

        # The failed resource is retained on the still-existing stack.
        retained = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        assert [r["LogicalResourceId"] for r in retained] == ["CR"]
        assert retained[0]["ResourceStatus"] == "DELETE_FAILED"
        assert fn in retained[0]["ResourceStatusReason"]
        detail = cfn.describe_stack_resource(
            StackName=stack_name, LogicalResourceId="CR")["StackResourceDetail"]
        assert detail["ResourceStatus"] == "DELETE_FAILED"
        assert fn in detail["ResourceStatusReason"]
        assert detail["LastUpdatedTimestamp"]

        # And the failure is visible as a stack-level event.
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(
            e["ResourceType"] == "AWS::CloudFormation::Stack"
            and e["ResourceStatus"] == "DELETE_FAILED"
            for e in events
        )

        # Restoring the handler and retrying the delete completes it.
        create_handler()
        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_stack_delete_failed_keeps_exports(cfn, lam):
    """A stack landing in DELETE_FAILED keeps its exports — they belong to
    the still-existing stack and only go away with DELETE_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-del-exp-{suffix}"
    stack_name = f"cfn-delete-failed-exp-{suffix}"
    export_name = f"cfn-del-exp-{suffix}"

    def create_handler():
        lam.create_function(
            FunctionName=fn,
            Runtime="python3.12",
            Role=_CR_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SUCCESS)},
        )

    create_handler()
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": "Custom::Tester",
                "Properties": {
                    "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}",
                },
            },
        },
        "Outputs": {
            "Kept": {"Value": "still-here", "Export": {"Name": export_name}},
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        # Remove the custom resource's handler so its Delete cannot be delivered.
        lam.delete_function(FunctionName=fn)
        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_FAILED"

        exports = {e["Name"]: e["Value"]
                   for e in _all_pages(cfn, "list_exports", "Exports")}
        assert exports.get(export_name) == "still-here"

        # The completed retry removes the export with the stack.
        create_handler()
        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE"
        exports = {e["Name"] for e in _all_pages(cfn, "list_exports", "Exports")}
        assert export_name not in exports
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def _stack_with_a_failed_cleanup_delete(cfn, lam, fn, stack_name, queue_name):
    """Create a stack (queue, SSM marker, custom resource whose handler
    refuses the Delete), then drop the custom resource from the template.
    Returns the template without it; ``Rev`` changes the marker's value."""
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    resources = {
        "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": queue_name}},
        "Marker": {
            "Type": "AWS::SSM::Parameter",
            "Properties": {"Name": f"/{stack_name}/rev", "Type": "String",
                           "Value": {"Ref": "Rev"}},
        },
    }
    cr = {
        "Type": "Custom::Tester",
        "Properties": {
            "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}",
        },
    }
    without_cr = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {"Rev": {"Type": "String", "Default": "1"}},
        "Resources": resources,
    }
    with_cr = json.loads(json.dumps(without_cr))
    with_cr["Resources"]["CR"] = cr
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(with_cr))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(without_cr))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    return without_cr


def _swap_cr_handler(lam, fn, code):
    lam.delete_function(FunctionName=fn)
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(code)},
    )


def test_cfn_update_cleanup_delete_failure_keeps_the_resource_visible(cfn, lam, ssm):
    """A resource dropped from the template whose delete fails during the
    cleanup phase stays in the stack as DELETE_FAILED with its reason: the
    update still ends UPDATE_COMPLETE (the new template is in effect), and the
    next update with a changed template retries the delete, which removes the
    resource once the delete works."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-cleanup-fail-{suffix}"
    stack_name = f"cfn-cleanup-failed-{suffix}"
    queue_name = f"cfn-cleanup-failed-q-{suffix}"
    try:
        without_cr = _stack_with_a_failed_cleanup_delete(cfn, lam, fn, stack_name, queue_name)

        # The failed cleanup is an event and a visible resource, not a log line.
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(e["LogicalResourceId"] == "CR" and e["ResourceStatus"] == "DELETE_FAILED"
                   for e in events)
        by_id = {r["LogicalResourceId"]: r
                 for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]}
        assert set(by_id) == {"Queue", "Marker", "CR"}
        assert by_id["CR"]["ResourceStatus"] == "DELETE_FAILED"
        assert "delete refused for testing" in by_id["CR"]["ResourceStatusReason"]
        assert by_id["Queue"]["ResourceStatus"] == "UPDATE_COMPLETE"
        assert "ResourceStatusReason" not in by_id["Queue"]
        detail = cfn.describe_stack_resource(
            StackName=stack_name, LogicalResourceId="CR")["StackResourceDetail"]
        assert detail["ResourceStatus"] == "DELETE_FAILED"
        assert "delete refused for testing" in detail["ResourceStatusReason"]
        assert detail["LastUpdatedTimestamp"]
        # The new template is what the stack runs.
        assert "CR" not in cfn.get_template(StackName=stack_name)["TemplateBody"]["Resources"]

        # A changed template retries the delete; still refused, the resource
        # stays DELETE_FAILED and the update itself succeeds.
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(without_cr),
                         Parameters=[{"ParameterKey": "Rev", "ParameterValue": "2"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert ssm.get_parameter(Name=f"/{stack_name}/rev")["Parameter"]["Value"] == "2"
        by_id = {r["LogicalResourceId"]: r
                 for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]}
        assert by_id["CR"]["ResourceStatus"] == "DELETE_FAILED"
        assert "delete refused for testing" in by_id["CR"]["ResourceStatusReason"]

        # With a handler that accepts the delete, the next update removes it.
        _swap_cr_handler(lam, fn, _CR_HANDLER_SUCCESS)
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(without_cr),
                         Parameters=[{"ParameterKey": "Rev", "ParameterValue": "3"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        listed = {r["LogicalResourceId"]
                  for r in cfn.describe_stack_resources(StackName=stack_name)["StackResources"]}
        assert listed == {"Queue", "Marker"}
        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_delete_stack_retries_a_failed_cleanup_delete(cfn, lam, sqs):
    """DeleteStack reaches a resource the template no longer declares: the
    retry is refused again, the stack lands DELETE_FAILED naming it while the
    declared resources are gone, and a delete that works completes the stack."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-cleanup-del-{suffix}"
    stack_name = f"cfn-cleanup-delete-{suffix}"
    queue_name = f"cfn-cleanup-delete-q-{suffix}"
    try:
        _stack_with_a_failed_cleanup_delete(cfn, lam, fn, stack_name, queue_name)

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_FAILED"
        assert "CR" in stack.get("StackStatusReason", "")
        remaining = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        assert [r["LogicalResourceId"] for r in remaining] == ["CR"]
        assert "delete refused for testing" in remaining[0]["ResourceStatusReason"]
        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName=queue_name)

        _swap_cr_handler(lam, fn, _CR_HANDLER_SUCCESS)
        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_create_rollback_delete_failure_lands_rollback_failed(cfn, lam):
    """A rollback that cannot undo what it created reports ROLLBACK_FAILED
    instead of pretending the rollback completed."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-rb-fail-{suffix}"
    stack_name = f"cfn-rollback-failed-{suffix}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": "Custom::Tester",
                "Properties": {
                    "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}",
                },
            },
            # Provisioned after CR; its create fails (custom resource whose
            # Lambda does not exist) and triggers the rollback that has to
            # delete CR again.
            "Bad": {**_FAILING_RESOURCE, "DependsOn": "CR"},
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "ROLLBACK_FAILED", stack.get("StackStatusReason")

        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(
            e["LogicalResourceId"] == "CR" and e["ResourceStatus"] == "DELETE_FAILED"
            for e in events
        )
        assert any(
            e["ResourceType"] == "AWS::CloudFormation::Stack"
            and e["ResourceStatus"] == "ROLLBACK_FAILED"
            for e in events
        )
    finally:
        # Let the handler accept deletes again so the stack can be cleaned up.
        try:
            lam.update_function_code(
                FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS)
            )
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_update_rollback_delete_failure_lands_update_rollback_failed(cfn, lam):
    """An update rollback whose delete fails lands UPDATE_ROLLBACK_FAILED and
    keeps the previous resources on the stack."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-urb-fail-{suffix}"
    stack_name = f"cfn-update-rollback-failed-{suffix}"
    topic_name = f"cfn-urb-topic-{suffix}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    base = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Topic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": topic_name},
            },
        },
    }
    updated = json.loads(json.dumps(base))
    updated["Resources"]["CR"] = {
        "Type": "Custom::Tester",
        "Properties": {
            "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}",
        },
    }
    updated["Resources"]["Bad"] = {
        **_FAILING_RESOURCE,
        "DependsOn": "CR",
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED", stack.get("StackStatusReason")

        # The pre-update resources survive on the stack.
        retained = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        assert "Topic" in [r["LogicalResourceId"] for r in retained]

        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(
            e["ResourceType"] == "AWS::CloudFormation::Stack"
            and e["ResourceStatus"] == "UPDATE_ROLLBACK_FAILED"
            for e in events
        )
    finally:
        try:
            lam.update_function_code(
                FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS)
            )
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


_CR_HANDLER_SLOW = """\
import json, time, urllib.request

def handler(event, context):
    if event["RequestType"] == "Create":
        time.sleep(2)
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": event.get("PhysicalResourceId") or "slow-custom-resource",
        "Data": {},
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"], data=payload, method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_cfn_continue_update_rollback_recovers_the_stack(cfn, lam):
    """A stack in UPDATE_ROLLBACK_FAILED is not updatable; ContinueUpdateRollback
    retries the failed deletes (and fails again while the resource still cannot
    be deleted), an unknown ResourcesToSkip entry is refused, and once the
    delete works the retry lands UPDATE_ROLLBACK_COMPLETE and the stack is
    updatable again. Any other status is refused."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-cur-{suffix}"
    stack_name = f"cfn-continue-rollback-{suffix}"
    lam.create_function(
        FunctionName=fn, Runtime="python3.12", Role=_CR_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    base = {"Resources": {"Topic": {"Type": "AWS::SNS::Topic",
                                    "Properties": {"TopicName": f"cfn-cur-{suffix}"}}}}
    updated = json.loads(json.dumps(base))
    updated["Resources"]["CR"] = {"Type": "Custom::Tester", "Properties": {
        "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"}}
    updated["Resources"]["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "CR"}
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        with pytest.raises(ClientError) as exc:
            cfn.continue_update_rollback(StackName=stack_name)
        assert "cannot be called from current stack status" in (
            exc.value.response["Error"]["Message"])

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED", stack.get("StackStatusReason")

        cfn.continue_update_rollback(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED", stack.get("StackStatusReason")

        with pytest.raises(ClientError) as exc:
            cfn.continue_update_rollback(StackName=stack_name, ResourcesToSkip=["Topic"])
        assert "Topic" in exc.value.response["Error"]["Message"]

        # Once the resource can be deleted, the retry completes the rollback
        # without skipping anything.
        lam.update_function_code(FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS))
        cfn.continue_update_rollback(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(e["LogicalResourceId"] == "CR" and e["ResourceStatus"] == "DELETE_COMPLETE"
                   for e in events)

        again = json.loads(json.dumps(base))
        again["Resources"]["Topic"]["Properties"]["DisplayName"] = "after"
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(again))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS))
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_continue_update_rollback_skips_the_named_resource(cfn, lam):
    """ResourcesToSkip leaves the resource in place (an UPDATE_COMPLETE event)
    and the stack still reaches UPDATE_ROLLBACK_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-cur-skip-{suffix}"
    stack_name = f"cfn-continue-skip-{suffix}"
    lam.create_function(
        FunctionName=fn, Runtime="python3.12", Role=_CR_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    base = {"Resources": {"Topic": {"Type": "AWS::SNS::Topic",
                                    "Properties": {"TopicName": f"cfn-cur-skip-{suffix}"}}}}
    updated = json.loads(json.dumps(base))
    updated["Resources"]["CR"] = {"Type": "Custom::Tester", "Properties": {
        "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"}}
    updated["Resources"]["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "CR"}
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_ROLLBACK_FAILED"

        cfn.continue_update_rollback(StackName=stack_name, ResourcesToSkip=["CR"])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert any(e["LogicalResourceId"] == "CR" and e["ResourceStatus"] == "UPDATE_COMPLETE"
                   for e in events)
    finally:
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS))
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_cancel_update_stack_rolls_the_update_back(cfn, lam):
    """CancelUpdateStack on an UPDATE_IN_PROGRESS stack stops before the next
    resource and rolls back to the previous state ("User Initiated"); on any
    other status it is refused."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-cancel-{suffix}"
    stack_name = f"cfn-cancel-{suffix}"
    lam.create_function(
        FunctionName=fn, Runtime="python3.12", Role=_CR_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_SLOW)}, Timeout=30,
    )
    base = {"Resources": {"Topic": {"Type": "AWS::SNS::Topic",
                                    "Properties": {"TopicName": f"cfn-cancel-{suffix}"}}}}
    updated = json.loads(json.dumps(base))
    updated["Resources"]["Slow"] = {"Type": "Custom::Tester", "Properties": {
        "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"}}
    updated["Resources"]["Later"] = {"Type": "AWS::SNS::Topic", "DependsOn": "Slow",
                                     "Properties": {"TopicName": f"cfn-cancel-later-{suffix}"}}
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        with pytest.raises(ClientError) as exc:
            cfn.cancel_update_stack(StackName=stack_name)
        assert exc.value.response["Error"]["Message"] == (
            "CancelUpdateStack cannot be called from current stack status")

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        assert cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"] == (
            "UPDATE_IN_PROGRESS")
        cfn.cancel_update_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name, timeout=60)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert [r["LogicalResourceId"] for r in
                cfn.describe_stack_resources(StackName=stack_name)["StackResources"]] == ["Topic"]
        assert "User Initiated" in _stack_event_reasons(cfn, stack_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_lambda_version_stack_delete_removes_version(cfn, lam):
    """AWS::Lambda::Version now has a real delete handler: deleting the stack
    removes the published version instead of leaking it, and Ref resolves to
    the qualified version ARN as on AWS."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cfn-ver-del-{suffix}"
    stack_name = f"cfn-ver-del-{suffix}"
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fn, Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Ver": {
                "Type": "AWS::Lambda::Version",
                "Properties": {"FunctionName": fn},
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        detail = cfn.describe_stack_resource(
            StackName=stack_name, LogicalResourceId="Ver"
        )["StackResourceDetail"]
        assert detail["PhysicalResourceId"].endswith(f":function:{fn}:1")

        versions = lam.list_versions_by_function(FunctionName=fn)["Versions"]
        assert "1" in [v["Version"] for v in versions]

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE"

        # The version is gone; the (externally owned) function is untouched.
        versions = lam.list_versions_by_function(FunctionName=fn)["Versions"]
        assert [v["Version"] for v in versions] == ["$LATEST"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_appsync_schema_stack_delete_removes_schema(cfn, appsync):
    """AWS::AppSync::GraphQLSchema now has a real delete handler: deleting the
    stack removes the schema from the (externally owned) API."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-schema-{suffix}"
    api = appsync.create_graphql_api(
        name=f"cfn-schema-api-{suffix}", authenticationType="API_KEY"
    )["graphqlApi"]
    api_id = api["apiId"]
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Schema": {
                "Type": "AWS::AppSync::GraphQLSchema",
                "Properties": {
                    "ApiId": api_id,
                    "Definition": "type Query { hello: String }",
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert len(appsync.list_types(apiId=api_id, format="SDL")["types"]) == 1

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE"
        assert appsync.list_types(apiId=api_id, format="SDL")["types"] == []
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            appsync.delete_graphql_api(apiId=api_id)
        except ClientError:
            pass


# ===========================================================================
# In-place update handlers — deploy, update a mutable property, assert the
# physical id survived and the new value is visible through the service API
# ===========================================================================

def _cfn_update_roundtrip(cfn, stack_name, template_v1, template_v2):
    """Create with v1, assert CREATE_COMPLETE; update to v2, assert
    UPDATE_COMPLETE. Returns nothing — callers assert on the services."""
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template_v1))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template_v2))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")


def _stack_physical_id(cfn, stack_name, logical_id):
    return cfn.describe_stack_resource(
        StackName=stack_name, LogicalResourceId=logical_id
    )["StackResourceDetail"]["PhysicalResourceId"]


def test_cfn_update_lambda_function_in_place(cfn, lam):
    """A Lambda function updates through UpdateFunctionCode/-Configuration:
    the published version and the resource policy — which the old fall-back
    re-create wiped — survive, and the new code/config are live."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-lambda-{suffix}"
    fn = f"cfn-up-lambda-{suffix}"

    def tpl(source, timeout, env_value):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Fn": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {
                        "FunctionName": fn,
                        "Runtime": "python3.12",
                        "Handler": "index.handler",
                        "Role": _CR_LAMBDA_ROLE,
                        "Timeout": timeout,
                        "Environment": {"Variables": {"STAGE": env_value}},
                        "Code": {"ZipFile": source},
                    },
                },
                "Ver": {
                    "Type": "AWS::Lambda::Version",
                    "Properties": {"FunctionName": {"Ref": "Fn"}},
                },
            },
        }

    v1 = tpl("def handler(e,c): return 1", 3, "one")
    v2 = tpl("def handler(e,c): return 2", 7, "two")
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(v1))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        # State CloudFormation doesn't own: a resource-policy statement.
        lam.add_permission(
            FunctionName=fn, StatementId="ext", Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
        )
        sha_before = lam.get_function_configuration(FunctionName=fn)["CodeSha256"]

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(v2))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        assert _stack_physical_id(cfn, stack_name, "Fn") == fn
        config = lam.get_function_configuration(FunctionName=fn)
        assert config["Timeout"] == 7
        assert config["Environment"]["Variables"] == {"STAGE": "two"}
        assert config["CodeSha256"] != sha_before

        # The version published by the stack and the externally added policy
        # statement both survive the update.
        versions = [v["Version"] for v in
                    lam.list_versions_by_function(FunctionName=fn)["Versions"]]
        assert "1" in versions
        policy = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        assert [s["Sid"] for s in policy["Statement"]] == ["ext"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_update_dynamodb_table_in_place(cfn, ddb):
    """A DynamoDB table updates through UpdateTable: items survive, a GSI is
    added, the stream specification lands."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-ddb-{suffix}"
    table = f"cfn-up-ddb-{suffix}"

    def tpl(with_gsi, with_stream):
        props = {
            "TableName": table,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsipk", "AttributeType": "S"},
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        }
        if with_gsi:
            props["GlobalSecondaryIndexes"] = [{
                "IndexName": "by-gsipk",
                "KeySchema": [{"AttributeName": "gsipk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }]
        if with_stream:
            props["StreamSpecification"] = {"StreamViewType": "NEW_AND_OLD_IMAGES"}
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"Table": {"Type": "AWS::DynamoDB::Table", "Properties": props}},
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(False, False)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        ddb.put_item(TableName=table, Item={"pk": {"S": "x"}, "gsipk": {"S": "y"}})

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(True, True)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        assert _stack_physical_id(cfn, stack_name, "Table") == table
        desc = ddb.describe_table(TableName=table)["Table"]
        assert [g["IndexName"] for g in desc.get("GlobalSecondaryIndexes", [])] == ["by-gsipk"]
        assert desc.get("LatestStreamArn")
        # The item survived — the table was not re-created.
        item = ddb.get_item(TableName=table, Key={"pk": {"S": "x"}})
        assert item.get("Item", {}).get("gsipk", {}).get("S") == "y"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_apigw_rest_api_in_place(cfn, apigw_v1):
    """A REST API's mutable fields patch in place; the API id survives."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-restapi-{suffix}"

    def tpl(description):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Api": {
                    "Type": "AWS::ApiGateway::RestApi",
                    "Properties": {"Name": f"cfn-up-api-{suffix}", "Description": description},
                },
            },
        }

    try:
        _cfn_update_roundtrip(cfn, stack_name, tpl("before"), tpl("after"))
        api_id = _stack_physical_id(cfn, stack_name, "Api")
        api = apigw_v1.get_rest_api(restApiId=api_id)
        assert api["description"] == "after"
        assert api["name"] == f"cfn-up-api-{suffix}"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_apigw_resource_method_stage_in_place(cfn, apigw_v1):
    """Method and Stage update in place under a stable REST API; a Resource's
    PathPart change is a replacement (all its properties are create-only)."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-apigw-{suffix}"

    def tpl(path_part, api_key_required, stage_description):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Api": {
                    "Type": "AWS::ApiGateway::RestApi",
                    "Properties": {"Name": f"cfn-up-apigw-{suffix}"},
                },
                "Res": {
                    "Type": "AWS::ApiGateway::Resource",
                    "Properties": {
                        "RestApiId": {"Ref": "Api"},
                        "ParentId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
                        "PathPart": path_part,
                    },
                },
                "Method": {
                    "Type": "AWS::ApiGateway::Method",
                    "Properties": {
                        "RestApiId": {"Ref": "Api"},
                        "ResourceId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
                        "HttpMethod": "GET",
                        "AuthorizationType": "NONE",
                        "ApiKeyRequired": api_key_required,
                        "Integration": {"Type": "MOCK"},
                    },
                },
                "Deployment": {
                    "Type": "AWS::ApiGateway::Deployment",
                    "DependsOn": "Method",
                    "Properties": {"RestApiId": {"Ref": "Api"}},
                },
                "Stage": {
                    "Type": "AWS::ApiGateway::Stage",
                    "Properties": {
                        "RestApiId": {"Ref": "Api"},
                        "StageName": "test",
                        "DeploymentId": {"Ref": "Deployment"},
                        "Description": stage_description,
                    },
                },
            },
        }

    try:
        _cfn_update_roundtrip(
            cfn, stack_name,
            tpl("orders", False, "before"),
            tpl("invoices", True, "after"),
        )
        api_id = _stack_physical_id(cfn, stack_name, "Api")

        # Resource replaced: only the new path exists.
        paths = {r["path"] for r in apigw_v1.get_resources(restApiId=api_id)["items"]}
        assert "/invoices" in paths
        assert "/orders" not in paths

        # Method updated in place on the same identity.
        root_id = next(
            r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"]
            if r["path"] == "/"
        )
        method = apigw_v1.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
        assert method["apiKeyRequired"] is True

        # Stage updated in place under its stable name.
        stage = apigw_v1.get_stage(restApiId=api_id, stageName="test")
        assert stage["description"] == "after"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_iot_topic_rule_in_place(cfn, iot_client):
    """A topic rule's payload is replaced in place, as ReplaceTopicRule does;
    the rule name, ARN and creation time survive."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-iotrule-{suffix}"
    rule_name = f"cfn_up_rule_{suffix}"

    def tpl(topic):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Rule": {
                    "Type": "AWS::IoT::TopicRule",
                    "Properties": {
                        "RuleName": rule_name,
                        "TopicRulePayload": {
                            "Sql": f"SELECT * FROM '{topic}'",
                            "Actions": [{"Republish": {
                                "Topic": "out/topic",
                                "RoleArn": "arn:aws:iam::000000000000:role/iot-role",
                            }}],
                        },
                    },
                },
            },
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("things/in")))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        created_at = iot_client.get_topic_rule(ruleName=rule_name)["rule"]["createdAt"]

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("things/other")))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        assert _stack_physical_id(cfn, stack_name, "Rule") == rule_name
        rule = iot_client.get_topic_rule(ruleName=rule_name)["rule"]
        assert rule["sql"] == "SELECT * FROM 'things/other'"
        assert rule["createdAt"] == created_at
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_sns_topic_in_place(cfn, sns):
    """A topic's DisplayName updates in place: the ARN — and a subscription
    created outside the stack — survive."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-sns-{suffix}"
    topic_name = f"cfn-up-sns-{suffix}"

    def tpl(display_name):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Topic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {"TopicName": topic_name, "DisplayName": display_name},
                },
            },
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("before")))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        topic_arn = _stack_physical_id(cfn, stack_name, "Topic")
        sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint="ops@example.com")

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("after")))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        assert _stack_physical_id(cfn, stack_name, "Topic") == topic_arn
        attrs = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        assert attrs["DisplayName"] == "after"
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        assert [s["Endpoint"] for s in subs] == ["ops@example.com"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_sns_topic_keeps_standalone_subscription(cfn, sns):
    """Removing an inline Subscription entry must not delete a standalone
    AWS::SNS::Subscription record carrying the same protocol and endpoint."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-sns-sub-{suffix}"
    topic_name = f"cfn-up-sns-sub-{suffix}"
    endpoint = "shared@example.com"

    def tpl(with_inline):
        topic_props = {"TopicName": topic_name}
        if with_inline:
            topic_props["Subscription"] = [{"Protocol": "email", "Endpoint": endpoint}]
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Topic": {"Type": "AWS::SNS::Topic", "Properties": topic_props},
                "Sub": {
                    "Type": "AWS::SNS::Subscription",
                    "Properties": {
                        "TopicArn": {"Ref": "Topic"},
                        "Protocol": "email",
                        "Endpoint": endpoint,
                    },
                },
            },
        }

    try:
        _cfn_update_roundtrip(cfn, stack_name, tpl(True), tpl(False))
        topic_arn = _stack_physical_id(cfn, stack_name, "Topic")
        standalone_arn = _stack_physical_id(cfn, stack_name, "Sub")
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        assert [s["SubscriptionArn"] for s in subs] == [standalone_arn]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_sqs_queue_in_place(cfn, sqs):
    """A queue's attributes update in place: the URL and the messages in the
    queue survive."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-sqs-{suffix}"
    queue_name = f"cfn-up-sqs-{suffix}"

    def tpl(visibility):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": queue_name, "VisibilityTimeout": visibility},
                },
            },
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(30)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        queue_url = _stack_physical_id(cfn, stack_name, "Queue")
        sqs.send_message(QueueUrl=queue_url, MessageBody="survives")

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(120)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        assert _stack_physical_id(cfn, stack_name, "Queue") == queue_url
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]
        assert attrs["VisibilityTimeout"] == "120"
        messages = sqs.receive_message(QueueUrl=queue_url).get("Messages", [])
        assert [m["Body"] for m in messages] == ["survives"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_sqs_queue_rename_replaces(cfn, sqs):
    """Changing QueueName is a replacement: the stack tracks the new queue's
    URL as the physical id and the old queue is gone."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-sqs-rn-{suffix}"
    old_name = f"cfn-up-sqs-rn-old-{suffix}"
    new_name = f"cfn-up-sqs-rn-new-{suffix}"

    def tpl(queue_name):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": queue_name},
                },
            },
        }

    try:
        _cfn_update_roundtrip(cfn, stack_name, tpl(old_name), tpl(new_name))
        queue_url = _stack_physical_id(cfn, stack_name, "Queue")
        assert queue_url.endswith(f"/{new_name}")
        assert sqs.get_queue_url(QueueName=new_name)["QueueUrl"] == queue_url
        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName=old_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_logs_log_group_in_place(cfn, logs):
    """A log group's retention updates in place; its streams survive."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-logs-{suffix}"
    group_name = f"/cfn/up/{suffix}"

    def tpl(retention):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Group": {
                    "Type": "AWS::Logs::LogGroup",
                    "Properties": {"LogGroupName": group_name, "RetentionInDays": retention},
                },
            },
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(7)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        logs.create_log_stream(logGroupName=group_name, logStreamName="ext-stream")

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl(30)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        group = logs.describe_log_groups(
            logGroupNamePrefix=group_name)["logGroups"][0]
        assert group["retentionInDays"] == 30
        streams = logs.describe_log_streams(logGroupName=group_name)["logStreams"]
        assert [s["logStreamName"] for s in streams] == ["ext-stream"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_s3_bucket_policy_in_place(cfn, s3):
    """A bucket policy's document is rewritten in place on the same bucket."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-bucketpolicy-{suffix}"
    bucket = f"cfn-up-bucketpolicy-{suffix}"
    s3.create_bucket(Bucket=bucket)

    def tpl(sid):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Policy": {
                    "Type": "AWS::S3::BucketPolicy",
                    "Properties": {
                        "Bucket": bucket,
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Sid": sid,
                                "Effect": "Allow",
                                "Principal": "*",
                                "Action": "s3:GetObject",
                                "Resource": f"arn:aws:s3:::{bucket}/*",
                            }],
                        },
                    },
                },
            },
        }

    try:
        _cfn_update_roundtrip(cfn, stack_name, tpl("Before"), tpl("After"))
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
        assert policy["Statement"][0]["Sid"] == "After"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            s3.delete_bucket(Bucket=bucket)
        except ClientError:
            pass


def test_cfn_update_iam_role_in_place(cfn, iam):
    """A role updates in place: ARN and RoleId survive, the template's inline
    policies and managed attachments reconcile, and an attachment made outside
    the stack is untouched."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-role-{suffix}"
    role_name = f"cfn-up-role-{suffix}"
    ext_policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
    assume = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    def tpl(inline_name, max_session):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Role": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "RoleName": role_name,
                        "AssumeRolePolicyDocument": assume,
                        "MaxSessionDuration": max_session,
                        "Policies": [{
                            "PolicyName": inline_name,
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [{
                                    "Effect": "Allow",
                                    "Action": "s3:ListBucket",
                                    "Resource": "*",
                                }],
                            },
                        }],
                    },
                },
            },
        }

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("inline-a", 3600)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        role_id_before = iam.get_role(RoleName=role_name)["Role"]["RoleId"]
        iam.attach_role_policy(RoleName=role_name, PolicyArn=ext_policy_arn)

        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(tpl("inline-b", 7200)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        role = iam.get_role(RoleName=role_name)["Role"]
        assert role["RoleId"] == role_id_before
        assert role["MaxSessionDuration"] == 7200
        inline = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
        assert inline == ["inline-b"]
        attached = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
        assert [p["PolicyArn"] for p in attached] == [ext_policy_arn]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_update_iam_managed_policy_in_place(cfn, iam):
    """A managed policy's document update creates a new default version on
    the same ARN, exactly as CloudFormation's handler does."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-up-mp-{suffix}"
    policy_name = f"cfn-up-mp-{suffix}"

    def tpl(action):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Policy": {
                    "Type": "AWS::IAM::ManagedPolicy",
                    "Properties": {
                        "ManagedPolicyName": policy_name,
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Effect": "Allow",
                                "Action": action,
                                "Resource": "*",
                            }],
                        },
                    },
                },
            },
        }

    try:
        _cfn_update_roundtrip(cfn, stack_name, tpl("s3:ListBucket"), tpl("s3:GetObject"))
        policy_arn = _stack_physical_id(cfn, stack_name, "Policy")
        policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
        assert policy["DefaultVersionId"] == "v2"
        version = iam.get_policy_version(
            PolicyArn=policy_arn, VersionId="v2"
        )["PolicyVersion"]
        document = version["Document"]
        if isinstance(document, str):
            document = json.loads(document)
        assert document["Statement"][0]["Action"] == "s3:GetObject"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_stack_addressable_by_id_for_delete_and_update(cfn):
    """Every StackName parameter accepts the unique stack ID; the CDK CLI
    addresses stacks by ARN, so DeleteStack/UpdateStack must resolve it —
    a missed DeleteStack returned 200 and deleted nothing."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {"Topic": {"Type": "AWS::SNS::Topic",
                                "Properties": {"TopicName": "cfn-by-id-a"}}},
    }
    cfn.create_stack(StackName="cfn-by-id", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-by-id")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    stack_id = stack["StackId"]

    template["Resources"]["Topic"]["Properties"]["TopicName"] = "cfn-by-id-b"
    cfn.update_stack(StackName=stack_id, TemplateBody=json.dumps(template))
    assert _wait_stack(cfn, "cfn-by-id")["StackStatus"] == "UPDATE_COMPLETE"

    cfn.delete_stack(StackName=stack_id)
    _wait_stack(cfn, "cfn-by-id")
    described = cfn.describe_stacks(StackName=stack_id)["Stacks"]
    assert described[0]["StackStatus"] == "DELETE_COMPLETE"


def test_cfn_cognito_user_pool_enabled_mfas(cfn, cognito_idp):
    """EnabledMfas (what CDK emits for mfaSecondFactor otp) maps onto the
    software-token config block GetUserPoolMfaConfig reports."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {
            "UserPoolName": "cfn-enabled-mfas",
            "MfaConfiguration": "OPTIONAL",
            "EnabledMfas": ["SOFTWARE_TOKEN_MFA"]}}},
        "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
    }
    cfn.create_stack(StackName="cfn-enabled-mfas", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-enabled-mfas")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    pool_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["PoolId"]

    cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pool_id)
    assert cfg["MfaConfiguration"] == "OPTIONAL"
    assert cfg["SoftwareTokenMfaConfiguration"]["Enabled"] is True

    cfn.delete_stack(StackName="cfn-enabled-mfas")
    _wait_stack(cfn, "cfn-enabled-mfas")


def test_cfn_cognito_user_pool_update_preserves_users(cfn, cognito_idp):
    """A property change on AWS::Cognito::UserPool updates in place: the pool
    keeps its id and its users (the create-fallback minted a new empty pool)."""
    def template(mfa):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {
                "UserPoolName": "cfn-upd-pool", "MfaConfiguration": mfa}}},
            "Outputs": {"Id": {"Value": {"Ref": "Pool"}}},
        }
    cfn.create_stack(StackName="cfn-pool-upd", TemplateBody=json.dumps(template("OFF")))
    stack = _wait_stack(cfn, "cfn-pool-upd")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    pool_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["Id"]
    cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice", MessageAction="SUPPRESS")

    cfn.update_stack(StackName="cfn-pool-upd", TemplateBody=json.dumps(template("OPTIONAL")))
    stack = _wait_stack(cfn, "cfn-pool-upd")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    new_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["Id"]
    assert new_id == pool_id
    pool = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
    assert pool["MfaConfiguration"] == "OPTIONAL"
    users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
    assert [u["Username"] for u in users] == ["alice"]

    cfn.delete_stack(StackName="cfn-pool-upd")
    _wait_stack(cfn, "cfn-pool-upd")


def test_cfn_secret_update_keeps_arn_and_versions(cfn):
    """A changed SecretString becomes the new AWSCURRENT on the same secret —
    same ARN, history kept (the fallback minted a new ARN and dropped it all)."""
    import boto3
    sm = boto3.client("secretsmanager", endpoint_url=os.environ.get(
        "MINISTACK_ENDPOINT", "http://localhost:4566"), region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test")

    def template(value):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {"S": {"Type": "AWS::SecretsManager::Secret", "Properties": {
                "Name": "cfn-upd-secret", "SecretString": value}}},
        }
    cfn.create_stack(StackName="cfn-secret-upd", TemplateBody=json.dumps(template("v1")))
    assert _wait_stack(cfn, "cfn-secret-upd")["StackStatus"] == "CREATE_COMPLETE"
    arn1 = sm.describe_secret(SecretId="cfn-upd-secret")["ARN"]

    cfn.update_stack(StackName="cfn-secret-upd", TemplateBody=json.dumps(template("v2")))
    assert _wait_stack(cfn, "cfn-secret-upd")["StackStatus"] == "UPDATE_COMPLETE"
    desc = sm.describe_secret(SecretId="cfn-upd-secret")
    assert desc["ARN"] == arn1
    assert sm.get_secret_value(SecretId="cfn-upd-secret")["SecretString"] == "v2"

    cfn.delete_stack(StackName="cfn-secret-upd")
    _wait_stack(cfn, "cfn-secret-upd")


def test_cfn_event_bus_and_record_set_update_in_place(cfn):
    """An EventBus property change no longer fails the stack with "already
    exists", and a RecordSet value change updates instead of raising."""
    import boto3
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    r53 = boto3.client("route53", endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test")

    def template(ttl):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Bus": {"Type": "AWS::Events::EventBus", "Properties": {
                    "Name": "cfn-upd-bus", "Description": f"ttl-{ttl}"}},
                "Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {
                    "Name": "cfn-upd.example.com"}},
                "Rec": {"Type": "AWS::Route53::RecordSet", "Properties": {
                    "HostedZoneId": {"Ref": "Zone"},
                    "Name": "api.cfn-upd.example.com", "Type": "A",
                    "TTL": str(ttl), "ResourceRecords": ["192.0.2.7"]}},
            },
            "Outputs": {"ZoneId": {"Value": {"Ref": "Zone"}}},
        }
    cfn.create_stack(StackName="cfn-bus-rec", TemplateBody=json.dumps(template(60)))
    stack = _wait_stack(cfn, "cfn-bus-rec")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    zone_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["ZoneId"]

    cfn.update_stack(StackName="cfn-bus-rec", TemplateBody=json.dumps(template(300)))
    assert _wait_stack(cfn, "cfn-bus-rec")["StackStatus"] == "UPDATE_COMPLETE"
    recs = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    rec = next(r for r in recs if r["Name"].startswith("api."))
    assert rec["TTL"] == 300

    cfn.delete_stack(StackName="cfn-bus-rec")
    _wait_stack(cfn, "cfn-bus-rec")
# -- Template pre-flight and Fn::GetAtt strictness ---------------------------
# Measured on a real account (eu-central-1, 2026-09-02): CreateStack,
# UpdateStack, CreateChangeSet and ValidateTemplate reject an unrecognized
# resource type synchronously with the exact message below and create no stack;
# a Fn::GetAtt to an attribute the type does not have passes validation, the
# resource is created, then the stack rolls back with
# "Requested attribute X does not exist in schema for T".

_UNRECOGNIZED_MESSAGE = (
    "Template format error: Unrecognized resource types: [AWS::Baz::Qux, AWS::Foo::Bar]"
)


def _bad_types_template():
    return json.dumps({"Resources": {
        "Queue": {"Type": "AWS::SQS::Queue"},
        "Thing": {"Type": "AWS::Foo::Bar", "Properties": {}},
        "Other": {"Type": "AWS::Baz::Qux", "Properties": {}},
    }})


def test_cfn_unrecognized_resource_type_rejected_up_front(cfn, sqs):
    """No stack, no change set and no resource exist after the rejections."""
    bad = _bad_types_template()
    name = "cfn-preflight-types"
    for call in (
        lambda: cfn.validate_template(TemplateBody=bad),
        lambda: cfn.create_stack(StackName=name, TemplateBody=bad),
        lambda: cfn.create_change_set(StackName=name, ChangeSetName="cs",
                                      ChangeSetType="CREATE", TemplateBody=bad),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == _UNRECOGNIZED_MESSAGE

    with pytest.raises(ClientError) as exc:
        cfn.describe_stacks(StackName=name)
    assert "does not exist" in str(exc.value)
    assert not [s for s in _all_pages(cfn, "list_stacks", "StackSummaries")
                if s["StackName"] == name]
    # The valid sibling was never provisioned.
    assert not [u for u in sqs.list_queues().get("QueueUrls", [])
                if "cfn-preflight-types" in u]


def test_cfn_unrecognized_resource_type_rejected_on_update(cfn, sqs):
    """An update carrying an unrecognized type is refused without touching the
    stack: status and resources stay as they were."""
    name = "cfn-preflight-update"
    good = json.dumps({"Resources": {"Queue": {"Type": "AWS::SQS::Queue"}}})
    cfn.create_stack(StackName=name, TemplateBody=good)
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        with pytest.raises(ClientError) as exc:
            cfn.update_stack(StackName=name, TemplateBody=_bad_types_template())
        assert exc.value.response["Error"]["Message"] == _UNRECOGNIZED_MESSAGE
        stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        assert [r["LogicalResourceId"] for r in
                cfn.list_stack_resources(StackName=name)["StackResourceSummaries"]] == ["Queue"]
    finally:
        cfn.delete_stack(StackName=name)
        _wait_stack(cfn, name)


def test_cfn_unrecognized_type_behind_false_condition_is_fine(cfn):
    """Condition-false resources are not provisioned, so their type is not
    checked either (same rule as before)."""
    name = "cfn-preflight-cond"
    tpl = json.dumps({
        "Conditions": {"Never": {"Fn::Equals": ["a", "b"]}},
        "Resources": {
            "Queue": {"Type": "AWS::SQS::Queue"},
            "Thing": {"Type": "AWS::Foo::Bar", "Condition": "Never", "Properties": {}},
        },
    })
    cfn.validate_template(TemplateBody=tpl)
    cfn.create_stack(StackName=name, TemplateBody=tpl)
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
    finally:
        cfn.delete_stack(StackName=name)
        _wait_stack(cfn, name)


def test_cfn_unknown_cloudformation_type_is_rejected_like_any_other(cfn):
    """An unregistered AWS::CloudFormation::* type (a Macro, a typo) is
    'Unrecognized resource types', not a silent no-op placeholder; the
    registered ones (WaitConditionHandle) still deploy."""
    name = "cfn-preflight-cfn-types"
    bad = json.dumps({"Resources": {
        "Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"},
        "Typo": {"Type": "AWS::CloudFormation::DoesNotExist", "Properties": {}},
    }})
    with pytest.raises(ClientError) as exc:
        cfn.create_stack(StackName=name, TemplateBody=bad)
    assert exc.value.response["Error"]["Message"] == (
        "Template format error: Unrecognized resource types: "
        "[AWS::CloudFormation::DoesNotExist]")
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=name)

    good = json.dumps({"Resources": {
        "Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"}}})
    cfn.create_stack(StackName=name, TemplateBody=good)
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_validate_template_accepts_a_sam_template(cfn):
    """A template that declares a Transform is exempt from the unrecognized
    type check: a macro can rewrite any resource, so real CloudFormation does
    not pre-validate types through one — `sam validate` sends templates with
    AWS::Serverless::* resources to ValidateTemplate and they pass."""
    tpl = json.dumps({
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {"Fn": {"Type": "AWS::Serverless::Function", "Properties": {
            "Handler": "index.handler", "Runtime": "python3.12",
            "InlineCode": "def handler(e, c): return {}"}}},
    })
    cfn.validate_template(TemplateBody=tpl)


def test_cfn_getatt_unknown_attribute_fails_the_stack(cfn, sqs):
    """Fn::GetAtt to an attribute the resource does not expose is no longer
    answered with the physical id: the stack fails and rolls back with the
    reason CloudFormation reports."""
    name = "cfn-preflight-getatt"
    tpl = json.dumps({
        "Resources": {"Queue": {"Type": "AWS::SQS::Queue",
                                "Properties": {"QueueName": "cfn-preflight-getatt-q"}}},
        "Outputs": {"X": {"Value": {"Fn::GetAtt": ["Queue", "NoSuchAttr"]}}},
    })
    cfn.validate_template(TemplateBody=tpl)  # passes validation, as on AWS
    cfn.create_stack(StackName=name, TemplateBody=tpl)
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "ROLLBACK_COMPLETE"
        reasons = [e.get("ResourceStatusReason", "") for e in
                   cfn.describe_stack_events(StackName=name)["StackEvents"]]
        assert any("Requested attribute NoSuchAttr does not exist in schema "
                   "for AWS::SQS::Queue" in r for r in reasons), reasons
        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName="cfn-preflight-getatt-q")
        assert all(
            e["ExportingStackId"] != cfn.describe_stacks(StackName=name)["Stacks"][0]["StackId"]
            for e in _all_pages(cfn, "list_exports", "Exports"))
    finally:
        cfn.delete_stack(StackName=name)
        _wait_stack(cfn, name)


def test_cfn_getatt_unknown_attribute_in_properties_fails_the_resource(cfn, sqs):
    """The same rule inside Properties: the consumer fails, the stack rolls back
    and the producer created before it is removed again."""
    name = "cfn-preflight-getatt-prop"
    tpl = json.dumps({"Resources": {
        "A": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "cfn-preflight-gp-a"}},
        "B": {"Type": "AWS::SNS::Topic",
              "Properties": {"DisplayName": {"Fn::GetAtt": ["A", "Nope"]}}},
    }})
    cfn.create_stack(StackName=name, TemplateBody=tpl)
    try:
        assert _wait_stack(cfn, name)["StackStatus"] == "ROLLBACK_COMPLETE"
        events = cfn.describe_stack_events(StackName=name)["StackEvents"]
        b = [e for e in events if e["LogicalResourceId"] == "B"
             and e["ResourceStatus"] == "CREATE_FAILED"]
        assert b and "Requested attribute Nope does not exist in schema for AWS::SQS::Queue" in b[0]["ResourceStatusReason"]
        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName="cfn-preflight-gp-a")
    finally:
        cfn.delete_stack(StackName=name)
        _wait_stack(cfn, name)


def test_cfn_dynamic_reference_to_an_unsupported_service_is_rejected(cfn):
    """{{resolve:ssm}}, {{resolve:ssm-secure}} and {{resolve:secretsmanager}}
    resolve at provisioning time; a reference to any other service is refused
    up front, before a stack record exists."""
    tpl = json.dumps({"Resources": {"P": {
        "Type": "AWS::SSM::Parameter",
        "Properties": {"Type": "String", "Name": "/cfn-preflight/dyn",
                       "Value": "{{resolve:vault:my-secret:key}}"}}}})
    with pytest.raises(ClientError) as exc:
        cfn.create_stack(StackName="cfn-preflight-dyn", TemplateBody=tpl)
    msg = exc.value.response["Error"]["Message"]
    assert msg.startswith("Template format error: unsupported dynamic reference")
    assert "{{resolve:vault:my-secret:key}}" in msg
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName="cfn-preflight-dyn")


def test_cfn_events_rule_arn_matches_the_service(cfn, eb):
    """GetAtt Arn on a rule is the ARN the EventBridge service reports for
    it: no bus segment on the default bus, rule/<bus>/<name> on a custom
    bus. The provisioner used to put a default/ segment in that the service
    itself never produces."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-arn-{uid}"
    bus_name = f"cfn-rule-arn-bus-{uid}"
    default_rule, bus_rule = f"cfn-rule-arn-def-{uid}", f"cfn-rule-arn-on-bus-{uid}"
    template = json.dumps({
        "Resources": {
            "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": bus_name}},
            "OnDefault": {"Type": "AWS::Events::Rule", "Properties": {
                "Name": default_rule, "ScheduleExpression": "rate(1 hour)"}},
            "OnBus": {"Type": "AWS::Events::Rule", "Properties": {
                "Name": bus_rule, "EventBusName": {"Ref": "Bus"},
                "EventPattern": {"source": ["cfn.test"]}}},
        },
        "Outputs": {"DefaultArn": {"Value": {"Fn::GetAtt": ["OnDefault", "Arn"]}},
                    "BusArn": {"Value": {"Fn::GetAtt": ["OnBus", "Arn"]}}},
    })

    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
        prefix = f"arn:aws:events:{eb.meta.region_name}:000000000000:rule/"

        assert outputs["DefaultArn"] == f"{prefix}{default_rule}"
        assert outputs["DefaultArn"] == eb.describe_rule(Name=default_rule)["Arn"]
        assert outputs["BusArn"] == f"{prefix}{bus_name}/{bus_rule}"
        assert outputs["BusArn"] == eb.describe_rule(Name=bus_rule, EventBusName=bus_name)["Arn"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_name_is_the_userpoolname_property(cfn, cognito_idp):
    """UserPoolName is the template property the resource reference defines;
    DescribeUserPool reports it. PoolName, the API's name for the same
    thing, stays accepted for templates written against MiniStack."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-poolname-{uid}"
    template = json.dumps({
        "Resources": {
            "Named": {"Type": "AWS::Cognito::UserPool",
                      "Properties": {"UserPoolName": f"cfn-named-pool-{uid}"}},
            "Legacy": {"Type": "AWS::Cognito::UserPool",
                       "Properties": {"PoolName": f"cfn-legacy-pool-{uid}"}},
        },
        "Outputs": {"NamedId": {"Value": {"Ref": "Named"}},
                    "LegacyId": {"Value": {"Ref": "Legacy"}}},
    })

    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}

        named = cognito_idp.describe_user_pool(UserPoolId=outputs["NamedId"])["UserPool"]
        assert named["Name"] == f"cfn-named-pool-{uid}"
        legacy = cognito_idp.describe_user_pool(UserPoolId=outputs["LegacyId"])["UserPool"]
        assert legacy["Name"] == f"cfn-legacy-pool-{uid}"
        listed, token = {}, None
        while True:
            page = cognito_idp.list_user_pools(MaxResults=60, **({"NextToken": token} if token else {}))
            listed.update({p["Id"]: p["Name"] for p in page["UserPools"]})
            token = page.get("NextToken")
            if not token:
                break
        assert listed[outputs["NamedId"]] == f"cfn-named-pool-{uid}"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _user_pool_ids_named(cognito_idp, name):
    ids, token = [], None
    while True:
        page = cognito_idp.list_user_pools(MaxResults=60, **({"NextToken": token} if token else {}))
        ids += [p["Id"] for p in page["UserPools"] if p["Name"] == name]
        token = page.get("NextToken")
        if not token:
            return ids


def test_cfn_cognito_user_pool_update_keeps_id_and_users(cfn, cognito_idp):
    """Changing MfaConfiguration and the password policy updates the pool in
    place: same pool id, the user created before the update is still there,
    and DescribeUserPool reports the new settings."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-upd-{uid}"

    def template(mfa, min_len):
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {
                "UserPoolName": f"cfn-pool-upd-{uid}",
                "MfaConfiguration": mfa,
                "Policies": {"PasswordPolicy": {"MinimumLength": min_len}},
            }}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "PoolArn": {"Value": {"Fn::GetAtt": ["Pool", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("OFF", 8))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id, pool_arn = _output(stack, "PoolId"), _output(stack, "PoolArn")
        assert cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]["Name"] == (
            f"cfn-pool-upd-{uid}"
        )
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("OPTIONAL", 12))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        assert _output(stack, "PoolArn") == pool_arn

        pool = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
        assert pool["MfaConfiguration"] == "OPTIONAL"
        assert pool["Policies"]["PasswordPolicy"]["MinimumLength"] == 12
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
        assert _user_pool_ids_named(cognito_idp, f"cfn-pool-upd-{uid}") == [pool_id]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_rename_keeps_id_and_users(cfn, cognito_idp):
    """UserPoolName updates without interruption per the resource reference:
    the pool keeps its id and its users and DescribeUserPool reports the
    new name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-ren-{uid}"

    def template(name):
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool",
                                   "Properties": {"UserPoolName": name}}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-pool-a-{uid}"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-pool-b-{uid}"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        assert cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]["Name"] == (
            f"cfn-pool-b-{uid}"
        )
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


@pytest.mark.parametrize("prop", ["AliasAttributes", "UsernameAttributes"])
def test_cfn_cognito_user_pool_sign_in_attributes_change_fails_loudly(cfn, cognito_idp, prop):
    """AliasAttributes and UsernameAttributes are fixed at CreateUserPool and
    no API call changes them afterwards, so MiniStack refuses the update
    instead of altering the record: the stack rolls back, the pool keeps
    its id, its users and its sign-in settings."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-attr-{uid}"

    def template(attributes):
        props = {"UserPoolName": f"cfn-pool-attr-{uid}"}
        if attributes:
            props[prop] = attributes
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": props}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(None))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(["email"]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        assert f"AWS::Cognito::UserPool {prop} is set at CreateUserPool" in _stack_event_reasons(cfn, stack_name)
        assert _output(stack, "PoolId") == pool_id

        pool = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
        assert pool.get(prop, []) == []
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_client_update_keeps_client_id(cfn, cognito_idp):
    """ClientName and CallbackURLs update in place through
    UpdateUserPoolClient; Ref keeps returning the same ClientId."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-client-upd-{uid}"

    def template(client_name, callbacks):
        props = {"UserPoolId": {"Ref": "Pool"}, "ClientName": client_name,
                 "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]}
        if callbacks:
            props["CallbackURLs"] = callbacks
        return json.dumps({
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool",
                         "Properties": {"UserPoolName": f"cfn-client-pool-{uid}"}},
                "Client": {"Type": "AWS::Cognito::UserPoolClient", "Properties": props},
            },
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "ClientId": {"Value": {"Ref": "Client"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("app-v1", None))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id, client_id = _output(stack, "PoolId"), _output(stack, "ClientId")

        cfn.update_stack(
            StackName=stack_name,
            TemplateBody=template("app-v2", ["https://example.com/callback"]),
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "ClientId") == client_id

        client = cognito_idp.describe_user_pool_client(
            UserPoolId=pool_id, ClientId=client_id
        )["UserPoolClient"]
        assert client["ClientName"] == "app-v2"
        assert client["CallbackURLs"] == ["https://example.com/callback"]
        assert client["ExplicitAuthFlows"] == ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
        clients = cognito_idp.list_user_pool_clients(UserPoolId=pool_id, MaxResults=60)
        assert [c["ClientId"] for c in clients["UserPoolClients"]] == [client_id]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_client_generate_secret_change_replaces_the_client(cfn, cognito_idp):
    """GenerateSecret requires replacement: the new client is created before
    the old one is removed, Ref moves to the new ClientId, and the new
    client carries the secret. The pool lives outside the stack, so the
    stack delete leaves it alone."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-client-repl-{uid}"
    pool_id = cognito_idp.create_user_pool(PoolName=f"cfn-client-repl-pool-{uid}")["UserPool"]["Id"]

    def template(generate_secret):
        return json.dumps({
            "Resources": {"Client": {"Type": "AWS::Cognito::UserPoolClient", "Properties": {
                "UserPoolId": pool_id, "ClientName": "app",
                "GenerateSecret": generate_secret}}},
            "Outputs": {"ClientId": {"Value": {"Ref": "Client"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        client_id = _output(stack, "ClientId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_id = _output(stack, "ClientId")
        assert new_id != client_id
        clients = cognito_idp.list_user_pool_clients(UserPoolId=pool_id, MaxResults=60)
        assert [c["ClientId"] for c in clients["UserPoolClients"]] == [new_id]
        client = cognito_idp.describe_user_pool_client(
            UserPoolId=pool_id, ClientId=new_id
        )["UserPoolClient"]
        assert client["ClientSecret"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        cognito_idp.delete_user_pool(UserPoolId=pool_id)


def test_cfn_cognito_user_pool_client_pool_move_replaces_the_client(cfn, cognito_idp):
    """UserPoolId requires replacement: the client is created in the new
    pool before the old one is removed from the old pool, and Ref moves to
    the new ClientId. The pools live outside the stack."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-client-move-{uid}"
    pool_a = cognito_idp.create_user_pool(PoolName=f"cfn-client-move-a-{uid}")["UserPool"]["Id"]
    pool_b = cognito_idp.create_user_pool(PoolName=f"cfn-client-move-b-{uid}")["UserPool"]["Id"]

    def template(pool_id):
        return json.dumps({
            "Resources": {"Client": {"Type": "AWS::Cognito::UserPoolClient", "Properties": {
                "UserPoolId": pool_id, "ClientName": "app"}}},
            "Outputs": {"ClientId": {"Value": {"Ref": "Client"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(pool_a))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        client_id = _output(stack, "ClientId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(pool_b))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_id = _output(stack, "ClientId")
        assert new_id != client_id
        assert cognito_idp.list_user_pool_clients(UserPoolId=pool_a, MaxResults=60)["UserPoolClients"] == []
        clients = cognito_idp.list_user_pool_clients(UserPoolId=pool_b, MaxResults=60)
        assert [c["ClientId"] for c in clients["UserPoolClients"]] == [new_id]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        left = cognito_idp.list_user_pool_clients(UserPoolId=pool_b, MaxResults=60)["UserPoolClients"]
        for pool_id in (pool_a, pool_b):
            cognito_idp.delete_user_pool(UserPoolId=pool_id)
    assert left == []


def test_cfn_cognito_user_pool_dropped_property_reverts_to_default(cfn, cognito_idp):
    """A property the new template no longer declares reverts to the default
    the create handler applies, as CloudFormation does: MfaConfiguration
    goes back to OFF and AutoVerifiedAttributes is cleared, while the pool
    keeps its id and its user."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-drop-{uid}"

    def template(declared):
        props = {"UserPoolName": f"cfn-pool-drop-{uid}"}
        if declared:
            props["MfaConfiguration"] = "OPTIONAL"
            props["AutoVerifiedAttributes"] = ["email"]
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": props}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        pool = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
        assert pool["MfaConfiguration"] == "OPTIONAL"
        assert pool["AutoVerifiedAttributes"] == ["email"]
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        pool = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]
        assert pool["MfaConfiguration"] == "OFF"
        assert pool.get("AutoVerifiedAttributes", []) == []
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_identity_pool_update_keeps_id_and_roles(cfn, cognito_identity):
    """AllowUnauthenticatedIdentities flips in place through
    UpdateIdentityPool: same IdentityPoolId, and the role mapping set outside
    the template survives."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-idp-upd-{uid}"

    def template(allow_unauth):
        return json.dumps({
            "Resources": {"IdPool": {"Type": "AWS::Cognito::IdentityPool", "Properties": {
                "IdentityPoolName": f"cfn_idp_upd_{uid}",
                "AllowUnauthenticatedIdentities": allow_unauth,
            }}},
            "Outputs": {"PoolId": {"Value": {"Ref": "IdPool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        cognito_identity.set_identity_pool_roles(
            IdentityPoolId=pool_id,
            Roles={"authenticated": "arn:aws:iam::000000000000:role/auth-upd"},
        )

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id

        pool = cognito_identity.describe_identity_pool(IdentityPoolId=pool_id)
        assert pool["AllowUnauthenticatedIdentities"] is True
        assert pool["IdentityPoolName"] == f"cfn_idp_upd_{uid}"
        roles = cognito_identity.get_identity_pool_roles(IdentityPoolId=pool_id)["Roles"]
        assert roles == {"authenticated": "arn:aws:iam::000000000000:role/auth-upd"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_group_update_keeps_members(cfn, cognito_idp):
    """Description and Precedence update the group in place: Ref still
    returns the group name and the user added to the group stays a member."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-group-upd-{uid}"

    def template(description, precedence):
        return json.dumps({
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool",
                         "Properties": {"UserPoolName": f"cfn-group-upd-pool-{uid}"}},
                "Group": {"Type": "AWS::Cognito::UserPoolGroup", "Properties": {
                    "UserPoolId": {"Ref": "Pool"}, "GroupName": "admins",
                    "Description": description, "Precedence": precedence}},
            },
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "GroupRef": {"Value": {"Ref": "Group"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("Administrators", 1))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")
        cognito_idp.admin_add_user_to_group(UserPoolId=pool_id, Username="alice", GroupName="admins")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("Platform admins", 5))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "GroupRef") == "admins"

        group = cognito_idp.get_group(UserPoolId=pool_id, GroupName="admins")["Group"]
        assert group["Description"] == "Platform admins"
        assert group["Precedence"] == 5
        members = cognito_idp.list_users_in_group(UserPoolId=pool_id, GroupName="admins")["Users"]
        assert [u["Username"] for u in members] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_group_rename_replaces_the_group(cfn, cognito_idp):
    """GroupName requires replacement: the renamed group is created before
    the old one is removed, and Ref follows the new name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-group-ren-{uid}"

    def template(group_name):
        return json.dumps({
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool",
                         "Properties": {"UserPoolName": f"cfn-group-ren-pool-{uid}"}},
                "Group": {"Type": "AWS::Cognito::UserPoolGroup", "Properties": {
                    "UserPoolId": {"Ref": "Pool"}, "GroupName": group_name}},
            },
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "GroupRef": {"Value": {"Ref": "Group"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("admins"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("operators"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "GroupRef") == "operators"
        groups = cognito_idp.list_groups(UserPoolId=pool_id)["Groups"]
        assert [g["GroupName"] for g in groups] == ["operators"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_group_move_under_custom_name_fails_loudly(cfn, cognito_idp):
    """UserPoolId requires replacement, which CloudFormation refuses for a
    custom-named group: the stack rolls back and the group stays in its
    pool with its members. The pools live outside the stack, so the
    rollback has nothing else to undo."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-group-move-{uid}"
    pool_a = cognito_idp.create_user_pool(PoolName=f"cfn-group-move-a-{uid}")["UserPool"]["Id"]
    pool_b = cognito_idp.create_user_pool(PoolName=f"cfn-group-move-b-{uid}")["UserPool"]["Id"]

    def template(pool_id):
        return json.dumps({
            "Resources": {"Group": {"Type": "AWS::Cognito::UserPoolGroup", "Properties": {
                "UserPoolId": pool_id, "GroupName": "admins"}}},
            "Outputs": {"GroupRef": {"Value": {"Ref": "Group"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(pool_a))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        cognito_idp.admin_create_user(UserPoolId=pool_a, Username="alice")
        cognito_idp.admin_add_user_to_group(UserPoolId=pool_a, Username="alice", GroupName="admins")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(pool_b))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        assert "custom-named resource requires replacing" in _stack_event_reasons(cfn, stack_name)
        assert _output(stack, "GroupRef") == "admins"

        members = cognito_idp.list_users_in_group(UserPoolId=pool_a, GroupName="admins")["Users"]
        assert [u["Username"] for u in members] == ["alice"]
        assert cognito_idp.list_groups(UserPoolId=pool_b)["Groups"] == []
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for pool_id in (pool_a, pool_b):
            cognito_idp.delete_user_pool(UserPoolId=pool_id)


def test_cfn_cognito_user_pool_client_dropped_properties_revert_to_defaults(cfn, cognito_idp):
    """A client property the new template drops goes back to the default
    CreateUserPoolClient applies ("If you don't specify a value for a
    parameter, Amazon Cognito sets it to a default value" on the resource
    reference): the callback list empties, PreventUserExistenceErrors is
    LEGACY again and RefreshTokenValidity is the service default, while
    Ref keeps the ClientId."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-client-drop-{uid}"

    def template(declared):
        props = {"UserPoolId": {"Ref": "Pool"}, "ClientName": "app"}
        if declared:
            props.update({"CallbackURLs": ["https://example.com/cb"],
                          "PreventUserExistenceErrors": "ENABLED",
                          "RefreshTokenValidity": 10})
        return json.dumps({
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool",
                         "Properties": {"UserPoolName": f"cfn-client-drop-pool-{uid}"}},
                "Client": {"Type": "AWS::Cognito::UserPoolClient", "Properties": props},
            },
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "ClientId": {"Value": {"Ref": "Client"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id, client_id = _output(stack, "PoolId"), _output(stack, "ClientId")
        client = cognito_idp.describe_user_pool_client(
            UserPoolId=pool_id, ClientId=client_id
        )["UserPoolClient"]
        assert client["CallbackURLs"] == ["https://example.com/cb"]
        assert client["PreventUserExistenceErrors"] == "ENABLED"
        assert client["RefreshTokenValidity"] == 10

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "ClientId") == client_id
        client = cognito_idp.describe_user_pool_client(
            UserPoolId=pool_id, ClientId=client_id
        )["UserPoolClient"]
        assert client.get("CallbackURLs", []) == []
        assert client["PreventUserExistenceErrors"] == "LEGACY"
        assert client["RefreshTokenValidity"] == 30
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_client_replacement_carries_the_template_over(cfn, cognito_idp):
    """The replacement a GenerateSecret change forces creates the new client
    from the whole template: token validity, PreventUserExistenceErrors and
    the OAuth settings arrive on the new ClientId, not only the six
    properties the old create handler copied."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-client-carry-{uid}"
    pool_id = cognito_idp.create_user_pool(PoolName=f"cfn-client-carry-pool-{uid}")["UserPool"]["Id"]

    def template(generate_secret):
        return json.dumps({
            "Resources": {"Client": {"Type": "AWS::Cognito::UserPoolClient", "Properties": {
                "UserPoolId": pool_id, "ClientName": "app", "GenerateSecret": generate_secret,
                "AccessTokenValidity": 15, "TokenValidityUnits": {"AccessToken": "minutes"},
                "PreventUserExistenceErrors": "ENABLED",
                "AllowedOAuthFlowsUserPoolClient": True,
                "AllowedOAuthFlows": ["code"], "AllowedOAuthScopes": ["openid"],
                "ReadAttributes": ["email"], "AuthSessionValidity": 5}}},
            "Outputs": {"ClientId": {"Value": {"Ref": "Client"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        client_id = _output(stack, "ClientId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_id = _output(stack, "ClientId")
        assert new_id != client_id
        client = cognito_idp.describe_user_pool_client(
            UserPoolId=pool_id, ClientId=new_id
        )["UserPoolClient"]
        assert client["ClientSecret"]
        assert client["AccessTokenValidity"] == 15
        assert client["TokenValidityUnits"] == {"AccessToken": "minutes"}
        assert client["PreventUserExistenceErrors"] == "ENABLED"
        assert client["AllowedOAuthFlowsUserPoolClient"] is True
        assert client["AllowedOAuthFlows"] == ["code"]
        assert client["AllowedOAuthScopes"] == ["openid"]
        assert client["ReadAttributes"] == ["email"]
        assert client["AuthSessionValidity"] == 5
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        cognito_idp.delete_user_pool(UserPoolId=pool_id)


def test_cfn_cognito_identity_pool_dropped_properties_revert_and_principal_tags_survive(
    cfn, cognito_identity
):
    """An identity pool property the new template drops reverts to the create
    default (AllowClassicFlow false, no DeveloperProviderName) while the
    pool keeps its id, the principal-tag mapping set outside the template
    is still there after the update, and Fn::GetAtt Name is the name the
    resource reference documents."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-idp-drop-{uid}"
    provider = f"cognito-idp.us-east-1.amazonaws.com/us-east-1_{uid}"

    def template(declared):
        props = {"IdentityPoolName": f"cfn_idp_drop_{uid}",
                 "AllowUnauthenticatedIdentities": False}
        if declared:
            props.update({"AllowClassicFlow": True, "DeveloperProviderName": "login.example"})
        return json.dumps({
            "Resources": {"IdPool": {"Type": "AWS::Cognito::IdentityPool", "Properties": props}},
            "Outputs": {"PoolId": {"Value": {"Ref": "IdPool"}},
                        "PoolName": {"Value": {"Fn::GetAtt": ["IdPool", "Name"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        assert _output(stack, "PoolName") == f"cfn_idp_drop_{uid}"
        pool = cognito_identity.describe_identity_pool(IdentityPoolId=pool_id)
        assert pool["AllowClassicFlow"] is True
        assert pool["DeveloperProviderName"] == "login.example"
        cognito_identity.set_principal_tag_attribute_map(
            IdentityPoolId=pool_id, IdentityProviderName=provider,
            PrincipalTags={"tenant": "custom:tenant"},
        )

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        assert _output(stack, "PoolName") == f"cfn_idp_drop_{uid}"
        pool = cognito_identity.describe_identity_pool(IdentityPoolId=pool_id)
        assert pool["AllowClassicFlow"] is False
        assert pool.get("DeveloperProviderName", "") == ""
        mapping = cognito_identity.get_principal_tag_attribute_map(
            IdentityPoolId=pool_id, IdentityProviderName=provider
        )
        assert mapping["PrincipalTags"] == {"tenant": "custom:tenant"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_group_dropped_properties_revert(cfn, cognito_idp):
    """Dropping Description and Precedence from a group reverts them: the
    description empties and the precedence goes away, since "The default
    Precedence value is null" on the resource reference. The group keeps
    its name and its member."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-group-drop-{uid}"

    def template(declared):
        props = {"UserPoolId": {"Ref": "Pool"}, "GroupName": "admins"}
        if declared:
            props.update({"Description": "Administrators", "Precedence": 3})
        return json.dumps({
            "Resources": {
                "Pool": {"Type": "AWS::Cognito::UserPool",
                         "Properties": {"UserPoolName": f"cfn-group-drop-pool-{uid}"}},
                "Group": {"Type": "AWS::Cognito::UserPoolGroup", "Properties": props},
            },
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}},
                        "GroupRef": {"Value": {"Ref": "Group"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        group = cognito_idp.get_group(UserPoolId=pool_id, GroupName="admins")["Group"]
        assert group["Precedence"] == 3
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")
        cognito_idp.admin_add_user_to_group(UserPoolId=pool_id, Username="alice", GroupName="admins")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "GroupRef") == "admins"
        group = cognito_idp.get_group(UserPoolId=pool_id, GroupName="admins")["Group"]
        assert group.get("Description", "") == ""
        assert "Precedence" not in group
        members = cognito_idp.list_users_in_group(UserPoolId=pool_id, GroupName="admins")["Users"]
        assert [u["Username"] for u in members] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_group_under_generated_name_moves_and_renames(cfn, cognito_idp):
    """A group without a GroupName carries a generated name, which
    CloudFormation may replace: a UserPoolId change moves it to the other
    pool (same generated name, so Ref is unchanged, and the old pool is
    left without it), and declaring a GroupName afterwards replaces it once
    more with Ref following the new name. The pools live outside the
    stack."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-group-gen-{uid}"
    pool_a = cognito_idp.create_user_pool(PoolName=f"cfn-group-gen-a-{uid}")["UserPool"]["Id"]
    pool_b = cognito_idp.create_user_pool(PoolName=f"cfn-group-gen-b-{uid}")["UserPool"]["Id"]

    def template(pool_id, group_name=None):
        props = {"UserPoolId": pool_id, "Description": "generated"}
        if group_name:
            props["GroupName"] = group_name
        return json.dumps({
            "Resources": {"Group": {"Type": "AWS::Cognito::UserPoolGroup", "Properties": props}},
            "Outputs": {"GroupRef": {"Value": {"Ref": "Group"}}},
        })

    def group_names(pool_id):
        return [g["GroupName"] for g in cognito_idp.list_groups(UserPoolId=pool_id)["Groups"]]

    cfn.create_stack(StackName=stack_name, TemplateBody=template(pool_a))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        generated = _output(stack, "GroupRef")
        assert group_names(pool_a) == [generated]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(pool_b))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "GroupRef") == generated
        assert group_names(pool_a) == []
        assert group_names(pool_b) == [generated]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(pool_b, "admins"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "GroupRef") == "admins"
        assert group_names(pool_b) == ["admins"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for pool_id in (pool_a, pool_b):
            cognito_idp.delete_user_pool(UserPoolId=pool_id)


_COGNITO_POOL_PROPERTY_CHANGES = [
    ("LambdaConfig",
     {"PreSignUp": "arn:aws:lambda:us-east-1:000000000000:function:pre-signup-v1"},
     {"PreSignUp": "arn:aws:lambda:us-east-1:000000000000:function:pre-signup-v2",
      "PostConfirmation": "arn:aws:lambda:us-east-1:000000000000:function:post-confirm"}),
    ("AdminCreateUserConfig",
     {"AllowAdminCreateUserOnly": False, "UnusedAccountValidityDays": 7},
     {"AllowAdminCreateUserOnly": True, "UnusedAccountValidityDays": 3}),
    ("UserPoolTags", {"env": "dev"}, {"env": "prod", "team": "platform"}),
    ("AccountRecoverySetting",
     {"RecoveryMechanisms": [{"Name": "verified_email", "Priority": 1}]},
     {"RecoveryMechanisms": [{"Name": "verified_phone_number", "Priority": 1},
                             {"Name": "verified_email", "Priority": 2}]}),
    ("DeviceConfiguration",
     {"ChallengeRequiredOnNewDevice": False},
     {"ChallengeRequiredOnNewDevice": True, "DeviceOnlyRememberedOnUserPrompt": True}),
]


@pytest.mark.parametrize("prop,before,after", _COGNITO_POOL_PROPERTY_CHANGES,
                         ids=[c[0] for c in _COGNITO_POOL_PROPERTY_CHANGES])
def test_cfn_cognito_user_pool_property_changes_in_place(cfn, cognito_idp, prop, before, after):
    """Each of these properties is "Update requires: No interruption" on the
    resource reference: the change lands on the same pool id and
    DescribeUserPool reports the new value, with the user kept."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-{prop.lower()[:12]}-{uid}"

    def template(value):
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {
                "UserPoolName": f"cfn-pool-prop-{uid}", prop: value}}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(before))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        observed = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"][prop]
        if prop == "UserPoolTags":
            observed = _template_tags(observed)
        assert observed == before
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(after))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        observed = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"][prop]
        if prop == "UserPoolTags":
            observed = _template_tags(observed)
        assert observed == after
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_schema_update_adds_custom_attributes(cfn, cognito_idp):
    """A Schema entry the pool does not have yet is added through
    AddCustomAttributes on update: the pool keeps its id and its user, the
    attribute from the first template is still there, and the new one shows
    up under its custom: prefix. A standard attribute the template also
    lists (email) is left as it is."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-schema-{uid}"

    def template(names):
        schema = [{"Name": "email", "AttributeDataType": "String", "Required": True}]
        schema += [{"Name": n, "AttributeDataType": "String", "Mutable": True} for n in names]
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {
                "UserPoolName": f"cfn-pool-schema-{uid}", "Schema": schema}}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    def custom_names(pool_id):
        attrs = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]["SchemaAttributes"]
        return sorted(a["Name"] for a in attrs if a["Name"].startswith("custom:"))

    cfn.create_stack(StackName=stack_name, TemplateBody=template(["tier"]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        assert custom_names(pool_id) == ["custom:tier"]
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(["tier", "plan"]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        assert custom_names(pool_id) == ["custom:plan", "custom:tier"]
        attrs = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]["SchemaAttributes"]
        email = next(a for a in attrs if a["Name"] == "email")
        assert email["Required"] is True
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cognito_user_pool_enabled_mfas_update(cfn, cognito_idp):
    """EnabledMfas is "Update requires: No interruption" on the resource
    reference: adding SOFTWARE_TOKEN_MFA on update switches the software
    token block on in GetUserPoolMfaConfig, and dropping the property
    switches it off again, on the same pool id with its user kept."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cog-pool-mfas-{uid}"

    def template(enabled_mfas):
        props = {"UserPoolName": f"cfn-pool-mfas-{uid}", "MfaConfiguration": "OPTIONAL"}
        if enabled_mfas is not None:
            props["EnabledMfas"] = enabled_mfas
        return json.dumps({
            "Resources": {"Pool": {"Type": "AWS::Cognito::UserPool", "Properties": props}},
            "Outputs": {"PoolId": {"Value": {"Ref": "Pool"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(None))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        pool_id = _output(stack, "PoolId")
        cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pool_id)
        assert "SoftwareTokenMfaConfiguration" not in cfg
        cognito_idp.admin_create_user(UserPoolId=pool_id, Username="alice")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(["SOFTWARE_TOKEN_MFA"]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pool_id)
        assert cfg["MfaConfiguration"] == "OPTIONAL"
        assert cfg["SoftwareTokenMfaConfiguration"]["Enabled"] is True

        cfn.update_stack(StackName=stack_name, TemplateBody=template(None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "PoolId") == pool_id
        cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pool_id)
        assert cfg["SoftwareTokenMfaConfiguration"]["Enabled"] is False
        users = cognito_idp.list_users(UserPoolId=pool_id)["Users"]
        assert [u["Username"] for u in users] == ["alice"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_secret_update_publishes_a_new_version(cfn, sm):
    """A changed SecretString publishes a new AWSCURRENT version of the same
    secret: same ARN, the first value still there as AWSPREVIOUS, and the
    Description updated alongside."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-secret-upd-{uid}"
    secret_name = f"cfn/secret-upd-{uid}"

    def template(value, description):
        return json.dumps({
            "Resources": {"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": {
                "Name": secret_name, "Description": description, "SecretString": value,
                "Tags": [{"Key": "stage", "Value": description}],
            }}},
            "Outputs": {"SecretRef": {"Value": {"Ref": "Secret"}},
                        "SecretArn": {"Value": {"Fn::GetAtt": ["Secret", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("v1", "first"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        ref, arn = _output(stack, "SecretRef"), _output(stack, "SecretArn")
        first = sm.get_secret_value(SecretId=secret_name)
        assert first["SecretString"] == "v1"

        cfn.update_stack(StackName=stack_name, TemplateBody=template("v2", "second"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "SecretRef") == ref
        assert _output(stack, "SecretArn") == arn

        current = sm.get_secret_value(SecretId=secret_name)
        assert current["SecretString"] == "v2"
        assert current["ARN"] == arn
        assert current["VersionId"] != first["VersionId"]
        previous = sm.get_secret_value(SecretId=secret_name, VersionStage="AWSPREVIOUS")
        assert previous["SecretString"] == "v1"
        assert previous["VersionId"] == first["VersionId"]
        described = sm.describe_secret(SecretId=secret_name)
        assert described["Description"] == "second"
        assert _template_tags(described["Tags"]) == [{"Key": "stage", "Value": "second"}]
        assert set(described["VersionIdsToStages"]) == {first["VersionId"], current["VersionId"]}

        _delete_cfn_test_stack(cfn, stack_name)
        with pytest.raises(ClientError):
            sm.describe_secret(SecretId=secret_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_secret_update_keeps_an_undeclared_kms_key(cfn, sm):
    """A property the template never declared is not the stack's to clear: a
    KmsKeyId set through UpdateSecret outside the stack survives an update
    that changes the Description, and one that drops it."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-secret-kms-{uid}"
    secret_name = f"cfn/secret-kms-{uid}"

    def template(value, description=None):
        props = {"Name": secret_name, "SecretString": value}
        if description is not None:
            props["Description"] = description
        return json.dumps({
            "Resources": {"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": props}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("v1", "declared"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        sm.update_secret(SecretId=secret_name, KmsKeyId=f"alias/cfn-secret-kms-{uid}")
        assert sm.describe_secret(SecretId=secret_name)["KmsKeyId"] == f"alias/cfn-secret-kms-{uid}"

        cfn.update_stack(StackName=stack_name, TemplateBody=template("v1", "changed"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        described = sm.describe_secret(SecretId=secret_name)
        assert described["KmsKeyId"] == f"alias/cfn-secret-kms-{uid}"
        assert described["Description"] == "changed"

        # Dropping the Description from the template clears it, as on AWS.
        cfn.update_stack(StackName=stack_name, TemplateBody=template("v2"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        described = sm.describe_secret(SecretId=secret_name)
        assert described.get("Description", "") == ""
        assert described["KmsKeyId"] == f"alias/cfn-secret-kms-{uid}"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_secret_rename_replaces_the_secret(cfn, sm):
    """Name is create-only: renaming creates the new secret and removes the
    old one, in CloudFormation's replacement order."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-secret-ren-{uid}"

    def template(name):
        return json.dumps({
            "Resources": {"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": {
                "Name": name, "SecretString": "same"}}},
            "Outputs": {"SecretRef": {"Value": {"Ref": "Secret"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-secret-a-{uid}"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-secret-b-{uid}"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "SecretRef") == f"cfn-secret-b-{uid}"
        assert sm.get_secret_value(SecretId=f"cfn-secret-b-{uid}")["SecretString"] == "same"
        with pytest.raises(ClientError):
            sm.describe_secret(SecretId=f"cfn-secret-a-{uid}")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_secret_generate_secret_string_change_publishes_a_new_version(cfn, sm):
    """"When you make a change to this property, a new secret version is
    created" (GenerateSecretString, AWS::SecretsManager::Secret reference):
    a changed PasswordLength regenerates the value as a new AWSCURRENT version
    of the same secret, the previous one staying behind as AWSPREVIOUS."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-secret-gen-{uid}"
    secret_name = f"cfn/secret-gen-{uid}"

    def template(length):
        return json.dumps({
            "Resources": {"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": {
                "Name": secret_name,
                "GenerateSecretString": {
                    "SecretStringTemplate": '{"username": "admin"}',
                    "GenerateStringKey": "password",
                    "PasswordLength": length,
                    "ExcludeCharacters": '"@/\\',
                },
            }}},
            "Outputs": {"SecretArn": {"Value": {"Fn::GetAtt": ["Secret", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(16))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "SecretArn")
        first = sm.get_secret_value(SecretId=secret_name)
        assert len(json.loads(first["SecretString"])["password"]) == 16

        cfn.update_stack(StackName=stack_name, TemplateBody=template(24))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "SecretArn") == arn

        current = sm.get_secret_value(SecretId=secret_name)
        assert current["ARN"] == arn
        assert current["VersionId"] != first["VersionId"]
        assert current["VersionStages"] == ["AWSCURRENT"]
        generated = json.loads(current["SecretString"])
        assert generated["username"] == "admin"
        assert len(generated["password"]) == 24
        previous = sm.get_secret_value(SecretId=secret_name, VersionStage="AWSPREVIOUS")
        assert previous["VersionId"] == first["VersionId"]
        assert previous["SecretString"] == first["SecretString"]

        # An update that leaves GenerateSecretString alone does not regenerate.
        cfn.update_stack(StackName=stack_name, TemplateBody=template(24), Tags=[{"Key": "touch", "Value": "1"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert sm.get_secret_value(SecretId=secret_name)["VersionId"] == current["VersionId"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_secret_replica_regions_apply_on_create_and_update(cfn, sm):
    """ReplicaRegions is "Update requires: No interruption" (reference): the
    regions a template declares are replicated on create, a region added or
    re-keyed on update is applied to the same secret, a new value reaches the
    replicas, and the replicas go away with the stack."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-secret-rep-{uid}"
    secret_name = f"cfn/secret-rep-{uid}"
    home = sm.meta.region_name
    west = _regional_cfn_test_client("secretsmanager", "us-west-2")
    frankfurt = _regional_cfn_test_client("secretsmanager", "eu-central-1")

    def template(value, replicas):
        return json.dumps({
            "Resources": {"Secret": {"Type": "AWS::SecretsManager::Secret", "Properties": {
                "Name": secret_name, "SecretString": value, "ReplicaRegions": replicas,
            }}},
            "Outputs": {"SecretArn": {"Value": {"Fn::GetAtt": ["Secret", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("v1", [{"Region": "us-west-2"}]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "SecretArn")
        primary = sm.describe_secret(SecretId=secret_name)
        assert [s["Region"] for s in primary["ReplicationStatus"]] == ["us-west-2"]
        replica = west.describe_secret(SecretId=secret_name)
        assert replica["PrimaryRegion"] == home
        assert replica["ARN"] == arn.replace(f":{home}:", ":us-west-2:")
        assert west.get_secret_value(SecretId=secret_name)["SecretString"] == "v1"
        with pytest.raises(ClientError):
            frankfurt.describe_secret(SecretId=secret_name)

        cfn.update_stack(StackName=stack_name, TemplateBody=template("v2", [
            {"Region": "us-west-2", "KmsKeyId": f"alias/cfn-secret-rep-{uid}"},
            {"Region": "eu-central-1"},
        ]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "SecretArn") == arn
        primary = sm.describe_secret(SecretId=secret_name)
        assert sorted(s["Region"] for s in primary["ReplicationStatus"]) == ["eu-central-1", "us-west-2"]
        assert west.describe_secret(SecretId=secret_name)["KmsKeyId"] == f"alias/cfn-secret-rep-{uid}"
        assert west.get_secret_value(SecretId=secret_name)["SecretString"] == "v2"
        assert frankfurt.get_secret_value(SecretId=secret_name)["SecretString"] == "v2"

        _delete_cfn_test_stack(cfn, stack_name)
        for client in (sm, west, frankfurt):
            with pytest.raises(ClientError):
                client.describe_secret(SecretId=secret_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_events_rule_update_keeps_arn_and_foreign_targets(cfn, eb, sqs):
    """Schedule, description and the declared targets update in place through
    PutRule / PutTargets / RemoveTargets: same rule ARN, a target added with
    PutTargets outside the template survives, a target dropped from the
    template is removed and a changed one is rewritten."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-upd-{uid}"
    rule_name = f"cfn-rule-upd-{uid}"
    queues, queue_urls = {}, {}
    for label in ("a", "b", "c"):
        queue_urls[label] = sqs.create_queue(QueueName=f"cfn-rule-upd-{uid}-{label}")["QueueUrl"]
        queues[label] = sqs.get_queue_attributes(
            QueueUrl=queue_urls[label], AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

    def template(schedule, description, targets):
        return json.dumps({
            "Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": {
                "Name": rule_name, "ScheduleExpression": schedule,
                "Description": description, "State": "ENABLED", "Targets": targets,
            }}},
            "Outputs": {"RuleRef": {"Value": {"Ref": "Rule"}},
                        "RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        "rate(5 minutes)", "before",
        [{"Id": "A", "Arn": queues["a"], "Input": '{"v": 1}'},
         {"Id": "B", "Arn": queues["b"]}],
    ))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "RuleArn")
        assert eb.describe_rule(Name=rule_name)["Arn"] == arn
        eb.put_targets(Rule=rule_name, Targets=[{"Id": "Foreign", "Arn": queues["c"]}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            "rate(10 minutes)", "after",
            [{"Id": "A", "Arn": queues["a"], "Input": '{"v": 2}'},
             {"Id": "C", "Arn": queues["c"]}],
        ))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleRef") == rule_name
        assert _output(stack, "RuleArn") == arn

        rule = eb.describe_rule(Name=rule_name)
        assert rule["Arn"] == arn
        assert rule["ScheduleExpression"] == "rate(10 minutes)"
        assert rule["Description"] == "after"
        assert rule["State"] == "ENABLED"
        targets = {t["Id"]: t for t in eb.list_targets_by_rule(Rule=rule_name)["Targets"]}
        assert set(targets) == {"A", "C", "Foreign"}
        assert targets["A"]["Input"] == '{"v": 2}'
        assert targets["Foreign"]["Arn"] == queues["c"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for queue_url in queue_urls.values():
            sqs.delete_queue(QueueUrl=queue_url)
    with pytest.raises(ClientError):
        eb.describe_rule(Name=rule_name)


def test_cfn_events_rule_rename_replaces_the_rule(cfn, eb):
    """Name is create-only: renaming creates the new rule and removes the old
    one, and Ref follows the new name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-ren-{uid}"

    def template(name):
        return json.dumps({
            "Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": {
                "Name": name, "ScheduleExpression": "rate(1 hour)"}}},
            "Outputs": {"RuleRef": {"Value": {"Ref": "Rule"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-rule-a-{uid}"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-rule-b-{uid}"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleRef") == f"cfn-rule-b-{uid}"
        assert eb.describe_rule(Name=f"cfn-rule-b-{uid}")["ScheduleExpression"] == "rate(1 hour)"
        with pytest.raises(ClientError):
            eb.describe_rule(Name=f"cfn-rule-a-{uid}")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_events_rule_bus_move_keeps_name_and_targets(cfn, eb, sqs):
    """EventBusName updates with some interruptions per the resource
    reference: the rule moves to the other bus under the same name, its
    targets come along, GetAtt Arn follows the bus, and nothing is left on
    the bus it came from."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-move-{uid}"
    rule_name = f"cfn-rule-move-{uid}"
    bus_name = f"cfn-rule-move-bus-{uid}"
    url = sqs.create_queue(QueueName=f"cfn-rule-move-{uid}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    def template(on_bus):
        props = {"Name": rule_name, "EventPattern": {"source": ["cfn.test"]},
                 "Targets": [{"Id": "Q", "Arn": queue_arn}]}
        if on_bus:
            props["EventBusName"] = {"Ref": "Bus"}
        return json.dumps({
            "Resources": {
                "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": bus_name}},
                "Rule": {"Type": "AWS::Events::Rule", "Properties": props},
            },
            "Outputs": {"RuleRef": {"Value": {"Ref": "Rule"}},
                        "RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleArn") == eb.describe_rule(Name=rule_name)["Arn"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleRef") == rule_name
        moved = eb.describe_rule(Name=rule_name, EventBusName=bus_name)
        assert moved["Arn"].endswith(f"rule/{bus_name}/{rule_name}")
        assert _output(stack, "RuleArn") == moved["Arn"]
        targets = eb.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)["Targets"]
        assert [t["Arn"] for t in targets] == [queue_arn]
        with pytest.raises(ClientError):
            eb.describe_rule(Name=rule_name)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        sqs.delete_queue(QueueUrl=url)
    with pytest.raises(ClientError):
        eb.describe_rule(Name=rule_name, EventBusName=bus_name)


def test_cfn_events_rule_move_to_a_missing_bus_rolls_back(cfn, eb, sqs):
    """A move onto a bus that does not exist fails the update before the
    rule is touched: the stack rolls back, the rule stays on the default
    bus with its target, and nothing is left under the other bus name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-nobus-{uid}"
    rule_name = f"cfn-rule-nobus-{uid}"
    missing_bus = f"cfn-rule-nobus-missing-{uid}"
    url = sqs.create_queue(QueueName=f"cfn-rule-nobus-{uid}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    def template(bus):
        props = {"Name": rule_name, "EventPattern": {"source": ["cfn.test"]},
                 "Targets": [{"Id": "Q", "Arn": queue_arn}]}
        if bus:
            props["EventBusName"] = bus
        return json.dumps({
            "Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": props}},
            "Outputs": {"RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(None))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "RuleArn")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(missing_bus))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "AWS::Events::Rule update failed" in _stack_event_reasons(cfn, stack_name)
        assert _output(stack, "RuleArn") == arn
        assert eb.describe_rule(Name=rule_name)["Arn"] == arn
        targets = eb.list_targets_by_rule(Rule=rule_name)["Targets"]
        assert [t["Arn"] for t in targets] == [queue_arn]
        with pytest.raises(ClientError):
            eb.describe_rule(Name=rule_name, EventBusName=missing_bus)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        sqs.delete_queue(QueueUrl=url)


def test_cfn_events_rule_tags_apply_on_create_and_update(cfn, eb):
    """Tags update with no interruption per the resource reference: the
    template's tags are on the rule after create, a changed value and a new
    key are written on update, a key the template dropped is removed, and a
    tag added with TagResource outside the stack survives."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-tags-{uid}"
    rule_name = f"cfn-rule-tags-{uid}"

    def template(tags):
        return json.dumps({
            "Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": {
                "Name": rule_name, "ScheduleExpression": "rate(1 hour)",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
            }}},
            "Outputs": {"RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}}},
        })

    def tags_of(arn):
        return {t["Key"]: t["Value"] for t in eb.list_tags_for_resource(ResourceARN=arn)["Tags"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template({"env": "dev", "team": "a"}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "RuleArn")
        assert _template_tags(tags_of(arn)) == {"env": "dev", "team": "a"}
        eb.tag_resource(ResourceARN=arn, Tags=[{"Key": "foreign", "Value": "kept"}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template({"env": "prod", "owner": "b"}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleArn") == arn
        assert _template_tags(tags_of(arn)) == {"env": "prod", "owner": "b", "foreign": "kept"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError):
        eb.list_tags_for_resource(ResourceARN=arn)


def test_cfn_events_rule_tags_follow_a_bus_move(cfn, eb):
    """A rule moved to another bus by an EventBusName update keeps its tags
    under the new ARN, a tag changed in the same update applies there, and
    nothing is left under the ARN it had on the default bus."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-tagmove-{uid}"
    rule_name = f"cfn-rule-tagmove-{uid}"
    bus_name = f"cfn-rule-tagmove-bus-{uid}"

    def template(on_bus, env):
        props = {"Name": rule_name, "EventPattern": {"source": ["cfn.test"]},
                 "Tags": [{"Key": "env", "Value": env}, {"Key": "team", "Value": "a"}]}
        if on_bus:
            props["EventBusName"] = {"Ref": "Bus"}
        return json.dumps({
            "Resources": {
                "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": bus_name}},
                "Rule": {"Type": "AWS::Events::Rule", "Properties": props},
            },
            "Outputs": {"RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}}},
        })

    def tags_of(arn):
        return {t["Key"]: t["Value"] for t in eb.list_tags_for_resource(ResourceARN=arn)["Tags"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False, "dev"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        old_arn = _output(stack, "RuleArn")
        eb.tag_resource(ResourceARN=old_arn, Tags=[{"Key": "foreign", "Value": "kept"}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True, "prod"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_arn = _output(stack, "RuleArn")
        assert new_arn == eb.describe_rule(Name=rule_name, EventBusName=bus_name)["Arn"]
        assert new_arn != old_arn
        assert tags_of(new_arn) == {"env": "prod", "team": "a", "foreign": "kept"}
        with pytest.raises(ClientError):
            eb.list_tags_for_resource(ResourceARN=old_arn)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_events_rule_dropped_description_reverts(cfn, eb):
    """Description updates with no interruption, and per the resource
    reference an argument omitted from PutRule is not kept: a Description
    dropped from the template is gone from the rule after the update."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-desc-{uid}"
    rule_name = f"cfn-rule-desc-{uid}"

    def template(description):
        props = {"Name": rule_name, "ScheduleExpression": "rate(1 hour)"}
        if description is not None:
            props["Description"] = description
        return json.dumps({"Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": props}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template("described"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert eb.describe_rule(Name=rule_name)["Description"] == "described"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        rule = eb.describe_rule(Name=rule_name)
        assert "Description" not in rule
        assert rule["ScheduleExpression"] == "rate(1 hour)"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_events_rule_removing_every_target_removes_them(cfn, eb, sqs):
    """Targets update with no interruption: a template that drops all of
    its targets leaves the rule with none of them, while a target added
    with PutTargets outside the stack stays."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-notgt-{uid}"
    rule_name = f"cfn-rule-notgt-{uid}"
    queue_urls, queue_arns = [], []
    for label in ("a", "b", "c"):
        url = sqs.create_queue(QueueName=f"cfn-rule-notgt-{uid}-{label}")["QueueUrl"]
        queue_urls.append(url)
        queue_arns.append(sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"])

    def template(targets):
        props = {"Name": rule_name, "ScheduleExpression": "rate(1 hour)"}
        if targets is not None:
            props["Targets"] = targets
        return json.dumps({"Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": props}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        [{"Id": "A", "Arn": queue_arns[0]}, {"Id": "B", "Arn": queue_arns[1]}]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert {t["Id"] for t in eb.list_targets_by_rule(Rule=rule_name)["Targets"]} == {"A", "B"}
        eb.put_targets(Rule=rule_name, Targets=[{"Id": "Foreign", "Arn": queue_arns[2]}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        targets = eb.list_targets_by_rule(Rule=rule_name)["Targets"]
        assert [t["Id"] for t in targets] == ["Foreign"]
        assert eb.describe_rule(Name=rule_name)["ScheduleExpression"] == "rate(1 hour)"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for url in queue_urls:
            sqs.delete_queue(QueueUrl=url)


def test_cfn_events_rule_generated_name_updates_in_place(cfn, eb):
    """A rule without Name gets a generated physical ID (Ref returns the
    rule ID, such as mystack-ScheduledRule-ABCDEFGHIJK, per the resource
    reference) and updates in place: Ref, GetAtt Arn and GetAtt RuleName
    are unchanged across a schedule change, and the service shows the new
    schedule under that name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rule-gen-{uid}"

    def template(schedule):
        return json.dumps({
            "Resources": {"Rule": {"Type": "AWS::Events::Rule", "Properties": {
                "ScheduleExpression": schedule}}},
            "Outputs": {"RuleRef": {"Value": {"Ref": "Rule"}},
                        "RuleArn": {"Value": {"Fn::GetAtt": ["Rule", "Arn"]}},
                        "RuleName": {"Value": {"Fn::GetAtt": ["Rule", "RuleName"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("rate(5 minutes)"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        name, arn = _output(stack, "RuleRef"), _output(stack, "RuleArn")
        assert name.startswith(f"{stack_name}-Rule-")
        assert _output(stack, "RuleName") == name
        assert eb.describe_rule(Name=name)["Arn"] == arn

        cfn.update_stack(StackName=stack_name, TemplateBody=template("rate(10 minutes)"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "RuleRef") == name
        assert _output(stack, "RuleArn") == arn
        assert _output(stack, "RuleName") == name
        rule = eb.describe_rule(Name=name)
        assert rule["Arn"] == arn
        assert rule["ScheduleExpression"] == "rate(10 minutes)"
        assert len(eb.list_rules(NamePrefix=f"{stack_name}-Rule-")["Rules"]) == 1
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError):
        eb.describe_rule(Name=name)


def _sfn_definition_json(comment):
    return json.dumps({
        "Comment": comment,
        "StartAt": "Done",
        "States": {"Done": {"Type": "Pass", "End": True}},
    })


def test_cfn_state_machine_update_keeps_arn_and_executions(cfn, sfn):
    """A changed definition and role update the machine in place through
    UpdateStateMachine: same ARN, the execution started before the update is
    still listed, DescribeStateMachine shows the new definition."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-upd-{uid}"

    def template(comment, role):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": f"cfn-sfn-upd-{uid}",
                "DefinitionString": _sfn_definition_json(comment),
                "RoleArn": f"arn:aws:iam::000000000000:role/{role}",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}},
                        "Name": {"Value": {"Fn::GetAtt": ["SM", "Name"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("v1", "sfn-role-a"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        execution = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template("v2", "sfn-role-b"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Arn") == arn
        assert _output(stack, "Name") == f"cfn-sfn-upd-{uid}"

        described = sfn.describe_state_machine(stateMachineArn=arn)
        assert json.loads(described["definition"])["Comment"] == "v2"
        assert described["roleArn"] == "arn:aws:iam::000000000000:role/sfn-role-b"
        executions = sfn.list_executions(stateMachineArn=arn)["executions"]
        assert [e["executionArn"] for e in executions] == [execution]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_type_change_under_custom_name_fails_loudly(cfn, sfn):
    """StateMachineType requires replacement, which CloudFormation refuses
    for a custom-named machine: the stack rolls back and the machine keeps
    its type."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-type-{uid}"

    def template(sm_type):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": f"cfn-sfn-type-{uid}",
                "StateMachineType": sm_type,
                "DefinitionString": _sfn_definition_json("typed"),
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("STANDARD"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("EXPRESS"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        reasons = _stack_event_reasons(cfn, stack_name)
        assert "custom-named resource requires replacing" in reasons
        assert sfn.describe_state_machine(stateMachineArn=arn)["type"] == "STANDARD"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_type_change_under_generated_name_fails_loudly(cfn, sfn):
    """Under a generated name the deterministic derivation cannot yield a
    fresh identity for the replacement, so MiniStack fails the update
    naming the property instead of rebuilding the machine in place: the
    stack rolls back and the machine keeps its ARN and type."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-gentype-{uid}"

    def template(sm_type):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineType": sm_type,
                "DefinitionString": _sfn_definition_json("generated"),
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("STANDARD"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("EXPRESS"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        reasons = _stack_event_reasons(cfn, stack_name)
        assert "StateMachineType (STANDARD -> EXPRESS) requires replacement" in reasons
        assert _output(stack, "Arn") == arn
        assert sfn.describe_state_machine(stateMachineArn=arn)["type"] == "STANDARD"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_rename_replaces_the_machine(cfn, sfn):
    """StateMachineName requires replacement: the renamed machine is created
    before the old one is removed, Ref follows the new ARN, and a logging
    configuration set outside the stack on the old machine does not carry
    over, since the new one is created from the template alone."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-ren-{uid}"

    def template(name):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": name,
                "DefinitionString": _sfn_definition_json("renamed"),
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}},
                        "Name": {"Value": {"Fn::GetAtt": ["SM", "Name"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-sfn-a-{uid}"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        old_arn = _output(stack, "Arn")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-sfn-b-{uid}"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_arn = _output(stack, "Arn")
        assert new_arn != old_arn
        assert _output(stack, "Name") == f"cfn-sfn-b-{uid}"
        assert sfn.describe_state_machine(stateMachineArn=new_arn)["name"] == f"cfn-sfn-b-{uid}"
        with pytest.raises(ClientError):
            sfn.describe_state_machine(stateMachineArn=old_arn)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_update_keeps_an_undeclared_logging_configuration(cfn, sfn):
    """A property the template never declared is not the stack's to reset: a
    LoggingConfiguration set through UpdateStateMachine outside the stack
    survives an update that only changes the definition."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-log-{uid}"

    def template(comment):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": f"cfn-sfn-log-{uid}",
                "DefinitionString": _sfn_definition_json(comment),
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("v1"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        sfn.update_state_machine(
            stateMachineArn=arn,
            loggingConfiguration={"level": "ALL", "includeExecutionData": True},
        )

        cfn.update_stack(StackName=stack_name, TemplateBody=template("v2"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        described = sfn.describe_state_machine(stateMachineArn=arn)
        assert json.loads(described["definition"])["Comment"] == "v2"
        assert described["loggingConfiguration"]["level"] == "ALL"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_update_drops_a_declared_logging_configuration(cfn, sfn):
    """LoggingConfiguration updates without interruption ("By default, the
    level is set to OFF", AWS::StepFunctions::StateMachine reference): a
    template that stops declaring it reverts the machine to OFF, since the
    property was the stack's to set."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-unlog-{uid}"

    def template(logging):
        props = {
            "StateMachineName": f"cfn-sfn-unlog-{uid}",
            "DefinitionString": _sfn_definition_json("logged"),
            "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
        }
        if logging:
            props["LoggingConfiguration"] = {"level": "ALL", "includeExecutionData": True}
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": props}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        assert sfn.describe_state_machine(stateMachineArn=arn)["loggingConfiguration"]["level"] == "ALL"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        described = sfn.describe_state_machine(stateMachineArn=arn)
        assert described["loggingConfiguration"] == {"level": "OFF", "includeExecutionData": False}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_update_applies_definition_substitutions(cfn, sfn):
    """DefinitionSubstitutions updates without interruption: a changed value
    lands in the definition through UpdateStateMachine, and the
    StateMachineRevisionId attribute ("Identifier for a state machine
    revision", AWS::StepFunctions::StateMachine reference) moves with it."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-subs-{uid}"
    definition = json.dumps({
        "StartAt": "Call",
        "States": {"Call": {"Type": "Task", "Resource": "${Target}", "End": True}},
    })

    def template(target):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": f"cfn-sfn-subs-{uid}",
                "DefinitionString": definition,
                "DefinitionSubstitutions": {"Target": target},
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}},
                        "Revision": {"Value": {"Fn::GetAtt": ["SM", "StateMachineRevisionId"]}}},
        })

    fn_a = "arn:aws:lambda:us-east-1:000000000000:function:target-a"
    fn_b = "arn:aws:lambda:us-east-1:000000000000:function:target-b"
    cfn.create_stack(StackName=stack_name, TemplateBody=template(fn_a))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        described = sfn.describe_state_machine(stateMachineArn=arn)
        assert json.loads(described["definition"])["States"]["Call"]["Resource"] == fn_a
        revision = _output(stack, "Revision")
        assert revision == described["revisionId"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(fn_b))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Arn") == arn
        described = sfn.describe_state_machine(stateMachineArn=arn)
        assert json.loads(described["definition"])["States"]["Call"]["Resource"] == fn_b
        assert _output(stack, "Revision") == described["revisionId"]
        assert _output(stack, "Revision") != revision
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_state_machine_update_changes_tags_in_place(cfn, sfn):
    """Tags update without interruption (AWS::StepFunctions::StateMachine
    reference): the machine is tagged on create, and an update rewrites a
    value, adds a key and drops another under the same ARN."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sfn-tags-{uid}"

    def template(tags):
        return json.dumps({
            "Resources": {"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
                "StateMachineName": f"cfn-sfn-tags-{uid}",
                "DefinitionString": _sfn_definition_json("tagged"),
                "RoleArn": "arn:aws:iam::000000000000:role/sfn-role",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
            }}},
            "Outputs": {"Arn": {"Value": {"Ref": "SM"}}},
        })

    def tags_of(arn):
        return {t["key"]: t["value"] for t in sfn.list_tags_for_resource(resourceArn=arn)["tags"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template({"env": "dev", "team": "iot"}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        assert _template_tags(tags_of(arn)) == {"env": "dev", "team": "iot"}

        cfn.update_stack(StackName=stack_name, TemplateBody=template({"env": "prod", "owner": "ops"}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Arn") == arn
        assert _template_tags(tags_of(arn)) == {"env": "prod", "owner": "ops"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudwatch_alarm_update_keeps_name_and_state(cfn, cw):
    """Threshold and description update the alarm in place as PutMetricAlarm
    does: same name and ARN, and the alarm state set before the update is
    left unchanged, as CloudFormation documents — its timestamp included,
    since the state did not change."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-alarm-upd-{uid}"
    alarm_name = f"cfn-alarm-upd-{uid}"

    def template(threshold, description):
        return json.dumps({
            "Resources": {"Alarm": {"Type": "AWS::CloudWatch::Alarm", "Properties": {
                "AlarmName": alarm_name, "AlarmDescription": description,
                "MetricName": "CPUUtilization", "Namespace": f"CfnAlarmUpd/{uid}",
                "Statistic": "Average", "Period": 60, "EvaluationPeriods": 1,
                "Threshold": threshold, "ComparisonOperator": "GreaterThanThreshold",
            }}},
            "Outputs": {"Name": {"Value": {"Ref": "Alarm"}},
                        "Arn": {"Value": {"Fn::GetAtt": ["Alarm", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(80, "before"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="seeded")
        seeded = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"][0]
        time.sleep(1.1)  # timestamps are whole seconds; a reset must be visible

        cfn.update_stack(StackName=stack_name, TemplateBody=template(90, "after"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == alarm_name
        assert _output(stack, "Arn") == arn

        alarms = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"]
        assert len(alarms) == 1
        assert float(alarms[0]["Threshold"]) == 90.0
        assert alarms[0]["AlarmDescription"] == "after"
        assert alarms[0]["AlarmArn"] == arn
        assert alarms[0]["StateValue"] == "ALARM"
        assert alarms[0]["StateReason"] == "seeded"
        assert alarms[0]["StateUpdatedTimestamp"] == seeded["StateUpdatedTimestamp"]
        assert alarms[0]["AlarmConfigurationUpdatedTimestamp"] > seeded["AlarmConfigurationUpdatedTimestamp"]

        _delete_cfn_test_stack(cfn, stack_name)
        assert cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"] == []
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudwatch_alarm_rename_replaces_the_alarm(cfn, cw):
    """AlarmName is create-only: renaming creates the new alarm and removes
    the old one."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-alarm-ren-{uid}"

    def template(name):
        return json.dumps({
            "Resources": {"Alarm": {"Type": "AWS::CloudWatch::Alarm", "Properties": {
                "AlarmName": name, "MetricName": "Errors", "Namespace": f"CfnAlarmRen/{uid}",
                "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
                "Threshold": 1, "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            }}},
            "Outputs": {"Name": {"Value": {"Ref": "Alarm"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-alarm-a-{uid}"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-alarm-b-{uid}"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == f"cfn-alarm-b-{uid}"
        names = {a["AlarmName"] for a in cw.describe_alarms(
            AlarmNamePrefix="cfn-alarm-")["MetricAlarms"]}
        assert f"cfn-alarm-b-{uid}" in names
        assert f"cfn-alarm-a-{uid}" not in names
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudwatch_alarm_update_drops_omitted_properties(cfn, cw):
    """"the update completely overwrites the previous configuration of the
    alarm" (aws-resource-cloudwatch-alarm.html): a property left out of the
    new template falls back to its default rather than surviving from the
    old one."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-alarm-drop-{uid}"
    alarm_name = f"cfn-alarm-drop-{uid}"
    base = {
        "AlarmName": alarm_name, "MetricName": "Errors", "Namespace": f"CfnAlarmDrop/{uid}",
        "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
        "Threshold": 1, "ComparisonOperator": "GreaterThanThreshold",
    }

    def template(**extra):
        return json.dumps({"Resources": {"Alarm": {
            "Type": "AWS::CloudWatch::Alarm", "Properties": {**base, **extra}}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        AlarmDescription="described", TreatMissingData="notBreaching",
        OKActions=[f"arn:aws:sns:us-east-1:000000000000:ok-{uid}"], Unit="Count"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        before = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"][0]
        assert before["AlarmDescription"] == "described"
        assert before["TreatMissingData"] == "notBreaching"
        assert before["OKActions"] == [f"arn:aws:sns:us-east-1:000000000000:ok-{uid}"]
        assert before["Unit"] == "Count"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(Threshold=2))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        after = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"][0]
        assert float(after["Threshold"]) == 2.0
        assert after.get("AlarmDescription", "") == ""
        assert after["TreatMissingData"] == "missing"
        assert after["OKActions"] == []
        assert "Unit" not in after
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudwatch_alarm_update_recreates_an_alarm_deleted_out_of_band(cfn, cw):
    """An alarm removed through DeleteAlarms while its stack still declares it
    is created again by the next update (the replacement path), under the
    same name so Ref and the ARN stay what the stack reported."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-alarm-oob-{uid}"
    alarm_name = f"cfn-alarm-oob-{uid}"

    def template(threshold):
        return json.dumps({
            "Resources": {"Alarm": {"Type": "AWS::CloudWatch::Alarm", "Properties": {
                "AlarmName": alarm_name, "MetricName": "Errors", "Namespace": f"CfnAlarmOob/{uid}",
                "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
                "Threshold": threshold, "ComparisonOperator": "GreaterThanThreshold",
            }}},
            "Outputs": {"Name": {"Value": {"Ref": "Alarm"}},
                        "Arn": {"Value": {"Fn::GetAtt": ["Alarm", "Arn"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(1))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        cw.delete_alarms(AlarmNames=[alarm_name])
        assert cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"] == []

        cfn.update_stack(StackName=stack_name, TemplateBody=template(2))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == alarm_name
        assert _output(stack, "Arn") == arn
        alarms = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"]
        assert len(alarms) == 1
        assert float(alarms[0]["Threshold"]) == 2.0
        assert alarms[0]["StateValue"] == "INSUFFICIENT_DATA"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_cloudwatch_alarm_tags_apply_on_create_and_update(cfn, cw):
    """Tags is "No interruption" (aws-resource-cloudwatch-alarm.html) and
    PutMetricAlarm ignores Tags on an existing alarm ("To change the tags of
    an existing alarm, use TagResource or UntagResource"), so the template's
    Tags are applied as a whole: a changed value lands, a dropped key goes,
    and the tags leave with the alarm."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-alarm-tags-{uid}"
    alarm_name = f"cfn-alarm-tags-{uid}"

    def template(tags):
        return json.dumps({
            "Resources": {"Alarm": {"Type": "AWS::CloudWatch::Alarm", "Properties": {
                "AlarmName": alarm_name, "MetricName": "Errors", "Namespace": f"CfnAlarmTags/{uid}",
                "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
                "Threshold": 1, "ComparisonOperator": "GreaterThanThreshold",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
            }}},
            "Outputs": {"Arn": {"Value": {"Fn::GetAtt": ["Alarm", "Arn"]}}},
        })

    def tags_of(arn):
        return {t["Key"]: t["Value"] for t in cw.list_tags_for_resource(ResourceARN=arn)["Tags"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template({"env": "dev", "team": "a"}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _output(stack, "Arn")
        assert _template_tags(tags_of(arn)) == {"env": "dev", "team": "a"}

        cfn.update_stack(StackName=stack_name, TemplateBody=template({"env": "prod"}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _template_tags(tags_of(arn)) == {"env": "prod"}

        _delete_cfn_test_stack(cfn, stack_name)
        with pytest.raises(ClientError) as exc:
            cw.list_tags_for_resource(ResourceARN=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFound"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_permission_test_function(lam, fn_name):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(e, c): return {}")
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.11",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )


def _lambda_policy_statements(lam, fn_name):
    try:
        return json.loads(lam.get_policy(FunctionName=fn_name)["Policy"])["Statement"]
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
        return []


def test_cfn_lambda_permission_update_replaces_the_statement(cfn, lam):
    """Every AWS::Lambda::Permission property is create-only, so a changed
    Principal removes the old statement and adds the new one: the function
    policy ends up with exactly one statement carrying the new principal,
    not two under the same Sid."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-upd-{uid}"
    fn_name = f"cfn-perm-upd-{uid}"
    _cfn_permission_test_function(lam, fn_name)

    def template(principal, source_arn):
        return json.dumps({
            "Resources": {"Perm": {"Type": "AWS::Lambda::Permission", "Properties": {
                "FunctionName": fn_name, "Action": "lambda:InvokeFunction",
                "Principal": principal, "SourceArn": source_arn,
            }}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(
            "s3.amazonaws.com", f"arn:aws:s3:::cfn-perm-upd-{uid}"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert "s3.amazonaws.com" in json.dumps(statements[0]["Principal"])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            "events.amazonaws.com", f"arn:aws:events:us-east-1:000000000000:rule/cfn-perm-{uid}"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Sid"].startswith(f"{stack_name}-Perm-")
        assert "events.amazonaws.com" in json.dumps(statements[0]["Principal"])
        assert statements[0]["Condition"]["ArnLike"]["AWS:SourceArn"].endswith(f"rule/cfn-perm-{uid}")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_delete_removes_the_statement_it_added(cfn, lam):
    """A permission declared without an Id gets the logical id as its Sid on
    create; the delete resolves the same default, so deleting the stack
    removes the statement instead of leaving it on the function."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-del-{uid}"
    fn_name = f"cfn-perm-del-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    lam.add_permission(
        FunctionName=fn_name, StatementId="kept", Action="lambda:InvokeFunction",
        Principal="sns.amazonaws.com",
    )
    template = json.dumps({
        "Resources": {"Perm": {"Type": "AWS::Lambda::Permission", "Properties": {
            "FunctionName": fn_name, "Action": "lambda:InvokeFunction",
            "Principal": "s3.amazonaws.com",
        }}},
    })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        sids = {s["Sid"] for s in _lambda_policy_statements(lam, fn_name)}
        assert "kept" in sids and len(sids) == 2
        assert any(s.startswith(f"{stack_name}-Perm-") for s in sids)

        cfn.delete_stack(StackName=stack_name)
        _wait_stack(cfn, stack_name)
        assert {s["Sid"] for s in _lambda_policy_statements(lam, fn_name)} == {"kept"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_id_change_replaces_the_statement(cfn, lam):
    """Id is create-only like every other property: changing it removes the
    statement under the old Sid and adds one under the new, so the function
    policy never carries both."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-sid-{uid}"
    fn_name = f"cfn-perm-sid-{uid}"
    _cfn_permission_test_function(lam, fn_name)

    def template(sid):
        return json.dumps({
            "Resources": {"Perm": {"Type": "AWS::Lambda::Permission", "Properties": {
                "FunctionName": fn_name, "Action": "lambda:InvokeFunction",
                "Principal": "s3.amazonaws.com", "Id": sid,
            }}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("first"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert {s["Sid"] for s in _lambda_policy_statements(lam, fn_name)} == {"first"}

        cfn.update_stack(StackName=stack_name, TemplateBody=template("second"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert {s["Sid"] for s in _lambda_policy_statements(lam, fn_name)} == {"second"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_rollback_of_a_replacement_removes_the_new_statement(cfn, lam, ddb):
    """When an update that replaced the permission fails later in the run,
    the rollback removes the statement the replacement added, since the
    delete resolves the Sid the way the create did. The previous statement
    is not re-added: the rollback restores the stack record without
    re-provisioning, which is disclosed in the PR."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-rb-{uid}"
    fn_name = f"cfn-perm-rb-{uid}"
    table_name = f"cfn-perm-rb-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    lam.add_permission(
        FunctionName=fn_name, StatementId="kept", Action="lambda:InvokeFunction",
        Principal="sns.amazonaws.com",
    )

    def template(principal, key_type):
        return json.dumps({
            "Resources": {
                "Perm": {"Type": "AWS::Lambda::Permission", "Properties": {
                    "FunctionName": fn_name, "Action": "lambda:InvokeFunction",
                    "Principal": principal,
                }},
                "Table": {"Type": "AWS::DynamoDB::Table", "DependsOn": "Perm", "Properties": {
                    "TableName": table_name,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": key_type}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                }},
            },
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("s3.amazonaws.com", "S"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        sids = {s["Sid"] for s in _lambda_policy_statements(lam, fn_name)}
        assert "kept" in sids and len(sids) == 2
        assert any(s.startswith(f"{stack_name}-Perm-") for s in sids)

        cfn.update_stack(StackName=stack_name, TemplateBody=template("events.amazonaws.com", "N"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert {s["Sid"] for s in statements} == {"kept"}
        assert ddb.describe_table(TableName=table_name)["Table"]["AttributeDefinitions"] == [
            {"AttributeName": "pk", "AttributeType": "S"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def _cfn_permission_template(fn_name, **props):
    return json.dumps({
        "Resources": {"Perm": {"Type": "AWS::Lambda::Permission", "Properties": {
            "FunctionName": fn_name, "Action": "lambda:InvokeFunction",
            "Principal": "s3.amazonaws.com", **props,
        }}},
    })


def test_cfn_lambda_permission_create_keeps_every_condition_property(cfn, lam):
    """AWS::Lambda::Permission forwards all of EventSourceToken,
    FunctionUrlAuthType, InvokedViaFunctionUrl, PrincipalOrgID, SourceAccount
    and SourceArn to AddPermission (the nine documented properties, per the
    AWS::Lambda::Permission reference), so the statement carries them as
    conditions instead of silently dropping them."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-props-{uid}"
    fn_name = f"cfn-perm-props-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(
            fn_name, SourceArn=f"arn:aws:s3:::cfn-perm-props-{uid}", SourceAccount="111122223333",
            PrincipalOrgID="o-a1b2c3d4e5", FunctionUrlAuthType="AWS_IAM", InvokedViaFunctionUrl=True,
            EventSourceToken="amzn1.ask.skill.cfn-perm-props",
        ))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Condition"] == {
            "ArnLike": {"AWS:SourceArn": f"arn:aws:s3:::cfn-perm-props-{uid}"},
            "StringEquals": {
                "AWS:SourceAccount": "111122223333",
                "aws:PrincipalOrgID": "o-a1b2c3d4e5",
                "lambda:FunctionUrlAuthType": "AWS_IAM",
                "lambda:EventSourceToken": "amzn1.ask.skill.cfn-perm-props",
            },
            "Bool": {"lambda:InvokedViaFunctionUrl": "true"},
        }
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_function_name_change_replaces_the_statement(cfn, lam):
    """FunctionName is create-only (AWS::Lambda::Permission reference: Update
    requires Replacement), so pointing the permission at another function
    removes the statement from the old function's policy and adds exactly one
    to the new function's."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-fn-{uid}"
    old_fn, new_fn = f"cfn-perm-fn-old-{uid}", f"cfn-perm-fn-new-{uid}"
    _cfn_permission_test_function(lam, old_fn)
    _cfn_permission_test_function(lam, new_fn)
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(old_fn))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        old_sids = {s["Sid"] for s in _lambda_policy_statements(lam, old_fn)}
        assert len(old_sids) == 1 and next(iter(old_sids)).startswith(f"{stack_name}-Perm-")
        assert _lambda_policy_statements(lam, new_fn) == []

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(new_fn))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _lambda_policy_statements(lam, old_fn) == []
        statements = _lambda_policy_statements(lam, new_fn)
        assert len(statements) == 1
        assert statements[0]["Sid"].startswith(f"{stack_name}-Perm-")
        assert statements[0]["Resource"].endswith(f":function:{new_fn}")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=old_fn)
        lam.delete_function(FunctionName=new_fn)


def test_cfn_lambda_permission_action_change_replaces_the_statement(cfn, lam):
    """Action is create-only (AWS::Lambda::Permission reference), so a changed
    Action leaves exactly one statement, carrying the new action."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-action-{uid}"
    fn_name = f"cfn-perm-action-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(fn_name))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert [s["Action"] for s in _lambda_policy_statements(lam, fn_name)] == ["lambda:InvokeFunction"]

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(
            fn_name, Action="lambda:InvokeFunctionUrl"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Sid"].startswith(f"{stack_name}-Perm-")
        assert statements[0]["Action"] == "lambda:InvokeFunctionUrl"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_source_account_change_replaces_the_statement(cfn, lam):
    """SourceAccount is create-only (AWS::Lambda::Permission reference) and
    reaches the statement as an AWS:SourceAccount condition, so a changed
    account leaves exactly one statement carrying the new value."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-acct-{uid}"
    fn_name = f"cfn-perm-acct-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    source_arn = f"arn:aws:s3:::cfn-perm-acct-{uid}"
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(
            fn_name, SourceArn=source_arn, SourceAccount="111111111111"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Condition"]["StringEquals"] == {"AWS:SourceAccount": "111111111111"}

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(
            fn_name, SourceArn=source_arn, SourceAccount="222222222222"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Sid"].startswith(f"{stack_name}-Perm-")
        assert statements[0]["Condition"] == {
            "ArnLike": {"AWS:SourceArn": source_arn},
            "StringEquals": {"AWS:SourceAccount": "222222222222"},
        }
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def test_cfn_lambda_permission_qualified_arn_update_replaces_on_the_qualified_resource(cfn, lam):
    """The update counterpart of the qualified-ARN create: a permission on an
    alias ARN is replaced in the base function's policy, and the one statement
    left still names the alias ARN as its Resource (AWS::Lambda::Permission
    reference: "specify a qualifier to restrict access to a single version or
    alias")."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-perm-qual-upd-{uid}"
    fn_name = f"cfn-perm-qual-upd-{uid}"
    _cfn_permission_test_function(lam, fn_name)
    try:
        version = lam.publish_version(FunctionName=fn_name)["Version"]
        alias_arn = lam.create_alias(FunctionName=fn_name, Name="live", FunctionVersion=version)["AliasArn"]
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(alias_arn))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert [s["Resource"] for s in _lambda_policy_statements(lam, fn_name)] == [alias_arn]

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_permission_template(
            alias_arn, Principal="events.amazonaws.com"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        statements = _lambda_policy_statements(lam, fn_name)
        assert len(statements) == 1
        assert statements[0]["Sid"].startswith(f"{stack_name}-Perm-")
        assert statements[0]["Resource"] == alias_arn
        assert "events.amazonaws.com" in json.dumps(statements[0]["Principal"])
        assert _lambda_policy_statements(lam, f"{fn_name}:live") == statements
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        lam.delete_function(FunctionName=fn_name)


def _cfn_policy_test_roles(iam, roles):
    assume = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"}]})
    for role in roles:
        iam.create_role(RoleName=role, AssumeRolePolicyDocument=assume)


def _cfn_policy_test_roles_cleanup(iam, roles):
    for role in roles:
        for p in iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role, PolicyArn=p["PolicyArn"])
        iam.delete_role(RoleName=role)


def test_cfn_iam_policy_update_in_place(cfn, iam):
    """PolicyDocument and Roles update the inline policy in place, as
    PutRolePolicy does: Ref keeps the policy name, GetAtt Id the same policy
    id, the document is rewritten and the second role gets the attachment."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-upd-{uid}"
    roles = [f"cfn-pol-role-a-{uid}", f"cfn-pol-role-b-{uid}"]
    _cfn_policy_test_roles(iam, roles)

    def template(actions, role_names):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
                "PolicyName": f"cfn-pol-upd-{uid}",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": actions, "Resource": "*"}]},
                "Roles": role_names,
            }}},
            "Outputs": {"Name": {"Value": {"Ref": "Pol"}},
                        "Id": {"Value": {"Fn::GetAtt": ["Pol", "Id"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(["s3:GetObject"], roles[:1]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_id = _output(stack, "Id")
        attached = iam.list_attached_role_policies(RoleName=roles[0])["AttachedPolicies"]
        assert [p["PolicyName"] for p in attached] == [f"cfn-pol-upd-{uid}"]
        policy_arn = attached[0]["PolicyArn"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            ["s3:GetObject", "s3:PutObject"], roles))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == f"cfn-pol-upd-{uid}"
        assert _output(stack, "Id") == policy_id

        policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
        assert policy["PolicyId"] == policy_id
        assert policy["AttachmentCount"] == 2
        assert policy["DefaultVersionId"] == "v2"
        version = iam.get_policy_version(
            PolicyArn=policy_arn, VersionId=policy["DefaultVersionId"])["PolicyVersion"]
        document = version["Document"]
        if isinstance(document, str):
            document = json.loads(document)
        assert document["Statement"][0]["Action"] == ["s3:GetObject", "s3:PutObject"]
        for role in roles:
            attached = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
            assert [p["PolicyArn"] for p in attached] == [policy_arn]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, roles)


def test_cfn_iam_policy_rename_keeps_id_versions_and_attachments(cfn, iam):
    """"PolicyName ... Update requires: No interruption", and "GetAtt Id: The
    stable and unique string identifying the policy"
    (aws-resource-iam-policy.html) — a renamed policy keeps its id, its
    version history and its attachments; Ref follows the new name."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-ren-{uid}"
    role = f"cfn-pol-ren-role-{uid}"
    _cfn_policy_test_roles(iam, [role])

    def template(name, action):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
                "PolicyName": name,
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": action, "Resource": "*"}]},
                "Roles": [role],
            }}},
            "Outputs": {"Name": {"Value": {"Ref": "Pol"}},
                        "Id": {"Value": {"Fn::GetAtt": ["Pol", "Id"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-pol-a-{uid}", "s3:GetObject"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_id = _output(stack, "Id")
        old_arn = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"][0]["PolicyArn"]
        created = iam.get_policy(PolicyArn=old_arn)["Policy"]["CreateDate"]

        # A document change first, so the rename has a version history to keep.
        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-pol-a-{uid}", "s3:PutObject"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert iam.get_policy(PolicyArn=old_arn)["Policy"]["DefaultVersionId"] == "v2"

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-pol-b-{uid}", "s3:PutObject"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Name") == f"cfn-pol-b-{uid}"
        assert _output(stack, "Id") == policy_id

        attached = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
        assert [p["PolicyName"] for p in attached] == [f"cfn-pol-b-{uid}"]
        new_arn = attached[0]["PolicyArn"]
        assert new_arn != old_arn
        policy = iam.get_policy(PolicyArn=new_arn)["Policy"]
        assert policy["PolicyId"] == policy_id
        assert policy["PolicyName"] == f"cfn-pol-b-{uid}"
        assert policy["AttachmentCount"] == 1
        assert policy["DefaultVersionId"] == "v2"
        assert policy["CreateDate"] == created
        versions = iam.list_policy_versions(PolicyArn=new_arn)["Versions"]
        assert sorted(v["VersionId"] for v in versions) == ["v1", "v2"]
        entities = iam.list_entities_for_policy(PolicyArn=new_arn)
        assert [r["RoleName"] for r in entities["PolicyRoles"]] == [role]
        with pytest.raises(ClientError) as exc_info:
            iam.get_policy(PolicyArn=old_arn)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchEntity"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, [role])


def test_cfn_iam_policy_role_removed_from_roles_is_detached(cfn, iam):
    """A role dropped from Roles loses the policy, the way the inline policy
    goes away on AWS, while the document, the policy id and the other
    role's attachment stay."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-rm-{uid}"
    roles = [f"cfn-pol-rm-role-a-{uid}", f"cfn-pol-rm-role-b-{uid}"]
    _cfn_policy_test_roles(iam, roles)

    def template(role_names):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
                "PolicyName": f"cfn-pol-rm-{uid}",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]},
                "Roles": role_names,
            }}},
            "Outputs": {"Id": {"Value": {"Fn::GetAtt": ["Pol", "Id"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(roles))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_id = _output(stack, "Id")
        policy_arn = iam.list_attached_role_policies(RoleName=roles[0])["AttachedPolicies"][0]["PolicyArn"]
        assert iam.get_policy(PolicyArn=policy_arn)["Policy"]["AttachmentCount"] == 2

        cfn.update_stack(StackName=stack_name, TemplateBody=template(roles[1:]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Id") == policy_id
        assert iam.list_attached_role_policies(RoleName=roles[0])["AttachedPolicies"] == []
        attached = iam.list_attached_role_policies(RoleName=roles[1])["AttachedPolicies"]
        assert [p["PolicyArn"] for p in attached] == [policy_arn]
        policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
        assert policy["PolicyId"] == policy_id
        assert policy["AttachmentCount"] == 1
        assert policy["DefaultVersionId"] == "v1"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, roles)


def test_cfn_iam_policy_users_and_groups_reconcile(cfn, iam):
    """"Users ... Update requires: No interruption" and "Groups ... Update
    requires: No interruption" (aws-resource-iam-policy.html) — a user or
    group dropped from the list is detached, one added is attached, and the
    policy id survives the update."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-ug-{uid}"
    users = [f"cfn-pol-user-a-{uid}", f"cfn-pol-user-b-{uid}"]
    groups = [f"cfn-pol-group-a-{uid}", f"cfn-pol-group-b-{uid}"]
    for user in users:
        iam.create_user(UserName=user)
    for group in groups:
        iam.create_group(GroupName=group)

    def template(user_names, group_names):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
                "PolicyName": f"cfn-pol-ug-{uid}",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]},
                "Users": user_names,
                "Groups": group_names,
            }}},
            "Outputs": {"Id": {"Value": {"Fn::GetAtt": ["Pol", "Id"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(users[:1], groups[:1]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_id = _output(stack, "Id")
        policy_arn = iam.list_attached_user_policies(UserName=users[0])["AttachedPolicies"][0]["PolicyArn"]
        assert [p["PolicyArn"] for p in
                iam.list_attached_group_policies(GroupName=groups[0])["AttachedPolicies"]] == [policy_arn]
        assert iam.get_policy(PolicyArn=policy_arn)["Policy"]["AttachmentCount"] == 2

        cfn.update_stack(StackName=stack_name, TemplateBody=template(users[1:], groups[1:]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Id") == policy_id
        assert iam.list_attached_user_policies(UserName=users[0])["AttachedPolicies"] == []
        assert iam.list_attached_group_policies(GroupName=groups[0])["AttachedPolicies"] == []
        assert [p["PolicyArn"] for p in
                iam.list_attached_user_policies(UserName=users[1])["AttachedPolicies"]] == [policy_arn]
        assert [p["PolicyArn"] for p in
                iam.list_attached_group_policies(GroupName=groups[1])["AttachedPolicies"]] == [policy_arn]
        policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
        assert policy["PolicyId"] == policy_id
        assert policy["AttachmentCount"] == 2
        entities = iam.list_entities_for_policy(PolicyArn=policy_arn)
        assert [u["UserName"] for u in entities["PolicyUsers"]] == users[1:]
        assert [g["GroupName"] for g in entities["PolicyGroups"]] == groups[1:]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for user in users:
            for p in iam.list_attached_user_policies(UserName=user)["AttachedPolicies"]:
                iam.detach_user_policy(UserName=user, PolicyArn=p["PolicyArn"])
            iam.delete_user(UserName=user)
        for group in groups:
            iam.delete_group(GroupName=group)


def test_cfn_iam_policy_delete_detaches_entities(cfn, iam):
    """Deleting the stack takes the inline policy off the role it was
    embedded in ("Adds or updates an inline policy document that is embedded
    in the specified IAM group, user or role", aws-resource-iam-policy.html):
    the role's attached list is empty and the policy is gone once the stack
    reports DELETE_COMPLETE — checked before the fixture cleanup, which
    would otherwise mask a leftover attachment."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-del-{uid}"
    role = f"cfn-pol-del-role-{uid}"
    _cfn_policy_test_roles(iam, [role])
    template = json.dumps({
        "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
            "PolicyName": f"cfn-pol-del-{uid}",
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]},
            "Roles": [role],
        }}},
    })

    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        attached = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]
        assert [p["PolicyName"] for p in attached] == [f"cfn-pol-del-{uid}"]
        policy_arn = attached[0]["PolicyArn"]

        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
        assert iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"] == []
        with pytest.raises(ClientError) as exc_info:
            iam.get_policy(PolicyArn=policy_arn)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchEntity"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, [role])


def test_cfn_iam_managed_policy_delete_detaches_entities(cfn, iam):
    """Deleting a stack detaches its AWS::IAM::ManagedPolicy from the role it
    named before the policy goes, so the role does not keep a dangling ARN
    in its attached list — checked before the fixture cleanup runs."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-mpol-del-{uid}"
    role = f"cfn-mpol-del-role-{uid}"
    _cfn_policy_test_roles(iam, [role])
    template = json.dumps({
        "Resources": {"Pol": {"Type": "AWS::IAM::ManagedPolicy", "Properties": {
            "ManagedPolicyName": f"cfn-mpol-del-{uid}",
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]},
            "Roles": [role],
        }}},
        "Outputs": {"Arn": {"Value": {"Ref": "Pol"}}},
    })

    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_arn = _output(stack, "Arn")
        assert [p["PolicyArn"] for p in
                iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]] == [policy_arn]

        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
        assert iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"] == []
        with pytest.raises(ClientError) as exc_info:
            iam.get_policy(PolicyArn=policy_arn)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchEntity"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, [role])


def test_cfn_iam_policy_version_cap_prunes_on_sixth_document(cfn, iam):
    """Every PolicyDocument change becomes a new default version; at the IAM
    five-version cap the oldest non-default version is pruned first, so the
    sixth document lands as v6 with five versions listed and the policy id
    unchanged throughout."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-iam-pol-cap-{uid}"
    role = f"cfn-pol-cap-role-{uid}"
    _cfn_policy_test_roles(iam, [role])

    def template(n):
        return json.dumps({
            "Resources": {"Pol": {"Type": "AWS::IAM::Policy", "Properties": {
                "PolicyName": f"cfn-pol-cap-{uid}",
                "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::bucket-{n}/*"}]},
                "Roles": [role],
            }}},
            "Outputs": {"Id": {"Value": {"Fn::GetAtt": ["Pol", "Id"]}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(1))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policy_id = _output(stack, "Id")
        policy_arn = iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"][0]["PolicyArn"]

        for n in range(2, 7):
            cfn.update_stack(StackName=stack_name, TemplateBody=template(n))
            stack = _wait_stack(cfn, stack_name)
            assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
            assert _output(stack, "Id") == policy_id
            assert iam.get_policy(PolicyArn=policy_arn)["Policy"]["DefaultVersionId"] == f"v{n}"

        versions = iam.list_policy_versions(PolicyArn=policy_arn)["Versions"]
        assert sorted(v["VersionId"] for v in versions) == ["v2", "v3", "v4", "v5", "v6"]
        assert [v["VersionId"] for v in versions if v["IsDefaultVersion"]] == ["v6"]
        document = iam.get_policy_version(PolicyArn=policy_arn, VersionId="v6")["PolicyVersion"]["Document"]
        if isinstance(document, str):
            document = json.loads(document)
        assert document["Statement"][0]["Resource"] == "arn:aws:s3:::bucket-6/*"
        assert iam.get_policy(PolicyArn=policy_arn)["Policy"]["AttachmentCount"] == 1
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _cfn_policy_test_roles_cleanup(iam, [role])
def test_cfn_update_rollback_keeps_the_resources_that_existed_before(cfn, sqs, ddb):
    """A failed update rolls back only what the update created. The queue
    existed before the update and kept its physical id through an in-place
    change, so the rollback leaves it alone; the queue the update added is
    deleted; the table, whose attribute type change is refused under its
    custom name, is what fails the update. The in-place change itself is not
    reverted: the queue keeps the new VisibilityTimeout while the stack
    records the old template."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rb-keep-{uid}"
    queue_name = f"cfn-rb-keep-{uid}"
    added_name = f"cfn-rb-added-{uid}"
    table_name = f"cfn-rb-keep-{uid}"

    def template(visibility, key_type, with_added):
        resources = {
            "Queue": {"Type": "AWS::SQS::Queue", "Properties": {
                "QueueName": queue_name, "VisibilityTimeout": visibility}},
            "Table": {"Type": "AWS::DynamoDB::Table", "DependsOn": "Queue", "Properties": {
                "TableName": table_name,
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": key_type}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
            }},
        }
        if with_added:
            resources["Added"] = {"Type": "AWS::SQS::Queue", "DependsOn": "Queue",
                                  "Properties": {"QueueName": added_name}}
            resources["Table"]["DependsOn"] = "Added"
        return json.dumps({"Resources": resources})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(30, "S", False))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        sqs.send_message(QueueUrl=queue_url, MessageBody="kept")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(45, "N", True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert not [e for e in events if e["LogicalResourceId"] == "Queue"
                    and e["ResourceStatus"].startswith("DELETE")]
        assert [e for e in events if e["LogicalResourceId"] == "Added"
                and e["ResourceStatus"] == "DELETE_COMPLETE"]

        assert sqs.get_queue_url(QueueName=queue_name)["QueueUrl"] == queue_url
        attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout", "ApproximateNumberOfMessages"]
        )["Attributes"]
        assert attributes["ApproximateNumberOfMessages"] == "1"
        assert attributes["VisibilityTimeout"] == "45"  # the in-place change is not reverted
        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName=added_name)
        table = ddb.describe_table(TableName=table_name)["Table"]
        assert table["AttributeDefinitions"] == [{"AttributeName": "pk", "AttributeType": "S"}]
        resources = {r["LogicalResourceId"]: r for r in cfn.describe_stack_resources(
            StackName=stack_name)["StackResources"]}
        assert set(resources) == {"Queue", "Table"}
        assert resources["Queue"]["PhysicalResourceId"].endswith(f"/{queue_name}")
        assert resources["Table"]["PhysicalResourceId"] == table_name
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    with pytest.raises(ClientError):
        sqs.get_queue_url(QueueName=queue_name)


def test_cfn_update_rollback_deletes_the_replacement_and_restores_the_old_record(cfn, sqs, ddb):
    """A resource replaced under a new physical id during a failed update has
    the replacement deleted on rollback, and the restored stack record points
    at the old physical id again. The queue is renamed, which is a replacement
    (QueueName is create-only); the table, whose attribute type change is
    refused under its custom name, is what fails the update.

    AWS, "Understand update behaviors of stack resources": a replacement
    "recreates the resource during an update, which also generates a new
    physical ID. CloudFormation usually creates the replacement resource
    first, changes references from other dependent resources to point to the
    replacement resource, and then deletes the old resource." And on a failed
    operation ("Managing AWS resources as a single unit"): "CloudFormation
    rolls the stack back and automatically deletes any resources that were
    created."

    The emulator's replacement deletes the old queue as soon as the new one
    exists, so the rollback cannot bring it back: the restored record names a
    queue that no longer exists. That is the disclosed limit this test pins.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rb-replace-{uid}"
    old_name = f"cfn-rb-old-{uid}"
    new_name = f"cfn-rb-new-{uid}"
    table_name = f"cfn-rb-replace-{uid}"

    def template(queue_name, key_type):
        return json.dumps({"Resources": {
            "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": queue_name}},
            "Table": {"Type": "AWS::DynamoDB::Table", "DependsOn": "Queue", "Properties": {
                "TableName": table_name,
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": key_type}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
            }},
        }})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(old_name, "S"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        old_url = sqs.get_queue_url(QueueName=old_name)["QueueUrl"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(new_name, "N"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        events = [e for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
                  if e["LogicalResourceId"] == "Queue"]
        deleted = [e for e in events if e["ResourceStatus"] == "DELETE_COMPLETE"]
        assert [e["PhysicalResourceId"] for e in deleted] == [f"{old_url.rsplit('/', 1)[0]}/{new_name}"]

        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName=new_name)
        with pytest.raises(ClientError):  # the replacement already removed it; not restored
            sqs.get_queue_url(QueueName=old_name)
        resources = {r["LogicalResourceId"]: r for r in cfn.describe_stack_resources(
            StackName=stack_name)["StackResources"]}
        assert set(resources) == {"Queue", "Table"}
        assert resources["Queue"]["PhysicalResourceId"] == old_url
        assert resources["Table"]["PhysicalResourceId"] == table_name
        table = ddb.describe_table(TableName=table_name)["Table"]
        assert table["AttributeDefinitions"] == [{"AttributeName": "pk", "AttributeType": "S"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
def _queue_exists(sqs, name):
    try:
        sqs.get_queue_url(QueueName=name)
        return True
    except ClientError as exc:
        assert "NonExistentQueue" in exc.response["Error"]["Code"], exc.response["Error"]
        return False


def _delete_queue_if_present(sqs, name):
    try:
        sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=name)["QueueUrl"])
    except ClientError:
        pass


def _resource_events(cfn, stack_name, logical_id):
    return [(e["ResourceStatus"], e.get("PhysicalResourceId", ""))
            for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
            if e["LogicalResourceId"] == logical_id]


def test_cfn_deletion_policy_retain_survives_the_stack_delete(cfn, sqs):
    """DeletionPolicy Retain keeps the resource when the stack is deleted: the
    queue survives with a DELETE_SKIPPED event, its sibling is deleted, the
    stack still reaches DELETE_COMPLETE."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-retain-del-{uid}"
    keep, gone = f"cfn-retain-keep-{uid}", f"cfn-retain-gone-{uid}"
    template = json.dumps({"Resources": {
        "Keep": {"Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain",
                 "Properties": {"QueueName": keep}},
        "Gone": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": gone}},
    }})
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        stack_id = stack["StackId"]

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE"
        assert _queue_exists(sqs, keep)
        assert not _queue_exists(sqs, gone)
        events = _resource_events(cfn, stack_id, "Keep")
        assert ("DELETE_SKIPPED", sqs.get_queue_url(QueueName=keep)["QueueUrl"]) in events
        assert "DELETE_IN_PROGRESS" not in [status for status, _ in events]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _delete_queue_if_present(sqs, keep)
        _delete_queue_if_present(sqs, gone)


def test_cfn_update_replace_policy_retain_keeps_the_predecessor(cfn, sqs):
    """A replacement under UpdateReplacePolicy Retain leaves the old physical
    resource in place (DELETE_SKIPPED); without the policy the predecessor is
    deleted in the cleanup phase."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-retain-repl-{uid}"
    names = [f"cfn-retain-repl-{uid}-{i}" for i in ("a", "b", "c")]

    def template(name, policy):
        queue = {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": name}}
        if policy:
            queue["UpdateReplacePolicy"] = policy
        return json.dumps({"Resources": {"Queue": queue}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(names[0], "Retain"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(names[1], "Retain"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _queue_exists(sqs, names[0])
        assert _queue_exists(sqs, names[1])
        assert "DELETE_SKIPPED" in [s for s, _ in _resource_events(cfn, stack_name, "Queue")]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(names[2], None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert not _queue_exists(sqs, names[1])
        assert _queue_exists(sqs, names[2])
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for name in names:
            _delete_queue_if_present(sqs, name)


def test_cfn_deletion_policy_retain_on_a_removed_resource(cfn, sqs):
    """A resource dropped from the template on update keeps existing when its
    (previous) DeletionPolicy was Retain; it leaves the stack's scope."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-retain-rm-{uid}"
    keep, other = f"cfn-retain-rm-keep-{uid}", f"cfn-retain-rm-other-{uid}"
    with_keep = json.dumps({"Resources": {
        "Keep": {"Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain",
                 "Properties": {"QueueName": keep}},
        "Other": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": other}},
    }})
    without = json.dumps({"Resources": {
        "Other": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": other}},
    }})
    cfn.create_stack(StackName=stack_name, TemplateBody=with_keep)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        cfn.update_stack(StackName=stack_name, TemplateBody=without)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _queue_exists(sqs, keep)
        assert [r["LogicalResourceId"] for r in
                cfn.describe_stack_resources(StackName=stack_name)["StackResources"]] == ["Other"]
        assert "DELETE_SKIPPED" in [s for s, _ in _resource_events(cfn, stack_name, "Keep")]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        _delete_queue_if_present(sqs, keep)


def test_cfn_retain_except_on_create_is_deleted_by_the_create_rollback(cfn, sqs):
    """Retain survives the rollback of the operation that created the resource;
    RetainExceptOnCreate does not, and neither does Retain when the request
    carries RetainExceptOnCreate=true."""
    uid = _uuid_mod.uuid4().hex[:8]
    retained, except_on_create, flagged = (
        f"cfn-reoc-retain-{uid}", f"cfn-reoc-except-{uid}", f"cfn-reoc-flag-{uid}")

    def template(name, policy):
        return json.dumps({"Resources": {
            "Queue": {"Type": "AWS::SQS::Queue", "DeletionPolicy": policy,
                      "Properties": {"QueueName": name}},
            "Bad": {**_FAILING_RESOURCE, "DependsOn": "Queue"},
        }})

    stacks = [f"cfn-reoc-a-{uid}", f"cfn-reoc-b-{uid}", f"cfn-reoc-c-{uid}"]
    try:
        cfn.create_stack(StackName=stacks[0], TemplateBody=template(retained, "Retain"))
        cfn.create_stack(StackName=stacks[1],
                         TemplateBody=template(except_on_create, "RetainExceptOnCreate"))
        cfn.create_stack(StackName=stacks[2], TemplateBody=template(flagged, "Retain"),
                         RetainExceptOnCreate=True)
        for name in stacks:
            stack = _wait_stack(cfn, name)
            assert stack["StackStatus"] == "ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _queue_exists(sqs, retained)
        assert "DELETE_SKIPPED" in [s for s, _ in _resource_events(cfn, stacks[0], "Queue")]
        assert not _queue_exists(sqs, except_on_create)
        assert not _queue_exists(sqs, flagged)
    finally:
        for name in stacks:
            _delete_cfn_test_stack(cfn, name)
        for name in (retained, except_on_create, flagged):
            _delete_queue_if_present(sqs, name)


def test_cfn_retain_except_on_create_on_an_update_rollback(cfn, sqs):
    """A resource that an update adds is "created" by that update: on the
    update's rollback RetainExceptOnCreate deletes it, Retain keeps it."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-reoc-upd-{uid}"
    base_name, kept, dropped = (f"cfn-reoc-upd-{uid}", f"cfn-reoc-upd-keep-{uid}",
                                f"cfn-reoc-upd-drop-{uid}")
    base = {"Resources": {"Base": {"Type": "AWS::SQS::Queue",
                                   "Properties": {"QueueName": base_name}}}}
    updated = json.loads(json.dumps(base))
    updated["Resources"]["Kept"] = {"Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain",
                                    "Properties": {"QueueName": kept}}
    updated["Resources"]["Dropped"] = {"Type": "AWS::SQS::Queue",
                                       "DeletionPolicy": "RetainExceptOnCreate",
                                       "Properties": {"QueueName": dropped}}
    updated["Resources"]["Bad"] = {**_FAILING_RESOURCE, "DependsOn": ["Kept", "Dropped"]}
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _queue_exists(sqs, base_name)
        assert _queue_exists(sqs, kept)
        assert not _queue_exists(sqs, dropped)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        for name in (base_name, kept, dropped):
            _delete_queue_if_present(sqs, name)


def test_cfn_deletion_policy_from_an_intrinsic(cfn, sqs):
    """A DeletionPolicy given as Fn::If resolves against the stack's
    conditions before it is applied."""
    uid = _uuid_mod.uuid4().hex[:8]
    template = json.dumps({
        "Parameters": {"Keep": {"Type": "String", "Default": "no"}},
        "Conditions": {"KeepIt": {"Fn::Equals": [{"Ref": "Keep"}, "yes"]}},
        "Resources": {"Queue": {
            "Type": "AWS::SQS::Queue",
            "DeletionPolicy": {"Fn::If": ["KeepIt", "Retain", "Delete"]},
            "Properties": {"QueueName": {"Ref": "AWS::StackName"}}}},
    })
    stacks = {f"cfn-policy-if-keep-{uid}": "yes", f"cfn-policy-if-drop-{uid}": "no"}
    try:
        for name, keep in stacks.items():
            cfn.create_stack(StackName=name, TemplateBody=template,
                             Parameters=[{"ParameterKey": "Keep", "ParameterValue": keep}])
        for name in stacks:
            assert _wait_stack(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
            cfn.delete_stack(StackName=name)
            assert _wait_stack(cfn, name)["StackStatus"] == "DELETE_COMPLETE"
        assert _queue_exists(sqs, f"cfn-policy-if-keep-{uid}")
        assert not _queue_exists(sqs, f"cfn-policy-if-drop-{uid}")
    finally:
        for name in stacks:
            _delete_cfn_test_stack(cfn, name)
            _delete_queue_if_present(sqs, name)


def test_cfn_delete_stack_retain_resources_only_for_delete_failed(cfn, sqs, lam):
    """RetainResources is refused on a healthy stack; on a DELETE_FAILED stack
    it skips the named resource and the stack reaches DELETE_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-retain-res-{suffix}"
    stack_name = f"cfn-retain-resources-{suffix}"
    queue = f"cfn-retain-resources-{suffix}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_CR_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_CR_HANDLER_DELETE_FAILS)},
    )
    template = json.dumps({"Resources": {
        "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": queue}},
        "CR": {"Type": "Custom::Tester", "Properties": {
            "ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"}},
    }})
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        with pytest.raises(ClientError) as exc:
            cfn.delete_stack(StackName=stack_name, RetainResources=["CR"])
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert "DELETE_FAILED" in exc.value.response["Error"]["Message"]

        cfn.delete_stack(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_FAILED", stack.get("StackStatusReason")
        assert not _queue_exists(sqs, queue)

        with pytest.raises(ClientError) as exc:
            cfn.delete_stack(StackName=stack_name, RetainResources=["Nope"])
        assert "Nope" in exc.value.response["Error"]["Message"]

        cfn.delete_stack(StackName=stack_name, RetainResources=["CR"])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "DELETE_COMPLETE", stack.get("StackStatusReason")
    finally:
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=_cr_make_zip(_CR_HANDLER_SUCCESS))
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        _delete_queue_if_present(sqs, queue)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_delete_change_set_existing_succeeds(cfn):
    """DeleteChangeSet of an existing change set succeeds -- by stack name and
    by stack ID -- and the response parses (boto3 needs the
    DeleteChangeSetResult element; the success answer used to lack it)."""
    name = f"cfn-delcs-ok-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=name, TemplateBody=json.dumps(
        {"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}))
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        stack_id = stack["StackId"]
        update = json.dumps({"Resources": {
            "Q": {"Type": "AWS::SQS::Queue"},
            "P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/{name}/p", "Type": "String", "Value": "v"}}}})
        for cs_name, by in (("cs-by-name", name), ("cs-by-id", stack_id)):
            cfn.create_change_set(StackName=name, ChangeSetName=cs_name,
                                  TemplateBody=update, ChangeSetType="UPDATE")
            assert cfn.describe_change_set(
                StackName=name, ChangeSetName=cs_name)["Status"] == "CREATE_COMPLETE"
            resp = cfn.delete_change_set(StackName=by, ChangeSetName=cs_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            with pytest.raises(ClientError) as exc:
                cfn.describe_change_set(StackName=name, ChangeSetName=cs_name)
            assert exc.value.response["Error"]["Code"] == "ChangeSetNotFound"
        names = [c["ChangeSetName"] for c in cfn.list_change_sets(StackName=name)["Summaries"]]
        assert names == []
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_delete_change_set_missing_is_idempotent(cfn):
    """DeleteChangeSet of a change set that does not exist succeeds on an
    existing stack, addressed by name or by stack ID (measured on a real
    account) -- the CDK removes a possible leftover `cdk-deploy-change-set`
    before every deploy, addresses the stack by ARN, and aborts on any error
    other than ChangeSetNotFoundException. A missing stack is still a
    ValidationError, and so is a deleted one."""
    name = f"cfn-delcs-idem-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=name, TemplateBody=json.dumps(
        {"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}))
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        for by in (name, stack["StackId"]):
            resp = cfn.delete_change_set(StackName=by, ChangeSetName="cdk-deploy-change-set")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        with pytest.raises(ClientError) as exc:
            cfn.describe_change_set(StackName=name, ChangeSetName="cdk-deploy-change-set")
        assert exc.value.response["Error"]["Code"] == "ChangeSetNotFound"
        with pytest.raises(ClientError) as exc:
            cfn.delete_change_set(StackName=f"{name}-nope", ChangeSetName="x")
        assert exc.value.response["Error"]["Code"] == "ValidationError"
    finally:
        _delete_cfn_test_stack(cfn, name)
    with pytest.raises(ClientError) as exc:
        cfn.delete_change_set(StackName=name, ChangeSetName="cdk-deploy-change-set")
    assert exc.value.response["Error"]["Code"] == "ValidationError"


def test_cfn_iot_thing_group_lifecycle(cfn, iot_client):
    """AWS::IoT::ThingGroup provisions with its properties and parent, Ref is
    the group id and Fn::GetAtt serves Arn and Id; a ThingGroupProperties
    change updates the group in place under the same id, a dropped
    description or attribute set is cleared, and deleting the stack removes
    the group."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-thing-group-{uid}"
    parent_name = f"cfn-tg-parent-{uid}"
    group_name = f"cfn-tg-child-{uid}"

    def template(description, attributes):
        # ParentGroupName takes the parent's NAME; Ref would give its id.
        properties = {"ThingGroupName": group_name, "ParentGroupName": parent_name}
        if description is not None or attributes is not None:
            properties["ThingGroupProperties"] = {}
            if description is not None:
                properties["ThingGroupProperties"]["ThingGroupDescription"] = description
            if attributes is not None:
                properties["ThingGroupProperties"]["AttributePayload"] = {"Attributes": attributes}
        return json.dumps({
            "Resources": {
                "Parent": {"Type": "AWS::IoT::ThingGroup", "Properties": {
                    "ThingGroupName": parent_name}},
                "Group": {"Type": "AWS::IoT::ThingGroup", "DependsOn": "Parent",
                          "Properties": properties},
            },
            "Outputs": {
                "Id": {"Value": {"Ref": "Group"}},
                "Arn": {"Value": {"Fn::GetAtt": ["Group", "Arn"]}},
                "AttId": {"Value": {"Fn::GetAtt": ["Group", "Id"]}},
                "ParentRef": {"Value": {"Ref": "Parent"}},
            },
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("fleet", {"site": "a"}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        group = iot_client.describe_thing_group(thingGroupName=group_name)
        assert _output(stack, "Id") == group["thingGroupId"] == _output(stack, "AttId")
        assert _output(stack, "Arn") == group["thingGroupArn"]
        assert group["thingGroupProperties"]["thingGroupDescription"] == "fleet"
        assert group["thingGroupProperties"]["attributePayload"]["attributes"] == {"site": "a"}
        assert group["thingGroupMetadata"]["parentGroupName"] == parent_name
        parent = iot_client.describe_thing_group(thingGroupName=parent_name)
        assert _output(stack, "ParentRef") == parent["thingGroupId"]
        # The Ref is the id, not the name, as the resource reference documents.
        assert _output(stack, "Id") != group_name
        group_id = group["thingGroupId"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template("fleet-b", {"site": "b", "tier": "1"}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        group = iot_client.describe_thing_group(thingGroupName=group_name)
        assert group["thingGroupId"] == group_id == _output(stack, "Id")
        assert group["thingGroupProperties"]["thingGroupDescription"] == "fleet-b"
        assert group["thingGroupProperties"]["attributePayload"]["attributes"] == {"site": "b", "tier": "1"}
        assert group["version"] == 2

        cfn.update_stack(StackName=stack_name, TemplateBody=template(None, None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        group = iot_client.describe_thing_group(thingGroupName=group_name)
        assert group["thingGroupId"] == group_id
        assert not group["thingGroupProperties"].get("thingGroupDescription")
        assert group["thingGroupProperties"]["attributePayload"]["attributes"] == {}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    for name in (group_name, parent_name):
        with pytest.raises(ClientError) as exc:
            iot_client.describe_thing_group(thingGroupName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_cfn_iot_thing_group_rename_replaces_and_parent_change_is_refused(cfn, iot_client):
    """ThingGroupName and ParentGroupName require replacement: a renamed group
    is created before the old one is removed and Ref follows the new id, while
    a parent change under an unchanged custom name gets CloudFormation's own
    refusal; a group without ThingGroupName gets a generated name and can be
    re-parented, since the replacement runs under a fresh identity."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-thing-group-repl-{uid}"
    parents = (f"cfn-tg-p1-{uid}", f"cfn-tg-p2-{uid}")

    def template(name, parent):
        return json.dumps({
            "Resources": {
                "P1": {"Type": "AWS::IoT::ThingGroup", "Properties": {"ThingGroupName": parents[0]}},
                "P2": {"Type": "AWS::IoT::ThingGroup", "Properties": {"ThingGroupName": parents[1]}},
                "Group": {"Type": "AWS::IoT::ThingGroup", "DependsOn": ["P1", "P2"], "Properties": {
                    "ThingGroupName": name, "ParentGroupName": parent}},
                "Unnamed": {"Type": "AWS::IoT::ThingGroup", "DependsOn": ["P1", "P2"], "Properties": {
                    "ParentGroupName": parent}},
            },
            "Outputs": {"Id": {"Value": {"Ref": "Group"}},
                        "UnnamedId": {"Value": {"Ref": "Unnamed"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"cfn-tg-a-{uid}", parents[0]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        first_id = _output(stack, "Id")
        unnamed_id = _output(stack, "UnnamedId")
        unnamed_name = next(
            g["groupName"] for g in iot_client.list_thing_groups()["thingGroups"]
            if g["groupName"].startswith(f"{stack_name}-Unnamed-"))

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-tg-b-{uid}", parents[0]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        renamed = iot_client.describe_thing_group(thingGroupName=f"cfn-tg-b-{uid}")
        assert _output(stack, "Id") == renamed["thingGroupId"] != first_id
        with pytest.raises(ClientError):
            iot_client.describe_thing_group(thingGroupName=f"cfn-tg-a-{uid}")
        assert _output(stack, "UnnamedId") == unnamed_id

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"cfn-tg-b-{uid}", parents[1]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        reasons = _stack_event_reasons(cfn, stack_name)
        assert "custom-named resource requires replacing" in reasons, reasons
        assert iot_client.describe_thing_group(thingGroupName=f"cfn-tg-b-{uid}")[
            "thingGroupMetadata"]["parentGroupName"] == parents[0]
        # The generated-name group was re-parented before the failure and
        # rolled back with the stack: it still reports the first parent.
        unnamed = iot_client.describe_thing_group(thingGroupName=unnamed_name)
        assert unnamed["thingGroupMetadata"]["parentGroupName"] in parents
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_apigateway_authorizer_update_in_place_and_replacement(cfn, apigw_v1):
    """An authorizer property change updates the authorizer under the same id
    (Ref and AuthorizerId keep their value), a property the template drops
    reverts to the create default, and a RestApiId change replaces the
    authorizer: it appears on the new API and is gone from the old one."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-authz-upd-{uid}"
    uri = ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
           "arn:aws:lambda:us-east-1:000000000000:function:noop/invocations")

    def template(api, name, ttl, identity_source, auth_type, kind="TOKEN", providers=None):
        properties = {"Name": name, "Type": kind, "RestApiId": {"Ref": api},
                      "AuthorizerUri": uri, "IdentitySource": identity_source}
        if providers:
            properties["ProviderARNs"] = providers
        if ttl is not None:
            properties["AuthorizerResultTtlInSeconds"] = ttl
        if auth_type is not None:
            properties["AuthType"] = auth_type
        return json.dumps({
            "Resources": {
                "ApiA": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": f"cfn-authz-a-{uid}"}},
                "ApiB": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": f"cfn-authz-b-{uid}"}},
                "Auth": {"Type": "AWS::ApiGateway::Authorizer", "Properties": properties},
            },
            "Outputs": {"Id": {"Value": {"Ref": "Auth"}},
                        "AttId": {"Value": {"Fn::GetAtt": ["Auth", "AuthorizerId"]}},
                        "ApiA": {"Value": {"Ref": "ApiA"}}, "ApiB": {"Value": {"Ref": "ApiB"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        "ApiA", "first", 60, "method.request.header.X-Token", "custom"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        api_a, api_b = _output(stack, "ApiA"), _output(stack, "ApiB")
        auth_id = _output(stack, "Id")
        assert _output(stack, "AttId") == auth_id
        authorizer = apigw_v1.get_authorizer(restApiId=api_a, authorizerId=auth_id)
        assert (authorizer["name"], authorizer["authorizerResultTtlInSeconds"],
                authorizer["identitySource"], authorizer["authType"]) == (
            "first", 60, "method.request.header.X-Token", "custom")

        pool_arn = f"arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_{uid}"
        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            "ApiA", "second", None, "method.request.header.Authorization", None,
            kind="COGNITO_USER_POOLS", providers=[pool_arn]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Id") == auth_id
        authorizer = apigw_v1.get_authorizer(restApiId=api_a, authorizerId=auth_id)
        assert (authorizer["name"], authorizer["authorizerResultTtlInSeconds"],
                authorizer["identitySource"], authorizer["type"], authorizer["providerARNs"]) == (
            "second", 300, "method.request.header.Authorization", "COGNITO_USER_POOLS", [pool_arn])
        assert "authType" not in authorizer
        assert len(apigw_v1.get_authorizers(restApiId=api_a)["items"]) == 1

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            "ApiB", "second", None, "method.request.header.Authorization", None))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_id = _output(stack, "Id")
        assert new_id != auth_id
        assert apigw_v1.get_authorizers(restApiId=api_a)["items"] == []
        assert [a["id"] for a in apigw_v1.get_authorizers(restApiId=api_b)["items"]] == [new_id]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_apigateway_deployment_update_in_place_and_replacement(cfn, apigw_v1):
    """A deployment's Description, StageName and StageDescription update in
    place under the same deployment id: the description patches the
    deployment, a new stage name deploys the same deployment to that stage
    and leaves the previous stage standing. A RestApiId change replaces the
    deployment on the other API."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-deploy-upd-{uid}"

    def template(api, description, stage_name, stage_description, canary=None):
        properties = {"RestApiId": {"Ref": api}, "Description": description}
        if canary is not None:
            properties["DeploymentCanarySettings"] = {"PercentTraffic": canary}
        if stage_name:
            properties["StageName"] = stage_name
            properties["StageDescription"] = {"Description": stage_description,
                                              "Variables": {"stage": stage_name}}
        return json.dumps({
            "Resources": {
                "ApiA": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": f"cfn-deploy-a-{uid}"}},
                "ApiB": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": f"cfn-deploy-b-{uid}"}},
                "Dep": {"Type": "AWS::ApiGateway::Deployment", "Properties": properties},
            },
            "Outputs": {"Id": {"Value": {"Ref": "Dep"}},
                        "AttId": {"Value": {"Fn::GetAtt": ["Dep", "DeploymentId"]}},
                        "ApiA": {"Value": {"Ref": "ApiA"}}, "ApiB": {"Value": {"Ref": "ApiB"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("ApiA", "one", "alpha", "stage one"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        api_a, api_b = _output(stack, "ApiA"), _output(stack, "ApiB")
        dep_id = _output(stack, "Id")
        assert _output(stack, "AttId") == dep_id
        assert apigw_v1.get_deployment(restApiId=api_a, deploymentId=dep_id)["description"] == "one"
        stage = apigw_v1.get_stage(restApiId=api_a, stageName="alpha")
        assert (stage["deploymentId"], stage["description"], stage["variables"]) == (
            dep_id, "stage one", {"stage": "alpha"})

        cfn.update_stack(StackName=stack_name, TemplateBody=template("ApiA", "two", "beta", "stage two"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Id") == dep_id
        assert apigw_v1.get_deployment(restApiId=api_a, deploymentId=dep_id)["description"] == "two"
        assert len(apigw_v1.get_deployments(restApiId=api_a)["items"]) == 1
        beta = apigw_v1.get_stage(restApiId=api_a, stageName="beta")
        assert (beta["deploymentId"], beta["description"], beta["variables"]) == (
            dep_id, "stage two", {"stage": "beta"})
        assert apigw_v1.get_stage(restApiId=api_a, stageName="alpha")["deploymentId"] == dep_id

        cfn.update_stack(StackName=stack_name, TemplateBody=template("ApiB", "two", "beta", "stage two"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        new_id = _output(stack, "Id")
        assert new_id != dep_id
        assert apigw_v1.get_deployments(restApiId=api_a)["items"] == []
        assert [d["id"] for d in apigw_v1.get_deployments(restApiId=api_b)["items"]] == [new_id]
        assert apigw_v1.get_stage(restApiId=api_b, stageName="beta")["deploymentId"] == new_id

        # DeploymentCanarySettings requires replacement on the reference: a
        # new deployment id, the previous one removed by the engine.
        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            "ApiB", "two", "beta", "stage two", canary=10.0))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        canary_id = _output(stack, "Id")
        assert canary_id != new_id
        assert [d["id"] for d in apigw_v1.get_deployments(restApiId=api_b)["items"]] == [canary_id]
        assert apigw_v1.get_stage(restApiId=api_b, stageName="beta")["deploymentId"] == canary_id
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_iot_thing_group_missing_parent_fails_and_unmodelled_properties_are_accepted(cfn, iot_client):
    """A parent that does not exist fails the stack with the service's own
    ResourceNotFoundException; QueryString (dynamic groups) and Tags, which
    the service does not model, are accepted without effect."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-thing-group-edge-{uid}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps({
        "Resources": {"Group": {"Type": "AWS::IoT::ThingGroup", "Properties": {
            "ThingGroupName": f"cfn-tg-orphan-{uid}", "ParentGroupName": f"cfn-tg-nowhere-{uid}"}}},
    }))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert "ResourceNotFoundException" in _stack_event_reasons(cfn, stack_name)
        with pytest.raises(ClientError):
            iot_client.describe_thing_group(thingGroupName=f"cfn-tg-orphan-{uid}")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)

    stack_name = f"cfn-thing-group-dyn-{uid}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps({
        "Resources": {"Group": {"Type": "AWS::IoT::ThingGroup", "Properties": {
            "ThingGroupName": f"cfn-tg-dyn-{uid}",
            "QueryString": "attributes.site:a",
            "Tags": [{"Key": "owner", "Value": "fleet"}],
            "ThingGroupProperties": {"ThingGroupDescription": "dynamic on AWS, static here"}}}},
        "Outputs": {"Id": {"Value": {"Ref": "Group"}}},
    }))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        group = iot_client.describe_thing_group(thingGroupName=f"cfn-tg-dyn-{uid}")
        assert group["thingGroupId"] == _output(stack, "Id")
        assert group["thingGroupProperties"]["thingGroupDescription"] == "dynamic on AWS, static here"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_kms_alias_target_change_in_place_and_rename_replaces(cfn, kms_client):
    """A TargetKeyId change re-points the alias under its name (Ref keeps its
    value), an AliasName change replaces the alias — the new name exists, the
    old one is gone — and an alias name without the alias/ prefix fails the
    stack as the resource reference requires."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-kms-alias-{uid}"

    def template(alias_name, key):
        return json.dumps({
            "Resources": {
                "KeyA": {"Type": "AWS::KMS::Key", "Properties": {"Description": f"a-{uid}"}},
                "KeyB": {"Type": "AWS::KMS::Key", "Properties": {"Description": f"b-{uid}"}},
                "Alias": {"Type": "AWS::KMS::Alias", "Properties": {
                    "AliasName": alias_name, "TargetKeyId": {"Ref": key}}},
            },
            "Outputs": {"Alias": {"Value": {"Ref": "Alias"}},
                        "KeyA": {"Value": {"Ref": "KeyA"}}, "KeyB": {"Value": {"Ref": "KeyB"}}},
        })

    def alias_targets():
        return {a["AliasName"]: a.get("TargetKeyId")
                for a in kms_client.list_aliases()["Aliases"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"alias/cfn-a-{uid}", "KeyA"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        key_a, key_b = _output(stack, "KeyA"), _output(stack, "KeyB")
        assert _output(stack, "Alias") == f"alias/cfn-a-{uid}"
        assert alias_targets()[f"alias/cfn-a-{uid}"] == key_a

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"alias/cfn-a-{uid}", "KeyB"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Alias") == f"alias/cfn-a-{uid}"
        assert alias_targets()[f"alias/cfn-a-{uid}"] == key_b

        cfn.update_stack(StackName=stack_name, TemplateBody=template(f"alias/cfn-b-{uid}", "KeyB"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _output(stack, "Alias") == f"alias/cfn-b-{uid}"
        targets = alias_targets()
        assert targets[f"alias/cfn-b-{uid}"] == key_b
        assert f"alias/cfn-a-{uid}" not in targets

        for bad_name in (f"cfn-c-{uid}", f"alias/aws/cfn-c-{uid}", f"alias/cfn c {uid}"):
            cfn.update_stack(StackName=stack_name, TemplateBody=template(bad_name, "KeyB"))
            stack = _wait_stack(cfn, stack_name)
            assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
            assert "must begin with alias/" in _stack_event_reasons(cfn, stack_name)
            assert alias_targets()[f"alias/cfn-b-{uid}"] == key_b
            assert bad_name not in alias_targets()
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    assert f"alias/cfn-b-{uid}" not in alias_targets()


def test_cfn_update_rollback_reports_the_previous_template_parameters_and_tags(cfn, sqs):
    """After UPDATE_ROLLBACK_COMPLETE the stack describes what it ran before
    the failed update: GetTemplate returns the previous template body,
    DescribeStacks the previous parameter values and tags. The update fails on
    the custom-named table whose key type change requires replacement."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rb-report-{uid}"
    queue_name = f"cfn-rb-report-{uid}"
    table_name = f"cfn-rb-report-{uid}"

    def template(key_type, description):
        return json.dumps({
            "Description": description,
            "Parameters": {"Visibility": {"Type": "Number", "Default": 30}},
            "Resources": {
                "Queue": {"Type": "AWS::SQS::Queue", "Properties": {
                    "QueueName": queue_name,
                    "VisibilityTimeout": {"Ref": "Visibility"}}},
                "Table": {"Type": "AWS::DynamoDB::Table", "DependsOn": "Queue", "Properties": {
                    "TableName": table_name,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": key_type}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                }},
            },
        })

    def normalized(body):
        return json.loads(body) if isinstance(body, str) else body

    original = template("S", "before")
    cfn.create_stack(
        StackName=stack_name, TemplateBody=original,
        Parameters=[{"ParameterKey": "Visibility", "ParameterValue": "30"}],
        Tags=[{"Key": "stage", "Value": "before"}],
    )
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        cfn.update_stack(
            StackName=stack_name, TemplateBody=template("N", "after"),
            Parameters=[{"ParameterKey": "Visibility", "ParameterValue": "45"}],
            Tags=[{"Key": "stage", "Value": "after"}],
        )
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")

        assert normalized(cfn.get_template(StackName=stack_name)["TemplateBody"]) == json.loads(original)
        assert stack["Parameters"] == [
            {"ParameterKey": "Visibility", "ParameterValue": "30"}]
        assert stack["Tags"] == [{"Key": "stage", "Value": "before"}]
        assert stack.get("Description") in (None, "before")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_export_in_use_is_an_import_not_a_mention(cfn, ssm):
    """Deleting an exporting stack is refused only while another stack imports
    the export through Fn::ImportValue, its argument resolved with that stack's
    parameters. A stack whose template merely carries the export name in a
    string value, or an Fn::ImportValue under Metadata, does not block the
    delete."""
    uid = _uuid_mod.uuid4().hex[:8]
    export_name = f"cfn-export-use-{uid}"
    producer = f"cfn-export-producer-{uid}"
    mention = f"cfn-export-mention-{uid}"
    importer = f"cfn-export-importer-{uid}"
    cfn.create_stack(StackName=producer, TemplateBody=json.dumps({
        "Resources": {},
        "Outputs": {"Shared": {"Value": "shared", "Export": {"Name": export_name}}},
    }))
    try:
        assert _wait_stack(cfn, producer)["StackStatus"] == "CREATE_COMPLETE"
        cfn.create_stack(StackName=mention, TemplateBody=json.dumps({
            "Metadata": {"Note": {"Fn::ImportValue": export_name}},
            "Resources": {"P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn/export-use/{uid}/mention", "Type": "String",
                "Value": f"the export is called {export_name}"}}},
        }))
        assert _wait_stack(cfn, mention)["StackStatus"] == "CREATE_COMPLETE"
        cfn.create_stack(StackName=importer, TemplateBody=json.dumps({
            "Parameters": {"Prefix": {"Type": "String"}},
            "Resources": {"P": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn/export-use/{uid}/import", "Type": "String",
                "Value": {"Fn::ImportValue": {"Fn::Sub": "${Prefix}-" + uid}}}}},
        }), Parameters=[{"ParameterKey": "Prefix", "ParameterValue": "cfn-export-use"}])
        stack = _wait_stack(cfn, importer)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        assert ssm.get_parameter(Name=f"/cfn/export-use/{uid}/import")["Parameter"]["Value"] == "shared"

        with pytest.raises(ClientError) as exc:
            cfn.delete_stack(StackName=producer)
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert f"Export {export_name} is imported by stack {importer}" in exc.value.response["Error"]["Message"]

        cfn.delete_stack(StackName=importer)
        _wait_stack(cfn, importer)
        cfn.delete_stack(StackName=producer)
        assert _wait_stack(cfn, producer)["StackStatus"] == "DELETE_COMPLETE"
    finally:
        for name in (importer, mention, producer):
            _delete_cfn_test_stack(cfn, name)


def test_cfn_update_stack_without_changes_is_refused(cfn, sqs):
    """UpdateStack with the template and parameters the stack already runs is
    refused with CloudFormation's ``No updates are to be performed.``, for an
    identical TemplateBody and for UsePreviousTemplate alike; a changed
    parameter value or a changed tag set is an update."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-no-updates-{uid}"
    template = json.dumps({
        "Parameters": {"Visibility": {"Type": "Number", "Default": 30}},
        "Resources": {"Queue": {"Type": "AWS::SQS::Queue", "Properties": {
            "QueueName": f"cfn-no-updates-{uid}",
            "VisibilityTimeout": {"Ref": "Visibility"}}}},
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template,
                     Tags=[{"Key": "stage", "Value": "one"}])
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        for kwargs in (
            {"TemplateBody": template},
            {"UsePreviousTemplate": True},
            {"TemplateBody": template, "Parameters": [
                {"ParameterKey": "Visibility", "ParameterValue": "30"}]},
            {"UsePreviousTemplate": True, "Tags": [{"Key": "stage", "Value": "one"}]},
        ):
            with pytest.raises(ClientError) as exc:
                cfn.update_stack(StackName=stack_name, **kwargs)
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert exc.value.response["Error"]["Message"] == "No updates are to be performed."
        events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        assert not [e for e in events if e["ResourceStatus"].startswith("UPDATE")]

        cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True,
                         Parameters=[{"ParameterKey": "Visibility", "ParameterValue": "45"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        queue_url = sqs.get_queue_url(QueueName=f"cfn-no-updates-{uid}")["QueueUrl"]
        assert sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]["VisibilityTimeout"] == "45"

        # A parameter left out of an update reverts to its template default,
        # so a tags-only update keeps the value with UsePreviousValue.
        cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True,
                         Parameters=[{"ParameterKey": "Visibility", "UsePreviousValue": True}],
                         Tags=[{"Key": "stage", "Value": "two"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert stack["Tags"] == [{"Key": "stage", "Value": "two"}]
        assert stack["Parameters"] == [
            {"ParameterKey": "Visibility", "ParameterValue": "45"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)



# ---------------------------------------------------------------------------
# Rules section
# ---------------------------------------------------------------------------

def _rules_template(rules, params):
    """A template with the given Rules and Parameters and one resource that
    provisions instantly."""
    return json.dumps({
        "Parameters": params,
        "Rules": rules,
        "Resources": {"Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"}},
    })


def _refused_by_rules(cfn, stack_name, body, parameters, expected):
    with pytest.raises(ClientError) as exc:
        cfn.create_stack(StackName=stack_name, TemplateBody=body, Parameters=parameters)
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert exc.value.response["Error"]["Message"] == expected
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=stack_name)


def test_cfn_rules_cdk_check_bootstrap_version(cfn, ssm):
    """The rule every CDK-synthesized template carries: `BootstrapVersion` is an
    `AWS::SSM::Parameter::Value<String>` and the assertion refuses the values
    1 to 5. The rule passes when the SSM parameter holds "30" and refuses the
    stack with the AssertDescription when it holds "5", on CreateStack,
    UpdateStack and CreateChangeSet (both types), and no stack record is left
    behind. The failure text is unmeasured."""
    uid = _uuid_mod.uuid4().hex[:8]
    ssm_name = f"/cdk-bootstrap/{uid}/version"
    description = ("CDK bootstrap stack version 6 required. Please run 'cdk bootstrap' "
                   "with a recent version of the CDK CLI.")
    template = json.dumps({
        "Parameters": {"BootstrapVersion": {
            "Type": "AWS::SSM::Parameter::Value<String>", "Default": ssm_name,
            "Description": "Version of the CDK Bootstrap resources in this environment, "
                           "automatically retrieved from SSM Parameter Store."}},
        "Rules": {"CheckBootstrapVersion": {"Assertions": [{
            "Assert": {"Fn::Not": [{"Fn::Contains": [
                ["1", "2", "3", "4", "5"], {"Ref": "BootstrapVersion"}]}]},
            "AssertDescription": description}]}},
        "Resources": {"Handle": {"Type": "AWS::CloudFormation::WaitConditionHandle"}},
    })
    expected = f"Template error: rule CheckBootstrapVersion failed: {description}"
    stack_name = f"cfn-rules-cdk-{uid}"

    ssm.put_parameter(Name=ssm_name, Value="5", Type="String")
    try:
        _refused_by_rules(cfn, stack_name, template, [], expected)
        with pytest.raises(ClientError) as exc:
            cfn.create_change_set(StackName=stack_name, ChangeSetName="cs",
                                  ChangeSetType="CREATE", TemplateBody=template)
        assert exc.value.response["Error"]["Message"] == expected
        with pytest.raises(ClientError):
            cfn.describe_stacks(StackName=stack_name)

        ssm.put_parameter(Name=ssm_name, Value="30", Type="String", Overwrite=True)
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        ssm.put_parameter(Name=ssm_name, Value="5", Type="String", Overwrite=True)
        with pytest.raises(ClientError) as exc:
            cfn.update_stack(StackName=stack_name, TemplateBody=template,
                             Tags=[{"Key": "stage", "Value": "two"}])
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == expected
        # UsePreviousTemplate re-resolves the SSM parameter and re-runs the rule.
        with pytest.raises(ClientError) as exc:
            cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True,
                             Parameters=[{"ParameterKey": "BootstrapVersion",
                                          "UsePreviousValue": True}],
                             Tags=[{"Key": "stage", "Value": "two"}])
        assert exc.value.response["Error"]["Message"] == expected
        with pytest.raises(ClientError) as exc:
            cfn.create_change_set(StackName=stack_name, ChangeSetName="cs",
                                  TemplateBody=template, Tags=[{"Key": "stage", "Value": "two"}])
        assert exc.value.response["Error"]["Message"] == expected
        assert cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"] \
            == "CREATE_COMPLETE"
        assert cfn.list_change_sets(StackName=stack_name)["Summaries"] == []

        ssm.put_parameter(Name=ssm_name, Value="31", Type="String", Overwrite=True)
        cfn.update_stack(StackName=stack_name, TemplateBody=template,
                         Tags=[{"Key": "stage", "Value": "two"}])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            ssm.delete_parameter(Name=ssm_name)
        except ClientError:
            pass


def test_cfn_rules_condition_and_list_functions(cfn):
    """A rule whose RuleCondition is false is skipped; Fn::Contains,
    Fn::EachMemberEquals and Fn::EachMemberIn see a CommaDelimitedList
    parameter as a list of trimmed members; Fn::And, Fn::Or, Fn::Equals and
    Fn::If nest; the pseudo parameters resolve."""
    uid = _uuid_mod.uuid4().hex[:8]
    params = {
        "Env": {"Type": "String", "Default": "test"},
        "Size": {"Type": "String", "Default": "t3.medium"},
        "Subnets": {"Type": "CommaDelimitedList", "Default": "a, b"},
        "Zones": {"Type": "CommaDelimitedList", "Default": "us-east-1a,us-east-1b"},
    }
    rules = {
        "ProdOnly": {  # RuleCondition false: the failing assertion is never checked
            "RuleCondition": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
            "Assertions": [{"Assert": {"Fn::Equals": [{"Ref": "Size"}, "t3.large"]},
                            "AssertDescription": "prod needs t3.large"}]},
        "TestSize": {
            "RuleCondition": {"Fn::Equals": [{"Ref": "Env"}, "test"]},
            "Assertions": [
                {"Assert": {"Fn::Contains": [["t3.medium", "t3.small"], {"Ref": "Size"}]},
                 "AssertDescription": "test needs t3.medium or t3.small"},
                {"Assert": {"Fn::EachMemberIn": [{"Ref": "Subnets"}, ["a", "b", "c"]]}},
                {"Assert": {"Fn::EachMemberEquals": [{"Ref": "Subnets"}, "a"]},
                 "AssertDescription": "all subnets must be a"}]},
        "Nested": {"Assertions": [{"Assert": {"Fn::And": [
            {"Fn::Or": [{"Fn::Equals": [{"Ref": "AWS::Region"}, "us-east-1"]},
                        {"Fn::Equals": [{"Ref": "AWS::AccountId"}, "000000000000"]}]},
            {"Fn::Not": [{"Fn::Equals": [{"Ref": "AWS::Partition"}, "aws-cn"]}]},
            {"Fn::Contains": [{"Ref": "Zones"},
                              {"Fn::If": [{"Fn::Equals": [{"Ref": "Env"}, "test"]},
                                          "us-east-1b", "us-east-1z"]}]},
        ]}}]},
    }
    body = _rules_template(rules, params)
    name = f"cfn-rules-fn-{uid}"

    _refused_by_rules(cfn, name, body, [{"ParameterKey": "Subnets", "ParameterValue": "a,b,z"}],
                      "Template error: rule TestSize failed: assertion 2 evaluated to false")
    _refused_by_rules(cfn, name, body, [{"ParameterKey": "Subnets", "ParameterValue": "a,b"},
                                        {"ParameterKey": "Size", "ParameterValue": "t3.large"}],
                      "Template error: rule TestSize failed: test needs t3.medium or t3.small")
    _refused_by_rules(cfn, name, body, [{"ParameterKey": "Env", "ParameterValue": "prod"}],
                      "Template error: rule ProdOnly failed: prod needs t3.large")

    cfn.create_stack(StackName=name, TemplateBody=body,
                     Parameters=[{"ParameterKey": "Subnets", "ParameterValue": "a, a"}])
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        _delete_cfn_test_stack(cfn, name)


def test_cfn_rules_shape_and_function_errors(cfn):
    """An assertion that is not a boolean, a RuleCondition that is not a
    boolean, a Ref to something that is not a parameter, a function outside
    the rule set (measured for ``[Fn::If]`` in cloudformation-coverage-roadmap
    issue 921; the bracket lists the offending names), a rule without
    Assertions and a malformed argument list are refused before a stack
    exists with a ValidationError, never an internal error; the function
    and shape checks also run on ValidateTemplate."""
    uid = _uuid_mod.uuid4().hex[:8]
    params = {"P": {"Type": "String", "Default": "x"}}
    name = f"cfn-rules-shape-{uid}"

    _refused_by_rules(
        cfn, name, _rules_template({"R": {"Assertions": [{"Assert": {"Ref": "P"}}]}}, params),
        [], "Template error: assertion 1 of rule R must evaluate to true or false, got 'x'")
    _refused_by_rules(
        cfn, name, _rules_template(
            {"R": {"Assertions": [{"Assert": {"Fn::Equals": [{"Ref": "Handle"}, "x"]}}]}},
            params),
        [], "Template format error: Unresolved resource dependencies [Handle] in the Rules "
            "block of the template")
    unsupported = _rules_template(
        {"R": {"Assertions": [{"Assert": {"Fn::Equals": [{"Fn::Sub": "${P}"}, "x"]}}]}}, params)
    expected = ("Template format error: Following functions are not supported in the Rules "
                "block of the template: [Fn::Sub]")
    _refused_by_rules(cfn, name, unsupported, [], expected)
    with pytest.raises(ClientError) as exc:
        cfn.validate_template(TemplateBody=unsupported)
    assert exc.value.response["Error"]["Message"] == expected
    _refused_by_rules(cfn, name, _rules_template({"R": {"RuleCondition": True}}, params), [],
                      "Template format error: Rule R must contain an Assertions list")
    _refused_by_rules(
        cfn, name, _rules_template({"R": {"RuleCondition": {"Ref": "P"},
                                          "Assertions": [{"Assert": True}]}}, params),
        [], "Template error: the RuleCondition of rule R must evaluate to true or false, got 'x'")
    for assertion, expected in (
        ({"Fn::Equals": "x"},
         "Template format error: Fn::Equals in rule R must be a list of 2 elements"),
        ({"Fn::Equals": [{"Ref": "P"}]},
         "Template format error: Fn::Equals in rule R must be a list of 2 elements"),
        ({"Ref": ["P"]}, "Template format error: Ref in rule R must name a parameter"),
        ({"Fn::ValueOf": "P"},
         "Template format error: Fn::ValueOf in rule R must be a list of 2 elements"),
        ({"Fn::ValueOf": [{"Ref": "P"}, "VpcId"]},
         "Template format error: Fn::ValueOf in rule R takes two strings; no function "
         "can be used within it"),
        ({"Fn::And": None},
         "Template format error: Fn::And in rule R must be a list of conditions"),
        ({"Fn::Not": [{"Fn::Equals": [{"Ref": "P"}, "x"]}, True]},
         "Template format error: Fn::Not in rule R must be a list of 1 elements"),
    ):
        body = _rules_template({"R": {"Assertions": [{"Assert": assertion}]}}, params)
        _refused_by_rules(cfn, name, body, [], expected)
        with pytest.raises(ClientError) as exc:
            cfn.validate_template(TemplateBody=body)
        assert exc.value.response["Error"]["Message"] == expected
    # A template whose rules pass validates; ValidateTemplate has no values to
    # evaluate against, so a rule that would fail on CreateStack passes there.
    failing = _rules_template(
        {"R": {"Assertions": [{"Assert": {"Fn::Equals": [{"Ref": "P"}, "y"]},
                               "AssertDescription": "P must be y"}]}}, params)
    cfn.validate_template(TemplateBody=failing)
    _refused_by_rules(cfn, name, failing, [], "Template error: rule R failed: P must be y")


def test_cfn_rules_account_lookups(cfn, ec2, ssm):
    """Fn::RefAll, Fn::ValueOf and Fn::ValueOfAll are served from the EC2 store
    for the VPC, subnet and security-group parameter types, with every
    attribute the reference lists, also behind an SSM ``Value<List<...>>``
    parameter type; a rule over any other type is skipped (logged), not
    failed."""
    uid = _uuid_mod.uuid4().hex[:8]
    vpc_id = ec2.create_vpc(CidrBlock="10.77.0.0/16", TagSpecifications=[{
        "ResourceType": "vpc", "Tags": [{"Key": "Department", "Value": "IT"}]}])["Vpc"]["VpcId"]
    subnet_ids, other_vpc, other_subnet = [], None, None
    try:
        subnet_ids = [
            ec2.create_subnet(VpcId=vpc_id, CidrBlock=f"10.77.{i}.0/24",
                              AvailabilityZone="us-east-1a")["Subnet"]["SubnetId"]
            for i in (1, 2)]
        other_vpc = ec2.create_vpc(CidrBlock="10.78.0.0/16")["Vpc"]["VpcId"]
        other_subnet = ec2.create_subnet(
            VpcId=other_vpc, CidrBlock="10.78.1.0/24")["Subnet"]["SubnetId"]
        vpc = {
            "DefaultSecurityGroupId": ec2.describe_security_groups(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": ["default"]}])["SecurityGroups"][0]["GroupId"],
            "DefaultNetworkAclId": ec2.describe_network_acls(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]}])["NetworkAcls"][0]["NetworkAclId"],
        }
        ssm_name = f"/cfn-rules/{uid}/subnets"
        ssm.put_parameter(Name=ssm_name, Value=",".join(subnet_ids), Type="StringList")
        _rules_account_lookups(cfn, uid, vpc_id, subnet_ids, other_vpc, other_subnet,
                               vpc, ssm_name)
    finally:
        for sid in subnet_ids + ([other_subnet] if other_subnet else []):
            ec2.delete_subnet(SubnetId=sid)
        ec2.delete_vpc(VpcId=vpc_id)
        if other_vpc:
            ec2.delete_vpc(VpcId=other_vpc)
        try:
            ssm.delete_parameter(Name=f"/cfn-rules/{uid}/subnets")
        except ClientError:
            pass


def _rules_account_lookups(cfn, uid, vpc_id, subnet_ids, other_vpc, other_subnet, vpc, ssm_name):
    params = {
        "VpcId": {"Type": "AWS::EC2::VPC::Id"},
        "Subnets": {"Type": "List<AWS::EC2::Subnet::Id>"},
        "SsmSubnets": {"Type": "AWS::SSM::Parameter::Value<List<AWS::EC2::Subnet::Id>>",
                       "Default": ssm_name},
        "KeyName": {"Type": "AWS::EC2::KeyPair::KeyName", "Default": "any"},
    }
    rules = {
        "VpcExists": {"Assertions": [{
            "Assert": {"Fn::Contains": [{"Fn::RefAll": "AWS::EC2::VPC::Id"}, {"Ref": "VpcId"}]},
            "AssertDescription": "The VPC must exist"}]},
        "Department": {"Assertions": [{
            "Assert": {"Fn::Equals": [{"Fn::ValueOf": ["VpcId", "Tags.Department"]}, "IT"]},
            "AssertDescription": "The VPC must belong to IT"}]},
        "SubnetsInVpc": {"Assertions": [{
            "Assert": {"Fn::EachMemberEquals": [{"Fn::ValueOf": ["Subnets", "VpcId"]},
                                                {"Ref": "VpcId"}]},
            "AssertDescription": "All subnets must be in the VPC"}]},
        "VpcAttributes": {"Assertions": [
            {"Assert": {"Fn::Equals": [{"Fn::ValueOf": ["VpcId", "DefaultSecurityGroup"]},
                                       vpc["DefaultSecurityGroupId"]]}},
            {"Assert": {"Fn::Equals": [{"Fn::ValueOf": ["VpcId", "DefaultNetworkAcl"]},
                                       vpc["DefaultNetworkAclId"]]}},
            {"Assert": {"Fn::EachMemberEquals": [{"Fn::ValueOf": ["Subnets", "AvailabilityZone"]},
                                                 "us-east-1a"]},
             "AssertDescription": "All subnets must be in us-east-1a"}]},
        "SubnetsExist": {"Assertions": [{
            "Assert": {"Fn::EachMemberIn": [{"Ref": "Subnets"},
                                            {"Fn::RefAll": "AWS::EC2::Subnet::Id"}]}}]},
        "SsmSubnetsInVpc": {"Assertions": [{
            "Assert": {"Fn::EachMemberEquals": [{"Fn::ValueOf": ["SsmSubnets", "VpcId"]},
                                                {"Ref": "VpcId"}]},
            "AssertDescription": "All SSM subnets must be in the VPC"}]},
        "SomeVpcIsIT": {"Assertions": [{
            "Assert": {"Fn::Contains": [{"Fn::ValueOfAll": ["AWS::EC2::VPC::Id", "Tags.Department"]},
                                        "IT"]}}]},
        "Skipped": {"Assertions": [{  # key pairs are not listed: skipped, never failed
            "Assert": {"Fn::Contains": [{"Fn::RefAll": "AWS::EC2::KeyPair::KeyName"},
                                        {"Ref": "KeyName"}]}}]},
    }
    body = _rules_template(rules, params)
    name = f"cfn-rules-account-{uid}"

    def parameters(vpc, subnets):
        return [{"ParameterKey": "VpcId", "ParameterValue": vpc},
                {"ParameterKey": "Subnets", "ParameterValue": ",".join(subnets)}]

    _refused_by_rules(cfn, name, body, parameters("vpc-0000000000000dead", subnet_ids),
                      "Template error: rule VpcExists failed: The VPC must exist")
    _refused_by_rules(cfn, name, body, parameters(vpc_id, subnet_ids + [other_subnet]),
                      "Template error: rule SubnetsInVpc failed: All subnets must be in the VPC")
    _refused_by_rules(cfn, name, body, parameters(other_vpc, [other_subnet]),
                      "Template error: rule Department failed: The VPC must belong to IT")

    cfn.create_stack(StackName=name, TemplateBody=body, Parameters=parameters(vpc_id, subnet_ids))
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        _delete_cfn_test_stack(cfn, name)


# ---------------------------------------------------------------------------
# Template and stack quotas
# ---------------------------------------------------------------------------

_HANDLE = {"Type": "AWS::CloudFormation::WaitConditionHandle"}


def _refused_quota(cfn, stack_name, body, expected, parameters=None):
    with pytest.raises(ClientError) as exc:
        cfn.create_stack(StackName=stack_name, TemplateBody=body, Parameters=parameters or [])
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    message = exc.value.response["Error"]["Message"]
    if callable(expected):
        assert expected(message), message
    else:
        assert message == expected
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=stack_name)


def test_cfn_template_body_size_quotas(cfn, s3):
    """A TemplateBody of 51,200 bytes is accepted and one of 51,201 refused by
    the request-level constraint (quoted: ``at 'templateBody' failed to
    satisfy constraint: Member must have length less than or equal to
    51200``), on CreateStack, UpdateStack, ValidateTemplate and
    GetTemplateSummary; a template behind TemplateURL may not exceed
    1,000,000 bytes (quoted: ``Template may not exceed 1000000 bytes in
    size.``). ValidateTemplate reads its template through the same path, so it
    takes a TemplateURL now."""
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"cfn-body-size-{uid}"

    def body_of(size):
        skeleton = json.dumps({"Metadata": {"Pad": ""}, "Resources": {"Handle": _HANDLE}})
        return json.dumps({"Metadata": {"Pad": "x" * (size - len(skeleton))},
                           "Resources": {"Handle": _HANDLE}})

    assert len(body_of(51200).encode()) == 51200
    too_big = body_of(51201)

    def refused(message):
        return (message.startswith("1 validation error detected: Value '")
                and message.endswith("' at 'templateBody' failed to satisfy constraint: "
                                     "Member must have length less than or equal to 51200"))

    _refused_quota(cfn, name, too_big, refused)
    with pytest.raises(ClientError) as exc:
        cfn.validate_template(TemplateBody=too_big)
    assert refused(exc.value.response["Error"]["Message"])
    with pytest.raises(ClientError) as exc:
        cfn.get_template_summary(TemplateBody=too_big)
    assert refused(exc.value.response["Error"]["Message"])

    cfn.create_stack(StackName=name, TemplateBody=body_of(51200))
    try:
        stack = _wait_stack(cfn, name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        with pytest.raises(ClientError) as exc:
            cfn.update_stack(StackName=name, TemplateBody=too_big)
        assert refused(exc.value.response["Error"]["Message"])
    finally:
        _delete_cfn_test_stack(cfn, name)

    bucket = f"cfn-body-size-{uid}"
    s3.create_bucket(Bucket=bucket)
    try:
        endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
        s3.put_object(Bucket=bucket, Key="big.json", Body=body_of(1000001).encode())
        s3.put_object(Bucket=bucket, Key="ok.json", Body=body_of(1000000).encode())
        s3.put_object(Bucket=bucket, Key="validate.json", Body=json.dumps({
            "Description": "a template behind a URL",
            "Parameters": {"P": {"Type": "String", "Default": "d"}},
            "Resources": {"Handle": _HANDLE}}).encode())
        # ValidateTemplate took TemplateBody only; it reads the same path now.
        validated = cfn.validate_template(TemplateURL=f"{endpoint}/{bucket}/validate.json")
        assert validated["Description"] == "a template behind a URL"
        assert [p["ParameterKey"] for p in validated["Parameters"]] == ["P"]
        with pytest.raises(ClientError) as exc:
            cfn.create_stack(StackName=name, TemplateURL=f"{endpoint}/{bucket}/big.json")
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == \
            "Template may not exceed 1000000 bytes in size."
        with pytest.raises(ClientError):
            cfn.describe_stacks(StackName=name)
        cfn.create_stack(StackName=name, TemplateURL=f"{endpoint}/{bucket}/ok.json")
        try:
            stack = _wait_stack(cfn, name)
            assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        finally:
            _delete_cfn_test_stack(cfn, name)
    finally:
        for key in ("big.json", "ok.json", "validate.json"):
            s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)


def test_cfn_template_section_quotas(cfn):
    """501 resources (quoted: ``Template format error: Number of resources,
    536, is greater than maximum allowed, 500``), 201 parameters, outputs or
    mappings, 201 attributes in a mapping, a 256-character name of any
    section, a 256-character mapping attribute name, a 1,025-byte
    description, a parameter default over 4,096 bytes (quoted shape) and a
    provided value over 4,096 bytes are refused before a stack exists; the
    maximum of each passes the check in process."""
    from ministack.services.cloudformation.engine import validate_template_support

    uid = _uuid_mod.uuid4().hex[:8]
    name = f"cfn-quotas-{uid}"

    def template(**sections):
        return {"Resources": {"Handle": _HANDLE}, **sections}

    resources = {f"H{i}": _HANDLE for i in range(501)}
    _refused_quota(cfn, name, json.dumps({"Resources": resources}),
                   "Template format error: Number of resources, 501, is greater than "
                   "maximum allowed, 500")
    params = {f"P{i}": {"Type": "String", "Default": "x"} for i in range(201)}
    _refused_quota(cfn, name, json.dumps(template(Parameters=params)),
                   "Template format error: Number of parameters, 201, is greater than "
                   "maximum allowed, 200")
    outputs = {f"O{i}": {"Value": "x"} for i in range(201)}
    _refused_quota(cfn, name, json.dumps(template(Outputs=outputs)),
                   "Template format error: Number of outputs, 201, is greater than "
                   "maximum allowed, 200")
    mappings = {f"M{i}": {"a": {"b": "c"}} for i in range(201)}
    _refused_quota(cfn, name, json.dumps(template(Mappings=mappings)),
                   "Template format error: Number of mappings, 201, is greater than "
                   "maximum allowed, 200")
    wide = {"M": {f"a{i}": {"b": "c"} for i in range(201)}}
    _refused_quota(cfn, name, json.dumps(template(Mappings=wide)),
                   "Template format error: Number of attributes in mapping M, 201, is "
                   "greater than maximum allowed, 200")
    long_name = "R" * 256
    _refused_quota(cfn, name, json.dumps({"Resources": {long_name: _HANDLE}}),
                   f"Template format error: Resource name {'R' * 32}... may not exceed "
                   "255 characters")
    _refused_quota(cfn, name, json.dumps(template(Description="d" * 1025)),
                   "Template format error: Template description may not exceed 1024 "
                   "bytes in size")
    big_default = "v" * 4097
    _refused_quota(cfn, name, json.dumps(template(
        Parameters={"P": {"Type": "String", "Default": big_default}})),
        f"Template format error: Parameter 'P' default value '{big_default}' length is "
        "greater than 4096.")
    _refused_quota(cfn, name, json.dumps(template(Parameters={"P": {"Type": "String"}})),
                   f"1 validation error detected: Value '{big_default}' at "
                   "'parameters.1.member.parameterValue' failed to satisfy constraint: "
                   "Member must have length less than or equal to 4096",
                   parameters=[{"ParameterKey": "P", "ParameterValue": big_default}])

    # The name-length branch of the other sections and the attribute name of
    # a mapping, in process: they refuse before a stack record exists too, but
    # the message is what this checks.
    for singular, section in (
        ("Parameter", {"Parameters": {"N" * 256: {"Type": "String"}}}),
        ("Output", {"Outputs": {"N" * 256: {"Value": "x"}}}),
        ("Mapping", {"Mappings": {"N" * 256: {"a": {"b": "c"}}}}),
    ):
        with pytest.raises(ValueError) as err:
            validate_template_support(template(**section), {})
        assert str(err.value) == (
            f"Template format error: {singular} name {'N' * 32}... may not exceed "
            "255 characters")
    with pytest.raises(ValueError) as err:
        validate_template_support(template(Mappings={"M": {"a" * 256: {"b": "c"}}}), {})
    assert str(err.value) == (
        f"Template format error: Mapping attribute name {'a' * 32}... of mapping M "
        "may not exceed 255 characters")

    # The maximum of each quota passes (checked in process: 500 handles would
    # deploy, but the check is what this test is about).
    validate_template_support({
        "Description": "d" * 1024,
        "Parameters": {f"P{i}": {"Type": "String", "Default": "v" * 4096} for i in range(200)},
        "Mappings": {f"M{i}": {f"a{j}": {"b": "c"} for j in range(200)} for i in range(200)},
        "Resources": {"R" * 252 + f"{i:03d}": _HANDLE for i in range(500)},
        "Outputs": {f"O{i}": {"Value": "x"} for i in range(200)},
    }, {})


def test_cfn_stack_name_quotas(cfn):
    """CreateStack and a CREATE change set refuse a stack name that is not
    alphanumeric-and-hyphens starting with a letter (quoted: ``at
    'stackName' failed to satisfy constraint: Member must satisfy regular
    expression pattern``) or longer than 128 characters, before a stack
    exists; a 128-character name is accepted."""
    uid = _uuid_mod.uuid4().hex[:8]
    body = json.dumps({"Resources": {"Handle": _HANDLE}})
    pattern = ("failed to satisfy constraint: Member must satisfy regular expression "
               "pattern: [a-zA-Z][-a-zA-Z0-9]*")
    for bad in (f"cfn_name_{uid}", f"1cfn-name-{uid}", f"cfn.name.{uid}"):
        _refused_quota(cfn, bad, body,
                       f"1 validation error detected: Value '{bad}' at 'stackName' {pattern}")
    with pytest.raises(ClientError) as exc:
        cfn.create_change_set(StackName=f"cfn_cs_{uid}", ChangeSetName="cs",
                              ChangeSetType="CREATE", TemplateBody=body)
    assert exc.value.response["Error"]["Message"] == \
        f"1 validation error detected: Value 'cfn_cs_{uid}' at 'stackName' {pattern}"
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=f"cfn_cs_{uid}")

    long = f"cfn-long-{uid}-" + "x" * (129 - len(f"cfn-long-{uid}-"))
    assert len(long) == 129
    _refused_quota(cfn, long, body,
                   f"1 validation error detected: Value '{long}' at 'stackName' failed to "
                   "satisfy constraint: Member must have length less than or equal to 128")
    both = f"cfn_long_{uid}_" + "x" * (129 - len(f"cfn_long_{uid}_"))
    _refused_quota(cfn, both, body,
                   f"2 validation errors detected: Value '{both}' at 'stackName' failed to "
                   "satisfy constraint: Member must have length less than or equal to 128; "
                   f"Value '{both}' at 'stackName' {pattern}")

    # Every request-level violation is reported in one message: a bad name
    # and an oversized body together, on CreateStack and on a CREATE change
    # set alike (the name problem first, order unmeasured).
    big = json.dumps({"Metadata": {"Pad": "x" * 51200}, "Resources": {"Handle": _HANDLE}})
    joined = (f"2 validation errors detected: Value 'cfn_both_{uid}' at 'stackName' {pattern}; "
              f"Value '{big}' at 'templateBody' failed to satisfy constraint: Member must have "
              "length less than or equal to 51200")
    _refused_quota(cfn, f"cfn_both_{uid}", big, joined)
    with pytest.raises(ClientError) as exc:
        cfn.create_change_set(StackName=f"cfn_both_{uid}", ChangeSetName="cs",
                              ChangeSetType="CREATE", TemplateBody=big)
    assert exc.value.response["Error"]["Message"] == joined
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName=f"cfn_both_{uid}")

    ok = long[:128]
    cfn.create_stack(StackName=ok, TemplateBody=body)
    try:
        stack = _wait_stack(cfn, ok)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    finally:
        _delete_cfn_test_stack(cfn, ok)


# ---------------------------------------------------------------------------
# AWS::Include transform
# ---------------------------------------------------------------------------

def test_cfn_include_transform(cfn, s3, ssm):
    """An embedded ``Fn::Transform`` naming ``AWS::Include`` is replaced by the
    S3 object it points to (JSON or plain YAML, a key-value object): next to
    a sibling resource inside Resources, as a whole Properties map, inside a
    Properties map next to other properties, inside Outputs and inside
    Mappings (the reference names the mappings section as an example of where
    the transform may sit). A location
    that is not an ``s3://`` URI and a snippet with a YAML shorthand tag are
    refused with the sentences a real account answers, bare (no ``Template
    format error:`` prefix); a missing object, a snippet that is not an
    object and a nested include are refused too, all before a stack exists.
    On UpdateStack and CreateChangeSet the include is expanded the same way
    and the change set lists the included resource; with UsePreviousTemplate
    the stored processed template is used, so a snippet edited in S3 since
    the deploy is not picked up."""
    uid = _uuid_mod.uuid4().hex[:8]
    bucket = f"cfn-include-{uid}"
    name = f"cfn-include-{uid}"
    s3.create_bucket(Bucket=bucket)
    try:
        _include_transform(cfn, s3, ssm, uid, bucket, name)
    finally:
        _delete_cfn_test_stack(cfn, name)
        for key in ("resources.yaml", "properties.json", "value.json", "list.json",
                    "nested.json", "shorthand.yaml", "outputs.json", "second.json",
                    "mappings.json"):
            s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)


def _include_transform(cfn, s3, ssm, uid, bucket, name):
    s3.put_object(Bucket=bucket, Key="resources.yaml", Body=(
        "Included:\n"
        "  Type: AWS::SSM::Parameter\n"
        "  Properties:\n"
        f"    Name: /cfn-include/{uid}/from-yaml\n"
        "    Type: String\n"
        "    Value: {Ref: Handle}\n").encode())
    s3.put_object(Bucket=bucket, Key="shorthand.yaml", Body=(
        "Included:\n"
        "  Type: AWS::SSM::Parameter\n"
        "  Properties:\n"
        f"    Name: /cfn-include/{uid}/shorthand\n"
        "    Type: String\n"
        "    Value: !Ref Handle\n").encode())
    s3.put_object(Bucket=bucket, Key="properties.json", Body=json.dumps({
        "Name": f"/cfn-include/{uid}/whole", "Type": "String", "Value": "whole"}).encode())
    s3.put_object(Bucket=bucket, Key="value.json", Body=json.dumps({
        "Value": "merged", "Type": "String"}).encode())
    s3.put_object(Bucket=bucket, Key="outputs.json", Body=json.dumps({
        "IncludedOutput": {"Value": {"Ref": "Handle"}}}).encode())
    s3.put_object(Bucket=bucket, Key="second.json", Body=json.dumps({
        "Second": {"Type": "AWS::SSM::Parameter", "Properties": {
            "Name": f"/cfn-include/{uid}/second", "Type": "String", "Value": "second"}}}).encode())
    s3.put_object(Bucket=bucket, Key="mappings.json", Body=json.dumps({
        "Sizes": {"small": {"Value": "from-map"}}}).encode())
    s3.put_object(Bucket=bucket, Key="list.json", Body=b"[1, 2]")
    s3.put_object(Bucket=bucket, Key="nested.json", Body=json.dumps({
        "Fn::Transform": {"Name": "AWS::Include",
                          "Parameters": {"Location": f"s3://{bucket}/value.json"}}}).encode())

    def include(key):
        return {"Fn::Transform": {"Name": "AWS::Include",
                                  "Parameters": {"Location": f"s3://{bucket}/{key}"}}}

    template = {
        "Resources": {
            "Handle": _HANDLE,
            "Whole": {"Type": "AWS::SSM::Parameter", "Properties": include("properties.json")},
            "Merged": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-include/{uid}/merged", **include("value.json")}},
            "FromMap": {"Type": "AWS::SSM::Parameter", "Properties": {
                "Name": f"/cfn-include/{uid}/from-map", "Type": "String",
                "Value": {"Fn::FindInMap": ["Sizes", "small", "Value"]}}},
            **include("resources.yaml"),
        },
        "Mappings": include("mappings.json"),
        "Outputs": include("outputs.json"),
    }

    def refused(body, expected):
        with pytest.raises(ClientError) as exc:
            cfn.create_stack(StackName=name, TemplateBody=json.dumps(body))
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == expected, \
            exc.value.response["Error"]["Message"]
        with pytest.raises(ClientError):
            cfn.describe_stacks(StackName=name)

    def with_include(key_or_location):
        location = key_or_location if "://" in key_or_location \
            else f"s3://{bucket}/{key_or_location}"
        return {"Resources": {"Handle": _HANDLE, "Fn::Transform": {
            "Name": "AWS::Include", "Parameters": {"Location": location}}}}

    refused(with_include(f"https://{bucket}.s3.amazonaws.com/resources.yaml"),
            "Transform AWS::Include failed with: The location parameter is not a valid S3 uri.")
    refused(with_include("shorthand.yaml"),
            "Transform AWS::Include failed with: The specified S3 object's content should be "
            "valid Yaml/JSON")
    refused(with_include("missing.yaml"),
            f"Transform AWS::Include failed with: The S3 object s3://{bucket}/missing.yaml "
            "does not exist.")
    refused(with_include("list.json"),
            f"Transform AWS::Include failed with: The snippet at s3://{bucket}/list.json must "
            "be a key-value object.")
    refused(with_include("nested.json"),
            f"Transform AWS::Include failed with: The snippet at s3://{bucket}/nested.json uses "
            "AWS::Include, which cannot be nested.")

    cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
    logical_ids = sorted(r["LogicalResourceId"] for r in
                         cfn.describe_stack_resources(StackName=name)["StackResources"])
    assert logical_ids == ["FromMap", "Handle", "Included", "Merged", "Whole"]
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/whole")["Parameter"]["Value"] == "whole"
    # The Mappings section is expanded too, so Fn::FindInMap reads the snippet.
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/from-map")["Parameter"]["Value"] \
        == "from-map"
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/merged")["Parameter"]["Value"] \
        == "merged"
    handle_url = ssm.get_parameter(Name=f"/cfn-include/{uid}/from-yaml")["Parameter"]["Value"]
    assert handle_url.startswith("http")
    assert _output(stack, "IncludedOutput") == handle_url
    # The stored template is the one the caller sent: the include is not
    # baked into it.
    stored = cfn.get_template(StackName=name)["TemplateBody"]
    assert "Fn::Transform" in stored["Resources"] and "Included" not in stored["Resources"]

    # A snippet edited in S3 after the deploy is not picked up by an update
    # that reuses the template: nothing changed from the stack's point of view.
    s3.put_object(Bucket=bucket, Key="value.json", Body=json.dumps({
        "Value": "edited", "Type": "String"}).encode())
    with pytest.raises(ClientError) as exc:
        cfn.update_stack(StackName=name, UsePreviousTemplate=True)
    assert exc.value.response["Error"]["Message"] == "No updates are to be performed."
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/merged")["Parameter"]["Value"] \
        == "merged"

    # A change set and an update with a new body expand the include again:
    # the change set lists the included resource, the update provisions it.
    second = json.loads(json.dumps(template))
    second["Resources"].update(include("second.json"))
    cfn.create_change_set(StackName=name, ChangeSetName="second",
                          TemplateBody=json.dumps(second))
    for _ in range(40):
        described = cfn.describe_change_set(StackName=name, ChangeSetName="second")
        if described["Status"] in ("CREATE_COMPLETE", "FAILED"):
            break
        time.sleep(0.25)
    assert described["Status"] == "CREATE_COMPLETE", described.get("StatusReason")
    added = sorted(c["ResourceChange"]["LogicalResourceId"] for c in described["Changes"]
                   if c["ResourceChange"]["Action"] == "Add")
    assert added == ["Second"]
    cfn.delete_change_set(StackName=name, ChangeSetName="second")
    cfn.update_stack(StackName=name, TemplateBody=json.dumps(second))
    stack = _wait_stack(cfn, name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/second")["Parameter"]["Value"] \
        == "second"
    assert ssm.get_parameter(Name=f"/cfn-include/{uid}/merged")["Parameter"]["Value"] \
        == "edited"
