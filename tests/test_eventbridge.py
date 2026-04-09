import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def test_eventbridge_bus_rule(eb):
    eb.create_event_bus(Name="test-bus")
    eb.put_rule(
        Name="test-rule",
        EventBusName="test-bus",
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    rules = eb.list_rules(EventBusName="test-bus")
    assert any(r["Name"] == "test-rule" for r in rules["Rules"])

def test_eventbridge_put_events(eb):
    resp = eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "UserSignup",
                "Detail": json.dumps({"userId": "123"}),
                "EventBusName": "default",
            },
            {
                "Source": "myapp",
                "DetailType": "OrderPlaced",
                "Detail": json.dumps({"orderId": "456"}),
                "EventBusName": "default",
            },
        ]
    )
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 2

def test_eventbridge_targets(eb):
    eb.put_rule(Name="target-rule", ScheduleExpression="rate(1 minute)", State="ENABLED")
    eb.put_targets(
        Rule="target-rule",
        Targets=[
            {
                "Id": "1",
                "Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-func",
            },
        ],
    )
    resp = eb.list_targets_by_rule(Rule="target-rule")
    assert len(resp["Targets"]) == 1

def test_eventbridge_permission(eb):
    eb.create_event_bus(Name="perm-bus")
    eb.put_permission(
        EventBusName="perm-bus",
        Action="events:PutEvents",
        Principal="123456789012",
        StatementId="AllowAcct",
    )
    eb.remove_permission(EventBusName="perm-bus", StatementId="AllowAcct")

def test_eventbridge_connection(eb):
    resp = eb.create_connection(
        Name="test-conn",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "x-api-key", "ApiKeyValue": "secret"}},
    )
    assert "ConnectionArn" in resp
    desc = eb.describe_connection(Name="test-conn")
    assert desc["Name"] == "test-conn"
    eb.delete_connection(Name="test-conn")

def test_eventbridge_api_destination(eb):
    eb.create_connection(
        Name="apid-conn",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "k", "ApiKeyValue": "v"}},
    )
    resp = eb.create_api_destination(
        Name="test-apid",
        ConnectionArn="arn:aws:events:us-east-1:000000000000:connection/apid-conn",
        InvocationEndpoint="https://example.com/webhook",
        HttpMethod="POST",
    )
    assert "ApiDestinationArn" in resp
    desc = eb.describe_api_destination(Name="test-apid")
    assert desc["Name"] == "test-apid"
    eb.delete_api_destination(Name="test-apid")

def test_eventbridge_lambda_target(eb, lam):
    """PutEvents dispatches to a Lambda target when the rule matches."""
    import uuid as _uuid

    fname = f"intg-eb-fn-{_uuid.uuid4().hex[:8]}"
    bus_name = f"intg-eb-bus-{_uuid.uuid4().hex[:8]}"
    rule_name = f"intg-eb-rule-{_uuid.uuid4().hex[:8]}"

    code = b"events = []\ndef handler(event, context):\n    events.append(event)\n    return {'processed': True}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    fn_arn = lam.get_function(FunctionName=fname)["Configuration"]["FunctionArn"]

    eb.create_event_bus(Name=bus_name)
    eb.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.test"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[{"Id": "lambda-target", "Arn": fn_arn}],
    )

    resp = eb.put_events(
        Entries=[
            {
                "Source": "myapp.test",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": bus_name,
            }
        ]
    )
    assert resp["FailedEntryCount"] == 0

    # Cleanup
    eb.remove_targets(Rule=rule_name, EventBusName=bus_name, Ids=["lambda-target"])
    eb.delete_rule(Name=rule_name, EventBusName=bus_name)
    eb.delete_event_bus(Name=bus_name)
    lam.delete_function(FunctionName=fname)

# Migrated from test_eb.py
def test_eventbridge_create_event_bus_v2(eb):
    resp = eb.create_event_bus(Name="eb-bus-v2")
    assert "eb-bus-v2" in resp["EventBusArn"]
    buses = eb.list_event_buses()
    assert any(b["Name"] == "eb-bus-v2" for b in buses["EventBuses"])

    desc = eb.describe_event_bus(Name="eb-bus-v2")
    assert desc["Name"] == "eb-bus-v2"

    resp = eb.update_event_bus(Name="eb-bus-v2", Description="updated description")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    updated = eb.describe_event_bus(Name="eb-bus-v2")
    assert updated["Description"] == "updated description"

def test_eventbridge_put_rule_v2(eb):
    eb.create_event_bus(Name="eb-rule-bus")
    resp = eb.put_rule(
        Name="eb-rule-v2",
        EventBusName="eb-rule-bus",
        EventPattern=json.dumps({"source": ["my.app"]}),
        State="ENABLED",
    )
    assert "RuleArn" in resp

    rules = eb.list_rules(EventBusName="eb-rule-bus")
    assert any(r["Name"] == "eb-rule-v2" for r in rules["Rules"])

    described = eb.describe_rule(Name="eb-rule-v2", EventBusName="eb-rule-bus")
    assert described["Name"] == "eb-rule-v2"
    assert described["State"] == "ENABLED"

def test_eventbridge_put_targets_v2(eb):
    eb.put_rule(Name="eb-tgt-v2", ScheduleExpression="rate(10 minutes)", State="ENABLED")
    eb.put_targets(
        Rule="eb-tgt-v2",
        Targets=[
            {"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:f1"},
            {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:000000000000:q1"},
        ],
    )
    resp = eb.list_targets_by_rule(Rule="eb-tgt-v2")
    assert len(resp["Targets"]) == 2
    ids = {t["Id"] for t in resp["Targets"]}
    assert ids == {"t1", "t2"}

def test_eventbridge_list_targets_v2(eb):
    eb.put_rule(Name="eb-lt-v2", ScheduleExpression="rate(1 hour)", State="ENABLED")
    eb.put_targets(
        Rule="eb-lt-v2",
        Targets=[
            {"Id": "a", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:fa"},
        ],
    )
    resp = eb.list_targets_by_rule(Rule="eb-lt-v2")
    assert resp["Targets"][0]["Id"] == "a"
    assert "fa" in resp["Targets"][0]["Arn"]

def test_eventbridge_put_events_v2(eb):
    resp = eb.put_events(
        Entries=[
            {
                "Source": "app.v2",
                "DetailType": "Ev1",
                "Detail": json.dumps({"a": 1}),
                "EventBusName": "default",
            },
            {
                "Source": "app.v2",
                "DetailType": "Ev2",
                "Detail": json.dumps({"b": 2}),
                "EventBusName": "default",
            },
            {
                "Source": "app.v2",
                "DetailType": "Ev3",
                "Detail": json.dumps({"c": 3}),
                "EventBusName": "default",
            },
        ]
    )
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 3
    assert all("EventId" in e for e in resp["Entries"])

def test_eventbridge_remove_targets_v2(eb):
    eb.put_rule(Name="eb-rm-v2", ScheduleExpression="rate(1 minute)", State="ENABLED")
    eb.put_targets(
        Rule="eb-rm-v2",
        Targets=[
            {"Id": "rm1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:f"},
            {"Id": "rm2", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:g"},
        ],
    )
    assert len(eb.list_targets_by_rule(Rule="eb-rm-v2")["Targets"]) == 2

    eb.remove_targets(Rule="eb-rm-v2", Ids=["rm1"])
    remaining = eb.list_targets_by_rule(Rule="eb-rm-v2")["Targets"]
    assert len(remaining) == 1
    assert remaining[0]["Id"] == "rm2"

def test_eventbridge_delete_rule_v2(eb):
    eb.put_rule(Name="eb-del-v2", ScheduleExpression="rate(1 day)", State="ENABLED")
    eb.delete_rule(Name="eb-del-v2")
    with pytest.raises(ClientError) as exc:
        eb.describe_rule(Name="eb-del-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_eventbridge_tags_v2(eb):
    resp = eb.put_rule(Name="eb-tag-v2", ScheduleExpression="rate(1 hour)", State="ENABLED")
    arn = resp["RuleArn"]
    eb.tag_resource(
        ResourceARN=arn,
        Tags=[
            {"Key": "stage", "Value": "dev"},
            {"Key": "team", "Value": "ops"},
        ],
    )
    tags = eb.list_tags_for_resource(ResourceARN=arn)["Tags"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["stage"] == "dev"
    assert tag_map["team"] == "ops"

    eb.untag_resource(ResourceARN=arn, TagKeys=["stage"])
    tags2 = eb.list_tags_for_resource(ResourceARN=arn)["Tags"]
    assert not any(t["Key"] == "stage" for t in tags2)
    assert any(t["Key"] == "team" for t in tags2)

def test_eventbridge_archive(eb):
    import uuid as _uuid

    archive_name = f"intg-archive-{_uuid.uuid4().hex[:8]}"
    resp = eb.create_archive(
        ArchiveName=archive_name,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
        Description="test archive",
        RetentionDays=7,
    )
    assert "ArchiveArn" in resp
    desc = eb.describe_archive(ArchiveName=archive_name)
    assert desc["ArchiveName"] == archive_name
    assert desc["RetentionDays"] == 7
    archives = eb.list_archives()
    assert any(a["ArchiveName"] == archive_name for a in archives["Archives"])
    eb.delete_archive(ArchiveName=archive_name)
    archives2 = eb.list_archives()
    assert not any(a["ArchiveName"] == archive_name for a in archives2["Archives"])

def test_eventbridge_put_remove_permission(eb):
    import uuid as _uuid

    bus_name = f"intg-perm-bus-{_uuid.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus_name)
    eb.put_permission(
        EventBusName=bus_name,
        StatementId="AllowAccount123",
        Action="events:PutEvents",
        Principal="123456789012",
    )
    # Describe bus — policy should be set (no explicit DescribeEventBus assert needed, just no error)
    eb.remove_permission(EventBusName=bus_name, StatementId="AllowAccount123")
    eb.delete_event_bus(Name=bus_name)

def test_eventbridge_content_filter_prefix(eb, sqs):
    """EventBridge prefix content filter matches events correctly."""
    bus_name = "qa-eb-prefix-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-prefix-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-prefix-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"], "detail": {"env": [{"prefix": "prod"}]}}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-prefix-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"env": "production"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"env": "staging"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0

def test_eventbridge_wildcard_detail_type(eb, sqs):
    """EventBridge wildcard pattern matches detail-type field."""
    bus_name = "qa-eb-wc-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-wc-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-wc-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"detail-type": [{"wildcard": "*simple*"}]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-wc-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    # Should match: detail-type contains "simple"
    eb.put_events(
        Entries=[{
            "Source": "test-source",
            "DetailType": "simple-detail",
            "Detail": json.dumps({"key1": "value1"}),
            "EventBusName": bus_name,
        }]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1, "Wildcard *simple* should match 'simple-detail'"
    # Should NOT match: detail-type does not contain "simple"
    eb.put_events(
        Entries=[{
            "Source": "test-source",
            "DetailType": "complex-detail",
            "Detail": json.dumps({"key1": "value1"}),
            "EventBusName": bus_name,
        }]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0, "Wildcard *simple* should not match 'complex-detail'"


def test_eventbridge_wildcard_in_detail(eb, sqs):
    """EventBridge wildcard pattern works inside detail fields too."""
    bus_name = "qa-eb-wcd-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-wcd-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-wcd-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"detail": {"env": [{"wildcard": "prod*"}]}}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-wcd-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    eb.put_events(
        Entries=[{
            "Source": "app",
            "DetailType": "deploy",
            "Detail": json.dumps({"env": "production"}),
            "EventBusName": bus_name,
        }]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    eb.put_events(
        Entries=[{
            "Source": "app",
            "DetailType": "deploy",
            "Detail": json.dumps({"env": "staging"}),
            "EventBusName": bus_name,
        }]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0


def test_eventbridge_anything_but_filter(eb, sqs):
    """EventBridge anything-but filter excludes specified values."""
    bus_name = "qa-eb-anybut-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-anybut-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-anybut-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps(
            {
                "source": ["myapp"],
                "detail": {"status": [{"anything-but": ["error", "failed"]}]},
            }
        ),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-anybut-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": json.dumps({"status": "success"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": json.dumps({"status": "error"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0

def test_eventbridge_input_transformer(eb, sqs):
    """InputTransformer rewrites event payload before delivery."""
    bus_name = "qa-eb-transform-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-transform-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-transform-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-transform-rule",
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {"src": "$.source"},
                    "InputTemplate": '{"transformed": "<src>"}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": "{}",
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body.get("transformed") == "myapp"

