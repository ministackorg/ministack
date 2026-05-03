import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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


def test_eventbridge_list_rule_names_by_target(eb):
    fn_arn = "arn:aws:lambda:us-east-1:000000000000:function:list-by-tgt-fn"
    eb.create_event_bus(Name="lrt-bus")
    eb.put_rule(
        Name="rule-a",
        EventBusName="lrt-bus",
        EventPattern=json.dumps({"source": ["my.app"]}),
        State="ENABLED",
    )
    eb.put_rule(
        Name="rule-b",
        EventBusName="lrt-bus",
        EventPattern=json.dumps({"source": ["other.app"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="rule-a",
        EventBusName="lrt-bus",
        Targets=[{"Id": "t1", "Arn": fn_arn}],
    )
    eb.put_targets(
        Rule="rule-b",
        EventBusName="lrt-bus",
        Targets=[{"Id": "t1", "Arn": fn_arn}],
    )
    out = eb.list_rule_names_by_target(TargetArn=fn_arn, EventBusName="lrt-bus")
    assert sorted(out["RuleNames"]) == ["rule-a", "rule-b"]


def test_eventbridge_test_event_pattern_match(eb):
    event = json.dumps({
        "source": "orders.service",
        "detail-type": "Order Placed",
        "detail": {"orderId": "42", "amount": 10},
    })
    pattern = json.dumps({
        "source": ["orders.service"],
        "detail-type": ["Order Placed"],
    })
    r = eb.test_event_pattern(Event=event, EventPattern=pattern)
    assert r["Result"] is True


def test_eventbridge_test_event_pattern_no_match(eb):
    event = json.dumps({"source": "other", "detail-type": "X", "detail": {}})
    pattern = json.dumps({"source": ["orders.service"]})
    r = eb.test_event_pattern(Event=event, EventPattern=pattern)
    assert r["Result"] is False


def test_eventbridge_test_event_pattern_invalid_event(eb):
    with pytest.raises(ClientError) as exc:
        eb.test_event_pattern(Event="not-json", EventPattern="{}")
    assert exc.value.response["Error"]["Code"] == "InvalidEventPatternException"


def test_eventbridge_list_rule_names_by_target_pagination(eb):
    fn_arn = "arn:aws:lambda:us-east-1:000000000000:function:page-fn"
    eb.put_rule(Name="r1", ScheduleExpression="rate(1 hour)", State="ENABLED")
    eb.put_rule(Name="r2", ScheduleExpression="rate(1 hour)", State="ENABLED")
    eb.put_targets(Rule="r1", Targets=[{"Id": "1", "Arn": fn_arn}])
    eb.put_targets(Rule="r2", Targets=[{"Id": "1", "Arn": fn_arn}])
    p1 = eb.list_rule_names_by_target(TargetArn=fn_arn, Limit=1)
    assert len(p1["RuleNames"]) == 1
    assert "NextToken" in p1
    p2 = eb.list_rule_names_by_target(TargetArn=fn_arn, Limit=1, NextToken=p1["NextToken"])
    assert len(p2["RuleNames"]) == 1
    assert p1["RuleNames"][0] != p2["RuleNames"][0]


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


def test_eventbridge_deauthorize_connection(eb):
    eb.create_connection(
        Name="deauth-conn",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "k", "ApiKeyValue": "v"}},
    )
    out = eb.deauthorize_connection(Name="deauth-conn")
    assert out["ConnectionState"] == "DEAUTHORIZED"
    desc = eb.describe_connection(Name="deauth-conn")
    assert desc["ConnectionState"] == "DEAUTHORIZED"
    eb.delete_connection(Name="deauth-conn")


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
        Runtime="python3.12",
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
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"

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


def test_eventbridge_endpoints_and_partner_stubs(eb):
    eb.create_endpoint(
        Name="my-global-endpoint",
        Description="stub",
        RoleArn="arn:aws:iam::000000000000:role/r",
        RoutingConfig={
            "FailoverConfig": {
                "Primary": {"HealthCheck": "arn:aws:route53:::healthcheck/primary"},
                "Secondary": {"Route": "secondary-route"},
            }
        },
        EventBuses=[
            {"EventBusArn": "arn:aws:events:us-east-1:000000000000:event-bus/default"},
            {"EventBusArn": "arn:aws:events:us-east-1:000000000000:event-bus/backup"},
        ],
    )
    d = eb.describe_endpoint(Name="my-global-endpoint")
    assert d["State"] == "ACTIVE"
    assert "Arn" in d
    lst = eb.list_endpoints()
    assert any(e["Name"] == "my-global-endpoint" for e in lst["Endpoints"])
    eb.update_endpoint(Name="my-global-endpoint", Description="updated")
    eb.delete_endpoint(Name="my-global-endpoint")

    eb.activate_event_source(Name="aws.partner/saas/foo")
    eb.deactivate_event_source(Name="aws.partner/saas/foo")
    src = eb.describe_event_source(Name="aws.partner/saas/foo")
    # AWS EventSourceState enum: PENDING / ACTIVE / DELETED. (Was "ENABLED" — invalid.)
    assert src["State"] == "ACTIVE"

    r = eb.create_partner_event_source(Name="saas.src", Account="111111111111")
    assert "EventSourceArn" in r
    eb.describe_partner_event_source(Name="saas.src")
    pl = eb.list_partner_event_sources(NamePrefix="saas")
    assert len(pl["PartnerEventSources"]) >= 1
    eb.delete_partner_event_source(Name="saas.src", Account="111111111111")

    acc = eb.list_partner_event_source_accounts(EventSourceName="x")
    assert acc["PartnerEventSourceAccounts"] == []

    es = eb.list_event_sources()
    assert es["EventSources"] == []

    pe = eb.put_partner_events(Entries=[{"Source": "p", "DetailType": "t", "Detail": "{}"}])
    assert pe["FailedEntryCount"] == 0


def test_eventbridge_replay_lifecycle(eb):
    arch = f"replay-arch-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(
        ArchiveName=arch,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
    )
    archive_arn = eb.describe_archive(ArchiveName=arch)["ArchiveArn"]
    rep_name = f"replay-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    from datetime import datetime, timezone

    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    start = eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=t0,
        EventEndTime=t1,
        Destination={"Arn": bus_arn},
    )
    # Real AWS returns STARTING as the immediate state; the background
    # dispatch flips through RUNNING to COMPLETED.
    assert start["State"] == "STARTING"
    desc = eb.describe_replay(ReplayName=rep_name)
    assert desc["ReplayName"] == rep_name
    assert desc["State"] in ("STARTING", "RUNNING", "COMPLETED")
    listed = eb.list_replays(NamePrefix=rep_name)
    assert any(r["ReplayName"] == rep_name for r in listed["Replays"])
    from botocore.exceptions import ClientError as _CE
    try:
        cancel = eb.cancel_replay(ReplayName=rep_name)
        assert cancel["State"] == "CANCELLED"
        desc2 = eb.describe_replay(ReplayName=rep_name)
        assert desc2["State"] == "CANCELLED"
    except _CE as e:
        # Replay may have already completed before the cancel call
        assert e.response["Error"]["Code"] == "ValidationException"
        assert "completed" in e.response["Error"]["Message"].lower()
    eb.delete_archive(ArchiveName=arch)


def test_eventbridge_update_archive(eb):
    name = f"upd-archive-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(
        ArchiveName=name,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
        Description="old",
        RetentionDays=1,
    )
    eb.update_archive(
        ArchiveName=name,
        Description="new desc",
        RetentionDays=30,
        EventPattern=json.dumps({"source": ["app"]}),
    )
    desc = eb.describe_archive(ArchiveName=name)
    assert desc["Description"] == "new desc"
    assert desc["RetentionDays"] == 30
    assert "app" in desc["EventPattern"]
    eb.delete_archive(ArchiveName=name)


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


def test_eventbridge_put_events_with_arn_as_bus_name(eb, sqs):
    """PutEvents with an ARN as EventBusName should dispatch to rules using the bus name."""
    bus_name = "qa-eb-arn-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-arn-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-arn-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-arn-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{bus_name}"
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": bus_arn,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    assert len(msgs.get("Messages", [])) == 1


def test_eventbridge_cfn_rule_accessible_via_api(eb, sqs, cfn):
    """Rules created via CloudFormation should be accessible via the EventBridge API."""
    bus_name = "qa-eb-cfn-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-cfn-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "TestRule": {
                "Type": "AWS::Events::Rule",
                "Properties": {
                    "Name": "qa-eb-cfn-rule",
                    "EventBusName": bus_name,
                    "EventPattern": {"source": ["myapp.cfn"]},
                    "State": "ENABLED",
                    "Targets": [{"Id": "t1", "Arn": q_arn}],
                },
            },
        },
    })
    cfn.create_stack(StackName="qa-eb-cfn-stack", TemplateBody=template)

    rule = eb.describe_rule(Name="qa-eb-cfn-rule", EventBusName=bus_name)
    assert rule["Name"] == "qa-eb-cfn-rule"

    targets = eb.list_targets_by_rule(Rule="qa-eb-cfn-rule", EventBusName=bus_name)
    assert len(targets["Targets"]) == 1

    eb.put_events(
        Entries=[
            {
                "Source": "myapp.cfn",
                "DetailType": "test",
                "Detail": json.dumps({"from": "cfn"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    assert len(msgs.get("Messages", [])) == 1

    cfn.delete_stack(StackName="qa-eb-cfn-stack")


def test_eventbridge_archive_stores_events(eb):
    """PutEvents writes to a matching archive and increments EventCount."""
    arch_name = f"store-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "archiver.test",
                "DetailType": "Stored",
                "Detail": json.dumps({"x": 1}),
                "EventBusName": "default",
            }
        ]
    )
    desc = eb.describe_archive(ArchiveName=arch_name)
    assert desc["EventCount"] == 1
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_archive_filters_by_pattern(eb):
    """Events that do not match the archive EventPattern are not stored."""
    arch_name = f"filter-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(
        ArchiveName=arch_name,
        EventSourceArn=bus_arn,
        EventPattern=json.dumps({"source": ["only.this"]}),
    )
    eb.put_events(
        Entries=[
            {
                "Source": "not.this",
                "DetailType": "NoMatch",
                "Detail": json.dumps({}),
                "EventBusName": "default",
            }
        ]
    )
    desc = eb.describe_archive(ArchiveName=arch_name)
    assert desc["EventCount"] == 0
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_start_replay_initial_state_is_starting(eb):
    """StartReplay's immediate response must return State=STARTING per the
    AWS Replay state machine (STARTING → RUNNING → COMPLETED). The
    background dispatch thread flips it to RUNNING then COMPLETED — but
    callers reading the start_replay() return value must see STARTING."""
    arch_name = f"replay-init-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-init-{_uuid_mod.uuid4().hex[:8]}"
    resp = eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    assert resp["State"] == "STARTING", resp
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_completes(eb):
    """StartReplay dispatches archived events and reaches COMPLETED state."""
    arch_name = f"replay-cmp-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "replay.src",
                "DetailType": "ReplayMe",
                "Detail": json.dumps({"seq": 1}),
                "EventBusName": "default",
            }
        ]
    )
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-cmp-{_uuid_mod.uuid4().hex[:8]}"
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    time.sleep(0.3)
    desc = eb.describe_replay(ReplayName=rep_name)
    assert desc["State"] == "COMPLETED"
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_not_found(eb):
    """StartReplay with a nonexistent archive returns ResourceNotFoundException."""
    from botocore.exceptions import ClientError
    nonexistent_arn = "arn:aws:events:us-east-1:000000000000:archive/does-not-exist"
    rep_name = f"rep-nf-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=rep_name,
            EventSourceArn=nonexistent_arn,
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": "arn:aws:events:us-east-1:000000000000:event-bus/default"},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eventbridge_archive_event_count_accumulation(eb):
    """EventCount increments once per matching PutEvents call."""
    arch_name = f"accum-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    for i in range(5):
        eb.put_events(
            Entries=[
                {
                    "Source": "accum.test",
                    "DetailType": "Tick",
                    "Detail": json.dumps({"seq": i}),
                    "EventBusName": "default",
                }
            ]
        )
    desc = eb.describe_archive(ArchiveName=arch_name)
    assert desc["EventCount"] == 5
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_archive_empty_pattern_stores_all_events(eb):
    """An archive with no EventPattern captures every event on the source bus."""
    arch_name = f"nopat-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "source.a",
                "DetailType": "EventA",
                "Detail": json.dumps({}),
                "EventBusName": "default",
            },
            {
                "Source": "source.b",
                "DetailType": "EventB",
                "Detail": json.dumps({}),
                "EventBusName": "default",
            },
        ]
    )
    desc = eb.describe_archive(ArchiveName=arch_name)
    assert desc["EventCount"] == 2
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_multiple_archives_same_bus(eb):
    """One PutEvents call stores the event in every matching archive on that bus."""
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    arch_a = f"multi-a-{_uuid_mod.uuid4().hex[:8]}"
    arch_b = f"multi-b-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(
        ArchiveName=arch_a,
        EventSourceArn=bus_arn,
        EventPattern=json.dumps({"source": ["multi.src"]}),
    )
    eb.create_archive(
        ArchiveName=arch_b,
        EventSourceArn=bus_arn,
        EventPattern=json.dumps({"source": ["multi.src"]}),
    )
    eb.put_events(
        Entries=[
            {
                "Source": "multi.src",
                "DetailType": "Both",
                "Detail": json.dumps({}),
                "EventBusName": "default",
            }
        ]
    )
    assert eb.describe_archive(ArchiveName=arch_a)["EventCount"] == 1
    assert eb.describe_archive(ArchiveName=arch_b)["EventCount"] == 1
    eb.delete_archive(ArchiveName=arch_a)
    eb.delete_archive(ArchiveName=arch_b)


def test_eventbridge_replay_time_range_filtering(eb, sqs):
    """Events outside the replay time window are not dispatched to the destination."""
    bus_name = "rp-trange-bus"
    bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{bus_name}"
    eb.create_event_bus(Name=bus_name)

    q_url = sqs.create_queue(QueueName="rp-trange-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="rp-trange-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["trange.src"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="rp-trange-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )

    src_bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    arch_name = f"trange-arch-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=src_bus_arn)

    # Put one event now — its Time will be approximately now (float)
    eb.put_events(
        Entries=[
            {
                "Source": "trange.src",
                "DetailType": "InRange",
                "Detail": json.dumps({"marker": "in"}),
                "EventBusName": "default",
            }
        ]
    )
    now = time.time()

    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-trange-{_uuid_mod.uuid4().hex[:8]}"
    # Replay window ends BEFORE the event was stored — nothing should be dispatched.
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=now - 3600,
        Destination={"Arn": bus_arn},
    )
    time.sleep(0.3)
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 0, (
        "Events outside the replay time window should not be dispatched"
    )
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_empty_archive_completes(eb):
    """A replay on an archive with zero events still reaches COMPLETED state."""
    arch_name = f"empty-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-empty-{_uuid_mod.uuid4().hex[:8]}"
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    time.sleep(0.3)
    desc = eb.describe_replay(ReplayName=rep_name)
    assert desc["State"] == "COMPLETED"
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_destination_receives_events(eb, sqs):
    """Archived events are actually delivered to the destination bus during replay."""
    bus_name = "rp-dest-bus"
    bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{bus_name}"
    eb.create_event_bus(Name=bus_name)

    q_url = sqs.create_queue(QueueName="rp-dest-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="rp-dest-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["dest.replay"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="rp-dest-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )

    src_bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    arch_name = f"dest-arch-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=src_bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "dest.replay",
                "DetailType": "ReplayDelivery",
                "Detail": json.dumps({"check": "delivered"}),
                "EventBusName": "default",
            }
        ]
    )
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-dest-{_uuid_mod.uuid4().hex[:8]}"
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert len(msgs.get("Messages", [])) >= 1, (
        "Replayed events should be dispatched to the destination bus and arrive in SQS"
    )
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_archive_event_count_unchanged_after_replay(eb):
    """Replay reads archived events non-destructively; EventCount stays the same."""
    arch_name = f"postcnt-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "postcnt.src",
                "DetailType": "CountCheck",
                "Detail": json.dumps({}),
                "EventBusName": "default",
            }
        ]
    )
    count_before = eb.describe_archive(ArchiveName=arch_name)["EventCount"]
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-postcnt-{_uuid_mod.uuid4().hex[:8]}"
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    time.sleep(0.3)
    count_after = eb.describe_archive(ArchiveName=arch_name)["EventCount"]
    assert count_after == count_before, (
        "Replay must not consume or modify archived events"
    )
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_duplicate_replay_name_fails(eb):
    """Starting a replay with the same name twice returns ResourceAlreadyExistsException."""
    from botocore.exceptions import ClientError
    arch_name = f"dup-rep-arch-{_uuid_mod.uuid4().hex[:8]}"
    bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-dup-{_uuid_mod.uuid4().hex[:8]}"
    eb.start_replay(
        ReplayName=rep_name,
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": bus_arn},
    )
    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=rep_name,
            EventSourceArn=archive_arn,
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": bus_arn},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_log_config_round_trip(eb):
    """LogConfig accept-and-echo (2026-03 AWS additive change) — must
    persist on Create, echo on Describe, update via UpdateEventBus.
    Older botocore strict-validates this new field, so call via raw HTTP."""
    import urllib.request as _r
    name = f"log-bus-{int(time.time()*1000)}"
    log_cfg = {"IncludeDetail": "FULL", "Level": "INFO"}

    def _post(target, payload):
        req = _r.Request(
            "http://localhost:4566/",
            data=json.dumps(payload).encode(),
            headers={
                "X-Amz-Target": f"AWSEvents.{target}",
                "Content-Type": "application/x-amz-json-1.1",
                "Authorization": ("AWS4-HMAC-SHA256 Credential=test/20260101/"
                                  "us-east-1/events/aws4_request, SignedHeaders=, Signature=x"),
            },
        )
        return json.loads(_r.urlopen(req).read())

    _post("CreateEventBus", {"Name": name, "LogConfig": log_cfg})
    desc = _post("DescribeEventBus", {"Name": name})
    assert desc.get("LogConfig") == log_cfg

    new_cfg = {"IncludeDetail": "NONE", "Level": "ERROR"}
    _post("UpdateEventBus", {"Name": name, "LogConfig": new_cfg})
    desc2 = _post("DescribeEventBus", {"Name": name})
    assert desc2.get("LogConfig") == new_cfg

    eb.delete_event_bus(Name=name)
# ---------------------------------------------------------------------------
# Unit tests: _parse_rate_seconds
# ---------------------------------------------------------------------------

import pytest as _pytest
from ministack.services import eventbridge as _eb


@_pytest.mark.parametrize("expr,expected", [
    ("rate(1 minute)",   60),
    ("rate(5 minutes)",  300),
    ("rate(1 hour)",     3600),
    ("rate(2 hours)",    7200),
    ("rate(1 day)",      86400),
    ("rate(3 days)",     259200),
    # invalid — should return None
    ("cron(0 12 * * ? *)", None),
    ("rate(1 second)",    None),
    ("rate(1 seconds)",   None),
    ("",                  None),
    ("rate(1 week)",      None),
    ("not-a-rate",        None),
])
def test_scheduler_parse_rate_seconds(expr, expected):
    assert _eb._parse_rate_seconds(expr) == expected


# ---------------------------------------------------------------------------
# Unit tests: _tick_scheduled_rules
# ---------------------------------------------------------------------------

@_pytest.fixture()
def isolated_scheduler():
    """Save and restore scheduler module state so unit tests don't bleed.

    Also installs a MagicMock as ``_invoke_target`` for the **entire test
    duration** (yielded as the fixture value). This is wider than a
    ``with patch(...)`` block: any concurrent caller (the eb-scheduler daemon
    if it's running, an in-process ASGI lifespan, etc.) hits the mock too,
    so tests can assert on call counts without racing.
    """
    from unittest.mock import MagicMock

    saved_rules = dict(_eb._rules._data)
    saved_targets = dict(_eb._targets._data)
    saved_fired = dict(_eb._rule_last_fired)
    saved_invoke = _eb._invoke_target
    _eb._rules._data.clear()
    _eb._targets._data.clear()
    _eb._rule_last_fired.clear()
    mock_invoke = MagicMock(name="_invoke_target")
    _eb._invoke_target = mock_invoke
    yield mock_invoke
    _eb._invoke_target = saved_invoke
    _eb._rules._data.clear()
    _eb._rules._data.update(saved_rules)
    _eb._targets._data.clear()
    _eb._targets._data.update(saved_targets)
    _eb._rule_last_fired.clear()
    _eb._rule_last_fired.update(saved_fired)


_ACCOUNT = "000000000000"
_RULE_KEY = "default|unit-test-rule"
_STATE_KEY = (_ACCOUNT, _RULE_KEY)
_DUMMY_TARGET = [{"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:dummy"}]

def _seed_rule(schedule="rate(1 minute)", state="ENABLED"):
    _eb._rules._data[_STATE_KEY] = {
        "Name": "unit-test-rule",
        "ScheduleExpression": schedule,
        "State": state,
        "EventBusName": "default",
        "Arn": "arn:aws:events:us-east-1:000000000000:rule/unit-test-rule",
    }
    _eb._targets._data[_STATE_KEY] = list(_DUMMY_TARGET)


from unittest.mock import patch as _patch


def test_scheduler_first_sight_initializes_countdown(isolated_scheduler):
    """First tick records the timestamp but must NOT dispatch."""
    _seed_rule()
    _eb._tick_scheduled_rules()
    assert _STATE_KEY in _eb._rule_last_fired
    isolated_scheduler.assert_not_called()


def test_scheduler_fires_after_interval(isolated_scheduler):
    """Tick dispatches when last-fired is older than the rule interval."""
    _seed_rule()
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 65  # 65 s ago > 60 s interval
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_called_once()
    target_arg = isolated_scheduler.call_args[0][0]
    assert target_arg["Id"] == "t1"


def test_scheduler_skips_rule_before_interval(isolated_scheduler):
    """Tick must NOT dispatch when interval hasn't elapsed."""
    _seed_rule()
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 10  # only 10 s ago
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_not_called()


def test_scheduler_skips_disabled_rule(isolated_scheduler):
    """Disabled rules must never be dispatched even if past interval."""
    _seed_rule(state="DISABLED")
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 120
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_not_called()


@_pytest.mark.parametrize("expr,valid", [
    ("cron(0 12 * * ? *)",       True),   # noon every day
    ("cron(0/5 * * * ? *)",      True),   # every 5 minutes
    ("cron(0 0 ? * MON-FRI *)",  True),   # midnight Mon–Fri
    ("cron(30 6 1 * ? *)",       True),   # 06:30 on 1st of each month
    ("cron(0 0 1 1 ? 2030)",     True),   # specific year
    ("cron(0 0 L * ? *)",        True),   # last day of every month
    ("cron(0 0 LW * ? *)",       True),   # last weekday of every month
    ("cron(0 12 15W * ? *)",     True),   # nearest weekday to the 15th
    ("cron(0 12 ? * 6L *)",      True),   # last Friday of every month (AWS Fri=6)
    ("cron(0 9 ? * 2#1 *)",      True),   # first Monday of every month (AWS Mon=2)
    ("rate(1 minute)",            False),  # not a cron expression
    ("",                          False),
    ("cron(0 12 * * *)",          False),  # 5 fields — missing Year
    ("cron()",                    False),
    ("cron(0 12 * * * *)",        False),  # both DoM and DoW non-'?' — AWS rejects
    ("cron(0 12 1 * MON *)",      False),  # both DoM and DoW non-'?' — AWS rejects
    ("cron(0 12 32W * ? *)",      False),  # day-of-month out of range in <n>W
    ("cron(0 12 ? * 8L *)",       False),  # AWS DoW only goes 1..7
    ("cron(0 12 ? * 6#6 *)",      False),  # nth occurrence only valid 1..5
])
def test_scheduler_parse_cron_fields_validity(expr, valid):
    result = _eb._parse_cron_fields(expr)
    assert (result is not None) == valid


def test_scheduler_cron_next_fire_same_day():
    """cron(0 12 * * ? *): next noon after 11:00 is 12:00 same day."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 * * ? *)")
    after = _dt(2024, 1, 1, 11, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 1, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_next_fire_wraps_to_next_day():
    """cron(0 12 * * ? *): after noon, next occurrence is noon tomorrow."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 * * ? *)")
    after = _dt(2024, 1, 1, 12, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 2, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_next_fire_weekday():
    """cron(0 0 ? * MON-FRI *): after Friday 23:00, next is Monday 00:00."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 0 ? * MON-FRI *)")
    after = _dt(2024, 1, 5, 23, 0, tzinfo=_tz.utc)   # Friday
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 8, 0, 0, tzinfo=_tz.utc)  # Monday


def test_scheduler_cron_first_sight_initializes_countdown(isolated_scheduler):
    """First tick of a cron() rule records the timestamp but must NOT dispatch."""
    _seed_rule(schedule="cron(0 12 * * ? *)")
    _eb._tick_scheduled_rules()
    assert _STATE_KEY in _eb._rule_last_fired
    isolated_scheduler.assert_not_called()


def test_scheduler_cron_fires_after_scheduled_time(isolated_scheduler):
    """cron() rule dispatches when the next scheduled occurrence has passed."""
    _seed_rule(schedule="cron(0 * * * ? *)")  # every hour on the hour
    # last_fired 2 hours ago → next occurrence is ~1 hour ago → should fire now
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 7200
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_called_once()


def test_scheduler_cron_skips_before_scheduled_time(isolated_scheduler):
    """cron() rule does NOT dispatch before the next scheduled occurrence arrives."""
    _seed_rule(schedule="cron(0 * * * ? *)")  # every hour on the hour
    # last_fired 10 s ago → next occurrence is ~59m50s from now → must not fire
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 10
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_not_called()


def test_scheduler_cron_last_day_of_month():
    """cron(0 0 L * ? *): next fire after Jan 30 is Jan 31 (last day)."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 0 L * ? *)")
    after = _dt(2024, 1, 30, 12, 0, tzinfo=_tz.utc)
    # Jan has 31 days
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 31, 0, 0, tzinfo=_tz.utc)
    # Feb 2024 (leap year) has 29 days
    after = _dt(2024, 2, 1, 0, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 2, 29, 0, 0, tzinfo=_tz.utc)


def test_scheduler_cron_last_weekday_of_month():
    """cron(0 0 LW * ? *): last Mon-Fri of the month."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 0 LW * ? *)")
    # March 2024: 31st = Sunday → last weekday is Fri Mar 29.
    after = _dt(2024, 3, 1, 0, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 3, 29, 0, 0, tzinfo=_tz.utc)


def test_scheduler_cron_nearest_weekday():
    """cron(0 12 15W * ? *): nearest Mon-Fri to the 15th, never crossing month."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 15W * ? *)")
    # Jan 15 2024 = Monday → fires on the 15th itself.
    assert _eb._cron_next_fire(fields, _dt(2024, 1, 14, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 1, 15, 12, 0, tzinfo=_tz.utc)
    # Jun 15 2024 = Saturday → fires on Friday Jun 14.
    assert _eb._cron_next_fire(fields, _dt(2024, 6, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 6, 14, 12, 0, tzinfo=_tz.utc)
    # Sep 15 2024 = Sunday → fires on Monday Sep 16.
    assert _eb._cron_next_fire(fields, _dt(2024, 9, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 9, 16, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_last_dow_of_month():
    """cron(0 12 ? * 6L *): last Friday of the month (AWS Friday = 6)."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 ? * 6L *)")
    # Jan 2024: Fridays are 5, 12, 19, 26 → last is Fri Jan 26.
    assert _eb._cron_next_fire(fields, _dt(2024, 1, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 1, 26, 12, 0, tzinfo=_tz.utc)
    # Mar 2024: Fridays are 1, 8, 15, 22, 29 → last is Fri Mar 29.
    assert _eb._cron_next_fire(fields, _dt(2024, 3, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 3, 29, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_nth_dow_of_month():
    """cron(0 9 ? * 2#1 *): first Monday of every month (AWS Monday = 2)."""
    from datetime import datetime as _dt, timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 9 ? * 2#1 *)")
    # Jan 2024: Mondays are 1, 8, 15, 22, 29 → 1st Monday = Jan 1.
    assert _eb._cron_next_fire(fields, _dt(2023, 12, 31, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 1, 1, 9, 0, tzinfo=_tz.utc)
    # Feb 2024: Mondays are 5, 12, 19, 26 → 1st = Feb 5.
    assert _eb._cron_next_fire(fields, _dt(2024, 1, 2, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 2, 5, 9, 0, tzinfo=_tz.utc)


def test_scheduler_validate_rejects_dom_and_dow_both_non_question_mark():
    """PutRule must reject cron expressions where both DoM and DoW are non-'?' (AWS rule)."""
    assert _eb._validate_schedule_expression("cron(0 12 * * * *)") is False
    assert _eb._validate_schedule_expression("cron(0 12 1 * MON *)") is False
    # Valid: at least one of DoM/DoW is '?'.
    assert _eb._validate_schedule_expression("cron(0 12 * * ? *)") is True
    assert _eb._validate_schedule_expression("cron(0 12 ? * MON *)") is True


def test_scheduler_no_error_without_targets(isolated_scheduler):
    """A rule with no targets must not raise; just skip dispatch."""
    _seed_rule()
    _eb._targets._data[_STATE_KEY] = []  # empty targets list
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts() - 120
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_not_called()


def test_scheduler_reset_clears_last_fired(isolated_scheduler):
    """reset() must empty _rule_last_fired."""
    _eb._rule_last_fired[_STATE_KEY] = _eb._now_ts()
    _eb.reset()
    assert _eb._rule_last_fired == {}


def test_scheduler_first_sight_with_old_creation_time_fires_immediately(isolated_scheduler):
    """AWS doc: 'the countdown begins when you create the rule'. A rule whose
    CreationTime is already older than the interval must fire on the first
    scheduler tick that observes it, not wait another full interval."""
    _eb._rules._data[_STATE_KEY] = {
        "Name": "old-rule",
        "ScheduleExpression": "rate(1 minute)",
        "State": "ENABLED",
        "EventBusName": "default",
        "Arn": "arn:aws:events:us-east-1:000000000000:rule/old-rule",
        "CreationTime": _eb._now_ts() - 120,  # created 2 min ago, interval = 1 min
    }
    _eb._targets._data[_STATE_KEY] = list(_DUMMY_TARGET)
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_called_once()


def test_scheduler_first_sight_with_recent_creation_time_waits(isolated_scheduler):
    """A rule created within the last interval must NOT fire on first sight —
    AWS countdown begins at PutRule, so the first fire is one full interval later."""
    _eb._rules._data[_STATE_KEY] = {
        "Name": "fresh-rule",
        "ScheduleExpression": "rate(1 minute)",
        "State": "ENABLED",
        "EventBusName": "default",
        "Arn": "arn:aws:events:us-east-1:000000000000:rule/fresh-rule",
        "CreationTime": _eb._now_ts() - 5,  # created 5s ago, interval = 60s
    }
    _eb._targets._data[_STATE_KEY] = list(_DUMMY_TARGET)
    _eb._tick_scheduled_rules()
    isolated_scheduler.assert_not_called()
