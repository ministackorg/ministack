import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile

import boto3
import pytest
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _events_client(region_name):
    return boto3.client(
        "events",
        endpoint_url=_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region_name,
    )


def _sqs_client(region_name):
    return boto3.client(
        "sqs",
        endpoint_url=_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region_name,
    )


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


def test_eventbridge_buses_rules_and_targets_are_region_scoped(eb):
    west = _events_client("us-west-2")
    bus_name = f"region-bus-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"region-rule-{_uuid_mod.uuid4().hex[:8]}"

    east_bus_arn = eb.create_event_bus(Name=bus_name)["EventBusArn"]
    west_bus_arn = west.create_event_bus(Name=bus_name)["EventBusArn"]
    assert east_bus_arn == f"arn:aws:events:us-east-1:000000000000:event-bus/{bus_name}"
    assert west_bus_arn == f"arn:aws:events:us-west-2:000000000000:event-bus/{bus_name}"

    eb.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    west.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        ScheduleExpression="rate(10 minutes)",
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "east",
                "Arn": "arn:aws:lambda:us-east-1:000000000000:function:east-fn",
            }
        ],
    )
    west.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "west",
                "Arn": "arn:aws:lambda:us-west-2:000000000000:function:west-fn",
            }
        ],
    )

    assert eb.describe_event_bus(Name=bus_name)["Arn"] == east_bus_arn
    assert west.describe_event_bus(Name=bus_name)["Arn"] == west_bus_arn
    assert eb.describe_rule(Name=rule_name, EventBusName=bus_name)["Arn"] == (
        f"arn:aws:events:us-east-1:000000000000:rule/{bus_name}/{rule_name}"
    )
    assert west.describe_rule(Name=rule_name, EventBusName=bus_name)["Arn"] == (
        f"arn:aws:events:us-west-2:000000000000:rule/{bus_name}/{rule_name}"
    )
    assert [t["Id"] for t in eb.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)["Targets"]] == ["east"]
    assert [t["Id"] for t in west.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)["Targets"]] == ["west"]

    eb.delete_rule(Name=rule_name, EventBusName=bus_name)
    eb.delete_event_bus(Name=bus_name)

    assert west.describe_event_bus(Name=bus_name)["Arn"] == west_bus_arn
    assert [t["Id"] for t in west.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)["Targets"]] == ["west"]


def test_eventbridge_rule_names_by_target_and_remove_targets_are_region_scoped(eb):
    west = _events_client("us-west-2")
    rule_name = f"region-target-index-{_uuid_mod.uuid4().hex[:8]}"
    east_target_arn = "arn:aws:lambda:us-east-1:000000000000:function:index-east"
    west_target_arn = "arn:aws:lambda:us-west-2:000000000000:function:index-west"

    eb.put_rule(
        Name=rule_name,
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    west.put_rule(
        Name=rule_name,
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    eb.put_targets(Rule=rule_name, Targets=[{"Id": "east", "Arn": east_target_arn}])
    west.put_targets(Rule=rule_name, Targets=[{"Id": "west", "Arn": west_target_arn}])

    assert eb.list_rule_names_by_target(TargetArn=east_target_arn)["RuleNames"] == [rule_name]
    assert eb.list_rule_names_by_target(TargetArn=west_target_arn)["RuleNames"] == []
    assert west.list_rule_names_by_target(TargetArn=west_target_arn)["RuleNames"] == [rule_name]
    assert west.list_rule_names_by_target(TargetArn=east_target_arn)["RuleNames"] == []

    eb.remove_targets(Rule=rule_name, Ids=["east"])

    assert eb.list_targets_by_rule(Rule=rule_name)["Targets"] == []
    assert [t["Id"] for t in west.list_targets_by_rule(Rule=rule_name)["Targets"]] == ["west"]
    assert west.list_rule_names_by_target(TargetArn=west_target_arn)["RuleNames"] == [rule_name]


def test_eventbridge_put_targets_rejects_malformed_target_arn(eb):
    rule_name = f"target-malformed-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, ScheduleExpression="rate(1 minute)", State="ENABLED")

    with pytest.raises(ClientError) as exc:
        eb.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "bad", "Arn": "not-an-arn"}],
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "Provided Arn is not in correct format" in exc.value.response["Error"]["Message"]
    assert eb.list_targets_by_rule(Rule=rule_name)["Targets"] == []


def test_eventbridge_put_targets_rejects_unsupported_target_service(eb):
    rule_name = f"target-wrong-service-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, ScheduleExpression="rate(1 minute)", State="ENABLED")

    with pytest.raises(ClientError) as exc:
        eb.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "rds",
                    "Arn": "arn:aws:rds:us-east-1:000000000000:db:not-a-target",
                }
            ],
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "rds is not a supported service for a target" in exc.value.response["Error"]["Message"]
    assert eb.list_targets_by_rule(Rule=rule_name)["Targets"] == []


def test_eventbridge_put_targets_accepts_foreign_region_non_bus_target_arns(eb):
    rule_name = f"target-foreign-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, ScheduleExpression="rate(1 minute)", State="ENABLED")

    resp = eb.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "lambda-west",
                "Arn": "arn:aws:lambda:us-west-2:000000000000:function:foreign-fn",
            },
            {
                "Id": "sqs-west",
                "Arn": "arn:aws:sqs:us-west-2:000000000000:foreign-q",
            },
            {
                "Id": "sns-west",
                "Arn": "arn:aws:sns:us-west-2:000000000000:foreign-topic",
            },
            {
                "Id": "sfn-west",
                "Arn": "arn:aws:states:us-west-2:000000000000:stateMachine:foreign-sm",
            },
        ],
    )

    assert resp["FailedEntryCount"] == 0
    ids = {target["Id"] for target in eb.list_targets_by_rule(Rule=rule_name)["Targets"]}
    assert ids == {"lambda-west", "sqs-west", "sns-west", "sfn-west"}


def test_eventbridge_put_targets_accepts_supported_non_delivery_target_services(eb):
    rule_name = f"target-services-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, ScheduleExpression="rate(1 minute)", State="ENABLED")

    resp = eb.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "logs",
                "Arn": "arn:aws:logs:us-east-1:000000000000:log-group:/aws/events/test",
            },
            {
                "Id": "kinesis",
                "Arn": "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream",
            },
            {
                "Id": "firehose",
                "Arn": "arn:aws:firehose:us-east-1:000000000000:deliverystream/test-stream",
            },
            {
                "Id": "batch",
                "Arn": "arn:aws:batch:us-east-1:000000000000:job-queue/test-queue",
            },
            {
                "Id": "ecs",
                "Arn": "arn:aws:ecs:us-east-1:000000000000:cluster/test-cluster",
            },
            {
                "Id": "apigw",
                "Arn": "arn:aws:execute-api:us-east-1:000000000000:api-id/stage/GET/path",
            },
            {
                "Id": "appsync",
                "Arn": "arn:aws:appsync:us-east-1:000000000000:apis/api-id",
            },
            {
                "Id": "ssm",
                "Arn": "arn:aws:ssm:us-east-1:000000000000:document/AWS-RunShellScript",
            },
        ],
    )

    assert resp["FailedEntryCount"] == 0
    ids = {target["Id"] for target in eb.list_targets_by_rule(Rule=rule_name)["Targets"]}
    assert ids == {"logs", "kinesis", "firehose", "batch", "ecs", "apigw", "appsync", "ssm"}


def test_eventbridge_put_targets_requires_role_for_foreign_region_event_bus(eb):
    rule_name = f"target-foreign-bus-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, ScheduleExpression="rate(1 minute)", State="ENABLED")

    with pytest.raises(ClientError) as exc:
        eb.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "bus-west",
                    "Arn": "arn:aws:events:us-west-2:000000000000:event-bus/foreign-bus",
                }
            ],
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "RoleArn is required" in exc.value.response["Error"]["Message"]


def test_eventbridge_foreign_region_sqs_target_does_not_deliver_to_same_name_queue(eb, sqs):
    queue_name = f"target-foreign-sqs-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"target-foreign-sqs-{_uuid_mod.uuid4().hex[:8]}"
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    west_arn = f"arn:aws:sqs:us-west-2:000000000000:{queue_name}"
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({"source": ["foreign.sqs"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "foreign-sqs", "Arn": west_arn}],
    )

    eb.put_events(
        Entries=[
            {
                "Source": "foreign.sqs",
                "DetailType": "ForeignRegionSqs",
                "Detail": json.dumps({"should": "not-deliver"}),
                "EventBusName": "default",
            }
        ]
    )

    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert msgs.get("Messages", []) == []


def test_eventbridge_same_region_sns_target_dispatches_to_topic(eb, sns, sqs):
    topic_name = f"target-sns-{_uuid_mod.uuid4().hex[:8]}"
    queue_name = f"target-sns-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"target-sns-{_uuid_mod.uuid4().hex[:8]}"
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({"source": ["same.region.sns"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "local-sns", "Arn": topic_arn}],
    )

    resp = eb.put_events(
        Entries=[
            {
                "Source": "same.region.sns",
                "DetailType": "SameRegionSns",
                "Detail": json.dumps({"delivered": True}),
                "EventBusName": "default",
            }
        ]
    )

    assert resp["FailedEntryCount"] == 0
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    payload = json.loads(body["Message"])
    assert payload["source"] == "same.region.sns"
    assert payload["detail"] == {"delivered": True}


def test_eventbridge_foreign_region_sns_target_does_not_deliver_to_same_name_topic(eb, sns, sqs):
    topic_name = f"target-foreign-sns-{_uuid_mod.uuid4().hex[:8]}"
    queue_name = f"target-foreign-sns-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"target-foreign-sns-{_uuid_mod.uuid4().hex[:8]}"
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
    west_arn = f"arn:aws:sns:us-west-2:000000000000:{topic_name}"
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({"source": ["foreign.sns"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "foreign-sns", "Arn": west_arn}],
    )

    resp = eb.put_events(
        Entries=[
            {
                "Source": "foreign.sns",
                "DetailType": "ForeignRegionSns",
                "Detail": json.dumps({"should": "not-deliver"}),
                "EventBusName": "default",
            }
        ]
    )

    assert resp["FailedEntryCount"] == 0
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert msgs.get("Messages", []) == []


def test_eventbridge_foreign_region_bus_target_does_not_deliver_to_same_name_bus(eb, sqs):
    bus_name = f"target-foreign-bus-{_uuid_mod.uuid4().hex[:8]}"
    source_rule = f"source-foreign-bus-{_uuid_mod.uuid4().hex[:8]}"
    local_rule = f"local-foreign-bus-{_uuid_mod.uuid4().hex[:8]}"
    queue_name = f"target-foreign-bus-{_uuid_mod.uuid4().hex[:8]}"
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    eb.create_event_bus(Name=bus_name)
    eb.put_rule(
        Name=local_rule,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["foreign.bus"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=local_rule,
        EventBusName=bus_name,
        Targets=[{"Id": "local-q", "Arn": q_arn}],
    )
    eb.put_rule(
        Name=source_rule,
        EventPattern=json.dumps({"source": ["foreign.bus"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=source_rule,
        Targets=[
            {
                "Id": "foreign-bus",
                "Arn": f"arn:aws:events:us-west-2:000000000000:event-bus/{bus_name}",
                "RoleArn": "arn:aws:iam::000000000000:role/eb-cross-region",
            },
        ],
    )

    eb.put_events(
        Entries=[
            {
                "Source": "foreign.bus",
                "DetailType": "ForeignRegionBus",
                "Detail": json.dumps({"should": "not-deliver"}),
                "EventBusName": "default",
            }
        ]
    )

    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert msgs.get("Messages", []) == []


def test_eventbridge_same_bus_target_does_not_recursively_dispatch(eb, sqs):
    rule_name = f"target-self-bus-{_uuid_mod.uuid4().hex[:8]}"
    queue_name = f"target-self-bus-{_uuid_mod.uuid4().hex[:8]}"
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({"source": ["self.bus"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "same-bus",
                "Arn": "arn:aws:events:us-east-1:000000000000:event-bus/default",
            },
            {
                "Id": "local-q",
                "Arn": q_arn,
            },
        ],
    )

    resp = eb.put_events(
        Entries=[
            {
                "Source": "self.bus",
                "DetailType": "SelfBus",
                "Detail": json.dumps({"ok": True}),
                "EventBusName": "default",
            }
        ]
    )

    assert resp["FailedEntryCount"] == 0
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1


def test_eventbridge_event_bus_target_archives_forwarded_event(eb):
    source_bus = f"target-archive-source-{_uuid_mod.uuid4().hex[:8]}"
    dest_bus = f"target-archive-dest-{_uuid_mod.uuid4().hex[:8]}"
    source_rule = f"target-archive-rule-{_uuid_mod.uuid4().hex[:8]}"
    archive_name = f"target-archive-{_uuid_mod.uuid4().hex[:8]}"
    dest_bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{dest_bus}"
    eb.create_event_bus(Name=source_bus)
    eb.create_event_bus(Name=dest_bus)
    eb.create_archive(ArchiveName=archive_name, EventSourceArn=dest_bus_arn)
    eb.put_rule(
        Name=source_rule,
        EventBusName=source_bus,
        EventPattern=json.dumps({"source": ["archive.forward"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=source_rule,
        EventBusName=source_bus,
        Targets=[
            {
                "Id": "dest-bus",
                "Arn": dest_bus_arn,
            },
        ],
    )

    eb.put_events(
        Entries=[
            {
                "Source": "archive.forward",
                "DetailType": "ArchiveForward",
                "Detail": json.dumps({"ok": True}),
                "EventBusName": source_bus,
            }
        ]
    )

    assert eb.describe_archive(ArchiveName=archive_name)["EventCount"] == 1
    eb.delete_archive(ArchiveName=archive_name)


def test_eventbridge_event_bus_target_honors_input_override(eb, sqs):
    source_bus = f"target-input-source-{_uuid_mod.uuid4().hex[:8]}"
    dest_bus = f"target-input-dest-{_uuid_mod.uuid4().hex[:8]}"
    source_rule = f"target-input-source-rule-{_uuid_mod.uuid4().hex[:8]}"
    dest_rule = f"target-input-dest-rule-{_uuid_mod.uuid4().hex[:8]}"
    queue_name = f"target-input-q-{_uuid_mod.uuid4().hex[:8]}"
    dest_bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{dest_bus}"
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.create_event_bus(Name=source_bus)
    eb.create_event_bus(Name=dest_bus)
    eb.put_rule(
        Name=dest_rule,
        EventBusName=dest_bus,
        EventPattern=json.dumps({"source": ["bus.input"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=dest_rule,
        EventBusName=dest_bus,
        Targets=[{"Id": "local-q", "Arn": q_arn}],
    )
    eb.put_rule(
        Name=source_rule,
        EventBusName=source_bus,
        EventPattern=json.dumps({"source": ["bus.input"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=source_rule,
        EventBusName=source_bus,
        Targets=[
            {
                "Id": "dest-bus",
                "Arn": dest_bus_arn,
                "Input": json.dumps({"rewritten": True}),
            },
        ],
    )

    eb.put_events(
        Entries=[
            {
                "Source": "bus.input",
                "DetailType": "BusInput",
                "Detail": json.dumps({"original": True}),
                "EventBusName": source_bus,
            }
        ]
    )

    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["detail"] == {"rewritten": True}


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


def test_eventbridge_stepfunctions_target(eb, sfn):
    """PutEvents dispatches to a Step Functions state machine target when the rule matches."""
    sm_name = f"intg-eb-sfn-{_uuid_mod.uuid4().hex[:8]}"
    bus_name = f"intg-eb-bus-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"intg-eb-rule-{_uuid_mod.uuid4().hex[:8]}"

    sm_arn = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps({
            "StartAt": "Done",
            "States": {"Done": {"Type": "Pass", "End": True}},
        }),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

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
        Targets=[{
            "Id": "sfn-target",
            "Arn": sm_arn,
            "RoleArn": "arn:aws:iam::000000000000:role/eb-invoke-sfn",
        }],
    )

    resp = eb.put_events(Entries=[{
        "Source": "myapp.test",
        "DetailType": "TestEvent",
        "Detail": json.dumps({"key": "value"}),
        "EventBusName": bus_name,
    }])
    assert resp["FailedEntryCount"] == 0

    # Dispatch runs in a background daemon thread; poll briefly.
    deadline = time.time() + 5
    executions = []
    while time.time() < deadline:
        executions = sfn.list_executions(stateMachineArn=sm_arn)["executions"]
        if executions:
            break
        time.sleep(0.1)

    assert len(executions) == 1, "EventBridge should have started one execution"
    exec_arn = executions[0]["executionArn"]

    desc = sfn.describe_execution(executionArn=exec_arn)
    payload = json.loads(desc["input"])
    assert payload["source"] == "myapp.test"
    assert payload["detail-type"] == "TestEvent"
    assert payload["detail"] == {"key": "value"}

    # Cleanup
    eb.remove_targets(Rule=rule_name, EventBusName=bus_name, Ids=["sfn-target"])
    eb.delete_rule(Name=rule_name, EventBusName=bus_name)
    eb.delete_event_bus(Name=bus_name)
    sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_eventbridge_stepfunctions_version_and_alias_targets(eb, sfn):
    """EventBridge accepts both published-version and alias ARNs as SFN targets.

    Real AWS dispatches `PutEvents` to a `stateMachine:<name>:<version>` or
    `stateMachine:<name>:<alias>` target. The resolver in stepfunctions walks
    base / version / alias stores so both forms reach the right executor.
    """
    sm_name = f"intg-eb-sfn-vers-{_uuid_mod.uuid4().hex[:8]}"
    bus_name = f"intg-eb-bus-{_uuid_mod.uuid4().hex[:8]}"

    sm_arn = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps({
            "StartAt": "Done",
            "States": {"Done": {"Type": "Pass", "End": True}},
        }),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    pub = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
    version_arn = pub["stateMachineVersionArn"]
    alias = sfn.create_state_machine_alias(
        name="live",
        routingConfiguration=[{
            "stateMachineVersionArn": version_arn,
            "weight": 100,
        }],
    )
    alias_arn = alias["stateMachineAliasArn"]

    eb.create_event_bus(Name=bus_name)

    for target_arn, marker in [(version_arn, "vers"), (alias_arn, "alias")]:
        rule_name = f"intg-eb-rule-{marker}-{_uuid_mod.uuid4().hex[:6]}"
        eb.put_rule(
            Name=rule_name,
            EventBusName=bus_name,
            EventPattern=json.dumps({"source": [f"myapp.{marker}"]}),
            State="ENABLED",
        )
        eb.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[{"Id": f"sfn-{marker}", "Arn": target_arn}],
        )
        eb.put_events(Entries=[{
            "Source": f"myapp.{marker}",
            "DetailType": "PingEvent",
            "Detail": json.dumps({"marker": marker}),
            "EventBusName": bus_name,
        }])

    # The base state machine should accumulate one execution per dispatch.
    deadline = time.time() + 5
    while time.time() < deadline:
        execs = sfn.list_executions(stateMachineArn=sm_arn)["executions"]
        if len(execs) >= 2:
            break
        time.sleep(0.1)
    assert len(execs) >= 2, (
        f"version+alias targets should each have dispatched an execution; got {len(execs)}"
    )

    # Cleanup
    eb.delete_event_bus(Name=bus_name)
    sfn.delete_state_machine_alias(stateMachineAliasArn=alias_arn)
    sfn.delete_state_machine_version(stateMachineVersionArn=version_arn)
    sfn.delete_state_machine(stateMachineArn=sm_arn)


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


@pytest.mark.parametrize(
    ("arn", "code"),
    [
        ("not-an-arn", "ValidationException"),
        ("arn:aws:sqs:us-east-1:000000000000:rule/missing", "ValidationException"),
        ("arn:aws:events:us-west-2:000000000000:rule/missing", "ResourceNotFoundException"),
        ("arn:aws:events:us-east-1:000000000000:rule/missing", "ResourceNotFoundException"),
    ],
)
def test_eventbridge_tag_resource_requires_local_eventbridge_arn(eb, arn, code):
    with pytest.raises(ClientError) as exc:
        eb.tag_resource(ResourceARN=arn, Tags=[{"Key": "env", "Value": "test"}])

    assert exc.value.response["Error"]["Code"] == code


def test_eventbridge_tag_resource_rejects_same_name_other_region_bus(eb):
    name = f"eb-tag-region-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_event_bus(Name=name)
    west = boto3.client(
        "events",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    west_arn = f"arn:aws:events:us-west-2:000000000000:event-bus/{name}"

    with pytest.raises(ClientError) as exc:
        west.tag_resource(ResourceARN=west_arn, Tags=[{"Key": "env", "Value": "test"}])

    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eventbridge_tag_resource_accepts_default_bus_in_secondary_region(eb):
    eb.list_tags_for_resource(
        ResourceARN="arn:aws:events:us-east-1:000000000000:event-bus/default",
    )
    west = boto3.client(
        "events",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    west_arn = "arn:aws:events:us-west-2:000000000000:event-bus/default"

    west.tag_resource(ResourceARN=west_arn, Tags=[{"Key": "env", "Value": "west"}])
    tags = west.list_tags_for_resource(ResourceARN=west_arn)["Tags"]

    assert {t["Key"]: t["Value"] for t in tags} == {"env": "west"}


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


def test_eventbridge_put_events_rejects_bad_bus_arn_without_local_fallback(eb, sqs):
    bus_name = f"qa-eb-bad-bus-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName=f"qa-eb-bad-bus-q-{_uuid_mod.uuid4().hex[:8]}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-bad-bus-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-bad-bus-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )

    response = eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": f"arn:aws:sqs:us-east-1:000000000000:event-bus/{bus_name}",
            }
        ]
    )

    assert response["FailedEntryCount"] == 1
    assert response["Entries"][0]["ErrorCode"] == "ValidationException"
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert msgs.get("Messages", []) == []


def test_eventbridge_put_events_rejects_foreign_region_bus_arn_without_local_fallback(eb, sqs):
    bus_name = f"qa-eb-region-bus-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName=f"qa-eb-region-bus-q-{_uuid_mod.uuid4().hex[:8]}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-region-bus-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-region-bus-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )

    response = eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": f"arn:aws:events:us-west-2:000000000000:event-bus/{bus_name}",
            }
        ]
    )

    assert response["FailedEntryCount"] == 1
    assert response["Entries"][0]["ErrorCode"] == "ResourceNotFoundException"
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert msgs.get("Messages", []) == []


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


def test_eventbridge_replay_rejects_wrong_service_source_arn_without_local_fallback(eb):
    arch_name = f"replay-bad-src-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(
        ArchiveName=arch_name,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
    )

    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=f"rep-bad-src-{_uuid_mod.uuid4().hex[:8]}",
            EventSourceArn=f"arn:aws:sqs:us-east-1:000000000000:archive/{arch_name}",
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": "arn:aws:events:us-east-1:000000000000:event-bus/default"},
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_rejects_foreign_region_destination_without_local_fallback(eb):
    arch_name = f"replay-bad-dest-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(
        ArchiveName=arch_name,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
    )
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]

    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=f"rep-bad-dest-{_uuid_mod.uuid4().hex[:8]}",
            EventSourceArn=archive_arn,
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": "arn:aws:events:us-west-2:000000000000:event-bus/default"},
        )

    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    eb.delete_archive(ArchiveName=arch_name)


def test_eventbridge_replay_rejects_non_source_destination(eb):
    arch_name = f"replay-wrong-dest-{_uuid_mod.uuid4().hex[:8]}"
    replay_name = f"rep-wrong-dest-{_uuid_mod.uuid4().hex[:8]}"
    other_bus = f"replay-other-{_uuid_mod.uuid4().hex[:8]}"
    source_bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    other_bus_arn = f"arn:aws:events:us-east-1:000000000000:event-bus/{other_bus}"
    eb.create_event_bus(Name=other_bus)
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=source_bus_arn)
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]

    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=replay_name,
            EventSourceArn=archive_arn,
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": other_bus_arn},
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError) as nf:
        eb.describe_replay(ReplayName=replay_name)
    assert nf.value.response["Error"]["Code"] == "ResourceNotFoundException"
    eb.delete_archive(ArchiveName=arch_name)
    eb.delete_event_bus(Name=other_bus)


def test_eventbridge_replay_rejects_plain_name_source(eb):
    arch_name = f"replay-plain-src-{_uuid_mod.uuid4().hex[:8]}"
    source_bus_arn = "arn:aws:events:us-east-1:000000000000:event-bus/default"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=source_bus_arn)

    with pytest.raises(ClientError) as exc:
        eb.start_replay(
            ReplayName=f"rep-plain-src-{_uuid_mod.uuid4().hex[:8]}",
            EventSourceArn=arch_name,
            EventStartTime=0,
            EventEndTime=time.time() + 3600,
            Destination={"Arn": source_bus_arn},
        )

    assert exc.value.response["Error"]["Code"] == "ValidationException"
    eb.delete_archive(ArchiveName=arch_name)


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

    arch_name = f"trange-arch-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)

    # Put one event now; its Time will be approximately now.
    eb.put_events(
        Entries=[
            {
                "Source": "trange.src",
                "DetailType": "InRange",
                "Detail": json.dumps({"marker": "in"}),
                "EventBusName": bus_name,
            }
        ]
    )
    now = time.time()
    # Drain the live delivery from PutEvents so the assertion below isolates
    # whether StartReplay dispatched anything outside the requested window.
    sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)

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

    arch_name = f"dest-arch-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(ArchiveName=arch_name, EventSourceArn=bus_arn)
    eb.put_events(
        Entries=[
            {
                "Source": "dest.replay",
                "DetailType": "ReplayDelivery",
                "Detail": json.dumps({"check": "delivered"}),
                "EventBusName": bus_name,
            }
        ]
    )
    archive_arn = eb.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    rep_name = f"rep-dest-{_uuid_mod.uuid4().hex[:8]}"
    # Drain live delivery from the seed PutEvents call so the final assertion
    # proves StartReplay delivered a fresh event.
    sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
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


def test_eventbridge_replay_dispatch_uses_replay_region(eb, sqs):
    """Replay worker dispatches under the Region where StartReplay was called."""
    west = _events_client("us-west-2")
    west_sqs = _sqs_client("us-west-2")
    suffix = _uuid_mod.uuid4().hex[:8]
    bus_name = f"rp-region-bus-{suffix}"
    rule_name = f"rp-region-rule-{suffix}"
    source = f"region.replay.{suffix}"

    east_bus_arn = eb.create_event_bus(Name=bus_name)["EventBusArn"]
    west_bus_arn = west.create_event_bus(Name=bus_name)["EventBusArn"]
    assert east_bus_arn.endswith(f":event-bus/{bus_name}")
    assert west_bus_arn == f"arn:aws:events:us-west-2:000000000000:event-bus/{bus_name}"

    east_q_url = sqs.create_queue(QueueName=f"rp-region-east-{suffix}")["QueueUrl"]
    east_q_arn = sqs.get_queue_attributes(
        QueueUrl=east_q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    west_q_url = west_sqs.create_queue(QueueName=f"rp-region-west-{suffix}")["QueueUrl"]
    west_q_arn = west_sqs.get_queue_attributes(
        QueueUrl=west_q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    for client, target_arn in ((eb, east_q_arn), (west, west_q_arn)):
        client.put_rule(
            Name=rule_name,
            EventBusName=bus_name,
            EventPattern=json.dumps({"source": [source]}),
            State="ENABLED",
        )
        client.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[{"Id": "target", "Arn": target_arn}],
        )

    arch_name = f"rp-region-arch-{suffix}"
    west.create_archive(ArchiveName=arch_name, EventSourceArn=west_bus_arn)
    west.put_events(
        Entries=[
            {
                "Source": source,
                "DetailType": "ReplayRegion",
                "Detail": json.dumps({"region": "us-west-2"}),
                "EventBusName": bus_name,
            }
        ]
    )
    archive_arn = west.describe_archive(ArchiveName=arch_name)["ArchiveArn"]
    # Drain live delivery from the seed PutEvents call; the final assertion
    # should only observe the replay delivery.
    west_sqs.receive_message(QueueUrl=west_q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)

    west.start_replay(
        ReplayName=f"rp-region-{suffix}",
        EventSourceArn=archive_arn,
        EventStartTime=0,
        EventEndTime=time.time() + 3600,
        Destination={"Arn": west_bus_arn},
    )
    time.sleep(0.5)

    west_msgs = west_sqs.receive_message(
        QueueUrl=west_q_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
    )
    east_msgs = sqs.receive_message(
        QueueUrl=east_q_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
    )
    assert len(west_msgs.get("Messages", [])) >= 1
    assert east_msgs.get("Messages", []) == []


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
            f"{_ENDPOINT}/",
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
_REGION = "us-east-1"
_RULE_KEY = "default|unit-test-rule"
_STATE_KEY = (_ACCOUNT, _REGION, _RULE_KEY)
_DUMMY_TARGET = [{"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:dummy"}]

def _seed_rule(schedule="rate(1 minute)", state="ENABLED", region=_REGION):
    state_key = (_ACCOUNT, region, _RULE_KEY)
    _eb._rules._data[state_key] = {
        "Name": "unit-test-rule",
        "ScheduleExpression": schedule,
        "State": state,
        "EventBusName": "default",
        "Arn": f"arn:aws:events:{region}:000000000000:rule/unit-test-rule",
    }
    _eb._targets._data[state_key] = list(_DUMMY_TARGET)
    return state_key




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


def test_scheduler_restores_rule_region_while_dispatching(isolated_scheduler):
    from ministack.core.responses import get_region, set_request_region

    original_region = get_region()
    observed = []
    try:
        set_request_region("us-east-1")
        west_state_key = _seed_rule(region="us-west-2")
        _eb._targets._data[west_state_key] = [
            {
                "Id": "sfn",
                "Arn": "arn:aws:states:us-west-2:000000000000:stateMachine:scheduled",
            }
        ]
        _eb._rule_last_fired[west_state_key] = _eb._now_ts() - 65
        isolated_scheduler.side_effect = (
            lambda _target, event, _rule, _view=None: observed.append(
                (_eb.get_region(), event["Region"]))
        )

        _eb._tick_scheduled_rules()

        assert observed == [("us-west-2", "us-west-2")]
        assert get_region() == "us-east-1"
    finally:
        set_request_region(original_region)


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
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 * * ? *)")
    after = _dt(2024, 1, 1, 11, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 1, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_next_fire_wraps_to_next_day():
    """cron(0 12 * * ? *): after noon, next occurrence is noon tomorrow."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 * * ? *)")
    after = _dt(2024, 1, 1, 12, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 2, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_next_fire_weekday():
    """cron(0 0 ? * MON-FRI *): after Friday 23:00, next is Monday 00:00."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
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
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 0 L * ? *)")
    after = _dt(2024, 1, 30, 12, 0, tzinfo=_tz.utc)
    # Jan has 31 days
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 1, 31, 0, 0, tzinfo=_tz.utc)
    # Feb 2024 (leap year) has 29 days
    after = _dt(2024, 2, 1, 0, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 2, 29, 0, 0, tzinfo=_tz.utc)


def test_scheduler_cron_last_weekday_of_month():
    """cron(0 0 LW * ? *): last Mon-Fri of the month."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 0 LW * ? *)")
    # March 2024: 31st = Sunday → last weekday is Fri Mar 29.
    after = _dt(2024, 3, 1, 0, 0, tzinfo=_tz.utc)
    assert _eb._cron_next_fire(fields, after) == _dt(2024, 3, 29, 0, 0, tzinfo=_tz.utc)


def test_scheduler_cron_nearest_weekday():
    """cron(0 12 15W * ? *): nearest Mon-Fri to the 15th, never crossing month."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 15W * ? *)")
    # Jan 15 2024 = Monday → fires on the 15th itself.
    assert _eb._cron_next_fire(fields, _dt(2024, 1, 14, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 1, 15, 12, 0, tzinfo=_tz.utc)
    # Jun 15 2024 = Saturday → fires on Friday Jun 14.
    assert _eb._cron_next_fire(fields, _dt(2024, 6, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 6, 14, 12, 0, tzinfo=_tz.utc)
    # Sep 15 2024 = Sunday → fires on Monday Sep 16.
    assert _eb._cron_next_fire(fields, _dt(2024, 9, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 9, 16, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_last_dow_of_month():
    """cron(0 12 ? * 6L *): last Friday of the month (AWS Friday = 6)."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
    fields = _eb._parse_cron_fields("cron(0 12 ? * 6L *)")
    # Jan 2024: Fridays are 5, 12, 19, 26 → last is Fri Jan 26.
    assert _eb._cron_next_fire(fields, _dt(2024, 1, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 1, 26, 12, 0, tzinfo=_tz.utc)
    # Mar 2024: Fridays are 1, 8, 15, 22, 29 → last is Fri Mar 29.
    assert _eb._cron_next_fire(fields, _dt(2024, 3, 1, 0, 0, tzinfo=_tz.utc)) == _dt(2024, 3, 29, 12, 0, tzinfo=_tz.utc)


def test_scheduler_cron_nth_dow_of_month():
    """cron(0 9 ? * 2#1 *): first Monday of every month (AWS Monday = 2)."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz
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


# -- EventBridge → FIFO SQS target requires MessageGroupId --------------


def test_eventbridge_dispatch_to_fifo_sqs_stamps_message_group_id(eb, sqs):
    """When a rule's target is a FIFO SQS queue, EventBridge must read
    SqsParameters.MessageGroupId from the target spec and stamp it on the
    delivered message. Before this fix MS dropped MessageGroupId at
    dispatch, so FIFO targets received messages with no group_id."""
    q_url = sqs.create_queue(
        QueueName=f"intg-eb-fifo-{_uuid_mod.uuid4().hex[:8]}.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    rule_name = f"intg-eb-fifo-rule-{_uuid_mod.uuid4().hex[:8]}"
    eb.put_rule(Name=rule_name, EventPattern=json.dumps({"source": ["app.test"]}))
    eb.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": "1",
            "Arn": q_arn,
            "SqsParameters": {"MessageGroupId": "orders"},
        }],
    )
    eb.put_events(Entries=[{
        "Source": "app.test",
        "DetailType": "Order",
        "Detail": json.dumps({"orderId": "o1"}),
    }])

    # FIFO queues require MessageGroupId; ReceiveMessage with the attribute name
    # surfaces it.
    time.sleep(0.5)
    resp = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=10,
        AttributeNames=["MessageGroupId"],
    )
    msgs = resp.get("Messages") or []
    assert msgs, "FIFO queue received no messages from EventBridge"
    attrs = msgs[0].get("Attributes", {})
    assert attrs.get("MessageGroupId") == "orders"


# ── anything-but with nested content filters (#849) ──────────────────


def _eb_rule_to_queue(eb, sqs, slug, pattern, bus):
    """Helper: a queue plus an ENABLED rule on ``bus`` filtering on ``pattern``
    and targeting it. Returns the queue URL."""
    q_url = sqs.create_queue(QueueName=f"qa-eb-{slug}-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    rule = f"qa-eb-{slug}-rule"
    eb.put_rule(Name=rule, EventBusName=bus, State="ENABLED",
                EventPattern=json.dumps(pattern))
    eb.put_targets(Rule=rule, EventBusName=bus, Targets=[{"Id": "t1", "Arn": q_arn}])
    return q_url


def _eb_setup_anybut_rule(eb, sqs, suffix, pattern_value):
    """Helper: create bus + queue + rule with a given anything-but pattern."""
    bus = f"qa-eb-anybut-{suffix}-bus"
    eb.create_event_bus(Name=bus)
    return bus, _eb_rule_to_queue(
        eb, sqs, f"anybut-{suffix}",
        {"source": ["myapp"], "detail": {"id": [{"anything-but": pattern_value}]}}, bus)


def _eb_send(eb, bus, id_value):
    eb.put_events(Entries=[{
        "Source": "myapp", "DetailType": "t",
        "Detail": json.dumps({"id": id_value}),
        "EventBusName": bus,
    }])


def test_eventbridge_anything_but_prefix_excludes_matching(eb, sqs):
    bus, q_url = _eb_setup_anybut_rule(eb, sqs, "prefix", {"prefix": "TEST-"})
    _eb_send(eb, bus, "PROD-42")   # does not start with TEST- → should deliver
    _eb_send(eb, bus, "TEST-99")   # excluded by prefix → should NOT deliver
    msgs = sqs.receive_message(
        QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)["Messages"]
    ids = [json.loads(m["Body"])["detail"]["id"] for m in msgs]
    assert ids == ["PROD-42"]


def test_eventbridge_anything_but_suffix_excludes_matching(eb, sqs):
    bus, q_url = _eb_setup_anybut_rule(eb, sqs, "suffix", {"suffix": "-OLD"})
    _eb_send(eb, bus, "ITEM-NEW")   # no -OLD suffix → deliver
    _eb_send(eb, bus, "ITEM-OLD")   # excluded by suffix → skip
    msgs = sqs.receive_message(
        QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)["Messages"]
    ids = [json.loads(m["Body"])["detail"]["id"] for m in msgs]
    assert ids == ["ITEM-NEW"]


def test_eventbridge_anything_but_wildcard_excludes_matching(eb, sqs):
    bus, q_url = _eb_setup_anybut_rule(eb, sqs, "wildcard", {"wildcard": "*-test-*"})
    _eb_send(eb, bus, "prod-app-1")     # no -test- → deliver
    _eb_send(eb, bus, "abc-test-xyz")   # matches wildcard → skip
    msgs = sqs.receive_message(
        QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)["Messages"]
    ids = [json.loads(m["Body"])["detail"]["id"] for m in msgs]
    assert ids == ["prod-app-1"]


# ---------------------------------------------------------------------------
# Reserved input-transformer variables
# ---------------------------------------------------------------------------

def test_eventbridge_input_transformer_event_json(eb, sqs):
    """<aws.events.event.json> embeds the full event envelope as a raw JSON object."""
    bus_name = "qa-eb-reserved-evtjson-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-reserved-evtjson-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-reserved-evtjson-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.reserved"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-reserved-evtjson-rule",
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {},
                    "InputTemplate": '{"sourceEvent": <aws.events.event.json>}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp.reserved",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"foo": "bar"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["sourceEvent"]["source"] == "myapp.reserved"
    assert body["sourceEvent"]["detail-type"] == "TestEvent"
    assert body["sourceEvent"]["detail"] == {"foo": "bar"}
    assert body["sourceEvent"]["version"] == "0"


def test_eventbridge_input_transformer_event_escaped(eb, sqs):
    """<aws.events.event> embeds the event as a JSON object with the detail field removed."""
    bus_name = "qa-eb-reserved-evtesc-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-reserved-evtesc-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-reserved-evtesc-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.escaped"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-reserved-evtesc-rule",
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {},
                    "InputTemplate": '{"evt": <aws.events.event>}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp.escaped",
                "DetailType": "EscTest",
                "Detail": json.dumps({"x": 1}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    # <aws.events.event> renders a JSON object (not an escaped string) with detail removed
    assert isinstance(body["evt"], dict)
    assert body["evt"]["source"] == "myapp.escaped"
    assert body["evt"]["detail-type"] == "EscTest"
    assert body["evt"]["version"] == "0"
    assert "detail" not in body["evt"]


def test_eventbridge_input_transformer_renders_the_event_it_would_have_sent(eb, sqs):
    """``<aws.events.event.json>`` is the event, not a paraphrase of it: AWS
    substitutes the same envelope the target would have received without the
    transformer. Building a second copy for the substitution is how ``time`` came
    to render as the raw epoch second the entry carried while the plain target
    beside it received the ISO-8601 string — a difference invisible until a
    consumer parses the timestamp. Both targets are on one rule, so the two
    payloads describe the same delivery of the same event."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bus_name = f"qa-eb-one-envelope-bus-{suffix}"
    rule_name = f"qa-eb-one-envelope-rule-{suffix}"
    eb.create_event_bus(Name=bus_name)
    plain_url = sqs.create_queue(QueueName=f"qa-eb-one-envelope-plain-{suffix}")["QueueUrl"]
    tr_url = sqs.create_queue(QueueName=f"qa-eb-one-envelope-tr-{suffix}")["QueueUrl"]
    arn_of = lambda url: sqs.get_queue_attributes(  # noqa: E731
        QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(Name=rule_name, EventBusName=bus_name, State="ENABLED",
                EventPattern=json.dumps({"source": ["myapp.one-envelope"]}))
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[
            {"Id": "plain", "Arn": arn_of(plain_url)},
            {"Id": "transformed", "Arn": arn_of(tr_url),
             "InputTransformer": {"InputPathsMap": {},
                                  "InputTemplate": '{"sent": <aws.events.event.json>}'}},
        ],
    )
    eb.put_events(Entries=[{"Source": "myapp.one-envelope", "DetailType": "T",
                            "Detail": json.dumps({"k": "v"}),
                            "EventBusName": bus_name}])
    plain = json.loads(sqs.receive_message(
        QueueUrl=plain_url, MaxNumberOfMessages=1,
        WaitTimeSeconds=2)["Messages"][0]["Body"])
    rendered = json.loads(sqs.receive_message(
        QueueUrl=tr_url, MaxNumberOfMessages=1,
        WaitTimeSeconds=2)["Messages"][0]["Body"])["sent"]
    assert rendered == plain, (rendered, plain)
    # The field the two copies actually disagreed on, named so a regression
    # reports the cause rather than a whole-envelope inequality.
    assert rendered["time"] == plain["time"]
    assert isinstance(plain["time"], str) and plain["time"].endswith("Z")


def test_eventbridge_input_transformer_rule_name_and_arn(eb, sqs):
    """<aws.events.rule-name> and <aws.events.rule-arn> resolve to the rule's Name and Arn."""
    bus_name = "qa-eb-reserved-rn-bus"
    rule_name = "qa-eb-reserved-rn-rule"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-reserved-rn-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    put_rule_resp = eb.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.rname"]}),
        State="ENABLED",
    )
    expected_rule_arn = put_rule_resp["RuleArn"]
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {},
                    "InputTemplate": '{"rn": "<aws.events.rule-name>", "ra": "<aws.events.rule-arn>"}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp.rname",
                "DetailType": "RnTest",
                "Detail": "{}",
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["rn"] == rule_name
    assert body["ra"] == expected_rule_arn


def test_eventbridge_input_transformer_setdefault_precedence(eb, sqs):
    """An explicit InputPathsMap entry named like a reserved var must win over the reserved value."""
    bus_name = "qa-eb-reserved-prec-bus"
    rule_name = "qa-eb-reserved-prec-rule"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-reserved-prec-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.prec"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    # explicitly map the reserved key name to $.source — must win
                    "InputPathsMap": {"aws.events.rule-name": "$.source"},
                    "InputTemplate": '{"rn": "<aws.events.rule-name>"}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp.prec",
                "DetailType": "PrecTest",
                "Detail": "{}",
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    # explicit InputPathsMap maps $.source → "myapp.prec", not the rule name
    assert body["rn"] == "myapp.prec"


def test_eventbridge_input_transformer_ingestion_time(eb, sqs):
    """<aws.events.event.ingestion-time> is substituted with a non-empty time string."""
    bus_name = "qa-eb-reserved-itime-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-reserved-itime-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-reserved-itime-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.itime"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-reserved-itime-rule",
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {},
                    "InputTemplate": '{"it": "<aws.events.event.ingestion-time>"}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp.itime",
                "DetailType": "ItTest",
                "Detail": "{}",
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    # substituted (not the literal placeholder) and non-empty; time is the event's stored value
    assert body["it"] != "<aws.events.event.ingestion-time>"
    assert body["it"] != ""


# ---------------------------------------------------------------------------
# API destination dispatch
# ---------------------------------------------------------------------------

def _start_api_dest_capture_server(status_plan=None):
    """Local HTTPS-endpoint stand-in for an API destination. Captures every
    request and answers with the next status in ``status_plan`` (the last one
    repeats). Returns (server, captured)."""
    import threading as _threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    captured = []
    plan = list(status_plan or [200])

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            captured.append({
                "method": self.command,
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
            })
            status = plan.pop(0) if len(plan) > 1 else plan[0]
            payload = b"{}"
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_POST = _handle
        do_PUT = _handle
        do_GET = _handle
        do_DELETE = _handle

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    _threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, captured


def _start_oauth_issuer(tokens=("tok-1",), expires_in=3600):
    """Minimal client-credentials issuer: captures every token request and
    hands out tokens from ``tokens`` in order (the last one repeats)."""
    import threading as _threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs

    token_requests = []
    remaining = list(tokens)

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            token_requests.append({
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "form": {k: v[0] for k, v in parse_qs(body, keep_blank_values=True).items()},
            })
            token = remaining.pop(0) if len(remaining) > 1 else remaining[0]
            payload = json.dumps(
                {"access_token": token, "token_type": "bearer", "expires_in": expires_in}
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    _threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, token_requests


def _wait_until(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _api_dest_pipeline(eb, slug, endpoint, auth_type, auth_params, target_extras=None, http_method="POST"):
    """Create bus → rule → connection → API destination → target for one test."""
    bus_name = f"qa-eb-apidest-{slug}-bus"
    source = f"myapp.apidest.{slug}"
    eb.create_event_bus(Name=bus_name)
    conn = eb.create_connection(
        Name=f"qa-eb-apidest-{slug}-conn",
        AuthorizationType=auth_type,
        AuthParameters=auth_params,
    )
    dest = eb.create_api_destination(
        Name=f"qa-eb-apidest-{slug}-dest",
        ConnectionArn=conn["ConnectionArn"],
        InvocationEndpoint=endpoint,
        HttpMethod=http_method,
    )
    eb.put_rule(
        Name=f"qa-eb-apidest-{slug}-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": [source]}),
        State="ENABLED",
    )
    target = {"Id": "t1", "Arn": dest["ApiDestinationArn"]}
    target.update(target_extras or {})
    eb.put_targets(Rule=f"qa-eb-apidest-{slug}-rule", EventBusName=bus_name, Targets=[target])
    return bus_name, source


def test_eventbridge_api_destination_basic_auth_full_envelope(eb):
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "basic",
            f"http://127.0.0.1:{port}/hooks/ingest",
            "BASIC",
            {"BasicAuthParameters": {"Username": "u", "Password": "p"}},
        )
        resp = eb.put_events(Entries=[{
            "Source": source,
            "DetailType": "UserSignup",
            "Detail": json.dumps({"userId": "u-1"}),
            "EventBusName": bus_name,
        }])
        assert resp["FailedEntryCount"] == 0

        assert _wait_until(lambda: len(captured) >= 1)
        req = captured[0]
        assert req["method"] == "POST"
        assert req["path"] == "/hooks/ingest"
        # base64("u:p")
        assert req["headers"]["authorization"] == "Basic dTpw"
        assert req["headers"]["content-type"] == "application/json; charset=utf-8"
        assert req["headers"]["user-agent"] == "Amazon/EventBridge/ApiDestinations"
        assert req["headers"]["range"] == "bytes=0-1048575"
        body = json.loads(req["body"])
        assert body["version"] == "0"
        assert body["source"] == source
        assert body["detail-type"] == "UserSignup"
        assert body["detail"] == {"userId": "u-1"}
    finally:
        server.shutdown()


def test_eventbridge_api_destination_input_path_detail(eb):
    """InputPath $.detail delivers exactly the event payload — the webhook body shape."""
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "inputpath",
            f"http://127.0.0.1:{port}/webhook",
            "API_KEY",
            {"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k-123"}},
            target_extras={"InputPath": "$.detail"},
        )
        detail = {"orderId": "o-1", "amount": 42}
        eb.put_events(Entries=[{
            "Source": source,
            "DetailType": "OrderPlaced",
            "Detail": json.dumps(detail),
            "EventBusName": bus_name,
        }])

        assert _wait_until(lambda: len(captured) >= 1)
        req = captured[0]
        assert req["headers"]["x-api-key"] == "k-123"
        assert json.loads(req["body"]) == detail
    finally:
        server.shutdown()


def test_eventbridge_api_destination_parameter_merging_connection_precedence(eb):
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "params",
            f"http://127.0.0.1:{port}/hooks/*/notify",
            "API_KEY",
            {
                "ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k-9"},
                "InvocationHttpParameters": {
                    "HeaderParameters": [
                        {"Key": "X-Env", "Value": "conn"},
                        # Non-overridable in real AWS — must be dropped, not honored.
                        {"Key": "User-Agent", "Value": "evil"},
                    ],
                    "QueryStringParameters": [{"Key": "tenant", "Value": "conn-t"}],
                    "BodyParameters": [{"Key": "injected", "Value": "yes"}],
                },
            },
            target_extras={
                "HttpParameters": {
                    "HeaderParameters": {"X-Env": "target", "X-Target-Only": "t"},
                    "QueryStringParameters": {"tenant": "target-t", "extra": "1"},
                    "PathParameterValues": ["partner-1"],
                }
            },
        )
        eb.put_events(Entries=[{
            "Source": source,
            "DetailType": "Ping",
            "Detail": json.dumps({"k": "v"}),
            "EventBusName": bus_name,
        }])

        assert _wait_until(lambda: len(captured) >= 1)
        req = captured[0]
        path, _, query = req["path"].partition("?")
        assert path == "/hooks/partner-1/notify"
        from urllib.parse import parse_qs
        params = {k: v[0] for k, v in parse_qs(query).items()}
        # Connection values win over target values; target-only keys survive.
        assert params == {"tenant": "conn-t", "extra": "1"}
        assert req["headers"]["x-env"] == "conn"
        assert req["headers"]["x-target-only"] == "t"
        assert req["headers"]["user-agent"] == "Amazon/EventBridge/ApiDestinations"
        body = json.loads(req["body"])
        assert body["injected"] == "yes"
        assert body["detail"] == {"k": "v"}
    finally:
        server.shutdown()


def test_eventbridge_api_destination_oauth_client_credentials(eb):
    issuer, token_requests = _start_oauth_issuer(tokens=("tok-1",))
    server, captured = _start_api_dest_capture_server()
    try:
        issuer_port = issuer.server_address[1]
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "oauth",
            f"http://127.0.0.1:{port}/secured",
            "OAUTH_CLIENT_CREDENTIALS",
            {
                "OAuthParameters": {
                    "AuthorizationEndpoint": f"http://127.0.0.1:{issuer_port}/oauth/token",
                    "HttpMethod": "POST",
                    "ClientParameters": {"ClientID": "cid", "ClientSecret": "csec"},
                    "OAuthHttpParameters": {
                        "BodyParameters": [{"Key": "audience", "Value": "https://api.example.test"}]
                    },
                }
            },
        )
        entry = {
            "Source": source,
            "DetailType": "Ping",
            "Detail": json.dumps({"n": 1}),
            "EventBusName": bus_name,
        }
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        # Second event rides the cached token — the issuer is not called again.
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 2)

        assert len(token_requests) == 1
        token_req = token_requests[0]
        assert token_req["path"] == "/oauth/token"
        assert token_req["headers"]["content-type"] == "application/x-www-form-urlencoded"
        assert token_req["form"]["grant_type"] == "client_credentials"
        assert token_req["form"]["client_id"] == "cid"
        assert token_req["form"]["client_secret"] == "csec"
        assert token_req["form"]["audience"] == "https://api.example.test"
        assert captured[0]["headers"]["authorization"] == "Bearer tok-1"
        assert captured[1]["headers"]["authorization"] == "Bearer tok-1"
    finally:
        issuer.shutdown()
        server.shutdown()


def _oauth_params(issuer_port, client_secret="csec"):
    return {
        "OAuthParameters": {
            "AuthorizationEndpoint": f"http://127.0.0.1:{issuer_port}/oauth/token",
            "HttpMethod": "POST",
            "ClientParameters": {"ClientID": "cid", "ClientSecret": client_secret},
            "OAuthHttpParameters": {
                "BodyParameters": [{"Key": "grant_type", "Value": "client_credentials"}]
            },
        }
    }


def _oauth_entry(bus_name, source):
    return {
        "Source": source,
        "DetailType": "Ping",
        "Detail": json.dumps({"n": 1}),
        "EventBusName": bus_name,
    }


def test_eventbridge_api_destination_oauth_token_dies_with_the_connection(eb):
    """A recreated connection re-authorizes: the token cache is keyed by name,
    and names are reusable, so a delete must not leave a token behind for the
    next connection to inherit. Terraform replacing a connection in place is
    the everyday way to hit this."""
    issuer, token_requests = _start_oauth_issuer(tokens=("tok-1", "tok-2"))
    server, captured = _start_api_dest_capture_server()
    try:
        issuer_port = issuer.server_address[1]
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "oauthrecreate",
            f"http://127.0.0.1:{port}/secured",
            "OAUTH_CLIENT_CREDENTIALS",
            _oauth_params(issuer_port),
        )
        entry = _oauth_entry(bus_name, source)
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        assert len(token_requests) == 1

        # Same name, so the API destination's ConnectionArn still resolves.
        conn_name = "qa-eb-apidest-oauthrecreate-conn"
        eb.delete_connection(Name=conn_name)
        eb.create_connection(
            Name=conn_name,
            AuthorizationType="OAUTH_CLIENT_CREDENTIALS",
            AuthParameters=_oauth_params(issuer_port, client_secret="rotated"),
        )

        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 2)
        assert len(token_requests) == 2
        assert token_requests[1]["form"]["client_secret"] == "rotated"
        assert captured[1]["headers"]["authorization"] == "Bearer tok-2"
    finally:
        issuer.shutdown()
        server.shutdown()


def test_eventbridge_api_destination_oauth_token_evicted_on_reauthorization(eb):
    """Rotating the client secret must take effect on the next invocation, not
    whenever the old token happens to expire."""
    issuer, token_requests = _start_oauth_issuer(tokens=("tok-1", "tok-2"))
    server, captured = _start_api_dest_capture_server()
    try:
        issuer_port = issuer.server_address[1]
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "oauthrotate",
            f"http://127.0.0.1:{port}/secured",
            "OAUTH_CLIENT_CREDENTIALS",
            _oauth_params(issuer_port),
        )
        entry = _oauth_entry(bus_name, source)
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        assert len(token_requests) == 1

        eb.update_connection(
            Name="qa-eb-apidest-oauthrotate-conn",
            AuthorizationType="OAUTH_CLIENT_CREDENTIALS",
            AuthParameters=_oauth_params(issuer_port, client_secret="rotated"),
        )

        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 2)
        assert len(token_requests) == 2
        assert token_requests[1]["form"]["client_secret"] == "rotated"
        assert captured[1]["headers"]["authorization"] == "Bearer tok-2"
    finally:
        issuer.shutdown()
        server.shutdown()


def test_eventbridge_api_destination_oauth_token_survives_metadata_update(eb):
    """The mirror image: a description-only update does not re-authorize, so
    the cached token stays and no needless exchange is made."""
    issuer, token_requests = _start_oauth_issuer(tokens=("tok-1", "tok-2"))
    server, captured = _start_api_dest_capture_server()
    try:
        issuer_port = issuer.server_address[1]
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "oauthdescr",
            f"http://127.0.0.1:{port}/secured",
            "OAUTH_CLIENT_CREDENTIALS",
            _oauth_params(issuer_port),
        )
        entry = _oauth_entry(bus_name, source)
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)

        eb.update_connection(Name="qa-eb-apidest-oauthdescr-conn", Description="renamed")

        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 2)
        assert len(token_requests) == 1
        assert captured[1]["headers"]["authorization"] == "Bearer tok-1"
    finally:
        issuer.shutdown()
        server.shutdown()


def test_eventbridge_api_destination_oauth_refresh_on_401(eb):
    issuer, token_requests = _start_oauth_issuer(tokens=("tok-old", "tok-new"))
    server, captured = _start_api_dest_capture_server(status_plan=[401, 200])
    try:
        issuer_port = issuer.server_address[1]
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "oauth401",
            f"http://127.0.0.1:{port}/secured",
            "OAUTH_CLIENT_CREDENTIALS",
            {
                "OAuthParameters": {
                    "AuthorizationEndpoint": f"http://127.0.0.1:{issuer_port}/oauth/token",
                    "HttpMethod": "POST",
                    "ClientParameters": {"ClientID": "cid", "ClientSecret": "csec"},
                }
            },
        )
        eb.put_events(Entries=[{
            "Source": source,
            "DetailType": "Ping",
            "Detail": json.dumps({"n": 1}),
            "EventBusName": bus_name,
        }])

        # 401 → token refresh → one retry with the new token.
        assert _wait_until(lambda: len(captured) >= 2)
        assert captured[0]["headers"]["authorization"] == "Bearer tok-old"
        assert captured[1]["headers"]["authorization"] == "Bearer tok-new"
        assert len(token_requests) == 2
    finally:
        issuer.shutdown()
        server.shutdown()


# ---------------------------------------------------------------------------
# API-destination outbound-HTTP hardening regression tests
# ---------------------------------------------------------------------------

def _start_redirecting_server(location):
    """A stand-in endpoint that answers every request with 302 -> location."""
    import threading as _threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    seen = []

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            seen.append({"headers": {k.lower(): v for k, v in self.headers.items()}})
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_POST = _handle
        do_PUT = _handle
        do_GET = _handle

        def log_message(self, _f, *_a):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    _threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, seen


def test_eventbridge_api_destination_does_not_follow_redirect(eb):
    """A 3xx must not be followed — urllib would carry the Authorization header
    to the redirect target, leaking credentials. The secondary endpoint must
    receive nothing."""
    secondary, secondary_captured = _start_api_dest_capture_server()
    try:
        secondary_port = secondary.server_address[1]
        primary, primary_seen = _start_redirecting_server(
            f"http://127.0.0.1:{secondary_port}/stolen"
        )
        try:
            primary_port = primary.server_address[1]
            bus_name, source = _api_dest_pipeline(
                eb,
                "redirect",
                f"http://127.0.0.1:{primary_port}/hook",
                "BASIC",
                {"BasicAuthParameters": {"Username": "svcuser", "Password": "hunter2"}},
            )
            eb.put_events(Entries=[{
                "Source": source, "DetailType": "Ping",
                "Detail": json.dumps({"n": 1}), "EventBusName": bus_name,
            }])
            # The primary must actually be hit, or "the secondary saw nothing"
            # is vacuously true and this stops guarding anything.
            assert _wait_until(lambda: len(primary_seen) >= 1)
            # Then give the redirect time to be followed; it must not be.
            _wait_until(lambda: len(secondary_captured) >= 1, timeout=2.0)
            assert secondary_captured == []
        finally:
            primary.shutdown()
    finally:
        secondary.shutdown()


def test_eventbridge_api_destination_rejects_non_http_endpoint(eb):
    """file://, ftp://, gopher://, and non-URLs are rejected at create/update;
    http(s) endpoints are accepted."""
    conn = eb.create_connection(
        Name=f"qa-eb-scheme-conn-{_uuid_mod.uuid4().hex[:8]}",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k"}},
    )
    conn_arn = conn["ConnectionArn"]
    for bad in ("file:///etc/passwd", "ftp://127.0.0.1/x", "gopher://x/", "not a url at all"):
        with pytest.raises(ClientError) as exc:
            eb.create_api_destination(
                Name=f"qa-eb-scheme-{_uuid_mod.uuid4().hex[:8]}",
                ConnectionArn=conn_arn,
                InvocationEndpoint=bad,
                HttpMethod="POST",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
        assert "InvocationEndpoint" in exc.value.response["Error"]["Message"]

    good_name = f"qa-eb-scheme-ok-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_api_destination(
        Name=good_name, ConnectionArn=conn_arn,
        InvocationEndpoint="https://example.test/hook", HttpMethod="POST",
    )
    with pytest.raises(ClientError) as exc:
        eb.update_api_destination(Name=good_name, InvocationEndpoint="file:///etc/shadow")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_eventbridge_api_destination_api_key_cannot_override_reserved_header(eb):
    """An ApiKeyName colliding with a reserved header (User-Agent) must not
    override EventBridge's fixed value at delivery time."""
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "hdrinject",
            f"http://127.0.0.1:{port}/hook",
            "API_KEY",
            {"ApiKeyAuthParameters": {"ApiKeyName": "User-Agent", "ApiKeyValue": "pwned-agent"}},
        )
        eb.put_events(Entries=[{
            "Source": source, "DetailType": "Ping",
            "Detail": json.dumps({"n": 1}), "EventBusName": bus_name,
        }])
        assert _wait_until(lambda: len(captured) >= 1)
        assert captured[0]["headers"]["user-agent"] == "Amazon/EventBridge/ApiDestinations"
    finally:
        server.shutdown()


def test_eventbridge_api_destination_endpoint_scheme_is_case_insensitive(eb):
    """URL schemes are case-insensitive (RFC 3986) and the outbound opener
    lowercases before comparing, so ``HTTPS://`` must be accepted at the API —
    rejecting it would fail a destination the emulator can in fact deliver to."""
    conn = eb.create_connection(
        Name=f"qa-eb-scheme-case-conn-{_uuid_mod.uuid4().hex[:8]}",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k"}},
    )
    name = f"qa-eb-scheme-case-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_api_destination(
        Name=name, ConnectionArn=conn["ConnectionArn"],
        InvocationEndpoint="HTTPS://api.example.test/hook", HttpMethod="POST",
    )
    assert eb.describe_api_destination(Name=name)["InvocationEndpoint"] == \
        "HTTPS://api.example.test/hook"
    eb.update_api_destination(Name=name, InvocationEndpoint="Http://api.example.test/hook2")
    assert eb.describe_api_destination(Name=name)["InvocationEndpoint"] == \
        "Http://api.example.test/hook2"


def test_eventbridge_api_destination_rejects_endpoint_without_host(eb):
    """An endpoint with no host is not dialable. Storing it only moves the
    failure to a background delivery thread, where the caller never sees it.
    ``http://@`` and ``http://:80`` carry a non-empty authority but still no
    host, so an authority check alone is not enough."""
    conn = eb.create_connection(
        Name=f"qa-eb-scheme-host-conn-{_uuid_mod.uuid4().hex[:8]}",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k"}},
    )
    conn_arn = conn["ConnectionArn"]
    for bad in ("http://", "https://", "http:///hook", "https://?q=1",
                "http://@", "http://:80", "http://[::1/hook",
                # urlsplit strips CR/LF/TAB before reporting a hostname
                # (bpo-43882), so these clear a host check but reach connect
                # as something the caller never wrote — "http://ho\nst/hook"
                # resolves to "host", and the CRLF form to "hostx-injected".
                "http://ho\nst/hook", "http://host\t/hook",
                "http://host\r\nX-Injected: 1/hook", "http://ho st/hook"):
        with pytest.raises(ClientError) as exc:
            eb.create_api_destination(
                Name=f"qa-eb-scheme-host-{_uuid_mod.uuid4().hex[:8]}",
                ConnectionArn=conn_arn,
                InvocationEndpoint=bad,
                HttpMethod="POST",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
        assert "InvocationEndpoint" in exc.value.response["Error"]["Message"]

    name = f"qa-eb-scheme-host-ok-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_api_destination(
        Name=name, ConnectionArn=conn_arn,
        InvocationEndpoint="https://api.example.test/hook", HttpMethod="POST",
    )
    with pytest.raises(ClientError) as exc:
        eb.update_api_destination(Name=name, InvocationEndpoint="http://")
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert eb.describe_api_destination(Name=name)["InvocationEndpoint"] == \
        "https://api.example.test/hook"


def test_eventbridge_connection_rejects_non_http_oauth_endpoint(eb):
    """CreateConnection POSTs the client_id/client_secret to the OAuth
    AuthorizationEndpoint, so a non-http(s) one is rejected at the API instead
    of surfacing much later as a log line on a delivery worker."""
    for bad in ("file:///etc/passwd", "ftp://127.0.0.1/token", "https://", "nope"):
        with pytest.raises(ClientError) as exc:
            eb.create_connection(
                Name=f"qa-eb-oauth-scheme-{_uuid_mod.uuid4().hex[:8]}",
                AuthorizationType="OAUTH_CLIENT_CREDENTIALS",
                AuthParameters={"OAuthParameters": {
                    "AuthorizationEndpoint": bad,
                    "HttpMethod": "POST",
                    "ClientParameters": {"ClientID": "cid", "ClientSecret": "csecret"},
                }},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
        assert "AuthorizationEndpoint" in exc.value.response["Error"]["Message"]

    # A connection with no OAuth parameters at all is not an endpoint carrier
    # and must stay creatable.
    api_key_name = f"qa-eb-oauth-scheme-apikey-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_connection(
        Name=api_key_name,
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k"}},
    )
    assert eb.describe_connection(Name=api_key_name)["ConnectionState"] == "AUTHORIZED"


def test_eventbridge_connection_update_rejects_non_http_oauth_endpoint(eb):
    """UpdateConnection can repoint the token request at a new URL, so it gets
    the same check — and a rejected update must leave the stored endpoint
    untouched. A description-only update carries no endpoint and is unaffected."""
    name = f"qa-eb-oauth-update-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_connection(
        Name=name,
        AuthorizationType="OAUTH_CLIENT_CREDENTIALS",
        AuthParameters={"OAuthParameters": {
            "AuthorizationEndpoint": "https://idp.example.test/token",
            "HttpMethod": "POST",
            "ClientParameters": {"ClientID": "cid", "ClientSecret": "csecret"},
        }},
    )
    with pytest.raises(ClientError) as exc:
        eb.update_connection(
            Name=name,
            AuthParameters={"OAuthParameters": {
                "AuthorizationEndpoint": "file:///etc/passwd",
                "HttpMethod": "POST",
                "ClientParameters": {"ClientID": "cid", "ClientSecret": "csecret"},
            }},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "AuthorizationEndpoint" in exc.value.response["Error"]["Message"]

    stored = eb.describe_connection(Name=name)["AuthParameters"]["OAuthParameters"]
    assert stored["AuthorizationEndpoint"] == "https://idp.example.test/token"

    eb.update_connection(Name=name, Description="still fine")
    assert eb.describe_connection(Name=name)["Description"] == "still fine"


def test_eventbridge_deauthorized_connection_stops_delivering_credentials(eb):
    """DeauthorizeConnection "removes all authorization parameters from the
    connection", so the credentials must stop going out. Deauthorizing used to
    flip the state and evict the cached OAuth token while leaving
    AuthParameters in place, so every later delivery still presented the same
    Basic credentials to the endpoint."""
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "deauth",
            f"http://127.0.0.1:{port}/hook",
            "BASIC",
            {"BasicAuthParameters": {"Username": "svcuser", "Password": "hunter2"}},
        )
        entry = {"Source": source, "DetailType": "Ping",
                 "Detail": json.dumps({"n": 1}), "EventBusName": bus_name}

        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        assert captured[0]["headers"]["authorization"].startswith("Basic ")

        conn_name = "qa-eb-apidest-deauth-conn"
        assert eb.deauthorize_connection(
            Name=conn_name)["ConnectionState"] == "DEAUTHORIZED"
        assert eb.describe_connection(Name=conn_name).get("AuthParameters", {}) == {}

        captured.clear()
        eb.put_events(Entries=[entry])
        # Give delivery time to run; nothing must arrive.
        _wait_until(lambda: len(captured) >= 1, timeout=2.0)
        assert captured == []
    finally:
        server.shutdown()


def test_eventbridge_reauthorized_connection_delivers_again(eb):
    """Deauthorizing is not a one-way door, and the not-AUTHORIZED delivery gate
    must not be indistinguishable from breaking the connection for good.
    UpdateConnection supplies fresh credentials and returns the connection to
    AUTHORIZED, so the destination delivers again — presenting the *new*
    credentials, not the ones cleared by the deauthorize."""
    server, captured = _start_api_dest_capture_server()
    try:
        port = server.server_address[1]
        bus_name, source = _api_dest_pipeline(
            eb,
            "reauth",
            f"http://127.0.0.1:{port}/hook",
            "BASIC",
            {"BasicAuthParameters": {"Username": "olduser", "Password": "oldpass"}},
        )
        entry = {"Source": source, "DetailType": "Ping",
                 "Detail": json.dumps({"n": 1}), "EventBusName": bus_name}
        conn_name = "qa-eb-apidest-reauth-conn"

        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        # base64("olduser:oldpass")
        assert captured[0]["headers"]["authorization"] == "Basic b2xkdXNlcjpvbGRwYXNz"

        eb.deauthorize_connection(Name=conn_name)
        captured.clear()
        eb.put_events(Entries=[entry])
        _wait_until(lambda: len(captured) >= 1, timeout=2.0)
        assert captured == []

        eb.update_connection(
            Name=conn_name,
            AuthorizationType="BASIC",
            AuthParameters={"BasicAuthParameters": {"Username": "newuser",
                                                    "Password": "newpass"}},
        )
        assert eb.describe_connection(Name=conn_name)["ConnectionState"] == "AUTHORIZED"

        captured.clear()
        eb.put_events(Entries=[entry])
        assert _wait_until(lambda: len(captured) >= 1)
        # base64("newuser:newpass") — the re-authorized credentials, not the old ones
        assert captured[0]["headers"]["authorization"] == "Basic bmV3dXNlcjpuZXdwYXNz"
    finally:
        server.shutdown()


def test_eventbridge_api_destination_rejects_unknown_http_method(eb):
    """HttpMethod rides the outbound request line, and AWS models it as an
    enum. It was stored unvalidated and silently became POST at delivery."""
    conn = eb.create_connection(
        Name=f"qa-eb-method-conn-{_uuid_mod.uuid4().hex[:8]}",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "k"}},
    )
    with pytest.raises(ClientError) as exc:
        eb.create_api_destination(
            Name=f"qa-eb-method-{_uuid_mod.uuid4().hex[:8]}",
            ConnectionArn=conn["ConnectionArn"],
            InvocationEndpoint="https://api.example.test/hook",
            HttpMethod="TRACE",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "httpMethod" in exc.value.response["Error"]["Message"]

    name = f"qa-eb-method-ok-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_api_destination(
        Name=name, ConnectionArn=conn["ConnectionArn"],
        InvocationEndpoint="https://api.example.test/hook", HttpMethod="PATCH",
    )
    with pytest.raises(ClientError):
        eb.update_api_destination(Name=name, HttpMethod="TRACE")
    assert eb.describe_api_destination(Name=name)["HttpMethod"] == "PATCH"


def test_eventbridge_connection_rejects_unknown_authorization_type(eb):
    """AuthorizationType decides which credentials are attached, and an
    unrecognized one matched nothing — the connection was created and then
    delivered with no credentials at all, silently."""
    with pytest.raises(ClientError) as exc:
        eb.create_connection(
            Name=f"qa-eb-authtype-{_uuid_mod.uuid4().hex[:8]}",
            AuthorizationType="basic",
            AuthParameters={"BasicAuthParameters": {"Username": "u", "Password": "p"}},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "authorizationType" in exc.value.response["Error"]["Message"]

    name = f"qa-eb-authtype-ok-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_connection(
        Name=name, AuthorizationType="BASIC",
        AuthParameters={"BasicAuthParameters": {"Username": "u", "Password": "p"}},
    )
    with pytest.raises(ClientError):
        eb.update_connection(Name=name, AuthorizationType="Api_Key")
    assert eb.describe_connection(Name=name)["AuthorizationType"] == "BASIC"


def test_eventbridge_connection_non_object_auth_parameters_still_succeeds(eb):
    """The endpoint check reaches into AuthParameters, which is raw caller
    JSON. A non-object value was stored verbatim and answered 200 before this
    validation existed, and it must still — the guard has to hold at the
    handler, not only in the helper it calls. botocore strict-validates the
    shape, so call via raw HTTP."""
    import urllib.request as _r

    name = f"qa-eb-authparams-wire-{_uuid_mod.uuid4().hex[:8]}"

    def _post(target, payload):
        req = _r.Request(
            f"{_ENDPOINT}/",
            data=json.dumps(payload).encode(),
            headers={
                "X-Amz-Target": f"AWSEvents.{target}",
                "Content-Type": "application/x-amz-json-1.1",
                "Authorization": ("AWS4-HMAC-SHA256 Credential=test/20260101/"
                                  "us-east-1/events/aws4_request, SignedHeaders=, Signature=x"),
            },
        )
        with _r.urlopen(req) as resp:
            return resp.status

    assert _post("CreateConnection", {
        "Name": name, "AuthorizationType": "API_KEY", "AuthParameters": "oops",
    }) == 200
    assert _post("UpdateConnection", {"Name": name, "AuthParameters": ["oops"]}) == 200


# ---------------------------------------------------------------------------
# Unit tests: _http_open / _finalize_api_dest_headers /
#             _validate_oauth_authorization_endpoint
# ---------------------------------------------------------------------------

def test_eventbridge_api_dest_headers_match_reserved_names_trimmed():
    """A padded spelling must not slip past the reserved-header filter.
    ``"User-Agent "`` is not in the removed set, and http.client only rejects
    whitespace as the FIRST character of a name, so an untrimmed comparison
    puts an attacker-chosen User-Agent on the wire beside EventBridge's own."""
    out = _eb._finalize_api_dest_headers({
        "User-Agent ": "pwned-agent", "Host\t": "pwned-host", "X-Keep": "kept",
    })
    assert out["User-Agent"] == "Amazon/EventBridge/ApiDestinations"
    assert out["X-Keep"] == "kept"
    assert all(k == k.strip() for k in out)          # no padded spelling survives
    assert "pwned-agent" not in out.values()
    assert "pwned-host" not in out.values()


def test_eventbridge_http_open_refuses_non_http_scheme():
    """The opener-level scheme guard is the half of this that survives a state
    restore: load_state repopulates api_destinations and connections verbatim,
    so a record written before the API validation existed never revalidates.
    Create/Update now reject these, which is exactly why no black-box test can
    reach the guard — it is pinned in-process instead."""
    import urllib.request

    for bad in ("file:///etc/passwd", "ftp://127.0.0.1/token", "gopher://127.0.0.1/1"):
        with pytest.raises(ValueError, match="unsupported URL scheme"):
            _eb._http_open(urllib.request.Request(bad))


def test_eventbridge_oauth_token_request_uses_the_guarded_opener():
    """The token POST carries the client_id/client_secret, so it must ride the
    same guarded opener as delivery rather than a bare urlopen — otherwise the
    no-redirect handler and the scheme guard both fall off the one request
    guaranteed to be carrying credentials. A non-http(s) AuthorizationEndpoint
    is the cheap proof of routing: a bare urlopen would happily read the file
    and fail later, in the JSON decode."""
    with pytest.raises(ValueError, match="unsupported URL scheme"):
        _eb._fetch_oauth_token({
            "AuthorizationEndpoint": "file:///etc/passwd",
            "ClientParameters": {"ClientID": "id", "ClientSecret": "secret"},
        })


def test_eventbridge_oauth_endpoint_check_tolerates_non_dict_auth_parameters():
    """AuthParameters is raw caller JSON and botocore rejects a non-object
    shape client-side, so only a hand-rolled request reaches this check. It
    must stay a no-op there: v1.4.15 stored the value verbatim and answered
    200, and adding validation must not turn that into a 500."""
    for junk in (None, "oops", ["x"], 5, True, ""):
        assert _eb._validate_oauth_authorization_endpoint(junk) is None


# ---------------------------------------------------------------------------
# Content-pattern matcher parity regression tests
# ---------------------------------------------------------------------------

def _matches_event(eb, event, pattern):
    """``TestEventPattern`` over a whole event and a whole pattern — the call
    every narrower helper below ends in."""
    return eb.test_event_pattern(Event=json.dumps(event),
                                 EventPattern=json.dumps(pattern))["Result"]


def _pattern_matches(eb, detail_pattern, detail_value):
    """A ``detail`` pattern fragment against a ``detail`` value."""
    return _matches_event(eb, {"source": "wc.test", "detail-type": "T", "detail": detail_value},
                          {"source": ["wc.test"], "detail": detail_pattern})


def _pattern_refused(eb, pattern):
    """The reason the service gives for refusing ``pattern``, or ``None`` when it
    accepts it. AWS validates a pattern at every API that takes one, so a
    malformed operand is a ``400 InvalidEventPatternException`` rather than a
    quiet non-match."""
    try:
        eb.test_event_pattern(
            Event=json.dumps({"source": "v.test", "detail-type": "T", "detail": {"k": "x"}}),
            EventPattern=json.dumps(pattern))
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidEventPatternException", exc
        return exc.response["Error"]["Message"]
    return None


def _put_rule_refused(eb, pattern):
    """The same question asked of PutRule, which must answer identically —
    TestEventPattern is how you check a pattern before creating the rule, so the
    two disagreeing would make it useless."""
    try:
        eb.put_rule(Name=f"qa-eb-reject-{_uuid_mod.uuid4().hex[:8]}",
                    EventPattern=json.dumps(pattern), State="ENABLED")
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidEventPatternException", exc
        return exc.response["Error"]["Message"]
    return None


def _matcher_answer(pattern, detail=None, source="v.test"):
    """What the matcher answers for a pattern that never went through
    validation. That is the path a rule restored from persisted state takes —
    it is loaded verbatim and never revalidated — so the fail-closed guards are
    the only thing between a malformed operand and either a 500 out of
    PutEvents or a rule that matches every event."""
    event = {"Source": source, "DetailType": "T",
             "Detail": json.dumps({} if detail is None else detail),
             "Account": "000000000000", "Region": "us-east-1", "Resources": [],
             "EventId": "id-1", "Time": "2026-08-14T00:00:00Z"}
    return _eb._matches_pattern(json.dumps(pattern), event)


def test_eventbridge_wildcard_matches_aws_semantics(eb):
    """EventBridge wildcards special-case only '*', with '\\' as the escape
    char; '?' and '[seq]' are literal (not fnmatch). Rows verified against real
    AWS test-event-pattern."""
    # (pattern, value, expected) — backslashes are single in the JSON operand.
    assert _pattern_matches(eb, {"k": [{"wildcard": "*\\*"}]}, {"k": "zzz\\admin"}) is False
    assert _pattern_matches(eb, {"k": [{"wildcard": "*\\\\*"}]}, {"k": "zzz\\admin"}) is True
    assert _pattern_matches(eb, {"k": [{"wildcard": "a?c"}]}, {"k": "abc"}) is False
    assert _pattern_matches(eb, {"k": [{"wildcard": "a[bc]d"}]}, {"k": "abd"}) is False
    assert _pattern_matches(eb, {"k": [{"wildcard": "a[bc]d"}]}, {"k": "a[bc]d"}) is True
    assert _pattern_matches(eb, {"k": [{"wildcard": "*.txt"}]}, {"k": "report.txt"}) is True


def test_eventbridge_malformed_operand_is_refused_at_rule_creation(eb):
    """A malformed operand is a 400 on AWS, from every API that takes a pattern,
    and this used to be a Python raise surfacing as `500 InternalError` — from
    TestEventPattern and from dispatch, where it aborted the whole PutEvents
    rather than failing one rule."""
    for bad in ({"k": [{"prefix": ["a", "b"]}]},
                {"k": [{"suffix": 5}]},
                {"k": [{"wildcard": 5}]},
                {"k": [{"prefix": True}]}):
        assert _pattern_refused(eb, {"source": ["v.test"], "detail": bad})
        assert _put_rule_refused(eb, {"source": ["v.test"], "detail": bad})
    # The message is the service's own text, not ours.
    assert "wildcard match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": 5}]}})
    assert "Only one key allowed in match expression" in _pattern_refused(
        eb, {"detail": {"k": [{"prefix": "a", "suffix": "b"}]}})
    assert "Unrecognized match type nonesuch" in _pattern_refused(
        eb, {"detail": {"k": [{"nonesuch": "a"}]}})

    # Behind that front door the matcher still has to fail closed, because a
    # rule restored from persisted state is loaded verbatim and never
    # revalidated.
    for bad in ({"prefix": ["a", "b"]}, {"suffix": 5}, {"wildcard": 5}, {"nonesuch": "a"}):
        assert _matcher_answer({"detail": {"k": [bad]}}, {"k": "x"}) is False

    # anything-but: {wildcard: [...]} is a valid AWS form and still works.
    assert _pattern_matches(
        eb, {"k": [{"anything-but": {"wildcard": ["*/*"]}}]}, {"k": "no-slash"}
    ) is True
    assert _pattern_matches(
        eb, {"k": [{"anything-but": {"wildcard": ["*/*"]}}]}, {"k": "has/slash"}
    ) is False

    # A non-string value against a string matcher is the other half of the
    # guard, and it is about the EVENT rather than the pattern — so it stays a
    # plain non-match, with no 400 anywhere.
    for rule in ({"n": [{"wildcard": "5*"}]}, {"n": [{"prefix": "5"}]}, {"n": [{"suffix": "5"}]}):
        assert _pattern_matches(eb, rule, {"n": 5}) is False


def test_eventbridge_numeric_operand_grammar_is_enforced(eb):
    """``numeric`` is a grammar, not a list of pairs: one comparison, or a lower
    bound followed by an upper one. AWS rejects everything else at rule
    creation, and each of these used to be either a `500 InternalError` — the
    operand reached ``len()`` or ``float()`` unguarded — or, worse, a quiet
    *true*, which made the rule match EVERY event and fan it out to its
    targets."""
    for bad in (5, "5", {"=": 5},                      # not an array
                [], [">"], [">", 1, "<"],              # truncated
                ["!!", 1], [[">"], 1], [{"a": 1}, 5],  # bad operator slot
                [">", "abc"], [">", True], [">", None],  # non-numeric threshold
                [">", 1, ">", 10],                     # second bound must be < or <=
                ["=", 1, "<", 5], ["<", 10, ">", 1],   # =, <, <= are terminal
                [">", 1, "<", 10, ">", 2],             # too many terms
                [">", 10, "<", 1], [">", 5, "<", 5],   # empty range
                [">", 10 ** 400]):                     # not representable
        assert _pattern_refused(eb, {"detail": {"k": [{"numeric": bad}]}})
        # ...and the matcher declines it too, for a rule that never saw the door.
        assert _matcher_answer({"detail": {"k": [{"numeric": bad}]}}, {"k": 5}) is False
    assert "Value of numeric must be an array." in _pattern_refused(
        eb, {"detail": {"k": [{"numeric": 5}]}})
    assert "Bottom must be less than top" in _pattern_refused(
        eb, {"detail": {"k": [{"numeric": [">", 10, "<", 1]}]}})
    assert "Unrecognized numeric range operator: !!" in _pattern_refused(
        eb, {"detail": {"k": [{"numeric": ["!!", 1]}]}})

    # The forms AWS does accept.
    assert _pattern_matches(eb, {"k": [{"numeric": [">", 10, "<=", 20]}]}, {"k": 15}) is True
    assert _pattern_matches(eb, {"k": [{"numeric": ["=", 3.018e2]}]}, {"k": 301.8}) is True
    # An oversized value out of the EVENT is not a pattern fault, so it stays a
    # non-match rather than becoming a 400.
    assert _pattern_matches(eb, {"k": [{"numeric": [">", 1]}]}, {"k": 10 ** 400}) is False


def test_eventbridge_numeric_multi_condition_still_matches(eb):
    """The guards must not cost the well-formed multi-condition form."""
    between = {"k": [{"numeric": [">", 10, "<=", 20]}]}
    assert _pattern_matches(eb, between, {"k": 15}) is True
    assert _pattern_matches(eb, between, {"k": 20}) is True
    assert _pattern_matches(eb, between, {"k": 10}) is False
    assert _pattern_matches(eb, between, {"k": 25}) is False


def test_eventbridge_anything_but_never_inverts_what_it_cannot_evaluate(eb):
    """``anything-but`` inverts its nested matcher, so an operand the matcher can
    only answer "no match" to becomes a rule matching EVERY event and fanning it
    out to the rule's targets. The type guards made that strictly worse by
    turning raises into exactly that "no match". AWS refuses all of these at
    rule creation; the matcher declines rather than inverting."""
    for bad in ({"prefix": 5}, {"suffix": ["a", 5]}, {"wildcard": 5}, {"numeric": 5},
                {"wildcard": []}, {"prefix": ""}, {"equals-ignore-case": 5}):
        assert _pattern_refused(eb, {"detail": {"k": [{"anything-but": bad}]}})
        assert _matcher_answer({"detail": {"k": [{"anything-but": bad}]}}, {"k": "x"}) is False
    assert "Null prefix/suffix not allowed" in _pattern_refused(
        eb, {"detail": {"k": [{"anything-but": {"prefix": ""}}]}})
    assert "Anything-But expression name not found" in _pattern_refused(
        eb, {"detail": {"k": [{"anything-but": {}}]}})
    # A mixed or empty literal list is refused with AWS's own wording.
    for bad in (["x", 5], [], [1, "b"]):
        assert "mixed type is not supported" in _pattern_refused(
            eb, {"detail": {"k": [{"anything-but": bad}]}})
    for bad in (["x", None], ["x", True], ["x", ["y"]]):
        assert "start|null|boolean is not supported." in _pattern_refused(
            eb, {"detail": {"k": [{"anything-but": bad}]}})
    assert "Value of anything-but must be an array or single string/number value." in (
        _pattern_refused(eb, {"detail": {"k": [{"anything-but": True}]}}))


def test_eventbridge_anything_but_declines_mixed_nested_matchers(eb):
    """A nested operand carrying two matcher keys is refused. The gate and the
    matcher dispatch keys in different orders, so a mixed operand let the gate
    approve on the well-formed key while the matcher answered on the malformed
    one — and the inversion turned that back into match-everything."""
    for mixed in ({"prefix": "x", "wildcard": "a*"}, {"suffix": "x", "numeric": [">", 5]},
                  {"prefix": "a", "equals-ignore-case": "b"},
                  {"anything-but": {"cidr": "10.0.0.0/8"}, "numeric": [">", 5]},
                  {"exists": True, "prefix": "a"}):
        assert _pattern_refused(eb, {"detail": {"k": [{"anything-but": mixed}]}})
        assert _matcher_answer({"detail": {"k": [{"anything-but": mixed}]}}, {"k": "zzz"}) is False


def test_eventbridge_anything_but_still_inverts_well_formed_nested_operands(eb):
    """The guard must not cost the forms it is protecting: every nested matcher
    AWS allows — ``prefix``, ``suffix``, ``wildcard``, ``equals-ignore-case`` —
    still inverts, in the single and the list form."""
    assert _pattern_matches(eb, {"k": [{"anything-but": {"prefix": "init"}}]}, {"k": "running"}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": {"prefix": "init"}}]}, {"k": "initial"}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": {"suffix": ".txt"}}]}, {"k": "a.log"}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": {"suffix": ".txt"}}]}, {"k": "a.txt"}) is False
    listed = {"k": [{"anything-but": {"prefix": ["init", "pend"]}}]}
    assert _pattern_matches(eb, listed, {"k": "running"}) is True
    assert _pattern_matches(eb, listed, {"k": "pending"}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": {"suffix": [".txt", ".log"]}}]},
                            {"k": "a.log"}) is False
    # A literal and a list of literals are the plain forms.
    assert _pattern_matches(eb, {"k": [{"anything-but": "x"}]}, {"k": "y"}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": ["x", "y"]}]}, {"k": "y"}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": [1, 2]}]}, {"k": 3}) is True


def test_eventbridge_anything_but_refuses_operators_aws_will_not_nest(eb):
    """``anything-but`` nests only ``prefix``, ``suffix``, ``wildcard`` and
    ``equals-ignore-case``. ``cidr`` and ``numeric`` are evaluable everywhere
    else, but AWS refuses them here at rule creation, so there is no positive
    answer to invert — and inverting the "no match" they would otherwise give is
    what turns a bad rule into one matching every event."""
    for nested in ({"cidr": "10.0.0.0/8"}, {"numeric": [">", 10]},
                   {"exists": True}, {"anything-but": "x"}, {"exactly": "x"}):
        reason = _pattern_refused(eb, {"detail": {"k": [{"anything-but": nested}]}})
        assert "Unsupported anything-but pattern" in reason, reason
        for value in ("10.1.2.3", "8.8.8.8", 7, 15):
            assert _matcher_answer({"detail": {"k": [{"anything-but": nested}]}},
                                   {"k": value}) is False


def test_eventbridge_wildcard_anchors_whole_value(eb):
    """The translator must anchor on ``\\Z``, not ``$``: Python's ``$`` also
    matches just before a trailing newline, so ``*.txt`` would match
    ``"report.txt\\n"``. fnmatch anchored this way too, so this pins a property
    the emulator already had against a plausible way of losing it."""
    assert _pattern_matches(eb, {"k": [{"wildcard": "*.txt"}]}, {"k": "report.txt\n"}) is False
    assert _pattern_matches(eb, {"k": [{"wildcard": "abc"}]}, {"k": "abc\n"}) is False
    assert _pattern_matches(eb, {"k": [{"wildcard": "abc"}]}, {"k": "abc"}) is True


def test_eventbridge_wildcard_rejects_consecutive_stars_and_bad_escapes(eb):
    """AWS refuses a run of ``*`` at rule creation, and reports the position as a
    1-based offset into the *quoted* operand counted in UTF-8 bytes — so the
    opening quote is 0 and a multi-byte character before the fault shifts it."""
    assert "Consecutive wildcard characters at pos 1" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": "**"}]}})
    assert "Consecutive wildcard characters at pos 2" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": "a**z"}]}})
    assert "Consecutive wildcard characters at pos 5" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": "\U0001f600**"}]}})
    # A backslash may only escape ``*`` or another backslash. A trailing one
    # escapes the closing quote AWS appends, which is why it reads as an invalid
    # escape rather than a dangling one.
    assert "Invalid escape character at pos 2" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": "a\\"}]}})
    assert "Invalid escape character at pos 2" in _pattern_refused(
        eb, {"detail": {"k": [{"wildcard": "a\\nb"}]}})
    # The escapes that ARE legal stay legal, and a single ``*`` run is fine.
    for good in ("*", "a*z", "*.txt", "a\\*b", "a\\\\b", ""):
        assert _pattern_refused(eb, {"detail": {"k": [{"wildcard": good}]}}) is None
    assert _matcher_answer({"detail": {"k": [{"wildcard": "a**z"}]}}, {"k": "a-mid-z"}) is False


def test_eventbridge_wildcard_regex_collapses_star_runs_and_is_cached():
    """A run of ``*`` translates to a single ``.*``. The compiled form is
    cached because the translation otherwise reruns per event x rule x pattern
    on the PutEvents hot path."""
    assert _eb._wildcard_to_regex("*" * 20 + "x") == _eb._wildcard_to_regex("*x")
    # An escaped '*' is a literal and must not be swallowed by the collapse.
    assert _eb._wildcard_regex("a\\**b").match("a*mid-b") is not None
    assert _eb._wildcard_regex("a\\**b").match("a-mid-b") is None

    _eb._wildcard_regex.cache_clear()
    _eb._wildcard_regex("qa-collapse-*")
    _eb._wildcard_regex("qa-collapse-*")
    assert _eb._wildcard_regex.cache_info().hits == 1


def test_eventbridge_wildcard_does_not_backtrack_exponentially():
    """A pattern of interior ``*``-separated literals against a long value that
    just misses is the catastrophic-backtracking shape. fnmatch was immune
    because ``fnmatch.translate`` emits ``(?=(?P<gN>.*?lit))(?P=gN)``; a naive
    ``.*lit.*lit`` chain is not, and this runs per rule per event on the
    dispatch path. ``*/*/…/*.json`` over a 165-character key took seconds
    before the translator emitted the same construct.

    The bound is deliberately loose — it is here to catch an exponential
    blow-up, not to police constant factors on a busy machine."""
    pattern = "*/" * 9 + "*.json"
    value = "/".join(["seg"] * 40) + ".jsonX"
    _eb._wildcard_regex.cache_clear()
    _eb._wildcard_regex(pattern)          # compile once, outside the measurement
    started = time.monotonic()
    assert _eb._matches_wildcard(value, pattern) is False
    assert time.monotonic() - started < 1.0


def _eb_badop_target(eb, sqs, bus, suffix, detail_pattern):
    """Helper: queue + rule on ``bus`` filtering on ``detail_pattern``."""
    return _eb_rule_to_queue(eb, sqs, f"badop-{suffix}",
                             {"source": ["myapp.badop"], "detail": detail_pattern}, bus)


def test_eventbridge_hostile_event_value_does_not_break_dispatch(eb, sqs):
    """On the PutEvents path a raising matcher took the whole call down as a
    `500` — every other rule on the bus with it. Validation cannot help here:
    these patterns are all valid, and it is the EVENT that is hostile, so the
    matcher's guards are the only thing holding. An integer too large for a
    double is the sharpest case, because ``float()`` answers that with
    ``OverflowError``, which is neither ``TypeError`` nor ``ValueError``."""
    bus = "qa-eb-badev-bus"
    eb.create_event_bus(Name=bus)
    q_numeric = _eb_badop_target(eb, sqs, bus, "numeric", {"n": [{"numeric": [">", 1]}]})
    q_cidr = _eb_badop_target(eb, sqs, bus, "cidr", {"ip": [{"cidr": "10.0.0.0/24"}]})
    q_prefix = _eb_badop_target(eb, sqs, bus, "prefix", {"s": [{"prefix": "a"}]})
    q_ok = _eb_badop_target(eb, sqs, bus, "ok", {"id": [{"prefix": "ID-"}]})

    resp = eb.put_events(Entries=[{
        "Source": "myapp.badop", "DetailType": "t",
        "Detail": json.dumps({
            "id": "ID-1",
            "n": 10 ** 400,        # no double can hold it
            "ip": 167772161,       # the integer form of 10.0.0.1
            "s": {"nested": "a"},  # an object where a string is expected
        }),
        "EventBusName": bus,
    }])
    assert resp["FailedEntryCount"] == 0

    # The well-formed rule still delivers, proving the others were evaluated
    # rather than skipped.
    ok_msgs = sqs.receive_message(
        QueueUrl=q_ok, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages") or []
    assert len(ok_msgs) == 1
    for q_url in (q_numeric, q_cidr, q_prefix):
        assert not sqs.receive_message(
            QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages")


# ---------------------------------------------------------------------------
# equals-ignore-case, cidr and $or
# ---------------------------------------------------------------------------

_DETAIL_OMITTED = object()


def _event_matches(eb, pattern, detail=_DETAIL_OMITTED, source="op.test", detail_type="T"):
    """Helper: TestEventPattern over a WHOLE pattern rather than just its
    ``detail``, since ``$or`` is a top-level construct too. ``detail`` is left
    out of the event unless given, so passing ``None`` means a JSON null — which
    is a leaf, and a different answer from an absent or empty detail."""
    event = {"source": source, "detail-type": detail_type}
    if detail is not _DETAIL_OMITTED:
        event["detail"] = detail
    return _matches_event(eb, event, pattern)


def test_eventbridge_equals_ignore_case_matches_whole_value(eb):
    """``equals-ignore-case`` was accepted and then answered "no match" for
    everything, so a rule guarding on it filtered out every event. It compares
    the whole value — not a prefix — and only ever matches a string: a number
    whose text equals the operand is a different JSON type and does not."""
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "alice"}]}, {"k": "ALICE"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "alice"}]}, {"k": "Alice"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "aLiCe"}]}, {"k": "alice"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "alice"}]}, {"k": "alicex"}) is False
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "alice"}]}, {"k": "xalice"}) is False
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": ""}]}, {"k": ""}) is True
    for value in (123, True, None, ["ALICE"], {"a": "ALICE"}):
        assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "123"}]}, {"k": value}) is False
    # It works on the envelope fields too, not only inside detail.
    assert _event_matches(eb, {"source": [{"equals-ignore-case": "OP.Test"}]}) is True


def test_eventbridge_equals_ignore_case_folds_each_character_independently(eb):
    """AWS lower- and upper-cases every operand character *on its own* and
    accepts either form at that position, so a mapping that changes length
    counts. That makes the relation neither ``lower()`` (which cannot match
    ``straße`` against ``STRASSE`` at all, and would wrongly make the relation
    symmetric) nor ``casefold()`` (which would wrongly accept ``strasse``).
    Rows taken from aws/event-ruler, the engine EventBridge runs."""
    strasse = {"k": [{"equals-ignore-case": "straße"}]}
    assert _pattern_matches(eb, strasse, {"k": "STRASSE"}) is True
    assert _pattern_matches(eb, strasse, {"k": "STRAßE"}) is True
    assert _pattern_matches(eb, strasse, {"k": "strasse"}) is False
    # Each position picks a case independently, but the form it picks is then
    # literal: 'ß' contributes "SS", never "sS".
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "maße"}]}, {"k": "MAsSE"}) is False
    # Not symmetric: capital sharp s lowercases to 'ß', but 'ß' uppercases to
    # "SS" and so never reaches U+1E9E.
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ẞ"}]}, {"k": "ß"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ß"}]}, {"k": "ẞ"}) is False
    # Same shape for the Kelvin sign, and 'ﬁ' reaches "FI" but never "fi".
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "K"}]}, {"k": "k"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "k"}]}, {"k": "K"}) is False
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ﬁ"}]}, {"k": "FI"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ﬁ"}]}, {"k": "fi"}) is False
    # Not even reflexive: a titlecase character is neither its own lower nor
    # its own upper case.
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ǅ"}]}, {"k": "ǅ"}) is False
    # Case mapping is locale-independent, so dotless 'ı' reaches 'I' but 'I'
    # only ever reaches 'i'.
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "ı"}]}, {"k": "I"}) is True
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "I"}]}, {"k": "ı"}) is False
    # A deliberate divergence: AWS walks UTF-16 code units and mangles an
    # astral character into "?", so its own operand stops matching the value
    # it was written for. Matching it is the useful answer, not the faithful
    # one, and this pins which of the two this emulator gives.
    assert _pattern_matches(eb, {"k": [{"equals-ignore-case": "\U0001f600"}]},
                            {"k": "\U0001f600"}) is True


def test_eventbridge_equals_ignore_case_operand_must_be_a_string(eb):
    """The operand reaches ``str`` methods, so a non-string one has to be turned
    away before it raises — the class of bug that surfaced the other operators
    as `500 InternalError`. AWS refuses it at rule creation; a list is legal only
    under ``anything-but``."""
    for bad in (5, None, True, {"a": "b"}, [], ["a"], ["a", 5]):
        assert _pattern_refused(eb, {"detail": {"k": [{"equals-ignore-case": bad}]}})
        assert _matcher_answer({"detail": {"k": [{"equals-ignore-case": bad}]}},
                               {"k": "a"}) is False
    assert "equals-ignore-case match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"equals-ignore-case": 5}]}})


def test_eventbridge_anything_but_inverts_equals_ignore_case(eb):
    """``equals-ignore-case`` is one of the four matchers AWS lets
    ``anything-but`` nest, in both the single and the list form — the list
    form being the one place AWS accepts a list operand at all."""
    single = {"k": [{"anything-but": {"equals-ignore-case": "initializing"}}]}
    assert _pattern_matches(eb, single, {"k": "INITIALIZING"}) is False
    assert _pattern_matches(eb, single, {"k": "running"}) is True
    listed = {"k": [{"anything-but": {"equals-ignore-case": ["initializing", "stopped"]}}]}
    assert _pattern_matches(eb, listed, {"k": "STOPPED"}) is False
    assert _pattern_matches(eb, listed, {"k": "Initializing"}) is False
    assert _pattern_matches(eb, listed, {"k": "running"}) is True


def test_eventbridge_cidr_matches_addresses_inside_the_block(eb):
    """``cidr`` was accepted and answered "no match" for every address, so an
    IP-range guard silently filtered out everything. Host bits in the operand
    are floored the way AWS floors them, and IPv6 compares as addresses rather
    than as text, so case and zero-compression do not matter."""
    v4 = {"ip": [{"cidr": "10.0.0.0/24"}]}
    assert _pattern_matches(eb, v4, {"ip": "10.0.0.5"}) is True
    assert _pattern_matches(eb, v4, {"ip": "10.0.0.255"}) is True
    assert _pattern_matches(eb, v4, {"ip": "10.0.1.5"}) is False
    # Host bits set: AWS takes the floor of the range rather than rejecting.
    floored = {"ip": [{"cidr": "10.0.0.5/24"}]}
    assert _pattern_matches(eb, floored, {"ip": "10.0.0.0"}) is True
    assert _pattern_matches(eb, floored, {"ip": "10.0.1.0"}) is False
    assert _pattern_matches(eb, {"ip": [{"cidr": "0.0.0.0/0"}]}, {"ip": "8.8.8.8"}) is True
    v6 = {"ip": [{"cidr": "2001:0DB8:0000::/32"}]}
    assert _pattern_matches(eb, v6, {"ip": "2001:db8:0:0:0:0:0:1"}) is True
    assert _pattern_matches(eb, v6, {"ip": "2001:DB8::1"}) is True
    assert _pattern_matches(eb, v6, {"ip": "2001:db9::1"}) is False
    # An address of the other family is not in the block. AWS's own engine
    # leaks here — it compares the two as hex text, so an IPv4 /24 can match
    # an IPv6 value whose leading digits fall inside it — and this does not
    # reproduce that.
    assert _pattern_matches(eb, v4, {"ip": "0a00:0000::"}) is False
    assert _pattern_matches(eb, {"ip": [{"cidr": "::/0"}]}, {"ip": "10.0.0.5"}) is False
    # Several blocks are written as sibling objects in the value list, which
    # is also the only form AWS accepts.
    either = {"ip": [{"cidr": "10.0.0.0/24"}, {"cidr": "192.168.0.0/24"}]}
    assert _pattern_matches(eb, either, {"ip": "192.168.0.7"}) is True
    assert _pattern_matches(eb, either, {"ip": "172.16.0.7"}) is False


def test_eventbridge_cidr_operands_aws_rejects_are_refused(eb):
    """Every row here fails rule creation on AWS, with the reason AWS gives.
    ``/32`` and ``/128`` are the surprising ones: AWS caps the mask *below* the
    address width, a single host being written as a plain equality value.
    ``ipaddress`` would accept all but two of these, several silently — a bare
    address as a ``/32``, a dotted netmask, a zone id."""
    expectations = [
        ("10.0.0.1/32", "IPv4 mask bits must be < 32"),
        ("10.0.0.1/33", "IPv4 mask bits must be < 32"),
        ("::1/128", "IPv6 mask bits must be < 128"),
        ("10.0.0.1", "Malformed CIDR, one '/' required"),
        ("", "Malformed CIDR, one '/' required"),
        # A trailing slash leaves no mask field for AWS's split, so it reads as
        # the missing-slash complaint rather than a bad-integer one.
        ("10.0.0.0/", "Malformed CIDR, one '/' required"),
        ("10.0.0.0/99999999999", "Malformed CIDR, mask bits must be an integer"),
        ("10.0.0.0//24", "Malformed CIDR, one '/' required"),
        ("10.0.0.0/xx", "Malformed CIDR, mask bits must be an integer"),
        ("10.0.0.0/ 24", "Malformed CIDR, mask bits must be an integer"),
        ("10.0.0.0/-1", "Malformed CIDR, mask bits must not be negative"),
        ("10.0.0.0/255.255.255.0", "Malformed CIDR, mask bits must be an integer"),
        ("999.1.1.1/24", "Invalid IP address: 999.1.1.1"),
        ("example.com/24", "Nonstandard IP address: example.com"),
        ("/24", "Nonstandard IP address: "),
        ("10.0.0/24", "Nonstandard IP address: 10.0.0"),
        ("::ffff:10.0.0.1/24", "Nonstandard IP address: ::ffff:10.0.0.1"),
        ("fe80::1%eth0/64", "Nonstandard IP address: fe80::1%eth0"),
        ("10.0.0.0/" + "1" * 4301, "Malformed CIDR, mask bits must be an integer"),
    ]
    for operand, reason in expectations:
        refusal = _pattern_refused(eb, {"detail": {"ip": [{"cidr": operand}]}})
        assert refusal and reason in refusal, (operand, refusal)
        assert _matcher_answer({"detail": {"ip": [{"cidr": operand}]}},
                               {"ip": "10.0.0.1"}) is False
    # A list operand is refused too — AWS takes exactly one block per object,
    # and reports it against ``prefix``, which is its own copy-paste slip.
    for bad in (["10.0.0.0/24"], 24, None, True, {"a": "10.0.0.0/24"}):
        assert "prefix match pattern must be a string" in _pattern_refused(
            eb, {"detail": {"ip": [{"cidr": bad}]}})
    # The mask spellings AWS's integer parse does accept.
    for operand in ("10.0.0.0/+24", "10.0.0.0/024", "1.2.3.4/-0", "10.0.0.0/31"):
        assert _pattern_refused(eb, {"detail": {"ip": [{"cidr": operand}]}}) is None
    assert _pattern_matches(eb, {"ip": [{"cidr": "10.0.0.0/+24"}]}, {"ip": "10.0.0.5"}) is True
    assert _pattern_matches(eb, {"ip": [{"cidr": "1.2.3.4/-0"}]}, {"ip": "8.8.8.8"}) is True


def test_eventbridge_cidr_never_reads_a_non_string_value_as_an_address(eb):
    """``ipaddress`` accepts an integer as an address — ``167772161`` is
    ``10.0.0.1`` and ``True`` is ``0.0.0.1`` — so an unguarded numeric or
    boolean detail value would match a block that AWS, which matches only
    JSON strings, never matches it against. Malformed strings are the other
    half: they must answer "no match" rather than raise."""
    for value in (167772161, 0, True, False, 1.5, None, {"ip": "10.0.0.5"}, [167772161]):
        assert _pattern_matches(eb, {"ip": [{"cidr": "0.0.0.0/0"}]}, {"ip": value}) is False
    # An array is a different matter — AWS flattens it and offers each element.
    assert _pattern_matches(eb, {"ip": [{"cidr": "10.0.0.0/24"}]},
                            {"ip": ["8.8.8.8", "10.0.0.5"]}) is True
    for value in ("not-an-ip", "", "10.0.0.5 ", "10.0.0.5/24", "10.0.0", "999.1.1.1"):
        assert _pattern_matches(eb, {"ip": [{"cidr": "10.0.0.0/24"}]}, {"ip": value}) is False
    # A leading-zero octet is decimal, as AWS reads it — not octal, and not a
    # refusal, which is what Python's own address parser would give.
    assert _pattern_matches(eb, {"ip": [{"cidr": "10.0.0.0/24"}]}, {"ip": "010.0.0.5"}) is True
    assert _pattern_matches(eb, {"ip": [{"cidr": "0177.0.0.0/24"}]}, {"ip": "177.0.0.5"}) is True
    assert _pattern_matches(eb, {"ip": [{"cidr": "0177.0.0.0/24"}]}, {"ip": "127.0.0.5"}) is False


def test_eventbridge_or_matches_any_branch_and_ands_siblings(eb):
    """A top-level ``$or`` was not read at all, so a pattern whose only key is
    ``$or`` matched EVERY event and fanned it out to the rule's targets — the
    same match-everything class as an inverted bad operand. Sibling keys still
    AND with the branches."""
    only_or = {"$or": [{"source": ["nope"]}, {"source": ["also-nope"]}]}
    assert _event_matches(eb, only_or) is False
    assert _event_matches(eb, {"$or": [{"source": ["op.test"]}, {"source": ["x"]}]}) is True

    both = {"source": ["op.test"],
            "$or": [{"detail": {"x": ["1"]}}, {"detail": {"y": ["2"]}}]}
    assert _event_matches(eb, both, detail={"x": "1"}) is True
    assert _event_matches(eb, both, detail={"y": "2"}) is True
    assert _event_matches(eb, both, detail={"x": "9", "y": "9"}) is False
    assert _event_matches(eb, both, detail={"x": "1"}, source="other") is False


def test_eventbridge_or_inside_detail_matches_aws_documented_example(eb):
    """``$or`` is a field-position keyword: legal wherever a field name is,
    including inside ``detail`` and inside objects nested under it. This is
    AWS's own documented example, verbatim."""
    pattern = {"detail": {"$or": [
        {"c-count": [{"numeric": [">", 0, "<=", 5]}]},
        {"d-count": [{"numeric": ["<", 10]}]},
        {"x-limit": [{"numeric": ["=", 3.018e2]}]},
    ]}}
    assert _event_matches(eb, pattern, detail={"c-count": 3}) is True
    assert _event_matches(eb, pattern, detail={"d-count": 9}) is True
    assert _event_matches(eb, pattern, detail={"x-limit": 301.8}) is True
    assert _event_matches(eb, pattern, detail={"c-count": 9, "d-count": 99, "x-limit": 1}) is False
    # A sibling of the nested $or still ANDs with it.
    with_sibling = {"detail": {"$or": [{"a": ["1"]}, {"b": ["2"]}], "c": ["3"]}}
    assert _event_matches(eb, with_sibling, detail={"a": "1", "c": "3"}) is True
    assert _event_matches(eb, with_sibling, detail={"a": "1"}) is False
    # ...and it nests as deep as the objects do.
    deep = {"detail": {"inner": {"$or": [{"p": ["1"]}, {"q": ["2"]}]}}}
    assert _event_matches(eb, deep, detail={"inner": {"q": "2"}}) is True
    assert _event_matches(eb, deep, detail={"inner": {"r": "3"}}) is False


def test_eventbridge_or_branches_and_their_own_keys_and_nest(eb):
    """A branch is a whole pattern fragment: several keys in one branch AND
    together, and a branch may carry its own ``$or``. Any operator is allowed
    inside a branch — including ``exists: false``, which is the only one that
    matches an absent field."""
    anded = {"$or": [{"detail": {"m": ["1"], "n": ["2"]}}, {"detail": {"z": ["9"]}}]}
    assert _event_matches(eb, anded, detail={"m": "1", "n": "2"}) is True
    assert _event_matches(eb, anded, detail={"m": "1"}) is False
    assert _event_matches(eb, anded, detail={"z": "9"}) is True

    nested = {"$or": [{"$or": [{"source": ["b"]}, {"source": ["c"]}]}, {"source": ["d"]}]}
    assert _event_matches(eb, nested, source="c") is True
    assert _event_matches(eb, nested, source="d") is True
    assert _event_matches(eb, nested, source="z") is False

    operators = {"$or": [{"detail": {"a": [{"anything-but": "x"}]}},
                         {"detail": {"b": [{"exists": False}]}}]}
    assert _event_matches(eb, operators, detail={"a": "y", "b": "present"}) is True
    assert _event_matches(eb, operators, detail={"a": "x", "b": "present"}) is False
    assert _event_matches(eb, operators, detail={"a": "x"}) is True


def test_eventbridge_or_that_is_not_the_operator(eb):
    """AWS reads ``$or`` as the operator only when it holds at least two
    non-empty objects whose field names are not reserved matcher keywords. Every
    other shape is retried against its pre-``$or`` compiler, which reads ``$or``
    as an ordinary field name — so some shapes are refused outright and others
    are accepted as a field the envelope does not have, which matches nothing.
    Answering "matches" is the hole being closed here, so neither route may
    fall back into it."""
    for bad in ("x", 5, None, True,                       # not an array or object
                [{"source": ["op.test"]}],                # only one branch
                [{}, {}],                                 # empty branches
                [],
                [{"source": ["op.test"]}, "not-an-object"],
                [{"prefix": "op."}, {"source": ["op.test"]}]):  # reserved field name
        assert _pattern_refused(eb, {"$or": bad}), bad
        assert _pattern_refused(eb, {"source": ["op.test"], "$or": bad}), bad
        assert _matcher_answer({"$or": bad}) is False

    # Accepted, as an ordinary field named "$or" — which the event envelope has
    # no member for, so it matches nothing.
    for literal in ({"m": ["1"], "n": ["2"]}, ["a", "b"], [1, 2]):
        assert _pattern_refused(eb, {"$or": literal}) is None, literal
        assert _event_matches(eb, {"$or": literal}) is False

    # $or is exactly those three characters. A case variant is an ordinary field
    # name, which means its array elements are read as match expressions — and
    # `{"a": [...]}` is not one, so the pattern is refused rather than merely
    # never matching.
    assert "Unrecognized match type a" in _pattern_refused(
        eb, {"detail": {"$OR": [{"a": ["1"]}, {"b": ["2"]}]}})
    # As a plain field it works like any other.
    assert _event_matches(eb, {"detail": {"$OR": ["x"]}}, detail={"$OR": "x"}) is True
    assert _event_matches(eb, {"detail": {"$OR": ["x"]}}, detail={"$OR": "y"}) is False


def test_eventbridge_or_is_an_ordinary_field_name_inside_detail(eb):
    """Inside ``detail`` the fallback means something different from the top
    level: a detail payload really can carry a field called ``$or``, and AWS
    matches it as one, so an unrecognized ``$or`` there is a field match
    rather than an automatic failure."""
    literal = {"detail": {"$or": ["hello"]}}
    assert _event_matches(eb, literal, detail={"$or": "hello"}) is True
    assert _event_matches(eb, literal, detail={"$or": "other"}) is False
    assert _event_matches(eb, literal, detail={}) is False


def test_eventbridge_unjudged_detail_value_does_not_match_every_content_filter(eb):
    """A null- or object-valued detail field is neither a string, a number nor
    an array, so it fell through the value-level dispatch entirely and was
    never judged — and an unjudged key reads as a matched key. Every content
    filter therefore matched such a field, so a rule guarding on one fired on
    the events it was written to exclude. AWS matches a null against a literal
    null and against ``anything-but``, an object against nothing at all."""
    filters = ({"prefix": "a"}, {"suffix": "a"}, {"wildcard": "a*"}, {"numeric": [">", 1]},
               {"equals-ignore-case": "null"}, {"cidr": "0.0.0.0/0"})
    for rule in filters:
        assert _pattern_matches(eb, {"k": [rule]}, {"k": None}) is False
        assert _pattern_matches(eb, {"k": [rule]}, {"k": {"nested": "a"}}) is False
    assert _pattern_matches(eb, {"k": ["a"]}, {"k": {"nested": "a"}}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": "x"}]}, {"k": {"nested": "a"}}) is False
    # A null is still a value: a literal null matches it, and anything-but
    # inverts against it, both as on AWS.
    assert _pattern_matches(eb, {"k": [None]}, {"k": None}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": "x"}]}, {"k": None}) is True
    # The well-formed cases these share a code path with are unaffected.
    assert _pattern_matches(eb, {"k": [{"prefix": "a"}]}, {"k": "abc"}) is True
    assert _pattern_matches(eb, {"k": ["v"]}, {"k": "v"}) is True
    assert _pattern_matches(eb, {"k": [{"exists": True}]}, {"k": None}) is True
    # An object is not a leaf, and AWS's operators only work on leaf nodes, so
    # it does not "exist" for this purpose — see the dedicated exists test.
    assert _pattern_matches(eb, {"k": [{"exists": True}]}, {"k": {"nested": "a"}}) is False
    # A pattern object against an object value is a nested pattern, not a
    # value-level filter, and still recurses.
    assert _pattern_matches(eb, {"k": {"nested": ["a"]}}, {"k": {"nested": "a"}}) is True


def test_eventbridge_anything_but_gate_approves_only_operands_the_matcher_reads():
    """``_nested_matcher_ok`` approves a nested operand written as a pattern
    string or as a list of them, for every operator ``anything-but`` may invert.
    An operator that was approved here but whose matcher does not read the list
    form could only answer "no match" to its own operand, and ``anything-but``
    inverts that into a rule matching every event — the failure the gate exists
    to prevent. So each nestable operator is exercised in both forms, and the set
    is checked against the derived tuple so that a fifth operator cannot be added
    without being exercised too — a name added to ``_ANYTHING_BUT_NESTABLE`` alone
    would drop straight out of the derived tuple, which is what
    ``test_eventbridge_every_accepted_operator_has_a_matcher`` refuses."""
    operands = {"prefix": "ab", "suffix": "bc", "wildcard": "a*c",
                "equals-ignore-case": "ABC"}
    assert set(operands) == set(_eb._NESTED_MATCHER_KEYS)
    for key, operand in operands.items():
        for form in (operand, [operand]):
            assert _eb._nested_matcher_ok({key: form}) is True, (key, form)
            assert _eb._matches_content_filter("abc", {key: form}) is True, (key, form)
    # And nothing else. Each of these is a "no match" from the matcher — a key
    # ``anything-but`` cannot nest, an operand with nothing to exclude, a
    # non-string operand, two keys at once — which the inversion would turn into
    # a rule matching every event.
    for declined in ({}, {"cidr": ["10.0.0.0/24"]}, {"cidr": "10.0.0.0/24"},
                     {"numeric": [">", 1]}, {"exactly": "abc"},
                     {"prefix": []}, {"prefix": [5]},
                     {"prefix": "a", "suffix": "b"}):
        assert _eb._nested_matcher_ok(declined) is False, declined
        assert _eb._matches_content_filter(
            "anything at all", {"anything-but": declined}) is False, declined


def test_eventbridge_every_accepted_operator_has_a_matcher():
    """An operator validation accepts but the dispatch cannot answer matches
    NOTHING for every event — the failure this change set exists to fix for
    ``cidr``, ``equals-ignore-case``, ``exactly`` and the case-insensitive
    affixes. ``exists`` is the one legal absence: it is answered from whether the
    path reaches a leaf, in ``_matches_detail``, before any value reaches the
    registry."""
    assert set(_eb._MATCH_OPERATORS) - set(_eb._CONTENT_MATCHERS) == {"exists"}
    assert set(_eb._CONTENT_MATCHERS) - set(_eb._MATCH_OPERATORS) == set()
    # The same question for the nested position. ``_validate_anything_but``
    # accepts whatever ``_ANYTHING_BUT_NESTABLE`` holds, while the gate scans the
    # registry, so a name in one and not the other is an operand accepted at rule
    # creation that the matcher can only answer "no match" to — which
    # ``anything-but`` inverts into a rule matching every event.
    assert set(_eb._ANYTHING_BUT_NESTABLE) <= set(_eb._CONTENT_MATCHERS)
    assert set(_eb._AFFIX_NESTABLE) <= set(_eb._CONTENT_MATCHERS)


def test_eventbridge_new_operators_dispatch_on_put_events(eb, sqs):
    """The operators have to work on the PutEvents path too, not just through
    TestEventPattern — that is where a matcher that raises takes down the
    whole call, and where one that wrongly matches fans an event out to real
    targets."""
    bus = "qa-eb-newops-bus"
    eb.create_event_bus(Name=bus)
    q_cidr = _eb_badop_target(eb, sqs, bus, "cidr", {"ip": [{"cidr": "10.0.0.0/24"}]})
    q_eic = _eb_badop_target(eb, sqs, bus, "eic", {"state": [{"equals-ignore-case": "running"}]})
    q_miss = _eb_badop_target(eb, sqs, bus, "miss", {"ip": [{"cidr": "192.168.0.0/24"}]})

    q_or_url = _eb_rule_to_queue(
        eb, sqs, "newops-or",
        {"source": ["myapp.badop"], "$or": [{"detail": {"state": ["never"]}},
                                            {"detail": {"ip": [{"cidr": "10.0.0.0/8"}]}}]}, bus)

    resp = eb.put_events(Entries=[{
        "Source": "myapp.badop", "DetailType": "t",
        "Detail": json.dumps({"ip": "10.0.0.7", "state": "RUNNING"}),
        "EventBusName": bus,
    }])
    assert resp["FailedEntryCount"] == 0

    for q_url in (q_cidr, q_eic, q_or_url):
        msgs = sqs.receive_message(
            QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages") or []
        assert len(msgs) == 1
    assert not sqs.receive_message(
        QueueUrl=q_miss, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages")


def test_eventbridge_hostile_pattern_never_escapes_the_matcher(eb):
    """Anything that raises inside the matcher surfaces as `500 InternalError`
    from TestEventPattern and, worse, aborts the whole `PutEvents` — every other
    rule on the bus with it. Each shape below did exactly that. They are refused
    at the door now, and the matcher answers "no match" for the same input,
    which is the path a rule restored from persisted state takes."""
    hostile = [
        {"detail": {"ip": [{"cidr": "10.0.0.0/" + "1" * 4301}]}},
        {"$or": [{"resources": 5}, {"source": ["never"]}]},
        {"resources": None},
    ]
    for pattern in hostile:
        assert _pattern_refused(eb, pattern), pattern
        assert _matcher_answer(pattern) is False

    # Past the depth bound the answer is "no match" rather than a raise. The
    # innermost branch matches, so an unbounded walk would answer True — which
    # pins the bound rather than merely surviving it. The deeper row is past what
    # the handler's remaining stack can even JSON-decode, and has to come back
    # the same way: the parse runs before any bound of ours, and a
    # `RecursionError` let out of it damages more than its own request.
    def nest(depth):
        pattern = {"source": ["op.test"]}
        for _ in range(depth):
            pattern = {"$or": [pattern, {"source": ["other"]}]}
        return pattern
    assert _event_matches(eb, nest(5)) is True
    assert _pattern_refused(eb, nest(60))
    assert _matcher_answer(nest(60), source="op.test") is False
    assert _matcher_answer(nest(300), source="op.test") is False
    # ...and the server is still healthy afterwards, which is the part that
    # matters: a stack exhausted on a pooled worker thread resurfaces in
    # whatever that thread handles next.
    assert _event_matches(eb, {"source": ["op.test"]}) is True

    # A pattern that is not an object at all is not a pattern.
    for shape in (["source"], "source", 5, None):
        assert _pattern_refused(eb, shape), shape
    assert "Filter is not an object" in _pattern_refused(eb, ["source"])
    # Nor is an empty one, at any depth.
    assert "Empty objects are not allowed" in _pattern_refused(eb, {})
    assert "Empty objects are not allowed" in _pattern_refused(eb, {"detail": {}})
    assert "Empty objects are not allowed" in _pattern_refused(eb, {"detail": {"a": {"b": {}}}})
    assert "Empty arrays are not allowed" in _pattern_refused(eb, {"source": []})
    assert '"source" must be an object or an array' in _pattern_refused(eb, {"source": "op.test"})


def test_eventbridge_unmodelled_top_level_key_is_a_path_not_an_error(eb):
    """AWS has no allow-list of top-level pattern fields — it compiles a pattern
    to paths, so an unrecognized key is simply a path the event does not have.
    That cuts both ways, and the second half is the part a fail-closed emulator
    gets wrong: a value list can never be satisfied, but ``exists: false`` on
    such a key is satisfied by EVERY event."""
    for key in ("nonesuch", "Detail", "SOURCE", "detail-Type", "$and"):
        assert _pattern_refused(eb, {key: ["anything"]}) is None, key
        assert _event_matches(eb, {key: ["anything"]}) is False
        assert _event_matches(eb, {key: [{"exists": False}]}) is True
        assert _event_matches(eb, {key: [{"exists": True}]}) is False
    # Sibling keys still AND, so one unsatisfiable key sinks the pattern.
    assert _event_matches(eb, {"source": ["op.test"], "nonesuch": ["x"]}) is False
    assert _event_matches(eb, {"source": ["op.test"], "nonesuch": [{"exists": False}]}) is True
    # An envelope field may itself be written as an object path.
    assert _event_matches(eb, {"source": {"x": ["y"]}}) is False
    assert _event_matches(eb, {"source": {"x": [{"exists": False}]}}) is True


def test_eventbridge_every_envelope_field_is_filterable(eb):
    """All ten envelope fields can be filtered on, including the three the walk
    used to skip entirely — a skipped key is a matched key, so a pattern
    filtering on ``id``, ``time`` or ``version`` matched every event."""
    event = {"version": "0", "id": "abc-123", "source": "op.test",
             "account": "000000000000", "time": "2026-08-14T12:00:00Z",
             "region": "us-east-1", "resources": ["arn:aws:x:::a"],
             "detail-type": "T", "detail": {"k": "v"}}

    assert _matches_event(eb, event, {"version": ["0"]}) is True
    assert _matches_event(eb, event, {"version": ["9"]}) is False
    assert _matches_event(eb, event, {"id": ["abc-123"]}) is True
    assert _matches_event(eb, event, {"id": [{"prefix": "abc-"}]}) is True
    assert _matches_event(eb, event, {"id": ["nope"]}) is False
    # `time` is plain lexical text — there is no date handling anywhere.
    assert _matches_event(eb, event, {"time": [{"prefix": "2026-08"}]}) is True
    assert _matches_event(eb, event, {"time": [{"prefix": "2026-8"}]}) is False
    assert _matches_event(eb, event, {"time": ["2026-08-14T12:00:00Z"]}) is True
    # Values are type-strict: a string pattern does not match a numeric leaf.
    assert _matches_event(eb, event, {"account": [000000000000]}) is False
    assert _matches_event(eb, event, {"account": ["000000000000"]}) is True
    # ...and case-sensitive, which is what equals-ignore-case is for.
    assert _matches_event(eb, event, {"source": ["OP.TEST"]}) is False
    assert _matches_event(eb, event, {"source": [{"equals-ignore-case": "OP.TEST"}]}) is True
    # All ten at once, ANDed.
    assert _matches_event(eb, event, {
        "version": ["0"], "id": ["abc-123"], "source": ["op.test"],
        "account": ["000000000000"], "time": [{"prefix": "2026"}],
        "region": ["us-east-1"], "resources": ["arn:aws:x:::a"],
        "detail-type": ["T"], "detail": {"k": ["v"]}}) is True


def test_eventbridge_resources_is_an_intersection_not_a_requirement(eb):
    """``resources`` is an array in the event, and AWS matches it the way it
    matches any array: the pattern list is an OR and the field matches when the
    intersection is non-empty. This required every pattern value to be present,
    so a rule naming two ARNs matched no event carrying only one — and because
    the check was plain equality, a content filter compared a dict against ARN
    strings and could never match at all."""
    event = {"source": "op.test", "detail-type": "T", "detail": {},
             "resources": ["arn:a", "arn:b"]}

    assert _matches_event(eb, event, {"resources": ["arn:a"]}) is True
    assert _matches_event(eb, event, {"resources": ["arn:a", "arn:zzz"]}) is True
    assert _matches_event(eb, event, {"resources": ["arn:zzz", "arn:b"]}) is True
    assert _matches_event(eb, event, {"resources": ["arn:zzz"]}) is False
    assert _matches_event(eb, event, {"resources": ["arn:zzz", "arn:yyy"]}) is False
    # Content filters work on it, OR-ed over the event's elements.
    assert _matches_event(eb, event, {"resources": [{"prefix": "arn:a"}]}) is True
    assert _matches_event(eb, event, {"resources": [{"suffix": ":b"}]}) is True
    assert _matches_event(eb, event, {"resources": [{"wildcard": "arn:*"}]}) is True
    assert _matches_event(eb, event, {"resources": [{"prefix": "arn:z"}]}) is False
    assert _matches_event(eb, event, {"resources": [{"exists": True}]}) is True
    # anything-but is OR-ed over the elements too, so it means "some element is
    # not this", not "no element is this".
    assert _matches_event(eb, event, {"resources": [{"anything-but": ["arn:a"]}]}) is True
    assert _matches_event(eb, event, {"resources": [{"anything-but": ["arn:a", "arn:b"]}]}) is False
    # An empty array is indistinguishable from an absent field.
    empty = {"source": "op.test", "detail-type": "T", "detail": {}, "resources": []}
    assert _matches_event(eb, empty, {"resources": [{"exists": False}]}) is True
    assert _matches_event(eb, empty, {"resources": [{"exists": True}]}) is False
    assert _matches_event(eb, empty, {"resources": ["arn:a"]}) is False


def test_eventbridge_cidr_admits_only_the_address_forms_aws_reads(eb):
    """The value side needs the same admission test as the operand side: AWS
    parses an address with one of two narrow grammars, while ``ipaddress``
    also accepts a zone id and the dotted IPv4-mapped form, so a value in
    either of those shapes matched a block AWS would not have read as an
    address at all."""
    assert _pattern_matches(eb, {"ip": [{"cidr": "fe80::/16"}]}, {"ip": "fe80::1%eth0"}) is False
    assert _pattern_matches(eb, {"ip": [{"cidr": "::ffff:0:0/96"}]},
                            {"ip": "::ffff:10.0.0.1"}) is False
    assert _pattern_matches(eb, {"ip": [{"cidr": "::ffff:0:0/96"}]},
                            {"ip": "::ffff:a00:1"}) is True


def test_eventbridge_cidr_operand_admission_does_not_backtrack():
    """The IPv6 admission pattern reads as two overlapping character classes
    around a colon, which makes the match quadratic on a long colon-only
    string — 100k characters took the better part of a minute, inside one
    request, on the same dispatch hot path ``_wildcard_to_regex`` goes out of
    its way to protect. The bound is loose on purpose: it is here to catch a
    return to quadratic, not to police constant factors."""
    _eb._cidr_network.cache_clear()
    started = time.monotonic()
    assert _eb._matches_cidr("10.0.0.1", ":" * 100_000 + "z/24") is False
    assert time.monotonic() - started < 1.0


# ---------------------------------------------------------------------------
# exists, arrays, $or expansion, and the remaining operator forms
# ---------------------------------------------------------------------------

def test_eventbridge_exists_asks_whether_the_path_reaches_a_leaf(eb):
    """AWS's operators "only work on leaf nodes", and ``exists`` is no exception:
    it asks whether the path reaches a leaf, not whether the key is present. So
    an object-valued field does not exist for this purpose, and neither does an
    empty array or an empty object — all three are indistinguishable from a
    missing key. A null *does* exist; it is a leaf."""
    has_leaf = ("v", "", 0, False, None, ["a", "b"], [None])
    no_leaf = ({"a": 1}, [], {}, [{"x": 1}], [[]])
    for value in has_leaf:
        assert _pattern_matches(eb, {"k": [{"exists": True}]}, {"k": value}) is True, value
        assert _pattern_matches(eb, {"k": [{"exists": False}]}, {"k": value}) is False, value
    for value in no_leaf:
        assert _pattern_matches(eb, {"k": [{"exists": True}]}, {"k": value}) is False, value
        assert _pattern_matches(eb, {"k": [{"exists": False}]}, {"k": value}) is True, value
    # An absent key, and an absent parent chain.
    assert _pattern_matches(eb, {"k": [{"exists": False}]}, {"other": 1}) is True
    assert _pattern_matches(eb, {"k": [{"exists": True}]}, {"other": 1}) is False
    assert _event_matches(eb, {"detail": {"a": {"b": [{"exists": False}]}}}) is True
    # A nested pattern under a non-object value asks about the PATH, so
    # exists:false is satisfied — the leaf is absent however malformed the
    # branch above it is.
    for detail in ("hello", 42, None, [], ["a"]):
        assert _event_matches(eb, {"detail": {"state": [{"exists": False}]}},
                              detail=detail) is True
        assert _event_matches(eb, {"detail": {"state": ["running"]}}, detail=detail) is False
    # `detail` itself is a leaf when it is a scalar, and an object otherwise.
    assert _event_matches(eb, {"detail": ["hello"]}, detail="hello") is True
    assert _event_matches(eb, {"detail": [{"prefix": "hel"}]}, detail="hello") is True
    assert _event_matches(eb, {"detail": [{"exists": True}]}, detail={"a": 1}) is False
    assert _event_matches(eb, {"detail": [{"exists": True}]}, detail=None) is True


def test_eventbridge_array_values_are_flattened_per_element(eb):
    """AWS flattens an array value and offers each element to the matcher on its
    own, so the field matches when ANY element does — and that applies to every
    content filter, not just to plain literals. This matched arrays by literal
    intersection only, so no operator was ever applied to their elements."""
    for rule, value in (({"prefix": "ru"}, ["stopped", "running"]),
                        ({"suffix": "ing"}, ["stopped", "running"]),
                        ({"wildcard": "run*"}, ["x", "running"]),
                        ({"equals-ignore-case": "RUNNING"}, ["x", "running"]),
                        ({"cidr": "10.0.0.0/24"}, ["8.8.8.8", "10.0.0.5"]),
                        ({"numeric": [">", 10]}, [1, 99])):
        assert _pattern_matches(eb, {"k": [rule]}, {"k": value}) is True, rule
    assert _pattern_matches(eb, {"k": [{"prefix": "zz"}]}, {"k": ["a", "b"]}) is False
    # Flattening is recursive, and elements keep their types.
    assert _pattern_matches(eb, {"k": ["a"]}, {"k": [["a"]]}) is True
    assert _pattern_matches(eb, {"k": ["123"]}, {"k": [123]}) is False
    assert _pattern_matches(eb, {"k": [123]}, {"k": [123]}) is True
    # An empty array offers nothing, so nothing matches it.
    assert _pattern_matches(eb, {"k": [{"prefix": ""}]}, {"k": []}) is False
    # anything-but is OR-ed over the elements too: it means "some element is not
    # this", not "no element is this".
    assert _pattern_matches(eb, {"k": [{"anything-but": "stopped"}]},
                            {"k": ["stopped", "running"]}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": "stopped"}]},
                            {"k": ["stopped", "stopped"]}) is False


def test_eventbridge_array_of_objects_matches_per_element(eb):
    """A pattern object under a field whose value is an array of objects is
    offered each element — and has to be satisfied by ONE of them. Two pattern
    fields cannot be answered by two different elements, which is AWS's array
    consistency rule."""
    pattern = {"detail": {"items": {"sku": ["A"], "qty": [2]}}}
    assert _event_matches(eb, pattern, detail={"items": [{"sku": "A", "qty": 2}]}) is True
    assert _event_matches(eb, pattern,
                          detail={"items": [{"sku": "B", "qty": 9},
                                            {"sku": "A", "qty": 2}]}) is True
    # The two fields satisfied by DIFFERENT elements must not match.
    assert _event_matches(eb, pattern,
                          detail={"items": [{"sku": "A", "qty": 9},
                                            {"sku": "B", "qty": 2}]}) is False
    single = {"detail": {"items": {"sku": ["A"]}}}
    assert _event_matches(eb, single, detail={"items": [{"sku": "B"}, {"sku": "A"}]}) is True
    assert _event_matches(eb, single, detail={"items": [{"sku": "B"}]}) is False


def test_eventbridge_or_expands_with_last_write_wins(eb):
    """``$or`` is expanded at rule creation, not evaluated at match time: each
    branch is built by writing leaves into the alternatives accumulated so far,
    so a leaf constrained both inside and outside a ``$or`` keeps only the LAST
    value written, in document order. Writing the two keys the other way round
    reverses the answers — which a recursive evaluator, being order-independent,
    cannot reproduce."""
    or_last = {"detail": {"a": ["1"], "$or": [{"a": ["2"]}, {"b": ["3"]}]}}
    assert _event_matches(eb, or_last, detail={"a": "2"}) is True
    assert _event_matches(eb, or_last, detail={"a": "1"}) is False
    assert _event_matches(eb, or_last, detail={"a": "1", "b": "3"}) is True
    assert _event_matches(eb, or_last, detail={"a": "2", "b": "3"}) is True

    or_first = {"detail": {"$or": [{"a": ["2"]}, {"b": ["3"]}], "a": ["1"]}}
    assert _event_matches(eb, or_first, detail={"a": "2"}) is False
    assert _event_matches(eb, or_first, detail={"a": "1"}) is True
    assert _event_matches(eb, or_first, detail={"a": "1", "b": "3"}) is True

    # A DIFFERENT leaf merges rather than overwriting, at leaf granularity.
    merged = {"detail": {"a": ["1"], "$or": [{"b": ["2"]}, {"c": ["3"]}]}}
    assert _event_matches(eb, merged, detail={"a": "1", "b": "2"}) is True
    assert _event_matches(eb, merged, detail={"b": "2"}) is False
    assert _event_matches(eb, merged, detail={"a": "1", "c": "3"}) is True
    # A duplicate key without any $or is the same rule, and json keeps the last.
    assert _event_matches(eb, {"detail": {"a": ["1"], "b": ["2"]}},
                          detail={"a": "1", "b": "2"}) is True


def test_eventbridge_or_combination_cap_is_enforced(eb):
    """AWS refuses a pattern whose ``$or`` arrays multiply out to more than 1000
    sub-patterns. Parallel ``$or`` at different paths multiply, so eleven of
    arity two are 2048 — and expanding them is work done per rule per event on
    the dispatch path, which is why the cap is a refusal and not a shrug."""
    def parallel(count):
        return {"detail": {f"f{i}": {"$or": [{"x": ["1"]}, {"y": ["1"]}]}
                           for i in range(count)}}
    assert _pattern_refused(eb, parallel(9)) is None
    refusal = _pattern_refused(eb, parallel(11))
    assert refusal and "1000 rule combinations" in refusal, refusal
    # A nested $or contributes its own arity rather than multiplying the outer
    # one, so this stays small and is accepted.
    nested = {"detail": {"$or": [{"a": ["1"]},
                                 {"$or": [{"b": ["2"]}, {"c": ["3"]}]},
                                 {"d": ["4"]}]}}
    assert _pattern_refused(eb, nested) is None
    assert _event_matches(eb, nested, detail={"c": "3"}) is True
    assert _event_matches(eb, nested, detail={"z": "9"}) is False


def test_eventbridge_case_insensitive_prefix_and_suffix(eb):
    """``prefix`` and ``suffix`` take a nested ``equals-ignore-case``, which is
    AWS's spelling of a case-insensitive affix. The same per-character case
    alternation applies, so a length-changing mapping counts at the edges too."""
    assert _pattern_matches(eb, {"k": [{"prefix": {"equals-ignore-case": "EventB"}}]},
                            {"k": "eventbridge"}) is True
    assert _pattern_matches(eb, {"k": [{"prefix": {"equals-ignore-case": "EventB"}}]},
                            {"k": "notevent"}) is False
    assert _pattern_matches(eb, {"k": [{"suffix": {"equals-ignore-case": ".PNG"}}]},
                            {"k": "photo.png"}) is True
    assert _pattern_matches(eb, {"k": [{"suffix": {"equals-ignore-case": ".PNG"}}]},
                            {"k": "photo.jpg"}) is False
    assert _pattern_matches(eb, {"k": [{"prefix": {"equals-ignore-case": "straße"}}]},
                            {"k": "STRASSEN"}) is True
    # A plain affix stays case-sensitive.
    assert _pattern_matches(eb, {"k": [{"prefix": "EventB"}]}, {"k": "eventbridge"}) is False
    # Only equals-ignore-case may nest there.
    assert "Unsupported prefix pattern: suffix" in _pattern_refused(
        eb, {"detail": {"k": [{"prefix": {"suffix": "a"}}]}})
    assert "Prefix expression name not found" in _pattern_refused(
        eb, {"detail": {"k": [{"prefix": {}}]}})


def test_eventbridge_empty_affix_is_refused_only_under_anything_but(eb):
    """``Null prefix/suffix not allowed`` is AWS's rule for ``anything-but``'s
    nested affix and for nothing else: a plain ``{"prefix": ""}`` compiles.
    An affix is matched against the QUOTED form of the value, so the empty operand
    is the quote itself — the opening one for ``prefix``, the closing one for
    ``suffix`` — and the rule reads "this field is a string" rather than filtering
    nothing. Reading the asymmetry as a bug and hoisting
    the refusal into the shared affix validator would reject a pattern real AWS
    accepts."""
    for affix in ({"prefix": ""}, {"suffix": ""},
                  {"prefix": {"equals-ignore-case": ""}},
                  {"suffix": {"equals-ignore-case": ""}}):
        assert _pattern_refused(eb, {"detail": {"k": [affix]}}) is None, affix
        # Every string, the empty one included; no number, boolean, null or
        # object — and an array matches through its string elements.
        assert _pattern_matches(eb, {"k": [affix]}, {"k": "foo"}) is True, affix
        assert _pattern_matches(eb, {"k": [affix]}, {"k": ""}) is True, affix
        for value in (5, True, None, {"n": 1}):
            assert _pattern_matches(eb, {"k": [affix]}, {"k": value}) is False, (affix, value)
        assert _pattern_matches(eb, {"k": [affix]}, {"k": ["foo", 5]}) is True, affix
    # The refusal keeps its own position, and the list form reaches it too.
    reason = _pattern_refused(eb, {"detail": {"k": [{"anything-but": {"prefix": [""]}}]}})
    assert reason and "Null prefix/suffix not allowed" in reason, reason


def test_eventbridge_affix_type_complaint_names_its_own_position(eb):
    """AWS reports a non-string affix operand differently depending on where it
    sits: the top-level branches name the one operator they read, while
    ``anything-but`` handles both in a single branch and says
    ``prefix/suffix``. Both wordings are the service's own, so unifying them
    would send text AWS never sends in either position."""
    assert "Reason: prefix match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"prefix": 5}]}})
    assert "Reason: suffix match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"suffix": 5}]}})
    for bad in ({"prefix": 5}, {"suffix": 5}, {"prefix": [5]}):
        reason = _pattern_refused(eb, {"detail": {"k": [{"anything-but": bad}]}})
        assert "prefix/suffix match pattern must be a string" in reason, (bad, reason)
    # A nested affix operand is reported against ``equals-ignore-case``, whose
    # operand it actually is.
    assert "equals-ignore-case match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"prefix": {"equals-ignore-case": 5}}]}})


def test_eventbridge_exactly_operator_is_an_explicit_equality(eb):
    """``exactly`` is Event Ruler's spelling of a plain equality value, and it is
    a reserved name inside ``$or`` for that reason."""
    assert _pattern_matches(eb, {"k": [{"exactly": "running"}]}, {"k": "running"}) is True
    assert _pattern_matches(eb, {"k": [{"exactly": "running"}]}, {"k": "RUNNING"}) is False
    assert _pattern_matches(eb, {"k": [{"exactly": "5"}]}, {"k": 5}) is False
    assert "exact match pattern must be a string" in _pattern_refused(
        eb, {"detail": {"k": [{"exactly": 5}]}})


def test_eventbridge_pattern_values_compare_as_json_not_as_text(eb):
    """AWS compares a pattern value against an event value as JSON, so a string
    never equals a number and ``true`` never equals ``1`` — where Python holds
    ``True == 1``. Coercing the two to text instead made a rule match values of
    a type it never named."""
    assert _pattern_matches(eb, {"k": ["5"]}, {"k": 5}) is False
    assert _pattern_matches(eb, {"k": [5]}, {"k": "5"}) is False
    assert _pattern_matches(eb, {"k": [5]}, {"k": 5}) is True
    assert _pattern_matches(eb, {"k": [1]}, {"k": True}) is False
    assert _pattern_matches(eb, {"k": [True]}, {"k": 1}) is False
    assert _pattern_matches(eb, {"k": [True]}, {"k": True}) is True
    assert _pattern_matches(eb, {"k": [None]}, {"k": None}) is True
    assert _pattern_matches(eb, {"k": ["None"]}, {"k": None}) is False


def test_eventbridge_replay_name_marks_replayed_events(eb):
    """AWS stamps a replayed event with the replay's name, and its own archive
    managed rule — ``{"replay-name": [{"exists": false}]}`` — is how an archive
    avoids capturing its own replays. The field was never populated, so that
    rule could not tell replayed traffic from live."""
    live = {"source": "op.test", "detail-type": "T", "detail": {}}
    replayed = dict(live, **{"replay-name": "my-replay"})

    assert _matches_event(eb, live, {"replay-name": [{"exists": False}]}) is True
    assert _matches_event(eb, replayed, {"replay-name": [{"exists": False}]}) is False
    assert _matches_event(eb, replayed, {"replay-name": [{"exists": True}]}) is True
    assert _matches_event(eb, replayed, {"replay-name": ["my-replay"]}) is True
    assert _matches_event(eb, replayed, {"replay-name": ["other"]}) is False


def test_eventbridge_replay_name_is_stamped_by_start_replay(eb, sqs):
    """The assertions above pose the question to ``TestEventPattern`` with the
    field written by hand, so they pass whether or not anything ever stamps it.
    This is the one that fails if the stamping regresses: a real archive, a real
    replay, and AWS's own "live traffic only" rule on the receiving end."""
    slug = _uuid_mod.uuid4().hex[:8]
    bus = f"qa-eb-replayname-{slug}-bus"
    eb.create_event_bus(Name=bus)
    bus_arn = eb.describe_event_bus(Name=bus)["Arn"]
    q_url = _eb_rule_to_queue(
        eb, sqs, f"replayname-{slug}",
        {"source": ["op.replay"], "replay-name": [{"exists": False}]}, bus)

    archive = f"qa-eb-replayname-{slug}"
    eb.create_archive(ArchiveName=archive, EventSourceArn=bus_arn)
    eb.put_events(Entries=[{"Source": "op.replay", "DetailType": "T",
                            "Detail": json.dumps({"n": 1}), "EventBusName": bus}])
    live = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10,
                               WaitTimeSeconds=2).get("Messages", [])
    assert len(live) == 1, "the live event must reach the exists:false rule"
    sqs.delete_message(QueueUrl=q_url, ReceiptHandle=live[0]["ReceiptHandle"])

    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    eb.start_replay(
        ReplayName=f"qa-eb-replay-{slug}",
        EventSourceArn=eb.describe_archive(ArchiveName=archive)["ArchiveArn"],
        EventStartTime=now - timedelta(days=1),
        EventEndTime=now + timedelta(days=1),
        # Must be the archive's own source bus, or StartReplay refuses it.
        Destination={"Arn": bus_arn})

    # The replayed event carries replay-name, so the rule must decline it. Polling
    # rather than sleeping: the replay dispatches on a background thread, and an
    # arrival at any point is the regression.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        again = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=10,
                                    WaitTimeSeconds=1).get("Messages", [])
        assert not again, f"replayed event reached a live-traffic-only rule: {again}"


def test_eventbridge_archive_apis_validate_their_event_pattern(eb):
    """Every API that takes a pattern validates it, not just PutRule — otherwise
    a pattern refused at one door gets in through another and the rule that
    carries it can never be created but can be archived on."""
    bus = f"qa-eb-archive-validate-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus)
    bus_arn = eb.describe_event_bus(Name=bus)["Arn"]
    bad = json.dumps({"detail": {"k": [{"numeric": [">"]}]}})

    with pytest.raises(ClientError) as created:
        eb.create_archive(ArchiveName=f"qa-arch-{_uuid_mod.uuid4().hex[:8]}",
                          EventSourceArn=bus_arn, EventPattern=bad)
    assert created.value.response["Error"]["Code"] == "InvalidEventPatternException"

    name = f"qa-arch-ok-{_uuid_mod.uuid4().hex[:8]}"
    eb.create_archive(ArchiveName=name, EventSourceArn=bus_arn,
                      EventPattern=json.dumps({"source": ["op.test"]}))
    with pytest.raises(ClientError) as updated:
        eb.update_archive(ArchiveName=name, EventPattern=bad)
    assert updated.value.response["Error"]["Code"] == "InvalidEventPatternException"
    # The good pattern survived the failed update.
    assert json.loads(eb.describe_archive(ArchiveName=name)["EventPattern"]) == {
        "source": ["op.test"]}


def _raw_event(source="op.test", detail='{"k": "v"}', **extra):
    """A stored event in the shape ``_pattern_event_view`` reads."""
    return {"Source": source, "DetailType": "T", "Detail": detail,
            "Resources": [], "Time": 1_760_000_000, **extra}


def test_eventbridge_compiled_pattern_is_cached_and_caches_refusals():
    """A pattern is parsed and expanded once, not once per event: this runs per
    rule per event on the dispatch path, and expanding a ``$or`` allocates. A
    refusal is cached too, as ``None`` — a rule restored from persisted state
    carries its pattern verbatim, so an unacceptable one would otherwise be
    re-compiled and re-refused for every event on the bus."""
    _eb._compiled_pattern.cache_clear()
    good = json.dumps({"source": ["qa.cache"], "detail": {"k": [{"prefix": "a"}]}})
    assert _eb._compiled_pattern(good) is not None
    assert _eb._compiled_pattern(good) is not None
    assert _eb._compiled_pattern.cache_info().hits == 1

    bad = json.dumps({"source": []})
    assert _eb._compiled_pattern(bad) is None
    assert _eb._compiled_pattern(bad) is None
    assert _eb._compiled_pattern.cache_info().hits == 2
    # Dispatch builds the event view once and reuses it across rules, so the
    # two halves have to be separable.
    view = _eb._pattern_event_view(_raw_event("qa.cache", json.dumps({"k": "abc"})))
    assert _eb._matches_pattern_view(good, view) is True
    assert _eb._matches_pattern_view(bad, view) is False


def test_eventbridge_matcher_takes_the_pattern_as_json_text_only():
    """The matcher's pattern argument is JSON text — the form the API takes a
    pattern in and the form the rule and archive stores keep — and anything else
    is a non-match. Rule records are not all written by PutRule: a CloudFormation
    template's pattern is only stringified when it is an object, and persisted
    state is restored without revalidation, so an unhashable EventPattern can
    reach the matcher, and the compiler is memoized — handing one to lru_cache
    raises TypeError, which out of dispatch is a 500 on the whole PutEvents batch
    rather than one skipped rule."""
    view = _eb._pattern_event_view(_raw_event("qa.nontext", json.dumps({"k": "abc"})))
    assert _eb._matches_pattern_view(json.dumps({"source": ["qa.nontext"]}), view) is True
    for not_text in ({"source": ["qa.nontext"]}, ["qa.nontext"], 7, None):
        assert _eb._matches_pattern_view(not_text, view) is False


def test_eventbridge_reset_clears_the_pattern_compilation_caches():
    """``reset()`` must drop the compilation memos along with the stores. Each is
    keyed on values a caller sent — a pattern, a wildcard operand, a cidr block,
    an event ``time`` — and nothing else evicts an entry below the 1024-entry
    cap, so a run of distinct TestEventPattern calls pins them all: an expanded
    near-cap ``$or`` retains a few hundred times the bytes of its own source. Reset
    has already dropped every rule that could want a result, so keeping them is
    memory that comes back to no one."""
    caches = (_eb._compiled_pattern, _eb._wildcard_regex,
              _eb._cidr_network, _eb._iso_time)
    # One match seeds all four: the view renders the event time, and reaching the
    # operators is what compiles their operands.
    assert _eb._matches_pattern(
        json.dumps({"source": ["qa.reset"],
                    "detail": {"w": [{"wildcard": "a*b"}],
                               "ip": [{"cidr": "10.0.0.0/24"}]}}),
        _raw_event("qa.reset", json.dumps({"w": "aXb", "ip": "10.0.0.7"})),
    ) is True
    seeded = {cache.__name__: cache.cache_info().currsize for cache in caches}
    assert all(seeded.values()), seeded

    _eb.reset()
    assert ({cache.__name__: cache.cache_info().currsize for cache in caches}
            == dict.fromkeys(seeded, 0))


def test_eventbridge_numeric_applies_to_json_numbers_only(eb):
    """A numeric operator applies to a JSON number and nothing else. Python
    converts the text ``"50"`` and the boolean ``true`` happily, so without a
    type check a numeric threshold matched values of a type the rule never
    named — and a rule written to bound an amount fired on a string."""
    below = {"amount": [{"numeric": ["<", 100]}]}
    assert _pattern_matches(eb, below, {"amount": 50}) is True
    assert _pattern_matches(eb, below, {"amount": 50.5}) is True
    assert _pattern_matches(eb, below, {"amount": "50"}) is False
    assert _pattern_matches(eb, below, {"amount": True}) is False
    assert _pattern_matches(eb, below, {"amount": None}) is False
    assert _pattern_matches(eb, below, {"amount": ["50"]}) is False
    assert _pattern_matches(eb, below, {"amount": [50]}) is True


def test_eventbridge_or_leaf_and_object_at_one_path_cannot_be_satisfied(eb):
    """AWS keys its expanded sub-patterns by the full path, so a path constrained
    both as a leaf and as an object keeps BOTH constraints — and no event is
    both, so that alternative matches nothing. Dropping one side instead would
    quietly widen the rule."""
    leaf_then_object = {"detail": {"a": ["1"], "$or": [{"a": {"b": ["2"]}}, {"z": ["9"]}]}}
    assert _event_matches(eb, leaf_then_object, detail={"a": "1"}) is False
    assert _event_matches(eb, leaf_then_object, detail={"a": {"b": "2"}}) is False
    # The other branch is unaffected and still matches.
    assert _event_matches(eb, leaf_then_object, detail={"a": "1", "z": "9"}) is True

    object_then_leaf = {"detail": {"a": {"b": ["1"]}, "$or": [{"a": ["2"]}, {"c": ["3"]}]}}
    assert _event_matches(eb, object_then_leaf, detail={"a": "2"}) is False
    assert _event_matches(eb, object_then_leaf, detail={"a": {"b": "1"}}) is False
    assert _event_matches(eb, object_then_leaf, detail={"a": {"b": "1"}, "c": "3"}) is True


def test_eventbridge_or_cap_counts_aws_documented_product(eb):
    """AWS documents the cap as the product of every ``$or`` array's length. A
    nested ``$or`` produces fewer alternatives than that product, so counting
    only the alternatives would accept a pattern AWS refuses — the direction that
    costs a surprise in production. Both counts are checked."""
    def chain(depth):
        pattern = {"a": ["1"]}
        for _ in range(depth):
            pattern = {"$or": [pattern, {"b": ["2"]}]}
        return {"detail": pattern}
    assert _pattern_refused(eb, chain(9)) is None
    refusal = _pattern_refused(eb, chain(10))
    assert refusal and "1000 rule combinations" in refusal, refusal


def test_eventbridge_or_cap_counts_a_field_named_or_too(eb):
    """AWS states the cap as a product over the ``$or`` arrays the pattern text
    holds — a question the text answers on its own, without being compiled — so
    it is counted before the ``$or``-aware compiler runs and outside the retry
    that re-reads ``$or`` as an ordinary field name. These arrays hold only
    reserved names, so that retry is the whole reason nine of them are legal, and
    it would have made ten legal too. Ten are refused anyway: where the two
    readings disagree, refusing costs a wall here, and accepting risks a rule that
    passes locally and is refused at ``PutRule`` in production."""
    def reserved(count):
        return {"detail": {f"f{i}": {"$or": [{"prefix": "a"}, {"suffix": "b"}]}
                           for i in range(count)}}
    # Nine multiply out to 512 and are accepted — as nine fields literally called
    # "$or", which a detail payload really can carry.
    assert _pattern_refused(eb, reserved(9)) is None
    one = {"f0": {"$or": [{"prefix": "a"}, {"suffix": "b"}]}}
    assert _pattern_matches(eb, one, {"f0": {"$or": "abc"}}) is True
    assert _pattern_matches(eb, one, {"f0": {"$or": "zzb"}}) is True
    assert _pattern_matches(eb, one, {"f0": {"$or": "zzz"}}) is False
    # Ten multiply out to 1024 — the first count past the cap, so this is the
    # boundary itself rather than a number comfortably beyond it. The reason is
    # the count, not anything the branches contain.
    refusal = _pattern_refused(eb, reserved(10))
    assert refusal and "1000 rule combinations" in refusal, refusal
    assert _pattern_refused(eb, reserved(11)) is not None


def test_eventbridge_non_json_numeric_literals_are_refused(eb):
    """``NaN``/``Infinity``/``-Infinity`` are Python decoder extensions, not JSON.
    AWS parses a pattern with Jackson, which refuses them, so accepting one here
    would leave a pattern that is a `400` in production matching nothing locally —
    the silence this whole change exists to remove. A literal that IS valid JSON
    and merely overflows a double (``1e400``) stays accepted."""
    def refusal(pattern_text):
        """Asked with raw pattern TEXT: ``1e400`` is a JSON literal Python cannot
        round-trip through a float, so json.dumps of a dict cannot pose this."""
        try:
            eb.test_event_pattern(
                Event=json.dumps({"source": "v.test", "detail-type": "T",
                                  "detail": {"a": 1}}),
                EventPattern=pattern_text)
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == "InvalidEventPatternException", exc
            return exc.response["Error"]["Message"]
        return None

    for literal in ("NaN", "Infinity", "-Infinity"):
        reason = refusal('{"detail": {"a": [' + literal + ']}}')
        assert reason and "Invalid JSON" in reason, (literal, reason)
    assert refusal('{"detail": {"a": [1e400]}}') is None
    assert refusal('{"detail": {"a": [1]}}') is None


def test_eventbridge_unmodelled_top_level_key_is_matchable_not_dropped(eb):
    """AWS compiles a pattern to a set of paths and has no envelope allow-list, so
    a ``TestEventPattern`` event carrying an unmodelled top-level key must be
    matchable on it. Dropping the key would answer "no match" for a pattern AWS
    satisfies — and would quietly contradict the ``exists`` reading below, which
    is the same claim from the other side."""
    carried = {"source": "v.test", "detail-type": "T", "detail": {}, "nonesuch": "x"}
    bare = {"source": "v.test", "detail-type": "T", "detail": {}}
    assert _matches_event(eb, carried, {"nonesuch": ["x"]}) is True
    assert _matches_event(eb, carried, {"nonesuch": ["y"]}) is False
    assert _matches_event(eb, bare, {"nonesuch": ["x"]}) is False
    assert _matches_event(eb, bare, {"nonesuch": [{"exists": False}]}) is True
    # A modelled name cannot be shadowed by the caller's spelling of it.
    assert _matches_event(eb, dict(carried, Source="evil"), {"source": ["v.test"]}) is True


def test_eventbridge_dotted_pattern_key_resolves_as_nested_path(eb):
    """Event Ruler joins keys with ".", so the dotted and nested spellings are one
    sub-rule. The residual divergence stays pinned: an event FIELD literally named
    "a.b" flattens to the same path on AWS, where it stays one key here."""
    dotted = {"a.b": ["v"]}
    assert _pattern_matches(eb, dotted, {"a": {"b": "v"}}) is True
    assert _pattern_matches(eb, {"a": {"b": ["v"]}}, {"a": {"b": "v"}}) is True
    assert _pattern_matches(eb, dotted, {"a.b": "v"}) is False  # AWS: True


def test_eventbridge_pattern_nesting_bound_clears_real_patterns(eb):
    """The depth bound is ours, not AWS's — it keeps compilation and matching off
    the end of a pooled worker thread's stack. It has to sit above anything a
    real pattern expresses: AWS's documented 4096-character limit allows a few
    hundred levels of one-character keys, so a few dozen must be fine."""
    def deep(depth):
        pattern = {"k": ["v"]}
        for _ in range(depth):
            pattern = {"a": pattern}
        return {"detail": pattern}
    for depth in (10, 52, 80):
        assert _pattern_refused(eb, deep(depth)) is None, depth
    assert _pattern_refused(eb, deep(150))


def test_eventbridge_validator_reports_the_reason_aws_reports(eb):
    """The reason text is the service's own, and which reason it is depends on
    the order AWS's parser reaches things: it dispatches on the first key of a
    match expression and consumes that operand before it can see a second key,
    and it walks an ``anything-but`` list element by element. So a two-key
    expression whose first operand is also wrong reports the operand."""
    rows = [
        ({"k": [{"exists": "x", "y": 1}]}, "exists match pattern must be either true or false."),
        ({"k": [{"prefix": "a", "suffix": "b"}]}, "Only one key allowed in match expression"),
        ({"k": [{"anything-but": ["x", 5, True]}]},
         "Inside anything but list, start|null|boolean is not supported."),
        ({"k": [{"anything-but": ["x", 5]}]}, "mixed type is not supported"),
        # A threshold AWS's own double conversion would move is refused. Only an
        # integer can be checked here — a fractional literal's text is gone by
        # the time the JSON is parsed.
        ({"k": [{"numeric": [">", 2 ** 60 + 1]}]}, "Cannot compare number"),
    ]
    for detail_pattern, reason in rows:
        refusal = _pattern_refused(eb, {"detail": detail_pattern})
        assert refusal and reason in refusal, (detail_pattern, refusal)
    # A threshold that survives the conversion exactly is fine.
    assert _pattern_refused(eb, {"detail": {"k": [{"numeric": [">", 2 ** 53]}]}}) is None
    # A whitespace-only pattern is an attempt at a pattern, not bad JSON.
    try:
        eb.test_event_pattern(Event=json.dumps({"source": "v.test"}), EventPattern="   ")
        raise AssertionError("expected a refusal")
    except ClientError as exc:
        assert "Filter is not an object" in exc.response["Error"]["Message"]


def test_eventbridge_event_time_that_is_not_a_time_does_not_raise(eb):
    """The envelope every pattern is matched against renders ``time`` from the
    stored epoch seconds, so a number no platform clock can represent used to
    raise while BUILDING the view — before any pattern was even consulted — which
    surfaced as `500 InternalError` and took the whole PutEvents with it."""
    for stamp in (10 ** 30, -(10 ** 30), 1e30, 1e300):
        assert _eb._event_time_string({"Time": stamp}) == ""
    assert _eb._event_time_string({"Time": 1_760_000_000}).startswith("20")
    # Reachable through the API, and answered rather than raised.
    resp = eb.test_event_pattern(
        Event=json.dumps({"source": "op.test", "detail-type": "T",
                          "time": 10 ** 30, "detail": {}}),
        EventPattern=json.dumps({"source": ["op.test"]}))
    assert resp["Result"] is True
    assert eb.test_event_pattern(
        Event=json.dumps({"source": "op.test", "detail-type": "T",
                          "time": 10 ** 30, "detail": {}}),
        EventPattern=json.dumps({"time": [{"prefix": "20"}]}))["Result"] is False


def test_eventbridge_detail_is_decoded_only_for_patterns_that_ask_about_it(eb):
    """``detail`` carries the bulk of an event, and the view is built once per
    event on the dispatch path — so decoding a payload no rule on the bus looks
    at is work paid for nothing. An alternative naming no top-level ``detail``
    never asks the matcher about one, which is what makes the deferral safe."""
    view = _eb._pattern_event_view(_raw_event())
    assert _eb._matches_alternative(view, {"source": ["op.test"]}) is True
    assert view._with_detail is None             # nothing decoded yet
    assert _eb._matches_alternative(view, {"detail": {"k": ["v"]}}) is True
    assert view._with_detail["detail"] == {"k": "v"}
    # Decoded once and shared: every rule on the bus matches against this view.
    decoded = view._with_detail
    assert _eb._matches_alternative(view, {"detail": {"k": [{"exists": True}]}}) is True
    assert view._with_detail is decoded
    # A detail that is not JSON stays the leaf it is rather than becoming {}.
    other = _eb._pattern_event_view(_raw_event(detail="not json"))
    assert _eb._matches_alternative(other, {"detail": [{"prefix": "not"}]}) is True


def test_eventbridge_one_payload_decode_per_event_however_many_targets(eb, monkeypatch):
    """Every target of every matching rule is delivered the same event, so the
    payload is decoded once for the event rather than once per target — the
    deferral the view exists for is worth little if a fan-out re-parses. Delivery
    takes the tree the rule matched on, which is also what keeps a rule from
    firing on a field whose delivered value differs."""
    decoded = []
    real = _eb._decoded_detail
    monkeypatch.setattr(_eb, "_decoded_detail",
                        lambda raw: (decoded.append(raw), real(raw))[1])
    event = _raw_event("qa.fanout", EventId="id-1", EventBusName="default",
                       Account="000000000000", Region="us-east-1")
    rule = {"Name": "qa-fanout-rule",
            "Arn": "arn:aws:events:us-east-1:000000000000:rule/qa-fanout-rule"}
    view = _eb._pattern_event_view(event)
    # An ARN no target type claims: delivery stops at the dispatch switch, after
    # the payload the switch is given has been built.
    for target_id in ("t1", "t2", "t3"):
        _eb._invoke_target({"Id": target_id, "Arn": "arn:aws:qa-none:us-east-1:000000000000:x"},
                           event, rule, view)
    assert len(decoded) == 1, decoded


def test_eventbridge_event_view_never_hands_out_an_undecoded_detail(eb):
    """Every tree the view hands out carries the event's own ``detail`` or no
    ``detail`` at all — an absent field is what AWS matches against, where a
    fabricated ``null`` is a leaf a content filter or ``{"exists": true}`` would
    answer about. Why a ``dict`` subclass cannot keep that promise is in
    ``_EventView``."""
    view = _eb._pattern_event_view(_raw_event())
    assert not isinstance(view, dict)
    # Whatever a consumer does with a tree the view hands out — walk it, log it,
    # re-encode it — ``detail`` is the event's own or it is not there at all.
    envelope_only = json.loads(json.dumps(view.envelope()))
    assert "detail" not in envelope_only
    with_detail = json.loads(json.dumps(view.event_with_detail()))
    assert with_detail["detail"] == {"k": "v"}


def test_eventbridge_non_json_detail_is_delivered_not_raised(eb, sqs):
    """A ``Detail`` that is not JSON is a leaf a pattern can match on, so delivery
    has to read it the same tolerant way the matcher does. Decoding it there with
    a bare ``json.loads`` raised out of the target invocation, which surfaces as
    `500 InternalError` from the enclosing PutEvents and takes every other rule on
    the bus with it — and the prefix filter below is exactly the pattern that
    makes such an event reach a target in the first place."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bus_name = f"qa-eb-raw-detail-bus-{suffix}"
    eb.create_event_bus(Name=bus_name)
    q_url = _eb_rule_to_queue(eb, sqs, f"raw-detail-{suffix}",
                              {"detail": [{"prefix": "not"}]}, bus_name)
    resp = eb.put_events(Entries=[{"Source": "myapp", "DetailType": "t",
                                   "Detail": "not json",
                                   "EventBusName": bus_name}])
    assert resp["FailedEntryCount"] == 0
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    assert len(msgs.get("Messages", [])) == 1
    assert json.loads(msgs["Messages"][0]["Body"])["detail"] == "not json"


def test_eventbridge_hostile_pattern_and_event_text_never_raises(eb):
    """Three shapes that escaped the guards as `500 InternalError`, each reachable
    from a short ASCII request body. A pattern fault must be a 400 and an event
    fault a non-match — never an exception, which from dispatch takes the whole
    PutEvents and every other rule on the bus with it."""
    # A JSON escape may carry a lone surrogate, which survives the parse and
    # cannot be UTF-8 encoded — and the wildcard fault position is a byte count.
    surrogate = {"detail": {"k": [{"wildcard": "\ud800**"}]}}
    refusal = _pattern_refused(eb, surrogate)
    assert refusal and "Consecutive wildcard characters at pos 2" in refusal, refusal
    assert _matcher_answer(surrogate, {"k": "x"}) is False
    # An integer literal of more digits than Python will convert raises a plain
    # ValueError out of the parse, not a JSONDecodeError.
    try:
        eb.test_event_pattern(Event=json.dumps({"source": "v.test"}),
                              EventPattern='{"k":[1' + "0" * 5000 + "]}")
        raise AssertionError("expected a refusal")
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidEventPatternException"
    # An event may nest arrays as deep as its author likes, and flattening
    # recurses per level while building the view every rule is matched against.
    assert _eb._flatten(json.loads("[" * 60 + "1" + "]" * 60), False) == []
    assert _eb._flatten(json.loads("[" * 3 + "1" + "]" * 3), False) == [1]


def test_eventbridge_anything_but_compares_literals_as_json(eb):
    """``anything-but`` excludes literals, and it has to compare them as JSON:
    Python holds ``True == 1``, so a rule excluding ``1`` also excluded ``true``
    — a value it never named."""
    assert _pattern_matches(eb, {"k": [{"anything-but": [1]}]}, {"k": True}) is True
    assert _pattern_matches(eb, {"k": [{"anything-but": [1]}]}, {"k": 1}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": ["x"]}]}, {"k": "x"}) is False
    assert _pattern_matches(eb, {"k": [{"anything-but": ["x"]}]}, {"k": "y"}) is True


def test_eventbridge_case_insensitive_suffix_does_not_scan_quadratically():
    """A case-insensitive suffix used to run the forward walk from every start
    offset in the value, which is quadratic in the two lengths — one match could
    hold a worker thread, on the same dispatch path the wildcard translator is
    careful about. Walking the operand backwards from the end answers it in one
    pass. The bound is loose on purpose: it catches a return to quadratic."""
    value = "x" * 200_000 + ".png"
    started = time.monotonic()
    assert _eb._matches_content_filter(
        value, {"suffix": {"equals-ignore-case": ".PNG" * 200}}) is False
    assert _eb._matches_content_filter(
        value, {"suffix": {"equals-ignore-case": ".PNG"}}) is True
    assert time.monotonic() - started < 1.0


# ---------------------------------------------------------------------------
# Dotted pattern keys — a dotted key and the nested form are the same sub-rule.
# ---------------------------------------------------------------------------

def test_eventbridge_dotted_key_matches_nested_field(eb):
    assert _matches_event(
        eb,
        {"source": "dot.test", "detail-type": "T", "detail": {"name": "user.created"}},
        {"detail.name": ["user.created"]}) is True
    assert _matches_event(
        eb,
        {"source": "dot.test", "detail-type": "T", "detail": {"name": "other"}},
        {"detail.name": ["user.created"]}) is False


def test_eventbridge_dotted_key_equals_nested_form(eb):
    event = {"source": "dot.test", "detail-type": "T",
             "detail": {"state": {"status": "running"}}}
    assert _matches_event(eb, event, {"detail.state.status": ["running"]}) is True
    assert _matches_event(eb, event, {"detail": {"state.status": ["running"]}}) is True
    assert _matches_event(eb, event, {"detail": {"state": {"status": ["running"]}}}) is True


def test_eventbridge_dotted_key_with_object_value(eb):
    event = {"source": "dot.test", "detail-type": "T",
             "detail": {"state": {"status": "running"}}}
    assert _matches_event(eb, event, {"detail.state": {"status": ["running"]}}) is True
    assert _matches_event(eb, event, {"detail.state": {"status": ["stopped"]}}) is False


def test_eventbridge_dotted_key_operators(eb):
    event = {"source": "dot.test", "detail-type": "T", "detail": {"name": "order.paid"}}
    assert _matches_event(eb, event, {"detail.name": [{"prefix": "order."}]}) is True
    assert _matches_event(eb, event, {"detail.name": [{"exists": True}]}) is True
    assert _matches_event(eb, event, {"detail.other": [{"exists": False}]}) is True


def test_eventbridge_dotted_key_inside_or(eb):
    event = {"source": "dot.test", "detail-type": "T", "detail": {"name": "a"}}
    pattern = {"$or": [{"detail.name": ["a"]}, {"detail.name": ["b"]}]}
    assert _matches_event(eb, event, pattern) is True
    assert _matches_event(
        eb, {"source": "dot.test", "detail-type": "T", "detail": {"name": "c"}},
        pattern) is False


def test_eventbridge_dotted_key_leaf_collision_is_last_write_wins(eb):
    """Both spellings write the same leaf, so document order decides."""
    event = {"source": "dot.test", "detail-type": "T", "detail": {"name": "b"}}
    assert _matches_event(
        eb, event, {"detail.name": ["a"], "detail": {"name": ["b"]}}) is True
    assert _matches_event(
        eb, event, {"detail": {"name": ["b"]}, "detail.name": ["a"]}) is False


def test_eventbridge_dotted_key_rule_delivers_to_sqs(eb, sqs):
    queue_name = f"dotted-key-{_uuid_mod.uuid4().hex[:8]}"
    rule_name = f"dotted-key-{_uuid_mod.uuid4().hex[:8]}"
    q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(Name=rule_name,
                EventPattern=json.dumps({"detail.name": ["dotted.ping"]}),
                State="ENABLED")
    eb.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": queue_arn}])

    eb.put_events(Entries=[{
        "Source": "dot.test",
        "DetailType": "T",
        "Detail": json.dumps({"name": "dotted.ping", "payload": {"ok": True}}),
        "EventBusName": "default",
    }])

    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["detail"]["name"] == "dotted.ping"
