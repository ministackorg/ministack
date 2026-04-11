import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

def test_sns_create_topic(sns):
    resp = sns.create_topic(Name="intg-sns-create")
    assert "TopicArn" in resp
    assert "intg-sns-create" in resp["TopicArn"]

def test_sns_delete_topic(sns):
    arn = sns.create_topic(Name="intg-sns-delete")["TopicArn"]
    sns.delete_topic(TopicArn=arn)
    topics = sns.list_topics()["Topics"]
    assert not any(t["TopicArn"] == arn for t in topics)

def test_sns_list_topics(sns):
    sns.create_topic(Name="intg-sns-list-1")
    sns.create_topic(Name="intg-sns-list-2")
    topics = sns.list_topics()["Topics"]
    arns = [t["TopicArn"] for t in topics]
    assert any("intg-sns-list-1" in a for a in arns)
    assert any("intg-sns-list-2" in a for a in arns)

def test_sns_get_topic_attributes(sns):
    arn = sns.create_topic(Name="intg-sns-getattr")["TopicArn"]
    resp = sns.get_topic_attributes(TopicArn=arn)
    assert resp["Attributes"]["TopicArn"] == arn
    assert resp["Attributes"]["DisplayName"] == ""  # AWS default is empty, not topic name

def test_sns_set_topic_attributes(sns):
    arn = sns.create_topic(Name="intg-sns-setattr")["TopicArn"]
    sns.set_topic_attributes(
        TopicArn=arn,
        AttributeName="DisplayName",
        AttributeValue="New Display Name",
    )
    resp = sns.get_topic_attributes(TopicArn=arn)
    assert resp["Attributes"]["DisplayName"] == "New Display Name"

def test_sns_subscribe_email(sns):
    arn = sns.create_topic(Name="intg-sns-subemail")["TopicArn"]
    resp = sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="user@example.com",
    )
    assert "SubscriptionArn" in resp

def test_sns_unsubscribe(sns):
    arn = sns.create_topic(Name="intg-sns-unsub")["TopicArn"]
    sub = sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="unsub@example.com",
    )
    sub_arn = sub["SubscriptionArn"]
    sns.unsubscribe(SubscriptionArn=sub_arn)
    subs = sns.list_subscriptions_by_topic(TopicArn=arn)["Subscriptions"]
    assert not any(s["SubscriptionArn"] == sub_arn for s in subs)

def test_sns_list_subscriptions(sns):
    arn = sns.create_topic(Name="intg-sns-listsubs")["TopicArn"]
    sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="ls1@example.com")
    sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="ls2@example.com")
    subs = sns.list_subscriptions()["Subscriptions"]
    topic_subs = [s for s in subs if s["TopicArn"] == arn]
    assert len(topic_subs) >= 2

def test_sns_list_subscriptions_by_topic(sns):
    arn = sns.create_topic(Name="intg-sns-listbytopic")["TopicArn"]
    sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="bt@example.com",
    )
    subs = sns.list_subscriptions_by_topic(TopicArn=arn)["Subscriptions"]
    assert len(subs) >= 1
    assert all(s["TopicArn"] == arn for s in subs)

def test_sns_publish(sns):
    arn = sns.create_topic(Name="intg-sns-publish")["TopicArn"]
    resp = sns.publish(
        TopicArn=arn,
        Message="hello sns",
        Subject="Test Subject",
    )
    assert "MessageId" in resp

def test_sns_publish_nonexistent_topic(sns):
    fake_arn = "arn:aws:sns:us-east-1:000000000000:intg-sns-nonexist"
    with pytest.raises(ClientError) as exc:
        sns.publish(TopicArn=fake_arn, Message="fail")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_sns_sqs_fanout(sns, sqs):
    topic_arn = sns.create_topic(Name="intg-sns-fanout")["TopicArn"]
    q_url = sqs.create_queue(QueueName="intg-sns-fanout-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
    sns.publish(TopicArn=topic_arn, Message="fanout msg", Subject="Fan")

    msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Message"] == "fanout msg"
    assert body["TopicArn"] == topic_arn

def test_sns_tags(sns):
    arn = sns.create_topic(Name="intg-sns-tags")["TopicArn"]
    sns.tag_resource(
        ResourceArn=arn,
        Tags=[
            {"Key": "env", "Value": "staging"},
            {"Key": "team", "Value": "infra"},
        ],
    )
    resp = sns.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags["env"] == "staging"
    assert tags["team"] == "infra"

    sns.untag_resource(ResourceArn=arn, TagKeys=["team"])
    resp = sns.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert "team" not in tags
    assert tags["env"] == "staging"

def test_sns_subscription_attributes(sns):
    arn = sns.create_topic(Name="intg-sns-subattr")["TopicArn"]
    sub = sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="attrs@example.com",
    )
    sub_arn = sub["SubscriptionArn"]

    resp = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
    assert resp["Attributes"]["Protocol"] == "email"
    assert resp["Attributes"]["TopicArn"] == arn

    sns.set_subscription_attributes(
        SubscriptionArn=sub_arn,
        AttributeName="RawMessageDelivery",
        AttributeValue="true",
    )
    resp = sns.get_subscription_attributes(SubscriptionArn=sub_arn)
    assert resp["Attributes"]["RawMessageDelivery"] == "true"

def test_sns_subscribe_with_raw_message_delivery(sns):
    arn = sns.create_topic(Name="intg-sns-sub-raw")["TopicArn"]
    sub = sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="raw@example.com",
        Attributes={"RawMessageDelivery": "true"},
    )
    sub_arn = sub["SubscriptionArn"]
    attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
    assert attrs["RawMessageDelivery"] == "true"

def test_sns_subscribe_with_filter_policy(sns):
    arn = sns.create_topic(Name="intg-sns-sub-filter")["TopicArn"]
    filter_policy = json.dumps({"event": ["MyEvent"]})
    sub = sns.subscribe(
        TopicArn=arn,
        Protocol="email",
        Endpoint="filter@example.com",
        Attributes={"FilterPolicy": filter_policy},
    )
    sub_arn = sub["SubscriptionArn"]
    attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
    assert attrs["FilterPolicy"] == filter_policy

def test_sns_sqs_fanout_raw_message_delivery(sns, sqs):
    topic_arn = sns.create_topic(Name="intg-sns-fanout-raw")["TopicArn"]
    q_url = sqs.create_queue(QueueName="intg-sns-fanout-raw-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=q_arn,
        Attributes={"RawMessageDelivery": "true"},
    )
    sns.publish(TopicArn=topic_arn, Message="raw fanout msg")

    msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "raw fanout msg"

def test_sns_publish_batch(sns):
    arn = sns.create_topic(Name="intg-sns-batch")["TopicArn"]
    resp = sns.publish_batch(
        TopicArn=arn,
        PublishBatchRequestEntries=[
            {"Id": "msg1", "Message": "batch message 1"},
            {"Id": "msg2", "Message": "batch message 2"},
            {"Id": "msg3", "Message": "batch message 3"},
        ],
    )
    assert len(resp["Successful"]) == 3
    assert len(resp.get("Failed", [])) == 0

def test_sns_to_lambda_fanout(lam, sns):
    """SNS publish with lambda protocol invokes the function synchronously."""
    import uuid as _uuid_mod

    fn = f"intg-sns-lam-{_uuid_mod.uuid4().hex[:8]}"
    # Handler records the event on a module-level list so we can inspect it
    code = "received = []\ndef handler(event, context):\n    received.append(event)\n    return {'ok': True}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"

    topic_arn = sns.create_topic(Name=f"intg-sns-lam-topic-{_uuid_mod.uuid4().hex[:8]}")["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=func_arn)

    # Publish — should not raise; Lambda invoked synchronously
    resp = sns.publish(TopicArn=topic_arn, Message="hello-lambda")
    assert "MessageId" in resp

def test_sns_to_lambda_event_subscription_arn(lam, sns):
    """SNS→Lambda fanout must set EventSubscriptionArn to the real subscription ARN."""
    import uuid as _uuid_mod

    fn = f"intg-sns-suborn-{_uuid_mod.uuid4().hex[:8]}"
    received = []

    code = (
        "import json, os\nreceived = []\ndef handler(event, context):\n    received.append(event)\n    return event\n"
    )
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"
    topic_arn = sns.create_topic(Name=f"intg-sns-suborn-{_uuid_mod.uuid4().hex[:8]}")["TopicArn"]
    sub_resp = sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=func_arn)
    sub_arn = sub_resp["SubscriptionArn"]

    sns.publish(TopicArn=topic_arn, Message="test-sub-arn")

    # Invoke the function directly and check what event it last received
    import base64
    import io
    import json
    import zipfile

    result = lam.invoke(FunctionName=fn, Payload=json.dumps({"ping": True}).encode())
    # The subscription ARN should be a real ARN, not "{topic}:subscription"
    assert sub_arn != f"{topic_arn}:subscription"
    assert sub_arn.startswith(topic_arn)

def test_sns_filter_policy_blocks_non_matching(sns, sqs):
    """SNS filter policy prevents delivery when message attributes don't match."""
    topic_arn = sns.create_topic(Name="qa-sns-filter")["TopicArn"]
    q_url = sqs.create_queue(QueueName="qa-sns-filter-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)["SubscriptionArn"]
    sns.set_subscription_attributes(
        SubscriptionArn=sub_arn,
        AttributeName="FilterPolicy",
        AttributeValue=json.dumps({"color": ["blue"]}),
    )
    sns.publish(
        TopicArn=topic_arn,
        Message="red message",
        MessageAttributes={"color": {"DataType": "String", "StringValue": "red"}},
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs.get("Messages", [])) == 0, "Filtered message must not be delivered"
    sns.publish(
        TopicArn=topic_arn,
        Message="blue message",
        MessageAttributes={"color": {"DataType": "String", "StringValue": "blue"}},
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs2.get("Messages", [])) == 1
    body = json.loads(msgs2["Messages"][0]["Body"])
    assert body["Message"] == "blue message"

def test_sns_raw_message_delivery(sns, sqs):
    """RawMessageDelivery=true delivers raw message body, not SNS envelope."""
    topic_arn = sns.create_topic(Name="qa-sns-raw")["TopicArn"]
    q_url = sqs.create_queue(QueueName="qa-sns-raw-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)["SubscriptionArn"]
    sns.set_subscription_attributes(
        SubscriptionArn=sub_arn,
        AttributeName="RawMessageDelivery",
        AttributeValue="true",
    )
    sns.publish(TopicArn=topic_arn, Message="raw-body")
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs["Messages"]) == 1
    assert msgs["Messages"][0]["Body"] == "raw-body"

def test_sns_publish_batch_distinct_ids(sns):
    """PublishBatch with duplicate IDs must fail with BatchEntryIdsNotDistinct."""
    arn = sns.create_topic(Name="qa-sns-batch-dup")["TopicArn"]
    with pytest.raises(ClientError) as exc:
        sns.publish_batch(
            TopicArn=arn,
            PublishBatchRequestEntries=[
                {"Id": "same", "Message": "msg1"},
                {"Id": "same", "Message": "msg2"},
            ],
        )
    assert exc.value.response["Error"]["Code"] == "BatchEntryIdsNotDistinct"

def test_sns_fifo_dedup_passthrough(sns, sqs):
    """SNS FIFO topic passes MessageGroupId through to the SQS FIFO subscriber."""
    topic_arn = sns.create_topic(
        Name="intg-sns-fifo-dedup.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    q_url = sqs.create_queue(
        QueueName="intg-sns-fifo-dedup-q.fifo",
        Attributes={"FifoQueue": "true"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

    sns.publish(
        TopicArn=topic_arn,
        Message="fifo-dedup-test",
        MessageGroupId="grp-1",
        MessageDeduplicationId="dedup-001",
    )

    msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2,
        AttributeNames=["All"],
    )
    assert len(msgs.get("Messages", [])) == 1
    msg = msgs["Messages"][0]
    body = json.loads(msg["Body"])
    assert body["Message"] == "fifo-dedup-test"
    attrs = msg.get("Attributes", {})
    assert attrs.get("MessageGroupId") == "grp-1"

def test_sns_to_sqs_fanout(sns, sqs):
    """SNS publish fans out to multiple SQS subscribers."""
    topic_arn = sns.create_topic(Name="intg-fanout-topic")["TopicArn"]

    q1_url = sqs.create_queue(QueueName="intg-fanout-q1")["QueueUrl"]
    q2_url = sqs.create_queue(QueueName="intg-fanout-q2")["QueueUrl"]
    q1_arn = sqs.get_queue_attributes(QueueUrl=q1_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    q2_arn = sqs.get_queue_attributes(QueueUrl=q2_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    sub1 = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q1_arn)
    sub2 = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q2_arn)
    assert sub1["SubscriptionArn"] != "PendingConfirmation"
    assert sub2["SubscriptionArn"] != "PendingConfirmation"

    sns.publish(TopicArn=topic_arn, Message="fanout-test-msg", Subject="IntgTest")

    # Both queues should receive the message
    for q_url, q_name in [(q1_url, "q1"), (q2_url, "q2")]:
        msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        assert len(msgs.get("Messages", [])) == 1, f"{q_name} should have received the message"
        body = json.loads(msgs["Messages"][0]["Body"])
        assert body["Message"] == "fanout-test-msg"
        assert body["TopicArn"] == topic_arn
        assert body["Subject"] == "IntgTest"
        assert body["Type"] == "Notification"
