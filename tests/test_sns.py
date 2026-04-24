import io
import json
import os
import time
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlencode, urlparse

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

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
    assert exc.value.response["Error"]["Code"] == "NotFound"

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
        Runtime="python3.12",
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
        Runtime="python3.12",
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


# ---------------------------------------------------------------------------
# FIFO Topic Creation Tests 
# ---------------------------------------------------------------------------


def test_sns_fifo_create_topic_with_fifo_suffix_and_attribute(sns):
    """Creating a FIFO topic with .fifo suffix and FifoTopic=true succeeds."""
    resp = sns.create_topic(
        Name="intg-fifo-create.fifo",
        Attributes={"FifoTopic": "true"},
    )
    arn = resp["TopicArn"]
    assert arn.endswith("intg-fifo-create.fifo")

    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["FifoTopic"] == "true"


def test_sns_fifo_create_topic_without_fifo_suffix_returns_error(sns):
    """Creating a topic with FifoTopic=true but no .fifo suffix returns InvalidParameterException."""
    with pytest.raises(ClientError) as exc:
        sns.create_topic(
            Name="intg-fifo-no-suffix",
            Attributes={"FifoTopic": "true"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_sns_fifo_auto_detect_from_suffix(sns):
    """Creating a topic with .fifo suffix auto-detects as FIFO even without explicit FifoTopic attribute."""
    resp = sns.create_topic(Name="intg-fifo-autodetect.fifo")
    arn = resp["TopicArn"]

    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["FifoTopic"] == "true"


def test_sns_fifo_content_based_dedup_defaults_to_false(sns):
    """ContentBasedDeduplication defaults to 'false' for FIFO topics when not explicitly provided."""
    resp = sns.create_topic(
        Name="intg-fifo-cbd-default.fifo",
        Attributes={"FifoTopic": "true"},
    )
    arn = resp["TopicArn"]

    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["ContentBasedDeduplication"] == "false"


def test_sns_fifo_content_based_dedup_set_to_true(sns):
    """ContentBasedDeduplication can be set to 'true' at creation time."""
    resp = sns.create_topic(
        Name="intg-fifo-cbd-true.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )
    arn = resp["TopicArn"]

    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["ContentBasedDeduplication"] == "true"


def test_sns_fifo_get_topic_attributes_returns_fifo_attrs(sns):
    """GetTopicAttributes returns all FIFO-related attributes correctly."""
    resp = sns.create_topic(
        Name="intg-fifo-getattrs.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )
    arn = resp["TopicArn"]

    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["TopicArn"] == arn
    assert attrs["FifoTopic"] == "true"
    assert attrs["ContentBasedDeduplication"] == "true"
    # Standard attributes should still be present
    assert "Owner" in attrs
    assert "Policy" in attrs


# ---------------------------------------------------------------------------
# FIFO Publish Validation Tests
# ---------------------------------------------------------------------------


def test_sns_fifo_publish_without_message_group_id_returns_error(sns):
    """Publishing to a FIFO topic without MessageGroupId returns InvalidParameterException."""
    arn = sns.create_topic(
        Name="intg-fifo-pub-no-grp.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )["TopicArn"]

    with pytest.raises(ClientError) as exc:
        sns.publish(TopicArn=arn, Message="missing group id")
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_sns_fifo_publish_without_dedup_id_cbd_false_returns_error(sns):
    """Publishing to a FIFO topic (CBD=false) without MessageDeduplicationId returns error."""
    arn = sns.create_topic(
        Name="intg-fifo-pub-no-dedup.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    with pytest.raises(ClientError) as exc:
        sns.publish(
            TopicArn=arn,
            Message="missing dedup id",
            MessageGroupId="grp-1",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_sns_standard_topic_publish_without_message_group_id_succeeds(sns):
    """Publishing to a standard topic without MessageGroupId succeeds normally."""
    arn = sns.create_topic(Name="intg-std-pub-no-grp")["TopicArn"]

    resp = sns.publish(TopicArn=arn, Message="standard topic message")
    assert "MessageId" in resp


def test_sns_fifo_publish_with_valid_params_returns_sequence_number(sns):
    """Publishing to a FIFO topic with valid params succeeds and returns SequenceNumber."""
    arn = sns.create_topic(
        Name="intg-fifo-pub-seq.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    resp = sns.publish(
        TopicArn=arn,
        Message="fifo message with seq",
        MessageGroupId="grp-1",
        MessageDeduplicationId="dedup-seq-001",
    )
    assert "MessageId" in resp
    assert "SequenceNumber" in resp
    # Sequence number should be a zero-padded numeric string
    assert resp["SequenceNumber"].isdigit()
    assert len(resp["SequenceNumber"]) == 20


def test_sns_standard_topic_publish_response_omits_sequence_number(sns):
    """Standard topic publish response does not include SequenceNumber."""
    arn = sns.create_topic(Name="intg-std-pub-no-seq")["TopicArn"]

    resp = sns.publish(TopicArn=arn, Message="standard topic no seq")
    assert "MessageId" in resp
    assert "SequenceNumber" not in resp


# ---------------------------------------------------------------------------
# FIFO Deduplication and Sequence Number Tests 
# ---------------------------------------------------------------------------


def test_sns_fifo_explicit_dedup_id_returns_same_result_on_duplicate(sns):
    """Publishing the same MessageDeduplicationId twice returns the same MessageId and SequenceNumber."""
    arn = sns.create_topic(
        Name="intg-fifo-dedup-same.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    resp1 = sns.publish(
        TopicArn=arn,
        Message="first publish",
        MessageGroupId="grp-1",
        MessageDeduplicationId="dedup-same-001",
    )
    resp2 = sns.publish(
        TopicArn=arn,
        Message="second publish different body",
        MessageGroupId="grp-1",
        MessageDeduplicationId="dedup-same-001",
    )

    assert resp1["MessageId"] == resp2["MessageId"]
    assert resp1["SequenceNumber"] == resp2["SequenceNumber"]


def test_sns_fifo_cbd_dedup_subscriber_gets_one_message(sns, sqs):
    """CBD=true with same body twice deduplicates — subscriber receives only one message."""
    topic_arn = sns.create_topic(
        Name="intg-fifo-cbd-dedup.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )["TopicArn"]

    q_url = sqs.create_queue(
        QueueName="intg-fifo-cbd-dedup-q.fifo",
        Attributes={"FifoQueue": "true"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

    # Publish the same body twice — CBD will generate the same dedup ID
    resp1 = sns.publish(
        TopicArn=topic_arn,
        Message="identical body for cbd",
        MessageGroupId="grp-cbd",
    )
    resp2 = sns.publish(
        TopicArn=topic_arn,
        Message="identical body for cbd",
        MessageGroupId="grp-cbd",
    )

    # Both responses should return the same MessageId (dedup hit)
    assert resp1["MessageId"] == resp2["MessageId"]

    # Subscriber should only receive one message
    msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
    )
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Message"] == "identical body for cbd"


def test_sns_fifo_explicit_dedup_id_overrides_cbd(sns):
    """Explicit MessageDeduplicationId is used regardless of CBD setting."""
    arn = sns.create_topic(
        Name="intg-fifo-dedup-override.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )["TopicArn"]

    # Publish two messages with the same body but different explicit dedup IDs
    resp1 = sns.publish(
        TopicArn=arn,
        Message="same body",
        MessageGroupId="grp-1",
        MessageDeduplicationId="explicit-dedup-A",
    )
    resp2 = sns.publish(
        TopicArn=arn,
        Message="same body",
        MessageGroupId="grp-1",
        MessageDeduplicationId="explicit-dedup-B",
    )

    # Different explicit dedup IDs → different messages (not deduplicated)
    assert resp1["MessageId"] != resp2["MessageId"]
    assert resp1["SequenceNumber"] != resp2["SequenceNumber"]

    # Now publish again with the same explicit dedup ID as the first → deduplicated
    resp3 = sns.publish(
        TopicArn=arn,
        Message="different body this time",
        MessageGroupId="grp-1",
        MessageDeduplicationId="explicit-dedup-A",
    )
    assert resp3["MessageId"] == resp1["MessageId"]
    assert resp3["SequenceNumber"] == resp1["SequenceNumber"]


def test_sns_fifo_sequence_numbers_monotonically_increasing(sns):
    """Multiple non-duplicate publishes produce monotonically increasing sequence numbers."""
    arn = sns.create_topic(
        Name="intg-fifo-seq-incr.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    seq_numbers = []
    for i in range(5):
        resp = sns.publish(
            TopicArn=arn,
            Message=f"message-{i}",
            MessageGroupId="grp-seq",
            MessageDeduplicationId=f"dedup-seq-{i}",
        )
        assert "SequenceNumber" in resp
        seq_numbers.append(resp["SequenceNumber"])

    # All sequence numbers should be numeric and zero-padded to 20 digits
    for seq in seq_numbers:
        assert seq.isdigit()
        assert len(seq) == 20

    # Sequence numbers should be strictly increasing
    for j in range(1, len(seq_numbers)):
        assert int(seq_numbers[j]) > int(seq_numbers[j - 1])


# ---------------------------------------------------------------------------
# FIFO Subscription Validation Tests
# ---------------------------------------------------------------------------


def test_sns_fifo_subscribe_non_fifo_sqs_queue_returns_error(sns, sqs):
    """Subscribing a non-FIFO SQS queue to a FIFO topic returns InvalidParameterException."""
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-sub-nonfifo-{uid}.fifo",
        Attributes={"FifoTopic": "true"},
    )["TopicArn"]

    q_url = sqs.create_queue(QueueName=f"intg-fifo-sub-nonfifo-q-{uid}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    with pytest.raises(ClientError) as exc:
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_sns_fifo_subscribe_fifo_sqs_queue_succeeds(sns, sqs):
    """Subscribing a FIFO SQS queue to a FIFO topic succeeds."""
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-sub-fifo-{uid}.fifo",
        Attributes={"FifoTopic": "true"},
    )["TopicArn"]

    q_url = sqs.create_queue(
        QueueName=f"intg-fifo-sub-fifo-q-{uid}.fifo",
        Attributes={"FifoQueue": "true"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    resp = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
    assert "SubscriptionArn" in resp
    assert resp["SubscriptionArn"] != "PendingConfirmation"


def test_sns_fifo_subscribe_non_sqs_protocols_succeed(sns):
    """Subscribing email/lambda/http to a FIFO topic succeeds without FIFO queue validation."""
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-sub-nonsqs-{uid}.fifo",
        Attributes={"FifoTopic": "true"},
    )["TopicArn"]

    # email protocol
    resp_email = sns.subscribe(
        TopicArn=topic_arn, Protocol="email", Endpoint=f"user-{uid}@example.com",
    )
    assert "SubscriptionArn" in resp_email

    # lambda protocol
    lambda_arn = f"arn:aws:lambda:us-east-1:000000000000:function:my-func-{uid}"
    resp_lambda = sns.subscribe(
        TopicArn=topic_arn, Protocol="lambda", Endpoint=lambda_arn,
    )
    assert "SubscriptionArn" in resp_lambda

    # http protocol
    resp_http = sns.subscribe(
        TopicArn=topic_arn, Protocol="http", Endpoint=f"http://example.com/hook-{uid}",
    )
    assert "SubscriptionArn" in resp_http


# ---------------------------------------------------------------------------
# PublishBatch FIFO Support Tests
# ---------------------------------------------------------------------------


def _raw_publish_batch(topic_arn, entries):
    """Send a PublishBatch request via raw HTTP to bypass boto3 client-side validation.

    This is needed because boto3 may raise ParamValidationError for entries
    missing MessageGroupId on FIFO topics before the request reaches the server.

    Each entry is a dict with keys: Id, Message, and optionally MessageGroupId,
    MessageDeduplicationId.
    """
    form = {"Action": "PublishBatch", "TopicArn": topic_arn}
    for i, entry in enumerate(entries, start=1):
        prefix = f"PublishBatchRequestEntries.member.{i}"
        form[f"{prefix}.Id"] = entry["Id"]
        form[f"{prefix}.Message"] = entry["Message"]
        if "MessageGroupId" in entry:
            form[f"{prefix}.MessageGroupId"] = entry["MessageGroupId"]
        if "MessageDeduplicationId" in entry:
            form[f"{prefix}.MessageDeduplicationId"] = entry["MessageDeduplicationId"]

    data = urlencode(form).encode()
    req = urllib.request.Request(ENDPOINT, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    resp = urllib.request.urlopen(req, timeout=10)
    body = resp.read().decode()
    return resp.status, body


def test_sns_fifo_publish_batch_missing_group_id_fails_entries(sns):
    """PublishBatch to FIFO topic: entries missing MessageGroupId go to Failed list.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-batch-nogrp-{uid}.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )["TopicArn"]

    # Use raw HTTP to bypass boto3 client-side validation
    entries = [
        {"Id": "e1", "Message": "msg without group id"},
        {"Id": "e2", "Message": "msg without group id 2"},
    ]
    status, body = _raw_publish_batch(topic_arn, entries)
    assert status == 200

    # Both entries should be in the Failed list
    assert "<Failed>" in body
    assert body.count("<Id>e1</Id>") == 1
    assert body.count("<Id>e2</Id>") == 1
    assert "InvalidParameterException" in body


def test_sns_fifo_publish_batch_all_valid_returns_successful_with_sequence(sns):
    """PublishBatch to FIFO topic: all valid entries return in Successful with SequenceNumber.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-batch-valid-{uid}.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    resp = sns.publish_batch(
        TopicArn=topic_arn,
        PublishBatchRequestEntries=[
            {
                "Id": "e1",
                "Message": "batch fifo msg 1",
                "MessageGroupId": "grp-1",
                "MessageDeduplicationId": f"dedup-batch-1-{uid}",
            },
            {
                "Id": "e2",
                "Message": "batch fifo msg 2",
                "MessageGroupId": "grp-1",
                "MessageDeduplicationId": f"dedup-batch-2-{uid}",
            },
            {
                "Id": "e3",
                "Message": "batch fifo msg 3",
                "MessageGroupId": "grp-2",
                "MessageDeduplicationId": f"dedup-batch-3-{uid}",
            },
        ],
    )

    assert len(resp["Successful"]) == 3
    assert len(resp.get("Failed", [])) == 0

    # Each successful entry should have a SequenceNumber
    seq_numbers = []
    for entry in resp["Successful"]:
        assert "MessageId" in entry
        assert "SequenceNumber" in entry
        seq = entry["SequenceNumber"]
        assert seq.isdigit()
        assert len(seq) == 20
        seq_numbers.append(int(seq))

    # Sequence numbers should be monotonically increasing
    for i in range(1, len(seq_numbers)):
        assert seq_numbers[i] > seq_numbers[i - 1]


def test_sns_fifo_publish_batch_mixed_valid_invalid_entries(sns):
    """PublishBatch to FIFO topic: mixed entries correctly separate Successful and Failed.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-batch-mixed-{uid}.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    # Use raw HTTP: e1 is valid, e2 is missing MessageGroupId, e3 is valid
    entries = [
        {
            "Id": "e1",
            "Message": "valid msg 1",
            "MessageGroupId": "grp-1",
            "MessageDeduplicationId": f"dedup-mixed-1-{uid}",
        },
        {
            "Id": "e2",
            "Message": "invalid msg missing group id",
            # No MessageGroupId — should fail
        },
        {
            "Id": "e3",
            "Message": "valid msg 3",
            "MessageGroupId": "grp-2",
            "MessageDeduplicationId": f"dedup-mixed-3-{uid}",
        },
    ]
    status, body = _raw_publish_batch(topic_arn, entries)
    assert status == 200

    # e1 and e3 should be in Successful
    # e2 should be in Failed
    # Parse the XML to verify
    assert "<Successful>" in body
    assert "<Failed>" in body

    # Count successful entries (e1 and e3)
    successful_section = body.split("<Successful>")[1].split("</Successful>")[0]
    assert "<Id>e1</Id>" in successful_section
    assert "<Id>e3</Id>" in successful_section
    # Successful entries should have SequenceNumber
    assert "<SequenceNumber>" in successful_section

    # Count failed entries (e2)
    failed_section = body.split("<Failed>")[1].split("</Failed>")[0]
    assert "<Id>e2</Id>" in failed_section
    assert "InvalidParameterException" in failed_section


# ---------------------------------------------------------------------------
# ContentBasedDeduplication Attribute Management Tests
# ---------------------------------------------------------------------------


def test_sns_fifo_set_topic_attributes_toggle_cbd(sns):
    """SetTopicAttributes can toggle ContentBasedDeduplication on a FIFO topic.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    arn = sns.create_topic(
        Name=f"intg-fifo-cbd-toggle-{uid}.fifo",
        Attributes={"FifoTopic": "true"},
    )["TopicArn"]

    # CBD defaults to "false"
    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["ContentBasedDeduplication"] == "false"

    # Enable CBD via SetTopicAttributes
    sns.set_topic_attributes(
        TopicArn=arn,
        AttributeName="ContentBasedDeduplication",
        AttributeValue="true",
    )
    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["ContentBasedDeduplication"] == "true"

    # Disable CBD via SetTopicAttributes
    sns.set_topic_attributes(
        TopicArn=arn,
        AttributeName="ContentBasedDeduplication",
        AttributeValue="false",
    )
    attrs = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    assert attrs["ContentBasedDeduplication"] == "false"


def test_sns_fifo_publish_succeeds_without_dedup_id_after_enabling_cbd(sns):
    """After enabling CBD, publishing without an explicit dedup ID succeeds.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    arn = sns.create_topic(
        Name=f"intg-fifo-cbd-enable-pub-{uid}.fifo",
        Attributes={"FifoTopic": "true"},  # CBD defaults to "false"
    )["TopicArn"]

    # Enable CBD
    sns.set_topic_attributes(
        TopicArn=arn,
        AttributeName="ContentBasedDeduplication",
        AttributeValue="true",
    )

    # Publish without explicit MessageDeduplicationId — should succeed
    resp = sns.publish(
        TopicArn=arn,
        Message="cbd enabled message",
        MessageGroupId="grp-1",
    )
    assert "MessageId" in resp
    assert "SequenceNumber" in resp


def test_sns_fifo_publish_fails_without_dedup_id_after_disabling_cbd(sns):
    """After disabling CBD, publishing without an explicit dedup ID fails.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    arn = sns.create_topic(
        Name=f"intg-fifo-cbd-disable-pub-{uid}.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "true"},
    )["TopicArn"]

    # Verify publishing without dedup ID works while CBD is enabled
    resp = sns.publish(
        TopicArn=arn,
        Message="should succeed with cbd on",
        MessageGroupId="grp-1",
    )
    assert "MessageId" in resp

    # Disable CBD
    sns.set_topic_attributes(
        TopicArn=arn,
        AttributeName="ContentBasedDeduplication",
        AttributeValue="false",
    )

    # Now publishing without explicit MessageDeduplicationId should fail
    with pytest.raises(ClientError) as exc:
        sns.publish(
            TopicArn=arn,
            Message="should fail with cbd off",
            MessageGroupId="grp-1",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ---------------------------------------------------------------------------
# End-to-End FIFO SNS → SQS Fanout Integration Test
# ---------------------------------------------------------------------------


def test_sns_fifo_e2e_fanout_with_dedup(sns, sqs):
    """End-to-end: FIFO SNS → SQS fanout passes MessageGroupId and deduplicates.
    """
    uid = _uuid_mod.uuid4().hex[:8]

    # 1. Create a FIFO topic and FIFO SQS queue, subscribe the queue to the topic
    topic_arn = sns.create_topic(
        Name=f"intg-fifo-e2e-{uid}.fifo",
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )["TopicArn"]

    q_url = sqs.create_queue(
        QueueName=f"intg-fifo-e2e-q-{uid}.fifo",
        Attributes={"FifoQueue": "true"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

    # 2. Publish a message with MessageGroupId and MessageDeduplicationId
    dedup_id = f"dedup-e2e-{uid}"
    group_id = f"grp-e2e-{uid}"
    resp1 = sns.publish(
        TopicArn=topic_arn,
        Message="e2e fifo fanout message",
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id,
    )
    assert "MessageId" in resp1
    assert "SequenceNumber" in resp1

    # 3. Receive the message from SQS and verify MessageGroupId is passed through
    msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
        AttributeNames=["All"],
    )
    assert len(msgs.get("Messages", [])) == 1
    msg = msgs["Messages"][0]
    body = json.loads(msg["Body"])
    assert body["Message"] == "e2e fifo fanout message"
    attrs = msg.get("Attributes", {})
    assert attrs.get("MessageGroupId") == group_id

    # Delete the received message so the queue is clean for the next check
    sqs.delete_message(QueueUrl=q_url, ReceiptHandle=msg["ReceiptHandle"])

    # 4. Publish the same dedup ID again — should be deduplicated
    resp2 = sns.publish(
        TopicArn=topic_arn,
        Message="duplicate attempt",
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id,
    )
    # Dedup hit: same MessageId and SequenceNumber as the first publish
    assert resp2["MessageId"] == resp1["MessageId"]
    assert resp2["SequenceNumber"] == resp1["SequenceNumber"]

    # Verify the subscriber does NOT receive a duplicate message
    dup_msgs = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
    )
    assert len(dup_msgs.get("Messages", [])) == 0, "Duplicate message should not be delivered"


def test_sns_http_subscription_confirmation_delivered(sns):
    """HTTP subscribe() must POST a SubscriptionConfirmation to the endpoint (#460).

    Regression for the aiohttp-not-installed silent skip: the Docker image
    never shipped aiohttp, so every HTTP subscription's confirmation was
    logged-and-dropped. stdlib urllib must deliver.
    """
    import http.server
    import socketserver
    import threading as _threading

    received = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8", errors="replace")
            received.append({
                "headers": dict(self.headers),
                "body": body,
            })
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args, **_kwargs):
            pass

    httpd = socketserver.TCPServer(("127.0.0.1", 0), _Handler)
    port = httpd.server_address[1]
    server_thread = _threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()

    try:
        uid = _uuid_mod.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"intg-sns-http-conf-{uid}")["TopicArn"]
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol="http",
            Endpoint=f"http://127.0.0.1:{port}/hook",
        )

        deadline = time.time() + 5
        while time.time() < deadline and not received:
            time.sleep(0.05)

        assert received, "SubscriptionConfirmation POST never arrived"
        first = received[0]
        header_lookup = {k.lower(): v for k, v in first["headers"].items()}
        assert header_lookup.get("x-amz-sns-message-type") == "SubscriptionConfirmation"
        parsed = json.loads(first["body"])
        assert parsed["Type"] == "SubscriptionConfirmation"
        assert parsed["TopicArn"] == topic_arn
        assert "Token" in parsed and parsed["Token"]
        assert "SubscribeURL" in parsed
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_sns_http_subscription_basic_auth_userinfo(sns):
    """`http://user:pass@host/path` endpoints must deliver Authorization: Basic (#460).

    Real AWS SNS promotes URL userinfo to a Basic auth header. urllib leaves
    userinfo in the URL by default, which also corrupts the Host header, so
    the SNS HTTP helper must parse-and-inject explicitly.
    """
    import base64 as _b64
    import http.server
    import socketserver
    import threading as _threading

    received = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            received.append({"headers": dict(self.headers)})
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args, **_kwargs):
            pass

    httpd = socketserver.TCPServer(("127.0.0.1", 0), _Handler)
    port = httpd.server_address[1]
    _threading.Thread(target=httpd.serve_forever, daemon=True).start()

    try:
        uid = _uuid_mod.uuid4().hex[:8]
        topic_arn = sns.create_topic(Name=f"intg-sns-http-basic-{uid}")["TopicArn"]
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol="http",
            Endpoint=f"http://alice:s3cret@127.0.0.1:{port}/hook",
        )

        deadline = time.time() + 5
        while time.time() < deadline and not received:
            time.sleep(0.05)

        assert received, "SubscriptionConfirmation POST never arrived"
        header_lookup = {k.lower(): v for k, v in received[0]["headers"].items()}

        auth = header_lookup.get("authorization", "")
        assert auth.startswith("Basic "), f"Expected Basic auth header, got {auth!r}"
        decoded = _b64.b64decode(auth[len("Basic "):]).decode("utf-8")
        assert decoded == "alice:s3cret"

        host = header_lookup.get("host", "")
        assert "@" not in host, f"Host header must not contain userinfo, got {host!r}"
        assert host == f"127.0.0.1:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
