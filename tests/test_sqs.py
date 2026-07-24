import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _regional_sqs(region_name):
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    return boto3.client(
        "sqs",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region_name,
        config=Config(region_name=region_name, retries={"mode": "standard"}),
    )


def test_sqs_create_queue(sqs):
    resp = sqs.create_queue(QueueName="intg-sqs-create")
    assert "QueueUrl" in resp
    assert "intg-sqs-create" in resp["QueueUrl"]

def test_sqs_delete_queue(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-delete")["QueueUrl"]
    sqs.delete_queue(QueueUrl=url)
    with pytest.raises(ClientError):
        sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])

def test_sqs_delete_queue_nonexistent_raises(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-delete-404")["QueueUrl"]
    sqs.delete_queue(QueueUrl=url)
    with pytest.raises(ClientError) as exc:
        sqs.delete_queue(QueueUrl=url)          # second delete — queue is already gone
    assert exc.value.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

def test_sqs_list_queues(sqs):
    sqs.create_queue(QueueName="intg-sqs-list-alpha")
    sqs.create_queue(QueueName="intg-sqs-list-beta")
    resp = sqs.list_queues(QueueNamePrefix="intg-sqs-list-")
    urls = resp.get("QueueUrls", [])
    assert len(urls) >= 2
    assert any("intg-sqs-list-alpha" in u for u in urls)
    assert any("intg-sqs-list-beta" in u for u in urls)

def test_sqs_list_queues_paginates_with_max_results(sqs):
    prefix = f"intg-sqs-page-{_uuid_mod.uuid4().hex[:8]}-"
    for i in range(5):
        sqs.create_queue(QueueName=f"{prefix}{i}")

    first = sqs.list_queues(QueueNamePrefix=prefix, MaxResults=2)
    assert len(first["QueueUrls"]) == 2
    assert "NextToken" in first

    collected = list(first["QueueUrls"])
    token = first["NextToken"]
    while token:
        page = sqs.list_queues(QueueNamePrefix=prefix, MaxResults=2, NextToken=token)
        collected.extend(page["QueueUrls"])
        token = page.get("NextToken")

    assert len(collected) == 5
    assert len(set(collected)) == 5


def test_sqs_list_queues_no_next_token_without_max_results(sqs):
    prefix = f"intg-sqs-nopage-{_uuid_mod.uuid4().hex[:8]}-"
    sqs.create_queue(QueueName=f"{prefix}only")
    resp = sqs.list_queues(QueueNamePrefix=prefix)
    assert len(resp["QueueUrls"]) == 1
    assert "NextToken" not in resp


def test_sqs_list_queues_no_next_token_on_exact_fit(sqs):
    prefix = f"intg-sqs-exact-{_uuid_mod.uuid4().hex[:8]}-"
    for i in range(3):
        sqs.create_queue(QueueName=f"{prefix}{i}")
    resp = sqs.list_queues(QueueNamePrefix=prefix, MaxResults=3)
    assert len(resp["QueueUrls"]) == 3
    assert "NextToken" not in resp


def test_sqs_get_queue_url(sqs):
    sqs.create_queue(QueueName="intg-sqs-geturl")
    resp = sqs.get_queue_url(QueueName="intg-sqs-geturl")
    assert "intg-sqs-geturl" in resp["QueueUrl"]


def test_sqs_queues_are_region_scoped_by_name(sqs):
    name = f"mr-sqs-same-name-{_uuid_mod.uuid4().hex[:8]}"
    west = _regional_sqs("us-west-2")

    east_url = sqs.create_queue(QueueName=name)["QueueUrl"]
    west_url = west.create_queue(QueueName=name)["QueueUrl"]

    east_arn = sqs.get_queue_attributes(
        QueueUrl=east_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    west_arn = west.get_queue_attributes(
        QueueUrl=west_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    assert east_arn == f"arn:aws:sqs:us-east-1:000000000000:{name}"
    assert west_arn == f"arn:aws:sqs:us-west-2:000000000000:{name}"

    sqs.send_message(QueueUrl=east_url, MessageBody="east")
    west.send_message(QueueUrl=west_url, MessageBody="west")

    east_msgs = sqs.receive_message(
        QueueUrl=east_url, MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    west_msgs = west.receive_message(
        QueueUrl=west_url, MaxNumberOfMessages=1, WaitTimeSeconds=0
    )
    assert [m["Body"] for m in east_msgs["Messages"]] == ["east"]
    assert [m["Body"] for m in west_msgs["Messages"]] == ["west"]

    west.delete_queue(QueueUrl=west_url)
    with pytest.raises(ClientError):
        west.get_queue_attributes(QueueUrl=west_url, AttributeNames=["QueueArn"])
    assert sqs.get_queue_attributes(
        QueueUrl=east_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"] == east_arn


def test_sqs_queue_url_reflects_env_host(sqs):
    """QueueUrl host must come from MINISTACK_HOST env var, not hardcoded localhost."""
    import os

    expected_host = os.environ.get("MINISTACK_HOST", "localhost")
    resp = sqs.create_queue(QueueName="intg-sqs-urlhost")
    url = resp["QueueUrl"]
    assert expected_host in url
    assert "intg-sqs-urlhost" in url

def test_sqs_send_receive_delete(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-srd")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="test-body")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert len(msgs["Messages"]) == 1
    assert msgs["Messages"][0]["Body"] == "test-body"
    sqs.delete_message(
        QueueUrl=url,
        ReceiptHandle=msgs["Messages"][0]["ReceiptHandle"],
    )
    empty = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    assert len(empty.get("Messages", [])) == 0

def test_sqs_message_attributes(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-attrs")["QueueUrl"]
    sqs.send_message(
        QueueUrl=url,
        MessageBody="with-attrs",
        MessageAttributes={
            "color": {"DataType": "String", "StringValue": "blue"},
            "count": {"DataType": "Number", "StringValue": "42"},
        },
    )
    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
    )
    attrs = msgs["Messages"][0]["MessageAttributes"]
    assert attrs["color"]["StringValue"] == "blue"
    assert attrs["count"]["StringValue"] == "42"

def test_sqs_batch_send(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-batchsend")["QueueUrl"]
    resp = sqs.send_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "m1", "MessageBody": "batch-1"},
            {"Id": "m2", "MessageBody": "batch-2"},
            {"Id": "m3", "MessageBody": "batch-3"},
        ],
    )
    assert len(resp["Successful"]) == 3
    assert len(resp.get("Failed", [])) == 0

def test_sqs_batch_delete(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-batchdel")["QueueUrl"]
    for i in range(3):
        sqs.send_message(QueueUrl=url, MessageBody=f"del-{i}")

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs["Messages"])]
    resp = sqs.delete_message_batch(QueueUrl=url, Entries=entries)
    assert len(resp["Successful"]) == len(entries)

def test_sqs_purge_queue(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-purge")["QueueUrl"]
    for i in range(5):
        sqs.send_message(QueueUrl=url, MessageBody=f"purge-{i}")
    sqs.purge_queue(QueueUrl=url)
    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=0,
    )
    assert len(msgs.get("Messages", [])) == 0

def test_sqs_visibility_timeout(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-vis")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="vis-test")

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    rh = msgs["Messages"][0]["ReceiptHandle"]
    sqs.change_message_visibility(
        QueueUrl=url,
        ReceiptHandle=rh,
        VisibilityTimeout=0,
    )
    msgs2 = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert len(msgs2["Messages"]) == 1
    assert msgs2["Messages"][0]["Body"] == "vis-test"

def test_sqs_change_visibility_batch(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-visbatch")["QueueUrl"]
    for i in range(2):
        sqs.send_message(QueueUrl=url, MessageBody=f"vb-{i}")

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    entries = [
        {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
        for i, m in enumerate(msgs["Messages"])
    ]
    resp = sqs.change_message_visibility_batch(QueueUrl=url, Entries=entries)
    assert len(resp["Successful"]) == len(entries)

    msgs2 = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    assert len(msgs2["Messages"]) == 2

def test_sqs_queue_attributes(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-qattr")["QueueUrl"]
    sqs.set_queue_attributes(
        QueueUrl=url,
        Attributes={"VisibilityTimeout": "60"},
    )
    resp = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["VisibilityTimeout"],
    )
    assert resp["Attributes"]["VisibilityTimeout"] == "60"

def test_sqs_queue_tags(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-tags")["QueueUrl"]
    sqs.tag_queue(QueueUrl=url, Tags={"env": "test", "team": "backend"})
    resp = sqs.list_queue_tags(QueueUrl=url)
    assert resp["Tags"]["env"] == "test"
    assert resp["Tags"]["team"] == "backend"

    sqs.untag_queue(QueueUrl=url, TagKeys=["team"])
    resp = sqs.list_queue_tags(QueueUrl=url)
    assert "team" not in resp.get("Tags", {})
    assert resp["Tags"]["env"] == "test"

def test_sqs_fifo_queue(sqs):
    url = sqs.create_queue(
        QueueName="intg-sqs-fifo.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )["QueueUrl"]

    for i in range(3):
        sqs.send_message(
            QueueUrl=url,
            MessageBody=f"fifo-msg-{i}",
            MessageGroupId="group-1",
        )

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    assert len(msgs["Messages"]) >= 1
    assert msgs["Messages"][0]["Body"] == "fifo-msg-0"

def test_sqs_fifo_deduplication(sqs):
    url = sqs.create_queue(
        QueueName="intg-sqs-dedup.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        },
    )["QueueUrl"]

    r1 = sqs.send_message(
        QueueUrl=url,
        MessageBody="dedup-body",
        MessageGroupId="g1",
        MessageDeduplicationId="dedup-001",
    )
    r2 = sqs.send_message(
        QueueUrl=url,
        MessageBody="dedup-body",
        MessageGroupId="g1",
        MessageDeduplicationId="dedup-001",
    )
    assert r1["MessageId"] == r2["MessageId"]

def test_sqs_fifo_dedup_scope_message_group(sqs):
    """DeduplicationScope=messageGroup: same body in different groups must both enqueue."""
    url = sqs.create_queue(
        QueueName="intg-sqs-dedup-scope-mg.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
            "DeduplicationScope": "messageGroup",
            "FifoThroughputLimit": "perMessageGroupId",
        },
    )["QueueUrl"]

    r1 = sqs.send_message(
        QueueUrl=url,
        MessageBody="same-body",
        MessageGroupId="G1",
    )
    r2 = sqs.send_message(
        QueueUrl=url,
        MessageBody="same-body",
        MessageGroupId="G2",
    )
    # Different groups → different MessageIds
    assert r1["MessageId"] != r2["MessageId"]

    # Duplicate within the same group → same MessageId
    r3 = sqs.send_message(
        QueueUrl=url,
        MessageBody="same-body",
        MessageGroupId="G1",
    )
    assert r1["MessageId"] == r3["MessageId"]

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    assert len(msgs.get("Messages", [])) == 2

def test_sqs_dlq(sqs):
    dlq_url = sqs.create_queue(QueueName="intg-sqs-dlq-target")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    src_url = sqs.create_queue(
        QueueName="intg-sqs-dlq-source",
        Attributes={
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": "2",
                }
            ),
        },
    )["QueueUrl"]

    sqs.send_message(QueueUrl=src_url, MessageBody="dlq-test")

    for _ in range(2):
        msgs = sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=1)
        assert len(msgs["Messages"]) == 1
        rh = msgs["Messages"][0]["ReceiptHandle"]
        sqs.change_message_visibility(
            QueueUrl=src_url,
            ReceiptHandle=rh,
            VisibilityTimeout=0,
        )

    time.sleep(0.1)
    empty = sqs.receive_message(
        QueueUrl=src_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    assert len(empty.get("Messages", [])) == 0

    dlq_msgs = sqs.receive_message(
        QueueUrl=dlq_url,
        MaxNumberOfMessages=1,
    )
    assert len(dlq_msgs["Messages"]) == 1
    assert dlq_msgs["Messages"][0]["Body"] == "dlq-test"

def test_sqs_delay_seconds(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-delay")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="delayed", DelaySeconds=2)

    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    assert len(msgs.get("Messages", [])) == 0

    time.sleep(2.5)
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert len(msgs["Messages"]) == 1
    assert msgs["Messages"][0]["Body"] == "delayed"

def test_sqs_message_system_attributes(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-sysattr")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="sysattr-test")

    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        AttributeNames=["ApproximateReceiveCount"],
    )
    assert msgs["Messages"][0]["Attributes"]["ApproximateReceiveCount"] == "1"

    rh = msgs["Messages"][0]["ReceiptHandle"]
    sqs.change_message_visibility(
        QueueUrl=url,
        ReceiptHandle=rh,
        VisibilityTimeout=0,
    )
    msgs2 = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        AttributeNames=["ApproximateReceiveCount"],
    )
    assert msgs2["Messages"][0]["Attributes"]["ApproximateReceiveCount"] == "2"

def test_sqs_message_system_attribute_names_modern_field(sqs):
    """Regression: AWS SDK v2 / Java sends MessageSystemAttributeNames, not the
    deprecated AttributeNames. Ministack must honor the modern field name."""
    url = sqs.create_queue(QueueName="intg-sqs-msa-modern")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="msa-modern")

    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=["All"],
    )
    attrs = msgs["Messages"][0].get("Attributes", {})
    assert attrs.get("ApproximateReceiveCount") == "1"
    assert "SentTimestamp" in attrs


def test_sqs_tag_queue_rejects_null_tag_value(sqs):
    """TagQueue rejects JSON null tag VALUES with InvalidParameterValue (400).

    boto3's Python client enforces the `string -> string` map shape locally so
    we can't trigger this through it directly. Java SDK v2, Go SDK, and raw
    HTTP callers can send `{"Tags": {"key": null}}`; previously the null was
    stored as Python None then serialised back as the literal string "null".
    Real AWS rejects at intake.
    """
    import json as _json
    import urllib.request
    url = sqs.create_queue(QueueName="intg-sqs-tag-null")["QueueUrl"]

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        endpoint + "/",
        data=_json.dumps({
            "QueueUrl": url,
            "Tags": {"valid": "ok", "broken": None},
        }).encode(),
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "AmazonSQS.TagQueue",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5).read()
        pytest.fail("expected 400 InvalidParameterValue for null tag value")
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = e.read().decode()
        assert "InvalidParameterValue" in body, body[:300]
        assert "null" in body.lower(), body[:300]

    # And — the valid tag in the same call MUST NOT have been partially
    # applied. AWS is all-or-nothing on this validation.
    tags = sqs.list_queue_tags(QueueUrl=url).get("Tags", {})
    assert "valid" not in tags
    assert "broken" not in tags


def test_sqs_send_message_batch_rejects_oversized_aggregate(sqs):
    """SendMessageBatch's aggregate payload cap is 1 MiB. The contributor PR
    enforced per-message size against the queue's MaximumMessageSize but never
    checked the batch sum, so 10 × 150 KiB messages (queue allows them
    individually) snuck through. Real AWS returns BatchRequestTooLong (400)
    for the whole batch when the sum is over.
    See: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
    """
    url = sqs.create_queue(
        QueueName="intg-sqs-batch-too-long",
        Attributes={"MaximumMessageSize": "262144"},  # default
    )["QueueUrl"]

    # 10 × 150 KiB → 1.5 MiB total. Each individual entry is within the queue's
    # 256-KiB MaximumMessageSize, but the batch aggregate exceeds the 1-MiB cap.
    body = "x" * (150 * 1024)
    entries = [{"Id": f"m{i}", "MessageBody": body} for i in range(10)]

    with pytest.raises(ClientError) as exc:
        sqs.send_message_batch(QueueUrl=url, Entries=entries)
    code = exc.value.response["Error"]["Code"]
    assert code in (
        "AWS.SimpleQueueService.BatchRequestTooLong",
        "BatchRequestTooLong",
    ), f"unexpected error code: {code}"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    # Under-cap batch should succeed (5 × 150 KiB = 750 KiB).
    ok = sqs.send_message_batch(QueueUrl=url, Entries=entries[:5])
    assert len(ok["Successful"]) == 5
    assert not ok.get("Failed")


def test_sqs_send_message_preserves_awstraceheader(sqs):
    """SendMessage's MessageSystemAttributes.AWSTraceHeader carries an X-Ray
    trace context that AWS preserves through the queue and returns to the
    receiver via ReceiveMessage's MessageSystemAttributeNames=['AWSTraceHeader']
    (or 'All'). Previously this was captured then dropped silently.
    See: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    """
    url = sqs.create_queue(QueueName="intg-sqs-trace-hdr")["QueueUrl"]
    trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    sqs.send_message(
        QueueUrl=url,
        MessageBody="traced",
        MessageSystemAttributes={
            "AWSTraceHeader": {"DataType": "String", "StringValue": trace},
        },
    )

    # Explicit name
    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=["AWSTraceHeader"],
    )
    assert msgs["Messages"][0]["Attributes"]["AWSTraceHeader"] == trace

    # Re-receive after the visibility window via "All"
    msg = msgs["Messages"][0]
    sqs.change_message_visibility(
        QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=0,
    )
    msgs2 = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=["All"],
    )
    assert msgs2["Messages"][0]["Attributes"]["AWSTraceHeader"] == trace

def test_sqs_nonexistent_queue(sqs):
    with pytest.raises(ClientError) as exc:
        sqs.get_queue_url(QueueName="intg-sqs-does-not-exist")
    assert exc.value.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"

def test_sqs_receive_empty(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-empty")["QueueUrl"]
    msgs = sqs.receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    assert len(msgs.get("Messages", [])) == 0

def test_sqs_batch_delete_invalid_receipt_handle(sqs):
    """DeleteMessageBatch with an invalid ReceiptHandle must populate the Failed list."""
    url = sqs.create_queue(QueueName="intg-sqs-batchdel-invalid")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="msg")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    valid_rh = msgs["Messages"][0]["ReceiptHandle"]

    resp = sqs.delete_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "good", "ReceiptHandle": valid_rh},
            {"Id": "bad", "ReceiptHandle": "INVALID-HANDLE-XYZ"},
        ],
    )
    successful_ids = [e["Id"] for e in resp["Successful"]]
    failed_ids = [e["Id"] for e in resp["Failed"]]
    assert "good" in successful_ids
    assert "bad" in failed_ids
    assert resp["Failed"][0]["Code"] == "ReceiptHandleIsInvalid"

def test_sqs_delete_message_invalid_receipt_handle(sqs):
    """DeleteMessage with an invalid ReceiptHandle must raise ReceiptHandleIsInvalid."""
    url = sqs.create_queue(QueueName="intg-sqs-del-invalid")["QueueUrl"]
    with pytest.raises(ClientError) as exc_info:
        sqs.delete_message(QueueUrl=url, ReceiptHandle="INVALID-HANDLE-XYZ")
    assert exc_info.value.response["Error"]["Code"] == "ReceiptHandleIsInvalid"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_sqs_change_message_visibility_invalid_receipt_handle(sqs):
    """ChangeMessageVisibility with an invalid ReceiptHandle must raise ReceiptHandleIsInvalid."""
    url = sqs.create_queue(QueueName="intg-sqs-vis-invalid")["QueueUrl"]
    with pytest.raises(ClientError) as exc_info:
        sqs.change_message_visibility(QueueUrl=url, ReceiptHandle="INVALID-HANDLE-XYZ", VisibilityTimeout=60)
    assert exc_info.value.response["Error"]["Code"] == "ReceiptHandleIsInvalid"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_sqs_receive_max_10(sqs):
    """ReceiveMessage with MaxNumberOfMessages > 10 is capped at 10."""
    url = sqs.create_queue(QueueName="qa-sqs-max10")["QueueUrl"]
    for i in range(15):
        sqs.send_message(QueueUrl=url, MessageBody=f"msg{i}")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=15)
    assert len(msgs.get("Messages", [])) <= 10

def test_sqs_visibility_timeout_zero_makes_visible(sqs):
    """ChangeMessageVisibility to 0 makes message immediately visible again."""
    url = sqs.create_queue(QueueName="qa-sqs-vis0")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="vis-test")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, VisibilityTimeout=30)
    rh = msgs["Messages"][0]["ReceiptHandle"]
    sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=rh, VisibilityTimeout=0)
    msgs2 = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert len(msgs2.get("Messages", [])) == 1

def test_sqs_batch_delete_invalid_receipt_handle_in_failed(sqs):
    """DeleteMessageBatch with invalid receipt handle puts entry in Failed."""
    url = sqs.create_queue(QueueName="qa-sqs-batchdel-fail")["QueueUrl"]
    resp = sqs.delete_message_batch(
        QueueUrl=url,
        Entries=[{"Id": "bad1", "ReceiptHandle": "totally-invalid-handle"}],
    )
    assert len(resp.get("Failed", [])) == 1
    assert resp["Failed"][0]["Id"] == "bad1"
    assert len(resp.get("Successful", [])) == 0

def test_sqs_fifo_group_ordering(sqs):
    """FIFO queue delivers messages in send order within a group."""
    url = sqs.create_queue(
        QueueName="qa-sqs-fifo-order.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    for i in range(3):
        sqs.send_message(QueueUrl=url, MessageBody=f"msg{i}", MessageGroupId="g1")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert msgs["Messages"][0]["Body"] == "msg0"

def test_sqs_approximate_message_count(sqs):
    """ApproximateNumberOfMessages reflects messages in queue."""
    url = sqs.create_queue(QueueName="qa-sqs-count")["QueueUrl"]
    for i in range(5):
        sqs.send_message(QueueUrl=url, MessageBody=f"m{i}")
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])
    count = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
    assert count == 5

def test_sqs_purge_empties_queue(sqs):
    """PurgeQueue removes all messages."""
    url = sqs.create_queue(QueueName="qa-sqs-purge2")["QueueUrl"]
    for i in range(5):
        sqs.send_message(QueueUrl=url, MessageBody=f"m{i}")
    sqs.purge_queue(QueueUrl=url)
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
    assert len(msgs.get("Messages", [])) == 0

def test_sqs_send_message_batch_limit(sqs):
    import pytest
    from botocore.exceptions import ClientError

    q = sqs.create_queue(QueueName="batch-limit-regression")["QueueUrl"]
    entries = [{"Id": str(i), "MessageBody": f"msg {i}"} for i in range(11)]
    with pytest.raises(ClientError) as exc_info:
        sqs.send_message_batch(QueueUrl=q, Entries=entries)
    assert exc_info.value.response["Error"]["Code"] == "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"
    sqs.delete_queue(QueueUrl=q)

def test_sqs_typed_exception_queue_not_found(sqs):
    """client.exceptions.QueueDoesNotExist must be raised (not generic ClientError)
    when accessing a non-existent queue — requires <Type> in the XML error response."""
    import pytest

    with pytest.raises(sqs.exceptions.QueueDoesNotExist):
        sqs.get_queue_url(QueueName="queue-that-does-not-exist-typed-exc")

def test_sqs_query_compat_header_nonexistent_queue(sqs):
    """Error.Code must be the legacy 'AWS.SimpleQueueService.NonExistentQueue'
    (not 'QueueDoesNotExist') when x-amzn-query-error header is present."""
    with pytest.raises(ClientError) as exc:
        sqs.get_queue_url(QueueName="queue-compat-header-test-xyz")
    code = exc.value.response["Error"]["Code"]
    assert code == "AWS.SimpleQueueService.NonExistentQueue", f"Expected legacy query-compat code, got '{code}'"

def test_sqs_query_compat_header_batch_limit(sqs):
    """TooManyEntriesInBatchRequest must surface as the legacy namespaced code."""
    q = sqs.create_queue(QueueName="compat-batch-limit-q")["QueueUrl"]
    entries = [{"Id": str(i), "MessageBody": f"m{i}"} for i in range(11)]
    with pytest.raises(ClientError) as exc:
        sqs.send_message_batch(QueueUrl=q, Entries=entries)
    code = exc.value.response["Error"]["Code"]
    assert code == "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", (
        f"Expected legacy query-compat code, got '{code}'"
    )
    sqs.delete_queue(QueueUrl=q)

def test_sqs_event_source_mapping_to_lambda(lam, sqs):
    """SQS messages trigger Lambda invocation via event source mapping."""
    queue_name = "intg-sqsesm-q"
    fn_name = "intg-sqsesm-fn"

    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'received': len(event.get('Records', []))}\n"
    )
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    esm = lam.create_event_source_mapping(
        FunctionName=fn_name,
        EventSourceArn=queue_arn,
        BatchSize=5,
    )
    assert esm["EventSourceArn"] == queue_arn
    assert esm["FunctionArn"].endswith(fn_name)

    # Send messages to SQS
    for i in range(3):
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"idx": i}))

    # Allow the ESM poller to pick up and process
    time.sleep(3)

    # Messages should have been consumed by the ESM (queue should be empty or near-empty)
    # Retry with backoff to account for variable Lambda invocation latency
    max_retries = 5
    retry_delay = 2
    for attempt in range(max_retries):
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        remaining = len(msgs.get("Messages", []))
        if remaining == 0:
            break
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    assert remaining == 0, f"ESM should have consumed all messages, but {remaining} remain after {max_retries} retries"

    # Cleanup
    lam.delete_event_source_mapping(UUID=esm["UUID"])


def test_sqs_bare_queue_name_as_url(sqs):
    """Passing a bare queue name instead of a full URL should work (AWS compatibility)."""
    queue_name = "intg-sqs-bare-name"
    sqs.create_queue(QueueName=queue_name)

    # Send using full URL (normal)
    url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="via-url")

    # Send using bare queue name instead of full URL
    sqs.send_message(QueueUrl=queue_name, MessageBody="via-name")

    # Both messages should be receivable
    msgs = []
    for _ in range(2):
        resp = sqs.receive_message(QueueUrl=queue_name, MaxNumberOfMessages=10)
        msgs.extend(resp.get("Messages", []))
    assert len(msgs) == 2
    bodies = sorted(m["Body"] for m in msgs)
    assert bodies == ["via-name", "via-url"]


def test_sqs_localstack_queue_path_alias(sqs):
    queue_name = "intg-sqs-localstack-alias"
    sqs.create_queue(QueueName=queue_name)
    alias_url = f"http://localhost:4566/queue/{queue_name}"

    sqs.send_message(QueueUrl=alias_url, MessageBody="via-alias")

    resp = sqs.receive_message(QueueUrl=queue_name, MaxNumberOfMessages=1)
    assert resp["Messages"][0]["Body"] == "via-alias"


# -- AWS-parity gaps from competitor audit ------------------------------
# Three regressions, all surfaced when comparing MS behaviour against the
# AWS SQS API reference: SendMessage size enforcement, AddPermission,
# RemovePermission.


def test_sqs_send_message_rejects_oversized_body(sqs):
    """SendMessage must reject bodies exceeding the queue's MaximumMessageSize
    attribute with InvalidParameterValue (400). MaximumMessageSize defaults to
    262144 (256 KiB) per AWS. Before this fix MS silently accepted oversized
    messages locally while real AWS rejected — masking client bugs."""
    import pytest as _pytest

    q = sqs.create_queue(QueueName="intg-sqs-size-default")["QueueUrl"]
    body = "x" * (262144 + 1)
    with _pytest.raises(ClientError) as exc:
        sqs.send_message(QueueUrl=q, MessageBody=body)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"


def test_sqs_send_message_respects_configured_maximum_message_size(sqs):
    """A queue with a tighter MaximumMessageSize must reject bodies that fit
    in the default 262144 but exceed the configured value."""
    import pytest as _pytest

    q = sqs.create_queue(
        QueueName="intg-sqs-size-tight",
        Attributes={"MaximumMessageSize": "1024"},
    )["QueueUrl"]
    body = "x" * 2048
    with _pytest.raises(ClientError) as exc:
        sqs.send_message(QueueUrl=q, MessageBody=body)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    assert "1024" in exc.value.response["Error"]["Message"]


def test_sqs_add_permission_appends_policy_statement(sqs):
    """AddPermission appends an Allow statement to the queue's Policy
    attribute matching the AWS resource-policy shape."""
    q = sqs.create_queue(QueueName="intg-sqs-addperm-basic")["QueueUrl"]
    sqs.add_permission(
        QueueUrl=q,
        Label="perm-1",
        AWSAccountIds=["123456789012"],
        Actions=["SendMessage", "ReceiveMessage"],
    )
    attrs = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["Policy"])
    policy = json.loads(attrs["Attributes"]["Policy"])
    assert policy["Version"] == "2012-10-17"
    sids = [s["Sid"] for s in policy["Statement"]]
    assert "perm-1" in sids
    stmt = next(s for s in policy["Statement"] if s["Sid"] == "perm-1")
    assert stmt["Effect"] == "Allow"
    # AWS canonical Policy shape: bare 12-digit account ID, lowercase sqs: prefix.
    assert "123456789012" in stmt["Principal"]["AWS"]
    assert "sqs:SendMessage" in stmt["Action"]


def test_sqs_add_permission_rejects_duplicate_label(sqs):
    """Real AWS rejects duplicate Labels with InvalidParameterValue."""
    import pytest as _pytest

    q = sqs.create_queue(QueueName="intg-sqs-addperm-dup")["QueueUrl"]
    sqs.add_permission(
        QueueUrl=q, Label="dup", AWSAccountIds=["111111111111"],
        Actions=["SendMessage"],
    )
    with _pytest.raises(ClientError) as exc:
        sqs.add_permission(
            QueueUrl=q, Label="dup", AWSAccountIds=["222222222222"],
            Actions=["ReceiveMessage"],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"


def test_sqs_remove_permission_drops_matching_statement(sqs):
    """RemovePermission removes only the statement with the matching Label."""
    q = sqs.create_queue(QueueName="intg-sqs-remperm")["QueueUrl"]
    sqs.add_permission(
        QueueUrl=q, Label="keep", AWSAccountIds=["111111111111"],
        Actions=["SendMessage"],
    )
    sqs.add_permission(
        QueueUrl=q, Label="drop", AWSAccountIds=["222222222222"],
        Actions=["ReceiveMessage"],
    )
    sqs.remove_permission(QueueUrl=q, Label="drop")
    attrs = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["Policy"])
    policy = json.loads(attrs["Attributes"]["Policy"])
    sids = [s["Sid"] for s in policy["Statement"]]
    assert "keep" in sids
    assert "drop" not in sids


# ---------------------------------------------------------------------------
# RedrivePolicy validation (issue #644)
# ---------------------------------------------------------------------------

def _make_dlq(sqs, name="rp-dlq"):
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def test_sqs_create_queue_rejects_double_encoded_redrive_policy(sqs):
    """Regression for #644: a double-JSON-encoded RedrivePolicy (jq tostring
    quirk) used to slip through CreateQueue, get stored as `"\\"{...}\\""`,
    and then crash ReceiveMessage with `'str' object has no attribute 'get'`
    when `_dlq_sweep` tried to `.get(...)` on the inner string. CreateQueue
    now rejects this at intake with InvalidAttributeValue, matching AWS.
    """
    _, dlq_arn = _make_dlq(sqs, name="rp-double-dlq")
    # Inner JSON wrapped in an additional pair of quotes — what `jq tostring`
    # produces if the caller doesn't realize it already serializes to JSON.
    bad = '"' + json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}).replace('"', '\\"') + '"'
    with pytest.raises(ClientError) as exc:
        sqs.create_queue(
            QueueName="rp-double-src",
            Attributes={"RedrivePolicy": bad},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidAttributeValue"
    assert "RedrivePolicy" in exc.value.response["Error"]["Message"]


@pytest.mark.parametrize("bad_value,label", [
    ("not json at all",                          "non-json"),
    ("[1, 2, 3]",                                "json-array"),
    ('"just a string"',                          "json-string"),
    ("42",                                       "json-number"),
    ('{"maxReceiveCount":"2"}',                  "missing-arn"),
    ('{"deadLetterTargetArn":"arn:x"}',          "missing-mrc"),
    ('{"deadLetterTargetArn":"arn:x","maxReceiveCount":"NaN"}', "non-numeric-mrc"),
    ('{"deadLetterTargetArn":"arn:x","maxReceiveCount":0}',     "mrc-too-low"),
    ('{"deadLetterTargetArn":"arn:x","maxReceiveCount":1001}',  "mrc-too-high"),
    ('{"deadLetterTargetArn":"","maxReceiveCount":2}',          "empty-arn"),
])
def test_sqs_create_queue_redrive_policy_invalid_shape(sqs, bad_value, label):
    """All malformed RedrivePolicy shapes return InvalidAttributeValue (400),
    not InternalError (500) — covering every branch of the validator.
    """
    with pytest.raises(ClientError) as exc:
        sqs.create_queue(
            QueueName=f"rp-bad-{label}",
            Attributes={"RedrivePolicy": bad_value},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidAttributeValue", f"label={label}"


def test_sqs_create_queue_redrive_policy_accepts_int_max_receive_count(sqs):
    """maxReceiveCount as a JSON integer (not string) is accepted — AWS allows
    both, and the existing test in this file already uses the string form.
    """
    _, dlq_arn = _make_dlq(sqs, name="rp-int-dlq")
    sqs.create_queue(
        QueueName="rp-int-src",
        Attributes={"RedrivePolicy": json.dumps(
            {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5}
        )},
    )


def test_sqs_redrive_policy_rejects_cross_region_dlq(sqs):
    west = _regional_sqs("us-west-2")
    west_dlq_url = west.create_queue(
        QueueName=f"rp-west-dlq-{_uuid_mod.uuid4().hex[:8]}"
    )["QueueUrl"]
    west_dlq_arn = west.get_queue_attributes(
        QueueUrl=west_dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    with pytest.raises(ClientError) as exc:
        sqs.create_queue(
            QueueName=f"rp-east-src-{_uuid_mod.uuid4().hex[:8]}",
            Attributes={"RedrivePolicy": json.dumps({
                "deadLetterTargetArn": west_dlq_arn,
                "maxReceiveCount": "2",
            })},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidAttributeValue"


def test_sqs_set_queue_attributes_validates_redrive_policy(sqs):
    """SetQueueAttributes runs the same validator — otherwise CreateQueue
    rejects the bad value but SetQueueAttributes would let it through and
    crash later receives, defeating the gate.
    """
    _, dlq_arn = _make_dlq(sqs, name="rp-setattr-dlq")
    src_url = sqs.create_queue(QueueName="rp-setattr-src")["QueueUrl"]
    bad = '"' + json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}).replace('"', '\\"') + '"'
    with pytest.raises(ClientError) as exc:
        sqs.set_queue_attributes(QueueUrl=src_url, Attributes={"RedrivePolicy": bad})
    assert exc.value.response["Error"]["Code"] == "InvalidAttributeValue"


def test_sqs_fifo_receive_with_redrive_policy_does_not_500(sqs):
    """The original symptom from #644: ReceiveMessage on a FIFO queue with a
    valid RedrivePolicy attached must not crash with InternalError.
    """
    _, dlq_arn = _make_dlq(sqs, name="rp-fifo-recv-dlq.fifo")  # name irrelevant
    # Actually make a FIFO DLQ for parity with the issue's repro.
    dlq_url = sqs.create_queue(
        QueueName="rp-fifo-dlq.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
    )["QueueUrl"]
    fifo_dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    src_url = sqs.create_queue(
        QueueName="rp-fifo-src.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "VisibilityTimeout": "5",
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": fifo_dlq_arn, "maxReceiveCount": "2"}
            ),
        },
    )["QueueUrl"]

    # Empty-queue receive must not 500.
    r = sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert "Messages" not in r or r["Messages"] == []

    # After a SendMessage, the message must come back via ReceiveMessage.
    sqs.send_message(
        QueueUrl=src_url,
        MessageBody="hello",
        MessageGroupId="g1",
        MessageDeduplicationId="d-1",
    )
    r = sqs.receive_message(QueueUrl=src_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
    assert len(r["Messages"]) == 1
    assert r["Messages"][0]["Body"] == "hello"


def test_sqs_dlq_sweep_survives_legacy_double_encoded_policy():
    """Defence-in-depth unit test: if a legacy queue carried a double-encoded
    RedrivePolicy from a pre-fix MS version (persistence-load can't be
    rejected the way an API call is), `_dlq_sweep` must skip rather than
    crash. Called directly here — boto3 over HTTP would talk to a separately
    process with its own `_queues` instance, so we can't inject the bad state
    that way.
    """
    from ministack.services.sqs import _dlq_sweep

    bad_rp = '"' + json.dumps(
        {"deadLetterTargetArn": "arn:x", "maxReceiveCount": "2"}
    ).replace('"', '\\"') + '"'
    fake_q = {
        "messages": [],
        "attributes": {"RedrivePolicy": bad_rp},
    }
    # Pre-fix this raised: AttributeError: 'str' object has no attribute 'get'
    _dlq_sweep(fake_q)
    assert fake_q["messages"] == []  # nothing to sweep, no crash


# ---------------------------------------------------------------------------
# /_ministack/sqs/messages — pure introspection over the queue store
# ---------------------------------------------------------------------------

def test_sqs_messages_endpoint_basic(sqs):
    """GET /_ministack/sqs/messages returns sent messages grouped by account
    and queue URL, without affecting subsequent ReceiveMessage."""
    import urllib.request
    qurl = sqs.create_queue(QueueName=f"intg-peek-{_uuid_mod.uuid4().hex[:8]}")["QueueUrl"]
    sqs.send_message(QueueUrl=qurl, MessageBody="hello-peek-1")
    sqs.send_message(QueueUrl=qurl, MessageBody="hello-peek-2")
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    with urllib.request.urlopen(f"{endpoint}/_ministack/sqs/messages?QueueUrl={qurl}") as r:
        data = json.loads(r.read())

    # One account, one queue, two messages.
    accts = list(data["messages"].keys())
    assert len(accts) == 1
    assert qurl in data["messages"][accts[0]]["us-east-1"]
    msgs = data["messages"][accts[0]]["us-east-1"][qurl]
    bodies = sorted(m["Body"] for m in msgs)
    assert bodies == ["hello-peek-1", "hello-peek-2"]
    # Peek must not have receive-counted the messages.
    for m in msgs:
        assert m["ReceiveCount"] == 0
        assert m["IsVisible"] is True

    # Subsequent ReceiveMessage still returns both — peek did not mutate.
    received = []
    for _ in range(2):
        resp = sqs.receive_message(QueueUrl=qurl, MaxNumberOfMessages=1, VisibilityTimeout=0)
        received.extend(resp.get("Messages", []))
    assert sorted(m["Body"] for m in received) == ["hello-peek-1", "hello-peek-2"]
    sqs.delete_queue(QueueUrl=qurl)


def test_sqs_messages_endpoint_separates_same_url_regions(sqs):
    """QueueUrl peeks keep same-name regional queues separate."""
    import urllib.request

    name = f"intg-peek-region-{_uuid_mod.uuid4().hex[:8]}"
    west = _regional_sqs("us-west-2")
    east_url = sqs.create_queue(QueueName=name)["QueueUrl"]
    west_url = west.create_queue(QueueName=name)["QueueUrl"]
    sqs.send_message(QueueUrl=east_url, MessageBody="east-peek")
    west.send_message(QueueUrl=west_url, MessageBody="west-peek")
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    with urllib.request.urlopen(f"{endpoint}/_ministack/sqs/messages?QueueUrl={east_url}") as r:
        data = json.loads(r.read())

    acct = next(iter(data["messages"]))
    by_region = data["messages"][acct]
    assert [m["Body"] for m in by_region["us-east-1"][east_url]] == ["east-peek"]
    assert [m["Body"] for m in by_region["us-west-2"][west_url]] == ["west-peek"]


def test_sqs_messages_endpoint_invalid_account_rejected(sqs):
    """?account=<not-12-digit> returns 400 InvalidAccountID."""
    import urllib.error
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    try:
        urllib.request.urlopen(f"{endpoint}/_ministack/sqs/messages?account=abc")
        raise AssertionError("expected 400")
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert body["__type"] == "InvalidAccountID"


def test_sqs_restore_rebuilds_legacy_name_index_from_queue_arn():
    import ministack.services.sqs as _sqs
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    original_queues = dict(_sqs._queues._data)
    original_names = dict(_sqs._queue_name_to_url._data)
    try:
        _sqs._queues.clear()
        _sqs._queue_name_to_url.clear()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")

        legacy_queues = AccountScopedDict()
        legacy_names = AccountScopedDict()
        queue_name = f"legacy-west-{_uuid_mod.uuid4().hex[:8]}"
        queue_url = f"http://localhost:4566/000000000000/{queue_name}"
        legacy_queues[queue_url] = {
            "name": queue_name,
            "url": queue_url,
            "attributes": {"QueueArn": f"arn:aws:sqs:us-west-2:000000000000:{queue_name}"},
            "messages": [],
            "tags": {},
            "is_fifo": False,
            "dedup_cache": {},
            "fifo_seq": 0,
        }
        legacy_names[queue_name] = queue_url

        _sqs.restore_state({"queues": legacy_queues, "queue_name_to_url": legacy_names})

        assert _sqs._queue_name_to_url.get(queue_name) is None
        set_request_region("us-west-2")
        assert _sqs._queue_name_to_url.get(queue_name) == queue_url
        assert _sqs._get_q(queue_url)["attributes"]["QueueArn"].endswith(queue_name)
    finally:
        _sqs._queues.clear()
        _sqs._queues._data.update(original_queues)
        _sqs._queue_name_to_url.clear()
        _sqs._queue_name_to_url._data.update(original_names)
        set_request_account_id(original_account)
        set_request_region(original_region)


# ── Numeric attribute range validation (#841) ────────────────────────


def test_sqs_create_queue_rejects_visibility_timeout_too_high(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-vt-high", Attributes={"VisibilityTimeout": "99999"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName="q-vt-high")["QueueUrl"])
        raise AssertionError("expected InvalidAttributeValue for VisibilityTimeout=99999")


def test_sqs_create_queue_rejects_visibility_timeout_negative(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-vt-neg", Attributes={"VisibilityTimeout": "-1"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        raise AssertionError("expected InvalidAttributeValue for VisibilityTimeout=-1")


def test_sqs_create_queue_rejects_delay_seconds_out_of_range(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-ds-bad", Attributes={"DelaySeconds": "901"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        raise AssertionError("expected InvalidAttributeValue for DelaySeconds=901")


def test_sqs_create_queue_rejects_maximum_message_size_below_min(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-mms-low", Attributes={"MaximumMessageSize": "512"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        raise AssertionError("expected InvalidAttributeValue for MaximumMessageSize=512")


def test_sqs_create_queue_rejects_receive_wait_too_long(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-rwt-bad",
                          Attributes={"ReceiveMessageWaitTimeSeconds": "21"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        raise AssertionError("expected InvalidAttributeValue for ReceiveMessageWaitTimeSeconds=21")


def test_sqs_set_queue_attributes_rejects_visibility_timeout_too_high(sqs):
    import botocore.exceptions
    url = sqs.create_queue(QueueName="q-set-vt-bad")["QueueUrl"]
    try:
        try:
            sqs.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "100000"})
        except botocore.exceptions.ClientError as exc:
            assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
        else:
            raise AssertionError("expected InvalidAttributeValue on SetQueueAttributes")
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_sqs_set_queue_attributes_accepts_valid_visibility_timeout(sqs):
    # Regression guard — must NOT reject in-range values.
    url = sqs.create_queue(QueueName="q-set-vt-ok")["QueueUrl"]
    try:
        sqs.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "120"})
        attrs = sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["VisibilityTimeout"])["Attributes"]
        assert attrs["VisibilityTimeout"] == "120"
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_sqs_create_queue_rejects_non_numeric_visibility_timeout(sqs):
    import botocore.exceptions
    try:
        sqs.create_queue(QueueName="q-vt-nan", Attributes={"VisibilityTimeout": "abc"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "InvalidAttributeValue"
    else:
        raise AssertionError("expected InvalidAttributeValue for non-numeric")


def test_sqs_send_message_rejects_control_chars(sqs):
    """SendMessage must reject message bodies containing XML 1.0 forbidden characters.

    AWS SQS only allows: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
    Control characters like NULL, BEL, VT, etc. must result in InvalidMessageContents (400).
    See: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
    """
    url = sqs.create_queue(QueueName="intg-sqs-invalid-chars")["QueueUrl"]

    forbidden = [
        "\x00",        # NULL
        "\x01",        # SOH
        "\x08",        # BS (last before tab)
        "\x0b",        # VT (vertical tab)
        "\x0c",        # FF (form feed)
        "\x0e",        # SO (first after CR)
        "\x1f",        # US (last C0 control char)
        "hello\x00world",  # control char embedded in normal text
        "\ufffe",      # non-character
        "\uffff",      # non-character
    ]
    for body in forbidden:
        with pytest.raises(ClientError) as exc:
            sqs.send_message(QueueUrl=url, MessageBody=body)
        code = exc.value.response["Error"]["Code"]
        assert code == "InvalidMessageContents", (
            f"expected InvalidMessageContents for {repr(body)}, got {code}"
        )
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_sqs_send_message_allows_valid_chars(sqs):
    """SendMessage must allow all XML 1.0 valid characters including tab, LF, CR, and Unicode."""
    url = sqs.create_queue(QueueName="intg-sqs-valid-chars")["QueueUrl"]

    allowed = [
        "hello world",
        "tab\there",
        "newline\nhere",
        "cr\rhere",
        "こんにちは世界",
        "héllo wörld",
        "emoji \U0001f600",
        "!@#$%^&*()",
    ]
    for body in allowed:
        resp = sqs.send_message(QueueUrl=url, MessageBody=body)
        assert "MessageId" in resp, f"send failed for {repr(body)}"


def test_sqs_xml_query_error_code_uses_legacy_namespace():
    """XML Query API error responses must use legacy namespaced codes
    (e.g. AWS.SimpleQueueService.NonExistentQueue) not the short JSON codes
    (e.g. QueueDoesNotExist). .NET SDK and other XML-protocol callers match
    on the namespaced string (#1066)."""
    import urllib.error
    import urllib.request
    import xml.etree.ElementTree as ET

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    try:
        urllib.request.urlopen(f"{endpoint}/?Action=GetQueueUrl&QueueName=nonexistent-xml-test-queue")
        raise AssertionError("expected HTTP error")
    except urllib.error.HTTPError as e:
        body = e.read()
        root = ET.fromstring(body)
        ns = {"ns": "http://queue.amazonaws.com/doc/2012-11-05/"}
        code = root.find(".//ns:Code", ns).text
        assert code == "AWS.SimpleQueueService.NonExistentQueue", (
            f"XML error code must be legacy namespaced, got '{code}'"
        )


def test_sqs_invalid_chars_regex_matches_xml10_complement():
    """The forbidden-char regex must be the exact complement of AWS SQS's allowed
    set (#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF), which
    excludes the surrogate block #xD800-#xDFFF. Lone surrogates can't be sent
    through boto3 (client-side UnicodeEncodeError), so this guards the regex
    directly rather than via the wire."""
    from ministack.services.sqs import _INVALID_SQS_CHARS_RE as rx

    # Forbidden: C0 controls (except tab/LF/CR), surrogates, #xFFFE/#xFFFF.
    for c in ("\x00", "\x08", "\x0b", "\x0c", "\x0e", "\x1f",
              "\ud800", "\udfff", "￾", "￿"):
        assert rx.search(c), f"must reject {c!r}"
    # Allowed: tab/LF/CR, the range boundaries, BMP, supplementary.
    for c in ("\t", "\n", "\r", "퟿", "", "�",
              "a", "こ", "\U0001f600"):
        assert not rx.search(c), f"must allow {c!r}"
