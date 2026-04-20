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

def test_sqs_create_queue(sqs):
    resp = sqs.create_queue(QueueName="intg-sqs-create")
    assert "QueueUrl" in resp
    assert "intg-sqs-create" in resp["QueueUrl"]

def test_sqs_delete_queue(sqs):
    url = sqs.create_queue(QueueName="intg-sqs-delete")["QueueUrl"]
    sqs.delete_queue(QueueUrl=url)
    with pytest.raises(ClientError):
        sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])

def test_sqs_list_queues(sqs):
    sqs.create_queue(QueueName="intg-sqs-list-alpha")
    sqs.create_queue(QueueName="intg-sqs-list-beta")
    resp = sqs.list_queues(QueueNamePrefix="intg-sqs-list-")
    urls = resp.get("QueueUrls", [])
    assert len(urls) >= 2
    assert any("intg-sqs-list-alpha" in u for u in urls)
    assert any("intg-sqs-list-beta" in u for u in urls)

def test_sqs_get_queue_url(sqs):
    sqs.create_queue(QueueName="intg-sqs-geturl")
    resp = sqs.get_queue_url(QueueName="intg-sqs-geturl")
    assert "intg-sqs-geturl" in resp["QueueUrl"]

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
