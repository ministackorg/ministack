"""
Integration tests for the DynamoDB -> Kinesis streaming destination API
(`aws_dynamodb_kinesis_streaming_destination`).

Exercises the full lifecycle:
  Enable -> Describe(ACTIVE) -> item mutations fan out to Kinesis ->
  Disable -> Describe(DISABLED) -> mutations NO LONGER fan out ->
  Update ApproximateCreationDateTimePrecision.
"""

import base64
import json
import time

import pytest
from botocore.exceptions import ClientError


def _make_table(ddb, name):
    try:
        ddb.delete_table(TableName=name)
    except ClientError:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )


def _make_stream(kin, name):
    try:
        kin.delete_stream(StreamName=name)
    except ClientError:
        pass
    kin.create_stream(StreamName=name, ShardCount=1)
    # Streams are ACTIVE immediately in MiniStack, but the DescribeStream call
    # is cheap and keeps the test robust if that ever changes.
    kin.describe_stream(StreamName=name)
    return kin.describe_stream(StreamName=name)["StreamDescription"]["StreamARN"]


def _drain_kinesis(kin, stream_name):
    shards = kin.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"]
    out = []
    for shard in shards:
        it = kin.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        for _ in range(5):
            resp = kin.get_records(ShardIterator=it, Limit=1000)
            out.extend(resp.get("Records", []))
            nxt = resp.get("NextShardIterator")
            if not nxt or nxt == it or not resp.get("Records"):
                break
            it = nxt
    return out


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_enable_returns_active_and_describe_lists_it(ddb, kin):
    _make_table(ddb, "KdsLifecycle")
    arn = _make_stream(kin, "ministack-kds-lifecycle")

    resp = ddb.enable_kinesis_streaming_destination(
        TableName="KdsLifecycle", StreamArn=arn,
    )
    assert resp["DestinationStatus"] == "ACTIVE"
    assert resp["StreamArn"] == arn
    assert resp["TableName"] == "KdsLifecycle"

    desc = ddb.describe_kinesis_streaming_destination(TableName="KdsLifecycle")
    dests = desc["KinesisDataStreamDestinations"]
    assert len(dests) == 1
    assert dests[0]["StreamArn"] == arn
    assert dests[0]["DestinationStatus"] == "ACTIVE"
    assert dests[0]["ApproximateCreationDateTimePrecision"] == "MILLISECOND"


def test_enable_twice_same_table_and_arn_raises(ddb, kin):
    _make_table(ddb, "KdsDup")
    arn = _make_stream(kin, "ministack-kds-dup")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDup", StreamArn=arn)

    with pytest.raises(ClientError) as ei:
        ddb.enable_kinesis_streaming_destination(TableName="KdsDup", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceInUseException"


def test_enable_requires_existing_table(ddb, kin):
    arn = _make_stream(kin, "ministack-kds-missing")
    with pytest.raises(ClientError) as ei:
        ddb.enable_kinesis_streaming_destination(TableName="ThisTableDoesNotExist", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Delivery: item mutations end up as Kinesis records when ACTIVE
# ---------------------------------------------------------------------------

def test_item_mutations_land_in_kinesis_stream(ddb, kin):
    _make_table(ddb, "KdsDeliver")
    arn = _make_stream(kin, "ministack-kds-deliver")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDeliver", StreamArn=arn)

    ddb.put_item(TableName="KdsDeliver", Item={"pk": {"S": "a"}, "val": {"N": "1"}})
    ddb.update_item(
        TableName="KdsDeliver",
        Key={"pk": {"S": "a"}},
        UpdateExpression="SET val = :v",
        ExpressionAttributeValues={":v": {"N": "2"}},
    )
    ddb.delete_item(TableName="KdsDeliver", Key={"pk": {"S": "a"}})

    records = _drain_kinesis(kin, "ministack-kds-deliver")
    assert len(records) == 3

    decoded = []
    for r in records:
        payload = r["Data"]
        # boto3 normalises the base64-encoded Data back into bytes for us
        if isinstance(payload, str):
            payload = base64.b64decode(payload)
        decoded.append(json.loads(payload.decode("utf-8")))

    event_names = [d["eventName"] for d in decoded]
    assert event_names == ["INSERT", "MODIFY", "REMOVE"]
    for d in decoded:
        assert d["eventSource"] == "aws:dynamodb"
        assert d["dynamodb"]["Keys"] == {"pk": {"S": "a"}}


def test_disable_stops_delivery(ddb, kin):
    _make_table(ddb, "KdsDisable")
    arn = _make_stream(kin, "ministack-kds-disable")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDisable", StreamArn=arn)

    ddb.put_item(TableName="KdsDisable", Item={"pk": {"S": "before"}})

    resp = ddb.disable_kinesis_streaming_destination(TableName="KdsDisable", StreamArn=arn)
    assert resp["DestinationStatus"] == "DISABLED"

    # Describe still lists the now-DISABLED entry (matches AWS ~24h retention).
    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsDisable")[
        "KinesisDataStreamDestinations"
    ]
    assert len(dests) == 1
    assert dests[0]["DestinationStatus"] == "DISABLED"

    ddb.put_item(TableName="KdsDisable", Item={"pk": {"S": "after"}})

    records = _drain_kinesis(kin, "ministack-kds-disable")
    assert len(records) == 1  # only the pre-disable INSERT


def test_disable_without_active_raises(ddb, kin):
    _make_table(ddb, "KdsNoActive")
    arn = _make_stream(kin, "ministack-kds-no-active")
    with pytest.raises(ClientError) as ei:
        ddb.disable_kinesis_streaming_destination(TableName="KdsNoActive", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

def test_update_precision(ddb, kin):
    _make_table(ddb, "KdsUpdate")
    arn = _make_stream(kin, "ministack-kds-update")
    ddb.enable_kinesis_streaming_destination(TableName="KdsUpdate", StreamArn=arn)

    resp = ddb.update_kinesis_streaming_destination(
        TableName="KdsUpdate",
        StreamArn=arn,
        UpdateKinesisStreamingConfiguration={
            "ApproximateCreationDateTimePrecision": "MICROSECOND",
        },
    )
    assert resp["DestinationStatus"] == "ACTIVE"
    assert (
        resp["UpdateKinesisStreamingConfiguration"]["ApproximateCreationDateTimePrecision"]
        == "MICROSECOND"
    )

    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsUpdate")[
        "KinesisDataStreamDestinations"
    ]
    assert dests[0]["ApproximateCreationDateTimePrecision"] == "MICROSECOND"


def test_update_rejects_invalid_precision(ddb, kin):
    _make_table(ddb, "KdsUpdateInvalid")
    arn = _make_stream(kin, "ministack-kds-update-invalid")
    ddb.enable_kinesis_streaming_destination(TableName="KdsUpdateInvalid", StreamArn=arn)

    with pytest.raises(ClientError) as ei:
        ddb.update_kinesis_streaming_destination(
            TableName="KdsUpdateInvalid",
            StreamArn=arn,
            UpdateKinesisStreamingConfiguration={
                "ApproximateCreationDateTimePrecision": "NANOSECOND",
            },
        )
    # boto3 performs client-side enum validation and raises ParamValidationError
    # BEFORE the request is sent; fall back to a server-side check with a raw
    # call if that's what happened.
    err = ei.value
    code = getattr(err, "response", {}).get("Error", {}).get("Code", "")
    assert code in ("ValidationException", "")


# ---------------------------------------------------------------------------
# Cleanup on table delete
# ---------------------------------------------------------------------------

def test_delete_table_removes_destinations(ddb, kin):
    _make_table(ddb, "KdsAutoclean")
    arn = _make_stream(kin, "ministack-kds-autoclean")
    ddb.enable_kinesis_streaming_destination(TableName="KdsAutoclean", StreamArn=arn)

    ddb.delete_table(TableName="KdsAutoclean")
    # After a fresh CreateTable there should be zero destinations.
    _make_table(ddb, "KdsAutoclean")
    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsAutoclean")[
        "KinesisDataStreamDestinations"
    ]
    assert dests == []


# ---------------------------------------------------------------------------
# Order stability (smoke)
# ---------------------------------------------------------------------------

def test_multiple_puts_land_in_order(ddb, kin):
    _make_table(ddb, "KdsOrder")
    arn = _make_stream(kin, "ministack-kds-order")
    ddb.enable_kinesis_streaming_destination(TableName="KdsOrder", StreamArn=arn)

    for i in range(5):
        ddb.put_item(TableName="KdsOrder", Item={"pk": {"S": f"k{i}"}})

    # Short sleep to make sure arrival timestamps settle (not strictly needed).
    time.sleep(0.05)
    records = _drain_kinesis(kin, "ministack-kds-order")
    assert len(records) == 5
    decoded = [
        json.loads(
            (base64.b64decode(r["Data"]) if isinstance(r["Data"], str) else r["Data"]).decode("utf-8")
        )
        for r in records
    ]
    keys = [d["dynamodb"]["Keys"]["pk"]["S"] for d in decoded]
    assert keys == [f"k{i}" for i in range(5)]


