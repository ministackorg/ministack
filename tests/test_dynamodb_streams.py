"""
Integration tests for the DynamoDB Streams read API.

These verify that records emitted by the main DynamoDB service (through
PutItem/UpdateItem/DeleteItem/TransactWriteItems/BatchWriteItem) become
visible through the public Streams API used by boto3.client("dynamodbstreams")
and by Lambda event-source mappings.
"""

import pytest
from botocore.exceptions import ClientError

# MiniStack exposes a single synthetic shard per stream; its id matches this.
_DEFAULT_SHARD_ID = "shardId-00000000000000000000-00000000"


def _make_table(ddb, name, view_type="NEW_AND_OLD_IMAGES", stream_enabled=True):
    try:
        ddb.delete_table(TableName=name)
    except ClientError:
        pass
    kwargs = dict(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    if stream_enabled:
        kwargs["StreamSpecification"] = {
            "StreamEnabled": True,
            "StreamViewType": view_type,
        }
    ddb.create_table(**kwargs)


def _stream_arn(ddb, table_name):
    desc = ddb.describe_table(TableName=table_name)["Table"]
    return desc.get("LatestStreamArn")


def _collect_all(ddb_streams, iterator, max_pages=20):
    """Drain a stream iterator until no new records arrive or max_pages hit."""
    all_records = []
    next_iter = iterator
    for _ in range(max_pages):
        resp = ddb_streams.get_records(ShardIterator=next_iter, Limit=1000)
        all_records.extend(resp.get("Records", []))
        new_iter = resp.get("NextShardIterator")
        if not new_iter or new_iter == next_iter or not resp.get("Records"):
            break
        next_iter = new_iter
    return all_records


# ---------------------------------------------------------------------------
# ListStreams / DescribeStream
# ---------------------------------------------------------------------------

def test_list_streams_only_includes_enabled(ddb, ddb_streams):
    _make_table(ddb, "StreamsEnabled")
    _make_table(ddb, "StreamsDisabled", stream_enabled=False)

    streams = ddb_streams.list_streams()["Streams"]
    names = {s["TableName"] for s in streams}
    assert "StreamsEnabled" in names
    assert "StreamsDisabled" not in names


def test_list_streams_filter_by_table(ddb, ddb_streams):
    _make_table(ddb, "StreamsFilterA")
    _make_table(ddb, "StreamsFilterB")

    streams = ddb_streams.list_streams(TableName="StreamsFilterA")["Streams"]
    assert len(streams) == 1
    assert streams[0]["TableName"] == "StreamsFilterA"
    assert streams[0]["StreamArn"].endswith("/stream/" + streams[0]["StreamLabel"])


def test_describe_stream_returns_single_shard_and_view_type(ddb, ddb_streams):
    _make_table(ddb, "StreamsDescribe", view_type="NEW_IMAGE")
    arn = _stream_arn(ddb, "StreamsDescribe")

    desc = ddb_streams.describe_stream(StreamArn=arn)["StreamDescription"]
    assert desc["StreamArn"] == arn
    assert desc["StreamStatus"] == "ENABLED"
    assert desc["StreamViewType"] == "NEW_IMAGE"
    assert desc["TableName"] == "StreamsDescribe"
    assert len(desc["Shards"]) == 1
    assert desc["Shards"][0]["ShardId"]


def test_describe_stream_unknown_arn_raises(ddb_streams):
    bogus = "arn:aws:dynamodb:us-east-1:000000000000:table/NoSuch/stream/1970-01-01T00:00:00.000"
    with pytest.raises(ClientError) as exc:
        ddb_streams.describe_stream(StreamArn=bogus)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# GetShardIterator / GetRecords
# ---------------------------------------------------------------------------

def test_insert_modify_remove_via_public_api(ddb, ddb_streams):
    _make_table(ddb, "StreamsCRUD")
    arn = _stream_arn(ddb, "StreamsCRUD")
    shard_id = ddb_streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"][0]["ShardId"]

    # TRIM_HORIZON: iterator starts before any records so all three events
    # (INSERT, MODIFY, REMOVE) must be visible in order.
    iterator = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]

    ddb.put_item(TableName="StreamsCRUD", Item={"pk": {"S": "a"}, "v": {"S": "1"}})
    ddb.update_item(
        TableName="StreamsCRUD",
        Key={"pk": {"S": "a"}},
        UpdateExpression="SET v = :v",
        ExpressionAttributeValues={":v": {"S": "2"}},
    )
    ddb.delete_item(TableName="StreamsCRUD", Key={"pk": {"S": "a"}})

    records = _collect_all(ddb_streams, iterator)
    event_names = [r["eventName"] for r in records]
    assert event_names == ["INSERT", "MODIFY", "REMOVE"]

    insert, modify, remove = records
    assert insert["dynamodb"]["NewImage"]["v"]["S"] == "1"
    assert "OldImage" not in insert["dynamodb"]
    assert modify["dynamodb"]["NewImage"]["v"]["S"] == "2"
    assert modify["dynamodb"]["OldImage"]["v"]["S"] == "1"
    assert remove["dynamodb"]["OldImage"]["v"]["S"] == "2"
    assert "NewImage" not in remove["dynamodb"]


def test_latest_iterator_skips_existing_records(ddb, ddb_streams):
    _make_table(ddb, "StreamsLatest")
    arn = _stream_arn(ddb, "StreamsLatest")

    ddb.put_item(TableName="StreamsLatest", Item={"pk": {"S": "before"}})

    iterator = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=_DEFAULT_SHARD_ID, ShardIteratorType="LATEST",
    )["ShardIterator"]

    ddb.put_item(TableName="StreamsLatest", Item={"pk": {"S": "after"}})

    records = _collect_all(ddb_streams, iterator)
    keys = [r["dynamodb"]["Keys"]["pk"]["S"] for r in records]
    assert keys == ["after"]


def test_at_and_after_sequence_number(ddb, ddb_streams):
    _make_table(ddb, "StreamsSeq")
    arn = _stream_arn(ddb, "StreamsSeq")

    for i in range(3):
        ddb.put_item(TableName="StreamsSeq", Item={"pk": {"S": f"k{i}"}})

    trim = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=_DEFAULT_SHARD_ID, ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]
    all_records = _collect_all(ddb_streams, trim)
    assert len(all_records) == 3
    middle_seq = all_records[1]["dynamodb"]["SequenceNumber"]

    at_iter = ddb_streams.get_shard_iterator(
        StreamArn=arn,
        ShardId=_DEFAULT_SHARD_ID,
        ShardIteratorType="AT_SEQUENCE_NUMBER",
        SequenceNumber=middle_seq,
    )["ShardIterator"]
    at_records = _collect_all(ddb_streams, at_iter)
    assert [r["dynamodb"]["Keys"]["pk"]["S"] for r in at_records] == ["k1", "k2"]

    after_iter = ddb_streams.get_shard_iterator(
        StreamArn=arn,
        ShardId=_DEFAULT_SHARD_ID,
        ShardIteratorType="AFTER_SEQUENCE_NUMBER",
        SequenceNumber=middle_seq,
    )["ShardIterator"]
    after_records = _collect_all(ddb_streams, after_iter)
    assert [r["dynamodb"]["Keys"]["pk"]["S"] for r in after_records] == ["k2"]


def test_get_records_limit_and_next_iterator(ddb, ddb_streams):
    _make_table(ddb, "StreamsPage")
    arn = _stream_arn(ddb, "StreamsPage")

    for i in range(5):
        ddb.put_item(TableName="StreamsPage", Item={"pk": {"S": f"k{i}"}})

    iterator = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=_DEFAULT_SHARD_ID, ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]

    first = ddb_streams.get_records(ShardIterator=iterator, Limit=2)
    assert len(first["Records"]) == 2
    assert first["NextShardIterator"]

    second = ddb_streams.get_records(ShardIterator=first["NextShardIterator"], Limit=10)
    assert len(second["Records"]) == 3
    tail = ddb_streams.get_records(ShardIterator=second["NextShardIterator"], Limit=10)
    assert tail["Records"] == []
    assert tail["NextShardIterator"]  # still valid, polls for new records


# ---------------------------------------------------------------------------
# View type behaviour
# ---------------------------------------------------------------------------

def test_keys_only_view_omits_images(ddb, ddb_streams):
    _make_table(ddb, "StreamsKeysOnly", view_type="KEYS_ONLY")
    arn = _stream_arn(ddb, "StreamsKeysOnly")

    iterator = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=_DEFAULT_SHARD_ID, ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]

    ddb.put_item(TableName="StreamsKeysOnly", Item={"pk": {"S": "k"}, "x": {"N": "1"}})
    ddb.delete_item(TableName="StreamsKeysOnly", Key={"pk": {"S": "k"}})

    records = _collect_all(ddb_streams, iterator)
    assert [r["eventName"] for r in records] == ["INSERT", "REMOVE"]
    for r in records:
        assert "NewImage" not in r["dynamodb"]
        assert "OldImage" not in r["dynamodb"]
        assert r["dynamodb"]["Keys"] == {"pk": {"S": "k"}}
        assert r["dynamodb"]["StreamViewType"] == "KEYS_ONLY"


def test_old_image_only_view(ddb, ddb_streams):
    _make_table(ddb, "StreamsOldImg", view_type="OLD_IMAGE")
    arn = _stream_arn(ddb, "StreamsOldImg")

    iterator = ddb_streams.get_shard_iterator(
        StreamArn=arn, ShardId=_DEFAULT_SHARD_ID, ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]

    ddb.put_item(TableName="StreamsOldImg", Item={"pk": {"S": "k"}, "v": {"S": "1"}})
    ddb.update_item(
        TableName="StreamsOldImg",
        Key={"pk": {"S": "k"}},
        UpdateExpression="SET v = :v",
        ExpressionAttributeValues={":v": {"S": "2"}},
    )

    records = _collect_all(ddb_streams, iterator)
    modify = records[-1]
    assert "NewImage" not in modify["dynamodb"]
    assert modify["dynamodb"]["OldImage"]["v"]["S"] == "1"


# ---------------------------------------------------------------------------
# Validation / errors
# ---------------------------------------------------------------------------

def test_get_shard_iterator_missing_sequence_number(ddb, ddb_streams):
    _make_table(ddb, "StreamsValidateSeq")
    arn = _stream_arn(ddb, "StreamsValidateSeq")
    with pytest.raises(ClientError) as exc:
        ddb_streams.get_shard_iterator(
            StreamArn=arn,
            ShardId=_DEFAULT_SHARD_ID,
            ShardIteratorType="AT_SEQUENCE_NUMBER",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_get_records_rejects_garbage_iterator(ddb_streams):
    with pytest.raises(ClientError) as exc:
        ddb_streams.get_records(ShardIterator="not-a-valid-iterator")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# Terraform compatibility tests
#
# These verify the shape that the Terraform AWS provider's aws_dynamodb_table
# resource reads back when stream_enabled = true, and that the stream ARN
# round-trips through DescribeTable -> DescribeStream so a downstream
# aws_lambda_event_source_mapping can hook in.
# ---------------------------------------------------------------------------


def test_terraform_stream_arn_round_trips_describe_table_to_describe_stream(ddb, ddb_streams):
    """aws_dynamodb_table exposes stream_arn + stream_label; both must match
    what describe-stream returns so aws_lambda_event_source_mapping works."""
    tname = "tf-stream-roundtrip"
    _make_table(ddb, tname, view_type="NEW_AND_OLD_IMAGES")
    try:
        desc = ddb.describe_table(TableName=tname)["Table"]
        stream_arn = desc["LatestStreamArn"]
        stream_label = desc["LatestStreamLabel"]

        assert stream_arn.startswith("arn:aws:dynamodb:")
        assert f":table/{tname}/stream/{stream_label}" in stream_arn

        stream = ddb_streams.describe_stream(StreamArn=stream_arn)["StreamDescription"]
        assert stream["StreamArn"] == stream_arn
        assert stream["StreamLabel"] == stream_label
        assert stream["TableName"] == tname
        assert stream["StreamViewType"] == "NEW_AND_OLD_IMAGES"
        assert stream["StreamStatus"] in ("ENABLED", "ENABLING")
        assert stream["Shards"], "DescribeStream must report at least one shard"
    finally:
        ddb.delete_table(TableName=tname)


def test_terraform_update_table_toggles_stream_specification(ddb, ddb_streams):
    """``terraform apply`` flipping stream_enabled false->true->false must
    land on the same DescribeTable contract."""
    tname = "tf-stream-toggle"
    _make_table(ddb, tname, stream_enabled=False)
    try:
        before = ddb.describe_table(TableName=tname)["Table"]
        assert "LatestStreamArn" not in before or not before["LatestStreamArn"]

        ddb.update_table(
            TableName=tname,
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"},
        )
        after = ddb.describe_table(TableName=tname)["Table"]
        assert after["StreamSpecification"]["StreamEnabled"] is True
        assert after["StreamSpecification"]["StreamViewType"] == "KEYS_ONLY"
    finally:
        ddb.delete_table(TableName=tname)


def test_terraform_list_streams_filters_by_table(ddb, ddb_streams):
    """aws_dynamodb_table data source calls ListStreams(TableName=...) to
    discover the current stream ARN without a full DescribeTable."""
    tname = "tf-stream-listfilter"
    _make_table(ddb, tname)
    try:
        arn = _stream_arn(ddb, tname)
        listed = ddb_streams.list_streams(TableName=tname)["Streams"]
        assert any(s["StreamArn"] == arn and s["TableName"] == tname for s in listed)
    finally:
        ddb.delete_table(TableName=tname)
