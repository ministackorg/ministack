import asyncio
import base64
import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from types import SimpleNamespace

import boto3
import cbor2
import pytest
from botocore.config import Config
from botocore.eventstream import EventStreamBuffer
from botocore.exceptions import ClientError

from ministack.core.responses import AccountRegionScopedDict, request_scope
from ministack.services import kinesis

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _regional_kin(region_name, account_id="test"):
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    return boto3.client(
        "kinesis",
        endpoint_url=endpoint,
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        region_name=region_name,
        config=Config(region_name=region_name, retries={"mode": "standard"}),
    )


def test_kinesis_put_get(kin):
    kin.create_stream(StreamName="test-stream", ShardCount=1)
    kin.put_record(StreamName="test-stream", Data=b"hello kinesis", PartitionKey="pk1")
    kin.put_record(StreamName="test-stream", Data=b"second record", PartitionKey="pk2")
    desc = kin.describe_stream(StreamName="test-stream")
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(StreamName="test-stream", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")
    records = kin.get_records(ShardIterator=it["ShardIterator"])
    assert len(records["Records"]) == 2

def test_kinesis_batch(kin):
    kin.create_stream(StreamName="test-stream-batch", ShardCount=1)
    resp = kin.put_records(
        StreamName="test-stream-batch",
        Records=[{"Data": f"record-{i}".encode(), "PartitionKey": f"pk{i}"} for i in range(5)],
    )
    assert resp["FailedRecordCount"] == 0
    assert len(resp["Records"]) == 5

def test_kinesis_list(kin):
    resp = kin.list_streams()
    assert "test-stream" in resp["StreamNames"]

def test_kinesis_create_stream_v2(kin):
    kin.create_stream(StreamName="kin-cs-v2", ShardCount=2)
    desc = kin.describe_stream(StreamName="kin-cs-v2")
    sd = desc["StreamDescription"]
    assert sd["StreamName"] == "kin-cs-v2"
    assert sd["StreamStatus"] == "ACTIVE"
    assert len(sd["Shards"]) == 2

def test_kinesis_put_get_records_v2(kin):
    kin.create_stream(StreamName="kin-pgr-v2", ShardCount=1)
    kin.put_record(StreamName="kin-pgr-v2", Data=b"rec1", PartitionKey="pk1")
    kin.put_record(StreamName="kin-pgr-v2", Data=b"rec2", PartitionKey="pk2")
    kin.put_record(StreamName="kin-pgr-v2", Data=b"rec3", PartitionKey="pk3")

    desc = kin.describe_stream(StreamName="kin-pgr-v2")
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(
        StreamName="kin-pgr-v2",
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    records = kin.get_records(ShardIterator=it["ShardIterator"])
    assert len(records["Records"]) == 3
    assert records["Records"][0]["Data"] == b"rec1"

def test_kinesis_put_records_batch_v2(kin):
    kin.create_stream(StreamName="kin-batch-v2", ShardCount=1)
    resp = kin.put_records(
        StreamName="kin-batch-v2",
        Records=[{"Data": f"b{i}".encode(), "PartitionKey": f"pk{i}"} for i in range(7)],
    )
    assert resp["FailedRecordCount"] == 0
    assert len(resp["Records"]) == 7
    for r in resp["Records"]:
        assert "ShardId" in r
        assert "SequenceNumber" in r

def test_kinesis_list_streams_v2(kin):
    kin.create_stream(StreamName="kin-ls-v2a", ShardCount=1)
    kin.create_stream(StreamName="kin-ls-v2b", ShardCount=1)
    resp = kin.list_streams()
    assert "kin-ls-v2a" in resp["StreamNames"]
    assert "kin-ls-v2b" in resp["StreamNames"]

def test_kinesis_list_shards_v2(kin):
    kin.create_stream(StreamName="kin-lsh-v2", ShardCount=3)
    resp = kin.list_shards(StreamName="kin-lsh-v2")
    assert len(resp["Shards"]) == 3
    for shard in resp["Shards"]:
        assert "ShardId" in shard
        assert "HashKeyRange" in shard

def test_kinesis_describe_stream_v2(kin):
    kin.create_stream(StreamName="kin-desc-v2", ShardCount=1)
    resp = kin.describe_stream(StreamName="kin-desc-v2")
    sd = resp["StreamDescription"]
    assert sd["StreamName"] == "kin-desc-v2"
    assert sd["RetentionPeriodHours"] == 24
    assert "StreamARN" in sd
    assert len(sd["Shards"]) == 1

    summary = kin.describe_stream_summary(StreamName="kin-desc-v2")
    assert summary["StreamDescriptionSummary"]["StreamName"] == "kin-desc-v2"


def test_kinesis_stream_arn_rejects_foreign_region_without_name_fallback(kin):
    """StreamARN inputs must be parsed before matching account-scoped state."""
    west = _regional_kin("us-west-2")
    stream_name = f"kin-arn-scope-{_uuid_mod.uuid4().hex[:8]}"
    west.create_stream(StreamName=stream_name, ShardCount=1)
    west_arn = west.describe_stream(StreamName=stream_name)["StreamDescription"]["StreamARN"]

    with pytest.raises(ClientError) as exc:
        kin.describe_stream(StreamARN=west_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    with pytest.raises(ClientError) as exc:
        kin.put_record(StreamARN=west_arn, Data=b"no-fallback", PartitionKey="pk")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_kinesis_same_name_streams_are_region_scoped(kin):
    west = _regional_kin("us-west-2")
    stream_name = f"kin-region-scope-{_uuid_mod.uuid4().hex[:8]}"

    kin.create_stream(StreamName=stream_name, ShardCount=1)
    west.create_stream(StreamName=stream_name, ShardCount=1)

    east_desc = kin.describe_stream(StreamName=stream_name)["StreamDescription"]
    west_desc = west.describe_stream(StreamName=stream_name)["StreamDescription"]

    assert east_desc["StreamARN"] == f"arn:aws:kinesis:us-east-1:000000000000:stream/{stream_name}"
    assert west_desc["StreamARN"] == f"arn:aws:kinesis:us-west-2:000000000000:stream/{stream_name}"
    assert stream_name in kin.list_streams()["StreamNames"]
    assert stream_name in west.list_streams()["StreamNames"]


def test_kinesis_stream_names_do_not_fallback_across_region_or_account(kin):
    owner_account = "111111111111"
    west_owner = _regional_kin("us-west-2", account_id=owner_account)
    east_owner = _regional_kin("us-east-1", account_id=owner_account)
    west_default = _regional_kin("us-west-2")
    stream_name = f"kin-no-fallback-{_uuid_mod.uuid4().hex[:8]}"

    west_owner.create_stream(StreamName=stream_name, ShardCount=1)
    west_arn = west_owner.describe_stream(StreamName=stream_name)["StreamDescription"]["StreamARN"]

    with pytest.raises(ClientError) as exc:
        east_owner.describe_stream(StreamName=stream_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    with pytest.raises(ClientError) as exc:
        west_default.describe_stream(StreamName=stream_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    with pytest.raises(ClientError) as exc:
        kin.describe_stream(StreamARN=west_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_kinesis_tags_v2(kin):
    kin.create_stream(StreamName="kin-tag-v2", ShardCount=1)
    kin.add_tags_to_stream(StreamName="kin-tag-v2", Tags={"env": "test", "team": "data"})
    resp = kin.list_tags_for_stream(StreamName="kin-tag-v2")
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "data"

    kin.remove_tags_from_stream(StreamName="kin-tag-v2", TagKeys=["team"])
    resp2 = kin.list_tags_for_stream(StreamName="kin-tag-v2")
    tag_map2 = {t["Key"]: t["Value"] for t in resp2["Tags"]}
    assert "team" not in tag_map2
    assert tag_map2["env"] == "test"

def test_kinesis_delete_stream_v2(kin):
    kin.create_stream(StreamName="kin-del-v2", ShardCount=1)
    kin.delete_stream(StreamName="kin-del-v2")
    with pytest.raises(ClientError) as exc:
        kin.describe_stream(StreamName="kin-del-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"

def test_kinesis_stream_encryption(kin):
    import uuid as _uuid

    sname = f"intg-enc-str-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=1)
    time.sleep(0.5)
    kin.start_stream_encryption(StreamName=sname, EncryptionType="KMS", KeyId="alias/aws/kinesis")
    resp = kin.describe_stream(StreamName=sname)
    assert resp["StreamDescription"]["EncryptionType"] == "KMS"
    kin.stop_stream_encryption(StreamName=sname, EncryptionType="KMS", KeyId="alias/aws/kinesis")
    resp2 = kin.describe_stream(StreamName=sname)
    assert resp2["StreamDescription"]["EncryptionType"] == "NONE"
    kin.delete_stream(StreamName=sname)

def test_kinesis_enhanced_monitoring(kin):
    import uuid as _uuid

    sname = f"intg-mon-str-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=1)
    time.sleep(0.5)
    resp = kin.enable_enhanced_monitoring(StreamName=sname, ShardLevelMetrics=["IncomingBytes", "OutgoingBytes"])
    assert "IncomingBytes" in resp.get("DesiredShardLevelMetrics", [])
    resp2 = kin.disable_enhanced_monitoring(StreamName=sname, ShardLevelMetrics=["IncomingBytes"])
    assert "IncomingBytes" not in resp2.get("DesiredShardLevelMetrics", [])
    kin.delete_stream(StreamName=sname)

def test_kinesis_split_shard(kin):
    import uuid as _uuid

    sname = f"intg-split-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=1)
    time.sleep(0.3)
    desc = kin.describe_stream(StreamName=sname)
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    start_hash = int(desc["StreamDescription"]["Shards"][0]["HashKeyRange"]["StartingHashKey"])
    end_hash = int(desc["StreamDescription"]["Shards"][0]["HashKeyRange"]["EndingHashKey"])
    mid = str((start_hash + end_hash) // 2)
    kin.split_shard(StreamName=sname, ShardToSplit=shard_id, NewStartingHashKey=mid)
    time.sleep(0.3)
    desc2 = kin.describe_stream(StreamName=sname)
    assert len(desc2["StreamDescription"]["Shards"]) == 2
    kin.delete_stream(StreamName=sname)

def test_kinesis_merge_shards(kin):
    import uuid as _uuid

    sname = f"intg-merge-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=2)
    time.sleep(0.3)
    desc = kin.describe_stream(StreamName=sname)
    shards = desc["StreamDescription"]["Shards"]
    assert len(shards) == 2
    # Sort by starting hash key to get adjacent shards
    shards_sorted = sorted(shards, key=lambda s: int(s["HashKeyRange"]["StartingHashKey"]))
    kin.merge_shards(
        StreamName=sname,
        ShardToMerge=shards_sorted[0]["ShardId"],
        AdjacentShardToMerge=shards_sorted[1]["ShardId"],
    )
    time.sleep(0.3)
    desc2 = kin.describe_stream(StreamName=sname)
    assert len(desc2["StreamDescription"]["Shards"]) == 1
    kin.delete_stream(StreamName=sname)

def test_kinesis_update_shard_count(kin):
    import uuid as _uuid

    sname = f"intg-usc-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=1)
    time.sleep(0.3)
    resp = kin.update_shard_count(StreamName=sname, TargetShardCount=2, ScalingType="UNIFORM_SCALING")
    assert resp["TargetShardCount"] == 2
    kin.delete_stream(StreamName=sname)

def test_kinesis_register_deregister_consumer(kin):
    import uuid as _uuid

    sname = f"intg-consumer-{_uuid.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=1)
    time.sleep(0.3)
    desc = kin.describe_stream(StreamName=sname)
    stream_arn = desc["StreamDescription"]["StreamARN"]
    resp = kin.register_stream_consumer(StreamARN=stream_arn, ConsumerName="my-consumer")
    assert resp["Consumer"]["ConsumerName"] == "my-consumer"
    assert resp["Consumer"]["ConsumerStatus"] == "ACTIVE"
    consumer_arn = resp["Consumer"]["ConsumerARN"]
    consumers = kin.list_stream_consumers(StreamARN=stream_arn)
    assert any(c["ConsumerName"] == "my-consumer" for c in consumers["Consumers"])
    desc_c = kin.describe_stream_consumer(ConsumerARN=consumer_arn)
    assert desc_c["ConsumerDescription"]["ConsumerName"] == "my-consumer"
    kin.deregister_stream_consumer(ConsumerARN=consumer_arn)
    consumers2 = kin.list_stream_consumers(StreamARN=stream_arn)
    assert not any(c["ConsumerName"] == "my-consumer" for c in consumers2["Consumers"])
    kin.delete_stream(StreamName=sname)


def _subscribe_setup(kin, shards=1):
    sname = f"intg-sub-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=sname, ShardCount=shards)
    stream_arn = kin.describe_stream(StreamName=sname)["StreamDescription"]["StreamARN"]
    consumer_arn = kin.register_stream_consumer(
        StreamARN=stream_arn, ConsumerName="efo")["Consumer"]["ConsumerARN"]
    return sname, consumer_arn


def test_kinesis_subscribe_to_shard_pushes_backlog_then_new_records(kin):
    sname, consumer_arn = _subscribe_setup(kin)
    first = [kin.put_record(StreamName=sname, Data=f"r{i}".encode(), PartitionKey="k")["SequenceNumber"]
             for i in range(2)]
    resp = kin.subscribe_to_shard(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
                                  StartingPosition={"Type": "TRIM_HORIZON"})
    events = iter(resp["EventStream"])
    event = next(events)["SubscribeToShardEvent"]
    assert [r["Data"] for r in event["Records"]] == [b"r0", b"r1"]
    assert [r["SequenceNumber"] for r in event["Records"]] == first
    assert event["ContinuationSequenceNumber"] == first[-1]
    assert event["MillisBehindLatest"] == 0

    third = kin.put_record(StreamName=sname, Data=b"r2", PartitionKey="k")["SequenceNumber"]
    event = next(events)["SubscribeToShardEvent"]
    assert [(r["Data"], r["PartitionKey"]) for r in event["Records"]] == [(b"r2", "k")]
    assert event["ContinuationSequenceNumber"] == third
    resp["EventStream"].close()

    resumed = kin.subscribe_to_shard(
        ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
        StartingPosition={"Type": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": first[0]})
    with pytest.raises(ClientError) as exc:  # refused within 5 s of the first call
        kin.subscribe_to_shard(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
                               StartingPosition={"Type": "LATEST"})
    assert exc.value.response["Error"]["Code"] == "ResourceInUseException"
    event = next(iter(resumed["EventStream"]))["SubscribeToShardEvent"]
    assert [r["SequenceNumber"] for r in event["Records"]] == [first[1], third]
    resumed["EventStream"].close()
    kin.delete_stream(StreamName=sname)


def test_kinesis_subscribe_to_shard_latest_starts_empty_and_takeover_ends_the_old_stream(kin):
    sname, consumer_arn = _subscribe_setup(kin)
    kin.put_record(StreamName=sname, Data=b"old", PartitionKey="k")
    old = kin.subscribe_to_shard(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
                                 StartingPosition={"Type": "LATEST"})
    old_events = iter(old["EventStream"])
    event = next(old_events)["SubscribeToShardEvent"]
    assert event["Records"] == [] and event["ContinuationSequenceNumber"]

    time.sleep(5.2)
    new = kin.subscribe_to_shard(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
                                 StartingPosition={"Type": "LATEST"})
    with pytest.raises(ClientError) as exc:
        for _ in old_events:
            pass
    assert exc.value.response["Error"]["Code"] == "ResourceInUseException"
    assert next(iter(new["EventStream"]))["SubscribeToShardEvent"]["Records"] == []
    new["EventStream"].close()
    kin.delete_stream(StreamName=sname)


def test_kinesis_subscribe_to_shard_ends_with_child_shards_after_a_split(kin):
    sname, consumer_arn = _subscribe_setup(kin)
    resp = kin.subscribe_to_shard(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
                                  StartingPosition={"Type": "TRIM_HORIZON"})
    events = iter(resp["EventStream"])
    assert "ChildShards" not in next(events)["SubscribeToShardEvent"]
    kin.split_shard(StreamName=sname, ShardToSplit="shardId-000000000000",
                    NewStartingHashKey=str(2**127))
    last = [e["SubscribeToShardEvent"] for e in events][-1]
    assert sorted(c["ShardId"] for c in last["ChildShards"]) == [
        "shardId-000000000001", "shardId-000000000002"]
    assert all(c["ParentShards"] == ["shardId-000000000000"] for c in last["ChildShards"])
    kin.delete_stream(StreamName=sname)


def test_kinesis_subscribe_to_shard_errors(kin):
    sname, consumer_arn = _subscribe_setup(kin)
    cases = [
        (dict(ConsumerARN=consumer_arn + "0", ShardId="shardId-000000000000",
              StartingPosition={"Type": "LATEST"}), "ResourceNotFoundException"),
        (dict(ConsumerARN=consumer_arn, ShardId="shardId-000000000009",
              StartingPosition={"Type": "LATEST"}), "ResourceNotFoundException"),
        (dict(ConsumerARN=consumer_arn, ShardId="shardId-000000000000",
              StartingPosition={"Type": "AT_SEQUENCE_NUMBER"}), "InvalidArgumentException"),
    ]
    for kwargs, code in cases:
        with pytest.raises(ClientError) as exc:
            kin.subscribe_to_shard(**kwargs)
        assert exc.value.response["Error"]["Code"] == code
    kin.delete_stream(StreamName=sname)


def test_kinesis_subscribe_to_shard_cbor_events(kin):
    import urllib.request

    import cbor2
    from botocore.eventstream import EventStreamBuffer

    sname, consumer_arn = _subscribe_setup(kin)
    kin.put_record(StreamName=sname, Data=b"\x00\x01", PartitionKey="k")
    request = urllib.request.Request(
        kin.meta.endpoint_url, method="POST",
        data=cbor2.dumps({"ConsumerARN": consumer_arn, "ShardId": "shardId-000000000000",
                          "StartingPosition": {"Type": "TRIM_HORIZON"}}),
        headers={"Content-Type": "application/x-amz-cbor-1.1",
                 "X-Amz-Target": "Kinesis_20131202.SubscribeToShard",
                 "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/kinesis/aws4_request"})
    buffer, messages = EventStreamBuffer(), []
    with urllib.request.urlopen(request, timeout=10) as response:
        while len(messages) < 2:
            buffer.add_data(response.read1(65536))
            messages.extend(buffer)
    assert messages[0].headers[":event-type"] == "initial-response"
    event = messages[1]
    assert event.headers[":content-type"] == "application/x-amz-cbor-1.1"
    record = cbor2.loads(event.payload)["Records"][0]
    assert record["Data"] == b"\x00\x01"
    kin.delete_stream(StreamName=sname)


def test_kinesis_consumer_arns_reject_foreign_region_without_exact_key_fallback(kin):
    """ConsumerARN paths must not resolve a consumer owned by another request region."""
    west = _regional_kin("us-west-2")
    stream_name = f"kin-consumer-arn-scope-{_uuid_mod.uuid4().hex[:8]}"
    consumer_name = "west-consumer"
    west.create_stream(StreamName=stream_name, ShardCount=1)
    west_arn = west.describe_stream(StreamName=stream_name)["StreamDescription"]["StreamARN"]
    consumer_arn = west.register_stream_consumer(
        StreamARN=west_arn,
        ConsumerName=consumer_name,
    )["Consumer"]["ConsumerARN"]

    with pytest.raises(ClientError) as exc:
        kin.list_stream_consumers(StreamARN=west_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    for kwargs in (
        {"ConsumerARN": consumer_arn},
        {"StreamARN": west_arn, "ConsumerName": consumer_name},
    ):
        with pytest.raises(ClientError) as exc:
            kin.describe_stream_consumer(**kwargs)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    with pytest.raises(ClientError) as exc:
        kin.deregister_stream_consumer(ConsumerARN=consumer_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    assert west.describe_stream_consumer(ConsumerARN=consumer_arn)["ConsumerDescription"]["ConsumerName"] == consumer_name


def test_kinesis_restore_legacy_account_scoped_state_uses_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import kinesis as _kin

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    stream_name = "LegacyKinesisStream"
    stream_arn = f"arn:aws:kinesis:us-west-2:{account_id}:stream/{stream_name}"
    consumer_arn = f"{stream_arn}/consumer/legacy-consumer:1700000000"

    legacy_streams = AccountScopedDict()
    legacy_streams._data[(account_id, stream_name)] = {
        "StreamName": stream_name,
        "StreamARN": stream_arn,
        "StreamStatus": "ACTIVE",
        "StreamModeDetails": {"StreamMode": "PROVISIONED"},
        "RetentionPeriodHours": 24,
        "shards": {},
        "tags": {},
        "CreationTimestamp": 1700000000,
        "EncryptionType": "NONE",
    }
    legacy_iterators = AccountScopedDict()
    legacy_iterators._data[(account_id, "legacy-iterator-with-arn")] = {
        "stream": stream_name,
        "stream_arn": stream_arn,
        "shard_id": "shardId-000000000000",
        "position": 0,
        "created_at": 1700000000,
    }
    legacy_iterators._data[(account_id, "legacy-iterator-with-stream-name")] = {
        "stream": stream_name,
        "shard_id": "shardId-000000000000",
        "position": 0,
        "created_at": 1700000000,
    }
    legacy_consumers = AccountScopedDict()
    legacy_consumers._data[(account_id, consumer_arn)] = {
        "ConsumerARN": consumer_arn,
        "ConsumerName": "legacy-consumer",
        "ConsumerStatus": "ACTIVE",
        "ConsumerCreationTimestamp": 1700000000,
        "StreamARN": stream_arn,
    }

    _kin.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")

        _kin.load_persisted_state({
            "streams": legacy_streams,
            "shard_iterators": legacy_iterators,
            "consumers": legacy_consumers,
        })

        assert _kin._streams.get_scoped(account_id, "us-east-1", stream_name) is None
        assert _kin._streams.get_scoped(account_id, "us-west-2", stream_name)["StreamARN"] == stream_arn
        assert _kin._shard_iterators.get_scoped(
            account_id, "us-west-2", "legacy-iterator-with-arn"
        )["stream_arn"] == stream_arn
        stream_name_iterator = _kin._shard_iterators.get_scoped(
            account_id, "us-west-2", "legacy-iterator-with-stream-name"
        )
        assert stream_name_iterator["stream"] == stream_name
        assert "stream_arn" not in stream_name_iterator
        assert _kin._consumers.get_scoped(account_id, "us-west-2", consumer_arn)["ConsumerARN"] == consumer_arn
    finally:
        _kin.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_kinesis_at_timestamp_iterator(kin):
    """AT_TIMESTAMP shard iterator returns records after the given timestamp."""
    kin.create_stream(StreamName="qa-kin-ts", ShardCount=1)
    time.sleep(0.1)
    before = time.time()
    kin.put_record(StreamName="qa-kin-ts", Data=b"after-ts", PartitionKey="pk")
    shards = kin.describe_stream(StreamName="qa-kin-ts")["StreamDescription"]["Shards"]
    shard_id = shards[0]["ShardId"]
    it = kin.get_shard_iterator(
        StreamName="qa-kin-ts",
        ShardId=shard_id,
        ShardIteratorType="AT_TIMESTAMP",
        Timestamp=before,
    )["ShardIterator"]
    records = kin.get_records(ShardIterator=it, Limit=10)["Records"]
    assert len(records) >= 1
    assert any(r["Data"] == b"after-ts" for r in records)

def test_kinesis_retention_period(kin):
    """IncreaseStreamRetentionPeriod / DecreaseStreamRetentionPeriod."""
    kin.create_stream(StreamName="qa-kin-retention", ShardCount=1)
    kin.increase_stream_retention_period(StreamName="qa-kin-retention", RetentionPeriodHours=48)
    desc = kin.describe_stream(StreamName="qa-kin-retention")["StreamDescription"]
    assert desc["RetentionPeriodHours"] == 48
    kin.decrease_stream_retention_period(StreamName="qa-kin-retention", RetentionPeriodHours=24)
    desc2 = kin.describe_stream(StreamName="qa-kin-retention")["StreamDescription"]
    assert desc2["RetentionPeriodHours"] == 24

def test_kinesis_stream_encryption_toggle(kin):
    """StartStreamEncryption / StopStreamEncryption."""
    kin.create_stream(StreamName="qa-kin-enc", ShardCount=1)
    kin.start_stream_encryption(
        StreamName="qa-kin-enc",
        EncryptionType="KMS",
        KeyId="alias/aws/kinesis",
    )
    desc = kin.describe_stream(StreamName="qa-kin-enc")["StreamDescription"]
    assert desc["EncryptionType"] == "KMS"
    kin.stop_stream_encryption(
        StreamName="qa-kin-enc",
        EncryptionType="KMS",
        KeyId="alias/aws/kinesis",
    )
    desc2 = kin.describe_stream(StreamName="qa-kin-enc")["StreamDescription"]
    assert desc2["EncryptionType"] == "NONE"

def test_kinesis_put_record_oversized(kin):
    kin.create_stream(StreamName="kin-limits", ShardCount=1)
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        kin.put_record(StreamName="kin-limits", Data=b"x" * (1024 * 1024 + 1), PartitionKey="pk")
    assert "1048576" in str(exc.value)

def test_kinesis_put_record_partition_key_too_long(kin):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        kin.put_record(StreamName="kin-limits", Data=b"ok", PartitionKey="k" * 257)
    assert "256" in str(exc.value)

def test_kinesis_put_records_batch_over_500(kin):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        kin.put_records(
            StreamName="kin-limits",
            Records=[{"Data": b"x", "PartitionKey": "pk"} for _ in range(501)],
        )
    assert "500" in str(exc.value)

def test_kinesis_put_records_total_payload_over_5mb(kin):
    from botocore.exceptions import ClientError
    # 6 records of ~1MB each = ~6MB > 5MB limit
    with pytest.raises(ClientError) as exc:
        kin.put_records(
            StreamName="kin-limits",
            Records=[{"Data": b"x" * (1024 * 1024), "PartitionKey": "pk"} for _ in range(6)],
        )
    assert "5 MB" in str(exc.value)

def test_kinesis_esm_creates_and_lists(lam, kin):
    """Kinesis ESM can be created and listed."""
    kin.create_stream(StreamName="esm-kin-stream", ShardCount=1)
    stream = kin.describe_stream(StreamName="esm-kin-stream")["StreamDescription"]
    stream_arn = stream["StreamARN"]

    code = "def handler(event, context): return len(event.get('Records', []))"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="esm-kin-fn", Runtime="python3.11",
        Role=_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    esm = lam.create_event_source_mapping(
        FunctionName="esm-kin-fn",
        EventSourceArn=stream_arn,
        StartingPosition="TRIM_HORIZON",
        BatchSize=10,
    )
    assert esm["EventSourceArn"] == stream_arn
    assert esm["FunctionArn"].endswith("esm-kin-fn")

    esms = lam.list_event_source_mappings(FunctionName="esm-kin-fn")["EventSourceMappings"]
    assert any(e["UUID"] == esm["UUID"] for e in esms)

    lam.delete_event_source_mapping(UUID=esm["UUID"])
    lam.delete_function(FunctionName="esm-kin-fn")


def test_kinesis_iterator_reuse_on_retry(kin):
    """Same shard iterator can be used multiple times (client retry), matching AWS behavior."""
    kin.create_stream(StreamName="kin-iter-retry", ShardCount=1)
    kin.put_record(StreamName="kin-iter-retry", Data=b"rec1", PartitionKey="pk1")
    kin.put_record(StreamName="kin-iter-retry", Data=b"rec2", PartitionKey="pk2")

    desc = kin.describe_stream(StreamName="kin-iter-retry")
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(
        StreamName="kin-iter-retry", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    # First call with iterator
    resp1 = kin.get_records(ShardIterator=it)
    assert len(resp1["Records"]) == 2

    # Retry with the same iterator — should succeed and return identical data
    resp2 = kin.get_records(ShardIterator=it)
    assert len(resp2["Records"]) == 2
    assert resp2["Records"][0]["Data"] == resp1["Records"][0]["Data"]

    # NextShardIterator from first call should advance past existing records
    resp3 = kin.get_records(ShardIterator=resp1["NextShardIterator"])
    assert len(resp3["Records"]) == 0


def test_kinesis_cbor_put_record(kin):
    """Java SDK sends CBOR-encoded PutRecord; ministack must decode it."""
    import urllib.request

    import cbor2

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    kin.create_stream(StreamName="cbor-test-stream", ShardCount=1)

    # Build a CBOR-encoded PutRecord payload (same as AWS Java SDK v2 sends)
    cbor_body = cbor2.dumps({
        "StreamName": "cbor-test-stream",
        "Data": b'{ "test": "123"}',
        "PartitionKey": "1",
    })

    req = urllib.request.Request(
        endpoint,
        data=cbor_body,
        headers={
            "Content-Type": "application/x-amz-cbor-1.1",
            "X-Amz-Target": "Kinesis_20131202.PutRecord",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        resp_body = cbor2.loads(resp.read())
        assert "ShardId" in resp_body
        assert "SequenceNumber" in resp_body

    # Verify the record is retrievable via normal JSON path
    desc = kin.describe_stream(StreamName="cbor-test-stream")
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(
        StreamName="cbor-test-stream", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )
    records = kin.get_records(ShardIterator=it["ShardIterator"])
    assert len(records["Records"]) == 1


def test_kinesis_describe_echoes_the_encryption_key_id(kin):
    """StartStreamEncryption stores KeyId; the describe paths must return it.

    AWS reports KeyId alongside EncryptionType in both StreamDescription and
    StreamDescriptionSummary. Storing it without echoing it means the Terraform
    provider reads kms_key_id as empty on a stream that is in fact encrypted, so
    every plan proposes setting it again.
    """
    name = "qa-kinesis-keyid"
    kin.create_stream(StreamName=name, ShardCount=1)
    key = "arn:aws:kms:us-east-1:000000000000:alias/aws/kinesis"
    kin.start_stream_encryption(
        StreamName=name, EncryptionType="KMS", KeyId=key
    )

    desc = kin.describe_stream(StreamName=name)["StreamDescription"]
    assert desc["EncryptionType"] == "KMS"
    assert desc.get("KeyId") == key

    summary = kin.describe_stream_summary(StreamName=name)[
        "StreamDescriptionSummary"
    ]
    assert summary["EncryptionType"] == "KMS"
    assert summary.get("KeyId") == key

    # An unencrypted stream must not report a KeyId at all — AWS omits the field
    # rather than returning an empty one, and an empty string would read as drift.
    kin.stop_stream_encryption(
        StreamName=name, EncryptionType="KMS", KeyId=key
    )
    desc = kin.describe_stream(StreamName=name)["StreamDescription"]
    assert desc["EncryptionType"] == "NONE"
    assert "KeyId" not in desc


# --- SubscribeToShard across retention pruning (in-process dispatch) ---

_SUB_STARTS = ("TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP")
_SUB_SHARD = "shardId-000000000000"


@pytest.fixture
def subscription_state(monkeypatch):
    clock = [1_700_000_000.0]
    monkeypatch.setattr(kinesis, "time", SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(kinesis, "_streams", AccountRegionScopedDict())
    monkeypatch.setattr(kinesis, "_consumers", AccountRegionScopedDict())
    monkeypatch.setattr(kinesis, "_subscriptions", {})
    monkeypatch.setattr(kinesis, "_SUBSCRIPTION_IDLE_SECONDS", 0)
    return clock


async def _sub_call(action, data, is_cbor=False):
    encode = cbor2.dumps if is_cbor else lambda value: json.dumps(value).encode()
    headers = {"x-amz-target": f"Kinesis_20131202.{action}"}
    if is_cbor:
        headers["content-type"] = "application/x-amz-cbor-1.1"
    result = await kinesis.handle_request("POST", "/", headers, encode(data), {})
    assert result[0] == 200, result
    if action == "SubscribeToShard":
        return result[2]
    return cbor2.loads(result[2]) if is_cbor else json.loads(result[2])


async def _sub_setup():
    await _sub_call("CreateStream", {"StreamName": "retention", "ShardCount": 1})
    arn = (await _sub_call("DescribeStream", {"StreamName": "retention"}))["StreamDescription"]["StreamARN"]
    consumer = await _sub_call("RegisterStreamConsumer", {"StreamARN": arn, "ConsumerName": "reader"})
    return consumer["Consumer"]["ConsumerARN"]


async def _sub_put(value):
    return (await _sub_call("PutRecord", {
        "StreamName": "retention", "PartitionKey": "p", "Data": base64.b64encode(value).decode(),
    }))["SequenceNumber"]


def _sub_starting(kind, sequence, timestamp):
    result = {"Type": kind}
    if "SEQUENCE_NUMBER" in kind:
        result["SequenceNumber"] = sequence
    if kind == "AT_TIMESTAMP":
        result["Timestamp"] = timestamp
    return result


async def _sub_run(response, is_cbor, on_event):
    disconnect = asyncio.Event()
    events = []

    async def receive():
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        if not message.get("body"):
            return
        buffer = EventStreamBuffer()
        buffer.add_data(message["body"])
        for frame in buffer:
            assert frame.headers[":message-type"] == "event", frame.headers
            assert frame.headers[":content-type"] == (
                "application/x-amz-cbor-1.1" if is_cbor else "application/json")
            if frame.headers[":event-type"] == "initial-response":
                continue
            assert frame.headers[":event-type"] == "SubscribeToShardEvent"
            event = cbor2.loads(frame.payload) if is_cbor else json.loads(frame.payload)
            events.append(event)
            if await on_event(event, len(events)):
                disconnect.set()

    await asyncio.wait_for(response.runner(send, receive), timeout=5)
    return events


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", _SUB_STARTS)
def test_subscription_delivers_after_repeated_retention_and_idle_polls(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _sub_setup()
            old = await _sub_put(b"old")
            timestamp = clock[0]
            clock[0] += 3
            retained = await _sub_put(b"retained")
            clock[0] += 24 * 3600 - 4
            response = await _sub_call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": _SUB_SHARD,
                "StartingPosition": _sub_starting(kind, old, timestamp),
            }, is_cbor)
            expected = []

            async def on_event(event, number):
                # The two backlog records expire separately within the normal
                # five-minute subscription lifetime. Idle and append after
                # each pruning, while keeping newly delivered records alive.
                if number == 1:
                    clock[0] += 2
                    kinesis._expire_records(kinesis._streams["retention"])
                elif number == 4:
                    clock[0] += 3
                    kinesis._expire_records(kinesis._streams["retention"])
                elif number in (2, 5):
                    expected.append(await _sub_put(f"new-{number}".encode()))
                return number == 7

            events = await _sub_run(response, is_cbor, on_event)
            initial = ([] if kind == "LATEST" else [retained] if kind == "AFTER_SEQUENCE_NUMBER"
                       else [old, retained])
            assert [r["SequenceNumber"] for r in events[0]["Records"]] == initial
            assert [r["SequenceNumber"] for e in events[1:] for r in e["Records"]] == expected
            assert events[1]["Records"] == events[4]["Records"] == events[6]["Records"] == []
            assert events[1]["ContinuationSequenceNumber"] == events[0]["ContinuationSequenceNumber"]
            assert events[2]["ContinuationSequenceNumber"] == expected[0]
            assert events[3]["ContinuationSequenceNumber"] == expected[0]
            assert events[4]["ContinuationSequenceNumber"] == expected[0]
            assert events[5]["ContinuationSequenceNumber"] == expected[1]
            assert events[6]["ContinuationSequenceNumber"] == expected[1]
            assert all(e["MillisBehindLatest"] == 0 and "ChildShards" not in e for e in events)
            records = events[2]["Records"] + events[5]["Records"]
            assert [r["Data"] if is_cbor else base64.b64decode(r["Data"]) for r in records] == [
                b"new-2", b"new-5"]
            assert not kinesis._subscriptions

    asyncio.run(exercise())


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", _SUB_STARTS)
def test_subscription_keeps_checkpoint_when_all_records_expire_before_runner(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _sub_setup()
            old = await _sub_put(b"old")
            timestamp = clock[0]
            shard_start = kinesis._streams["retention"]["shards"][_SUB_SHARD]["starting_sequence_number"]
            clock[0] += 24 * 3600 - 1
            response = await _sub_call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": _SUB_SHARD,
                "StartingPosition": _sub_starting(kind, old, timestamp),
            }, is_cbor)
            clock[0] += 2
            kinesis._expire_records(kinesis._streams["retention"])

            async def on_event(event, number):
                return True

            events = await _sub_run(response, is_cbor, on_event)
            assert events[0]["Records"] == []
            # Preserve the existing selected checkpoint value; do not invent
            # an AWS empty-shard checkpoint number.
            checkpoint = old if kind in ("LATEST", "AFTER_SEQUENCE_NUMBER") else shard_start
            assert events[0]["ContinuationSequenceNumber"] == checkpoint

    asyncio.run(exercise())


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", _SUB_STARTS)
def test_subscription_captures_selection_before_runner_and_pruning(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _sub_setup()
            await _sub_put(b"expired-prefix")
            clock[0] += 1
            selected = await _sub_put(b"selected")
            timestamp = clock[0]
            clock[0] += 24 * 3600 - 1
            response = await _sub_call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": _SUB_SHARD,
                "StartingPosition": _sub_starting(kind, selected, timestamp),
            }, is_cbor)
            # Only the prefix expires. Append between response creation and
            # runner startup, so LATEST must retain the establishment boundary.
            clock[0] += 1
            appended = await _sub_put(b"appended")

            async def on_event(event, number):
                return True

            # Run with another request's context to exercise the saved tenant.
            with request_scope("111111111111", "eu-west-1"):
                events = await _sub_run(response, is_cbor, on_event)
            expected = [appended] if kind in ("LATEST", "AFTER_SEQUENCE_NUMBER") else [selected, appended]
            assert [r["SequenceNumber"] for r in events[0]["Records"]] == expected
            assert events[0]["ContinuationSequenceNumber"] == appended

    asyncio.run(exercise())


def _kin_error(exc):
    err = exc.value.response["Error"]
    return exc.value.response["ResponseMetadata"]["HTTPStatusCode"], err["Code"], err["Message"]


def _unused_account():
    return "9" + str(_uuid_mod.uuid4().int)[:11]


def test_kinesis_resource_policy_follows_aws(kin):
    """Captured on AWS (us-east-1, 2026-10-26): codes, messages and check order of the resource-policy calls."""
    name = f"rp-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=name, ShardCount=1)
    arn = kin.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
    account = arn.split(":")[4]
    consumer = kin.register_stream_consumer(StreamARN=arn, ConsumerName="c")["Consumer"]["ConsumerARN"]

    def policy(resource, actions):
        return json.dumps({"Version": "2012-10-17", "Statement": [{"Sid": "x", "Effect": "Allow",
                           "Principal": {"AWS": f"arn:aws:iam::{account}:root"}, "Action": actions, "Resource": resource}]})

    try:
        assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"
        with pytest.raises(ClientError) as exc:
            kin.delete_resource_policy(ResourceARN=arn)
        assert _kin_error(exc) == (400, "ResourceNotFoundException", f"No resource policy found for resource ARN {arn}.")
        for document, message in (
            ("not json", "Policy validation error: This policy contains invalid Json"),
            ("{}", "Policy validation error: Missing required field Version"),
            ('{"Version":"2012-10-17"}', "Policy validation error: Missing required field Statement"),
            (policy(consumer, ["kinesis:GetRecords"]), "Policy validation error: Resource policy contains invalid statement"
             " at line 1, column 41: The resource policy's resource must be the same as the resource ARN."),
        ):
            with pytest.raises(ClientError) as exc:
                kin.put_resource_policy(ResourceARN=arn, Policy=document)
            assert _kin_error(exc) == (400, "InvalidArgumentException", message)
        with pytest.raises(ClientError) as exc:
            kin.put_resource_policy(ResourceARN=consumer, Policy=policy(consumer, ["kinesis:GetRecords"]))
        assert _kin_error(exc) == (400, "InvalidArgumentException", "Policy validation error: Resource policy contains invalid"
                                   ' statement at line 1, column 41: The following action names are invalid: "kinesis:GetRecords"')

        stream_policy = policy(arn, ["kinesis:GetRecords", "kinesis:GetShardIterator"])
        kin.put_resource_policy(ResourceARN=arn, Policy=stream_policy)
        # AWS stores the document compact (separators without spaces).
        assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == json.dumps(
            json.loads(stream_policy), separators=(",", ":"))
        kin.put_resource_policy(ResourceARN=consumer, Policy=policy(consumer, ["kinesis:SubscribeToShard"]))
        kin.delete_resource_policy(ResourceARN=arn)
        assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"

        missing = arn.rsplit("/", 1)[0] + "/no-such-stream"
        with pytest.raises(ClientError) as exc:
            kin.get_resource_policy(ResourceARN=missing)
        assert _kin_error(exc) == (400, "ResourceNotFoundException", f"Stream no-such-stream under account {account} not found.")
        with pytest.raises(ClientError) as exc:
            # An account no request has used does not exist.
            kin.get_resource_policy(ResourceARN=arn.replace(f":{account}:", f":{_unused_account()}:"))
        assert _kin_error(exc) == (400, "UnrecognizedClientException", "No account found for the given parameters")
        west = arn.replace(":us-east-1:", ":us-west-2:")
        with pytest.raises(ClientError) as exc:
            kin.get_resource_policy(ResourceARN=west)
        assert _kin_error(exc) == (400, "InvalidArgumentException",
                                   f"The region specified in the ARN '{west}' does not match the endpoint region")
    finally:
        kin.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


def test_kinesis_resource_policy_goes_with_its_stream(kin):
    name = f"rp-del-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=name, ShardCount=1)
    arn = kin.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
    account = arn.split(":")[4]
    kin.put_resource_policy(ResourceARN=arn, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{account}:root"}, "Action": "kinesis:GetRecords",
        "Resource": arn}]}))
    kin.delete_stream(StreamName=name)
    kin.create_stream(StreamName=name, ShardCount=1)
    try:
        assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"
    finally:
        kin.delete_stream(StreamName=name)
_FILTER_HANDLER = """
import base64

def handler(event, context):
    for r in event["Records"]:
        print("XREC " + base64.b64decode(r["kinesis"]["data"]).decode())
    return len(event["Records"])
"""


@pytest.mark.parametrize("pattern, expected", [
    ({"data": {"kind": ["keep"]}}, ['{"kind": "keep"}', '{"kind": "keep", "n": 2}']),
    ({"data": {"n": [{"numeric": [">", 1]}]}}, ['{"kind": "keep", "n": 2}']),
    ({"partitionKey": ["pk-b"]}, ['{"kind": "drop"}', "not json"]),
    ({"partitionKey": [{"prefix": "pk-"}], "data": {"kind": [{"exists": True}]}},
     ['{"kind": "keep"}', '{"kind": "drop"}', '{"kind": "keep", "n": 2}']),
], ids=["data-field", "data-numeric", "metadata-only-matches-non-json", "metadata-and-data"])
def test_kinesis_esm_filter_criteria_match_decoded_data_and_metadata(lam, kin, logs, pattern, expected):
    """Black box: the function only receives the records the pattern selects."""
    stream = f"kin-filter-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=stream, ShardCount=1)
    arn = kin.describe_stream_summary(StreamName=stream)["StreamDescriptionSummary"]["StreamARN"]
    for data, pk in (('{"kind": "keep"}', "pk-a"), ('{"kind": "drop"}', "pk-b"),
                     ("not json", "pk-b"), ('{"kind": "keep", "n": 2}', "pk-a")):
        kin.put_record(StreamName=stream, Data=data.encode(), PartitionKey=pk)

    fn = f"kin-filter-fn-{_uuid_mod.uuid4().hex[:8]}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", _FILTER_HANDLER)
    lam.create_function(FunctionName=fn, Runtime="python3.12", Role=_LAMBDA_ROLE,
                        Handler="index.handler", Code={"ZipFile": buf.getvalue()})
    esm = lam.create_event_source_mapping(
        FunctionName=fn, EventSourceArn=arn, StartingPosition="TRIM_HORIZON",
        FilterCriteria={"Filters": [{"Pattern": json.dumps(pattern)}]})["UUID"]
    try:
        received = []
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                events = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}")["events"]
            except ClientError:
                events = []
            received = [line[len("XREC "):] for e in events for line in e["message"].splitlines()
                        if line.startswith("XREC ")]
            if len(received) >= len(expected):
                break
            time.sleep(0.5)
        time.sleep(2)  # nothing that should have been filtered turns up later
        events = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}")["events"]
        received = [line[len("XREC "):] for e in events for line in e["message"].splitlines()
                    if line.startswith("XREC ")]
        assert received == expected
    finally:
        lam.delete_event_source_mapping(UUID=esm)
        lam.delete_function(FunctionName=fn)
        kin.delete_stream(StreamName=stream)


def _account_client(service, account):
    return boto3.client(service, endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
                        aws_access_key_id=account, aws_secret_access_key="test", region_name="us-east-1")


def test_kinesis_esm_reads_a_stream_in_another_account(lam, logs, auth_enabled):
    """A function in one account maps a stream in another (same region); records reach it. Under AUTH=true
    the stream's resource policy must allow the function's execution role."""
    if auth_enabled:
        pytest.skip("account selection by access key needs AUTH unset")
    owner = _account_client("kinesis", "111111111111")
    stream = f"xacct-{_uuid_mod.uuid4().hex[:8]}"
    owner.create_stream(StreamName=stream, ShardCount=1)
    arn = owner.describe_stream_summary(StreamName=stream)["StreamDescriptionSummary"]["StreamARN"]
    for n in range(3):
        owner.put_record(StreamName=stream, Data=f"rec-{n}".encode(), PartitionKey="pk")

    fn = f"xacct-fn-{_uuid_mod.uuid4().hex[:8]}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", _FILTER_HANDLER)
    lam.create_function(FunctionName=fn, Runtime="python3.12", Role=_LAMBDA_ROLE,
                        Handler="index.handler", Code={"ZipFile": buf.getvalue()})
    try:
        missing = arn.rsplit("/", 1)[0] + "/no-such-stream"
        with pytest.raises(ClientError) as exc:
            lam.create_event_source_mapping(FunctionName=fn, EventSourceArn=missing, StartingPosition="LATEST")
        assert exc.value.response["Error"]["Message"] == (
            "Received Exception while reading from provided stream. "
            "Stream no-such-stream under account 111111111111 not found.")
        esm = lam.create_event_source_mapping(FunctionName=fn, EventSourceArn=arn,
                                              StartingPosition="TRIM_HORIZON")["UUID"]
        received, deadline = [], time.time() + 30
        while time.time() < deadline and len(received) < 3:
            try:
                events = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}")["events"]
            except ClientError:
                events = []
            received = [line[len("XREC "):] for e in events for line in e["message"].splitlines()
                        if line.startswith("XREC ")]
            time.sleep(0.5)
        assert received == ["rec-0", "rec-1", "rec-2"]
        lam.delete_event_source_mapping(UUID=esm)
    finally:
        lam.delete_function(FunctionName=fn)
        owner.delete_stream(StreamName=stream)


def test_kinesis_mapping_read_needs_every_read_in_the_stream_policy():
    """The stream policy must allow the function's role session every read a mapping makes."""
    from ministack.core.responses import request_scope
    from ministack.services import kinesis as kmod

    owner, consumer, name = "111111111111", "222222222222", f"pol-{_uuid_mod.uuid4().hex[:8]}"
    arn = f"arn:aws:kinesis:us-east-1:{owner}:stream/{name}"
    role = f"arn:aws:iam::{consumer}:role/reader"
    with request_scope(owner, "us-east-1"):
        kmod._create_stream({"StreamName": name, "ShardCount": 1})
    try:
        assert kmod.mapping_read_denied(arn, role, "fn", consumer)[1] == "kinesis:DescribeStream"
        reads = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
                 "kinesis:GetShardIterator", "kinesis:ListShards"]

        def grant(actions):
            with request_scope(owner, "us-east-1"):
                kmod._resource_policies[arn] = json.dumps({"Version": "2012-10-17", "Statement": [{
                    "Effect": "Allow", "Principal": {"AWS": role}, "Action": actions, "Resource": arn}]})

        grant(reads[:-1])
        assert kmod.mapping_read_denied(arn, role, "fn", consumer)[1] == "kinesis:ListShards"
        grant(reads)
        assert kmod.mapping_read_denied(arn, role, "fn", consumer) is None
        assert kmod.mapping_read_denied(arn, f"arn:aws:iam::{consumer}:role/other", "fn", consumer) is not None
    finally:
        with request_scope(owner, "us-east-1"):
            kmod._resource_policies.pop(arn, None)
            kmod._streams.pop(name, None)


# ---------------------------------------------------------------------------
# Resource policies as AWS validates and stores them (recorded in us-east-1)
# ---------------------------------------------------------------------------

_CONSUMER = "222222222222"


def _read_policy(stream_arn, principal, actions=("kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                                                 "kinesis:GetRecords", "kinesis:GetShardIterator",
                                                 "kinesis:ListShards")):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": principal},
                       "Action": list(actions) if isinstance(actions, tuple) else actions, "Resource": stream_arn}],
    })


def _use_account(account_id):
    """MiniStack knows an account once it holds an IAM identity or a stream; give it a role."""
    _consumer_role(account=account_id)


def _consumer_role(name="consumer-role", account=_CONSUMER):
    iam = _account_client("iam", account)
    try:
        iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                           "Action": "sts:AssumeRole"}],
        }))
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    return f"arn:aws:iam::{account}:role/{name}"


def _new_stream(kin, prefix):
    name = f"{prefix}-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=name, ShardCount=1)
    return name, kin.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]


def test_kinesis_resource_policy_put_get_delete(kin):
    name, arn = _new_stream(kin, "kin-rp")
    role = _consumer_role()
    # Sent pretty, with one-element lists and a bare account id: AWS stores it
    # compact, in its own key order, with lists of one collapsed and the
    # account id as its root ARN, principals sorted as written.
    policy = json.dumps({
        "Statement": [{"Resource": [arn], "Action": ["kinesis:GetRecords"],
                       "Principal": {"AWS": [_CONSUMER, role]}, "Effect": "Allow", "Sid": "Read"}],
        "Version": "2012-10-17",
    }, indent=2)

    assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"
    kin.put_resource_policy(ResourceARN=arn, Policy=policy)
    assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == (
        '{"Version":"2012-10-17","Statement":[{"Sid":"Read","Effect":"Allow",'
        f'"Principal":{{"AWS":["arn:aws:iam::{_CONSUMER}:root","{role}"]}},'
        f'"Action":"kinesis:GetRecords","Resource":"{arn}"}}]}}'
    )

    kin.delete_resource_policy(ResourceARN=arn)
    assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"
    with pytest.raises(ClientError) as exc:
        kin.delete_resource_policy(ResourceARN=arn)
    assert exc.value.response["Error"] == {
        "Code": "ResourceNotFoundException",
        "Message": f"No resource policy found for resource ARN {arn}.",
    }


# Recorded against Kinesis (us-east-1): what PutResourcePolicy answers for each
# document, given the stream ARN. "{arn}" is the stream's ARN.
_POLICY_CASES = {
    "not json": ("not json", "Policy validation error: This policy contains invalid Json"),
    "array": ("[]", "Policy validation error: This policy contains invalid Json"),
    "empty": ("", "Policy validation error: Resource policy size should be greater than 0 and less than 20000 bytes"),
    "too big": ("x" * 20001,
                "Policy validation error: Resource policy size should be greater than 0 and less than 20000 bytes"),
    "no version": ('{"Statement": []}', "Policy validation error: Missing required field Version"),
    "bad version": ('{"Version": "bogus", "Statement": []}',
                    "Policy validation error: The policy must contain a valid version string"),
    "no statement": ('{"Version": "2012-10-17"}', "Policy validation error: Missing required field Statement"),
    "empty statement": ('{"Version": "2012-10-17", "Statement": []}',
                        "Policy validation error: Could not parse the policy: Statement is empty!"),
    "unknown field": ('{"Version": "2012-10-17", "Statement": [], "Foo": 1}',
                      "Policy validation error: Unknown field Foo"),
    "not-action": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "%(acct)s"}, '
                   '"NotAction": "kinesis:PutRecord", "Resource": "{arn}"}]}',
                   "Policy validation error: Has prohibited field NotAction"),
    "no effect": ('{"Version": "2012-10-17", "Statement": [{"Principal": {"AWS": "%(acct)s"}, '
                  '"Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                  "Policy validation error: Missing required field Effect"),
    "sid not a string": ('{"Version": "2012-10-17", "Statement": [{"Sid": 5, "Effect": "Allow", '
                         '"Principal": {"AWS": "%(acct)s"}, "Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                         "Policy validation error: Syntax error at position (1,50)"),
    "invalid sid": ('{"Version": "2012-10-17", "Statement": [{"Sid": "a b", "Effect": "Allow", '
                    '"Principal": {"AWS": "%(acct)s"}, "Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                    "Policy validation error: Resource policy contains invalid statement at line 1, column 41: "
                    "The following Statement Ids are invalid: a b, "),
    "invalid action": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "%(acct)s"}, '
                       '"Action": ["kinesis:CreateStream", "kinesis:Nope"], "Resource": "{arn}"}]}',
                       "Policy validation error: Resource policy contains invalid statement at line 1, column 41: "
                       'The following action names are invalid: "kinesis:CreateStream", "kinesis:Nope"'),
    "wildcard action": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "%(acct)s"}, '
                        '"Action": "kinesis:*", "Resource": "{arn}"}]}',
                        "Policy validation error: Resource policy contains invalid statement at line 1, column 41: "
                        "The resource policy cannot contain invalid actions: [kinesis:*]."),
    "wildcard principal": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", '
                           '"Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                           "Policy validation error: Resource policy contains invalid statement at line 1, "
                           "column 41: The resource policy cannot contain the wildcard principal."),
    "service principal": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
                          '"Principal": {"Service": "lambda.amazonaws.com"}, "Action": "kinesis:GetRecords", '
                          '"Resource": "{arn}"}]}',
                          "Policy validation error: Resource policy contains invalid statement at line 1, "
                          "column 41: The resource policy cannot contain Service principals."),
    "other resource": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "%(acct)s"}, '
                       '"Action": "kinesis:GetRecords", "Resource": "*"}]}',
                       "Policy validation error: Resource policy contains invalid statement at line 1, column 41: "
                       "The resource policy's resource must be the same as the resource ARN."),
    "second statement, pretty": ('{\n  "Version": "2012-10-17",\n  "Statement": [\n    {"Sid": "a", "Effect": "Allow", '
                                 '"Principal": {"AWS": "%(acct)s"}, "Action": "kinesis:GetRecords", '
                                 '"Resource": "{arn}"},\n    {"Sid": "a", "Effect": "Allow", '
                                 '"Principal": {"AWS": "%(acct)s"}, "Action": "kinesis:GetRecords", '
                                 '"Resource": "{arn}"}\n  ]\n}',
                                 "Policy validation error: Resource policy contains invalid statement at line 4, "
                                 "column {col}: The Statement Ids in the policy are not unique"),
    "unknown account principal": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
                                  '"Principal": {"AWS": "arn:aws:iam::%(unused)s:root"}, '
                                  '"Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                                  "Invalid principal in policy document."),
    "missing role principal": ('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
                               '"Principal": {"AWS": "arn:aws:iam::%(acct)s:role/no-such-role"}, '
                               '"Action": "kinesis:GetRecords", "Resource": "{arn}"}]}',
                               "Invalid principal in policy document."),
}


@pytest.mark.parametrize("case", list(_POLICY_CASES))
def test_kinesis_resource_policy_validation_matches_aws(kin, case):
    _name, arn = _new_stream(kin, "kin-rp-val")
    _use_account(_CONSUMER)
    unused = _unused_account()
    document, message = _POLICY_CASES[case]
    document = (document % {"acct": _CONSUMER, "unused": unused} if "%(" in document else document)
    document = document.replace("{arn}", arn)
    if "{col}" in message:
        # Reported just past the token before the offending statement: the
        # first statement's closing brace, on line 4.
        message = message.replace("{col}", str(document.splitlines()[3].rindex("},") + 2))
    with pytest.raises(ClientError) as exc:
        kin.put_resource_policy(ResourceARN=arn, Policy=document)
    assert exc.value.response["Error"] == {"Code": "InvalidArgumentException", "Message": message}
    assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"


def test_kinesis_resource_policy_resource_errors(kin):
    name, arn = _new_stream(kin, "kin-rp-err")
    _use_account(_CONSUMER)
    missing = arn.rsplit("/", 1)[0] + "/no-such-stream"
    other_region = arn.replace(":us-east-1:", ":us-west-2:")
    unused = arn.replace(f":{arn.split(':')[4]}:", f":{_unused_account()}:")

    def error(call):
        with pytest.raises(ClientError) as exc:
            call()
        return exc.value.response["Error"]

    # The policy is checked before the stream: a policy for a missing stream
    # naming the stream it was sent for is refused for its resource.
    assert error(lambda: kin.get_resource_policy(ResourceARN=missing)) == {
        "Code": "ResourceNotFoundException",
        "Message": f"Stream no-such-stream under account {arn.split(':')[4]} not found."}
    assert error(lambda: kin.put_resource_policy(ResourceARN=missing, Policy=_read_policy(missing, _CONSUMER))) == {
        "Code": "ResourceNotFoundException",
        "Message": f"Stream no-such-stream under account {arn.split(':')[4]} not found."}
    assert error(lambda: kin.put_resource_policy(ResourceARN=missing, Policy=_read_policy(arn, _CONSUMER)))[
        "Message"].endswith("The resource policy's resource must be the same as the resource ARN.")
    assert error(lambda: kin.get_resource_policy(ResourceARN=other_region)) == {
        "Code": "InvalidArgumentException",
        "Message": f"The region specified in the ARN '{other_region}' does not match the endpoint region"}
    assert error(lambda: kin.delete_resource_policy(ResourceARN=unused)) == {
        "Code": "UnrecognizedClientException", "Message": "No account found for the given parameters"}

    # Only the owner's account manages the policy.
    other = _account_client("kinesis", _CONSUMER)
    assert error(lambda: other.put_resource_policy(ResourceARN=arn, Policy=_read_policy(arn, _CONSUMER))) == {
        "Code": "AccessDeniedException",
        "Message": f"User: arn:aws:iam::{_CONSUMER}:root is not authorized to perform: kinesis:PutResourcePolicy "
                   f"on resource: {arn} because no resource-based policy allows the kinesis:PutResourcePolicy action"}


def test_kinesis_resource_policy_follows_stream_and_consumer_lifecycle(kin):
    _use_account(_CONSUMER)
    name, arn = _new_stream(kin, "kin-rp-life")
    consumer_arn = kin.register_stream_consumer(StreamARN=arn, ConsumerName="efo")["Consumer"]["ConsumerARN"]
    consumer_policy = _read_policy(consumer_arn, _CONSUMER, ["kinesis:SubscribeToShard"])

    with pytest.raises(ClientError) as exc:  # a stream action on a consumer
        kin.put_resource_policy(ResourceARN=consumer_arn, Policy=_read_policy(consumer_arn, _CONSUMER))
    assert 'The following action names are invalid: "kinesis:DescribeStream"' in exc.value.response["Error"]["Message"]
    kin.put_resource_policy(ResourceARN=consumer_arn, Policy=consumer_policy)
    assert json.loads(kin.get_resource_policy(ResourceARN=consumer_arn)["Policy"])["Statement"][0]["Action"] == (
        "kinesis:SubscribeToShard")
    kin.deregister_stream_consumer(ConsumerARN=consumer_arn)
    consumer_arn = kin.register_stream_consumer(StreamARN=arn, ConsumerName="efo")["Consumer"]["ConsumerARN"]
    assert kin.get_resource_policy(ResourceARN=consumer_arn)["Policy"] == "{}"

    kin.put_resource_policy(ResourceARN=arn, Policy=_read_policy(arn, _CONSUMER))
    kin.delete_stream(StreamName=name)
    kin.create_stream(StreamName=name, ShardCount=1)
    assert kin.get_resource_policy(ResourceARN=arn)["Policy"] == "{}"


@pytest.mark.auth_contract
def test_kinesis_stream_policy_grants_a_named_user_contract(auth_enabled, iam_user, kin):
    """In the owner's account, a stream policy naming a user allows that user
    even without an identity policy."""
    name = f"kin-rp-auth-{_uuid_mod.uuid4().hex[:8]}"
    kin.create_stream(StreamName=name, ShardCount=1)
    arn = kin.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
    user = iam_user()
    client = user.client("kinesis")
    try:
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                client.describe_stream_summary(StreamARN=arn)
            assert exc.value.response["Error"] == {
                "Code": "AccessDeniedException",
                "Message": f"User: {user.arn} is not authorized to perform: kinesis:DescribeStreamSummary on resource: "
                           f"{arn} because no identity-based policy allows the kinesis:DescribeStreamSummary action"}
        else:
            client.describe_stream_summary(StreamARN=arn)

        kin.put_resource_policy(ResourceARN=arn, Policy=_read_policy(
            arn, user.arn, "kinesis:DescribeStreamSummary"))
        assert client.describe_stream_summary(StreamARN=arn)["StreamDescriptionSummary"]["StreamARN"] == arn
    finally:
        kin.delete_stream(StreamName=name)
