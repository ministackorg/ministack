import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

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
