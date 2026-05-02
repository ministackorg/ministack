import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_firehose_create_and_describe(fh):
    name = "intg-fh-basic"
    arn = fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::my-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )["DeliveryStreamARN"]
    assert "firehose" in arn
    assert name in arn

    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert desc["DeliveryStreamName"] == name
    assert desc["DeliveryStreamStatus"] == "ACTIVE"
    assert desc["DeliveryStreamType"] == "DirectPut"
    assert len(desc["Destinations"]) == 1
    assert "ExtendedS3DestinationDescription" in desc["Destinations"][0]
    assert desc["VersionId"] == "1"

def test_firehose_list_streams(fh):
    fh.create_delivery_stream(DeliveryStreamName="intg-fh-list-a", DeliveryStreamType="DirectPut")
    fh.create_delivery_stream(DeliveryStreamName="intg-fh-list-b", DeliveryStreamType="DirectPut")
    resp = fh.list_delivery_streams()
    names = resp["DeliveryStreamNames"]
    assert "intg-fh-list-a" in names
    assert "intg-fh-list-b" in names
    assert resp["HasMoreDeliveryStreams"] is False

def test_firehose_put_record(fh):
    name = "intg-fh-put"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    import base64

    data = base64.b64encode(b"hello firehose").decode()
    resp = fh.put_record(DeliveryStreamName=name, Record={"Data": data})
    assert "RecordId" in resp
    assert len(resp["RecordId"]) > 0
    assert resp["Encrypted"] is False

def test_firehose_put_record_batch(fh):
    name = "intg-fh-batch"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    import base64

    records = [{"Data": base64.b64encode(f"record-{i}".encode()).decode()} for i in range(5)]
    resp = fh.put_record_batch(DeliveryStreamName=name, Records=records)
    assert resp["FailedPutCount"] == 0
    assert len(resp["RequestResponses"]) == 5
    for r in resp["RequestResponses"]:
        assert "RecordId" in r

def test_firehose_delete_stream(fh):
    name = "intg-fh-delete"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    fh.delete_delivery_stream(DeliveryStreamName=name)
    from botocore.exceptions import ClientError

    try:
        fh.describe_delivery_stream(DeliveryStreamName=name)
        assert False, "should have raised"
    except ClientError as e:
        assert e.response["Error"]["Code"] == "ResourceNotFoundException"

def test_firehose_tags(fh):
    name = "intg-fh-tags"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    fh.tag_delivery_stream(
        DeliveryStreamName=name,
        Tags=[
            {"Key": "Env", "Value": "test"},
            {"Key": "Team", "Value": "data"},
        ],
    )
    resp = fh.list_tags_for_delivery_stream(DeliveryStreamName=name)
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["Env"] == "test"
    assert tag_map["Team"] == "data"

    fh.untag_delivery_stream(DeliveryStreamName=name, TagKeys=["Env"])
    resp2 = fh.list_tags_for_delivery_stream(DeliveryStreamName=name)
    keys = [t["Key"] for t in resp2["Tags"]]
    assert "Env" not in keys
    assert "Team" in keys

def test_firehose_update_destination(fh):
    name = "intg-fh-update-dest"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::original-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest_id = desc["Destinations"][0]["DestinationId"]
    version_id = desc["VersionId"]

    fh.update_destination(
        DeliveryStreamName=name,
        DestinationId=dest_id,
        CurrentDeliveryStreamVersionId=version_id,
        ExtendedS3DestinationUpdate={
            "BucketARN": "arn:aws:s3:::updated-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )
    desc2 = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert desc2["VersionId"] == "2"
    s3_cfg = desc2["Destinations"][0]["ExtendedS3DestinationDescription"]
    assert s3_cfg["BucketARN"] == "arn:aws:s3:::updated-bucket"

def test_firehose_encryption(fh):
    name = "intg-fh-enc"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    fh.start_delivery_stream_encryption(
        DeliveryStreamName=name,
        DeliveryStreamEncryptionConfigurationInput={"KeyType": "AWS_OWNED_CMK"},
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert desc["DeliveryStreamEncryptionConfiguration"]["Status"] == "ENABLED"

    fh.stop_delivery_stream_encryption(DeliveryStreamName=name)
    desc2 = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert desc2["DeliveryStreamEncryptionConfiguration"]["Status"] == "DISABLED"

def test_firehose_duplicate_create_error(fh):
    name = "intg-fh-dup"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    from botocore.exceptions import ClientError

    try:
        fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
        assert False, "should have raised"
    except ClientError as e:
        assert e.response["Error"]["Code"] == "ResourceInUseException"

def test_firehose_not_found_error(fh):
    from botocore.exceptions import ClientError

    try:
        fh.describe_delivery_stream(DeliveryStreamName="no-such-stream-xyz")
        assert False, "should have raised"
    except ClientError as e:
        assert e.response["Error"]["Code"] == "ResourceNotFoundException"

def test_firehose_list_with_type_filter(fh):
    fh.create_delivery_stream(DeliveryStreamName="intg-fh-type-dp", DeliveryStreamType="DirectPut")
    resp = fh.list_delivery_streams(DeliveryStreamType="DirectPut")
    assert "intg-fh-type-dp" in resp["DeliveryStreamNames"]

def test_firehose_s3_dest_has_encryption_config(fh):
    name = "intg-fh-enc-cfg"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::my-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    s3 = desc["Destinations"][0]["ExtendedS3DestinationDescription"]
    assert "EncryptionConfiguration" in s3
    assert s3["EncryptionConfiguration"] == {"NoEncryptionConfig": "NoEncryption"}

def test_firehose_no_enc_config_when_not_set(fh):
    name = "intg-fh-no-enc"
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert "DeliveryStreamEncryptionConfiguration" not in desc

def test_firehose_kinesis_source_block(fh):
    name = "intg-fh-kinesis-src"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="KinesisStreamAsSource",
        KinesisStreamSourceConfiguration={
            "KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::my-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert "Source" in desc
    ks = desc["Source"]["KinesisStreamSourceDescription"]
    assert ks["KinesisStreamARN"] == "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"
    assert ks["RoleARN"] == "arn:aws:iam::000000000000:role/firehose-role"
    assert "DeliveryStartTimestamp" in ks

def test_firehose_update_destination_merges_same_type(fh):
    name = "intg-fh-merge"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::original-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
            "Prefix": "original/",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest_id = desc["Destinations"][0]["DestinationId"]

    fh.update_destination(
        DeliveryStreamName=name,
        DestinationId=dest_id,
        CurrentDeliveryStreamVersionId=desc["VersionId"],
        ExtendedS3DestinationUpdate={
            "BucketARN": "arn:aws:s3:::updated-bucket",
        },
    )
    desc2 = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    s3 = desc2["Destinations"][0]["ExtendedS3DestinationDescription"]
    # Updated field
    assert s3["BucketARN"] == "arn:aws:s3:::updated-bucket"
    # Merged field preserved
    assert s3["Prefix"] == "original/"
    assert s3["RoleARN"] == "arn:aws:iam::000000000000:role/firehose-role"

def test_firehose_update_destination_replaces_on_type_change(fh):
    name = "intg-fh-type-change"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::my-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest_id = desc["Destinations"][0]["DestinationId"]

    fh.update_destination(
        DeliveryStreamName=name,
        DestinationId=dest_id,
        CurrentDeliveryStreamVersionId=desc["VersionId"],
        HttpEndpointDestinationUpdate={
            "EndpointConfiguration": {"Url": "https://my-endpoint.example.com"},
        },
    )
    desc2 = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest = desc2["Destinations"][0]
    assert "HttpEndpointDestinationDescription" in dest
    assert "ExtendedS3DestinationDescription" not in dest

def test_firehose_put_record_batch_failure_count(fh):
    """PutRecordBatch with valid records returns FailedPutCount=0."""
    fh.create_delivery_stream(
        DeliveryStreamName="qa-fh-batch-fail",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::qa-fh-bucket",
            "RoleARN": "arn:aws:iam::000000000000:role/r",
        },
    )
    resp = fh.put_record_batch(
        DeliveryStreamName="qa-fh-batch-fail",
        Records=[{"Data": "aGVsbG8="}, {"Data": "d29ybGQ="}],
    )
    assert resp["FailedPutCount"] == 0
    assert len(resp["RequestResponses"]) == 2

def test_firehose_update_destination_version_mismatch(fh):
    """UpdateDestination with wrong version raises ConcurrentModificationException."""
    fh.create_delivery_stream(
        DeliveryStreamName="qa-fh-version-check",
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::qa-fh-bucket2",
            "RoleARN": "arn:aws:iam::000000000000:role/r",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName="qa-fh-version-check")
    dest_id = desc["DeliveryStreamDescription"]["Destinations"][0]["DestinationId"]
    with pytest.raises(ClientError) as exc:
        fh.update_destination(
            DeliveryStreamName="qa-fh-version-check",
            CurrentDeliveryStreamVersionId="999",
            DestinationId=dest_id,
            ExtendedS3DestinationUpdate={
                "BucketARN": "arn:aws:s3:::qa-fh-bucket2-updated",
                "RoleARN": "arn:aws:iam::000000000000:role/r",
            },
        )
    assert exc.value.response["Error"]["Code"] == "ConcurrentModificationException"

def test_firehose_s3_destination_writes(s3, fh):
    """PutRecord with S3 destination actually writes data to the S3 bucket."""
    import base64
    import time as _time
    bucket = "fh-s3-dest-v39"
    s3.create_bucket(Bucket=bucket)
    fh.create_delivery_stream(
        DeliveryStreamName="fh-s3-test-v39",
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
            "Prefix": "data/",
        },
    )
    fh.put_record(DeliveryStreamName="fh-s3-test-v39", Record={"Data": b"hello from firehose"})
    _time.sleep(1)  # allow async delivery
    objs = s3.list_objects_v2(Bucket=bucket, Prefix="data/")
    assert objs.get("KeyCount", 0) > 0, "Firehose should have written to S3"
    key = objs["Contents"][0]["Key"]
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    assert b"hello from firehose" in body


def test_firehose_describe_nonexistent_carries_errortype(fh):
    """Real AWS sends `x-amzn-errortype` on JSON-protocol errors. Java/Go SDK
    v2 read it; without it they raise SdkClientException(unknown error type)."""
    with pytest.raises(ClientError) as exc:
        fh.describe_delivery_stream(DeliveryStreamName="missing-fh")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"
