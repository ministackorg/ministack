import base64
import io
import json
import os
import re
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _regional_client(service: str, region: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


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


@pytest.mark.parametrize(
    "config_key",
    ("ExtendedS3DestinationConfiguration", "S3DestinationConfiguration"),
)
@pytest.mark.parametrize(
    "bucket_arn",
    (
        "not-an-arn",
        "arn:aws:sns:us-east-1:000000000000:topic-name",
        "arn:aws:s3:us-east-1::my-bucket",
        "arn:aws:s3::000000000000:my-bucket",
        "arn:aws:s3:::my-bucket/path/to/object",
        "arn:aws:s3:::my-bucket:extra",
        "arn:aws:s3:::bad*bucket",
    ),
)
def test_firehose_rejects_invalid_s3_bucket_arns(fh, config_key, bucket_arn):
    with pytest.raises(ClientError) as exc:
        fh.create_delivery_stream(
            DeliveryStreamName=f"intg-fh-bad-bucket-arn-{_uuid_mod.uuid4().hex[:8]}",
            DeliveryStreamType="DirectPut",
            **{
                config_key: {
                    "BucketARN": bucket_arn,
                    "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
                },
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgumentException"


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


def test_firehose_update_destination_rejects_invalid_s3_bucket_arn(fh):
    name = f"qa-fh-update-bad-bucket-arn-{_uuid_mod.uuid4().hex[:8]}"
    fh.create_delivery_stream(
        DeliveryStreamName=name,
        ExtendedS3DestinationConfiguration={
            "BucketARN": "arn:aws:s3:::qa-fh-bucket3",
            "RoleARN": "arn:aws:iam::000000000000:role/r",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest_id = desc["Destinations"][0]["DestinationId"]
    version_id = desc["VersionId"]

    with pytest.raises(ClientError) as exc:
        fh.update_destination(
            DeliveryStreamName=name,
            CurrentDeliveryStreamVersionId=version_id,
            DestinationId=dest_id,
            ExtendedS3DestinationUpdate={
                "BucketARN": "arn:aws:s3:::qa-fh-bucket3/object",
            },
        )

    assert exc.value.response["Error"]["Code"] == "InvalidArgumentException"
    after = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    s3 = after["Destinations"][0]["ExtendedS3DestinationDescription"]
    assert after["VersionId"] == version_id
    assert s3["BucketARN"] == "arn:aws:s3:::qa-fh-bucket3"


def test_firehose_s3_destination_writes(s3, fh):
    """PutRecord with S3 destination actually writes data to the S3 bucket."""
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


def test_firehose_kinesis_stream_as_source_fans_out_to_s3(fh, kin, s3):
    """KinesisStreamAsSource: records put into the source Kinesis stream
    must reach the configured S3 destination. Issue #744."""
    import time as _time
    import uuid as _uuid

    stream_name = f"src-stream-{_uuid.uuid4().hex[:8]}"
    delivery_name = f"fh-{_uuid.uuid4().hex[:8]}"
    bucket = f"fh-src-bucket-{_uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    try:
        kin.create_stream(StreamName=stream_name, ShardCount=1)
        stream_arn = kin.describe_stream(
            StreamName=stream_name)["StreamDescription"]["StreamARN"]
        fh.create_delivery_stream(
            DeliveryStreamName=delivery_name,
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={
                "KinesisStreamARN": stream_arn,
                "RoleARN": "arn:aws:iam::000000000000:role/test",
            },
            ExtendedS3DestinationConfiguration={
                "RoleARN": "arn:aws:iam::000000000000:role/test",
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "Prefix": "out/",
                "CompressionFormat": "UNCOMPRESSED",
            },
        )
        # Single PutRecord.
        kin.put_record(
            StreamName=stream_name, PartitionKey="pk", Data=b'{"a":1}')
        # Batch PutRecords.
        kin.put_records(StreamName=stream_name, Records=[
            {"PartitionKey": "pk1", "Data": b'{"a":2}'},
            {"PartitionKey": "pk2", "Data": b'{"a":3}'},
        ])
        for _ in range(20):
            objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
            if len(objs) >= 3:
                break
            _time.sleep(0.1)
        objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
        assert len(objs) >= 3, f"expected 3 records delivered to S3, got {len(objs)}"
        bodies = sorted(
            s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
            for o in objs
        )
        assert bodies == [b'{"a":1}', b'{"a":2}', b'{"a":3}']
    finally:
        try:
            fh.delete_delivery_stream(DeliveryStreamName=delivery_name)
        except Exception:
            pass
        try:
            kin.delete_stream(StreamName=stream_name)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ProcessingConfiguration — Lambda processor invocation.
#
# Real AWS Firehose invokes the configured Lambda for every batch of records
# in flight, with payload shape:
#   {invocationId, deliveryStreamArn, region, records:[{recordId, approximateArrivalTimestamp, data(base64)}]}
# and expects a response shape:
#   {records:[{recordId, result:"Ok"|"Dropped"|"ProcessingFailed", data(base64)}]}
# Records marked Dropped/ProcessingFailed are not written to the destination;
# Ok records are written with the transformed data. On any Lambda failure
# Firehose passes records through unchanged (best-effort by AWS contract).
# ---------------------------------------------------------------------------


def _processor_lambda_zip(handler_src: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_src)
    return buf.getvalue()


def _make_processor_fn(lam, name: str, handler_src: str) -> str:
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": _processor_lambda_zip(handler_src)},
    )
    return f"arn:aws:lambda:us-east-1:000000000000:function:{name}"


def _create_firehose_with_processor(fh, name: str, bucket: str, lambda_arn: str,
                                    delivery_type: str = "DirectPut",
                                    kinesis_arn: str | None = None) -> None:
    ext_s3 = {
        "BucketARN": f"arn:aws:s3:::{bucket}",
        "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
        "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 1},
        "CompressionFormat": "UNCOMPRESSED",
        "Prefix": "out/",
        "ProcessingConfiguration": {
            "Enabled": True,
            "Processors": [{
                "Type": "Lambda",
                "Parameters": [
                    {"ParameterName": "LambdaArn", "ParameterValue": lambda_arn},
                ],
            }],
        },
    }
    if delivery_type == "KinesisStreamAsSource" and kinesis_arn:
        fh.create_delivery_stream(
            DeliveryStreamName=name,
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={
                "KinesisStreamARN": kinesis_arn,
                "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
            },
            ExtendedS3DestinationConfiguration=ext_s3,
        )
    else:
        fh.create_delivery_stream(
            DeliveryStreamName=name,
            DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration=ext_s3,
        )


def test_firehose_lambda_processor_transforms_record(fh, s3, lam):
    """Lambda processor returns transformed data → S3 object contains the
    transform output, not the original record."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"fh-proc-{suffix}"
    fn = f"fh-proc-fn-{suffix}"
    delivery = f"fh-proc-{suffix}"
    s3.create_bucket(Bucket=bucket)
    handler = (
        "import base64, json\n"
        "def handler(event, context):\n"
        "    out = []\n"
        "    for r in event['records']:\n"
        "        raw = base64.b64decode(r['data'])\n"
        "        transformed = raw.upper()\n"
        "        out.append({\n"
        "            'recordId': r['recordId'],\n"
        "            'result': 'Ok',\n"
        "            'data': base64.b64encode(transformed).decode(),\n"
        "        })\n"
        "    return {'records': out}\n"
    )
    arn = _make_processor_fn(lam, fn, handler)
    _create_firehose_with_processor(fh, delivery, bucket, arn)
    try:
        fh.put_record(DeliveryStreamName=delivery, Record={"Data": b"hello"})
        for _ in range(20):
            objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
            if objs:
                break
            time.sleep(0.1)
        objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
        assert len(objs) == 1, f"expected 1 transformed record, got {len(objs)}"
        body = s3.get_object(Bucket=bucket, Key=objs[0]["Key"])["Body"].read()
        assert body == b"HELLO", body
    finally:
        try:
            fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


def test_firehose_lambda_processor_dropped_record_not_written(fh, s3, lam):
    """``result: Dropped`` → record is filtered out, no S3 object written."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"fh-drop-{suffix}"
    fn = f"fh-drop-fn-{suffix}"
    delivery = f"fh-drop-{suffix}"
    s3.create_bucket(Bucket=bucket)
    handler = (
        "def handler(event, context):\n"
        "    return {'records': [\n"
        "        {'recordId': r['recordId'], 'result': 'Dropped', 'data': r['data']}\n"
        "        for r in event['records']\n"
        "    ]}\n"
    )
    arn = _make_processor_fn(lam, fn, handler)
    _create_firehose_with_processor(fh, delivery, bucket, arn)
    try:
        fh.put_record(DeliveryStreamName=delivery, Record={"Data": b"hello"})
        time.sleep(0.6)  # give the fire-and-forget S3 task time
        objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
        assert objs == [], f"expected no S3 objects, got {objs}"
    finally:
        try:
            fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


def test_firehose_lambda_processor_not_found_passes_through(fh, s3):
    """Configured LambdaArn that doesn't exist → record delivered unchanged
    (Firehose is best-effort; processor failure must not break the stream)."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"fh-miss-{suffix}"
    delivery = f"fh-miss-{suffix}"
    s3.create_bucket(Bucket=bucket)
    missing_arn = (
        f"arn:aws:lambda:us-east-1:000000000000:function:fh-no-such-{suffix}"
    )
    _create_firehose_with_processor(fh, delivery, bucket, missing_arn)
    try:
        fh.put_record(DeliveryStreamName=delivery, Record={"Data": b"untouched"})
        for _ in range(20):
            objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
            if objs:
                break
            time.sleep(0.1)
        objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
        assert len(objs) == 1
        body = s3.get_object(Bucket=bucket, Key=objs[0]["Key"])["Body"].read()
        assert body == b"untouched"
    finally:
        try:
            fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception:
            pass


def test_firehose_kinesis_source_with_lambda_processor(fh, s3, lam, kin):
    """End-to-end Kinesis → Firehose → Lambda processor → S3 — the user's
    exact repro from issue #744 follow-up."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket = f"fh-kin-proc-{suffix}"
    fn = f"fh-kin-proc-fn-{suffix}"
    delivery = f"fh-kin-proc-{suffix}"
    stream = f"fh-kin-proc-src-{suffix}"
    s3.create_bucket(Bucket=bucket)
    kin.create_stream(StreamName=stream, ShardCount=1)
    # Wait for stream to be ACTIVE.
    for _ in range(20):
        if kin.describe_stream(StreamName=stream)["StreamDescription"]["StreamStatus"] == "ACTIVE":
            break
        time.sleep(0.1)
    stream_arn = kin.describe_stream(StreamName=stream)["StreamDescription"]["StreamARN"]
    handler = (
        "import base64\n"
        "def handler(event, context):\n"
        "    out = []\n"
        "    for r in event['records']:\n"
        "        raw = base64.b64decode(r['data'])\n"
        "        out.append({\n"
        "            'recordId': r['recordId'],\n"
        "            'result': 'Ok',\n"
        "            'data': base64.b64encode(b'proc:' + raw).decode(),\n"
        "        })\n"
        "    return {'records': out}\n"
    )
    arn = _make_processor_fn(lam, fn, handler)
    _create_firehose_with_processor(
        fh, delivery, bucket, arn,
        delivery_type="KinesisStreamAsSource", kinesis_arn=stream_arn,
    )
    try:
        kin.put_record(StreamName=stream, PartitionKey="pk", Data=b"payload")
        for _ in range(30):
            objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
            if objs:
                break
            time.sleep(0.1)
        objs = s3.list_objects_v2(Bucket=bucket, Prefix="out/").get("Contents", [])
        assert len(objs) >= 1, "expected at least 1 record delivered to S3"
        body = s3.get_object(Bucket=bucket, Key=objs[0]["Key"])["Body"].read()
        assert body == b"proc:payload", body
    finally:
        try:
            fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception:
            pass
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass
        try:
            kin.delete_stream(StreamName=stream)
        except Exception:
            pass


def test_firehose_same_name_streams_are_region_scoped(fh):
    """Same-named delivery streams in two regions stay isolated with region-bearing ARNs."""
    west = _regional_client("firehose", "us-west-2")
    name = f"intg-fh-region-scope-{_uuid_mod.uuid4().hex[:8]}"

    east_arn = fh.create_delivery_stream(
        DeliveryStreamName=name, DeliveryStreamType="DirectPut"
    )["DeliveryStreamARN"]
    west_arn = west.create_delivery_stream(
        DeliveryStreamName=name, DeliveryStreamType="DirectPut"
    )["DeliveryStreamARN"]

    assert east_arn == f"arn:aws:firehose:us-east-1:000000000000:deliverystream/{name}"
    assert west_arn == f"arn:aws:firehose:us-west-2:000000000000:deliverystream/{name}"

    east_desc = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    west_desc = west.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    assert east_desc["DeliveryStreamARN"] == east_arn
    assert west_desc["DeliveryStreamARN"] == west_arn

    # Use a high Limit so the assertion is robust to other streams the shared
    # session fixture accumulates (default page size is 10).
    assert name in fh.list_delivery_streams(Limit=10000)["DeliveryStreamNames"]
    assert name in west.list_delivery_streams(Limit=10000)["DeliveryStreamNames"]


def test_firehose_stream_names_do_not_fallback_across_region(fh):
    """A stream created in one region is not visible from another region."""
    west = _regional_client("firehose", "us-west-2")
    name = f"intg-fh-no-fallback-{_uuid_mod.uuid4().hex[:8]}"

    west.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut")

    with pytest.raises(ClientError) as exc:
        fh.describe_delivery_stream(DeliveryStreamName=name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # West still resolves it, and it never leaks into the east listing.
    assert name in west.list_delivery_streams(Limit=10000)["DeliveryStreamNames"]
    assert name not in fh.list_delivery_streams(Limit=10000)["DeliveryStreamNames"]


def test_firehose_restore_legacy_account_scoped_state_uses_arn_region():
    """Legacy account-scoped persisted streams migrate to the region in their ARN."""
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import firehose as _fh

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    name = "LegacyFirehoseStream"
    stream_arn = f"arn:aws:firehose:us-west-2:{account_id}:deliverystream/{name}"

    legacy_streams = AccountScopedDict()
    legacy_streams._data[(account_id, name)] = {
        "name": name,
        "arn": stream_arn,
        "status": "ACTIVE",
        "type": "DirectPut",
        "version": 1,
        "created_at": 1700000000,
        "updated_at": 1700000000,
        "destinations": [],
        "tags": {},
        "encryption": None,
        "kinesis_source": None,
    }

    _fh.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")

        _fh.load_persisted_state({"_streams": legacy_streams})

        # The legacy stream lands in the region carried by its ARN, not the
        # active request region.
        assert _fh._streams.get_scoped(account_id, "us-east-1", name) is None
        assert _fh._streams.get_scoped(account_id, "us-west-2", name)["arn"] == stream_arn
    finally:
        _fh.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_firehose_kinesis_source_ingest_is_region_scoped():
    """ingest_from_kinesis_source only fans out to same-region delivery streams.

    A KinesisStreamAsSource delivery stream registered in us-west-2 must NOT be
    matched by a Kinesis-source ingest running under a different request region,
    even when the source ARN matches. Exercises the regional filtering inside
    ``ingest_from_kinesis_source`` (``_streams.values()`` is scoped to the
    current account+region), which the positive same-region tests don't cover.
    """
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import firehose as _fh

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    name = "KinSrcRegionScoped"
    source_arn = f"arn:aws:kinesis:us-west-2:{account_id}:stream/src"

    def _make_stream():
        return {
            "name": name,
            "arn": f"arn:aws:firehose:us-west-2:{account_id}:deliverystream/{name}",
            "status": "ACTIVE",
            "type": "KinesisStreamAsSource",
            "version": 1,
            "created_at": 1700000000,
            "updated_at": 1700000000,
            "destinations": [
                {
                    "id": "destinationId-000000000001",
                    "type": "ExtendedS3",
                    "config": {"BucketARN": "arn:aws:s3:::no-such-bucket", "Prefix": "out/"},
                    "records": [],
                }
            ],
            "tags": {},
            "encryption": None,
            "kinesis_source": {
                "KinesisStreamARN": source_arn,
                "RoleARN": "arn:aws:iam::000000000000:role/test",
                "DeliveryStartTimestamp": 0,
            },
        }

    _fh.reset()
    try:
        set_request_account_id(account_id)

        # Register the delivery stream in us-west-2.
        set_request_region("us-west-2")
        west_stream = _make_stream()
        _fh._streams.set_scoped(account_id, "us-west-2", name, west_stream)

        # Ingest under a DIFFERENT region: the us-west-2 stream is out of scope,
        # so nothing is delivered even though the source ARN matches.
        set_request_region("us-east-1")
        _fh.ingest_from_kinesis_source(source_arn, [("pk", b'{"a":1}')])
        assert west_stream["destinations"][0]["records"] == []

        # Control: same-region ingest matches and appends the record.
        set_request_region("us-west-2")
        _fh.ingest_from_kinesis_source(source_arn, [("pk", b'{"a":1}')])
        assert len(west_stream["destinations"][0]["records"]) == 1
    finally:
        _fh.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def _iceberg_test_con(bucket_arn):
    """DuckDB connection attached to MiniStack's S3 Tables Iceberg REST catalog.
    Skips the test if the iceberg/httpfs extensions or REST-catalog writes are
    unavailable in this environment (e.g. offline CI or an old DuckDB)."""
    import duckdb

    parsed = urlparse(ENDPOINT)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4566
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        con.execute(
            f"CREATE SECRET s (TYPE S3, KEY_ID 'test', SECRET 'test', "
            f"ENDPOINT '{host}:{port}', URL_STYLE 'path', USE_SSL false, "
            f"REGION 'us-east-1')"
        )
        con.execute(
            f"ATTACH '{bucket_arn}' AS cat (TYPE ICEBERG, "
            f"ENDPOINT '{ENDPOINT}/iceberg', AUTHORIZATION_TYPE 'none')"
        )
    except Exception as exc:  # pragma: no cover - environment dependent
        con.close()
        pytest.skip(f"DuckDB Iceberg REST writes unavailable: {exc}")
    return con


def test_firehose_iceberg_delivery_writes_queryable_rows(fh):
    """DirectPut records delivered to an Iceberg (S3 Tables) destination are
    written into the table via the Iceberg REST catalog and are queryable,
    matching real Amazon Data Firehose. Operation defaults to insert."""
    suffix = _uuid_mod.uuid4().hex[:8]
    s3t = _regional_client("s3tables", "us-east-1")
    bucket = s3t.create_table_bucket(name=f"fh-ice-{suffix}")["arn"]
    s3t.create_namespace(tableBucketARN=bucket, namespace=["analytics"])
    con = _iceberg_test_con(bucket)
    con.execute("CREATE TABLE cat.analytics.events (id INTEGER, name VARCHAR)")
    con.close()

    stream = f"fh-ice-{suffix}"
    fh.create_delivery_stream(
        DeliveryStreamName=stream,
        DeliveryStreamType="DirectPut",
        IcebergDestinationConfiguration={
            "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
            "CatalogConfiguration": {"CatalogARN": bucket},
            "S3Configuration": {
                "BucketARN": "arn:aws:s3:::fh-ice-errors",
                "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
            },
            "DestinationTableConfigurationList": [{
                "DestinationDatabaseName": "analytics",
                "DestinationTableName": "events",
                "UniqueKeys": ["id"],
            }],
        },
    )
    try:
        fh.put_record(
            DeliveryStreamName=stream,
            Record={"Data": json.dumps({"id": 1, "name": "alice"}).encode()},
        )
        rows = None
        for _ in range(60):
            try:
                q = _iceberg_test_con(bucket)
                rows = q.execute(
                    "SELECT id, name FROM cat.analytics.events ORDER BY id"
                ).fetchall()
                q.close()
                if rows == [(1, "alice")]:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        assert rows == [(1, "alice")]
    finally:
        fh.delete_delivery_stream(DeliveryStreamName=stream)


def _wait_objects(s3, bucket, prefix, count=1):
    for _ in range(40):
        objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        if len(objs) >= count:
            return objs
        time.sleep(0.1)
    return s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])


def _partitioned_parquet_stream(fh, glue, s3, suffix, query):
    bucket, database, name = f"fh-dp-{suffix}", f"fh_dp_{suffix}", f"fh-dp-{suffix}"
    s3.create_bucket(Bucket=bucket)
    glue.create_database(DatabaseInput={"Name": database})
    glue.create_table(DatabaseName=database, TableInput={"Name": "events", "StorageDescriptor": {
        "Columns": [{"Name": "timestamp", "Type": "string"}, {"Name": "action", "Type": "int"}],
        "Location": f"s3://{bucket}/usage/"}})
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut",
                              ExtendedS3DestinationConfiguration={
        "RoleARN": "arn:aws:iam::000000000000:role/firehose", "BucketARN": f"arn:aws:s3:::{bucket}",
        "Prefix": "usage/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/"
                  "day=!{partitionKeyFromQuery:day}/",
        "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
        "DynamicPartitioningConfiguration": {"Enabled": True},
        "ProcessingConfiguration": {"Enabled": True, "Processors": [{"Type": "MetadataExtraction", "Parameters": [
            {"ParameterName": "JsonParsingEngine", "ParameterValue": "JQ-1.6"},
            {"ParameterName": "MetadataExtractionQuery", "ParameterValue": query}]}]},
        "DataFormatConversionConfiguration": {"Enabled": True, "SchemaConfiguration": {
            "RoleARN": "arn:aws:iam::000000000000:role/firehose", "DatabaseName": database,
            "TableName": "events", "Region": "us-east-1"},
            "InputFormatConfiguration": {"Deserializer": {"HiveJsonSerDe": {}}},
            "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {"Compression": "SNAPPY"}}}},
    })
    return bucket, name


def test_firehose_dynamic_partitioning_writes_parquet(fh, glue, s3, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    bucket, name = _partitioned_parquet_stream(
        fh, glue, s3, _uuid_mod.uuid4().hex[:8],
        "{year: .timestamp[0:4], month: .timestamp[5:7], day: .timestamp[8:10]}")
    fh.put_record(DeliveryStreamName=name,
                  Record={"Data": json.dumps({"timestamp": "2026-04-23T11:30:00Z", "action": 1})})
    objs = _wait_objects(s3, bucket, "usage/")
    assert len(objs) == 1, objs
    key = objs[0]["Key"]
    # No timestamp expression in the prefix, so Firehose appends yyyy/MM/dd/HH/.
    assert re.fullmatch(rf"usage/year=2026/month=04/day=23/\d{{4}}/\d{{2}}/\d{{2}}/\d{{2}}/{name}-1-"
                        r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-[0-9a-f-]{36}\.parquet", key), key
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert body[:4] == b"PAR1" and body[-4:] == b"PAR1"
    path = tmp_path / "out.parquet"
    path.write_bytes(body)
    con = duckdb.connect()
    assert con.execute(f"SELECT * FROM '{path}'").fetchall() == [("2026-04-23T11:30:00Z", 1)]
    assert con.execute(f"SELECT DISTINCT compression FROM parquet_metadata('{path}')").fetchall() == [("SNAPPY",)]


def test_firehose_failed_records_go_to_the_error_prefix(fh, glue, s3):
    pytest.importorskip("duckdb")
    bucket, name = _partitioned_parquet_stream(
        fh, glue, s3, _uuid_mod.uuid4().hex[:8], "{year: .timestamp[0:4], month: .month, day: .day}")
    missing_key = json.dumps({"timestamp": "2026-04-23T11:30:00Z", "action": 1})
    fh.put_record(DeliveryStreamName=name, Record={"Data": missing_key})
    failed = _wait_objects(s3, bucket, "errors/processing-failed/")
    assert len(failed) == 1, failed
    document = json.loads(s3.get_object(Bucket=bucket, Key=failed[0]["Key"])["Body"].read())
    assert base64.b64decode(document["rawData"]).decode() == missing_key
    assert {"attemptsMade", "arrivalTimestamp", "attemptEndingTimestamp", "ErrorMessage"} <= set(document)

    bad_type = json.dumps({"timestamp": "2026-04-23T11:30:00Z", "action": "abc", "month": "04", "day": "23"})
    fh.put_record(DeliveryStreamName=name, Record={"Data": bad_type})
    failed = _wait_objects(s3, bucket, "errors/format-conversion-failed/")
    assert len(failed) == 1, failed
    document = json.loads(s3.get_object(Bucket=bucket, Key=failed[0]["Key"])["Body"].read())
    assert document["ErrorCode"] == "DataFormatConversion.MalformedData"
    assert document["ErrorMessage"] == "Data does not match the schema."
    assert document["dataCatalogTable"]["tableName"] == "events"
    assert not s3.list_objects_v2(Bucket=bucket, Prefix="usage/").get("Contents")


def test_firehose_prefix_timestamp_expressions_replace_the_default(fh, s3):
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, name = f"fh-ts-{suffix}", f"fh-ts-{suffix}"
    s3.create_bucket(Bucket=bucket)
    fh.create_delivery_stream(DeliveryStreamName=name, ExtendedS3DestinationConfiguration={
        "RoleARN": "arn:aws:iam::000000000000:role/firehose", "BucketARN": f"arn:aws:s3:::{bucket}",
        "Prefix": "logs/year=!{timestamp:yyyy}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/"})
    fh.put_record(DeliveryStreamName=name, Record={"Data": b"raw"})
    objs = _wait_objects(s3, bucket, "logs/")
    assert len(objs) == 1
    assert re.fullmatch(rf"logs/year=\d{{4}}/{name}-1-\d{{4}}(-\d{{2}}){{5}}-[0-9a-f-]{{36}}", objs[0]["Key"]), objs
    assert s3.get_object(Bucket=bucket, Key=objs[0]["Key"])["Body"].read() == b"raw"


def test_firehose_partition_keys_from_lambda(fh, s3, lam):
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, fn, name = f"fh-lk-{suffix}", f"fh-lk-fn-{suffix}", f"fh-lk-{suffix}"
    s3.create_bucket(Bucket=bucket)
    handler = (
        "import base64, json\n"
        "def handler(event, context):\n"
        "    out = []\n"
        "    for r in event['records']:\n"
        "        doc = json.loads(base64.b64decode(r['data']))\n"
        "        out.append({'recordId': r['recordId'], 'result': 'Ok', 'data': r['data'],\n"
        "                    'metadata': {'partitionKeys': {'customer': doc['customer']}}})\n"
        "    return {'records': out}\n"
    )
    arn = _make_processor_fn(lam, fn, handler)
    fh.create_delivery_stream(DeliveryStreamName=name, ExtendedS3DestinationConfiguration={
        "RoleARN": "arn:aws:iam::000000000000:role/firehose", "BucketARN": f"arn:aws:s3:::{bucket}",
        "Prefix": "c=!{partitionKeyFromLambda:customer}/", "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
        "DynamicPartitioningConfiguration": {"Enabled": True},
        "ProcessingConfiguration": {"Enabled": True, "Processors": [{"Type": "Lambda", "Parameters": [
            {"ParameterName": "LambdaArn", "ParameterValue": arn}]}]}})
    try:
        fh.put_record(DeliveryStreamName=name, Record={"Data": json.dumps({"customer": "acme"})})
        objs = _wait_objects(s3, bucket, "c=acme/")
        assert len(objs) == 1, s3.list_objects_v2(Bucket=bucket).get("Contents")
    finally:
        fh.delete_delivery_stream(DeliveryStreamName=name)
        lam.delete_function(FunctionName=fn)


def test_firehose_region_delivery_into_regional_bucket():
    """A stream in eu-west-1 delivers into an eu-west-1 bucket; the internal
    S3 write is unsigned, so the bucket home-region check doesn't apply."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, name = f"fh-euw1-{suffix}", f"fh-euw1-{suffix}"
    fh, s3 = _regional_client("firehose", "eu-west-1"), _regional_client("s3", "eu-west-1")
    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut",
                              ExtendedS3DestinationConfiguration={
                                  "RoleARN": "arn:aws:iam::000000000000:role/firehose",
                                  "BucketARN": f"arn:aws:s3:::{bucket}", "Prefix": "out/",
                                  "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 1}})
    try:
        fh.put_record(DeliveryStreamName=name, Record={"Data": b"regional"})
        objs = _wait_objects(s3, bucket, "out/")
        assert len(objs) == 1, objs
        assert s3.get_object(Bucket=bucket, Key=objs[0]["Key"])["Body"].read() == b"regional"
    finally:
        fh.delete_delivery_stream(DeliveryStreamName=name)
        for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket)


# ─── Iceberg destination on the Glue Data Catalog ─────────────────────────────

_ICE_COLUMNS = [
    {"id": 1, "name": "tenant_id", "required": False, "type": "string"},
    {"id": 2, "name": "integration_id", "required": False, "type": "string"},
    {"id": 3, "name": "sub_integration_id", "required": False, "type": "string"},
    {"id": 4, "name": "raw", "required": False, "type": "string"},
    {"id": 5, "name": "extracted_at", "required": False, "type": "timestamp"},
]
_GLUE_CATALOG = "arn:aws:glue:us-east-1:000000000000:catalog"


def _glue_iceberg_table(s3, glue, suffix):
    """A format-version 2 table partitioned by three varchar columns, laid out as Trino creates one."""
    bucket, database, table = f"fh-glue-ice-{suffix}", f"fh_glue_ice_{suffix}", "itm_events"
    s3.create_bucket(Bucket=bucket)
    glue.create_database(DatabaseInput={"Name": database})
    location = f"s3://{bucket}/{database}/{table}"
    metadata = {
        "format-version": 2, "table-uuid": str(_uuid_mod.uuid4()), "location": location,
        "last-sequence-number": 0, "last-updated-ms": int(time.time() * 1000), "last-column-id": 5,
        "current-schema-id": 0, "schemas": [{"type": "struct", "schema-id": 0, "fields": _ICE_COLUMNS}],
        "default-spec-id": 0, "last-partition-id": 1002, "partition-specs": [{"spec-id": 0, "fields": [
            {"name": n, "transform": "identity", "source-id": i, "field-id": 999 + i}
            for i, n in ((1, "tenant_id"), (2, "integration_id"), (3, "sub_integration_id"))]}],
        "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {"write.format.default": "PARQUET"}, "current-snapshot-id": -1, "refs": {},
        "snapshots": [], "snapshot-log": [], "metadata-log": [],
    }
    key = f"{database}/{table}/metadata/00000-{_uuid_mod.uuid4()}.metadata.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(metadata).encode())
    glue.create_table(DatabaseName=database, TableInput={
        "Name": table, "TableType": "EXTERNAL_TABLE",
        "Parameters": {"table_type": "ICEBERG", "metadata_location": f"s3://{bucket}/{key}"}})
    return bucket, database, table


def _glue_iceberg_stream(fh, bucket, database, table, name, interval=0, **extra):
    fh.create_delivery_stream(DeliveryStreamName=name, DeliveryStreamType="DirectPut", IcebergDestinationConfiguration={
        "RoleARN": "arn:aws:iam::000000000000:role/firehose", "AppendOnly": True,
        "CatalogConfiguration": {"CatalogARN": _GLUE_CATALOG},
        "DestinationTableConfigurationList": [{"DestinationDatabaseName": database, "DestinationTableName": table}],
        "BufferingHints": {"IntervalInSeconds": interval, "SizeInMBs": 128},
        "RetryOptions": {"DurationInSeconds": 7200},
        "S3Configuration": {"RoleARN": "arn:aws:iam::000000000000:role/firehose",
                            "BucketARN": f"arn:aws:s3:::{bucket}", "ErrorOutputPrefix": f"{name}-errors/"},
        "CloudWatchLoggingOptions": {"Enabled": True, "LogGroupName": "/firehose", "LogStreamName": "delivery"},
        **extra})


def _s3_body(s3, uri):
    bucket, key = uri.split("://", 1)[1].split("/", 1)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def _iceberg_state(s3, glue, database, table, tmp_path):
    """(metadata.json, rows) of the table's current snapshot, read from its manifests and Parquet files."""
    import duckdb

    from ministack.services.firehose_iceberg import avro_read

    location = glue.get_table(DatabaseName=database, Name=table)["Table"]["Parameters"]["metadata_location"]
    metadata = json.loads(_s3_body(s3, location))
    snapshot = next((s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"]), None)
    rows = []
    if snapshot:
        con = duckdb.connect()
        for manifest in avro_read(_s3_body(s3, snapshot["manifest-list"])):
            for entry in avro_read(_s3_body(s3, manifest["manifest_path"])):
                data_file = entry["data_file"]
                path = tmp_path / f"{_uuid_mod.uuid4()}.parquet"
                path.write_bytes(_s3_body(s3, data_file["file_path"]))
                found = con.execute(f"SELECT * FROM '{path}'").fetchall()
                assert len(found) == data_file["record_count"]
                for row in found:
                    assert tuple(data_file["partition"].values()) == row[:3]
                rows += found
    return metadata, sorted(rows, key=lambda r: (r[4] is None, r[4] or 0, r[3] or ""))


def _ice_record(sub, n, extracted_at=None, **extra):
    return json.dumps({"tenant_id": "t1", "integration_id": "i1", "sub_integration_id": sub, "raw": f"r{n}",
                       "extracted_at": extracted_at or f"2026-11-11T14:46:53.{377500 + n:06d}", **extra}).encode()


@pytest.fixture
def glue_iceberg(fh, glue, s3):
    pytest.importorskip("duckdb")
    return _glue_iceberg_table(s3, glue, _uuid_mod.uuid4().hex[:8])


def test_firehose_put_record_batch_limits(fh, glue_iceberg):
    bucket, database, table = glue_iceberg
    name = f"fh-limits-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    with pytest.raises(ClientError) as exc:
        fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": b"{}"}] * 501)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == ("1 validation error detected: Value at 'records' failed to "
                                                      "satisfy constraint: Member must have length less than or "
                                                      "equal to 500")
    with pytest.raises(ClientError) as exc:
        fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": b"{}"}, {"Data": b"x" * (1024 * 1000 + 1)}])
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "Value at 'records.2.member.data'" in exc.value.response["Error"]["Message"]
    with pytest.raises(ClientError) as exc:
        fh.put_record(DeliveryStreamName=name, Record={"Data": b"x" * (1024 * 1000 + 1)})
    assert "Value at 'record.data'" in exc.value.response["Error"]["Message"]
    with pytest.raises(ClientError) as exc:
        fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": b"x" * (1024 * 1000)}] * 4
                            + [{"Data": b"x" * (4 * 1024 * 1024 - 4 * 1024 * 1000 + 1)}])
    assert exc.value.response["Error"] == {"Code": "InvalidArgumentException",
                                           "Message": "Records size exceeds 4 MB limit"}
    ok = fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": _ice_record("s1", n)} for n in range(3)])
    assert ok["FailedPutCount"] == 0
    assert len(ok["RequestResponses"]) == 3 and all(r["RecordId"] for r in ok["RequestResponses"])


def test_firehose_iceberg_destination_config(fh, glue_iceberg):
    bucket, database, table = glue_iceberg
    name = f"fh-cfg-{bucket[-8:]}"
    with pytest.raises(ClientError) as exc:
        _glue_iceberg_stream(fh, bucket, database, table, name, interval=901)
    assert exc.value.response["Error"]["Message"] == (
        "1 validation error detected: Value at 'icebergDestinationConfiguration.bufferingHints.intervalInSeconds' "
        "failed to satisfy constraint: Member must have value less than or equal to 900")
    _glue_iceberg_stream(fh, bucket, database, table, name)
    stream = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]
    dest = stream["Destinations"][0]["IcebergDestinationDescription"]
    assert dest["AppendOnly"] is True
    assert dest["CatalogConfiguration"] == {"CatalogARN": _GLUE_CATALOG}
    assert dest["RetryOptions"] == {"DurationInSeconds": 7200}
    assert dest["BufferingHints"] == {"IntervalInSeconds": 0, "SizeInMBs": 128}
    assert dest["S3DestinationDescription"]["ErrorOutputPrefix"] == f"{name}-errors/"
    assert dest["CloudWatchLoggingOptions"]["LogGroupName"] == "/firehose"
    with pytest.raises(ClientError) as exc:
        fh.update_destination(DeliveryStreamName=name, CurrentDeliveryStreamVersionId=stream["VersionId"],
                              DestinationId=stream["Destinations"][0]["DestinationId"],
                              IcebergDestinationUpdate={"AppendOnly": False})
    assert exc.value.response["Error"] == {"Code": "InvalidArgumentException",
                                           "Message": "AppendOnly field cannot be disabled once set to true"}

    other = f"fh-cfg-defaults-{bucket[-8:]}"
    fh.create_delivery_stream(DeliveryStreamName=other, IcebergDestinationConfiguration={
        "RoleARN": "arn:aws:iam::000000000000:role/firehose", "CatalogConfiguration": {"CatalogARN": _GLUE_CATALOG},
        "DestinationTableConfigurationList": [{"DestinationDatabaseName": database, "DestinationTableName": table}],
        "S3Configuration": {"RoleARN": "arn:aws:iam::000000000000:role/firehose",
                            "BucketARN": f"arn:aws:s3:::{bucket}"}})
    dest = fh.describe_delivery_stream(DeliveryStreamName=other)["DeliveryStreamDescription"]["Destinations"][0]
    assert dest["IcebergDestinationDescription"]["AppendOnly"] is False
    assert dest["IcebergDestinationDescription"]["RetryOptions"] == {"DurationInSeconds": 300}


def test_firehose_iceberg_delivers_one_partitioned_commit(fh, glue, s3, glue_iceberg, tmp_path):
    bucket, database, table = glue_iceberg
    name = f"fh-commit-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    before = glue.get_table(DatabaseName=database, Name=table)["Table"]["Parameters"]["metadata_location"]
    records = [_ice_record(f"s{n % 2}", n, ignored="extra key") for n in range(4)]
    fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": r} for r in records])

    params = glue.get_table(DatabaseName=database, Name=table)["Table"]["Parameters"]
    assert params["metadata_location"] != before
    assert params["previous_metadata_location"] == before
    metadata, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    assert len(metadata["snapshots"]) == 1
    assert [r[:4] for r in rows] == [("t1", "i1", f"s{n % 2}", f"r{n}") for n in range(4)]
    summary = metadata["snapshots"][0]["summary"]
    assert summary["added-records"] == "4" and summary["added-data-files"] == "2"
    assert summary["changed-partition-count"] == "2"
    paths = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=f"{database}/{table}/data/")["Contents"]]
    assert sorted(p.rsplit("/", 1)[0] for p in paths) == [
        f"{database}/{table}/data/tenant_id=t1/integration_id=i1/sub_integration_id=s0",
        f"{database}/{table}/data/tenant_id=t1/integration_id=i1/sub_integration_id=s1"]


def test_firehose_iceberg_timestamps_and_failed_records(fh, glue, s3, glue_iceberg, tmp_path):
    import datetime

    bucket, database, table = glue_iceberg
    name = f"fh-types-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    malformed = _ice_record("s1", 2, extracted_at="2026-11-11 14:46:53.377548")
    fh.put_record_batch(DeliveryStreamName=name, Records=[
        {"Data": _ice_record("s1", 1, extracted_at="2026-11-11T14:46:53.377548")},
        {"Data": malformed}, {"Data": b"not json"}])

    _, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    assert rows == [("t1", "i1", "s1", "r1", datetime.datetime(2026, 11, 11, 14, 46, 53, 377548))]
    errors = s3.list_objects_v2(Bucket=bucket, Prefix=f"{name}-errors/")["Contents"]
    assert len(errors) == 1
    assert re.fullmatch(rf"{name}-errors/iceberg-failed/\d{{4}}/\d{{2}}/\d{{2}}/\d{{2}}/{name}-1-"
                        r"\d{4}(-\d{2}){5}-[0-9a-f-]{36}", errors[0]["Key"]), errors[0]["Key"]
    lines = [json.loads(line) for line in _s3_body(s3, f"s3://{bucket}/{errors[0]['Key']}").splitlines()]
    assert [list(line) for line in lines] == [["attemptsMade", "arrivalTimestamp", "errorCode", "errorMessage",
                                               "attemptEndingTimestamp", "rawData"]] * 2
    assert lines[0]["errorCode"] == "Iceberg.MalformedFieldWithinRecord" and lines[0]["attemptsMade"] == 1
    assert lines[0]["errorMessage"] == ("Firehose is unable to convert column data in your record to the column "
                                        "type specified within the schema.")
    assert base64.b64decode(lines[0]["rawData"]) == malformed
    assert lines[1]["errorCode"] == "Iceberg.InvalidPayload" and lines[1]["attemptsMade"] == 0


def test_firehose_iceberg_snapshot_summary(fh, glue, s3, glue_iceberg, tmp_path):
    bucket, database, table = glue_iceberg
    name = f"fh-summary-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    created = fh.describe_delivery_stream(DeliveryStreamName=name)["DeliveryStreamDescription"]["CreateTimestamp"]
    for n in range(2):
        fh.put_record(DeliveryStreamName=name, Record={"Data": _ice_record("s1", n)})
    metadata, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    first, second = (s["summary"] for s in metadata["snapshots"])
    account, stream, created_ms = first["FIREHOSE-DELIVERY-STREAM-ID"].split(":")
    assert (account, stream) == ("000000000000", name)
    assert abs(int(created_ms) - created.timestamp() * 1000) < 1000
    assert int(second["FIREHOSE-INTERNAL-CHECKPOINT"].split(",")[0]) == (
        int(first["FIREHOSE-INTERNAL-CHECKPOINT"].split(",")[0]) + 1)
    assert second["total-records"] == "2" and second["added-records"] == "1"
    assert metadata["snapshots"][1]["parent-snapshot-id"] == metadata["snapshots"][0]["snapshot-id"]
    assert len(rows) == 2


def test_firehose_iceberg_failed_commit_keeps_arrival_order(fh, glue, s3, glue_iceberg, tmp_path):
    bucket, database, table = glue_iceberg
    name = f"fh-order-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    entry = glue.get_table(DatabaseName=database, Name=table)["Table"]
    good = entry["Parameters"]["metadata_location"]
    table_input = {"Name": table, "TableType": "EXTERNAL_TABLE", "Parameters": dict(entry["Parameters"])}
    glue.update_table(DatabaseName=database, TableInput={
        **table_input, "Parameters": {**table_input["Parameters"], "metadata_location": good + ".missing"}})
    fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": _ice_record("s1", n)} for n in range(3)])
    glue.update_table(DatabaseName=database, TableInput=table_input)
    assert glue.get_table(DatabaseName=database, Name=table)["Table"]["Parameters"]["metadata_location"] == good

    fh.put_record_batch(DeliveryStreamName=name, Records=[{"Data": _ice_record("s1", n)} for n in range(3, 5)])
    metadata, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    assert [r[3] for r in rows] == [f"r{n}" for n in range(5)]
    assert all(int(s["summary"]["total-records"]) >= 3 for s in metadata["snapshots"])


def test_firehose_iceberg_concurrent_streams_retry_catalog_conflicts(fh, glue, s3, glue_iceberg, tmp_path):
    import concurrent.futures

    bucket, database, table = glue_iceberg
    names = [f"fh-race{i}-{bucket[-8:]}" for i in range(2)]
    for name in names:
        _glue_iceberg_stream(fh, bucket, database, table, name)

    def send(i):
        for n in range(5):
            fh.put_record(DeliveryStreamName=names[i], Record={"Data": _ice_record(f"s{i}", i * 10 + n)})

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        list(pool.map(send, range(2)))
    metadata, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    assert sorted(r[3] for r in rows) == sorted(f"r{i * 10 + n}" for i in range(2) for n in range(5))
    assert len(metadata["snapshots"]) == 10
    for i in range(2):
        own = [r[3] for r in rows if r[2] == f"s{i}"]
        assert own == [f"r{i * 10 + n}" for n in range(5)]


def test_firehose_iceberg_delivery_metrics(fh, cw, glue_iceberg):
    import datetime

    bucket, database, table = glue_iceberg
    name = f"fh-metrics-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=2)
    fh.put_record_batch(DeliveryStreamName=name, Records=[
        {"Data": _ice_record("s1", 1)}, {"Data": _ice_record("s1", 2)},
        {"Data": _ice_record("s1", 3, extracted_at="2026-11-11 14:46:53")}])
    queries = []
    for metric, stat in (("DataFreshness", "Maximum"), ("SuccessfulRowCount", "Sum"), ("FailedRowCount", "Sum")):
        for dims in ([{"Name": "DeliveryStreamName", "Value": name}],
                     [{"Name": "DeliveryStreamName", "Value": name},
                      {"Name": "IcebergTableName", "Value": f"{database}.{table}"}]):
            queries.append({"Id": f"m{len(queries)}", "MetricStat": {"Metric": {
                "Namespace": "AWS/Firehose", "MetricName": f"DeliveryToIceberg.{metric}", "Dimensions": dims},
                "Period": 60, "Stat": stat}})
    result = cw.get_metric_data(MetricDataQueries=queries, StartTime=start,
                                EndTime=start + datetime.timedelta(minutes=4))["MetricDataResults"]
    values = [sum(r["Values"]) for r in result]
    assert all(len(r["Values"]) == 1 for r in result), result
    assert 0 <= values[0] < 60 and values[0] == values[1]
    assert values[2:] == [2, 2, 1, 1]


def test_firehose_iceberg_buffers_until_the_interval(fh, glue, s3, glue_iceberg, tmp_path):
    bucket, database, table = glue_iceberg
    name = f"fh-buffer-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name, interval=2)
    fh.put_record(DeliveryStreamName=name, Record={"Data": _ice_record("s1", 1)})
    fh.put_record(DeliveryStreamName=name, Record={"Data": _ice_record("s1", 2)})
    assert _iceberg_state(s3, glue, database, table, tmp_path)[1] == []
    for _ in range(50):
        metadata, rows = _iceberg_state(s3, glue, database, table, tmp_path)
        if rows:
            break
        time.sleep(0.2)
    assert [r[3] for r in rows] == ["r1", "r2"]
    assert len(metadata["snapshots"]) == 1


def test_firehose_iceberg_unconvertible_values_do_not_block_the_stream(fh, glue, s3, glue_iceberg, tmp_path):
    bucket, database, table = glue_iceberg
    name = f"fh-stuck-{bucket[-8:]}"
    _glue_iceberg_stream(fh, bucket, database, table, name)
    fh.put_record_batch(DeliveryStreamName=name, Records=[
        {"Data": _ice_record("s1", 1)},
        {"Data": json.dumps({"tenant_id": "t1", "extracted_at": 10 ** 20}).encode()},
        {"Data": b'{"tenant_id": "t1", "raw": "\\ud800"}'}])
    _, rows = _iceberg_state(s3, glue, database, table, tmp_path)
    assert [r[3] for r in rows] == ["r1"]
    errors = s3.list_objects_v2(Bucket=bucket, Prefix=f"{name}-errors/")["Contents"]
    lines = _s3_body(s3, f"s3://{bucket}/{errors[0]['Key']}").splitlines()
    assert [json.loads(line)["errorCode"] for line in lines] == ["Iceberg.MalformedFieldWithinRecord"] * 2
