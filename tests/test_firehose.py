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
    import base64 as _b64
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
        try: fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception: pass
        try: lam.delete_function(FunctionName=fn)
        except Exception: pass


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
        try: fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception: pass
        try: lam.delete_function(FunctionName=fn)
        except Exception: pass


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
        try: fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception: pass


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
        try: fh.delete_delivery_stream(DeliveryStreamName=delivery)
        except Exception: pass
        try: lam.delete_function(FunctionName=fn)
        except Exception: pass
        try: kin.delete_stream(StreamName=stream)
        except Exception: pass
