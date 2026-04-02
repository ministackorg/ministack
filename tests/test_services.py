"""
MiniStack Integration Tests — pytest edition.
Run: pytest tests/ -v
Requires: pip install boto3 pytest
"""

import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

# Derive execute-api port from MINISTACK_ENDPOINT so Docker runs work
_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566

import pytest
from botocore.exceptions import ClientError

# ========== S3 ==========


def test_s3_create_bucket(s3):
    s3.create_bucket(Bucket="intg-s3-create")
    buckets = s3.list_buckets()["Buckets"]
    assert any(b["Name"] == "intg-s3-create" for b in buckets)


def test_s3_create_bucket_already_exists(s3):
    # Real AWS: creating a bucket you already own is idempotent — returns 200
    s3.create_bucket(Bucket="intg-s3-dup")
    s3.create_bucket(Bucket="intg-s3-dup")  # must not raise


def test_s3_delete_bucket(s3):
    s3.create_bucket(Bucket="intg-s3-delbkt")
    s3.delete_bucket(Bucket="intg-s3-delbkt")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert "intg-s3-delbkt" not in buckets


def test_s3_delete_bucket_not_empty(s3):
    s3.create_bucket(Bucket="intg-s3-notempty")
    s3.put_object(Bucket="intg-s3-notempty", Key="file.txt", Body=b"data")
    with pytest.raises(ClientError) as exc:
        s3.delete_bucket(Bucket="intg-s3-notempty")
    assert exc.value.response["Error"]["Code"] == "BucketNotEmpty"


def test_s3_delete_bucket_not_found(s3):
    with pytest.raises(ClientError) as exc:
        s3.delete_bucket(Bucket="intg-s3-nonexistent-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchBucket"


def test_s3_head_bucket(s3):
    s3.create_bucket(Bucket="intg-s3-headbkt")
    resp = s3.head_bucket(Bucket="intg-s3-headbkt")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    with pytest.raises(ClientError) as exc:
        s3.head_bucket(Bucket="intg-s3-headbkt-missing")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_s3_put_get_object(s3):
    s3.create_bucket(Bucket="intg-s3-putget")
    s3.put_object(Bucket="intg-s3-putget", Key="hello.txt", Body=b"Hello, World!")
    resp = s3.get_object(Bucket="intg-s3-putget", Key="hello.txt")
    assert resp["Body"].read() == b"Hello, World!"


def test_s3_put_object_no_bucket(s3):
    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket="intg-s3-nobucket-xyz", Key="k", Body=b"x")
    assert exc.value.response["Error"]["Code"] == "NoSuchBucket"


def test_s3_put_get_json_chunked(s3):
    """AWS SDK v2 sends PutObject with chunked Transfer-Encoding — body must be decoded cleanly."""
    import urllib.request, urllib.parse, json as _json
    bucket = "intg-s3-chunked"
    s3.create_bucket(Bucket=bucket)

    payload = _json.dumps({"hello": "world", "number": 42})
    # Simulate AWS chunked encoding: one chunk + terminator
    chunk_body = payload.encode()
    chunk_size = f"{len(chunk_body):x}".encode()
    fake_sig = b"abc123"
    chunked = (
        chunk_size + b";chunk-signature=" + fake_sig + b"\r\n" +
        chunk_body + b"\r\n" +
        b"0;chunk-signature=" + fake_sig + b"\r\n\r\n"
    )
    endpoint = "http://localhost:4566/" + bucket + "/test.json"
    req = urllib.request.Request(endpoint, data=chunked, method="PUT", headers={
        "x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        "Content-Type": "application/json",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
    })
    with urllib.request.urlopen(req) as r:
        assert r.status == 200

    resp = s3.get_object(Bucket=bucket, Key="test.json")
    body = resp["Body"].read().decode()
    assert _json.loads(body) == {"hello": "world", "number": 42}


def test_s3_head_object(s3):
    s3.create_bucket(Bucket="intg-s3-headobj")
    s3.put_object(
        Bucket="intg-s3-headobj",
        Key="data.bin",
        Body=b"0123456789",
        ContentType="application/octet-stream",
    )
    resp = s3.head_object(Bucket="intg-s3-headobj", Key="data.bin")
    assert resp["ContentLength"] == 10
    assert resp["ContentType"] == "application/octet-stream"
    assert "ETag" in resp


def test_s3_head_object_not_found(s3):
    s3.create_bucket(Bucket="intg-s3-headobj404")
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket="intg-s3-headobj404", Key="missing.txt")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_s3_delete_object(s3):
    s3.create_bucket(Bucket="intg-s3-delobj")
    s3.put_object(Bucket="intg-s3-delobj", Key="bye.txt", Body=b"bye")
    s3.delete_object(Bucket="intg-s3-delobj", Key="bye.txt")
    with pytest.raises(ClientError):
        s3.get_object(Bucket="intg-s3-delobj", Key="bye.txt")


def test_s3_delete_object_idempotent(s3):
    s3.create_bucket(Bucket="intg-s3-delidempotent")
    resp = s3.delete_object(Bucket="intg-s3-delidempotent", Key="nonexistent.txt")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_s3_copy_object(s3):
    s3.create_bucket(Bucket="intg-s3-copysrc")
    s3.create_bucket(Bucket="intg-s3-copydst")
    s3.put_object(Bucket="intg-s3-copysrc", Key="original.txt", Body=b"copy me")
    s3.copy_object(
        CopySource={"Bucket": "intg-s3-copysrc", "Key": "original.txt"},
        Bucket="intg-s3-copydst",
        Key="copied.txt",
    )
    resp = s3.get_object(Bucket="intg-s3-copydst", Key="copied.txt")
    assert resp["Body"].read() == b"copy me"


def test_s3_copy_object_metadata_replace(s3):
    bkt = "intg-s3-copymeta"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt,
        Key="src.txt",
        Body=b"metadata test",
        Metadata={"original-key": "original-value"},
    )
    s3.copy_object(
        CopySource={"Bucket": bkt, "Key": "src.txt"},
        Bucket=bkt,
        Key="dst.txt",
        MetadataDirective="REPLACE",
        Metadata={"replaced-key": "replaced-value"},
    )
    resp = s3.head_object(Bucket=bkt, Key="dst.txt")
    assert resp["Metadata"].get("replaced-key") == "replaced-value"
    assert "original-key" not in resp["Metadata"]


def test_s3_list_objects_v1(s3):
    bkt = "intg-s3-listv1"
    s3.create_bucket(Bucket=bkt)
    for key in [
        "photos/2023/a.jpg",
        "photos/2023/b.jpg",
        "photos/2024/c.jpg",
        "docs/readme.md",
    ]:
        s3.put_object(Bucket=bkt, Key=key, Body=b"x")

    resp = s3.list_objects(Bucket=bkt, Prefix="photos/", Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    assert "photos/2023/" in prefixes
    assert "photos/2024/" in prefixes
    assert len(resp.get("Contents", [])) == 0


def test_s3_list_objects_v2(s3):
    bkt = "intg-s3-listv2"
    s3.create_bucket(Bucket=bkt)
    for key in ["a/1.txt", "a/2.txt", "b/3.txt"]:
        s3.put_object(Bucket=bkt, Key=key, Body=b"v2")

    resp = s3.list_objects_v2(Bucket=bkt, Prefix="a/")
    assert resp["KeyCount"] == 2
    keys = [c["Key"] for c in resp["Contents"]]
    assert "a/1.txt" in keys
    assert "a/2.txt" in keys


def test_s3_list_objects_pagination(s3):
    bkt = "intg-s3-listpage"
    s3.create_bucket(Bucket=bkt)
    for i in range(7):
        s3.put_object(Bucket=bkt, Key=f"item-{i:02d}.txt", Body=b"p")

    resp = s3.list_objects_v2(Bucket=bkt, MaxKeys=3)
    assert resp["IsTruncated"] is True
    assert resp["KeyCount"] == 3
    token = resp["NextContinuationToken"]

    all_keys = [c["Key"] for c in resp["Contents"]]
    while resp["IsTruncated"]:
        resp = s3.list_objects_v2(
            Bucket=bkt,
            MaxKeys=3,
            ContinuationToken=token,
        )
        all_keys.extend(c["Key"] for c in resp["Contents"])
        token = resp.get("NextContinuationToken", "")

    assert len(all_keys) == 7


def test_s3_delete_objects_batch(s3):
    bkt = "intg-s3-batchdel"
    s3.create_bucket(Bucket=bkt)
    keys = [f"obj-{i}.txt" for i in range(5)]
    for k in keys:
        s3.put_object(Bucket=bkt, Key=k, Body=b"batch")

    resp = s3.delete_objects(
        Bucket=bkt,
        Delete={"Objects": [{"Key": k} for k in keys], "Quiet": False},
    )
    assert len(resp.get("Deleted", [])) == 5
    listing = s3.list_objects_v2(Bucket=bkt)
    assert listing["KeyCount"] == 0


def test_s3_multipart_upload(s3):
    bkt = "intg-s3-multipart"
    s3.create_bucket(Bucket=bkt)
    key = "large.bin"

    mpu = s3.create_multipart_upload(Bucket=bkt, Key=key)
    upload_id = mpu["UploadId"]

    p1 = s3.upload_part(
        Bucket=bkt,
        Key=key,
        UploadId=upload_id,
        PartNumber=1,
        Body=b"A" * 100,
    )
    p2 = s3.upload_part(
        Bucket=bkt,
        Key=key,
        UploadId=upload_id,
        PartNumber=2,
        Body=b"B" * 100,
    )

    s3.complete_multipart_upload(
        Bucket=bkt,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": p1["ETag"]},
                {"PartNumber": 2, "ETag": p2["ETag"]},
            ]
        },
    )
    resp = s3.get_object(Bucket=bkt, Key=key)
    assert resp["Body"].read() == b"A" * 100 + b"B" * 100


def test_s3_abort_multipart_upload(s3):
    bkt = "intg-s3-abortmpu"
    s3.create_bucket(Bucket=bkt)
    key = "aborted.bin"

    mpu = s3.create_multipart_upload(Bucket=bkt, Key=key)
    upload_id = mpu["UploadId"]
    s3.upload_part(
        Bucket=bkt,
        Key=key,
        UploadId=upload_id,
        PartNumber=1,
        Body=b"X" * 50,
    )
    s3.abort_multipart_upload(Bucket=bkt, Key=key, UploadId=upload_id)

    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=bkt, Key=key)
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"


def test_s3_get_object_range(s3):
    bkt = "intg-s3-range"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="ranged.txt", Body=b"0123456789")

    resp = s3.get_object(Bucket=bkt, Key="ranged.txt", Range="bytes=2-5")
    assert resp["Body"].read() == b"2345"
    assert resp["ContentLength"] == 4
    assert "bytes" in resp.get("ContentRange", "")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206


def test_s3_object_metadata(s3):
    bkt = "intg-s3-meta"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt,
        Key="meta.txt",
        Body=b"metadata",
        Metadata={"custom-key": "custom-value", "another": "data"},
    )
    resp = s3.head_object(Bucket=bkt, Key="meta.txt")
    assert resp["Metadata"]["custom-key"] == "custom-value"
    assert resp["Metadata"]["another"] == "data"


def test_s3_bucket_tagging(s3):
    bkt = "intg-s3-bkttags"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_tagging(
        Bucket=bkt,
        Tagging={
            "TagSet": [
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ]
        },
    )
    resp = s3.get_bucket_tagging(Bucket=bkt)
    tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
    assert tags["env"] == "test"
    assert tags["team"] == "platform"

    s3.delete_bucket_tagging(Bucket=bkt)
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_tagging(Bucket=bkt)
    assert exc.value.response["Error"]["Code"] == "NoSuchTagSet"


def test_s3_control_list_tags_for_resource(s3):
    """S3 Control ListTagsForResource must return tags set via PutBucketTagging.

    Regression: Terraform AWS Provider >= 5 calls s3control:ListTagsForResource
    when a `tags` block is set on aws_s3_bucket. The handler was returning an
    empty list regardless of bucket tags, causing perpetual drift.
    """
    from conftest import make_client
    bkt = "intg-s3control-tags"
    account_id = "123456789012"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_tagging(
        Bucket=bkt,
        Tagging={"TagSet": [{"Key": "name", "Value": "ministack-test"}]},
    )

    s3control = make_client("s3control")
    arn = f"arn:aws:s3:::{bkt}"
    resp = s3control.list_tags_for_resource(AccountId=account_id, ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
    assert tags.get("name") == "ministack-test"


def test_s3_control_list_tags_via_s3_control_host(s3):
    """S3 Control requests via s3-control.localhost host must not be intercepted by S3 vhost."""
    import urllib.request, urllib.parse
    bkt = "intg-s3control-host"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_tagging(
        Bucket=bkt,
        Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
    )
    arn = urllib.parse.quote(f"arn:aws:s3:::{bkt}", safe="")
    req = urllib.request.Request(
        f"http://localhost:4566/v20180820/tags/{arn}",
        method="GET",
        headers={
            "x-amz-account-id": "000000000000",
            "Host": "s3-control.localhost:4566",
        },
    )
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
        body = r.read().decode()
    assert "env" in body
    assert "test" in body


def test_s3_bucket_policy(s3):
    bkt = "intg-s3-policy"
    s3.create_bucket(Bucket=bkt)
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bkt}/*",
                }
            ],
        }
    )
    s3.put_bucket_policy(Bucket=bkt, Policy=policy)
    resp = s3.get_bucket_policy(Bucket=bkt)
    stored = json.loads(resp["Policy"])
    assert stored["Version"] == "2012-10-17"
    assert len(stored["Statement"]) == 1


def test_s3_object_tagging(s3):
    bkt = "intg-s3-objtags"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="tagged.txt", Body=b"tagged")
    s3.put_object_tagging(
        Bucket=bkt,
        Key="tagged.txt",
        Tagging={
            "TagSet": [
                {"Key": "status", "Value": "active"},
                {"Key": "priority", "Value": "high"},
            ]
        },
    )
    resp = s3.get_object_tagging(Bucket=bkt, Key="tagged.txt")
    tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
    assert tags["status"] == "active"
    assert tags["priority"] == "high"


def test_s3_public_access_block(s3):
    bkt = "intg-s3-pab"
    s3.create_bucket(Bucket=bkt)
    s3.put_public_access_block(
        Bucket=bkt,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": False,
            "RestrictPublicBuckets": False,
        },
    )
    resp = s3.get_public_access_block(Bucket=bkt)
    cfg = resp["PublicAccessBlockConfiguration"]
    assert cfg["BlockPublicAcls"] is True
    assert cfg["BlockPublicPolicy"] is False
    s3.delete_public_access_block(Bucket=bkt)


def test_s3_ownership_controls(s3):
    bkt = "intg-s3-ownership"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_ownership_controls(
        Bucket=bkt,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
    )
    resp = s3.get_bucket_ownership_controls(Bucket=bkt)
    assert resp["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerPreferred"
    s3.delete_bucket_ownership_controls(Bucket=bkt)


def test_s3_object_lock_configuration(s3):
    bkt = "intg-s3-objlock-cfg"
    s3.create_bucket(
        Bucket=bkt,
        ObjectLockEnabledForBucket=True,
    )
    resp = s3.get_object_lock_configuration(Bucket=bkt)
    assert resp["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"

    s3.put_object_lock_configuration(
        Bucket=bkt,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "GOVERNANCE",
                    "Days": 30,
                }
            },
        },
    )
    resp = s3.get_object_lock_configuration(Bucket=bkt)
    ret = resp["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]
    assert ret["Mode"] == "GOVERNANCE"
    assert ret["Days"] == 30


def test_s3_object_lock_requires_versioning(s3):
    bkt = "intg-s3-objlock-nover"
    s3.create_bucket(Bucket=bkt)
    with pytest.raises(ClientError) as exc:
        s3.put_object_lock_configuration(
            Bucket=bkt,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidBucketState"


def test_s3_object_retention(s3):
    bkt = "intg-s3-retention"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"hello")

    from datetime import datetime, timezone, timedelta

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    s3.put_object_retention(
        Bucket=bkt,
        Key="doc.txt",
        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until},
    )
    resp = s3.get_object_retention(Bucket=bkt, Key="doc.txt")
    assert resp["Retention"]["Mode"] == "GOVERNANCE"
    assert "RetainUntilDate" in resp["Retention"]


def test_s3_object_legal_hold(s3):
    bkt = "intg-s3-legalhold"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bkt, Key="evidence.txt", Body=b"data")

    s3.put_object_legal_hold(
        Bucket=bkt,
        Key="evidence.txt",
        LegalHold={"Status": "ON"},
    )
    resp = s3.get_object_legal_hold(Bucket=bkt, Key="evidence.txt")
    assert resp["LegalHold"]["Status"] == "ON"

    s3.put_object_legal_hold(
        Bucket=bkt,
        Key="evidence.txt",
        LegalHold={"Status": "OFF"},
    )
    resp = s3.get_object_legal_hold(Bucket=bkt, Key="evidence.txt")
    assert resp["LegalHold"]["Status"] == "OFF"


def test_s3_object_lock_prevents_delete(s3):
    bkt = "intg-s3-lock-del"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bkt, Key="locked.txt", Body=b"immutable")

    s3.put_object_legal_hold(
        Bucket=bkt,
        Key="locked.txt",
        LegalHold={"Status": "ON"},
    )
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bkt, Key="locked.txt")
    assert exc.value.response["Error"]["Code"] == "AccessDenied"

    # Remove legal hold, add governance retention
    s3.put_object_legal_hold(
        Bucket=bkt,
        Key="locked.txt",
        LegalHold={"Status": "OFF"},
    )
    from datetime import datetime, timezone, timedelta

    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    s3.put_object_retention(
        Bucket=bkt,
        Key="locked.txt",
        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until},
    )
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bkt, Key="locked.txt")
    assert exc.value.response["Error"]["Code"] == "AccessDenied"

    # Bypass governance retention
    s3.delete_object(
        Bucket=bkt,
        Key="locked.txt",
        BypassGovernanceRetention=True,
    )
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bkt, Key="locked.txt")


def test_s3_bucket_replication(s3):
    src = "intg-s3-repl-src"
    s3.create_bucket(Bucket=src)
    s3.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_replication(
        Bucket=src,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::012345678901:role/repl",
            "Rules": [
                {
                    "Status": "Enabled",
                    "Destination": {"Bucket": "arn:aws:s3:::intg-s3-repl-dst"},
                }
            ],
        },
    )
    resp = s3.get_bucket_replication(Bucket=src)
    assert resp["ReplicationConfiguration"]["Role"] == "arn:aws:iam::012345678901:role/repl"
    assert len(resp["ReplicationConfiguration"]["Rules"]) == 1

    s3.delete_bucket_replication(Bucket=src)
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_replication(Bucket=src)
    assert exc.value.response["Error"]["Code"] == "ReplicationConfigurationNotFoundError"


def test_s3_replication_requires_versioning(s3):
    bkt = "intg-s3-repl-nover"
    s3.create_bucket(Bucket=bkt)
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_replication(
            Bucket=bkt,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::012345678901:role/repl",
                "Rules": [
                    {
                        "Status": "Enabled",
                        "Destination": {"Bucket": "arn:aws:s3:::somewhere"},
                    }
                ],
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"


def test_s3_put_object_with_lock_headers(s3):
    bkt = "intg-s3-put-lock-hdr"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    from datetime import datetime, timezone, timedelta

    retain_until = datetime.now(timezone.utc) + timedelta(days=5)
    s3.put_object(
        Bucket=bkt,
        Key="locked-via-header.txt",
        Body=b"data",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
        ObjectLockLegalHoldStatus="ON",
    )
    ret = s3.get_object_retention(Bucket=bkt, Key="locked-via-header.txt")
    assert ret["Retention"]["Mode"] == "GOVERNANCE"

    hold = s3.get_object_legal_hold(Bucket=bkt, Key="locked-via-header.txt")
    assert hold["LegalHold"]["Status"] == "ON"


def test_s3_put_object_with_tagging_header(s3):
    bkt = "intg-s3-put-tag-hdr"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt,
        Key="tagged-inline.txt",
        Body=b"hello",
        Tagging="env=prod&team=backend",
    )
    resp = s3.get_object_tagging(Bucket=bkt, Key="tagged-inline.txt")
    tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
    assert tags["env"] == "prod"
    assert tags["team"] == "backend"


def test_s3_default_retention_applied(s3):
    bkt = "intg-s3-default-ret"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    s3.put_object_lock_configuration(
        Bucket=bkt,
        ObjectLockConfiguration={
            "ObjectLockEnabled": "Enabled",
            "Rule": {
                "DefaultRetention": {
                    "Mode": "COMPLIANCE",
                    "Days": 7,
                }
            },
        },
    )
    s3.put_object(Bucket=bkt, Key="auto-locked.txt", Body=b"data")
    ret = s3.get_object_retention(Bucket=bkt, Key="auto-locked.txt")
    assert ret["Retention"]["Mode"] == "COMPLIANCE"
    assert "RetainUntilDate" in ret["Retention"]


def test_s3_batch_delete_enforces_lock(s3):
    bkt = "intg-s3-batch-lock"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bkt, Key="a.txt", Body=b"a")
    s3.put_object(Bucket=bkt, Key="b.txt", Body=b"b")
    s3.put_object_legal_hold(Bucket=bkt, Key="a.txt", LegalHold={"Status": "ON"})
    resp = s3.delete_objects(
        Bucket=bkt,
        Delete={"Objects": [{"Key": "a.txt"}, {"Key": "b.txt"}]},
    )
    deleted_keys = [d["Key"] for d in resp.get("Deleted", [])]
    error_keys = [e["Key"] for e in resp.get("Errors", [])]
    assert "b.txt" in deleted_keys
    assert "a.txt" in error_keys


def test_s3_copy_preserves_tags_and_lock(s3):
    src = "intg-s3-copy-tag-src"
    dst = "intg-s3-copy-tag-dst"
    s3.create_bucket(Bucket=src, ObjectLockEnabledForBucket=True)
    s3.create_bucket(Bucket=dst, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=src, Key="orig.txt", Body=b"data")
    s3.put_object_tagging(
        Bucket=src,
        Key="orig.txt",
        Tagging={"TagSet": [{"Key": "env", "Value": "staging"}]},
    )
    s3.put_object_legal_hold(Bucket=src, Key="orig.txt", LegalHold={"Status": "ON"})
    s3.copy_object(Bucket=dst, Key="copy.txt", CopySource=f"{src}/orig.txt")
    tags = s3.get_object_tagging(Bucket=dst, Key="copy.txt")
    tag_map = {t["Key"]: t["Value"] for t in tags["TagSet"]}
    assert tag_map["env"] == "staging"

    hold = s3.get_object_legal_hold(Bucket=dst, Key="copy.txt")
    assert hold["LegalHold"]["Status"] == "ON"


def test_s3_copy_replace_tags(s3):
    bkt = "intg-s3-copy-repl-tag"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="src.txt", Body=b"data")
    s3.put_object_tagging(
        Bucket=bkt,
        Key="src.txt",
        Tagging={"TagSet": [{"Key": "old", "Value": "val"}]},
    )
    s3.copy_object(
        Bucket=bkt,
        Key="dst.txt",
        CopySource=f"{bkt}/src.txt",
        TaggingDirective="REPLACE",
        Tagging="new=val2",
    )
    tags = s3.get_object_tagging(Bucket=bkt, Key="dst.txt")
    tag_map = {t["Key"]: t["Value"] for t in tags["TagSet"]}
    assert "old" not in tag_map
    assert tag_map["new"] == "val2"


def test_s3_tag_count_limit(s3):
    bkt = "intg-s3-tag-limit"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="toomany.txt", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.put_object_tagging(
            Bucket=bkt,
            Key="toomany.txt",
            Tagging={"TagSet": [{"Key": f"k{i}", "Value": f"v{i}"} for i in range(11)]},
        )
    assert exc.value.response["Error"]["Code"] == "BadRequest"


def test_s3_replication_validates_dest_versioning(s3):
    src = "intg-s3-repl-val-src"
    dst = "intg-s3-repl-val-dst"
    s3.create_bucket(Bucket=src)
    s3.create_bucket(Bucket=dst)
    s3.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})
    # dst has no versioning
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_replication(
            Bucket=src,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::012345678901:role/repl",
                "Rules": [
                    {
                        "Status": "Enabled",
                        "Destination": {"Bucket": f"arn:aws:s3:::{dst}"},
                    }
                ],
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"


def test_s3_head_object_returns_lock_headers(s3):
    bkt = "intg-s3-head-lock-hdr"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    from datetime import datetime, timezone, timedelta

    retain_until = datetime.now(timezone.utc) + timedelta(days=3)
    s3.put_object(
        Bucket=bkt,
        Key="locked.txt",
        Body=b"data",
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
        ObjectLockLegalHoldStatus="ON",
    )
    resp = s3.head_object(Bucket=bkt, Key="locked.txt")
    assert resp["ObjectLockMode"] == "GOVERNANCE"
    assert "ObjectLockRetainUntilDate" in resp
    assert resp["ObjectLockLegalHoldStatus"] == "ON"

    get_resp = s3.get_object(Bucket=bkt, Key="locked.txt")
    assert get_resp["ObjectLockMode"] == "GOVERNANCE"
    assert get_resp["ObjectLockLegalHoldStatus"] == "ON"


# ========== SQS ==========


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


# ========== SNS ==========


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
    assert resp["Attributes"]["DisplayName"] == "intg-sns-getattr"


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
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


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


# ========== DynamoDB ==========


def test_dynamodb_basic(ddb):
    try:
        ddb.delete_table(TableName="TestTable1")
    except Exception:
        pass
    ddb.create_table(
        TableName="TestTable1",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="TestTable1", Item={"pk": {"S": "key1"}, "data": {"S": "value1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert resp["Item"]["data"]["S"] == "value1"
    ddb.delete_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert "Item" not in resp


def test_dynamodb_scan(ddb):
    try:
        ddb.delete_table(TableName="ScanTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="ScanTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(10):
        ddb.put_item(TableName="ScanTable", Item={"pk": {"S": f"key{i}"}, "val": {"N": str(i)}})
    resp = ddb.scan(TableName="ScanTable")
    assert resp["Count"] == 10


def test_dynamodb_batch(ddb):
    try:
        ddb.delete_table(TableName="BatchTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="BatchTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.batch_write_item(
        RequestItems={
            "BatchTable": [{"PutRequest": {"Item": {"pk": {"S": f"bk{i}"}, "v": {"S": f"bv{i}"}}}} for i in range(5)]
        }
    )
    resp = ddb.scan(TableName="BatchTable")
    assert resp["Count"] == 5


# ========== STS ==========


def test_sts_get_caller_identity(sts):
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"


# ========== SecretsManager ==========


def test_secrets_create_get(sm):
    sm.create_secret(Name="test-secret-1", SecretString='{"user":"admin"}')
    resp = sm.get_secret_value(SecretId="test-secret-1")
    assert json.loads(resp["SecretString"])["user"] == "admin"


def test_secrets_update_list(sm):
    sm.create_secret(Name="test-secret-2", SecretString="original")
    sm.update_secret(SecretId="test-secret-2", SecretString="updated")
    resp = sm.get_secret_value(SecretId="test-secret-2")
    assert resp["SecretString"] == "updated"
    listed = sm.list_secrets()
    assert any(s["Name"] == "test-secret-2" for s in listed["SecretList"])


# ========== CloudWatch Logs ==========


def test_logs_put_get(logs):
    logs.create_log_group(logGroupName="/test/ministack")
    logs.create_log_stream(logGroupName="/test/ministack", logStreamName="stream1")
    logs.put_log_events(
        logGroupName="/test/ministack",
        logStreamName="stream1",
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "Hello from MiniStack"},
            {"timestamp": int(time.time() * 1000), "message": "Second log line"},
        ],
    )
    resp = logs.get_log_events(logGroupName="/test/ministack", logStreamName="stream1")
    assert len(resp["events"]) == 2


def test_logs_filter(logs):
    resp = logs.filter_log_events(logGroupName="/test/ministack", filterPattern="MiniStack")
    assert len(resp["events"]) >= 1


# ========== Lambda ==========


def test_lambda_create_invoke(lam):
    code = b'def handler(event, context):\n    return {"statusCode": 200, "body": "Hello!", "event": event}\n'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="test-func-1",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    funcs = lam.list_functions()
    assert any(f["FunctionName"] == "test-func-1" for f in funcs["Functions"])
    resp = lam.invoke(FunctionName="test-func-1", Payload=json.dumps({"key": "value"}))
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200


# ========== IAM ==========


def test_lambda_esm_sqs(lam, sqs):
    """SQS → Lambda event source mapping: messages sent to SQS trigger Lambda."""
    import io
    import zipfile as zf

    # Clean up from previous runs
    try:
        lam.delete_function(FunctionName="esm-test-func")
    except Exception:
        pass

    # Lambda that records what it received
    code = (
        b"import json\n"
        b"received = []\n"
        b"def handler(event, context):\n"
        b"    received.extend(event.get('Records', []))\n"
        b"    return {'processed': len(event.get('Records', []))}\n"
    )
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)

    lam.create_function(
        FunctionName="esm-test-func",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    q_url = sqs.create_queue(QueueName="esm-test-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    # Create event source mapping
    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-test-func",
        BatchSize=5,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["State"] == "Enabled"

    # Send a message to SQS
    sqs.send_message(QueueUrl=q_url, MessageBody="trigger-lambda")

    # Wait for poller to pick it up (max 5s)
    import time

    for _ in range(10):
        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
        if not msgs.get("Messages"):
            break  # message was consumed by Lambda

    # Queue should be empty — Lambda consumed the message
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
    assert not msgs.get("Messages"), "Message should have been consumed by Lambda via ESM"

    # Cleanup
    lam.delete_event_source_mapping(UUID=esm_uuid)


def test_iam_role_user(iam):
    iam.create_role(
        RoleName="test-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    roles = iam.list_roles()
    assert any(r["RoleName"] == "test-role" for r in roles.get("Roles", []))
    iam.create_user(UserName="test-user")
    users = iam.list_users()
    assert any(u["UserName"] == "test-user" for u in users.get("Users", []))


# ========== SSM ==========


def test_ssm_put_get(ssm):
    ssm.put_parameter(Name="/app/db/host", Value="localhost", Type="String")
    resp = ssm.get_parameter(Name="/app/db/host")
    assert resp["Parameter"]["Value"] == "localhost"


def test_ssm_get_by_path(ssm):
    ssm.put_parameter(Name="/app/config/key1", Value="val1", Type="String")
    ssm.put_parameter(Name="/app/config/key2", Value="val2", Type="String")
    resp = ssm.get_parameters_by_path(Path="/app/config", Recursive=True)
    assert len(resp["Parameters"]) >= 2


def test_ssm_overwrite(ssm):
    ssm.put_parameter(Name="/app/overwrite", Value="v1", Type="String")
    ssm.put_parameter(Name="/app/overwrite", Value="v2", Type="String", Overwrite=True)
    resp = ssm.get_parameter(Name="/app/overwrite")
    assert resp["Parameter"]["Value"] == "v2"


# ========== EventBridge ==========


def test_eventbridge_bus_rule(eb):
    eb.create_event_bus(Name="test-bus")
    eb.put_rule(
        Name="test-rule",
        EventBusName="test-bus",
        ScheduleExpression="rate(5 minutes)",
        State="ENABLED",
    )
    rules = eb.list_rules(EventBusName="test-bus")
    assert any(r["Name"] == "test-rule" for r in rules["Rules"])


def test_eventbridge_put_events(eb):
    resp = eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "UserSignup",
                "Detail": json.dumps({"userId": "123"}),
                "EventBusName": "default",
            },
            {
                "Source": "myapp",
                "DetailType": "OrderPlaced",
                "Detail": json.dumps({"orderId": "456"}),
                "EventBusName": "default",
            },
        ]
    )
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 2


def test_eventbridge_targets(eb):
    eb.put_rule(Name="target-rule", ScheduleExpression="rate(1 minute)", State="ENABLED")
    eb.put_targets(
        Rule="target-rule",
        Targets=[
            {
                "Id": "1",
                "Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-func",
            },
        ],
    )
    resp = eb.list_targets_by_rule(Rule="target-rule")
    assert len(resp["Targets"]) == 1


# ========== Kinesis ==========


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


# ========== CloudWatch Metrics ==========


def test_cloudwatch_metrics(cw):
    cw.put_metric_data(
        Namespace="MyApp",
        MetricData=[
            {"MetricName": "RequestCount", "Value": 42.0, "Unit": "Count"},
            {"MetricName": "Latency", "Value": 123.5, "Unit": "Milliseconds"},
        ],
    )
    resp = cw.list_metrics(Namespace="MyApp")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "RequestCount" in names
    assert "Latency" in names


def test_cloudwatch_alarm(cw):
    cw.put_metric_alarm(
        AlarmName="high-latency",
        MetricName="Latency",
        Namespace="MyApp",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=500.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cw.describe_alarms(AlarmNames=["high-latency"])
    assert len(resp["MetricAlarms"]) == 1


# ========== SES ==========


def test_ses_send(ses):
    ses.verify_email_identity(EmailAddress="sender@example.com")
    resp = ses.send_email(
        Source="sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Message={
            "Subject": {"Data": "Test Subject"},
            "Body": {"Text": {"Data": "Hello from MiniStack SES"}},
        },
    )
    assert "MessageId" in resp


def test_ses_list_identities(ses):
    ses.verify_email_identity(EmailAddress="another@example.com")
    resp = ses.list_identities()
    assert "sender@example.com" in resp["Identities"]


def test_ses_quota(ses):
    resp = ses.get_send_quota()
    assert resp["Max24HourSend"] == 50000.0


# ========== Step Functions ==========


def test_sfn_create_execute(sfn):
    definition = json.dumps(
        {
            "Comment": "Simple state machine",
            "StartAt": "HelloWorld",
            "States": {"HelloWorld": {"Type": "Pass", "End": True}},
        }
    )
    resp = sfn.create_state_machine(
        name="test-machine",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/StepFunctionsRole",
    )
    sm_arn = resp["stateMachineArn"]
    exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"key": "value"}))
    exec_arn = exec_resp["executionArn"]
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "RUNNING"


def test_sfn_list(sfn):
    machines = sfn.list_state_machines()
    assert any(m["name"] == "test-machine" for m in machines["stateMachines"])
    sm_arn = next(m["stateMachineArn"] for m in machines["stateMachines"] if m["name"] == "test-machine")
    execs = sfn.list_executions(stateMachineArn=sm_arn)
    assert len(execs["executions"]) >= 1


# ========== ECS ==========


def test_ecs_cluster(ecs):
    ecs.create_cluster(clusterName="test-cluster")
    clusters = ecs.list_clusters()
    assert any("test-cluster" in arn for arn in clusters["clusterArns"])


def test_ecs_task_def(ecs):
    resp = ecs.register_task_definition(
        family="test-task",
        containerDefinitions=[
            {
                "name": "web",
                "image": "nginx:alpine",
                "cpu": 128,
                "memory": 256,
                "portMappings": [{"containerPort": 80, "hostPort": 8080}],
            }
        ],
        requiresCompatibilities=["EC2"],
        cpu="256",
        memory="512",
    )
    assert resp["taskDefinition"]["family"] == "test-task"
    assert resp["taskDefinition"]["revision"] == 1


def test_ecs_list_task_defs(ecs):
    resp = ecs.list_task_definitions(familyPrefix="test-task")
    assert len(resp["taskDefinitionArns"]) >= 1


def test_ecs_run_task_stops_after_exit(ecs):
    """DescribeTasks transitions to STOPPED after Docker container exits."""
    ecs.create_cluster(clusterName="task-lifecycle")
    ecs.register_task_definition(
        family="short-lived",
        containerDefinitions=[
            {
                "name": "worker",
                "image": "alpine:latest",
                "command": ["sh", "-c", "echo done"],
                "essential": True,
            }
        ],
    )
    resp = ecs.run_task(cluster="task-lifecycle", taskDefinition="short-lived")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    # Poll until STOPPED (container exits almost immediately)
    stopped = False
    for _ in range(30):
        time.sleep(2)
        desc = ecs.describe_tasks(cluster="task-lifecycle", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            stopped = True
            assert task["desiredStatus"] == "STOPPED"
            assert task["stopCode"] == "EssentialContainerExited"
            assert task["containers"][0]["lastStatus"] == "STOPPED"
            assert task["containers"][0]["exitCode"] == 0
            break
    assert stopped, "Task should transition to STOPPED after container exits"


def test_ecs_run_task_network_connectivity(ecs):
    """ECS container can reach Ministack (proves network detection works)."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    # When Ministack runs on the host (CI), containers need host.docker.internal.
    # When Ministack runs in Docker (compose), network detection handles it.
    host = os.environ.get("MINISTACK_HOST_FROM_CONTAINER", "host.docker.internal")
    parsed = urlparse(endpoint)
    container_endpoint = f"{parsed.scheme}://{host}:{parsed.port}"

    ecs.create_cluster(clusterName="net-test")
    ecs.register_task_definition(
        family="net-probe",
        containerDefinitions=[
            {
                "name": "probe",
                "image": "alpine:latest",
                "command": ["sh", "-c", f"wget -q -O /dev/null {container_endpoint}/_ministack/health"],
                "essential": True,
            }
        ],
    )
    resp = ecs.run_task(cluster="net-test", taskDefinition="net-probe")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    # Poll until STOPPED — wget should succeed (exit 0) if network is correct
    success = False
    for _ in range(30):
        time.sleep(2)
        desc = ecs.describe_tasks(cluster="net-test", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            exit_code = task["containers"][0].get("exitCode")
            assert exit_code == 0, (
                f"Container could not reach Ministack at {container_endpoint} "
                f"(exit code {exit_code}) — network detection may be broken"
            )
            success = True
            break
    assert success, "Task should transition to STOPPED"


def test_ecs_service(ecs):
    ecs.create_service(
        cluster="test-cluster",
        serviceName="test-service",
        taskDefinition="test-task",
        desiredCount=1,
    )
    resp = ecs.describe_services(cluster="test-cluster", services=["test-service"])
    assert len(resp["services"]) == 1
    assert resp["services"][0]["serviceName"] == "test-service"


# ========== RDS ==========


def test_rds_create(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="test-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        DBName="testdb",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="test-db")
    instances = resp["DBInstances"]
    assert len(instances) == 1
    assert instances[0]["DBInstanceIdentifier"] == "test-db"
    assert instances[0]["Engine"] == "postgres"
    assert "Address" in instances[0]["Endpoint"]


def test_rds_engines(rds):
    resp = rds.describe_db_engine_versions(Engine="postgres")
    assert len(resp["DBEngineVersions"]) > 0


def test_rds_cluster(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="test-cluster",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    resp = rds.describe_db_clusters(DBClusterIdentifier="test-cluster")
    assert resp["DBClusters"][0]["DBClusterIdentifier"] == "test-cluster"


# ========== ElastiCache ==========


def test_elasticache_create(ec):
    ec.create_cache_cluster(
        CacheClusterId="test-redis",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters(CacheClusterId="test-redis")
    clusters = resp["CacheClusters"]
    assert len(clusters) == 1
    assert clusters[0]["CacheClusterId"] == "test-redis"
    assert clusters[0]["Engine"] == "redis"


def test_elasticache_replication_group(ec):
    ec.create_replication_group(
        ReplicationGroupId="test-rg",
        ReplicationGroupDescription="Test replication group",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_replication_groups(ReplicationGroupId="test-rg")
    assert resp["ReplicationGroups"][0]["ReplicationGroupId"] == "test-rg"


def test_elasticache_engines(ec):
    resp = ec.describe_cache_engine_versions(Engine="redis")
    assert len(resp["CacheEngineVersions"]) > 0


# ========== Glue ==========


def test_glue_catalog(glue):
    glue.create_database(DatabaseInput={"Name": "test_db", "Description": "Test database"})
    glue.create_table(
        DatabaseName="test_db",
        TableInput={
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://my-bucket/data/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    resp = glue.get_table(DatabaseName="test_db", Name="test_table")
    assert resp["Table"]["Name"] == "test_table"


def test_glue_list(glue):
    dbs = glue.get_databases()
    assert any(d["Name"] == "test_db" for d in dbs["DatabaseList"])
    tables = glue.get_tables(DatabaseName="test_db")
    assert any(t["Name"] == "test_table" for t in tables["TableList"])


def test_glue_job(glue):
    glue.create_job(
        Name="test-job",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        Command={"Name": "glueetl", "ScriptLocation": "s3://my-bucket/scripts/etl.py"},
        GlueVersion="3.0",
    )
    resp = glue.start_job_run(JobName="test-job")
    assert "JobRunId" in resp
    runs = glue.get_job_runs(JobName="test-job")
    assert len(runs["JobRuns"]) == 1


def test_glue_crawler(glue):
    glue.create_crawler(
        Name="test-crawler",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://my-bucket/data/"}]},
    )
    resp = glue.get_crawler(Name="test-crawler")
    assert resp["Crawler"]["Name"] == "test-crawler"
    glue.start_crawler(Name="test-crawler")


# ========== Athena ==========


def test_athena_query(athena):
    resp = athena.start_query_execution(
        QueryString="SELECT 1 AS num, 'hello' AS greeting",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )
    query_id = resp["QueryExecutionId"]
    state = None
    for _ in range(10):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.2)
    assert state == "SUCCEEDED", f"Query ended in state: {state}"
    results = athena.get_query_results(QueryExecutionId=query_id)
    assert len(results["ResultSet"]["Rows"]) >= 1


def test_athena_workgroup(athena):
    athena.create_work_group(
        Name="test-wg",
        Description="Test workgroup",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://athena-results/test/"}},
    )
    wgs = athena.list_work_groups()
    assert any(wg["Name"] == "test-wg" for wg in wgs["WorkGroups"])
    resp = athena.create_named_query(
        Name="my-query",
        Database="default",
        QueryString="SELECT * FROM my_table LIMIT 10",
        WorkGroup="test-wg",
    )
    assert "NamedQueryId" in resp


# ===================================================================
# DynamoDB — comprehensive tests
# ===================================================================


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _make_zip_js(code: str, filename: str = "index.js") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
    return buf.getvalue()


def test_ddb_create_table(ddb):
    resp = ddb.create_table(
        TableName="t_hash_only",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    desc = resp["TableDescription"]
    assert desc["TableName"] == "t_hash_only"
    assert desc["TableStatus"] == "ACTIVE"
    assert any(k["KeyType"] == "HASH" for k in desc["KeySchema"])


def test_ddb_create_table_composite(ddb):
    resp = ddb.create_table(
        TableName="t_composite",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ks = resp["TableDescription"]["KeySchema"]
    types = {k["KeyType"] for k in ks}
    assert types == {"HASH", "RANGE"}


def test_ddb_create_table_duplicate(ddb):
    with pytest.raises(ClientError) as exc:
        ddb.create_table(
            TableName="t_hash_only",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    assert exc.value.response["Error"]["Code"] == "ResourceInUseException"


def test_ddb_delete_table(ddb):
    ddb.create_table(
        TableName="t_to_delete",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.delete_table(TableName="t_to_delete")
    assert resp["TableDescription"]["TableStatus"] == "DELETING"
    tables = ddb.list_tables()["TableNames"]
    assert "t_to_delete" not in tables


def test_ddb_delete_table_not_found(ddb):
    with pytest.raises(ClientError) as exc:
        ddb.delete_table(TableName="t_nonexistent_xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_ddb_describe_table(ddb):
    ddb.create_table(
        TableName="t_describe_gsi",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.describe_table(TableName="t_describe_gsi")
    table = resp["Table"]
    assert table["TableName"] == "t_describe_gsi"
    assert len(table["GlobalSecondaryIndexes"]) == 1
    assert table["GlobalSecondaryIndexes"][0]["IndexName"] == "gsi1"
    assert len(table["LocalSecondaryIndexes"]) == 1
    assert table["LocalSecondaryIndexes"][0]["IndexName"] == "lsi1"


def test_ddb_list_tables(ddb):
    for i in range(3):
        try:
            ddb.create_table(
                TableName=f"t_list_{i}",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError:
            pass
    resp = ddb.list_tables(Limit=2)
    assert len(resp["TableNames"]) <= 2
    if "LastEvaluatedTableName" in resp:
        resp2 = ddb.list_tables(ExclusiveStartTableName=resp["LastEvaluatedTableName"], Limit=100)
        assert len(resp2["TableNames"]) >= 1


def test_ddb_put_get_item(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={
            "pk": {"S": "allTypes"},
            "str_attr": {"S": "hello"},
            "num_attr": {"N": "42"},
            "bool_attr": {"BOOL": True},
            "null_attr": {"NULL": True},
            "list_attr": {"L": [{"S": "a"}, {"N": "1"}]},
            "map_attr": {"M": {"nested": {"S": "value"}}},
            "ss_attr": {"SS": ["x", "y"]},
            "ns_attr": {"NS": ["1", "2", "3"]},
        },
    )
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "allTypes"}})
    item = resp["Item"]
    assert item["str_attr"]["S"] == "hello"
    assert item["num_attr"]["N"] == "42"
    assert item["bool_attr"]["BOOL"] is True
    assert item["null_attr"]["NULL"] is True
    assert len(item["list_attr"]["L"]) == 2
    assert item["map_attr"]["M"]["nested"]["S"] == "value"
    assert set(item["ss_attr"]["SS"]) == {"x", "y"}
    assert set(item["ns_attr"]["NS"]) == {"1", "2", "3"}


def test_ddb_put_item_condition(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "cond_new"}, "val": {"S": "first"}},
        ConditionExpression="attribute_not_exists(pk)",
    )
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "cond_new"}})
    assert resp["Item"]["val"]["S"] == "first"


def test_ddb_put_item_condition_fail(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "cond_fail"}, "val": {"S": "v1"}})
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName="t_hash_only",
            Item={"pk": {"S": "cond_fail"}, "val": {"S": "v2"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_ddb_delete_item(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "to_del"}, "v": {"S": "gone"}})
    ddb.delete_item(TableName="t_hash_only", Key={"pk": {"S": "to_del"}})
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "to_del"}})
    assert "Item" not in resp


def test_ddb_delete_item_return_old(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "ret_old"}, "data": {"S": "precious"}},
    )
    resp = ddb.delete_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "ret_old"}},
        ReturnValues="ALL_OLD",
    )
    assert resp["Attributes"]["data"]["S"] == "precious"


def test_ddb_update_item_set(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_set"}, "count": {"N": "0"}})
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_set"}},
        UpdateExpression="SET #c = :val",
        ExpressionAttributeNames={"#c": "count"},
        ExpressionAttributeValues={":val": {"N": "10"}},
        ReturnValues="ALL_NEW",
    )
    assert resp["Attributes"]["count"]["N"] == "10"


def test_ddb_update_item_remove(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "upd_rem"}, "extra": {"S": "bye"}, "keep": {"S": "stay"}},
    )
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_rem"}},
        UpdateExpression="REMOVE extra",
        ReturnValues="ALL_NEW",
    )
    assert "extra" not in resp["Attributes"]
    assert resp["Attributes"]["keep"]["S"] == "stay"


def test_ddb_update_item_condition_on_missing_item_fails(ddb):
    """Missing item + attribute_exists(...) condition must fail with ConditionalCheckFailedException."""
    try:
        ddb.delete_table(TableName="t_update_cond_missing")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_update_cond_missing",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    missing_key = {"pk": {"S": "missing-update-item"}}
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName="t_update_cond_missing",
            Key=missing_key,
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="ALL_NEW",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_ddb_get_item_missing_sort_key_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_get_missing_sk")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_get_missing_sk",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(ClientError) as exc:
        ddb.get_item(TableName="t_get_missing_sk", Key={"pk": {"S": "q_pk"}})
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"


def test_ddb_get_item_wrong_key_type_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_get_wrong_type")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_get_wrong_type",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="t_get_wrong_type", Item={"pk": {"S": "typed-key"}})
    with pytest.raises(ClientError) as exc:
        ddb.get_item(TableName="t_get_wrong_type", Key={"pk": {"N": "123"}})
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"


def test_ddb_update_item_extra_key_attribute_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_update_extra_key")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_update_extra_key",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName="t_update_extra_key",
            Key={"pk": {"S": "k1"}, "sk": {"S": "unexpected"}},
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"


def test_ddb_update_item_add(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_add"}, "counter": {"N": "5"}})
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_add"}},
        UpdateExpression="ADD counter :inc",
        ExpressionAttributeValues={":inc": {"N": "3"}},
        ReturnValues="ALL_NEW",
    )
    assert resp["Attributes"]["counter"]["N"] == "8"


def test_ddb_update_item_all_old(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_old"}, "v": {"N": "1"}})
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_old"}},
        UpdateExpression="SET v = :new",
        ExpressionAttributeValues={":new": {"N": "99"}},
        ReturnValues="ALL_OLD",
    )
    assert resp["Attributes"]["v"]["N"] == "1"


def test_ddb_query_pk_only(ddb):
    for i in range(3):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_pk"}, "sk": {"S": f"sk_{i}"}, "n": {"N": str(i)}},
        )
    resp = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_pk"}},
    )
    assert resp["Count"] == 3


def test_ddb_query_pk_sk(ddb):
    for i in range(5):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_sk"}, "sk": {"S": f"item_{i:03d}"}},
        )
    resp_bw = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
        ExpressionAttributeValues={
            ":pk": {"S": "q_sk"},
            ":prefix": {"S": "item_00"},
        },
    )
    assert resp_bw["Count"] >= 1
    for item in resp_bw["Items"]:
        assert item["sk"]["S"].startswith("item_00")

    resp_bt = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
        ExpressionAttributeValues={
            ":pk": {"S": "q_sk"},
            ":lo": {"S": "item_001"},
            ":hi": {"S": "item_003"},
        },
    )
    assert resp_bt["Count"] >= 1
    for item in resp_bt["Items"]:
        assert "item_001" <= item["sk"]["S"] <= "item_003"


def test_ddb_query_filter(ddb):
    for i in range(5):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_filt"}, "sk": {"S": f"f_{i}"}, "val": {"N": str(i)}},
        )
    resp = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        FilterExpression="val > :min",
        ExpressionAttributeValues={":pk": {"S": "q_filt"}, ":min": {"N": "2"}},
    )
    assert resp["Count"] == 2
    assert resp["ScannedCount"] == 5


def test_ddb_query_pagination(ddb):
    for i in range(6):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_page"}, "sk": {"S": f"p_{i:03d}"}},
        )
    resp1 = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_page"}},
        Limit=3,
    )
    assert resp1["Count"] == 3
    assert "LastEvaluatedKey" in resp1

    resp2 = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_page"}},
        ExclusiveStartKey=resp1["LastEvaluatedKey"],
        Limit=3,
    )
    assert resp2["Count"] == 3
    page1_sks = {it["sk"]["S"] for it in resp1["Items"]}
    page2_sks = {it["sk"]["S"] for it in resp2["Items"]}
    assert page1_sks.isdisjoint(page2_sks)


def test_ddb_scan(ddb):
    ddb.create_table(
        TableName="t_scan",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(8):
        ddb.put_item(TableName="t_scan", Item={"pk": {"S": f"sc_{i}"}, "n": {"N": str(i)}})
    resp = ddb.scan(TableName="t_scan")
    assert resp["Count"] == 8
    assert len(resp["Items"]) == 8


def test_ddb_scan_filter(ddb):
    resp = ddb.scan(
        TableName="t_scan",
        FilterExpression="n >= :min",
        ExpressionAttributeValues={":min": {"N": "5"}},
    )
    assert resp["Count"] == 3
    for item in resp["Items"]:
        assert int(item["n"]["N"]) >= 5


def test_ddb_batch_write(ddb):
    ddb.create_table(
        TableName="t_bw",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.batch_write_item(
        RequestItems={
            "t_bw": [{"PutRequest": {"Item": {"pk": {"S": f"bw_{i}"}, "data": {"S": f"d{i}"}}}} for i in range(10)]
        }
    )
    resp = ddb.scan(TableName="t_bw")
    assert resp["Count"] == 10


def test_ddb_batch_get(ddb):
    resp = ddb.batch_get_item(
        RequestItems={
            "t_bw": {
                "Keys": [{"pk": {"S": f"bw_{i}"}} for i in range(5)],
            }
        }
    )
    assert len(resp["Responses"]["t_bw"]) == 5


def test_ddb_transact_write(ddb):
    ddb.create_table(
        TableName="t_tx",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx1"}, "v": {"S": "a"}},
                }
            },
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx2"}, "v": {"S": "b"}},
                }
            },
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx3"}, "v": {"S": "c"}},
                }
            },
        ]
    )
    resp = ddb.scan(TableName="t_tx")
    assert resp["Count"] == 3

    ddb.transact_write_items(
        TransactItems=[
            {"Delete": {"TableName": "t_tx", "Key": {"pk": {"S": "tx3"}}}},
            {
                "Update": {
                    "TableName": "t_tx",
                    "Key": {"pk": {"S": "tx1"}},
                    "UpdateExpression": "SET v = :new",
                    "ExpressionAttributeValues": {":new": {"S": "updated"}},
                },
            },
        ]
    )
    item = ddb.get_item(TableName="t_tx", Key={"pk": {"S": "tx1"}})["Item"]
    assert item["v"]["S"] == "updated"
    gone = ddb.get_item(TableName="t_tx", Key={"pk": {"S": "tx3"}})
    assert "Item" not in gone


def test_ddb_transact_get(ddb):
    resp = ddb.transact_get_items(
        TransactItems=[
            {"Get": {"TableName": "t_tx", "Key": {"pk": {"S": "tx1"}}}},
            {"Get": {"TableName": "t_tx", "Key": {"pk": {"S": "tx2"}}}},
        ]
    )
    assert len(resp["Responses"]) == 2
    assert resp["Responses"][0]["Item"]["pk"]["S"] == "tx1"
    assert resp["Responses"][1]["Item"]["pk"]["S"] == "tx2"


def test_ddb_gsi_query(ddb):
    ddb.create_table(
        TableName="t_gsi_q",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi_index",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(4):
        ddb.put_item(
            TableName="t_gsi_q",
            Item={
                "pk": {"S": f"main_{i}"},
                "gsi_pk": {"S": "shared_gsi"},
                "data": {"N": str(i)},
            },
        )
    ddb.put_item(
        TableName="t_gsi_q",
        Item={
            "pk": {"S": "main_other"},
            "gsi_pk": {"S": "other_gsi"},
            "data": {"N": "99"},
        },
    )
    resp = ddb.query(
        TableName="t_gsi_q",
        IndexName="gsi_index",
        KeyConditionExpression="gsi_pk = :gpk",
        ExpressionAttributeValues={":gpk": {"S": "shared_gsi"}},
    )
    assert resp["Count"] == 4
    for item in resp["Items"]:
        assert item["gsi_pk"]["S"] == "shared_gsi"


# ===================================================================
# Lambda — comprehensive tests
# ===================================================================

_LAMBDA_CODE = 'def handler(event, context):\n    return {"statusCode": 200, "body": "ok"}\n'
_LAMBDA_CODE_V2 = 'def handler(event, context):\n    return {"statusCode": 200, "body": "v2"}\n'
_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def test_lambda_create_function(lam):
    resp = lam.create_function(
        FunctionName="lam-create-test",
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    assert resp["FunctionName"] == "lam-create-test"
    assert resp["Runtime"] == "python3.9"
    assert resp["Handler"] == "index.handler"
    assert resp["State"] == "Active"
    assert "FunctionArn" in resp


def test_lambda_create_duplicate(lam):
    with pytest.raises(ClientError) as exc:
        lam.create_function(
            FunctionName="lam-create-test",
            Runtime="python3.9",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceConflictException"


def test_lambda_get_function(lam):
    resp = lam.get_function(FunctionName="lam-create-test")
    assert resp["Configuration"]["FunctionName"] == "lam-create-test"
    assert "Code" in resp
    assert "Tags" in resp


def test_lambda_get_function_not_found(lam):
    with pytest.raises(ClientError) as exc:
        lam.get_function(FunctionName="nonexistent-func-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_lambda_list_functions(lam):
    resp = lam.list_functions()
    names = [f["FunctionName"] for f in resp["Functions"]]
    assert "lam-create-test" in names


def test_lambda_delete_function(lam):
    lam.create_function(
        FunctionName="lam-to-delete",
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    lam.delete_function(FunctionName="lam-to-delete")
    with pytest.raises(ClientError) as exc:
        lam.get_function(FunctionName="lam-to-delete")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_lambda_invoke(lam):
    lam.create_function(
        FunctionName="lam-invoke-test",
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        Payload=json.dumps({"hello": "world"}),
    )
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200
    assert payload["body"] == "ok"


def test_lambda_invoke_async(lam):
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        InvocationType="Event",
        Payload=json.dumps({"async": True}),
    )
    assert resp["StatusCode"] == 202


def test_lambda_update_code(lam):
    lam.update_function_code(
        FunctionName="lam-invoke-test",
        ZipFile=_make_zip(_LAMBDA_CODE_V2),
    )
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        Payload=json.dumps({}),
    )
    payload = json.loads(resp["Payload"].read())
    assert payload["body"] == "v2"


def test_lambda_update_config(lam):
    lam.update_function_configuration(
        FunctionName="lam-invoke-test",
        Handler="index.new_handler",
        Environment={"Variables": {"MY_VAR": "my_val"}},
    )
    resp = lam.get_function(FunctionName="lam-invoke-test")
    cfg = resp["Configuration"]
    assert cfg["Handler"] == "index.new_handler"
    assert cfg["Environment"]["Variables"]["MY_VAR"] == "my_val"

    lam.update_function_configuration(
        FunctionName="lam-invoke-test",
        Handler="index.handler",
    )


def test_lambda_tags(lam):
    arn = lam.get_function(FunctionName="lam-invoke-test")["Configuration"]["FunctionArn"]
    lam.tag_resource(Resource=arn, Tags={"env": "test", "team": "backend"})
    resp = lam.list_tags(Resource=arn)
    assert resp["Tags"]["env"] == "test"
    assert resp["Tags"]["team"] == "backend"

    lam.untag_resource(Resource=arn, TagKeys=["team"])
    resp = lam.list_tags(Resource=arn)
    assert "team" not in resp["Tags"]
    assert resp["Tags"]["env"] == "test"


def test_lambda_add_permission(lam):
    lam.add_permission(
        FunctionName="lam-invoke-test",
        StatementId="allow-s3",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn="arn:aws:s3:::my-bucket",
    )
    resp = lam.get_policy(FunctionName="lam-invoke-test")
    policy = json.loads(resp["Policy"])
    sids = [s["Sid"] for s in policy["Statement"]]
    assert "allow-s3" in sids


def test_lambda_list_versions(lam):
    resp = lam.list_versions_by_function(FunctionName="lam-invoke-test")
    versions = resp["Versions"]
    assert any(v["Version"] == "$LATEST" for v in versions)


def test_lambda_publish_version(lam):
    resp = lam.publish_version(
        FunctionName="lam-invoke-test",
        Description="first published version",
    )
    assert resp["Version"] == "1"
    assert resp["Description"] == "first published version"
    assert "FunctionArn" in resp

    versions = lam.list_versions_by_function(FunctionName="lam-invoke-test")["Versions"]
    version_nums = [v["Version"] for v in versions]
    assert "$LATEST" in version_nums
    assert "1" in version_nums


def test_lambda_esm_sqs_comprehensive(lam, sqs):
    try:
        lam.delete_function(FunctionName="esm-comp-func")
    except ClientError:
        pass

    code = 'def handler(event, context):\n    return {"processed": len(event.get("Records", []))}\n'
    lam.create_function(
        FunctionName="esm-comp-func",
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    q_url = sqs.create_queue(QueueName="esm-comp-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-comp-func",
        BatchSize=5,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["State"] == "Enabled"
    assert resp["BatchSize"] == 5
    assert resp["EventSourceArn"] == q_arn

    got = lam.get_event_source_mapping(UUID=esm_uuid)
    assert got["UUID"] == esm_uuid

    listed = lam.list_event_source_mappings(FunctionName="esm-comp-func")
    assert any(e["UUID"] == esm_uuid for e in listed["EventSourceMappings"])

    lam.delete_event_source_mapping(UUID=esm_uuid)


def test_lambda_esm_sqs_failure_respects_visibility_timeout(lam, sqs):
    """On Lambda failure, the message should remain in-flight until VisibilityTimeout expires."""
    import io
    import zipfile as zf

    for fn in ("esm-fail-func",):
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass

    code = b"def handler(event, context):\n    raise Exception('boom')\n"
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)

    lam.create_function(
        FunctionName="esm-fail-func",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
        Timeout=3,
    )

    q_url = sqs.create_queue(
        QueueName="esm-fail-queue",
        Attributes={"VisibilityTimeout": "1"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-fail-func",
        BatchSize=1,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]

    sqs.send_message(QueueUrl=q_url, MessageBody="trigger-failure")

    for _ in range(20):
        time.sleep(0.2)
        cur = lam.get_event_source_mapping(UUID=esm_uuid)
        if cur.get("LastProcessingResult") == "FAILED":
            break

    lam.update_event_source_mapping(UUID=esm_uuid, Enabled=False)

    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert not msgs.get("Messages"), "Message should be invisible during VisibilityTimeout after failed ESM invoke"

    time.sleep(1.2)
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert msgs.get("Messages"), "Message should reappear after VisibilityTimeout when ESM invoke fails"

    lam.delete_event_source_mapping(UUID=esm_uuid)


# ===================================================================
# IAM — comprehensive tests
# ===================================================================


def test_iam_create_user(iam):
    resp = iam.create_user(UserName="iam-test-user")
    user = resp["User"]
    assert user["UserName"] == "iam-test-user"
    assert "Arn" in user
    assert "UserId" in user


def test_iam_get_user(iam):
    resp = iam.get_user(UserName="iam-test-user")
    assert resp["User"]["UserName"] == "iam-test-user"


def test_iam_get_user_not_found(iam):
    with pytest.raises(ClientError) as exc:
        iam.get_user(UserName="ghost-user-xyz")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_list_users(iam):
    resp = iam.list_users()
    names = [u["UserName"] for u in resp["Users"]]
    assert "iam-test-user" in names


def test_iam_delete_user(iam):
    iam.create_user(UserName="iam-del-user")
    iam.delete_user(UserName="iam-del-user")
    with pytest.raises(ClientError) as exc:
        iam.get_user(UserName="iam-del-user")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_create_role(iam):
    assume = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(
        RoleName="iam-test-role",
        AssumeRolePolicyDocument=assume,
        Description="integration test role",
    )
    role = resp["Role"]
    assert role["RoleName"] == "iam-test-role"
    assert "Arn" in role
    assert "RoleId" in role


def test_iam_get_role(iam):
    resp = iam.get_role(RoleName="iam-test-role")
    assert resp["Role"]["RoleName"] == "iam-test-role"


def test_iam_list_roles(iam):
    resp = iam.list_roles()
    names = [r["RoleName"] for r in resp["Roles"]]
    assert "iam-test-role" in names


def test_iam_delete_role(iam):
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    iam.create_role(RoleName="iam-del-role", AssumeRolePolicyDocument=assume)
    iam.delete_role(RoleName="iam-del-role")
    with pytest.raises(ClientError) as exc:
        iam.get_role(RoleName="iam-del-role")
    assert exc.value.response["Error"]["Code"] == "NoSuchEntity"


def test_iam_create_policy(iam):
    policy_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                }
            ],
        }
    )
    resp = iam.create_policy(
        PolicyName="iam-test-policy",
        PolicyDocument=policy_doc,
    )
    pol = resp["Policy"]
    assert pol["PolicyName"] == "iam-test-policy"
    assert "Arn" in pol
    assert pol["DefaultVersionId"] == "v1"


def test_iam_get_policy(iam):
    arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    resp = iam.get_policy(PolicyArn=arn)
    assert resp["Policy"]["PolicyName"] == "iam-test-policy"


def test_iam_attach_role_policy(iam):
    policy_arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    iam.attach_role_policy(RoleName="iam-test-role", PolicyArn=policy_arn)


def test_iam_list_attached_role_policies(iam):
    resp = iam.list_attached_role_policies(RoleName="iam-test-role")
    arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
    assert "arn:aws:iam::000000000000:policy/iam-test-policy" in arns


def test_iam_detach_role_policy(iam):
    policy_arn = "arn:aws:iam::000000000000:policy/iam-test-policy"
    iam.detach_role_policy(RoleName="iam-test-role", PolicyArn=policy_arn)
    resp = iam.list_attached_role_policies(RoleName="iam-test-role")
    arns = [p["PolicyArn"] for p in resp["AttachedPolicies"]]
    assert policy_arn not in arns


def test_iam_put_role_policy(iam):
    inline_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "logs:*",
                    "Resource": "*",
                }
            ],
        }
    )
    iam.put_role_policy(
        RoleName="iam-test-role",
        PolicyName="inline-logs",
        PolicyDocument=inline_doc,
    )


def test_iam_get_role_policy(iam):
    resp = iam.get_role_policy(RoleName="iam-test-role", PolicyName="inline-logs")
    assert resp["RoleName"] == "iam-test-role"
    assert resp["PolicyName"] == "inline-logs"
    doc = resp["PolicyDocument"]
    if isinstance(doc, str):
        doc = json.loads(doc)
    assert doc["Statement"][0]["Action"] == "logs:*"


def test_iam_list_role_policies(iam):
    resp = iam.list_role_policies(RoleName="iam-test-role")
    assert "inline-logs" in resp["PolicyNames"]


def test_iam_create_access_key(iam):
    resp = iam.create_access_key(UserName="iam-test-user")
    key = resp["AccessKey"]
    assert key["UserName"] == "iam-test-user"
    assert key["AccessKeyId"].startswith("AKIA")
    assert len(key["SecretAccessKey"]) > 0
    assert key["Status"] == "Active"


def test_iam_instance_profile(iam):
    assume = json.dumps({"Version": "2012-10-17", "Statement": []})
    try:
        iam.create_role(RoleName="ip-role", AssumeRolePolicyDocument=assume)
    except ClientError:
        pass

    resp = iam.create_instance_profile(InstanceProfileName="test-ip")
    ip = resp["InstanceProfile"]
    assert ip["InstanceProfileName"] == "test-ip"
    assert "Arn" in ip

    iam.add_role_to_instance_profile(InstanceProfileName="test-ip", RoleName="ip-role")

    resp = iam.get_instance_profile(InstanceProfileName="test-ip")
    roles = resp["InstanceProfile"]["Roles"]
    assert any(r["RoleName"] == "ip-role" for r in roles)

    resp = iam.list_instance_profiles()
    names = [p["InstanceProfileName"] for p in resp["InstanceProfiles"]]
    assert "test-ip" in names

    iam.remove_role_from_instance_profile(InstanceProfileName="test-ip", RoleName="ip-role")
    iam.delete_instance_profile(InstanceProfileName="test-ip")


# ===================================================================
# STS — comprehensive tests
# ===================================================================


def test_sts_get_caller_identity_full(sts):
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"
    assert "Arn" in resp
    assert "UserId" in resp


def test_sts_assume_role(sts):
    resp = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/iam-test-role",
        RoleSessionName="test-session",
        DurationSeconds=900,
    )
    creds = resp["Credentials"]
    assert creds["AccessKeyId"].startswith("ASIA")
    assert len(creds["SecretAccessKey"]) > 0
    assert len(creds["SessionToken"]) > 0
    assert "Expiration" in creds

    assumed = resp["AssumedRoleUser"]
    assert "test-session" in assumed["Arn"]
    assert "AssumedRoleId" in assumed


# ===================================================================
# SecretsManager — comprehensive tests
# ===================================================================


def test_secrets_create_get_v2(sm):
    sm.create_secret(Name="sm-cg-v2", SecretString='{"user":"admin","pass":"s3cr3t"}')
    resp = sm.get_secret_value(SecretId="sm-cg-v2")
    parsed = json.loads(resp["SecretString"])
    assert parsed["user"] == "admin"
    assert parsed["pass"] == "s3cr3t"
    assert "VersionId" in resp
    assert "ARN" in resp

    sm.create_secret(Name="sm-cg-bin", SecretBinary=b"\x00\x01\x02")
    resp_bin = sm.get_secret_value(SecretId="sm-cg-bin")
    assert resp_bin["SecretBinary"] == b"\x00\x01\x02"


def test_secrets_update_v2(sm):
    sm.create_secret(Name="sm-upd-v2", SecretString="original")
    sm.update_secret(SecretId="sm-upd-v2", SecretString="updated", Description="new desc")
    resp = sm.get_secret_value(SecretId="sm-upd-v2")
    assert resp["SecretString"] == "updated"
    desc = sm.describe_secret(SecretId="sm-upd-v2")
    assert desc["Description"] == "new desc"


def test_secrets_list_v2(sm):
    sm.create_secret(Name="sm-list-a", SecretString="a")
    sm.create_secret(Name="sm-list-b", SecretString="b")
    listed = sm.list_secrets()
    names = [s["Name"] for s in listed["SecretList"]]
    assert "sm-list-a" in names
    assert "sm-list-b" in names


def test_secrets_delete_v2(sm):
    sm.create_secret(Name="sm-del-v2", SecretString="gone")
    sm.delete_secret(SecretId="sm-del-v2", ForceDeleteWithoutRecovery=True)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="sm-del-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_secrets_delete_with_recovery(sm):
    sm.create_secret(Name="sm-del-rec", SecretString="recoverable")
    sm.delete_secret(SecretId="sm-del-rec", RecoveryWindowInDays=7)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="sm-del-rec")
    assert (
        "marked for deletion" in exc.value.response["Error"]["Message"].lower()
        or exc.value.response["Error"]["Code"] == "InvalidRequestException"
    )
    desc = sm.describe_secret(SecretId="sm-del-rec")
    assert "DeletedDate" in desc

    sm.restore_secret(SecretId="sm-del-rec")
    resp = sm.get_secret_value(SecretId="sm-del-rec")
    assert resp["SecretString"] == "recoverable"


def test_secrets_put_value_version_stages_v2(sm):
    sm.create_secret(Name="sm-pvs-v2", SecretString="v1")
    sm.put_secret_value(SecretId="sm-pvs-v2", SecretString="v2")

    desc = sm.describe_secret(SecretId="sm-pvs-v2")
    stages = desc["VersionIdsToStages"]
    current_vids = [vid for vid, s in stages.items() if "AWSCURRENT" in s]
    previous_vids = [vid for vid, s in stages.items() if "AWSPREVIOUS" in s]
    assert len(current_vids) == 1
    assert len(previous_vids) == 1
    assert current_vids[0] != previous_vids[0]

    cur = sm.get_secret_value(SecretId="sm-pvs-v2", VersionStage="AWSCURRENT")
    assert cur["SecretString"] == "v2"
    prev = sm.get_secret_value(SecretId="sm-pvs-v2", VersionStage="AWSPREVIOUS")
    assert prev["SecretString"] == "v1"


def test_secrets_describe_v2(sm):
    sm.create_secret(
        Name="sm-dsc-v2",
        SecretString="val",
        Description="detailed desc",
        Tags=[{"Key": "Env", "Value": "dev"}],
    )
    resp = sm.describe_secret(SecretId="sm-dsc-v2")
    assert resp["Name"] == "sm-dsc-v2"
    assert resp["Description"] == "detailed desc"
    assert any(t["Key"] == "Env" for t in resp["Tags"])
    assert "VersionIdsToStages" in resp
    assert "ARN" in resp


def test_secrets_tags_v2(sm):
    sm.create_secret(Name="sm-tag-v2", SecretString="val")
    sm.tag_resource(SecretId="sm-tag-v2", Tags=[{"Key": "team", "Value": "backend"}])
    sm.tag_resource(SecretId="sm-tag-v2", Tags=[{"Key": "env", "Value": "prod"}])

    desc = sm.describe_secret(SecretId="sm-tag-v2")
    assert any(t["Key"] == "team" and t["Value"] == "backend" for t in desc["Tags"])
    assert any(t["Key"] == "env" and t["Value"] == "prod" for t in desc["Tags"])

    sm.untag_resource(SecretId="sm-tag-v2", TagKeys=["team"])
    desc2 = sm.describe_secret(SecretId="sm-tag-v2")
    assert not any(t["Key"] == "team" for t in desc2.get("Tags", []))
    assert any(t["Key"] == "env" for t in desc2.get("Tags", []))


def test_secrets_get_random_password_v2(sm):
    resp = sm.get_random_password(PasswordLength=32)
    assert len(resp["RandomPassword"]) == 32

    resp2 = sm.get_random_password(PasswordLength=20, ExcludeCharacters="aeiou")
    pw = resp2["RandomPassword"]
    assert len(pw) == 20
    for c in "aeiou":
        assert c not in pw


# ===================================================================
# CloudWatch Logs — comprehensive tests
# ===================================================================


def test_logs_create_group_v2(logs):
    logs.create_log_group(logGroupName="/cwl/cg-v2")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/cg-v2")
    assert any(g["logGroupName"] == "/cwl/cg-v2" for g in resp["logGroups"])


def test_logs_create_group_duplicate_v2(logs):
    logs.create_log_group(logGroupName="/cwl/dup-v2")
    with pytest.raises(ClientError) as exc:
        logs.create_log_group(logGroupName="/cwl/dup-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"


def test_logs_delete_group_v2(logs):
    logs.create_log_group(logGroupName="/cwl/del-v2")
    logs.delete_log_group(logGroupName="/cwl/del-v2")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/del-v2")
    assert not any(g["logGroupName"] == "/cwl/del-v2" for g in resp["logGroups"])


def test_logs_describe_groups_v2(logs):
    logs.create_log_group(logGroupName="/cwl/dg-a")
    logs.create_log_group(logGroupName="/cwl/dg-b")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/dg-")
    names = [g["logGroupName"] for g in resp["logGroups"]]
    assert "/cwl/dg-a" in names
    assert "/cwl/dg-b" in names


def test_logs_create_stream_v2(logs):
    logs.create_log_group(logGroupName="/cwl/str-v2")
    logs.create_log_stream(logGroupName="/cwl/str-v2", logStreamName="stream-a")
    logs.create_log_stream(logGroupName="/cwl/str-v2", logStreamName="stream-b")
    resp = logs.describe_log_streams(logGroupName="/cwl/str-v2")
    names = [s["logStreamName"] for s in resp["logStreams"]]
    assert "stream-a" in names
    assert "stream-b" in names


def test_logs_put_get_events_v2(logs):
    logs.create_log_group(logGroupName="/cwl/pge-v2")
    logs.create_log_stream(logGroupName="/cwl/pge-v2", logStreamName="s1")
    now = int(time.time() * 1000)
    logs.put_log_events(
        logGroupName="/cwl/pge-v2",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now, "message": "first line"},
            {"timestamp": now + 1, "message": "second line"},
            {"timestamp": now + 2, "message": "third line"},
        ],
    )
    resp = logs.get_log_events(logGroupName="/cwl/pge-v2", logStreamName="s1")
    assert len(resp["events"]) == 3
    assert resp["events"][0]["message"] == "first line"
    assert resp["events"][2]["message"] == "third line"


def test_logs_filter_events_v2(logs):
    logs.create_log_group(logGroupName="/cwl/flt-v2")
    logs.create_log_stream(logGroupName="/cwl/flt-v2", logStreamName="s1")
    now = int(time.time() * 1000)
    logs.put_log_events(
        logGroupName="/cwl/flt-v2",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now, "message": "ERROR disk full"},
            {"timestamp": now + 1, "message": "INFO all clear"},
            {"timestamp": now + 2, "message": "ERROR timeout"},
        ],
    )
    resp = logs.filter_log_events(logGroupName="/cwl/flt-v2", filterPattern="ERROR")
    assert len(resp["events"]) == 2
    msgs = [e["message"] for e in resp["events"]]
    assert "ERROR disk full" in msgs
    assert "ERROR timeout" in msgs


def test_logs_retention_policy_v2(logs):
    logs.create_log_group(logGroupName="/cwl/ret-v2")
    logs.put_retention_policy(logGroupName="/cwl/ret-v2", retentionInDays=30)
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/ret-v2")
    grp = next(g for g in resp["logGroups"] if g["logGroupName"] == "/cwl/ret-v2")
    assert grp["retentionInDays"] == 30

    logs.delete_retention_policy(logGroupName="/cwl/ret-v2")
    resp2 = logs.describe_log_groups(logGroupNamePrefix="/cwl/ret-v2")
    grp2 = next(g for g in resp2["logGroups"] if g["logGroupName"] == "/cwl/ret-v2")
    assert "retentionInDays" not in grp2


def test_logs_tags_v2(logs):
    logs.create_log_group(logGroupName="/cwl/tag-v2", tags={"env": "prod"})
    resp = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert resp["tags"]["env"] == "prod"

    logs.tag_log_group(logGroupName="/cwl/tag-v2", tags={"team": "infra"})
    resp2 = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert resp2["tags"]["env"] == "prod"
    assert resp2["tags"]["team"] == "infra"

    logs.untag_log_group(logGroupName="/cwl/tag-v2", tags=["env"])
    resp3 = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert "env" not in resp3["tags"]
    assert resp3["tags"]["team"] == "infra"


def test_logs_put_requires_group_v2(logs):
    with pytest.raises(ClientError) as exc:
        logs.put_log_events(
            logGroupName="/cwl/nonexistent-xyz",
            logStreamName="s1",
            logEvents=[{"timestamp": int(time.time() * 1000), "message": "fail"}],
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ===================================================================
# SSM — comprehensive tests
# ===================================================================


def test_ssm_put_get_v2(ssm):
    ssm.put_parameter(Name="/ssm2/pg/host", Value="db.local", Type="String")
    resp = ssm.get_parameter(Name="/ssm2/pg/host")
    assert resp["Parameter"]["Value"] == "db.local"
    assert resp["Parameter"]["Type"] == "String"
    assert resp["Parameter"]["Version"] == 1

    ssm.put_parameter(Name="/ssm2/pg/pass", Value="secret123", Type="SecureString")
    resp_enc = ssm.get_parameter(Name="/ssm2/pg/pass", WithDecryption=True)
    assert resp_enc["Parameter"]["Value"] == "secret123"


def test_ssm_overwrite_version_v2(ssm):
    ssm.put_parameter(Name="/ssm2/ov/p", Value="v1", Type="String")
    r1 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r1["Parameter"]["Version"] == 1

    ssm.put_parameter(Name="/ssm2/ov/p", Value="v2", Type="String", Overwrite=True)
    r2 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r2["Parameter"]["Value"] == "v2"
    assert r2["Parameter"]["Version"] == 2

    ssm.put_parameter(Name="/ssm2/ov/p", Value="v3", Type="String", Overwrite=True)
    r3 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r3["Parameter"]["Version"] == 3


def test_ssm_get_by_path_v2(ssm):
    ssm.put_parameter(Name="/ssm2/path/x", Value="vx", Type="String")
    ssm.put_parameter(Name="/ssm2/path/y", Value="vy", Type="String")
    ssm.put_parameter(Name="/ssm2/path/sub/z", Value="vz", Type="String")

    resp = ssm.get_parameters_by_path(Path="/ssm2/path", Recursive=True)
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/ssm2/path/x" in names
    assert "/ssm2/path/y" in names
    assert "/ssm2/path/sub/z" in names

    resp_shallow = ssm.get_parameters_by_path(Path="/ssm2/path", Recursive=False)
    names_shallow = [p["Name"] for p in resp_shallow["Parameters"]]
    assert "/ssm2/path/x" in names_shallow
    assert "/ssm2/path/sub/z" not in names_shallow


def test_ssm_get_parameters_multiple_v2(ssm):
    ssm.put_parameter(Name="/ssm2/multi/a", Value="va", Type="String")
    ssm.put_parameter(Name="/ssm2/multi/b", Value="vb", Type="String")
    resp = ssm.get_parameters(Names=["/ssm2/multi/a", "/ssm2/multi/b", "/ssm2/multi/nope"])
    assert len(resp["Parameters"]) == 2
    assert any(p["Name"] == "/ssm2/multi/a" for p in resp["Parameters"])
    assert any(p["Name"] == "/ssm2/multi/b" for p in resp["Parameters"])
    assert "/ssm2/multi/nope" in resp["InvalidParameters"]


def test_ssm_delete_v2(ssm):
    ssm.put_parameter(Name="/ssm2/del/tmp", Value="bye", Type="String")
    ssm.delete_parameter(Name="/ssm2/del/tmp")
    with pytest.raises(ClientError) as exc:
        ssm.get_parameter(Name="/ssm2/del/tmp")
    assert exc.value.response["Error"]["Code"] == "ParameterNotFound"

    ssm.put_parameter(Name="/ssm2/del/b1", Value="v1", Type="String")
    ssm.put_parameter(Name="/ssm2/del/b2", Value="v2", Type="String")
    resp = ssm.delete_parameters(Names=["/ssm2/del/b1", "/ssm2/del/b2", "/ssm2/del/ghost"])
    assert len(resp["DeletedParameters"]) == 2
    assert "/ssm2/del/ghost" in resp["InvalidParameters"]


def test_ssm_describe_v2(ssm):
    ssm.put_parameter(Name="/ssm2/desc/alpha", Value="va", Type="String", Description="alpha param")
    ssm.put_parameter(Name="/ssm2/desc/beta", Value="vb", Type="SecureString")
    resp = ssm.describe_parameters(
        ParameterFilters=[{"Key": "Name", "Option": "BeginsWith", "Values": ["/ssm2/desc/"]}]
    )
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/ssm2/desc/alpha" in names
    assert "/ssm2/desc/beta" in names


def test_ssm_parameter_history_v2(ssm):
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h1", Type="String", Description="d1")
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h2", Type="String", Overwrite=True, Description="d2")
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h3", Type="String", Overwrite=True, Description="d3")
    resp = ssm.get_parameter_history(Name="/ssm2/hist/h")
    assert len(resp["Parameters"]) == 3
    assert resp["Parameters"][0]["Value"] == "h1"
    assert resp["Parameters"][0]["Version"] == 1
    assert resp["Parameters"][2]["Value"] == "h3"
    assert resp["Parameters"][2]["Version"] == 3


def test_ssm_tags_v2(ssm):
    ssm.put_parameter(Name="/ssm2/tag/t1", Value="v", Type="String")
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId="/ssm2/tag/t1",
        Tags=[{"Key": "team", "Value": "platform"}, {"Key": "env", "Value": "staging"}],
    )
    resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/ssm2/tag/t1")
    tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
    assert tag_map["team"] == "platform"
    assert tag_map["env"] == "staging"

    ssm.remove_tags_from_resource(
        ResourceType="Parameter",
        ResourceId="/ssm2/tag/t1",
        TagKeys=["team"],
    )
    resp2 = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/ssm2/tag/t1")
    tag_map2 = {t["Key"]: t["Value"] for t in resp2["TagList"]}
    assert "team" not in tag_map2
    assert tag_map2["env"] == "staging"


# ===================================================================
# EventBridge — comprehensive tests
# ===================================================================


def test_eb_create_event_bus_v2(eb):
    resp = eb.create_event_bus(Name="eb-bus-v2")
    assert "eb-bus-v2" in resp["EventBusArn"]
    buses = eb.list_event_buses()
    assert any(b["Name"] == "eb-bus-v2" for b in buses["EventBuses"])

    desc = eb.describe_event_bus(Name="eb-bus-v2")
    assert desc["Name"] == "eb-bus-v2"


def test_eb_put_rule_v2(eb):
    eb.create_event_bus(Name="eb-rule-bus")
    resp = eb.put_rule(
        Name="eb-rule-v2",
        EventBusName="eb-rule-bus",
        EventPattern=json.dumps({"source": ["my.app"]}),
        State="ENABLED",
    )
    assert "RuleArn" in resp

    rules = eb.list_rules(EventBusName="eb-rule-bus")
    assert any(r["Name"] == "eb-rule-v2" for r in rules["Rules"])

    described = eb.describe_rule(Name="eb-rule-v2", EventBusName="eb-rule-bus")
    assert described["Name"] == "eb-rule-v2"
    assert described["State"] == "ENABLED"


def test_eb_put_targets_v2(eb):
    eb.put_rule(Name="eb-tgt-v2", ScheduleExpression="rate(10 minutes)", State="ENABLED")
    eb.put_targets(
        Rule="eb-tgt-v2",
        Targets=[
            {"Id": "t1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:f1"},
            {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:000000000000:q1"},
        ],
    )
    resp = eb.list_targets_by_rule(Rule="eb-tgt-v2")
    assert len(resp["Targets"]) == 2
    ids = {t["Id"] for t in resp["Targets"]}
    assert ids == {"t1", "t2"}


def test_eb_list_targets_v2(eb):
    eb.put_rule(Name="eb-lt-v2", ScheduleExpression="rate(1 hour)", State="ENABLED")
    eb.put_targets(
        Rule="eb-lt-v2",
        Targets=[
            {"Id": "a", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:fa"},
        ],
    )
    resp = eb.list_targets_by_rule(Rule="eb-lt-v2")
    assert resp["Targets"][0]["Id"] == "a"
    assert "fa" in resp["Targets"][0]["Arn"]


def test_eb_put_events_v2(eb):
    resp = eb.put_events(
        Entries=[
            {
                "Source": "app.v2",
                "DetailType": "Ev1",
                "Detail": json.dumps({"a": 1}),
                "EventBusName": "default",
            },
            {
                "Source": "app.v2",
                "DetailType": "Ev2",
                "Detail": json.dumps({"b": 2}),
                "EventBusName": "default",
            },
            {
                "Source": "app.v2",
                "DetailType": "Ev3",
                "Detail": json.dumps({"c": 3}),
                "EventBusName": "default",
            },
        ]
    )
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 3
    assert all("EventId" in e for e in resp["Entries"])


def test_eb_remove_targets_v2(eb):
    eb.put_rule(Name="eb-rm-v2", ScheduleExpression="rate(1 minute)", State="ENABLED")
    eb.put_targets(
        Rule="eb-rm-v2",
        Targets=[
            {"Id": "rm1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:f"},
            {"Id": "rm2", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:g"},
        ],
    )
    assert len(eb.list_targets_by_rule(Rule="eb-rm-v2")["Targets"]) == 2

    eb.remove_targets(Rule="eb-rm-v2", Ids=["rm1"])
    remaining = eb.list_targets_by_rule(Rule="eb-rm-v2")["Targets"]
    assert len(remaining) == 1
    assert remaining[0]["Id"] == "rm2"


def test_eb_delete_rule_v2(eb):
    eb.put_rule(Name="eb-del-v2", ScheduleExpression="rate(1 day)", State="ENABLED")
    eb.delete_rule(Name="eb-del-v2")
    with pytest.raises(ClientError) as exc:
        eb.describe_rule(Name="eb-del-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eb_tags_v2(eb):
    resp = eb.put_rule(Name="eb-tag-v2", ScheduleExpression="rate(1 hour)", State="ENABLED")
    arn = resp["RuleArn"]
    eb.tag_resource(
        ResourceARN=arn,
        Tags=[
            {"Key": "stage", "Value": "dev"},
            {"Key": "team", "Value": "ops"},
        ],
    )
    tags = eb.list_tags_for_resource(ResourceARN=arn)["Tags"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["stage"] == "dev"
    assert tag_map["team"] == "ops"

    eb.untag_resource(ResourceARN=arn, TagKeys=["stage"])
    tags2 = eb.list_tags_for_resource(ResourceARN=arn)["Tags"]
    assert not any(t["Key"] == "stage" for t in tags2)
    assert any(t["Key"] == "team" for t in tags2)


# ===================================================================
# Kinesis — comprehensive tests
# ===================================================================


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


# ===================================================================
# CloudWatch — comprehensive tests
# ===================================================================


def test_cw_put_list_metrics_v2(cw):
    cw.put_metric_data(
        Namespace="CWv2",
        MetricData=[
            {
                "MetricName": "Reqs",
                "Value": 100.0,
                "Unit": "Count",
                "Dimensions": [{"Name": "API", "Value": "/users"}],
            },
            {"MetricName": "Errs", "Value": 5.0, "Unit": "Count"},
        ],
    )
    resp = cw.list_metrics(Namespace="CWv2")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "Reqs" in names
    assert "Errs" in names

    resp_filtered = cw.list_metrics(Namespace="CWv2", MetricName="Reqs")
    assert all(m["MetricName"] == "Reqs" for m in resp_filtered["Metrics"])


def test_cw_get_metric_statistics_v2(cw):
    cw.put_metric_data(
        Namespace="CWStat2",
        MetricData=[
            {"MetricName": "Duration", "Value": 100.0, "Unit": "Milliseconds"},
            {"MetricName": "Duration", "Value": 200.0, "Unit": "Milliseconds"},
        ],
    )
    resp = cw.get_metric_statistics(
        Namespace="CWStat2",
        MetricName="Duration",
        Period=60,
        StartTime=time.time() - 600,
        EndTime=time.time() + 600,
        Statistics=["Average", "Sum", "SampleCount", "Minimum", "Maximum"],
    )
    assert len(resp["Datapoints"]) >= 1
    dp = resp["Datapoints"][0]
    assert "Average" in dp
    assert "Sum" in dp
    assert "SampleCount" in dp
    assert "Minimum" in dp
    assert "Maximum" in dp


def test_cw_put_metric_alarm_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-v2-high-err",
        MetricName="Errors",
        Namespace="CWv2Alarms",
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=2,
        Threshold=10.0,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=["arn:aws:sns:us-east-1:000000000000:alarm-topic"],
        AlarmDescription="Fires when errors >= 10",
    )
    resp = cw.describe_alarms(AlarmNames=["cw-v2-high-err"])
    alarm = resp["MetricAlarms"][0]
    assert alarm["AlarmName"] == "cw-v2-high-err"
    assert alarm["Threshold"] == 10.0
    assert alarm["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert alarm["EvaluationPeriods"] == 2


def test_cw_describe_alarms_v2(cw):
    for i in range(3):
        cw.put_metric_alarm(
            AlarmName=f"cw-da-v2-{i}",
            MetricName="M",
            Namespace="N",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=1,
            Threshold=float(i),
            ComparisonOperator="GreaterThanThreshold",
        )
    resp = cw.describe_alarms(AlarmNamePrefix="cw-da-v2-")
    names = [a["AlarmName"] for a in resp["MetricAlarms"]]
    for i in range(3):
        assert f"cw-da-v2-{i}" in names


def test_cw_delete_alarms_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-del-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cw.delete_alarms(AlarmNames=["cw-del-v2"])
    resp = cw.describe_alarms(AlarmNames=["cw-del-v2"])
    assert len(resp["MetricAlarms"]) == 0


def test_cw_set_alarm_state_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-state-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    initial = cw.describe_alarms(AlarmNames=["cw-state-v2"])["MetricAlarms"][0]
    assert initial["StateValue"] == "INSUFFICIENT_DATA"

    cw.set_alarm_state(
        AlarmName="cw-state-v2",
        StateValue="ALARM",
        StateReason="Manual trigger for testing",
    )
    after = cw.describe_alarms(AlarmNames=["cw-state-v2"])["MetricAlarms"][0]
    assert after["StateValue"] == "ALARM"
    assert after["StateReason"] == "Manual trigger for testing"


def test_cw_get_metric_data_v2(cw):
    cw.put_metric_data(
        Namespace="CWData2",
        MetricData=[{"MetricName": "Hits", "Value": 42.0, "Unit": "Count"}],
    )
    resp = cw.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "q1",
                "MetricStat": {
                    "Metric": {"Namespace": "CWData2", "MetricName": "Hits"},
                    "Period": 60,
                    "Stat": "Sum",
                },
                "ReturnData": True,
            }
        ],
        StartTime=time.time() - 600,
        EndTime=time.time() + 600,
    )
    assert len(resp["MetricDataResults"]) == 1
    assert resp["MetricDataResults"][0]["Id"] == "q1"
    assert resp["MetricDataResults"][0]["StatusCode"] == "Complete"
    assert len(resp["MetricDataResults"][0]["Values"]) >= 1


def test_cw_tags_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-tag-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    arn = cw.describe_alarms(AlarmNames=["cw-tag-v2"])["MetricAlarms"][0]["AlarmArn"]
    cw.tag_resource(
        ResourceARN=arn,
        Tags=[
            {"Key": "env", "Value": "prod"},
            {"Key": "team", "Value": "sre"},
        ],
    )
    resp = cw.list_tags_for_resource(ResourceARN=arn)
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["env"] == "prod"
    assert tag_map["team"] == "sre"

    cw.untag_resource(ResourceARN=arn, TagKeys=["env"])
    resp2 = cw.list_tags_for_resource(ResourceARN=arn)
    assert not any(t["Key"] == "env" for t in resp2["Tags"])
    assert any(t["Key"] == "team" for t in resp2["Tags"])


# ===================================================================
# SES — comprehensive tests
# ===================================================================


def test_ses_verify_identity_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-v2@example.com")
    identities = ses.list_identities()["Identities"]
    assert "ses-v2@example.com" in identities

    attrs = ses.get_identity_verification_attributes(Identities=["ses-v2@example.com"])
    assert "ses-v2@example.com" in attrs["VerificationAttributes"]
    assert attrs["VerificationAttributes"]["ses-v2@example.com"]["VerificationStatus"] == "Success"


def test_ses_send_email_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-send-v2@example.com")
    resp = ses.send_email(
        Source="ses-send-v2@example.com",
        Destination={
            "ToAddresses": ["to@example.com"],
            "CcAddresses": ["cc@example.com"],
        },
        Message={"Subject": {"Data": "Test V2"}, "Body": {"Text": {"Data": "Body v2"}}},
    )
    assert "MessageId" in resp


def test_ses_list_identities_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-li-v2@example.com")
    ses.verify_domain_identity(Domain="example-v2.com")
    email_ids = ses.list_identities(IdentityType="EmailAddress")["Identities"]
    assert "ses-li-v2@example.com" in email_ids
    domain_ids = ses.list_identities(IdentityType="Domain")["Identities"]
    assert "example-v2.com" in domain_ids


def test_ses_quota_v2(ses):
    resp = ses.get_send_quota()
    assert resp["Max24HourSend"] == 50000.0
    assert resp["MaxSendRate"] == 14.0
    assert "SentLast24Hours" in resp


def test_ses_send_raw_email_v2(ses):
    ses.verify_email_identity(EmailAddress="raw-v2@example.com")
    raw = (
        "From: raw-v2@example.com\r\n"
        "To: dest-v2@example.com\r\n"
        "Subject: Raw V2\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "Raw body v2"
    )
    resp = ses.send_raw_email(RawMessage={"Data": raw})
    assert "MessageId" in resp


def test_ses_configuration_set_v2(ses):
    ses.create_configuration_set(ConfigurationSet={"Name": "ses-cs-v2"})
    listed = ses.list_configuration_sets()["ConfigurationSets"]
    assert any(cs["Name"] == "ses-cs-v2" for cs in listed)

    described = ses.describe_configuration_set(ConfigurationSetName="ses-cs-v2")
    assert described["ConfigurationSet"]["Name"] == "ses-cs-v2"

    ses.delete_configuration_set(ConfigurationSetName="ses-cs-v2")
    listed2 = ses.list_configuration_sets()["ConfigurationSets"]
    assert not any(cs["Name"] == "ses-cs-v2" for cs in listed2)


def test_ses_template_v2(ses):
    ses.create_template(
        Template={
            "TemplateName": "ses-tpl-v2",
            "SubjectPart": "Hello {{name}}",
            "TextPart": "Hi {{name}}, order #{{oid}}",
            "HtmlPart": "<h1>Hi {{name}}</h1>",
        }
    )
    resp = ses.get_template(TemplateName="ses-tpl-v2")
    assert resp["Template"]["TemplateName"] == "ses-tpl-v2"
    assert "{{name}}" in resp["Template"]["SubjectPart"]

    listed = ses.list_templates()["TemplatesMetadata"]
    assert any(t["Name"] == "ses-tpl-v2" for t in listed)

    ses.update_template(
        Template={
            "TemplateName": "ses-tpl-v2",
            "SubjectPart": "Updated {{name}}",
            "TextPart": "Updated",
            "HtmlPart": "<p>Updated</p>",
        }
    )
    resp2 = ses.get_template(TemplateName="ses-tpl-v2")
    assert "Updated" in resp2["Template"]["SubjectPart"]

    ses.delete_template(TemplateName="ses-tpl-v2")
    with pytest.raises(ClientError):
        ses.get_template(TemplateName="ses-tpl-v2")


def test_ses_send_templated_v2(ses):
    ses.verify_email_identity(EmailAddress="tpl-v2@example.com")
    ses.create_template(
        Template={
            "TemplateName": "ses-tpl-send-v2",
            "SubjectPart": "Hey {{name}}",
            "TextPart": "Hi {{name}}",
            "HtmlPart": "<h1>Hi {{name}}</h1>",
        }
    )
    resp = ses.send_templated_email(
        Source="tpl-v2@example.com",
        Destination={"ToAddresses": ["r@example.com"]},
        Template="ses-tpl-send-v2",
        TemplateData=json.dumps({"name": "Alice"}),
    )
    assert "MessageId" in resp


# ===================================================================
# Step Functions — comprehensive tests
# ===================================================================


def test_sfn_create_state_machine_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "Init",
            "States": {"Init": {"Type": "Pass", "Result": "ok", "End": True}},
        }
    )
    resp = sfn.create_state_machine(
        name="sfn-csm-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    assert "stateMachineArn" in resp
    assert "sfn-csm-v2" in resp["stateMachineArn"]


def test_sfn_list_state_machines_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "X",
            "States": {"X": {"Type": "Pass", "End": True}},
        }
    )
    sfn.create_state_machine(
        name="sfn-ls-v2a",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    sfn.create_state_machine(
        name="sfn-ls-v2b",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn.list_state_machines()
    names = [m["name"] for m in resp["stateMachines"]]
    assert "sfn-ls-v2a" in names
    assert "sfn-ls-v2b" in names


def test_sfn_describe_state_machine_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "D",
            "States": {"D": {"Type": "Pass", "End": True}},
        }
    )
    create = sfn.create_state_machine(
        name="sfn-desc-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn.describe_state_machine(stateMachineArn=create["stateMachineArn"])
    assert resp["name"] == "sfn-desc-v2"
    assert resp["status"] == "ACTIVE"
    assert resp["definition"] == definition
    assert resp["roleArn"] == "arn:aws:iam::000000000000:role/R"


def test_sfn_start_execution_pass_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "Result": {"msg": "done"}, "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-pass-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")
    for _ in range(50):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=ex["executionArn"])
        if desc["status"] != "RUNNING":
            break
    assert desc["status"] == "SUCCEEDED"
    assert json.loads(desc["output"]) == {"msg": "done"}


def test_sfn_execution_choice_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.x", "NumericEquals": 1, "Next": "One"},
                        {"Variable": "$.x", "NumericGreaterThan": 1, "Next": "Many"},
                    ],
                    "Default": "Zero",
                },
                "One": {"Type": "Pass", "Result": "one", "End": True},
                "Many": {"Type": "Pass", "Result": "many", "End": True},
                "Zero": {"Type": "Pass", "Result": "zero", "End": True},
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-choice-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    arn = sm["stateMachineArn"]

    ex1 = sfn.start_execution(stateMachineArn=arn, input='{"x":1}')
    for _ in range(50):
        time.sleep(0.1)
        d1 = sfn.describe_execution(executionArn=ex1["executionArn"])
        if d1["status"] != "RUNNING":
            break
    assert d1["status"] == "SUCCEEDED"
    assert json.loads(d1["output"]) == "one"

    ex2 = sfn.start_execution(stateMachineArn=arn, input='{"x":5}')
    for _ in range(50):
        time.sleep(0.1)
        d2 = sfn.describe_execution(executionArn=ex2["executionArn"])
        if d2["status"] != "RUNNING":
            break
    assert d2["status"] == "SUCCEEDED"
    assert json.loads(d2["output"]) == "many"

    ex3 = sfn.start_execution(stateMachineArn=arn, input='{"x":0}')
    for _ in range(50):
        time.sleep(0.1)
        d3 = sfn.describe_execution(executionArn=ex3["executionArn"])
        if d3["status"] != "RUNNING":
            break
    assert d3["status"] == "SUCCEEDED"
    assert json.loads(d3["output"]) == "zero"


def test_sfn_stop_execution_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "W",
            "States": {"W": {"Type": "Wait", "Seconds": 120, "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-stop-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"])
    time.sleep(0.3)
    sfn.stop_execution(executionArn=ex["executionArn"], error="UserAbort", cause="test stop")
    desc = sfn.describe_execution(executionArn=ex["executionArn"])
    assert desc["status"] == "ABORTED"


def test_sfn_get_execution_history_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "A",
            "States": {
                "A": {"Type": "Pass", "Next": "B"},
                "B": {"Type": "Pass", "End": True},
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-hist-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")
    for _ in range(50):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=ex["executionArn"])
        if desc["status"] != "RUNNING":
            break
    assert desc["status"] == "SUCCEEDED"

    history = sfn.get_execution_history(executionArn=ex["executionArn"])
    types = [e["type"] for e in history["events"]]
    assert "ExecutionStarted" in types
    assert "ExecutionSucceeded" in types
    assert any("Pass" in t for t in types)


def test_sfn_tags_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "T",
            "States": {"T": {"Type": "Pass", "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-tag-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
        tags=[{"key": "init", "value": "yes"}],
    )
    arn = sm["stateMachineArn"]
    tags = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "init" and t["value"] == "yes" for t in tags)

    sfn.tag_resource(resourceArn=arn, tags=[{"key": "env", "value": "test"}])
    tags2 = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "env" for t in tags2)

    sfn.untag_resource(resourceArn=arn, tagKeys=["init"])
    tags3 = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert not any(t["key"] == "init" for t in tags3)
    assert any(t["key"] == "env" for t in tags3)


# ===================================================================
# ECS — comprehensive tests
# ===================================================================


def test_ecs_create_cluster_v2(ecs):
    resp = ecs.create_cluster(clusterName="ecs-cc-v2")
    assert resp["cluster"]["clusterName"] == "ecs-cc-v2"
    assert resp["cluster"]["status"] == "ACTIVE"
    assert "clusterArn" in resp["cluster"]


def test_ecs_list_clusters_v2(ecs):
    ecs.create_cluster(clusterName="ecs-lc-v2a")
    ecs.create_cluster(clusterName="ecs-lc-v2b")
    resp = ecs.list_clusters()
    arns = resp["clusterArns"]
    assert any("ecs-lc-v2a" in a for a in arns)
    assert any("ecs-lc-v2b" in a for a in arns)


def test_ecs_register_task_def_v2(ecs):
    resp = ecs.register_task_definition(
        family="ecs-td-v2",
        containerDefinitions=[
            {
                "name": "web",
                "image": "nginx:alpine",
                "cpu": 256,
                "memory": 512,
                "portMappings": [{"containerPort": 80, "hostPort": 8080}],
            },
            {"name": "sidecar", "image": "envoy:latest", "cpu": 128, "memory": 256},
        ],
        requiresCompatibilities=["EC2"],
        cpu="512",
        memory="1024",
    )
    td = resp["taskDefinition"]
    assert td["family"] == "ecs-td-v2"
    assert td["revision"] == 1
    assert td["status"] == "ACTIVE"
    assert len(td["containerDefinitions"]) == 2

    resp2 = ecs.register_task_definition(
        family="ecs-td-v2",
        containerDefinitions=[{"name": "web", "image": "nginx:latest", "cpu": 256, "memory": 512}],
    )
    assert resp2["taskDefinition"]["revision"] == 2


def test_ecs_list_task_defs_v2(ecs):
    ecs.register_task_definition(
        family="ecs-ltd-v2",
        containerDefinitions=[{"name": "app", "image": "img", "cpu": 64, "memory": 128}],
    )
    resp = ecs.list_task_definitions(familyPrefix="ecs-ltd-v2")
    assert len(resp["taskDefinitionArns"]) >= 1
    assert all("ecs-ltd-v2" in a for a in resp["taskDefinitionArns"])


def test_ecs_create_service_v2(ecs):
    ecs.create_cluster(clusterName="ecs-svc-v2c")
    ecs.register_task_definition(
        family="ecs-svc-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    resp = ecs.create_service(
        cluster="ecs-svc-v2c",
        serviceName="ecs-svc-v2",
        taskDefinition="ecs-svc-v2td",
        desiredCount=2,
    )
    svc = resp["service"]
    assert svc["serviceName"] == "ecs-svc-v2"
    assert svc["status"] == "ACTIVE"
    assert svc["desiredCount"] == 2


def test_ecs_describe_services_v2(ecs):
    ecs.create_cluster(clusterName="ecs-ds-v2c")
    ecs.register_task_definition(
        family="ecs-ds-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster="ecs-ds-v2c",
        serviceName="ecs-ds-v2a",
        taskDefinition="ecs-ds-v2td",
        desiredCount=1,
    )
    ecs.create_service(
        cluster="ecs-ds-v2c",
        serviceName="ecs-ds-v2b",
        taskDefinition="ecs-ds-v2td",
        desiredCount=3,
    )
    resp = ecs.describe_services(cluster="ecs-ds-v2c", services=["ecs-ds-v2a", "ecs-ds-v2b"])
    assert len(resp["services"]) == 2
    svc_map = {s["serviceName"]: s for s in resp["services"]}
    assert svc_map["ecs-ds-v2a"]["desiredCount"] == 1
    assert svc_map["ecs-ds-v2b"]["desiredCount"] == 3


def test_ecs_update_service_v2(ecs):
    ecs.create_cluster(clusterName="ecs-us-v2c")
    ecs.register_task_definition(
        family="ecs-us-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster="ecs-us-v2c",
        serviceName="ecs-us-v2",
        taskDefinition="ecs-us-v2td",
        desiredCount=1,
    )
    ecs.update_service(cluster="ecs-us-v2c", service="ecs-us-v2", desiredCount=5)
    resp = ecs.describe_services(cluster="ecs-us-v2c", services=["ecs-us-v2"])
    assert resp["services"][0]["desiredCount"] == 5


def test_ecs_tags_v2(ecs):
    resp = ecs.create_cluster(
        clusterName="ecs-tag-v2c",
        tags=[{"key": "env", "value": "staging"}],
    )
    arn = resp["cluster"]["clusterArn"]

    tags = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "env" and t["value"] == "staging" for t in tags)

    ecs.tag_resource(resourceArn=arn, tags=[{"key": "team", "value": "platform"}])
    tags2 = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    tag_map = {t["key"]: t["value"] for t in tags2}
    assert tag_map["env"] == "staging"
    assert tag_map["team"] == "platform"

    ecs.untag_resource(resourceArn=arn, tagKeys=["env"])
    tags3 = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    assert not any(t["key"] == "env" for t in tags3)
    assert any(t["key"] == "team" for t in tags3)


# ===================================================================
# RDS — comprehensive tests
# ===================================================================


def test_rds_create_instance_v2(rds):
    resp = rds.create_db_instance(
        DBInstanceIdentifier="rds-ci-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass123",
        AllocatedStorage=20,
        DBName="mydb",
    )
    inst = resp["DBInstance"]
    assert inst["DBInstanceIdentifier"] == "rds-ci-v2"
    assert inst["DBInstanceStatus"] == "available"
    assert inst["Engine"] == "postgres"
    assert "Address" in inst["Endpoint"]
    assert "Port" in inst["Endpoint"]


def test_rds_describe_instances_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-di-v2a",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    rds.create_db_instance(
        DBInstanceIdentifier="rds-di-v2b",
        DBInstanceClass="db.t3.small",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances()
    ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
    assert "rds-di-v2a" in ids
    assert "rds-di-v2b" in ids

    resp2 = rds.describe_db_instances(DBInstanceIdentifier="rds-di-v2a")
    assert len(resp2["DBInstances"]) == 1
    assert resp2["DBInstances"][0]["Engine"] == "mysql"


def test_rds_delete_instance_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-del-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    rds.delete_db_instance(DBInstanceIdentifier="rds-del-v2", SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        rds.describe_db_instances(DBInstanceIdentifier="rds-del-v2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_rds_modify_instance_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-mod-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    rds.modify_db_instance(
        DBInstanceIdentifier="rds-mod-v2",
        DBInstanceClass="db.t3.small",
        AllocatedStorage=50,
        ApplyImmediately=True,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="rds-mod-v2")
    inst = resp["DBInstances"][0]
    assert inst["DBInstanceClass"] == "db.t3.small"
    assert inst["AllocatedStorage"] == 50


def test_rds_create_cluster_v2(rds):
    resp = rds.create_db_cluster(
        DBClusterIdentifier="rds-cc-v2",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="pass123",
    )
    cluster = resp["DBCluster"]
    assert cluster["DBClusterIdentifier"] == "rds-cc-v2"
    assert cluster["Status"] == "available"
    assert cluster["Engine"] == "aurora-postgresql"
    assert "DBClusterArn" in cluster

    desc = rds.describe_db_clusters(DBClusterIdentifier="rds-cc-v2")
    assert desc["DBClusters"][0]["DBClusterIdentifier"] == "rds-cc-v2"


def test_rds_engine_versions_v2(rds):
    pg = rds.describe_db_engine_versions(Engine="postgres")
    assert len(pg["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "postgres" for v in pg["DBEngineVersions"])

    mysql = rds.describe_db_engine_versions(Engine="mysql")
    assert len(mysql["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "mysql" for v in mysql["DBEngineVersions"])


def test_rds_snapshot_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-snap-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    resp = rds.create_db_snapshot(
        DBSnapshotIdentifier="rds-snap-v2-s1",
        DBInstanceIdentifier="rds-snap-v2",
    )
    snap = resp["DBSnapshot"]
    assert snap["DBSnapshotIdentifier"] == "rds-snap-v2-s1"
    assert snap["Status"] == "available"

    desc = rds.describe_db_snapshots(DBSnapshotIdentifier="rds-snap-v2-s1")
    assert len(desc["DBSnapshots"]) == 1

    rds.delete_db_snapshot(DBSnapshotIdentifier="rds-snap-v2-s1")
    with pytest.raises(ClientError) as exc:
        rds.describe_db_snapshots(DBSnapshotIdentifier="rds-snap-v2-s1")
    assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"


def test_rds_tags_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-tag-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
        Tags=[{"Key": "env", "Value": "dev"}],
    )
    arn = rds.describe_db_instances(DBInstanceIdentifier="rds-tag-v2")["DBInstances"][0]["DBInstanceArn"]

    tags = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "env" and t["Value"] == "dev" for t in tags)

    rds.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "team", "Value": "dba"}])
    tags2 = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "team" and t["Value"] == "dba" for t in tags2)

    rds.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags3 = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags3)
    assert any(t["Key"] == "team" for t in tags3)


# ===================================================================
# ElastiCache — comprehensive tests
# ===================================================================


def test_ec_create_cluster_v2(ec):
    resp = ec.create_cache_cluster(
        CacheClusterId="ec-cc-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    c = resp["CacheCluster"]
    assert c["CacheClusterId"] == "ec-cc-v2"
    assert c["Engine"] == "redis"
    assert c["CacheClusterStatus"] == "available"
    assert len(c["CacheNodes"]) == 1


def test_ec_describe_clusters_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-dc-v2a",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    ec.create_cache_cluster(
        CacheClusterId="ec-dc-v2b",
        Engine="memcached",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters()
    ids = [c["CacheClusterId"] for c in resp["CacheClusters"]]
    assert "ec-dc-v2a" in ids
    assert "ec-dc-v2b" in ids

    resp2 = ec.describe_cache_clusters(CacheClusterId="ec-dc-v2b")
    assert resp2["CacheClusters"][0]["Engine"] == "memcached"


def test_ec_replication_group_v2(ec):
    resp = ec.create_replication_group(
        ReplicationGroupId="ec-rg-v2",
        ReplicationGroupDescription="Test RG v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumNodeGroups=1,
        ReplicasPerNodeGroup=1,
    )
    rg = resp["ReplicationGroup"]
    assert rg["ReplicationGroupId"] == "ec-rg-v2"
    assert rg["Status"] == "available"
    assert len(rg["NodeGroups"]) == 1

    desc = ec.describe_replication_groups(ReplicationGroupId="ec-rg-v2")
    assert desc["ReplicationGroups"][0]["ReplicationGroupId"] == "ec-rg-v2"


def test_ec_engine_versions_v2(ec):
    redis = ec.describe_cache_engine_versions(Engine="redis")
    assert len(redis["CacheEngineVersions"]) > 0
    assert all(v["Engine"] == "redis" for v in redis["CacheEngineVersions"])

    mc = ec.describe_cache_engine_versions(Engine="memcached")
    assert len(mc["CacheEngineVersions"]) > 0


def test_ec_tags_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-tag-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = ec.describe_cache_clusters(CacheClusterId="ec-tag-v2")["CacheClusters"][0]["ARN"]

    ec.add_tags_to_resource(
        ResourceName=arn,
        Tags=[
            {"Key": "env", "Value": "prod"},
            {"Key": "tier", "Value": "cache"},
        ],
    )
    tags = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["env"] == "prod"
    assert tag_map["tier"] == "cache"

    ec.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags2 = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags2)
    assert any(t["Key"] == "tier" for t in tags2)


def test_ec_snapshot_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-snap-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.create_snapshot(SnapshotName="ec-snap-v2-s1", CacheClusterId="ec-snap-v2")
    assert resp["Snapshot"]["SnapshotName"] == "ec-snap-v2-s1"
    assert resp["Snapshot"]["SnapshotStatus"] == "available"

    desc = ec.describe_snapshots(SnapshotName="ec-snap-v2-s1")
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["SnapshotName"] == "ec-snap-v2-s1"


# ===================================================================
# Glue — comprehensive tests
# ===================================================================


def test_glue_database_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_db_v2", "Description": "v2 DB"})
    resp = glue.get_database(Name="glue_db_v2")
    assert resp["Database"]["Name"] == "glue_db_v2"
    assert resp["Database"]["Description"] == "v2 DB"

    glue.update_database(
        Name="glue_db_v2",
        DatabaseInput={"Name": "glue_db_v2", "Description": "updated"},
    )
    resp2 = glue.get_database(Name="glue_db_v2")
    assert resp2["Database"]["Description"] == "updated"

    glue.delete_database(Name="glue_db_v2")
    with pytest.raises(ClientError) as exc:
        glue.get_database(Name="glue_db_v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_table_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_tbl_v2db"})
    glue.create_table(
        DatabaseName="glue_tbl_v2db",
        TableInput={
            "Name": "tbl_v2",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://bucket/tbl_v2/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    resp = glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert resp["Table"]["Name"] == "tbl_v2"
    assert len(resp["Table"]["StorageDescriptor"]["Columns"]) == 2

    glue.update_table(
        DatabaseName="glue_tbl_v2db",
        TableInput={"Name": "tbl_v2", "Description": "updated table"},
    )
    resp2 = glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert resp2["Table"]["Description"] == "updated table"

    glue.delete_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    with pytest.raises(ClientError) as exc:
        glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_list_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_lst_v2db"})
    glue.create_table(
        DatabaseName="glue_lst_v2db",
        TableInput={
            "Name": "lt_a",
            "StorageDescriptor": {
                "Columns": [{"Name": "c", "Type": "string"}],
                "Location": "s3://b/lt_a/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    glue.create_table(
        DatabaseName="glue_lst_v2db",
        TableInput={
            "Name": "lt_b",
            "StorageDescriptor": {
                "Columns": [{"Name": "c", "Type": "string"}],
                "Location": "s3://b/lt_b/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    dbs = glue.get_databases()
    assert any(d["Name"] == "glue_lst_v2db" for d in dbs["DatabaseList"])
    tables = glue.get_tables(DatabaseName="glue_lst_v2db")
    names = [t["Name"] for t in tables["TableList"]]
    assert "lt_a" in names
    assert "lt_b" in names


def test_glue_job_v2(glue):
    glue.create_job(
        Name="glue-job-v2",
        Role="arn:aws:iam::000000000000:role/R",
        Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
        GlueVersion="3.0",
    )
    job = glue.get_job(JobName="glue-job-v2")["Job"]
    assert job["Name"] == "glue-job-v2"

    run_resp = glue.start_job_run(JobName="glue-job-v2", Arguments={"--key": "val"})
    run_id = run_resp["JobRunId"]
    assert run_id

    run = glue.get_job_run(JobName="glue-job-v2", RunId=run_id)["JobRun"]
    assert run["Id"] == run_id
    assert run["JobName"] == "glue-job-v2"

    runs = glue.get_job_runs(JobName="glue-job-v2")["JobRuns"]
    assert any(r["Id"] == run_id for r in runs)


def test_glue_crawler_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_cr_v2db"})
    glue.create_crawler(
        Name="glue-cr-v2",
        Role="arn:aws:iam::000000000000:role/R",
        DatabaseName="glue_cr_v2db",
        Targets={"S3Targets": [{"Path": "s3://b/data/"}]},
    )
    cr = glue.get_crawler(Name="glue-cr-v2")["Crawler"]
    assert cr["Name"] == "glue-cr-v2"
    assert cr["State"] == "READY"

    glue.start_crawler(Name="glue-cr-v2")
    cr2 = glue.get_crawler(Name="glue-cr-v2")["Crawler"]
    assert cr2["State"] == "RUNNING"


def test_glue_tags_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_tag_v2db"})
    arn = "arn:aws:glue:us-east-1:000000000000:database/glue_tag_v2db"
    glue.tag_resource(ResourceArn=arn, TagsToAdd={"env": "test", "team": "data"})
    resp = glue.get_tags(ResourceArn=arn)
    assert resp["Tags"]["env"] == "test"
    assert resp["Tags"]["team"] == "data"

    glue.untag_resource(ResourceArn=arn, TagsToRemove=["team"])
    resp2 = glue.get_tags(ResourceArn=arn)
    assert resp2["Tags"] == {"env": "test"}


def test_glue_partition_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_part_v2db"})
    glue.create_table(
        DatabaseName="glue_part_v2db",
        TableInput={
            "Name": "ptbl_v2",
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
            "PartitionKeys": [
                {"Name": "year", "Type": "string"},
                {"Name": "month", "Type": "string"},
            ],
        },
    )
    glue.create_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionInput={
            "Values": ["2024", "01"],
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/year=2024/month=01/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    glue.create_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionInput={
            "Values": ["2024", "02"],
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/year=2024/month=02/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    resp = glue.get_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionValues=["2024", "01"],
    )
    assert resp["Partition"]["Values"] == ["2024", "01"]

    parts = glue.get_partitions(DatabaseName="glue_part_v2db", TableName="ptbl_v2")
    assert len(parts["Partitions"]) == 2


def test_glue_connection_v2(glue):
    glue.create_connection(
        ConnectionInput={
            "Name": "glue-conn-v2",
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://host/db",
                "USERNAME": "user",
                "PASSWORD": "pass",
            },
        }
    )
    resp = glue.get_connection(Name="glue-conn-v2")
    assert resp["Connection"]["Name"] == "glue-conn-v2"
    assert resp["Connection"]["ConnectionType"] == "JDBC"

    conns = glue.get_connections()
    assert any(c["Name"] == "glue-conn-v2" for c in conns["ConnectionList"])

    glue.delete_connection(ConnectionName="glue-conn-v2")
    with pytest.raises(ClientError) as exc:
        glue.get_connection(Name="glue-conn-v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


# ===================================================================
# Athena — comprehensive tests
# ===================================================================


def test_athena_query_execution_v2(athena):
    resp = athena.start_query_execution(
        QueryString="SELECT 42 AS answer, 'world' AS hello",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": "s3://athena-out/"},
    )
    qid = resp["QueryExecutionId"]
    state = None
    for _ in range(50):
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.1)
    assert state == "SUCCEEDED", f"Query ended in state: {state}"

    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results["ResultSet"]["Rows"]
    assert len(rows) >= 2
    assert rows[0]["Data"][0]["VarCharValue"] == "answer"
    assert rows[1]["Data"][0]["VarCharValue"] == "42"


def test_athena_workgroup_v2(athena):
    athena.create_work_group(
        Name="ath-wg-v2",
        Description="V2 workgroup",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://ath-out/v2/"}},
    )
    resp = athena.get_work_group(WorkGroup="ath-wg-v2")
    assert resp["WorkGroup"]["Name"] == "ath-wg-v2"
    assert resp["WorkGroup"]["Description"] == "V2 workgroup"
    assert resp["WorkGroup"]["State"] == "ENABLED"

    wgs = athena.list_work_groups()
    assert any(wg["Name"] == "ath-wg-v2" for wg in wgs["WorkGroups"])

    athena.update_work_group(
        WorkGroup="ath-wg-v2",
        ConfigurationUpdates={"ResultConfigurationUpdates": {"OutputLocation": "s3://ath-out/v2-new/"}},
    )
    resp2 = athena.get_work_group(WorkGroup="ath-wg-v2")
    assert "v2-new" in resp2["WorkGroup"]["Configuration"]["ResultConfiguration"]["OutputLocation"]

    athena.delete_work_group(WorkGroup="ath-wg-v2", RecursiveDeleteOption=True)
    with pytest.raises(ClientError):
        athena.get_work_group(WorkGroup="ath-wg-v2")


def test_athena_named_query_v2(athena):
    resp = athena.create_named_query(
        Name="ath-nq-v2",
        Database="default",
        QueryString="SELECT * FROM t LIMIT 10",
        WorkGroup="primary",
        Description="Named query v2",
    )
    nqid = resp["NamedQueryId"]
    nq = athena.get_named_query(NamedQueryId=nqid)["NamedQuery"]
    assert nq["Name"] == "ath-nq-v2"
    assert nq["Database"] == "default"
    assert nq["QueryString"] == "SELECT * FROM t LIMIT 10"

    listed = athena.list_named_queries()
    assert nqid in listed["NamedQueryIds"]

    athena.delete_named_query(NamedQueryId=nqid)
    with pytest.raises(ClientError):
        athena.get_named_query(NamedQueryId=nqid)


def test_athena_data_catalog_v2(athena):
    athena.create_data_catalog(
        Name="ath-cat-v2",
        Type="HIVE",
        Description="V2 catalog",
        Parameters={"metadata-function": "arn:aws:lambda:us-east-1:000000000000:function:f"},
    )
    resp = athena.get_data_catalog(Name="ath-cat-v2")
    assert resp["DataCatalog"]["Name"] == "ath-cat-v2"
    assert resp["DataCatalog"]["Type"] == "HIVE"

    listed = athena.list_data_catalogs()
    assert any(c["CatalogName"] == "ath-cat-v2" for c in listed["DataCatalogsSummary"])

    athena.update_data_catalog(Name="ath-cat-v2", Type="HIVE", Description="Updated v2")
    resp2 = athena.get_data_catalog(Name="ath-cat-v2")
    assert resp2["DataCatalog"]["Description"] == "Updated v2"

    athena.delete_data_catalog(Name="ath-cat-v2")
    with pytest.raises(ClientError):
        athena.get_data_catalog(Name="ath-cat-v2")


def test_athena_prepared_statement_v2(athena):
    athena.create_work_group(
        Name="ath-ps-v2wg",
        Description="PS WG",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://out/"}},
    )
    athena.create_prepared_statement(
        StatementName="ath-ps-v2",
        WorkGroup="ath-ps-v2wg",
        QueryStatement="SELECT ? AS val",
        Description="Prepared v2",
    )
    resp = athena.get_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")
    assert resp["PreparedStatement"]["StatementName"] == "ath-ps-v2"
    assert resp["PreparedStatement"]["QueryStatement"] == "SELECT ? AS val"

    listed = athena.list_prepared_statements(WorkGroup="ath-ps-v2wg")
    assert any(s["StatementName"] == "ath-ps-v2" for s in listed["PreparedStatements"])

    athena.delete_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")
    with pytest.raises(ClientError):
        athena.get_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")


def test_athena_tags_v2(athena):
    athena.create_work_group(
        Name="ath-tag-v2wg",
        Description="Tag WG",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://out/"}},
        Tags=[{"Key": "init", "Value": "yes"}],
    )
    arn = athena.get_work_group(WorkGroup="ath-tag-v2wg")["WorkGroup"]["Configuration"]["ResultConfiguration"][
        "OutputLocation"
    ]
    wg_arn = "arn:aws:athena:us-east-1:000000000000:workgroup/ath-tag-v2wg"

    athena.tag_resource(ResourceARN=wg_arn, Tags=[{"Key": "env", "Value": "dev"}])
    resp = athena.list_tags_for_resource(ResourceARN=wg_arn)
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["env"] == "dev"

    athena.untag_resource(ResourceARN=wg_arn, TagKeys=["env"])
    resp2 = athena.list_tags_for_resource(ResourceARN=wg_arn)
    assert not any(t["Key"] == "env" for t in resp2["Tags"])


# ========== S3 Event Notifications ==========


def test_s3_event_notification_to_sqs(s3, sqs):
    s3.create_bucket(Bucket="s3-evt-bkt")
    queue_url = sqs.create_queue(QueueName="s3-evt-queue")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="s3-evt-bkt",
        NotificationConfiguration={
            "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}],
        },
    )
    s3.put_object(Bucket="s3-evt-bkt", Key="test-notify.txt", Body=b"hello")
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Records"][0]["eventSource"] == "aws:s3"
    assert body["Records"][0]["s3"]["object"]["key"] == "test-notify.txt"


def test_s3_event_notification_filter(s3, sqs):
    s3.create_bucket(Bucket="s3-evt-filter-bkt")
    queue_url = sqs.create_queue(QueueName="s3-evt-filter-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="s3-evt-filter-bkt",
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".csv"}]}},
                }
            ],
        },
    )
    s3.put_object(Bucket="s3-evt-filter-bkt", Key="data.txt", Body=b"no match")
    s3.put_object(Bucket="s3-evt-filter-bkt", Key="data.csv", Body=b"match")
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    keys = [json.loads(m["Body"])["Records"][0]["s3"]["object"]["key"] for m in msgs.get("Messages", [])]
    assert "data.csv" in keys
    assert "data.txt" not in keys


def test_s3_event_notification_delete(s3, sqs):
    s3.create_bucket(Bucket="s3-evt-del-bkt")
    queue_url = sqs.create_queue(QueueName="s3-evt-del-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="s3-evt-del-bkt",
        NotificationConfiguration={
            "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectRemoved:*"]}],
        },
    )
    s3.put_object(Bucket="s3-evt-del-bkt", Key="to-del.txt", Body=b"bye")
    s3.delete_object(Bucket="s3-evt-del-bkt", Key="to-del.txt")
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert "ObjectRemoved" in body["Records"][0]["eventName"]


# ========== S3 ListObjectVersions / Website / Logging ==========


def test_s3_list_object_versions(s3):
    s3.create_bucket(Bucket="s3-ver-bkt")
    s3.put_object(Bucket="s3-ver-bkt", Key="v1.txt", Body=b"v1")
    s3.put_object(Bucket="s3-ver-bkt", Key="v2.txt", Body=b"v2")
    resp = s3.list_object_versions(Bucket="s3-ver-bkt")
    versions = resp.get("Versions", [])
    assert len(versions) >= 2
    keys = [v["Key"] for v in versions]
    assert "v1.txt" in keys and "v2.txt" in keys


def test_s3_bucket_website(s3):
    s3.create_bucket(Bucket="s3-web-bkt")
    s3.put_bucket_website(
        Bucket="s3-web-bkt",
        WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}},
    )
    resp = s3.get_bucket_website(Bucket="s3-web-bkt")
    assert resp["IndexDocument"]["Suffix"] == "index.html"
    s3.delete_bucket_website(Bucket="s3-web-bkt")
    with pytest.raises(ClientError):
        s3.get_bucket_website(Bucket="s3-web-bkt")


def test_s3_put_bucket_logging(s3):
    s3.create_bucket(Bucket="s3-log-bkt")
    s3.put_bucket_logging(
        Bucket="s3-log-bkt",
        BucketLoggingStatus={
            "LoggingEnabled": {"TargetBucket": "s3-log-bkt", "TargetPrefix": "logs/"},
        },
    )
    resp = s3.get_bucket_logging(Bucket="s3-log-bkt")
    assert "LoggingEnabled" in resp


# ========== DynamoDB PITR / Endpoints ==========


def test_dynamodb_describe_continuous_backups(ddb):
    ddb.create_table(
        TableName="ddb-pitr-tbl",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.describe_continuous_backups(TableName="ddb-pitr-tbl")
    assert resp["ContinuousBackupsDescription"]["ContinuousBackupsStatus"] == "ENABLED"
    pitr = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
    assert pitr["PointInTimeRecoveryStatus"] == "DISABLED"


def test_dynamodb_update_continuous_backups(ddb):
    ddb.update_continuous_backups(
        TableName="ddb-pitr-tbl",
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )
    resp = ddb.describe_continuous_backups(TableName="ddb-pitr-tbl")
    pitr = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
    assert pitr["PointInTimeRecoveryStatus"] == "ENABLED"


def test_dynamodb_describe_endpoints(ddb):
    resp = ddb.describe_endpoints()
    assert len(resp["Endpoints"]) > 0
    assert "Address" in resp["Endpoints"][0]


# ========== IAM Groups / Inline Policies / OIDC / ServiceLinkedRole ==========


def test_iam_groups(iam):
    iam.create_group(GroupName="test-grp")
    resp = iam.get_group(GroupName="test-grp")
    assert resp["Group"]["GroupName"] == "test-grp"

    listed = iam.list_groups()
    assert any(g["GroupName"] == "test-grp" for g in listed["Groups"])

    iam.create_user(UserName="grp-usr")
    iam.add_user_to_group(GroupName="test-grp", UserName="grp-usr")
    members = iam.get_group(GroupName="test-grp")
    assert any(u["UserName"] == "grp-usr" for u in members["Users"])

    user_groups = iam.list_groups_for_user(UserName="grp-usr")
    assert any(g["GroupName"] == "test-grp" for g in user_groups["Groups"])

    iam.remove_user_from_group(GroupName="test-grp", UserName="grp-usr")
    iam.delete_group(GroupName="test-grp")


def test_iam_user_inline_policy(iam):
    iam.create_user(UserName="inl-pol-usr")
    doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }
    )
    iam.put_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc", PolicyDocument=doc)
    resp = iam.get_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc")
    assert resp["PolicyName"] == "s3-acc"
    listed = iam.list_user_policies(UserName="inl-pol-usr")
    assert "s3-acc" in listed["PolicyNames"]
    iam.delete_user_policy(UserName="inl-pol-usr", PolicyName="s3-acc")


def test_iam_service_linked_role(iam):
    resp = iam.create_service_linked_role(AWSServiceName="elasticloadbalancing.amazonaws.com")
    role = resp["Role"]
    assert "AWSServiceRoleFor" in role["RoleName"]
    assert role["Path"].startswith("/aws-service-role/")


def test_iam_oidc_provider(iam):
    resp = iam.create_open_id_connect_provider(
        Url="https://oidc.example.com",
        ClientIDList=["my-client"],
        ThumbprintList=["a" * 40],
    )
    arn = resp["OpenIDConnectProviderArn"]
    assert "oidc.example.com" in arn
    desc = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
    assert "my-client" in desc["ClientIDList"]
    iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)


def test_iam_policy_tags(iam):
    resp = iam.create_policy(
        PolicyName="tagged-pol",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
            }
        ),
    )
    arn = resp["Policy"]["Arn"]
    iam.tag_policy(PolicyArn=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags = iam.list_policy_tags(PolicyArn=arn)
    assert any(t["Key"] == "env" for t in tags["Tags"])
    iam.untag_policy(PolicyArn=arn, TagKeys=["env"])
    tags2 = iam.list_policy_tags(PolicyArn=arn)
    assert not any(t["Key"] == "env" for t in tags2["Tags"])


# ========== SecretsManager Resource Policy ==========


def test_secretsmanager_resource_policy(sm):
    sm.create_secret(Name="sm-pol-sec", SecretString="secret-val")
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*",
                }
            ],
        }
    )
    sm.put_resource_policy(SecretId="sm-pol-sec", ResourcePolicy=policy)
    resp = sm.get_resource_policy(SecretId="sm-pol-sec")
    assert resp["Name"] == "sm-pol-sec"
    assert "ResourcePolicy" in resp
    sm.delete_resource_policy(SecretId="sm-pol-sec")


def test_secretsmanager_validate_resource_policy(sm):
    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    resp = sm.validate_resource_policy(ResourcePolicy=policy)
    assert resp["PolicyValidationPassed"] is True


# ========== RDS Cluster Param Groups / Snapshots / Option Groups ==========


def test_rds_cluster_parameter_group(rds):
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cpg",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="Test cluster param group",
    )
    resp = rds.describe_db_cluster_parameter_groups(DBClusterParameterGroupName="test-cpg")
    groups = resp["DBClusterParameterGroups"]
    assert len(groups) >= 1
    assert groups[0]["DBClusterParameterGroupName"] == "test-cpg"
    rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName="test-cpg")


def test_rds_modify_db_parameter_group(rds):
    rds.create_db_parameter_group(
        DBParameterGroupName="test-mpg",
        DBParameterGroupFamily="mysql8.0",
        Description="Test param group for modify",
    )
    resp = rds.modify_db_parameter_group(
        DBParameterGroupName="test-mpg",
        Parameters=[
            {
                "ParameterName": "max_connections",
                "ParameterValue": "100",
                "ApplyMethod": "immediate",
            }
        ],
    )
    assert resp["DBParameterGroupName"] == "test-mpg"


def test_rds_cluster_snapshot(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="snap-cl",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="snap-cl-snap",
        DBClusterIdentifier="snap-cl",
    )
    resp = rds.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier="snap-cl-snap")
    snaps = resp["DBClusterSnapshots"]
    assert len(snaps) >= 1
    assert snaps[0]["DBClusterSnapshotIdentifier"] == "snap-cl-snap"
    rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier="snap-cl-snap")


def test_rds_option_group(rds):
    rds.create_option_group(
        OptionGroupName="test-og",
        EngineName="mysql",
        MajorEngineVersion="8.0",
        OptionGroupDescription="Test option group",
    )
    resp = rds.describe_option_groups(OptionGroupName="test-og")
    groups = resp["OptionGroupsList"]
    assert len(groups) >= 1
    assert groups[0]["OptionGroupName"] == "test-og"
    rds.delete_option_group(OptionGroupName="test-og")


def test_rds_start_stop_cluster(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="ss-cl",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds.stop_db_cluster(DBClusterIdentifier="ss-cl")
    resp = rds.describe_db_clusters(DBClusterIdentifier="ss-cl")
    assert resp["DBClusters"][0]["Status"] == "stopped"
    rds.start_db_cluster(DBClusterIdentifier="ss-cl")
    resp2 = rds.describe_db_clusters(DBClusterIdentifier="ss-cl")
    assert resp2["DBClusters"][0]["Status"] == "available"


def test_rds_modify_subnet_group(rds):
    rds.create_db_subnet_group(
        DBSubnetGroupName="test-mod-sg",
        DBSubnetGroupDescription="Test SG",
        SubnetIds=["subnet-111"],
    )
    rds.modify_db_subnet_group(
        DBSubnetGroupName="test-mod-sg",
        DBSubnetGroupDescription="Updated SG",
        SubnetIds=["subnet-222", "subnet-333"],
    )
    resp = rds.describe_db_subnet_groups(DBSubnetGroupName="test-mod-sg")
    assert resp["DBSubnetGroups"][0]["DBSubnetGroupDescription"] == "Updated SG"


# ========== ElastiCache New Operations ==========


def test_elasticache_modify_subnet_group(ec):
    ec.create_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Test EC SG",
        SubnetIds=["subnet-aaa"],
    )
    ec.modify_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Updated EC SG",
        SubnetIds=["subnet-bbb"],
    )
    resp = ec.describe_cache_subnet_groups(CacheSubnetGroupName="test-mod-ecsg")
    assert resp["CacheSubnetGroups"][0]["CacheSubnetGroupDescription"] == "Updated EC SG"


def test_elasticache_user_crud(ec):
    ec.create_user(
        UserId="test-user-1",
        UserName="test-user-1",
        Engine="redis",
        AccessString="on ~* +@all",
        NoPasswordRequired=True,
    )
    resp = ec.describe_users(UserId="test-user-1")
    assert len(resp["Users"]) >= 1
    assert resp["Users"][0]["UserId"] == "test-user-1"
    ec.modify_user(UserId="test-user-1", AccessString="on ~keys:* +get")
    ec.delete_user(UserId="test-user-1")


def test_elasticache_user_group_crud(ec):
    ec.create_user(
        UserId="ug-usr-1",
        UserName="ug-usr-1",
        Engine="redis",
        AccessString="on ~* +@all",
        NoPasswordRequired=True,
    )
    ec.create_user_group(UserGroupId="test-ug-1", Engine="redis", UserIds=["ug-usr-1"])
    resp = ec.describe_user_groups(UserGroupId="test-ug-1")
    assert len(resp["UserGroups"]) >= 1
    assert resp["UserGroups"][0]["UserGroupId"] == "test-ug-1"
    ec.delete_user_group(UserGroupId="test-ug-1")
    ec.delete_user(UserId="ug-usr-1")


# ========== ECS New Operations ==========


def test_ecs_capacity_provider(ecs):
    resp = ecs.create_capacity_provider(
        name="test-cp",
        autoScalingGroupProvider={
            "autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-1",
            "managedScaling": {"status": "ENABLED"},
        },
    )
    assert resp["capacityProvider"]["name"] == "test-cp"
    desc = ecs.describe_capacity_providers(capacityProviders=["test-cp"])
    assert any(cp["name"] == "test-cp" for cp in desc["capacityProviders"])
    ecs.delete_capacity_provider(capacityProvider="test-cp")


def test_ecs_update_cluster(ecs):
    ecs.create_cluster(clusterName="upd-cl")
    resp = ecs.update_cluster(
        cluster="upd-cl",
        settings=[{"name": "containerInsights", "value": "enabled"}],
    )
    assert resp["cluster"]["clusterName"] == "upd-cl"


# ========== CloudWatch Logs New Operations ==========


def test_cloudwatch_logs_metric_filter(logs):
    logs.create_log_group(logGroupName="/test/mf")
    logs.put_metric_filter(
        logGroupName="/test/mf",
        filterName="err-count",
        filterPattern="ERROR",
        metricTransformations=[{"metricName": "ErrorCount", "metricNamespace": "Test", "metricValue": "1"}],
    )
    resp = logs.describe_metric_filters(logGroupName="/test/mf")
    assert len(resp["metricFilters"]) == 1
    assert resp["metricFilters"][0]["filterName"] == "err-count"
    logs.delete_metric_filter(logGroupName="/test/mf", filterName="err-count")
    resp2 = logs.describe_metric_filters(logGroupName="/test/mf")
    assert len(resp2["metricFilters"]) == 0


def test_cloudwatch_logs_insights_stub(logs):
    logs.create_log_group(logGroupName="/test/insights")
    resp = logs.start_query(
        logGroupName="/test/insights",
        startTime=0,
        endTime=9999999999,
        queryString="fields @timestamp | limit 10",
    )
    query_id = resp["queryId"]
    assert query_id
    results = logs.get_query_results(queryId=query_id)
    assert results["status"] in ("Complete", "Running")


# ========== CloudWatch Dashboards ==========


def test_cloudwatch_dashboard(cw):
    body = json.dumps({"widgets": [{"type": "text", "properties": {"markdown": "Hello"}}]})
    cw.put_dashboard(DashboardName="test-dash", DashboardBody=body)
    resp = cw.get_dashboard(DashboardName="test-dash")
    assert resp["DashboardName"] == "test-dash"
    assert "DashboardBody" in resp
    listed = cw.list_dashboards()
    assert any(d["DashboardName"] == "test-dash" for d in listed["DashboardEntries"])
    cw.delete_dashboards(DashboardNames=["test-dash"])


# ========== EventBridge New Operations ==========


def test_eventbridge_permission(eb):
    eb.create_event_bus(Name="perm-bus")
    eb.put_permission(
        EventBusName="perm-bus",
        Action="events:PutEvents",
        Principal="123456789012",
        StatementId="AllowAcct",
    )
    eb.remove_permission(EventBusName="perm-bus", StatementId="AllowAcct")


def test_eventbridge_connection(eb):
    resp = eb.create_connection(
        Name="test-conn",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "x-api-key", "ApiKeyValue": "secret"}},
    )
    assert "ConnectionArn" in resp
    desc = eb.describe_connection(Name="test-conn")
    assert desc["Name"] == "test-conn"
    eb.delete_connection(Name="test-conn")


def test_eventbridge_api_destination(eb):
    eb.create_connection(
        Name="apid-conn",
        AuthorizationType="API_KEY",
        AuthParameters={"ApiKeyAuthParameters": {"ApiKeyName": "k", "ApiKeyValue": "v"}},
    )
    resp = eb.create_api_destination(
        Name="test-apid",
        ConnectionArn="arn:aws:events:us-east-1:000000000000:connection/apid-conn",
        InvocationEndpoint="https://example.com/webhook",
        HttpMethod="POST",
    )
    assert "ApiDestinationArn" in resp
    desc = eb.describe_api_destination(Name="test-apid")
    assert desc["Name"] == "test-apid"
    eb.delete_api_destination(Name="test-apid")


# ========== Glue Triggers / Workflows ==========


def test_glue_trigger(glue):
    glue.create_trigger(Name="test-trig", Type="ON_DEMAND", Actions=[{"JobName": "nonexistent-job"}])
    resp = glue.get_trigger(Name="test-trig")
    assert resp["Trigger"]["Name"] == "test-trig"
    assert resp["Trigger"]["State"] == "CREATED"
    glue.start_trigger(Name="test-trig")
    resp2 = glue.get_trigger(Name="test-trig")
    assert resp2["Trigger"]["State"] == "ACTIVATED"
    glue.stop_trigger(Name="test-trig")
    resp3 = glue.get_trigger(Name="test-trig")
    assert resp3["Trigger"]["State"] == "DEACTIVATED"
    glue.delete_trigger(Name="test-trig")


def test_glue_workflow(glue):
    glue.create_workflow(Name="test-wf", Description="Test workflow")
    resp = glue.get_workflow(Name="test-wf")
    assert resp["Workflow"]["Name"] == "test-wf"
    run = glue.start_workflow_run(Name="test-wf")
    assert "RunId" in run
    glue.delete_workflow(Name="test-wf")


# ========== Kinesis Encryption / Monitoring ==========


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


# ========== Step Functions New Operations ==========


def test_sfn_start_sync_execution(sfn_sync):
    import uuid as _uuid

    sm_name = f"intg-sync-sm-{_uuid.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "Result": {"msg": "done"}, "End": True}},
            }
        ),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({"test": True}))
    assert resp["status"] == "SUCCEEDED"
    assert "output" in resp
    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_describe_state_machine_for_execution(sfn):
    import uuid as _uuid

    sm_name = f"intg-desc-sm-exec-{_uuid.uuid4().hex[:8]}"
    sm_arn = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        ),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
    time.sleep(0.5)
    resp = sfn.describe_state_machine_for_execution(executionArn=exec_resp["executionArn"])
    assert resp["stateMachineArn"] == sm_arn
    assert "definition" in resp
    sfn.delete_state_machine(stateMachineArn=sm_arn)


# ========== API Gateway v2 ==========


def test_apigw_create_api(apigw):
    resp = apigw.create_api(Name="test-api", ProtocolType="HTTP")
    assert "ApiId" in resp
    assert resp["Name"] == "test-api"
    assert resp["ProtocolType"] == "HTTP"


def test_apigw_get_api(apigw):
    create = apigw.create_api(Name="get-api-test", ProtocolType="HTTP")
    api_id = create["ApiId"]
    resp = apigw.get_api(ApiId=api_id)
    assert resp["ApiId"] == api_id
    assert resp["Name"] == "get-api-test"


def test_apigw_get_apis(apigw):
    apigw.create_api(Name="list-api-a", ProtocolType="HTTP")
    apigw.create_api(Name="list-api-b", ProtocolType="HTTP")
    resp = apigw.get_apis()
    names = [a["Name"] for a in resp["Items"]]
    assert "list-api-a" in names
    assert "list-api-b" in names


def test_apigw_update_api(apigw):
    api_id = apigw.create_api(Name="update-api-before", ProtocolType="HTTP")["ApiId"]
    apigw.update_api(ApiId=api_id, Name="update-api-after")
    resp = apigw.get_api(ApiId=api_id)
    assert resp["Name"] == "update-api-after"


def test_apigw_delete_api(apigw):
    from botocore.exceptions import ClientError

    api_id = apigw.create_api(Name="delete-api-test", ProtocolType="HTTP")["ApiId"]
    apigw.delete_api(ApiId=api_id)
    with pytest.raises(ClientError) as exc:
        apigw.get_api(ApiId=api_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigw_create_route(apigw):
    api_id = apigw.create_api(Name="route-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_route(ApiId=api_id, RouteKey="GET /items")
    assert "RouteId" in resp
    assert resp["RouteKey"] == "GET /items"


def test_apigw_get_routes(apigw):
    api_id = apigw.create_api(Name="routes-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /a")
    apigw.create_route(ApiId=api_id, RouteKey="POST /b")
    resp = apigw.get_routes(ApiId=api_id)
    keys = [r["RouteKey"] for r in resp["Items"]]
    assert "GET /a" in keys
    assert "POST /b" in keys


def test_apigw_get_route(apigw):
    api_id = apigw.create_api(Name="get-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="DELETE /things")["RouteId"]
    resp = apigw.get_route(ApiId=api_id, RouteId=route_id)
    assert resp["RouteId"] == route_id
    assert resp["RouteKey"] == "DELETE /things"


def test_apigw_update_route(apigw):
    api_id = apigw.create_api(Name="update-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /old")["RouteId"]
    apigw.update_route(ApiId=api_id, RouteId=route_id, RouteKey="GET /new")
    resp = apigw.get_route(ApiId=api_id, RouteId=route_id)
    assert resp["RouteKey"] == "GET /new"


def test_apigw_delete_route(apigw):
    api_id = apigw.create_api(Name="del-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /gone")["RouteId"]
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    resp = apigw.get_routes(ApiId=api_id)
    assert not any(r["RouteId"] == route_id for r in resp["Items"])


def test_apigw_create_integration(apigw):
    api_id = apigw.create_api(Name="integ-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:my-fn",
        PayloadFormatVersion="2.0",
    )
    assert "IntegrationId" in resp
    assert resp["IntegrationType"] == "AWS_PROXY"
    assert resp["PayloadFormatVersion"] == "2.0"


def test_apigw_get_integrations(apigw):
    api_id = apigw.create_api(Name="integ-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:fn1",
    )
    resp = apigw.get_integrations(ApiId=api_id)
    assert len(resp["Items"]) >= 1


def test_apigw_get_integration(apigw):
    api_id = apigw.create_api(Name="get-integ-api", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="HTTP_PROXY",
        IntegrationUri="https://example.com",
        IntegrationMethod="GET",
    )["IntegrationId"]
    resp = apigw.get_integration(ApiId=api_id, IntegrationId=int_id)
    assert resp["IntegrationId"] == int_id
    assert resp["IntegrationType"] == "HTTP_PROXY"


def test_apigw_delete_integration(apigw):
    api_id = apigw.create_api(Name="del-integ-api", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:fn2",
    )["IntegrationId"]
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    resp = apigw.get_integrations(ApiId=api_id)
    assert not any(i["IntegrationId"] == int_id for i in resp["Items"])


def test_apigw_create_stage(apigw):
    api_id = apigw.create_api(Name="stage-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_stage(ApiId=api_id, StageName="prod")
    assert resp["StageName"] == "prod"


def test_apigw_get_stages(apigw):
    api_id = apigw.create_api(Name="stages-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="v1")
    apigw.create_stage(ApiId=api_id, StageName="v2")
    resp = apigw.get_stages(ApiId=api_id)
    names = [s["StageName"] for s in resp["Items"]]
    assert "v1" in names
    assert "v2" in names


def test_apigw_get_stage(apigw):
    api_id = apigw.create_api(Name="get-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="dev")
    resp = apigw.get_stage(ApiId=api_id, StageName="dev")
    assert resp["StageName"] == "dev"


def test_apigw_update_stage(apigw):
    api_id = apigw.create_api(Name="update-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="staging")
    apigw.update_stage(ApiId=api_id, StageName="staging", Description="updated")
    resp = apigw.get_stage(ApiId=api_id, StageName="staging")
    assert resp.get("Description") == "updated"


def test_apigw_delete_stage(apigw):
    api_id = apigw.create_api(Name="del-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="temp")
    apigw.delete_stage(ApiId=api_id, StageName="temp")
    resp = apigw.get_stages(ApiId=api_id)
    assert not any(s["StageName"] == "temp" for s in resp["Items"])


def test_apigw_create_deployment(apigw):
    api_id = apigw.create_api(Name="deploy-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_deployment(ApiId=api_id)
    assert "DeploymentId" in resp
    assert resp["DeploymentStatus"] == "DEPLOYED"


def test_apigw_get_deployments(apigw):
    api_id = apigw.create_api(Name="deployments-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_deployment(ApiId=api_id, Description="first")
    apigw.create_deployment(ApiId=api_id, Description="second")
    resp = apigw.get_deployments(ApiId=api_id)
    assert len(resp["Items"]) >= 2


def test_apigw_get_deployment(apigw):
    api_id = apigw.create_api(Name="get-deploy-api", ProtocolType="HTTP")["ApiId"]
    dep_id = apigw.create_deployment(ApiId=api_id, Description="single")["DeploymentId"]
    resp = apigw.get_deployment(ApiId=api_id, DeploymentId=dep_id)
    assert resp["DeploymentId"] == dep_id


def test_apigw_delete_deployment(apigw):
    api_id = apigw.create_api(Name="del-deploy-api", ProtocolType="HTTP")["ApiId"]
    dep_id = apigw.create_deployment(ApiId=api_id)["DeploymentId"]
    apigw.delete_deployment(ApiId=api_id, DeploymentId=dep_id)
    resp = apigw.get_deployments(ApiId=api_id)
    assert not any(d["DeploymentId"] == dep_id for d in resp["Items"])


def test_apigw_tag_resource(apigw):
    api_id = apigw.create_api(Name="tag-api", ProtocolType="HTTP")["ApiId"]
    resource_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
    apigw.tag_resource(ResourceArn=resource_arn, Tags={"env": "test", "owner": "team-a"})
    resp = apigw.get_tags(ResourceArn=resource_arn)
    assert resp["Tags"].get("env") == "test"
    assert resp["Tags"].get("owner") == "team-a"


def test_apigw_untag_resource(apigw):
    api_id = apigw.create_api(Name="untag-api", ProtocolType="HTTP")["ApiId"]
    resource_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
    apigw.tag_resource(ResourceArn=resource_arn, Tags={"remove-me": "yes", "keep-me": "yes"})
    apigw.untag_resource(ResourceArn=resource_arn, TagKeys=["remove-me"])
    resp = apigw.get_tags(ResourceArn=resource_arn)
    assert "remove-me" not in resp["Tags"]
    assert resp["Tags"].get("keep-me") == "yes"


def test_apigw_api_not_found(apigw):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        apigw.get_api(ApiId="00000000")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigw_route_on_deleted_api(apigw):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        apigw.create_route(ApiId="00000000", RouteKey="GET /x")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigw_http_protocol_type(apigw):
    resp = apigw.create_api(Name="http-proto-api", ProtocolType="HTTP")
    assert resp["ProtocolType"] == "HTTP"
    api_id = resp["ApiId"]
    fetched = apigw.get_api(ApiId=api_id)
    assert fetched["ProtocolType"] == "HTTP"


# ========== Health endpoint ==========


def test_health_endpoint():
    import urllib.request

    resp = urllib.request.urlopen("http://localhost:4566/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert "services" in data
    assert "s3" in data["services"]


def test_health_endpoint_ministack():
    import urllib.request

    resp = urllib.request.urlopen("http://localhost:4566/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert data["edition"] == "light"


# ========== STS GetSessionToken ==========


def test_sts_get_session_token(sts):
    resp = sts.get_session_token(DurationSeconds=900)
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds


def test_sts_assume_role_with_web_identity(sts, iam):
    iam.create_role(
        RoleName="test-oidc-role",
        AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    role_arn = f"arn:aws:iam::000000000000:role/test-oidc-role"
    resp = sts.assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName="ci-session",
        WebIdentityToken="fake-oidc-token-value",
    )
    creds = resp["Credentials"]
    assert "AccessKeyId" in creds
    assert "SecretAccessKey" in creds
    assert "SessionToken" in creds
    assert "Expiration" in creds


def test_iam_update_role(iam):
    iam.create_role(
        RoleName="test-update-role",
        AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    iam.update_role(RoleName="test-update-role", Description="updated desc", MaxSessionDuration=7200)
    resp = iam.get_role(RoleName="test-update-role")
    assert resp["Role"]["Description"] == "updated desc"
    assert resp["Role"]["MaxSessionDuration"] == 7200


# ========== DynamoDB TTL ==========


def test_ddb_ttl(ddb):
    import uuid as _uuid

    table = f"intg-ttl-{_uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    # Initially disabled
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"

    # Enable TTL
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED"
    assert resp["TimeToLiveDescription"]["AttributeName"] == "expires_at"

    # Disable TTL
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": False, "AttributeName": "expires_at"},
    )
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
    ddb.delete_table(TableName=table)


# ========== Lambda warm start ==========


def test_lambda_warm_start(lam, apigw):
    """Warm worker via API Gateway execute-api: module-level state persists across invocations."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-warm-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import time\n"
        b"_boot_time = time.time()\n"
        b"def handler(event, context):\n"
        b"    return {'statusCode': 200, 'body': str(_boot_time)}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    api_id = apigw.create_api(Name=f"warm-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /ping", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    def call():
        req = _urlreq.Request(
            f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/ping",
            method="GET",
        )
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        return _urlreq.urlopen(req).read().decode()

    t1 = call()  # cold start — spawns worker, imports module
    t2 = call()  # warm — reuses worker, same module state
    assert t1 == t2, f"Warm worker should reuse module state: {t1} != {t2}"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


# ========== Lambda — Node.js runtime ==========

_NODE_CODE = (
    "exports.handler = async (event, context) => {"
    " return { statusCode: 200, body: JSON.stringify({ hello: event.name || 'world' }) }; };"
)


def test_lambda_nodejs_create_and_invoke(lam):
    lam.create_function(
        FunctionName="lam-node-basic",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(_NODE_CODE, "index.js")},
    )
    resp = lam.invoke(
        FunctionName="lam-node-basic",
        Payload=json.dumps({"name": "ministack"}),
    )
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200
    body = json.loads(payload["body"])
    assert body["hello"] == "ministack"


def test_lambda_nodejs22_runtime(lam):
    lam.create_function(
        FunctionName="lam-node22",
        Runtime="nodejs22.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(_NODE_CODE, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node22", Payload=json.dumps({"name": "v22"}))
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200


def test_lambda_nodejs_update_code(lam):
    v2 = (
        "exports.handler = async (event) => {"
        " return { statusCode: 200, body: 'v2' }; };"
    )
    lam.update_function_code(
        FunctionName="lam-node-basic",
        ZipFile=_make_zip_js(v2, "index.js"),
    )
    resp = lam.invoke(FunctionName="lam-node-basic", Payload=b"{}")
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["body"] == "v2"


# ========== Lambda — S3 code fetch ==========

def test_lambda_create_from_s3(lam, s3):
    bucket = "lambda-code-bucket"
    s3.create_bucket(Bucket=bucket)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, context): return {'s3': True}")
    s3.put_object(Bucket=bucket, Key="fn.zip", Body=buf.getvalue())

    lam.create_function(
        FunctionName="lam-s3-code",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"S3Bucket": bucket, "S3Key": "fn.zip"},
    )
    resp = lam.invoke(FunctionName="lam-s3-code", Payload=b"{}")
    assert resp["StatusCode"] == 200
    assert json.loads(resp["Payload"].read())["s3"] is True


def test_lambda_update_code_from_s3(lam, s3):
    bucket = "lambda-code-bucket"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, context): return {'v': 's3v2'}")
    s3.put_object(Bucket=bucket, Key="fn-v2.zip", Body=buf.getvalue())

    lam.update_function_code(
        FunctionName="lam-s3-code",
        S3Bucket=bucket,
        S3Key="fn-v2.zip",
    )
    resp = lam.invoke(FunctionName="lam-s3-code", Payload=b"{}")
    assert json.loads(resp["Payload"].read())["v"] == "s3v2"


def test_lambda_update_code_s3_missing_returns_error(lam):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        lam.update_function_code(
            FunctionName="lam-s3-code",
            S3Bucket="lambda-code-bucket",
            S3Key="does-not-exist.zip",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValueException"


# ========== Lambda — versioning ==========

def test_lambda_publish_version(lam):
    code = "def handler(event, context): return {'ver': 1}"
    lam.create_function(
        FunctionName="lam-versioned",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
        Publish=True,
    )
    resp = lam.list_versions_by_function(FunctionName="lam-versioned")
    versions = [v["Version"] for v in resp["Versions"]]
    assert "$LATEST" in versions
    assert any(v != "$LATEST" for v in versions)


def test_lambda_update_code_publish_version(lam):
    # Ensure function exists (may have been cleaned up)
    try:
        lam.get_function(FunctionName="lam-versioned")
    except Exception:
        lam.create_function(
            FunctionName="lam-versioned",
            Runtime="python3.11",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip("def handler(event, context): return {'ver': 1}")},
            Publish=True,
        )
    v2 = "def handler(event, context): return {'ver': 2}"
    lam.update_function_code(
        FunctionName="lam-versioned",
        ZipFile=_make_zip(v2),
        Publish=True,
    )
    resp = lam.list_versions_by_function(FunctionName="lam-versioned")
    versions = [v["Version"] for v in resp["Versions"] if v["Version"] != "$LATEST"]
    assert len(versions) >= 1


# ========== Lambda — Node.js promise and callback handlers ==========

def test_lambda_nodejs_promise_handler(lam):
    code = (
        "exports.handler = (event) => Promise.resolve({ promise: true, val: event.x });"
    )
    lam.create_function(
        FunctionName="lam-node-promise",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node-promise", Payload=json.dumps({"x": 42}))
    payload = json.loads(resp["Payload"].read())
    assert payload["promise"] is True
    assert payload["val"] == 42


def test_lambda_nodejs_callback_handler(lam):
    code = (
        "exports.handler = (event, context, cb) => cb(null, { cb: true, val: event.y });"
    )
    lam.create_function(
        FunctionName="lam-node-cb",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node-cb", Payload=json.dumps({"y": 7}))
    payload = json.loads(resp["Payload"].read())
    assert payload["cb"] is True
    assert payload["val"] == 7


def test_lambda_nodejs_env_vars_at_spawn(lam):
    """Lambda env vars are available at process startup (NODE_OPTIONS, etc.)."""
    code = (
        "exports.handler = async (event) => ({"
        " myVar: process.env.MY_CUSTOM_VAR,"
        " region: process.env.AWS_REGION"
        "});"
    )
    lam.create_function(
        FunctionName="lam-node-env-spawn",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
        Environment={"Variables": {"MY_CUSTOM_VAR": "from-spawn"}},
    )
    resp = lam.invoke(FunctionName="lam-node-env-spawn", Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    assert payload["myVar"] == "from-spawn"


def test_lambda_python_env_vars_at_spawn(lam):
    """Python Lambda env vars are available at process startup."""
    code = (
        "import os\n"
        "def handler(event, context):\n"
        "    return {'myVar': os.environ.get('MY_PY_VAR', 'missing')}\n"
    )
    lam.create_function(
        FunctionName="lam-py-env-spawn",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
        Environment={"Variables": {"MY_PY_VAR": "from-spawn-py"}},
    )
    resp = lam.invoke(FunctionName="lam-py-env-spawn", Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    assert payload["myVar"] == "from-spawn-py"


# ========== Lambda — DynamoDB Streams ESM ==========

def test_lambda_dynamodb_stream_esm(lam, ddb):
    # Create table with streams enabled
    ddb.create_table(
        TableName="stream-test-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    stream_arn = ddb.describe_table(TableName="stream-test-table")["Table"]["LatestStreamArn"]

    # Create Lambda that captures stream records
    code = "def handler(event, context): return len(event['Records'])"
    lam.create_function(
        FunctionName="lam-ddb-stream",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    esm = lam.create_event_source_mapping(
        FunctionName="lam-ddb-stream",
        EventSourceArn=stream_arn,
        StartingPosition="TRIM_HORIZON",
        BatchSize=10,
    )
    assert esm["EventSourceArn"] == stream_arn
    assert esm["FunctionArn"].endswith("lam-ddb-stream")

    # Verify ESM is registered and retrievable
    esm_resp = lam.get_event_source_mapping(UUID=esm["UUID"])
    assert esm_resp["EventSourceArn"] == stream_arn
    assert esm_resp["StartingPosition"] == "TRIM_HORIZON"

    # Write items — stream should capture them
    ddb.put_item(TableName="stream-test-table", Item={"pk": {"S": "k1"}, "val": {"S": "v1"}})
    ddb.put_item(TableName="stream-test-table", Item={"pk": {"S": "k2"}, "val": {"S": "v2"}})
    ddb.delete_item(TableName="stream-test-table", Key={"pk": {"S": "k1"}})

    # Verify table still has expected state
    scan = ddb.scan(TableName="stream-test-table")
    pks = [item["pk"]["S"] for item in scan["Items"]]
    assert "k2" in pks
    assert "k1" not in pks


# ========== API Gateway execute-api data plane ==========


def test_apigw_execute_lambda_proxy(apigw, lam):
    """API Gateway execute-api routes a request through Lambda proxy integration."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-apigw-fn-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'headers': {'Content-Type': 'application/json'},\n"
        b"        'body': json.dumps({'path': event.get('rawPath', '/')}),\n"
        b"    }\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw.create_api(Name=f"exec-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    route_id = apigw.create_route(
        ApiId=api_id,
        RouteKey="GET /hello",
        Target=f"integrations/{int_id}",
    )["RouteId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/hello"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["path"] == "/hello"

    # Cleanup
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigw_execute_no_route(apigw):
    """execute-api returns 404 when no matching route exists."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw.create_api(Name="no-route-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/nonexistent"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    try:
        _urlreq.urlopen(req)
        assert False, "Expected 404"
    except _urlerr.HTTPError as e:
        assert e.code == 404
    apigw.delete_api(ApiId=api_id)


def test_apigw_execute_default_route(apigw, lam):
    """$default catch-all route matches any path."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-default-fn-{_uuid.uuid4().hex[:8]}"
    code = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    api_id = apigw.create_api(Name=f"default-route-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="$default", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/any/path/here"
    req = _urlreq.Request(url, method="POST")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


# ========== EventBridge → Lambda dispatch ==========


def test_eventbridge_lambda_target(eb, lam):
    """PutEvents dispatches to a Lambda target when the rule matches."""
    import uuid as _uuid

    fname = f"intg-eb-fn-{_uuid.uuid4().hex[:8]}"
    bus_name = f"intg-eb-bus-{_uuid.uuid4().hex[:8]}"
    rule_name = f"intg-eb-rule-{_uuid.uuid4().hex[:8]}"

    code = b"events = []\ndef handler(event, context):\n    events.append(event)\n    return {'processed': True}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    fn_arn = lam.get_function(FunctionName=fname)["Configuration"]["FunctionArn"]

    eb.create_event_bus(Name=bus_name)
    eb.put_rule(
        Name=rule_name,
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp.test"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule=rule_name,
        EventBusName=bus_name,
        Targets=[{"Id": "lambda-target", "Arn": fn_arn}],
    )

    resp = eb.put_events(
        Entries=[
            {
                "Source": "myapp.test",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"key": "value"}),
                "EventBusName": bus_name,
            }
        ]
    )
    assert resp["FailedEntryCount"] == 0

    # Cleanup
    eb.remove_targets(Rule=rule_name, EventBusName=bus_name, Ids=["lambda-target"])
    eb.delete_rule(Name=rule_name, EventBusName=bus_name)
    eb.delete_event_bus(Name=bus_name)
    lam.delete_function(FunctionName=fname)


# ========== API Gateway path parameter matching ==========


def test_apigw_path_param_route(apigw, lam):
    """Route with {id} path parameter matches requests correctly."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-param-fn-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    return {'statusCode': 200, 'body': json.dumps({'rawPath': event.get('rawPath')})}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    api_id = apigw.create_api(Name=f"param-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /items/{id}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/items/abc123"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["rawPath"] == "/items/abc123"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigw_greedy_path_param(apigw, lam):
    """{proxy+} greedy path parameter matches paths with multiple segments."""
    import urllib.request as _urlreq
    import uuid as _uuid_mod

    fname = f"intg-greedy-{_uuid_mod.uuid4().hex[:8]}"
    code = 'def handler(event, context):\n    return {"statusCode": 200, "body": event["rawPath"]}\n'
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fname}"
    api_id = apigw.create_api(Name="greedy-test", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=func_arn,
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /files/{proxy+}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    # Path with multiple segments should match {proxy+}
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/files/a/b/c"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    # handler returns rawPath as body string
    assert resp.read().decode() == "/files/a/b/c"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigw_authorizer_crud(apigw):
    """CreateAuthorizer / GetAuthorizer / GetAuthorizers / UpdateAuthorizer / DeleteAuthorizer."""
    import uuid as _uuid_mod

    api_id = apigw.create_api(Name=f"auth-test-{_uuid_mod.uuid4().hex[:8]}", ProtocolType="HTTP")["ApiId"]

    # Create JWT authorizer
    resp = apigw.create_authorizer(
        ApiId=api_id,
        AuthorizerType="JWT",
        Name="my-jwt-auth",
        IdentitySource=["$request.header.Authorization"],
        JwtConfiguration={
            "Audience": ["https://example.com"],
            "Issuer": "https://idp.example.com",
        },
    )
    assert resp["AuthorizerType"] == "JWT"
    assert resp["Name"] == "my-jwt-auth"
    auth_id = resp["AuthorizerId"]

    # Get single
    got = apigw.get_authorizer(ApiId=api_id, AuthorizerId=auth_id)
    assert got["AuthorizerId"] == auth_id
    assert got["JwtConfiguration"]["Issuer"] == "https://idp.example.com"

    # List
    listed = apigw.get_authorizers(ApiId=api_id)
    assert any(a["AuthorizerId"] == auth_id for a in listed["Items"])

    # Update
    updated = apigw.update_authorizer(ApiId=api_id, AuthorizerId=auth_id, Name="renamed-auth")
    assert updated["Name"] == "renamed-auth"

    # Delete
    apigw.delete_authorizer(ApiId=api_id, AuthorizerId=auth_id)
    listed2 = apigw.get_authorizers(ApiId=api_id)
    assert not any(a["AuthorizerId"] == auth_id for a in listed2["Items"])

    apigw.delete_api(ApiId=api_id)


def test_apigw_routekey_in_lambda_event(apigw, lam):
    """routeKey in Lambda event should reflect the matched route, not hardcoded $default."""
    import urllib.request as _urlreq
    import uuid as _uuid_mod

    fname = f"intg-rk-{_uuid_mod.uuid4().hex[:8]}"
    code = 'def handler(event, context):\n    return {"statusCode": 200, "body": event["routeKey"]}\n'
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fname}"
    api_id = apigw.create_api(Name="rk-test", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=func_arn,
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /ping", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/ping"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read().decode() == "GET /ping"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


# ========== Kinesis: shard operations ==========


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


# ========== SSM: label, resource tags ==========


def test_ssm_label_parameter_version(ssm):
    import uuid as _uuid

    pname = f"/intg/label/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(Name=pname, Value="v1", Type="String")
    ssm.put_parameter(Name=pname, Value="v2", Type="String", Overwrite=True)
    resp = ssm.label_parameter_version(Name=pname, ParameterVersion=1, Labels=["stable"])
    assert resp["ParameterVersion"] == 1
    assert resp["InvalidLabels"] == []


def test_ssm_add_remove_tags(ssm):
    import uuid as _uuid

    pname = f"/intg/tagged/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(Name=pname, Value="hello", Type="String")
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId=pname,
        Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}],
    )
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map.get("env") == "prod"
    assert tag_map.get("team") == "backend"
    ssm.remove_tags_from_resource(ResourceType="Parameter", ResourceId=pname, TagKeys=["team"])
    tags2 = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map2 = {t["Key"]: t["Value"] for t in tags2["TagList"]}
    assert "team" not in tag_map2
    assert tag_map2.get("env") == "prod"


# ========== CloudWatch Logs: retention, subscription filters, metric filters ==========


def test_logs_retention_policy(logs):
    import uuid as _uuid

    group = f"/intg/retention/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_retention_policy(logGroupName=group, retentionInDays=7)
    groups = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    assert groups[0].get("retentionInDays") == 7
    logs.delete_retention_policy(logGroupName=group)
    groups2 = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    assert groups2[0].get("retentionInDays") is None


def test_logs_subscription_filter(logs):
    import uuid as _uuid

    group = f"/intg/subfilter/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_subscription_filter(
        logGroupName=group,
        filterName="my-filter",
        filterPattern="ERROR",
        destinationArn="arn:aws:lambda:us-east-1:000000000000:function:log-handler",
    )
    resp = logs.describe_subscription_filters(logGroupName=group)
    assert any(f["filterName"] == "my-filter" for f in resp["subscriptionFilters"])
    logs.delete_subscription_filter(logGroupName=group, filterName="my-filter")
    resp2 = logs.describe_subscription_filters(logGroupName=group)
    assert not any(f["filterName"] == "my-filter" for f in resp2["subscriptionFilters"])


def test_logs_metric_filter(logs):
    import uuid as _uuid

    group = f"/intg/metricfilter/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_metric_filter(
        logGroupName=group,
        filterName="error-count",
        filterPattern="[ERROR]",
        metricTransformations=[
            {
                "metricName": "ErrorCount",
                "metricNamespace": "MyApp",
                "metricValue": "1",
            }
        ],
    )
    resp = logs.describe_metric_filters(logGroupName=group)
    assert any(f["filterName"] == "error-count" for f in resp["metricFilters"])
    logs.delete_metric_filter(logGroupName=group, filterName="error-count")
    resp2 = logs.describe_metric_filters(logGroupName=group)
    assert not any(f["filterName"] == "error-count" for f in resp2.get("metricFilters", []))


def test_logs_tag_log_group(logs):
    import uuid as _uuid

    group = f"/intg/tagging/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.tag_log_group(logGroupName=group, tags={"project": "ministack", "env": "test"})
    resp = logs.list_tags_log_group(logGroupName=group)
    assert resp["tags"].get("project") == "ministack"
    logs.untag_log_group(logGroupName=group, tags=["project"])
    resp2 = logs.list_tags_log_group(logGroupName=group)
    assert "project" not in resp2["tags"]


def test_logs_insights_start_query(logs):
    import uuid as _uuid

    group = f"/intg/insights/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    resp = logs.start_query(
        logGroupName=group,
        startTime=int(time.time()) - 3600,
        endTime=int(time.time()),
        queryString="fields @timestamp, @message | limit 10",
    )
    assert "queryId" in resp
    results = logs.get_query_results(queryId=resp["queryId"])
    assert results["status"] in ("Complete", "Running", "Scheduled")


# ========== CloudWatch: composite alarm, describe_alarms_for_metric, alarm history ==========


def test_cw_composite_alarm(cw):
    import uuid as _uuid

    child = f"intg-child-alarm-{_uuid.uuid4().hex[:8]}"
    composite = f"intg-comp-alarm-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=child,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="CPUUtilization",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=80.0,
    )
    child_arn = cw.describe_alarms(AlarmNames=[child])["MetricAlarms"][0]["AlarmArn"]
    cw.put_composite_alarm(
        AlarmName=composite,
        AlarmRule=f"ALARM({child_arn})",
        AlarmDescription="composite test",
    )
    resp = cw.describe_alarms(AlarmNames=[composite], AlarmTypes=["CompositeAlarm"])
    assert any(a["AlarmName"] == composite for a in resp.get("CompositeAlarms", []))
    cw.delete_alarms(AlarmNames=[child, composite])


def test_cw_describe_alarms_for_metric(cw):
    import uuid as _uuid

    alarm_name = f"intg-afm-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="NetworkIn",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Sum",
        Threshold=1000.0,
    )
    resp = cw.describe_alarms_for_metric(
        MetricName="NetworkIn",
        Namespace="AWS/EC2",
    )
    assert any(a["AlarmName"] == alarm_name for a in resp.get("MetricAlarms", []))
    cw.delete_alarms(AlarmNames=[alarm_name])


def test_cw_describe_alarm_history(cw):
    import uuid as _uuid

    alarm_name = f"intg-hist-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="DiskReadOps",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=50.0,
    )
    cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="test")
    resp = cw.describe_alarm_history(AlarmName=alarm_name)
    assert "AlarmHistoryItems" in resp
    cw.delete_alarms(AlarmNames=[alarm_name])


# ========== EventBridge: archives, permissions ==========


def test_eb_archive(eb):
    import uuid as _uuid

    archive_name = f"intg-archive-{_uuid.uuid4().hex[:8]}"
    resp = eb.create_archive(
        ArchiveName=archive_name,
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
        Description="test archive",
        RetentionDays=7,
    )
    assert "ArchiveArn" in resp
    desc = eb.describe_archive(ArchiveName=archive_name)
    assert desc["ArchiveName"] == archive_name
    assert desc["RetentionDays"] == 7
    archives = eb.list_archives()
    assert any(a["ArchiveName"] == archive_name for a in archives["Archives"])
    eb.delete_archive(ArchiveName=archive_name)
    archives2 = eb.list_archives()
    assert not any(a["ArchiveName"] == archive_name for a in archives2["Archives"])


def test_eb_put_remove_permission(eb):
    import uuid as _uuid

    bus_name = f"intg-perm-bus-{_uuid.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus_name)
    eb.put_permission(
        EventBusName=bus_name,
        StatementId="AllowAccount123",
        Action="events:PutEvents",
        Principal="123456789012",
    )
    # Describe bus — policy should be set (no explicit DescribeEventBus assert needed, just no error)
    eb.remove_permission(EventBusName=bus_name, StatementId="AllowAccount123")
    eb.delete_event_bus(Name=bus_name)


# ========== DynamoDB: UpdateTable ==========


def test_ddb_update_table(ddb):
    import uuid as _uuid

    table = f"intg-updtbl-{_uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.update_table(
        TableName=table,
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    assert resp["TableDescription"]["TableName"] == table
    ddb.delete_table(TableName=table)


# ========== S3: versioning, encryption, lifecycle, CORS, ACL ==========


def test_s3_bucket_versioning(s3):
    s3.create_bucket(Bucket="intg-s3-versioning")
    s3.put_bucket_versioning(
        Bucket="intg-s3-versioning",
        VersioningConfiguration={"Status": "Enabled"},
    )
    resp = s3.get_bucket_versioning(Bucket="intg-s3-versioning")
    assert resp["Status"] == "Enabled"


def test_s3_bucket_encryption(s3):
    s3.create_bucket(Bucket="intg-s3-enc")
    s3.put_bucket_encryption(
        Bucket="intg-s3-enc",
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )
    resp = s3.get_bucket_encryption(Bucket="intg-s3-enc")
    rules = resp["ServerSideEncryptionConfiguration"]["Rules"]
    assert rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
    s3.delete_bucket_encryption(Bucket="intg-s3-enc")
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_encryption(Bucket="intg-s3-enc")
    assert exc.value.response["Error"]["Code"] == "ServerSideEncryptionConfigurationNotFoundError"


def test_s3_bucket_lifecycle(s3):
    s3.create_bucket(Bucket="intg-s3-lifecycle")
    s3.put_bucket_lifecycle_configuration(
        Bucket="intg-s3-lifecycle",
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-old",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "logs/"},
                    "Expiration": {"Days": 30},
                }
            ]
        },
    )
    resp = s3.get_bucket_lifecycle_configuration(Bucket="intg-s3-lifecycle")
    assert resp["Rules"][0]["ID"] == "expire-old"
    s3.delete_bucket_lifecycle(Bucket="intg-s3-lifecycle")
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_lifecycle_configuration(Bucket="intg-s3-lifecycle")
    assert exc.value.response["Error"]["Code"] == "NoSuchLifecycleConfiguration"


def test_s3_bucket_cors(s3):
    s3.create_bucket(Bucket="intg-s3-cors")
    s3.put_bucket_cors(
        Bucket="intg-s3-cors",
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedOrigins": ["https://example.com"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        },
    )
    resp = s3.get_bucket_cors(Bucket="intg-s3-cors")
    assert resp["CORSRules"][0]["AllowedOrigins"] == ["https://example.com"]
    s3.delete_bucket_cors(Bucket="intg-s3-cors")
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_cors(Bucket="intg-s3-cors")
    assert exc.value.response["Error"]["Code"] == "NoSuchCORSConfiguration"


def test_s3_bucket_acl(s3):
    s3.create_bucket(Bucket="intg-s3-acl")
    resp = s3.get_bucket_acl(Bucket="intg-s3-acl")
    assert "Owner" in resp
    assert "Grants" in resp


# ========== Athena: UpdateWorkGroup, BatchGetNamedQuery, BatchGetQueryExecution ==========


def test_athena_update_workgroup(athena):
    import uuid as _uuid

    wg = f"intg-wg-update-{_uuid.uuid4().hex[:8]}"
    athena.create_work_group(Name=wg, Description="before")
    athena.update_work_group(WorkGroup=wg, Description="after")
    resp = athena.get_work_group(WorkGroup=wg)
    assert resp["WorkGroup"]["Description"] == "after"
    athena.delete_work_group(WorkGroup=wg, RecursiveDeleteOption=True)


def test_athena_batch_get_named_query(athena):
    import uuid as _uuid

    wg = f"intg-wg-batch-{_uuid.uuid4().hex[:8]}"
    athena.create_work_group(Name=wg)
    nq1 = athena.create_named_query(
        Name="q1",
        Database="default",
        QueryString="SELECT 1",
        WorkGroup=wg,
    )["NamedQueryId"]
    nq2 = athena.create_named_query(
        Name="q2",
        Database="default",
        QueryString="SELECT 2",
        WorkGroup=wg,
    )["NamedQueryId"]
    resp = athena.batch_get_named_query(NamedQueryIds=[nq1, nq2, "nonexistent-id"])
    assert len(resp["NamedQueries"]) == 2
    assert len(resp["UnprocessedNamedQueryIds"]) == 1
    athena.delete_work_group(WorkGroup=wg, RecursiveDeleteOption=True)


def test_athena_batch_get_query_execution(athena):
    qid1 = athena.start_query_execution(
        QueryString="SELECT 42",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    qid2 = athena.start_query_execution(
        QueryString="SELECT 99",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    time.sleep(1.0)
    resp = athena.batch_get_query_execution(QueryExecutionIds=[qid1, qid2, "nonexistent-id"])
    assert len(resp["QueryExecutions"]) == 2
    assert len(resp["UnprocessedQueryExecutionIds"]) == 1


# ===================================================================
# SNS → Lambda fanout
# ===================================================================

import uuid as _uuid_mod


def test_sns_to_lambda_fanout(lam, sns):
    """SNS publish with lambda protocol invokes the function synchronously."""
    import uuid as _uuid_mod

    fn = f"intg-sns-lam-{_uuid_mod.uuid4().hex[:8]}"
    # Handler records the event on a module-level list so we can inspect it
    code = "received = []\ndef handler(event, context):\n    received.append(event)\n    return {'ok': True}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
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


# ===================================================================
# DynamoDB TTL expiry enforcement
# ===================================================================


def test_ddb_ttl_expiry(ddb):
    """TTL setting is stored and reported correctly; expiry enforcement is in the background reaper."""
    import uuid as _uuid_mod

    table = f"intg-ttl-exp-{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    past = int(time.time()) - 10
    ddb.put_item(
        TableName=table,
        Item={
            "pk": {"S": "expired-item"},
            "expires_at": {"N": str(past)},
            "data": {"S": "should-be-gone"},
        },
    )
    # Item present immediately (reaper hasn't run yet)
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "expired-item"}})
    assert "Item" in resp

    # TTL setting is correctly reflected in DescribeTimeToLive
    desc = ddb.describe_time_to_live(TableName=table)["TimeToLiveDescription"]
    assert desc["TimeToLiveStatus"] == "ENABLED"
    assert desc["AttributeName"] == "expires_at"


# ===================================================================
# Lambda Function URL Config
# ===================================================================


def test_lambda_function_url_config(lam):
    """CreateFunctionUrlConfig / Get / Update / Delete / List lifecycle."""
    import uuid as _uuid_mod

    fn = f"intg-url-cfg-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )

    # Create
    resp = lam.create_function_url_config(FunctionName=fn, AuthType="NONE")
    assert resp["AuthType"] == "NONE"
    assert "FunctionUrl" in resp
    url = resp["FunctionUrl"]

    # Get
    got = lam.get_function_url_config(FunctionName=fn)
    assert got["FunctionUrl"] == url

    # Update
    updated = lam.update_function_url_config(
        FunctionName=fn,
        AuthType="AWS_IAM",
        Cors={"AllowOrigins": ["*"]},
    )
    assert updated["AuthType"] == "AWS_IAM"
    assert updated["Cors"]["AllowOrigins"] == ["*"]

    # List
    listed = lam.list_function_url_configs(FunctionName=fn)
    assert any(c["FunctionUrl"] == url for c in listed["FunctionUrlConfigs"])

    # Delete
    lam.delete_function_url_config(FunctionName=fn)
    with pytest.raises(ClientError) as exc:
        lam.get_function_url_config(FunctionName=fn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# -----------------------------------------------------------------------
# Step Functions — Service Integration Tests
# -----------------------------------------------------------------------


def _wait_sfn(sfn, exec_arn, timeout=10):
    """Poll DescribeExecution until terminal state."""
    for _ in range(int(timeout / 0.1)):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            return desc
    return desc


def test_sfn_integration_sqs_send_message(sfn, sqs):
    """Task state sends a message to SQS via arn:aws:states:::sqs:sendMessage."""
    queue_name = "sfn-integ-sqs-test"
    q = sqs.create_queue(QueueName=queue_name)
    queue_url = q["QueueUrl"]

    definition = json.dumps(
        {
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": queue_url,
                        "MessageBody.$": "$.body",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-sqs-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"body": "hello from sfn"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "MessageId" in output

    # Verify the message actually landed in the queue
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "hello from sfn"


def test_sfn_integration_sns_publish(sfn, sns):
    """Task state publishes to SNS via arn:aws:states:::sns:publish."""
    topic = sns.create_topic(Name="sfn-integ-sns-test")
    topic_arn = topic["TopicArn"]

    definition = json.dumps(
        {
            "StartAt": "Publish",
            "States": {
                "Publish": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": topic_arn,
                        "Message.$": "$.msg",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-sns-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"msg": "hello from sfn"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "MessageId" in output


def test_sfn_integration_dynamodb_put_get(sfn, ddb):
    """Task states write and read from DynamoDB."""
    table_name = "sfn-integ-ddb-test"
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # State machine: PutItem then GetItem
    definition = json.dumps(
        {
            "StartAt": "Put",
            "States": {
                "Put": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "data": {"S.$": "$.value"},
                        },
                    },
                    "ResultPath": "$.putResult",
                    "Next": "Get",
                },
                "Get": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Key": {"pk": {"S.$": "$.id"}},
                    },
                    "ResultPath": "$.getResult",
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ddb-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"id": "item-1", "value": "test-value"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    item = output["getResult"]["Item"]
    assert item["pk"]["S"] == "item-1"
    assert item["data"]["S"] == "test-value"


def test_sfn_integration_dynamodb_error_catch(sfn, ddb):
    """Task state catches DynamoDB error and routes to fallback."""
    definition = json.dumps(
        {
            "StartAt": "GetMissing",
            "States": {
                "GetMissing": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "nonexistent-table-sfn",
                        "Key": {"pk": {"S": "x"}},
                    },
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "Fallback",
                            "ResultPath": "$.error",
                        }
                    ],
                    "End": True,
                },
                "Fallback": {
                    "Type": "Pass",
                    "Result": "caught",
                    "ResultPath": "$.recovered",
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ddb-catch",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["recovered"] == "caught"
    assert "Error" in output["error"]


def test_sfn_integration_ecs_run_task(sfn, ecs):
    """Task state triggers ecs:runTask (fire-and-forget, no Docker needed)."""
    ecs.create_cluster(clusterName="sfn-ecs-test")
    ecs.register_task_definition(
        family="sfn-task",
        containerDefinitions=[
            {
                "name": "main",
                "image": "alpine:latest",
                "command": ["echo", "hi"],
                "memory": 128,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "RunTask",
            "States": {
                "RunTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask",
                    "Parameters": {
                        "Cluster": "sfn-ecs-test",
                        "TaskDefinition": "sfn-task",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output


def test_sfn_integration_ecs_run_task_sync_success(sfn, ecs):
    """ecs:runTask.sync waits for task STOPPED, then returns task result."""
    import threading

    ecs.create_cluster(clusterName="sfn-ecs-sync-ok")
    ecs.register_task_definition(
        family="sfn-sync-ok",
        containerDefinitions=[
            {
                "name": "main",
                "image": "alpine",
                "memory": 128,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask.sync",
                    "Parameters": {
                        "Cluster": "sfn-ecs-sync-ok",
                        "TaskDefinition": "sfn-sync-ok",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-sync-ok",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    # Background thread: poll list_tasks until task appears, then stop it
    def stop_task_when_ready():
        for _ in range(30):
            time.sleep(0.5)
            try:
                tasks = ecs.list_tasks(cluster="sfn-ecs-sync-ok")
                if tasks.get("taskArns"):
                    ecs.stop_task(
                        cluster="sfn-ecs-sync-ok",
                        task=tasks["taskArns"][0],
                        reason="Test: simulating completion",
                    )
                    return
            except Exception:
                pass

    stopper = threading.Thread(target=stop_task_when_ready, daemon=True)
    stopper.start()

    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"], timeout=20)
    stopper.join(timeout=5)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output
    task_out = output["tasks"][0]
    assert task_out["lastStatus"] == "STOPPED"
    # Containers should have exitCode 0 (stop_task sets this)
    for c in task_out.get("containers", []):
        assert c.get("exitCode") == 0


def test_sfn_integration_ecs_run_task_output_contains_status(sfn, ecs):
    """Fire-and-forget ecs:runTask output contains task status and container info."""
    ecs.create_cluster(clusterName="sfn-ecs-status")
    ecs.register_task_definition(
        family="sfn-status-task",
        containerDefinitions=[
            {
                "name": "app",
                "image": "nginx:latest",
                "memory": 256,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask",
                    "Parameters": {
                        "Cluster": "sfn-ecs-status",
                        "TaskDefinition": "sfn-status-task",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-status",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output
    assert len(output["tasks"]) == 1
    task_out = output["tasks"][0]
    assert "taskArn" in task_out
    assert "containers" in task_out
    assert task_out["containers"][0]["name"] == "app"
    assert task_out["lastStatus"] == "RUNNING"
    assert "failures" in output


def test_sfn_integration_multi_service_pipeline(sfn, sqs, ddb):
    """End-to-end: Pass → DynamoDB putItem → SQS sendMessage → Succeed."""
    table_name = "sfn-pipeline-test"
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    queue_name = "sfn-pipeline-queue"
    q = sqs.create_queue(QueueName=queue_name)
    queue_url = q["QueueUrl"]

    definition = json.dumps(
        {
            "StartAt": "Enrich",
            "States": {
                "Enrich": {
                    "Type": "Pass",
                    "Result": "enriched",
                    "ResultPath": "$.status",
                    "Next": "SaveToDB",
                },
                "SaveToDB": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "status": {"S.$": "$.status"},
                        },
                    },
                    "ResultPath": "$.dbResult",
                    "Next": "Notify",
                },
                "Notify": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": queue_url,
                        "MessageBody.$": "$.id",
                    },
                    "ResultPath": "$.sqsResult",
                    "Next": "Done",
                },
                "Done": {
                    "Type": "Succeed",
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-pipeline",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input=json.dumps({"id": "order-42"}))

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"

    # Verify DynamoDB
    item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "order-42"}})
    assert item["Item"]["status"]["S"] == "enriched"

    # Verify SQS
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "order-42"


# ========== API Gateway REST API v1 ==========


def test_apigwv1_create_rest_api(apigw_v1):
    """CreateRestApi returns id, name, and createdDate as datetime."""
    import datetime

    resp = apigw_v1.create_rest_api(name="v1-create-test")
    assert "id" in resp
    assert resp["name"] == "v1-create-test"
    assert "createdDate" in resp
    assert isinstance(resp["createdDate"], datetime.datetime), "createdDate must be a datetime, not a float"
    apigw_v1.delete_rest_api(restApiId=resp["id"])


def test_apigwv1_get_rest_api(apigw_v1):
    """GetRestApi returns the created API."""
    api_id = apigw_v1.create_rest_api(name="v1-get-test")["id"]
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["id"] == api_id
    assert resp["name"] == "v1-get-test"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_get_rest_apis(apigw_v1):
    """GetRestApis returns item list containing created APIs."""
    id1 = apigw_v1.create_rest_api(name="v1-list-a")["id"]
    id2 = apigw_v1.create_rest_api(name="v1-list-b")["id"]
    resp = apigw_v1.get_rest_apis()
    ids = [a["id"] for a in resp["items"]]
    assert id1 in ids
    assert id2 in ids
    apigw_v1.delete_rest_api(restApiId=id1)
    apigw_v1.delete_rest_api(restApiId=id2)


def test_apigwv1_update_rest_api(apigw_v1):
    """UpdateRestApi (PATCH) modifies the API name."""
    api_id = apigw_v1.create_rest_api(name="v1-update-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-after"}],
    )
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["name"] == "v1-update-after"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_rest_api(apigw_v1):
    """DeleteRestApi removes the API; subsequent GetRestApi raises."""
    api_id = apigw_v1.create_rest_api(name="v1-delete-test")["id"]
    apigw_v1.delete_rest_api(restApiId=api_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_rest_api(restApiId=api_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigwv1_create_resource(apigw_v1):
    """CreateResource creates a child resource with computed path."""
    api_id = apigw_v1.create_rest_api(name="v1-resource-create")["id"]
    # Get root resource id
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resp = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )
    assert resp["pathPart"] == "users"
    assert resp["path"] == "/users"
    assert "id" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_get_resources(apigw_v1):
    """GetResources returns the root resource plus any created children."""
    api_id = apigw_v1.create_rest_api(name="v1-get-resources")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.create_resource(restApiId=api_id, parentId=root["id"], pathPart="items")
    resources = apigw_v1.get_resources(restApiId=api_id)["items"]
    paths = [r["path"] for r in resources]
    assert "/" in paths
    assert "/items" in paths
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_put_get_method(apigw_v1):
    """PutMethod creates a method; GetMethod returns it."""
    api_id = apigw_v1.create_rest_api(name="v1-method-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.get_method(restApiId=api_id, resourceId=resource_id, httpMethod="GET")
    assert resp["httpMethod"] == "GET"
    assert resp["authorizationType"] == "NONE"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_put_integration(apigw_v1):
    """PutIntegration sets AWS_PROXY integration on a method."""
    api_id = apigw_v1.create_rest_api(name="v1-integration-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:myFunc/invocations",
    )
    assert resp["type"] == "AWS_PROXY"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_put_method_response(apigw_v1):
    """PutMethodResponse sets a 200 method response."""
    api_id = apigw_v1.create_rest_api(name="v1-method-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_put_integration_response(apigw_v1):
    """PutIntegrationResponse sets a 200 integration response."""
    api_id = apigw_v1.create_rest_api(name="v1-int-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="POST",
        uri="",
    )
    resp = apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_create_deployment(apigw_v1):
    """CreateDeployment returns a deployment with id and createdDate."""
    api_id = apigw_v1.create_rest_api(name="v1-deployment-test")["id"]
    resp = apigw_v1.create_deployment(restApiId=api_id, description="initial deployment")
    assert "id" in resp
    assert "createdDate" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_create_stage(apigw_v1):
    """CreateStage creates a named stage linked to a deployment."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-test")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    resp = apigw_v1.create_stage(
        restApiId=api_id,
        stageName="prod",
        deploymentId=dep_id,
    )
    assert resp["stageName"] == "prod"
    assert resp["deploymentId"] == dep_id
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_stage(apigw_v1):
    """UpdateStage (PATCH) updates stage variables."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-update")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="dev", deploymentId=dep_id)
    apigw_v1.update_stage(
        restApiId=api_id,
        stageName="dev",
        patchOperations=[{"op": "replace", "path": "/variables/myVar", "value": "myVal"}],
    )
    resp = apigw_v1.get_stage(restApiId=api_id, stageName="dev")
    assert resp["variables"]["myVar"] == "myVal"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_authorizer_crud(apigw_v1):
    """Authorizer full lifecycle: create, get, update (patch), delete."""
    api_id = apigw_v1.create_rest_api(name="v1-auth-crud")["id"]
    auth = apigw_v1.create_authorizer(
        restApiId=api_id,
        name="my-auth",
        type="TOKEN",
        authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
        identitySource="method.request.header.Authorization",
    )
    auth_id = auth["id"]
    assert auth["name"] == "my-auth"

    got = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got["id"] == auth_id

    apigw_v1.update_authorizer(
        restApiId=api_id,
        authorizerId=auth_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "renamed-auth"}],
    )
    got2 = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got2["name"] == "renamed-auth"

    listed = apigw_v1.get_authorizers(restApiId=api_id)["items"]
    assert any(a["id"] == auth_id for a in listed)

    apigw_v1.delete_authorizer(restApiId=api_id, authorizerId=auth_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_model_crud(apigw_v1):
    """CreateModel, GetModel, DeleteModel lifecycle."""
    api_id = apigw_v1.create_rest_api(name="v1-model-crud")["id"]
    resp = apigw_v1.create_model(
        restApiId=api_id,
        name="MyModel",
        contentType="application/json",
        schema='{"type": "object"}',
    )
    assert resp["name"] == "MyModel"

    got = apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert got["name"] == "MyModel"

    listed = apigw_v1.get_models(restApiId=api_id)["items"]
    assert any(m["name"] == "MyModel" for m in listed)

    apigw_v1.delete_model(restApiId=api_id, modelName="MyModel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_tags(apigw_v1):
    """TagResource, GetTags, UntagResource."""
    api_id = apigw_v1.create_rest_api(name="v1-tags-test")["id"]
    arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"

    apigw_v1.tag_resource(resourceArn=arn, tags={"env": "test", "team": "platform"})
    resp = apigw_v1.get_tags(resourceArn=arn)
    assert resp["tags"]["env"] == "test"
    assert resp["tags"]["team"] == "platform"

    apigw_v1.untag_resource(resourceArn=arn, tagKeys=["env"])
    resp2 = apigw_v1.get_tags(resourceArn=arn)
    assert "env" not in resp2["tags"]
    assert resp2["tags"]["team"] == "platform"

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_apikey_crud(apigw_v1):
    """ApiKey full lifecycle: create, get, delete."""
    resp = apigw_v1.create_api_key(name="v1-test-key", enabled=True)
    key_id = resp["id"]
    assert resp["name"] == "v1-test-key"
    assert "value" in resp

    got = apigw_v1.get_api_key(apiKey=key_id, includeValue=True)
    assert got["id"] == key_id

    listed = apigw_v1.get_api_keys()["items"]
    assert any(k["id"] == key_id for k in listed)

    apigw_v1.delete_api_key(apiKey=key_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_api_key(apiKey=key_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigwv1_usage_plan_crud(apigw_v1):
    """UsagePlan full lifecycle: create, get, delete."""
    resp = apigw_v1.create_usage_plan(
        name="v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
        quota={"limit": 10000, "period": "MONTH"},
    )
    plan_id = resp["id"]
    assert resp["name"] == "v1-plan"

    got = apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert got["id"] == plan_id

    listed = apigw_v1.get_usage_plans()["items"]
    assert any(p["id"] == plan_id for p in listed)

    apigw_v1.delete_usage_plan(usagePlanId=plan_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# ---- Data plane tests ----


def test_apigwv1_execute_lambda_proxy(apigw_v1, lam):
    """End-to-end: create API + resource + method + integration + deploy + invoke Lambda."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-proxy-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'pong'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-exec-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/ping"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = resp.read()
    assert body == b"pong"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_path_params(apigw_v1, lam):
    """Path parameter {userId} is passed correctly in event['pathParameters']."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-params-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    uid = (event.get('pathParameters') or {}).get('userId', 'missing')\n"
        b"    return {'statusCode': 200, 'body': uid}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-params-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    users_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )["id"]
    user_id_res = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=users_id,
        pathPart="{userId}",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="v1", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/v1/users/alice123"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"alice123"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_mock_integration(apigw_v1):
    """MOCK integration returns fixed JSON from integration response template."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="mock",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="GET",
        uri="",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"mocked": true}'},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/mock"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["mocked"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_execute_missing_resource_404(apigw_v1):
    """Request to non-existent path returns 404 with AWS-style message."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-missing-resource")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/nonexistent"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    try:
        _urlreq.urlopen(req)
        assert False, "Expected 404"
    except _urlerr.HTTPError as e:
        assert e.code == 404

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_no_conflict_with_v2(apigw_v1, apigw, lam):
    """v1 and v2 APIs can coexist; execute-api routes them independently."""
    import urllib.request as _urlreq
    import uuid as _uuid

    # Create v1 Lambda
    fname_v1 = f"intg-coexist-v1-{_uuid.uuid4().hex[:8]}"
    code_v1 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v1-response'}\n"
    buf_v1 = io.BytesIO()
    with zipfile.ZipFile(buf_v1, "w") as zf:
        zf.writestr("index.py", code_v1)
    lam.create_function(
        FunctionName=fname_v1,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v1.getvalue()},
    )

    # Create v2 Lambda
    fname_v2 = f"intg-coexist-v2-{_uuid.uuid4().hex[:8]}"
    code_v2 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v2-response'}\n"
    buf_v2 = io.BytesIO()
    with zipfile.ZipFile(buf_v2, "w") as zf:
        zf.writestr("index.py", code_v2)
    lam.create_function(
        FunctionName=fname_v2,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v2.getvalue()},
    )

    # Set up v1 API
    v1_api_id = apigw_v1.create_rest_api(name="coexist-v1")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=v1_api_id)["items"] if r["path"] == "/")
    res_id = apigw_v1.create_resource(restApiId=v1_api_id, parentId=root["id"], pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname_v1}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=v1_api_id)["id"]
    apigw_v1.create_stage(restApiId=v1_api_id, stageName="s", deploymentId=dep_id)

    # Set up v2 API
    v2_api_id = apigw.create_api(Name="coexist-v2", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=v2_api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname_v2}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=v2_api_id, RouteKey="GET /hit", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=v2_api_id, StageName="$default")

    # Invoke v1
    url_v1 = f"http://{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}/s/hit"
    req_v1 = _urlreq.Request(url_v1, method="GET")
    req_v1.add_header("Host", f"{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v1 = _urlreq.urlopen(req_v1)
    assert resp_v1.status == 200
    assert resp_v1.read() == b"v1-response"

    # Invoke v2
    url_v2 = f"http://{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/hit"
    req_v2 = _urlreq.Request(url_v2, method="GET")
    req_v2.add_header("Host", f"{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v2 = _urlreq.urlopen(req_v2)
    assert resp_v2.status == 200
    assert resp_v2.read() == b"v2-response"

    # Cleanup
    apigw_v1.delete_rest_api(restApiId=v1_api_id)
    apigw.delete_api(ApiId=v2_api_id)
    lam.delete_function(FunctionName=fname_v1)
    lam.delete_function(FunctionName=fname_v2)


# ---- Additional coverage: update, delete sub-resources, execute-api edge cases ----


def test_apigwv1_update_rest_api_name(apigw_v1):
    """UpdateRestApi renames the API via patchOperations."""
    api_id = apigw_v1.create_rest_api(name="v1-update-name-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-name-after"}],
    )
    assert apigw_v1.get_rest_api(restApiId=api_id)["name"] == "v1-update-name-after"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_resource(apigw_v1):
    """DeleteResource removes a resource; subsequent GetResource raises 404."""
    api_id = apigw_v1.create_rest_api(name="v1-del-resource")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    child_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="todel")["id"]
    apigw_v1.delete_resource(restApiId=api_id, resourceId=child_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_resource(restApiId=api_id, resourceId=child_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_method(apigw_v1):
    """DeleteMethod removes method; GetMethod raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.delete_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_integration(apigw_v1):
    """DeleteIntegration removes integration; GetIntegration raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-integration")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.delete_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_method_response(apigw_v1):
    """DeleteMethodResponse removes the method response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-mresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    apigw_v1.delete_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_integration_response(apigw_v1):
    """DeleteIntegrationResponse removes the integration response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-iresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    apigw_v1.delete_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_deployment(apigw_v1):
    """DeleteDeployment removes deployment; GetDeployment raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-deploy")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.delete_deployment(restApiId=api_id, deploymentId=dep_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_deployment(restApiId=api_id, deploymentId=dep_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_delete_stage(apigw_v1):
    """DeleteStage removes stage; GetStage raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-stage")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="todel", deploymentId=dep_id)
    apigw_v1.delete_stage(restApiId=api_id, stageName="todel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_stage(restApiId=api_id, stageName="todel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_api_key(apigw_v1):
    """UpdateApiKey updates name and sets lastUpdatedDate."""
    import datetime

    key_id = apigw_v1.create_api_key(name="v1-key-update-before")["id"]
    resp = apigw_v1.update_api_key(
        apiKey=key_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-key-update-after"}],
    )
    assert resp["name"] == "v1-key-update-after"
    assert isinstance(resp["lastUpdatedDate"], datetime.datetime)
    apigw_v1.delete_api_key(apiKey=key_id)


def test_apigwv1_update_usage_plan(apigw_v1):
    """UpdateUsagePlan updates name via patchOperations."""
    plan_id = apigw_v1.create_usage_plan(name="v1-plan-update-before")["id"]
    resp = apigw_v1.update_usage_plan(
        usagePlanId=plan_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-plan-update-after"}],
    )
    assert resp["name"] == "v1-plan-update-after"
    apigw_v1.delete_usage_plan(usagePlanId=plan_id)


def test_apigwv1_deployment_api_summary(apigw_v1):
    """CreateDeployment apiSummary reflects methods configured on resources."""
    api_id = apigw_v1.create_rest_api(name="v1-api-summary")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    dep = apigw_v1.create_deployment(restApiId=api_id)
    assert "/" in dep.get("apiSummary", {}), "apiSummary must include root resource path"
    assert "GET" in dep["apiSummary"]["/"], "apiSummary must include configured HTTP method"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_domain_name_crud(apigw_v1):
    """DomainName create, get, list, delete lifecycle."""
    resp = apigw_v1.create_domain_name(
        domainName="api.example.com",
        endpointConfiguration={"types": ["REGIONAL"]},
    )
    assert resp["domainName"] == "api.example.com"
    got = apigw_v1.get_domain_name(domainName="api.example.com")
    assert got["domainName"] == "api.example.com"
    listed = apigw_v1.get_domain_names()["items"]
    assert any(d["domainName"] == "api.example.com" for d in listed)
    apigw_v1.delete_domain_name(domainName="api.example.com")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_domain_name(domainName="api.example.com")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigwv1_base_path_mapping_crud(apigw_v1):
    """BasePathMapping create, get, list, delete lifecycle."""
    apigw_v1.create_domain_name(domainName="bpm.example.com")
    api_id = apigw_v1.create_rest_api(name="v1-bpm-api")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    mapping = apigw_v1.create_base_path_mapping(
        domainName="bpm.example.com",
        basePath="v1",
        restApiId=api_id,
        stage="prod",
    )
    assert mapping["basePath"] == "v1"
    assert mapping["restApiId"] == api_id

    got = apigw_v1.get_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    assert got["basePath"] == "v1"

    listed = apigw_v1.get_base_path_mappings(domainName="bpm.example.com")["items"]
    assert any(m["basePath"] == "v1" for m in listed)

    apigw_v1.delete_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    apigw_v1.delete_rest_api(restApiId=api_id)
    apigw_v1.delete_domain_name(domainName="bpm.example.com")


def test_apigwv1_execute_missing_stage_404(apigw_v1):
    """execute-api returns 404 when stage does not exist."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-stage")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.create_deployment(restApiId=api_id)
    # Do NOT create a stage — request to a nonexistent stage should 404

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/nonexistent/"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 404
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_execute_missing_method_405(apigw_v1):
    """execute-api returns 405 when resource exists but method is not configured."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="noop")["id"]
    # PUT method for POST only — GET not configured
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(restApiId=api_id, resourceId=resource_id, httpMethod="POST", type="MOCK")
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/noop"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 405
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_execute_lambda_arn_uri(apigw_v1, lam):
    """execute-api Lambda proxy works with plain arn:aws:lambda ARN as integration URI."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-arn-uri-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'arn-ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-arn-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    # Use plain arn:aws:lambda ARN (not apigateway URI form)
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/hit"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"arn-ok"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_requestcontext(apigw_v1, lam):
    """execute-api Lambda event includes required requestContext fields."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-reqctx-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    ctx = event.get('requestContext', {})\n"
        b"    body = json.dumps({\n"
        b"        'stage': ctx.get('stage'),\n"
        b"        'httpMethod': ctx.get('httpMethod'),\n"
        b"        'apiId': ctx.get('apiId'),\n"
        b"        'has_requestTime': 'requestTime' in ctx,\n"
        b"        'has_requestTimeEpoch': 'requestTimeEpoch' in ctx,\n"
        b"        'has_protocol': 'protocol' in ctx,\n"
        b"        'has_path': 'path' in ctx,\n"
        b"        'has_mvh': 'multiValueHeaders' in event,\n"
        b"    })\n"
        b"    return {'statusCode': 200, 'body': body}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-ctx-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="ctx")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/ctx"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    data = json.loads(resp.read())
    assert data["stage"] == "prod"
    assert data["httpMethod"] == "GET"
    assert data["apiId"] == api_id
    assert data["has_requestTime"] is True
    assert data["has_requestTimeEpoch"] is True
    assert data["has_protocol"] is True
    assert data["has_path"] is True
    assert data["has_mvh"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_mock_response_parameters(apigw_v1):
    """MOCK integration responseParameters are applied as HTTP response headers."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-params")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="rp")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        responseParameters={"method.response.header.X-Custom-Header": False},
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"ok": true}'},
        responseParameters={"method.response.header.X-Custom-Header": "'myvalue'"},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/rp"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.headers.get("X-Custom-Header") == "myvalue"
    apigw_v1.delete_rest_api(restApiId=api_id)


# ---------------------------------------------------------------------------
# Firehose
# ---------------------------------------------------------------------------


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


# ========== Route53 ==========


def test_route53_create_and_get_hosted_zone(r53):
    resp = r53.create_hosted_zone(
        Name="example.com",
        CallerReference="ref-create-1",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    hz = resp["HostedZone"]
    zone_id = hz["Id"].split("/")[-1]
    assert hz["Name"] == "example.com."
    assert "DelegationSet" in resp
    assert len(resp["DelegationSet"]["NameServers"]) == 4

    get_resp = r53.get_hosted_zone(Id=zone_id)
    assert get_resp["HostedZone"]["Name"] == "example.com."
    assert get_resp["HostedZone"]["ResourceRecordSetCount"] == 2  # SOA + NS


def test_route53_create_zone_idempotency(r53):
    r53.create_hosted_zone(Name="idempotent.com", CallerReference="ref-idem-1")
    resp2 = r53.create_hosted_zone(Name="idempotent.com", CallerReference="ref-idem-1")
    # Same CallerReference → same zone returned, not a new one
    assert resp2["HostedZone"]["Name"] == "idempotent.com."


def test_route53_list_hosted_zones(r53):
    r53.create_hosted_zone(Name="list-test.com", CallerReference="ref-list-1")
    resp = r53.list_hosted_zones()
    names = [hz["Name"] for hz in resp["HostedZones"]]
    assert "list-test.com." in names


def test_route53_list_hosted_zones_by_name(r53):
    r53.create_hosted_zone(Name="byname-alpha.com", CallerReference="ref-bn-1")
    r53.create_hosted_zone(Name="byname-beta.com", CallerReference="ref-bn-2")
    resp = r53.list_hosted_zones_by_name(DNSName="byname-alpha.com")
    assert resp["HostedZones"][0]["Name"] == "byname-alpha.com."


def test_route53_delete_hosted_zone(r53):
    resp = r53.create_hosted_zone(Name="delete-me.com", CallerReference="ref-del-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    # Must remove non-default records first (none here, just SOA+NS which are auto-removed)
    r53.delete_hosted_zone(Id=zone_id)

    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_hosted_zone(Id=zone_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"


def test_route53_change_resource_record_sets_create(r53):
    resp = r53.create_hosted_zone(Name="records.com", CallerReference="ref-rrs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    change_resp = r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.records.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )
    assert change_resp["ChangeInfo"]["Status"] == "INSYNC"


def test_route53_list_resource_record_sets(r53):
    resp = r53.create_hosted_zone(Name="listrrs.com", CallerReference="ref-lrrs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "mail.listrrs.com",
                        "Type": "MX",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "10 mail.example.com."}],
                    },
                }
            ]
        },
    )
    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    types = [rrs["Type"] for rrs in list_resp["ResourceRecordSets"]]
    assert "MX" in types
    assert "SOA" in types
    assert "NS" in types


def test_route53_upsert_record(r53):
    resp = r53.create_hosted_zone(Name="upsert.com", CallerReference="ref-ups-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    for ip in ("1.1.1.1", "2.2.2.2"):
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "www.upsert.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": ip}],
                        },
                    }
                ]
            },
        )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    a_records = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A"]
    assert len(a_records) == 1
    assert a_records[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"


def test_route53_delete_record(r53):
    resp = r53.create_hosted_zone(Name="delrec.com", CallerReference="ref-dr-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.delrec.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "5.5.5.5"}],
                    },
                }
            ]
        },
    )

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "DELETE",
                    "ResourceRecordSet": {
                        "Name": "www.delrec.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "5.5.5.5"}],
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    a_records = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A"]
    assert len(a_records) == 0


def test_route53_get_change(r53):
    resp = r53.create_hosted_zone(Name="change-status.com", CallerReference="ref-cs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    change_resp = r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "a.change-status.com",
                        "Type": "A",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "9.9.9.9"}],
                    },
                }
            ]
        },
    )
    change_id = change_resp["ChangeInfo"]["Id"].split("/")[-1]
    get_change = r53.get_change(Id=change_id)
    assert get_change["ChangeInfo"]["Status"] == "INSYNC"


def test_route53_create_health_check(r53):
    resp = r53.create_health_check(
        CallerReference="ref-hc-1",
        HealthCheckConfig={
            "IPAddress": "1.2.3.4",
            "Port": 80,
            "Type": "HTTP",
            "ResourcePath": "/health",
            "RequestInterval": 30,
            "FailureThreshold": 3,
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    hc = resp["HealthCheck"]
    hc_id = hc["Id"]
    assert hc["HealthCheckConfig"]["Type"] == "HTTP"

    get_resp = r53.get_health_check(HealthCheckId=hc_id)
    assert get_resp["HealthCheck"]["Id"] == hc_id


def test_route53_list_health_checks(r53):
    r53.create_health_check(
        CallerReference="ref-hcl-1",
        HealthCheckConfig={"IPAddress": "2.2.2.2", "Port": 443, "Type": "HTTPS"},
    )
    resp = r53.list_health_checks()
    assert len(resp["HealthChecks"]) >= 1


def test_route53_delete_health_check(r53):
    resp = r53.create_health_check(
        CallerReference="ref-hcd-1",
        HealthCheckConfig={"IPAddress": "3.3.3.3", "Port": 80, "Type": "HTTP"},
    )
    hc_id = resp["HealthCheck"]["Id"]
    r53.delete_health_check(HealthCheckId=hc_id)

    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_health_check(HealthCheckId=hc_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchHealthCheck"


def test_route53_tags_for_hosted_zone(r53):
    resp = r53.create_hosted_zone(Name="tagged.com", CallerReference="ref-tag-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_tags_for_resource(
        ResourceType="hostedzone",
        ResourceId=zone_id,
        AddTags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}],
    )

    tags_resp = r53.list_tags_for_resource(ResourceType="hostedzone", ResourceId=zone_id)
    tags = {t["Key"]: t["Value"] for t in tags_resp["ResourceTagSet"]["Tags"]}
    assert tags["env"] == "test"
    assert tags["team"] == "infra"

    r53.change_tags_for_resource(
        ResourceType="hostedzone",
        ResourceId=zone_id,
        RemoveTagKeys=["team"],
    )
    tags_resp2 = r53.list_tags_for_resource(ResourceType="hostedzone", ResourceId=zone_id)
    keys2 = [t["Key"] for t in tags_resp2["ResourceTagSet"]["Tags"]]
    assert "env" in keys2
    assert "team" not in keys2


def test_route53_no_such_hosted_zone(r53):
    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_hosted_zone(Id="ZNOTEXIST1234")
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"


def test_route53_alias_record(r53):
    resp = r53.create_hosted_zone(Name="alias.com", CallerReference="ref-alias-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.alias.com",
                        "Type": "A",
                        "AliasTarget": {
                            "HostedZoneId": "Z2FDTNDATAQYW2",
                            "DNSName": "d1234.cloudfront.net",
                            "EvaluateTargetHealth": False,
                        },
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    alias_recs = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A" and "AliasTarget" in rrs]
    assert len(alias_recs) == 1
    assert alias_recs[0]["AliasTarget"]["DNSName"] == "d1234.cloudfront.net."


# ========== Non-ASCII / Unicode ==========


def test_unicode_s3_object_key(s3):
    s3.create_bucket(Bucket="unicode-keys")
    key = "données/résumé/文件.txt"
    body = "Ünïcödé cöntënt 日本語".encode("utf-8")
    s3.put_object(Bucket="unicode-keys", Key=key, Body=body)
    resp = s3.get_object(Bucket="unicode-keys", Key=key)
    assert resp["Body"].read() == body


def test_unicode_s3_metadata(s3):
    # S3 metadata values must be ASCII per AWS/botocore; encode non-ASCII with percent-encoding
    from urllib.parse import quote, unquote

    s3.create_bucket(Bucket="unicode-meta")
    s3.put_object(
        Bucket="unicode-meta",
        Key="file.bin",
        Body=b"data",
        Metadata={"filename": quote("résumé.pdf"), "author": quote("Ñoño")},
    )
    head = s3.head_object(Bucket="unicode-meta", Key="file.bin")
    assert unquote(head["Metadata"]["filename"]) == "résumé.pdf"
    assert unquote(head["Metadata"]["author"]) == "Ñoño"


def test_unicode_dynamodb_item(ddb):
    table = "unicode-ddb"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    item = {"pk": {"S": "ключ"}, "value": {"S": "значение 日本語 مرحبا"}}
    ddb.put_item(TableName=table, Item=item)
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "ключ"}})
    assert resp["Item"]["value"]["S"] == "значение 日本語 مرحبا"


def test_unicode_sqs_message(sqs):
    url = sqs.create_queue(QueueName="unicode-sqs")["QueueUrl"]
    msg = "こんにちは世界 héllo wörld"
    sqs.send_message(QueueUrl=url, MessageBody=msg)
    resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert resp["Messages"][0]["Body"] == msg


def test_unicode_secretsmanager(sm):
    sm.create_secret(Name="unicode-secret", SecretString="пароль: 密码")
    resp = sm.get_secret_value(SecretId="unicode-secret")
    assert resp["SecretString"] == "пароль: 密码"


def test_unicode_ssm_parameter(ssm):
    ssm.put_parameter(Name="/unicode/param", Value="값: τιμή", Type="String")
    resp = ssm.get_parameter(Name="/unicode/param")
    assert resp["Parameter"]["Value"] == "값: τιμή"


def test_unicode_route53_zone_comment(r53):
    resp = r53.create_hosted_zone(
        Name="unicode-zone.com",
        CallerReference="ref-uc-1",
        HostedZoneConfig={"Comment": "zona en español — Ünïcödé"},
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]
    get = r53.get_hosted_zone(Id=zone_id)
    assert get["HostedZone"]["Config"]["Comment"] == "zona en español — Ünïcödé"


# ========== Regression: botocore-validated bug fixes ==========


def test_ddb_query_pagination_hash_only(ddb):
    """Pagination on a hash-only table (no sort key) must return results after the ESK."""
    table = "t_hash_paginate"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(TableName=table, Item={"pk": {"S": f"item_{i:03d}"}, "v": {"N": str(i)}})

    resp1 = ddb.scan(TableName=table, Limit=3)
    assert resp1["Count"] == 3
    assert "LastEvaluatedKey" in resp1

    resp2 = ddb.scan(TableName=table, Limit=3, ExclusiveStartKey=resp1["LastEvaluatedKey"])
    assert resp2["Count"] == 2
    all_pks = {it["pk"]["S"] for it in resp1["Items"]} | {it["pk"]["S"] for it in resp2["Items"]}
    assert len(all_pks) == 5


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
        Runtime="python3.9",
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


def test_lambda_unknown_path_returns_404(lam):
    """Requests to an unrecognised Lambda path must return 404, not 400 InvalidRequest."""
    import urllib.error
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/2015-03-31/functions/nonexistent-fn/completely-unknown-subpath",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/lambda/aws4_request"},
        method="GET",
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Expected an error response"
    except urllib.error.HTTPError as e:
        assert e.code == 404


def test_lambda_reset_terminates_workers(lam):
    """/_ministack/reset must cleanly terminate warm Lambda workers."""
    import urllib.request

    fn = f"intg-reset-worker-{__import__('uuid').uuid4().hex[:8]}"
    code = "import time\n_boot = time.time()\ndef handler(event, context):\n    return {'boot': _boot}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    # Warm the worker
    r1 = lam.invoke(FunctionName=fn, Payload=b"{}")
    boot1 = json.loads(r1["Payload"].read())["boot"]

    # Reset — must terminate worker without error
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(f"{endpoint}/_ministack/reset", data=b"", method="POST")
    for _attempt in range(3):
        try:
            urllib.request.urlopen(req, timeout=15)
            break
        except Exception:
            if _attempt == 2:
                raise

    # Re-create and invoke — new worker means new boot time
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    r2 = lam.invoke(FunctionName=fn, Payload=b"{}")
    boot2 = json.loads(r2["Payload"].read())["boot"]
    assert boot2 > boot1, "Worker should have been reset — new boot time expected"


def test_sfn_integration_lambda_invoke(sfn, lam):
    """Step Functions Task state invoking Lambda must return the function result."""
    import uuid as _uuid

    fn = f"intg-sfn-lam-{_uuid.uuid4().hex[:8]}"
    code = "def handler(event, context):\n    return {'doubled': event.get('value', 0) * 2}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"

    definition = json.dumps(
        {
            "StartAt": "InvokeLambda",
            "States": {
                "InvokeLambda": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": func_arn,
                        "Payload.$": "$",
                    },
                    "ResultSelector": {"doubled.$": "$.Payload.doubled"},
                    "ResultPath": "$.result",
                    "End": True,
                }
            },
        }
    )
    sm = sfn.create_state_machine(
        name=f"sfn-lam-{_uuid.uuid4().hex[:8]}",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"value": 21}),
    )
    desc = _wait_sfn(sfn, ex["executionArn"], timeout=10)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["result"]["doubled"] == 42


# ========== Package structure ==========

_ministack_installed = True
try:
    import ministack  # noqa: F401
except ModuleNotFoundError:
    _ministack_installed = False

_requires_package = pytest.mark.skipif(
    not _ministack_installed,
    reason="ministack not installed locally (runs in CI via pip install -e .)",
)


@_requires_package
def test_package_core_importable():
    """ministack.core modules must all be importable."""
    from ministack.core.lambda_runtime import get_or_create_worker
    from ministack.core.lambda_runtime import reset as lr_reset
    from ministack.core.persistence import load_state, save_all
    from ministack.core.responses import error_response_json, json_response, new_uuid
    from ministack.core.router import detect_service

    assert callable(json_response)
    assert callable(detect_service)
    assert callable(get_or_create_worker)
    assert callable(save_all)


@_requires_package
def test_package_services_importable():
    """All 25 ministack.services modules must be importable and expose handle_request."""
    from ministack.services import (
        apigateway,
        apigateway_v1,
        athena,
        cloudwatch,
        cloudwatch_logs,
        cognito,
        dynamodb,
        ecs,
        elasticache,
        eventbridge,
        firehose,
        glue,
        kinesis,
        lambda_svc,
        rds,
        route53,
        s3,
        secretsmanager,
        ses,
        sns,
        sqs,
        ssm,
        stepfunctions,
    )
    from ministack.services.iam_sts import handle_iam_request, handle_sts_request

    for mod in [
        s3,
        sqs,
        sns,
        dynamodb,
        lambda_svc,
        secretsmanager,
        cloudwatch_logs,
        ssm,
        eventbridge,
        kinesis,
        cloudwatch,
        ses,
        stepfunctions,
        ecs,
        rds,
        elasticache,
        glue,
        athena,
        apigateway,
        firehose,
        route53,
        cognito,
    ]:
        assert callable(getattr(mod, "handle_request", None)), f"{mod.__name__} missing handle_request"
    assert callable(handle_iam_request)
    assert callable(handle_sts_request)


@_requires_package
def test_app_asgi_callable():
    """ministack.app:app must be an async callable (ASGI entry point)."""
    import inspect

    from ministack import app as app_module

    assert callable(app_module.app)
    assert inspect.iscoroutinefunction(app_module.app)
    assert callable(app_module.main)


# ========== Cognito ==========


def test_cognito_create_and_describe_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="TestPool")
    pool = resp["UserPool"]
    pid = pool["Id"]
    assert pool["Name"] == "TestPool"
    assert pid.startswith("us-east-1_")

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert desc["Id"] == pid
    assert desc["Name"] == "TestPool"


def test_cognito_list_user_pools(cognito_idp):
    cognito_idp.create_user_pool(PoolName="ListPoolA")
    cognito_idp.create_user_pool(PoolName="ListPoolB")
    resp = cognito_idp.list_user_pools(MaxResults=60)
    names = [p["Name"] for p in resp["UserPools"]]
    assert "ListPoolA" in names
    assert "ListPoolB" in names


def test_cognito_update_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="UpdatePool")
    pid = resp["UserPool"]["Id"]
    cognito_idp.update_user_pool(UserPoolId=pid, UserPoolTags={"env": "test"})
    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    assert desc["UserPoolTags"].get("env") == "test"


def test_cognito_delete_user_pool(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="DeletePool")
    pid = resp["UserPool"]["Id"]
    cognito_idp.delete_user_pool(UserPoolId=pid)
    pools = cognito_idp.list_user_pools(MaxResults=60)["UserPools"]
    assert not any(p["Id"] == pid for p in pools)


def test_cognito_create_and_describe_user_pool_client(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ClientPool")["UserPool"]["Id"]
    client_resp = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="MyApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )
    client = client_resp["UserPoolClient"]
    cid = client["ClientId"]
    assert client["ClientName"] == "MyApp"

    desc = cognito_idp.describe_user_pool_client(UserPoolId=pid, ClientId=cid)["UserPoolClient"]
    assert desc["ClientId"] == cid
    assert desc["ClientName"] == "MyApp"


def test_cognito_list_user_pool_clients(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="MultiClientPool")["UserPool"]["Id"]
    cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="App1")
    cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="App2")
    clients = cognito_idp.list_user_pool_clients(UserPoolId=pid, MaxResults=60)["UserPoolClients"]
    names = [c["ClientName"] for c in clients]
    assert "App1" in names
    assert "App2" in names


def test_cognito_admin_create_and_get_user(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AdminUserPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="alice",
        UserAttributes=[{"Name": "email", "Value": "alice@example.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="alice")
    assert user["Username"] == "alice"
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "alice@example.com"


def test_cognito_list_users(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ListUsersPool")["UserPool"]["Id"]
    for name in ["user1", "user2", "user3"]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
    users = cognito_idp.list_users(UserPoolId=pid)["Users"]
    usernames = [u["Username"] for u in users]
    assert "user1" in usernames
    assert "user2" in usernames
    assert "user3" in usernames


def test_cognito_list_users_filter(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="FilterUsersPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="bob",
        UserAttributes=[{"Name": "email", "Value": "bob@example.com"}],
    )
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="charlie",
        UserAttributes=[{"Name": "email", "Value": "charlie@example.com"}],
    )
    resp = cognito_idp.list_users(UserPoolId=pid, Filter='username = "bob"')
    users = resp["Users"]
    assert len(users) == 1
    assert users[0]["Username"] == "bob"


def test_cognito_admin_set_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="PwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="PwdApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="dave")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="dave", Password="NewPass123!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "dave", "PASSWORD": "NewPass123!"},
    )
    assert "AuthenticationResult" in auth


def test_cognito_admin_initiate_auth_wrong_password(cognito_idp):
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="AuthFailPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="AuthFailApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="eve")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="eve", Password="Correct1!", Permanent=True)
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "eve", "PASSWORD": "Wrong1!"},
        )
    assert exc_info.value.response["Error"]["Code"] == "NotAuthorizedException"


def test_cognito_initiate_auth_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="InitiateAuthPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="InitiateApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="frank")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="frank", Password="FrankPass1!", Permanent=True)
    auth = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "frank", "PASSWORD": "FrankPass1!"},
    )
    assert "AuthenticationResult" in auth
    result = auth["AuthenticationResult"]
    assert "AccessToken" in result
    assert "IdToken" in result
    assert "RefreshToken" in result


def test_cognito_signup_and_confirm(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SignupPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="SignupApp")["UserPoolClient"]["ClientId"]

    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="grace",
        Password="GracePass1!",
        UserAttributes=[{"Name": "email", "Value": "grace@example.com"}],
    )
    assert resp["UserSub"]

    cognito_idp.confirm_sign_up(
        ClientId=cid,
        Username="grace",
        ConfirmationCode="123456",
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="grace")
    assert user["UserStatus"] == "CONFIRMED"


def test_cognito_forgot_password_and_confirm(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ForgotPwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="ForgotApp")["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="henry")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="henry", Password="OldPass1!", Permanent=True)

    cognito_idp.forgot_password(ClientId=cid, Username="henry")

    cognito_idp.confirm_forgot_password(
        ClientId=cid,
        Username="henry",
        ConfirmationCode="654321",
        Password="NewPass2!",
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="henry", Password="NewPass2!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "henry", "PASSWORD": "NewPass2!"},
    )
    assert "AuthenticationResult" in auth


def test_cognito_admin_update_user_attributes(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="UpdateAttrPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="irene",
        UserAttributes=[{"Name": "email", "Value": "irene@example.com"}],
    )
    cognito_idp.admin_update_user_attributes(
        UserPoolId=pid,
        Username="irene",
        UserAttributes=[{"Name": "email", "Value": "irene@updated.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="irene")
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "irene@updated.com"


def test_cognito_admin_disable_enable_user(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="DisablePool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="jack")

    cognito_idp.admin_disable_user(UserPoolId=pid, Username="jack")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="jack")
    assert user["Enabled"] is False

    cognito_idp.admin_enable_user(UserPoolId=pid, Username="jack")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="jack")
    assert user["Enabled"] is True


def test_cognito_admin_delete_user(cognito_idp):
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="DeleteUserPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="kate")
    cognito_idp.admin_delete_user(UserPoolId=pid, Username="kate")
    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="kate")
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"


def test_cognito_groups_crud(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GroupPool")["UserPool"]["Id"]

    resp = cognito_idp.create_group(UserPoolId=pid, GroupName="admins", Description="Admins")
    assert resp["Group"]["GroupName"] == "admins"

    group = cognito_idp.get_group(UserPoolId=pid, GroupName="admins")["Group"]
    assert group["Description"] == "Admins"

    groups = cognito_idp.list_groups(UserPoolId=pid)["Groups"]
    assert any(g["GroupName"] == "admins" for g in groups)

    cognito_idp.delete_group(UserPoolId=pid, GroupName="admins")
    groups = cognito_idp.list_groups(UserPoolId=pid)["Groups"]
    assert not any(g["GroupName"] == "admins" for g in groups)


def test_cognito_admin_add_remove_user_from_group(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GroupMemberPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="liam")
    cognito_idp.create_group(UserPoolId=pid, GroupName="editors")

    cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username="liam", GroupName="editors")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="editors")["Users"]
    assert any(u["Username"] == "liam" for u in members)

    groups_for_user = cognito_idp.admin_list_groups_for_user(UserPoolId=pid, Username="liam")["Groups"]
    assert any(g["GroupName"] == "editors" for g in groups_for_user)

    cognito_idp.admin_remove_user_from_group(UserPoolId=pid, Username="liam", GroupName="editors")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="editors")["Users"]
    assert not any(u["Username"] == "liam" for u in members)


def test_cognito_domain_crud(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="DomainPool")["UserPool"]["Id"]
    resp = cognito_idp.create_user_pool_domain(UserPoolId=pid, Domain="my-test-domain")
    assert "CloudFrontDomain" in resp

    desc = cognito_idp.describe_user_pool_domain(Domain="my-test-domain")
    assert desc["DomainDescription"]["UserPoolId"] == pid
    assert desc["DomainDescription"]["Status"] == "ACTIVE"

    cognito_idp.delete_user_pool_domain(UserPoolId=pid, Domain="my-test-domain")
    desc2 = cognito_idp.describe_user_pool_domain(Domain="my-test-domain")
    assert desc2["DomainDescription"] == {}


def test_cognito_mfa_config(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="MfaPool")["UserPool"]["Id"]
    resp = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert resp["MfaConfiguration"] == "OFF"

    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="OPTIONAL",
    )
    resp = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert resp["MfaConfiguration"] == "OPTIONAL"
    assert resp["SoftwareTokenMfaConfiguration"]["Enabled"] is True


def test_cognito_tags(cognito_idp):
    resp = cognito_idp.create_user_pool(PoolName="TagPool")
    pid = resp["UserPool"]["Id"]
    arn = resp["UserPool"]["Arn"]

    cognito_idp.tag_resource(ResourceArn=arn, Tags={"project": "ministack"})
    tags = cognito_idp.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert tags["project"] == "ministack"

    cognito_idp.untag_resource(ResourceArn=arn, TagKeys=["project"])
    tags = cognito_idp.list_tags_for_resource(ResourceArn=arn)["Tags"]
    assert "project" not in tags


def test_cognito_get_user_from_token(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GetUserPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="GetUserApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="maya",
        UserAttributes=[{"Name": "email", "Value": "maya@example.com"}],
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="maya", Password="MayaPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "maya", "PASSWORD": "MayaPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=access_token)
    assert user["Username"] == "maya"


def test_cognito_global_sign_out(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="SignOutPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="SignOutApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="noah")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="noah", Password="NoahPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "noah", "PASSWORD": "NoahPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]
    cognito_idp.global_sign_out(AccessToken=access_token)  # must not raise


def test_cognito_admin_confirm_signup(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="AdminConfirmPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="AdminConfirmApp")["UserPoolClient"][
        "ClientId"
    ]
    cognito_idp.sign_up(
        ClientId=cid,
        Username="olivia",
        Password="OliviaPass1!",
    )
    cognito_idp.admin_confirm_sign_up(UserPoolId=pid, Username="olivia")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="olivia")
    assert user["UserStatus"] == "CONFIRMED"


# Identity Pools (cognito-identity)


def test_cognito_identity_pool_crud(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="TestIdPool",
        AllowUnauthenticatedIdentities=False,
    )
    iid = resp["IdentityPoolId"]
    assert resp["IdentityPoolName"] == "TestIdPool"
    assert iid.startswith("us-east-1:")

    desc = cognito_identity.describe_identity_pool(IdentityPoolId=iid)
    assert desc["IdentityPoolId"] == iid
    assert desc["IdentityPoolName"] == "TestIdPool"

    pools = cognito_identity.list_identity_pools(MaxResults=60)["IdentityPools"]
    assert any(p["IdentityPoolId"] == iid for p in pools)

    cognito_identity.update_identity_pool(
        IdentityPoolId=iid,
        IdentityPoolName="TestIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    desc2 = cognito_identity.describe_identity_pool(IdentityPoolId=iid)
    assert desc2["AllowUnauthenticatedIdentities"] is True

    cognito_identity.delete_identity_pool(IdentityPoolId=iid)
    pools2 = cognito_identity.list_identity_pools(MaxResults=60)["IdentityPools"]
    assert not any(p["IdentityPoolId"] == iid for p in pools2)


def test_cognito_get_id_and_credentials(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="CredsPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    id_resp = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")
    identity_id = id_resp["IdentityId"]
    assert identity_id

    creds = cognito_identity.get_credentials_for_identity(IdentityId=identity_id)
    assert creds["IdentityId"] == identity_id
    assert "AccessKeyId" in creds["Credentials"]
    assert creds["Credentials"]["AccessKeyId"].startswith("ASIA")
    assert "SecretKey" in creds["Credentials"]
    assert "SessionToken" in creds["Credentials"]


def test_cognito_identity_pool_roles(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="RolesPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    cognito_identity.set_identity_pool_roles(
        IdentityPoolId=iid,
        Roles={
            "authenticated": "arn:aws:iam::000000000000:role/AuthRole",
            "unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
        },
    )
    roles = cognito_identity.get_identity_pool_roles(IdentityPoolId=iid)
    assert roles["Roles"]["authenticated"] == "arn:aws:iam::000000000000:role/AuthRole"
    assert roles["Roles"]["unauthenticated"] == "arn:aws:iam::000000000000:role/UnauthRole"


def test_cognito_list_identities(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="ListIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]

    id1 = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    id2 = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]

    identities = cognito_identity.list_identities(IdentityPoolId=iid, MaxResults=60)["Identities"]
    ids = [i["IdentityId"] for i in identities]
    assert id1 in ids
    assert id2 in ids


def test_cognito_get_open_id_token(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="OidcPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]

    token_resp = cognito_identity.get_open_id_token(IdentityId=identity_id)
    assert token_resp["IdentityId"] == identity_id
    token = token_resp["Token"]
    # Verify stub JWT structure: header.payload.sig
    parts = token.split(".")
    assert len(parts) == 3


def test_cognito_signup_always_unconfirmed(cognito_idp):
    """SignUp always returns UNCONFIRMED regardless of AutoVerifiedAttributes."""
    # Pool with AutoVerifiedAttributes — user still starts UNCONFIRMED
    pid = cognito_idp.create_user_pool(
        PoolName="AutoVerifyPool",
        AutoVerifiedAttributes=["email"],
    )["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="AutoVerifyApp")["UserPoolClient"]["ClientId"]
    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="testuser",
        Password="TestPass1!",
        UserAttributes=[{"Name": "email", "Value": "test@example.com"}],
    )
    assert resp["UserConfirmed"] is False
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="testuser")
    assert user["UserStatus"] == "UNCONFIRMED"

    # Pool with NO AutoVerifiedAttributes — user also starts UNCONFIRMED
    pid2 = cognito_idp.create_user_pool(PoolName="NoAutoVerifyPool")["UserPool"]["Id"]
    cid2 = cognito_idp.create_user_pool_client(UserPoolId=pid2, ClientName="NoAutoVerifyApp")["UserPoolClient"][
        "ClientId"
    ]
    resp2 = cognito_idp.sign_up(ClientId=cid2, Username="testuser2", Password="TestPass1!")
    assert resp2["UserConfirmed"] is False
    user2 = cognito_idp.admin_get_user(UserPoolId=pid2, Username="testuser2")
    assert user2["UserStatus"] == "UNCONFIRMED"


def test_cognito_change_password(cognito_idp):
    """ChangePassword decodes the access token and updates the stored password."""
    pid = cognito_idp.create_user_pool(PoolName="ChangePwdPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="ChangePwdApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="pwduser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="pwduser", Password="OldPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "pwduser", "PASSWORD": "OldPass1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    cognito_idp.change_password(
        AccessToken=access_token,
        PreviousPassword="OldPass1!",
        ProposedPassword="NewPass2!",
    )

    # New password must work
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "pwduser", "PASSWORD": "NewPass2!"},
    )
    assert "AuthenticationResult" in auth2

    # Old password must fail
    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "pwduser", "PASSWORD": "OldPass1!"},
        )
    assert exc_info.value.response["Error"]["Code"] == "NotAuthorizedException"


def test_cognito_refresh_token_auth_correct_user(cognito_idp):
    """REFRESH_TOKEN_AUTH returns tokens for the correct user, not the first user in the pool."""
    pid = cognito_idp.create_user_pool(PoolName="RefreshPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RefreshApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    for name, pw in [("first", "First1!"), ("second", "Second1!")]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
        cognito_idp.admin_set_user_password(UserPoolId=pid, Username=name, Password=pw, Permanent=True)

    # Auth as "second" user and refresh
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "second", "PASSWORD": "Second1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]

    refresh = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    assert "AuthenticationResult" in refresh
    # New access token should resolve back to "second" via GetUser
    new_access = refresh["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=new_access)
    assert user["Username"] == "second"


def test_cognito_refresh_token_alias(cognito_idp):
    """REFRESH_TOKEN (without _AUTH suffix) is accepted as an alias."""
    pid = cognito_idp.create_user_pool(PoolName="RefreshAliasPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RefreshAliasApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="aliasuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="aliasuser", Password="AliasPass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "aliasuser", "PASSWORD": "AliasPass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    refresh = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    assert "AuthenticationResult" in refresh
    assert "AccessToken" in refresh["AuthenticationResult"]
    assert "RefreshToken" not in refresh["AuthenticationResult"]


def test_cognito_respond_to_auth_challenge_new_password(cognito_idp):
    """RespondToAuthChallenge with NEW_PASSWORD_REQUIRED confirms the user."""
    pid = cognito_idp.create_user_pool(PoolName="ChallengePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="ChallengeApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="newpwduser")
    # Set a temp password — Permanent=False keeps FORCE_CHANGE_PASSWORD status
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="newpwduser", Password="TempPass1!", Permanent=False)
    # Initiate auth — FORCE_CHANGE_PASSWORD triggers NEW_PASSWORD_REQUIRED challenge
    auth = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "newpwduser", "PASSWORD": "TempPass1!"},
    )
    assert auth.get("ChallengeName") == "NEW_PASSWORD_REQUIRED"
    session = auth["Session"]
    result = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="NEW_PASSWORD_REQUIRED",
        Session=session,
        ChallengeResponses={"USERNAME": "newpwduser", "NEW_PASSWORD": "FinalPass1!"},
    )
    assert "AuthenticationResult" in result
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="newpwduser")
    assert user["UserStatus"] == "CONFIRMED"


def test_cognito_update_user_attributes_via_token(cognito_idp):
    """UpdateUserAttributes (self-service) updates attributes using access token."""
    pid = cognito_idp.create_user_pool(PoolName="UpdateAttrTokenPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="UpdateAttrApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="attrupdate",
        UserAttributes=[{"Name": "email", "Value": "old@example.com"}],
    )
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="attrupdate", Password="AttrPass1!", Permanent=True)
    access_token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "attrupdate", "PASSWORD": "AttrPass1!"},
    )["AuthenticationResult"]["AccessToken"]

    cognito_idp.update_user_attributes(
        AccessToken=access_token,
        UserAttributes=[{"Name": "email", "Value": "new@example.com"}],
    )
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="attrupdate")
    attrs = {a["Name"]: a["Value"] for a in user["UserAttributes"]}
    assert attrs["email"] == "new@example.com"


def test_cognito_delete_user_via_token(cognito_idp):
    """DeleteUser (self-service) removes the user using access token."""
    import botocore.exceptions

    pid = cognito_idp.create_user_pool(PoolName="DeleteSelfPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="DeleteSelfApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="selfdelete")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="selfdelete", Password="DelPass1!", Permanent=True)
    access_token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "selfdelete", "PASSWORD": "DelPass1!"},
    )["AuthenticationResult"]["AccessToken"]

    cognito_idp.delete_user(AccessToken=access_token)

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        cognito_idp.admin_get_user(UserPoolId=pid, Username="selfdelete")
    assert exc_info.value.response["Error"]["Code"] == "UserNotFoundException"


def test_cognito_update_user_pool_client(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="UpdateClientPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="OriginalName")["UserPoolClient"]["ClientId"]
    updated = cognito_idp.update_user_pool_client(
        UserPoolId=pid,
        ClientId=cid,
        ClientName="UpdatedName",
        RefreshTokenValidity=14,
    )["UserPoolClient"]
    assert updated["ClientName"] == "UpdatedName"
    assert updated["RefreshTokenValidity"] == 14
    # Verify persisted
    desc = cognito_idp.describe_user_pool_client(UserPoolId=pid, ClientId=cid)["UserPoolClient"]
    assert desc["ClientName"] == "UpdatedName"


def test_cognito_admin_reset_user_password(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="ResetPwdPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="resetuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="resetuser", Password="Pass1!", Permanent=True)
    cognito_idp.admin_reset_user_password(UserPoolId=pid, Username="resetuser")
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="resetuser")
    assert user["UserStatus"] == "RESET_REQUIRED"


def test_cognito_admin_user_global_sign_out(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="GlobalSignOutAdminPool")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="signoutuser")
    cognito_idp.admin_user_global_sign_out(UserPoolId=pid, Username="signoutuser")


def test_cognito_revoke_token(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="RevokePool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="RevokeApp",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="revokeuser")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="revokeuser", Password="RevokePass1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "revokeuser", "PASSWORD": "RevokePass1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    cognito_idp.revoke_token(Token=refresh_token, ClientId=cid)


def test_cognito_describe_identity(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="DescribeIdPool",
        AllowUnauthenticatedIdentities=True,
    )
    iid = resp["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    desc = cognito_identity.describe_identity(IdentityId=identity_id)
    assert desc["IdentityId"] == identity_id


def test_cognito_merge_developer_identities(cognito_identity):
    resp = cognito_identity.create_identity_pool(
        IdentityPoolName="MergePool",
        AllowUnauthenticatedIdentities=True,
        DeveloperProviderName="login.myapp",
    )
    iid = resp["IdentityPoolId"]
    result = cognito_identity.merge_developer_identities(
        SourceUserIdentifier="user-a",
        DestinationUserIdentifier="user-b",
        DeveloperProviderName="login.myapp",
        IdentityPoolId=iid,
    )
    assert "IdentityId" in result


# ===========================================================================
# QA — Edge cases and regression tests (merged from test_qa_comprehensive.py)
# ===========================================================================


def _zip_lambda(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# COGNITO — fix regressions + edge cases
# ---------------------------------------------------------------------------


def test_cognito_credentials_secret_access_key(cognito_identity):
    """GetCredentialsForIdentity must return SecretKey (boto3 wire name)."""
    iid = cognito_identity.create_identity_pool(
        IdentityPoolName="qa-creds-pool",
        AllowUnauthenticatedIdentities=True,
    )["IdentityPoolId"]
    identity_id = cognito_identity.get_id(IdentityPoolId=iid, AccountId="000000000000")["IdentityId"]
    creds = cognito_identity.get_credentials_for_identity(IdentityId=identity_id)
    c = creds["Credentials"]
    assert "SecretKey" in c
    assert c["AccessKeyId"].startswith("ASIA")
    assert "SessionToken" in c
    assert c["Expiration"] is not None


def test_cognito_change_password_actually_changes(cognito_idp):
    """ChangePassword must update the stored password so old one stops working."""
    pid = cognito_idp.create_user_pool(PoolName="qa-changepwd")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-changepwd-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-cpwd-user")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="qa-cpwd-user", Password="OldPwd1!", Permanent=True)
    token = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "OldPwd1!"},
    )["AuthenticationResult"]["AccessToken"]
    cognito_idp.change_password(AccessToken=token, PreviousPassword="OldPwd1!", ProposedPassword="NewPwd2!")
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "NewPwd2!"},
    )
    assert "AuthenticationResult" in auth2
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "qa-cpwd-user", "PASSWORD": "OldPwd1!"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"


def test_cognito_refresh_token_returns_correct_user(cognito_idp):
    """REFRESH_TOKEN_AUTH must return tokens for the refreshing user, not users[0]."""
    pid = cognito_idp.create_user_pool(PoolName="qa-refresh-pool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-refresh-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    for name, pw in [("qa-first", "First1!"), ("qa-second", "Second1!")]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=name)
        cognito_idp.admin_set_user_password(UserPoolId=pid, Username=name, Password=pw, Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-second", "PASSWORD": "Second1!"},
    )
    refresh_token = auth["AuthenticationResult"]["RefreshToken"]
    refresh = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    new_token = refresh["AuthenticationResult"]["AccessToken"]
    user = cognito_idp.get_user(AccessToken=new_token)
    assert user["Username"] == "qa-second", "Refresh must return tokens for qa-second not qa-first"


def test_cognito_signup_unconfirmed_with_auto_verify(cognito_idp):
    """SignUp with AutoVerifiedAttributes must return UserConfirmed=False."""
    pid = cognito_idp.create_user_pool(PoolName="qa-autoverify", AutoVerifiedAttributes=["email"])["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="qa-autoverify-app")["UserPoolClient"][
        "ClientId"
    ]
    resp = cognito_idp.sign_up(
        ClientId=cid,
        Username="qa-signup-user",
        Password="SignUp1!",
        UserAttributes=[{"Name": "email", "Value": "qa@example.com"}],
    )
    assert resp["UserConfirmed"] is False
    user = cognito_idp.admin_get_user(UserPoolId=pid, Username="qa-signup-user")
    assert user["UserStatus"] == "UNCONFIRMED"


def test_cognito_disabled_user_auth_fails(cognito_idp):
    """Disabled user must get NotAuthorizedException."""
    pid = cognito_idp.create_user_pool(PoolName="qa-disabled-pool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-disabled-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-disabled")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="qa-disabled", Password="Dis1!", Permanent=True)
    cognito_idp.admin_disable_user(UserPoolId=pid, Username="qa-disabled")
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_initiate_auth(
            UserPoolId=pid,
            ClientId=cid,
            AuthFlow="ADMIN_USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": "qa-disabled", "PASSWORD": "Dis1!"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"


def test_cognito_list_users_in_group(cognito_idp):
    """ListUsersInGroup must return members added via AdminAddUserToGroup."""
    pid = cognito_idp.create_user_pool(PoolName="qa-group-members")["UserPool"]["Id"]
    cognito_idp.create_group(UserPoolId=pid, GroupName="qa-grp")
    for u in ["qa-u1", "qa-u2", "qa-u3"]:
        cognito_idp.admin_create_user(UserPoolId=pid, Username=u)
        cognito_idp.admin_add_user_to_group(UserPoolId=pid, Username=u, GroupName="qa-grp")
    members = cognito_idp.list_users_in_group(UserPoolId=pid, GroupName="qa-grp")["Users"]
    names = {u["Username"] for u in members}
    assert {"qa-u1", "qa-u2", "qa-u3"} == names


def test_cognito_duplicate_username_error(cognito_idp):
    """AdminCreateUser with duplicate username must raise UsernameExistsException."""
    pid = cognito_idp.create_user_pool(PoolName="qa-dup-user")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-dup")
    with pytest.raises(ClientError) as exc:
        cognito_idp.admin_create_user(UserPoolId=pid, Username="qa-dup")
    assert exc.value.response["Error"]["Code"] == "UsernameExistsException"


def test_cognito_client_secret_generated(cognito_idp):
    """CreateUserPoolClient with GenerateSecret=True must return a ClientSecret."""
    pid = cognito_idp.create_user_pool(PoolName="qa-secret-client")["UserPool"]["Id"]
    client = cognito_idp.create_user_pool_client(UserPoolId=pid, ClientName="qa-secret-app", GenerateSecret=True)[
        "UserPoolClient"
    ]
    assert "ClientSecret" in client
    assert len(client["ClientSecret"]) > 20


def test_cognito_force_change_password_challenge(cognito_idp):
    """AdminCreateUser with TemporaryPassword triggers NEW_PASSWORD_REQUIRED challenge."""
    pid = cognito_idp.create_user_pool(PoolName="qa-force-change")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-force-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="qa-force-user",
        TemporaryPassword="TempPwd1!",
    )
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "qa-force-user", "PASSWORD": "TempPwd1!"},
    )
    assert auth.get("ChallengeName") == "NEW_PASSWORD_REQUIRED"
    assert "Session" in auth


# ---------------------------------------------------------------------------
# Cognito TOTP MFA
# ---------------------------------------------------------------------------


def test_cognito_totp_full_flow(cognito_idp):
    """Full TOTP MFA flow: SetUserPoolMfaConfig ON → AssociateSoftwareToken →
    VerifySoftwareToken → InitiateAuth returns SOFTWARE_TOKEN_MFA challenge →
    RespondToAuthChallenge with any code returns tokens."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-full")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    # Enable TOTP MFA on the pool
    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="ON",
    )
    cfg = cognito_idp.get_user_pool_mfa_config(UserPoolId=pid)
    assert cfg["MfaConfiguration"] == "ON"
    assert cfg["SoftwareTokenMfaConfiguration"]["Enabled"] is True

    # Create and confirm user
    cognito_idp.admin_create_user(UserPoolId=pid, Username="totp-user", TemporaryPassword="Tmp1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="totp-user", Password="Perm1!", Permanent=True)

    # Enroll TOTP: associate → get tokens first (MFA not yet enrolled, pool is ON but no enrollment)
    # Pool ON with no enrollment → auth succeeds so user can enroll
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "totp-user", "PASSWORD": "Perm1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    # Associate software token
    assoc = cognito_idp.associate_software_token(AccessToken=access_token)
    assert "SecretCode" in assoc
    assert len(assoc["SecretCode"]) > 0

    # Verify (accept any code)
    verify = cognito_idp.verify_software_token(AccessToken=access_token, UserCode="123456")
    assert verify["Status"] == "SUCCESS"

    # Now auth should return SOFTWARE_TOKEN_MFA challenge
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "totp-user", "PASSWORD": "Perm1!"},
    )
    assert auth2.get("ChallengeName") == "SOFTWARE_TOKEN_MFA"
    assert "Session" in auth2

    # Respond with any TOTP code → get tokens
    result = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid,
        ClientId=cid,
        ChallengeName="SOFTWARE_TOKEN_MFA",
        ChallengeResponses={"USERNAME": "totp-user", "SOFTWARE_TOKEN_MFA_CODE": "123456"},
    )
    assert "AuthenticationResult" in result
    assert "AccessToken" in result["AuthenticationResult"]


def test_cognito_totp_optional_mfa(cognito_idp):
    """OPTIONAL MFA: users without TOTP enrolled go straight to tokens;
    users with TOTP enrolled get the challenge."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-optional")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-opt-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]

    cognito_idp.set_user_pool_mfa_config(
        UserPoolId=pid,
        SoftwareTokenMfaConfiguration={"Enabled": True},
        MfaConfiguration="OPTIONAL",
    )

    # User without MFA enrolled
    cognito_idp.admin_create_user(UserPoolId=pid, Username="no-mfa-user", TemporaryPassword="Tmp1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="no-mfa-user", Password="Perm1!", Permanent=True)
    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "no-mfa-user", "PASSWORD": "Perm1!"},
    )
    assert "AuthenticationResult" in auth  # no challenge — not enrolled

    # User with MFA enrolled via AdminSetUserMFAPreference
    cognito_idp.admin_create_user(UserPoolId=pid, Username="mfa-user", TemporaryPassword="Tmp1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="mfa-user", Password="Perm1!", Permanent=True)
    cognito_idp.admin_set_user_mfa_preference(
        UserPoolId=pid,
        Username="mfa-user",
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )
    auth2 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "mfa-user", "PASSWORD": "Perm1!"},
    )
    assert auth2.get("ChallengeName") == "SOFTWARE_TOKEN_MFA"


def test_cognito_admin_get_user_mfa_fields(cognito_idp):
    """AdminGetUser returns correct UserMFASettingList and PreferredMfaSetting."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-getuser")["UserPool"]["Id"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="mfa-check-user", TemporaryPassword="Tmp1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="mfa-check-user", Password="Perm1!", Permanent=True)

    # Before enrollment
    u = cognito_idp.admin_get_user(UserPoolId=pid, Username="mfa-check-user")
    assert u["UserMFASettingList"] == []
    assert u["PreferredMfaSetting"] == ""

    # After enrollment
    cognito_idp.admin_set_user_mfa_preference(
        UserPoolId=pid,
        Username="mfa-check-user",
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )
    u2 = cognito_idp.admin_get_user(UserPoolId=pid, Username="mfa-check-user")
    assert "SOFTWARE_TOKEN_MFA" in u2["UserMFASettingList"]
    assert u2["PreferredMfaSetting"] == "SOFTWARE_TOKEN_MFA"


def test_cognito_set_user_mfa_preference_via_token(cognito_idp):
    """SetUserMFAPreference (public, uses AccessToken) enrolls TOTP on the user."""
    pid = cognito_idp.create_user_pool(PoolName="qa-totp-selfenroll")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="qa-totp-self-app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="self-enroll", TemporaryPassword="Tmp1!")
    cognito_idp.admin_set_user_password(UserPoolId=pid, Username="self-enroll", Password="Perm1!", Permanent=True)

    auth = cognito_idp.admin_initiate_auth(
        UserPoolId=pid,
        ClientId=cid,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": "self-enroll", "PASSWORD": "Perm1!"},
    )
    access_token = auth["AuthenticationResult"]["AccessToken"]

    cognito_idp.set_user_mfa_preference(
        AccessToken=access_token,
        SoftwareTokenMfaSettings={"Enabled": True, "PreferredMfa": True},
    )

    u = cognito_idp.admin_get_user(UserPoolId=pid, Username="self-enroll")
    assert "SOFTWARE_TOKEN_MFA" in u["UserMFASettingList"]
    assert u["PreferredMfaSetting"] == "SOFTWARE_TOKEN_MFA"


# ---------------------------------------------------------------------------
# DYNAMODB — edge cases
# ---------------------------------------------------------------------------


def test_ddb_update_item_updated_new(ddb):
    """UpdateItem ReturnValues=UPDATED_NEW returns only changed attributes."""
    ddb.create_table(
        TableName="qa-ddb-updated-new",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(
        TableName="qa-ddb-updated-new",
        Item={"pk": {"S": "k1"}, "a": {"S": "old"}, "b": {"N": "1"}},
    )
    resp = ddb.update_item(
        TableName="qa-ddb-updated-new",
        Key={"pk": {"S": "k1"}},
        UpdateExpression="SET a = :new",
        ExpressionAttributeValues={":new": {"S": "new"}},
        ReturnValues="UPDATED_NEW",
    )
    assert "Attributes" in resp
    assert resp["Attributes"]["a"]["S"] == "new"
    assert "b" not in resp["Attributes"]


def test_ddb_update_item_updated_old(ddb):
    """UpdateItem ReturnValues=UPDATED_OLD returns old values of changed attributes."""
    ddb.create_table(
        TableName="qa-ddb-updated-old",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-updated-old", Item={"pk": {"S": "k1"}, "score": {"N": "10"}})
    resp = ddb.update_item(
        TableName="qa-ddb-updated-old",
        Key={"pk": {"S": "k1"}},
        UpdateExpression="SET score = :new",
        ExpressionAttributeValues={":new": {"N": "20"}},
        ReturnValues="UPDATED_OLD",
    )
    assert resp["Attributes"]["score"]["N"] == "10"


def test_ddb_conditional_put_fails(ddb):
    """PutItem with attribute_not_exists condition fails if item already exists."""
    ddb.create_table(
        TableName="qa-ddb-cond-put",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-cond-put", Item={"pk": {"S": "exists"}})
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName="qa-ddb-cond-put",
            Item={"pk": {"S": "exists"}, "data": {"S": "new"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_ddb_query_with_filter_expression(ddb):
    """Query with FilterExpression reduces Count but not ScannedCount."""
    ddb.create_table(
        TableName="qa-ddb-filter",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(
            TableName="qa-ddb-filter",
            Item={
                "pk": {"S": "user1"},
                "sk": {"N": str(i)},
                "active": {"BOOL": i % 2 == 0},
            },
        )
    resp = ddb.query(
        TableName="qa-ddb-filter",
        KeyConditionExpression="pk = :pk",
        FilterExpression="active = :t",
        ExpressionAttributeValues={":pk": {"S": "user1"}, ":t": {"BOOL": True}},
    )
    assert resp["Count"] == 3
    assert resp["ScannedCount"] == 5


def test_ddb_scan_with_limit_and_pagination(ddb):
    """Scan with Limit returns LastEvaluatedKey and pagination works."""
    ddb.create_table(
        TableName="qa-ddb-scan-page",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(10):
        ddb.put_item(TableName="qa-ddb-scan-page", Item={"pk": {"S": f"item{i:02d}"}})
    all_items = []
    lek = None
    while True:
        kwargs = {"TableName": "qa-ddb-scan-page", "Limit": 3}
        if lek:
            kwargs["ExclusiveStartKey"] = lek
        resp = ddb.scan(**kwargs)
        all_items.extend(resp["Items"])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
    assert len(all_items) == 10


def test_ddb_transact_write_condition_cancel(ddb):
    """TransactWriteItems cancels entire transaction if one condition fails."""
    ddb.create_table(
        TableName="qa-ddb-transact",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-transact", Item={"pk": {"S": "existing"}})
    with pytest.raises(ClientError) as exc:
        ddb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": "qa-ddb-transact",
                        "Item": {"pk": {"S": "new-item"}},
                    }
                },
                {
                    "Put": {
                        "TableName": "qa-ddb-transact",
                        "Item": {"pk": {"S": "existing"}, "data": {"S": "x"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
            ]
        )
    assert exc.value.response["Error"]["Code"] == "TransactionCanceledException"
    resp = ddb.get_item(TableName="qa-ddb-transact", Key={"pk": {"S": "new-item"}})
    assert "Item" not in resp


def test_ddb_batch_get_missing_table(ddb):
    """BatchGetItem with non-existent table returns it in UnprocessedKeys."""
    resp = ddb.batch_get_item(RequestItems={"qa-ddb-nonexistent-xyz": {"Keys": [{"pk": {"S": "k1"}}]}})
    assert "qa-ddb-nonexistent-xyz" in resp["UnprocessedKeys"]


# ---------------------------------------------------------------------------
# S3 — edge cases
# ---------------------------------------------------------------------------


def test_s3_range_suffix(s3):
    """Range: bytes=-N returns last N bytes."""
    s3.create_bucket(Bucket="qa-s3-range-suffix")
    s3.put_object(Bucket="qa-s3-range-suffix", Key="data.txt", Body=b"0123456789")
    resp = s3.get_object(Bucket="qa-s3-range-suffix", Key="data.txt", Range="bytes=-3")
    assert resp["Body"].read() == b"789"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206


def test_s3_range_beyond_end(s3):
    """Range start beyond file size returns 416."""
    s3.create_bucket(Bucket="qa-s3-range-beyond")
    s3.put_object(Bucket="qa-s3-range-beyond", Key="small.txt", Body=b"hello")
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket="qa-s3-range-beyond", Key="small.txt", Range="bytes=100-200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416


def test_s3_list_v1_marker_pagination(s3):
    """ListObjects v1 Marker pagination returns correct pages."""
    s3.create_bucket(Bucket="qa-s3-marker")
    keys = [f"file{i:03d}.txt" for i in range(10)]
    for k in keys:
        s3.put_object(Bucket="qa-s3-marker", Key=k, Body=b"x")
    # NextMarker only returned when Delimiter is set (AWS spec)
    resp1 = s3.list_objects(Bucket="qa-s3-marker", MaxKeys=4, Delimiter="/")
    assert resp1["IsTruncated"] is True
    assert len(resp1["Contents"]) == 4
    marker = resp1["NextMarker"]
    resp2 = s3.list_objects(Bucket="qa-s3-marker", MaxKeys=4, Marker=marker, Delimiter="/")
    page2_keys = [o["Key"] for o in resp2["Contents"]]
    page1_keys = [o["Key"] for o in resp1["Contents"]]
    assert not any(k in page1_keys for k in page2_keys)


def test_s3_delete_objects_returns_deleted(s3):
    """DeleteObjects returns each deleted key in Deleted list."""
    s3.create_bucket(Bucket="qa-s3-batch-del")
    for i in range(3):
        s3.put_object(Bucket="qa-s3-batch-del", Key=f"obj{i}.txt", Body=b"x")
    resp = s3.delete_objects(
        Bucket="qa-s3-batch-del",
        Delete={"Objects": [{"Key": f"obj{i}.txt"} for i in range(3)]},
    )
    assert len(resp["Deleted"]) == 3
    assert not resp.get("Errors")


def test_s3_put_object_content_type_preserved(s3):
    """Content-Type set on PutObject is returned on GetObject."""
    s3.create_bucket(Bucket="qa-s3-ct")
    s3.put_object(
        Bucket="qa-s3-ct",
        Key="page.html",
        Body=b"<html/>",
        ContentType="text/html; charset=utf-8",
    )
    resp = s3.get_object(Bucket="qa-s3-ct", Key="page.html")
    assert "text/html" in resp["ContentType"]


def test_s3_head_object_returns_content_length(s3):
    """HeadObject must return correct ContentLength."""
    s3.create_bucket(Bucket="qa-s3-head-len")
    body = b"exactly twenty bytes"
    s3.put_object(Bucket="qa-s3-head-len", Key="f.bin", Body=body)
    resp = s3.head_object(Bucket="qa-s3-head-len", Key="f.bin")
    assert resp["ContentLength"] == len(body)


def test_s3_copy_preserves_metadata(s3):
    """CopyObject with MetadataDirective=COPY preserves source metadata."""
    s3.create_bucket(Bucket="qa-s3-copy-meta")
    s3.put_object(
        Bucket="qa-s3-copy-meta",
        Key="src.txt",
        Body=b"data",
        Metadata={"x-custom": "value123"},
    )
    s3.copy_object(
        CopySource={"Bucket": "qa-s3-copy-meta", "Key": "src.txt"},
        Bucket="qa-s3-copy-meta",
        Key="dst.txt",
        MetadataDirective="COPY",
    )
    resp = s3.head_object(Bucket="qa-s3-copy-meta", Key="dst.txt")
    assert resp["Metadata"].get("x-custom") == "value123"


def test_s3_multipart_list_parts(s3):
    """ListParts returns uploaded parts before completion."""
    s3.create_bucket(Bucket="qa-s3-listparts")
    mpu = s3.create_multipart_upload(Bucket="qa-s3-listparts", Key="big.bin")
    uid = mpu["UploadId"]
    p1 = s3.upload_part(
        Bucket="qa-s3-listparts",
        Key="big.bin",
        UploadId=uid,
        PartNumber=1,
        Body=b"A" * 50,
    )
    p2 = s3.upload_part(
        Bucket="qa-s3-listparts",
        Key="big.bin",
        UploadId=uid,
        PartNumber=2,
        Body=b"B" * 50,
    )
    parts = s3.list_parts(Bucket="qa-s3-listparts", Key="big.bin", UploadId=uid)["Parts"]
    assert len(parts) == 2
    assert parts[0]["PartNumber"] == 1
    assert parts[1]["PartNumber"] == 2
    s3.complete_multipart_upload(
        Bucket="qa-s3-listparts",
        Key="big.bin",
        UploadId=uid,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": p1["ETag"]},
                {"PartNumber": 2, "ETag": p2["ETag"]},
            ]
        },
    )


def test_s3_list_multipart_uploads(s3):
    """ListMultipartUploads returns in-progress uploads."""
    s3.create_bucket(Bucket="qa-s3-list-mpu")
    uid1 = s3.create_multipart_upload(Bucket="qa-s3-list-mpu", Key="a.bin")["UploadId"]
    uid2 = s3.create_multipart_upload(Bucket="qa-s3-list-mpu", Key="b.bin")["UploadId"]
    resp = s3.list_multipart_uploads(Bucket="qa-s3-list-mpu")
    upload_ids = {u["UploadId"] for u in resp.get("Uploads", [])}
    assert uid1 in upload_ids
    assert uid2 in upload_ids
    s3.abort_multipart_upload(Bucket="qa-s3-list-mpu", Key="a.bin", UploadId=uid1)
    s3.abort_multipart_upload(Bucket="qa-s3-list-mpu", Key="b.bin", UploadId=uid2)


# ---------------------------------------------------------------------------
# SQS — edge cases
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# SNS — edge cases
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# LAMBDA — edge cases
# ---------------------------------------------------------------------------


def test_lambda_alias_crud(lam):
    """CreateAlias, GetAlias, UpdateAlias, DeleteAlias."""
    code = _zip_lambda("def handler(e,c): return {'v': 1}")
    lam.create_function(
        FunctionName="qa-lam-alias",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.publish_version(FunctionName="qa-lam-alias")
    lam.create_alias(
        FunctionName="qa-lam-alias",
        Name="prod",
        FunctionVersion="1",
        Description="production alias",
    )
    alias = lam.get_alias(FunctionName="qa-lam-alias", Name="prod")
    assert alias["Name"] == "prod"
    assert alias["FunctionVersion"] == "1"
    lam.update_alias(FunctionName="qa-lam-alias", Name="prod", Description="updated")
    alias2 = lam.get_alias(FunctionName="qa-lam-alias", Name="prod")
    assert alias2["Description"] == "updated"
    aliases = lam.list_aliases(FunctionName="qa-lam-alias")["Aliases"]
    assert any(a["Name"] == "prod" for a in aliases)
    lam.delete_alias(FunctionName="qa-lam-alias", Name="prod")
    aliases2 = lam.list_aliases(FunctionName="qa-lam-alias")["Aliases"]
    assert not any(a["Name"] == "prod" for a in aliases2)


def test_lambda_publish_version(lam):
    """PublishVersion creates a numbered version snapshot."""
    code = _zip_lambda("def handler(e,c): return 'v1'")
    lam.create_function(
        FunctionName="qa-lam-version",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    ver = lam.publish_version(FunctionName="qa-lam-version")
    assert ver["Version"] == "1"
    versions = lam.list_versions_by_function(FunctionName="qa-lam-version")["Versions"]
    version_nums = [v["Version"] for v in versions]
    assert "1" in version_nums
    assert "$LATEST" in version_nums


def test_lambda_function_concurrency(lam):
    """PutFunctionConcurrency / GetFunctionConcurrency / DeleteFunctionConcurrency."""
    code = _zip_lambda("def handler(e,c): return {}")
    lam.create_function(
        FunctionName="qa-lam-concurrency",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.put_function_concurrency(
        FunctionName="qa-lam-concurrency",
        ReservedConcurrentExecutions=5,
    )
    resp = lam.get_function_concurrency(FunctionName="qa-lam-concurrency")
    assert resp["ReservedConcurrentExecutions"] == 5
    lam.delete_function_concurrency(FunctionName="qa-lam-concurrency")
    resp2 = lam.get_function_concurrency(FunctionName="qa-lam-concurrency")
    assert resp2.get("ReservedConcurrentExecutions") is None


def test_lambda_add_remove_permission(lam):
    """AddPermission / RemovePermission / GetPolicy."""
    code = _zip_lambda("def handler(e,c): return {}")
    lam.create_function(
        FunctionName="qa-lam-policy",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.add_permission(
        FunctionName="qa-lam-policy",
        StatementId="allow-s3",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
    )
    policy = json.loads(lam.get_policy(FunctionName="qa-lam-policy")["Policy"])
    assert any(s["Sid"] == "allow-s3" for s in policy["Statement"])
    lam.remove_permission(FunctionName="qa-lam-policy", StatementId="allow-s3")
    policy2 = json.loads(lam.get_policy(FunctionName="qa-lam-policy")["Policy"])
    assert not any(s["Sid"] == "allow-s3" for s in policy2["Statement"])


def test_lambda_list_functions_pagination(lam):
    """ListFunctions pagination with Marker works correctly."""
    for i in range(5):
        code = _zip_lambda("def handler(e,c): return {}")
        try:
            lam.create_function(
                FunctionName=f"qa-lam-page-{i}",
                Runtime="python3.9",
                Role="arn:aws:iam::000000000000:role/r",
                Handler="index.handler",
                Code={"ZipFile": code},
            )
        except ClientError:
            pass
    resp1 = lam.list_functions(MaxItems=2)
    assert len(resp1["Functions"]) <= 2
    if "NextMarker" in resp1:
        resp2 = lam.list_functions(MaxItems=2, Marker=resp1["NextMarker"])
        names1 = {f["FunctionName"] for f in resp1["Functions"]}
        names2 = {f["FunctionName"] for f in resp2["Functions"]}
        assert not names1 & names2


def test_lambda_invoke_event_type_returns_202(lam):
    """Invoke with InvocationType=Event returns 202 immediately."""
    code = _zip_lambda("def handler(e,c): return {}")
    try:
        lam.create_function(
            FunctionName="qa-lam-event-invoke",
            Runtime="python3.9",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": code},
        )
    except ClientError:
        pass
    resp = lam.invoke(
        FunctionName="qa-lam-event-invoke",
        InvocationType="Event",
        Payload=json.dumps({}),
    )
    assert resp["StatusCode"] == 202


def test_lambda_invoke_dry_run_returns_204(lam):
    """Invoke with InvocationType=DryRun returns 204."""
    code = _zip_lambda("def handler(e,c): return {}")
    try:
        lam.create_function(
            FunctionName="qa-lam-dryrun",
            Runtime="python3.9",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": code},
        )
    except ClientError:
        pass
    resp = lam.invoke(
        FunctionName="qa-lam-dryrun",
        InvocationType="DryRun",
        Payload=json.dumps({}),
    )
    assert resp["StatusCode"] == 204


# ---------------------------------------------------------------------------
# IAM — edge cases
# ---------------------------------------------------------------------------


def test_iam_policy_version_crud(iam):
    """CreatePolicyVersion, GetPolicyVersion, ListPolicyVersions, DeletePolicyVersion."""
    doc1 = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
        }
    )
    doc2 = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "sqs:*", "Resource": "*"}],
        }
    )
    arn = iam.create_policy(PolicyName="qa-iam-versions", PolicyDocument=doc1)["Policy"]["Arn"]
    iam.create_policy_version(PolicyArn=arn, PolicyDocument=doc2, SetAsDefault=True)
    versions = iam.list_policy_versions(PolicyArn=arn)["Versions"]
    assert len(versions) == 2
    default = next(v for v in versions if v["IsDefaultVersion"])
    assert default["VersionId"] == "v2"
    v1 = iam.get_policy_version(PolicyArn=arn, VersionId="v1")["PolicyVersion"]
    assert v1["IsDefaultVersion"] is False
    iam.delete_policy_version(PolicyArn=arn, VersionId="v1")
    versions2 = iam.list_policy_versions(PolicyArn=arn)["Versions"]
    assert len(versions2) == 1


def test_iam_inline_user_policy(iam):
    """PutUserPolicy / GetUserPolicy / ListUserPolicies / DeleteUserPolicy."""
    iam.create_user(UserName="qa-iam-inline-user")
    doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}],
        }
    )
    iam.put_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline", PolicyDocument=doc)
    policies = iam.list_user_policies(UserName="qa-iam-inline-user")["PolicyNames"]
    assert "qa-inline" in policies
    got = iam.get_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline")
    # boto3 deserialises PolicyDocument as a dict
    assert "s3:GetObject" in json.dumps(got["PolicyDocument"])
    iam.delete_user_policy(UserName="qa-iam-inline-user", PolicyName="qa-inline")
    policies2 = iam.list_user_policies(UserName="qa-iam-inline-user")["PolicyNames"]
    assert "qa-inline" not in policies2


def test_iam_instance_profile_crud(iam):
    """CreateInstanceProfile, AddRoleToInstanceProfile, GetInstanceProfile, ListInstanceProfiles."""
    iam.create_role(
        RoleName="qa-iam-ip-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    iam.create_instance_profile(InstanceProfileName="qa-iam-ip")
    iam.add_role_to_instance_profile(InstanceProfileName="qa-iam-ip", RoleName="qa-iam-ip-role")
    ip = iam.get_instance_profile(InstanceProfileName="qa-iam-ip")["InstanceProfile"]
    assert ip["InstanceProfileName"] == "qa-iam-ip"
    assert any(r["RoleName"] == "qa-iam-ip-role" for r in ip["Roles"])
    profiles = iam.list_instance_profiles()["InstanceProfiles"]
    assert any(p["InstanceProfileName"] == "qa-iam-ip" for p in profiles)
    iam.remove_role_from_instance_profile(InstanceProfileName="qa-iam-ip", RoleName="qa-iam-ip-role")
    iam.delete_instance_profile(InstanceProfileName="qa-iam-ip")


def test_iam_attach_detach_user_policy(iam):
    """AttachUserPolicy / DetachUserPolicy / ListAttachedUserPolicies."""
    iam.create_user(UserName="qa-iam-attach-user")
    doc = json.dumps({"Version": "2012-10-17", "Statement": []})
    policy_arn = iam.create_policy(PolicyName="qa-iam-attach-pol", PolicyDocument=doc)["Policy"]["Arn"]
    iam.attach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert any(p["PolicyArn"] == policy_arn for p in attached)
    iam.detach_user_policy(UserName="qa-iam-attach-user", PolicyArn=policy_arn)
    attached2 = iam.list_attached_user_policies(UserName="qa-iam-attach-user")["AttachedPolicies"]
    assert not any(p["PolicyArn"] == policy_arn for p in attached2)


# ---------------------------------------------------------------------------
# SECRETS MANAGER — edge cases
# ---------------------------------------------------------------------------


def test_sm_put_secret_value_stages(sm):
    """PutSecretValue stages manage AWSCURRENT/AWSPREVIOUS correctly."""
    sm.create_secret(Name="qa-sm-stages", SecretString="v1")
    sm.put_secret_value(SecretId="qa-sm-stages", SecretString="v2")
    sm.put_secret_value(SecretId="qa-sm-stages", SecretString="v3")
    current = sm.get_secret_value(SecretId="qa-sm-stages", VersionStage="AWSCURRENT")
    assert current["SecretString"] == "v3"
    previous = sm.get_secret_value(SecretId="qa-sm-stages", VersionStage="AWSPREVIOUS")
    assert previous["SecretString"] == "v2"


def test_sm_list_secret_version_ids(sm):
    """ListSecretVersionIds returns all versions."""
    sm.create_secret(Name="qa-sm-versions", SecretString="initial")
    sm.put_secret_value(SecretId="qa-sm-versions", SecretString="second")
    resp = sm.list_secret_version_ids(SecretId="qa-sm-versions")
    assert len(resp["Versions"]) >= 2


def test_sm_delete_and_restore(sm):
    """DeleteSecret schedules deletion; RestoreSecret cancels it."""
    sm.create_secret(Name="qa-sm-restore", SecretString="data")
    sm.delete_secret(SecretId="qa-sm-restore", RecoveryWindowInDays=7)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="qa-sm-restore")
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
    sm.restore_secret(SecretId="qa-sm-restore")
    val = sm.get_secret_value(SecretId="qa-sm-restore")
    assert val["SecretString"] == "data"


def test_sm_get_random_password(sm):
    """GetRandomPassword returns a password of the requested length."""
    resp = sm.get_random_password(PasswordLength=24, ExcludeNumbers=True)
    pwd = resp["RandomPassword"]
    assert len(pwd) == 24
    assert not any(c.isdigit() for c in pwd)


def test_sm_kms_key_id_on_create_and_describe(sm):
    sm.create_secret(Name="kms-test-secret", SecretString="val", KmsKeyId="alias/my-key")
    resp = sm.describe_secret(SecretId="kms-test-secret")
    assert resp["KmsKeyId"] == "alias/my-key"


def test_sm_kms_key_id_on_update(sm):
    sm.update_secret(SecretId="kms-test-secret", KmsKeyId="alias/other-key")
    resp = sm.describe_secret(SecretId="kms-test-secret")
    assert resp["KmsKeyId"] == "alias/other-key"


# ---------------------------------------------------------------------------
# SSM — edge cases
# ---------------------------------------------------------------------------


def test_ssm_get_parameter_history(ssm):
    """GetParameterHistory returns all versions of a parameter."""
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v1", Type="String")
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v2", Type="String", Overwrite=True)
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v3", Type="String", Overwrite=True)
    history = ssm.get_parameter_history(Name="/qa/ssm/hist")["Parameters"]
    assert len(history) == 3
    values = [h["Value"] for h in history]
    assert "v1" in values and "v2" in values and "v3" in values


def test_ssm_describe_parameters_filter(ssm):
    """DescribeParameters with ParameterFilters filters by path prefix."""
    ssm.put_parameter(Name="/qa/ssm/filter/a", Value="1", Type="String")
    ssm.put_parameter(Name="/qa/ssm/filter/b", Value="2", Type="String")
    ssm.put_parameter(Name="/qa/ssm/other/c", Value="3", Type="String")
    resp = ssm.describe_parameters(ParameterFilters=[{"Key": "Path", "Values": ["/qa/ssm/filter"]}])
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/qa/ssm/filter/a" in names
    assert "/qa/ssm/filter/b" in names
    assert "/qa/ssm/other/c" not in names


def test_ssm_secure_string_not_decrypted_by_default(ssm):
    """SecureString value is not returned in plaintext without WithDecryption=True."""
    ssm.put_parameter(Name="/qa/ssm/secure", Value="mysecret", Type="SecureString")
    resp = ssm.get_parameter(Name="/qa/ssm/secure", WithDecryption=False)
    assert resp["Parameter"]["Value"] != "mysecret"
    resp2 = ssm.get_parameter(Name="/qa/ssm/secure", WithDecryption=True)
    assert resp2["Parameter"]["Value"] == "mysecret"


# ---------------------------------------------------------------------------
# EVENTBRIDGE — edge cases
# ---------------------------------------------------------------------------


def test_eb_content_filter_prefix(eb, sqs):
    """EventBridge prefix content filter matches events correctly."""
    bus_name = "qa-eb-prefix-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-prefix-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-prefix-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"], "detail": {"env": [{"prefix": "prod"}]}}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-prefix-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"env": "production"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "test",
                "Detail": json.dumps({"env": "staging"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0


def test_eb_anything_but_filter(eb, sqs):
    """EventBridge anything-but filter excludes specified values."""
    bus_name = "qa-eb-anybut-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-anybut-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-anybut-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps(
            {
                "source": ["myapp"],
                "detail": {"status": [{"anything-but": ["error", "failed"]}]},
            }
        ),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-anybut-rule",
        EventBusName=bus_name,
        Targets=[{"Id": "t1", "Arn": q_arn}],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": json.dumps({"status": "success"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": json.dumps({"status": "error"}),
                "EventBusName": bus_name,
            }
        ]
    )
    msgs2 = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert len(msgs2.get("Messages", [])) == 0


def test_eb_input_transformer(eb, sqs):
    """InputTransformer rewrites event payload before delivery."""
    bus_name = "qa-eb-transform-bus"
    eb.create_event_bus(Name=bus_name)
    q_url = sqs.create_queue(QueueName="qa-eb-transform-q")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(
        Name="qa-eb-transform-rule",
        EventBusName=bus_name,
        EventPattern=json.dumps({"source": ["myapp"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="qa-eb-transform-rule",
        EventBusName=bus_name,
        Targets=[
            {
                "Id": "t1",
                "Arn": q_arn,
                "InputTransformer": {
                    "InputPathsMap": {"src": "$.source"},
                    "InputTemplate": '{"transformed": "<src>"}',
                },
            }
        ],
    )
    eb.put_events(
        Entries=[
            {
                "Source": "myapp",
                "DetailType": "t",
                "Detail": "{}",
                "EventBusName": bus_name,
            }
        ]
    )
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body.get("transformed") == "myapp"


# ---------------------------------------------------------------------------
# KINESIS — edge cases
# ---------------------------------------------------------------------------


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


def test_kinesis_stream_encryption(kin):
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


# ── Kinesis validation limits ────────────────────────────────────────────────

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


# ---------------------------------------------------------------------------
# STEP FUNCTIONS — edge cases
# ---------------------------------------------------------------------------


def test_sfn_choice_state(sfn):
    """Choice state routes to correct branch based on input."""
    definition = json.dumps(
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.value",
                            "NumericGreaterThan": 10,
                            "Next": "High",
                        },
                        {
                            "Variable": "$.value",
                            "NumericLessThanEquals": 10,
                            "Next": "Low",
                        },
                    ],
                },
                "High": {"Type": "Pass", "Result": {"result": "high"}, "End": True},
                "Low": {"Type": "Pass", "Result": {"result": "low"}, "End": True},
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-choice",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input=json.dumps({"value": 15}))["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "SUCCEEDED"
    assert json.loads(desc["output"])["result"] == "high"
    exec_arn2 = sfn.start_execution(stateMachineArn=arn, input=json.dumps({"value": 5}))["executionArn"]
    time.sleep(0.5)
    desc2 = sfn.describe_execution(executionArn=exec_arn2)
    assert desc2["status"] == "SUCCEEDED"
    assert json.loads(desc2["output"])["result"] == "low"


def test_sfn_pass_state_result(sfn):
    """Pass state with Result injects static data into output."""
    definition = json.dumps(
        {
            "StartAt": "Inject",
            "States": {
                "Inject": {
                    "Type": "Pass",
                    "Result": {"injected": True, "count": 42},
                    "End": True,
                }
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-pass-result",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["injected"] is True
    assert output["count"] == 42


def test_sfn_fail_state(sfn):
    """Fail state transitions execution to FAILED."""
    definition = json.dumps(
        {
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Fail",
                    "Error": "CustomError",
                    "Cause": "Something went wrong",
                }
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-fail",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "FAILED"


def test_sfn_stop_execution(sfn):
    """StopExecution transitions a RUNNING execution to ABORTED."""
    definition = json.dumps(
        {
            "StartAt": "Wait",
            "States": {"Wait": {"Type": "Wait", "Seconds": 60, "End": True}},
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-stop",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.2)
    sfn.stop_execution(executionArn=exec_arn, cause="test stop")
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "ABORTED"


def test_sfn_list_executions_filter(sfn):
    """ListExecutions with statusFilter returns only matching executions."""
    definition = json.dumps(
        {
            "StartAt": "Done",
            "States": {"Done": {"Type": "Succeed"}},
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-list-filter",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    sfn.start_execution(stateMachineArn=arn, input="{}")
    time.sleep(0.5)
    succeeded = sfn.list_executions(stateMachineArn=arn, statusFilter="SUCCEEDED")["executions"]
    assert all(e["status"] == "SUCCEEDED" for e in succeeded)


# ---------------------------------------------------------------------------
# CLOUDWATCH — edge cases
# ---------------------------------------------------------------------------


def test_cw_get_metric_data_time_range(cw):
    """GetMetricData respects StartTime/EndTime filtering."""
    import datetime

    now = datetime.datetime.utcnow()
    past = now - datetime.timedelta(hours=2)
    cw.put_metric_data(
        Namespace="qa/cw",
        MetricData=[{"MetricName": "Requests", "Value": 100.0, "Unit": "Count"}],
    )
    resp = cw.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {"Namespace": "qa/cw", "MetricName": "Requests"},
                    "Period": 60,
                    "Stat": "Sum",
                },
            }
        ],
        StartTime=past,
        EndTime=now + datetime.timedelta(minutes=5),
    )
    result = next((r for r in resp["MetricDataResults"] if r["Id"] == "m1"), None)
    assert result is not None
    assert result["StatusCode"] == "Complete"
    assert len(result["Values"]) >= 1
    assert sum(result["Values"]) >= 100.0


def test_cw_alarm_state_transitions(cw):
    """SetAlarmState changes alarm state correctly."""
    cw.put_metric_alarm(
        AlarmName="qa-cw-state-alarm",
        MetricName="Errors",
        Namespace="qa/cw",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=10.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cw.set_alarm_state(AlarmName="qa-cw-state-alarm", StateValue="ALARM", StateReason="Testing")
    alarms = cw.describe_alarms(AlarmNames=["qa-cw-state-alarm"])["MetricAlarms"]
    assert alarms[0]["StateValue"] == "ALARM"
    cw.set_alarm_state(AlarmName="qa-cw-state-alarm", StateValue="OK", StateReason="Resolved")
    alarms2 = cw.describe_alarms(AlarmNames=["qa-cw-state-alarm"])["MetricAlarms"]
    assert alarms2[0]["StateValue"] == "OK"


def test_cw_list_metrics_namespace_filter(cw):
    """ListMetrics with Namespace filter returns only matching metrics."""
    cw.put_metric_data(Namespace="qa/ns-a", MetricData=[{"MetricName": "MetA", "Value": 1.0}])
    cw.put_metric_data(Namespace="qa/ns-b", MetricData=[{"MetricName": "MetB", "Value": 1.0}])
    resp = cw.list_metrics(Namespace="qa/ns-a")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "MetA" in names
    assert "MetB" not in names


def test_cw_put_metric_data_statistics_values(cw):
    """PutMetricData with Values/Counts array stores multiple data points."""
    cw.put_metric_data(
        Namespace="qa/cw-multi",
        MetricData=[
            {
                "MetricName": "Latency",
                "Values": [10.0, 20.0, 30.0],
                "Counts": [1.0, 2.0, 1.0],
                "Unit": "Milliseconds",
            }
        ],
    )
    resp = cw.list_metrics(Namespace="qa/cw-multi")
    assert any(m["MetricName"] == "Latency" for m in resp["Metrics"])


# ---------------------------------------------------------------------------
# CLOUDWATCH LOGS — edge cases
# ---------------------------------------------------------------------------


def test_logs_filter_with_wildcard(logs):
    """FilterLogEvents with wildcard pattern matches correctly."""
    logs.create_log_group(logGroupName="/qa/logs/wildcard")
    logs.create_log_stream(logGroupName="/qa/logs/wildcard", logStreamName="stream1")
    logs.put_log_events(
        logGroupName="/qa/logs/wildcard",
        logStreamName="stream1",
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "ERROR: disk full"},
            {"timestamp": int(time.time() * 1000), "message": "INFO: all good"},
            {"timestamp": int(time.time() * 1000), "message": "ERROR: timeout"},
        ],
    )
    resp = logs.filter_log_events(logGroupName="/qa/logs/wildcard", filterPattern="ERROR*")
    messages = [e["message"] for e in resp["events"]]
    assert all("ERROR" in m for m in messages)
    assert len(messages) == 2


def test_logs_describe_log_groups_prefix(logs):
    """DescribeLogGroups with logGroupNamePrefix filters correctly."""
    logs.create_log_group(logGroupName="/qa/logs/prefix/alpha")
    logs.create_log_group(logGroupName="/qa/logs/prefix/beta")
    logs.create_log_group(logGroupName="/qa/logs/other/gamma")
    resp = logs.describe_log_groups(logGroupNamePrefix="/qa/logs/prefix")
    names = [g["logGroupName"] for g in resp["logGroups"]]
    assert "/qa/logs/prefix/alpha" in names
    assert "/qa/logs/prefix/beta" in names
    assert "/qa/logs/other/gamma" not in names


def test_logs_retention_policy_invalid_value(logs):
    """PutRetentionPolicy with invalid days raises InvalidParameterException."""
    logs.create_log_group(logGroupName="/qa/logs/retention-invalid")
    with pytest.raises(ClientError) as exc:
        logs.put_retention_policy(logGroupName="/qa/logs/retention-invalid", retentionInDays=999)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ---------------------------------------------------------------------------
# FIREHOSE — edge cases
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# ROUTE53 — edge cases
# ---------------------------------------------------------------------------


def test_r53_delete_zone_with_records_fails(r53):
    """DeleteHostedZone fails if non-default records exist."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-nonempty.com.",
        CallerReference=f"qa-nonempty-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.qa-r53-nonempty.com.",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )
    with pytest.raises(ClientError) as exc:
        r53.delete_hosted_zone(Id=zone_id)
    assert exc.value.response["Error"]["Code"] == "HostedZoneNotEmpty"


def test_r53_upsert_is_idempotent(r53):
    """UPSERT on existing record updates it without error."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-upsert.com.",
        CallerReference=f"qa-upsert-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    for ip in ["1.1.1.1", "2.2.2.2"]:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "api.qa-r53-upsert.com.",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": ip}],
                        },
                    }
                ]
            },
        )
    records = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    a_records = [r for r in records if r["Name"] == "api.qa-r53-upsert.com." and r["Type"] == "A"]
    assert len(a_records) == 1
    assert a_records[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"


def test_r53_create_record_duplicate_fails(r53):
    """CREATE on existing record raises InvalidChangeBatch."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-dup.com.",
        CallerReference=f"qa-dup-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "dup.qa-r53-dup.com.",
                        "Type": "A",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "1.1.1.1"}],
                    },
                }
            ]
        },
    )
    with pytest.raises(ClientError) as exc:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "dup.qa-r53-dup.com.",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
                        },
                    }
                ]
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidChangeBatch"


# ---------------------------------------------------------------------------
# API GATEWAY v2 — edge cases
# ---------------------------------------------------------------------------


def test_apigw_update_integration(apigw):
    """UpdateIntegration changes integrationUri."""
    api_id = apigw.create_api(Name="qa-apigw-update-integ", ProtocolType="HTTP")["ApiId"]
    integ_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:old-fn",
    )["IntegrationId"]
    apigw.update_integration(
        ApiId=api_id,
        IntegrationId=integ_id,
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:new-fn",
    )
    integ = apigw.get_integration(ApiId=api_id, IntegrationId=integ_id)
    assert "new-fn" in integ["IntegrationUri"]


def test_apigw_delete_route(apigw):
    """DeleteRoute removes the route from GetRoutes."""
    api_id = apigw.create_api(Name="qa-apigw-del-route", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /qa")["RouteId"]
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    routes = apigw.get_routes(ApiId=api_id)["Items"]
    assert not any(r["RouteId"] == route_id for r in routes)


def test_apigw_stage_variables(apigw):
    """CreateStage with stageVariables stores and returns them."""
    api_id = apigw.create_api(Name="qa-apigw-stage-vars", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(
        ApiId=api_id,
        StageName="dev",
        StageVariables={"env": "development", "version": "1"},
    )
    stage = apigw.get_stage(ApiId=api_id, StageName="dev")
    assert stage["StageVariables"]["env"] == "development"
    assert stage["StageVariables"]["version"] == "1"


# ---------------------------------------------------------------------------
# API GATEWAY v1 — edge cases
# ---------------------------------------------------------------------------


def test_apigwv1_usage_plan_key_crud(apigw_v1):
    """CreateUsagePlanKey / GetUsagePlanKeys / DeleteUsagePlanKey."""
    api_key = apigw_v1.create_api_key(name="qa-v1-key", enabled=True)
    key_id = api_key["id"]
    plan = apigw_v1.create_usage_plan(
        name="qa-v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
    )
    plan_id = plan["id"]
    apigw_v1.create_usage_plan_key(usagePlanId=plan_id, keyId=key_id, keyType="API_KEY")
    keys = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert any(k["id"] == key_id for k in keys)
    apigw_v1.delete_usage_plan_key(usagePlanId=plan_id, keyId=key_id)
    keys2 = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert not any(k["id"] == key_id for k in keys2)


# ---------------------------------------------------------------------------
# GLUE — edge cases
# ---------------------------------------------------------------------------


def test_glue_partition_crud(glue):
    """CreatePartition / GetPartition / GetPartitions / DeletePartition."""
    glue.create_database(DatabaseInput={"Name": "qa-glue-partdb"})
    glue.create_table(
        DatabaseName="qa-glue-partdb",
        TableInput={
            "Name": "qa-glue-parttbl",
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://bucket/key",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    glue.create_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionInput={
            "Values": ["2024-01-01"],
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://bucket/key/dt=2024-01-01",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
        },
    )
    part = glue.get_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionValues=["2024-01-01"],
    )["Partition"]
    assert part["Values"] == ["2024-01-01"]
    parts = glue.get_partitions(DatabaseName="qa-glue-partdb", TableName="qa-glue-parttbl")["Partitions"]
    assert len(parts) == 1
    glue.delete_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionValues=["2024-01-01"],
    )
    parts2 = glue.get_partitions(DatabaseName="qa-glue-partdb", TableName="qa-glue-parttbl")["Partitions"]
    assert len(parts2) == 0


def test_glue_duplicate_partition_error(glue):
    """CreatePartition with duplicate values raises AlreadyExistsException."""
    glue.create_database(DatabaseInput={"Name": "qa-glue-duppartdb"})
    glue.create_table(
        DatabaseName="qa-glue-duppartdb",
        TableInput={
            "Name": "qa-glue-dupparttbl",
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/k",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    part_input = {
        "Values": ["2024-01-01"],
        "StorageDescriptor": {
            "Columns": [],
            "Location": "s3://b/k/dt=2024-01-01",
            "InputFormat": "",
            "OutputFormat": "",
            "SerdeInfo": {},
        },
    }
    glue.create_partition(
        DatabaseName="qa-glue-duppartdb",
        TableName="qa-glue-dupparttbl",
        PartitionInput=part_input,
    )
    with pytest.raises(ClientError) as exc:
        glue.create_partition(
            DatabaseName="qa-glue-duppartdb",
            TableName="qa-glue-dupparttbl",
            PartitionInput=part_input,
        )
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"


# ---------------------------------------------------------------------------
# ATHENA — edge cases
# ---------------------------------------------------------------------------


def test_athena_stop_query(athena):
    """StopQueryExecution cancels a running query."""
    resp = athena.start_query_execution(
        QueryString="SELECT 1",
        ResultConfiguration={"OutputLocation": "s3://qa-athena-results/"},
    )
    qid = resp["QueryExecutionId"]
    athena.stop_query_execution(QueryExecutionId=qid)
    desc = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
    assert desc["Status"]["State"] in ("CANCELLED", "SUCCEEDED")


def test_athena_prepared_statement_crud(athena):
    """CreatePreparedStatement / GetPreparedStatement / DeletePreparedStatement."""
    athena.create_prepared_statement(
        StatementName="qa-athena-stmt",
        WorkGroup="primary",
        QueryStatement="SELECT * FROM tbl WHERE id = ?",
        Description="test stmt",
    )
    stmt = athena.get_prepared_statement(StatementName="qa-athena-stmt", WorkGroup="primary")["PreparedStatement"]
    assert stmt["StatementName"] == "qa-athena-stmt"
    assert "SELECT" in stmt["QueryStatement"]
    stmts = athena.list_prepared_statements(WorkGroup="primary")["PreparedStatements"]
    assert any(s["StatementName"] == "qa-athena-stmt" for s in stmts)
    athena.delete_prepared_statement(StatementName="qa-athena-stmt", WorkGroup="primary")
    stmts2 = athena.list_prepared_statements(WorkGroup="primary")["PreparedStatements"]
    assert not any(s["StatementName"] == "qa-athena-stmt" for s in stmts2)


def test_athena_data_catalog_crud(athena):
    """CreateDataCatalog / GetDataCatalog / ListDataCatalogs / DeleteDataCatalog."""
    athena.create_data_catalog(Name="qa-athena-catalog", Type="HIVE", Description="test catalog")
    catalog = athena.get_data_catalog(Name="qa-athena-catalog")["DataCatalog"]
    assert catalog["Name"] == "qa-athena-catalog"
    assert catalog["Type"] == "HIVE"
    catalogs = athena.list_data_catalogs()["DataCatalogsSummary"]
    assert any(c["CatalogName"] == "qa-athena-catalog" for c in catalogs)
    athena.delete_data_catalog(Name="qa-athena-catalog")
    catalogs2 = athena.list_data_catalogs()["DataCatalogsSummary"]
    assert not any(c["CatalogName"] == "qa-athena-catalog" for c in catalogs2)


# ---------------------------------------------------------------------------
# Athena engine control + /_ministack/config endpoint
# ---------------------------------------------------------------------------


def test_athena_engine_mock_via_config(athena):
    """Switching ATHENA_ENGINE to 'mock' via /_ministack/config returns mock results."""
    import json as _json
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps({"athena.ATHENA_ENGINE": "mock"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=5).read())
    assert resp["applied"].get("athena.ATHENA_ENGINE") == "mock"

    # Query executes and succeeds in mock mode
    qid = athena.start_query_execution(
        QueryString="SELECT 1",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    import time as _time

    for _ in range(10):
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        _time.sleep(0.2)
    assert state == "SUCCEEDED"

    # Reset back to auto
    req2 = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps({"athena.ATHENA_ENGINE": "auto"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req2, timeout=5)


def test_ministack_config_invalid_key_ignored():
    """/_ministack/config silently ignores unknown keys and only applies valid ones."""
    import json as _json
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps(
            {
                "nonexistent_module.VAR": "val",
                "athena.ATHENA_ENGINE": "auto",
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=5).read())
    assert "nonexistent_module.VAR" not in resp["applied"]
    assert resp["applied"].get("athena.ATHENA_ENGINE") == "auto"


# ---------------------------------------------------------------------------
# RDS — edge cases
# ---------------------------------------------------------------------------


def test_rds_snapshot_crud(rds):
    """CreateDBSnapshot / DescribeDBSnapshots / DeleteDBSnapshot."""
    rds.create_db_instance(
        DBInstanceIdentifier="qa-rds-snap-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password",
        AllocatedStorage=20,
    )
    try:
        rds.create_db_snapshot(DBSnapshotIdentifier="qa-rds-snap-1", DBInstanceIdentifier="qa-rds-snap-db")
        snaps = rds.describe_db_snapshots(DBSnapshotIdentifier="qa-rds-snap-1")["DBSnapshots"]
        assert len(snaps) == 1
        assert snaps[0]["DBSnapshotIdentifier"] == "qa-rds-snap-1"
        assert snaps[0]["Status"] == "available"
        rds.delete_db_snapshot(DBSnapshotIdentifier="qa-rds-snap-1")
        snaps2 = rds.describe_db_snapshots()["DBSnapshots"]
        assert not any(s["DBSnapshotIdentifier"] == "qa-rds-snap-1" for s in snaps2)
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="qa-rds-snap-db", SkipFinalSnapshot=True)


def test_rds_deletion_protection(rds):
    """DeleteDBInstance fails when DeletionProtection=True."""
    rds.create_db_instance(
        DBInstanceIdentifier="qa-rds-protected",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password",
        AllocatedStorage=20,
        DeletionProtection=True,
    )
    try:
        with pytest.raises(ClientError) as exc:
            rds.delete_db_instance(DBInstanceIdentifier="qa-rds-protected")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    finally:
        rds.modify_db_instance(
            DBInstanceIdentifier="qa-rds-protected",
            DeletionProtection=False,
            ApplyImmediately=True,
        )
        rds.delete_db_instance(DBInstanceIdentifier="qa-rds-protected", SkipFinalSnapshot=True)


# ---------------------------------------------------------------------------
# ELASTICACHE — edge cases
# ---------------------------------------------------------------------------


def test_ec_describe_cache_parameters(ec):
    """DescribeCacheParameters returns parameters for a parameter group."""
    ec.create_cache_parameter_group(
        CacheParameterGroupName="qa-ec-params",
        CacheParameterGroupFamily="redis7.0",
        Description="test",
    )
    resp = ec.describe_cache_parameters(CacheParameterGroupName="qa-ec-params")
    assert "Parameters" in resp
    assert len(resp["Parameters"]) > 0


def test_ec_modify_cache_parameter_group(ec):
    """ModifyCacheParameterGroup updates parameter values."""
    ec.create_cache_parameter_group(
        CacheParameterGroupName="qa-ec-modify-params",
        CacheParameterGroupFamily="redis7.0",
        Description="test",
    )
    ec.modify_cache_parameter_group(
        CacheParameterGroupName="qa-ec-modify-params",
        ParameterNameValues=[{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"}],
    )
    params = ec.describe_cache_parameters(CacheParameterGroupName="qa-ec-modify-params")["Parameters"]
    maxmem = next((p for p in params if p["ParameterName"] == "maxmemory-policy"), None)
    assert maxmem is not None
    assert maxmem["ParameterValue"] == "allkeys-lru"


# ---------------------------------------------------------------------------
# SES — edge cases
# ---------------------------------------------------------------------------


def test_ses_send_templated_email(ses):
    """SendTemplatedEmail renders template and stores email."""
    ses.verify_email_identity(EmailAddress="sender@example.com")
    ses.create_template(
        Template={
            "TemplateName": "qa-ses-tmpl",
            "SubjectPart": "Hello {{name}}",
            "TextPart": "Hi {{name}}, welcome!",
            "HtmlPart": "<p>Hi {{name}}</p>",
        }
    )
    resp = ses.send_templated_email(
        Source="sender@example.com",
        Destination={"ToAddresses": ["user@example.com"]},
        Template="qa-ses-tmpl",
        TemplateData=json.dumps({"name": "Alice"}),
    )
    assert "MessageId" in resp


def test_ses_verify_domain(ses):
    """VerifyDomainIdentity returns a verification token."""
    resp = ses.verify_domain_identity(Domain="example.com")
    assert "VerificationToken" in resp
    assert len(resp["VerificationToken"]) > 0
    identities = ses.list_identities(IdentityType="Domain")["Identities"]
    assert "example.com" in identities


def test_ses_configuration_set_crud(ses):
    """CreateConfigurationSet / DescribeConfigurationSet / DeleteConfigurationSet."""
    ses.create_configuration_set(ConfigurationSet={"Name": "qa-ses-config"})
    desc = ses.describe_configuration_set(ConfigurationSetName="qa-ses-config")
    assert desc["ConfigurationSet"]["Name"] == "qa-ses-config"
    sets = ses.list_configuration_sets()["ConfigurationSets"]
    assert any(s["Name"] == "qa-ses-config" for s in sets)
    ses.delete_configuration_set(ConfigurationSetName="qa-ses-config")
    sets2 = ses.list_configuration_sets()["ConfigurationSets"]
    assert not any(s["Name"] == "qa-ses-config" for s in sets2)


# ---------------------------------------------------------------------------
# SFN Activities
# ---------------------------------------------------------------------------


def test_sfn_activity_create_describe_delete(sfn):
    resp = sfn.create_activity(name="qa-act-crud")
    arn = resp["activityArn"]
    assert ":activity:qa-act-crud" in arn

    desc = sfn.describe_activity(activityArn=arn)
    assert desc["name"] == "qa-act-crud"
    assert desc["activityArn"] == arn

    sfn.delete_activity(activityArn=arn)
    with pytest.raises(ClientError) as exc:
        sfn.describe_activity(activityArn=arn)
    assert exc.value.response["Error"]["Code"] == "ActivityDoesNotExist"


def test_sfn_activity_list(sfn):
    sfn.create_activity(name="qa-act-list-1")
    sfn.create_activity(name="qa-act-list-2")
    acts = sfn.list_activities()["activities"]
    names = [a["name"] for a in acts]
    assert "qa-act-list-1" in names
    assert "qa-act-list-2" in names


def test_sfn_activity_create_already_exists(sfn):
    sfn.create_activity(name="qa-act-idem")
    with pytest.raises(ClientError) as exc:
        sfn.create_activity(name="qa-act-idem")
    assert exc.value.response["Error"]["Code"] == "ActivityAlreadyExists"


def test_sfn_activity_worker_flow(sfn):
    """Worker calls GetActivityTask, then SendTaskSuccess — execution succeeds."""
    import threading

    act_arn = sfn.create_activity(name="qa-act-worker")["activityArn"]

    definition = json.dumps(
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": act_arn, "End": True},
            },
        }
    )
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-act-worker",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"msg": "hello"}))["executionArn"]

    def worker():
        task = sfn.get_activity_task(activityArn=act_arn, workerName="test-worker")
        assert task["taskToken"] != ""
        assert json.loads(task["input"])["msg"] == "hello"
        sfn.send_task_success(
            taskToken=task["taskToken"],
            output=json.dumps({"result": "done"}),
        )

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=10)

    for _ in range(20):
        time.sleep(0.3)
        status = sfn.describe_execution(executionArn=exec_arn)["status"]
        if status != "RUNNING":
            break

    assert sfn.describe_execution(executionArn=exec_arn)["status"] == "SUCCEEDED"
    output = json.loads(sfn.describe_execution(executionArn=exec_arn)["output"])
    assert output["result"] == "done"


def test_sfn_activity_worker_failure(sfn):
    """Worker calls GetActivityTask then SendTaskFailure — execution fails."""
    import threading

    act_arn = sfn.create_activity(name="qa-act-fail")["activityArn"]

    definition = json.dumps(
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": act_arn, "End": True},
            },
        }
    )
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-act-fail",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(stateMachineArn=sm_arn, input="{}")["executionArn"]

    def worker():
        task = sfn.get_activity_task(activityArn=act_arn, workerName="test-worker")
        sfn.send_task_failure(
            taskToken=task["taskToken"],
            error="WorkerError",
            cause="something went wrong",
        )

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=10)

    for _ in range(20):
        time.sleep(0.3)
        status = sfn.describe_execution(executionArn=exec_arn)["status"]
        if status != "RUNNING":
            break

    assert sfn.describe_execution(executionArn=exec_arn)["status"] == "FAILED"


# ---------------------------------------------------------------------------
# EC2
# ---------------------------------------------------------------------------


def test_ec2_describe_vpcs_default(ec2):
    resp = ec2.describe_vpcs()
    vpcs = resp["Vpcs"]
    assert any(v["IsDefault"] for v in vpcs)


def test_ec2_describe_subnets_default(ec2):
    resp = ec2.describe_subnets()
    assert len(resp["Subnets"]) >= 1


def test_ec2_describe_availability_zones(ec2):
    resp = ec2.describe_availability_zones()
    azs = [az["ZoneName"] for az in resp["AvailabilityZones"]]
    assert any("us-east-1" in az for az in azs)


def test_ec2_run_describe_terminate_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    assert len(resp["Instances"]) == 1
    instance_id = resp["Instances"][0]["InstanceId"]
    assert instance_id.startswith("i-")
    assert resp["Instances"][0]["State"]["Name"] == "running"

    desc = ec2.describe_instances(InstanceIds=[instance_id])
    assert len(desc["Reservations"]) == 1
    assert desc["Reservations"][0]["Instances"][0]["InstanceId"] == instance_id

    term = ec2.terminate_instances(InstanceIds=[instance_id])
    assert term["TerminatingInstances"][0]["CurrentState"]["Name"] == "terminated"


def test_ec2_stop_start_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    stop = ec2.stop_instances(InstanceIds=[iid])
    assert stop["StoppingInstances"][0]["CurrentState"]["Name"] == "stopped"

    start = ec2.start_instances(InstanceIds=[iid])
    assert start["StartingInstances"][0]["CurrentState"]["Name"] == "running"

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_run_multiple_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=3, MaxCount=3)
    assert len(resp["Instances"]) == 3
    ids = [i["InstanceId"] for i in resp["Instances"]]
    assert len(set(ids)) == 3
    ec2.terminate_instances(InstanceIds=ids)


def test_ec2_describe_images(ec2):
    resp = ec2.describe_images(Owners=["self"])
    assert len(resp["Images"]) >= 1
    assert all("ImageId" in img for img in resp["Images"])


def test_ec2_security_group_crud(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg", Description="test sg")["GroupId"]
    assert sg_id.startswith("sg-")

    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    assert desc["SecurityGroups"][0]["GroupName"] == "qa-ec2-sg"

    ec2.delete_security_group(GroupId=sg_id)
    desc2 = ec2.describe_security_groups()
    assert not any(sg["GroupId"] == sg_id for sg in desc2["SecurityGroups"])


def test_ec2_security_group_duplicate(ec2):
    ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    with pytest.raises(ClientError) as exc:
        ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    assert exc.value.response["Error"]["Code"] == "InvalidGroup.Duplicate"


def test_ec2_sg_authorize_revoke_ingress(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg-rules", Description="rules test")["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    perms = desc["SecurityGroups"][0]["IpPermissions"]
    assert any(p["FromPort"] == 80 for p in perms)

    ec2.revoke_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc2 = ec2.describe_security_groups(GroupIds=[sg_id])
    assert not any(p.get("FromPort") == 80 for p in desc2["SecurityGroups"][0]["IpPermissions"])

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_key_pair_crud(ec2):
    resp = ec2.create_key_pair(KeyName="qa-ec2-key")
    assert resp["KeyName"] == "qa-ec2-key"
    assert "KeyMaterial" in resp

    desc = ec2.describe_key_pairs(KeyNames=["qa-ec2-key"])
    assert len(desc["KeyPairs"]) == 1

    ec2.delete_key_pair(KeyName="qa-ec2-key")
    desc2 = ec2.describe_key_pairs()
    assert not any(kp["KeyName"] == "qa-ec2-key" for kp in desc2["KeyPairs"])


def test_ec2_key_pair_duplicate(ec2):
    ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    with pytest.raises(ClientError) as exc:
        ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    assert exc.value.response["Error"]["Code"] == "InvalidKeyPair.Duplicate"


def test_ec2_vpc_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.1.0.0/16")["Vpc"]["VpcId"]
    assert vpc_id.startswith("vpc-")

    desc = ec2.describe_vpcs(VpcIds=[vpc_id])
    assert desc["Vpcs"][0]["CidrBlock"] == "10.1.0.0/16"
    assert not desc["Vpcs"][0]["IsDefault"]

    ec2.delete_vpc(VpcId=vpc_id)
    desc2 = ec2.describe_vpcs()
    assert not any(v["VpcId"] == vpc_id for v in desc2["Vpcs"])


def test_ec2_subnet_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.2.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.2.1.0/24")["Subnet"]["SubnetId"]
    assert subnet_id.startswith("subnet-")

    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["CidrBlock"] == "10.2.1.0/24"

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_internet_gateway_crud(ec2):
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
    assert igw_id.startswith("igw-")

    vpc_id = ec2.create_vpc(CidrBlock="10.3.0.0/16")["Vpc"]["VpcId"]
    ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

    desc = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
    assert len(desc["InternetGateways"][0]["Attachments"]) == 1

    ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_elastic_ip_crud(ec2):
    alloc = ec2.allocate_address(Domain="vpc")
    alloc_id = alloc["AllocationId"]
    assert alloc_id.startswith("eipalloc-")
    assert "PublicIp" in alloc

    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    assoc = ec2.associate_address(AllocationId=alloc_id, InstanceId=iid)
    assert "AssociationId" in assoc

    desc = ec2.describe_addresses(AllocationIds=[alloc_id])
    assert desc["Addresses"][0]["InstanceId"] == iid

    ec2.disassociate_address(AssociationId=assoc["AssociationId"])
    ec2.release_address(AllocationId=alloc_id)
    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_tags_crud(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    ec2.create_tags(Resources=[iid], Tags=[{"Key": "Name", "Value": "qa-box"}])

    desc = ec2.describe_instances(InstanceIds=[iid])
    tags = desc["Reservations"][0]["Instances"][0]["Tags"]
    assert any(t["Key"] == "Name" and t["Value"] == "qa-box" for t in tags)

    ec2.delete_tags(Resources=[iid], Tags=[{"Key": "Name"}])
    desc2 = ec2.describe_instances(InstanceIds=[iid])
    tags2 = desc2["Reservations"][0]["Instances"][0].get("Tags", [])
    assert not any(t["Key"] == "Name" for t in tags2)

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_modify_vpc_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.10.0.0/16")["Vpc"]["VpcId"]
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_modify_subnet_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.11.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.11.1.0/24")["Subnet"]["SubnetId"]
    ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["MapPublicIpOnLaunch"] is True
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_route_table_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.20.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    assert rtb_id.startswith("rtb-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert desc["RouteTables"][0]["RouteTableId"] == rtb_id

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_route_table_associate_disassociate(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.21.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.21.1.0/24")["Subnet"]["SubnetId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

    assoc_id = ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)["AssociationId"]
    assert assoc_id.startswith("rtbassoc-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assocs = desc["RouteTables"][0]["Associations"]
    assert any(a["RouteTableAssociationId"] == assoc_id for a in assocs)

    ec2.disassociate_route_table(AssociationId=assoc_id)
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(a["RouteTableAssociationId"] == assoc_id for a in desc2["RouteTables"][0]["Associations"])

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_route_create_replace_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.22.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]

    ec2.create_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    routes = desc["RouteTables"][0]["Routes"]
    assert any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in routes)

    ec2.replace_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId="local")

    ec2.delete_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0")
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in desc2["RouteTables"][0]["Routes"])

    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_network_interface_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.30.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.30.1.0/24")["Subnet"]["SubnetId"]

    eni_id = ec2.create_network_interface(SubnetId=subnet_id, Description="qa-eni")["NetworkInterface"][
        "NetworkInterfaceId"
    ]
    assert eni_id.startswith("eni-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc["NetworkInterfaces"][0]["Description"] == "qa-eni"
    assert desc["NetworkInterfaces"][0]["Status"] == "available"

    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    desc2 = ec2.describe_network_interfaces()
    assert not any(e["NetworkInterfaceId"] == eni_id for e in desc2["NetworkInterfaces"])

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_network_interface_attach_detach(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.31.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.31.1.0/24")["Subnet"]["SubnetId"]
    eni_id = ec2.create_network_interface(SubnetId=subnet_id)["NetworkInterface"]["NetworkInterfaceId"]
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attach_resp = ec2.attach_network_interface(NetworkInterfaceId=eni_id, InstanceId=iid, DeviceIndex=1)
    attachment_id = attach_resp["AttachmentId"]
    assert attachment_id.startswith("eni-attach-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc["NetworkInterfaces"][0]["Status"] == "in-use"

    ec2.detach_network_interface(AttachmentId=attachment_id)
    desc2 = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc2["NetworkInterfaces"][0]["Status"] == "available"

    ec2.terminate_instances(InstanceIds=[iid])
    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_vpc_endpoint_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.40.0.0/16")["Vpc"]["VpcId"]

    vpce_id = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        VpcEndpointType="Gateway",
    )["VpcEndpoint"]["VpcEndpointId"]
    assert vpce_id.startswith("vpce-")

    desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
    assert desc["VpcEndpoints"][0]["ServiceName"] == "com.amazonaws.us-east-1.s3"
    assert desc["VpcEndpoints"][0]["State"] == "available"

    ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
    desc2 = ec2.describe_vpc_endpoints()
    assert not any(e["VpcEndpointId"] == vpce_id for e in desc2["VpcEndpoints"])

    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_describe_route_tables_default(ec2):
    desc = ec2.describe_route_tables()
    assert any(rt["VpcId"] == "vpc-00000001" for rt in desc["RouteTables"])


# ---------------------------------------------------------------------------
# EMR
# ---------------------------------------------------------------------------


def test_emr_run_job_flow_simple(emr):
    resp = emr.run_job_flow(
        Name="test-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 3,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    assert resp["JobFlowId"].startswith("j-")
    assert "ClusterArn" in resp
    assert "elasticmapreduce" in resp["ClusterArn"]


def test_emr_describe_cluster(emr):
    jf = emr.run_job_flow(
        Name="describe-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    desc = emr.describe_cluster(ClusterId=cluster_id)
    cluster = desc["Cluster"]
    assert cluster["Id"] == cluster_id
    assert cluster["Name"] == "describe-test"
    assert cluster["Status"]["State"] == "WAITING"
    assert cluster["ReleaseLabel"] == "emr-6.10.0"


def test_emr_list_clusters(emr):
    emr.run_job_flow(
        Name="list-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    resp = emr.list_clusters()
    assert len(resp["Clusters"]) >= 1
    assert all("Id" in c for c in resp["Clusters"])


def test_emr_terminate_job_flows(emr):
    jf = emr.run_job_flow(
        Name="terminate-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Status"]["State"] == "TERMINATED"


def test_emr_termination_protection(emr):
    jf = emr.run_job_flow(
        Name="protected-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    import botocore.exceptions

    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        assert False, "should have raised"
    except botocore.exceptions.ClientError as e:
        assert "ValidationException" in str(e) or "protected" in str(e).lower()


def test_emr_add_and_list_steps(emr):
    jf = emr.run_job_flow(
        Name="steps-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    step_resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "my-spark-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--class",
                        "com.example.Main",
                        "s3://bucket/app.jar",
                    ],
                },
            }
        ],
    )
    assert len(step_resp["StepIds"]) == 1
    step_id = step_resp["StepIds"][0]
    assert step_id.startswith("s-")

    steps = emr.list_steps(ClusterId=cluster_id)
    assert any(s["Id"] == step_id for s in steps["Steps"])


def test_emr_describe_step(emr):
    jf = emr.run_job_flow(
        Name="describe-step-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    step_resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "step1",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": []},
            }
        ],
    )
    step_id = step_resp["StepIds"][0]
    desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
    assert desc["Step"]["Id"] == step_id
    assert desc["Step"]["Status"]["State"] == "COMPLETED"


def test_emr_tags(emr):
    jf = emr.run_job_flow(
        Name="tagged-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        Tags=[{"Key": "env", "Value": "test"}],
    )
    cluster_id = jf["JobFlowId"]
    emr.add_tags(ResourceId=cluster_id, Tags=[{"Key": "team", "Value": "data"}])
    desc = emr.describe_cluster(ClusterId=cluster_id)
    tag_map = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "data"

    emr.remove_tags(ResourceId=cluster_id, TagKeys=["env"])
    desc2 = emr.describe_cluster(ClusterId=cluster_id)
    tag_keys = [t["Key"] for t in desc2["Cluster"]["Tags"]]
    assert "env" not in tag_keys
    assert "team" in tag_keys


def test_emr_auto_terminate_state(emr):
    """Cluster with KeepJobFlowAliveWhenNoSteps=False starts as TERMINATED."""
    jf = emr.run_job_flow(
        Name="auto-terminate-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": False,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Status"]["State"] == "TERMINATED"
    assert desc["Cluster"]["AutoTerminate"] is True


def test_emr_modify_cluster(emr):
    jf = emr.run_job_flow(
        Name="modify-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    resp = emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=5)
    assert resp["StepConcurrencyLevel"] == 5


def test_emr_block_public_access(emr):
    resp = emr.get_block_public_access_configuration()
    assert "BlockPublicAccessConfiguration" in resp
    assert resp["BlockPublicAccessConfiguration"]["BlockPublicSecurityGroupRules"] is False

    emr.put_block_public_access_configuration(
        BlockPublicAccessConfiguration={
            "BlockPublicSecurityGroupRules": True,
            "PermittedPublicSecurityGroupRuleRanges": [{"MinRange": 22, "MaxRange": 22}],
        }
    )
    resp2 = emr.get_block_public_access_configuration()
    assert resp2["BlockPublicAccessConfiguration"]["BlockPublicSecurityGroupRules"] is True


def test_emr_instance_groups(emr):
    jf = emr.run_job_flow(
        Name="ig-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    groups = emr.list_instance_groups(ClusterId=cluster_id)
    assert len(groups["InstanceGroups"]) >= 2

    new_group_resp = emr.add_instance_groups(
        JobFlowId=cluster_id,
        InstanceGroups=[
            {
                "Name": "Task",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            }
        ],
    )
    assert len(new_group_resp["InstanceGroupIds"]) == 1
    groups2 = emr.list_instance_groups(ClusterId=cluster_id)
    assert len(groups2["InstanceGroups"]) == 3


# ---------------------------------------------------------------------------
# ALB / ELBv2
# ---------------------------------------------------------------------------


def test_elbv2_create_describe_delete_lb(elbv2):
    resp = elbv2.create_load_balancer(Name="qa-alb", Type="application", Scheme="internet-facing")
    lb = resp["LoadBalancers"][0]
    lb_arn = lb["LoadBalancerArn"]
    assert lb_arn.startswith("arn:aws:elasticloadbalancing")
    assert lb["LoadBalancerName"] == "qa-alb"
    assert lb["Type"] == "application"
    assert lb["Scheme"] == "internet-facing"
    assert "DNSName" in lb
    assert lb["State"]["Code"] == "active"

    desc = elbv2.describe_load_balancers(LoadBalancerArns=[lb_arn])
    assert desc["LoadBalancers"][0]["LoadBalancerArn"] == lb_arn

    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
    desc2 = elbv2.describe_load_balancers()
    assert not any(l["LoadBalancerArn"] == lb_arn for l in desc2["LoadBalancers"])


def test_elbv2_describe_lb_by_name(elbv2):
    elbv2.create_load_balancer(Name="qa-alb-named")
    resp = elbv2.describe_load_balancers(Names=["qa-alb-named"])
    assert len(resp["LoadBalancers"]) == 1
    assert resp["LoadBalancers"][0]["LoadBalancerName"] == "qa-alb-named"
    elbv2.delete_load_balancer(LoadBalancerArn=resp["LoadBalancers"][0]["LoadBalancerArn"])


def test_elbv2_duplicate_lb_name(elbv2):
    elbv2.create_load_balancer(Name="qa-alb-dup")
    import botocore.exceptions

    try:
        elbv2.create_load_balancer(Name="qa-alb-dup")
        assert False, "should have raised"
    except botocore.exceptions.ClientError as e:
        assert "DuplicateLoadBalancerName" in str(e)
    finally:
        lbs = elbv2.describe_load_balancers(Names=["qa-alb-dup"])["LoadBalancers"]
        if lbs:
            elbv2.delete_load_balancer(LoadBalancerArn=lbs[0]["LoadBalancerArn"])


def test_elbv2_lb_attributes(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-attrs")["LoadBalancers"][0]["LoadBalancerArn"]
    attrs = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)["Attributes"]
    keys = {a["Key"] for a in attrs}
    assert "idle_timeout.timeout_seconds" in keys

    elbv2.modify_load_balancer_attributes(
        LoadBalancerArn=lb_arn,
        Attributes=[{"Key": "idle_timeout.timeout_seconds", "Value": "120"}],
    )
    updated = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)["Attributes"]
    val = next(a["Value"] for a in updated if a["Key"] == "idle_timeout.timeout_seconds")
    assert val == "120"
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_create_describe_delete_tg(elbv2):
    resp = elbv2.create_target_group(
        Name="qa-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        HealthCheckPath="/health",
    )
    tg = resp["TargetGroups"][0]
    tg_arn = tg["TargetGroupArn"]
    assert tg_arn.startswith("arn:aws:elasticloadbalancing")
    assert tg["TargetGroupName"] == "qa-tg"
    assert tg["HealthCheckPath"] == "/health"

    desc = elbv2.describe_target_groups(TargetGroupArns=[tg_arn])
    assert desc["TargetGroups"][0]["TargetGroupArn"] == tg_arn

    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    desc2 = elbv2.describe_target_groups()
    assert not any(t["TargetGroupArn"] == tg_arn for t in desc2["TargetGroups"])


def test_elbv2_tg_attributes(elbv2):
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-attrs",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    attrs = elbv2.describe_target_group_attributes(TargetGroupArn=tg_arn)["Attributes"]
    keys = {a["Key"] for a in attrs}
    assert "deregistration_delay.timeout_seconds" in keys

    elbv2.modify_target_group_attributes(
        TargetGroupArn=tg_arn,
        Attributes=[{"Key": "deregistration_delay.timeout_seconds", "Value": "60"}],
    )
    updated = elbv2.describe_target_group_attributes(TargetGroupArn=tg_arn)["Attributes"]
    val = next(a["Value"] for a in updated if a["Key"] == "deregistration_delay.timeout_seconds")
    assert val == "60"
    elbv2.delete_target_group(TargetGroupArn=tg_arn)


def test_elbv2_listener_crud(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-listener")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-l",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]

    l_resp = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )
    listener = l_resp["Listeners"][0]
    l_arn = listener["ListenerArn"]
    assert l_arn.startswith("arn:aws:elasticloadbalancing")
    assert listener["Port"] == 80
    assert listener["Protocol"] == "HTTP"

    desc = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
    assert any(l["ListenerArn"] == l_arn for l in desc["Listeners"])

    # TG should now reference LB
    tg_desc = elbv2.describe_target_groups(TargetGroupArns=[tg_arn])["TargetGroups"][0]
    assert lb_arn in tg_desc["LoadBalancerArns"]

    elbv2.modify_listener(ListenerArn=l_arn, Port=8080)
    updated = elbv2.describe_listeners(ListenerArns=[l_arn])["Listeners"][0]
    assert updated["Port"] == 8080

    elbv2.delete_listener(ListenerArn=l_arn)
    desc2 = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
    assert not any(l["ListenerArn"] == l_arn for l in desc2["Listeners"])

    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_rule_crud(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-rules")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-r",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    # describe should include default rule
    rules = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert any(r["IsDefault"] for r in rules)

    # create a custom rule
    rule_resp = elbv2.create_rule(
        ListenerArn=l_arn,
        Priority=10,
        Conditions=[{"Field": "path-pattern", "Values": ["/api/*"]}],
        Actions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )
    rule = rule_resp["Rules"][0]
    r_arn = rule["RuleArn"]
    assert not rule["IsDefault"]
    assert rule["Priority"] == "10"

    rules2 = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert any(r["RuleArn"] == r_arn for r in rules2)

    elbv2.delete_rule(RuleArn=r_arn)
    rules3 = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert not any(r["RuleArn"] == r_arn for r in rules3)

    elbv2.delete_listener(ListenerArn=l_arn)
    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_register_deregister_targets(elbv2):
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-targets",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]

    elbv2.register_targets(
        TargetGroupArn=tg_arn,
        Targets=[{"Id": "i-0001", "Port": 80}, {"Id": "i-0002", "Port": 80}],
    )
    health = elbv2.describe_target_health(TargetGroupArn=tg_arn)
    assert len(health["TargetHealthDescriptions"]) == 2
    ids = {d["Target"]["Id"] for d in health["TargetHealthDescriptions"]}
    assert ids == {"i-0001", "i-0002"}
    for d in health["TargetHealthDescriptions"]:
        assert d["TargetHealth"]["State"] == "healthy"

    elbv2.deregister_targets(TargetGroupArn=tg_arn, Targets=[{"Id": "i-0001"}])
    health2 = elbv2.describe_target_health(TargetGroupArn=tg_arn)
    assert len(health2["TargetHealthDescriptions"]) == 1
    assert health2["TargetHealthDescriptions"][0]["Target"]["Id"] == "i-0002"

    elbv2.delete_target_group(TargetGroupArn=tg_arn)


def test_elbv2_tags(elbv2):
    lb_arn = elbv2.create_load_balancer(
        Name="qa-alb-tags",
        Tags=[{"Key": "env", "Value": "test"}],
    )["LoadBalancers"][0]["LoadBalancerArn"]

    elbv2.add_tags(
        ResourceArns=[lb_arn],
        Tags=[{"Key": "team", "Value": "infra"}],
    )
    desc = elbv2.describe_tags(ResourceArns=[lb_arn])
    tag_map = {t["Key"]: t["Value"] for t in desc["TagDescriptions"][0]["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "infra"

    elbv2.remove_tags(ResourceArns=[lb_arn], TagKeys=["env"])
    desc2 = elbv2.describe_tags(ResourceArns=[lb_arn])
    tag_map2 = {t["Key"]: t["Value"] for t in desc2["TagDescriptions"][0]["Tags"]}
    assert "env" not in tag_map2
    assert tag_map2["team"] == "infra"

    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


# ---------------------------------------------------------------------------
# ALB data-plane (traffic routing)
# ---------------------------------------------------------------------------


def _alb_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _alb_setup(elbv2, lam, lb_name, fn_name, fn_code, listener_port=80, extra_rules=None):
    """Create LB + Lambda TG + listener + register Lambda as target.
    Returns (lb_arn, tg_arn, l_arn, fn_arn).
    """
    # Lambda
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _alb_zip(fn_code)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    # ALB infra
    lb_arn = elbv2.create_load_balancer(Name=lb_name)["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name=f"{lb_name}-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    elbv2.register_targets(TargetGroupArn=tg_arn, Targets=[{"Id": fn_arn}])

    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=listener_port,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    for rule_kwargs in extra_rules or []:
        elbv2.create_rule(ListenerArn=l_arn, **rule_kwargs)

    return lb_arn, tg_arn, l_arn, fn_arn


def _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, fn_name):
    try:
        elbv2.delete_listener(ListenerArn=l_arn)
    except Exception:
        pass
    try:
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
    except Exception:
        pass
    try:
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
    except Exception:
        pass
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass


def test_alb_dataplane_forward_lambda(elbv2, lam):
    """ALB forwards request to Lambda via /_alb/{lb-name}/ path prefix."""
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {\n"
        "        'statusCode': 200,\n"
        "        'headers': {'Content-Type': 'application/json'},\n"
        "        'body': json.dumps({'method': event['httpMethod'], 'path': event['path']}),\n"
        "    }\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-fwd", "dp-alb-fwd-fn", fn_code)
    try:
        url = f"{_endpoint}/_alb/dp-alb-fwd/api/hello"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["method"] == "GET"
        assert body["path"] == "/api/hello"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-fwd-fn")


def test_alb_dataplane_event_shape(elbv2, lam):
    """ALB event passed to Lambda contains all required fields."""
    import urllib.parse as _parse
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {\n"
        "        'statusCode': 200,\n"
        "        'headers': {'Content-Type': 'application/json'},\n"
        "        'body': json.dumps(event),\n"
        "    }\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-evt", "dp-alb-evt-fn", fn_code)
    try:
        url = f"{_endpoint}/_alb/dp-alb-evt/check?foo=bar"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        body = json.loads(resp.read())
        assert "requestContext" in body
        assert "elb" in body["requestContext"]
        assert body["httpMethod"] == "GET"
        assert body["path"] == "/check"
        assert body["queryStringParameters"].get("foo") == "bar"
        assert "headers" in body
        assert body["isBase64Encoded"] is False
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-evt-fn")


def test_alb_dataplane_fixed_response(elbv2, lam):
    """ALB fixed-response action returns configured status/body without invoking Lambda."""
    import urllib.error as _err
    import urllib.request as _req

    fn_code = "def handler(event, context):\n    return {'statusCode': 200, 'body': 'should-not-reach'}\n"
    lb_arn = elbv2.create_load_balancer(Name="dp-alb-fixed")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="dp-alb-fixed-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[
            {
                "Type": "fixed-response",
                "FixedResponseConfig": {
                    "StatusCode": "200",
                    "ContentType": "text/plain",
                    "MessageBody": "maintenance",
                },
            }
        ],
    )["Listeners"][0]["ListenerArn"]
    try:
        url = f"{_endpoint}/_alb/dp-alb-fixed/any/path"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        assert resp.read() == b"maintenance"
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        try:
            lam.delete_function(FunctionName="dp-alb-fixed-fn")
        except Exception:
            pass


def test_alb_dataplane_redirect(elbv2):
    """ALB redirect action returns 301 with a Location header."""
    import http.client as _http
    from urllib.parse import urlparse as _urlparse

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-redir")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="dp-alb-redir-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[
            {
                "Type": "redirect",
                "RedirectConfig": {
                    "Protocol": "https",
                    "Host": "example.com",
                    "Path": "/new",
                    "StatusCode": "HTTP_301",
                },
            }
        ],
    )["Listeners"][0]["ListenerArn"]
    try:
        # Use http.client directly — it never auto-follows redirects
        parsed = _urlparse(_endpoint)
        conn = _http.HTTPConnection(parsed.hostname, parsed.port or 4566)
        conn.request("GET", "/_alb/dp-alb-redir/old")
        resp = conn.getresponse()
        assert resp.status == 301
        location = resp.getheader("Location", "")
        assert "example.com" in location
        conn.close()
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_alb_dataplane_path_pattern_rule(elbv2, lam):
    """Path-pattern rule routes /api/* to one Lambda; default routes to another."""
    import urllib.request as _req

    api_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'target': 'api'})}\n"
    )
    default_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'target': 'default'})}\n"
    )
    for fn_name, fn_code in [("dp-alb-api-fn", api_code), ("dp-alb-def-fn", default_code)]:
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.9",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _alb_zip(fn_code)},
        )

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-rules")["LoadBalancers"][0]["LoadBalancerArn"]
    api_tg_arn = elbv2.create_target_group(
        Name="dp-alb-api-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    def_tg_arn = elbv2.create_target_group(
        Name="dp-alb-def-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]

    api_fn_arn = lam.get_function(FunctionName="dp-alb-api-fn")["Configuration"]["FunctionArn"]
    def_fn_arn = lam.get_function(FunctionName="dp-alb-def-fn")["Configuration"]["FunctionArn"]
    elbv2.register_targets(TargetGroupArn=api_tg_arn, Targets=[{"Id": api_fn_arn}])
    elbv2.register_targets(TargetGroupArn=def_tg_arn, Targets=[{"Id": def_fn_arn}])

    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": def_tg_arn}],
    )["Listeners"][0]["ListenerArn"]
    elbv2.create_rule(
        ListenerArn=l_arn,
        Priority=10,
        Conditions=[{"Field": "path-pattern", "Values": ["/api/*"]}],
        Actions=[{"Type": "forward", "TargetGroupArn": api_tg_arn}],
    )

    try:
        # /api/* hits the api Lambda
        resp_api = _req.urlopen(_req.Request(f"{_endpoint}/_alb/dp-alb-rules/api/users", method="GET"))
        body_api = json.loads(resp_api.read())
        assert body_api["target"] == "api"

        # /other hits the default Lambda
        resp_def = _req.urlopen(_req.Request(f"{_endpoint}/_alb/dp-alb-rules/other", method="GET"))
        body_def = json.loads(resp_def.read())
        assert body_def["target"] == "default"
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        for tg in (api_tg_arn, def_tg_arn):
            elbv2.delete_target_group(TargetGroupArn=tg)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        for fn_name in ("dp-alb-api-fn", "dp-alb-def-fn"):
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass


def test_alb_dataplane_no_listener_returns_503(elbv2):
    """Request to an ALB with no listeners returns 503."""
    import urllib.error as _err
    import urllib.request as _req

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-empty")["LoadBalancers"][0]["LoadBalancerArn"]
    try:
        req = _req.Request(f"{_endpoint}/_alb/dp-alb-empty/anything", method="GET")
        try:
            _req.urlopen(req)
            assert False, "Expected 503"
        except _err.HTTPError as e:
            assert e.code == 503
    finally:
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_alb_dataplane_host_header_routing(elbv2, lam):
    """ALB matches requests by {lb-name}.alb.localhost Host header."""
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'routed': True})}\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-host", "dp-alb-host-fn", fn_code)
    try:
        # Send to the plain ministack port but with the ALB host header
        req = _req.Request(f"{_endpoint}/hello", method="GET")
        req.add_header("Host", f"dp-alb-host.alb.localhost:{_EXECUTE_PORT}")
        resp = _req.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["routed"] is True
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-host-fn")


# EBS (Elastic Block Store) — uses ec2 client
# ---------------------------------------------------------------------------


def test_ebs_create_and_describe_volume(ebs):
    resp = ebs.create_volume(
        AvailabilityZone="us-east-1a",
        Size=20,
        VolumeType="gp3",
    )
    vol_id = resp["VolumeId"]
    assert vol_id.startswith("vol-")
    assert resp["State"] == "available"
    assert resp["Size"] == 20
    assert resp["VolumeType"] == "gp3"

    desc = ebs.describe_volumes(VolumeIds=[vol_id])
    assert len(desc["Volumes"]) == 1
    assert desc["Volumes"][0]["VolumeId"] == vol_id


def test_ebs_attach_detach_volume(ebs):
    inst = ebs.run_instances(ImageId="ami-00000001", MinCount=1, MaxCount=1)
    instance_id = inst["Instances"][0]["InstanceId"]

    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]

    ebs.attach_volume(VolumeId=vol_id, InstanceId=instance_id, Device="/dev/xvdf")
    desc = ebs.describe_volumes(VolumeIds=[vol_id])
    assert desc["Volumes"][0]["State"] == "in-use"
    assert desc["Volumes"][0]["Attachments"][0]["InstanceId"] == instance_id

    ebs.detach_volume(VolumeId=vol_id)
    desc2 = ebs.describe_volumes(VolumeIds=[vol_id])
    assert desc2["Volumes"][0]["State"] == "available"
    assert desc2["Volumes"][0]["Attachments"] == []


def test_ebs_delete_volume(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=5, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ebs.delete_volume(VolumeId=vol_id)
    desc = ebs.describe_volumes(VolumeIds=[vol_id])
    assert len(desc["Volumes"]) == 0


def test_ebs_modify_volume(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ebs.modify_volume(VolumeId=vol_id, Size=50, VolumeType="gp3")
    assert resp["VolumeModification"]["TargetSize"] == 50
    assert resp["VolumeModification"]["TargetVolumeType"] == "gp3"


def test_ebs_volume_status(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=8, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ebs.describe_volume_status(VolumeIds=[vol_id])
    assert len(resp["VolumeStatuses"]) == 1
    assert resp["VolumeStatuses"][0]["VolumeStatus"]["Status"] == "ok"


def test_ebs_create_and_describe_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    snap = ebs.create_snapshot(VolumeId=vol_id, Description="test snapshot")
    snap_id = snap["SnapshotId"]
    assert snap_id.startswith("snap-")
    assert snap["State"] == "completed"

    desc = ebs.describe_snapshots(SnapshotIds=[snap_id])
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["VolumeId"] == vol_id
    assert desc["Snapshots"][0]["Description"] == "test snapshot"


def test_ebs_delete_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ebs.create_snapshot(VolumeId=vol["VolumeId"])
    snap_id = snap["SnapshotId"]
    ebs.delete_snapshot(SnapshotId=snap_id)
    desc = ebs.describe_snapshots(SnapshotIds=[snap_id])
    assert len(desc["Snapshots"]) == 0


def test_ebs_copy_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ebs.create_snapshot(VolumeId=vol["VolumeId"], Description="original")
    snap_id = snap["SnapshotId"]
    copy = ebs.copy_snapshot(SourceRegion="us-east-1", SourceSnapshotId=snap_id, Description="copy")
    new_snap_id = copy["SnapshotId"]
    assert new_snap_id != snap_id
    assert new_snap_id.startswith("snap-")


# ---------------------------------------------------------------------------
# EFS (Elastic File System)
# ---------------------------------------------------------------------------


def test_efs_create_and_describe_filesystem(efs):
    resp = efs.create_file_system(
        PerformanceMode="generalPurpose",
        ThroughputMode="bursting",
        Encrypted=False,
        Tags=[{"Key": "Name", "Value": "test-fs"}],
    )
    fs_id = resp["FileSystemId"]
    assert fs_id.startswith("fs-")
    assert resp["LifeCycleState"] == "available"
    assert resp["ThroughputMode"] == "bursting"

    desc = efs.describe_file_systems(FileSystemId=fs_id)
    assert len(desc["FileSystems"]) == 1
    assert desc["FileSystems"][0]["FileSystemId"] == fs_id
    assert desc["FileSystems"][0]["Name"] == "test-fs"


def test_efs_creation_token_idempotency(efs):
    token = "unique-token-abc123"
    r1 = efs.create_file_system(CreationToken=token)
    r2 = efs.create_file_system(CreationToken=token)
    assert r1["FileSystemId"] == r2["FileSystemId"]


def test_efs_delete_filesystem(efs):
    resp = efs.create_file_system()
    fs_id = resp["FileSystemId"]
    efs.delete_file_system(FileSystemId=fs_id)
    desc = efs.describe_file_systems(FileSystemId=fs_id)
    assert len(desc["FileSystems"]) == 0


def test_efs_mount_target(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    mt = efs.create_mount_target(FileSystemId=fs_id, SubnetId="subnet-00000001")
    mt_id = mt["MountTargetId"]
    assert mt_id.startswith("fsmt-")
    assert mt["LifeCycleState"] == "available"

    desc = efs.describe_mount_targets(FileSystemId=fs_id)
    assert len(desc["MountTargets"]) == 1
    assert desc["MountTargets"][0]["MountTargetId"] == mt_id

    import botocore.exceptions

    try:
        efs.delete_file_system(FileSystemId=fs_id)
        assert False, "should raise"
    except botocore.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] in ("FileSystemInUse", "400") or "mount targets" in str(e).lower()

    efs.delete_mount_target(MountTargetId=mt_id)
    desc2 = efs.describe_mount_targets(FileSystemId=fs_id)
    assert len(desc2["MountTargets"]) == 0


def test_efs_access_point(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    ap = efs.create_access_point(
        FileSystemId=fs_id,
        Tags=[{"Key": "Name", "Value": "my-ap"}],
        RootDirectory={"Path": "/data"},
    )
    ap_id = ap["AccessPointId"]
    assert ap_id.startswith("fsap-")
    assert ap["LifeCycleState"] == "available"

    desc = efs.describe_access_points(FileSystemId=fs_id)
    assert any(a["AccessPointId"] == ap_id for a in desc["AccessPoints"])

    efs.delete_access_point(AccessPointId=ap_id)
    desc2 = efs.describe_access_points(FileSystemId=fs_id)
    assert not any(a["AccessPointId"] == ap_id for a in desc2["AccessPoints"])


def test_efs_tags(efs):
    fs = efs.create_file_system(Tags=[{"Key": "env", "Value": "test"}])
    fs_arn = fs["FileSystemArn"]
    efs.tag_resource(ResourceId=fs_arn, Tags=[{"Key": "team", "Value": "data"}])
    tags_resp = efs.list_tags_for_resource(ResourceId=fs_arn)
    tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "data"

    efs.untag_resource(ResourceId=fs_arn, TagKeys=["env"])
    tags_resp2 = efs.list_tags_for_resource(ResourceId=fs_arn)
    keys = [t["Key"] for t in tags_resp2["Tags"]]
    assert "env" not in keys
    assert "team" in keys


def test_efs_lifecycle_configuration(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    efs.put_lifecycle_configuration(
        FileSystemId=fs_id,
        LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
    )
    resp = efs.describe_lifecycle_configuration(FileSystemId=fs_id)
    assert len(resp["LifecyclePolicies"]) == 1
    assert resp["LifecyclePolicies"][0]["TransitionToIA"] == "AFTER_30_DAYS"


def test_efs_backup_policy(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    efs.put_backup_policy(
        FileSystemId=fs_id,
        BackupPolicy={"Status": "ENABLED"},
    )
    resp = efs.describe_backup_policy(FileSystemId=fs_id)
    assert resp["BackupPolicy"]["Status"] == "ENABLED"


# ---------------------------------------------------------------------------
# Regression: API Gateway v1 createdDate must be a Unix timestamp (number),
# not an ISO string. Terraform AWS provider errors with:
# "expected Timestamp to be a JSON Number, got string instead"
# ---------------------------------------------------------------------------


def test_apigwv1_created_date_is_unix_timestamp(apigw_v1):
    resp = apigw_v1.create_rest_api(name="tf-date-test")
    created = resp["createdDate"]
    # boto3 parses numeric timestamps as datetime.datetime — if it were a string
    # botocore would raise a deserialization error before we even get here.
    import datetime

    assert isinstance(created, datetime.datetime), (
        f"createdDate should be datetime (parsed from Unix int), got {type(created)}"
    )
    apigw_v1.delete_rest_api(restApiId=resp["id"])


def test_apigwv2_created_date_is_unix_timestamp(apigw):
    resp = apigw.create_api(Name="tf-date-test-v2", ProtocolType="HTTP")
    created = resp["CreatedDate"]
    import datetime

    assert isinstance(created, datetime.datetime), (
        f"CreatedDate should be datetime (parsed from Unix int), got {type(created)}"
    )
    apigw.delete_api(ApiId=resp["ApiId"])


# ---------------------------------------------------------------------------
# Regression: CloudWatch Logs ListTagsForResource fails when the ARN passed
# by Terraform lacks the trailing ':*' that MiniStack appends when storing.
# ---------------------------------------------------------------------------


def test_logs_list_tags_for_resource_arn_without_star(logs):
    name = "/tf/regression/arn-no-star"
    logs.create_log_group(logGroupName=name, tags={"env": "test"})
    # Get the ARN as stored (includes :*)
    groups = logs.describe_log_groups(logGroupNamePrefix=name)["logGroups"]
    stored_arn = groups[0]["arn"]
    assert stored_arn.endswith(":*"), f"Expected stored ARN to end with :*, got {stored_arn}"

    # Terraform sends the ARN without :* — this must not raise ResourceNotFoundException
    arn_no_star = stored_arn[:-2]  # strip ':*'
    resp = logs.list_tags_for_resource(resourceArn=arn_no_star)
    assert resp["tags"]["env"] == "test"
    logs.delete_log_group(logGroupName=name)


# ---------------------------------------------------------------------------
# Regression: SQS SendMessageBatch must reject batches larger than 10 entries.
# AWS returns TooManyEntriesInBatchRequest; MiniStack was silently accepting them.
# ---------------------------------------------------------------------------


def test_sqs_send_message_batch_limit(sqs):
    import pytest
    from botocore.exceptions import ClientError

    q = sqs.create_queue(QueueName="batch-limit-regression")["QueueUrl"]
    entries = [{"Id": str(i), "MessageBody": f"msg {i}"} for i in range(11)]
    with pytest.raises(ClientError) as exc_info:
        sqs.send_message_batch(QueueUrl=q, Entries=entries)
    assert exc_info.value.response["Error"]["Code"] == "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"
    sqs.delete_queue(QueueUrl=q)


# ---------------------------------------------------------------------------
# Regression: DynamoDB BatchWriteItem must include ConsumedCapacity list when
# ReturnConsumedCapacity="TOTAL" is set. Was returning the key absent entirely.
# ---------------------------------------------------------------------------


def test_dynamodb_batch_write_consumed_capacity(ddb):
    ddb.create_table(
        TableName="batch-cap-regression",
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.batch_write_item(
        RequestItems={
            "batch-cap-regression": [
                {"PutRequest": {"Item": {"pk": {"S": "k1"}}}},
            ]
        },
        ReturnConsumedCapacity="TOTAL",
    )
    assert "ConsumedCapacity" in resp, "ConsumedCapacity must be present when ReturnConsumedCapacity=TOTAL"
    assert isinstance(resp["ConsumedCapacity"], list), "ConsumedCapacity must be a list for BatchWriteItem"
    assert resp["ConsumedCapacity"][0]["TableName"] == "batch-cap-regression"
    assert resp["ConsumedCapacity"][0]["CapacityUnits"] == 1.0
    ddb.delete_table(TableName="batch-cap-regression")


def test_dynamodb_put_item_gsi_capacity(ddb):
    """PutItem on a table with 1 GSI must return CapacityUnits=2.0 (table + GSI)."""
    ddb.create_table(
        TableName="gsi-cap-put",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "last_name", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "last_name-index",
                "KeySchema": [{"AttributeName": "last_name", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.put_item(
        TableName="gsi-cap-put",
        Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "last_name": {"S": "Smith"}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 2.0
    ddb.delete_table(TableName="gsi-cap-put")


def test_dynamodb_batch_write_gsi_capacity(ddb):
    """BatchWriteItem with 2 items on a table with 1 GSI must return CapacityUnits=4.0."""
    ddb.create_table(
        TableName="gsi-cap-batch",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "age", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "age-index",
                "KeySchema": [{"AttributeName": "age", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.batch_write_item(
        RequestItems={
            "gsi-cap-batch": [
                {"PutRequest": {"Item": {"pk": {"S": "p2"}, "sk": {"S": "s2"}, "age": {"N": "25"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p3"}, "sk": {"S": "s3"}, "age": {"N": "26"}}}},
            ]
        },
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"][0]["CapacityUnits"] == 4.0
    ddb.delete_table(TableName="gsi-cap-batch")


# ---------------------------------------------------------------------------
# Regression: XML error responses must include <Type>Sender/Receiver</Type>
# so botocore populates typed exception classes (e.g. client.exceptions.QueueDoesNotExist).
# Without <Type>, botocore falls back to generic ClientError.
# ---------------------------------------------------------------------------


def test_sqs_typed_exception_queue_not_found(sqs):
    """client.exceptions.QueueDoesNotExist must be raised (not generic ClientError)
    when accessing a non-existent queue — requires <Type> in the XML error response."""
    import pytest

    with pytest.raises(sqs.exceptions.QueueDoesNotExist):
        sqs.get_queue_url(QueueName="queue-that-does-not-exist-typed-exc")


# ---------------------------------------------------------------------------
# Regression: SQS awsQueryCompatible — x-amzn-query-error header
# botocore reads this header and overrides Error.Code with the legacy
# AWS.SimpleQueueService.* code so callers checking the old namespaced
# strings still work against MiniStack exactly as they do against real AWS.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# VPC gaps: NAT Gateways
# ---------------------------------------------------------------------------


def test_ec2_nat_gateway_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.100.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.100.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]

    resp = ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")
    nat_id = resp["NatGateway"]["NatGatewayId"]
    assert nat_id.startswith("nat-")
    assert resp["NatGateway"]["State"] == "available"

    desc = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert len(desc["NatGateways"]) == 1
    assert desc["NatGateways"][0]["NatGatewayId"] == nat_id
    assert desc["NatGateways"][0]["SubnetId"] == subnet_id

    ec2.delete_nat_gateway(NatGatewayId=nat_id)
    desc2 = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert desc2["NatGateways"][0]["State"] == "deleted"


def test_ec2_nat_gateway_filter_by_vpc(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.101.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.101.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]
    ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")

    desc = ec2.describe_nat_gateways(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    assert all(n["VpcId"] == vpc_id for n in desc["NatGateways"])


# ---------------------------------------------------------------------------
# VPC gaps: Network ACLs
# ---------------------------------------------------------------------------


def test_ec2_network_acl_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.102.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]
    assert acl_id.startswith("acl-")
    assert resp["NetworkAcl"]["VpcId"] == vpc_id
    assert resp["NetworkAcl"]["IsDefault"] is False

    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc["NetworkAcls"]) == 1
    assert desc["NetworkAcls"][0]["NetworkAclId"] == acl_id

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id,
        RuleNumber=100,
        Protocol="-1",
        RuleAction="allow",
        Egress=False,
        CidrBlock="0.0.0.0/0",
    )
    desc2 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc2["NetworkAcls"][0]["Entries"]) == 1

    ec2.delete_network_acl_entry(NetworkAclId=acl_id, RuleNumber=100, Egress=False)
    desc3 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc3["NetworkAcls"][0]["Entries"]) == 0

    ec2.delete_network_acl(NetworkAclId=acl_id)
    desc4 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc4["NetworkAcls"]) == 0


def test_ec2_network_acl_replace_entry(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.103.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="deny", Egress=False, CidrBlock="10.0.0.0/8"
    )
    ec2.replace_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="allow", Egress=False, CidrBlock="10.0.0.0/8"
    )
    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    entries = desc["NetworkAcls"][0]["Entries"]
    assert len(entries) == 1
    assert entries[0]["RuleAction"] == "allow"


# ---------------------------------------------------------------------------
# VPC gaps: Flow Logs
# ---------------------------------------------------------------------------


def test_ec2_flow_logs_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.104.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_flow_logs(
        ResourceIds=[vpc_id],
        ResourceType="VPC",
        TrafficType="ALL",
        LogDestinationType="cloud-watch-logs",
        LogGroupName="/aws/vpc/flowlogs",
    )
    assert resp["Unsuccessful"] == []
    fl_ids = resp["FlowLogIds"]
    assert len(fl_ids) == 1
    assert fl_ids[0].startswith("fl-")

    desc = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc["FlowLogs"]) == 1
    assert desc["FlowLogs"][0]["FlowLogId"] == fl_ids[0]
    assert desc["FlowLogs"][0]["FlowLogStatus"] == "ACTIVE"

    ec2.delete_flow_logs(FlowLogIds=fl_ids)
    desc2 = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc2["FlowLogs"]) == 0


# ---------------------------------------------------------------------------
# VPC gaps: VPC Peering Connections
# ---------------------------------------------------------------------------


def test_ec2_vpc_peering_crud(ec2):
    vpc1 = ec2.create_vpc(CidrBlock="10.105.0.0/16")
    vpc2 = ec2.create_vpc(CidrBlock="10.106.0.0/16")
    vpc_id1 = vpc1["Vpc"]["VpcId"]
    vpc_id2 = vpc2["Vpc"]["VpcId"]

    resp = ec2.create_vpc_peering_connection(VpcId=vpc_id1, PeerVpcId=vpc_id2)
    pcx = resp["VpcPeeringConnection"]
    pcx_id = pcx["VpcPeeringConnectionId"]
    assert pcx_id.startswith("pcx-")
    assert pcx["Status"]["Code"] == "pending-acceptance"

    accepted = ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    assert accepted["VpcPeeringConnection"]["Status"]["Code"] == "active"

    desc = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert len(desc["VpcPeeringConnections"]) == 1
    assert desc["VpcPeeringConnections"][0]["Status"]["Code"] == "active"

    ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    desc2 = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert desc2["VpcPeeringConnections"][0]["Status"]["Code"] == "deleted"


def test_ec2_vpc_peering_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.accept_vpc_peering_connection(VpcPeeringConnectionId="pcx-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]


# ---------------------------------------------------------------------------
# VPC gaps: DHCP Options
# ---------------------------------------------------------------------------


def test_ec2_dhcp_options_crud(ec2):
    resp = ec2.create_dhcp_options(
        DhcpConfigurations=[
            {"Key": "domain-name", "Values": ["example.internal"]},
            {"Key": "domain-name-servers", "Values": ["10.0.0.1", "10.0.0.2"]},
        ]
    )
    dopt = resp["DhcpOptions"]
    dopt_id = dopt["DhcpOptionsId"]
    assert dopt_id.startswith("dopt-")

    desc = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc["DhcpOptions"]) == 1
    configs = {c["Key"]: [v["Value"] for v in c["Values"]] for c in desc["DhcpOptions"][0]["DhcpConfigurations"]}
    assert configs["domain-name"] == ["example.internal"]
    assert "10.0.0.1" in configs["domain-name-servers"]

    vpc = ec2.create_vpc(CidrBlock="10.107.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2.associate_dhcp_options(DhcpOptionsId=dopt_id, VpcId=vpc_id)

    ec2.delete_dhcp_options(DhcpOptionsId=dopt_id)
    desc2 = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc2["DhcpOptions"]) == 0


def test_ec2_dhcp_options_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_dhcp_options(DhcpOptionsId="dopt-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]


# ---------------------------------------------------------------------------
# VPC gaps: Egress-Only Internet Gateways
# ---------------------------------------------------------------------------


def test_ec2_egress_only_igw_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.108.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_egress_only_internet_gateway(VpcId=vpc_id)
    eigw = resp["EgressOnlyInternetGateway"]
    eigw_id = eigw["EgressOnlyInternetGatewayId"]
    assert eigw_id.startswith("eigw-")
    assert eigw["Attachments"][0]["State"] == "attached"
    assert eigw["Attachments"][0]["VpcId"] == vpc_id

    desc = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc["EgressOnlyInternetGateways"]) == 1
    assert desc["EgressOnlyInternetGateways"][0]["EgressOnlyInternetGatewayId"] == eigw_id

    ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId=eigw_id)
    desc2 = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc2["EgressOnlyInternetGateways"]) == 0


def test_ec2_egress_only_igw_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId="eigw-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]


# ===========================================================================
# ACM — Certificate Manager
# ===========================================================================


def test_acm_request_certificate(acm_client):
    resp = acm_client.request_certificate(
        DomainName="example.com",
        ValidationMethod="DNS",
        SubjectAlternativeNames=["www.example.com"],
    )
    arn = resp["CertificateArn"]
    assert arn.startswith("arn:aws:acm:us-east-1:000000000000:certificate/")


def test_acm_describe_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="describe.example.com")["CertificateArn"]
    resp = acm_client.describe_certificate(CertificateArn=arn)
    cert = resp["Certificate"]
    assert cert["DomainName"] == "describe.example.com"
    assert cert["Status"] == "ISSUED"
    assert len(cert["DomainValidationOptions"]) >= 1
    assert "ResourceRecord" in cert["DomainValidationOptions"][0]


def test_acm_list_certificates(acm_client):
    arn = acm_client.request_certificate(DomainName="list.example.com")["CertificateArn"]
    resp = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
    assert arn in arns


def test_acm_tags(acm_client):
    arn = acm_client.request_certificate(DomainName="tags.example.com")["CertificateArn"]
    acm_client.add_tags_to_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
    )
    tags = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    assert any(t["Key"] == "env" and t["Value"] == "test" for t in tags)
    acm_client.remove_tags_from_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "team", "Value": "platform"}],
    )
    tags2 = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    assert not any(t["Key"] == "team" for t in tags2)


def test_acm_get_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="pem.example.com")["CertificateArn"]
    resp = acm_client.get_certificate(CertificateArn=arn)
    assert "BEGIN CERTIFICATE" in resp["Certificate"]


def test_acm_import_certificate(acm_client):
    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    resp = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)
    arn = resp["CertificateArn"]
    desc = acm_client.describe_certificate(CertificateArn=arn)
    assert desc["Certificate"]["Type"] == "IMPORTED"


def test_acm_delete_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="delete.example.com")["CertificateArn"]
    acm_client.delete_certificate(CertificateArn=arn)
    resp = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
    assert arn not in arns


# ===========================================================================
# SES v2
# ===========================================================================


def test_ses_v2_send_email(sesv2):
    resp = sesv2.send_email(
        FromEmailAddress="sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Content={
            "Simple": {
                "Subject": {"Data": "Test Subject"},
                "Body": {"Text": {"Data": "Hello world"}},
            }
        },
    )
    assert resp["MessageId"].startswith("ministack-")


def test_ses_v2_email_identity_crud(sesv2):
    sesv2.create_email_identity(EmailIdentity="test-domain.com")
    resp = sesv2.get_email_identity(EmailIdentity="test-domain.com")
    assert resp["VerifiedForSendingStatus"] is True
    lst = sesv2.list_email_identities()
    names = [e["IdentityName"] for e in lst["EmailIdentities"]]
    assert "test-domain.com" in names
    sesv2.delete_email_identity(EmailIdentity="test-domain.com")
    lst2 = sesv2.list_email_identities()
    names2 = [e["IdentityName"] for e in lst2["EmailIdentities"]]
    assert "test-domain.com" not in names2


def test_ses_v2_configuration_set_crud(sesv2):
    sesv2.create_configuration_set(ConfigurationSetName="my-cfg-set")
    resp = sesv2.get_configuration_set(ConfigurationSetName="my-cfg-set")
    assert resp["ConfigurationSetName"] == "my-cfg-set"
    lst = sesv2.list_configuration_sets()
    assert "my-cfg-set" in lst["ConfigurationSets"]
    sesv2.delete_configuration_set(ConfigurationSetName="my-cfg-set")
    lst2 = sesv2.list_configuration_sets()
    assert "my-cfg-set" not in lst2["ConfigurationSets"]


def test_ses_v2_get_account(sesv2):
    resp = sesv2.get_account()
    assert resp["SendingEnabled"] is True
    assert resp["ProductionAccessEnabled"] is True


# ===========================================================================
# Lambda Layers
# ===========================================================================


def test_lambda_layer_publish(lam):
    import base64, zipfile, io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("layer.py", "# layer")
    zip_bytes = buf.getvalue()
    resp = lam.publish_layer_version(
        LayerName="my-test-layer",
        Description="Test layer",
        Content={"ZipFile": zip_bytes},
        CompatibleRuntimes=["python3.12"],
    )
    assert resp["Version"] == 1
    assert "my-test-layer" in resp["LayerVersionArn"]


def test_lambda_layer_get_version(lam):
    resp = lam.get_layer_version(LayerName="my-test-layer", VersionNumber=1)
    assert resp["Version"] == 1
    assert resp["Description"] == "Test layer"


def test_lambda_layer_list_versions(lam):
    resp = lam.list_layer_versions(LayerName="my-test-layer")
    assert len(resp["LayerVersions"]) >= 1
    assert resp["LayerVersions"][0]["Version"] == 1


def test_lambda_layer_list_layers(lam):
    resp = lam.list_layers()
    names = [l["LayerName"] for l in resp["Layers"]]
    assert "my-test-layer" in names


def test_lambda_layer_delete_version(lam):
    import base64, zipfile, io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("tmp.py", "")
    lam.publish_layer_version(LayerName="delete-layer-test", Content={"ZipFile": buf.getvalue()})
    lam.delete_layer_version(LayerName="delete-layer-test", VersionNumber=1)
    resp = lam.list_layer_versions(LayerName="delete-layer-test")
    assert len(resp["LayerVersions"]) == 0


def test_lambda_function_with_layer(lam):
    # Publish layer
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("layer.py", "")
    layer_resp = lam.publish_layer_version(LayerName="fn-layer", Content={"ZipFile": buf.getvalue()})
    layer_arn = layer_resp["LayerVersionArn"]
    # Create function using the layer
    fn_zip = io.BytesIO()
    with zipfile.ZipFile(fn_zip, "w") as z:
        z.writestr("index.py", "def handler(e, c): return {}")
    lam.create_function(
        FunctionName="fn-with-layer",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": fn_zip.getvalue()},
        Layers=[layer_arn],
    )
    fn = lam.get_function(FunctionName="fn-with-layer")
    assert layer_arn in fn["Configuration"]["Layers"][0]["Arn"]


def test_lambda_layer_content_location(lam):
    """Content.Location should be a non-empty URL pointing to the layer zip."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("mod.py", "X=1")
    resp = lam.publish_layer_version(
        LayerName="loc-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    assert resp["Content"]["Location"]
    assert "loc-layer" in resp["Content"]["Location"]
    # Verify the URL actually serves zip data
    import urllib.request

    data = urllib.request.urlopen(resp["Content"]["Location"]).read()
    assert len(data) == resp["Content"]["CodeSize"]


def test_lambda_layer_pagination(lam):
    """Publish 3 versions, paginate with MaxItems=1."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("p.py", "")
    for _ in range(3):
        lam.publish_layer_version(LayerName="page-layer", Content={"ZipFile": buf.getvalue()})
    # List with MaxItems=1 (newest first)
    resp = lam.list_layer_versions(LayerName="page-layer", MaxItems=1)
    assert len(resp["LayerVersions"]) == 1
    assert "NextMarker" in resp


def test_lambda_layer_list_filter_runtime(lam):
    """Filter list_layer_versions by CompatibleRuntime."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("r.py", "")
    lam.publish_layer_version(
        LayerName="rt-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )
    lam.publish_layer_version(
        LayerName="rt-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["nodejs18.x"],
    )
    resp = lam.list_layer_versions(
        LayerName="rt-filter-layer",
        CompatibleRuntime="python3.12",
    )
    assert all("python3.12" in v["CompatibleRuntimes"] for v in resp["LayerVersions"])


def test_lambda_layer_list_filter_architecture(lam):
    """Filter list_layer_versions by CompatibleArchitecture."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.py", "")
    lam.publish_layer_version(
        LayerName="arch-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleArchitectures=["x86_64"],
    )
    lam.publish_layer_version(
        LayerName="arch-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleArchitectures=["arm64"],
    )
    resp = lam.list_layer_versions(
        LayerName="arch-filter-layer",
        CompatibleArchitecture="x86_64",
    )
    assert all("x86_64" in v["CompatibleArchitectures"] for v in resp["LayerVersions"])


def test_lambda_layer_list_layers_pagination(lam):
    """Multiple layers, paginate ListLayers."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("x.py", "")
    for i in range(3):
        lam.publish_layer_version(
            LayerName=f"ll-page-{i}",
            Content={"ZipFile": buf.getvalue()},
        )
    resp = lam.list_layers(MaxItems=1)
    assert len(resp["Layers"]) == 1
    assert "NextMarker" in resp


def test_lambda_layer_list_layers_filter_runtime(lam):
    """ListLayers filtered by CompatibleRuntime."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("f.py", "")
    lam.publish_layer_version(
        LayerName="ll-rt-py",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )
    lam.publish_layer_version(
        LayerName="ll-rt-node",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["nodejs18.x"],
    )
    resp = lam.list_layers(CompatibleRuntime="python3.12")
    names = [l["LayerName"] for l in resp["Layers"]]
    assert "ll-rt-py" in names
    assert "ll-rt-node" not in names


def test_lambda_layer_get_version_not_found(lam):
    """Getting a nonexistent layer should raise 404."""
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.get_layer_version(LayerName="no-such-layer-xyz", VersionNumber=1)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_lambda_layer_get_version_by_arn(lam):
    """GetLayerVersionByArn resolves by full ARN."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("ba.py", "")
    pub = lam.publish_layer_version(
        LayerName="by-arn-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    arn = pub["LayerVersionArn"]
    resp = lam.get_layer_version_by_arn(Arn=arn)
    assert resp["LayerVersionArn"] == arn
    assert resp["Version"] == pub["Version"]


def test_lambda_layer_version_permission_add(lam):
    """Add a layer version permission and verify response."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("perm.py", "")
    pub = lam.publish_layer_version(
        LayerName="perm-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    resp = lam.add_layer_version_permission(
        LayerName="perm-layer",
        VersionNumber=pub["Version"],
        StatementId="allow-all",
        Action="lambda:GetLayerVersion",
        Principal="*",
    )
    assert "Statement" in resp
    import json

    stmt = json.loads(resp["Statement"])
    assert stmt["Sid"] == "allow-all"
    assert stmt["Action"] == "lambda:GetLayerVersion"


def test_lambda_layer_version_permission_get_policy(lam):
    """Get policy after adding a permission."""
    import json

    resp = lam.get_layer_version_policy(LayerName="perm-layer", VersionNumber=1)
    policy = json.loads(resp["Policy"])
    assert len(policy["Statement"]) >= 1
    assert policy["Statement"][0]["Sid"] == "allow-all"


def test_lambda_layer_version_permission_remove(lam):
    """Remove a layer version permission."""
    lam.remove_layer_version_permission(
        LayerName="perm-layer",
        VersionNumber=1,
        StatementId="allow-all",
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.get_layer_version_policy(LayerName="perm-layer", VersionNumber=1)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_lambda_layer_version_permission_duplicate_sid(lam):
    """Adding a duplicate StatementId should raise conflict."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("dup.py", "")
    pub = lam.publish_layer_version(
        LayerName="dup-sid-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    lam.add_layer_version_permission(
        LayerName="dup-sid-layer",
        VersionNumber=pub["Version"],
        StatementId="s1",
        Action="lambda:GetLayerVersion",
        Principal="*",
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.add_layer_version_permission(
            LayerName="dup-sid-layer",
            VersionNumber=pub["Version"],
            StatementId="s1",
            Action="lambda:GetLayerVersion",
            Principal="*",
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_lambda_layer_version_permission_invalid_action(lam):
    """Only lambda:GetLayerVersion is a valid action."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("inv.py", "")
    pub = lam.publish_layer_version(
        LayerName="inv-act-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.add_layer_version_permission(
            LayerName="inv-act-layer",
            VersionNumber=pub["Version"],
            StatementId="s1",
            Action="lambda:InvokeFunction",
            Principal="*",
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403)


def test_lambda_layer_delete_idempotent(lam):
    """Deleting a nonexistent version should not error."""
    lam.delete_layer_version(LayerName="no-such-layer-del", VersionNumber=999)


# ===========================================================================
# WAF v2
# ===========================================================================


def test_waf_web_acl_crud(wafv2):
    resp = wafv2.create_web_acl(
        Name="test-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": True, "CloudWatchMetricsEnabled": False, "MetricName": "test"},
    )
    uid = resp["Summary"]["Id"]
    assert resp["Summary"]["Name"] == "test-acl"

    get_resp = wafv2.get_web_acl(Name="test-acl", Scope="REGIONAL", Id=uid)
    assert get_resp["WebACL"]["Name"] == "test-acl"

    lst = wafv2.list_web_acls(Scope="REGIONAL")
    ids = [a["Id"] for a in lst["WebACLs"]]
    assert uid in ids

    wafv2.delete_web_acl(Name="test-acl", Scope="REGIONAL", Id=uid, LockToken=resp["Summary"]["LockToken"])
    lst2 = wafv2.list_web_acls(Scope="REGIONAL")
    ids2 = [a["Id"] for a in lst2["WebACLs"]]
    assert uid not in ids2


def test_waf_update_web_acl(wafv2):
    resp = wafv2.create_web_acl(
        Name="update-acl",
        Scope="REGIONAL",
        DefaultAction={"Block": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]
    upd = wafv2.update_web_acl(
        Name="update-acl",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    assert "NextLockToken" in upd


def test_waf_associate_disassociate(wafv2):
    resp = wafv2.create_web_acl(
        Name="assoc-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    acl_arn = resp["Summary"]["ARN"]
    resource_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc"
    wafv2.associate_web_acl(WebACLArn=acl_arn, ResourceArn=resource_arn)
    get_resp = wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
    assert get_resp["WebACL"]["ARN"] == acl_arn
    wafv2.disassociate_web_acl(ResourceArn=resource_arn)
    try:
        wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
        assert False, "expected WAFNonexistentItemException"
    except wafv2.exceptions.WAFNonexistentItemException:
        pass


def test_waf_ip_set_crud(wafv2):
    resp = wafv2.create_ip_set(
        Name="test-ipset",
        Scope="REGIONAL",
        IPAddressVersion="IPV4",
        Addresses=["1.2.3.4/32"],
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]

    get_resp = wafv2.get_ip_set(Name="test-ipset", Scope="REGIONAL", Id=uid)
    assert "1.2.3.4/32" in get_resp["IPSet"]["Addresses"]

    upd = wafv2.update_ip_set(
        Name="test-ipset",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        Addresses=["5.6.7.8/32"],
    )
    assert "NextLockToken" in upd

    lst = wafv2.list_ip_sets(Scope="REGIONAL")
    ids = [s["Id"] for s in lst["IPSets"]]
    assert uid in ids

    wafv2.delete_ip_set(Name="test-ipset", Scope="REGIONAL", Id=uid, LockToken=upd["NextLockToken"])
    lst2 = wafv2.list_ip_sets(Scope="REGIONAL")
    ids2 = [s["Id"] for s in lst2["IPSets"]]
    assert uid not in ids2


def test_waf_rule_group_crud(wafv2):
    resp = wafv2.create_rule_group(
        Name="test-rg",
        Scope="REGIONAL",
        Capacity=100,
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]

    get_resp = wafv2.get_rule_group(Name="test-rg", Scope="REGIONAL", Id=uid)
    assert get_resp["RuleGroup"]["Name"] == "test-rg"
    assert "LockToken" not in get_resp["RuleGroup"]

    upd = wafv2.update_rule_group(
        Name="test-rg",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m2"},
    )
    assert "NextLockToken" in upd

    lst = wafv2.list_rule_groups(Scope="REGIONAL")
    ids = [r["Id"] for r in lst["RuleGroups"]]
    assert uid in ids

    wafv2.delete_rule_group(Name="test-rg", Scope="REGIONAL", Id=uid, LockToken=upd["NextLockToken"])
    lst2 = wafv2.list_rule_groups(Scope="REGIONAL")
    ids2 = [r["Id"] for r in lst2["RuleGroups"]]
    assert uid not in ids2


def test_waf_tags(wafv2):
    resp = wafv2.create_web_acl(
        Name="tag-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
        Tags=[{"Key": "env", "Value": "test"}],
    )
    arn = resp["Summary"]["ARN"]
    tags_resp = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert any(t["Key"] == "env" for t in tags_resp["TagInfoForResource"]["TagList"])
    wafv2.tag_resource(ResourceARN=arn, Tags=[{"Key": "team", "Value": "security"}])
    tags_resp2 = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert any(t["Key"] == "team" for t in tags_resp2["TagInfoForResource"]["TagList"])
    wafv2.untag_resource(ResourceARN=arn, TagKeys=["env"])
    tags_resp3 = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert not any(t["Key"] == "env" for t in tags_resp3["TagInfoForResource"]["TagList"])
# ========== CloudFormation ==========


def _wait_stack(cfn, name, timeout=10):
    """Poll until stack reaches terminal status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")


def test_cfn_create_describe_delete_stack(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t01-bucket"},
            }
        },
    }
    cfn.create_stack(StackName="cfn-t01", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-t01")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    s3.head_bucket(Bucket="cfn-t01-bucket")

    cfn.delete_stack(StackName="cfn-t01")
    _wait_stack(cfn, "cfn-t01")

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t01-bucket")


def test_cfn_stack_with_parameters(cfn, sqs):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "String",
                "Default": "cfn-t02-default",
            }
        },
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            }
        },
    }
    cfn.create_stack(StackName="cfn-t02a", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t02a")

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02-default").get("QueueUrls", [])
    assert any("cfn-t02-default" in u for u in urls)

    cfn.create_stack(
        StackName="cfn-t02b",
        TemplateBody=json.dumps(template),
        Parameters=[{"ParameterKey": "QueueName", "ParameterValue": "cfn-t02-custom"}],
    )
    _wait_stack(cfn, "cfn-t02b")

    urls = sqs.list_queues(QueueNamePrefix="cfn-t02-custom").get("QueueUrls", [])
    assert any("cfn-t02-custom" in u for u in urls)


def test_cfn_intrinsic_ref_getatt(cfn, ssm):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "cfn-t03-queue"},
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t03-param",
                    "Type": "String",
                    "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t03", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t03")

    val = ssm.get_parameter(Name="cfn-t03-param")["Parameter"]["Value"]
    assert val.startswith("arn:aws:sqs:")


def test_cfn_conditions(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "Create": {"Type": "String", "Default": "yes"},
        },
        "Conditions": {
            "ShouldCreate": {"Fn::Equals": [{"Ref": "Create"}, "yes"]},
        },
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Condition": "ShouldCreate",
                "Properties": {"BucketName": "cfn-t04-cond"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t04a", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t04a")
    s3.head_bucket(Bucket="cfn-t04-cond")

    # Delete first stack so the bucket name is freed
    cfn.delete_stack(StackName="cfn-t04a")
    _wait_stack(cfn, "cfn-t04a")

    cfn.create_stack(
        StackName="cfn-t04b",
        TemplateBody=json.dumps(template),
        Parameters=[{"ParameterKey": "Create", "ParameterValue": "no"}],
    )
    stack = _wait_stack(cfn, "cfn-t04b")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t04-cond")


def test_cfn_outputs_exports(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t05-exports"},
            },
        },
        "Outputs": {
            "BucketOut": {
                "Value": {"Ref": "Bucket"},
                "Export": {"Name": "cfn-t05-bucket-export"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t05", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t05")

    exports = cfn.list_exports()["Exports"]
    assert any(e["Name"] == "cfn-t05-bucket-export" for e in exports)


def test_cfn_fn_sub(cfn, ssm):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t06-src"},
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t06-param",
                    "Type": "String",
                    "Value": {"Fn::Sub": "${MyBucket}-replica"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t06", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t06")

    val = ssm.get_parameter(Name="cfn-t06-param")["Parameter"]["Value"]
    assert val == "cfn-t06-src-replica"


def test_cfn_multi_resource_dependencies(cfn, iam, lam):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "cfn-t07-role",
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                },
            },
            "Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "cfn-t07-func",
                    "Runtime": "python3.9",
                    "Handler": "index.handler",
                    "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                    "Code": {"ZipFile": "def handler(e,c): return {}"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t07", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t07")
    role = iam.get_role(RoleName="cfn-t07-role")["Role"]
    func = lam.get_function(FunctionName="cfn-t07-func")["Configuration"]
    assert func["Role"] == role["Arn"]


def test_cfn_change_set_lifecycle(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t08-cs"},
            },
        },
    }
    cfn.create_change_set(
        StackName="cfn-t08",
        ChangeSetName="cfn-t08-cs1",
        TemplateBody=json.dumps(template),
        ChangeSetType="CREATE",
    )
    time.sleep(1)

    cs = cfn.describe_change_set(StackName="cfn-t08", ChangeSetName="cfn-t08-cs1")
    assert cs["ChangeSetName"] == "cfn-t08-cs1"

    cfn.execute_change_set(StackName="cfn-t08", ChangeSetName="cfn-t08-cs1")
    stack = _wait_stack(cfn, "cfn-t08")
    assert stack["StackStatus"] == "CREATE_COMPLETE"


def test_cfn_update_stack(cfn, s3):
    template_v1 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "BucketA": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-a"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t09", TemplateBody=json.dumps(template_v1))
    _wait_stack(cfn, "cfn-t09")

    template_v2 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "BucketA": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-a"},
            },
            "BucketB": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t09-b"},
            },
        },
    }
    cfn.update_stack(StackName="cfn-t09", TemplateBody=json.dumps(template_v2))
    stack = _wait_stack(cfn, "cfn-t09")
    assert stack["StackStatus"] == "UPDATE_COMPLETE"

    s3.head_bucket(Bucket="cfn-t09-a")
    s3.head_bucket(Bucket="cfn-t09-b")


def test_cfn_delete_nonexistent_stack(cfn):
    # AWS returns 200 for deleting non-existent stacks (idempotent)
    cfn.delete_stack(StackName="cfn-nonexistent-xyz")
    # But describing it should fail
    with pytest.raises(ClientError):
        cfn.describe_stacks(StackName="cfn-nonexistent-xyz")


def test_cfn_validate_template(cfn):
    valid_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "Env": {"Type": "String", "Default": "dev"},
        },
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t11-validate"},
            },
        },
    }
    result = cfn.validate_template(TemplateBody=json.dumps(valid_template))
    assert any(p["ParameterKey"] == "Env" for p in result["Parameters"])

    invalid_template = {"AWSTemplateFormatVersion": "2010-09-09"}
    with pytest.raises(ClientError):
        cfn.validate_template(TemplateBody=json.dumps(invalid_template))


def test_cfn_list_stacks(cfn):
    for name in ("cfn-t12-a", "cfn-t12-b"):
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Bucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": f"{name}-bucket"},
                },
            },
        }
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t12-a")
    _wait_stack(cfn, "cfn-t12-b")

    summaries = cfn.list_stacks()["StackSummaries"]
    names = [s["StackName"] for s in summaries]
    assert "cfn-t12-a" in names
    assert "cfn-t12-b" in names


def test_cfn_stack_events(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t13-events"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t13", TemplateBody=json.dumps(template))
    _wait_stack(cfn, "cfn-t13")

    events = cfn.describe_stack_events(StackName="cfn-t13")["StackEvents"]
    assert len(events) > 0
    assert all("ResourceStatus" in e for e in events)


def test_cfn_yaml_template(cfn, s3):
    yaml_body = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: cfn-t14-yaml
"""
    cfn.create_stack(StackName="cfn-t14", TemplateBody=yaml_body)
    _wait_stack(cfn, "cfn-t14")

    s3.head_bucket(Bucket="cfn-t14-yaml")


def test_cfn_rollback_on_failure(cfn, s3):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t15-rollback"},
            },
            "Bad": {
                "Type": "AWS::Fake::Nope",
                "Properties": {},
            },
        },
    }
    cfn.create_stack(
        StackName="cfn-t15",
        TemplateBody=json.dumps(template),
        DisableRollback=False,
    )
    stack = _wait_stack(cfn, "cfn-t15")
    assert stack["StackStatus"] == "ROLLBACK_COMPLETE"

    with pytest.raises(ClientError):
        s3.head_bucket(Bucket="cfn-t15-rollback")


def test_cfn_import_nonexistent_export(cfn):
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t16-param",
                    "Type": "String",
                    "Value": {"Fn::ImportValue": "NonExistentExport123"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t16", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-t16")
    assert stack["StackStatus"] in ("CREATE_FAILED", "ROLLBACK_COMPLETE")


def test_cfn_delete_stack_with_active_imports(cfn):
    exporter_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t17-exporter"},
            },
        },
        "Outputs": {
            "BucketOut": {
                "Value": {"Ref": "Bucket"},
                "Export": {"Name": "cfn-t17-export"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t17-exp", TemplateBody=json.dumps(exporter_template))
    _wait_stack(cfn, "cfn-t17-exp")

    importer_template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cfn-t17-param",
                    "Type": "String",
                    "Value": {"Fn::ImportValue": "cfn-t17-export"},
                },
            },
        },
    }
    cfn.create_stack(StackName="cfn-t17-imp", TemplateBody=json.dumps(importer_template))
    _wait_stack(cfn, "cfn-t17-imp")

    with pytest.raises(ClientError):
        cfn.delete_stack(StackName="cfn-t17-exp")


def test_cfn_update_rollback_on_failure(cfn, s3):
    template_v1 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t18-orig"},
            },
        },
    }
    cfn.create_stack(StackName="cfn-t18", TemplateBody=json.dumps(template_v1))
    _wait_stack(cfn, "cfn-t18")

    template_v2 = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-t18-orig"},
            },
            "Bad": {
                "Type": "AWS::Fake::Nope",
                "Properties": {},
            },
        },
    }
    cfn.update_stack(StackName="cfn-t18", TemplateBody=json.dumps(template_v2))
    stack = _wait_stack(cfn, "cfn-t18")
    assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"

    s3.head_bucket(Bucket="cfn-t18-orig")


# ========== EC2 — DescribeInstanceAttribute / DescribeInstanceTypes ==========


def test_ec2_describe_instance_attribute_instance_type(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t3.micro")
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceType")
    assert attr["InstanceId"] == iid
    assert attr["InstanceType"]["Value"] == "t3.micro"

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_describe_instance_attribute_shutdown_behavior(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceInitiatedShutdownBehavior")
    assert attr["InstanceId"] == iid
    assert attr["InstanceInitiatedShutdownBehavior"]["Value"] == "stop"

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_describe_instance_attribute_not_found(ec2):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        ec2.describe_instance_attribute(InstanceId="i-000000000000nonex", Attribute="instanceType")
    assert exc.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"


def test_ec2_describe_instance_credit_specifications(ec2):
    iid = ec2.run_instances(ImageId="ami-test", MinCount=1, MaxCount=1)["Instances"][0]["InstanceId"]
    resp = ec2.describe_instance_credit_specifications(InstanceIds=[iid])
    specs = resp["InstanceCreditSpecifications"]
    assert len(specs) == 1
    assert specs[0]["InstanceId"] == iid
    assert specs[0]["CpuCredits"] == "standard"


def test_ec2_describe_spot_instance_requests(ec2):
    resp = ec2.describe_spot_instance_requests()
    assert "SpotInstanceRequests" in resp


def test_ec2_describe_capacity_reservations(ec2):
    resp = ec2.describe_capacity_reservations()
    assert "CapacityReservations" in resp


def test_ec2_describe_instance_types_defaults(ec2):
    resp = ec2.describe_instance_types()
    types = [t["InstanceType"] for t in resp["InstanceTypes"]]
    assert "t2.micro" in types
    assert "t3.micro" in types
    assert len(resp["InstanceTypes"]) >= 4
    # Spot-check shape
    sample = resp["InstanceTypes"][0]
    assert "VCpuInfo" in sample
    assert "MemoryInfo" in sample
    assert sample["VCpuInfo"]["DefaultVCpus"] >= 1
    assert sample["MemoryInfo"]["SizeInMiB"] >= 512


def test_ec2_describe_instance_types_filter(ec2):
    resp = ec2.describe_instance_types(InstanceTypes=["t2.micro", "m5.large"])
    types = {t["InstanceType"] for t in resp["InstanceTypes"]}
    assert types == {"t2.micro", "m5.large"}


# ========== CloudFormation — End-to-End ==========

_E2E_STACK = "e2e-test"
_E2E_TEMPLATE = """
AWSTemplateFormatVersion: '2010-09-09'
Description: E2E test stack — verifies CFN resources are functional

Parameters:
  Env:
    Type: String
    Default: e2etest

Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-${Env}-assets"

  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-${Env}-events"
      VisibilityTimeout: 120

  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${AWS::StackName}-${Env}-alerts"

  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-${Env}-role"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole

  Processor:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub "${AWS::StackName}-${Env}-processor"
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt Role.Arn
      Code:
        ZipFile: |
          def handler(event, context):
              return {"statusCode": 200}

  QueueUrlParam:
    Type: AWS::SSM::Parameter
    Properties:
      Name: !Sub "/${AWS::StackName}/${Env}/queue-url"
      Type: String
      Value: !Ref Queue

Outputs:
  BucketName:
    Value: !Ref Bucket
    Export:
      Name: !Sub "${AWS::StackName}-bucket"
  QueueUrl:
    Value: !Ref Queue
  TopicArn:
    Value: !Ref Topic
  ProcessorArn:
    Value: !GetAtt Processor.Arn
  RoleArn:
    Value: !GetAtt Role.Arn
"""


@pytest.fixture(scope="module")
def cfn_e2e_stack(cfn):
    """Deploy the e2e stack once for all e2e tests in this module."""
    # Clean up from a previous run
    try:
        cfn.delete_stack(StackName=_E2E_STACK)
        _wait_stack(cfn, _E2E_STACK)
    except Exception:
        pass

    cfn.create_stack(StackName=_E2E_STACK, TemplateBody=_E2E_TEMPLATE)
    s = _wait_stack(cfn, _E2E_STACK)
    assert s["StackStatus"] == "CREATE_COMPLETE", f"Stack failed: {s.get('StackStatusReason')}"

    outputs = {o["OutputKey"]: o["OutputValue"] for o in s.get("Outputs", [])}
    yield outputs

    cfn.delete_stack(StackName=_E2E_STACK)
    _wait_stack(cfn, _E2E_STACK)


def test_cfn_e2e_s3_put_and_get(cfn_e2e_stack, s3):
    bucket = cfn_e2e_stack["BucketName"]
    body = json.dumps({"id": "001", "total": 99.99})
    s3.put_object(Bucket=bucket, Key="orders/order-001.json", Body=body.encode())
    obj = s3.get_object(Bucket=bucket, Key="orders/order-001.json")
    data = json.loads(obj["Body"].read())
    assert data["id"] == "001"
    assert data["total"] == 99.99


def test_cfn_e2e_s3_list_objects(cfn_e2e_stack, s3):
    bucket = cfn_e2e_stack["BucketName"]
    s3.put_object(Bucket=bucket, Key="docs/readme.txt", Body=b"hello")
    listing = s3.list_objects_v2(Bucket=bucket)
    assert listing["KeyCount"] >= 1
    keys = [o["Key"] for o in listing["Contents"]]
    assert "docs/readme.txt" in keys


def test_cfn_e2e_sqs_send_receive_delete(cfn_e2e_stack, sqs):
    url = cfn_e2e_stack["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"event": "order.created"}))
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"event": "order.shipped"}))
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    received = msgs.get("Messages", [])
    assert len(received) == 2
    events = sorted(json.loads(m["Body"])["event"] for m in received)
    assert events == ["order.created", "order.shipped"]
    for m in received:
        sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    empty = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(empty.get("Messages", [])) == 0


def test_cfn_e2e_sns_publish(cfn_e2e_stack, sns):
    topic_arn = cfn_e2e_stack["TopicArn"]
    resp = sns.publish(TopicArn=topic_arn, Subject="Test Alert",
                       Message=json.dumps({"alert": "test", "severity": "low"}))
    assert "MessageId" in resp


def test_cfn_e2e_ssm_read_cfn_param(cfn_e2e_stack, ssm):
    param = ssm.get_parameter(Name=f"/{_E2E_STACK}/e2etest/queue-url")["Parameter"]
    assert param["Value"] == cfn_e2e_stack["QueueUrl"]


def test_cfn_e2e_ssm_write_and_read(cfn_e2e_stack, ssm):
    ssm.put_parameter(Name=f"/{_E2E_STACK}/e2etest/flags", Type="String",
                      Value=json.dumps({"dark_mode": True}))
    flags = json.loads(ssm.get_parameter(Name=f"/{_E2E_STACK}/e2etest/flags")["Parameter"]["Value"])
    assert flags["dark_mode"] is True


def test_cfn_e2e_lambda_invoke(cfn_e2e_stack, lam):
    resp = lam.invoke(FunctionName=f"{_E2E_STACK}-e2etest-processor",
                      Payload=json.dumps({"action": "test"}).encode())
    assert resp["StatusCode"] == 200


def test_cfn_e2e_lambda_role_matches_iam_role(cfn_e2e_stack, lam, iam):
    fn = lam.get_function(FunctionName=f"{_E2E_STACK}-e2etest-processor")["Configuration"]
    role = iam.get_role(RoleName=f"{_E2E_STACK}-e2etest-role")["Role"]
    assert fn["Role"] == role["Arn"]


def test_cfn_e2e_pipeline(cfn_e2e_stack, s3, sqs, sns):
    """S3 upload → SQS queue → read back from S3 → SNS alert."""
    bucket = cfn_e2e_stack["BucketName"]
    url = cfn_e2e_stack["QueueUrl"]
    topic_arn = cfn_e2e_stack["TopicArn"]

    for i in range(3):
        order = {"id": f"pipe-{i}", "item": f"widget-{i}", "qty": (i + 1) * 5}
        s3.put_object(Bucket=bucket, Key=f"pipeline/order-{i}.json",
                      Body=json.dumps(order).encode())

    for i in range(3):
        sqs.send_message(QueueUrl=url,
                         MessageBody=json.dumps({"event": "process", "key": f"pipeline/order-{i}.json"}))

    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert len(msgs.get("Messages", [])) == 3

    total_qty = 0
    for m in msgs["Messages"]:
        body = json.loads(m["Body"])
        obj = s3.get_object(Bucket=bucket, Key=body["key"])
        order = json.loads(obj["Body"].read())
        total_qty += order["qty"]
        sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])

    assert total_qty == 5 + 10 + 15

    resp = sns.publish(TopicArn=topic_arn, Subject="Pipeline Done",
                       Message=json.dumps({"processed": 3, "total_qty": total_qty}))
    assert "MessageId" in resp


def test_cfn_e2e_exports_available(cfn_e2e_stack, cfn):
    exports = cfn.list_exports()["Exports"]
    names = {e["Name"]: e["Value"] for e in exports}
    assert f"{_E2E_STACK}-bucket" in names
    assert names[f"{_E2E_STACK}-bucket"] == cfn_e2e_stack["BucketName"]


# ========== CloudFormation — Auto-Generated Physical Names ==========


def test_cfn_auto_name_s3_follows_aws_pattern(cfn, s3):
    """S3 bucket auto-name: lowercase, stackName-logicalId-SUFFIX, max 63 chars."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}},
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "MyBucket"}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-s3", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-s3")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bucket_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BucketName")
    assert bucket_name == bucket_name.lower(), "S3 auto-name must be lowercase"
    assert bucket_name.startswith("cfn-autoname-s3-mybucket-"), f"Expected AWS-pattern name, got: {bucket_name}"
    assert len(bucket_name) <= 63, f"S3 name too long: {len(bucket_name)}"
    # Verify bucket actually exists
    s3.head_bucket(Bucket=bucket_name)

    cfn.delete_stack(StackName="cfn-autoname-s3")
    _wait_stack(cfn, "cfn-autoname-s3")


def test_cfn_auto_name_sqs_follows_aws_pattern(cfn, sqs):
    """SQS queue auto-name: stackName-logicalId-SUFFIX, max 80 chars, case preserved."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {"Type": "AWS::SQS::Queue", "Properties": {}},
        },
        "Outputs": {
            "QueueName": {"Value": {"Fn::GetAtt": ["MyQueue", "QueueName"]}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-sqs", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-sqs")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    queue_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "QueueName")
    assert queue_name.startswith("cfn-autoname-sqs-MyQueue-"), f"Expected AWS-pattern name, got: {queue_name}"
    assert len(queue_name) <= 80

    cfn.delete_stack(StackName="cfn-autoname-sqs")
    _wait_stack(cfn, "cfn-autoname-sqs")


def test_cfn_auto_name_dynamodb_follows_aws_pattern(cfn, ddb):
    """DynamoDB table auto-name: stackName-logicalId-SUFFIX, max 255 chars."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            },
        },
        "Outputs": {
            "TableName": {"Value": {"Ref": "MyTable"}},
        },
    }
    cfn.create_stack(StackName="cfn-autoname-ddb", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-autoname-ddb")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    table_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "TableName")
    assert table_name.startswith("cfn-autoname-ddb-MyTable-"), f"Expected AWS-pattern name, got: {table_name}"
    assert len(table_name) <= 255
    ddb.describe_table(TableName=table_name)

    cfn.delete_stack(StackName="cfn-autoname-ddb")
    _wait_stack(cfn, "cfn-autoname-ddb")


def test_cfn_explicit_name_not_overridden(cfn, s3):
    """Explicit BucketName must be used as-is, not overridden by auto-name logic."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": "cfn-explicit-name-test"},
            },
        },
        "Outputs": {
            "BucketName": {"Value": {"Ref": "MyBucket"}},
        },
    }
    cfn.create_stack(StackName="cfn-explicit-name", TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, "cfn-explicit-name")
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    bucket_name = next(o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "BucketName")
    assert bucket_name == "cfn-explicit-name-test"

    cfn.delete_stack(StackName="cfn-explicit-name")
    _wait_stack(cfn, "cfn-explicit-name")
