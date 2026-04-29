import json
import time
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

from conftest import make_client, patch_endpoint_dns, ENDPOINT_HOST

def test_s3_create_bucket(s3):
    s3.create_bucket(Bucket="intg-s3-create")
    buckets = s3.list_buckets()["Buckets"]
    assert any(b["Name"] == "intg-s3-create" for b in buckets)

def test_s3_list_buckets_returns_arn_and_region(s3):
    """ListBuckets should return BucketArn and BucketRegion for each bucket."""
    bkt = "intg-s3-arn-test"
    s3.create_bucket(Bucket=bkt)
    buckets = s3.list_buckets()["Buckets"]
    match = [b for b in buckets if b["Name"] == bkt]
    assert len(match) == 1
    b = match[0]
    assert b["BucketArn"] == f"arn:aws:s3:::{bkt}"
    assert "BucketRegion" in b
    assert len(b["BucketRegion"]) > 0


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
    import json as _json
    import urllib.parse
    import urllib.request
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

def test_s3_put_zero_byte_chunked(s3):
    """Zero-byte PutObject via AWS chunked encoding must store empty body and return correct ETag."""
    import hashlib
    import urllib.request
    bucket = "intg-s3-zero-byte"
    s3.create_bucket(Bucket=bucket)

    fake_sig = b"abc123"
    chunked = b"0;chunk-signature=" + fake_sig + b"\r\n\r\n"
    endpoint = "http://localhost:4566/" + bucket + "/empty.bin"
    req = urllib.request.Request(endpoint, data=chunked, method="PUT", headers={
        "x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
    })
    with urllib.request.urlopen(req) as r:
        assert r.status == 200
        etag = r.headers.get("ETag", "").strip('"')
    assert etag == hashlib.md5(b"").hexdigest()

    resp = s3.get_object(Bucket=bucket, Key="empty.bin")
    assert resp["Body"].read() == b""
    assert resp["ContentLength"] == 0

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

def test_s3_control_tag_resource_post_xml_stores_tags(s3):
    """Regression for #447: S3Control TagResource must accept POST with an XML
    TagResourceRequest body (what AWS SDK Go v2 / terraform-aws-provider v6+
    send) and persist the tags. Previously the handler only had GET/PUT/DELETE
    and parsed bodies as JSON, silently dropping all tags.
    """
    import urllib.parse
    import urllib.request
    bkt = "intg-s3control-tag-post"
    s3.create_bucket(Bucket=bkt)
    arn = urllib.parse.quote(f"arn:aws:s3:::{bkt}", safe="")
    xml_body = (
        '<TagResourceRequest xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">'
        "<Tags>"
        "<Tag><Key>demo:environment</Key><Value>repro</Value></Tag>"
        "<Tag><Key>demo:owner</Key><Value>ministack</Value></Tag>"
        "</Tags>"
        "</TagResourceRequest>"
    ).encode()
    req = urllib.request.Request(
        f"http://localhost:4566/v20180820/tags/{arn}",
        method="POST",
        data=xml_body,
        headers={
            "x-amz-account-id": "000000000000",
            "Content-Type": "application/xml",
        },
    )
    with urllib.request.urlopen(req) as r:
        assert r.status in (200, 204)

    # Visible via the regular S3 API (same _bucket_tags dict)
    got = s3.get_bucket_tagging(Bucket=bkt)
    tags = {t["Key"]: t["Value"] for t in got["TagSet"]}
    assert tags["demo:environment"] == "repro"
    assert tags["demo:owner"] == "ministack"

    # And via S3 Control GET /v20180820/tags/{arn}
    get_req = urllib.request.Request(
        f"http://localhost:4566/v20180820/tags/{arn}",
        method="GET",
        headers={"x-amz-account-id": "000000000000"},
    )
    with urllib.request.urlopen(get_req) as r:
        body = r.read().decode()
    assert "demo:environment" in body
    assert "repro" in body


def test_s3_control_list_tags_via_s3_control_host(s3):
    """S3 Control requests via s3-control.localhost host must not be intercepted by S3 vhost."""
    import urllib.parse
    import urllib.request
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

class TestS3VhostGetPutObject:
    """
    Ensure vhost style and path style requests work correctly.

    Test with both a simple bucket name and a max length one with dot and hyphen
    """

    BKT = "intg-s3-vhost"
    # max length and dotted with hyphen
    BKT_DOTTED_BASE = "intg-s3.vhost-nested.bucket"
    BKT_DOTTED = BKT_DOTTED_BASE + "x" * (63 - len(BKT_DOTTED_BASE))

    @pytest.fixture(autouse=True)
    def _init_buckets(self, s3):
        self.s3 = s3
        print(s3)
        assert len(self.BKT_DOTTED) == 63
        self.s3_path = make_client("s3", additional_config_kwargs=dict(s3={"addressing_style": "path"}))
        self.s3_virtual = make_client("s3", additional_config_kwargs=dict(s3={"addressing_style": "virtual"}))

        s3.create_bucket(Bucket=self.BKT)
        s3.put_object(Bucket=self.BKT, Key="vhost-test.txt", Body=b"vhost content")

        s3.create_bucket(Bucket=self.BKT_DOTTED)
        s3.put_object(Bucket=self.BKT_DOTTED, Key="vhost-test.txt", Body=b"vhost content")

    def test_path_style_get(self):
        resp = self.s3_path.get_object(Bucket=self.BKT, Key="vhost-test.txt")
        assert resp["Body"].read() == b"vhost content"

    def test_virtual_hosted_style_get(self):
        with patch_endpoint_dns():
            resp = self.s3_virtual.get_object(Bucket=self.BKT, Key="vhost-test.txt")
        assert resp["Body"].read() == b"vhost content"

    @pytest.mark.skip(reason="Dotted Nested Bucket is not supported yet")
    def test_dotted_bucket_virtual_hosted_style_get(self):
        with patch_endpoint_dns():
            resp = self.s3_virtual.get_object(Bucket=self.BKT_DOTTED, Key="vhost-test.txt")
        assert resp["Body"].read() == b"vhost content"

    def test_dotted_bucket_path_style_get(self):
        resp = self.s3_path.get_object(Bucket=self.BKT_DOTTED, Key="vhost-test.txt")
        assert resp["Body"].read() == b"vhost content"


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

    from datetime import datetime, timedelta, timezone

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
    from datetime import datetime, timedelta, timezone

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
    from datetime import datetime, timedelta, timezone

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
    from datetime import datetime, timedelta, timezone

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
    s3_msgs = [m for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
    assert len(s3_msgs) > 0
    body = json.loads(s3_msgs[0]["Body"])
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
    keys = [json.loads(m["Body"])["Records"][0]["s3"]["object"]["key"] for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
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
    s3_msgs = [m for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
    assert len(s3_msgs) > 0
    body = json.loads(s3_msgs[0]["Body"])
    assert "ObjectRemoved" in body["Records"][0]["eventName"]

def test_s3_put_notification_sends_test_event(s3, sqs):
    bkt = "s3-test-evt-bkt"
    s3.create_bucket(Bucket=bkt)
    queue_url = sqs.create_queue(QueueName="s3-test-evt-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket=bkt,
        NotificationConfiguration={
            "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}],
        },
    )
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) == 1
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Event"] == "s3:TestEvent"
    assert body["Bucket"] == bkt
    assert "Records" not in body


def test_s3_put_notification_no_test_event_for_missing_bucket(s3, sqs):
    queue_url = sqs.create_queue(QueueName="s3-test-evt-missing-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_notification_configuration(
            Bucket="no-such-bucket-xyz",
            NotificationConfiguration={
                "QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}],
            },
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchBucket"
    time.sleep(0.5)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert "Messages" not in msgs


def test_s3_eventbridge_notification(s3, sqs, eb):
    """S3 EventBridgeConfiguration sends events to EventBridge, routed to SQS via rule."""
    s3.create_bucket(Bucket="s3-eb-bkt")
    queue_url = sqs.create_queue(QueueName="s3-eb-target-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    # Enable EventBridge on bucket
    s3.put_bucket_notification_configuration(
        Bucket="s3-eb-bkt",
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )

    # Create EventBridge rule matching S3 events → SQS target
    eb.put_rule(
        Name="s3-to-sqs-rule",
        EventPattern=json.dumps({"source": ["aws.s3"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="s3-to-sqs-rule",
        Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
    )

    # Upload object — should trigger S3 → EventBridge → SQS
    s3.put_object(Bucket="s3-eb-bkt", Key="hello.txt", Body=b"world")
    time.sleep(0.5)

    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["source"] == "aws.s3"
    assert body["detail"]["bucket"]["name"] == "s3-eb-bkt"
    assert body["detail"]["object"]["key"] == "hello.txt"

def test_s3_list_object_versions(s3):
    s3.create_bucket(Bucket="s3-ver-bkt")
    s3.put_object(Bucket="s3-ver-bkt", Key="v1.txt", Body=b"v1")
    s3.put_object(Bucket="s3-ver-bkt", Key="v2.txt", Body=b"v2")
    resp = s3.list_object_versions(Bucket="s3-ver-bkt")
    versions = resp.get("Versions", [])
    assert len(versions) >= 2
    keys = [v["Key"] for v in versions]
    assert "v1.txt" in keys and "v2.txt" in keys

def test_s3_list_object_versions_multiple_puts_same_key(s3):
    """Multiple PUTs to the same key with versioning enabled should return all versions."""
    bkt = "s3-ver-multi"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    r1 = s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v1")
    r2 = s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v2")
    r3 = s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v3")

    assert r1["VersionId"] != r2["VersionId"]
    assert r2["VersionId"] != r3["VersionId"]

    resp = s3.list_object_versions(Bucket=bkt)
    versions = resp.get("Versions", [])
    assert len(versions) == 3

    version_ids = [v["VersionId"] for v in versions]
    assert r1["VersionId"] in version_ids
    assert r2["VersionId"] in version_ids
    assert r3["VersionId"] in version_ids

    latest = [v for v in versions if v["IsLatest"]]
    assert len(latest) == 1
    assert latest[0]["VersionId"] == r3["VersionId"]


def test_s3_multipart_upload_returns_version_id(s3):
    """CompleteMultipartUpload should return VersionId when versioning is enabled."""
    bkt = "s3-ver-mpu"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    mpu = s3.create_multipart_upload(Bucket=bkt, Key="big.bin")
    upload_id = mpu["UploadId"]
    part = s3.upload_part(Bucket=bkt, Key="big.bin", UploadId=upload_id, PartNumber=1, Body=b"x" * 1000)
    resp = s3.complete_multipart_upload(
        Bucket=bkt, Key="big.bin", UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
    )
    assert "VersionId" in resp, "CompleteMultipartUpload must return VersionId"
    first_vid = resp["VersionId"]

    # Second multipart to same key — different version
    mpu2 = s3.create_multipart_upload(Bucket=bkt, Key="big.bin")
    part2 = s3.upload_part(Bucket=bkt, Key="big.bin", UploadId=mpu2["UploadId"], PartNumber=1, Body=b"y" * 1000)
    resp2 = s3.complete_multipart_upload(
        Bucket=bkt, Key="big.bin", UploadId=mpu2["UploadId"],
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part2["ETag"]}]},
    )
    assert resp2["VersionId"] != first_vid

    # Both versions should appear in list_object_versions
    versions = s3.list_object_versions(Bucket=bkt).get("Versions", [])
    vids = [v["VersionId"] for v in versions]
    assert first_vid in vids
    assert resp2["VersionId"] in vids
    latest = [v for v in versions if v["IsLatest"]]
    assert len(latest) == 1
    assert latest[0]["VersionId"] == resp2["VersionId"]


def test_s3_copy_object_returns_version_id(s3):
    """CopyObject should return VersionId and track versions when versioning is enabled."""
    bkt = "s3-ver-copy"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=bkt, Key="src.txt", Body=b"original")
    resp = s3.copy_object(Bucket=bkt, Key="dst.txt", CopySource=f"{bkt}/src.txt")
    assert "VersionId" in resp, "CopyObject must return VersionId"
    first_vid = resp["VersionId"]

    # Copy again — different version
    resp2 = s3.copy_object(Bucket=bkt, Key="dst.txt", CopySource=f"{bkt}/src.txt")
    assert resp2["VersionId"] != first_vid

    versions = s3.list_object_versions(Bucket=bkt, Prefix="dst.txt").get("Versions", [])
    assert len(versions) == 2, f"Expected 2 versions for dst.txt, got {len(versions)}"
    latest = [v for v in versions if v["IsLatest"]]
    assert len(latest) == 1


def test_s3_multipart_no_version_without_versioning(s3):
    """CompleteMultipartUpload should NOT return VersionId when versioning is disabled."""
    bkt = "s3-nover-mpu"
    s3.create_bucket(Bucket=bkt)
    mpu = s3.create_multipart_upload(Bucket=bkt, Key="file.bin")
    part = s3.upload_part(Bucket=bkt, Key="file.bin", UploadId=mpu["UploadId"], PartNumber=1, Body=b"data")
    resp = s3.complete_multipart_upload(
        Bucket=bkt, Key="file.bin", UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
    )
    assert "VersionId" not in resp, "Should not return VersionId without versioning"


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

def test_s3_bucket_versioning(s3):
    s3.create_bucket(Bucket="intg-s3-versioning")
    s3.put_bucket_versioning(
        Bucket="intg-s3-versioning",
        VersioningConfiguration={"Status": "Enabled"},
    )
    resp = s3.get_bucket_versioning(Bucket="intg-s3-versioning")
    assert resp["Status"] == "Enabled"

def test_s3_put_object_returns_version_id(s3):
    s3.create_bucket(Bucket="intg-s3-ver-put")
    s3.put_bucket_versioning(
        Bucket="intg-s3-ver-put",
        VersioningConfiguration={"Status": "Enabled"},
    )
    resp = s3.put_object(Bucket="intg-s3-ver-put", Key="hello.txt", Body=b"v1")
    assert "VersionId" in resp
    assert len(resp["VersionId"]) > 0

    # Second put should get a different version
    resp2 = s3.put_object(Bucket="intg-s3-ver-put", Key="hello.txt", Body=b"v2")
    assert resp2["VersionId"] != resp["VersionId"]

def test_s3_put_object_no_version_id_without_versioning(s3):
    s3.create_bucket(Bucket="intg-s3-nover-put")
    resp = s3.put_object(Bucket="intg-s3-nover-put", Key="hello.txt", Body=b"data")
    assert "VersionId" not in resp

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

def test_s3_get_object_with_version_id(s3):
    """Enable versioning, put 2 versions of same key, verify version IDs differ."""
    bucket = "s3-version-get-test"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    # Put version 1
    r1 = s3.put_object(Bucket=bucket, Key="file.txt", Body=b"version-1")
    vid1 = r1.get("VersionId")
    assert vid1 is not None

    # Put version 2
    r2 = s3.put_object(Bucket=bucket, Key="file.txt", Body=b"version-2")
    vid2 = r2.get("VersionId")
    assert vid2 is not None
    assert vid1 != vid2

    # GetObject returns latest version with its VersionId
    get_resp = s3.get_object(Bucket=bucket, Key="file.txt")
    assert get_resp["Body"].read() == b"version-2"
    assert get_resp.get("VersionId") == vid2

def test_s3_eventbridge_notification_on_delete(s3, sqs, eb):
    """S3 delete_object should send EventBridge event when EventBridgeConfiguration is enabled."""
    bucket = "s3-eb-del-bkt"
    s3.create_bucket(Bucket=bucket)
    queue_url = sqs.create_queue(QueueName="s3-eb-del-target-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    # Enable EventBridge on bucket
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )

    # Create EventBridge rule matching S3 events -> SQS target
    eb.put_rule(
        Name="s3-del-to-sqs-rule",
        EventPattern=json.dumps({"source": ["aws.s3"]}),
        State="ENABLED",
    )
    eb.put_targets(
        Rule="s3-del-to-sqs-rule",
        Targets=[{"Id": "sqs-del-target", "Arn": queue_arn}],
    )

    # Put then delete object
    s3.put_object(Bucket=bucket, Key="del-test.txt", Body=b"data")
    # Drain the put event
    time.sleep(0.5)
    sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)

    # Now delete
    s3.delete_object(Bucket=bucket, Key="del-test.txt")
    time.sleep(0.5)

    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["source"] == "aws.s3"
    assert body["detail"]["bucket"]["name"] == bucket
    assert body["detail"]["object"]["key"] == "del-test.txt"

def test_s3_upload_part_copy(s3):
    """Multipart upload with UploadPartCopy (x-amz-copy-source) produces correct final object."""
    bkt = "intg-s3-partcopy"
    s3.create_bucket(Bucket=bkt)
    src_key = "source-obj.txt"
    dst_key = "dest-obj.txt"
    src_data = b"COPIED-DATA-FROM-SOURCE"
    s3.put_object(Bucket=bkt, Key=src_key, Body=src_data)

    mpu = s3.create_multipart_upload(Bucket=bkt, Key=dst_key)
    upload_id = mpu["UploadId"]

    copy_resp = s3.upload_part_copy(
        Bucket=bkt,
        Key=dst_key,
        UploadId=upload_id,
        PartNumber=1,
        CopySource={"Bucket": bkt, "Key": src_key},
    )
    etag = copy_resp["CopyPartResult"]["ETag"]

    s3.complete_multipart_upload(
        Bucket=bkt,
        Key=dst_key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [{"PartNumber": 1, "ETag": etag}]
        },
    )

    resp = s3.get_object(Bucket=bkt, Key=dst_key)
    assert resp["Body"].read() == src_data

def test_s3_event_to_sqs(s3, sqs):
    """S3 notification delivers event to SQS on object creation and deletion."""
    bucket = "intg-s3evt-sqs"
    queue_name = "intg-s3evt-sqs-q"

    s3.create_bucket(Bucket=bucket)
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                }
            ],
        },
    )

    # Put an object — should fire ObjectCreated event
    s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"world")
    time.sleep(1)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    s3_msgs = [m for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
    assert len(s3_msgs) >= 1
    body = json.loads(s3_msgs[0]["Body"])
    assert body["Records"][0]["eventSource"] == "aws:s3"
    assert body["Records"][0]["eventName"].startswith("ObjectCreated:")
    assert body["Records"][0]["s3"]["bucket"]["name"] == bucket
    assert body["Records"][0]["s3"]["object"]["key"] == "hello.txt"

    # Delete receipts so queue is clean
    for m in msgs.get("Messages", []):
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

    # Delete the object — should fire ObjectRemoved event
    s3.delete_object(Bucket=bucket, Key="hello.txt")
    time.sleep(1)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    s3_msgs = [m for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
    assert len(s3_msgs) >= 1
    del_body = json.loads(s3_msgs[0]["Body"])
    assert del_body["Records"][0]["eventName"].startswith("ObjectRemoved:")


def test_s3_lifecycle_transition_round_trip(s3):
    """PUT lifecycle with Transition, verify GET returns canonical XML with correct fields."""
    bucket = "intg-s3-lc-transition"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "archive-rule",
                "Status": "Enabled",
                "Filter": {"Prefix": "data/"},
                "Transitions": [
                    {"Days": 30, "StorageClass": "STANDARD_IA"},
                    {"Days": 90, "StorageClass": "GLACIER"},
                ],
                "Expiration": {"Days": 365},
            }]
        },
    )
    resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    rule = resp["Rules"][0]
    assert rule["ID"] == "archive-rule"
    assert rule["Status"] == "Enabled"
    assert rule["Filter"]["Prefix"] == "data/"
    transitions = rule["Transitions"]
    assert len(transitions) == 2
    assert transitions[0]["Days"] == 30
    assert transitions[0]["StorageClass"] == "STANDARD_IA"
    assert transitions[1]["Days"] == 90
    assert transitions[1]["StorageClass"] == "GLACIER"
    assert rule["Expiration"]["Days"] == 365


def test_s3_lifecycle_noncurrent_version(s3):
    """PUT lifecycle with NoncurrentVersionExpiration, verify round-trip."""
    bucket = "intg-s3-lc-noncurrent"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "noncurrent-cleanup",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
            }]
        },
    )
    resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    rule = resp["Rules"][0]
    assert rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 30


def test_s3_lifecycle_multiple_rules(s3):
    """Multiple lifecycle rules survive PUT/GET round-trip."""
    bucket = "intg-s3-lc-multi"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {"ID": "rule-1", "Status": "Enabled", "Filter": {"Prefix": "a/"}, "Expiration": {"Days": 10}},
                {"ID": "rule-2", "Status": "Disabled", "Filter": {"Prefix": "b/"}, "Expiration": {"Days": 20}},
                {"ID": "rule-3", "Status": "Enabled", "Filter": {"Prefix": "c/"}, "Expiration": {"Days": 30}},
            ]
        },
    )
    resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    assert len(resp["Rules"]) == 3
    ids = [r["ID"] for r in resp["Rules"]]
    assert "rule-1" in ids
    assert "rule-2" in ids
    assert "rule-3" in ids
    disabled = [r for r in resp["Rules"] if r["ID"] == "rule-2"][0]
    assert disabled["Status"] == "Disabled"


def test_s3_lifecycle_abort_multipart(s3):
    """AbortIncompleteMultipartUpload round-trip."""
    bucket = "intg-s3-lc-abort"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "abort-uploads",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
            }]
        },
    )
    resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    assert resp["Rules"][0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"] == 7
