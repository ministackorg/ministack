import json
import os
import re
import time
import uuid as _uuid_mod
from datetime import datetime
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError
from conftest import make_client, patch_endpoint_dns

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

# Last-Modified on S3 HTTP responses must be RFC 7231 HTTP-date (AWS / Smithy).
_RFC7231_LAST_MODIFIED_RE = re.compile(
    r"^[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT$"
)

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


# ─── Conditional PUT (If-Match / If-None-Match) ──────────────────────────────

def test_s3_put_object_if_none_match_star_no_existing(s3):
    """If-None-Match: * succeeds when no object exists at the key (create-once)."""
    bucket = "intg-s3-ifnm-star-create"
    s3.create_bucket(Bucket=bucket)

    # botocore strips IfNoneMatch on PutObject (added by S3 in 2024); send via low-level
    # event handler so the header reaches the wire.
    def _add_ifnm(request, **_kwargs):
        request.headers["If-None-Match"] = "*"

    s3.meta.events.register_first(
        "before-send.s3.PutObject", _add_ifnm,
    )
    try:
        s3.put_object(Bucket=bucket, Key="first.txt", Body=b"hello")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_ifnm)

    resp = s3.get_object(Bucket=bucket, Key="first.txt")
    assert resp["Body"].read() == b"hello"


def test_s3_put_object_if_none_match_star_existing_fails(s3):
    """If-None-Match: * returns 412 when an object already exists at the key."""
    bucket = "intg-s3-ifnm-star-conflict"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="taken.txt", Body=b"original")

    def _add_ifnm(request, **_kwargs):
        request.headers["If-None-Match"] = "*"

    s3.meta.events.register_first(
        "before-send.s3.PutObject", _add_ifnm,
    )
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bucket, Key="taken.txt", Body=b"second")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_ifnm)

    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    # The original bytes must remain — the failed PUT must not overwrite.
    resp = s3.get_object(Bucket=bucket, Key="taken.txt")
    assert resp["Body"].read() == b"original"


def test_s3_put_object_if_none_match_etag(s3):
    """If-None-Match: <etag> succeeds when existing ETag differs, fails when it matches."""
    bucket = "intg-s3-ifnm-etag"
    s3.create_bucket(Bucket=bucket)
    first = s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v1")
    first_etag = first["ETag"]

    # Wrong ETag → condition satisfied, PUT succeeds.
    def _add_wrong(request, **_kwargs):
        request.headers["If-None-Match"] = '"00000000000000000000000000000000"'

    s3.meta.events.register_first("before-send.s3.PutObject", _add_wrong)
    try:
        s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v2")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_wrong)

    # Matching ETag → condition violated, PUT fails 412.
    def _add_match(request, **_kwargs):
        # Use the new ETag from v2.
        v2_etag = s3.head_object(Bucket=bucket, Key="obj.txt")["ETag"]
        request.headers["If-None-Match"] = v2_etag

    s3.meta.events.register_first("before-send.s3.PutObject", _add_match)
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v3")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_match)

    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    _ = first_etag  # unused; kept to show v1 etag captured at write time


def test_s3_put_object_if_match_star_requires_existing(s3):
    """If-Match: * succeeds when an object exists, 404s when none does.

    The missing-key answer is NoSuchKey — not the RFC 7232 412 — for the "*"
    form just like the ETag form: AWS documents one If-Match error row for a
    key that doesn't exist, and ceph/s3-tests pins 404 for both forms.
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response
    """
    bucket = "intg-s3-ifm-star"
    s3.create_bucket(Bucket=bucket)

    def _add_ifm_star(request, **_kwargs):
        request.headers["If-Match"] = "*"

    # No existing object → 404 NoSuchKey.
    s3.meta.events.register_first("before-send.s3.PutObject", _add_ifm_star)
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bucket, Key="missing.txt", Body=b"x")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_ifm_star)
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Now create it, then If-Match: * succeeds.
    s3.put_object(Bucket=bucket, Key="present.txt", Body=b"a")
    s3.meta.events.register_first("before-send.s3.PutObject", _add_ifm_star)
    try:
        s3.put_object(Bucket=bucket, Key="present.txt", Body=b"b")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_ifm_star)
    assert s3.get_object(Bucket=bucket, Key="present.txt")["Body"].read() == b"b"


def test_s3_put_object_if_match_etag(s3):
    """If-Match: <etag> succeeds when ETag matches, 412 when stale."""
    bucket = "intg-s3-ifm-etag"
    s3.create_bucket(Bucket=bucket)
    initial = s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v1")
    initial_etag = initial["ETag"]

    def _add_match(request, **_kwargs):
        request.headers["If-Match"] = initial_etag

    # Matching ETag → succeed.
    s3.meta.events.register_first("before-send.s3.PutObject", _add_match)
    try:
        s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v2")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_match)

    # Old (stale) ETag against the new object → 412.
    s3.meta.events.register_first("before-send.s3.PutObject", _add_match)
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bucket, Key="obj.txt", Body=b"v3")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_match)

    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_s3_put_object_if_match_etag_missing_object_returns_404(s3):
    """If-Match: <etag> against a non-existent key returns 404 NoSuchKey (per AWS docs).

    AWS S3 specifically returns 404 — not 412 — when If-Match: <etag> targets a key
    that doesn't exist (or whose current version is a delete marker). Documented at
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response
    """
    bucket = "intg-s3-ifm-missing"
    s3.create_bucket(Bucket=bucket)

    def _add_etag(request, **_kwargs):
        request.headers["If-Match"] = '"00000000000000000000000000000000"'

    s3.meta.events.register_first("before-send.s3.PutObject", _add_etag)
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bucket, Key="absent.txt", Body=b"x")
    finally:
        s3.meta.events.unregister("before-send.s3.PutObject", _add_etag)

    assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# ─── Conditional CompleteMultipartUpload (If-Match / If-None-Match) ──────────
# Unlike PutObject, botocore models IfMatch/IfNoneMatch on
# CompleteMultipartUpload directly, so no header-injection hack is needed.

def _mpu_complete(s3, bucket, key, body=b"part-data", **conditions):
    """Run a full multipart upload for `key`, completing with `conditions`."""
    mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    part = s3.upload_part(Bucket=bucket, Key=key, UploadId=mp["UploadId"],
                          PartNumber=1, Body=body)
    return s3.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=mp["UploadId"],
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
        **conditions)


def test_s3_complete_multipart_if_none_match_star(s3):
    """If-None-Match: * on CompleteMultipartUpload is create-once: the first
    complete lands, a second over the existing key fails 412 and must not
    overwrite the object."""
    bucket = "intg-s3-mpu-ifnm-star"
    s3.create_bucket(Bucket=bucket)

    _mpu_complete(s3, bucket, "obj", body=b"first", IfNoneMatch="*")
    with pytest.raises(ClientError) as exc:
        _mpu_complete(s3, bucket, "obj", body=b"second", IfNoneMatch="*")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert s3.get_object(Bucket=bucket, Key="obj")["Body"].read() == b"first"


def test_s3_complete_multipart_if_match_etag(s3):
    """If-Match on CompleteMultipartUpload succeeds against the current ETag
    and fails 412 against a stale one."""
    bucket = "intg-s3-mpu-ifm-etag"
    s3.create_bucket(Bucket=bucket)
    etag = s3.put_object(Bucket=bucket, Key="obj", Body=b"v1")["ETag"]

    _mpu_complete(s3, bucket, "obj", body=b"v2", IfMatch=etag)
    assert s3.get_object(Bucket=bucket, Key="obj")["Body"].read() == b"v2"

    with pytest.raises(ClientError) as exc:
        _mpu_complete(s3, bucket, "obj", body=b"v3", IfMatch=etag)  # now stale
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert s3.get_object(Bucket=bucket, Key="obj")["Body"].read() == b"v2"


def test_s3_complete_multipart_if_match_missing_key_404(s3):
    """If-Match on CompleteMultipartUpload against a missing key is 404
    NoSuchKey — for the "*" form as well as the ETag form, like PutObject."""
    bucket = "intg-s3-mpu-ifm-missing"
    s3.create_bucket(Bucket=bucket)

    for cond in ("*", '"00000000000000000000000000000000"'):
        with pytest.raises(ClientError) as exc:
            _mpu_complete(s3, bucket, "absent", IfMatch=cond)
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

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
    endpoint = f"{ENDPOINT}/{bucket}/test.json"
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
    endpoint = f"{ENDPOINT}/{bucket}/empty.bin"
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

def test_s3_put_repeated_content_encoding_header(s3):
    """Repeated Content-Encoding lines mean one comma-joined value.

    The AWS SDK for Java v2 puts the caller's encoding and the aws-chunked
    marker on separate header lines, so reading only one of them strips the
    caller's encoding along with the marker.
    """
    import http.client
    from urllib.parse import urlparse
    bucket = "intg-s3-repeated-ce"
    s3.create_bucket(Bucket=bucket)

    payload = b"body-bytes"
    fake_sig = b"abc123"
    chunked = (
        f"{len(payload):x}".encode() + b";chunk-signature=" + fake_sig + b"\r\n" +
        payload + b"\r\n" +
        b"0;chunk-signature=" + fake_sig + b"\r\n\r\n"
    )
    parsed = urlparse(ENDPOINT)

    # (key, the Content-Encoding lines sent, what must be stored)
    cases = [
        ("caller-first", ["gzip", "aws-chunked"], "gzip"),
        ("marker-first", ["aws-chunked", "gzip"], "gzip"),
        ("two-encodings", ["deflate", "gzip", "aws-chunked"], "deflate, gzip"),
        ("marker-only", ["aws-chunked"], None),
    ]
    for key, lines, expected in cases:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 4566, timeout=10)
        conn.putrequest("PUT", f"/{bucket}/{key}")
        conn.putheader("Content-Length", str(len(chunked)))
        conn.putheader("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        conn.putheader(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request,"
            " SignedHeaders=host, Signature=fake",
        )
        for line in lines:
            conn.putheader("Content-Encoding", line)
        conn.endheaders()
        conn.send(chunked)
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 200, f"{key}: PUT returned {resp.status}"
        conn.close()

        head = s3.head_object(Bucket=bucket, Key=key)
        assert head.get("ContentEncoding") == expected, key
        # The chunk framing is still stripped from the body.
        assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == payload, key

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

def test_s3_head_object_website_redirection(s3):
    s3.create_bucket(Bucket="intg-s3-website-redirection")
    s3.put_object(
        Bucket="intg-s3-website-redirection",
        Key="redirect",
        WebsiteRedirectLocation='http://my-redirect-website',
    )
    resp = s3.head_object(Bucket="intg-s3-website-redirection", Key="redirect")
    assert resp["ContentLength"] == 0
    assert resp["WebsiteRedirectLocation"] == "http://my-redirect-website"

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


def test_s3_get_object_rejects_response_overrides_on_unsigned_request(s3):
    """AWS rejects unsigned GetObject requests carrying any of the six
    ``response-*`` override query parameters with HTTP 400 InvalidRequest.

    Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    "When you use these parameters, you must sign the request by using
    either an Authorization header or a presigned URL. These parameters
    cannot be used with an unsigned (anonymous) request."
    """
    import urllib.request
    bkt = "intg-s3-unsigned-resp-override"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="data.txt", Body=b"hello")

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    # No Authorization header, no presign markers — raw anonymous GET.
    for param in (
        "response-cache-control=no-cache",
        "response-content-disposition=attachment%3B%20filename%3Dfoo.txt",
        "response-content-encoding=gzip",
        "response-content-language=en",
        "response-content-type=text%2Fplain",
        "response-expires=0",
    ):
        url = f"{endpoint}/{bkt}/data.txt?{param}"
        try:
            urllib.request.urlopen(url, timeout=5).read()
            pytest.fail(f"expected 400 for unsigned request with {param}")
        except urllib.error.HTTPError as e:
            assert e.code == 400, f"{param} → wrong status {e.code}"
            body = e.read().decode()
            assert "InvalidRequest" in body, f"{param} → missing InvalidRequest in {body[:200]}"
            assert "anonymous" in body, f"{param} → missing 'anonymous' phrase in {body[:200]}"

    # And — same params on a SIGNED boto3 call must still work, untouched.
    resp = s3.get_object(
        Bucket=bkt,
        Key="data.txt",
        ResponseContentDisposition="attachment; filename=foo.txt",
    )
    assert resp["Body"].read() == b"hello"


def test_s3_get_object_response_overrides_replace_headers(s3):
    """Real S3 lets a signed GetObject override response headers via six
    ``response-*`` query parameters: Cache-Control, Content-Disposition,
    Content-Encoding, Content-Language, Content-Type, Expires. boto3 exposes
    them as ``ResponseCacheControl`` / ``ResponseContentDisposition`` / etc.
    Each override REPLACES the corresponding header on the response.
    """
    bkt = "intg-s3-resp-overrides"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt, Key="orig.txt", Body=b"payload",
        ContentType="text/x-original",
        CacheControl="max-age=600",
    )

    resp = s3.get_object(
        Bucket=bkt, Key="orig.txt",
        ResponseContentType="application/json",
        ResponseContentDisposition='attachment; filename="renamed.json"',
        ResponseCacheControl="no-store",
        ResponseContentEncoding="identity",
        ResponseContentLanguage="en-US",
        ResponseExpires="Thu, 01 Jan 1970 00:00:00 GMT",
    )
    assert resp["Body"].read() == b"payload"
    assert resp["ContentType"] == "application/json"
    h = resp["ResponseMetadata"]["HTTPHeaders"]
    assert h["content-type"] == "application/json"
    assert h["content-disposition"] == 'attachment; filename="renamed.json"'
    assert h["cache-control"] == "no-store"
    assert h["content-encoding"] == "identity"
    assert h["content-language"] == "en-US"
    assert h["expires"] == "Thu, 01 Jan 1970 00:00:00 GMT"

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


def test_s3_versioned_object_metadata(s3):
    """User metadata must round-trip on a versioned GetObject(VersionId). (#1342)

    Each version keeps its own metadata; addressing a version by id returns
    that version's metadata, and the current-version read returns the latest.
    """
    bkt = "intg-s3-meta-versioned"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(
        Bucket=bkt, VersioningConfiguration={"Status": "Enabled"}
    )

    v1 = s3.put_object(
        Bucket=bkt, Key="k", Body=b"one",
        Metadata={"gen": "one"}, ContentEncoding="gzip",
    )["VersionId"]
    v2 = s3.put_object(
        Bucket=bkt, Key="k", Body=b"two", Metadata={"gen": "two"},
    )["VersionId"]
    assert v1 and v2 and v1 != v2

    g1 = s3.get_object(Bucket=bkt, Key="k", VersionId=v1)
    assert g1["Metadata"]["gen"] == "one"
    assert g1["ContentEncoding"] == "gzip"
    assert g1["Body"].read() == b"one"

    g2 = s3.get_object(Bucket=bkt, Key="k", VersionId=v2)
    assert g2["Metadata"]["gen"] == "two"

    # Current-version read (no VersionId) targets the latest.
    cur = s3.get_object(Bucket=bkt, Key="k")
    assert cur["Metadata"]["gen"] == "two"


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

def test_s3_create_bucket_with_tags(s3):
    """Tags supplied in the CreateBucket request body must be applied to the
    bucket, so a follow-up GetBucketTagging returns them.
    """
    bkt = "intg-s3-createbkt-tags"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={
            "Tags": [
                {"Key": "project", "Value": "Trinity"},
                {"Key": "env", "Value": "prod"},
            ]
        },
    )
    resp = s3.get_bucket_tagging(Bucket=bkt)
    tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
    assert tags == {"project": "Trinity", "env": "prod"}

def test_s3_create_bucket_with_tags_and_location(s3):
    """Tags and LocationConstraint can be supplied together in the CreateBucket
    body; both must take effect."""
    bkt = "intg-s3-createbkt-tags-loc"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={
            "LocationConstraint": "us-west-2",
            "Tags": [{"Key": "project", "Value": "Trinity"}],
        },
    )
    resp = s3.get_bucket_tagging(Bucket=bkt)
    tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
    assert tags == {"project": "Trinity"}
    loc = s3.get_bucket_location(Bucket=bkt)
    assert loc["LocationConstraint"] == "us-west-2"

def test_s3_get_bucket_location_explicit_constraint(s3):
    """A bucket created with an explicit LocationConstraint must echo it back
    from GetBucketLocation."""
    bkt = f"intg-s3-loc-explicit-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    loc = s3.get_bucket_location(Bucket=bkt)
    assert loc["LocationConstraint"] == "eu-west-1"

def test_s3_get_bucket_location_us_east_1_is_none(s3):
    """AWS returns an empty LocationConstraint for us-east-1 buckets, which
    boto3 surfaces as None."""
    bkt = f"intg-s3-loc-useast1-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    loc = s3.get_bucket_location(Bucket=bkt)
    assert loc["LocationConstraint"] is None

def test_s3_get_bucket_location_defaults_to_signing_region(s3):
    """A bucket created WITHOUT CreateBucketConfiguration lands in the region
    the request was signed for — GetBucketLocation must echo that region."""
    import boto3
    from botocore.config import Config

    west_s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-1",
        config=Config(
            region_name="eu-west-1",
            retries={"mode": "standard"},
            max_pool_connections=50,
        ),
    )
    bkt = f"intg-s3-loc-signing-{_uuid_mod.uuid4().hex[:8]}"
    west_s3.create_bucket(Bucket=bkt)
    loc = west_s3.get_bucket_location(Bucket=bkt)
    assert loc["LocationConstraint"] == "eu-west-1"

def test_s3_create_bucket_without_tags_has_no_tag_set(s3):
    """A CreateBucket with no tags must not create an empty tag set — a
    GetBucketTagging should still return NoSuchTagSet."""
    bkt = "intg-s3-createbkt-notags"
    s3.create_bucket(Bucket=bkt)
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_tagging(Bucket=bkt)
    assert exc.value.response["Error"]["Code"] == "NoSuchTagSet"

def test_s3_create_bucket_empty_tag_value_allowed(s3):
    """Tag values may be empty (minimum length 0); only the key is required."""
    bkt = "intg-s3-createbkt-emptyval"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={"Tags": [{"Key": "project", "Value": ""}]},
    )
    resp = s3.get_bucket_tagging(Bucket=bkt)
    assert resp["TagSet"] == [{"Key": "project", "Value": ""}]

def test_s3_create_bucket_rejects_key_too_long(s3):
    """A tag key longer than 128 characters is rejected with InvalidTag."""
    bkt = "intg-s3-createbkt-longkey"
    with pytest.raises(ClientError) as exc:
        s3.create_bucket(
            Bucket=bkt,
            CreateBucketConfiguration={"Tags": [{"Key": "p" * 129, "Value": "Trinity"}]},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidTag"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Message"] == "The TagKey you have provided is invalid"
    # The bucket must not have been created.
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

def test_s3_create_bucket_rejects_value_too_long(s3):
    """A tag value longer than 256 characters is rejected with InvalidTag."""
    bkt = "intg-s3-createbkt-longval"
    with pytest.raises(ClientError) as exc:
        s3.create_bucket(
            Bucket=bkt,
            CreateBucketConfiguration={"Tags": [{"Key": "project", "Value": "T" * 257}]},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidTag"
    assert err["Message"] == "The TagValue you have provided is invalid"
    # The bucket must not have been created.
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

def test_s3_create_bucket_rejects_reserved_aws_prefix(s3):
    """Tag keys starting with the reserved 'aws:' prefix are rejected."""
    bkt = "intg-s3-createbkt-awsprefix"
    with pytest.raises(ClientError) as exc:
        s3.create_bucket(
            Bucket=bkt,
            CreateBucketConfiguration={"Tags": [{"Key": "aws:project", "Value": "Trinity"}]},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidTag"
    assert err["Message"] == (
        'User-defined tag keys can\'t start with "aws:". This prefix is '
        'reserved for system tags. Remove "aws:" from your tag keys and '
        "try again."
    )
    # The bucket must not have been created.
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

def test_s3_create_bucket_duplicate_keys_internal_error(s3):
    """A duplicate tag key in a CreateBucket body returns a 500 InternalError."""
    bkt = "intg-s3-createbkt-dupkey"
    with pytest.raises(ClientError) as exc:
        s3.create_bucket(
            Bucket=bkt,
            CreateBucketConfiguration={
                "Tags": [
                    {"Key": "env", "Value": "prod"},
                    {"Key": "env", "Value": "staging"},
                ]
            },
        )
    assert exc.value.response["Error"]["Code"] == "InternalError"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 500
    # The bucket must not have been created.
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

def test_s3_create_bucket_rejects_too_many_tags(s3):
    """A bucket accepts at most 50 tags in the CreateBucket body."""
    bkt = "intg-s3-createbkt-toomany"
    with pytest.raises(ClientError) as exc:
        s3.create_bucket(
            Bucket=bkt,
            CreateBucketConfiguration={
                "Tags": [{"Key": f"project{i}", "Value": "Trinity"} for i in range(51)]
            },
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "BadRequest"
    assert err["Message"] == "Bucket tag count cannot be greater than 50"
    # The bucket must not have been created.
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

def test_s3_create_bucket_tags_readable_via_s3control(s3):
    """Tags set in the CreateBucket body must also be visible through the
    S3 Control ListTagsForResource API, not just GetBucketTagging."""
    from conftest import make_client

    bkt = "intg-s3control-createbkt-tags"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={
            "Tags": [
                {"Key": "project", "Value": "Trinity"},
                {"Key": "env", "Value": "prod"},
            ]
        },
    )
    s3control = make_client("s3control")
    account_id = "123456789012"
    arn = f"arn:aws:s3:::{bkt}"
    with patch_endpoint_dns():
        resp = s3control.list_tags_for_resource(AccountId=account_id, ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
    assert tags == {"project": "Trinity", "env": "prod"}

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
    with patch_endpoint_dns():
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
        f"{ENDPOINT}/v20180820/tags/{arn}",
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
        f"{ENDPOINT}/v20180820/tags/{arn}",
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
        f"{ENDPOINT}/v20180820/tags/{arn}",
        method="GET",
        headers={
            "x-amz-account-id": "000000000000",
            "Host": f"s3-control.{urlparse(ENDPOINT).netloc}",
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


class TestParseAbsoluteFormRequestTarget:
    """_parse_bucket_key must strip scheme+authority when hypercorn passes an
    absolute-form request target (e.g. AWS SDK for .NET v4 over HTTP/1.1)."""

    def _parse(self, path):
        from ministack.services.s3 import _parse_bucket_key
        return _parse_bucket_key(path, {})

    def test_http_absolute_form(self):
        assert self._parse("http://ministack:4566/mybucket/mykey") == ("mybucket", "mykey")

    def test_https_absolute_form(self):
        assert self._parse("https://ministack:4566/mybucket/mykey") == ("mybucket", "mykey")

    def test_absolute_form_bucket_only(self):
        assert self._parse("http://ministack:4566/mybucket") == ("mybucket", "")

    def test_path_style_unaffected(self):
        assert self._parse("/mybucket/mykey") == ("mybucket", "mykey")


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


def test_s3_get_object_returns_tag_count(s3):
    """GetObject must surface x-amz-tagging-count as TagCount when the object has
    tags, and omit it when it has none (matches AWS / boto3 behavior) — #1026."""
    bkt = "intg-s3-tagcount"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt, Key="tagged.txt", Body=b"hi",
        Tagging="environment=dev&owner=test&project=p&version=1.0&region=eu",
    )
    resp = s3.get_object(Bucket=bkt, Key="tagged.txt")
    assert resp["TagCount"] == 5

    # An object with no tags: AWS omits the header, so boto3 has no TagCount key.
    s3.put_object(Bucket=bkt, Key="untagged.txt", Body=b"hi")
    resp2 = s3.get_object(Bucket=bkt, Key="untagged.txt")
    assert "TagCount" not in resp2


def test_s3_object_tagging_per_version(s3):
    """Tags must be stored per object version, not collapsed onto the key.

    Repro for #N: in a versioned bucket, tagging two versions of the same
    object resulted in only the last-written tag set being returned for
    either version.
    """
    bkt = "intg-s3-objtags-versioned"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(
        Bucket=bkt, VersioningConfiguration={"Status": "Enabled"}
    )

    v1 = s3.put_object(Bucket=bkt, Key="k", Body=b"one")["VersionId"]
    v2 = s3.put_object(Bucket=bkt, Key="k", Body=b"two")["VersionId"]
    assert v1 and v2 and v1 != v2

    s3.put_object_tagging(
        Bucket=bkt, Key="k", VersionId=v1,
        Tagging={"TagSet": [{"Key": "ver", "Value": "1"}]},
    )
    s3.put_object_tagging(
        Bucket=bkt, Key="k", VersionId=v2,
        Tagging={"TagSet": [{"Key": "ver", "Value": "2"}]},
    )

    g1 = s3.get_object_tagging(Bucket=bkt, Key="k", VersionId=v1)
    g2 = s3.get_object_tagging(Bucket=bkt, Key="k", VersionId=v2)
    assert {t["Key"]: t["Value"] for t in g1["TagSet"]} == {"ver": "1"}
    assert {t["Key"]: t["Value"] for t in g2["TagSet"]} == {"ver": "2"}
    assert g1["VersionId"] == v1
    assert g2["VersionId"] == v2

    # GetObjectTagging without VersionId targets the current version (v2).
    g_current = s3.get_object_tagging(Bucket=bkt, Key="k")
    assert {t["Key"]: t["Value"] for t in g_current["TagSet"]} == {"ver": "2"}

    # DeleteObjectTagging on v1 must not touch v2's tag set.
    s3.delete_object_tagging(Bucket=bkt, Key="k", VersionId=v1)
    g1_after = s3.get_object_tagging(Bucket=bkt, Key="k", VersionId=v1)
    g2_after = s3.get_object_tagging(Bucket=bkt, Key="k", VersionId=v2)
    assert g1_after["TagSet"] == []
    assert {t["Key"]: t["Value"] for t in g2_after["TagSet"]} == {"ver": "2"}


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
    # After delete the config is gone: GetPublicAccessBlock must 404 instead of
    # returning a default block (otherwise Terraform's delete waiter times out).
    with pytest.raises(ClientError) as exc:
        s3.get_public_access_block(Bucket=bkt)
    assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"

def test_s3_ownership_controls(s3):
    bkt = "intg-s3-ownership"
    s3.create_bucket(Bucket=bkt)
    # Never configured: real S3 reports the default Object Ownership, not a 404.
    resp = s3.get_bucket_ownership_controls(Bucket=bkt)
    assert resp["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced"
    s3.put_bucket_ownership_controls(
        Bucket=bkt,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
    )
    resp = s3.get_bucket_ownership_controls(Bucket=bkt)
    assert resp["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerPreferred"
    s3.delete_bucket_ownership_controls(Bucket=bkt)
    # After delete the config is gone: GetBucketOwnershipControls must 404 instead
    # of returning a default block (otherwise Terraform's delete waiter times out).
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_ownership_controls(Bucket=bkt)
    assert exc.value.response["Error"]["Code"] == "OwnershipControlsNotFoundError"

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

def test_s3_put_object_with_tagging_header_no_value(s3):
    """A --tagging value with no '=' (bare key, e.g. `tagging-hdr-no-value`) is a valid tag
    with an empty value — matches real AWS, repro from `aws s3api put-object
    --tagging tagging-hdr-no-value` followed by `get-object-tagging` returning
    {"Key": "tagging-hdr-no-value", "Value": ""}."""
    bkt = "intg-s3-put-tag-hdr-noval"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(
        Bucket=bkt,
        Key="tagged-without-value.txt",
        Body=b"hello",
        Tagging="tagging-hdr-no-value",
    )
    resp = s3.get_object_tagging(Bucket=bkt, Key="tagged-without-value.txt")
    assert resp["TagSet"] == [{"Key": "tagging-hdr-no-value", "Value": ""}]

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


@pytest.mark.parametrize(
    ("config_key", "arn_key", "target_arn"),
    [
        ("QueueConfigurations", "QueueArn", "not-an-arn"),
        ("QueueConfigurations", "QueueArn", "arn:aws:rds:us-east-1:000000000000:db:wrong-service"),
        ("TopicConfigurations", "TopicArn", "arn:aws:sqs:us-east-1:000000000000:wrong-service"),
        (
            "LambdaFunctionConfigurations",
            "LambdaFunctionArn",
            "arn:aws:sns:us-east-1:000000000000:wrong-service",
        ),
        ("QueueConfigurations", "QueueArn", "arn:aws:sqs:us-east-1:000000000001:s3-foreign-account-q"),
        ("TopicConfigurations", "TopicArn", "arn:aws:sns:us-east-1:000000000001:s3-foreign-account-topic"),
        (
            "LambdaFunctionConfigurations",
            "LambdaFunctionArn",
            "arn:aws:lambda:us-east-1:000000000001:function:s3-foreign-account-fn",
        ),
        ("QueueConfigurations", "QueueArn", "arn:aws:sqs:us-west-2:000000000000:s3-foreign-region-q"),
        ("TopicConfigurations", "TopicArn", "arn:aws:sns:us-west-2:000000000000:s3-foreign-region-topic"),
        (
            "LambdaFunctionConfigurations",
            "LambdaFunctionArn",
            "arn:aws:lambda:us-west-2:000000000000:function:s3-foreign-region-fn",
        ),
    ],
)
def test_s3_notification_rejects_invalid_or_out_of_scope_target_arns(
    s3, config_key, arn_key, target_arn,
):
    bkt = f"s3-evt-invalid-arn-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)

    with pytest.raises(ClientError) as exc:
        s3.put_bucket_notification_configuration(
            Bucket=bkt,
            NotificationConfiguration={
                config_key: [
                    {
                        arn_key: target_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ],
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


def test_s3_notification_validates_target_region_against_bucket_region(s3):
    bkt = f"s3-evt-bucket-region-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(
        Bucket=bkt,
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )

    s3.put_bucket_notification_configuration(
        Bucket=bkt,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": "arn:aws:sqs:us-west-2:000000000000:s3-west-region-q",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
        },
    )

    with pytest.raises(ClientError) as exc:
        s3.put_bucket_notification_configuration(
            Bucket=bkt,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": "arn:aws:sqs:us-east-1:000000000000:s3-east-region-q",
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ],
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


@pytest.mark.parametrize(
    "target_arn",
    [
        "arn:aws:sqs:us-east-1:000000000001:shared-q",
        "arn:aws:sqs:us-west-2:000000000000:shared-q",
    ],
)
def test_s3_notification_sqs_delivery_rejects_out_of_scope_arns_without_name_fallback(
    monkeypatch, target_arn,
):
    from ministack.services import s3 as s3mod
    from ministack.services import sqs as sqsmod

    messages = []
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(sqsmod, "_queue_url", lambda name: f"url/{name}")
    monkeypatch.setattr(sqsmod, "_queues", {"url/shared-q": {"messages": messages}})
    monkeypatch.setattr(sqsmod, "_ensure_msg_fields", lambda msg: None)

    s3mod._deliver_event_to_sqs(target_arn, {"Records": []}, "us-east-1")

    assert messages == []


@pytest.mark.parametrize(
    "target_arn",
    [
        "arn:aws:sns:us-east-1:000000000001:shared-topic",
        "arn:aws:sns:us-west-2:000000000000:shared-topic",
    ],
)
def test_s3_notification_sns_delivery_rejects_out_of_scope_arns_before_fanout(
    monkeypatch, target_arn,
):
    from ministack.services import s3 as s3mod
    from ministack.services import sns as snsmod

    fanouts = []
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(snsmod, "_topics", {target_arn: {"subscriptions": []}})
    monkeypatch.setattr(snsmod, "_fanout", lambda *args, **kwargs: fanouts.append(args))

    s3mod._deliver_event_to_sns(target_arn, {"Records": []}, "us-east-1")

    assert fanouts == []


@pytest.mark.parametrize(
    "target_arn",
    [
        "arn:aws:lambda:us-east-1:000000000001:function:shared-fn",
        "arn:aws:lambda:us-west-2:000000000000:function:shared-fn",
    ],
)
def test_s3_notification_lambda_delivery_rejects_out_of_scope_arns_before_lookup(
    monkeypatch, target_arn,
):
    from ministack.services import lambda_svc as lambdamod
    from ministack.services import s3 as s3mod

    lookups = []
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(
        lambdamod,
        "_get_func_record_for_ref",
        lambda ref: lookups.append(ref) or ({}, {}, "shared-fn"),
    )

    s3mod._deliver_event_to_lambda(target_arn, {"Records": []}, "us-east-1")

    assert lookups == []
def test_s3_event_notification_cross_account():
    """Regression for #876: S3 event notifications must fire for non-default
    accounts. The event is delivered from a background thread; if that thread
    does not inherit the request's account context it falls back to
    000000000000, the account-scoped bucket-notification lookup comes back
    empty, and the event is silently dropped. Every other notification test
    runs under the default account, so none of them exercise this path."""
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    account = "512354813215"

    def _acct_client(service):
        return boto3.client(
            service,
            endpoint_url=endpoint,
            aws_access_key_id=account,
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(retries={"max_attempts": 0}),
        )

    s3c = _acct_client("s3")
    sqsc = _acct_client("sqs")

    s3c.create_bucket(Bucket="s3-evt-xacct-bkt")
    queue_url = sqsc.create_queue(QueueName="s3-evt-xacct-q")["QueueUrl"]
    queue_arn = sqsc.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    # Confirm the clients really resolve to the non-default account.
    assert f":{account}:" in queue_arn

    s3c.put_bucket_notification_configuration(
        Bucket="s3-evt-xacct-bkt",
        NotificationConfiguration={
            "QueueConfigurations": [
                {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}
            ],
        },
    )
    s3c.put_object(Bucket="s3-evt-xacct-bkt", Key="x.txt", Body=b"hello")
    time.sleep(0.5)
    msgs = sqsc.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
    )
    s3_msgs = [m for m in msgs.get("Messages", []) if "Records" in json.loads(m["Body"])]
    assert len(s3_msgs) > 0, "no S3 event delivered to the non-default account queue (#876)"
    body = json.loads(s3_msgs[0]["Body"])
    assert body["Records"][0]["s3"]["object"]["key"] == "x.txt"


def _wait_lambda_invoked(logs_client, function_name, marker, timeout=5.0):
    """Poll the function's log group for a marker substring. Returns True on
    first match, False after timeout."""
    log_group = f"/aws/lambda/{function_name}"
    end = time.time() + timeout
    while time.time() < end:
        try:
            streams = logs_client.describe_log_streams(logGroupName=log_group)["logStreams"]
        except Exception:
            time.sleep(0.2)
            continue
        for s in streams:
            try:
                events = logs_client.get_log_events(
                    logGroupName=log_group, logStreamName=s["logStreamName"],
                )["events"]
            except Exception:
                continue
            if any(marker in (e.get("message") or "") for e in events):
                return True
        time.sleep(0.2)
    return False


def _regional_client(service: str, region: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        service,
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}, max_pool_connections=50),
    )


def _create_event_lambda(lam, name, marker="S3EVT"):
    import io as _io
    import zipfile as _zip
    code = (
        "def handler(event, context):\n"
        "    import json\n"
        f"    print('{marker}', context.invoked_function_arn, json.dumps(event))\n"
        "    return {'ok': True}\n"
    )
    buf = _io.BytesIO()
    with _zip.ZipFile(buf, "w") as z:
        z.writestr("lambda_function.py", code)
    lam.create_function(
        FunctionName=name,
        Runtime="python3.13",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="lambda_function.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    return lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]


def test_s3_event_notification_to_lambda_boto3_default(s3, lam, logs):
    """Regression: boto3's default put_bucket_notification_configuration
    (botocore wire serializes LambdaFunctionArn as <CloudFunction>) must keep
    invoking the Lambda on uploads.
    """
    fname = "s3-evt-lam-boto3"
    arn = _create_event_lambda(lam, fname)
    s3.create_bucket(Bucket="s3-evt-lam-boto3-bkt")
    s3.put_bucket_notification_configuration(
        Bucket="s3-evt-lam-boto3-bkt",
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {"LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"]},
            ],
        },
    )
    s3.put_object(Bucket="s3-evt-lam-boto3-bkt", Key="boto3.txt", Body=b"hi")
    assert _wait_lambda_invoked(logs, fname, "boto3.txt"), \
        "Lambda was not invoked for boto3-shaped notification config"


def test_s3_event_notification_to_lambda_validates_bucket_region(s3, lam):
    fname = "s3-evt-lam-region"
    _create_event_lambda(lam, fname, marker="east")
    west_lam = _regional_client("lambda", "us-west-2")
    west_logs = _regional_client("logs", "us-west-2")
    west_arn = _create_event_lambda(west_lam, fname, marker="west")

    s3.create_bucket(Bucket="s3-evt-lam-region-bkt")
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_notification_configuration(
            Bucket="s3-evt-lam-region-bkt",
            NotificationConfiguration={
                "LambdaFunctionConfigurations": [
                    {"LambdaFunctionArn": west_arn, "Events": ["s3:ObjectCreated:*"]},
                ],
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"

    s3.create_bucket(
        Bucket="s3-evt-lam-west-bkt",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    s3.put_bucket_notification_configuration(
        Bucket="s3-evt-lam-west-bkt",
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {"LambdaFunctionArn": west_arn, "Events": ["s3:ObjectCreated:*"]},
            ],
        },
    )
    s3.put_object(Bucket="s3-evt-lam-west-bkt", Key="regional.txt", Body=b"hi")

    assert _wait_lambda_invoked(west_logs, fname, "regional.txt"), \
        "S3 notification did not invoke the Lambda from the ARN's region"


def test_s3_event_notification_to_lambda_modern_xml(s3, lam, logs):
    """Issue #649: AWS SDK for Java v2, Go SDK, Terraform, and hand-crafted XML
    all send <LambdaFunctionArn> instead of the legacy <CloudFunction> tag.
    MS used to drop these configs silently — uploads succeeded but the
    Lambda never fired. Modern shape is now parsed.
    """
    import urllib.request as _urlreq
    fname = "s3-evt-lam-modern"
    arn = _create_event_lambda(lam, fname)
    s3.create_bucket(Bucket="s3-evt-lam-modern-bkt")

    modern_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        '<LambdaFunctionConfiguration>'
        '<Id>modern</Id>'
        f'<LambdaFunctionArn>{arn}</LambdaFunctionArn>'
        '<Event>s3:ObjectCreated:*</Event>'
        '</LambdaFunctionConfiguration>'
        '</NotificationConfiguration>'
    )
    req = _urlreq.Request(
        f"{os.environ.get('MINISTACK_ENDPOINT', 'http://localhost:4566')}/s3-evt-lam-modern-bkt?notification",
        data=modern_xml.encode(),
        method="PUT",
        headers={"Content-Type": "application/xml", "Authorization": "AWS test:test"},
    )
    _urlreq.urlopen(req)

    s3.put_object(Bucket="s3-evt-lam-modern-bkt", Key="modern.txt", Body=b"hi")
    assert _wait_lambda_invoked(logs, fname, "modern.txt"), \
        "Lambda was not invoked for modern <LambdaFunctionArn> XML — regression for #649"


def test_s3_event_notification_to_lambda_with_filter(s3, lam, logs):
    """Modern XML + prefix filter — only matching keys invoke the Lambda."""
    import urllib.request as _urlreq
    fname = "s3-evt-lam-filter"
    arn = _create_event_lambda(lam, fname)
    s3.create_bucket(Bucket="s3-evt-lam-filter-bkt")

    modern_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        '<LambdaFunctionConfiguration>'
        '<Id>filtered</Id>'
        f'<LambdaFunctionArn>{arn}</LambdaFunctionArn>'
        '<Event>s3:ObjectCreated:*</Event>'
        '<Filter><S3Key><FilterRule><Name>prefix</Name><Value>data/</Value></FilterRule></S3Key></Filter>'
        '</LambdaFunctionConfiguration>'
        '</NotificationConfiguration>'
    )
    req = _urlreq.Request(
        f"{os.environ.get('MINISTACK_ENDPOINT', 'http://localhost:4566')}/s3-evt-lam-filter-bkt?notification",
        data=modern_xml.encode(), method="PUT",
        headers={"Content-Type": "application/xml", "Authorization": "AWS test:test"},
    )
    _urlreq.urlopen(req)

    s3.put_object(Bucket="s3-evt-lam-filter-bkt", Key="other/skipme.txt", Body=b"x")
    s3.put_object(Bucket="s3-evt-lam-filter-bkt", Key="data/match.txt", Body=b"x")

    assert _wait_lambda_invoked(logs, fname, "data/match.txt"), \
        "Lambda was not invoked for filter-matched key"
    # The non-matching key must NOT show up in logs. Short additional wait to
    # be sure no late delivery sneaks through.
    time.sleep(0.5)
    saw_skipme = _wait_lambda_invoked(logs, fname, "skipme", timeout=0.1)
    assert not saw_skipme, "Lambda was invoked for a filter-mismatched key"


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
    """S3 EventBridgeConfiguration sends AWS-conformant events to EventBridge, routed to SQS.

    The emitted ``detail-type`` must be the fixed value Amazon S3 uses (``Object Created``),
    not the granular ``s3:ObjectCreated:Put`` event name — so the rule below matches on the
    documented detail-type, not on ``source`` alone, and would fail to route a non-conformant
    event.
    """
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

    # Rule matches the AWS-documented detail-type, not source alone.
    eb.put_rule(
        Name="s3-to-sqs-rule",
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Created"]}),
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
    assert body["detail-type"] == "Object Created"
    assert body["detail"]["bucket"]["name"] == "s3-eb-bkt"
    assert body["detail"]["object"]["key"] == "hello.txt"
    assert body["detail"]["reason"] == "PutObject"


def test_s3_eventbridge_notification_dispatches_in_bucket_region(s3):
    """S3 EventBridge delivery should use the bucket region, not the request region."""
    uid = _uuid_mod.uuid4().hex[:8]
    bucket_name = f"s3-eb-west-bkt-{uid}"
    queue_name = f"s3-eb-west-q-{uid}"
    rule_name = f"s3-eb-west-rule-{uid}"
    west_eb = _regional_client("events", "us-west-2")
    west_sqs = _regional_client("sqs", "us-west-2")

    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    queue_url = west_sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = west_sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    assert ":us-west-2:" in queue_arn

    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )
    west_eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Created"]}),
        State="ENABLED",
    )
    west_eb.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "west-sqs-target", "Arn": queue_arn}],
    )

    # The S3 client fixture is signed for us-east-1; the bucket itself is us-west-2.
    s3.put_object(Bucket=bucket_name, Key="west-region.txt", Body=b"world")
    time.sleep(0.5)

    msgs = west_sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["region"] == "us-west-2"
    assert body["detail"]["bucket"]["name"] == bucket_name
    assert body["detail"]["object"]["key"] == "west-region.txt"


def test_s3_eventbridge_notification_copy_reason(s3, sqs, eb):
    """A CopyObject create carries reason ``CopyObject`` (not a hardcoded ``PutObject``)."""
    s3.create_bucket(Bucket="s3-eb-copy-bkt")
    queue_url = sqs.create_queue(QueueName="s3-eb-copy-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="s3-eb-copy-bkt",
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )
    eb.put_rule(
        Name="s3-copy-rule",
        EventPattern=json.dumps(
            {
                "source": ["aws.s3"],
                "detail-type": ["Object Created"],
                "detail": {"reason": ["CopyObject"]},
            }
        ),
        State="ENABLED",
    )
    eb.put_targets(Rule="s3-copy-rule", Targets=[{"Id": "t", "Arn": queue_arn}])

    s3.put_object(Bucket="s3-eb-copy-bkt", Key="src.txt", Body=b"x")
    s3.copy_object(
        Bucket="s3-eb-copy-bkt", Key="dst.txt", CopySource="s3-eb-copy-bkt/src.txt"
    )
    time.sleep(0.5)

    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["detail-type"] == "Object Created"
    assert body["detail"]["object"]["key"] == "dst.txt"
    assert body["detail"]["reason"] == "CopyObject"


def test_s3_eventbridge_notification_object_deleted(s3, sqs, eb):
    """A delete emits ``Object Deleted`` with reason ``DeleteObject`` and a deletion-type."""
    s3.create_bucket(Bucket="s3-eb-del-bkt")
    queue_url = sqs.create_queue(QueueName="s3-eb-del-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="s3-eb-del-bkt",
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )
    eb.put_rule(
        Name="s3-del-rule",
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Deleted"]}),
        State="ENABLED",
    )
    eb.put_targets(Rule="s3-del-rule", Targets=[{"Id": "t", "Arn": queue_arn}])

    s3.put_object(Bucket="s3-eb-del-bkt", Key="gone.txt", Body=b"x")
    s3.delete_object(Bucket="s3-eb-del-bkt", Key="gone.txt")
    time.sleep(0.5)

    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
    assert "Messages" in msgs and len(msgs["Messages"]) > 0
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["detail-type"] == "Object Deleted"
    assert body["detail"]["reason"] == "DeleteObject"
    assert body["detail"]["deletion-type"] == "Permanently Deleted"

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


def test_s3_versioning_suspended_null_version(s3):
    """A PUT on a Suspended bucket is the "null" version: the response carries
    no VersionId, an overwrite replaces the null version rather than stacking
    a new one, and DeleteObject(VersionId='null') removes it."""
    bkt = "s3-ver-susp"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Suspended"})

    resp = s3.put_object(Bucket=bkt, Key="k", Body=b"one")
    assert "VersionId" not in resp
    s3.put_object(Bucket=bkt, Key="k", Body=b"two")

    versions = s3.list_object_versions(Bucket=bkt).get("Versions", [])
    assert [(v["VersionId"], v["IsLatest"]) for v in versions] == [("null", True)]
    assert s3.get_object(Bucket=bkt, Key="k", VersionId="null")["Body"].read() == b"two"

    s3.delete_object(Bucket=bkt, Key="k", VersionId="null")
    assert "Versions" not in s3.list_object_versions(Bucket=bkt)
    s3.delete_bucket(Bucket=bkt)


def test_s3_versioning_preserves_pre_versioning_null(s3):
    """An object written before versioning was enabled stays addressable as
    the "null" version once versioned writes land on top, and becomes current
    again when the newer version is deleted."""
    bkt = "s3-ver-null-keep"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="k", Body=b"fooz")
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    v2 = s3.put_object(Bucket=bkt, Key="k", Body=b"zzz")["VersionId"]
    assert s3.get_object(Bucket=bkt, Key="k")["Body"].read() == b"zzz"
    assert s3.get_object(Bucket=bkt, Key="k", VersionId="null")["Body"].read() == b"fooz"

    s3.delete_object(Bucket=bkt, Key="k", VersionId=v2)
    assert s3.get_object(Bucket=bkt, Key="k")["Body"].read() == b"fooz"

    s3.delete_object(Bucket=bkt, Key="k", VersionId="null")
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=bkt, Key="k")
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    assert "Versions" not in s3.list_object_versions(Bucket=bkt)
    s3.delete_bucket(Bucket=bkt)


def test_s3_delete_marker_signals(s3):
    """A delete on a versioned bucket reports its marker: DeleteMarker on the
    DELETE response, x-amz-delete-marker on the subsequent 404, and the
    literal "null" marker id when versioning is Suspended."""
    bkt = "s3-ver-marker"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="k", Body=b"x")

    resp = s3.delete_object(Bucket=bkt, Key="k")
    assert resp["DeleteMarker"] is True
    assert resp["VersionId"]

    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bkt, Key="k")
    headers = exc.value.response["ResponseMetadata"]["HTTPHeaders"]
    assert headers.get("x-amz-delete-marker") == "true"

    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Suspended"})
    s3.put_object(Bucket=bkt, Key="k2", Body=b"x")
    resp = s3.delete_object(Bucket=bkt, Key="k2")
    assert resp["DeleteMarker"] is True
    assert resp["VersionId"] == "null"


def test_s3_batch_delete_creates_marker(s3):
    """DeleteObjects without a VersionId on a versioned bucket creates a
    delete marker — for a key that never existed too — and reports it under
    DeleteMarker / DeleteMarkerVersionId, as AWS does."""
    bkt = "s3-ver-batch-marker"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    resp = s3.delete_objects(Bucket=bkt, Delete={"Objects": [{"Key": "never"}]})
    deleted = resp["Deleted"][0]
    assert deleted["DeleteMarker"] is True
    marker_id = deleted["DeleteMarkerVersionId"]

    markers = s3.list_object_versions(Bucket=bkt)["DeleteMarkers"]
    assert [m["VersionId"] for m in markers] == [marker_id]


def test_s3_upload_part_copy_versioned_source(s3):
    """UploadPartCopy with a source VersionId copies that version's bytes,
    not the current object's, and echoes x-amz-copy-source-version-id."""
    bkt = "s3-ver-partcopy"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=bkt, Key="src", Body=b"a" * 1024)["VersionId"]
    s3.put_object(Bucket=bkt, Key="src", Body=b"b" * 1024)

    mpu = s3.create_multipart_upload(Bucket=bkt, Key="dst")
    part = s3.upload_part_copy(
        Bucket=bkt, Key="dst", UploadId=mpu["UploadId"], PartNumber=1,
        CopySource={"Bucket": bkt, "Key": "src", "VersionId": v1})
    assert part["CopySourceVersionId"] == v1
    s3.complete_multipart_upload(
        Bucket=bkt, Key="dst", UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": [
            {"ETag": part["CopyPartResult"]["ETag"], "PartNumber": 1}]})
    assert s3.get_object(Bucket=bkt, Key="dst")["Body"].read() == b"a" * 1024


def test_s3_copy_versioned_source_keeps_metadata(s3):
    """CopyObject with a source VersionId carries that version's user
    metadata, like a current-object copy does."""
    bkt = "s3-ver-copy-meta"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=bkt, Key="src", Body=b"m",
                       Metadata={"foo": "bar"})["VersionId"]
    s3.put_object(Bucket=bkt, Key="src", Body=b"n")

    s3.copy_object(Bucket=bkt, Key="dst",
                   CopySource={"Bucket": bkt, "Key": "src", "VersionId": v1})
    assert s3.head_object(Bucket=bkt, Key="dst")["Metadata"] == {"foo": "bar"}


def test_s3_copy_source_key_containing_question_mark(s3):
    """A source key literally containing "?versionId" arrives percent-encoded
    and must not be mistaken for a versionId qualifier by CopyObject or
    UploadPartCopy."""
    bkt = "s3-ver-qmark"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="?versionId", Body=b"question")

    s3.copy_object(Bucket=bkt, Key="copied", CopySource={"Bucket": bkt, "Key": "?versionId"})
    assert s3.get_object(Bucket=bkt, Key="copied")["Body"].read() == b"question"

    mpu = s3.create_multipart_upload(Bucket=bkt, Key="dst")
    part = s3.upload_part_copy(
        Bucket=bkt, Key="dst", UploadId=mpu["UploadId"], PartNumber=1,
        CopySource={"Bucket": bkt, "Key": "?versionId"})
    s3.complete_multipart_upload(
        Bucket=bkt, Key="dst", UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": [
            {"ETag": part["CopyPartResult"]["ETag"], "PartNumber": 1}]})
    assert s3.get_object(Bucket=bkt, Key="dst")["Body"].read() == b"question"


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
    # Since 5 Jan 2023 every bucket has SSE-S3 default encryption, so after
    # deletion GetBucketEncryption returns the AES256 default instead of raising
    # ServerSideEncryptionConfigurationNotFoundError.
    default = s3.get_bucket_encryption(Bucket="intg-s3-enc")
    default_rules = default["ServerSideEncryptionConfiguration"]["Rules"]
    assert default_rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"


# ─── Server-side encryption (SSE-S3 / SSE-KMS / SSE-C) ──────────────────────
# MiniStack does not encrypt at rest; SSE is contract state — validated on
# write, persisted with the object, echoed on responses, and enforced on
# reads for SSE-C.  The customer key is never stored, only its MD5.

_SSE_C_KEY = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="
_SSE_C_MD5 = "DWygnHRtgiJ77HCm+1rvHw=="
_SSE_C_KEY2 = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4="
_SSE_C_MD5_2 = "arxBvwY2V4SiOne6yppVPQ=="
_SSE_C = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": _SSE_C_KEY,
          "SSECustomerKeyMD5": _SSE_C_MD5}
_SSE_C_2 = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": _SSE_C_KEY2,
            "SSECustomerKeyMD5": _SSE_C_MD5_2}


def test_s3_sse_s3_round_trip(s3):
    """x-amz-server-side-encryption: AES256 is echoed on the PUT response and
    persists to HEAD and GET."""
    import uuid as _u
    bucket = f"sse-s3-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    resp = s3.put_object(Bucket=bucket, Key="k", Body=b"x",
                         ServerSideEncryption="AES256")
    assert resp["ServerSideEncryption"] == "AES256"
    assert s3.head_object(Bucket=bucket, Key="k")["ServerSideEncryption"] == "AES256"
    got = s3.get_object(Bucket=bucket, Key="k")
    assert got["ServerSideEncryption"] == "AES256"
    assert got["Body"].read() == b"x"


def test_s3_sse_kms_round_trip_and_key_required(s3):
    """aws:kms echoes the key id everywhere; aws:kms without a key id is
    refused — MiniStack has no implicit account aws/s3 key to fall back to."""
    import uuid as _u
    bucket = f"sse-kms-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    resp = s3.put_object(Bucket=bucket, Key="k", Body=b"x",
                         ServerSideEncryption="aws:kms", SSEKMSKeyId="my-key-id")
    assert resp["ServerSideEncryption"] == "aws:kms"
    assert resp["SSEKMSKeyId"] == "my-key-id"
    head = s3.head_object(Bucket=bucket, Key="k")
    assert head["ServerSideEncryption"] == "aws:kms"
    assert head["SSEKMSKeyId"] == "my-key-id"

    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bucket, Key="k2", Body=b"x",
                      ServerSideEncryption="aws:kms")
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


def test_s3_sse_kms_and_sse_c_conflict(s3):
    """SSE-KMS and SSE-C on the same write are mutually exclusive (400)."""
    import uuid as _u
    bucket = f"sse-conflict-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bucket, Key="k", Body=b"x",
                      ServerSideEncryption="aws:kms", SSEKMSKeyId="kid", **_SSE_C)
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


def test_s3_sse_c_round_trip_and_read_gating(s3):
    """An SSE-C object echoes algorithm and key MD5, reads back only with its
    key, and answers 400 to a keyless read (GET or HEAD) but 403 AccessDenied
    to a wrong-key read, as AWS does."""
    import uuid as _u
    bucket = f"sse-c-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    resp = s3.put_object(Bucket=bucket, Key="k", Body=b"secret", **_SSE_C)
    assert resp["SSECustomerAlgorithm"] == "AES256"
    assert resp["SSECustomerKeyMD5"] == _SSE_C_MD5

    got = s3.get_object(Bucket=bucket, Key="k", **_SSE_C)
    assert got["Body"].read() == b"secret"
    assert got["SSECustomerKeyMD5"] == _SSE_C_MD5

    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=bucket, Key="k")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=bucket, Key="k", **_SSE_C_2)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bucket, Key="k")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_s3_sse_c_write_validation(s3):
    """Incoherent SSE-C write headers are refused: a wrong key MD5, a missing
    MD5, a missing key, and key material without the algorithm are each 400.
    (Injected as raw headers — boto3 would compute the MD5 itself.)"""
    import uuid as _u
    bucket = f"sse-c-bad-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)

    cases = [
        {  # wrong MD5 for the key
            "x-amz-server-side-encryption-customer-algorithm": "AES256",
            "x-amz-server-side-encryption-customer-key": _SSE_C_KEY,
            "x-amz-server-side-encryption-customer-key-md5": "AAAAAAAAAAAAAAAAAAAAAA==",
        },
        {  # no MD5
            "x-amz-server-side-encryption-customer-algorithm": "AES256",
            "x-amz-server-side-encryption-customer-key": _SSE_C_KEY,
        },
        {  # no key
            "x-amz-server-side-encryption-customer-algorithm": "AES256",
        },
        {  # key material without the algorithm
            "x-amz-server-side-encryption-customer-key": _SSE_C_KEY,
            "x-amz-server-side-encryption-customer-key-md5": _SSE_C_MD5,
        },
    ]
    for raw_headers in cases:
        def _inject(request, _h=raw_headers, **_kwargs):
            request.headers.update(_h)
        s3.meta.events.register_first("before-send.s3.PutObject", _inject)
        try:
            with pytest.raises(ClientError) as exc:
                s3.put_object(Bucket=bucket, Key="k", Body=b"x")
        finally:
            s3.meta.events.unregister("before-send.s3.PutObject", _inject)
        assert exc.value.response["Error"]["Code"] == "InvalidArgument", raw_headers


def test_s3_sse_bucket_default_persists_to_reads(s3):
    """A bucket's default encryption applies to the object, not only to the
    PUT reply: HEAD and GET of a plain upload report the configured SSE."""
    import uuid as _u
    bucket = f"sse-default-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    resp = s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    assert resp["ServerSideEncryption"] == "AES256"
    assert s3.head_object(Bucket=bucket, Key="k")["ServerSideEncryption"] == "AES256"
    assert s3.get_object(Bucket=bucket, Key="k")["ServerSideEncryption"] == "AES256"


def test_s3_sse_c_multipart(s3):
    """An SSE-C multipart upload echoes on initiate, requires the same key on
    every part and on the completion, and completes into an object readable
    only with the key."""
    import uuid as _u
    bucket = f"sse-c-mpu-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="m", **_SSE_C)
    assert mpu["SSECustomerKeyMD5"] == _SSE_C_MD5

    with pytest.raises(ClientError) as exc:
        s3.upload_part(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                       PartNumber=1, Body=b"z", **_SSE_C_2)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    with pytest.raises(ClientError) as exc:
        s3.upload_part(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                       PartNumber=1, Body=b"z")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    part = s3.upload_part(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                          PartNumber=1, Body=b"part-bytes", **_SSE_C)
    parts = {"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]}
    # The completion must present the create-time key again, as every part
    # did: keyless and wrong-key completions are refused.
    with pytest.raises(ClientError) as exc:
        s3.complete_multipart_upload(Bucket=bucket, Key="m",
                                     UploadId=mpu["UploadId"],
                                     MultipartUpload=parts)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    with pytest.raises(ClientError) as exc:
        s3.complete_multipart_upload(Bucket=bucket, Key="m",
                                     UploadId=mpu["UploadId"],
                                     MultipartUpload=parts, **_SSE_C_2)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    s3.complete_multipart_upload(Bucket=bucket, Key="m",
                                 UploadId=mpu["UploadId"],
                                 MultipartUpload=parts, **_SSE_C)
    assert s3.get_object(Bucket=bucket, Key="m", **_SSE_C)["Body"].read() == b"part-bytes"
    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket, Key="m")


def test_s3_sse_c_upload_part_copy_checks_both_keys_and_echoes(s3):
    """UploadPartCopy on an SSE-C upload must present the create-time key, and
    an SSE-C copy source must be addressed with its own key; the response
    echoes the stored algorithm, as the wire shape models."""
    import uuid as _u
    bucket = f"sse-c-upc-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="src", Body=b"source-bytes", **_SSE_C_2)
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="m", **_SSE_C)
    src = {"Bucket": bucket, "Key": "src"}
    src_key = {"CopySourceSSECustomerAlgorithm": "AES256",
               "CopySourceSSECustomerKey": _SSE_C_KEY2,
               "CopySourceSSECustomerKeyMD5": _SSE_C_MD5_2}
    # No part key: refused before the copy source is even read.
    with pytest.raises(ClientError) as exc:
        s3.upload_part_copy(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                            PartNumber=1, CopySource=src, **src_key)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    # Part key present, source key missing: the SSE-C source is unreadable.
    with pytest.raises(ClientError) as exc:
        s3.upload_part_copy(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                            PartNumber=1, CopySource=src, **_SSE_C)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    # Both keys: the copy succeeds and the response echoes the algorithm.
    resp = s3.upload_part_copy(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                               PartNumber=1, CopySource=src, **_SSE_C, **src_key)
    assert resp["SSECustomerAlgorithm"] == "AES256"
    assert resp["SSECustomerKeyMD5"] == _SSE_C_MD5
    parts = {"Parts": [{"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": 1}]}
    s3.complete_multipart_upload(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                                 MultipartUpload=parts, **_SSE_C)
    assert s3.get_object(Bucket=bucket, Key="m", **_SSE_C)["Body"].read() == b"source-bytes"


def test_s3_sse_multipart_complete_echoes_the_algorithm(s3):
    """CompleteMultipartUpload echoes the stored SSE algorithm on its
    response, not only on later GET/HEAD."""
    import uuid as _u
    bucket = f"sse-mpu-echo-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    mpu = s3.create_multipart_upload(Bucket=bucket, Key="m",
                                     ServerSideEncryption="AES256")
    part = s3.upload_part(Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
                          PartNumber=1, Body=b"bytes")
    resp = s3.complete_multipart_upload(
        Bucket=bucket, Key="m", UploadId=mpu["UploadId"],
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]})
    assert resp["ServerSideEncryption"] == "AES256"


def test_s3_sse_copy_semantics(s3):
    """The destination's encryption comes from the copy request, never the
    source: an SSE-C source needs its key as copy-source parameters (400
    without, 403 AccessDenied with the wrong one), and the copy lands
    unencrypted unless the request re-encrypts."""
    import uuid as _u
    bucket = f"sse-copy-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="src", Body=b"secret", **_SSE_C)

    with pytest.raises(ClientError) as exc:
        s3.copy_object(Bucket=bucket, Key="dst",
                       CopySource={"Bucket": bucket, "Key": "src"})
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    with pytest.raises(ClientError) as exc:
        s3.copy_object(Bucket=bucket, Key="dst",
                       CopySource={"Bucket": bucket, "Key": "src"},
                       CopySourceSSECustomerAlgorithm="AES256",
                       CopySourceSSECustomerKey=_SSE_C_KEY2,
                       CopySourceSSECustomerKeyMD5=_SSE_C_MD5_2)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    src_args = {"CopySourceSSECustomerAlgorithm": "AES256",
                "CopySourceSSECustomerKey": _SSE_C_KEY,
                "CopySourceSSECustomerKeyMD5": _SSE_C_MD5}
    s3.copy_object(Bucket=bucket, Key="dst",
                   CopySource={"Bucket": bucket, "Key": "src"}, **src_args)
    got = s3.get_object(Bucket=bucket, Key="dst")  # readable without a key
    assert got["Body"].read() == b"secret"
    assert "SSECustomerAlgorithm" not in got

    resp = s3.copy_object(Bucket=bucket, Key="dst2",
                          CopySource={"Bucket": bucket, "Key": "src"},
                          **src_args, **_SSE_C_2)
    assert resp["SSECustomerKeyMD5"] == _SSE_C_MD5_2
    assert s3.get_object(Bucket=bucket, Key="dst2",
                         **_SSE_C_2)["Body"].read() == b"secret"


def test_s3_post_object_sse_field(s3):
    """The POST form's x-amz-server-side-encryption field applies to the
    uploaded object like the header does on PutObject."""
    import uuid as _u

    import requests
    bucket = f"sse-post-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    post = s3.generate_presigned_post(
        Bucket=bucket, Key="k",
        Fields={"x-amz-server-side-encryption": "AES256"},
        Conditions=[{"x-amz-server-side-encryption": "AES256"}],
    )
    r = requests.post(post["url"], data=post["fields"], files={"file": ("k", b"x")})
    assert r.status_code == 204
    assert s3.head_object(Bucket=bucket, Key="k")["ServerSideEncryption"] == "AES256"


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

def test_s3_bucket_acl_canned(s3):
    """Canned x-amz-acl on CreateBucket and PutBucketAcl round-trips as the
    grants it implies. SDKs send the canned value as a header with an empty
    body, which used to be dropped, so `--acl public-read` read back as
    owner-only."""
    import uuid as _u
    bucket = f"acl-bkt-canned-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket, ACL="public-read")
    grants = s3.get_bucket_acl(Bucket=bucket)["Grants"]
    assert len(grants) == 2
    group = [g for g in grants if g["Grantee"]["Type"] == "Group"]
    assert group[0]["Grantee"]["URI"].endswith("global/AllUsers")
    assert group[0]["Permission"] == "READ"

    s3.put_bucket_acl(Bucket=bucket, ACL="public-read-write")
    grants = s3.get_bucket_acl(Bucket=bucket)["Grants"]
    perms = {g["Permission"] for g in grants if g["Grantee"]["Type"] == "Group"}
    assert perms == {"READ", "WRITE"}

    s3.put_bucket_acl(Bucket=bucket, ACL="private")
    grants = s3.get_bucket_acl(Bucket=bucket)["Grants"]
    assert len(grants) == 1
    assert grants[0]["Permission"] == "FULL_CONTROL"
    s3.delete_bucket(Bucket=bucket)

def test_s3_put_bucket_acl_invalid_canned(s3):
    """Invalid x-amz-acl values are rejected with InvalidArgument (400)."""
    import uuid as _u
    bucket = f"acl-bkt-bad-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_acl(Bucket=bucket, ACL="not-a-real-canned-acl")
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    s3.delete_bucket(Bucket=bucket)

def test_s3_put_bucket_acl_xml_body(s3):
    """A well-formed AccessControlPolicy XML body is accepted and round-trips."""
    import uuid as _u
    bucket = f"acl-bkt-xml-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_acl(
        Bucket=bucket,
        AccessControlPolicy={
            "Owner": {"ID": "test-owner-id", "DisplayName": "tester"},
            "Grants": [
                {
                    "Grantee": {
                        "Type": "CanonicalUser",
                        "ID": "test-owner-id",
                        "DisplayName": "tester",
                    },
                    "Permission": "FULL_CONTROL",
                },
                {
                    "Grantee": {
                        "Type": "Group",
                        "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
                    },
                    "Permission": "READ",
                },
            ],
        },
    )
    acl = s3.get_bucket_acl(Bucket=bucket)
    assert acl["Owner"]["ID"] == "test-owner-id"
    perms = sorted(g["Permission"] for g in acl["Grants"])
    assert perms == ["FULL_CONTROL", "READ"]
    s3.delete_bucket(Bucket=bucket)

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


def test_s3_versioned_get_object_preserves_content_type(s3):
    """Content-Type set on PutObject is returned when reading by VersionId."""
    bucket = "qa-s3-versioned-ct"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    put = s3.put_object(
        Bucket=bucket,
        Key="page.html",
        Body=b"<html/>",
        ContentType="text/html; charset=utf-8",
    )

    response = s3.get_object(
        Bucket=bucket,
        Key="page.html",
        VersionId=put["VersionId"],
    )
    assert response["ContentType"] == "text/html; charset=utf-8"


def test_s3_versioned_get_object_preserves_content_type_per_version(s3):
    """Each object version returns the Content-Type supplied for that version."""
    bucket = "qa-s3-versioned-ct-history"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    text_version = s3.put_object(
        Bucket=bucket,
        Key="document",
        Body=b"plain text",
        ContentType="text/plain",
    )["VersionId"]
    json_version = s3.put_object(
        Bucket=bucket,
        Key="document",
        Body=b'{"value": 1}',
        ContentType="application/json",
    )["VersionId"]

    text_response = s3.get_object(
        Bucket=bucket,
        Key="document",
        VersionId=text_version,
    )
    json_response = s3.get_object(
        Bucket=bucket,
        Key="document",
        VersionId=json_version,
    )

    assert text_response["ContentType"] == "text/plain"
    assert json_response["ContentType"] == "application/json"


def test_s3_put_object_storage_class_roundtrip(s3):
    """PutObject with StorageClass is returned by GetObject and HeadObject (#534)."""
    s3.create_bucket(Bucket="qa-s3-sc")
    s3.put_object(
        Bucket="qa-s3-sc",
        Key="cold.bin",
        Body=b"x",
        StorageClass="INTELLIGENT_TIERING",
    )
    g = s3.get_object(Bucket="qa-s3-sc", Key="cold.bin")
    assert g["StorageClass"] == "INTELLIGENT_TIERING"
    h = s3.head_object(Bucket="qa-s3-sc", Key="cold.bin")
    assert h["StorageClass"] == "INTELLIGENT_TIERING"


def test_s3_put_object_default_storage_class_is_standard(s3):
    """When PutObject does not set StorageClass, GetObject omits the field
    (botocore reports STANDARD via absence — no header on the wire)."""
    s3.create_bucket(Bucket="qa-s3-sc-default")
    s3.put_object(Bucket="qa-s3-sc-default", Key="f", Body=b"x")
    g = s3.get_object(Bucket="qa-s3-sc-default", Key="f")
    # AWS does not send the header for STANDARD; boto3 surfaces it as missing.
    assert g.get("StorageClass") in (None, "STANDARD")


def test_s3_invalid_storage_class_rejected(s3):
    """Unknown StorageClass values return InvalidStorageClass (#534)."""
    from botocore.exceptions import ClientError
    s3.create_bucket(Bucket="qa-s3-sc-bad")
    with pytest.raises(ClientError) as ei:
        s3.put_object(
            Bucket="qa-s3-sc-bad",
            Key="f",
            Body=b"x",
            StorageClass="NOT_A_CLASS",
        )
    assert ei.value.response["Error"]["Code"] == "InvalidStorageClass"


def test_s3_list_objects_reports_storage_class(s3):
    """ListObjectsV2 returns the per-object storage class, not a hardcoded STANDARD (#534)."""
    s3.create_bucket(Bucket="qa-s3-sc-list")
    s3.put_object(Bucket="qa-s3-sc-list", Key="hot", Body=b"x")
    s3.put_object(
        Bucket="qa-s3-sc-list", Key="cold", Body=b"x",
        StorageClass="GLACIER",
    )
    listing = {o["Key"]: o["StorageClass"]
               for o in s3.list_objects_v2(Bucket="qa-s3-sc-list")["Contents"]}
    assert listing["hot"] == "STANDARD"
    assert listing["cold"] == "GLACIER"


def test_s3_post_object_presigned(s3):
    """Browser POST upload via generate_presigned_post round-trips (#535)."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post")
    post = s3.generate_presigned_post(Bucket="qa-s3-post", Key="hello.txt")
    r = requests.post(
        post["url"], data=post["fields"],
        files={"file": ("hello.txt", b"hello world")},
    )
    assert r.status_code == 204
    assert r.headers["ETag"]
    assert "qa-s3-post" in r.headers["Location"] and "hello.txt" in r.headers["Location"]
    assert s3.get_object(Bucket="qa-s3-post", Key="hello.txt")["Body"].read() == b"hello world"


def test_s3_post_object_filename_substitution(s3):
    """`${filename}` in the key is replaced with the uploaded file's filename (#535)."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-fn")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-fn", Key="uploads/${filename}",
    )
    r = requests.post(
        post["url"], data=post["fields"],
        files={"file": ("photo.png", b"PNG-bytes")},
    )
    assert r.status_code == 204
    assert s3.get_object(Bucket="qa-s3-post-fn", Key="uploads/photo.png")["Body"].read() == b"PNG-bytes"


def test_s3_post_object_success_action_status_201(s3):
    """success_action_status=201 returns XML PostResponse (#535)."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-201")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-201", Key="k",
        Fields={"success_action_status": "201"},
        Conditions=[{"success_action_status": "201"}],
    )
    r = requests.post(post["url"], data=post["fields"], files={"file": ("x", b"x")})
    assert r.status_code == 201
    assert "<PostResponse>" in r.text and "<Bucket>qa-s3-post-201</Bucket>" in r.text
    assert "<Key>k</Key>" in r.text


def test_s3_post_object_content_type_passthrough(s3):
    """A `Content-Type` form field is stored on the object (#535)."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-ct")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-ct", Key="page.html",
        Fields={"Content-Type": "text/html; charset=utf-8"},
        Conditions=[["starts-with", "$Content-Type", "text/"]],
    )
    r = requests.post(post["url"], data=post["fields"], files={"file": ("p", b"<html/>")})
    assert r.status_code == 204
    assert "text/html" in s3.get_object(Bucket="qa-s3-post-ct", Key="page.html")["ContentType"]


def test_s3_post_object_storage_class(s3):
    """`x-amz-storage-class` form field is honored (#534 + #535)."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-sc")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-sc", Key="cold",
        Fields={"x-amz-storage-class": "GLACIER"},
        Conditions=[{"x-amz-storage-class": "GLACIER"}],
    )
    r = requests.post(post["url"], data=post["fields"], files={"file": ("x", b"x")})
    assert r.status_code == 204
    # Read the class off HeadObject: a GetObject of an unrestored GLACIER
    # object is exactly the call AWS rejects with InvalidObjectState.
    assert s3.head_object(Bucket="qa-s3-post-sc", Key="cold")["StorageClass"] == "GLACIER"


def test_s3_post_object_unquoted_field_names(s3):
    """`Content-Disposition: form-data; name=key` (token form) is accepted —
    .NET's MultipartFormDataContent emits this rather than the quoted form,
    and real S3 accepts both per RFC 2183."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-tok")
    boundary = "----testboundary"
    body = (
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=key\r\n\r\n"
        f"hello.txt\r\n"
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=file; filename=hello.txt\r\n"
        f"Content-Type: application/octet-stream\r\n\r\n"
        f"hello world\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    r = requests.post(
        f"{ENDPOINT}/qa-s3-post-tok",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert r.status_code == 204, r.text
    assert s3.get_object(Bucket="qa-s3-post-tok", Key="hello.txt")["Body"].read() == b"hello world"


def test_s3_post_object_content_length_range_enforced(s3):
    """`content-length-range` condition rejects oversize uploads with EntityTooLarge."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-clr")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-clr", Key="k",
        Conditions=[["content-length-range", 0, 5]],
    )
    # Within the limit -> 204
    ok = requests.post(post["url"], data=post["fields"], files={"file": ("f", b"abcde")})
    assert ok.status_code == 204

    # Over the limit -> 400 EntityTooLarge
    too_big = requests.post(post["url"], data=post["fields"], files={"file": ("f", b"abcdef")})
    assert too_big.status_code == 400
    assert "EntityTooLarge" in too_big.text


def test_s3_post_object_content_length_range_minimum(s3):
    """A minimum bound on `content-length-range` rejects undersize uploads."""
    import requests
    s3.create_bucket(Bucket="qa-s3-post-clr-min")
    post = s3.generate_presigned_post(
        Bucket="qa-s3-post-clr-min", Key="k",
        Conditions=[["content-length-range", 5, 1024]],
    )
    too_small = requests.post(post["url"], data=post["fields"], files={"file": ("f", b"abc")})
    assert too_small.status_code == 400
    assert "EntityTooSmall" in too_small.text


def test_s3_storage_class_persisted_to_disk(tmp_path, monkeypatch):
    """storage_class survives _persist_object → _load_persisted_bucket round-trip (#534)."""
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)

    obj = {
        "body": b"hello",
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": '"abc"',
        "last_modified": s3mod.now_iso(),
        "size": 5,
        "metadata": {},
        "preserved_headers": {},
        "storage_class": "GLACIER",
    }

    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    s3mod._persist_object("qa-bucket", "k", obj)

    s3mod._buckets._data.pop(("000000000000", "qa-bucket"), None)
    s3mod._load_persisted_bucket("000000000000", "qa-bucket",
                                 os.path.join(str(tmp_path), "000000000000", "qa-bucket"))
    restored = s3mod._buckets._data[("000000000000", "qa-bucket")]["objects"]["k"]
    assert restored["storage_class"] == "GLACIER"


def test_s3_version_id_persisted_to_disk(tmp_path, monkeypatch):
    """version_id survives _persist_object → _load_persisted_bucket round-trip (#1058)."""
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")

    obj = {
        "body": b"hello",
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": '"abc"',
        "last_modified": s3mod.now_iso(),
        "size": 5,
        "metadata": {},
        "preserved_headers": {},
        "storage_class": "STANDARD",
        "version_id": "test-version-1058",
    }
    s3mod._persist_object("ver-bucket", "k", obj)

    meta_path = os.path.join(str(tmp_path), "000000000000", "ver-bucket", "k.meta.json")
    with open(meta_path) as mf:
        assert json.load(mf)["version_id"] == "test-version-1058"

    s3mod._buckets._data.pop(("000000000000", "ver-bucket"), None)
    try:
        s3mod._load_persisted_bucket(
            "000000000000", "ver-bucket",
            os.path.join(str(tmp_path), "000000000000", "ver-bucket"))
        restored = s3mod._buckets._data[("000000000000", "ver-bucket")]["objects"]["k"]
        assert restored["version_id"] == "test-version-1058"
    finally:
        s3mod._buckets._data.pop(("000000000000", "ver-bucket"), None)


def test_s3_version_id_get_object_after_restore(tmp_path, monkeypatch):
    """GetObject(VersionId=...) must work after _load_persisted_bucket restores
    from disk. The version index (_object_versions) must be rebuilt so lookups
    by VersionId succeed (#1065)."""
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")

    version_id = "test-version-1065"
    obj = {
        "body": b"version-data",
        "content_type": "application/octet-stream",
        "content_encoding": None,
        "etag": '"abc"',
        "last_modified": s3mod.now_iso(),
        "size": 12,
        "metadata": {},
        "preserved_headers": {},
        "storage_class": "STANDARD",
        "version_id": version_id,
    }
    s3mod._persist_object("ver-get-bucket", "k", obj)

    # Clear in-memory state to simulate restart
    s3mod._buckets._data.pop(("000000000000", "ver-get-bucket"), None)
    s3mod._object_versions._data.pop(("000000000000", ("ver-get-bucket", "k")), None)

    try:
        s3mod._load_persisted_bucket(
            "000000000000", "ver-get-bucket",
            os.path.join(str(tmp_path), "000000000000", "ver-get-bucket"))

        # Verify _object_versions was rebuilt
        versions = s3mod._object_versions._data.get(("000000000000", ("ver-get-bucket", "k")), [])
        assert len(versions) == 1
        assert versions[0]["version_id"] == version_id
        assert versions[0]["data"] is None  # body stays on disk

        # Verify GetObject by VersionId returns the body from disk
        data = s3mod._get_object_data("ver-get-bucket", "k", version_id=version_id)
        assert data == b"version-data"
    finally:
        s3mod._buckets._data.pop(("000000000000", "ver-get-bucket"), None)
        s3mod._object_versions._data.pop(("000000000000", ("ver-get-bucket", "k")), None)


def test_s3_put_object_sidecar_carries_version_id(tmp_path, monkeypatch):
    """PutObject on a versioned bucket must persist AFTER version_id assignment,
    so the on-disk .meta.json carries the id returned in x-amz-version-id (#1058)."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")
    try:
        s3mod._create_bucket("ver-put-bucket", b"")
        s3mod._bucket_versioning["ver-put-bucket"] = "Enabled"
        status, resp_headers, _ = s3mod._put_object("ver-put-bucket", "k", b"hello", {})
        assert status == 200
        version_id = resp_headers["x-amz-version-id"]
        meta_path = os.path.join(
            str(tmp_path), "000000000000", "ver-put-bucket", "k.meta.json")
        with open(meta_path) as mf:
            assert json.load(mf)["version_id"] == version_id
    finally:
        s3mod._buckets._data.pop(("000000000000", "ver-put-bucket"), None)
        s3mod._bucket_versioning.pop("ver-put-bucket", None)
        s3mod._object_versions.pop(("ver-put-bucket", "k"), None)


def test_s3_create_bucket_persists_account_scoped(tmp_path, monkeypatch):
    """CreateBucket persists under DATA_DIR/<account>/<bucket>, never DATA_DIR/<bucket> (#824)."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")
    try:
        status, _, _ = s3mod._create_bucket("issue824-create", b"")
        assert status == 200
        # The on-disk dir is account-scoped...
        assert os.path.isdir(os.path.join(str(tmp_path), "000000000000", "issue824-create"))
        # ...and there is NO spurious folder at the data-dir root.
        assert not os.path.exists(os.path.join(str(tmp_path), "issue824-create"))
    finally:
        s3mod._buckets._data.pop(("000000000000", "issue824-create"), None)


def test_s3_put_object_no_spurious_root_folder(tmp_path, monkeypatch):
    """PutBucket + PutObject must not leave an empty folder at the data-dir root (#824).

    Mirrors the issue's repro: create 'my-bucket', put 'my-file', and assert the
    data-dir root contains only the account dir (no DATA_DIR/my-bucket)."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")
    try:
        s3mod._create_bucket("my-bucket", b"")
        obj = {
            "body": b"hello",
            "content_type": "text/plain",
            "content_encoding": None,
            "etag": '"abc"',
            "last_modified": s3mod.now_iso(),
            "size": 5,
            "metadata": {},
            "preserved_headers": {},
            "storage_class": "STANDARD",
        }
        s3mod._persist_object("my-bucket", "my-file", obj)
        # Object data lands under the account-scoped path...
        assert os.path.isfile(
            os.path.join(str(tmp_path), "000000000000", "my-bucket", "my-file")
        )
        # ...and the only top-level entry is the account dir — no spurious 'my-bucket'.
        assert sorted(os.listdir(str(tmp_path))) == ["000000000000"]
    finally:
        s3mod._buckets._data.pop(("000000000000", "my-bucket"), None)


def test_s3_delete_bucket_removes_persisted_dir(tmp_path, monkeypatch):
    """DeleteBucket removes the account-scoped on-disk directory (#824 cleanup gap)."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")
    try:
        s3mod._create_bucket("issue824-delete", b"")
        bucket_dir = os.path.join(str(tmp_path), "000000000000", "issue824-delete")
        assert os.path.isdir(bucket_dir)
        status, _, _ = s3mod._delete_bucket("issue824-delete")
        assert status == 204
        # The on-disk directory is cleaned up, not orphaned.
        assert not os.path.exists(bucket_dir)
    finally:
        s3mod._buckets._data.pop(("000000000000", "issue824-delete"), None)


def test_s3_copy_object_propagates_storage_class(s3):
    """CopyObject with explicit StorageClass overrides the source's class (#534)."""
    s3.create_bucket(Bucket="qa-s3-sc-copy")
    s3.put_object(Bucket="qa-s3-sc-copy", Key="src", Body=b"x")
    s3.copy_object(
        Bucket="qa-s3-sc-copy",
        Key="dst",
        CopySource={"Bucket": "qa-s3-sc-copy", "Key": "src"},
        StorageClass="STANDARD_IA",
    )
    assert s3.get_object(Bucket="qa-s3-sc-copy", Key="dst")["StorageClass"] == "STANDARD_IA"


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


def test_s3_get_object_non_latest_version_last_modified_is_rfc7231_http_date(s3):
    """GetObject with explicit VersionId must emit RFC 7231 Last-Modified.

    Non-latest versions are only reachable via ``VersionId``. That code path must
    not put ISO-8601 timestamps (with ``T`` / ``Z``) into the HTTP ``Last-Modified``
    header: AWS SDK for JavaScript v3 deserializes that header as RFC7231 and throws
    after HTTP 200 if the value is wrong.
    """
    import urllib.request

    bucket = "s3-ver-lastmod-http-date"
    key = "file.txt"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    r1 = s3.put_object(Bucket=bucket, Key=key, Body=b"first-version-body")
    vid1 = r1["VersionId"]
    assert vid1

    # Second object version — vid1 is no longer the latest; GET by VersionId hits
    # the versioned GetObject branch (not the generic object headers helper).
    s3.put_object(Bucket=bucket, Key=key, Body=b"second-version-body")

    got = s3.get_object(Bucket=bucket, Key=key, VersionId=vid1)
    assert got["Body"].read() == b"first-version-body"
    assert got["VersionId"] == vid1
    assert isinstance(got["LastModified"], datetime)

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key, "VersionId": vid1},
        ExpiresIn=120,
    )
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        last_modified_hdr = resp.headers.get("Last-Modified", "")

    assert last_modified_hdr, "Last-Modified header must be present on GetObject response"
    assert _RFC7231_LAST_MODIFIED_RE.match(last_modified_hdr), (
        f"Last-Modified must be RFC 7231 HTTP-date like real S3; got {last_modified_hdr!r}"
    )


def test_s3_presigned_url_signature_is_verified():
    """Presigned SigV4 URLs are verified against the server secret: a bogus-
    credential signature, or a signed header (content-type / content-length)
    tampered with after signing, is rejected with 403 SignatureDoesNotMatch —
    matching real S3. A valid, untampered presigned URL still succeeds."""
    import urllib.error
    import urllib.request

    import boto3
    from botocore.config import Config

    ep = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    # Explicit s3v4 + path style so ContentType / ContentLength are signed into
    # X-Amz-SignedHeaders (matches the reporter's config); otherwise boto3 would
    # not sign those headers and tampering them would be legitimately allowed.
    path_cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    good = boto3.client(
        "s3", endpoint_url=ep, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test", config=path_cfg,
    )
    bogus = boto3.client(
        "s3", endpoint_url=ep, region_name="us-east-1",
        aws_access_key_id="wrongkey", aws_secret_access_key="wrongsecret",
        config=path_cfg,
    )
    bucket = "presign-verify-bkt"
    good.create_bucket(Bucket=bucket)
    good.put_object(Bucket=bucket, Key="hello.txt", Body=b"hi")

    def status(req):
        try:
            return urllib.request.urlopen(req).status
        except urllib.error.HTTPError as exc:
            return exc.code

    # Bogus-credential signature is rejected.
    u = bogus.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "hello.txt"}, ExpiresIn=300)
    assert status(urllib.request.Request(u, method="GET")) == 403

    # Signed content-type tampered after signing is rejected.
    u = good.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": "up.txt", "ContentType": "text/plain"},
        ExpiresIn=300)
    assert status(urllib.request.Request(
        u, data=b"x", method="PUT",
        headers={"Content-Type": "application/octet-stream"})) == 403

    # Signed content-length tampered after signing is rejected.
    u = good.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": "up2.txt", "ContentLength": 2},
        ExpiresIn=300)
    assert status(urllib.request.Request(
        u, data=b"way more than two bytes", method="PUT")) == 403

    # A valid, untampered presigned URL still succeeds.
    u = good.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "hello.txt"}, ExpiresIn=300)
    assert status(urllib.request.Request(u, method="GET")) == 200


def test_s3_presigned_url_virtual_hosted_style_is_verified():
    """A virtual-hosted-style presigned URL signs the bucket-less canonical URI
    against the `{bucket}.host` Host header. MiniStack rewrites vhost → path-style
    internally, but verification must run against the URI the client actually
    signed, not the rewritten one (#1441)."""
    import urllib.error
    import urllib.request
    from urllib.parse import urlparse

    import boto3
    from botocore.config import Config

    ep = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    cli = boto3.client(
        "s3", endpoint_url=ep, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
    )
    bucket = "presign-vhost-bkt"
    cli.create_bucket(Bucket=bucket)

    u = cli.generate_presigned_url(
        "put_object", Params={"Bucket": bucket, "Key": "up.txt"}, ExpiresIn=300)
    parsed = urlparse(u)
    # The vhost host won't resolve; send to the endpoint with the bucket-less
    # path and the signed Host header, exactly as a fronting proxy would.
    target = ep + parsed.path + "?" + parsed.query
    req = urllib.request.Request(
        target, data=b"x", method="PUT", headers={"Host": parsed.netloc})
    try:
        code = urllib.request.urlopen(req).status
    except urllib.error.HTTPError as exc:
        code = exc.code
    assert code in (200, 204), f"vhost presigned PUT rejected with {code}"


def test_s3_presigned_put_metadata_hoisted_into_query(s3):
    """A presigned URL is sent by a caller who has only the URL and a body, so
    the `x-amz-*` headers the operation asked for may be hoisted into the query
    string, where the signature covers them like a signed header. Real S3
    applies them as the headers they stand for, so user metadata (and storage
    class, tagging, ...) must land on the object rather than being dropped."""
    import urllib.request

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    bucket = "presign-hoisted-bkt"
    key = "uploads/report.pdf"
    s3.create_bucket(Bucket=bucket)

    # Mixed-case `X-Amz-Meta-` too: header names are case-insensitive, and
    # SDKs differ on the casing they hoist with.
    hoisted = "&".join([
        "x-amz-meta-author=alice",
        "X-Amz-Meta-Title=quarterly%20report",
        "x-amz-storage-class=STANDARD_IA",
        "x-amz-tagging=env%3Dqa",
    ])
    signed = AWSRequest(method="PUT", url=f"{ENDPOINT}/{bucket}/{key}?{hoisted}")
    S3SigV4QueryAuth(
        Credentials("test", "test"), "s3", "us-east-1", expires=300
    ).add_auth(signed)

    resp = urllib.request.urlopen(
        urllib.request.Request(signed.url, data=b"file-bytes", method="PUT"))
    assert resp.status == 200

    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["Metadata"] == {"author": "alice", "title": "quarterly report"}
    assert head["StorageClass"] == "STANDARD_IA"
    assert s3.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] == [
        {"Key": "env", "Value": "qa"}
    ]


def test_s3_presigned_put_metadata_sent_as_signed_headers(s3):
    """The other half of the contract: when the presigner leaves the metadata
    in `X-Amz-SignedHeaders` instead of hoisting it, the uploader sends the
    `x-amz-meta-*` headers itself and they must still land on the object."""
    import urllib.request

    bucket = "presign-meta-hdr-bkt"
    key = "uploads/report.pdf"
    s3.create_bucket(Bucket=bucket)

    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key,
                "Metadata": {"author": "alice", "title": "quarterly report"}},
        ExpiresIn=300,
    )
    resp = urllib.request.urlopen(urllib.request.Request(
        url, data=b"file-bytes", method="PUT",
        headers={"x-amz-meta-author": "alice",
                 "x-amz-meta-title": "quarterly report"}))
    assert resp.status == 200

    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["Metadata"] == {"author": "alice", "title": "quarterly report"}


def test_s3_presigned_post_metadata_fields(s3):
    """A browser POST upload carries its metadata as `x-amz-meta-*` form
    fields, which the policy signs; they must land on the object too."""
    import requests

    bucket = "presign-meta-post-bkt"
    s3.create_bucket(Bucket=bucket)

    post = s3.generate_presigned_post(
        Bucket=bucket, Key="uploads/report.pdf",
        Fields={"x-amz-meta-author": "alice"},
        Conditions=[{"x-amz-meta-author": "alice"}],
    )
    r = requests.post(post["url"], data=post["fields"],
                      files={"file": ("report.pdf", b"file-bytes")})
    assert r.status_code == 204

    head = s3.head_object(Bucket=bucket, Key="uploads/report.pdf")
    assert head["Metadata"] == {"author": "alice"}


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


def test_s3_upload_part_copy_with_valid_range(s3):
    """UploadPartCopy with a valid x-amz-copy-source-range slices the source."""
    bkt = "intg-s3-partcopy-range"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="src", Body=b"0123456789")

    mpu = s3.create_multipart_upload(Bucket=bkt, Key="dst")
    upload_id = mpu["UploadId"]
    resp = s3.upload_part_copy(
        Bucket=bkt, Key="dst", UploadId=upload_id, PartNumber=1,
        CopySource={"Bucket": bkt, "Key": "src"},
        CopySourceRange="bytes=2-5",
    )
    s3.complete_multipart_upload(
        Bucket=bkt, Key="dst", UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": resp["CopyPartResult"]["ETag"]}]},
    )
    assert s3.get_object(Bucket=bkt, Key="dst")["Body"].read() == b"2345"


@pytest.mark.parametrize("bad_range", [
    "bytes=garbage",        # no hyphen
    "bytes=abc-def",        # non-numeric
    "bytes=10-20-30",       # too many segments
    "bytes=-",              # both empty
    "bytes=5-",             # missing end
    "bytes=-5",             # missing start
    "bytes=5-2",            # reversed
    "bytes=0-1,3-4",        # multi-range not allowed for UploadPartCopy
    "rows=0-5",             # wrong unit
    "0-5",                  # missing bytes= prefix
])
def test_s3_upload_part_copy_rejects_malformed_range(s3, bad_range):
    """Malformed x-amz-copy-source-range must return 400 InvalidArgument, not 500."""
    import requests
    bkt = "intg-s3-partcopy-bad-" + str(abs(hash(bad_range)))[:8]
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="src", Body=b"0123456789")

    upload_id = s3.create_multipart_upload(Bucket=bkt, Key="dst")["UploadId"]
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    r = requests.put(
        f"{endpoint}/{bkt}/dst",
        params={"partNumber": 1, "uploadId": upload_id},
        headers={
            "x-amz-copy-source": f"/{bkt}/src",
            "x-amz-copy-source-range": bad_range,
        },
        timeout=10,
    )
    assert r.status_code == 400, f"got {r.status_code} for {bad_range!r}: {r.text[:200]}"
    assert b"InvalidArgument" in r.content


def test_s3_upload_part_copy_rejects_out_of_bounds_range(s3):
    """Range past the end of the source object must return 400 InvalidArgument."""
    import requests
    bkt = "intg-s3-partcopy-oob"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="src", Body=b"0123456789")  # 10 bytes

    upload_id = s3.create_multipart_upload(Bucket=bkt, Key="dst")["UploadId"]
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    r = requests.put(
        f"{endpoint}/{bkt}/dst",
        params={"partNumber": 1, "uploadId": upload_id},
        headers={
            "x-amz-copy-source": f"/{bkt}/src",
            "x-amz-copy-source-range": "bytes=0-99",
        },
        timeout=10,
    )
    assert r.status_code == 400
    assert b"InvalidArgument" in r.content
    assert b"size: 10" in r.content


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


def test_s3_lifecycle_newer_noncurrent_versions_round_trip(s3):
    """NewerNoncurrentVersions must survive the PUT/GET round-trip.

    terraform-provider-aws waits after creating a lifecycle configuration until GET
    returns rules equal to what it sent. A dropped field never converges, so the
    create fails with 'timeout while waiting for state to become true' after 3m.
    """
    bucket = "intg-s3-lc-newer-noncurrent"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "noncurrent-cleanup",
                "Status": "Enabled",
                "Filter": {"Prefix": "integration-tests"},
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": 2,
                    "NewerNoncurrentVersions": 5,
                },
            }]
        },
    )
    rule = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]
    assert rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 2
    assert rule["NoncurrentVersionExpiration"]["NewerNoncurrentVersions"] == 5


def test_s3_lifecycle_noncurrent_version_transition_newer_versions(s3):
    """NewerNoncurrentVersions on a NoncurrentVersionTransition also round-trips."""
    bucket = "intg-s3-lc-nvt-newer"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "noncurrent-archive",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "NoncurrentVersionTransitions": [{
                    "NoncurrentDays": 7,
                    "NewerNoncurrentVersions": 3,
                    "StorageClass": "GLACIER",
                }],
            }]
        },
    )
    transition = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]["NoncurrentVersionTransitions"][0]
    assert transition["NoncurrentDays"] == 7
    assert transition["NewerNoncurrentVersions"] == 3
    assert transition["StorageClass"] == "GLACIER"


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


def test_s3_lifecycle_and_filter_echoes_object_size(s3):
    """An And operator must echo ObjectSizeGreaterThan (default 0), and a
    prefixless And must echo an empty Prefix. Real AWS injects both, and the
    Terraform aws provider's GetBucketLifecycleConfiguration equality waiter
    (reflect.DeepEqual against the expanded config, which carries
    ObjectSizeGreaterThan=0 / Prefix="") never converges without them."""
    bucket = "intg-s3-lc-and-size"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "prefix-tags",
                "Status": "Enabled",
                "Filter": {"And": {"Prefix": "logs/", "Tags": [{"Key": "tier", "Value": "ARCHIVE"}]}},
                "Expiration": {"Days": 90},
            }]
        },
    )
    and_op = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]["Filter"]["And"]
    assert and_op["Prefix"] == "logs/"
    assert and_op["ObjectSizeGreaterThan"] == 0

    # Prefixless (tags-only) And still echoes Prefix="" and ObjectSizeGreaterThan=0.
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "tags-only",
                "Status": "Enabled",
                "Filter": {"And": {"Tags": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}]}},
                "Expiration": {"Days": 10},
            }]
        },
    )
    and_op = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]["Filter"]["And"]
    assert and_op["Prefix"] == ""
    assert and_op["ObjectSizeGreaterThan"] == 0


def test_s3_lifecycle_object_size_round_trip(s3):
    """Explicit object-size filters round-trip, both inside an And and at the
    top level of a Filter (previously the parser dropped them)."""
    bucket = "intg-s3-lc-size-rt"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "and-sizes",
                "Status": "Enabled",
                "Filter": {"And": {"Prefix": "p/", "ObjectSizeGreaterThan": 100, "ObjectSizeLessThan": 200}},
                "Expiration": {"Days": 20},
            }]
        },
    )
    and_op = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]["Filter"]["And"]
    assert and_op["ObjectSizeGreaterThan"] == 100
    assert and_op["ObjectSizeLessThan"] == 200

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "filter-gt",
                "Status": "Enabled",
                "Filter": {"ObjectSizeGreaterThan": 500},
                "Expiration": {"Days": 30},
            }]
        },
    )
    filt = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"][0]["Filter"]
    assert filt["ObjectSizeGreaterThan"] == 500


# ============================================================================
# Object ACL (GetObjectAcl / PutObjectAcl)
# ============================================================================

def test_s3_get_object_acl_default(s3):
    """Default ACL returns one Grant of FULL_CONTROL to the owner."""
    import uuid as _u
    bucket = f"acl-default-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"hello")
    acl = s3.get_object_acl(Bucket=bucket, Key="k")
    assert acl["Owner"]["ID"]
    grants = acl["Grants"]
    assert len(grants) == 1
    assert grants[0]["Permission"] == "FULL_CONTROL"
    assert grants[0]["Grantee"]["Type"] == "CanonicalUser"
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_acl_canned(s3):
    """Canned ACL via x-amz-acl header is stored and round-trips via Get."""
    import uuid as _u
    bucket = f"acl-canned-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    s3.put_object_acl(Bucket=bucket, Key="k", ACL="public-read")
    acl = s3.get_object_acl(Bucket=bucket, Key="k")
    assert acl["Grants"]
    # Round-trip: the put succeeded and Get returns a well-formed policy.
    # We don't enforce ACL semantics, so the canned name is stored as a
    # comment in the body and not surfaced by boto3's parser; that's fine.
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_acl_invalid_canned(s3):
    """Invalid x-amz-acl values are rejected with InvalidArgument (400)."""
    import uuid as _u

    from botocore.exceptions import ClientError

    bucket = f"acl-bad-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.put_object_acl(Bucket=bucket, Key="k", ACL="not-a-real-canned-acl")
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_get_object_acl_no_such_key(s3):
    """GetObjectAcl on a missing key returns NoSuchKey (404)."""
    import uuid as _u

    from botocore.exceptions import ClientError

    bucket = f"acl-missing-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as exc:
        s3.get_object_acl(Bucket=bucket, Key="never-existed")
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_acl_xml_body(s3):
    """A well-formed AccessControlPolicy XML body is accepted and round-trips."""
    import uuid as _u
    bucket = f"acl-xml-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    s3.put_object_acl(
        Bucket=bucket, Key="k",
        AccessControlPolicy={
            "Owner": {"ID": "test-owner-id", "DisplayName": "tester"},
            "Grants": [
                {
                    "Grantee": {
                        "Type": "CanonicalUser",
                        "ID": "test-owner-id",
                        "DisplayName": "tester",
                    },
                    "Permission": "FULL_CONTROL",
                },
                {
                    "Grantee": {
                        "Type": "Group",
                        "URI": "http://acs.amazonaws.com/groups/global/AllUsers",
                    },
                    "Permission": "READ",
                },
            ],
        },
    )
    acl = s3.get_object_acl(Bucket=bucket, Key="k")
    assert acl["Owner"]["ID"] == "test-owner-id"
    perms = sorted(g["Permission"] for g in acl["Grants"])
    assert perms == ["FULL_CONTROL", "READ"]
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_object_acl_per_version(s3):
    """Object ACLs are per-version, like tags: `?versionId=` targets that
    version, and a later version is a separate object that starts from the
    default ACL while the addressed version keeps what was set."""
    import uuid as _u
    bucket = f"acl-ver-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
    s3.put_object(Bucket=bucket, Key="k", Body=b"two")

    s3.put_object_acl(Bucket=bucket, Key="k", VersionId=v1, ACL="public-read")

    acl = s3.get_object_acl(Bucket=bucket, Key="k", VersionId=v1)
    assert any(g["Grantee"].get("URI", "").endswith("global/AllUsers")
               for g in acl["Grants"])
    # The current version is a different object and keeps its default.
    assert len(s3.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 1

    # A fresh version starts from the default; v1 keeps what was set.
    s3.put_object(Bucket=bucket, Key="k", Body=b"three")
    assert len(s3.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 1
    acl = s3.get_object_acl(Bucket=bucket, Key="k", VersionId=v1)
    assert any(g["Grantee"].get("URI", "").endswith("global/AllUsers")
               for g in acl["Grants"])

    for v in s3.list_object_versions(Bucket=bucket).get("Versions", []):
        s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_canned_acl_versioned(s3):
    """x-amz-acl on PutObject sticks to the version that PUT created."""
    import uuid as _u
    bucket = f"acl-put-ver-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(
        Bucket=bucket, Key="k", Body=b"one", ACL="public-read")["VersionId"]
    assert len(s3.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 2

    s3.put_object(Bucket=bucket, Key="k", Body=b"two")
    assert len(s3.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 1
    acl = s3.get_object_acl(Bucket=bucket, Key="k", VersionId=v1)
    assert len(acl["Grants"]) == 2

    for v in s3.list_object_versions(Bucket=bucket).get("Versions", []):
        s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    s3.delete_bucket(Bucket=bucket)


def test_s3_post_object_acl_field(s3):
    """The POST form names its canned-ACL field `acl`; it applies to the
    uploaded object like x-amz-acl does on PutObject."""
    import uuid as _u

    import requests
    bucket = f"acl-post-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    post = s3.generate_presigned_post(
        Bucket=bucket, Key="k",
        Fields={"acl": "public-read"},
        Conditions=[{"acl": "public-read"}],
    )
    r = requests.post(post["url"], data=post["fields"], files={"file": ("k", b"x")})
    assert r.status_code == 204
    grants = s3.get_object_acl(Bucket=bucket, Key="k")["Grants"]
    assert any(g["Grantee"].get("URI", "").endswith("global/AllUsers")
               for g in grants)
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_with_sha256_checksum_roundtrips(s3):
    """PutObject + ChecksumAlgorithm=SHA256 must be retrievable via
    GetObject(ChecksumMode='ENABLED'). Issue #831."""
    import base64
    import hashlib

    bucket = "checksum-sha256-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"hello checksum world" * 64
    expected = base64.b64encode(hashlib.sha256(body).digest()).decode()

    s3.put_object(Bucket=bucket, Key="k", Body=body, ChecksumAlgorithm="SHA256")

    head = s3.head_object(Bucket=bucket, Key="k", ChecksumMode="ENABLED")
    assert head["ChecksumSHA256"] == expected

    got = s3.get_object(Bucket=bucket, Key="k", ChecksumMode="ENABLED")
    assert got["ChecksumSHA256"] == expected
    assert got["Body"].read() == body

    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_with_explicit_sha256_value_validated(s3):
    """PutObject with both ChecksumAlgorithm + ChecksumSHA256: the supplied
    value must match the server-computed one (BadDigest otherwise)."""
    import base64
    import hashlib

    from botocore.exceptions import ClientError

    bucket = "checksum-validate-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"trust but verify"
    good = base64.b64encode(hashlib.sha256(body).digest()).decode()

    # Matching value → accepted.
    s3.put_object(Bucket=bucket, Key="ok", Body=body,
                  ChecksumAlgorithm="SHA256", ChecksumSHA256=good)
    head = s3.head_object(Bucket=bucket, Key="ok", ChecksumMode="ENABLED")
    assert head["ChecksumSHA256"] == good

    # Mismatched value → BadDigest.
    bad = base64.b64encode(hashlib.sha256(b"tampered").digest()).decode()
    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bucket, Key="bad", Body=body,
                      ChecksumAlgorithm="SHA256", ChecksumSHA256=bad)
    assert exc.value.response["Error"]["Code"] == "BadDigest"

    s3.delete_object(Bucket=bucket, Key="ok")
    s3.delete_bucket(Bucket=bucket)


def test_s3_multipart_checksum_sha256_composite(s3):
    """A checksummed multipart upload carries its parts' checksums through to
    a composite on the completed object: the part echoes its own, the
    completion answers the digest-of-digests suffixed with the part count, and
    a read asks for it with ChecksumMode."""
    import base64
    import hashlib

    from botocore.exceptions import ClientError

    bucket = "checksum-multipart-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"A" * 1024
    part_sum = base64.b64encode(hashlib.sha256(body).digest()).decode()
    composite = base64.b64encode(
        hashlib.sha256(hashlib.sha256(body).digest()).digest()).decode() + "-1"

    mpu = s3.create_multipart_upload(Bucket=bucket, Key="k",
                                     ChecksumAlgorithm="SHA256")
    assert mpu["ChecksumAlgorithm"] == "SHA256"
    uid = mpu["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key="k", UploadId=uid, PartNumber=1,
                          Body=body, ChecksumAlgorithm="SHA256",
                          ChecksumSHA256=part_sum)
    assert part["ChecksumSHA256"] == part_sum
    assert s3.list_parts(Bucket=bucket, Key="k",
                         UploadId=uid)["Parts"][0]["ChecksumSHA256"] == part_sum

    parts = {"Parts": [{"PartNumber": 1, "ETag": part["ETag"],
                        "ChecksumSHA256": part_sum}]}

    # A composite the caller names must be the one the parts add up to.
    with pytest.raises(ClientError) as exc:
        s3.complete_multipart_upload(Bucket=bucket, Key="k", UploadId=uid,
                                     ChecksumSHA256="bad",
                                     MultipartUpload=parts)
    assert exc.value.response["Error"]["Code"] == "BadDigest"

    done = s3.complete_multipart_upload(Bucket=bucket, Key="k", UploadId=uid,
                                        ChecksumSHA256=composite,
                                        MultipartUpload=parts)
    assert done["ChecksumSHA256"] == composite

    # Silent unless the read opts in, as with any other stored checksum.
    assert "ChecksumSHA256" not in s3.head_object(Bucket=bucket, Key="k")
    head = s3.head_object(Bucket=bucket, Key="k", ChecksumMode="ENABLED")
    assert head["ChecksumSHA256"] == composite
    assert head["ChecksumType"] == "COMPOSITE"

    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_versioned_get_returns_stored_checksum(s3):
    """A versioned GetObject(?versionId=X) with ChecksumMode=ENABLED must
    return the per-version checksum that was stored at put time. Issue #831
    in-scope follow-up: the original fix added checksums to the current-version
    path; the versioned-read branch had its own early-return."""
    import base64
    import hashlib

    bucket = "checksum-versioned-bucket"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )

    body_a = b"version A body"
    body_b = b"version B body - different bytes entirely"
    expected_a = base64.b64encode(hashlib.sha256(body_a).digest()).decode()
    expected_b = base64.b64encode(hashlib.sha256(body_b).digest()).decode()

    pa = s3.put_object(Bucket=bucket, Key="k", Body=body_a, ChecksumAlgorithm="SHA256")
    pb = s3.put_object(Bucket=bucket, Key="k", Body=body_b, ChecksumAlgorithm="SHA256")
    va = pa["VersionId"]
    vb = pb["VersionId"]
    assert va != vb

    got_a = s3.get_object(Bucket=bucket, Key="k", VersionId=va, ChecksumMode="ENABLED")
    got_b = s3.get_object(Bucket=bucket, Key="k", VersionId=vb, ChecksumMode="ENABLED")
    assert got_a["ChecksumSHA256"] == expected_a
    assert got_b["ChecksumSHA256"] == expected_b
    assert got_a["Body"].read() == body_a
    assert got_b["Body"].read() == body_b

    s3.delete_object(Bucket=bucket, Key="k", VersionId=va)
    s3.delete_object(Bucket=bucket, Key="k", VersionId=vb)
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_rejects_unsupported_crc32c_explicitly(s3):
    """CRC32C requires an optional native library ministack doesn't bundle.
    Rather than silently accept-without-validation, the put must fail loudly
    so clients see the gap. Issue #831 follow-up: no silent failures."""
    import base64
    import os

    from botocore.exceptions import ClientError

    bucket = "checksum-crc32c-reject-bucket"
    s3.create_bucket(Bucket=bucket)
    fake_crc32c = base64.b64encode(os.urandom(4)).decode()
    with pytest.raises(ClientError) as exc:
        s3.put_object(
            Bucket=bucket, Key="k", Body=b"x",
            ChecksumAlgorithm="CRC32C",
            ChecksumCRC32C=fake_crc32c,
        )
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"
    s3.delete_bucket(Bucket=bucket)


def test_s3_copy_object_preserves_source_checksum(s3):
    """CopyObject must propagate the source's stored checksum to the
    destination so GetObject(dest, ChecksumMode='ENABLED') returns the same
    SHA256 as the source. Issue #831 in-scope follow-up."""
    import base64
    import hashlib

    src_bucket = "checksum-copy-src"
    dst_bucket = "checksum-copy-dst"
    s3.create_bucket(Bucket=src_bucket)
    s3.create_bucket(Bucket=dst_bucket)
    body = b"copy me with my checksum intact"
    expected = base64.b64encode(hashlib.sha256(body).digest()).decode()

    s3.put_object(Bucket=src_bucket, Key="k", Body=body, ChecksumAlgorithm="SHA256")
    s3.copy_object(
        Bucket=dst_bucket, Key="k",
        CopySource={"Bucket": src_bucket, "Key": "k"},
    )
    got = s3.get_object(Bucket=dst_bucket, Key="k", ChecksumMode="ENABLED")
    assert got["ChecksumSHA256"] == expected

    s3.delete_object(Bucket=src_bucket, Key="k")
    s3.delete_object(Bucket=dst_bucket, Key="k")
    s3.delete_bucket(Bucket=src_bucket)
    s3.delete_bucket(Bucket=dst_bucket)


def test_s3_put_object_with_crc32_checksum_roundtrips(s3):
    """CRC32 is the other stdlib-supported algorithm — verify the same path."""
    import base64
    import struct
    import zlib

    bucket = "checksum-crc32-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"crc32 payload"
    expected = base64.b64encode(struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)).decode()

    s3.put_object(Bucket=bucket, Key="k", Body=body, ChecksumAlgorithm="CRC32")
    got = s3.get_object(Bucket=bucket, Key="k", ChecksumMode="ENABLED")
    assert got["ChecksumCRC32"] == expected

    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


# CRC-64/NVME's published check value: the checksum of b"123456789". The tests
# below assert against it so the server is pinned to the specification rather
# than to its own implementation.
_CRC64NVME_CHECK = 0xAE8B14860A799888


def _crc64nvme_bitwise(data: bytes) -> int:
    """Bit-at-a-time CRC-64/NVME, independent of the server's table."""
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x9A6C9329AC4BC9B5 if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFFFFFFFFFF


def _put_with_checksum(bucket: str, key: str, body: bytes, headers: dict):
    """PUT over raw HTTP, the way the CLI sends a checksummed upload.

    boto3 can only compute CRC-64/NVME when the optional `awscrt` package is
    installed, so these tests build the headers themselves rather than making
    the suite depend on it.
    """
    import urllib.request
    req = urllib.request.Request(
        f"{ENDPOINT}/{bucket}/{key}", data=body, method="PUT", headers=headers)
    return urllib.request.urlopen(req)


def _head_checksum(bucket: str, key: str, algorithm: str):
    import urllib.request
    req = urllib.request.Request(
        f"{ENDPOINT}/{bucket}/{key}", method="HEAD",
        headers={"x-amz-checksum-mode": "ENABLED"})
    with urllib.request.urlopen(req) as r:
        return r.headers.get(f"x-amz-checksum-{algorithm}"), r.headers.get("x-amz-checksum-type")


def test_s3_put_object_with_crc64nvme_checksum_roundtrips(s3):
    """CRC-64/NVME is the algorithm current SDKs and the CLI checksum uploads
    with by default, so a stock `aws s3 cp` fails outright unless the server
    computes it. It is plain arithmetic and needs no native library."""
    import base64
    import struct

    bucket = "checksum-crc64nvme-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"123456789"
    expected = base64.b64encode(struct.pack(">Q", _CRC64NVME_CHECK)).decode()

    with _put_with_checksum(bucket, "k", body,
                            {"x-amz-sdk-checksum-algorithm": "CRC64NVME"}) as r:
        assert r.status == 200

    value, csum_type = _head_checksum(bucket, "k", "crc64nvme")
    assert value == expected
    assert csum_type == "FULL_OBJECT"
    assert s3.get_object(Bucket=bucket, Key="k")["Body"].read() == body

    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_crc64nvme_matches_bitwise_reference_over_long_body(s3):
    """The server's table-driven CRC must agree with a bit-at-a-time reference
    over a body long enough to reach every table entry."""
    import base64
    import struct

    # A wrong reference must not be able to rubber-stamp a wrong server.
    assert _crc64nvme_bitwise(b"123456789") == _CRC64NVME_CHECK

    bucket = "checksum-crc64nvme-long-bucket"
    s3.create_bucket(Bucket=bucket)
    body = bytes(range(256)) * 40 + b"tail"
    expected = base64.b64encode(struct.pack(">Q", _crc64nvme_bitwise(body))).decode()

    with _put_with_checksum(bucket, "k", body,
                            {"x-amz-sdk-checksum-algorithm": "CRC64NVME"}) as r:
        assert r.status == 200

    value, _ = _head_checksum(bucket, "k", "crc64nvme")
    assert value == expected

    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_with_explicit_crc64nvme_value_validated(s3):
    """A client-supplied CRC-64/NVME must match the server-computed one, so the
    algorithm is verified rather than echoed back unchecked."""
    import base64
    import struct
    import urllib.error

    bucket = "checksum-crc64nvme-validate-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"123456789"
    good = base64.b64encode(struct.pack(">Q", _CRC64NVME_CHECK)).decode()
    bad = base64.b64encode(struct.pack(">Q", _CRC64NVME_CHECK ^ 0xFF)).decode()

    with _put_with_checksum(bucket, "ok", body, {
        "x-amz-sdk-checksum-algorithm": "CRC64NVME",
        "x-amz-checksum-crc64nvme": good,
    }) as r:
        assert r.status == 200
    assert _head_checksum(bucket, "ok", "crc64nvme")[0] == good

    with pytest.raises(urllib.error.HTTPError) as exc:
        _put_with_checksum(bucket, "bad", body, {
            "x-amz-sdk-checksum-algorithm": "CRC64NVME",
            "x-amz-checksum-crc64nvme": bad,
        })
    assert exc.value.code == 400
    assert b"BadDigest" in exc.value.read()

    s3.delete_object(Bucket=bucket, Key="ok")
    s3.delete_bucket(Bucket=bucket)


def test_s3_delete_object_by_version_id_purges_version(s3):
    """DeleteObject with an explicit VersionId must physically remove exactly
    that version (not add a delete marker). Repro for the versioned-delete bug:
    the handler ignored VersionId and always appended a delete marker, so the
    version count went UP instead of to zero."""
    bkt = "intg-s3-verdel-single"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    v1 = s3.put_object(Bucket=bkt, Key="a", Body=b"v1")["VersionId"]
    v2 = s3.put_object(Bucket=bkt, Key="a", Body=b"v2")["VersionId"]
    assert v1 != v2

    # Delete the older version by id — the newer one must survive.
    s3.delete_object(Bucket=bkt, Key="a", VersionId=v1)
    versions = s3.list_object_versions(Bucket=bkt, Prefix="a").get("Versions", [])
    ids = [v["VersionId"] for v in versions]
    assert ids == [v2], f"expected only {v2!r} to remain, got {ids!r}"
    assert not s3.list_object_versions(Bucket=bkt, Prefix="a").get("DeleteMarkers")

    # Delete the last version by id — nothing should remain.
    s3.delete_object(Bucket=bkt, Key="a", VersionId=v2)
    listing = s3.list_object_versions(Bucket=bkt, Prefix="a")
    assert listing.get("Versions", []) == []
    assert listing.get("DeleteMarkers", []) == []


def test_s3_delete_object_without_version_id_still_creates_marker(s3):
    """Regression guard: DeleteObject WITHOUT a VersionId must keep creating a
    delete marker (logical delete) rather than purging history."""
    bkt = "intg-s3-verdel-marker"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=bkt, Key="a", Body=b"v1")
    resp = s3.delete_object(Bucket=bkt, Key="a")
    assert resp.get("DeleteMarker") is True

    listing = s3.list_object_versions(Bucket=bkt, Prefix="a")
    assert len(listing.get("Versions", [])) == 1
    assert len(listing.get("DeleteMarkers", [])) == 1
    # The current version is now the delete marker → HeadObject 404s.
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bkt, Key="a")
    assert exc.value.response["Error"]["Code"] in ("404", "NoSuchKey")


def test_s3_delete_objects_batch_by_version_id_purges_all(s3):
    """Batch DeleteObjects with explicit {Key, VersionId} entries must remove
    every addressed version AND delete marker. This is the reported repro:
    2 versions + 1 delete marker, purged by id, must leave ListObjectVersions
    completely empty."""
    bkt = "intg-s3-verdel-batch"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=bkt, Key="a", Body=b"v1")
    s3.put_object(Bucket=bkt, Key="a", Body=b"v2")
    s3.delete_object(Bucket=bkt, Key="a")  # delete marker

    listing = s3.list_object_versions(Bucket=bkt, Prefix="a")
    assert len(listing.get("Versions", [])) == 2
    assert len(listing.get("DeleteMarkers", [])) == 1

    objects = [
        {"Key": o["Key"], "VersionId": o["VersionId"]}
        for o in listing.get("Versions", []) + listing.get("DeleteMarkers", [])
    ]
    resp = s3.delete_objects(Bucket=bkt, Delete={"Objects": objects})
    assert len(resp.get("Deleted", [])) == 3
    assert resp.get("Errors", []) == []

    after = s3.list_object_versions(Bucket=bkt, Prefix="a")
    assert after.get("Versions", []) == []
    assert after.get("DeleteMarkers", []) == []


def test_s3_delete_delete_marker_by_version_id_restores_object(s3):
    """Deleting the latest delete marker by its VersionId must make the object
    visible again (its previous version becomes current)."""
    bkt = "intg-s3-verdel-restore"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})

    s3.put_object(Bucket=bkt, Key="a", Body=b"hello")
    marker_id = s3.delete_object(Bucket=bkt, Key="a")["VersionId"]

    # Marker shadows the object.
    with pytest.raises(ClientError):
        s3.head_object(Bucket=bkt, Key="a")

    # Remove the marker → the object reappears.
    s3.delete_object(Bucket=bkt, Key="a", VersionId=marker_id)
    got = s3.get_object(Bucket=bkt, Key="a")
    assert got["Body"].read() == b"hello"

    markers = s3.list_object_versions(Bucket=bkt, Prefix="a").get("DeleteMarkers", [])
    assert markers == []


def test_s3_get_object_attributes(s3):
    import hashlib
    bkt = f"intg-s3-attrs-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)

    # Single PUT (STANDARD): ETag has no quotes, StorageClass omitted, size echoed.
    body = b"hello world"
    s3.put_object(Bucket=bkt, Key="k1", Body=body)
    r = s3.get_object_attributes(
        Bucket=bkt, Key="k1", ObjectAttributes=["ETag", "ObjectSize", "StorageClass"])
    assert r["ETag"] == hashlib.md5(body).hexdigest()  # no surrounding quotes
    assert r["ObjectSize"] == len(body)
    assert "StorageClass" not in r  # AWS omits StorageClass for S3 Standard

    # Non-STANDARD storage class is reported.
    s3.put_object(Bucket=bkt, Key="k2", Body=b"x", StorageClass="STANDARD_IA")
    r = s3.get_object_attributes(Bucket=bkt, Key="k2", ObjectAttributes=["StorageClass"])
    assert r["StorageClass"] == "STANDARD_IA"

    # Checksum with its ChecksumType.
    s3.put_object(Bucket=bkt, Key="k3", Body=b"abc", ChecksumAlgorithm="SHA256")
    r = s3.get_object_attributes(Bucket=bkt, Key="k3", ObjectAttributes=["Checksum"])
    assert "ChecksumSHA256" in r["Checksum"]
    assert r["Checksum"]["ChecksumType"] == "FULL_OBJECT"

    # Only requested attributes are returned.
    r = s3.get_object_attributes(Bucket=bkt, Key="k1", ObjectAttributes=["ObjectSize"])
    assert "ETag" not in r and r["ObjectSize"] == len(body)

    # Multipart: ObjectParts lists the completed parts.
    mp = s3.create_multipart_upload(Bucket=bkt, Key="k4")
    uid = mp["UploadId"]
    parts = []
    for i in (1, 2):
        p = s3.upload_part(Bucket=bkt, Key="k4", PartNumber=i, UploadId=uid,
                           Body=b"z" * (5 * 1024 * 1024))
        parts.append({"PartNumber": i, "ETag": p["ETag"]})
    s3.complete_multipart_upload(Bucket=bkt, Key="k4", UploadId=uid,
                                 MultipartUpload={"Parts": parts})
    r = s3.get_object_attributes(Bucket=bkt, Key="k4", ObjectAttributes=["ObjectParts"])
    assert r["ObjectParts"]["TotalPartsCount"] == 2
    assert [p["PartNumber"] for p in r["ObjectParts"]["Parts"]] == [1, 2]

    # Missing key → NoSuchKey.
    with pytest.raises(ClientError) as exc:
        s3.get_object_attributes(Bucket=bkt, Key="absent", ObjectAttributes=["ETag"])
    assert exc.value.response["Error"]["Code"] == "NoSuchKey"

    # Versioned read by versionId.
    s3.put_bucket_versioning(Bucket=bkt,
                             VersioningConfiguration={"Status": "Enabled"})
    pv = s3.put_object(Bucket=bkt, Key="kv", Body=b"v1")
    r = s3.get_object_attributes(Bucket=bkt, Key="kv", VersionId=pv["VersionId"],
                                 ObjectAttributes=["ObjectSize"])
    assert r["ObjectSize"] == 2 and r["VersionId"] == pv["VersionId"]


# ---------------------------------------------------------------------------
# ceph/s3-tests conformance fixes (#1322)
# ---------------------------------------------------------------------------

def test_s3_post_object_field_with_filename_is_not_body(s3):
    """A form field carrying a filename (HTTP libraries set one on every field)
    must NOT be treated as the object body — only the field named `file` is.
    Regression for #1322 defect 1."""
    import requests
    from collections import OrderedDict
    bucket = "intg-s3-post-filename-field"
    s3.create_bucket(Bucket=bucket)
    # `key` is an ordinary form field that carries a filename; `file` is the body.
    r = requests.post(
        f"{ENDPOINT}/{bucket}",
        files=OrderedDict([
            ("key", ("key.txt", "up.txt")),
            ("file", ("f.txt", b"bar")),
        ]),
    )
    assert r.status_code == 204
    assert s3.get_object(Bucket=bucket, Key="up.txt")["Body"].read() == b"bar"


def test_s3_get_object_part_number(s3):
    """GetObject with partNumber returns that part as 206 with a parts count,
    not the whole object. Regression for #1322 defect 2."""
    bucket = "intg-s3-get-partnumber"
    s3.create_bucket(Bucket=bucket)
    uid = s3.create_multipart_upload(Bucket=bucket, Key="k")["UploadId"]
    p1 = s3.upload_part(Bucket=bucket, Key="k", UploadId=uid, PartNumber=1, Body=b"A" * 100)
    p2 = s3.upload_part(Bucket=bucket, Key="k", UploadId=uid, PartNumber=2, Body=b"B" * 50)
    s3.complete_multipart_upload(
        Bucket=bucket, Key="k", UploadId=uid,
        MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": p1["ETag"]},
            {"PartNumber": 2, "ETag": p2["ETag"]},
        ]},
    )
    g = s3.get_object(Bucket=bucket, Key="k", PartNumber=1)
    assert g["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert g["PartsCount"] == 2
    assert g["ContentLength"] == 100
    assert g["Body"].read() == b"A" * 100


def test_s3_list_object_versions_pagination_markers(s3):
    """A truncated ListObjectVersions returns NextKeyMarker and the marker
    advances a second page without repeats. Regression for #1322 defect 3."""
    bucket = "intg-s3-lov-markers"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    for i in range(5):
        s3.put_object(Bucket=bucket, Key=f"k{i}", Body=b"x")
    r1 = s3.list_object_versions(Bucket=bucket, MaxKeys=2)
    assert r1["IsTruncated"] is True
    assert r1.get("NextKeyMarker")
    r2 = s3.list_object_versions(Bucket=bucket, MaxKeys=2, KeyMarker=r1["NextKeyMarker"])
    first = {v["Key"] for v in r1["Versions"]}
    second = {v["Key"] for v in r2["Versions"]}
    assert first and second and first.isdisjoint(second)


def test_s3_complete_multipart_upload_empty_body_is_malformed_xml(s3):
    """CompleteMultipartUpload with an empty body returns 400 MalformedXML,
    not a 500 with a JSON document. Regression for #1322 defect 6."""
    import requests
    bucket = "intg-s3-cmu-emptybody"
    s3.create_bucket(Bucket=bucket)
    uid = s3.create_multipart_upload(Bucket=bucket, Key="k")["UploadId"]
    r = requests.post(f"{ENDPOINT}/{bucket}/k?uploadId={uid}", data=b"")
    assert r.status_code == 400
    assert "MalformedXML" in r.text


def test_s3_complete_multipart_upload_idempotent(s3):
    """A repeated CompleteMultipartUpload with the same upload id replays the
    original result instead of NoSuchUpload. Regression for #1322 defect 8."""
    bucket = "intg-s3-cmu-idempotent"
    s3.create_bucket(Bucket=bucket)
    uid = s3.create_multipart_upload(Bucket=bucket, Key="k")["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key="k", UploadId=uid, PartNumber=1, Body=b"data")
    mpu = {"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]}
    r1 = s3.complete_multipart_upload(Bucket=bucket, Key="k", UploadId=uid, MultipartUpload=mpu)
    r2 = s3.complete_multipart_upload(Bucket=bucket, Key="k", UploadId=uid, MultipartUpload=mpu)
    assert r1["ETag"] == r2["ETag"]


def test_s3_owner_id_consistent_between_listing_and_acl(s3):
    """The owner id in a listing matches the owner id returned by the object ACL.
    Regression for #1322 defect 9."""
    bucket = "intg-s3-owner-consistency"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    listed = s3.list_objects(Bucket=bucket)["Contents"][0]["Owner"]["ID"]
    acl_owner = s3.get_object_acl(Bucket=bucket, Key="k")["Owner"]["ID"]
    assert listed == acl_owner


def test_s3_presigned_url_expires():
    """An expired SigV4 presigned URL is rejected with 403. Regression for
    #1322 defect 10. Uses an explicit s3v4 client (the expiry check lives in the
    SigV4 verification path); the default fixture presigns with SigV2."""
    import boto3
    import requests
    from botocore.config import Config
    ep = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    v4 = boto3.client(
        "s3", endpoint_url=ep, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    bucket = "intg-s3-presign-expiry"
    v4.create_bucket(Bucket=bucket)
    v4.put_object(Bucket=bucket, Key="k", Body=b"x")
    url = v4.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "k"}, ExpiresIn=1
    )
    time.sleep(2)
    assert requests.get(url).status_code == 403


def test_s3_conditional_delete_if_match(s3):
    """DeleteObject with a non-matching If-Match is rejected 412 (object
    survives); DeleteObjects reports a stale ETag under Errors, not Deleted.
    Regression for #1322 defect 7 (delete side)."""
    bucket = "intg-s3-conditional-delete"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="cond", Body=b"x")

    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bucket, Key="cond", IfMatch="badetag")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert s3.get_object(Bucket=bucket, Key="cond")["Body"].read() == b"x"

    resp = s3.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": "cond", "ETag": "badetag"}]}
    )
    assert resp.get("Errors") and resp["Errors"][0]["Code"] == "PreconditionFailed"
    assert not resp.get("Deleted")
    assert s3.get_object(Bucket=bucket, Key="cond")["Body"].read() == b"x"


def test_s3_bucket_owner_id_is_consistent_and_canonical(s3):
    """The canonical owner ID is one stable 64-hex value across ListBuckets,
    GetBucketAcl and GetObjectAcl — not the account id or a placeholder (#1459)."""
    import re
    bkt = "intg-owner-1459"
    s3.create_bucket(Bucket=bkt)
    try:
        s3.put_object(Bucket=bkt, Key="k", Body=b"x")
        lb = s3.list_buckets()["Owner"]["ID"]
        bacl = s3.get_bucket_acl(Bucket=bkt)["Owner"]["ID"]
        oacl = s3.get_object_acl(Bucket=bkt, Key="k")["Owner"]["ID"]
        assert lb == bacl == oacl, (lb, bacl, oacl)
        assert re.fullmatch(r"[0-9a-f]{64}", lb), lb
    finally:
        s3.delete_object(Bucket=bkt, Key="k")
        s3.delete_bucket(Bucket=bkt)


def test_s3_conditional_delete_needs_a_current_object(s3):
    """A conditional delete is evaluated against the current version and nothing
    else: "Conditional delete evaluations only apply to the current version of the
    object." With no current object there is nothing for the condition to hold
    against, so S3 answers 404 NoSuchKey whatever the If-Match carries. Measured
    against S3 in eu-west-1; the guide's 412 for the delete-marker case is a slip
    the measurement overrides."""
    bucket = "intg-s3-cond-delete-current"
    s3.create_bucket(Bucket=bucket)

    # Never written.
    for condition in ("*", "badetag"):
        with pytest.raises(ClientError) as exc:
            s3.delete_object(Bucket=bucket, Key="gone", IfMatch=condition)
        assert exc.value.response["Error"]["Code"] == "NoSuchKey", condition
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Present: a wrong ETag fails and the object survives, the right one and `*` win.
    etag = s3.put_object(Bucket=bucket, Key="here", Body=b"x")["ETag"]
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bucket, Key="here", IfMatch="badetag")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert s3.get_object(Bucket=bucket, Key="here")["Body"].read() == b"x"
    assert s3.delete_object(Bucket=bucket, Key="here",
                            IfMatch="*")["ResponseMetadata"]["HTTPStatusCode"] == 204

    s3.put_object(Bucket=bucket, Key="exact", Body=b"y")
    exact = s3.head_object(Bucket=bucket, Key="exact")["ETag"]
    assert s3.delete_object(Bucket=bucket, Key="exact",
                            IfMatch=exact)["ResponseMetadata"]["HTTPStatusCode"] == 204
    assert etag  # the put's ETag round-tripped

    s3.delete_bucket(Bucket=bucket)


def test_s3_conditional_delete_does_not_look_under_a_delete_marker(s3):
    """S3 does not peek beneath a delete marker: a key whose history still holds a
    real version is as absent as one that was never written."""
    bucket = "intg-s3-cond-delete-marker"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket,
                             VersioningConfiguration={"Status": "Enabled"})
    try:
        put = s3.put_object(Bucket=bucket, Key="obj", Body=b"v1")
        live_etag = put["ETag"]
        s3.delete_object(Bucket=bucket, Key="obj")  # lays a delete marker on top

        for condition in ("*", "badetag", live_etag):
            with pytest.raises(ClientError) as exc:
                s3.delete_object(Bucket=bucket, Key="obj", IfMatch=condition)
            assert exc.value.response["Error"]["Code"] == "NoSuchKey", condition
        # The real version is still there underneath, untouched.
        versions = s3.list_object_versions(Bucket=bucket, Prefix="obj")
        assert len(versions.get("Versions", [])) == 1

        # Markers only, which is what a delete of an absent key leaves behind.
        s3.delete_object(Bucket=bucket, Key="never")
        for condition in ("*", "badetag"):
            with pytest.raises(ClientError) as exc:
                s3.delete_object(Bucket=bucket, Key="never", IfMatch=condition)
            assert exc.value.response["Error"]["Code"] == "NoSuchKey", condition
    finally:
        for v in (s3.list_object_versions(Bucket=bucket).get("Versions", [])
                  + s3.list_object_versions(Bucket=bucket).get("DeleteMarkers", [])):
            s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
        s3.delete_bucket(Bucket=bucket)


def test_s3_batch_conditional_delete_reports_a_missing_key_not_found(s3):
    """DeleteObjects: "If the object doesn't exist when evaluating either of the
    preconditions, S3 rejects the request and returns a Not Found error response",
    and `*` in the ETag element asks only that a current object exist."""
    bucket = "intg-s3-cond-delete-batch"
    s3.create_bucket(Bucket=bucket)
    try:
        resp = s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": "gone", "ETag": "*"}]})
        assert resp["Errors"][0]["Code"] == "NoSuchKey"
        assert not resp.get("Deleted")

        s3.put_object(Bucket=bucket, Key="here", Body=b"x")
        resp = s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": "here", "ETag": "*"}]})
        assert [d["Key"] for d in resp.get("Deleted", [])] == ["here"]
        assert not resp.get("Errors")
    finally:
        s3.delete_bucket(Bucket=bucket)


def test_s3_copy_object_specific_version(s3):
    """CopyObject with a source ?versionId copies that exact version, not the
    current object. Regression for #1328."""
    bucket = "intg-s3-copy-version"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"VERSION-ONE")["VersionId"]
    s3.put_object(Bucket=bucket, Key="k", Body=b"VERSION-TWO")

    r = s3.copy_object(
        Bucket=bucket, Key="copy",
        CopySource={"Bucket": bucket, "Key": "k", "VersionId": v1},
    )
    assert r["CopySourceVersionId"] == v1
    assert s3.get_object(Bucket=bucket, Key="copy")["Body"].read() == b"VERSION-ONE"

    # Copy without a versionId still takes the current version.
    s3.copy_object(Bucket=bucket, Key="copy2", CopySource={"Bucket": bucket, "Key": "k"})
    assert s3.get_object(Bucket=bucket, Key="copy2")["Body"].read() == b"VERSION-TWO"

    # A non-existent source version is rejected.
    with pytest.raises(ClientError) as exc:
        s3.copy_object(
            Bucket=bucket, Key="copy-bad",
            CopySource={"Bucket": bucket, "Key": "k", "VersionId": "does-not-exist"},
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchVersion"


def test_s3_head_object_specific_version(s3):
    """HeadObject with a versionId returns that version's metadata, not the
    current object's (same versionId-dropping class as #1328)."""
    bucket = "intg-s3-head-version"
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"AAAAA")["VersionId"]        # 5 bytes
    s3.put_object(Bucket=bucket, Key="k", Body=b"BBBBBBBBBB")                     # 10 bytes

    h = s3.head_object(Bucket=bucket, Key="k", VersionId=v1)
    assert h["ContentLength"] == 5
    assert h["VersionId"] == v1

    # HEAD carries no body, so boto3 surfaces a bad version as a 404.
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bucket, Key="k", VersionId="does-not-exist")
    assert exc.value.response["Error"]["Code"] in ("NoSuchVersion", "404")


def test_s3_list_encoding_type_url_preserves_slash(s3):
    """`encoding-type=url` percent-encodes spaces and `+` but leaves `/` intact,
    so delimiter-collapsed CommonPrefixes/Delimiter stay readable — matching real
    S3 and RGW. (#1322, defect 4)"""
    import urllib.request

    bkt = f"enc-url-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="foo+1/bar", Body=b"x")
    s3.put_object(Bucket=bkt, Key="quux ab/xyz", Body=b"x")
    s3.put_object(Bucket=bkt, Key="asdf", Body=b"x")

    # Raw GET (boto3 auto-decodes the response, hiding the wire form). A dummy
    # Authorization header keeps the request off the anonymous path; the signature
    # is not verified.
    url = f"{ENDPOINT}/{bkt}?delimiter=/&encoding-type=url"
    req = urllib.request.Request(url, headers={
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, "
                         "SignedHeaders=host, Signature=0",
    })
    body = urllib.request.urlopen(req, timeout=5).read().decode()

    assert "<Delimiter>/</Delimiter>" in body, body
    assert "<Prefix>foo%2B1/</Prefix>" in body, body
    assert "<Prefix>quux%20ab/</Prefix>" in body, body
    assert "%2F" not in body, f"forward slash was percent-encoded: {body}"


def test_s3_complete_multipart_location_uses_request_host(s3):
    """CompleteMultipartUpload's Location echoes the endpoint the client reached,
    so it is correct on any port/host rather than a hard-coded localhost:4566.
    (#1322, smaller wire omission)"""
    from urllib.parse import urlparse

    bkt = f"cmu-loc-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    u = s3.create_multipart_upload(Bucket=bkt, Key="k")["UploadId"]
    etag = s3.upload_part(Bucket=bkt, Key="k", UploadId=u, PartNumber=1, Body=b"hello")["ETag"]
    resp = s3.complete_multipart_upload(
        Bucket=bkt, Key="k", UploadId=u,
        MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]})
    loc = resp["Location"]
    host = urlparse(ENDPOINT).netloc
    assert host in loc, f"Location {loc!r} should reflect the request host {host!r}"
    assert loc.endswith(f"/{bkt}/k"), loc


def test_s3_content_md5_invalid_vs_bad_digest(s3):
    """A malformed or wrong-length Content-MD5 is InvalidDigest; only a well-formed
    16-byte digest that mismatches the body is BadDigest. (#1322, smaller wire omission)"""
    import base64
    import hashlib

    bkt = f"md5-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)

    # Wrong-length digest (valid base64, 3 bytes) -> InvalidDigest.
    with pytest.raises(ClientError) as e1:
        s3.put_object(Bucket=bkt, Key="k", Body=b"hello", ContentMD5="1234")
    assert e1.value.response["Error"]["Code"] == "InvalidDigest", e1.value.response["Error"]

    # Well-formed 16-byte digest that does not match the body -> BadDigest.
    wrong = base64.b64encode(hashlib.md5(b"other").digest()).decode()
    with pytest.raises(ClientError) as e2:
        s3.put_object(Bucket=bkt, Key="k", Body=b"hello", ContentMD5=wrong)
    assert e2.value.response["Error"]["Code"] == "BadDigest", e2.value.response["Error"]

    # Correct digest succeeds.
    good = base64.b64encode(hashlib.md5(b"hello").digest()).decode()
    s3.put_object(Bucket=bkt, Key="k", Body=b"hello", ContentMD5=good)


def test_s3_put_object_echoes_bucket_default_sse(s3):
    """After PutBucketEncryption, PutObject echoes the applied SSE algorithm on the
    reply (AES256, or aws:kms + key id). (#1322, smaller wire omission)"""
    bkt = f"sse-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_encryption(Bucket=bkt, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    r = s3.put_object(Bucket=bkt, Key="k", Body=b"x")
    assert r.get("ServerSideEncryption") == "AES256", r

    bkt2 = f"sse-kms-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt2)
    s3.put_bucket_encryption(Bucket=bkt2, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {
            "SSEAlgorithm": "aws:kms",
            "KMSMasterKeyID": "arn:aws:kms:us-east-1:000000000000:key/abc"}}]})
    r2 = s3.put_object(Bucket=bkt2, Key="k", Body=b"x")
    assert r2.get("ServerSideEncryption") == "aws:kms", r2
    assert r2.get("SSEKMSKeyId", "").endswith("key/abc"), r2

    # A bucket without explicit encryption does not stamp the header here.
    bkt3 = f"noenc-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt3)
    r3 = s3.put_object(Bucket=bkt3, Key="k", Body=b"x")
    assert "ServerSideEncryption" not in r3, r3


def test_s3_copy_object_source_date_preconditions(s3):
    """CopyObject honours the copy-source date preconditions with AWS precedence:
    If-Match beats If-Unmodified-Since, If-None-Match beats If-Modified-Since.
    (#1322, rest of defect 7)"""
    from datetime import datetime, timedelta, timezone

    bkt = f"copycond-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    src_etag = s3.put_object(Bucket=bkt, Key="src", Body=b"data")["ETag"]
    past = datetime.now(timezone.utc) - timedelta(days=1)
    future = datetime.now(timezone.utc) + timedelta(days=1)

    def copy(**kw):
        return s3.copy_object(Bucket=bkt, Key="dst",
                              CopySource={"Bucket": bkt, "Key": "src"}, **kw)

    # If-Unmodified-Since in the past -> source modified after -> 412.
    with pytest.raises(ClientError) as e:
        copy(CopySourceIfUnmodifiedSince=past)
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    # In the future -> succeeds.
    copy(CopySourceIfUnmodifiedSince=future)

    # If-Modified-Since in the future -> not modified since -> 412.
    with pytest.raises(ClientError) as e2:
        copy(CopySourceIfModifiedSince=future)
    assert e2.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    # In the past -> succeeds.
    copy(CopySourceIfModifiedSince=past)

    # Precedence: If-Match true wins over a failing If-Unmodified-Since -> copies.
    copy(CopySourceIfMatch=src_etag, CopySourceIfUnmodifiedSince=past)

    # Precedence: If-None-Match matching (fails) wins over If-Modified-Since -> 412.
    with pytest.raises(ClientError) as e3:
        copy(CopySourceIfNoneMatch=src_etag, CopySourceIfModifiedSince=past)
    assert e3.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def _conditional_read_bucket(s3):
    """A bucket holding one object, with its ETag and Last-Modified."""
    bkt = f"cond-read-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    etag = s3.put_object(Bucket=bkt, Key="obj", Body=b"payload")["ETag"]
    mtime = s3.head_object(Bucket=bkt, Key="obj")["LastModified"]
    return bkt, etag, mtime


def test_s3_get_object_etag_preconditions(s3):
    """GetObject answers If-Match and If-None-Match instead of always returning
    the body: a stale If-Match is 412, and a current If-None-Match is 304."""
    bkt, etag, _ = _conditional_read_bucket(s3)

    # The condition holds -> the read proceeds as normal.
    assert s3.get_object(Bucket=bkt, Key="obj", IfMatch=etag)["Body"].read() == b"payload"
    assert s3.get_object(Bucket=bkt, Key="obj", IfMatch="*")["Body"].read() == b"payload"
    assert s3.get_object(Bucket=bkt, Key="obj", IfNoneMatch='"ABCORZ"')["Body"].read() == b"payload"

    # If-Match against some other representation -> 412, nothing transferred.
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bkt, Key="obj", IfMatch='"ABCORZ"')
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert e.value.response["Error"]["Code"] == "PreconditionFailed"

    # If-None-Match naming the current representation -> the caller's copy is
    # current, so 304 and no body. The ETag confirms which one they revalidated.
    for value in (etag, "*", f'"nomatch", {etag}', f"W/{etag}"):
        with pytest.raises(ClientError) as e2:
            s3.get_object(Bucket=bkt, Key="obj", IfNoneMatch=value)
        resp = e2.value.response
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 304, value
        assert resp["ResponseMetadata"]["HTTPHeaders"]["etag"] == etag, value


def test_s3_get_object_date_preconditions(s3):
    """GetObject answers If-Modified-Since and If-Unmodified-Since: unchanged
    since the caller's timestamp is 304, changed after it is 412."""
    from datetime import timedelta

    bkt, etag, mtime = _conditional_read_bucket(s3)
    before = mtime - timedelta(days=1)
    after = mtime + timedelta(seconds=1)

    # Modified since a day ago (true) and unmodified since a second from now
    # (true) both leave the read alone.
    assert s3.get_object(Bucket=bkt, Key="obj", IfModifiedSince=before)["Body"].read() == b"payload"
    assert s3.get_object(Bucket=bkt, Key="obj", IfUnmodifiedSince=after)["Body"].read() == b"payload"

    # Not modified since a second from now -> 304.
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bkt, Key="obj", IfModifiedSince=after)
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304
    assert e.value.response["ResponseMetadata"]["HTTPHeaders"]["etag"] == etag

    # Modified since a day ago -> the If-Unmodified-Since condition fails -> 412.
    with pytest.raises(ClientError) as e2:
        s3.get_object(Bucket=bkt, Key="obj", IfUnmodifiedSince=before)
    assert e2.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
    assert e2.value.response["Error"]["Code"] == "PreconditionFailed"


def test_s3_read_precondition_precedence(s3):
    """An entity tag beats its date counterpart, per RFC 9110 13.2.2 and the AWS
    GetObject reference: the date header applies only when the tag is absent."""
    from datetime import timedelta

    bkt, etag, mtime = _conditional_read_bucket(s3)
    before = mtime - timedelta(days=1)
    after = mtime + timedelta(seconds=1)

    # If-Match holds, so the failing If-Unmodified-Since is not consulted -> 200.
    resp = s3.get_object(Bucket=bkt, Key="obj", IfMatch=etag, IfUnmodifiedSince=before)
    assert resp["Body"].read() == b"payload"

    # If-None-Match matches, so the satisfied If-Modified-Since is not
    # consulted -> 304 rather than the body.
    with pytest.raises(ClientError) as e:
        s3.get_object(Bucket=bkt, Key="obj", IfNoneMatch=etag, IfModifiedSince=before)
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    # The tag is consulted even when it does not decide the read: a present
    # If-None-Match that misses suppresses If-Modified-Since outright, so the
    # body comes back although that date alone would have meant 304.
    resp = s3.get_object(Bucket=bkt, Key="obj", IfNoneMatch='"ABCORZ"', IfModifiedSince=after)
    assert resp["Body"].read() == b"payload"


def test_s3_head_object_preconditions(s3):
    """HeadObject evaluates the same four conditions as GetObject."""
    from datetime import timedelta

    bkt, etag, mtime = _conditional_read_bucket(s3)

    assert s3.head_object(Bucket=bkt, Key="obj", IfMatch=etag)["ETag"] == etag

    with pytest.raises(ClientError) as e:
        s3.head_object(Bucket=bkt, Key="obj", IfMatch='"ABCORZ"')
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    with pytest.raises(ClientError) as e2:
        s3.head_object(Bucket=bkt, Key="obj", IfNoneMatch=etag)
    assert e2.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    with pytest.raises(ClientError) as e3:
        s3.head_object(Bucket=bkt, Key="obj", IfUnmodifiedSince=mtime - timedelta(days=1))
    assert e3.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_s3_not_modified_carries_no_payload(s3):
    """Checked on the wire, not through boto3: a 304 ends at the header section
    and can never carry a body (RFC 9110 15.4.5), so it must not describe one
    either. A stray body would desynchronise the next request on the connection."""
    import http.client

    bkt, etag, _ = _conditional_read_bucket(s3)
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 4566, timeout=10)
    # Checksum mode is on, as boto3 turns it on by default, so the checksum
    # headers below are ones the 200 really would have carried.
    conn.request("GET", f"/{bkt}/obj",
                 headers={"If-None-Match": etag, "x-amz-checksum-mode": "ENABLED"})
    resp = conn.getresponse()
    body = resp.read()

    assert resp.status == 304
    assert body == b""
    assert resp.getheader("ETag") == etag
    assert resp.getheader("Last-Modified")
    for absent in ("Content-Type", "Content-Encoding", "Accept-Ranges"):
        assert resp.getheader(absent) is None, absent
    assert (resp.getheader("Content-Length") or "0") == "0"
    # A checksum of bytes that were not sent belongs to no one: boto3 asks for
    # these by default and chokes on the empty body if it gets one back.
    assert not [h for h, _ in resp.getheaders() if h.lower().startswith("x-amz-checksum-")]

    # The connection is still usable, which is the point of getting the framing
    # right: a second request on it reads its own response.
    conn.request("GET", f"/{bkt}/obj")
    second = conn.getresponse()
    assert second.status == 200
    assert second.read() == b"payload"
    conn.close()


def test_s3_canned_acl_expands_to_group_grants(s3):
    """A canned ACL (at PutObject or PutObjectAcl) expands to the group grants it
    implies: public-read -> AllUsers READ; authenticated-read -> AuthenticatedUsers
    READ; plus the owner's FULL_CONTROL. Invalid canned values are rejected. (#1322)"""
    bkt = f"acl-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)

    def grants(key):
        acl = s3.get_object_acl(Bucket=bkt, Key=key)
        return [(g["Grantee"].get("URI", g["Grantee"].get("ID")), g["Permission"])
                for g in acl["Grants"]]

    # Canned ACL at PutObject time.
    s3.put_object(Bucket=bkt, Key="pub", Body=b"x", ACL="public-read")
    g = grants("pub")
    assert ("http://acs.amazonaws.com/groups/global/AllUsers", "READ") in g, g
    assert any(perm == "FULL_CONTROL" for _, perm in g), g

    # Canned ACL via PutObjectAcl subresource.
    s3.put_object(Bucket=bkt, Key="auth", Body=b"x")
    s3.put_object_acl(Bucket=bkt, Key="auth", ACL="authenticated-read")
    assert ("http://acs.amazonaws.com/groups/global/AuthenticatedUsers", "READ") in grants("auth")

    # public-read-write adds AllUsers WRITE too.
    s3.put_object(Bucket=bkt, Key="rw", Body=b"x", ACL="public-read-write")
    grw = grants("rw")
    assert ("http://acs.amazonaws.com/groups/global/AllUsers", "WRITE") in grw, grw

    # Invalid canned ACL is rejected.
    with pytest.raises(ClientError) as e:
        s3.put_object(Bucket=bkt, Key="bad", Body=b"x", ACL="nonsense-acl")
    assert e.value.response["Error"]["Code"] == "InvalidArgument", e.value.response["Error"]


def test_s3_list_delimiter_next_marker_is_common_prefix(s3):
    """When a delimited page ends on a collapsed group, NextMarker is the common
    prefix (boo/), not an underlying key, and resuming from it walks the rest of
    the bucket without re-emitting the group. (#1322, defect 5)"""
    bkt = f"delim-nm-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    for k in ("asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"):
        s3.put_object(Bucket=bkt, Key=k, Body=b"x")

    # Page 1: marker=asdf, one row -> CommonPrefix boo/, NextMarker boo/.
    p1 = s3.list_objects(Bucket=bkt, Delimiter="/", MaxKeys=1, Marker="asdf")
    assert p1["IsTruncated"] is True
    assert [c["Prefix"] for c in p1.get("CommonPrefixes", [])] == ["boo/"]
    assert p1["NextMarker"] == "boo/"

    # Page 2: resume from boo/ -> cquux/ only, no repeat of boo/, terminates.
    p2 = s3.list_objects(Bucket=bkt, Delimiter="/", MaxKeys=1, Marker=p1["NextMarker"])
    assert [c["Prefix"] for c in p2.get("CommonPrefixes", [])] == ["cquux/"]
    assert p2.get("IsTruncated") is False

    # Full unpaginated listing: the top-level view is asdf + boo/ + cquux/.
    full = s3.list_objects(Bucket=bkt, Delimiter="/")
    assert [c["Key"] for c in full.get("Contents", [])] == ["asdf"]
    assert [c["Prefix"] for c in full.get("CommonPrefixes", [])] == ["boo/", "cquux/"]


# ---------------------------------------------------------------------------
# Glacier restore (RestoreObject / x-amz-restore / s3:ObjectRestore:*)
# ---------------------------------------------------------------------------

def test_s3_restore_object_glacier_flow(s3):
    s3.create_bucket(Bucket="qa-s3-restore")
    s3.put_object(Bucket="qa-s3-restore", Key="cold", Body=b"frozen",
                  StorageClass="GLACIER")

    # Archived and unrestored: GetObject fails InvalidObjectState (403,
    # carrying StorageClass); HeadObject keeps working — that asymmetry is
    # how a caller polls for the copy to land.
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket="qa-s3-restore", Key="cold")
    assert exc.value.response["Error"]["Code"] == "InvalidObjectState"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    assert exc.value.response["Error"]["StorageClass"] == "GLACIER"
    head = s3.head_object(Bucket="qa-s3-restore", Key="cold")
    assert head["StorageClass"] == "GLACIER"
    assert "Restore" not in head

    resp = s3.restore_object(
        Bucket="qa-s3-restore", Key="cold",
        RestoreRequest={"Days": 1, "GlacierJobParameters": {"Tier": "Bulk"}})
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 202
    assert s3.head_object(Bucket="qa-s3-restore", Key="cold")["Restore"] == \
        'ongoing-request="true"'
    with pytest.raises(ClientError) as exc:
        s3.restore_object(Bucket="qa-s3-restore", Key="cold",
                          RestoreRequest={"Days": 1})
    assert exc.value.response["Error"]["Code"] == "RestoreAlreadyInProgress"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    with pytest.raises(ClientError):
        s3.get_object(Bucket="qa-s3-restore", Key="cold")

    # After the fixed retrieval window the copy is readable and the header
    # flips to ongoing-request="false" with an expiry-date.
    time.sleep(2.5)
    head = s3.head_object(Bucket="qa-s3-restore", Key="cold")
    assert head["Restore"].startswith('ongoing-request="false", expiry-date="')
    got = s3.get_object(Bucket="qa-s3-restore", Key="cold")
    assert got["Body"].read() == b"frozen"
    assert got["Restore"].startswith('ongoing-request="false"')
    assert got["StorageClass"] == "GLACIER"

    # Re-requesting while the copy is live only extends the expiry: 200.
    resp = s3.restore_object(Bucket="qa-s3-restore", Key="cold",
                             RestoreRequest={"Days": 3})
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_s3_restore_object_active_tier_rejected(s3):
    s3.create_bucket(Bucket="qa-s3-restore-active")
    s3.put_object(Bucket="qa-s3-restore-active", Key="warm", Body=b"x")
    s3.put_object(Bucket="qa-s3-restore-active", Key="ir", Body=b"y",
                  StorageClass="GLACIER_IR")
    # Active tier (STANDARD): ObjectAlreadyInActiveTierError.
    with pytest.raises(ClientError) as exc:
        s3.restore_object(Bucket="qa-s3-restore-active", Key="warm",
                          RestoreRequest={"Days": 1})
    assert exc.value.response["Error"]["Code"] == \
        "ObjectAlreadyInActiveTierError"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    # GLACIER_IR: real AWS answers InvalidObjectState ("Restore is not
    # allowed for the object's current storage class."), not the
    # active-tier error.
    with pytest.raises(ClientError) as exc:
        s3.restore_object(Bucket="qa-s3-restore-active", Key="ir",
                          RestoreRequest={"Days": 1})
    assert exc.value.response["Error"]["Code"] == "InvalidObjectState"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    # GLACIER_IR is instant retrieval: readable without any restore.
    got = s3.get_object(Bucket="qa-s3-restore-active", Key="ir")
    assert got["Body"].read() == b"y"


def test_s3_restore_notifications_to_sqs(s3, sqs):
    s3.create_bucket(Bucket="qa-s3-restore-evt")
    queue_url = sqs.create_queue(QueueName="qa-s3-restore-evt-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket="qa-s3-restore-evt",
        NotificationConfiguration={
            "QueueConfigurations": [
                {"QueueArn": queue_arn, "Events": ["s3:ObjectRestore:*"]}],
        },
    )
    s3.put_object(Bucket="qa-s3-restore-evt", Key="cold", Body=b"z",
                  StorageClass="DEEP_ARCHIVE")
    s3.restore_object(Bucket="qa-s3-restore-evt", Key="cold",
                      RestoreRequest={"Days": 1})
    time.sleep(3)
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10,
                               WaitTimeSeconds=2)
    records = [
        json.loads(m["Body"])["Records"][0]
        for m in msgs.get("Messages", [])
        if "Records" in json.loads(m["Body"])
    ]
    names = {r["eventName"] for r in records}
    assert "ObjectRestore:Post" in names
    assert "ObjectRestore:Completed" in names
    completed = next(r for r in records
                     if r["eventName"] == "ObjectRestore:Completed")
    red = completed["glacierEventData"]["restoreEventData"]
    assert red["lifecycleRestoreStorageClass"] == "DEEP_ARCHIVE"
    assert red["lifecycleRestorationExpiryTime"].endswith("Z")


def test_s3_delete_bucket_refuses_while_versions_remain(s3):
    """A versioned bucket is not empty while any version or marker is left.

    The delete markers a versioning bucket writes hide its objects without
    removing them, so the bucket still holds every version and AWS refuses
    to delete it until they go by version id."""
    bkt = "s3-ver-notempty"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v1")
    s3.delete_object(Bucket=bkt, Key="doc.txt")

    assert s3.list_objects_v2(Bucket=bkt).get("KeyCount", 0) == 0
    with pytest.raises(ClientError) as exc:
        s3.delete_bucket(Bucket=bkt)
    assert exc.value.response["Error"]["Code"] == "BucketNotEmpty"

    for v in s3.list_object_versions(Bucket=bkt).get("Versions", []):
        s3.delete_object(Bucket=bkt, Key=v["Key"], VersionId=v["VersionId"])
    for m in s3.list_object_versions(Bucket=bkt).get("DeleteMarkers", []):
        s3.delete_object(Bucket=bkt, Key=m["Key"], VersionId=m["VersionId"])
    s3.delete_bucket(Bucket=bkt)


def test_s3_reading_a_delete_marker_is_refused(s3):
    """A delete marker has no content: reading one by id answers 405.

    The version is there, it simply cannot be read, which AWS distinguishes
    from a missing version by answering MethodNotAllowed rather than 404 --
    and it names the marker on the way out."""
    bkt = "s3-ver-marker-read"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v1")
    marker = s3.delete_object(Bucket=bkt, Key="doc.txt")["VersionId"]

    for op in (s3.get_object, s3.head_object):
        with pytest.raises(ClientError) as exc:
            op(Bucket=bkt, Key="doc.txt", VersionId=marker)
        meta = exc.value.response["ResponseMetadata"]
        assert meta["HTTPStatusCode"] == 405
        assert meta["HTTPHeaders"]["x-amz-delete-marker"] == "true"
        assert meta["HTTPHeaders"]["x-amz-version-id"] == marker


def test_s3_subresource_of_a_version_that_does_not_exist(s3):
    """An ACL or tag op naming a version that is gone answers NoSuchVersion.

    A version id addresses a version the way a key addresses an object, so
    one that has been deleted cannot read back the default policy as though
    it were simply unpermissioned."""
    bkt = "s3-ver-subresource"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v1")
    gone = s3.put_object(Bucket=bkt, Key="doc.txt", Body=b"v2")["VersionId"]
    s3.delete_object(Bucket=bkt, Key="doc.txt", VersionId=gone)

    for call in (
        lambda: s3.get_object_acl(Bucket=bkt, Key="doc.txt", VersionId=gone),
        lambda: s3.get_object_tagging(Bucket=bkt, Key="doc.txt", VersionId=gone),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "NoSuchVersion"

    # The version that is still there answers normally.
    current = s3.head_object(Bucket=bkt, Key="doc.txt")["VersionId"]
    assert s3.get_object_acl(Bucket=bkt, Key="doc.txt", VersionId=current)["Grants"]


def test_s3_list_object_versions_rolls_up_prefixes(s3):
    """A delimiter groups versions exactly as it groups a plain listing."""
    bkt = "s3-ver-delimiter"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="dir/one.txt", Body=b"a")
    s3.put_object(Bucket=bkt, Key="dir/one.txt", Body=b"b")
    s3.put_object(Bucket=bkt, Key="dir/two.txt", Body=b"c")
    s3.put_object(Bucket=bkt, Key="top.txt", Body=b"d")

    resp = s3.list_object_versions(Bucket=bkt, Delimiter="/")
    assert [p["Prefix"] for p in resp.get("CommonPrefixes", [])] == ["dir/"]
    # The rolled-up keys' versions are not listed beside the group.
    assert [v["Key"] for v in resp.get("Versions", [])] == ["top.txt"]
    assert resp["Delimiter"] == "/"

    # Without the delimiter every version is listed as before.
    plain = s3.list_object_versions(Bucket=bkt)
    assert len(plain.get("Versions", [])) == 4
    assert not plain.get("CommonPrefixes")
def test_s3_copy_object_applies_canned_acl(s3):
    """x-amz-acl on a copy permissions the destination, as it does on a put."""
    bkt = "s3-copy-acl"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="src.txt", Body=b"body")

    s3.copy_object(Bucket=bkt, Key="dst.txt", ACL="public-read",
                   CopySource={"Bucket": bkt, "Key": "src.txt"})
    grants = s3.get_object_acl(Bucket=bkt, Key="dst.txt")["Grants"]
    assert any(g.get("Grantee", {}).get("URI", "").endswith("AllUsers")
               for g in grants)

    # A copy without one leaves the destination private.
    s3.copy_object(Bucket=bkt, Key="plain.txt",
                   CopySource={"Bucket": bkt, "Key": "src.txt"})
    grants = s3.get_object_acl(Bucket=bkt, Key="plain.txt")["Grants"]
    assert not any(g.get("Grantee", {}).get("URI", "").endswith("AllUsers")
                   for g in grants)

    with pytest.raises(ClientError) as exc:
        s3.copy_object(Bucket=bkt, Key="bad.txt", ACL="nonsense",
                       CopySource={"Bucket": bkt, "Key": "src.txt"})
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


def test_s3_upload_part_copy_honours_source_preconditions(s3):
    """UploadPartCopy carries the same copy-source conditions CopyObject does."""
    bkt = "s3-upc-precond"
    s3.create_bucket(Bucket=bkt)
    body = b"x" * (5 * 1024 * 1024)
    etag = s3.put_object(Bucket=bkt, Key="src.bin", Body=body)["ETag"]

    upload = s3.create_multipart_upload(Bucket=bkt, Key="dst.bin")["UploadId"]
    with pytest.raises(ClientError) as exc:
        s3.upload_part_copy(Bucket=bkt, Key="dst.bin", UploadId=upload,
                            PartNumber=1,
                            CopySource={"Bucket": bkt, "Key": "src.bin"},
                            CopySourceIfMatch='"00000000000000000000000000000000"')
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    with pytest.raises(ClientError) as exc:
        s3.upload_part_copy(Bucket=bkt, Key="dst.bin", UploadId=upload,
                            PartNumber=1,
                            CopySource={"Bucket": bkt, "Key": "src.bin"},
                            CopySourceIfNoneMatch=etag)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    # The condition that holds copies the range.
    part = s3.upload_part_copy(Bucket=bkt, Key="dst.bin", UploadId=upload,
                               PartNumber=1,
                               CopySource={"Bucket": bkt, "Key": "src.bin"},
                               CopySourceIfMatch=etag)
    assert part["CopyPartResult"]["ETag"]
    s3.abort_multipart_upload(Bucket=bkt, Key="dst.bin", UploadId=upload)
def test_s3_put_object_rejects_a_mismatched_checksum(s3):
    """A supplied checksum is verified against the body, not just stored.

    The client sends one to have the object checked; storing an unverified
    value would echo it on every later read as though it had been."""
    bkt = "s3-checksum-mismatch"
    s3.create_bucket(Bucket=bkt)

    with pytest.raises(ClientError) as exc:
        s3.put_object(Bucket=bkt, Key="bad.txt", Body=b"hello",
                      ChecksumSHA256="q6MvHqxNQz8xzL6UcqhbSY9dAMYo5S3VZjjuQFDJq4c=")
    assert exc.value.response["Error"]["Code"] == "BadDigest"

    # The rejected put stored nothing.
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bkt, Key="bad.txt")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # The matching value is accepted and echoed back.
    good = "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="
    s3.put_object(Bucket=bkt, Key="good.txt", Body=b"hello", ChecksumSHA256=good)
    head = s3.head_object(Bucket=bkt, Key="good.txt", ChecksumMode="ENABLED")
    assert head["ChecksumSHA256"] == good
