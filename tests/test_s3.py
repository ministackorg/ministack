import asyncio
import base64
import contextlib
import hashlib
import http.client
import json
import os
import re
import struct
import time
import urllib.error
import urllib.request
import uuid
import uuid as _uuid_mod
from datetime import datetime
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlsplit
from xml.etree import ElementTree
from xml.etree.ElementTree import fromstring

import boto3
import pytest
import requests
from botocore import UNSIGNED
from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from conftest import (
    inprocess_iam_user,
    make_client,
    patch_endpoint_dns,
    routed_request,
    second_access_key,
    sqs_policy_allow_s3,
)

from ministack.core.responses import AccountScopedDict, request_scope
from ministack.services import iam as _iam_mod
from ministack.services import s3 as _s3_mod
from ministack.services import sts as _sts_mod

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

def _create_bucket_error(client, **kwargs):
    with pytest.raises(ClientError) as exc:
        client.create_bucket(**kwargs)
    return exc.value.response


def test_s3_location_constraint_required_outside_us_east_1(s3):
    """A regional endpoint other than us-east-1 refuses CreateBucket without a
    LocationConstraint naming its own region (observed on AWS 2026-10-12)."""
    euw1 = _regional_client("s3", "eu-west-1")
    bkt = f"intg-s3-loc-signing-{_uuid_mod.uuid4().hex[:8]}"
    resp = _create_bucket_error(euw1, Bucket=bkt)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert resp["Error"]["Code"] == "IllegalLocationConstraintException"
    assert resp["Error"]["Message"] == ("The unspecified location constraint is incompatible for the "
                                        "region specific endpoint this request was sent to.")

    resp = _create_bucket_error(euw1, Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    assert resp["Error"]["Code"] == "IllegalLocationConstraintException"
    assert resp["Error"]["Message"] == ("The us-west-2 location constraint is incompatible for the "
                                        "region specific endpoint this request was sent to.")
    with pytest.raises(ClientError):
        s3.head_bucket(Bucket=bkt)

    euw1.create_bucket(Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    try:
        assert euw1.get_bucket_location(Bucket=bkt)["LocationConstraint"] == "eu-west-1"
        # The constraint check precedes BucketAlreadyOwnedByYou.
        resp = _create_bucket_error(euw1, Bucket=bkt)
        assert resp["Error"]["Code"] == "IllegalLocationConstraintException"
        resp = _create_bucket_error(euw1, Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
        assert resp["Error"]["Code"] == "BucketAlreadyOwnedByYou"
        assert resp["Error"]["BucketName"] == bkt
    finally:
        euw1.delete_bucket(Bucket=bkt)


def test_s3_location_constraint_eu_is_eu_west_1(s3):
    """EU is the legacy LocationConstraint for eu-west-1: eu-west-1 accepts it
    and GetBucketLocation answers EU, other regional endpoints refuse it
    (observed on AWS 2026-10-20)."""
    euw1 = _regional_client("s3", "eu-west-1")
    bkt = f"intg-s3-loc-eu-{_uuid_mod.uuid4().hex[:8]}"
    euw1.create_bucket(Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "EU"})
    try:
        assert euw1.get_bucket_location(Bucket=bkt)["LocationConstraint"] == "EU"
        euw1.put_object(Bucket=bkt, Key="k", Body=b"x")
        assert euw1.get_object(Bucket=bkt, Key="k")["Body"].read() == b"x"
    finally:
        euw1.delete_object(Bucket=bkt, Key="k")
        euw1.delete_bucket(Bucket=bkt)

    euc1 = _regional_client("s3", "eu-central-1")
    resp = _create_bucket_error(euc1, Bucket=f"intg-s3-loc-eu-c1-{_uuid_mod.uuid4().hex[:8]}",
                                CreateBucketConfiguration={"LocationConstraint": "EU"})
    assert resp["Error"]["Code"] == "IllegalLocationConstraintException"
    assert resp["Error"]["Message"] == ("The EU location constraint is incompatible for the "
                                        "region specific endpoint this request was sent to.")


def test_s3_location_constraint_us_east_1_is_invalid(s3):
    bkt = f"intg-s3-loc-use1-{_uuid_mod.uuid4().hex[:8]}"
    for client in (s3, _regional_client("s3", "eu-west-1")):
        resp = _create_bucket_error(client, Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "us-east-1"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert resp["Error"]["Code"] == "InvalidLocationConstraint"
        assert resp["Error"]["Message"] == "The specified location-constraint is not valid"
        assert resp["Error"]["LocationConstraint"] == "us-east-1"


def test_s3_location_constraint_internal_callers_unchanged():
    """Internal callers (Glue warehouse, Transcribe) pass no endpoint region and
    keep landing the bucket in the request region without a constraint."""
    import contextvars

    from ministack.core.responses import set_request_region
    from ministack.services import s3 as s3_svc

    name = f"intg-s3-loc-internal-{_uuid_mod.uuid4().hex[:8]}"

    def create_in_eu_west_1():
        set_request_region("eu-west-1")
        try:
            assert s3_svc._create_bucket(name, b"", {})[0] == 200
            assert s3_svc.bucket_region(name) == "eu-west-1"
        finally:
            s3_svc._buckets.pop(name, None)

    contextvars.copy_context().run(create_in_eu_west_1)


def test_s3_location_constraint_bucket_already_exists_names_bucket(s3):
    import boto3

    other = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                         aws_access_key_id="111122223333", aws_secret_access_key="test")
    bkt = f"intg-s3-loc-taken-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    try:
        resp = _create_bucket_error(other, Bucket=bkt)
        assert resp["Error"]["Code"] == "BucketAlreadyExists"
        assert resp["Error"]["BucketName"] == bkt
    finally:
        s3.delete_bucket(Bucket=bkt)


def test_s3_location_constraint_us_east_1_endpoint_accepts_any_region(s3):
    bkt = f"intg-s3-loc-any-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt, CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    try:
        assert s3.get_bucket_location(Bucket=bkt)["LocationConstraint"] == "ap-southeast-2"
    finally:
        _regional_client("s3", "ap-southeast-2").delete_bucket(Bucket=bkt)

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


@pytest.mark.auth_contract
@pytest.mark.parametrize("bucket,operation,action,reason", [
    ("other", "list_tags_for_resource", "ListTagsForResource", None),
    ("other", "tag_resource", "TagResource", None),
    ("other", "untag_resource", "UntagResource", None),
    ("allowed", "untag_resource", "UntagResource", "with an explicit deny in an identity-based policy"),
], ids=["list", "tag", "untag", "explicit-deny"])
def test_s3_control_tagging_authorization_contract(auth_enabled, iam_user, s3, bucket, operation, action, reason):
    """S3 Control tagging calls are authorized as s3:<operation> on the bucket."""
    name = f"s3c-auth-{_uuid_mod.uuid4().hex[:8]}"
    for suffix in ("allowed", "other"):
        s3.create_bucket(Bucket=f"{name}-{suffix}")
    try:
        user = iam_user(policy={"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListTagsForResource", "s3:TagResource"],
             "Resource": f"arn:aws:s3:::{name}-allowed"},
            {"Effect": "Deny", "Action": "s3:UntagResource", "Resource": f"arn:aws:s3:::{name}-allowed"},
        ]})
        control = user.client("s3control", config=Config(inject_host_prefix=False))
        allowed = f"arn:aws:s3:::{name}-allowed"
        control.tag_resource(AccountId="000000000000", ResourceArn=allowed, Tags=[{"Key": "k", "Value": "v"}])
        assert control.list_tags_for_resource(AccountId="000000000000", ResourceArn=allowed)["Tags"] == [
            {"Key": "k", "Value": "v"}]
        arn = f"arn:aws:s3:::{name}-{bucket}"
        params = {"tag_resource": {"Tags": [{"Key": "k", "Value": "v"}]}, "untag_resource": {"TagKeys": ["k"]}}
        call = getattr(control, operation)
        if not auth_enabled:
            call(AccountId="000000000000", ResourceArn=arn, **params.get(operation, {}))
            return
        with pytest.raises(ClientError) as exc:
            call(AccountId="000000000000", ResourceArn=arn, **params.get(operation, {}))
        reason = reason or f"because no identity-based policy allows the s3:{action} action"
        response = exc.value.response
        assert (response["ResponseMetadata"]["HTTPStatusCode"], response["Error"]["Code"]) == (403, "AccessDenied")
        assert response["Error"]["Message"] == (
            f'User: {user.arn} is not authorized to perform: s3:{action} on resource: "{arn}" {reason}')
        assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == "application/xml"
    finally:
        for suffix in ("allowed", "other"):
            s3.delete_bucket(Bucket=f"{name}-{suffix}")


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


def _policy_bucket(effect, principal):
    """A bucket with one object and a bucket policy statement on s3:GetObject."""
    bucket = f"rp-{uuid.uuid4().hex[:10]}"
    assert _lock_call("PUT", f"/{bucket}")[0] == 200
    assert _lock_call("PUT", f"/{bucket}/k", body=b"x")[0] == 200
    policy = {"Version": "2012-10-17", "Statement": [{
        "Effect": effect, "Principal": {"AWS": principal}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:::{bucket}/*"}]}
    assert _lock_call("PUT", f"/{bucket}", body=json.dumps(policy).encode(), query={"policy": [""]})[0] == 204
    return bucket


_ALLOW_S3 = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}


def test_bucket_policy_naming_the_user_allows_without_identity_policy(auth):
    key, user = inprocess_iam_user()
    bucket = _policy_bucket("Allow", user)
    assert routed_request(key, "s3", "GET", f"/{bucket}/k")[0] == 200


def test_bucket_policy_naming_the_account_needs_an_identity_policy(auth):
    bucket = _policy_bucket("Allow", "arn:aws:iam::000000000000:root")
    key, _user = inprocess_iam_user()
    status, _headers, body = routed_request(key, "s3", "GET", f"/{bucket}/k")
    assert status == 403 and b"<Code>AccessDenied</Code>" in body
    key, _user = inprocess_iam_user(_ALLOW_S3)
    assert routed_request(key, "s3", "GET", f"/{bucket}/k")[0] == 200


def test_bucket_policy_deny_beats_an_identity_allow(auth):
    import xml.etree.ElementTree as ET

    key, user = inprocess_iam_user(_ALLOW_S3)
    bucket = _policy_bucket("Deny", user)
    status, _headers, body = routed_request(key, "s3", "GET", f"/{bucket}/k")
    assert status == 403 and b"<Code>AccessDenied</Code>" in body
    assert ET.fromstring(body).findtext("Message") == (
        f'User: {user} is not authorized to perform: s3:GetObject on resource: "arn:aws:s3:::{bucket}/k" '
        "with an explicit deny in a resource-based policy")
    assert routed_request(key, "s3", "PUT", f"/{bucket}/k2", b"y")[0] == 200


def _root_request(method, path, body=b"", query=""):
    """routed_request with a query string, signed with the root key."""
    import asyncio

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    import ministack.app as app_mod

    url = f"http://localhost:4566{path}" + (f"?{query}" if query else "")
    request = AWSRequest(method, url, data=body, headers={"host": "localhost:4566"})
    S3SigV4Auth(Credentials("test", "test"), "s3", "us-east-1").add_auth(request)
    sent = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(app_mod.app({
        "type": "http", "method": method, "path": path, "raw_path": path.encode(), "query_string": query.encode(),
        "headers": [(k.lower().encode(), v.encode()) for k, v in request.headers.items()]}, receive, send))
    return sent[0]["status"], b"".join(m.get("body", b"") for m in sent[1:])


def test_bucket_policy_deny_applies_to_the_root_user(auth):
    """#2462, captured on AWS as the root user: an explicit Deny in the bucket policy refuses root too, but the
    bucket owner's root can still read, replace and delete that bucket policy."""
    import xml.etree.ElementTree as ET

    bucket = f"rp-root-{uuid.uuid4().hex[:8]}"
    assert _root_request("PUT", f"/{bucket}")[0] == 200
    deny_put = {"Version": "2012-10-17", "Statement": [{"Sid": "DenyPut", "Effect": "Deny", "Principal": "*",
                                                         "Action": "s3:PutObject", "Resource": f"arn:aws:s3:::{bucket}/*"}]}
    assert _root_request("PUT", f"/{bucket}", json.dumps(deny_put).encode(), "policy=")[0] == 204
    status, body = _root_request("PUT", f"/{bucket}/k", b"x")
    assert status == 403 and b"<Code>AccessDenied</Code>" in body
    assert ET.fromstring(body).findtext("Message") == (
        'User: arn:aws:iam::000000000000:root is not authorized to perform: s3:PutObject on resource: '
        f'"arn:aws:s3:::{bucket}/k" with an explicit deny in a resource-based policy')

    deny_all = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Sid": "DenyAll", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
        "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]}]}).encode()
    assert _root_request("PUT", f"/{bucket}", deny_all, "policy=")[0] == 204
    status, body = _root_request("GET", f"/{bucket}", query="list-type=2")
    assert status == 403 and ET.fromstring(body).findtext("Message") == (
        'User: arn:aws:iam::000000000000:root is not authorized to perform: s3:ListBucket on resource: '
        f'"arn:aws:s3:::{bucket}" with an explicit deny in a resource-based policy')
    assert _root_request("GET", f"/{bucket}", query="policy=")[0] == 200
    assert _root_request("PUT", f"/{bucket}", deny_all, "policy=")[0] == 204
    assert _root_request("DELETE", f"/{bucket}", query="policy=")[0] == 204
    assert _root_request("PUT", f"/{bucket}/k", b"x")[0] == 200


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



@pytest.mark.auth_contract
def test_s3_new_bucket_blocks_public_access_contract(auth_enabled, s3):
    """Under AUTH=true a new bucket has all four Block Public Access settings on until they are changed or deleted."""
    bkt = f"intg-s3-pab-new-{_uuid_mod.uuid4().hex[:8]}"
    public_policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bkt}/*"}]})
    s3.create_bucket(Bucket=bkt, ObjectOwnership="ObjectWriter")
    try:
        if not auth_enabled:
            with pytest.raises(ClientError) as exc:
                s3.get_public_access_block(Bucket=bkt)
            assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"
            s3.create_bucket(Bucket=f"{bkt}-x", ObjectOwnership="ObjectWriter", ACL="public-read")
            s3.delete_bucket(Bucket=f"{bkt}-x")
            return
        assert s3.get_public_access_block(Bucket=bkt)["PublicAccessBlockConfiguration"] == dict.fromkeys(
            ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)
        for call, kwargs in ((s3.put_bucket_policy, {"Policy": public_policy}),
                             (s3.put_bucket_acl, {"ACL": "public-read"}),
                             (s3.put_object, {"Key": "k", "Body": b"k", "ACL": "public-read"})):
            with pytest.raises(ClientError) as exc:
                call(Bucket=bkt, **kwargs)
            assert exc.value.response["Error"]["Code"] == "AccessDenied", kwargs
        with pytest.raises(ClientError) as exc:
            s3.create_bucket(Bucket=f"{bkt}-x", ObjectOwnership="ObjectWriter", ACL="public-read")
        assert exc.value.response["Error"]["Code"] == "InvalidBucketAclWithBlockPublicAccessError"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

        s3.delete_public_access_block(Bucket=bkt)
        with pytest.raises(ClientError) as exc:
            s3.get_public_access_block(Bucket=bkt)
        assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"
        s3.put_bucket_policy(Bucket=bkt, Policy=public_policy)
        s3.put_bucket_acl(Bucket=bkt, ACL="public-read")
        s3.delete_bucket_policy(Bucket=bkt)
    finally:
        s3.delete_bucket(Bucket=bkt)

def test_s3_block_public_policy_refuses_a_public_bucket_policy(s3):
    bkt = f"intg-s3-bpp-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)

    def policy(**extra):
        return json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bkt}/*", **extra}]})

    try:
        s3.put_bucket_policy(Bucket=bkt, Policy=policy())
        s3.put_public_access_block(Bucket=bkt, PublicAccessBlockConfiguration={
            "BlockPublicAcls": False, "IgnorePublicAcls": False,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": False})
        # The policy set before the setting stays.
        assert json.loads(s3.get_bucket_policy(Bucket=bkt)["Policy"]) == json.loads(policy())
        for public in (policy(), policy(Condition={"StringLike": {"aws:SourceVpc": "vpc-*"}})):
            with pytest.raises(ClientError) as exc:
                s3.put_bucket_policy(Bucket=bkt, Policy=public)
            assert exc.value.response["Error"]["Code"] == "AccessDenied"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        s3.put_bucket_policy(Bucket=bkt, Policy=policy(
            Condition={"StringEquals": {"aws:SourceVpc": "vpc-91237329"}}))
    finally:
        s3.delete_public_access_block(Bucket=bkt)
        s3.delete_bucket_policy(Bucket=bkt)
        s3.delete_bucket(Bucket=bkt)


def test_s3_block_public_acls_refuses_setting_a_public_acl(s3):
    """BlockPublicAcls refuses every call that sets a public ACL; private grants and IgnorePublicAcls alone pass."""
    import requests

    bkt = f"bpa-acl-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(s3, bkt)
    s3.put_object(Bucket=bkt, Key="k", Body=b"x")
    owner_id = s3.list_buckets()["Owner"]["ID"]
    all_users = "http://acs.amazonaws.com/groups/global/AllUsers"
    public_policy = {"Owner": {"ID": owner_id}, "Grants": [
        {"Grantee": {"Type": "Group", "URI": all_users}, "Permission": "READ"}]}
    s3.put_public_access_block(Bucket=bkt, PublicAccessBlockConfiguration={"BlockPublicAcls": True})
    try:
        for call, kwargs in (
            (s3.put_object, {"Key": "n", "Body": b"n", "ACL": "public-read"}),
            (s3.put_object, {"Key": "n", "Body": b"n", "ACL": "authenticated-read"}),
            (s3.put_object, {"Key": "n", "Body": b"n", "GrantRead": f'uri="{all_users}"'}),
            (s3.put_object_acl, {"Key": "k", "ACL": "public-read"}),
            (s3.put_object_acl, {"Key": "k", "GrantRead": f'uri="{all_users}"'}),
            (s3.put_object_acl, {"Key": "k", "AccessControlPolicy": public_policy}),
            (s3.copy_object, {"Key": "c", "CopySource": {"Bucket": bkt, "Key": "k"}, "ACL": "public-read"}),
            (s3.create_multipart_upload, {"Key": "m", "ACL": "public-read"}),
            (s3.put_bucket_acl, {"ACL": "authenticated-read"}),
            (s3.put_bucket_acl, {"AccessControlPolicy": public_policy}),
        ):
            with pytest.raises(ClientError) as exc:
                call(Bucket=bkt, **kwargs)
            assert exc.value.response["Error"]["Code"] == "AccessDenied", kwargs
            assert "BlockPublicAcls" in exc.value.response["Error"]["Message"]
        post = s3.generate_presigned_post(
            Bucket=bkt, Key="p", Fields={"acl": "public-read"}, Conditions=[{"acl": "public-read"}])
        assert requests.post(post["url"], data=post["fields"], files={"file": ("p", b"x")}).status_code == 403
        assert s3.list_objects_v2(Bucket=bkt)["KeyCount"] == 1

        s3.put_object(Bucket=bkt, Key="n", Body=b"n", ACL="bucket-owner-full-control")
        s3.put_object(Bucket=bkt, Key="n", Body=b"n", GrantRead=f'id="{owner_id}"')
        s3.put_object_acl(Bucket=bkt, Key="k", ACL="private")
        s3.put_bucket_acl(Bucket=bkt, ACL="log-delivery-write")
        assert not any("URI" in g["Grantee"] and g["Grantee"]["URI"].endswith("/global/AllUsers")
                       for g in s3.get_object_acl(Bucket=bkt, Key="k")["Grants"])

        s3.put_public_access_block(Bucket=bkt, PublicAccessBlockConfiguration={"IgnorePublicAcls": True})
        s3.put_object(Bucket=bkt, Key="n", Body=b"n", ACL="public-read")
    finally:
        _purge_foreign(s3, bkt)


def test_s3_bucket_owner_enforced_refuses_acls(s3):
    """With ACLs disabled every ACL write is refused; object writes and CreateBucket keep an owner-only ACL."""
    import requests

    bkt = f"boe-acl-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    s3.put_object(Bucket=bkt, Key="k", Body=b"x")
    owner_id = s3.list_buckets()["Owner"]["ID"]
    owner, source = f'id="{owner_id}"', {"Bucket": bkt, "Key": "k"}
    owner_policy = {"Owner": {"ID": owner_id}, "Grants": [
        {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"}]}
    try:
        for kwargs in ({"ACL": "public-read"}, {"ACL": "log-delivery-write"}, {"GrantRead": owner}):
            with pytest.raises(ClientError) as exc:
                s3.create_bucket(Bucket=f"{bkt}-x", **kwargs)
            assert exc.value.response["Error"]["Code"] == "InvalidBucketAclWithObjectOwnership", kwargs
        for kwargs in ({"ACL": "private"}, {"GrantFullControl": owner}):
            s3.create_bucket(Bucket=f"{bkt}-x", **kwargs)
            s3.delete_bucket(Bucket=f"{bkt}-x")

        for call, kwargs in (
            (s3.put_bucket_acl, {"ACL": "private"}),
            (s3.put_bucket_acl, {"ACL": "log-delivery-write"}),
            (s3.put_bucket_acl, {"AccessControlPolicy": owner_policy}),
            (s3.put_object_acl, {"Key": "k", "ACL": "bucket-owner-full-control"}),
            (s3.put_object_acl, {"Key": "k", "AccessControlPolicy": owner_policy}),
            (s3.put_object, {"Key": "n", "Body": b"n", "ACL": "public-read"}),
            (s3.put_object, {"Key": "n", "Body": b"n", "ACL": "aws-exec-read"}),
            (s3.put_object, {"Key": "n", "Body": b"n", "GrantRead": owner}),
            (s3.copy_object, {"Key": "c", "CopySource": source, "ACL": "public-read"}),
            (s3.create_multipart_upload, {"Key": "m", "GrantRead": owner}),
        ):
            with pytest.raises(ClientError) as exc:
                call(Bucket=bkt, **kwargs)
            assert exc.value.response["Error"]["Code"] == "AccessControlListNotSupported", kwargs
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        post = s3.generate_presigned_post(
            Bucket=bkt, Key="p", Fields={"acl": "public-read"}, Conditions=[{"acl": "public-read"}])
        assert requests.post(post["url"], data=post["fields"], files={"file": ("p", b"x")}).status_code == 400
        assert s3.list_objects_v2(Bucket=bkt)["KeyCount"] == 1

        for acl in ("private", "bucket-owner-read", "bucket-owner-full-control"):
            s3.put_object(Bucket=bkt, Key="n", Body=b"n", ACL=acl)
        s3.put_object(Bucket=bkt, Key="n", Body=b"n", GrantFullControl=owner)
        s3.copy_object(Bucket=bkt, Key="c", CopySource=source, ACL="private")
        upload = s3.create_multipart_upload(Bucket=bkt, Key="m", ACL="bucket-owner-full-control")
        s3.abort_multipart_upload(Bucket=bkt, Key="m", UploadId=upload["UploadId"])
        assert [g["Permission"] for g in s3.get_bucket_acl(Bucket=bkt)["Grants"]] == ["FULL_CONTROL"]
        assert [g["Permission"] for g in s3.get_object_acl(Bucket=bkt, Key="k")["Grants"]] == ["FULL_CONTROL"]
    finally:
        _purge_foreign(s3, bkt)

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
    """A delete naming a locked version is refused; a plain delete adds a delete marker (Object Lock docs)."""
    from datetime import datetime, timedelta, timezone

    bkt = "intg-s3-lock-del"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    version = s3.put_object(Bucket=bkt, Key="locked.txt", Body=b"immutable")["VersionId"]
    s3.put_object_legal_hold(Bucket=bkt, Key="locked.txt", LegalHold={"Status": "ON"})
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bkt, Key="locked.txt", VersionId=version)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"

    marker = s3.delete_object(Bucket=bkt, Key="locked.txt")
    assert marker["DeleteMarker"] is True
    assert s3.get_object(Bucket=bkt, Key="locked.txt", VersionId=version)["Body"].read() == b"immutable"
    assert s3.get_object_legal_hold(Bucket=bkt, Key="locked.txt", VersionId=version)["LegalHold"]["Status"] == "ON"

    s3.put_object_legal_hold(Bucket=bkt, Key="locked.txt", VersionId=version, LegalHold={"Status": "OFF"})
    retain_until = datetime.now(timezone.utc) + timedelta(days=1)
    s3.put_object_retention(Bucket=bkt, Key="locked.txt", VersionId=version,
                            Retention={"Mode": "GOVERNANCE", "RetainUntilDate": retain_until})
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bkt, Key="locked.txt", VersionId=version)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"
    s3.delete_object(Bucket=bkt, Key="locked.txt", VersionId=version, BypassGovernanceRetention=True)
    with pytest.raises(ClientError):
        s3.get_object(Bucket=bkt, Key="locked.txt", VersionId=version)

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
    a_version = s3.put_object(Bucket=bkt, Key="a.txt", Body=b"a")["VersionId"]
    b_version = s3.put_object(Bucket=bkt, Key="b.txt", Body=b"b")["VersionId"]
    s3.put_object_legal_hold(Bucket=bkt, Key="a.txt", LegalHold={"Status": "ON"})
    resp = s3.delete_objects(
        Bucket=bkt,
        Delete={"Objects": [{"Key": "a.txt", "VersionId": a_version}, {"Key": "b.txt", "VersionId": b_version}]},
    )
    deleted_keys = [d["Key"] for d in resp.get("Deleted", [])]
    error_keys = [e["Key"] for e in resp.get("Errors", [])]
    assert "b.txt" in deleted_keys
    assert "a.txt" in error_keys
    # Without a VersionId the locked key just gets a delete marker.
    resp = s3.delete_objects(Bucket=bkt, Delete={"Objects": [{"Key": "a.txt"}]})
    assert resp["Deleted"][0]["DeleteMarker"] is True

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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, "s3-evt-bkt", "000000000000"))})
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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, "s3-evt-filter-bkt", "000000000000"))})
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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, "s3-evt-del-bkt", "000000000000"))})
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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, bkt, "000000000000"))})
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
    # The destination has to exist: AWS verifies an SQS or SNS target by sending
    # it a test notification and fails the whole PUT when that does not arrive.
    _west_sqs = _regional_client("sqs", "us-west-2")
    _west_q_url = _west_sqs.create_queue(QueueName="s3-west-region-q")["QueueUrl"]
    _west_sqs.set_queue_attributes(
        QueueUrl=_west_q_url,
        Attributes={"Policy": json.dumps(sqs_policy_allow_s3(
            "arn:aws:sqs:us-west-2:000000000000:s3-west-region-q",
            bkt, "000000000000"))})

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


# A same-region queue in another account is in scope: S3 delivers to it when
# its policy allows (see test_messaging_policies.py). Only a wrong-region ARN
# stays out of scope here.
def test_s3_notification_sqs_delivery_rejects_out_of_scope_arns_without_name_fallback(
    monkeypatch,
):
    from ministack.services import s3 as s3mod
    from ministack.services import sqs as sqsmod

    messages = []
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(sqsmod, "_queue_url", lambda name: f"url/{name}")
    monkeypatch.setattr(sqsmod, "_queues", {"url/shared-q": {"messages": messages}})
    monkeypatch.setattr(sqsmod, "_ensure_msg_fields", lambda msg: None)

    s3mod._deliver_event_to_sqs(
        "arn:aws:sqs:us-west-2:000000000000:shared-q", {"Records": []}, "us-east-1")

    assert messages == []


# A same-region topic in another account is in scope: S3 delivers to it when
# its policy allows (see test_messaging_policies.py). Only a wrong-region ARN
# stays out of scope here.
def test_s3_notification_sns_delivery_rejects_out_of_scope_arns_before_fanout(
    monkeypatch,
):
    from ministack.services import s3 as s3mod
    from ministack.services import sns as snsmod

    target_arn = "arn:aws:sns:us-west-2:000000000000:shared-topic"
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
    sqsc.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, "s3-evt-xacct-bkt", account))})

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


def test_s3_put_notification_no_test_event_to_lambda(s3, lam, logs):
    """AWS delivers the s3:TestEvent only to SQS and SNS destinations. A Lambda
    target must not be invoked at configuration time, otherwise every deploy
    logs failures for handlers that assume the Records array is present.
    """
    fname = "s3-no-test-evt-lam"
    bkt = "s3-no-test-evt-bkt"
    arn = _create_event_lambda(lam, fname)
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_notification_configuration(
        Bucket=bkt,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {"LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"]},
            ],
        },
    )
    assert not _wait_lambda_invoked(logs, fname, "s3:TestEvent", timeout=3.0), \
        "s3:TestEvent was delivered to the Lambda target; AWS sends it to SQS and SNS only"

    s3.put_object(Bucket=bkt, Key="real.txt", Body=b"hi")
    assert _wait_lambda_invoked(logs, fname, "real.txt"), \
        "Lambda was not invoked for a real object event"


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
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Created"],
                                 "detail": {"bucket": {"name": ["s3-eb-bkt"]}}}),
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
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Created"],
                                 "detail": {"bucket": {"name": [bucket_name]}}}),
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
                "detail": {"reason": ["CopyObject"], "bucket": {"name": ["s3-eb-copy-bkt"]}},
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
        EventPattern=json.dumps({"source": ["aws.s3"], "detail-type": ["Object Deleted"],
                                 "detail": {"bucket": {"name": ["s3-eb-del-bkt"]}}}),
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

def _cors_headers(headers):
    return {k.lower(): v for k, v in headers.items() if k.lower().startswith("access-control-") or k.lower() == "vary"}


@pytest.mark.auth_contract
def test_s3_cors_follows_the_bucket_configuration_contract(auth_enabled, s3):
    """Under AUTH preflights and the CORS headers of a request follow the bucket's CORS rules, else the wildcard."""
    bucket, plain = f"cors-rules-{uuid.uuid4().hex[:8]}", f"cors-none-{uuid.uuid4().hex[:8]}"
    for name in (bucket, plain):
        s3.create_bucket(Bucket=name)
        s3.put_object(Bucket=name, Key="k", Body=b"k")
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration={"CORSRules": [
        {"AllowedOrigins": ["https://a.example"], "AllowedMethods": ["GET", "PUT"],
         "AllowedHeaders": ["x-amz-meta-*"], "ExposeHeaders": ["ETag"], "MaxAgeSeconds": 300},
        {"AllowedOrigins": ["*"], "AllowedMethods": ["HEAD"]}]})
    vary = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method"

    def preflight(name, vhost, origin="https://a.example", method="GET", **extra):
        headers = {"Origin": origin, "Access-Control-Request-Method": method, **extra}
        if vhost:
            headers["Host"] = f"{name}.{urlparse(ENDPOINT).netloc}"
        url = f"{ENDPOINT}/k" if vhost else f"{ENDPOINT}/{name}/k"
        return requests.options(url, headers={k: v for k, v in headers.items() if v})

    def get_from(origin, name, operation="get_object"):
        def add_origin(request, **_):
            request.headers["Origin"] = origin

        event = f"before-sign.s3.{''.join(p.title() for p in operation.split('_'))}"
        s3.meta.events.register(event, add_origin)
        try:
            return _cors_headers(getattr(s3, operation)(Bucket=name, Key="k")["ResponseMetadata"]["HTTPHeaders"])
        finally:
            s3.meta.events.unregister(event, add_origin)

    try:
        for vhost in (False, True):
            allowed = preflight(bucket, vhost, method="PUT", **{"Access-Control-Request-Headers": "x-amz-meta-a"})
            refused = [preflight(plain, vhost), preflight(bucket, vhost, method="DELETE"),
                       preflight(bucket, vhost, origin="https://b.example"),
                       preflight(bucket, vhost, **{"Access-Control-Request-Headers": "x-other"})]
            assert allowed.status_code == 200
            if not auth_enabled:
                assert allowed.headers["Access-Control-Allow-Origin"] == "*"
                assert [r.status_code for r in refused] == [200] * 4
                continue
            assert _cors_headers(allowed.headers) == {
                "access-control-allow-origin": "https://a.example", "access-control-allow-credentials": "true",
                "access-control-allow-methods": "GET, PUT", "access-control-allow-headers": "x-amz-meta-a",
                "access-control-expose-headers": "ETag", "access-control-max-age": "300", "vary": vary}
            assert [r.status_code for r in refused] == [403] * 4
            assert "CORSResponse: CORS is not enabled for this bucket." in refused[0].text
            assert all("<Code>AccessForbidden</Code>" in r.text and not _cors_headers(r.headers) for r in refused)
        no_origin = requests.options(f"{ENDPOINT}/{bucket}/k", headers={"Access-Control-Request-Method": "GET"})
        assert no_origin.status_code == (400 if auth_enabled else 200)

        if auth_enabled:
            assert get_from("https://a.example", bucket)["access-control-allow-origin"] == "https://a.example"
            assert get_from("https://b.example", bucket, "head_object") == {
                "access-control-allow-origin": "*", "access-control-allow-methods": "HEAD", "vary": vary}
            assert get_from("https://b.example", bucket) == {}
            assert get_from("https://a.example", plain) == {}
        else:
            assert get_from("https://b.example", plain)["access-control-allow-origin"] == "*"
    finally:
        for name in (bucket, plain):
            s3.delete_object(Bucket=name, Key="k")
            s3.delete_bucket(Bucket=name)


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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter", ACL="public-read")
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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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

def _grant_pairs(acl):
    """``(ID or URI, permission)`` for each grant of a GetBucketAcl/GetObjectAcl response, in order."""
    return [(g["Grantee"].get("ID") or g["Grantee"].get("URI"), g["Permission"]) for g in acl["Grants"]]


def test_s3_bucket_acl_grant_headers(s3):
    """x-amz-grant-* headers on CreateBucket and PutBucketAcl set exactly the grants they name."""
    bucket = f"acl-bkt-grant-{uuid.uuid4().hex[:8]}"
    owner_id = s3.list_buckets()["Owner"]["ID"]
    other_id = "c4c1ede66af53448b93c283ce9448c4ba468c9432aa01d700d3878632f77d2d0"
    log_delivery = "http://acs.amazonaws.com/groups/s3/LogDelivery"
    owner, other, logs = f'id="{owner_id}"', f'id="{other_id}"', f'uri="{log_delivery}"'
    s3.create_bucket(Bucket=bucket, ObjectOwnership="BucketOwnerPreferred", GrantFullControl=owner, GrantRead=other)
    try:
        assert _grant_pairs(s3.get_bucket_acl(Bucket=bucket)) == [(other_id, "READ"), (owner_id, "FULL_CONTROL")]

        s3.put_bucket_acl(Bucket=bucket, GrantFullControl=owner, GrantRead=f"{other}, {logs}", GrantWrite=logs,
                          GrantReadACP=other, GrantWriteACP=logs)
        expected = [(other_id, "READ"), (log_delivery, "READ"), (log_delivery, "WRITE"),
                    (owner_id, "FULL_CONTROL"), (other_id, "READ_ACP"), (log_delivery, "WRITE_ACP")]
        assert _grant_pairs(s3.get_bucket_acl(Bucket=bucket)) == expected

        policy = {"Owner": {"ID": owner_id}, "Grants": [
            {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"}]}
        for kwargs, code in (
            ({"GrantRead": other, "ACL": "private"}, "InvalidRequest"),
            ({"GrantRead": other, "AccessControlPolicy": policy}, "UnexpectedContent"),
            ({"GrantRead": "bogus"}, "InvalidArgument"),
            ({"GrantRead": 'id="abc"'}, "InvalidArgument"),
            ({"GrantRead": 'uri="http://acs.amazonaws.com/groups/global/Nobody"'}, "InvalidArgument"),
            ({"GrantRead": 'emailAddress="nobody@example.com"'}, "UnsupportedArgument"),
        ):
            with pytest.raises(ClientError) as exc:
                s3.put_bucket_acl(Bucket=bucket, **kwargs)
            assert exc.value.response["Error"]["Code"] == code, kwargs
        assert _grant_pairs(s3.get_bucket_acl(Bucket=bucket)) == expected

        s3.put_bucket_acl(Bucket=bucket, GrantRead=f"ID={owner_id}")
        assert _grant_pairs(s3.get_bucket_acl(Bucket=bucket)) == [(owner_id, "READ")]
        with pytest.raises(ClientError) as exc:
            s3.create_bucket(Bucket=f"{bucket}-x", ObjectOwnership="ObjectWriter", GrantRead="bogus")
        assert exc.value.response["Error"]["Code"] == "InvalidArgument"

        # BucketOwnerEnforced, the default, disables ACLs: grant headers on an ACL write are refused.
        s3.create_bucket(Bucket=f"{bucket}-e")
        s3.put_object(Bucket=f"{bucket}-e", Key="k", Body=b"x")
        for call, kwargs in ((s3.put_bucket_acl, {}), (s3.put_object_acl, {"Key": "k"})):
            with pytest.raises(ClientError) as exc:
                call(Bucket=f"{bucket}-e", GrantRead=other, **kwargs)
            assert exc.value.response["Error"]["Code"] == "AccessControlListNotSupported"
    finally:
        s3.delete_bucket(Bucket=bucket)
        s3.delete_object(Bucket=f"{bucket}-e", Key="k")
        s3.delete_bucket(Bucket=f"{bucket}-e")


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


# ---------------------------------------------------------------------------
# Delete-marker persistence (S3_PERSIST)
#
# A delete marker has no body, so the data file left on disk belongs to the
# version it hides. Persisting the marker to the key's .meta.json sidecar is
# what stops a restart from reloading that file as a live object.
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_persist(tmp_path, monkeypatch):
    """Point S3 at a throwaway DATA_DIR with persistence on, default account."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod

    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")

    buckets_before = set(s3mod._buckets._data)
    versions_before = set(s3mod._object_versions._data)
    versioning_before = set(s3mod._bucket_versioning._data)
    annotations_before = set(s3mod._object_annotations._data)
    yield s3mod
    for store, before in (
        (s3mod._buckets._data, buckets_before),
        (s3mod._object_versions._data, versions_before),
        (s3mod._bucket_versioning._data, versioning_before),
        (s3mod._object_annotations._data, annotations_before),
    ):
        for key in set(store) - before:
            store.pop(key, None)


def _reload_bucket(s3mod, tmp_path, bucket, account="000000000000"):
    """Drop one bucket from memory and reload it from disk (a restart)."""
    s3mod._buckets._data.pop((account, bucket), None)
    for vkey in [k for k in s3mod._object_versions._data
                 if k[0] == account and k[1][0] == bucket]:
        s3mod._object_versions._data.pop(vkey, None)
    s3mod._load_persisted_bucket(
        account, bucket, os.path.join(str(tmp_path), account, bucket))


def test_s3_delete_marker_survives_restart(s3_persist, tmp_path):
    """A versioned delete persists its marker; a restart keeps the key hidden
    instead of reloading the object's still-present data file as current."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-dm"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    s3mod._put_object(bucket, "gone.txt", b"payload", {})

    status, del_headers, _ = s3mod._delete_object(bucket, "gone.txt")
    assert status == 204
    assert del_headers["x-amz-delete-marker"] == "true"

    meta_path = os.path.join(str(tmp_path), "000000000000", bucket, "gone.txt.meta.json")
    with open(meta_path) as mf:
        meta = json.load(mf)
    assert meta["is_delete_marker"] is True
    assert meta["version_id"] == del_headers["x-amz-version-id"]
    assert meta["versions"][0]["etag"]

    _reload_bucket(s3mod, tmp_path, bucket)

    restored = s3mod._buckets._data[("000000000000", bucket)]
    assert "gone.txt" not in restored["objects"]
    versions = s3mod._object_versions._data[("000000000000", (bucket, "gone.txt"))]
    assert versions[-1]["is_delete_marker"] is True
    assert versions[-1]["is_latest"] is True

    # GetObject on the hidden key reports the miss as a delete marker.
    status, get_headers, _ = s3mod._get_object(bucket, "gone.txt", {})
    assert status == 404
    assert get_headers.get("x-amz-delete-marker") == "true"


def test_s3_delete_marker_restores_previous_version_metadata(s3_persist, tmp_path):
    """The version a marker displaced is rebuilt below it, so
    ListObjectVersions does not lose it across a restart."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-dm-prev"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    _, put_headers, _ = s3mod._put_object(bucket, "k", b"data", {})
    version_id = put_headers["x-amz-version-id"]

    s3mod._delete_object(bucket, "k")
    _reload_bucket(s3mod, tmp_path, bucket)

    versions = s3mod._object_versions._data[("000000000000", (bucket, "k"))]
    assert [bool(v.get("is_delete_marker")) for v in versions] == [False, True]
    assert versions[0]["version_id"] == version_id
    assert versions[0]["etag"] == put_headers["ETag"]
    assert versions[0]["is_latest"] is False
    assert versions[1]["is_latest"] is True


def test_s3_lone_delete_marker_sidecar_restores_without_data_file(s3_persist, tmp_path):
    """A marker whose data file is gone (key never had a body, or the version
    was purged) still survives as a lone .meta.json sidecar."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-dm-lone"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    s3mod._put_object(bucket, "k", b"data", {})
    s3mod._delete_object(bucket, "k")

    os.remove(os.path.join(str(tmp_path), "000000000000", bucket, "k"))

    _reload_bucket(s3mod, tmp_path, bucket)

    assert "k" not in s3mod._buckets._data[("000000000000", bucket)]["objects"]
    versions = s3mod._object_versions._data[("000000000000", (bucket, "k"))]
    assert versions[-1]["is_delete_marker"] is True


def test_s3_purging_delete_marker_restores_object_after_restart(s3_persist, tmp_path):
    """Deleting the marker by version id un-hides the object, and the sidecar
    must be rewritten so a restart does not keep it hidden."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-undelete"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    _, put_headers, _ = s3mod._put_object(bucket, "k", b"data", {})
    version_id = put_headers["x-amz-version-id"]
    _, del_headers, _ = s3mod._delete_object(bucket, "k")
    marker_id = del_headers["x-amz-version-id"]

    status, _, _ = s3mod._delete_object(bucket, "k", query_params={"versionId": marker_id})
    assert status == 204

    _reload_bucket(s3mod, tmp_path, bucket)

    restored = s3mod._buckets._data[("000000000000", bucket)]
    assert restored["objects"]["k"]["version_id"] == version_id
    versions = s3mod._object_versions._data[("000000000000", (bucket, "k"))]
    assert not versions[-1].get("is_delete_marker")


def test_s3_put_after_delete_marker_clears_marker_on_disk(s3_persist, tmp_path):
    """A write over a delete marker replaces the marker sidecar, so the new
    object is visible again after a restart."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-reput"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    s3mod._put_object(bucket, "k", b"one", {})
    s3mod._delete_object(bucket, "k")
    s3mod._put_object(bucket, "k", b"two", {})

    meta_path = os.path.join(str(tmp_path), "000000000000", bucket, "k.meta.json")
    with open(meta_path) as mf:
        assert "is_delete_marker" not in json.load(mf)

    _reload_bucket(s3mod, tmp_path, bucket)
    restored = s3mod._buckets._data[("000000000000", bucket)]
    assert s3mod._read_body(bucket, "k", restored["objects"]["k"]) == b"two"


def test_s3_batch_delete_objects_persists_delete_marker(s3_persist, tmp_path):
    """DeleteObjects on a versioned bucket creates a marker via the same path
    as DeleteObject, so it must persist too."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-batch-dm"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    s3mod._put_object(bucket, "k", b"data", {})

    body = (
        b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<Object><Key>k</Key></Object></Delete>"
    )
    status, _, _ = s3mod._delete_objects(bucket, body)
    assert status == 200

    _reload_bucket(s3mod, tmp_path, bucket)

    assert "k" not in s3mod._buckets._data[("000000000000", bucket)]["objects"]
    versions = s3mod._object_versions._data[("000000000000", (bucket, "k"))]
    assert versions[-1]["is_delete_marker"] is True


def test_s3_noncurrent_versions_keep_bytes_tags_and_acls_across_restart(s3_persist, tmp_path):
    """Every version comes back with its own bytes, tags and ACL, including
    after a noncurrent version is purged under a delete marker."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-history"
    s3mod._create_bucket(bucket, b"", {"x-amz-object-ownership": "ObjectWriter"})
    s3mod._bucket_versioning[bucket] = "Enabled"
    vids = [s3mod._put_object(bucket, "k", body, {"x-amz-tagging": f"n={body.decode()}"})[1]["x-amz-version-id"]
            for body in (b"one", b"two", b"three")]
    s3mod._put_object_acl(bucket, "k", b"", {"x-amz-acl": "public-read"}, {"versionId": [vids[0]]})
    s3mod._delete_object(bucket, "k")
    s3mod._delete_object(bucket, "k", query_params={"versionId": vids[2]})

    _reload_bucket(s3mod, tmp_path, bucket)

    versions = s3mod._object_versions._data[("000000000000", (bucket, "k"))]
    assert [v["version_id"] for v in versions[:-1]] == vids[:2]
    assert versions[-1]["is_delete_marker"] and versions[-1]["is_latest"]
    assert [s3mod._get_object_data(bucket, "k", version_id=v) for v in vids[:2]] == [b"one", b"two"]
    assert [s3mod._object_tags[(bucket, "k", v)] for v in vids[:2]] == [{"n": "one"}, {"n": "two"}]
    assert "AllUsers" in s3mod._object_acl[(bucket, "k", vids[0])]


def test_s3_unversioned_delete_removes_persisted_files(s3_persist, tmp_path):
    """Regression guard: a plain delete on an unversioned bucket still removes
    both the data file and its sidecar."""
    s3mod = s3_persist
    bucket = "qa-s3-persist-unversioned"
    s3mod._create_bucket(bucket, b"")
    s3mod._put_object(bucket, "k", b"data", {})

    data_path = os.path.join(str(tmp_path), "000000000000", bucket, "k")
    assert os.path.isfile(data_path)

    s3mod._delete_object(bucket, "k")
    assert not os.path.exists(data_path)
    assert not os.path.exists(data_path + ".meta.json")

    _reload_bucket(s3mod, tmp_path, bucket)
    assert s3mod._buckets._data.get(("000000000000", bucket), {}).get("objects", {}) == {}


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


@pytest.mark.auth_contract
def test_s3_presigned_url_signature_is_verified(auth_enabled):
    """Under AUTH=true presigned SigV4 URLs are verified against the server
    secret: a bogus-credential signature, or a signed header (content-type /
    content-length) tampered with after signing, is rejected with 403 —
    matching real S3. With AUTH off they are accepted, like header-signed
    requests. A valid, untampered presigned URL succeeds in both modes."""
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

    rejected = 403 if auth_enabled else 200

    # Bogus-credential signature.
    u = bogus.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "hello.txt"}, ExpiresIn=300)
    assert status(urllib.request.Request(u, method="GET")) == rejected

    # Signed content-type tampered after signing.
    u = good.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": "up.txt", "ContentType": "text/plain"},
        ExpiresIn=300)
    assert status(urllib.request.Request(
        u, data=b"x", method="PUT",
        headers={"Content-Type": "application/octet-stream"})) == rejected

    # Signed content-length tampered after signing.
    u = good.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": "up2.txt", "ContentLength": 2},
        ExpiresIn=300)
    assert status(urllib.request.Request(
        u, data=b"way more than two bytes", method="PUT")) == rejected

    # A valid, untampered presigned URL still succeeds.
    u = good.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "hello.txt"}, ExpiresIn=300)
    assert status(urllib.request.Request(u, method="GET")) == 200


@pytest.mark.auth_contract
def test_s3_presigned_put_and_get_with_unknown_key(s3, auth_enabled):
    """A client that presigns with its own local pair (not a key MiniStack
    knows) can upload and download with AUTH off, as its header-signed calls can."""
    import urllib.error
    import urllib.request
    import uuid

    import boto3
    from botocore.config import Config

    bucket = f"presign-unknown-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    client = boto3.client(
        "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
        aws_access_key_id="accessKey1", aws_secret_access_key="verySecretKey1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    params = {"Bucket": bucket, "Key": "probe.txt"}
    put = urllib.request.Request(client.generate_presigned_url("put_object", Params=params, ExpiresIn=300),
                                 data=b"hello", method="PUT")
    get = client.generate_presigned_url("get_object", Params=params, ExpiresIn=300)

    if auth_enabled:
        for request in (put, get):
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(request)
            assert exc.value.code == 403
            assert b"<Code>InvalidAccessKeyId</Code>" in exc.value.read()
        return
    assert urllib.request.urlopen(put).status == 200
    with urllib.request.urlopen(get) as response:
        assert (response.status, response.read()) == (200, b"hello")


@pytest.mark.auth_contract
def test_s3_presigned_url_uses_iam_access_key_secret_and_status(auth_enabled):
    import json
    import urllib.error
    import urllib.request
    import uuid

    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    account_id = "123456789012"
    user_name = f"presign-user-{uuid.uuid4().hex[:8]}"
    bucket = f"presign-iam-{uuid.uuid4().hex[:8]}"
    key = "hello.txt"
    tenant_iam = boto3.client(
        "iam",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
    )
    tenant_s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        config=Config(s3={"addressing_style": "path"}),
    )
    tenant_iam.create_user(UserName=user_name)
    tenant_iam.put_user_policy(UserName=user_name, PolicyName="read", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*"}],
    }))
    access_key = tenant_iam.create_access_key(UserName=user_name)["AccessKey"]
    tenant_s3.create_bucket(Bucket=bucket)
    tenant_s3.put_object(Bucket=bucket, Key=key, Body=b"hi")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key["AccessKeyId"],
        aws_secret_access_key=access_key["SecretAccessKey"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    url = client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
    )

    try:
        assert urllib.request.urlopen(url).status == 200
        tenant_iam.update_access_key(
            UserName=user_name,
            AccessKeyId=access_key["AccessKeyId"],
            Status="Inactive",
        )
        if auth_enabled:
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(url)
            assert exc.value.code == 403
            assert b"<Code>InvalidAccessKeyId</Code>" in exc.value.read()
        else:
            # With AUTH off the key is not checked, as for a header-signed request.
            assert urllib.request.urlopen(url).status == 200
    finally:
        tenant_iam.delete_access_key(
            UserName=user_name,
            AccessKeyId=access_key["AccessKeyId"],
        )
        tenant_iam.delete_user_policy(UserName=user_name, PolicyName="read")
        tenant_iam.delete_user(UserName=user_name)


@pytest.mark.auth_contract
def test_s3_presigned_url_requires_exact_sts_session_token(s3, sts, auth_enabled):
    import urllib.error
    import urllib.request
    import uuid

    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    bucket = f"presign-sts-{uuid.uuid4().hex[:8]}"
    key = "hello.txt"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=b"hi")
    credentials = sts.get_session_token(DurationSeconds=900)["Credentials"]
    config = Config(signature_version="s3v4", s3={"addressing_style": "path"})

    def presign(session_token=None):
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=session_token,
            config=config,
        )
        return client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
        )

    assert urllib.request.urlopen(presign(credentials["SessionToken"])).status == 200
    for bad_token in (None, "wrong-session-token"):
        if not auth_enabled:
            # With AUTH off the session token is not checked, as for a header-signed request.
            assert urllib.request.urlopen(presign(bad_token)).status == 200
            continue
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(presign(bad_token))
        # S3's error table: InvalidToken is 400, not 403.
        assert exc.value.code == 400
        assert b"<Code>InvalidToken</Code>" in exc.value.read()


def test_s3_presigned_url_accepts_lenient_session_origin(s3):
    import urllib.request
    import uuid

    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    bucket = f"presign-lenient-sts-{uuid.uuid4().hex[:8]}"
    key = "hello.txt"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=b"hi")
    lenient_sts = boto3.client(
        "sts",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="unknown-lenient-caller",
        aws_secret_access_key="unknown-lenient-secret",
    )
    credentials = lenient_sts.get_session_token(DurationSeconds=900)["Credentials"]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    url = client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
    )

    assert urllib.request.urlopen(url).status == 200


@pytest.mark.parametrize("auth_enabled", [False, True])
@pytest.mark.parametrize("bad_token", [False, True])
def test_s3_presign_resolves_tenant_in_both_auth_modes(monkeypatch, auth_enabled, bad_token):
    from urllib.parse import parse_qs, urlsplit

    import boto3
    from botocore.config import Config

    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, request_scope
    from ministack.services import iam as iam_svc
    from ministack.services import s3 as s3_svc

    key = "test-presign-mode-key"
    owner = "123456789012"
    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    iam_svc._access_keys.set_scoped(owner, None, key, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": "secret",
    })
    client = boto3.client(
        "s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id=key, aws_secret_access_key="secret",
        aws_session_token="unexpected-token" if bad_token else None,
        config=Config(signature_version="s3v4"),
    )
    url = urlsplit(client.generate_presigned_url(
        "get_object", Params={"Bucket": "test-bucket", "Key": "object"},
    ))
    try:
        with request_scope("000000000000", "us-east-1"):
            error = s3_svc._verify_presigned_sigv4(
                "GET", url.path, {"host": url.netloc}, parse_qs(url.query),
            )
            assert get_account_id() == owner
            if bad_token and auth_enabled:
                assert error[0] == 400
            else:
                assert error is None
    finally:
        iam_svc._access_keys.pop_scoped(owner, None, key, None)


@pytest.mark.parametrize("auth_enabled", [False, True])
@pytest.mark.parametrize("expires,skew,change,status,code", [
    (604800, 0, None, None, None),
    (604801, 0, None, 400, "AuthorizationQueryParametersError"),
    (3600, 840, None, None, None),
    (3600, 960, None, 403, "Request is not yet valid"),
    (3600, 0, ("X-Amz-Expires", "abc"), 400, "X-Amz-Expires should be a number"),
    (3600, 0, ("X-Amz-Expires", "-1"), 400, "X-Amz-Expires must be non-negative"),
    (3600, 0, ("X-Amz-Expires", None), 400, "requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, "
                                            "X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters."),
    (3600, 0, ("X-Amz-Date", "2026"), 400, "X-Amz-Date must be in the ISO8601 Long Format"),
])
def test_s3_presign_time_limits_under_auth(monkeypatch, auth_enabled, expires, skew, change, status, code):
    import datetime as dt
    from unittest import mock
    from urllib.parse import parse_qs, urlsplit

    import boto3
    import botocore.auth
    from botocore.config import Config

    from ministack import app as app_mod
    from ministack.services import s3 as s3_svc

    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    client = boto3.client(
        "s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test", config=Config(signature_version="s3v4"),
    )
    signed_at = botocore.auth.get_current_datetime() + dt.timedelta(seconds=skew)
    with mock.patch.object(botocore.auth, "get_current_datetime", return_value=signed_at):
        url = urlsplit(client.generate_presigned_url(
            "get_object", Params={"Bucket": "test-bucket", "Key": "object"}, ExpiresIn=expires))
    query = parse_qs(url.query)
    if change and change[1] is None:
        query.pop(change[0])
    elif change:
        query[change[0]] = [change[1]]

    error = s3_svc._verify_presigned_sigv4("GET", url.path, {"host": url.netloc}, query)

    if auth_enabled and status:
        assert error[0] == status and code in error[2].decode()
    elif change and auth_enabled:
        assert error[0] == 403  # SignatureDoesNotMatch, or Request has expired for a negative X-Amz-Expires
    elif change == ("X-Amz-Expires", "-1"):
        assert error[0] == 403  # Request has expired: expiry is checked with AUTH off too
    else:
        assert error is None


def test_presigned_mrap_resolves_alias_in_iam_owner_account(monkeypatch):
    import asyncio
    from urllib.parse import parse_qs, urlsplit

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, request_scope
    from ministack.services import iam as iam_svc
    from ministack.services import s3 as s3_svc

    key, owner, alias = "test-mrap-owner-key", "123456789012", "testalias.mrap"
    host = f"{alias}.accesspoint.s3-global.amazonaws.com"
    monkeypatch.setattr(app_mod, "AUTH", False)
    request = AWSRequest(method="GET", url=f"http://{host}/object")
    S3SigV4QueryAuth(Credentials(key, "secret"), "s3", "us-east-1").add_auth(request)
    url = urlsplit(request.url)
    routed = []

    async def capture(method, path, headers, body, query_params, **kwargs):
        routed.append((get_account_id(), path))
        return 200, {}, b"object"

    monkeypatch.setattr(s3_svc, "handle_request", capture)
    iam_svc._access_keys.set_scoped(owner, None, key, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": "secret",
    })
    s3_svc._mraps.set_scoped(owner, None, alias, {"Regions": ["member-bucket"]})
    try:
        with request_scope("000000000000", "us-east-1"):
            result = asyncio.run(app_mod._handle_s3_vhost_request(
                host, url.path, "GET", {"host": host}, b"", parse_qs(url.query),
            ))
        assert result[0] == 200
        assert routed == [(owner, "/member-bucket/object")]
    finally:
        iam_svc._access_keys.pop_scoped(owner, None, key, None)
        s3_svc._mraps.pop_scoped(owner, None, alias, None)


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


def test_s3_presigned_put_query_checksum_is_not_verified(s3):
    """A presigned URL's `x-amz-checksum-*` query parameter describes the empty
    body the presigner had, not the body the holder of the URL later uploads,
    so it cannot be an integrity value. Since v3.729.0 the JS SDK signs one into
    every presigned PutObject by default, and AWS answers 200 for a body that
    does not match it. Verifying it rejects a request real S3 accepts."""
    import urllib.request

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    bucket = "presign-qs-checksum-bkt"
    key = "probe.txt"
    s3.create_bucket(Bucket=bucket)

    # `AAAAAA==` is CRC32 of the empty body — the literal value the JS SDK
    # signs in, and the one AWS's own recorded snapshot carries.
    signed = AWSRequest(
        method="PUT",
        url=f"{ENDPOINT}/{bucket}/{key}"
            "?x-amz-checksum-crc32=AAAAAA%3D%3D"
            "&x-amz-sdk-checksum-algorithm=CRC32",
    )
    S3SigV4QueryAuth(
        Credentials("test", "test"), "s3", "us-east-1", expires=300
    ).add_auth(signed)

    resp = urllib.request.urlopen(
        urllib.request.Request(signed.url, data=b"123456", method="PUT"))
    assert resp.status == 200

    # AWS stores the body it was sent, mismatching checksum parameter and all.
    head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    assert head["ContentLength"] == 6
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"123456"

    # `x-amz-sdk-checksum-algorithm` names an algorithm to compute rather than
    # carrying a value, so it keeps hoisting: the stored checksum is CRC32 of
    # the body that actually arrived, never the empty-body value off the URL.
    assert head.get("ChecksumCRC32") == "CXLTYQ==", (
        "the hoisted algorithm selector was not honoured")


@pytest.mark.auth_contract
def test_s3_presigned_put_query_checksum_still_covered_by_signature(s3, auth_enabled):
    """The other half of the AWS contract: the parameter is ignored as an
    integrity value but still signed, so rewriting it after the fact breaks the
    signature under AUTH=true. Ignoring it must not mean stripping it before
    verification."""
    import urllib.error
    import urllib.request

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    bucket = "presign-qs-checksum-sig-bkt"
    key = "probe.txt"
    s3.create_bucket(Bucket=bucket)

    signed = AWSRequest(
        method="PUT",
        url=f"{ENDPOINT}/{bucket}/{key}?x-amz-checksum-crc32=AAAAAA%3D%3D",
    )
    S3SigV4QueryAuth(
        Credentials("test", "test"), "s3", "us-east-1", expires=300
    ).add_auth(signed)

    tampered = signed.url.replace("AAAAAA%3D%3D", "BBBBBB%3D%3D")
    assert tampered != signed.url
    try:
        status = urllib.request.urlopen(
            urllib.request.Request(tampered, data=b"123456", method="PUT")).status
    except urllib.error.HTTPError as exc:
        status = exc.code
    # With AUTH off the signature is not checked, and the query checksum is never verified.
    expected = 403 if auth_enabled else 200
    assert status == expected, f"tampered checksum parameter answered {status}"


def test_s3_put_header_checksum_is_still_verified(s3):
    """Excluding the query parameter must not loosen the header case: a
    `x-amz-checksum-*` sent as a real header is a value the client computed over
    the body it is sending, and a mismatch is still `BadDigest`."""
    import urllib.error
    import urllib.request

    bucket = "presign-hdr-checksum-bkt"
    key = "probe.txt"
    s3.create_bucket(Bucket=bucket)

    url = s3.generate_presigned_url(
        "put_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300)
    try:
        status = urllib.request.urlopen(urllib.request.Request(
            url, data=b"123456", method="PUT",
            headers={"x-amz-checksum-crc32": "AAAAAA=="})).status
    except urllib.error.HTTPError as exc:
        status = exc.code
    assert status == 400, f"mismatching checksum header accepted with {status}"


def test_s3_presigned_multipart_hoisted_checksum_algorithm_is_honoured(s3):
    """The exclusion covers checksum *values* only. `x-amz-checksum-algorithm`
    and `x-amz-sdk-checksum-algorithm` name an algorithm for the server to
    compute rather than carrying a value, so they cannot disagree with a body;
    a presigned CreateMultipartUpload hoists one of them to choose the algorithm
    its parts are digested with, and the completed object must carry it."""
    import urllib.request
    import xml.etree.ElementTree as ET

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    bucket = "presign-mpu-algo-bkt"
    s3.create_bucket(Bucket=bucket)

    for param in ("x-amz-checksum-algorithm", "x-amz-sdk-checksum-algorithm"):
        key = f"multipart/{param}.bin"
        signed = AWSRequest(
            method="POST", url=f"{ENDPOINT}/{bucket}/{key}?uploads&{param}=CRC32")
        S3SigV4QueryAuth(
            Credentials("test", "test"), "s3", "us-east-1", expires=300
        ).add_auth(signed)

        body = urllib.request.urlopen(urllib.request.Request(
            signed.url, data=b"", method="POST")).read()
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        upload_id = ET.fromstring(body).findtext(f"{ns}UploadId")
        assert upload_id

        part = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                              PartNumber=1, Body=b"x" * 16)
        done = s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": [
                {"ETag": part["ETag"], "PartNumber": 1}]})
        assert done.get("ChecksumCRC32"), (
            f"{param} hoisted into the query string was not honoured")


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
        EventPattern=json.dumps({"source": ["aws.s3"],
                                 "detail": {"bucket": {"name": [bucket]}}}),
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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, bucket, "000000000000"))})

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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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


def _grant_object_acl(client, bucket, key, grants, **kwargs):
    """Replace an object's ACL with the owner's FULL_CONTROL plus ``(canonical ID, permission)`` grants."""
    owner_id = client.list_buckets()["Owner"]["ID"]
    client.put_object_acl(Bucket=bucket, Key=key, **kwargs, AccessControlPolicy={
        "Owner": {"ID": owner_id},
        "Grants": [{"Grantee": {"Type": "CanonicalUser", "ID": grantee}, "Permission": permission}
                   for grantee, permission in [(owner_id, "FULL_CONTROL"), *grants]],
    })


def _acl_enabled_bucket(client, bucket):
    """A bucket with ACLs enabled (BucketOwnerPreferred), so object ACLs take effect."""
    client.create_bucket(Bucket=bucket)
    client.put_bucket_ownership_controls(
        Bucket=bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]})


def _denied(call, **kwargs):
    with pytest.raises(ClientError) as exc:
        call(**kwargs)
    return exc.value.response["Error"]["Code"] in ("AccessDenied", "403")


def test_s3_object_acl_grant_opens_reads_to_another_account(s3):
    """Another account's READ grant opens GetObject and HeadObject only; BucketOwnerEnforced drops it."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"acl-xacct-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"one")
        assert _denied(reader.get_object, Bucket=bucket, Key="k")

        _grant_object_acl(owner, bucket, "k", [(reader.list_buckets()["Owner"]["ID"], "READ")])
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"
        assert reader.head_object(Bucket=bucket, Key="k")["ContentLength"] == 3
        assert _denied(reader.get_object_acl, Bucket=bucket, Key="k")
        assert _denied(reader.get_object_tagging, Bucket=bucket, Key="k")
        assert _denied(reader.put_object, Bucket=bucket, Key="k", Body=b"two")

        owner.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"

        owner.put_bucket_ownership_controls(
            Bucket=bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]})
        assert _denied(reader.get_object, Bucket=bucket, Key="k")
    finally:
        owner.delete_object(Bucket=bucket, Key="k")
        owner.delete_bucket(Bucket=bucket)


def test_s3_bucket_owner_enforced_needs_an_owner_only_bucket_acl(s3):
    """BucketOwnerEnforced is refused while the bucket ACL grants others; object ACLs read as the owner's meanwhile."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"boe-switch-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    enforced = {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"one", ACL="public-read")
        owner.put_bucket_acl(Bucket=bucket, ACL="public-read")
        with pytest.raises(ClientError) as exc:
            owner.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls=enforced)
        assert exc.value.response["Error"]["Code"] == "InvalidBucketAclWithObjectOwnership"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert reader.list_objects_v2(Bucket=bucket)["KeyCount"] == 1

        owner.put_bucket_acl(Bucket=bucket, ACL="private")
        owner.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls=enforced)
        assert [g["Permission"] for g in owner.get_object_acl(Bucket=bucket, Key="k")["Grants"]] == ["FULL_CONTROL"]
        assert _denied(reader.get_object, Bucket=bucket, Key="k")

        owner.put_bucket_ownership_controls(
            Bucket=bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "ObjectWriter"}]})
        assert len(owner.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 2
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"
    finally:
        _purge_foreign(owner, bucket)


def test_s3_object_acl_grant_to_another_account_follows_the_version(s3):
    """The grant stays on the version current when it was set; READ_ACP opens that version's ACL."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    reader_id = reader.list_buckets()["Owner"]["ID"]
    bucket = f"acl-xacct-ver-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    try:
        owner.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        v1 = owner.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
        _grant_object_acl(owner, bucket, "k", [(reader_id, "READ")])
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"

        owner.put_object(Bucket=bucket, Key="k", Body=b"two")
        assert _denied(reader.get_object, Bucket=bucket, Key="k")
        assert reader.get_object(Bucket=bucket, Key="k", VersionId=v1)["Body"].read() == b"one"
        assert _denied(reader.get_object_acl, Bucket=bucket, Key="k", VersionId=v1)

        _grant_object_acl(owner, bucket, "k", [(reader_id, "READ_ACP")], VersionId=v1)
        grants = reader.get_object_acl(Bucket=bucket, Key="k", VersionId=v1)["Grants"]
        assert {(g["Grantee"]["ID"], g["Permission"]) for g in grants} >= {(reader_id, "READ_ACP")}
    finally:
        _purge_foreign(owner, bucket)


def test_s3_object_grant_headers_set_the_object_acl(s3):
    """x-amz-grant-* headers on PutObject and PutObjectAcl set the object ACL, which then answers reads."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    owner_id, reader_id = owner.list_buckets()["Owner"]["ID"], reader.list_buckets()["Owner"]["ID"]
    bucket = f"acl-grant-hdr-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"one", GrantRead=f'id="{reader_id}"')
        assert _grant_pairs(owner.get_object_acl(Bucket=bucket, Key="k")) == [(reader_id, "READ")]
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"

        owner.put_object_acl(Bucket=bucket, Key="k", GrantFullControl=f'id="{owner_id}"',
                             GrantReadACP=f'id="{reader_id}"')
        assert _grant_pairs(owner.get_object_acl(Bucket=bucket, Key="k")) == [
            (owner_id, "FULL_CONTROL"), (reader_id, "READ_ACP")]
        assert _denied(reader.get_object, Bucket=bucket, Key="k")
        assert _grant_pairs(reader.get_object_acl(Bucket=bucket, Key="k"))[1] == (reader_id, "READ_ACP")

        policy = {"Owner": {"ID": owner_id}, "Grants": [
            {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"}]}
        grant = f'id="{reader_id}"'
        for call, kwargs, code in (
            (owner.put_object, {"Key": "n", "Body": b"x", "GrantRead": grant, "ACL": "private"}, "InvalidRequest"),
            (owner.put_object, {"Key": "n", "Body": b"x", "GrantRead": "bogus"}, "InvalidArgument"),
            (owner.put_object_acl, {"Key": "k", "GrantRead": grant, "ACL": "private"}, "InvalidRequest"),
            (owner.put_object_acl, {"Key": "k", "GrantRead": grant, "AccessControlPolicy": policy},
             "UnexpectedContent"),
        ):
            with pytest.raises(ClientError) as exc:
                call(Bucket=bucket, **kwargs)
            assert exc.value.response["Error"]["Code"] == code, kwargs
        assert owner.list_objects_v2(Bucket=bucket)["KeyCount"] == 1
        assert len(owner.get_object_acl(Bucket=bucket, Key="k")["Grants"]) == 2
    finally:
        _purge_foreign(owner, bucket)


def test_s3_public_read_object_acl_opens_reads_to_another_account(s3):
    """A public-read object ACL answers another account's read until IgnorePublicAcls is set."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"acl-xacct-pub-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"one", ACL="public-read")
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"one"

        owner.put_public_access_block(
            Bucket=bucket, PublicAccessBlockConfiguration={"IgnorePublicAcls": True})
        assert _denied(reader.get_object, Bucket=bucket, Key="k")
    finally:
        owner.delete_object(Bucket=bucket, Key="k")
        owner.delete_bucket(Bucket=bucket)


def test_s3_block_public_access_settings_each_cover_their_own_grants(s3):
    """Only IgnorePublicAcls drops a public ACL and only RestrictPublicBuckets a public policy."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"bpa-each-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    settings = ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")
    try:
        owner.put_object(Bucket=bucket, Key="acl", Body=b"acl", ACL="public-read")
        owner.put_object(Bucket=bucket, Key="pol/k", Body=b"pol")
        owner.put_bucket_policy(Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket}/pol/*"}]}))
        for setting in settings:
            owner.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={setting: True})
            for key, ignored_by in (("acl", "IgnorePublicAcls"), ("pol/k", "RestrictPublicBuckets")):
                if setting == ignored_by:
                    assert _denied(reader.get_object, Bucket=bucket, Key=key), (setting, key)
                else:
                    assert reader.get_object(Bucket=bucket, Key=key)["Body"].read(), (setting, key)
    finally:
        owner.delete_bucket_policy(Bucket=bucket)
        _purge_foreign(owner, bucket)


def test_s3_restrict_public_buckets_keeps_a_policy_that_is_not_public(s3):
    """RestrictPublicBuckets drops only a public policy; a policy naming another account still grants it."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"bpa-named-{uuid.uuid4().hex[:8]}"
    owner.create_bucket(Bucket=bucket)
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"k")
        owner.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={"RestrictPublicBuckets": True})
        owner.put_bucket_policy(Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::222222222222:root"}, "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket}/*"}]}))
        assert reader.get_object(Bucket=bucket, Key="k")["Body"].read() == b"k"
    finally:
        owner.delete_bucket_policy(Bucket=bucket)
        _purge_foreign(owner, bucket)


def test_s3_public_bucket_acl_grants_only_what_the_bucket_permissions_cover(s3):
    """Bucket READ lists and WRITE adds and removes objects; neither opens objects, configuration or the ACL."""
    owner, reader = _s3_account("111111111111"), _s3_account("222222222222")
    bucket = f"bacl-pub-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(owner, bucket)
    try:
        owner.put_object(Bucket=bucket, Key="k", Body=b"k")
        owner.put_bucket_acl(Bucket=bucket, ACL="public-read")
        assert [o["Key"] for o in reader.list_objects_v2(Bucket=bucket)["Contents"]] == ["k"]
        reader.list_object_versions(Bucket=bucket)
        reader.list_multipart_uploads(Bucket=bucket)
        for call in (reader.get_object, reader.head_object, reader.get_object_acl):
            assert _denied(call, Bucket=bucket, Key="k"), call
        for call in (reader.get_bucket_acl, reader.get_bucket_versioning, reader.get_bucket_tagging):
            assert _denied(call, Bucket=bucket), call

        owner.put_bucket_acl(Bucket=bucket, ACL="public-read-write")
        reader.put_object(Bucket=bucket, Key="w", Body=b"w")
        reader.delete_object(Bucket=bucket, Key="w")
        assert _denied(reader.put_object_acl, Bucket=bucket, Key="k", ACL="public-read")
        assert _denied(reader.put_bucket_tagging, Bucket=bucket, Tagging={"TagSet": [{"Key": "a", "Value": "b"}]})
        assert _denied(reader.put_bucket_acl, Bucket=bucket, ACL="private")
        assert _denied(reader.delete_bucket, Bucket=bucket)
        grants = owner.get_bucket_acl(Bucket=bucket)["Grants"]
        assert sorted(g["Permission"] for g in grants if "URI" in g["Grantee"]) == ["READ", "WRITE"]
    finally:
        _purge_foreign(owner, bucket)


def test_s3_post_object_acl_field(s3):
    """The POST form names its canned-ACL field `acl`; it applies to the
    uploaded object like x-amz-acl does on PutObject."""
    import uuid as _u

    import requests
    bucket = f"acl-post-{_u.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket, ObjectOwnership="ObjectWriter")
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


def test_s3_complete_multipart_upload_checks_mpu_object_size(s3):
    bucket = "mpu-object-size-bucket"
    s3.create_bucket(Bucket=bucket)
    body = b"x" * 10
    uid = s3.create_multipart_upload(Bucket=bucket, Key="obj")["UploadId"]
    etag = s3.upload_part(Bucket=bucket, Key="obj", UploadId=uid, PartNumber=1, Body=body)["ETag"]
    parts = {"Parts": [{"PartNumber": 1, "ETag": etag}]}

    for declared in (11, 9, 0):
        with pytest.raises(ClientError) as exc:
            s3.complete_multipart_upload(Bucket=bucket, Key="obj", UploadId=uid,
                                         MultipartUpload=parts, MpuObjectSize=declared)
        error = exc.value.response
        assert error["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert error["Error"]["Code"] == "InvalidRequest"
        assert error["Error"]["Message"] == (
            f"The provided 'x-amz-mp-object-size' header value {declared} does not match what was computed: 10")

    def _invalid_size(request, **_):
        request.headers["x-amz-mp-object-size"] = "abc"

    s3.meta.events.register_first("before-sign.s3.CompleteMultipartUpload", _invalid_size)
    try:
        with pytest.raises(ClientError) as exc:
            s3.complete_multipart_upload(Bucket=bucket, Key="obj", UploadId=uid, MultipartUpload=parts)
    finally:
        s3.meta.events.unregister("before-sign.s3.CompleteMultipartUpload", _invalid_size)
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"
    assert exc.value.response["Error"]["Message"] == "Value for x-amz-mp-object-size header is invalid: 'abc'"

    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=bucket, Key="obj")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert [(p["PartNumber"], p["Size"]) for p in s3.list_parts(Bucket=bucket, Key="obj", UploadId=uid)["Parts"]] == [
        (1, 10)]

    s3.complete_multipart_upload(Bucket=bucket, Key="obj", UploadId=uid, MultipartUpload=parts, MpuObjectSize=10)
    assert s3.head_object(Bucket=bucket, Key="obj")["ContentLength"] == 10


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


def test_s3_list_buckets_paginates(s3):
    """ListBuckets honors Prefix, MaxBuckets and ContinuationToken; a further
    page is signalled by ContinuationToken alone (ListBucketsOutput has no
    IsTruncated member)."""
    names = [f"qa-lbp-{c}" for c in "abc"]
    for n in names:
        s3.create_bucket(Bucket=n)
    try:
        page1 = s3.list_buckets(Prefix="qa-lbp-", MaxBuckets=2)
        assert [b["Name"] for b in page1["Buckets"]] == names[:2]
        assert page1["Prefix"] == "qa-lbp-"
        assert "ContinuationToken" in page1
        page2 = s3.list_buckets(
            Prefix="qa-lbp-", MaxBuckets=2,
            ContinuationToken=page1["ContinuationToken"],
        )
        assert [b["Name"] for b in page2["Buckets"]] == names[2:]
        assert "ContinuationToken" not in page2
    finally:
        for n in names:
            s3.delete_bucket(Bucket=n)


def test_s3_delete_object_refuses_directory_bucket_conditions(s3):
    """x-amz-if-match-size / x-amz-if-match-last-modified-time are directory-
    bucket features; a general-purpose bucket answers NotImplemented and the
    object stays."""
    bucket = "qa-del-cond-bucket"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bucket, Key="k", IfMatchSize=1)
    assert exc.value.response["Error"]["Code"] == "NotImplemented"
    with pytest.raises(ClientError) as exc:
        s3.delete_object(
            Bucket=bucket, Key="k",
            IfMatchLastModifiedTime=datetime(2026, 1, 1),
        )
    assert exc.value.response["Error"]["Code"] == "NotImplemented"
    assert s3.get_object(Bucket=bucket, Key="k")["Body"].read() == b"x"
    s3.delete_object(Bucket=bucket, Key="k")
    s3.delete_bucket(Bucket=bucket)


def test_s3_delete_objects_refuses_size_and_mtime_conditions(s3):
    """A DeleteObjects entry carrying Size or LastModifiedTime draws a per-key
    NotImplemented error while unconditional entries in the batch delete."""
    bucket = "qa-del-batch-cond-bucket"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="cond", Body=b"x")
    s3.put_object(Bucket=bucket, Key="plain", Body=b"y")
    resp = s3.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [
                {"Key": "cond", "Size": 1},
                {"Key": "plain"},
            ],
            "Quiet": False,
        },
    )
    assert [e["Key"] for e in resp.get("Deleted", [])] == ["plain"]
    errors = resp.get("Errors", [])
    assert len(errors) == 1
    assert errors[0]["Key"] == "cond"
    assert errors[0]["Code"] == "NotImplemented"
    assert s3.get_object(Bucket=bucket, Key="cond")["Body"].read() == b"x"
    s3.delete_object(Bucket=bucket, Key="cond")
    s3.delete_bucket(Bucket=bucket)


def test_s3_put_object_validates_crc32c(s3):
    """CRC32C is computed in-process: a wrong client value draws BadDigest,
    a correct one is stored and surfaced when checksum mode is enabled."""
    from botocore.exceptions import ClientError

    bucket = "checksum-crc32c-validate-bucket"
    s3.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as exc:
        s3.put_object(
            Bucket=bucket, Key="k", Body=b"x",
            ChecksumAlgorithm="CRC32C",
            ChecksumCRC32C="AAAAAA==",
        )
    assert exc.value.response["Error"]["Code"] == "BadDigest"

    good = "qTxfkw=="  # CRC32C(b"x"), big-endian, base64
    s3.put_object(
        Bucket=bucket, Key="k", Body=b"x",
        ChecksumAlgorithm="CRC32C",
        ChecksumCRC32C=good,
    )
    got = s3.get_object(Bucket=bucket, Key="k", ChecksumMode="ENABLED")
    assert got["ChecksumCRC32C"] == good
    assert got["Body"].read() == b"x"
    s3.delete_object(Bucket=bucket, Key="k")
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
    from collections import OrderedDict

    import requests
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
    s3.create_bucket(Bucket=bkt, ObjectOwnership="ObjectWriter")

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
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, "qa-s3-restore-evt", "000000000000"))})
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
    s3.create_bucket(Bucket=bkt, ObjectOwnership="ObjectWriter")
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


def test_s3_copy_object_applies_grant_headers(s3):
    """x-amz-grant-* headers on a copy set the destination's ACL; together with x-amz-acl the copy is refused."""
    bkt = f"s3-copy-grant-{uuid.uuid4().hex[:8]}"
    _acl_enabled_bucket(s3, bkt)
    owner_id = s3.list_buckets()["Owner"]["ID"]
    other_id = "c4c1ede66af53448b93c283ce9448c4ba468c9432aa01d700d3878632f77d2d0"
    s3.put_object(Bucket=bkt, Key="src.txt", Body=b"body")
    source = {"Bucket": bkt, "Key": "src.txt"}

    s3.copy_object(Bucket=bkt, Key="dst.txt", CopySource=source, GrantRead=f'id="{other_id}"',
                   GrantFullControl=f'id="{owner_id}"')
    assert _grant_pairs(s3.get_object_acl(Bucket=bkt, Key="dst.txt")) == [
        (other_id, "READ"), (owner_id, "FULL_CONTROL")]

    with pytest.raises(ClientError) as exc:
        s3.copy_object(Bucket=bkt, Key="both.txt", CopySource=source, GrantRead=f'id="{other_id}"', ACL="private")
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"
    assert [o["Key"] for o in s3.list_objects_v2(Bucket=bkt)["Contents"]] == ["dst.txt", "src.txt"]


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


# ---------------------------------------------------------------------------
# Replication data plane (x-amz-replication-status)
# ---------------------------------------------------------------------------

def _replicated_pair(s3, slug):
    """Source + destination buckets, both versioned, one Enabled rule."""
    src, dst = f"qa-repl-src-{slug}", f"qa-repl-dst-{slug}"
    for b in (src, dst):
        s3.create_bucket(Bucket=b)
        s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_replication(
        Bucket=src,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::000000000000:role/repl",
            "Rules": [{
                "ID": "r1", "Status": "Enabled", "Prefix": "",
                "Destination": {"Bucket": f"arn:aws:s3:::{dst}"},
            }],
        },
    )
    return src, dst


def _purge_versioned(s3, bucket):
    lv = s3.list_object_versions(Bucket=bucket)
    for v in lv.get("Versions", []) + lv.get("DeleteMarkers", []):
        s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    s3.delete_bucket(Bucket=bucket)


def test_s3_replication_copies_object_and_stamps_status(s3):
    """A put under an Enabled rule lands in the destination; the source
    reports COMPLETED and the replica REPLICA, per S3's replication-status
    contract."""
    src, dst = _replicated_pair(s3, "basic")
    try:
        s3.put_object(Bucket=src, Key="a/k", Body=b"payload",
                      Tagging="team=core")
        head = s3.head_object(Bucket=src, Key="a/k")
        assert head["ReplicationStatus"] == "COMPLETED"
        replica = s3.get_object(Bucket=dst, Key="a/k")
        assert replica["ReplicationStatus"] == "REPLICA"
        assert replica["Body"].read() == b"payload"
        tags = s3.get_object_tagging(Bucket=dst, Key="a/k")["TagSet"]
        assert tags == [{"Key": "team", "Value": "core"}]
    finally:
        _purge_versioned(s3, src)
        _purge_versioned(s3, dst)


def test_s3_replication_honors_prefix_and_failure(s3):
    """A key outside the rule's Prefix carries no status; a destination that
    stops being usable stamps the source FAILED."""
    src, dst = "qa-repl-src-pref", "qa-repl-dst-pref"
    for b in (src, dst):
        s3.create_bucket(Bucket=b)
        s3.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_replication(
        Bucket=src,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::000000000000:role/repl",
            "Rules": [{
                "ID": "r1", "Status": "Enabled", "Prefix": "logs/",
                "Destination": {"Bucket": f"arn:aws:s3:::{dst}"},
            }],
        },
    )
    try:
        s3.put_object(Bucket=src, Key="other/k", Body=b"x")
        head = s3.head_object(Bucket=src, Key="other/k")
        assert "ReplicationStatus" not in head
        # Destination becomes unusable: versioning suspended.
        s3.put_bucket_versioning(Bucket=dst, VersioningConfiguration={"Status": "Suspended"})
        s3.put_object(Bucket=src, Key="logs/k", Body=b"x")
        assert s3.head_object(Bucket=src, Key="logs/k")["ReplicationStatus"] == "FAILED"
        from botocore.exceptions import ClientError
        try:
            s3.head_object(Bucket=dst, Key="logs/k")
            raise AssertionError("replica must not exist")
        except ClientError:
            pass
    finally:
        _purge_versioned(s3, src)
        lv = s3.list_object_versions(Bucket=dst)
        for v in lv.get("Versions", []) + lv.get("DeleteMarkers", []):
            s3.delete_object(Bucket=dst, Key=v["Key"], VersionId=v["VersionId"])
        s3.delete_bucket(Bucket=dst)


def test_s3_replication_covers_copy_and_versioned_reads(s3):
    """CopyObject into the source bucket replicates too, and the status rides
    on a version-addressed read."""
    src, dst = _replicated_pair(s3, "copy")
    try:
        s3.put_object(Bucket=src, Key="orig", Body=b"v")
        s3.copy_object(Bucket=src, Key="copied",
                       CopySource={"Bucket": src, "Key": "orig"})
        assert s3.head_object(Bucket=src, Key="copied")["ReplicationStatus"] == "COMPLETED"
        assert s3.get_object(Bucket=dst, Key="copied")["Body"].read() == b"v"
        vid = s3.head_object(Bucket=src, Key="copied")["VersionId"]
        versioned = s3.head_object(Bucket=src, Key="copied", VersionId=vid)
        assert versioned["ReplicationStatus"] == "COMPLETED"
    finally:
        _purge_versioned(s3, src)
        _purge_versioned(s3, dst)


def test_s3_control_list_tags_uses_the_tag_element_aws_uses(s3):
    """ListTagsForResource must wrap each tag in <Tag>, not <member>.

    The two tests above assert through boto3, which is lenient about the element
    name and so passes either way. AWS's own model is explicit — s3control's
    TagList declares `locationName: "Tag"` — and aws-sdk-go-v2 honours it
    strictly: given <member> wrappers it parses an empty tag list and reports no
    tags at all. That is what the Terraform AWS provider uses, so a bucket's
    `tags_all` refreshes to empty and every plan shows the same tag diff, which
    survives being applied.

    #447 fixed the TagResource *write* path to read <Tag>; the read path was
    left emitting <member>. Asserting on the wire format is the only way to
    catch that from Python.
    """
    from urllib.parse import quote

    import requests

    bkt = "intg-s3control-tag-element"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_tagging(
        Bucket=bkt, Tagging={"TagSet": [{"Key": "team", "Value": "platform"}]}
    )

    arn = quote(f"arn:aws:s3:::{bkt}", safe="")
    resp = requests.get(f"{ENDPOINT}/v20180820/tags/{arn}", timeout=10)
    assert resp.status_code == 200
    body = resp.text

    assert "<Tag><Key>team</Key><Value>platform</Value></Tag>" in body, body
    assert "<member>" not in body, body


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


def _wait_stack(cfn, name, timeout=30):
    """Poll until the stack reaches a terminal status; a deleted stack is
    addressable only by its id, so describe-by-name failing means gone."""
    deadline = time.time() + timeout
    status = "UNKNOWN"
    while time.time() < deadline:
        try:
            stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE", "StackName": name}
            raise
        status = stack["StackStatus"]
        if status.endswith("_COMPLETE") or status.endswith("_FAILED"):
            return stack
        time.sleep(0.2)
    raise AssertionError(f"stack {name} stuck in {status}")


def _mrap_template(name, buckets):
    return json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            **{f"B{i}": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": b}}
               for i, b in enumerate(buckets)},
            "Mrap": {"Type": "AWS::S3::MultiRegionAccessPoint",
                     "DependsOn": [f"B{i}" for i in range(len(buckets))],
                     "Properties": {"Name": name,
                                    "Regions": [{"Bucket": b} for b in buckets]}},
        },
        "Outputs": {"Alias": {"Value": {"Fn::GetAtt": ["Mrap", "Alias"]}}},
    })


def _mrap_get(alias, key, region=None):
    """GET through the MRAP hostname. The host is sent explicitly rather than
    resolved: <alias>.mrap.accesspoint.s3-global.amazonaws.com is a real public
    suffix, so letting DNS see it would leave the test dependent on egress."""
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    req = urllib.request.Request(f"{endpoint}/{key}")
    # The alias itself ends in ".mrap"; the hostname appends only the suffix.
    req.add_header("Host", f"{alias}.accesspoint.s3-global.amazonaws.com")
    if region:
        req.add_header("Authorization",
                       "AWS4-HMAC-SHA256 "
                       f"Credential=test/20260101/{region}/s3/aws4_request, "
                       "SignedHeaders=host, Signature=unsigned")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read().decode()


def test_s3_mrap_alias_serves_the_region_member(cfn, s3):
    """The MRAP hostname resolved nowhere — it is not a bucket name and matched
    no virtual-host rule. It now lands on the existing vhost path. AWS routes
    by network proximity, not by the signing region (observed 2026-10-12:
    us-east-1 and eu-west-1 clients in one location were served by the same
    member), so MiniStack serves the member homed in its own MINISTACK_REGION
    (us-east-1 for the test server) whatever region the request names. The
    buckets are created through the S3 API (one per region, like a real MRAP's
    members) and only the access point comes from the stack."""
    us_bucket, eu_bucket = "mrap-serve-us-app", "mrap-serve-eu-app"
    s3.create_bucket(Bucket=us_bucket)  # the client's own region, us-east-1
    s3.create_bucket(Bucket=eu_bucket, CreateBucketConfiguration={
        "LocationConstraint": "eu-west-1"})
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {"Mrap": {"Type": "AWS::S3::MultiRegionAccessPoint",
                               "Properties": {"Name": "serve-mrap-app",
                                              "Regions": [{"Bucket": eu_bucket},
                                                          {"Bucket": us_bucket}]}}},
        "Outputs": {"Alias": {"Value": {"Fn::GetAtt": ["Mrap", "Alias"]}}},
    })
    cfn.create_stack(StackName="cfn-s3-mrap-serve", TemplateBody=template)
    try:
        stack = _wait_stack(cfn, "cfn-s3-mrap-serve")
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        alias = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["Alias"]

        s3.put_object(Bucket=us_bucket, Key="who.txt", Body=b"US BUCKET")
        s3.put_object(Bucket=eu_bucket, Key="who.txt", Body=b"EU BUCKET")

        # eu-west-1 is listed first, so serving the us-east-1 member proves the
        # member is chosen by its stored region rather than taken off the front.
        for region in ("eu-west-1", "us-east-1", "ap-south-1", None):
            assert _mrap_get(alias, "who.txt", region=region) == (200, "US BUCKET")
    finally:
        cfn.delete_stack(StackName="cfn-s3-mrap-serve")
        _wait_stack(cfn, "cfn-s3-mrap-serve")
        for bucket in (us_bucket, eu_bucket):
            s3.delete_object(Bucket=bucket, Key="who.txt")
            s3.delete_bucket(Bucket=bucket)


def _reset():
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    urllib.request.urlopen(
        urllib.request.Request(f"{endpoint}/_ministack/reset", data=b"", method="POST"),
        timeout=10)


@pytest.mark.serial
def test_s3_mrap_cleared_by_reset(cfn, s3):
    """Every module-level store must be cleared by reset() — /_ministack/reset is
    what test isolation depends on, so an access point surviving it would leak
    an alias into the next test."""
    buckets = ["mrap-reset-us-east-1-app"]
    cfn.create_stack(StackName="cfn-s3-mrap-reset",
                     TemplateBody=_mrap_template("reset-mrap-app", buckets))
    stack = _wait_stack(cfn, "cfn-s3-mrap-reset")
    alias = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["Alias"]
    s3.put_object(Bucket=buckets[0], Key="who.txt", Body=b"US BUCKET")
    assert _mrap_get(alias, "who.txt") == (200, "US BUCKET")

    _reset()

    import urllib.error
    with pytest.raises(urllib.error.HTTPError) as ei:
        _mrap_get(alias, "who.txt")
    assert ei.value.code in (403, 404)


def _s3_lambda_arn(name="s3-notify-target"):
    return f"arn:aws:lambda:us-east-1:000000000000:function:{name}"


def test_s3_notification_is_stored_in_the_sdk_wire_form(s3):
    """Stored as the wire spells it, so any SDK reads back what another wrote."""
    import urllib.request

    bucket = f"notif-wire-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    arn = _s3_lambda_arn()
    xml = (
        '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<LambdaFunctionConfiguration>"
        f"<LambdaFunctionArn>{arn}</LambdaFunctionArn>"
        "<Event>s3:ObjectCreated:*</Event>"
        "</LambdaFunctionConfiguration></NotificationConfiguration>"
    )
    request = urllib.request.Request(
        f"{ENDPOINT}/{bucket}?notification", data=xml.encode(), method="PUT")
    with urllib.request.urlopen(request) as response:
        assert response.status == 200

    configs = s3.get_bucket_notification_configuration(
        Bucket=bucket)["LambdaFunctionConfigurations"]
    assert [c["LambdaFunctionArn"] for c in configs] == [arn]
    assert configs[0]["Events"] == ["s3:ObjectCreated:*"]


def test_s3_notification_auto_generates_a_base64_uuid_id(s3, sqs):
    """An omitted Id is base64 of a UUID (captured us-east-1 2026-09-20)."""
    import base64 as _b64

    bucket = f"notif-id-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    queue = sqs.create_queue(QueueName=f"notif-id-{_uuid_mod.uuid4().hex[:8]}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(
        QueueUrl=queue,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, bucket, "000000000000"))})
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"QueueConfigurations": [
            {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}]},
    )
    config = s3.get_bucket_notification_configuration(
        Bucket=bucket)["QueueConfigurations"][0]
    generated = config["Id"]
    assert generated, "AWS auto-generates an Id when the caller omits one"
    # base64 of a 36-char UUID: 48 chars, no padding.
    assert len(generated) == 48
    decoded = _b64.b64decode(generated + "==").decode()
    assert _uuid_mod.UUID(decoded)
    # It is stable across reads, not minted per GET.
    assert s3.get_bucket_notification_configuration(
        Bucket=bucket)["QueueConfigurations"][0]["Id"] == generated


def test_s3_notification_keeps_an_explicit_id(s3, sqs):
    bucket = f"notif-expl-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    queue = sqs.create_queue(QueueName=f"notif-expl-{_uuid_mod.uuid4().hex[:8]}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(
        QueueUrl=queue,
        Attributes={"Policy": json.dumps(
            sqs_policy_allow_s3(queue_arn, bucket, "000000000000"))})
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"QueueConfigurations": [
            {"Id": "mine", "QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}]},
    )
    assert s3.get_bucket_notification_configuration(
        Bucket=bucket)["QueueConfigurations"][0]["Id"] == "mine"


# --- Object annotations ---

def _bucket(s3, versioned=False, **create):
    name = f"annot-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=name, **create)
    if versioned:
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
    return name


def _code(exc):
    return exc.value.response["Error"]["Code"]


def _status(exc):
    return exc.value.response["ResponseMetadata"]["HTTPStatusCode"]


# ── Put / Get ──────────────────────────────────────────────────────────


def test_put_and_get_an_annotation(s3):
    bucket = _bucket(s3, versioned=True)
    payload = b'{"archive":"Images"}'
    put = s3.put_object(Bucket=bucket, Key="photo.jpg", Body=b"jpeg")
    out = s3.put_object_annotation(Bucket=bucket, Key="photo.jpg", AnnotationName="nexus.system", AnnotationPayload=payload)
    assert out["ObjectVersionId"] == put["VersionId"]
    assert out["ETag"] == f'"{hashlib.md5(payload).hexdigest()}"'
    got = s3.get_object_annotation(Bucket=bucket, Key="photo.jpg", AnnotationName="nexus.system")
    assert got["AnnotationPayload"].read() == payload
    assert got["ObjectVersionId"] == put["VersionId"]
    assert got["ContentLength"] == len(payload)
    assert got["ServerSideEncryption"] == "AES256"


def test_an_annotation_leaves_the_object_alone(s3):
    """"Adding, updating, or removing an annotation does not modify the parent
    object's ETag", and makes no new version."""
    bucket = _bucket(s3, versioned=True)
    put = s3.put_object(Bucket=bucket, Key="k", Body=b"body")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    head = s3.head_object(Bucket=bucket, Key="k")
    assert head["ETag"] == put["ETag"] and head["VersionId"] == put["VersionId"]
    assert len(s3.list_object_versions(Bucket=bucket)["Versions"]) == 1
    assert s3.get_object(Bucket=bucket, Key="k")["Body"].read() == b"body"


def test_putting_again_replaces_the_payload(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"first")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"second")
    got = s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert got["AnnotationPayload"].read() == b"second"


def test_get_of_a_missing_annotation(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="absent")
    assert (_code(exc), _status(exc)) == ("NoSuchAnnotation", 404)


def test_annotation_on_a_missing_key_or_bucket(s3):
    bucket = _bucket(s3)
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="nope", AnnotationName="a", AnnotationPayload=b"1")
    assert _code(exc) == "NoSuchKey"
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=f"missing-{uuid.uuid4().hex[:8]}", Key="k", AnnotationName="a")
    assert _code(exc) == "NoSuchBucket"


@pytest.mark.parametrize(
    "name, code",
    [
        ("aws.reserved", "InvalidAnnotationName"),
        ("AWSthing", "InvalidAnnotationName"),
        ("s3-thing", "InvalidAnnotationName"),
        ("S3", "InvalidAnnotationName"),
        ("has space", "InvalidAnnotationName"),
        ("slash/name", "InvalidAnnotationName"),
        ("   ", "InvalidAnnotationName"),
        ("n" * 513, "AnnotationNameTooLong"),
    ],
)
def test_annotation_naming_rules(s3, name, code):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName=name, AnnotationPayload=b"1")
    assert (_code(exc), _status(exc)) == (code, 400)


def test_names_may_use_letters_of_any_language(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    for name in ("données_1.2-3", "名前", "n" * 512):
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName=name, AnnotationPayload=b"1")


def test_payload_limits(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="max", AnnotationPayload=b"a" * (1024 * 1024))
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="big", AnnotationPayload=b"a" * (1024 * 1024 + 1))
    assert _code(exc) == "InvalidRequest"
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="empty", AnnotationPayload=b"")
    assert _code(exc) == "InvalidRequest"


def test_payload_must_be_utf8(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="bin", AnnotationPayload=b"\xff\xfe")
    assert (_code(exc), _status(exc)) == ("UnsupportedMediaType", 415)


def test_a_version_holds_at_most_1000_annotations(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    for i in range(1000):
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName=f"n{i}", AnnotationPayload=b"1")
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="n1000", AnnotationPayload=b"1")
    assert _code(exc) == "AnnotationLimitExceeded"
    # Replacing one of the thousand is not a new one.
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="n0", AnnotationPayload=b"2")


def test_object_if_match(s3):
    bucket = _bucket(s3)
    etag = s3.put_object(Bucket=bucket, Key="k", Body=b"x")["ETag"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1", ObjectIfMatch=etag)
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(
            Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"2", ObjectIfMatch='"deadbeef"'
        )
    assert _status(exc) == 412
    with pytest.raises(ClientError) as exc:
        s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", ObjectIfMatch='"deadbeef"')
    assert _status(exc) == 412
    s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", ObjectIfMatch=etag)


# ── Checksums ──────────────────────────────────────────────────────────


def test_default_checksum_is_crc64nvme(s3):
    """"If the annotation doesn't have a specified checksum algorithm or
    checksum value, Amazon S3 uses the CRC-64/NVME algorithm"."""
    plain = make_client("s3", {"request_checksum_calculation": "when_required"})
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    out = plain.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"123456789")
    expected = base64.b64encode(struct.pack(">Q", _crc64nvme_bitwise(b"123456789"))).decode()
    assert out["ChecksumCRC64NVME"] == expected
    got = s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", ChecksumMode="ENABLED")
    assert got["ChecksumCRC64NVME"] == expected
    # Only when asked: a client that does not send x-amz-checksum-mode gets none back.
    unasked = make_client("s3", {"response_checksum_validation": "when_required"})
    assert "ChecksumCRC64NVME" not in unasked.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")


@pytest.mark.parametrize(
    "member, digest",
    [
        ("ChecksumSHA256", lambda b: hashlib.sha256(b).digest()),
        ("ChecksumSHA512", lambda b: hashlib.sha512(b).digest()),
        ("ChecksumMD5", lambda b: hashlib.md5(b).digest()),
        # XXH64's published check value for b"abc" (the xxHash reference).
        ("ChecksumXXHASH64", lambda b: struct.pack(">Q", 0x44BC2CF5AD770999)),
    ],
)
def test_a_checksum_sent_is_verified(s3, member, digest):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    good = base64.b64encode(digest(b"abc")).decode()
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"abc", **{member: good})
    got = s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", ChecksumMode="ENABLED")
    assert got[member] == good and got["ChecksumType"] == "FULL_OBJECT"
    bad = base64.b64encode(bytes(x ^ 0xFF for x in digest(b"abc"))).decode()
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"abc", **{member: bad})
    assert _code(exc) == "BadDigest"


def test_xxh64_check_values():
    from ministack.services import s3 as s3mod

    assert s3mod._xxh64(b"") == 0xEF46DB3751D8E999
    assert s3mod._xxh64(b"abc") == 0x44BC2CF5AD770999
    # Past the 32-byte stripe loop, against the reference implementation's output.
    assert s3mod._xxh64(b"Nobody inspects the spammish repetition") == 0xFBCEA83C8A378BF1


def test_an_algorithm_ministack_cannot_compute_is_refused(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(
            Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"abc", ChecksumXXHASH3="AAAAAAAAAAA="
        )
    assert _code(exc) == "InvalidRequest"


# ── Encryption ─────────────────────────────────────────────────────────


def test_annotations_take_the_objects_encryption(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x", ServerSideEncryption="aws:kms", SSEKMSKeyId="alias/annot")
    out = s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    assert out["ServerSideEncryption"] == "aws:kms"
    got = s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert got["ServerSideEncryption"] == "aws:kms"


def test_sse_c_objects_cannot_have_annotations(s3):
    bucket = _bucket(s3)
    key = os.urandom(32)
    s3.put_object(
        Bucket=bucket,
        Key="k",
        Body=b"x",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=key,
    )
    with pytest.raises(ClientError) as exc:
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    assert _code(exc) == "InvalidRequest"


# ── Versions ───────────────────────────────────────────────────────────


def test_annotations_belong_to_one_version(s3):
    bucket = _bucket(s3, versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"on v1")
    v2 = s3.put_object(Bucket=bucket, Key="k", Body=b"two")["VersionId"]
    # "Creating a new version does not copy annotations from the previous version."
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert _code(exc) == "NoSuchAnnotation"
    got = s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=v1)
    assert got["AnnotationPayload"].read() == b"on v1" and got["ObjectVersionId"] == v1
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"on v2")
    assert s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=v2)["AnnotationPayload"].read() == b"on v2"
    assert s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=v1)["AnnotationPayload"].read() == b"on v1"


def test_a_delete_marker_keeps_the_versions_annotations(s3):
    bucket = _bucket(s3, versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"kept")
    marker = s3.delete_object(Bucket=bucket, Key="k")["VersionId"]
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert _code(exc) == "NoSuchKey"
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=marker)
    assert _status(exc) == 405
    assert s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=v1)["AnnotationPayload"].read() == b"kept"
    # Removing the marker brings the version back with its annotations.
    s3.delete_object(Bucket=bucket, Key="k", VersionId=marker)
    assert s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")["AnnotationPayload"].read() == b"kept"


def test_deleting_a_version_deletes_its_annotations(s3):
    bucket = _bucket(s3, versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    s3.put_object(Bucket=bucket, Key="k", Body=b"two")
    s3.delete_object(Bucket=bucket, Key="k", VersionId=v1)
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", VersionId=v1)
    assert _code(exc) == "NoSuchVersion"


def test_unversioned_overwrite_and_delete_drop_annotations(s3):
    """"In a non-versioned bucket, if you delete or overwrite the object, the
    annotations are deleted with it.\""""
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"one")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    s3.put_object(Bucket=bucket, Key="k", Body=b"two")
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert _code(exc) == "NoSuchAnnotation"
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="b", AnnotationPayload=b"1")
    s3.delete_object(Bucket=bucket, Key="k")
    s3.put_object(Bucket=bucket, Key="k", Body=b"three")
    assert s3.list_object_annotations(Bucket=bucket, Key="k")["Annotations"] == []


def test_batch_delete_drops_annotations(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"one")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "k"}]})
    s3.put_object(Bucket=bucket, Key="k", Body=b"two")
    assert s3.list_object_annotations(Bucket=bucket, Key="k")["Annotations"] == []


def test_annotations_need_no_restore_for_archived_objects(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="cold", Body=b"x", StorageClass="GLACIER")
    s3.put_object_annotation(Bucket=bucket, Key="cold", AnnotationName="a", AnnotationPayload=b"1")
    assert s3.get_object_annotation(Bucket=bucket, Key="cold", AnnotationName="a")["AnnotationPayload"].read() == b"1"


# ── List ───────────────────────────────────────────────────────────────


def test_list_annotations(s3):
    bucket = _bucket(s3, versioned=True)
    version = s3.put_object(Bucket=bucket, Key="k", Body=b"x")["VersionId"]
    for name, payload in (("ml.label", b"cat"), ("etl.status", b"done"), ("ml.score", b"0.97")):
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName=name, AnnotationPayload=payload, ChecksumAlgorithm="SHA256")
    out = s3.list_object_annotations(Bucket=bucket, Key="k")
    assert [a["AnnotationName"] for a in out["Annotations"]] == ["etl.status", "ml.label", "ml.score"]
    assert out["AnnotationCount"] == 3 and out["ObjectVersionId"] == version
    assert out["Bucket"] == bucket and out["Key"] == "k"
    entry = out["Annotations"][1]
    assert entry["Size"] == 3 and entry["ETag"] == f'"{hashlib.md5(b"cat").hexdigest()}"'
    assert entry["ChecksumAlgorithm"] == ["SHA256"] and "LastModified" in entry
    assert "NextContinuationToken" not in out


def test_list_annotations_by_prefix_and_page(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    for i in range(5):
        s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName=f"page.{i}", AnnotationPayload=b"1")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="other", AnnotationPayload=b"1")
    first = s3.list_object_annotations(Bucket=bucket, Key="k", AnnotationPrefix="page.", MaxAnnotationResults=2)
    assert [a["AnnotationName"] for a in first["Annotations"]] == ["page.0", "page.1"]
    assert first["AnnotationPrefix"] == "page." and first["MaxAnnotationResults"] == 2
    token = first["NextContinuationToken"]
    rest = s3.list_object_annotations(Bucket=bucket, Key="k", AnnotationPrefix="page.", ContinuationToken=token)
    assert [a["AnnotationName"] for a in rest["Annotations"]] == ["page.2", "page.3", "page.4"]
    assert rest["ContinuationToken"] == token and "NextContinuationToken" not in rest


def test_list_rejects_a_bad_page_size_or_prefix(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    plain = make_client("s3", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        plain.list_object_annotations(Bucket=bucket, Key="k", MaxAnnotationResults=1001)
    assert _code(exc) == "InvalidArgument"
    with pytest.raises(ClientError) as exc:
        s3.list_object_annotations(Bucket=bucket, Key="k", AnnotationPrefix="p" * 513)
    assert _code(exc) == "InvalidPrefix"


# ── Delete ─────────────────────────────────────────────────────────────


def test_delete_an_annotation(s3):
    bucket = _bucket(s3, versioned=True)
    version = s3.put_object(Bucket=bucket, Key="k", Body=b"x")["VersionId"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    out = s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert out["ResponseMetadata"]["HTTPStatusCode"] == 204 and out["ObjectVersionId"] == version
    with pytest.raises(ClientError) as exc:
        s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    assert _code(exc) == "NoSuchAnnotation"
    # Deleting one that is not there is not an error.
    s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")


# Observed on AWS (ap-southeast-2, 2026-09-28): annotations are writes to the object version, so Object Lock guards
# PutObjectAnnotation and DeleteObjectAnnotation alike. Governance retention yields to
# x-amz-bypass-governance-retention; compliance retention and a legal hold refuse both, bypass or not.


RETENTION = "Access Denied because object protected by object lock retention."
LEGAL_HOLD = "Access Denied because object protected by object lock legal hold."


_ROOT_AUTHORIZATION = (
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake"
)


def _lock_call(method, path, headers=None, body=b"", query=None):
    import asyncio

    from ministack.core.responses import request_scope
    from ministack.services import s3 as s3mod

    headers = {"authorization": _ROOT_AUTHORIZATION, **(headers or {})}
    with request_scope("000000000000", "us-east-1"):
        return asyncio.run(s3mod.handle_request(method, path, headers, body, query or {}))


def _locked_in_process(**lock_headers):
    bucket = f"ann-lock-{uuid.uuid4().hex[:10]}"
    assert _lock_call("PUT", f"/{bucket}", {"x-amz-bucket-object-lock-enabled": "true"})[0] == 200
    assert _lock_call("PUT", f"/{bucket}/k", lock_headers, b"x")[0] == 200
    return bucket


def _annotation_write(bucket, method, bypass=False):
    headers = {"x-amz-bypass-governance-retention": "true"} if bypass else {}
    query = {"annotation": [""], "annotationName": ["a"]}
    return _lock_call(method, f"/{bucket}/k", headers, b"1" if method == "PUT" else b"", query)


def _assert_refused(response, message):
    status, _, body = response
    assert status == 403
    assert b"<Code>AccessDenied</Code>" in body and message.encode() in body


def _until():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 3600))


def test_governance_retention_needs_the_bypass_to_put_or_delete_an_annotation(monkeypatch):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    bucket = _locked_in_process(**{"x-amz-object-lock-mode": "GOVERNANCE",
                                   "x-amz-object-lock-retain-until-date": _until()})
    _assert_refused(_annotation_write(bucket, "PUT"), RETENTION)
    assert _annotation_write(bucket, "PUT", bypass=True)[0] == 200
    _assert_refused(_annotation_write(bucket, "DELETE"), RETENTION)
    assert _annotation_write(bucket, "DELETE", bypass=True)[0] == 204


@pytest.mark.parametrize("lock_headers,message", [
    ({"x-amz-object-lock-mode": "COMPLIANCE"}, RETENTION),
    ({"x-amz-object-lock-legal-hold": "ON"}, LEGAL_HOLD),
])
def test_compliance_and_legal_hold_refuse_annotation_writes_even_with_the_bypass(monkeypatch, lock_headers, message):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    if "x-amz-object-lock-mode" in lock_headers:
        lock_headers = dict(lock_headers, **{"x-amz-object-lock-retain-until-date": _until()})
    bucket = _locked_in_process(**lock_headers)
    for method in ("PUT", "DELETE"):
        for bypass in (False, True):
            _assert_refused(_annotation_write(bucket, method, bypass), message)


def test_object_lock_does_not_refuse_annotation_writes_without_auth(monkeypatch):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", False)
    bucket = _locked_in_process(**{"x-amz-object-lock-legal-hold": "ON"})
    assert _annotation_write(bucket, "PUT")[0] == 200
    assert _annotation_write(bucket, "DELETE")[0] == 204


def _locked(s3, **lock):
    bucket = _bucket(s3, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x", **lock)
    return bucket


def test_an_unlocked_object_in_a_lock_enabled_bucket_takes_annotations_freely(s3):
    bucket = _locked(s3)
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"2")
    s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")


# ── CopyObject ─────────────────────────────────────────────────────────


def test_copy_carries_annotations_by_default(s3):
    src = _bucket(s3, versioned=True)
    dst = _bucket(s3, versioned=True)
    s3.put_object(Bucket=src, Key="landing/x.jpg", Body=b"x")
    s3.put_object_annotation(Bucket=src, Key="landing/x.jpg", AnnotationName="nexus.system", AnnotationPayload=b"{}")
    copied = s3.copy_object(Bucket=dst, Key="assets/x.jpg", CopySource={"Bucket": src, "Key": "landing/x.jpg"})
    got = s3.get_object_annotation(Bucket=dst, Key="assets/x.jpg", AnnotationName="nexus.system")
    assert got["AnnotationPayload"].read() == b"{}" and got["ObjectVersionId"] == copied["VersionId"]


def test_copy_of_a_named_version_carries_that_versions_annotations(s3):
    bucket = _bucket(s3, versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="k", Body=b"one")["VersionId"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"v1")
    s3.put_object(Bucket=bucket, Key="k", Body=b"two")
    s3.copy_object(Bucket=bucket, Key="copy", CopySource={"Bucket": bucket, "Key": "k", "VersionId": v1})
    assert s3.get_object_annotation(Bucket=bucket, Key="copy", AnnotationName="a")["AnnotationPayload"].read() == b"v1"


def test_copy_with_exclude_leaves_them_behind(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="src", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="src", AnnotationName="a", AnnotationPayload=b"1")
    s3.copy_object(Bucket=bucket, Key="dst", CopySource={"Bucket": bucket, "Key": "src"}, AnnotationDirective="EXCLUDE")
    assert s3.list_object_annotations(Bucket=bucket, Key="dst")["Annotations"] == []


def test_copy_rejects_an_unknown_directive(s3):
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="src", Body=b"x")
    plain = make_client("s3", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        plain.copy_object(Bucket=bucket, Key="dst", CopySource={"Bucket": bucket, "Key": "src"}, AnnotationDirective="REPLACE")
    assert _code(exc) == "InvalidArgument"


def test_copy_with_a_new_algorithm_rechecksums_the_annotations(s3):
    """"If you specify a different checksum algorithm in the copy request, the
    new algorithm applies to both the object and its annotations.\""""
    bucket = _bucket(s3)
    s3.put_object(Bucket=bucket, Key="src", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="src", AnnotationName="a", AnnotationPayload=b"abc")
    s3.copy_object(Bucket=bucket, Key="dst", CopySource={"Bucket": bucket, "Key": "src"}, ChecksumAlgorithm="SHA256")
    got = s3.get_object_annotation(Bucket=bucket, Key="dst", AnnotationName="a", ChecksumMode="ENABLED")
    assert got["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(b"abc").digest()).decode()


# ── Replication ────────────────────────────────────────────────────────


def test_annotations_replicate(s3):
    src = _bucket(s3, versioned=True)
    dst = _bucket(s3, versioned=True)
    s3.put_bucket_replication(
        Bucket=src,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::000000000000:role/repl",
            "Rules": [{"ID": "r1", "Status": "Enabled", "Prefix": "", "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}}],
        },
    )
    s3.put_object(Bucket=src, Key="k", Body=b"x")
    s3.put_object_annotation(Bucket=src, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    assert s3.get_object_annotation(Bucket=src, Key="k", AnnotationName="a")["ReplicationStatus"] == "COMPLETED"
    replica = s3.get_object_annotation(Bucket=dst, Key="k", AnnotationName="a")
    assert replica["AnnotationPayload"].read() == b"1" and replica["ReplicationStatus"] == "REPLICA"
    listed = s3.list_object_annotations(Bucket=dst, Key="k")["Annotations"]
    assert [(a["AnnotationName"], a["ReplicationStatus"]) for a in listed] == [("a", "REPLICA")]


def _s3_account(account_id):
    """An S3 client whose requests run inside *account_id* (12-digit key)."""
    import boto3 as _boto3
    return _boto3.client(
        "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
        aws_access_key_id=account_id, aws_secret_access_key="test",
    )


_SOURCE_ACCOUNT = "111111111111"
_DEST_ACCOUNT = "999999999999"
_REPL_ROLE = f"arn:aws:iam::{_SOURCE_ACCOUNT}:role/replication"


def _dest_policy(dest_bucket, actions=("s3:ReplicateObject", "s3:ObjectOwnerOverrideToBucketOwner")):
    """The destination-side grant AWS requires for cross-account replication."""
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowReplication",
            "Effect": "Allow",
            "Principal": {"AWS": _REPL_ROLE},
            "Action": list(actions),
            "Resource": f"arn:aws:s3:::{dest_bucket}/*",
        }],
    })


def _cross_account_pair(slug, policy="default", translate=True, ownership=None):
    """Source bucket in _SOURCE_ACCOUNT, destination bucket in _DEST_ACCOUNT,
    one Enabled rule (with Account and AccessControlTranslation unless
    ``translate`` is False). ``policy`` selects the destination bucket policy:
    ``"default"`` grants the replication role, ``None`` leaves the bucket
    without a policy, anything else is applied verbatim. ``ownership`` sets
    the destination's Object Ownership."""
    src_client, dst_client = _s3_account(_SOURCE_ACCOUNT), _s3_account(_DEST_ACCOUNT)
    src, dst = f"qa-xrepl-src-{slug}", f"qa-xrepl-dst-{slug}"
    dst_client.create_bucket(Bucket=dst)
    dst_client.put_bucket_versioning(Bucket=dst, VersioningConfiguration={"Status": "Enabled"})
    if ownership:
        dst_client.put_bucket_ownership_controls(
            Bucket=dst, OwnershipControls={"Rules": [{"ObjectOwnership": ownership}]})
    if policy == "default":
        dst_client.put_bucket_policy(Bucket=dst, Policy=_dest_policy(dst))
    elif policy is not None:
        dst_client.put_bucket_policy(Bucket=dst, Policy=policy)
    src_client.create_bucket(Bucket=src)
    src_client.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})
    src_client.put_bucket_replication(
        Bucket=src,
        ReplicationConfiguration={
            "Role": _REPL_ROLE,
            "Rules": [{
                "ID": "r1", "Status": "Enabled", "Filter": {"Prefix": ""},
                "DeleteMarkerReplication": {"Status": "Disabled"},
                "Destination": {"Bucket": f"arn:aws:s3:::{dst}"} | ({
                    "Account": _DEST_ACCOUNT,
                    "AccessControlTranslation": {"Owner": "Destination"},
                } if translate else {}),
            }],
        },
    )
    return src_client, dst_client, src, dst


def _purge_foreign(s3_client, bucket):
    lv = s3_client.list_object_versions(Bucket=bucket)
    for v in lv.get("Versions", []) + lv.get("DeleteMarkers", []):
        s3_client.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    s3_client.delete_bucket(Bucket=bucket)


def test_s3_replication_cross_account_writes_replica(s3):
    """A rule naming a destination in another account lands the replica there
    once the destination policy grants the replication role, matching AWS's
    documented cross-account requirement."""
    src_client, dst_client, src, dst = _cross_account_pair("ok")
    try:
        src_client.put_object(Bucket=src, Key="telemetry/k", Body=b"payload",
                              Tagging="team=core")
        head = src_client.head_object(Bucket=src, Key="telemetry/k")
        assert head["ReplicationStatus"] == "COMPLETED"
        replica = dst_client.get_object(Bucket=dst, Key="telemetry/k")
        assert replica["ReplicationStatus"] == "REPLICA"
        assert replica["Body"].read() == b"payload"
        tags = dst_client.get_object_tagging(Bucket=dst, Key="telemetry/k")["TagSet"]
        assert tags == [{"Key": "team", "Value": "core"}]
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


def test_s3_replication_cross_account_requires_destination_policy(s3):
    """Without a destination bucket policy granting the replication role,
    AWS cannot write the replica — the source reports FAILED."""
    src_client, dst_client, src, dst = _cross_account_pair("nopol", policy=None)
    try:
        src_client.put_object(Bucket=src, Key="telemetry/k", Body=b"x")
        assert src_client.head_object(Bucket=src, Key="telemetry/k")["ReplicationStatus"] == "FAILED"
        with pytest.raises(ClientError):
            dst_client.head_object(Bucket=dst, Key="telemetry/k")
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


def test_s3_replication_cross_account_requires_owner_override_grant(s3):
    """AccessControlTranslation needs s3:ObjectOwnerOverrideToBucketOwner in
    the destination policy unless the destination is BucketOwnerEnforced."""
    partial = _dest_policy("qa-xrepl-dst-own", actions=("s3:ReplicateObject",))
    src_client, dst_client, src, dst = _cross_account_pair("own", policy=partial, ownership="ObjectWriter")
    try:
        src_client.put_object(Bucket=src, Key="telemetry/k", Body=b"x")
        assert src_client.head_object(Bucket=src, Key="telemetry/k")["ReplicationStatus"] == "FAILED"
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


def test_s3_replication_cross_account_bucket_owner_enforced_needs_no_override_grant(s3):
    """BucketOwnerEnforced, the default, owns replicas without s3:ObjectOwnerOverrideToBucketOwner."""
    partial = _dest_policy("qa-xrepl-dst-enf", actions=("s3:ReplicateObject",))
    src_client, dst_client, src, dst = _cross_account_pair("enf", policy=partial)
    try:
        src_client.put_object(Bucket=src, Key="telemetry/k", Body=b"x")
        assert src_client.head_object(Bucket=src, Key="telemetry/k")["ReplicationStatus"] == "COMPLETED"
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


def test_s3_replication_cross_account_without_destination_account(s3):
    """The bucket name identifies the destination; Account is only for owner override."""
    src_client, dst_client, src, dst = _cross_account_pair("noacct", translate=False)
    try:
        src_client.put_object(Bucket=src, Key="telemetry/k", Body=b"x")
        assert src_client.head_object(Bucket=src, Key="telemetry/k")["ReplicationStatus"] == "COMPLETED"
        assert dst_client.head_object(Bucket=dst, Key="telemetry/k")["ReplicationStatus"] == "REPLICA"
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


def test_s3_replication_filter_round_trips_tag_and_and(s3):
    src, dst = _bucket(s3, versioned=True), _bucket(s3, versioned=True)
    rules = [
        {"ID": "tag", "Priority": 1, "Status": "Enabled", "DeleteMarkerReplication": {"Status": "Disabled"},
         "Filter": {"Tag": {"Key": "k", "Value": "v"}}, "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}},
        {"ID": "and", "Priority": 2, "Status": "Enabled", "DeleteMarkerReplication": {"Status": "Disabled"},
         "Filter": {"And": {"Prefix": "logs/", "Tags": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}]}},
         "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}},
    ]
    s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": rules})
    got = s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"]
    assert [r["Filter"] for r in got] == [r["Filter"] for r in rules]


# Replication rule options (#2311). Capture keys refer to the AWS captures: "run1 C.." to
# s3-replication-destination.json, "repeats ..." to repeats.json (keys s3:...), "U..", "M:..." to 2311-uncertainties.json, "r2 R.." to 2311-uncertainties-2.json
# "r3 P.." to 2311-uncertainties-3.json, "r4 P.." to 2311-uncertainties-4.json, "r5 U7w.." to
# 2311-uncertainties-5.json and "r7 .." to 2311-uncertainties-7.json.
_REPL_KEY = "arn:aws:kms:us-east-1:000000000000:key/replica"  # AWS accepts it (U6:tests-fake-key)
_REPL_MALFORMED = ("MalformedXML", "The XML you provided was not well-formed or did not validate "
                                   "against our published schema", None)
_REPL_RTC15 = {"Status": "Enabled", "Time": {"Minutes": 15}}
_REPL_MET15 = {"Status": "Enabled", "EventThreshold": {"Minutes": 15}}
_REPL_SSE_ON = {"SseKmsEncryptedObjects": {"Status": "Enabled"}}


def _repl_rule(dst, dest=None, rid="r", **kw):
    """A minimal rule AWS accepts (Filter, Priority, DeleteMarkerReplication), plus *kw*."""
    rule = {"ID": rid, "Priority": 1, "Filter": {"Prefix": ""}, "Status": "Enabled",
            "DeleteMarkerReplication": {"Status": "Disabled"}, "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}}
    rule.update(kw)
    rule["Destination"] = {**rule["Destination"], **(dest or {})}
    return rule


def _repl_v1_rule(dst, dest=None, rid="r", **kw):
    """A rule in the older Prefix schema: no Filter, Priority or DeleteMarkerReplication."""
    rule = {"ID": rid, "Prefix": "logs/", "Status": "Enabled", "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}}
    rule.update(kw)
    rule["Destination"] = {**rule["Destination"], **(dest or {})}
    return rule


def _repl_options_rule(dst):
    """The reproduction rule of the issue, accepted and returned unchanged (repeats s3:C9:roundtrip-wire-order)."""
    return _repl_rule(dst, rid="options", Filter={"Prefix": "kms/"}, DeleteMarkerReplication={"Status": "Enabled"},
                      SourceSelectionCriteria={"SseKmsEncryptedObjects": {"Status": "Enabled"},
                                               "ReplicaModifications": {"Status": "Enabled"}},
                      dest={"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY},
                            "ReplicationTime": _REPL_RTC15, "Metrics": _REPL_MET15})


def _repl_key(key):
    """The reproduction rule with *key* as its ReplicaKmsKeyID, the request AWS was sent for every key-form capture."""
    return {"key": key}


def _repl_case_rule(dst, extra):
    extra = dict(extra)
    if "key" in extra:
        rule = _repl_options_rule(dst)
        rule["Destination"]["EncryptionConfiguration"]["ReplicaKmsKeyID"] = extra["key"]
        return rule
    return (_repl_v1_rule if extra.pop("v1", False) else _repl_rule)(dst, **extra)


@pytest.fixture
def repl_pair(s3):
    src, dst = _bucket(s3, versioned=True), _bucket(s3, versioned=True)
    yield src, dst
    s3.delete_bucket_replication(Bucket=src)
    s3.delete_bucket(Bucket=src)
    s3.delete_bucket(Bucket=dst)


def test_s3_replication_round_trips_rule_and_destination_options(s3, repl_pair):
    """Each rule comes back exactly as put, options included, and nothing is defaulted in
    (repeats s3:C1:roundtrip-three-rule, U7:three-rule-round-trip). A second put replaces the
    whole configuration (repeats s3:C4:second-put-replaces, U7:second-put-replaces)."""
    src, dst = repl_pair
    full = _repl_options_rule(dst)
    plain = _repl_rule(dst, rid="plain", Priority=2, Filter={"Prefix": "plain/"})
    metrics = _repl_rule(dst, rid="metrics", Priority=3, Filter={"Prefix": "m/"}, dest={"Metrics": {"Status": "Disabled"}})
    s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [full, plain, metrics]})
    got = s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]
    assert got["Rules"] == [full, plain, metrics]
    assert got["Rules"][0]["Destination"]["ReplicationTime"]["Time"]["Minutes"] == 15

    s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={
        "Role": _REPL_ROLE, "Rules": [{**plain, "ID": "options"}]})
    assert s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"] == [{**plain, "ID": "options"}]


@pytest.mark.parametrize("extra", [
    pytest.param({"dest": {"Metrics": {"Status": "Enabled"}}}, id="U7:metrics-enabled-bare-no-rtc"),
    pytest.param({"dest": {"Metrics": {"Status": "Disabled"}}}, id="U7:metrics-disabled-bare-no-rtc"),
    pytest.param({"SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Disabled"}}},
                 id="U7:replicamods-disabled-only"),
    pytest.param({"dest": {"ReplicationTime": {"Status": "Disabled", "Time": {"Minutes": 15}},
                           "Metrics": {"Status": "Enabled"}}}, id="U3:rtc-disabled+metrics-enabled"),
    pytest.param({"dest": {"ReplicationTime": {"Status": "Disabled", "Time": {"Minutes": 15}},
                           "Metrics": {"Status": "Disabled", "EventThreshold": {"Minutes": 15}}}},
                 id="U3:rtc-disabled+metrics-disabled-et15"),
    pytest.param({"SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "Disabled"}},
                  "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY}}}, id="M:ssekms-disabled+enc"),
    pytest.param({"v1": True, "SourceSelectionCriteria": _REPL_SSE_ON,
                  "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY}}}, id="U7:v1-ssekms+enc"),
    pytest.param({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"EncryptionConfiguration": {
        "ReplicaKmsKeyID": "arn:aws:kms:us-east-1:000000000000:alias/replica"}}}, id="U6:alias-arn-nonexistent"),
    *[pytest.param(_repl_key(key), id=f"r2 R3:{name}") for name, key in (
        ("kms-key-empty-id", "arn:aws:kms:us-east-1:000000000000:key/"),
        ("kms-alias-empty", "arn:aws:kms:us-east-1:000000000000:alias/"),
        ("kms-no-account", "arn:aws:kms:us-east-1::key/5f0e8a54-1f6c-4d2a-9d55-2b8e3c1f0a11"),
        ("kms-account-abc", "arn:aws:kms:us-east-1:abc:key/5f0e8a54-1f6c-4d2a-9d55-2b8e3c1f0a11"))],
    *[pytest.param(_repl_key(key), id=f"r3 P2:{name}") for name, key in (
        ("resource-with-colon", "arn:aws:kms:us-east-1:000000000000:key/abc:def"),
        ("resource-with-slashes", "arn:aws:kms:us-east-1:000000000000:key/a/b"))],
    pytest.param(_repl_key("arn:aws:kms:us-east-1:000000000000:key/abc\ndef"), id="r7 K:key-newline-in-resource"),
])
def test_s3_replication_accepts_option_combinations(s3, repl_pair, extra):
    """Each of these single-rule configurations was accepted by AWS and came back unchanged
    (capture key in the parameter id)."""
    src, dst = repl_pair
    rule = _repl_case_rule(dst, extra)
    s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [rule]})
    assert s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"] == [rule]


def _repl_time(status, minutes):
    return {"Status": status, "Time": {"Minutes": minutes}}


def _repl_metrics(status, minutes=None):
    return {"Status": status, **({"EventThreshold": {"Minutes": minutes}} if minutes is not None else {})}


_REPL_TIME_MINUTES = ("InvalidArgument", "Invalid time minute value", "ReplicationTime")
_REPL_EVENT_MINUTES = ("InvalidArgument", "Invalid event threshold minute value", "ReplicationMetrics")
_REPL_BOTH_OR_NEITHER = ("InvalidRequest", "Replication destination must contain both ReplicationTime and "
                                           "Metrics or neither.", None)
_REPL_NO_THRESHOLD = ("InvalidArgument", "Metrics cannot contain an event threshold when ReplicationTime is "
                                         "not specified or Disabled", "Metrics")
_REPL_NEEDS_THRESHOLD = ("InvalidArgument", "ReplicationMetrics must contain an event threshold", "ReplicationMetrics")
_REPL_SAME_STATUS = ("InvalidArgument", "ReplicationTime and ReplicationMetrics should have the same status",
                     "ReplicationTime")
_REPL_NEEDS_KEY = ("InvalidRequest", "ReplicaKmsKeyID must be specified if SseKmsEncryptedObjects tag is present.", None)
_REPL_NEEDS_SSE = ("InvalidRequest", "SseKmsEncryptedObjects must be specified if EncryptionConfiguration is present.",
                   None)
_REPL_BAD_KEY = ("InvalidArgument", "Invalid ReplicaKmsKeyID ARN.", "ReplicaKmsKeyID")
_REPL_KEY_REGION = ("InvalidRequest", "KMS key must be in the same region as the destination bucket.", None)
_REPL_DEST_VERSIONING = ("InvalidRequest", "Destination bucket must have versioning enabled.", None)
_REPL_V1 = "{} cannot be used for this version of the replication configuration schema.{}Please refer to the S3 " \
           "Developer Guide for more information."


def _repl_dest(**dest):
    return {"dest": dest}


_REPL_REFUSALS = [
    # schema errors
    ({"dest": {"Metrics": {"Status": "On"}}}, _REPL_MALFORMED, "U7:metrics-status-On"),
    (_repl_dest(ReplicationTime=_repl_time("enabled", 15), Metrics=_REPL_MET15), _REPL_MALFORMED,
     "U7:rtc-status-lowercase"),
    ({"SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "On"}},
      "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY}}}, _REPL_MALFORMED, "M:ssekms-status-on"),
    ({"SourceSelectionCriteria": {"ReplicaModifications": {"Status": "On"}}}, _REPL_MALFORMED, "M:replicamods-status-on"),
    (_repl_dest(ReplicationTime=_repl_time("Enabled", 2147483648), Metrics=_REPL_MET15), _REPL_MALFORMED,
     "U4:rtc-minutes-2147483648"),
    (_repl_dest(ReplicationTime=_repl_time("Enabled", -2147483649), Metrics=_REPL_MET15), _REPL_MALFORMED,
     "r2 R1:rtc-minutes-below-int"),
    (_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Enabled", 2147483648)), _REPL_MALFORMED,
     "U4:et-minutes-2147483648"),
    (_repl_dest(ReplicationTime=_repl_time("On", 15)), _REPL_MALFORMED, "U2:rtc-status-on-no-metrics"),
    # minutes
    *[(_repl_dest(ReplicationTime=_repl_time("Enabled", n), Metrics=_REPL_MET15), _REPL_TIME_MINUTES, key)
      for n, key in {-2147483648: 'r2 R1:rtc-minutes-min-int', -1: 'U7:rtc-minutes-minus1', 0: 'U7:rtc-minutes-0', 1: 'U4:rtc-minutes-1', 5: 'U4:rtc-minutes-5', 10: 'run1 C26:rtc-minutes-10', 14: 'U7:rtc-minutes-14', 16: 'repeats C26:rtc-minutes-16', 30: 'repeats C26:rtc-minutes-30', 45: 'U4:rtc-minutes-45', 60: 'repeats C26:rtc-minutes-60', 120: 'U4:rtc-minutes-120', 1440: 'U4:rtc-minutes-1440', 2147483647: 'U4:rtc-minutes-2147483647'}.items()],
    *[(_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Enabled", n)), _REPL_EVENT_MINUTES, key)
      for n, key in {-1: 'U4:et-minutes--1', 0: 'U7:et-minutes-0', 5: 'U4:et-minutes-5', 10: 'run1 C26:et-minutes-10', 14: 'repeats C26:metrics-minutes-14', 16: 'repeats C26:metrics-minutes-16', 30: 'U7:et-minutes-30', 45: 'U4:et-minutes-45', 60: 'repeats C26:metrics-minutes-60'}.items()],
    (_repl_dest(ReplicationTime=_repl_time("Enabled", 10), Metrics=_repl_metrics("Enabled", 10)), _REPL_TIME_MINUTES,
     "U2:et-10+rtc-10"),
    # ReplicationTime and Metrics together
    (_repl_dest(ReplicationTime=_REPL_RTC15), _REPL_BOTH_OR_NEITHER, "U7:rtc-without-metrics"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 15)), _REPL_BOTH_OR_NEITHER, "U3:rtc-disabled-no-metrics"),
    (_repl_dest(ReplicationTime=_repl_time("Enabled", 10)), _REPL_TIME_MINUTES, "U2:rtc-10-no-metrics"),
    (_repl_dest(Metrics=_REPL_MET15), _REPL_NO_THRESHOLD, "U7:et15-no-rtc"),
    (_repl_dest(Metrics=_repl_metrics("Disabled", 15)), _REPL_NO_THRESHOLD, "U3:metrics-disabled-et15-no-rtc"),
    (_repl_dest(Metrics=_repl_metrics("Enabled", 10)), _REPL_NO_THRESHOLD, "U2:et-10+no-rtc"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 15), Metrics=_REPL_MET15), _REPL_NO_THRESHOLD,
     "U3:rtc-disabled+metrics-enabled-et15"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 15), Metrics=_repl_metrics("Enabled", 10)), _REPL_NO_THRESHOLD,
     "r2 R1:rtcD15+metE10"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 10), Metrics=_REPL_MET15), _REPL_TIME_MINUTES,
     "r2 R1:rtcD10+metE15"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 15), Metrics=_repl_metrics("Disabled", 10)),
     _REPL_EVENT_MINUTES, "r2 R1:rtcD15+metD10"),
    (_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Disabled", 10)), _REPL_EVENT_MINUTES,
     "U3:rtc-enabled+metrics-disabled-et10"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 15), Metrics=_repl_metrics("Disabled")), _REPL_NEEDS_THRESHOLD,
     "U3:rtc-disabled+metrics-disabled"),
    (_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Enabled")), _REPL_NEEDS_THRESHOLD,
     "U3:rtc-enabled+metrics-enabled-bare"),
    (_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Disabled")), _REPL_NEEDS_THRESHOLD,
     "U3:rtc-enabled+metrics-disabled"),
    (_repl_dest(ReplicationTime=_repl_time("Disabled", 10), Metrics=_repl_metrics("Disabled")), _REPL_TIME_MINUTES,
     "U3:rtc-disabled-10+metrics-disabled"),
    (_repl_dest(ReplicationTime=_repl_time("Enabled", 10), Metrics=_repl_metrics("Enabled")), _REPL_TIME_MINUTES,
     "r2 R1:rtcE10+metE-bare"),
    (_repl_dest(ReplicationTime=_REPL_RTC15, Metrics=_repl_metrics("Disabled", 15)), _REPL_SAME_STATUS,
     "U3:rtc-enabled+metrics-disabled-et15"),
    # SourceSelectionCriteria and EncryptionConfiguration
    ({"SourceSelectionCriteria": _REPL_SSE_ON}, _REPL_NEEDS_KEY, "U7:ssekms-enabled-no-enc"),
    ({"SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "Disabled"}}}, _REPL_NEEDS_KEY,
     "U7:ssekms-disabled-no-enc"),
    (_repl_dest(EncryptionConfiguration={"ReplicaKmsKeyID": _REPL_KEY}), _REPL_NEEDS_SSE, "U7:enc-no-ssc"),
    ({"SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Enabled"}},
      "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY}}}, _REPL_NEEDS_SSE, "M:replicamods-only+enc"),
    (_repl_dest(EncryptionConfiguration={"ReplicaKmsKeyID": "not-a-key"}), _REPL_NEEDS_SSE, "r2 R3:garbage-key-no-ssc"),
    ({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"ReplicationTime": _repl_time("Enabled", 10),
                                                         "Metrics": _REPL_MET15}}, _REPL_NEEDS_KEY,
     "U2:ssekms-no-enc+rtc-10"),
    ({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"ReplicationTime": _REPL_RTC15}}, _REPL_NEEDS_KEY,
     "r2 R2:ssekms-no-enc+rtc-no-metrics"),
    ({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"Metrics": _REPL_MET15}}, _REPL_NEEDS_KEY,
     "U2:et15-no-rtc+ssekms-no-enc"),
    (_repl_dest(EncryptionConfiguration={"ReplicaKmsKeyID": _REPL_KEY}, ReplicationTime=_REPL_RTC15), _REPL_NEEDS_SSE,
     "U2:enc-no-ssc+rtc-no-metrics"),
    (_repl_dest(EncryptionConfiguration={"ReplicaKmsKeyID": _REPL_KEY}, ReplicationTime=_repl_time("Enabled", 10),
                Metrics=_REPL_MET15), _REPL_NEEDS_SSE, "U2:enc-no-ssc+rtc-10"),
    # the replica key
    (_repl_key(""), ("InvalidArgument", "ReplicaKmsKeyID cannot be empty.", "ReplicaKmsKeyID"), "U6:empty-element"),
    *[(_repl_key(key), _REPL_BAD_KEY, f"U6/r2 R3/r3 P2:{name}") for name, key in (
        ("bare-random-uuid", "5f0e8a54-1f6c-4d2a-9d55-2b8e3c1f0a11"),
        ("alias-name-nonexistent", "alias/replica"),
        ("garbage", "not-a-key"),
        ("s3-arn", "arn:aws:s3:::some-bucket"),
        ("kms-other-resource", "arn:aws:kms:us-east-1:000000000000:grant/abc"),
        ("kms-five-parts", "arn:aws:kms:us-east-1:000000000000"),
        ("kms-padded", f" {_REPL_KEY} "),
        ("key-no-arn-prefix", "aws:kms:us-east-1:000000000000:key/replica"),
        ("key-no-slash", "arn:aws:kms:us-east-1:000000000000:key"),
        ("service-uppercase", "arn:aws:KMS:us-east-1:000000000000:key/replica"),
        ("arn-uppercase", "ARN:aws:kms:us-east-1:000000000000:key/replica"),
        ("partition-empty", "arn::kms:us-east-1:000000000000:key/replica"),
        ("whitespace-only", " "))],
    *[(_repl_key(key), _REPL_BAD_KEY, f"r7 K:{name}") for name, key in (
        ("key-resource-upper", "arn:aws:kms:us-east-1:000000000000:KEY/replica"),
        ("alias-resource-capital", "arn:aws:kms:us-east-1:000000000000:Alias/x"))],
    (_repl_key("arn:aws-cn:kms:us-east-1:000000000000:key/replica"),
     ("InvalidRequest", "KMS key ARN partition must match the destination bucket ARN partition.", None),
     "r2 R3:kms-partition-cn"),
    (_repl_key("arn:aws-cn:kms:us-west-2:000000000000:key/replica"),
     ("InvalidRequest", "KMS key ARN partition must match the destination bucket ARN partition.", None),
     "r3 P2:partition-cn-and-west"),
    ({"SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "Disabled"}}, "dest": {
        "EncryptionConfiguration": {"ReplicaKmsKeyID": "arn:aws:kms:us-west-2:000000000000:key/replica"}}},
     _REPL_KEY_REGION, "r3 P2:ssekms-disabled+west-key"),
    *[(_repl_key(key), _REPL_KEY_REGION, f"U6/r2 R3:{name}") for name, key in (
        ("other-region-key-arn", "arn:aws:kms:us-west-2:000000000000:key/replica"),
        ("kms-no-region", "arn:aws:kms::000000000000:key/replica"),
        ("kms-bogus-region", "arn:aws:kms:mars-1:000000000000:key/replica"),
        ("kms-other-region-alias", "arn:aws:kms:us-west-2:000000000000:alias/x"))],
    ({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": "not-a-key"},
                                                         "ReplicationTime": _repl_time("Enabled", 10),
                                                         "Metrics": _REPL_MET15}}, _REPL_BAD_KEY,
     "r2 R3:garbage-key+rtc10"),
    ({"SourceSelectionCriteria": _REPL_SSE_ON, "dest": {
        "EncryptionConfiguration": {"ReplicaKmsKeyID": "arn:aws:kms:us-west-2:000000000000:key/replica"},
        "ReplicationTime": _REPL_RTC15}}, _REPL_KEY_REGION, "r2 R3:west-key+rtc-no-metrics"),
    # the older Prefix schema, checked after the rule checks above
    ({"v1": True, "dest": {"ReplicationTime": _REPL_RTC15, "Metrics": _REPL_MET15}},
     ("InvalidRequest", _REPL_V1.format("ReplicationTime", ""), None), "M:v1-rtc+metrics"),
    ({"v1": True, "dest": {"ReplicationTime": _repl_time("Disabled", 15), "Metrics": _repl_metrics("Enabled")}},
     ("InvalidRequest", _REPL_V1.format("ReplicationTime", ""), None), "r2 R4:v1-rtc-disabled+metrics-enabled"),
    ({"v1": True, "dest": {"Metrics": _repl_metrics("Enabled")}},
     ("InvalidRequest", _REPL_V1.format("Metrics", ""), None), "M:v1-metrics-enabled"),
    ({"v1": True, "dest": {"Metrics": _repl_metrics("Disabled")}},
     ("InvalidRequest", _REPL_V1.format("Metrics", ""), None), "r2 R4:v1-metrics-disabled"),
    ({"v1": True, "SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Enabled"}}},
     ("InvalidRequest", _REPL_V1.format("ReplicaModifications", " "), None), "M:v1-replicamods"),
    ({"v1": True, "SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Disabled"}}},
     ("InvalidRequest", _REPL_V1.format("ReplicaModifications", " "), None), "r2 R4:v1-replicamods-disabled"),
    ({"v1": True, "SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "Enabled"},
                                              "ReplicaModifications": {"Status": "Enabled"}},
      "dest": {"EncryptionConfiguration": {"ReplicaKmsKeyID": _REPL_KEY}, "ReplicationTime": _REPL_RTC15,
               "Metrics": _REPL_MET15}},
     ("InvalidRequest", _REPL_V1.format("ReplicaModifications", " "), None), "M:v1-full-order"),
    ({"v1": True, "dest": {"ReplicationTime": _REPL_RTC15}}, _REPL_BOTH_OR_NEITHER, "M:v1-rtc-no-metrics"),
    ({"v1": True, "SourceSelectionCriteria": _REPL_SSE_ON}, _REPL_NEEDS_KEY, "M:v1-ssekms-no-enc"),
    ({"v1": True, "dest": {"ReplicationTime": _repl_time("Enabled", 10), "Metrics": _REPL_MET15}},
     _REPL_TIME_MINUTES, "r2 R4:v1-rtc10+metrics"),
    ({"v1": True, "dest": {"Metrics": _REPL_MET15}}, _REPL_NO_THRESHOLD, "r2 R4:v1-metrics-et15-no-rtc"),
    ({"v1": True, "SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Enabled"}},
      "dest": {"ReplicationTime": _REPL_RTC15}}, _REPL_BOTH_OR_NEITHER, "r2 R4:v1-replicamods+rtc-no-metrics"),
    ({"v1": True, "SourceSelectionCriteria": {"ReplicaModifications": {"Status": "Enabled"}, **_REPL_SSE_ON}},
     _REPL_NEEDS_KEY, "r2 R4:v1-replicamods+ssekms-no-enc"),
    ({"v1": True, "SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"ReplicationTime": _REPL_RTC15,
                                                                    "Metrics": _REPL_MET15}},
     _REPL_NEEDS_KEY, "r2 R4:v1-rtc+metrics+ssekms-no-enc"),
    ({"v1": True, "SourceSelectionCriteria": _REPL_SSE_ON, "dest": {"EncryptionConfiguration": {
        "ReplicaKmsKeyID": "not-a-key"}}}, _REPL_BAD_KEY, "r2 R3:v1-garbage-key"),
]


@pytest.mark.parametrize("extra,expected", [pytest.param(e, x, id=k) for e, x, k in _REPL_REFUSALS])
def test_s3_replication_refuses_options_like_aws(s3, repl_pair, extra, expected):
    """One single-rule put per AWS capture (key in the parameter id): the same code, message,
    status and ArgumentName, and nothing is stored."""
    src, dst = repl_pair
    rule = _repl_case_rule(dst, extra)
    with pytest.raises(ClientError) as exc:
        s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [rule]})
    code, message, argument = expected
    err = exc.value.response["Error"]
    assert (err["Code"], err["Message"], _status(exc)) == (code, message, 400)
    assert err.get("ArgumentName") == argument
    if argument == "ReplicaKmsKeyID" and rule["Destination"]["EncryptionConfiguration"]["ReplicaKmsKeyID"]:
        assert err["ArgumentValue"] == rule["Destination"]["EncryptionConfiguration"]["ReplicaKmsKeyID"]
    with pytest.raises(ClientError) as exc:
        s3.get_bucket_replication(Bucket=src)
    assert _code(exc) == "ReplicationConfigurationNotFoundError"


def test_s3_replication_checks_options_document_first_then_rule_by_rule(s3, repl_pair):
    """Schema errors anywhere in the document come first (U2:two-rules-crossfield-then-schema,
    U2:unversioned-dest+metrics-on). Then each rule in turn gets the destination versioning
    check and its option checks (r2 R5:rule1-crossfield+rule2-unversioned,
    R5:rule1-unversioned+rule2-crossfield, R5:rule1-minutes+rule2-unversioned,
    U2:unversioned-dest+rtc-no-metrics, U2:two-rules-minutes-then-crossfield,
    U2:two-rules-crossfield-then-minutes, U2:two-rules-second-bad). The Prefix-schema checks
    run after every rule (r2 R5:rule1-v1-rtc+rule2-unversioned)."""
    src, dst = repl_pair
    unv = _bucket(s3)
    try:
        cases = [
            ([_repl_rule(dst, rid="a", dest={"ReplicationTime": _REPL_RTC15}),
              _repl_rule(dst, rid="b", Priority=2, dest={"Metrics": {"Status": "On"}})], _REPL_MALFORMED),
            ([_repl_rule(unv, dest={"Metrics": {"Status": "On"}})], _REPL_MALFORMED),
            ([_repl_rule(unv, dest={"ReplicationTime": _REPL_RTC15})], _REPL_DEST_VERSIONING),
            ([_repl_rule(dst, rid="a", dest={"ReplicationTime": _REPL_RTC15}),
              _repl_rule(unv, rid="b", Priority=2)], _REPL_BOTH_OR_NEITHER),
            ([_repl_rule(unv, rid="a"),
              _repl_rule(dst, rid="b", Priority=2, dest={"ReplicationTime": _REPL_RTC15})], _REPL_DEST_VERSIONING),
            ([_repl_rule(dst, rid="a", dest={"ReplicationTime": _repl_time("Enabled", 10), "Metrics": _REPL_MET15}),
              _repl_rule(unv, rid="b", Priority=2)], _REPL_TIME_MINUTES),
            ([_repl_rule(dst, rid="a", dest={"ReplicationTime": _repl_time("Enabled", 10), "Metrics": _REPL_MET15}),
              _repl_rule(dst, rid="b", Priority=2, SourceSelectionCriteria=_REPL_SSE_ON)], _REPL_TIME_MINUTES),
            ([_repl_rule(dst, rid="a", SourceSelectionCriteria=_REPL_SSE_ON),
              _repl_rule(dst, rid="b", Priority=2, dest={"ReplicationTime": _repl_time("Enabled", 10),
                                                         "Metrics": _REPL_MET15})], _REPL_NEEDS_KEY),
            ([_repl_rule(dst, rid="a"), _repl_rule(dst, rid="b", Priority=2, dest={"ReplicationTime": _REPL_RTC15})],
             _REPL_BOTH_OR_NEITHER),
            ([_repl_v1_rule(dst, rid="a", Prefix="a/", dest={"ReplicationTime": _REPL_RTC15, "Metrics": _REPL_MET15}),
              _repl_v1_rule(unv, rid="b", Prefix="b/")], _REPL_DEST_VERSIONING),
        ]
        for rules, (code, message, argument) in cases:
            with pytest.raises(ClientError) as exc:
                s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": rules})
            err = exc.value.response["Error"]
            assert (err["Code"], err["Message"], err.get("ArgumentName")) == (code, message, argument), rules
            with pytest.raises(ClientError) as exc:
                s3.get_bucket_replication(Bucket=src)
            assert _code(exc) == "ReplicationConfigurationNotFoundError"
    finally:
        s3.delete_bucket(Bucket=unv)


def test_s3_replication_refused_put_keeps_the_stored_configuration(s3, repl_pair):
    """A refused put leaves the configuration already stored (U2:existing-then-refused,
    U2:existing-then-malformed)."""
    src, dst = repl_pair
    full = _repl_options_rule(dst)
    s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [full]})
    for dest, code in (({"ReplicationTime": _REPL_RTC15}, "InvalidRequest"), ({"Metrics": {"Status": "On"}}, "MalformedXML")):
        with pytest.raises(ClientError) as exc:
            s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={
                "Role": _REPL_ROLE, "Rules": [_repl_rule(dst, dest=dest)]})
        assert _code(exc) == code
        assert s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"] == [full]


def test_s3_replication_key_region_follows_the_destination_bucket(s3, repl_pair):
    """The replica key must be in the destination bucket's region: for a us-west-2 destination
    a us-east-1 key is refused and a us-west-2 key accepted (r5 U7w:west-dest+east-key,
    U7w:west-dest+west-key)."""
    src, _ = repl_pair
    dst = _bucket(s3, versioned=True, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    try:
        rule = _repl_rule(dst, SourceSelectionCriteria=_REPL_SSE_ON, dest={"EncryptionConfiguration": {
            "ReplicaKmsKeyID": _REPL_KEY}})
        with pytest.raises(ClientError) as exc:
            s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [rule]})
        assert exc.value.response["Error"]["Message"] == _REPL_KEY_REGION[1]
        rule["Destination"]["EncryptionConfiguration"]["ReplicaKmsKeyID"] = _REPL_KEY.replace("us-east-1", "us-west-2")
        s3.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [rule]})
        assert s3.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]["Rules"] == [rule]
    finally:
        s3.delete_bucket(Bucket=dst)


@pytest.fixture
def replication_state(monkeypatch):
    """A versioned bucket in fresh in-process S3 state, under account 000000000000."""
    fresh = {"_buckets": AccountScopedDict(), "_bucket_versioning": AccountScopedDict(),
             "_bucket_replication": AccountScopedDict()}
    for attr, store in fresh.items():
        monkeypatch.setattr(_s3_mod, attr, store)
    monkeypatch.setitem(_s3_mod._PERSISTED_BUCKET_DICTS, "bucket_versioning", fresh["_bucket_versioning"])
    monkeypatch.setitem(_s3_mod._PERSISTED_BUCKET_DICTS, "bucket_replication", fresh["_bucket_replication"])
    with request_scope("000000000000", "us-east-1"):
        _s3_mod._buckets["repl-options-src"] = {"objects": {}}
        _s3_mod._bucket_versioning["repl-options-src"] = "Enabled"
        yield "repl-options-src"


# The accepted reproduction rule as botocore serialises it (repeats s3:C9:raw-accepted-rule).
_REPL_OPTIONS_XML = (
    '<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Role>arn:aws:iam::000000000000:role/repl</Role>"
    "<Rule><ID>options</ID><Priority>1</Priority><Filter><Prefix>kms/</Prefix></Filter><Status>Enabled</Status>"
    "<SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects>"
    "<ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria>"
    "<Destination><Bucket>arn:aws:s3:::repl-options-dst</Bucket>"
    f"<EncryptionConfiguration><ReplicaKmsKeyID>{_REPL_KEY}</ReplicaKmsKeyID></EncryptionConfiguration>"
    "<ReplicationTime><Status>Enabled</Status><Time><Minutes>15</Minutes></Time></ReplicationTime>"
    "<Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics>"
    "</Destination><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication></Rule>"
    "</ReplicationConfiguration>")


def _replication_tree(body):
    def tree(el):
        name = el.tag.split("}")[-1]
        return {name: [tree(c) for c in el]} if len(el) else name
    return tree(ElementTree.fromstring(body))


def test_s3_replication_options_wire_form_and_persistence(replication_state):
    """GetBucketReplication emits the options where AWS does: SourceSelectionCriteria after the
    rule's existing members, and EncryptionConfiguration, Metrics, ReplicationTime last in the
    destination (repeats s3:C9:roundtrip-wire-order, M:full-dest-order), with Minutes as 15. The
    stored options survive a get_state/load round trip in the account that put them
    (MiniStack-only)."""
    bucket = replication_state
    assert _s3_mod._put_bucket_replication(bucket, _REPL_OPTIONS_XML.encode())[0] == 200
    state = _s3_mod.get_state()["bucket_replication"]
    _s3_mod._bucket_replication.clear()
    _s3_mod.load_persisted_state({"bucket_replication": state})

    status, _headers, body = _s3_mod._get_bucket_replication(bucket)
    assert status == 200
    (rule,) = [c for c in _replication_tree(body)["ReplicationConfiguration"] if isinstance(c, dict)]
    members = [next(iter(c)) if isinstance(c, dict) else c for c in rule["Rule"]]
    assert members[-2:] == ["SourceSelectionCriteria", "Destination"]
    assert rule["Rule"][-2] == {"SourceSelectionCriteria": [{"SseKmsEncryptedObjects": ["Status"]},
                                                           {"ReplicaModifications": ["Status"]}]}
    assert rule["Rule"][-1]["Destination"][-3:] == [
        {"EncryptionConfiguration": ["ReplicaKmsKeyID"]},
        {"Metrics": ["Status", {"EventThreshold": ["Minutes"]}]},
        {"ReplicationTime": ["Status", {"Time": ["Minutes"]}]}]
    root = ElementTree.fromstring(body)
    assert [el.text for el in root.iter(f"{{{_s3_mod.S3_NS}}}Minutes")] == ["15", "15"]
    with request_scope("111111111111", "us-east-1"):
        assert _s3_mod._bucket_replication.get(bucket) is None


def _repl_raw_error(bucket, body):
    status, _headers, out = _s3_mod._put_bucket_replication(bucket, body.encode())
    root = ElementTree.fromstring(out)
    return status, {el.tag: el.text for el in root}


_BOGUS = "<Bogus>x</Bogus>"


@pytest.mark.parametrize("old,new,expected", [
    pytest.param("<Metrics><Status>Enabled</Status>", "<Metrics>", _REPL_MALFORMED, id="U7:metrics-no-status"),
    pytest.param("<SseKmsEncryptedObjects><Status>Enabled</Status>", "<SseKmsEncryptedObjects>", _REPL_MALFORMED,
                 id="U7:ssekms-no-status"),
    pytest.param("<ReplicaModifications><Status>Enabled</Status>", "<ReplicaModifications>", _REPL_MALFORMED,
                 id="U7:replicamods-no-status"),
    pytest.param("<Time><Minutes>15</Minutes></Time>", "", _REPL_MALFORMED, id="U7:rtc-no-time"),
    pytest.param("<Time><Minutes>15</Minutes></Time>", "<Time></Time>", _REPL_MALFORMED, id="M:time-empty+metrics"),
    pytest.param("<EventThreshold><Minutes>15</Minutes></EventThreshold>", "<EventThreshold></EventThreshold>",
                 _REPL_MALFORMED, id="M:et-empty+rtc"),
    pytest.param("<Time><Minutes>15</Minutes>", "<Time><Minutes>fifteen</Minutes>", _REPL_MALFORMED,
                 id="U7:minutes-fifteen"),
    pytest.param("<Time><Minutes>15</Minutes>", "<Time><Minutes>15.0</Minutes>", _REPL_MALFORMED,
                 id="U4:rtc-minutes-15.0"),
    pytest.param("<ReplicationTime><Status>Enabled</Status>", "<ReplicationTime><Status> Enabled </Status>",
                 _REPL_MALFORMED, id="M:status-padded"),
    pytest.param("<ReplicationTime><Status>Enabled</Status>", "<ReplicationTime><Status></Status>", _REPL_MALFORMED,
                 id="M:rtc-empty-status"),
    pytest.param("<ReplicationTime><Status>Enabled</Status><Time><Minutes>15</Minutes></Time></ReplicationTime>",
                 "<ReplicationTime></ReplicationTime>", _REPL_MALFORMED, id="M:rtc-empty"),
    pytest.param("<Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics>",
                 "<Metrics></Metrics>", _REPL_MALFORMED, id="M:metrics-empty+rtc"),
    *[pytest.param(f"</{el}>", f"{_BOGUS}</{el}>", _REPL_MALFORMED, id=f"U5:bogus-in-{el}")
      for el in ("SourceSelectionCriteria", "SseKmsEncryptedObjects", "ReplicaModifications", "EncryptionConfiguration",
                 "ReplicationTime", "Time", "Metrics", "EventThreshold")],
    pytest.param("<Metrics><Status>Enabled</Status>", "<Metrics><Status>Enabled</Status><Status>Enabled</Status>",
                 _REPL_MALFORMED, id="r3 P1:dup-metrics-status"),
    pytest.param("<SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects>",
                 "<SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects>" * 2, _REPL_MALFORMED,
                 id="r3 P1:dup-ssekms"),
    pytest.param("<Time><Minutes>15</Minutes>", "<Time><Minutes>15</Minutes><Minutes>15</Minutes>", _REPL_MALFORMED,
                 id="r3 P1:dup-time-minutes"),
    pytest.param("</EncryptionConfiguration>", f"</EncryptionConfiguration><EncryptionConfiguration><ReplicaKmsKeyID>"
                 f"{_REPL_KEY}</ReplicaKmsKeyID></EncryptionConfiguration>", _REPL_MALFORMED,
                 id="r3 P1:dup-encryption-configuration"),
    pytest.param("</ReplicationTime>", "</ReplicationTime><ReplicationTime><Status>Enabled</Status><Time><Minutes>15"
                 "</Minutes></Time></ReplicationTime>", _REPL_MALFORMED, id="r3 P1:dup-replication-time"),
    pytest.param("</SourceSelectionCriteria>", "</SourceSelectionCriteria><SourceSelectionCriteria>"
                 "<ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria>",
                 _REPL_MALFORMED, id="r3 P1:dup-ssc"),
    *[pytest.param(old, new, _REPL_MALFORMED, id=f"r7 D:{name}") for name, old, new in (
        ("dup-metrics", "</Metrics>", "</Metrics><Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes>"
                                      "</EventThreshold></Metrics>"),
        ("dup-replicamods", "</ReplicaModifications>",
         "</ReplicaModifications><ReplicaModifications><Status>Enabled</Status></ReplicaModifications>"),
        ("dup-replica-kms-key-id", "</ReplicaKmsKeyID>", f"</ReplicaKmsKeyID><ReplicaKmsKeyID>{_REPL_KEY}</ReplicaKmsKeyID>"),
        ("dup-event-threshold", "</EventThreshold>", "</EventThreshold><EventThreshold><Minutes>15</Minutes></EventThreshold>"),
        ("dup-time", "</Time>", "</Time><Time><Minutes>15</Minutes></Time>"),
        ("dup-rtc-status", "<ReplicationTime><Status>Enabled</Status>",
         "<ReplicationTime><Status>Enabled</Status><Status>Enabled</Status>"),
        ("dup-ssekms-status", "<SseKmsEncryptedObjects><Status>Enabled</Status>",
         "<SseKmsEncryptedObjects><Status>Enabled</Status><Status>Enabled</Status>"),
        ("dup-et-minutes", "<EventThreshold><Minutes>15</Minutes>", "<EventThreshold><Minutes>15</Minutes><Minutes>15</Minutes>"))],
    pytest.param("<Time><Minutes>15</Minutes>", "<Time><Minutes></Minutes>", _REPL_TIME_MINUTES,
                 id="U4:rtc-minutes-empty"),
    pytest.param("<EventThreshold><Minutes>15</Minutes>", "<EventThreshold><Minutes></Minutes>", _REPL_EVENT_MINUTES,
                 id="r2 R1:rtcE15+metE-empty-minutes"),
    pytest.param("<Time><Minutes>15</Minutes>", "<Time><Minutes> </Minutes>", _REPL_TIME_MINUTES,
                 id="r4 P7:rtc-minutes-whitespace-only"),
    pytest.param("<EventThreshold><Minutes>15</Minutes>", "<EventThreshold><Minutes> </Minutes>", _REPL_EVENT_MINUTES,
                 id="r4 P7:et-minutes-whitespace-only"),
    pytest.param(f"<ReplicaKmsKeyID>{_REPL_KEY}</ReplicaKmsKeyID>", "", (
        "InvalidRequest", "ReplicaKmsKeyID tag must be specified in EncryptionConfiguration.", None),
        id="U7:empty-enc+ssekms"),
    pytest.param(f"<ReplicaKmsKeyID>{_REPL_KEY}</ReplicaKmsKeyID>" + "</EncryptionConfiguration><ReplicationTime>"
                 "<Status>Enabled</Status><Time><Minutes>15</Minutes>",
                 "</EncryptionConfiguration><ReplicationTime><Status>Enabled</Status><Time><Minutes>10</Minutes>", (
                     "InvalidRequest", "ReplicaKmsKeyID tag must be specified in EncryptionConfiguration.", None),
                 id="r2 R2:empty-enc+ssekms+rtc10"),
    pytest.param("<SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects>"
                 "<ReplicaModifications><Status>Enabled</Status></ReplicaModifications>", "", (
                     "InvalidRequest", "SourceSelectionCriteria cannot be empty.", None), id="U2:empty-ssc+enc"),
])
def test_s3_replication_raw_option_bodies_like_aws(replication_state, old, new, expected):
    """Raw-XML variants of the accepted rule that botocore cannot send (capture key in the
    parameter id): the same code, message and ArgumentName, and nothing is stored."""
    assert old in _REPL_OPTIONS_XML
    status, err = _repl_raw_error(replication_state, _REPL_OPTIONS_XML.replace(old, new, 1))
    code, message, argument = expected
    assert (status, err["Code"], err["Message"], err.get("ArgumentName")) == (400, code, message, argument)
    assert _s3_mod._bucket_replication.get(replication_state) is None


def test_s3_replication_raw_empty_selection_and_encryption(replication_state):
    """Empty SourceSelectionCriteria and EncryptionConfiguration elements on a minimal rule
    (U7:empty-ssc, U7:empty-enc-no-ssc, r2 R2:empty-ssc+rtc10)."""
    def body(rule_extra="", dest_extra=""):
        return ('<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                "<Role>arn:aws:iam::000000000000:role/repl</Role><Rule><ID>r</ID><Priority>1</Priority>"
                f"<Filter><Prefix></Prefix></Filter><Status>Enabled</Status>{rule_extra}"
                f"<Destination><Bucket>arn:aws:s3:::repl-options-dst</Bucket>{dest_extra}</Destination>"
                "<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication></Rule>"
                "</ReplicationConfiguration>")
    empty_ssc = ("InvalidRequest", "SourceSelectionCriteria cannot be empty.")
    for raw, expected in (
        (body("<SourceSelectionCriteria></SourceSelectionCriteria>"), empty_ssc),
        (body("<SourceSelectionCriteria></SourceSelectionCriteria>",
              "<ReplicationTime><Status>Enabled</Status><Time><Minutes>10</Minutes></Time></ReplicationTime>"
              "<Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics>"),
         empty_ssc),
        (body(dest_extra="<EncryptionConfiguration></EncryptionConfiguration>"), _REPL_NEEDS_SSE[:2]),
    ):
        status, err = _repl_raw_error(replication_state, raw)
        assert (status, err["Code"], err["Message"]) == (400, *expected)
        assert _s3_mod._bucket_replication.get(replication_state) is None


@pytest.mark.parametrize("minutes", ["015", "+15", " 15 ", "\t15\n"])
def test_s3_replication_reads_minutes_as_an_integer(replication_state, minutes):
    """xs:int forms of 15 are accepted and returned as 15 (U4:rtc-minutes-015, U4:rtc-minutes-+15,
    U4:rtc-minutes-padded-15, U4:et-minutes-015, r3 P1:minutes-tab-newline)."""
    body = _REPL_OPTIONS_XML.replace("<Minutes>15</Minutes>", f"<Minutes>{minutes}</Minutes>")
    assert _s3_mod._put_bucket_replication(replication_state, body.encode())[0] == 200
    dest = _s3_mod._bucket_replication[replication_state]["Rules"][0]["Destination"]
    assert dest["ReplicationTime"]["Time"] == dest["Metrics"]["EventThreshold"] == {"Minutes": 15}


def test_s3_replication_empty_key_argument_value(replication_state):
    """An empty ReplicaKmsKeyID answers InvalidArgument with an empty ArgumentValue (U6:empty-element)."""
    body = _REPL_OPTIONS_XML.replace(_REPL_KEY, "", 1)
    status, err = _repl_raw_error(replication_state, body)
    assert (status, err["Code"], err["Message"], err["ArgumentName"]) == (
        400, "InvalidArgument", "ReplicaKmsKeyID cannot be empty.", "ReplicaKmsKeyID")
    assert err["ArgumentValue"] in (None, "")


def test_s3_replication_config_round_trips_cross_account_fields(s3):
    """GetBucketReplication answers the cross-account fields the PUT stored."""
    src_client, dst_client, src, dst = _cross_account_pair("cfg")
    try:
        config = src_client.get_bucket_replication(Bucket=src)["ReplicationConfiguration"]
        rule = config["Rules"][0]
        dest = rule["Destination"]
        assert dest["Account"] == _DEST_ACCOUNT
        assert dest["AccessControlTranslation"] == {"Owner": "Destination"}
        assert dest["Bucket"] == f"arn:aws:s3:::{dst}"
        assert rule["Filter"] == {"Prefix": ""}
        assert rule["DeleteMarkerReplication"] == {"Status": "Disabled"}
    finally:
        _purge_foreign(src_client, src)
        _purge_foreign(dst_client, dst)


# ── Event notifications ────────────────────────────────────────────────


def _notified(s3, sqs, events, versioned=True):
    bucket = _bucket(s3, versioned=versioned)
    url = sqs.create_queue(QueueName=bucket)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket=bucket, NotificationConfiguration={"QueueConfigurations": [{"QueueArn": arn, "Events": events}]}
    )
    return bucket, url


def _records(sqs, url, expected, timeout=10):
    records = []
    deadline = time.time() + timeout
    while len(records) < expected and time.time() < deadline:
        for m in sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
            records.extend(json.loads(m["Body"]).get("Records", []))
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    return records


def test_annotation_events(s3, sqs):
    bucket, url = _notified(s3, sqs, ["s3:ObjectAnnotation:*", "s3:ObjectCreated:*"])
    version = s3.put_object(Bucket=bucket, Key="k", Body=b"x")["VersionId"]
    assert [r["eventName"] for r in _records(sqs, url, 1)] == ["ObjectCreated:Put"]
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"abc")
    s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    records = sorted(_records(sqs, url, 2), key=lambda r: r["eventName"], reverse=True)
    # "PutObjectAnnotation and DeleteObjectAnnotation send only annotation-specific events."
    assert [r["eventName"] for r in records] == ["ObjectAnnotation:Put", "ObjectAnnotation:Delete"]
    put, delete = records
    assert put["objectAnnotation"] == [{"name": "a", "size": 3, "eTag": hashlib.md5(b"abc").hexdigest()}]
    assert delete["objectAnnotation"] == [{"name": "a"}]
    assert put["s3"]["object"]["versionId"] == version
    assert "sequencer" not in put["s3"]["object"]


def test_get_and_list_send_no_events(s3, sqs):
    bucket, url = _notified(s3, sqs, ["s3:ObjectAnnotation:*"])
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    assert len(_records(sqs, url, 1)) == 1
    s3.get_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")
    s3.list_object_annotations(Bucket=bucket, Key="k")
    assert _records(sqs, url, 1, timeout=2) == []


def test_copy_event_says_whether_annotations_came(s3, sqs):
    bucket, url = _notified(s3, sqs, ["s3:ObjectCreated:Copy"])
    s3.put_object(Bucket=bucket, Key="src", Body=b"x")
    s3.put_object_annotation(Bucket=bucket, Key="src", AnnotationName="a", AnnotationPayload=b"1")
    s3.copy_object(Bucket=bucket, Key="with", CopySource={"Bucket": bucket, "Key": "src"})
    s3.copy_object(Bucket=bucket, Key="without", CopySource={"Bucket": bucket, "Key": "src"}, AnnotationDirective="EXCLUDE")
    flags = {r["s3"]["object"]["key"]: r["s3"]["object"]["hasObjectAnnotation"] for r in _records(sqs, url, 2)}
    assert flags == {"with": True, "without": False}


# ── Persistence ────────────────────────────────────────────────────────


def test_annotations_survive_a_restart(s3_persist, tmp_path):
    s3mod = s3_persist
    bucket = "qa-annot-persist"
    s3mod._create_bucket(bucket, b"")
    s3mod._bucket_versioning[bucket] = "Enabled"
    v1 = s3mod._put_object(bucket, "k", b"one", {})[1]["x-amz-version-id"]
    s3mod._put_object_annotation(bucket, "k", b"on v1", {}, {"annotationName": ["a"]})
    v2 = s3mod._put_object(bucket, "k", b"two", {})[1]["x-amz-version-id"]
    s3mod._put_object_annotation(bucket, "k", b"on v2", {}, {"annotationName": ["a"]})

    s3mod._buckets._data.pop(("000000000000", bucket), None)
    for store in (s3mod._object_versions._data, s3mod._object_annotations._data):
        for k in [k for k in store if k[0] == "000000000000" and k[1][0] == bucket]:
            store.pop(k, None)
    s3mod._load_persisted_bucket("000000000000", bucket, os.path.join(str(tmp_path), "000000000000", bucket))

    assert s3mod._object_annotations[(bucket, "k", v1)]["a"]["payload"] == "on v1"
    assert s3mod._object_annotations[(bucket, "k", v2)]["a"]["payload"] == "on v2"


# --- Event notification records ---

def _queue(sqs, name):
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def _event_records(sqs, url, expected, timeout=10):
    """Every S3 record delivered to the queue, waiting until `expected` have arrived."""
    records = []
    deadline = time.time() + timeout
    while len(records) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        for m in resp.get("Messages", []):
            body = json.loads(m["Body"])
            records.extend(body.get("Records", []))
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    return records


@pytest.fixture
def notified(s3, sqs):
    """A bucket whose object events go to a fresh queue: (bucket, queue url, configure)."""
    suffix = uuid.uuid4().hex[:8]
    bucket = f"evt-records-{suffix}"
    s3.create_bucket(Bucket=bucket)
    url, arn = _queue(sqs, f"evt-records-{suffix}")

    def configure(*events, versioned=False):
        if versioned:
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={"QueueConfigurations": [{"QueueArn": arn, "Events": list(events)}]},
        )
        # The configuration's own s3:TestEvent carries no Records; drain it.
        _event_records(sqs, url, 0, timeout=1)

    return bucket, url, configure


def test_record_key_is_url_encoded(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*")
    s3.put_object(Bucket=bucket, Key="photos/red flower+(1).jpg", Body=b"x")
    [record] = _event_records(sqs, url, 1)
    # "`red flower.jpg` becomes `red+flower.jpg`"; slashes stay, a literal + is escaped.
    assert record["s3"]["object"]["key"] == "photos/red+flower%2B%281%29.jpg"
    assert record["eventVersion"] == "2.6"


def test_record_carries_the_version_the_put_made(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*", versioned=True)
    first = s3.put_object(Bucket=bucket, Key="doc.txt", Body=b"one")["VersionId"]
    second = s3.put_object(Bucket=bucket, Key="doc.txt", Body=b"two")["VersionId"]
    records = _event_records(sqs, url, 2)
    assert sorted(r["s3"]["object"]["versionId"] for r in records) == sorted([first, second])


def test_record_has_no_version_in_an_unversioned_bucket(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*")
    s3.put_object(Bucket=bucket, Key="plain.txt", Body=b"x")
    [record] = _event_records(sqs, url, 1)
    assert "versionId" not in record["s3"]["object"]


def test_sequencer_grows_with_each_write(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*", "s3:ObjectRemoved:*", versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="seq.txt", Body=b"1")["VersionId"]
    v2 = s3.put_object(Bucket=bucket, Key="seq.txt", Body=b"2")["VersionId"]
    marker = s3.delete_object(Bucket=bucket, Key="seq.txt")["VersionId"]
    by_version = {r["s3"]["object"]["versionId"]: r["s3"]["object"]["sequencer"] for r in _event_records(sqs, url, 3)}
    # Compared as S3 says to: left-pad the shorter with zeros, then compare.
    width = max(len(v) for v in by_version.values())
    ordered = [by_version[v].rjust(width, "0") for v in (v1, v2, marker)]
    assert ordered == sorted(ordered) and len(set(ordered)) == 3
    assert all(int(s, 16) >= 0 for s in ordered)


def test_delete_without_version_reports_the_marker(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*", versioned=True)
    s3.put_object(Bucket=bucket, Key="gone.txt", Body=b"x")
    marker = s3.delete_object(Bucket=bucket, Key="gone.txt")["VersionId"]
    [record] = _event_records(sqs, url, 1)
    assert record["eventName"] == "ObjectRemoved:DeleteMarkerCreated"
    assert record["s3"]["object"]["versionId"] == marker


def test_marker_is_not_an_object_removed_delete(s3, sqs, notified):
    bucket, url, configure = notified
    # Subscribed to permanent deletes only: a marker is its own event type.
    configure("s3:ObjectRemoved:Delete", versioned=True)
    version = s3.put_object(Bucket=bucket, Key="kept.txt", Body=b"x")["VersionId"]
    s3.delete_object(Bucket=bucket, Key="kept.txt")
    s3.delete_object(Bucket=bucket, Key="kept.txt", VersionId=version)
    [record] = _event_records(sqs, url, 1)
    assert record["eventName"] == "ObjectRemoved:Delete"
    assert record["s3"]["object"]["versionId"] == version
    assert _event_records(sqs, url, 1, timeout=2) == []


def test_copy_record_names_the_new_version(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:Copy", versioned=True)
    s3.put_object(Bucket=bucket, Key="src.txt", Body=b"x")
    copied = s3.copy_object(Bucket=bucket, Key="dst.txt", CopySource={"Bucket": bucket, "Key": "src.txt"})
    [record] = _event_records(sqs, url, 1)
    assert record["eventName"] == "ObjectCreated:Copy"
    assert record["s3"]["object"]["versionId"] == copied["VersionId"]


def test_multipart_record_names_the_new_version(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:CompleteMultipartUpload", versioned=True)
    upload = s3.create_multipart_upload(Bucket=bucket, Key="big.bin")["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key="big.bin", UploadId=upload, PartNumber=1, Body=b"x" * 16)
    done = s3.complete_multipart_upload(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
    )
    [record] = _event_records(sqs, url, 1)
    assert record["s3"]["object"]["versionId"] == done["VersionId"]


def test_delete_objects_sends_an_event_per_object(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*", versioned=True)
    for k in ("a.txt", "b.txt"):
        s3.put_object(Bucket=bucket, Key=k, Body=b"x")
    version = s3.put_object(Bucket=bucket, Key="c.txt", Body=b"x")["VersionId"]
    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": "a.txt"}, {"Key": "b.txt"}, {"Key": "c.txt", "VersionId": version}]},
    )
    records = _event_records(sqs, url, 3)
    got = sorted((r["s3"]["object"]["key"], r["eventName"]) for r in records)
    assert got == [
        ("a.txt", "ObjectRemoved:DeleteMarkerCreated"),
        ("b.txt", "ObjectRemoved:DeleteMarkerCreated"),
        ("c.txt", "ObjectRemoved:Delete"),
    ]


def test_delete_objects_in_an_unversioned_bucket(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*")
    s3.put_object(Bucket=bucket, Key="u.txt", Body=b"x")
    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "u.txt"}, {"Key": "never-there.txt"}]})
    [record] = _event_records(sqs, url, 1)
    assert (record["s3"]["object"]["key"], record["eventName"]) == ("u.txt", "ObjectRemoved:Delete")


def test_s3_create_owned_bucket_again(s3):
    east = f"owned-east-{_uuid_mod.uuid4().hex[:8]}"
    west = f"owned-west-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=east, ObjectOwnership="ObjectWriter", ACL="public-read")
    s3.create_bucket(Bucket=west, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    try:
        s3.create_bucket(Bucket=east)
        grants = s3.get_bucket_acl(Bucket=east)["Grants"]
        assert [g["Permission"] for g in grants] == ["FULL_CONTROL"]
        with pytest.raises(ClientError) as exc:
            s3.create_bucket(Bucket=west, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        assert exc.value.response["Error"]["Code"] == "BucketAlreadyOwnedByYou"
        assert exc.value.response["Error"]["Message"] == (
            "Your previous request to create the named bucket succeeded and you already own it.")
        assert exc.value.response["Error"]["BucketName"] == west
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    finally:
        s3.delete_bucket(Bucket=east)
        s3.delete_bucket(Bucket=west)


# Under AUTH=true a request without credentials is anonymous. S3 answers it only
# through the bucket owner's public grants (AllUsers ACL grants or a bucket policy
# for Principal "*", unless Public Access Block shuts them off); otherwise 403
# AccessDenied. Source: the S3 User Guide on anonymous requests and Block Public
# Access; not measured against AWS here.

def _anonymous_bucket(monkeypatch, auth=True, **bucket_headers):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", auth)
    bucket = f"anon-{uuid.uuid4().hex[:10]}"
    assert _lock_call("PUT", f"/{bucket}", bucket_headers)[0] == 200
    assert _lock_call("DELETE", f"/{bucket}", query={"publicAccessBlock": [""]})[0] == 204
    assert _lock_call("PUT", f"/{bucket}/k", {}, b"secret")[0] == 200
    return bucket


def _anonymous_call(method, path, headers=None, body=b"", internal=False):
    import asyncio

    from ministack.core.responses import request_scope
    from ministack.services import s3 as s3mod

    with request_scope("000000000000", "us-east-1"):
        return asyncio.run(s3mod.handle_request(method, path, headers or {}, body, {}, internal=internal))


def _public_read_policy(bucket):
    return json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:::{bucket}/*",
    }]}).encode()


def test_anonymous_request_to_a_private_bucket_is_denied_under_auth(monkeypatch):
    bucket = _anonymous_bucket(monkeypatch)
    for method, path, body in [("GET", f"/{bucket}/k", b""), ("HEAD", f"/{bucket}/k", b""),
                               ("GET", f"/{bucket}/missing", b""), ("GET", f"/{bucket}", b""),
                               ("PUT", f"/{bucket}/new", b"x"), ("DELETE", f"/{bucket}/k", b"")]:
        status, _, resp = _anonymous_call(method, path, body=body)
        assert status == 403, (method, path)
        if method != "HEAD":
            assert b"<Code>AccessDenied</Code>" in resp
    # The owner still reads it, and the object is still there.
    status, _, resp = _lock_call("GET", f"/{bucket}/k")
    assert (status, resp) == (200, b"secret")


def test_s3_create_bucket_object_ownership_header(s3):
    name = f"own-hdr-{uuid.uuid4().hex[:10]}"
    s3.create_bucket(Bucket=name, ObjectOwnership="ObjectWriter")
    try:
        rules = s3.get_bucket_ownership_controls(Bucket=name)["OwnershipControls"]["Rules"]
        assert rules == [{"ObjectOwnership": "ObjectWriter"}]
    finally:
        s3.delete_bucket(Bucket=name)


def test_anonymous_request_reads_a_public_object_under_auth(monkeypatch):
    # Object ACLs only count on a bucket with ACLs enabled (not BucketOwnerEnforced).
    bucket = _anonymous_bucket(monkeypatch, **{"x-amz-object-ownership": "BucketOwnerPreferred"})
    assert _lock_call("PUT", f"/{bucket}/pub", {"x-amz-acl": "public-read"}, b"open")[0] == 200
    status, _, resp = _anonymous_call("GET", f"/{bucket}/pub")
    assert (status, resp) == (200, b"open")
    assert _anonymous_call("GET", f"/{bucket}/k")[0] == 403


def test_anonymous_request_follows_a_public_bucket_policy_and_public_access_block(monkeypatch):
    bucket = _anonymous_bucket(monkeypatch)
    import asyncio

    from ministack.core.responses import request_scope
    from ministack.services import s3 as s3mod

    with request_scope("000000000000", "us-east-1"):
        status, _, _ = asyncio.run(s3mod.handle_request(
            "PUT", f"/{bucket}", {"authorization": _ROOT_AUTHORIZATION}, _public_read_policy(bucket),
            {"policy": [""]}))
    assert status == 204
    status, _, resp = _anonymous_call("GET", f"/{bucket}/k")
    assert (status, resp) == (200, b"secret")
    # The policy allows only s3:GetObject.
    assert _anonymous_call("PUT", f"/{bucket}/k", body=b"overwrite")[0] == 403

    block = (b"<PublicAccessBlockConfiguration><RestrictPublicBuckets>true</RestrictPublicBuckets>"
             b"</PublicAccessBlockConfiguration>")
    with request_scope("000000000000", "us-east-1"):
        status, _, _ = asyncio.run(s3mod.handle_request(
            "PUT", f"/{bucket}", {"authorization": _ROOT_AUTHORIZATION}, block, {"publicAccessBlock": [""]}))
    assert status == 200
    assert _anonymous_call("GET", f"/{bucket}/k")[0] == 403


def test_s3_bucket_owner_enforced_ignores_a_stored_bucket_acl():
    """A public bucket ACL stored before ACLs were disabled, as older persisted state can hold, grants nothing."""
    from ministack.core.responses import request_scope
    from ministack.services import s3 as s3mod

    bucket = f"boe-stored-{uuid.uuid4().hex[:8]}"
    headers = {"x-amz-object-ownership": "ObjectWriter", "x-amz-acl": "public-read"}
    assert _lock_call("PUT", f"/{bucket}", headers)[0] == 200
    with request_scope("000000000000", "us-east-1"):
        assert s3mod._foreign_bucket_allows("000000000000", bucket, "", "GET", {}, "")
        s3mod._buckets[bucket].pop("_ownership_controls")
        assert not s3mod._foreign_bucket_allows("000000000000", bucket, "", "GET", {}, "")


def test_anonymous_request_is_served_without_auth(monkeypatch):
    bucket = _anonymous_bucket(monkeypatch, auth=False)
    status, _, resp = _anonymous_call("GET", f"/{bucket}/k")
    assert (status, resp) == (200, b"secret")


def test_internal_call_without_credentials_is_not_anonymous(monkeypatch):
    bucket = _anonymous_bucket(monkeypatch)
    assert _anonymous_call("PUT", f"/{bucket}/delivered", body=b"x", internal=True)[0] == 200
# ---------------------------------------------------------------------------
# Bucket home region (#2158)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("headers,query,expected", [
    ({"authorization": "AWS4-HMAC-SHA256 Credential=AKID/20261012/eu-west-1/s3/aws4_request, "
                       "SignedHeaders=host, Signature=abc"}, {}, "eu-west-1"),
    ({}, {"X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
          "X-Amz-Credential": ["AKID/20261012/us-west-2/s3/aws4_request"]}, "us-west-2"),
    ({"authorization": "AWS4-HMAC-SHA256 Credential=AKID/20261012/eu-west-1/sqs/aws4_request, "
                       "SignedHeaders=host, Signature=abc"}, {}, None),
    ({"authorization": "AWS4-ECDSA-P256-SHA256 Credential=AKID/20261012/s3/aws4_request, "
                       "SignedHeaders=host, Signature=abc", "x-amz-region-set": "eu-west-1"}, {}, None),
    ({"authorization": "AWS AKID:sig"}, {}, None),
    ({}, {}, None),
    ({"authorization": "AWS4-HMAC-SHA256 Credential=broken"}, {}, None),
])
def test_s3_signed_region(headers, query, expected):
    from ministack.services import s3 as s3_svc

    assert s3_svc._signed_region(headers, query) == expected


def test_s3_error_extra_elements_and_headers():
    from xml.etree.ElementTree import fromstring

    from ministack.services import s3 as s3_svc

    status, headers, body = s3_svc._error("Plain", "msg", 400, "/b")
    assert status == 400 and headers == {"Content-Type": "application/xml"}
    assert [c.tag for c in fromstring(body)] == ["Code", "Message", "Resource", "RequestId"]

    status, headers, body = s3_svc._error(
        "PermanentRedirect", "moved", 301, "/b",
        extra={"Endpoint": "b.s3-eu-west-1.amazonaws.com", "Bucket": "b"},
        headers={"x-amz-bucket-region": "eu-west-1"},
    )
    root = fromstring(body)
    assert status == 301
    assert headers == {"Content-Type": "application/xml", "x-amz-bucket-region": "eu-west-1"}
    assert [c.tag for c in root] == ["Code", "Message", "Resource", "Endpoint", "Bucket", "RequestId"]
    assert root.findtext("Endpoint") == "b.s3-eu-west-1.amazonaws.com"


def test_s3_list_buckets_region_filter(s3):
    tag = _uuid_mod.uuid4().hex[:8]
    east = [f"lbr-{tag}-e{i}" for i in range(2)]
    west = [f"lbr-{tag}-w{i}" for i in range(3)]
    for name in east:
        s3.create_bucket(Bucket=name)
    for name in west:
        s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    euw1 = _regional_client("s3", "eu-west-1")
    prefix = f"lbr-{tag}-"
    try:
        listed = euw1.list_buckets(Prefix=prefix, BucketRegion="eu-west-1")["Buckets"]
        assert [b["Name"] for b in listed] == west
        assert {b["BucketRegion"] for b in listed} == {"eu-west-1"}

        listed = s3.list_buckets(Prefix=prefix, BucketRegion="us-east-1")["Buckets"]
        assert [b["Name"] for b in listed] == east

        # Pagination stays within the filter.
        names, token = [], None
        while True:
            kwargs = {"Prefix": prefix, "BucketRegion": "eu-west-1", "MaxBuckets": 1}
            if token:
                kwargs["ContinuationToken"] = token
            page = euw1.list_buckets(**kwargs)
            names += [b["Name"] for b in page["Buckets"]]
            token = page.get("ContinuationToken")
            if not token:
                break
        assert names == west

        # No filter lists every region.
        listed = euw1.list_buckets(Prefix=prefix)["Buckets"]
        assert [b["Name"] for b in listed] == sorted(east + west)
    finally:
        for name in east:
            s3.delete_bucket(Bucket=name)
        for name in west:
            euw1.delete_bucket(Bucket=name)


def test_s3_list_buckets_region_filter_must_match_endpoint(s3):
    with pytest.raises(ClientError) as exc:
        s3.list_buckets(BucketRegion="eu-west-1")
    err = exc.value.response["Error"]
    assert err["Code"] == "InvalidArgument"
    assert err["Message"] == ("Requests with bucket-region specified must be made to the "
                              "corresponding regional endpoint")
    assert err["ArgumentName"] == "bucket-region"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def _raw_signed(method, path, region, host=None, presign=False, body=b""):
    """Send a SigV4-signed S3 request without any client-side redirect
    handling. ``host`` overrides the Host header (signed as sent)."""
    import http.client

    from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    parsed = urlparse(ENDPOINT)
    netloc = host or parsed.netloc
    req = AWSRequest(method=method, url=f"http://{netloc}{path}", data=body)
    creds = Credentials("test", "test")
    if presign:
        S3SigV4QueryAuth(creds, "s3", region, expires=300).add_auth(req)
    else:
        req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
        S3SigV4Auth(creds, "s3", region).add_auth(req)
    target = urlparse(req.url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    headers = {k: v for k, v in req.headers.items()}
    headers["Host"] = netloc
    conn.request(method, target.path + (f"?{target.query}" if target.query else ""), body=body or None,
                 headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data


def _xml_error(body):
    from xml.etree.ElementTree import fromstring

    root = fromstring(body)
    return {child.tag: child.text for child in root}


@pytest.fixture
def euw1_bucket(s3):
    name = f"home-euw1-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    euw1 = _regional_client("s3", "eu-west-1")
    euw1.put_object(Bucket=name, Key="k.txt", Body=b"hello")
    yield name
    for obj in euw1.list_objects_v2(Bucket=name).get("Contents", []):
        euw1.delete_object(Bucket=name, Key=obj["Key"])
    euw1.delete_bucket(Bucket=name)


_REDIRECT_MSG = ("The bucket you are attempting to access must be addressed using the specified "
                 "endpoint. Please send all future requests to this endpoint.")


def test_s3_wrong_region_get_is_permanent_redirect(euw1_bucket):
    for presign in (False, True):
        status, headers, body = _raw_signed("GET", f"/{euw1_bucket}/k.txt", "us-west-2", presign=presign)
        assert status == 301, body
        assert headers["x-amz-bucket-region"] == "eu-west-1"
        err = _xml_error(body)
        assert err["Code"] == "PermanentRedirect"
        assert err["Message"] == _REDIRECT_MSG
        assert err["Endpoint"] == f"{euw1_bucket}.s3-eu-west-1.amazonaws.com"
        assert err["Bucket"] == euw1_bucket

    # A us-east-1 client on the global host is pointed at the global endpoint.
    status, headers, body = _raw_signed("PUT", f"/{euw1_bucket}/new.txt", "us-east-1", body=b"x")
    assert status == 301
    assert _xml_error(body)["Endpoint"] == f"{euw1_bucket}.s3.amazonaws.com"
    assert headers["x-amz-bucket-region"] == "eu-west-1"


def test_s3_wrong_region_head_is_bodiless_redirect(euw1_bucket):
    for path in (f"/{euw1_bucket}", f"/{euw1_bucket}/k.txt"):
        status, headers, body = _raw_signed("HEAD", path, "us-east-1")
        assert (status, body) == (301, b"")
        assert headers["x-amz-bucket-region"] == "eu-west-1"


def test_s3_wrong_region_at_home_endpoint_is_authorization_error(euw1_bucket):
    host = f"s3.eu-west-1.{urlparse(ENDPOINT).netloc}"
    status, headers, body = _raw_signed("GET", f"/{euw1_bucket}/k.txt", "us-east-1", host=host)
    assert status == 400
    err = _xml_error(body)
    assert err["Code"] == "AuthorizationHeaderMalformed"
    assert err["Message"] == ("The authorization header is malformed; the region 'us-east-1' is wrong; "
                              "expecting 'eu-west-1'")
    assert err["Region"] == "eu-west-1"
    assert "x-amz-bucket-region" not in headers

    vhost = f"{euw1_bucket}.s3.eu-west-1.{urlparse(ENDPOINT).netloc}"
    status, headers, body = _raw_signed("GET", "/k.txt", "us-east-1", host=vhost, presign=True)
    assert status == 400
    err = _xml_error(body)
    assert err["Code"] == "AuthorizationQueryParametersError"
    assert err["Message"] == ("Error parsing the X-Amz-Credential parameter; the region 'us-east-1' is "
                              "wrong; expecting 'eu-west-1'")
    assert err["Region"] == "eu-west-1"

    status, headers, body = _raw_signed("HEAD", f"/{euw1_bucket}", "us-east-1", host=host)
    assert (status, body) == (400, b"")
    assert headers["x-amz-bucket-region"] == "eu-west-1"
    status, headers, body = _raw_signed("HEAD", f"/{euw1_bucket}/k.txt", "us-east-1", host=host)
    assert (status, body) == (400, b"")
    assert "x-amz-bucket-region" not in headers


def test_s3_wrong_region_exemptions_and_match(euw1_bucket):
    status, _, body = _raw_signed("GET", f"/{euw1_bucket}?location", "us-east-1")
    assert status == 200 and b"eu-west-1" in body
    status, _, body = _raw_signed("GET", "/", "ap-south-1")
    assert status == 200 and euw1_bucket.encode() in body
    status, _, body = _raw_signed("GET", f"/{euw1_bucket}/k.txt", "eu-west-1")
    assert (status, body) == (200, b"hello")
    status, _, _ = _raw_signed("GET", f"/no-such-{euw1_bucket}/k.txt", "us-west-2")
    assert status == 404


def test_s3_unsigned_region_request_is_not_redirected(euw1_bucket):
    """Anonymous requests carry no signing region, so a public-read object in
    eu-west-1 answers 200 wherever the request lands."""
    import urllib.request

    euw1 = _regional_client("s3", "eu-west-1")
    euw1.put_bucket_ownership_controls(
        Bucket=euw1_bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "ObjectWriter"}]})
    euw1.put_object(Bucket=euw1_bucket, Key="public.txt", Body=b"open", ACL="public-read")
    with urllib.request.urlopen(f"{ENDPOINT}/{euw1_bucket}/public.txt") as resp:
        assert (resp.status, resp.read()) == (200, b"open")


def test_s3_cross_region_boto3_follows_redirect(s3):
    """boto3 configured for us-east-1 reaches a us-west-2 bucket through the
    PermanentRedirect + x-amz-bucket-region it gets back."""
    name = f"redir-usw2-{_uuid_mod.uuid4().hex[:8]}"
    usw2 = _regional_client("s3", "us-west-2")
    usw2.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    try:
        s3.put_object(Bucket=name, Key="a.txt", Body=b"redirected")
        assert s3.get_object(Bucket=name, Key="a.txt")["Body"].read() == b"redirected"
        assert [o["Key"] for o in s3.list_objects_v2(Bucket=name)["Contents"]] == ["a.txt"]
        assert s3.head_bucket(Bucket=name)["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"] == "us-west-2"
        assert s3.head_object(Bucket=name, Key="a.txt")["ContentLength"] == 10
        s3.delete_object(Bucket=name, Key="a.txt")
        assert usw2.list_objects_v2(Bucket=name)["KeyCount"] == 0
    finally:
        usw2.delete_bucket(Bucket=name)


def test_s3_cross_region_copy_and_upload_part_copy(s3):
    tag = _uuid_mod.uuid4().hex[:8]
    src, dst = f"copy-src-usw2-{tag}", f"copy-dst-euw1-{tag}"
    usw2, euw1 = _regional_client("s3", "us-west-2"), _regional_client("s3", "eu-west-1")
    usw2.create_bucket(Bucket=src, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    euw1.create_bucket(Bucket=dst, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    part = b"p" * (5 * 1024 * 1024)
    try:
        usw2.put_object(Bucket=src, Key="obj", Body=b"copy me")
        usw2.put_object(Bucket=src, Key="big", Body=part)
        euw1.copy_object(Bucket=dst, Key="copied", CopySource={"Bucket": src, "Key": "obj"})
        assert euw1.get_object(Bucket=dst, Key="copied")["Body"].read() == b"copy me"

        upload = euw1.create_multipart_upload(Bucket=dst, Key="multi")
        etag = euw1.upload_part_copy(Bucket=dst, Key="multi", UploadId=upload["UploadId"], PartNumber=1,
                                     CopySource={"Bucket": src, "Key": "big"})["CopyPartResult"]["ETag"]
        euw1.complete_multipart_upload(Bucket=dst, Key="multi", UploadId=upload["UploadId"],
                                       MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]})
        assert euw1.head_object(Bucket=dst, Key="multi")["ContentLength"] == len(part)
    finally:
        for client, bucket in ((usw2, src), (euw1, dst)):
            for obj in client.list_objects_v2(Bucket=bucket).get("Contents", []):
                client.delete_object(Bucket=bucket, Key=obj["Key"])
            client.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Cross-Region Replication (#2158)
# ---------------------------------------------------------------------------

_REPL_ROLE = "arn:aws:iam::000000000000:role/repl"


def _versioned_bucket(region, slug):
    client = _regional_client("s3", region)
    name = f"crr-{slug}-{_uuid_mod.uuid4().hex[:8]}"
    kwargs = {} if region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region}}
    client.create_bucket(Bucket=name, **kwargs)
    client.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
    return client, name


def _replicate(client, src, dst, **rule):
    rule = {"ID": "r1", "Status": "Enabled", "Destination": {"Bucket": f"arn:aws:s3:::{dst}"}, **rule}
    client.put_bucket_replication(Bucket=src, ReplicationConfiguration={"Role": _REPL_ROLE, "Rules": [rule]})


def _purge(client, bucket):
    lv = client.list_object_versions(Bucket=bucket)
    for v in lv.get("Versions", []) + lv.get("DeleteMarkers", []):
        client.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    client.delete_bucket(Bucket=bucket)


@pytest.fixture
def crr_pair():
    """A us-east-1 source and an eu-west-1 destination, both versioned."""
    use1, src = _versioned_bucket("us-east-1", "src")
    euw1, dst = _versioned_bucket("eu-west-1", "dst")
    yield use1, src, euw1, dst
    _purge(use1, src)
    _purge(euw1, dst)


def test_s3_replication_cross_region_replica_readable_in_destination(crr_pair):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, Filter={"Prefix": ""}, DeleteMarkerReplication={"Status": "Disabled"})
    use1.put_object(Bucket=src, Key="a.txt", Body=b"across regions")
    assert use1.head_object(Bucket=src, Key="a.txt")["ReplicationStatus"] == "COMPLETED"
    replica = euw1.get_object(Bucket=dst, Key="a.txt")
    assert replica["ReplicationStatus"] == "REPLICA"
    assert replica["Body"].read() == b"across regions"


def _delete_markers(client, bucket, key):
    return [m for m in client.list_object_versions(Bucket=bucket).get("DeleteMarkers", []) if m["Key"] == key]


def test_s3_delete_marker_replication_enabled_v2_rule(crr_pair):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, Filter={"Prefix": ""}, DeleteMarkerReplication={"Status": "Enabled"})
    use1.put_object(Bucket=src, Key="gone.txt", Body=b"x")
    use1.put_object(Bucket=src, Key="batch.txt", Body=b"y")
    use1.delete_object(Bucket=src, Key="gone.txt")
    use1.delete_objects(Bucket=src, Delete={"Objects": [{"Key": "batch.txt"}]})
    for key in ("gone.txt", "batch.txt"):
        assert len(_delete_markers(euw1, dst, key)) == 1
        with pytest.raises(ClientError) as exc:
            euw1.get_object(Bucket=dst, Key=key)
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"


def test_s3_delete_marker_replication_v1_rule(crr_pair):
    """A rule without Filter is V1, which replicates delete markers from user deletes."""
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, Prefix="")
    use1.put_object(Bucket=src, Key="v1.txt", Body=b"x")
    use1.delete_object(Bucket=src, Key="v1.txt")
    assert len(_delete_markers(euw1, dst, "v1.txt")) == 1


def test_s3_delete_marker_replication_version_id_delete_stays_local(crr_pair):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, Filter={"Prefix": ""}, DeleteMarkerReplication={"Status": "Enabled"})
    vid = use1.put_object(Bucket=src, Key="v.txt", Body=b"x")["VersionId"]
    use1.delete_object(Bucket=src, Key="v.txt", VersionId=vid)
    assert euw1.get_object(Bucket=dst, Key="v.txt")["Body"].read() == b"x"
    assert _delete_markers(euw1, dst, "v.txt") == []


@pytest.mark.parametrize("rule", [
    {"Filter": {"Prefix": ""}, "DeleteMarkerReplication": {"Status": "Disabled"}},
    {"Filter": {"Tag": {"Key": "keep", "Value": "yes"}}, "DeleteMarkerReplication": {"Status": "Enabled"}},
], ids=["disabled", "tag-filter"])
def test_s3_delete_marker_replication_not_replicated(crr_pair, rule):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, **rule)
    use1.put_object(Bucket=src, Key="k.txt", Body=b"x", Tagging="keep=yes")
    assert euw1.get_object(Bucket=dst, Key="k.txt")["Body"].read() == b"x"
    use1.delete_object(Bucket=src, Key="k.txt")
    assert _delete_markers(euw1, dst, "k.txt") == []
    assert euw1.get_object(Bucket=dst, Key="k.txt")["Body"].read() == b"x"


def _replicated_keys(client, bucket):
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket=bucket).get("Contents", []))


def test_s3_replication_tag_filter_single_tag(crr_pair):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst, Filter={"Tag": {"Key": "replicate", "Value": "yes"}},
               DeleteMarkerReplication={"Status": "Disabled"})
    use1.put_object(Bucket=src, Key="tagged", Body=b"1", Tagging="replicate=yes&other=1")
    use1.put_object(Bucket=src, Key="wrong-value", Body=b"2", Tagging="replicate=no")
    use1.put_object(Bucket=src, Key="untagged", Body=b"3")
    use1.copy_object(Bucket=src, Key="copy-replaced", CopySource={"Bucket": src, "Key": "untagged"},
                     TaggingDirective="REPLACE", Tagging="replicate=yes")
    use1.copy_object(Bucket=src, Key="copy-kept", CopySource={"Bucket": src, "Key": "tagged"})
    assert _replicated_keys(euw1, dst) == ["copy-kept", "copy-replaced", "tagged"]
    assert "ReplicationStatus" not in use1.head_object(Bucket=src, Key="untagged")


def test_s3_replication_tag_filter_and(crr_pair):
    use1, src, euw1, dst = crr_pair
    _replicate(use1, src, dst,
               Filter={"And": {"Prefix": "logs/", "Tags": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}]}},
               DeleteMarkerReplication={"Status": "Disabled"})
    use1.put_object(Bucket=src, Key="logs/both", Body=b"x", Tagging="a=1&b=2")
    use1.put_object(Bucket=src, Key="logs/one", Body=b"x", Tagging="a=1")
    use1.put_object(Bucket=src, Key="data/both", Body=b"x", Tagging="a=1&b=2")
    assert _replicated_keys(euw1, dst) == ["logs/both"]


def test_s3_two_way_replication_has_no_loop():
    use1, a = _versioned_bucket("us-east-1", "a")
    euw1, b = _versioned_bucket("eu-west-1", "b")
    try:
        _replicate(use1, a, b, Filter={"Prefix": ""}, DeleteMarkerReplication={"Status": "Disabled"})
        _replicate(euw1, b, a, Filter={"Prefix": ""}, DeleteMarkerReplication={"Status": "Disabled"})
        use1.put_object(Bucket=a, Key="from-a", Body=b"a")
        euw1.put_object(Bucket=b, Key="from-b", Body=b"b")
        for client, bucket in ((use1, a), (euw1, b)):
            versions = client.list_object_versions(Bucket=bucket)["Versions"]
            assert sorted(v["Key"] for v in versions) == ["from-a", "from-b"]
        assert euw1.head_object(Bucket=b, Key="from-a")["ReplicationStatus"] == "REPLICA"
        assert use1.head_object(Bucket=a, Key="from-b")["ReplicationStatus"] == "REPLICA"
        assert use1.head_object(Bucket=a, Key="from-a")["ReplicationStatus"] == "COMPLETED"
    finally:
        _purge(use1, a)
        _purge(euw1, b)


# ---------------------------------------------------------------------------
# Multi-Region Access Points through S3 Control (#2158)
# ---------------------------------------------------------------------------

_ACCOUNT = "000000000000"


@pytest.fixture
def s3control_dns():
    """boto3 prefixes S3 Control hosts with the account id."""
    with patch_endpoint_dns():
        yield


@pytest.fixture
def mrap_members():
    """One bucket in us-east-1 and one in eu-west-1, each holding who.txt."""
    tag = _uuid_mod.uuid4().hex[:8]
    use1, use1_bucket = _versioned_bucket("us-east-1", f"mrap-{tag}-use1")
    euw1, euw1_bucket = _versioned_bucket("eu-west-1", f"mrap-{tag}-euw1")
    use1.put_object(Bucket=use1_bucket, Key="who.txt", Body=b"use1")
    euw1.put_object(Bucket=euw1_bucket, Key="who.txt", Body=b"euw1")
    yield use1, use1_bucket, euw1, euw1_bucket
    for client, bucket in ((use1, use1_bucket), (euw1, euw1_bucket)):
        try:
            _purge(client, bucket)
        except ClientError:
            pass


def _mrap_name():
    return f"mrap-{_uuid_mod.uuid4().hex[:10]}"


def test_s3_mrap_routes_default_for_records_persisted_before_routes():
    from ministack.services import s3 as s3_svc

    legacy = {"Name": "old", "Alias": "a.mrap", "Regions": ["b1", "b2"], "CreatedAt": "2026-01-01T00:00:00Z"}
    assert s3_svc.mrap_routes(legacy) == {"b1": 100, "b2": 100}
    assert s3_svc.mrap_routes({**legacy, "Routes": {"b1": 0}}) == {"b1": 0, "b2": 100}


def _mrap_create(ctl, name, buckets):
    token = ctl.create_multi_region_access_point(
        AccountId=_ACCOUNT, ClientToken=_uuid_mod.uuid4().hex,
        Details={"Name": name, "Regions": [{"Bucket": b} for b in buckets]})["RequestTokenARN"]
    return ctl.describe_multi_region_access_point_operation(
        AccountId=_ACCOUNT, RequestTokenARN=token)["AsyncOperation"]


def _mrap_delete(ctl, name):
    token = ctl.delete_multi_region_access_point(
        AccountId=_ACCOUNT, ClientToken=_uuid_mod.uuid4().hex, Details={"Name": name})["RequestTokenARN"]
    return ctl.describe_multi_region_access_point_operation(
        AccountId=_ACCOUNT, RequestTokenARN=token)["AsyncOperation"]


def test_s3_mrap_control_async_create_describe_delete(s3control_dns, mrap_members):
    _, use1_bucket, _, euw1_bucket = mrap_members
    ctl = _regional_client("s3control", "us-west-2")
    name = _mrap_name()
    op = _mrap_create(ctl, name, [use1_bucket, euw1_bucket])
    assert re.fullmatch(rf"arn:aws:s3:us-west-2:{_ACCOUNT}:async-request/mrap/create/.+", op["RequestTokenARN"])
    assert op["Operation"] == "CreateMultiRegionAccessPoint"
    assert op["RequestStatus"] == "SUCCEEDED"
    assert op["RequestParameters"]["CreateMultiRegionAccessPointRequest"]["Name"] == name
    regions = op["ResponseDetails"]["MultiRegionAccessPointDetails"]["Regions"]
    assert sorted(r["Name"] for r in regions) == ["eu-west-1", "us-east-1"]

    again = _mrap_create(ctl, name, [use1_bucket, euw1_bucket])
    assert again["RequestStatus"] == "FAILED"
    assert again["ResponseDetails"]["ErrorDetails"]["Code"] == "MultiRegionAccessPointAlreadyOwnedByYou"
    assert again["ResponseDetails"]["ErrorDetails"]["Message"] == (
        "You must provide a unique name for each Multi-Region Access Point in your account. "
        "A MRAP with this name already exists.")

    deleted = _mrap_delete(ctl, name)
    assert (deleted["Operation"], deleted["RequestStatus"]) == ("DeleteMultiRegionAccessPoint", "SUCCEEDED")
    with pytest.raises(ClientError) as exc:
        ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)
    assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"


def test_s3_mrap_control_async_validation_failures(s3control_dns, mrap_members):
    use1, use1_bucket, _, euw1_bucket = mrap_members
    ctl = _regional_client("s3control", "us-west-2")
    name = _mrap_name()
    op = _mrap_create(ctl, name, [use1_bucket, f"missing-{use1_bucket}"])
    assert op["RequestStatus"] == "FAILED"
    err = op["ResponseDetails"]["ErrorDetails"]
    assert (err["Code"], err["Message"], err["Resource"]) == (
        "NoSuchBucket", "The specified bucket does not exist.", name)
    assert op["ResponseDetails"]["MultiRegionAccessPointDetails"]["Regions"] == []

    second_use1 = f"{use1_bucket}-2"
    use1.create_bucket(Bucket=second_use1)
    try:
        op = _mrap_create(ctl, name, [use1_bucket, second_use1])
        assert op["ResponseDetails"]["ErrorDetails"]["Code"] == "MultiRegionAccessPointSameBucketRegion"
        assert op["ResponseDetails"]["ErrorDetails"]["Message"] == (
            "The buckets specified must be part of different Regions.")
    finally:
        use1.delete_bucket(Bucket=second_use1)
    with pytest.raises(ClientError):
        ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)


def test_s3_mrap_control_read_get_list_policy(s3control_dns, mrap_members):
    use1, use1_bucket, _, euw1_bucket = mrap_members
    ctl = _regional_client("s3control", "us-west-2")
    names = [_mrap_name(), _mrap_name()]
    for name in names:
        assert _mrap_create(ctl, name, [use1_bucket, euw1_bucket])["RequestStatus"] == "SUCCEEDED"
    try:
        ap = ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=names[0])["AccessPoint"]
        assert ap["Name"] == names[0]
        assert re.fullmatch(r"[a-z][a-z0-9]{12}\.mrap", ap["Alias"])
        assert ap["Status"] == "READY"
        assert {(r["Bucket"], r["Region"], r["BucketAccountId"]) for r in ap["Regions"]} == {
            (use1_bucket, "us-east-1", _ACCOUNT), (euw1_bucket, "eu-west-1", _ACCOUNT)}

        listed, token = [], None
        while True:
            kwargs = {"AccountId": _ACCOUNT, "MaxResults": 1, **({"NextToken": token} if token else {})}
            page = ctl.list_multi_region_access_points(**kwargs)
            listed += [a["Name"] for a in page["AccessPoints"]]
            token = page.get("NextToken")
            if not token:
                break
        assert set(names) <= set(listed)

        policy = json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{_ACCOUNT}:root"},
            "Action": "s3:GetObject", "Resource": f"arn:aws:s3::{_ACCOUNT}:accesspoint/{ap['Alias']}/object/*"}]})
        token = ctl.put_multi_region_access_point_policy(
            AccountId=_ACCOUNT, ClientToken="p", Details={"Name": names[0], "Policy": policy})["RequestTokenARN"]
        op = ctl.describe_multi_region_access_point_operation(AccountId=_ACCOUNT, RequestTokenARN=token)
        assert op["AsyncOperation"]["RequestStatus"] == "SUCCEEDED"
        doc = ctl.get_multi_region_access_point_policy(AccountId=_ACCOUNT, Name=names[0])["Policy"]
        assert doc["Established"]["Policy"] == policy
        assert doc["Proposed"]["Policy"] == policy

        with pytest.raises(ClientError) as exc:
            use1.delete_bucket(Bucket=use1_bucket)
        assert exc.value.response["Error"]["Code"] == "BucketHasAccessPointsAttached"
        assert exc.value.response["Error"]["Message"] == "Bucket has access points attached and cannot be deleted."
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    finally:
        for name in names:
            _mrap_delete(ctl, name)

    with pytest.raises(ClientError) as exc:
        ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=names[0])
    err = exc.value.response["Error"]
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert err["Message"] == "The specified Multi-Region Access Point does not exist."
    assert (err["MultiRegionAccessPointName"], err["AccountId"]) == (names[0], _ACCOUNT)


def _mrap_control_raw(method, path, key=("test", "test")):
    """A SigV4-signed S3 Control request for us-west-2, the path sent as given: status and body."""
    import http.client

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(method=method, url=f"{ENDPOINT}{path}", headers={"x-amz-account-id": _ACCOUNT})
    req.headers["x-amz-content-sha256"] = hashlib.sha256(b"").hexdigest()
    S3SigV4Auth(Credentials(*key), "s3", "us-west-2").add_auth(req)
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, headers=dict(req.headers.items()))
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


_MRAP_ARN_PATH = f"arn%3Aaws%3As3%3A%3A{_ACCOUNT}%3Aaccesspoint/nope.mrap"


@pytest.mark.parametrize("method,path,status,fields", [
    ("POST", "", 405, {"Method": "POST", "ResourceType": "MULTI_REGION_ACCESS_POINTS"}),
    ("DELETE", "/m1", 405, {"Method": "DELETE", "ResourceType": "MULTI_REGION_ACCESS_POINT"}),
    ("POST", "/m1/policy", 405, {"Method": "POST", "ResourceType": "GET_MULTI_REGION_ACCESS_POINT_POLICY"}),
    ("POST", f"/{_MRAP_ARN_PATH}/routes", 405, {"Method": "POST", "ResourceType": "MULTI_REGION_ACCESS_POINT_ROUTES"}),
    ("GET", "/", 400, {"URI": "mrap/instances/"}),
    ("GET", "/m1/", 400, {}),
    ("GET", "//m1", 400, {}),
    ("GET", "/m1/policy/", 400, {}),
    ("GET", "/m1/x", 400, {"URI": "mrap/instances/m1/x"}),
    ("GET", "/m1/policy/x", 400, {}),
    ("GET", f"/{_MRAP_ARN_PATH}", 400, {}),
    ("GET", f"/{_MRAP_ARN_PATH}/routes/", 400, {}),
    ("GET", "/routes", 404, {"MultiRegionAccessPointName": "routes"}),
    ("GET", "/m1/policystatus", 404, {}),
])
def test_s3_mrap_control_unserved_requests(method, path, status, fields):
    """Unserved methods answer 405, unparsed paths 400 InvalidURI; "routes" alone names an MRAP."""
    got, data = _mrap_control_raw(method, "/v20180820/mrap/instances" + path)
    code = {405: "MethodNotAllowed", 400: "InvalidURI", 404: "NoSuchMultiRegionAccessPoint"}[status]
    assert (got, re.search(rb"<Code>(\w+)</Code>", data).group(1).decode()) == (status, code)
    for tag, value in fields.items():
        assert f"<{tag}>{value}</{tag}>".encode() in data


@pytest.mark.auth_contract
def test_s3_mrap_named_policy_needs_the_get_action(auth_enabled, iam_user, s3control_dns, mrap_members):
    """/v20180820/mrap/instances/policy names an MRAP called "policy"."""
    _, use1_bucket, _, euw1_bucket = mrap_members
    ctl = _regional_client("s3control", "us-west-2")
    assert _mrap_create(ctl, "policy", [use1_bucket, euw1_bucket])["RequestStatus"] == "SUCCEEDED"
    try:
        user = iam_user(policy={"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "s3:GetMultiRegionAccessPointPolicy", "Resource": "*"}]}).client(
            "s3control", region="us-west-2")
        assert "Policy" in user.get_multi_region_access_point_policy(AccountId=_ACCOUNT, Name="policy")
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                user.get_multi_region_access_point(AccountId=_ACCOUNT, Name="policy")
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
            assert exc.value.response["Error"]["Code"] == "AccessDenied"
        else:
            assert user.get_multi_region_access_point(AccountId=_ACCOUNT, Name="policy")["AccessPoint"]["Name"] == (
                "policy")
    finally:
        _mrap_delete(ctl, "policy")


def test_s3_mrap_control_read_routes(s3control_dns, mrap_members):
    _, use1_bucket, _, euw1_bucket = mrap_members
    ctl, routes_ctl = _regional_client("s3control", "us-west-2"), _regional_client("s3control", "eu-west-1")
    name = _mrap_name()
    _mrap_create(ctl, name, [use1_bucket, euw1_bucket])
    try:
        alias = ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)["AccessPoint"]["Alias"]
        arn = f"arn:aws:s3::{_ACCOUNT}:accesspoint/{alias}"
        routes = routes_ctl.get_multi_region_access_point_routes(AccountId=_ACCOUNT, Mrap=arn)
        assert routes["Mrap"] == arn
        assert {(r["Bucket"], r["Region"], r["TrafficDialPercentage"]) for r in routes["Routes"]} == {
            (use1_bucket, "us-east-1", 100), (euw1_bucket, "eu-west-1", 100)}

        def submit_error(updates, mrap=arn):
            with pytest.raises(ClientError) as exc:
                routes_ctl.submit_multi_region_access_point_routes(AccountId=_ACCOUNT, Mrap=mrap, RouteUpdates=updates)
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            return exc.value.response["Error"]

        assert submit_error([{"Bucket": use1_bucket, "TrafficDialPercentage": 50}]) == {
            "Code": "InvalidRequest", "Message": "Request invalid"}
        assert submit_error([{"Bucket": f"x-{use1_bucket}", "TrafficDialPercentage": 0}])["Message"] == (
            "The provided Region or bucket is not part of the Multi-Region Access Point.")
        assert submit_error([{"Bucket": use1_bucket, "TrafficDialPercentage": 0},
                             {"Bucket": euw1_bucket, "TrafficDialPercentage": 0}])["Message"] == (
            "At least one Region or bucket in the Multi-Region Access Point must be configured to receive "
            "traffic. Set at least one Region or bucket to Active, and then try again.")
        assert submit_error([{"Bucket": use1_bucket, "TrafficDialPercentage": 0}], mrap=name)["Message"] == (
            "Request invalid")

        routes_ctl.submit_multi_region_access_point_routes(
            AccountId=_ACCOUNT, Mrap=arn, RouteUpdates=[{"Bucket": use1_bucket, "TrafficDialPercentage": 0}])
        dials = {r["Bucket"]: r["TrafficDialPercentage"]
                 for r in routes_ctl.get_multi_region_access_point_routes(AccountId=_ACCOUNT, Mrap=arn)["Routes"]}
        assert dials == {use1_bucket: 0, euw1_bucket: 100}
    finally:
        _mrap_delete(ctl, name)


def test_s3_mrap_control_region_rules(s3control_dns, mrap_members):
    """Management operations are served only from us-west-2 and route
    operations from five regions; anything else is redirected (observed)."""
    _, use1_bucket, _, euw1_bucket = mrap_members
    with pytest.raises(ClientError) as exc:
        _mrap_create(_regional_client("s3control", "us-east-1"), _mrap_name(), [use1_bucket, euw1_bucket])
    err = exc.value.response["Error"]
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 301
    assert err["Code"] == "PermanentRedirect"
    assert err["Message"] == ("This API operation is only available in the following Regions: us-west-2. "
                              "Make sure to send all future requests to a supported Region.")
    assert err["Endpoint"] == "https://s3-control.us-west-2.amazonaws.com/v20180820/async-requests/mrap/create"

    ctl = _regional_client("s3control", "us-west-2")
    name = _mrap_name()
    _mrap_create(ctl, name, [use1_bucket, euw1_bucket])
    try:
        alias = ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)["AccessPoint"]["Alias"]
        arn = f"arn:aws:s3::{_ACCOUNT}:accesspoint/{alias}"
        with pytest.raises(ClientError) as exc:
            _regional_client("s3control", "eu-central-1").submit_multi_region_access_point_routes(
                AccountId=_ACCOUNT, Mrap=arn, RouteUpdates=[{"Bucket": use1_bucket, "TrafficDialPercentage": 0}])
        assert exc.value.response["Error"]["Message"] == (
            "This API operation is only available in the following Regions: us-west-2, ap-southeast-2, "
            "ap-northeast-1, eu-west-1 and us-east-1. Make sure to send all future requests to a supported Region.")
        for region in ("us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-2"):
            _regional_client("s3control", region).get_multi_region_access_point_routes(AccountId=_ACCOUNT, Mrap=arn)
    finally:
        _mrap_delete(ctl, name)


def test_s3_mrap_control_account_check_only_under_auth(monkeypatch):
    """x-amz-account-id must name the caller's account; that is an authorization check, so AUTH=true only."""
    import asyncio

    import ministack.app as app_mod
    from ministack.services import s3 as s3_svc

    headers = {"x-amz-account-id": "111122223333"}
    status, _, body = s3_svc.s3control_account_error(headers)
    assert status == 403 and b"<Code>AccessDenied</Code>" in body
    monkeypatch.setattr(app_mod, "AUTH", False)
    status, _, _ = asyncio.run(app_mod._handle_s3_control_request(
        "/v20180820/mrap/instances", "GET", b"", {}, "rid", headers))
    assert status == 200


def _mrap_request(alias, method, key, body=b""):
    import http.client

    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, f"/{key}", body=body or None,
                 headers={"Host": f"{alias}.accesspoint.s3-global.amazonaws.com"})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def test_s3_mrap_end_to_end_routing_failover_and_replication(s3control_dns, mrap_members):
    use1, use1_bucket, euw1, euw1_bucket = mrap_members
    _replicate(use1, use1_bucket, euw1_bucket, Filter={"Prefix": "data/"},
               DeleteMarkerReplication={"Status": "Disabled"})
    _replicate(euw1, euw1_bucket, use1_bucket, Filter={"Prefix": "data/"},
               DeleteMarkerReplication={"Status": "Disabled"})
    ctl, routes_ctl = _regional_client("s3control", "us-west-2"), _regional_client("s3control", "eu-west-1")
    name = _mrap_name()
    # Listed eu-west-1 first: the us-east-1 member wins on region, not position.
    _mrap_create(ctl, name, [euw1_bucket, use1_bucket])
    alias = ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)["AccessPoint"]["Alias"]
    arn = f"arn:aws:s3::{_ACCOUNT}:accesspoint/{alias}"
    try:
        for region in ("us-east-1", "eu-west-1", None):
            assert _mrap_get(alias, "who.txt", region=region) == (200, "use1")

        routes_ctl.submit_multi_region_access_point_routes(
            AccountId=_ACCOUNT, Mrap=arn, RouteUpdates=[{"Bucket": use1_bucket, "TrafficDialPercentage": 0}])
        assert _mrap_get(alias, "who.txt") == (200, "euw1")
        with pytest.raises(ClientError):
            routes_ctl.submit_multi_region_access_point_routes(
                AccountId=_ACCOUNT, Mrap=arn, RouteUpdates=[{"Bucket": euw1_bucket, "TrafficDialPercentage": 0}])
        assert _mrap_get(alias, "who.txt") == (200, "euw1")

        assert _mrap_request(alias, "PUT", "data/new.txt", b"through mrap")[0] == 200
        assert euw1.get_object(Bucket=euw1_bucket, Key="data/new.txt")["ReplicationStatus"] == "COMPLETED"
        assert use1.get_object(Bucket=use1_bucket, Key="data/new.txt")["Body"].read() == b"through mrap"
    finally:
        _mrap_delete(ctl, name)
    assert _mrap_request(alias, "GET", "who.txt")[0] in (403, 404)


def _mrap_signed_request(credentials, alias, method, target, body=b""):
    """A SigV4-signed request through the MRAP hostname: status and body."""
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    host = f"{alias}.accesspoint.s3-global.amazonaws.com"
    request = AWSRequest(method, f"{ENDPOINT.rstrip('/')}{target}", data=body,
                         headers={"host": host, "x-amz-content-sha256": hashlib.sha256(body).hexdigest()})
    S3SigV4Auth(Credentials(*credentials), "s3", "us-east-1").add_auth(request)
    response = requests.request(method, request.url, data=body, headers=dict(request.headers.items()), timeout=30)
    return response.status_code, response.content


@pytest.mark.auth_contract
@pytest.mark.parametrize("grant,denied_on", [
    ("none", "bucket"), ("mrap", "bucket"), ("bucket", "mrap"), ("both", None), ("mrap-policy", None),
])
def test_s3_mrap_requests_are_authorized_on_the_bucket_and_the_mrap(
        auth_enabled, iam_user, s3control_dns, mrap_members, grant, denied_on):
    """Through an MRAP, S3 checks the member bucket first and then the MRAP; an MRAP policy can allow the latter."""
    _, use1_bucket, _, _ = mrap_members
    ctl = _regional_client("s3control", "us-west-2")
    name = _mrap_name()
    _mrap_create(ctl, name, [use1_bucket])
    alias = ctl.get_multi_region_access_point(AccountId=_ACCOUNT, Name=name)["AccessPoint"]["Alias"]
    mrap_arn = f"arn:aws:s3::{_ACCOUNT}:accesspoint/{alias}"
    bucket_arn = f"arn:aws:s3:::{use1_bucket}"

    def allow(prefix):
        return [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": f"{prefix}/*"},
                {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": prefix}]

    identity = {"none": [], "mrap": allow(mrap_arn), "bucket": allow(bucket_arn), "both": allow(mrap_arn)
                + allow(bucket_arn), "mrap-policy": allow(bucket_arn)}[grant]
    try:
        user = iam_user(policy={"Version": "2012-10-17", "Statement": identity} if identity else None)
        if grant == "mrap-policy":
            statements = [{**s, "Principal": {"AWS": user.arn}} for s in allow(mrap_arn)]
            ctl.put_multi_region_access_point_policy(
                AccountId=_ACCOUNT, ClientToken=_uuid_mod.uuid4().hex,
                Details={"Name": name, "Policy": json.dumps({"Version": "2012-10-17", "Statement": statements})})
        with second_access_key(user) as credentials:
            for action, method, target, body, resource in [
                ("GetObject", "GET", "/who.txt", b"", "/who.txt"),
                ("GetObject", "HEAD", "/who.txt", b"", "/who.txt"),
                ("PutObject", "PUT", "/mrap-put.txt", b"x", "/mrap-put.txt"),
                ("ListBucket", "GET", "/?list-type=2", b"", ""),
            ]:
                status, data = _mrap_signed_request(credentials, alias, method, target, body)
                if not auth_enabled or denied_on is None:
                    assert status == 200, data
                    continue
                arn = (bucket_arn if denied_on == "bucket" else mrap_arn + ("/object" if resource else "")) + resource
                assert status == 403
                if method == "HEAD":
                    assert data == b""
                    continue
                assert ElementTree.fromstring(data).findtext("Code") == "AccessDenied"
                assert ElementTree.fromstring(data).findtext("Message") == (
                    f'User: {user.arn} is not authorized to perform: s3:{action} on resource: "{arn}" '
                    f"because no identity-based policy allows the s3:{action} action")
    finally:
        _mrap_delete(ctl, name)


# IAM contracts, run by CI with AUTH=false and AUTH=true.
@pytest.fixture
def auth_s3_objects(s3):
    """Two private buckets with one source object; isolate and clean every test."""
    with contextlib.ExitStack() as cleanup:
        source = f"auth-source-{uuid.uuid4().hex}"
        destination = f"auth-dest-{uuid.uuid4().hex}"
        for bucket in (source, destination):
            s3.create_bucket(Bucket=bucket)
            cleanup.callback(s3.delete_bucket, Bucket=bucket)
        s3.put_object(Bucket=source, Key="source", Body=b"auth-contract")
        cleanup.callback(s3.delete_object, Bucket=source, Key="source")
        # Register before the test so even an unexpected successful write is removed.
        cleanup.callback(s3.delete_object, Bucket=destination, Key="copied")
        yield source, destination


@pytest.mark.auth_contract
@pytest.mark.parametrize("permission", ["none", "allow", "explicit-deny", "other-resource"])
def test_s3_read_permission_contract(auth_enabled, iam_user, auth_s3_objects, permission):
    source, _ = auth_s3_objects
    statements = []
    if permission != "none":
        resource = f"arn:aws:s3:::{source}/" + ("other" if permission == "other-resource" else "source")
        statements.append({"Effect": "Allow", "Action": "s3:GetObject", "Resource": resource})
    if permission == "explicit-deny":
        statements.append({"Effect": "Deny", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{source}/source"})
    policy = {"Version": "2012-10-17", "Statement": statements} if statements else None
    client = iam_user(policy=policy).client("s3")
    if auth_enabled and permission != "allow":
        with pytest.raises(ClientError) as exc:
            client.get_object(Bucket=source, Key="source")
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    else:
        response = client.get_object(Bucket=source, Key="source")
        with response["Body"] as body:
            assert body.read() == b"auth-contract"


@pytest.fixture
def auth_condition_bucket(s3):
    """A bucket whose policy denies a user an object action under a condition; emptied and removed after."""
    bucket = f"auth-cond-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=bucket)

    def deny(user_arn, action, condition):
        s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
            "Effect": "Deny", "Principal": {"AWS": user_arn}, "Action": action,
            "Resource": f"arn:aws:s3:::{bucket}/*", "Condition": condition}]}))
        return bucket

    yield deny
    for upload in s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
        s3.abort_multipart_upload(Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])
    for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=item["Key"])
    s3.delete_bucket(Bucket=bucket)


_ALLOW_ALL_S3 = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}


def _assert_s3_condition(auth_enabled, allowed, call):
    """Under AUTH a refused call answers 403 AccessDenied; anything else succeeds."""
    if auth_enabled and not allowed:
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc.value.response["Error"]["Code"] in ("AccessDenied", "403")
    else:
        assert call()["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


@pytest.mark.auth_contract
@pytest.mark.parametrize("condition", [
    {"StringNotEquals": {"s3:x-amz-server-side-encryption": "AES256"}},
    {"Null": {"s3:x-amz-server-side-encryption": "true"}},
], ids=["string-not-equals", "null"])
def test_s3_encryption_condition_contract(auth_enabled, iam_user, s3, auth_condition_bucket, condition):
    """A bucket policy on s3:x-amz-server-side-encryption sees the header of a PutObject and a CopyObject."""
    user = iam_user(policy=_ALLOW_ALL_S3)
    bucket = auth_condition_bucket(user.arn, "s3:PutObject", condition)
    s3.put_object(Bucket=bucket, Key="source", Body=b"x")
    client = user.client("s3")
    for sse in (None, "AES256"):
        extra = {"ServerSideEncryption": sse} if sse else {}
        _assert_s3_condition(auth_enabled, sse is not None,
                             lambda: client.put_object(Bucket=bucket, Key=f"put-{sse}", Body=b"x", **extra))
        _assert_s3_condition(auth_enabled, sse is not None, lambda: client.copy_object(
            Bucket=bucket, Key=f"copy-{sse}", CopySource={"Bucket": bucket, "Key": "source"}, **extra))


@pytest.mark.auth_contract
def test_s3_multipart_encryption_condition_contract(auth_enabled, iam_user, s3, auth_condition_bucket):
    """A part and the completion carry the encryption header their CreateMultipartUpload sent."""
    user = iam_user(policy=_ALLOW_ALL_S3)
    bucket = auth_condition_bucket(user.arn, "s3:PutObject", {"Null": {"s3:x-amz-server-side-encryption": "true"}})
    client = user.client("s3")
    upload_id = client.create_multipart_upload(Bucket=bucket, Key="sealed", ServerSideEncryption="AES256")["UploadId"]
    etag = client.upload_part(Bucket=bucket, Key="sealed", UploadId=upload_id, PartNumber=1, Body=b"x")["ETag"]
    client.complete_multipart_upload(Bucket=bucket, Key="sealed", UploadId=upload_id,
                                     MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]})
    plain = s3.create_multipart_upload(Bucket=bucket, Key="plain")["UploadId"]
    _assert_s3_condition(auth_enabled, False, lambda: client.upload_part(
        Bucket=bucket, Key="plain", UploadId=plain, PartNumber=1, Body=b"x"))


@pytest.mark.auth_contract
def test_s3_canned_acl_condition_contract(auth_enabled, iam_user, auth_condition_bucket):
    """A bucket policy on s3:x-amz-acl sees the canned ACL of a PutObject."""
    user = iam_user(policy=_ALLOW_ALL_S3)
    bucket = auth_condition_bucket(user.arn, "s3:PutObject",
                                   {"StringNotEquals": {"s3:x-amz-acl": "bucket-owner-full-control"}})
    client = user.client("s3")
    for acl in (None, "bucket-owner-full-control", "private"):
        extra = {"ACL": acl} if acl else {}
        _assert_s3_condition(auth_enabled, acl == "bucket-owner-full-control",
                             lambda: client.put_object(Bucket=bucket, Key=f"acl-{acl}", Body=b"x", **extra))


@pytest.mark.auth_contract
def test_s3_grant_header_condition_contract(auth_enabled, iam_user, s3, auth_condition_bucket):
    """A bucket policy on s3:x-amz-grant-read sees the grant header of a PutObject."""
    user = iam_user(policy=_ALLOW_ALL_S3)
    bucket = auth_condition_bucket(user.arn, "s3:PutObject", {"Null": {"s3:x-amz-grant-read": "false"}})
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "ObjectWriter"}]})
    client = user.client("s3")
    _assert_s3_condition(auth_enabled, True, lambda: client.put_object(Bucket=bucket, Key="plain", Body=b"x"))
    grant = 'id="c4c1ede66af53448b93c283ce9448c4ba468c9432aa01d700d3878632f77d2d0"'
    _assert_s3_condition(auth_enabled, False,
                         lambda: client.put_object(Bucket=bucket, Key="granted", Body=b"x", GrantRead=grant))


@pytest.mark.auth_contract
def test_s3_existing_object_tag_condition_contract(auth_enabled, iam_user, s3, auth_condition_bucket):
    """A bucket policy on s3:ExistingObjectTag sees the tags of the object a GetObject or HeadObject reads."""
    user = iam_user(policy=_ALLOW_ALL_S3)
    bucket = auth_condition_bucket(user.arn, "s3:GetObject", {"StringEquals": {"s3:ExistingObjectTag/class": "secret"}})
    for name, tagging in (("secret", "class=secret"), ("public", "class=public"), ("untagged", "")):
        s3.put_object(Bucket=bucket, Key=name, Body=b"x", **({"Tagging": tagging} if tagging else {}))
    client = user.client("s3")
    _assert_s3_condition(auth_enabled, False, lambda: client.get_object(Bucket=bucket, Key="secret"))
    _assert_s3_condition(auth_enabled, False, lambda: client.head_object(Bucket=bucket, Key="secret"))
    _assert_s3_condition(auth_enabled, True, lambda: client.get_object(Bucket=bucket, Key="public"))
    _assert_s3_condition(auth_enabled, True, lambda: client.get_object(Bucket=bucket, Key="untagged"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("addressing_style", ["path", "virtual"])
@pytest.mark.parametrize("allow_source_read", [False, True], ids=["no-source-read", "source-read"])
def test_s3_copy_permission_contract(
    auth_enabled, iam_user, s3, auth_s3_objects, addressing_style, allow_source_read,
):
    source, destination = auth_s3_objects
    statements = [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": f"arn:aws:s3:::{destination}/*"}]
    if allow_source_read:
        statements.append({"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{source}/*"})
    client = iam_user(policy={"Version": "2012-10-17", "Statement": statements}).client(
        "s3", config=Config(signature_version="s3v4", s3={"addressing_style": addressing_style}),
    )
    request = {"Bucket": destination, "Key": "copied", "CopySource": {"Bucket": source, "Key": "source"}}
    with patch_endpoint_dns():
        if auth_enabled and not allow_source_read:
            with pytest.raises(ClientError) as exc:
                client.copy_object(**request)
            assert exc.value.response["Error"]["Code"] == "AccessDenied"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
            # A rejected request must not leave the destination object behind.
            assert s3.list_objects_v2(Bucket=destination).get("Contents", []) == []
        else:
            response = client.copy_object(**request)
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
            result = s3.get_object(Bucket=destination, Key="copied")
            with result["Body"] as body:
                assert body.read() == b"auth-contract"


@pytest.fixture
def auth_s3_post_bucket(s3):
    """A versioned bucket supporting lock fields, with complete version cleanup."""
    bucket = f"auth-post-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=bucket, ObjectLockEnabledForBucket=True)
    try:
        yield bucket
    finally:
        versions = s3.list_object_versions(Bucket=bucket)
        for version in versions.get("Versions", []):
            s3.put_object_legal_hold(Bucket=bucket, Key=version["Key"], LegalHold={"Status": "OFF"})
            s3.delete_object(Bucket=bucket, Key=version["Key"], VersionId=version["VersionId"])
        for marker in versions.get("DeleteMarkers", []):
            s3.delete_object(Bucket=bucket, Key=marker["Key"], VersionId=marker["VersionId"])
        s3.delete_bucket(Bucket=bucket)


@pytest.mark.auth_contract
@pytest.mark.parametrize("permission,field,value,extra_action", [
    ("none", None, None, None),
    ("allow", None, None, None),
    ("explicit-deny", None, None, None),
    ("other-resource", None, None, None),
] + [
    (permission, field, value, action)
    for permission in ("missing-extra", "allow")
    for field, value, action in (
        ("acl", "private", "s3:PutObjectAcl"),
        ("x-amz-tagging", "purpose=test", "s3:PutObjectTagging"),
        ("x-amz-object-lock-legal-hold", "ON", "s3:PutObjectLegalHold"),
        ("x-amz-object-lock-mode", "GOVERNANCE", "s3:PutObjectRetention"),
    )
])
def test_s3_post_permission_contract(
    auth_enabled, iam_user, s3, auth_s3_post_bucket, permission, field, value, extra_action,
):
    """Multipart credentials identify the signer, including optional write actions."""
    from xml.etree import ElementTree

    import requests

    destination = auth_s3_post_bucket
    resource = f"arn:aws:s3:::{destination}/copied"
    statements = []
    if permission != "none":
        actions = ["s3:PutObject"]
        if extra_action and permission == "allow":
            actions.append(extra_action)
        statements.append({"Effect": "Allow", "Action": actions,
                           "Resource": resource + "-other" if permission == "other-resource" else resource})
    if permission == "explicit-deny":
        statements.append({"Effect": "Deny", "Action": "s3:PutObject", "Resource": resource})
    policy = {"Version": "2012-10-17", "Statement": statements} if statements else None
    client = iam_user(policy=policy).client(
        "s3", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    fields = {field: value} if field else {}
    form = client.generate_presigned_post(
        Bucket=destination, Key="copied", Fields=fields,
        Conditions=[{field: value}] if field else [],
    )
    response = requests.post(form["url"], data=form["fields"],
                             files={"file": ("contract.txt", b"post-auth-contract")}, timeout=10)
    if auth_enabled and permission != "allow":
        assert response.status_code == 403, response.text
        assert ElementTree.fromstring(response.content).findtext("Code") == "AccessDenied"
        assert s3.list_objects_v2(Bucket=destination).get("Contents", []) == []
    else:
        assert response.status_code == 204, response.text
        with s3.get_object(Bucket=destination, Key="copied")["Body"] as body:
            assert body.read() == b"post-auth-contract"


@pytest.fixture
def post_form(monkeypatch):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", False)
    monkeypatch.setattr(_s3_mod, "_buckets", AccountScopedDict())
    monkeypatch.setattr(_s3_mod, "S3_PERSIST", False)
    with request_scope("000000000000", "us-east-1"):
        bucket = "post-policy-" + uuid.uuid4().hex[:12]
        key, _arn = inprocess_iam_user({"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        _s3_mod._create_bucket(bucket, b"", {})

        class Form:
            def generate(self, *, fields=None, conditions=None, key_name="uploads/data.txt", expires=300,
                         secret=None, token=None, access_key=None, signature_version="s3v4"):
                client = boto3.client(
                    "s3", endpoint_url="http://localhost", region_name="us-east-1",
                    aws_access_key_id=access_key or key[0], aws_secret_access_key=secret or key[1],
                    aws_session_token=token, config=Config(signature_version=signature_version),
                )
                return client.generate_presigned_post(
                    Bucket=bucket, Key=key_name, Fields=fields or {},
                    Conditions=conditions or [], ExpiresIn=expires,
                )["fields"]

            def submit(self, fields, *, data=b"contents", filename="data.txt", routed=False):
                req = requests.Request(
                    "POST", "http://localhost/" + bucket,
                    data=fields, files={"file": (filename, data)},
                ).prepare()
                headers = {name.lower(): value for name, value in req.headers.items()}
                if routed:
                    return routed_request(None, "s3", "POST", "/" + bucket,
                                          body=req.body, headers=headers)
                return asyncio.run(_s3_mod.handle_request("POST", "/" + bucket, headers, req.body, {}))

            def assert_rejected(self, fields, code, **kwargs):
                response = self.submit(fields, **kwargs)
                assert response[0] in (400, 403)
                assert ElementTree.fromstring(response[2]).findtext("Code") == code
                assert _s3_mod._buckets[bucket]["objects"] == {}

        form = Form()
        form.bucket, form.key = bucket, key
        yield form
        _iam_mod._access_keys.pop(key[0], None)
        name = _arn.rsplit("/", 1)[-1]
        _iam_mod._users.pop(name, None)
        _iam_mod._user_inline_policies.pop(name, None)


@pytest.mark.parametrize("signature_version", ["s3v4", "s3"])
def test_signed_post_stores_bytes_and_metadata(post_form, signature_version):
    fields = post_form.generate(fields={"x-amz-meta-author": "alice", "success_action_status": "201"},
                                conditions=[{"x-amz-meta-author": "alice"}, {"success_action_status": "201"}],
                                signature_version=signature_version)
    assert post_form.submit(fields)[0] == 201
    obj = _s3_mod._buckets[post_form.bucket]["objects"]["uploads/data.txt"]
    assert obj["body"] == b"contents"
    assert obj["metadata"] == {"x-amz-meta-author": "alice"}


@pytest.mark.parametrize("name,value", [("key", "uploads/other.txt"), ("Content-Type", "text/plain"),
                                        ("x-amz-meta-author", "bob"), ("success_action_status", "204")])
def test_post_exact_conditions_reject_mismatched_fields(post_form, name, value):
    fields = {"Content-Type": "application/json", "x-amz-meta-author": "alice", "success_action_status": "201"}
    form = post_form.generate(fields=fields, conditions=[{k: v} for k, v in fields.items()])
    form[name] = value
    post_form.assert_rejected(form, "AccessDenied")


@pytest.fixture
def post_auth(monkeypatch):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)


@pytest.mark.parametrize("signature_version", ["s3v4", "s3"])
@pytest.mark.parametrize("auth_enabled", [False, True])
def test_post_signature_verified_under_auth_only(post_form, monkeypatch, auth_enabled, signature_version):
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    form = post_form.generate(secret=uuid.uuid4().hex, signature_version=signature_version)
    if auth_enabled:
        post_form.assert_rejected(form, "SignatureDoesNotMatch", routed=True)
    else:
        assert post_form.submit(form, routed=True)[0] == 204


@pytest.mark.parametrize("missing", ["x-amz-signature", "x-amz-date", "x-amz-credential", "policy"])
def test_incomplete_sigv4_form_rejected(post_form, missing):
    form = post_form.generate()
    del form[missing]
    post_form.assert_rejected(form, "InvalidArgument")


def test_expired_post_policy_rejected(post_form):
    post_form.assert_rejected(post_form.generate(expires=-60), "AccessDenied")


@pytest.mark.parametrize("content_type,accepted", [("image/png,image/jpeg", True), ("image/png,text/plain", False)])
def test_post_content_type_prefix_applies_to_each_list_item(post_form, content_type, accepted):
    form = post_form.generate(fields={"Content-Type": content_type},
                              conditions=[["starts-with", "$Content-Type", "image/"]])
    if accepted:
        assert post_form.submit(form)[0] == 204
    else:
        post_form.assert_rejected(form, "AccessDenied")


def test_post_variables_expanded_before_matching(post_form):
    form = post_form.generate(key_name="uploads/${filename}", fields={"x-amz-meta-name": "${filename}"},
                              conditions=[["eq", "$x-amz-meta-name", "data.txt"]])
    assert post_form.submit(form)[0] == 204
    obj = _s3_mod._buckets[post_form.bucket]["objects"]["uploads/data.txt"]
    assert obj["metadata"]["x-amz-meta-name"] == "data.txt"


def test_post_variables_expanded_once(post_form):
    form = post_form.generate(key_name="uploads/${filename}")
    assert post_form.submit(form, filename="${filename}.txt")[0] == 204
    assert list(_s3_mod._buckets[post_form.bucket]["objects"]) == ["uploads/${filename}.txt"]


@pytest.mark.parametrize("extra", ["x-amz-meta-extra", "acl"])
def test_post_uncovered_form_fields_rejected(post_form, extra):
    form = post_form.generate(fields={extra: "private"})
    post_form.assert_rejected(form, "AccessDenied")


def test_post_missing_condition_field_rejected(post_form):
    form = post_form.generate(conditions=[["starts-with", "$x-amz-meta-required", ""]])
    post_form.assert_rejected(form, "AccessDenied")


def test_post_ignore_fields_exempt_from_conditions(post_form):
    assert post_form.submit(post_form.generate(fields={"x-ignore-client": "local"}))[0] == 204


@pytest.mark.parametrize("data,code", [(b"a", "EntityTooSmall"), (b"1234", "EntityTooLarge"), (b"12", None), (b"123", None)])
def test_post_size_range_inclusive(post_form, data, code):
    form = post_form.generate(conditions=[["content-length-range", 2, 3]])
    if code:
        post_form.assert_rejected(form, code, data=data)
    else:
        assert post_form.submit(form, data=data)[0] == 204


@pytest.mark.parametrize("low,code", [("2", None), ("x", "AccessDenied")])
def test_post_size_range_string_bounds(post_form, low, code):
    form = post_form.generate(conditions=[["content-length-range", low, "3"]])
    if code:
        post_form.assert_rejected(form, code, data=b"12")
    else:
        assert post_form.submit(form, data=b"12")[0] == 204


def test_post_modified_policy_fails_signature(post_form, post_auth):
    form = post_form.generate()
    form["policy"] = form["policy"][:-2]
    post_form.assert_rejected(form, "SignatureDoesNotMatch")


def test_post_invalid_condition_rejected(post_form):
    post_form.assert_rejected(post_form.generate(conditions=[["unknown", "$key", ""]]), "InvalidPolicyDocument")


def test_post_policy_refusals_use_aws_messages(post_form):
    def message(form):
        return ElementTree.fromstring(post_form.submit(form)[2]).findtext("Message")
    form = post_form.generate(fields={"x-amz-meta-author": "alice"}, conditions=[{"x-amz-meta-author": "alice"}])
    form["x-amz-meta-author"] = "bob"
    assert message(form) == 'Invalid according to Policy: Policy Condition failed: ["eq", "$x-amz-meta-author", "alice"]'
    assert message(post_form.generate(expires=-60)) == "Invalid according to Policy: Policy expired."
    assert message(post_form.generate(fields={"x-amz-meta-extra": "x"})) == (
        "Invalid according to Policy: Extra input fields: x-amz-meta-extra")


def test_post_bucket_condition_uses_destination(post_form):
    post_form.assert_rejected(post_form.generate(conditions=[{"bucket": "different-bucket"}]), "AccessDenied")


def test_post_inactive_registered_key_rejected(post_form, post_auth):
    form = post_form.generate()
    _iam_mod._access_keys[post_form.key[0]]["Status"] = "Inactive"
    post_form.assert_rejected(form, "InvalidAccessKeyId")


def test_post_unexpected_session_token_rejected(post_form, post_auth):
    post_form.assert_rejected(post_form.generate(token=uuid.uuid4().hex), "InvalidToken")


def test_post_registered_key_resolves_nondefault_account(post_form):
    with request_scope("123456789012", "us-east-1"):
        key, arn = inprocess_iam_user()
        try:
            bucket = "tenant-post-" + uuid.uuid4().hex[:12]
            _s3_mod._create_bucket(bucket, b"", {})
            client = boto3.client("s3", endpoint_url="http://localhost", region_name="us-east-1",
                                  aws_access_key_id=key[0], aws_secret_access_key=key[1],
                                  config=Config(signature_version="s3v4"))
            form = client.generate_presigned_post(Bucket=bucket, Key="uploads/data.txt")["fields"]
            # submit() captures the fixture bucket, so route this request explicitly.
            req = requests.Request("POST", "http://localhost/" + bucket, data=form,
                                   files={"file": ("data.txt", b"tenant")}).prepare()
            result = asyncio.run(_s3_mod.handle_request("POST", "/" + bucket,
                {k.lower(): v for k, v in req.headers.items()}, req.body, {}))
            assert result[0] == 204
            assert _s3_mod._buckets[bucket]["objects"]["uploads/data.txt"]["body"] == b"tenant"
        finally:
            _iam_mod._access_keys.pop(key[0], None)
            _iam_mod._users.pop(arn.rsplit("/", 1)[-1], None)


@pytest.mark.parametrize("signature", ["", "not-hex", "é" * 64])
def test_post_malformed_signature_is_a_client_error(post_form, post_auth, signature):
    form = post_form.generate()
    form["x-amz-signature"] = signature
    post_form.assert_rejected(form, "InvalidArgument" if not signature else "SignatureDoesNotMatch")


@pytest.mark.parametrize("token_state", ["valid", "missing", "incorrect", "expired"])
def test_post_uses_sts_issued_credentials(post_form, post_auth, token_state):
    response = routed_request(post_form.key, "sts", "POST", "/",
        body=b"Action=GetSessionToken&DurationSeconds=900",
        headers={"content-type": "application/x-www-form-urlencoded"})
    assert response[0] == 200
    root = ElementTree.fromstring(response[2])
    credentials = {name: root.findtext(".//{*}" + name) for name in ("AccessKeyId", "SecretAccessKey", "SessionToken")}
    access_key = credentials["AccessKeyId"]
    try:
        token = credentials["SessionToken"]
        if token_state == "missing":
            token = None
        elif token_state == "incorrect":
            token = uuid.uuid4().hex
        elif token_state == "expired":
            _sts_mod._sessions[access_key]["Expiration"] = time.time() - 60
        form = post_form.generate(access_key=access_key, secret=credentials["SecretAccessKey"], token=token)
        if token_state == "valid":
            assert post_form.submit(form)[0] == 204
        else:
            post_form.assert_rejected(form, "ExpiredToken" if token_state == "expired" else "InvalidToken")
    finally:
        _sts_mod._sessions.pop(access_key, None)


def test_post_configured_root_credentials(post_form, monkeypatch):
    key, secret = "local-root-" + uuid.uuid4().hex, uuid.uuid4().hex
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    assert post_form.submit(post_form.generate(access_key=key, secret=secret))[0] == 204


def test_rejected_post_preserves_existing_object_and_disk(post_form, post_auth, monkeypatch, tmp_path):
    monkeypatch.setattr(_s3_mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(_s3_mod, "S3_PERSIST", True)
    events = []
    monkeypatch.setattr(_s3_mod, "_fire_s3_event_async", lambda *a, **kw: events.append(kw))
    assert post_form.submit(post_form.generate(), data=b"original")[0] == 204
    files = {str(path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    assert files
    invalid = post_form.generate(secret=uuid.uuid4().hex)
    assert post_form.submit(invalid, data=b"replacement")[0] == 403
    assert {str(path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == files
    assert len(events) == 1
    obj = _s3_mod._buckets[post_form.bucket]["objects"]["uploads/data.txt"]
    assert _s3_mod._read_body(post_form.bucket, "uploads/data.txt", obj) == b"original"
    _s3_mod._buckets.clear()
    _s3_mod._load_persisted_data()
    obj = _s3_mod._buckets[post_form.bucket]["objects"]["uploads/data.txt"]
    assert _s3_mod._read_body(post_form.bucket, "uploads/data.txt", obj) == b"original"


def test_post_after_persisted_reload(post_form, monkeypatch, tmp_path):
    monkeypatch.setattr(_s3_mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(_s3_mod, "S3_PERSIST", True)
    assert post_form.submit(post_form.generate(), data=b"first")[0] == 204
    _s3_mod._buckets.clear()
    _s3_mod._load_persisted_data()
    assert post_form.submit(post_form.generate(), data=b"second")[0] == 204


def test_s3_object_lock_headers_need_an_object_lock_bucket(s3):
    from datetime import datetime, timedelta, timezone

    bkt = f"intg-s3-nolock-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    try:
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket=bkt, Key="k", Body=b"x", ObjectLockMode="COMPLIANCE",
                          ObjectLockRetainUntilDate=datetime.now(timezone.utc) + timedelta(days=1))
        err = exc.value.response
        assert (err["Error"]["Code"], err["Error"]["Message"], err["ResponseMetadata"]["HTTPStatusCode"]) == (
            "InvalidRequest", "Bucket is missing ObjectLockConfiguration", 400)
        assert s3.list_objects_v2(Bucket=bkt).get("KeyCount") == 0
    finally:
        s3.delete_bucket(Bucket=bkt)


def test_s3_object_lock_applies_to_its_version_only(s3):
    from datetime import datetime, timedelta, timezone

    bkt = f"intg-s3-lock-ver-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt, ObjectLockEnabledForBucket=True)
    locked = s3.put_object(Bucket=bkt, Key="k", Body=b"v1", ObjectLockMode="COMPLIANCE",
                           ObjectLockRetainUntilDate=datetime.now(timezone.utc) + timedelta(days=1))["VersionId"]
    free = s3.put_object(Bucket=bkt, Key="k", Body=b"v2")["VersionId"]
    assert "ObjectLockMode" not in s3.head_object(Bucket=bkt, Key="k")
    assert s3.head_object(Bucket=bkt, Key="k", VersionId=locked)["ObjectLockMode"] == "COMPLIANCE"
    s3.delete_object(Bucket=bkt, Key="k", VersionId=free)
    with pytest.raises(ClientError) as exc:
        s3.delete_object(Bucket=bkt, Key="k", VersionId=locked)
    assert exc.value.response["Error"]["Code"] == "AccessDenied"


def test_s3_copy_and_multipart_refuse_object_lock_headers_without_object_lock(s3):
    from datetime import datetime, timedelta, timezone

    bkt = f"intg-s3-nolock-cp-{_uuid_mod.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bkt)
    s3.put_bucket_versioning(Bucket=bkt, VersioningConfiguration={"Status": "Enabled"})
    s3.put_object(Bucket=bkt, Key="src", Body=b"x")
    until = datetime.now(timezone.utc) + timedelta(days=1)
    calls = (
        lambda: s3.copy_object(Bucket=bkt, Key="dst", CopySource={"Bucket": bkt, "Key": "src"},
                               ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=until),
        lambda: s3.copy_object(Bucket=bkt, Key="dst", CopySource={"Bucket": bkt, "Key": "src"},
                               ObjectLockLegalHoldStatus="OFF"),
        lambda: s3.create_multipart_upload(Bucket=bkt, Key="mpu", ObjectLockMode="COMPLIANCE",
                                           ObjectLockRetainUntilDate=until),
        lambda: s3.create_multipart_upload(Bucket=bkt, Key="mpu", ObjectLockLegalHoldStatus="ON"),
    )
    try:
        for call in calls:
            with pytest.raises(ClientError) as exc:
                call()
            err = exc.value.response
            assert (err["Error"]["Code"], err["Error"]["Message"], err["ResponseMetadata"]["HTTPStatusCode"]) == (
                "InvalidRequest", "Bucket is missing ObjectLockConfiguration", 400)
        assert [o["Key"] for o in s3.list_objects_v2(Bucket=bkt)["Contents"]] == ["src"]
        assert "Uploads" not in s3.list_multipart_uploads(Bucket=bkt)
    finally:
        for v in s3.list_object_versions(Bucket=bkt).get("Versions", []):
            s3.delete_object(Bucket=bkt, Key=v["Key"], VersionId=v["VersionId"])
        s3.delete_bucket(Bucket=bkt)



def test_s3_bucket_config_enum_and_day_validation(s3):
    bucket = f"cfgval-{uuid.uuid4().hex[:10]}"
    s3.create_bucket(Bucket=bucket)
    malformed = "The XML you provided was not well-formed or did not validate against our published schema"

    def err(fn):
        with pytest.raises(ClientError) as exc:
            fn()
        return exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]

    for status in ("Disabled", "banana", "enabled"):
        assert err(lambda: s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": status})) == (
            "MalformedXML", malformed)
    assert err(lambda: s3.put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled", "MFADelete": "banana"}))[0] == "MalformedXML"
    assert "Status" not in s3.get_bucket_versioning(Bucket=bucket)

    def rule(**kw):
        return {"Rules": [{"ID": "r", "Status": "Enabled", "Filter": {"Prefix": ""}, **kw}]}

    for days in (0, -1):
        assert err(lambda: s3.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=rule(Expiration={"Days": days}))) == (
            "InvalidArgument", "'Days' for Expiration action must be a positive integer")
    assert err(lambda: s3.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=rule(NoncurrentVersionExpiration={"NoncurrentDays": 0}))) == (
        "InvalidArgument", "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer")
    assert err(lambda: s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
        {"ID": "r", "Status": "banana", "Filter": {"Prefix": ""}, "Expiration": {"Days": 1}}]}))[0] == "MalformedXML"
    assert err(lambda: s3.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=rule(Transitions=[{"Days": 30, "StorageClass": "banana"}])))[0] == "MalformedXML"
    assert err(lambda: s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={"Rules": [
        {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "banana"}}]}))[0] == "MalformedXML"

    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=rule(
        Expiration={"Days": 1}, Transitions=[{"Days": 30, "StorageClass": "GLACIER_IR"}]))
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={"Rules": [
        {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms:dsse"}}]})
    s3.delete_bucket(Bucket=bucket)


@pytest.mark.auth_contract
def test_s3_anonymous_copy_source_and_policy_action_contract(auth_enabled, s3):
    """A public PutObject policy lets anonymous callers write, not read another bucket through a copy or delete."""
    secret, upload = f"anon-src-{uuid.uuid4().hex[:8]}", f"anon-dst-{uuid.uuid4().hex[:8]}"
    for bucket in (secret, upload):
        s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=secret, Key="c", Body=b"secret")
    s3.put_object(Bucket=upload, Key="kept", Body=b"kept")
    s3.delete_public_access_block(Bucket=upload)
    s3.put_bucket_policy(Bucket=upload, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": "*", "Action": "s3:PutObject", "Resource": f"arn:aws:s3:::{upload}/*"}]}))
    try:
        assert requests.put(f"{ENDPOINT}/{upload}/plain", data=b"x").status_code == 200
        copy = requests.put(f"{ENDPOINT}/{upload}/loot", headers={"x-amz-copy-source": f"/{secret}/c"})
        delete = requests.delete(f"{ENDPOINT}/{upload}/kept")
        if auth_enabled:
            assert (copy.status_code, delete.status_code) == (403, 403)
        else:
            assert (copy.status_code, delete.status_code) == (200, 204)
    finally:
        s3.delete_bucket_policy(Bucket=upload)
        for bucket in (secret, upload):
            for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"])
            s3.delete_bucket(Bucket=bucket)


@pytest.mark.auth_contract
def test_s3_copy_source_in_the_query_string_is_authorized_contract(auth_enabled, iam_user, s3):
    """x-amz-copy-source hoisted into the query string is checked like the header."""
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials
    from conftest import second_access_key

    secret, upload = f"hoist-src-{uuid.uuid4().hex[:8]}", f"hoist-dst-{uuid.uuid4().hex[:8]}"
    for bucket in (secret, upload):
        s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=secret, Key="c", Body=b"secret")
    user = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:PutObject", "Resource": f"arn:aws:s3:::{upload}/*"}]})
    try:
        with second_access_key(user) as (key, secret_key):
            url = f"{ENDPOINT}/{upload}/b?x-amz-copy-source=%2F{secret}%2Fc"
            request = AWSRequest(method="PUT", url=url, data=b"")
            S3SigV4Auth(Credentials(key, secret_key), "s3", "us-east-1").add_auth(request)
            status = requests.put(url, data=b"", headers=dict(request.headers)).status_code
        assert status == (403 if auth_enabled else 200)
    finally:
        for bucket in (secret, upload):
            for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"])
            s3.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# S3 Access Points: the S3 Control API and the data plane through an access point ARN or alias.
# ---------------------------------------------------------------------------


ACCOUNT = "000000000000"


def _client(service, region, account_key="test"):
    return boto3.client(
        service, endpoint_url=ENDPOINT, region_name=region,
        aws_access_key_id=account_key, aws_secret_access_key="test",
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


@pytest.fixture(autouse=True)
def _s3control_dns():
    """boto3 prefixes S3 Control and access point hosts with the account id."""
    with patch_endpoint_dns():
        yield


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def bucket():
    """A bucket in us-east-1 holding k.txt."""
    s3 = _client("s3", "us-east-1")
    name = f"ap-data-{_uid()}"
    s3.create_bucket(Bucket=name)
    s3.put_object(Bucket=name, Key="k.txt", Body=b"hello")
    yield name
    for obj in s3.list_objects_v2(Bucket=name).get("Contents", []):
        s3.delete_object(Bucket=name, Key=obj["Key"])
    try:
        s3.delete_bucket(Bucket=name)
    except ClientError:
        pass


@pytest.fixture
def access_point(bucket):
    ctl = _client("s3control", "us-east-1")
    name = f"ap-{_uid()}"
    created = ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    yield name, created
    try:
        ctl.delete_access_point(AccountId=ACCOUNT, Name=name)
    except ClientError:
        pass


# --- Control plane ---------------------------------------------------------

def test_access_point_control_create_get_list_delete(bucket):
    ctl = _client("s3control", "us-east-1")
    name = f"ap-{_uid()}"
    created = ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    arn = f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}"
    assert created["AccessPointArn"] == arn
    assert re.fullmatch(rf"{name[:20]}-[a-z0-9]{{29}}use1b-s3alias", created["Alias"])

    ap = ctl.get_access_point(AccountId=ACCOUNT, Name=name)
    assert (ap["Name"], ap["Bucket"], ap["BucketAccountId"], ap["NetworkOrigin"]) == (
        name, bucket, ACCOUNT, "Internet")
    assert ap["AccessPointArn"] == arn and ap["Alias"] == created["Alias"]
    assert ap["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True}
    assert ap["Endpoints"]["ipv4"] == "s3-accesspoint.us-east-1.amazonaws.com"
    assert set(ap["Endpoints"]) == {"ipv4", "fips", "dualstack", "fips_dualstack"}
    assert "CreationDate" in ap

    second = f"ap-{_uid()}"
    ctl.create_access_point(AccountId=ACCOUNT, Name=second, Bucket=bucket,
                            VpcConfiguration={"VpcId": "vpc-0123456789abcdef0"})
    try:
        listed = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket)["AccessPointList"]
        assert sorted(a["Name"] for a in listed) == sorted([name, second])
        assert {a["Name"]: a["NetworkOrigin"] for a in listed}[second] == "VPC"
        page = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket, MaxResults=1)
        assert len(page["AccessPointList"]) == 1 and page["NextToken"]
        rest = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket, NextToken=page["NextToken"])
        assert len(rest["AccessPointList"]) == 1
    finally:
        ctl.delete_access_point(AccountId=ACCOUNT, Name=second)

    with pytest.raises(ClientError) as exc:
        ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    assert exc.value.response["Error"]["Code"] == "AccessPointAlreadyOwnedByYou"
    assert exc.value.response["Error"]["Message"] == (
        "Your previous request to create the named accesspoint succeeded and you already own it.")

    with pytest.raises(ClientError) as exc:
        _client("s3", "us-east-1").delete_bucket(Bucket=bucket)
    assert exc.value.response["Error"]["Code"] == "BucketHasAccessPointsAttached"

    ctl.delete_access_point(AccountId=ACCOUNT, Name=name)
    with pytest.raises(ClientError) as exc:
        ctl.get_access_point(AccountId=ACCOUNT, Name=name)
    err = exc.value.response["Error"]
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert (err["Code"], err["Message"], err["AccessPointName"]) == (
        "NoSuchAccessPoint", "The specified accesspoint does not exist", name)


def test_access_point_control_bucket_must_be_in_request_region(bucket):
    with pytest.raises(ClientError) as exc:
        _client("s3control", "eu-west-1").create_access_point(AccountId=ACCOUNT, Name=f"ap-{_uid()}", Bucket=bucket)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"
    assert exc.value.response["Error"]["Message"] == (
        "Amazon S3 AccessPoint can only be created for bucket in the same region")


def test_access_point_control_same_name_in_another_region(bucket):
    """Access point names are unique per account and region (observed)."""
    euw1_s3 = _client("s3", "eu-west-1")
    euw1_bucket = f"ap-euw1-{_uid()}"
    euw1_s3.create_bucket(Bucket=euw1_bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    name = f"ap-{_uid()}"
    use1, euw1 = _client("s3control", "us-east-1"), _client("s3control", "eu-west-1")
    use1.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    try:
        alias = euw1.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=euw1_bucket)["Alias"]
        assert alias.endswith("euw1b-s3alias")
        assert euw1.get_access_point(AccountId=ACCOUNT, Name=name)["Bucket"] == euw1_bucket
        assert use1.get_access_point(AccountId=ACCOUNT, Name=name)["Bucket"] == bucket
        euw1.delete_access_point(AccountId=ACCOUNT, Name=name)
    finally:
        use1.delete_access_point(AccountId=ACCOUNT, Name=name)
        euw1_s3.delete_bucket(Bucket=euw1_bucket)


def test_access_point_control_policy_round_trip(access_point):
    name, _ = access_point
    ctl = _client("s3control", "us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]})
    ctl.put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=policy)
    assert ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)["Policy"] == policy
    ctl.delete_access_point_policy(AccountId=ACCOUNT, Name=name)
    with pytest.raises(ClientError) as exc:
        ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.parametrize("method,path,status,fields", [
    ("POST", "/{ap}/policy", 405, {"Method": "POST", "ResourceType": "ACCESSPOINTPOLICY"}),
    ("PATCH", "/{ap}/policy", 405, {"Method": "PATCH", "ResourceType": "ACCESSPOINTPOLICY"}),
    ("POST", "/{ap}", 405, {"Method": "POST", "ResourceType": "ACCESSPOINT"}),
    ("POST", "", 405, {"Method": "POST", "ResourceType": "ACCESSPOINTS"}),
    ("GET", "/{ap}/x", 400, {"URI": "accesspoint/{ap}/x"}),
    ("DELETE", "/{ap}/x", 400, {}),
    ("DELETE", "/{ap}/policy/x", 400, {}),
    ("GET", "/{ap}/", 400, {}),
    ("GET", "//{ap}", 400, {}),
    ("PUT", "//{ap}/policy", 400, {}),
    ("GET", "/", 400, {}),    ("GET", "/{ap}/policyStatus", 404, {"AccessPointName": "{ap}/policyStatus"}),
    ("GET", "/{ap}/scope", 404, {}),
])
def test_access_point_control_unserved_requests(access_point, method, path, status, fields):
    """Unserved methods answer 405, unparsed paths 400, unemulated routes 404; none changes the access point."""
    name, _ = access_point
    ctl = _client("s3control", "us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]})
    ctl.put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=policy)
    body = b"<PutAccessPointPolicyRequest><Policy>{}</Policy></PutAccessPointPolicyRequest>"
    got, _, data = _raw(method, ENDPOINT.split("://", 1)[1], "/v20180820/accesspoint" + path.format(ap=name),
                        "us-east-1", body if method == "PUT" else b"", headers={"x-amz-account-id": ACCOUNT})
    code = {405: "MethodNotAllowed", 400: "InvalidURI", 404: "NoSuchAccessPoint"}[status]
    assert (got, re.search(rb"<Code>(\w+)</Code>", data).group(1).decode()) == (status, code)
    for tag, value in fields.items():
        assert f"<{tag}>{value.format(ap=name)}</{tag}>".encode() in data
    assert ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)["Policy"] == policy


@pytest.mark.auth_contract
def test_access_point_named_policy_needs_the_access_point_action(auth_enabled, iam_user, bucket):
    """/v20180820/accesspoint/policy names an access point called "policy"."""
    ctl = _client("s3control", "us-east-1")
    ctl.create_access_point(AccountId=ACCOUNT, Name="policy", Bucket=bucket)
    try:
        user = iam_user(policy={"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": ["s3:GetAccessPointPolicy", "s3:DeleteAccessPointPolicy"],
            "Resource": "*"}]}).client("s3control", region="us-east-1")
        user.delete_access_point_policy(AccountId=ACCOUNT, Name="policy")
        if auth_enabled:
            for call in (user.get_access_point, user.delete_access_point):
                with pytest.raises(ClientError) as exc:
                    call(AccountId=ACCOUNT, Name="policy")
                assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
                assert exc.value.response["Error"]["Code"] == "AccessDenied"
            assert ctl.get_access_point(AccountId=ACCOUNT, Name="policy")["Bucket"] == bucket
        else:
            assert user.get_access_point(AccountId=ACCOUNT, Name="policy")["Bucket"] == bucket
            user.delete_access_point(AccountId=ACCOUNT, Name="policy")
    finally:
        try:
            ctl.delete_access_point(AccountId=ACCOUNT, Name="policy")
        except ClientError:
            pass


@pytest.mark.auth_contract
def test_access_point_policy_unserved_method_is_refused_before_authorization(auth_enabled, iam_user, access_point):
    """POST on an access point policy answers 405 to any caller and returns no policy."""
    name, _ = access_point
    _client("s3control", "us-east-1").put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=json.dumps({
        "Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]}))
    user = iam_user()
    iam = make_client("iam")
    key = iam.create_access_key(UserName=user.name)["AccessKey"]
    try:
        signed = {"key": (key["AccessKeyId"], key["SecretAccessKey"]), "headers": {"x-amz-account-id": ACCOUNT}}
        host, path = ENDPOINT.split("://", 1)[1], f"/v20180820/accesspoint/{name}/policy"
        status, _, data = _raw("POST", host, path, "us-east-1", **signed)
        assert status == 405 and b"<ResourceType>ACCESSPOINTPOLICY</ResourceType>" in data
        assert b"<Policy>" not in data
        assert _raw("GET", host, path, "us-east-1", **signed)[0] == (403 if auth_enabled else 200)
    finally:
        iam.delete_access_key(UserName=user.name, AccessKeyId=key["AccessKeyId"])


# --- Data plane --------------------------------------------------------------

def _raw(method, host, path, region, body=b"", key=("test", "test"), headers=None):
    """A SigV4-signed request with an explicit Host, no redirect handling."""
    import hashlib
    import http.client
    from urllib.parse import urlparse

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(method=method, url=f"http://{host}{path}", data=body, headers=headers)
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    S3SigV4Auth(Credentials(*key), "s3", region).add_auth(req)
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, body=body or None, headers={**dict(req.headers.items()), "Host": host})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data


def test_access_point_arn_reaches_the_bucket(bucket, access_point):
    name, created = access_point
    s3 = _client("s3", "us-east-1")
    arn = created["AccessPointArn"]
    assert s3.get_object(Bucket=arn, Key="k.txt")["Body"].read() == b"hello"
    s3.put_object(Bucket=arn, Key="through-ap.txt", Body=b"via ap")
    assert s3.get_object(Bucket=bucket, Key="through-ap.txt")["Body"].read() == b"via ap"
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=arn)["Contents"]]
    assert keys == ["k.txt", "through-ap.txt"]
    # A client configured for another region still signs for the ARN's region.
    assert _client("s3", "eu-west-1").get_object(Bucket=arn, Key="k.txt")["Body"].read() == b"hello"


def test_access_point_arn_wrong_region_signing_gets_the_redirect(access_point):
    name, _ = access_point
    host = f"{name}-{ACCOUNT}.{ENDPOINT.split('://', 1)[1]}"
    status, headers, _ = _raw("GET", host, "/k.txt", "eu-west-1")
    assert status == 301
    assert headers["x-amz-bucket-region"] == "us-east-1"
    assert _raw("GET", host, "/k.txt", "us-east-1")[0] == 200


def test_access_point_arn_other_account_is_denied(access_point):
    _, created = access_point
    other = _client("s3", "us-east-1", account_key="111122223333")
    with pytest.raises(ClientError) as exc:
        other.get_object(Bucket=created["AccessPointArn"], Key="k.txt")
    assert exc.value.response["Error"]["Code"] in ("AccessDenied", "NoSuchAccessPoint")


@pytest.mark.auth_contract
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_access_point_arn_shaped_bucket_without_access_point_is_a_bucket(auth_enabled, method):
    """`<name>-<12 digits>` is a valid bucket name; with no access point of
    that name it is served as the bucket."""
    s3 = _client("s3", "us-east-1")
    name = f"plain-{_uid()}-123456789012"
    s3.create_bucket(Bucket=name)
    try:
        s3.put_object(Bucket=name, Key="b.txt", Body=b"bucket")
        host = f"{name}.{ENDPOINT.split('://', 1)[1]}"
        status, headers, body = _raw(method, host, "/b.txt", "us-east-1")
        assert status == 200
        assert body == (b"bucket" if method == "GET" else b"")
        if auth_enabled and method == "HEAD":
            assert re.fullmatch(r"[A-Z0-9]{16}", headers["x-amz-request-id"])
    finally:
        s3.delete_object(Bucket=name, Key="b.txt")
        s3.delete_bucket(Bucket=name)


def test_access_point_alias_works_as_a_bucket_name(bucket, access_point):
    _, created = access_point
    s3 = _client("s3", "us-east-1")
    alias = created["Alias"]
    assert s3.get_object(Bucket=alias, Key="k.txt")["Body"].read() == b"hello"
    s3.put_object(Bucket=alias, Key="alias.txt", Body=b"via alias")
    assert s3.get_object(Bucket=bucket, Key="alias.txt")["Body"].read() == b"via alias"
    s3.copy_object(Bucket=bucket, Key="copied.txt", CopySource={"Bucket": alias, "Key": "k.txt"})
    assert s3.get_object(Bucket=bucket, Key="copied.txt")["Body"].read() == b"hello"


@pytest.mark.auth_contract
@pytest.mark.parametrize("addressing_style", ["path", "virtual"])
@pytest.mark.parametrize("allow_upload", [False, True], ids=["no-permission", "allow"])
def test_access_point_alias_upload_permission_contract(
    bucket, access_point, auth_enabled, iam_user, addressing_style, allow_upload,
):
    _, created = access_point
    policy = {"Statement": [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}]}
    client = iam_user(policy=policy if allow_upload else None).client(
        "s3", config=Config(s3={"addressing_style": addressing_style}),
    )
    root = _client("s3", "us-east-1")
    if auth_enabled and not allow_upload:
        with pytest.raises(ClientError) as exc:
            client.put_object(Bucket=created["Alias"], Key="contract-upload", Body=b"via alias")
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
        assert root.list_objects_v2(Bucket=bucket, Prefix="contract-upload")["KeyCount"] == 0
    else:
        assert client.put_object(
            Bucket=created["Alias"], Key="contract-upload", Body=b"via alias",
        )["ResponseMetadata"]["HTTPStatusCode"] == 200
        with root.get_object(Bucket=bucket, Key="contract-upload")["Body"] as body:
            assert body.read() == b"via alias"


@pytest.mark.auth_contract
@pytest.mark.parametrize("target,addressing_style", [("Alias", "path"), ("Alias", "virtual"), ("AccessPointArn", "virtual")])
@pytest.mark.parametrize("allow_head", [False, True], ids=["no-permission", "allow"])
def test_access_point_head_permission_contract(access_point, auth_enabled, iam_user, target, addressing_style, allow_head):
    _, created = access_point
    policy = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
    client = iam_user(policy=policy if allow_head else None).client(
        "s3", region="us-east-1", config=Config(s3={"addressing_style": addressing_style}))
    if auth_enabled and not allow_head:
        with pytest.raises(ClientError) as exc:
            client.head_object(Bucket=created[target], Key="k.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    else:
        response = client.head_object(Bucket=created[target], Key="k.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["ContentLength"] == 5


@pytest.mark.auth_contract
@pytest.mark.parametrize("target", ["Alias", "AccessPointArn"])
@pytest.mark.parametrize("allow_delete", [False, True], ids=["no-permission", "allow"])
def test_access_point_delete_permission_contract(bucket, access_point, auth_enabled, iam_user, target, allow_delete):
    _, created = access_point
    policy = {"Statement": [{"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "*"}]}
    client = iam_user(policy=policy if allow_delete else None).client("s3", region="us-east-1")
    root = _client("s3", "us-east-1")
    if auth_enabled and not allow_delete:
        with pytest.raises(ClientError) as exc:
            client.delete_object(Bucket=created[target], Key="k.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        with root.get_object(Bucket=bucket, Key="k.txt")["Body"] as body:
            assert body.read() == b"hello"
    else:
        assert client.delete_object(Bucket=created[target], Key="k.txt")["ResponseMetadata"]["HTTPStatusCode"] == 204
        with pytest.raises(ClientError) as exc:
            root.head_object(Bucket=bucket, Key="k.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def _ap_status(call):
    try:
        return call()["ResponseMetadata"]["HTTPStatusCode"]
    except ClientError as exc:
        return exc.response["ResponseMetadata"]["HTTPStatusCode"], exc.response["Error"]["Code"]


def test_access_point_block_public_acls_refuses_public_acl_writes():
    """An access point's BlockPublicAcls refuses public ACLs sent through it, whatever the bucket's settings."""
    s3, ctl = _client("s3", "us-east-1"), _client("s3control", "us-east-1")
    name = f"ap-acl-{_uid()}"
    all_off = {k: False for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}
    s3.create_bucket(Bucket=name, ObjectOwnership="BucketOwnerPreferred")
    s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration=all_off)
    s3.put_object(Bucket=name, Key="k.txt", Body=b"hello")
    blocking = ctl.create_access_point(AccountId=ACCOUNT, Name=f"ap-on-{_uid()}", Bucket=name)
    open_ap = ctl.create_access_point(
        AccountId=ACCOUNT, Name=f"ap-off-{_uid()}", Bucket=name, PublicAccessBlockConfiguration=all_off)
    try:
        denied = (403, "AccessDenied")
        for target in (blocking["Alias"], blocking["AccessPointArn"]):
            assert _ap_status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="public-read")) == denied
            assert _ap_status(lambda: s3.put_object_acl(Bucket=target, Key="k.txt", ACL="public-read")) == denied
            assert _ap_status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="private")) == 200
        for target in (open_ap["Alias"], open_ap["AccessPointArn"], name):
            assert _ap_status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="public-read")) == 200
    finally:
        for ap in (blocking, open_ap):
            ctl.delete_access_point(AccountId=ACCOUNT, Name=ap["AccessPointArn"].rsplit("/", 1)[1])
        for obj in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)


def test_access_point_alias_from_another_account_is_denied(access_point):
    _name, created = access_point
    other = _client("s3", "us-east-1", account_key="111122223333")
    with pytest.raises(ClientError) as exc:
        other.get_object(Bucket=created["Alias"], Key="k.txt")
    assert exc.value.response["Error"]["Code"] == "AccessDenied"


def _access_point_policy(bucket, access_point_arn, grant, deny=None, condition=None):
    """An identity policy allowing reads and copies on ``grant`` ("bucket", "access point" or "both")."""
    arns = {"bucket": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            "access point": [access_point_arn, f"{access_point_arn}/object/*"]}
    resources = arns["bucket"] + arns["access point"] if grant == "both" else arns[grant]
    statements = [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                   "Resource": resources}]
    if deny or condition:
        statements.append({"Effect": "Deny", "Action": ["s3:GetObject", "s3:ListBucket"],
                           "Resource": arns[deny] if deny else "*", **({"Condition": condition} if condition else {})})
    return {"Version": "2012-10-17", "Statement": statements}


def _assert_access(auth_enabled, allowed, call):
    """Under AUTH a refused call answers 403 AccessDenied; anything else succeeds."""
    if auth_enabled and not allowed:
        with pytest.raises(ClientError) as exc:
            call()
        error = exc.value.response
        assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("AccessDenied", 403)
    else:
        call()


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("grant, deny, allowed", [
    ("both", None, True),
    ("bucket", None, False),
    ("access point", None, False),
    ("both", "access point", False),
    ("both", "bucket", False),
], ids=["both", "bucket-only", "access-point-only", "deny-access-point", "deny-bucket"])
def test_access_point_request_needs_access_point_and_bucket_contract(auth_enabled, iam_user, bucket, access_point,
                                                                     via, grant, deny, allowed):
    """Through an access point a read and a listing are checked on the access point and on its bucket."""
    _name, created = access_point
    client = iam_user(policy=_access_point_policy(bucket, created["AccessPointArn"], grant, deny)).client("s3")
    target = created[via]
    _assert_access(auth_enabled, allowed, lambda: client.get_object(Bucket=target, Key="k.txt"))
    _assert_access(auth_enabled, allowed, lambda: client.list_objects_v2(Bucket=target))
    _assert_access(auth_enabled, grant != "access point" and deny != "bucket",
                   lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("key, condition_value, through_access_point, through_bucket", [
    ("s3:DataAccessPointArn", "arn", False, True),
    ("s3:DataAccessPointAccount", ACCOUNT, False, True),
    ("s3:AccessPointNetworkOrigin", "Internet", False, True),
    ("s3:ExistingObjectTag/class", "secret", False, False),
    ("s3:DataAccessPointArn", None, True, False),
], ids=["arn", "account", "network-origin", "object-tag", "arn-absent"])
def test_access_point_condition_keys_contract(auth_enabled, iam_user, bucket, access_point, via, key,
                                              condition_value, through_access_point, through_bucket):
    """A request through an access point carries its ARN, account and network origin, and the object's tags."""
    _name, created = access_point
    _client("s3", "us-east-1").put_object_tagging(Bucket=bucket, Key="k.txt",
                                                  Tagging={"TagSet": [{"Key": "class", "Value": "secret"}]})
    arn = created["AccessPointArn"]
    condition = ({"Null": {key: "true"}} if condition_value is None
                 else {"StringEquals": {key: arn if condition_value == "arn" else condition_value}})
    client = iam_user(policy=_access_point_policy(bucket, arn, "both", condition=condition)).client("s3")
    _assert_access(auth_enabled, through_access_point, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, through_bucket, lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("condition, through_bucket", [(False, False), (True, True)], ids=["deny", "deny-on-arn-key"])
def test_access_point_request_meets_the_bucket_policy_contract(auth_enabled, iam_user, bucket, access_point, via,
                                                               condition, through_bucket):
    """The bucket policy applies through an access point and sees its ARN."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    user = iam_user(policy=_access_point_policy(bucket, arn, "both"))
    statement = {"Effect": "Deny", "Principal": {"AWS": user.arn}, "Action": "s3:GetObject",
                 "Resource": f"arn:aws:s3:::{bucket}/*"}
    if condition:
        statement["Condition"] = {"StringEquals": {"s3:DataAccessPointArn": arn}}
    _client("s3", "us-east-1").put_bucket_policy(
        Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [statement]}))
    client = user.client("s3")
    _assert_access(auth_enabled, False, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, through_bucket, lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("grant, effect, delegate, allowed", [
    ("bucket", "Allow", False, True),
    (None, "Allow", True, True),
    ("access point", None, True, True),
    ("access point", "Allow", False, False),
    ("both", "Deny", True, False),
], ids=["allow-identity-on-bucket", "allow-delegated", "delegated-identity-on-access-point",
        "allow-not-delegated", "deny"])
def test_access_point_policy_contract(auth_enabled, iam_user, bucket, access_point, via, grant, effect, delegate,
                                      allowed):
    """The access point policy answers for the access point, the bucket policy or identity for the bucket."""
    name, created = access_point
    arn = created["AccessPointArn"]
    user = iam_user(policy=_access_point_policy(bucket, arn, grant) if grant else None)
    if delegate:
        _client("s3", "us-east-1").put_bucket_policy(Bucket=bucket, Policy=json.dumps({
            "Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "*",
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
                "Condition": {"StringEquals": {"s3:DataAccessPointAccount": ACCOUNT}}}]}))
    if effect:
        _client("s3control", "us-east-1").put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=json.dumps({
            "Version": "2012-10-17", "Statement": [{
                "Effect": effect, "Principal": {"AWS": user.arn}, "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [arn, f"{arn}/object/*"]}]}))
    client = user.client("s3")
    _assert_access(auth_enabled, allowed, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, allowed, lambda: client.list_objects_v2(Bucket=created[via]))
    _assert_access(auth_enabled, grant in ("bucket", "both"), lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
def test_access_point_refuses_bucket_policy_writes_contract(auth_enabled, iam_user, bucket, access_point, via):
    """Under AUTH an access point refuses PutBucketPolicy and DeleteBucketPolicy and serves GetBucketPolicy."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    client = iam_user(policy={"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": ["s3:PutBucketPolicy", "s3:GetBucketPolicy", "s3:DeleteBucketPolicy"],
        "Resource": [f"arn:aws:s3:::{bucket}", arn]}]}).client("s3", region="us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:::{bucket}/*"}]})
    client.put_bucket_policy(Bucket=bucket, Policy=policy)
    assert client.get_bucket_policy(Bucket=created[via])["Policy"] == policy
    for write in (lambda: client.delete_bucket_policy(Bucket=created[via]),
                  lambda: client.put_bucket_policy(Bucket=created[via], Policy=policy)):
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                write()
            error = exc.value.response["Error"]
            assert error["Code"] == "AccessDenied"
            assert error["Message"].endswith(f'on resource: "{arn}" with an explicit deny in a resource-based policy')
            assert client.get_bucket_policy(Bucket=bucket)["Policy"] == policy
        else:
            write()
    client.delete_bucket_policy(Bucket=bucket)


@pytest.mark.auth_contract
def test_access_point_refuses_unsigned_requests_contract(auth_enabled, bucket, access_point):
    """Under AUTH an unsigned request through an access point is refused; the bucket still serves it."""
    _name, created = access_point
    s3 = _client("s3", "us-east-1")
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={
        "Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]})
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={k: False for k in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")})
    s3.put_object(Bucket=bucket, Key="pub.txt", Body=b"public", ACL="public-read")
    anon = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                        config=Config(signature_version=UNSIGNED, retries={"mode": "standard"}))
    assert anon.get_object(Bucket=bucket, Key="pub.txt")["Body"].read() == b"public"
    for target in (created["Alias"], created["AccessPointArn"]):
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                anon.get_object(Bucket=target, Key="pub.txt")
            error = exc.value.response
            assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidRequest", 400)
        else:
            assert anon.get_object(Bucket=target, Key="pub.txt")["Body"].read() == b"public"


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
def test_access_point_sigv2_requests_contract(auth_enabled, iam_user, bucket, access_point, via):
    """Through an access point a SigV2 presigned URL is served, a SigV2 Authorization header is refused under AUTH."""
    _name, created = access_point
    user = iam_user(policy=_access_point_policy(bucket, created["AccessPointArn"], "both"))
    client = user.client("s3", region="us-east-1", config=Config(signature_version="s3"))
    url = client.generate_presigned_url("get_object", Params={"Bucket": created[via], "Key": "k.txt"})
    assert "AWSAccessKeyId=" in url
    with urllib.request.urlopen(url, timeout=10) as resp:
        assert resp.read() == b"hello"
    tampered = re.sub(r"Signature=[^&]+", "Signature=AAAA", url)
    if auth_enabled:
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(tampered, timeout=10)
        assert exc.value.code == 403
        assert b"<Code>SignatureDoesNotMatch</Code>" in exc.value.read()
        with pytest.raises(ClientError) as exc:
            client.get_object(Bucket=created[via], Key="k.txt")
        error = exc.value.response
        assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidRequest", 400)
    else:
        with urllib.request.urlopen(tampered, timeout=10) as resp:
            assert resp.read() == b"hello"
        assert client.get_object(Bucket=created[via], Key="k.txt")["Body"].read() == b"hello"
    assert client.get_object(Bucket=bucket, Key="k.txt")["Body"].read() == b"hello"


@pytest.mark.auth_contract
@pytest.mark.parametrize("grant, deny, condition, allowed", [
    ("both", None, False, True),
    ("bucket", None, False, False),
    ("both", "access point", False, False),
    ("both", None, True, False),
], ids=["both", "bucket-only", "deny-access-point", "deny-on-arn-key"])
def test_access_point_alias_copy_source_contract(auth_enabled, iam_user, bucket, access_point, grant, deny,
                                                 condition, allowed):
    """A copy source named by an alias is read through its access point."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    policy = _access_point_policy(bucket, arn, grant, deny,
                                  {"StringEquals": {"s3:DataAccessPointArn": arn}} if condition else None)
    client = iam_user(policy=policy).client("s3")
    _assert_access(auth_enabled, allowed, lambda: client.copy_object(
        Bucket=bucket, Key="copy.txt", CopySource={"Bucket": created["Alias"], "Key": "k.txt"}))
    _assert_access(auth_enabled, True, lambda: client.copy_object(
        Bucket=bucket, Key="copy.txt", CopySource={"Bucket": bucket, "Key": "k.txt"}))


def test_access_point_control_invalid_name(bucket):
    ctl = _client("s3control", "us-east-1")
    with pytest.raises(ClientError) as exc:
        ctl.create_access_point(AccountId=ACCOUNT, Name="Bad_Name", Bucket=bucket)
    err = exc.value.response
    assert (err["Error"]["Code"], err["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidAccessPoint", 400)


# ---------------------------------------------------------------------------
# GetObject error order, as observed on AWS (Osaka and Tokyo, 2026-10-18), replayed locally.
# ---------------------------------------------------------------------------


HOME, TOKYO = "ap-northeast-3", "ap-northeast-1"
_DOWNLOAD_PAYLOAD = b"download error validation"


@pytest.fixture
def download_bucket(s3):
    name = f"download-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=_DOWNLOAD_PAYLOAD)
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def download_client(download_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{download_bucket}/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{download_bucket}"},
    ]}).client("s3", region=HOME)


def _download(bucket, client, *, credential="valid", endpoint_region=HOME, region=HOME,
              virtual=False, key="probe.txt", query="", root=False):
    frozen = client._request_signer._credentials.get_frozen_credentials()
    access_key, secret = frozen.access_key, frozen.secret_key
    if credential == "unknown":
        access_key, secret = "AKIAUNKNOWNKEY000001", "wrong"
    elif credential == "wrong-secret":
        secret = "wrong"
    if root:
        access_key, secret = "test", "any-secret"
    target = urlsplit(ENDPOINT)
    host = f"s3.{endpoint_region}.{target.netloc}"
    path = f"/{quote(bucket)}/{quote(key, safe='/')}"
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="GET", url=f"http://{host}{path}{query}")
    S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("GET", path + query, headers={**dict(request.headers), "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _assert_download_error(response, code, *, bucket, key="probe.txt", region=HOME, access_key=None):
    _, headers, body = response
    error = fromstring(body)
    messages = {
        "InvalidBucketName": "The specified bucket is not valid.",
        "InvalidAccessKeyId": "The AWS Access Key Id you provided does not exist in our records.",
        "SignatureDoesNotMatch": "The request signature we calculated does not match the signature you provided. "
                                 "Check your key and signing method.",
        "AuthorizationHeaderMalformed": f"The authorization header is malformed; the region '{region}' is wrong; "
                                        f"expecting '{HOME if region == TOKYO else TOKYO}'",
        "PermanentRedirect": "The bucket you are attempting to access must be addressed using the specified "
                             "endpoint. Please send all future requests to this endpoint.",
        "NoSuchBucket": "The specified bucket does not exist",
        "NoSuchKey": "The specified key does not exist.",
    }
    details = {
        "InvalidBucketName": ["BucketName"], "InvalidAccessKeyId": ["AWSAccessKeyId"],
        "AuthorizationHeaderMalformed": ["Region"], "PermanentRedirect": ["Bucket", "Endpoint"],
        "NoSuchBucket": ["BucketName"], "NoSuchKey": ["Key"],
        "SignatureDoesNotMatch": ["AWSAccessKeyId", "StringToSign", "SignatureProvided", "StringToSignBytes",
                                  "CanonicalRequest", "CanonicalRequestBytes"],
    }
    expected = ["Code", "Message", *details[code], "RequestId", "HostId"]
    assert error.findtext("Code") == code
    assert error.findtext("Message") == messages[code]
    if code == "SignatureDoesNotMatch":
        # Real GET responses varied the order of the signature diagnostics.
        assert sorted(child.tag for child in error) == sorted(expected)
        for name in ("CanonicalRequest", "StringToSign"):
            assert bytes.fromhex(error.findtext(name + "Bytes")).decode() == error.findtext(name)
        assert error.findtext("CanonicalRequest").startswith("GET\n")
        assert error.findtext("StringToSign").splitlines()[-1] == hashlib.sha256(
            error.findtext("CanonicalRequest").encode()).hexdigest()
        assert re.fullmatch(r"[a-f0-9]{64}", error.findtext("SignatureProvided"))
        signed = AWSRequest()
        document = error.findtext("StringToSign")
        signed.context["timestamp"] = document.splitlines()[1]
        signer = S3SigV4Auth(Credentials(error.findtext("AWSAccessKeyId"), "wrong"), "s3", region)
        assert signer.signature(document, signed) == error.findtext("SignatureProvided")
    else:
        assert [child.tag for child in error] == expected
    assert headers["content-type"] == "application/xml"
    assert error.findtext("RequestId") == headers["x-amz-request-id"]
    assert re.fullmatch(r"[A-Z0-9]{16}", error.findtext("RequestId"))
    assert error.findtext("HostId") == headers["x-amz-id-2"]
    assert len(base64.b64decode(error.findtext("HostId"), validate=True)) >= 48
    assert "location" not in headers
    if code == "PermanentRedirect":
        assert error.findtext("Endpoint") == f"{bucket}.s3.{HOME}.{urlsplit(ENDPOINT).netloc}"
        assert error.findtext("Bucket") == bucket
        assert headers["x-amz-bucket-region"] == HOME
    else:
        assert "x-amz-bucket-region" not in headers
    if code in {"InvalidBucketName", "NoSuchBucket"}:
        assert error.findtext("BucketName") == bucket
    if code == "NoSuchKey":
        assert error.findtext("Key") == key
    if code == "AuthorizationHeaderMalformed":
        assert error.findtext("Region") == (HOME if region == TOKYO else TOKYO)
    if access_key is not None:
        assert error.findtext("AWSAccessKeyId") == access_key


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations; AUTH=false is a separate local contract.
    (False, "valid", HOME, HOME, (200, None), (200, None)),
    (False, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (200, None)),
    (False, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (200, None)),
    (True, "valid", HOME, HOME, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
    (False, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (True, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    # Existing-bucket valid/unknown + Tokyo endpoint/Osaka signature live in
    # test_download_signing_region_precedes_home_and_credentials, including wire checks.
    (False, "wrong-secret", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (200, None)),
    (True, "valid", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (False, "valid", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "unknown", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "wrong-secret", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (True, "valid", TOKYO, TOKYO, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, TOKYO, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, TOKYO, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
])
def test_download_combined_errors(download_bucket, download_client, auth_enabled,
                                  virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{download_bucket}" if missing else download_bucket
    expected = expected if auth_enabled else compat_expected
    response = _download(bucket, download_client, credential=credential, endpoint_region=endpoint_region,
                         region=region, virtual=virtual)
    code = fromstring(response[2]).findtext("Code") if response[0] != 200 else None
    assert (response[0], code) == expected
    if response[0] == 200:
        assert response[2] == _DOWNLOAD_PAYLOAD
    elif auth_enabled:
        access_key = ("AKIAUNKNOWNKEY000001" if credential == "unknown"
                      else download_client._request_signer._credentials.access_key)
        _assert_download_error(response, code, bucket=bucket, region=region,
                      access_key=access_key if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch"} else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_download_signing_region_precedes_home_and_credentials(download_bucket, download_client, s3,
                                                                auth_enabled, virtual, credential):
    # Fixed AWS observation: Osaka bucket + Tokyo endpoint + Osaka signature.
    # Kept separately as the regression for GET's different priority from HEAD/DELETE.
    response = _download(download_bucket, download_client, virtual=virtual, credential=credential,
                         endpoint_region=TOKYO, region=HOME)
    assert response[0] == (400 if auth_enabled else 200)
    if auth_enabled:
        _assert_download_error(response, "AuthorizationHeaderMalformed", bucket=download_bucket, region=HOME)
    else:
        assert response[2] == _DOWNLOAD_PAYLOAD
    with s3.get_object(Bucket=download_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == _DOWNLOAD_PAYLOAD


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_download_missing_key(download_bucket, download_client, auth_enabled, virtual, credential, region):
    response = _download(download_bucket, download_client, virtual=virtual, credential=credential,
                         region=region, key="missing.txt")
    code = ("AuthorizationHeaderMalformed" if region != HOME else
            "InvalidAccessKeyId" if credential == "unknown" else
            "SignatureDoesNotMatch" if credential == "wrong-secret" else "NoSuchKey")
    if not auth_enabled:
        code = "AuthorizationHeaderMalformed" if region != HOME else "NoSuchKey"
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        400 if code == "AuthorizationHeaderMalformed" else 404 if code == "NoSuchKey" else 403, code,
    )
    if auth_enabled:
        _assert_download_error(response, code, bucket=download_bucket, key="missing.txt", region=region)


@pytest.mark.auth_contract
def test_download_invalid_name_precedes_credentials(download_client, auth_enabled):
    response = _download("invalid bucket", download_client, credential="unknown")
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        (400, "InvalidBucketName") if auth_enabled else (404, "NoSuchBucket")
    )
    if auth_enabled:
        _assert_download_error(response, "InvalidBucketName", bucket="invalid bucket")


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_download_still_enforces_iam(download_bucket, iam_user, auth_enabled, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": "s3:GetObject", "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _download(download_bucket, client, virtual=virtual)
    if auth_enabled:
        assert (response[0], fromstring(response[2]).findtext("Code")) == (403, "AccessDenied")
    else:
        assert (response[0], response[2]) == (200, _DOWNLOAD_PAYLOAD)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_download_reserved_root_compatibility(download_bucket, s3, virtual):
    response = _download(download_bucket, s3, virtual=virtual, root=True)
    assert (response[0], response[2]) == (200, _DOWNLOAD_PAYLOAD)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("query", ["?versionId=null", "?tagging"])
def test_download_subresources_keep_their_iam_path(download_bucket, iam_user, auth_enabled, virtual, query):
    client = iam_user().client("s3", region=HOME)
    response = _download(f"missing-{download_bucket}", client, virtual=virtual, query=query)
    assert (response[0], fromstring(response[2]).findtext("Code")) == (
        (403, "AccessDenied") if auth_enabled else (404, "NoSuchBucket")
    )


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_download_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-download-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=b"private")
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}).client("s3")
    try:
        response = _download(bucket, client, virtual=virtual)
        assert (response[0], fromstring(response[2]).findtext("Code")) == (403, "AccessDenied")
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()


# ---------------------------------------------------------------------------
# HeadObject and HeadBucket error order, as observed on AWS (Osaka and Tokyo, 2026-10-23), replayed locally.
# ---------------------------------------------------------------------------


_HEAD_PAYLOAD = b"head error validation"


@pytest.mark.auth_contract
@pytest.mark.parametrize("source", ["region", "authentication", "normal", "iam", "missing-request", "missing-host"])
def test_head_finalizer_preserves_valid_identifiers(source):
    """Pure formatting contract: no AUTH/IAM patch, no live AWS request."""
    from ministack.core.iam_actions import access_denied_response
    from ministack.services import s3

    host_id = base64.b64encode(b"existing-host-id" * 4).decode()
    if source in {"region", "authentication", "missing-host"}:
        code, status = ("PermanentRedirect", 301) if source == "region" else ("InvalidAccessKeyId", 403)
        response = s3._object_error(code, "formatting probe", status)
        if source == "missing-host":
            response[1].pop("x-amz-id-2")
    elif source == "iam":
        response = access_denied_response("s3", "s3:GetObject", "arn:aws:iam::000000000000:user/probe", "legacy-id")
    else:
        response = 200, {"Content-Type": "text/plain", "Content-Length": "4", "x-amz-id-2": host_id}, b""
        if source == "normal":
            response[1]["x-amz-request-id"] = "12345678-1234-1234-1234-123456789abc"
    original_headers = dict(response[1])
    result = s3.format_head_response("/id-format-probe/key", {}, response)
    request_id = result[1]["x-amz-request-id"]
    assert re.fullmatch(r"[A-Z0-9]{16}", request_id)
    if re.fullmatch(r"[A-Z0-9]{16}", original_headers.get("x-amz-request-id", "")):
        assert request_id == original_headers["x-amz-request-id"]
    if original_headers.get("x-amz-id-2"):
        assert result[1]["x-amz-id-2"] == original_headers["x-amz-id-2"]
    assert len(base64.b64decode(result[1]["x-amz-id-2"], validate=True)) >= 48
    assert result[2] == b""
    assert s3.format_head_response("/id-format-probe/key", {}, result) == result
    assert response[1] == original_headers


@pytest.fixture
def head_bucket(s3):
    name = f"head-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=_HEAD_PAYLOAD, ContentType="text/plain")
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def head_client(head_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{head_bucket}/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{head_bucket}"},
    ]}).client("s3", region=HOME)


def _head(bucket, client, *, key="probe.txt", virtual=False, credential="valid",
          endpoint_region=HOME, region=HOME, root=False, query="", extra_headers=None, presigned=False):
    frozen = client._request_signer._credentials.get_frozen_credentials()
    access_key, secret = frozen.access_key, frozen.secret_key
    if credential == "unknown":
        access_key, secret = "AKIAUNKNOWNKEY000001", "wrong"
    elif credential == "wrong-secret":
        secret = "wrong"
    if root:
        access_key, secret = "test", "any-secret"
    target = urlsplit(ENDPOINT)
    host = f"s3.{endpoint_region}.{target.netloc}"
    path = f"/{quote(bucket)}" + (f"/{quote(key, safe='/')}" if key else "")
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="HEAD", url=f"http://{host}{path}{query}", headers=extra_headers)
    if presigned:
        S3SigV4QueryAuth(Credentials(access_key, secret), "s3", region, expires=300).add_auth(request)
        signed_url = urlsplit(request.url)
        path, query = signed_url.path, "?" + signed_url.query
    else:
        S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("HEAD", path + query, headers={**dict(request.headers), "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _assert_wire(response, *, bucket, key, expected_region=None):
    status, headers, body = response
    assert body == b""
    assert headers.get("x-amz-bucket-region") == expected_region
    assert "x-amz-error-code" not in headers
    assert "x-amz-error-message" not in headers
    assert re.fullmatch(r"[A-Z0-9]{16}", headers["x-amz-request-id"])
    assert len(base64.b64decode(headers["x-amz-id-2"], validate=True)) >= 48
    if key and status == 200:
        assert headers["content-length"] == str(len(_HEAD_PAYLOAD))
        assert headers["content-type"] == "text/plain"
    else:
        assert "content-length" not in headers
        assert headers["content-type"] == "application/xml"
    assert headers.get("x-amz-bucket-arn") == (f"arn:aws:s3:::{bucket}" if not key and status == 200 else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""], ids=["HeadObject", "HeadBucket"])
@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations, separately for HeadObject and HeadBucket.
    # Unlike GET, HEAD checks the bucket's home before the signing scope.
    (False, "valid", HOME, HOME, 200, 200),
    (False, "unknown", HOME, HOME, 403, 200),
    (False, "wrong-secret", HOME, HOME, 403, 200),
    (True, "valid", HOME, HOME, 404, 404),
    (True, "unknown", HOME, HOME, 403, 404),
    (True, "wrong-secret", HOME, HOME, 403, 404),
    (False, "valid", HOME, TOKYO, 400, 400),
    (False, "unknown", HOME, TOKYO, 400, 400),
    (False, "wrong-secret", HOME, TOKYO, 400, 400),
    (True, "valid", HOME, TOKYO, 400, 404),
    (True, "unknown", HOME, TOKYO, 400, 404),
    (True, "wrong-secret", HOME, TOKYO, 400, 404),
    # Existing-bucket valid/unknown + Tokyo endpoint/Osaka signature live in
    # test_head_home_precedes_signing_region_and_credentials, including wire checks.
    (False, "wrong-secret", TOKYO, HOME, 301, 200),
    (True, "valid", TOKYO, HOME, 400, 404),
    (True, "unknown", TOKYO, HOME, 400, 404),
    (True, "wrong-secret", TOKYO, HOME, 400, 404),
    (False, "valid", TOKYO, TOKYO, 301, 301),
    (False, "unknown", TOKYO, TOKYO, 301, 301),
    (False, "wrong-secret", TOKYO, TOKYO, 301, 301),
    (True, "valid", TOKYO, TOKYO, 404, 404),
    (True, "unknown", TOKYO, TOKYO, 403, 404),
    (True, "wrong-secret", TOKYO, TOKYO, 403, 404),
])
def test_head_combined_errors(head_bucket, head_client, auth_enabled,
                              key, virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{head_bucket}" if missing else head_bucket
    expected = expected if auth_enabled else compat_expected
    response = _head(bucket, head_client, key=key, virtual=virtual, credential=credential,
                     endpoint_region=endpoint_region, region=region)
    assert response[0] == expected
    assert response[2] == b""
    if auth_enabled:
        _assert_wire(response, bucket=bucket, key=key,
                     expected_region=HOME if not missing and (not key or expected == 301) else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""], ids=["HeadObject", "HeadBucket"])
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_head_home_precedes_signing_region_and_credentials(head_bucket, head_client, s3,
                                                           auth_enabled, key, virtual, credential):
    # Fixed AWS regression: home-region redirect wins over signing/authentication errors.
    response = _head(head_bucket, head_client, key=key, virtual=virtual, credential=credential,
                     endpoint_region=TOKYO, region=HOME)
    assert response[0] == (301 if auth_enabled else 200)
    assert response[2] == b""
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME)
    with s3.get_object(Bucket=head_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == _HEAD_PAYLOAD


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_head_missing_key(head_bucket, head_client, auth_enabled, virtual, credential, region):
    response = _head(head_bucket, head_client, key="missing.txt", virtual=virtual, credential=credential, region=region)
    expected = 400 if region != HOME else 403 if auth_enabled and credential != "valid" else 404
    assert response[0] == expected
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key="missing.txt")


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""])
def test_head_invalid_name_precedes_credentials(head_client, auth_enabled, key):
    response = _head("invalid bucket", head_client, key=key, credential="unknown")
    assert response[0] == (400 if auth_enabled else 404)
    if auth_enabled:
        _assert_wire(response, bucket="invalid bucket", key=key)


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_head_still_enforces_iam(head_bucket, iam_user, auth_enabled, key, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _head(head_bucket, client, key=key, virtual=virtual)
    assert response[0] == (403 if auth_enabled else 200)
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME if not key else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
def test_head_reserved_root_compatibility(head_bucket, s3, auth_enabled, key, virtual):
    response = _head(head_bucket, s3, key=key, virtual=virtual, root=True)
    assert response[0] == 200
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key=key, expected_region=HOME if not key else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("key", ["probe.txt", ""])
@pytest.mark.parametrize("virtual", [False, True])
def test_head_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, key, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-head-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=_HEAD_PAYLOAD)
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}).client("s3")
    try:
        response = _head(bucket, client, key=key, virtual=virtual)
        assert response[0] == 403
        if auth_enabled:
            _assert_wire(response, bucket=bucket, key=key, expected_region=HOME if not key else None)
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_head_version_query_keeps_iam_path(head_bucket, iam_user, auth_enabled, virtual):
    client = iam_user().client("s3", region=HOME)
    response = _head(f"missing-{head_bucket}", client, virtual=virtual, query="?versionId=null")
    assert response[0] == (403 if auth_enabled else 404)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "wrong-secret"])
def test_head_presigned_validation_is_preserved(head_bucket, head_client, s3, auth_enabled, virtual, credential):
    response = _head(head_bucket, head_client, virtual=virtual, credential=credential, presigned=True)
    # With AUTH off a presigned signature is not checked, as for a header-signed request.
    accepted = credential == "valid" or not auth_enabled
    assert response[0] == (200 if accepted else 403)
    assert response[2] == b""
    if accepted:
        assert response[1]["content-length"] == str(len(_HEAD_PAYLOAD))
        assert response[1]["content-type"] == "text/plain"
    with s3.get_object(Bucket=head_bucket, Key="probe.txt")["Body"] as body:
        assert body.read() == _HEAD_PAYLOAD


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_head_conditional_metadata(head_bucket, head_client, auth_enabled, virtual):
    response = _head(head_bucket, head_client, virtual=virtual)
    assert response[0] == 200
    assert response[1]["content-length"] == str(len(_HEAD_PAYLOAD))
    assert response[2] == b""
    etag = response[1]["etag"]
    response = _head(head_bucket, head_client, virtual=virtual, extra_headers={"If-None-Match": etag})
    assert response[0] == 304
    assert response[2] == b""
    if auth_enabled:
        assert "content-length" not in response[1]
        assert "content-type" not in response[1]
    response = _head(head_bucket, head_client, virtual=virtual, extra_headers={"If-Match": '"incorrect"'})
    assert response[0] == 412
    if auth_enabled:
        _assert_wire(response, bucket=head_bucket, key="probe.txt")


# ---------------------------------------------------------------------------
# DeleteObject error order, as observed on AWS (Osaka and Tokyo, 2026-10-23), replayed locally.
# ---------------------------------------------------------------------------


_DELETE_PAYLOAD = b"delete error validation"


@pytest.fixture
def delete_bucket(s3):
    name = f"delete-errors-{uuid.uuid4().hex}"
    s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": HOME})
    s3.put_object(Bucket=name, Key="probe.txt", Body=_DELETE_PAYLOAD)
    yield name
    s3.delete_object(Bucket=name, Key="probe.txt")
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def delete_client(delete_bucket, iam_user):
    return iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"arn:aws:s3:::{delete_bucket}/*"},
    ]}).client("s3", region=HOME)


def _send(host, path, headers):
    target = urlsplit(ENDPOINT)
    connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=10)
    try:
        connection.request("DELETE", path, headers={**headers, "Host": host})
        response = connection.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        connection.close()


def _delete(bucket, client, *, credential="valid", endpoint_region=HOME, region=HOME,
            virtual=False, key="probe.txt", query="", root=False, extra_headers=None):
    frozen = client._request_signer._credentials.get_frozen_credentials()
    access_key, secret = frozen.access_key, frozen.secret_key
    if credential == "unknown":
        access_key, secret = "AKIAUNKNOWNKEY000001", "wrong"
    elif credential == "wrong-secret":
        secret = "wrong"
    if root:
        access_key, secret = "test", "any-secret"
    host = f"s3.{endpoint_region}.{urlsplit(ENDPOINT).netloc}"
    path = f"/{quote(bucket)}/{quote(key, safe='/')}"
    if virtual:
        host, path = f"{bucket}.{host}", f"/{quote(key, safe='/')}"
    request = AWSRequest(method="DELETE", url=f"http://{host}{path}{query}", headers=extra_headers)
    S3SigV4Auth(Credentials(access_key, secret), "s3", region).add_auth(request)
    return _send(host, path + query, dict(request.headers))


def _assert_state(client, bucket, *, deleted):
    if deleted:
        with pytest.raises(ClientError) as exc:
            client.get_object(Bucket=bucket, Key="probe.txt")
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"
    else:
        with client.get_object(Bucket=bucket, Key="probe.txt")["Body"] as body:
            assert body.read() == _DELETE_PAYLOAD


def _assert_delete_error(response, code, *, bucket, region=HOME, endpoint_region=HOME, access_key=None):
    _, headers, body = response
    error = fromstring(body)
    messages = {
        "InvalidBucketName": "The specified bucket is not valid.",
        "InvalidAccessKeyId": "The AWS Access Key Id you provided does not exist in our records.",
        "SignatureDoesNotMatch": "The request signature we calculated does not match the signature you provided. "
                                 "Check your key and signing method.",
        "AuthorizationHeaderMalformed": f"The authorization header is malformed; the region '{region}' is wrong; "
                                        f"expecting '{endpoint_region}'",
        "PermanentRedirect": "The bucket you are attempting to access must be addressed using the specified "
                             "endpoint. Please send all future requests to this endpoint.",
        "NoSuchBucket": "The specified bucket does not exist",
    }
    details = {
        "InvalidBucketName": ["BucketName"], "InvalidAccessKeyId": ["AWSAccessKeyId"],
        "AuthorizationHeaderMalformed": ["Region"], "PermanentRedirect": ["Endpoint", "Bucket"],
        "NoSuchBucket": ["BucketName"],
        "SignatureDoesNotMatch": ["AWSAccessKeyId", "StringToSign", "SignatureProvided", "StringToSignBytes",
                                  "CanonicalRequest", "CanonicalRequestBytes"],
    }
    expected = ["Code", "Message", *details[code], "RequestId", "HostId"]
    assert error.findtext("Code") == code
    assert error.findtext("Message") == messages[code]
    if code == "SignatureDoesNotMatch":
        # Diagnostic membership and consistency matter even if AWS varies field order.
        assert sorted(child.tag for child in error) == sorted(expected)
        for name in ("CanonicalRequest", "StringToSign"):
            assert bytes.fromhex(error.findtext(name + "Bytes")).decode() == error.findtext(name)
        assert error.findtext("CanonicalRequest").startswith("DELETE\n")
        document = error.findtext("StringToSign")
        assert document.splitlines()[-1] == hashlib.sha256(error.findtext("CanonicalRequest").encode()).hexdigest()
        signed = AWSRequest()
        signed.context["timestamp"] = document.splitlines()[1]
        signer = S3SigV4Auth(Credentials(error.findtext("AWSAccessKeyId"), "wrong"), "s3", region)
        assert signer.signature(document, signed) == error.findtext("SignatureProvided")
    else:
        assert [child.tag for child in error] == expected
    assert headers["content-type"] == "application/xml"
    assert error.findtext("RequestId") == headers["x-amz-request-id"]
    assert re.fullmatch(r"[A-Z0-9]{16}", error.findtext("RequestId"))
    assert error.findtext("HostId") == headers["x-amz-id-2"]
    assert len(base64.b64decode(error.findtext("HostId"), validate=True)) >= 48
    assert "location" not in headers
    if code == "PermanentRedirect":
        assert error.findtext("Endpoint") == f"{bucket}.s3.{HOME}.{urlsplit(ENDPOINT).netloc}"
        assert error.findtext("Bucket") == bucket
        assert headers["x-amz-bucket-region"] == HOME
    else:
        assert "x-amz-bucket-region" not in headers
    if code in {"InvalidBucketName", "NoSuchBucket"}:
        assert error.findtext("BucketName") == bucket
    if code == "AuthorizationHeaderMalformed":
        assert error.findtext("Region") == endpoint_region
    if access_key is not None:
        assert error.findtext("AWSAccessKeyId") == access_key


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True], ids=["path", "virtual"])
@pytest.mark.parametrize("missing,credential,endpoint_region,region,expected,compat_expected", [
    # Fixed first-response observations; DELETE success must also remove the object.
    (False, "valid", HOME, HOME, (204, None), (204, None)),
    (False, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (204, None)),
    (False, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (204, None)),
    (True, "valid", HOME, HOME, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, HOME, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, HOME, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
    (False, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (False, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (400, "AuthorizationHeaderMalformed")),
    (True, "valid", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", HOME, TOKYO, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    # Existing-bucket valid/unknown + Tokyo endpoint/Osaka signature live in
    # test_delete_home_precedes_signing_region_and_credentials, including state checks.
    (False, "wrong-secret", TOKYO, HOME, (301, "PermanentRedirect"), (204, None)),
    (True, "valid", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, HOME, (400, "AuthorizationHeaderMalformed"), (404, "NoSuchBucket")),
    (False, "valid", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "unknown", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (False, "wrong-secret", TOKYO, TOKYO, (301, "PermanentRedirect"), (301, "PermanentRedirect")),
    (True, "valid", TOKYO, TOKYO, (404, "NoSuchBucket"), (404, "NoSuchBucket")),
    (True, "unknown", TOKYO, TOKYO, (403, "InvalidAccessKeyId"), (404, "NoSuchBucket")),
    (True, "wrong-secret", TOKYO, TOKYO, (403, "SignatureDoesNotMatch"), (404, "NoSuchBucket")),
])
def test_delete_combined_errors(delete_bucket, delete_client, s3, auth_enabled,
                                virtual, missing, credential, endpoint_region, region, expected, compat_expected):
    bucket = f"missing-{delete_bucket}" if missing else delete_bucket
    expected = expected if auth_enabled else compat_expected
    response = _delete(bucket, delete_client, virtual=virtual, credential=credential,
                       endpoint_region=endpoint_region, region=region)
    assert (response[0], fromstring(response[2]).findtext("Code") if response[2] else None) == expected
    _assert_state(s3, delete_bucket, deleted=not missing and response[0] == 204)
    if response[0] == 204:
        assert response[2] == b""
    elif auth_enabled:
        access_key = "AKIAUNKNOWNKEY000001" if credential == "unknown" else delete_client._request_signer._credentials.access_key
        _assert_delete_error(response, expected[1], bucket=bucket, region=region, endpoint_region=endpoint_region,
                      access_key=access_key if expected[1] in {"InvalidAccessKeyId", "SignatureDoesNotMatch"} else None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential", ["valid", "unknown"])
def test_delete_home_precedes_signing_region_and_credentials(delete_bucket, delete_client, s3,
                                                             auth_enabled, virtual, credential):
    # Fixed AWS observation: redirect, including with an unknown access key.
    response = _delete(delete_bucket, delete_client, virtual=virtual, credential=credential,
                       endpoint_region=TOKYO, region=HOME)
    assert response[0] == (301 if auth_enabled else 204)
    if auth_enabled:
        _assert_delete_error(response, "PermanentRedirect", bucket=delete_bucket, endpoint_region=TOKYO)
    else:
        assert response[2] == b""
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("credential,region", [("valid", HOME), ("unknown", HOME), ("wrong-secret", HOME), ("valid", TOKYO)])
def test_delete_missing_key_is_idempotent(delete_bucket, delete_client, s3, auth_enabled, virtual, credential, region):
    response = _delete(delete_bucket, delete_client, virtual=virtual, credential=credential, region=region, key="missing.txt")
    code = ("AuthorizationHeaderMalformed" if region != HOME else
            "InvalidAccessKeyId" if auth_enabled and credential == "unknown" else
            "SignatureDoesNotMatch" if auth_enabled and credential == "wrong-secret" else None)
    assert (response[0], fromstring(response[2]).findtext("Code") if response[2] else None) == (
        400 if region != HOME else 403 if code else 204, code)
    if code and auth_enabled:
        _assert_delete_error(response, code, bucket=delete_bucket, region=region)
    if not code:
        assert response[2] == b""
        assert _delete(delete_bucket, delete_client, virtual=virtual, key="missing.txt")[0] == 204
    _assert_state(s3, delete_bucket, deleted=False)


@pytest.mark.auth_contract
def test_delete_invalid_name_precedes_credentials(delete_client, auth_enabled):
    response = _delete("invalid bucket", delete_client, credential="unknown")
    assert response[0] == (400 if auth_enabled else 404)
    if auth_enabled:
        _assert_delete_error(response, "InvalidBucketName", bucket="invalid bucket")


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("denial", ["implicit", "explicit"])
def test_delete_still_enforces_iam(delete_bucket, iam_user, s3, auth_enabled, virtual, denial):
    policy = {"Statement": [{"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]}
    client = iam_user(policy=policy if denial == "explicit" else None).client("s3", region=HOME)
    response = _delete(delete_bucket, client, virtual=virtual)
    assert response[0] == (403 if auth_enabled else 204)
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)
    assert _delete(delete_bucket, client, virtual=virtual, key="missing.txt")[0] == (403 if auth_enabled else 204)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_delete_reserved_root_compatibility(delete_bucket, s3, virtual):
    assert _delete(delete_bucket, s3, virtual=virtual, root=True)[0] == 204
    _assert_state(s3, delete_bucket, deleted=True)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_delete_does_not_grant_foreign_bucket_access(iam_user, auth_enabled, virtual):
    owner = boto3.client("s3", endpoint_url=ENDPOINT, region_name=HOME,
                         aws_access_key_id="333333333333", aws_secret_access_key="root-secret")
    bucket = f"foreign-delete-{uuid.uuid4().hex}"
    owner.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": HOME})
    owner.put_object(Bucket=bucket, Key="probe.txt", Body=_DELETE_PAYLOAD)
    client = iam_user(policy={"Statement": [{"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "*"}]}).client("s3")
    try:
        assert _delete(bucket, client, virtual=virtual)[0] == 403
        _assert_state(owner, bucket, deleted=False)
    finally:
        owner.delete_object(Bucket=bucket, Key="probe.txt")
        owner.delete_bucket(Bucket=bucket)
        owner.close()


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("query", ["?versionId=null", "?tagging"])
def test_delete_subresources_keep_their_iam_path(delete_bucket, iam_user, auth_enabled, virtual, query):
    client = iam_user().client("s3", region=HOME)
    response = _delete(f"missing-{delete_bucket}", client, virtual=virtual, query=query)
    assert response[0] == (403 if auth_enabled else 404)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_delete_version_needs_its_own_permission(delete_bucket, delete_client, s3, auth_enabled, virtual):
    response = _delete(delete_bucket, delete_client, virtual=virtual, query="?versionId=null")
    assert response[0] == (403 if auth_enabled else 204)
    _assert_state(s3, delete_bucket, deleted=not auth_enabled)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
def test_delete_failed_precondition_preserves_object(delete_bucket, delete_client, s3, virtual):
    response = _delete(delete_bucket, delete_client, virtual=virtual, extra_headers={"If-Match": '"incorrect"'})
    assert (response[0], fromstring(response[2]).findtext("Code")) == (412, "PreconditionFailed")
    _assert_state(s3, delete_bucket, deleted=False)


@pytest.mark.auth_contract
@pytest.mark.parametrize("virtual", [False, True])
@pytest.mark.parametrize("invalid", [False, True], ids=["valid", "bad-signature"])
def test_delete_presigned_validation_is_preserved(delete_bucket, iam_user, s3, auth_enabled, virtual, invalid):
    client = iam_user(policy={"Statement": [
        {"Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"arn:aws:s3:::{delete_bucket}/*"},
    ]}).client("s3", region=HOME, config=Config(signature_version="s3v4", s3={"addressing_style": "virtual" if virtual else "path"}))
    target = urlsplit(client.generate_presigned_url("delete_object", Params={"Bucket": delete_bucket, "Key": "probe.txt"}))
    query = dict(parse_qsl(target.query))
    if invalid:
        signature = query["X-Amz-Signature"]
        query["X-Amz-Signature"] = ("0" if signature[0] != "0" else "1") + signature[1:]
    response = _send(target.netloc, target.path + "?" + urlencode(query), {})
    # With AUTH off a presigned signature is not checked, as for a header-signed request.
    rejected = invalid and auth_enabled
    assert response[0] == (403 if rejected else 204)
    _assert_state(s3, delete_bucket, deleted=not rejected)


def test_s3_missing_key_shapes_do_not_depend_on_auth(download_bucket, head_bucket, s3):
    """The NoSuchKey body and the HEAD error shape are AWS's whether or not MiniStack checks auth."""
    status, _headers, body = _download(download_bucket, s3, key="no-such-key")
    error = fromstring(body)
    assert (status, error.findtext("Code"), error.findtext("Key")) == (404, "NoSuchKey", "no-such-key")
    assert error.find("Resource") is None
    status, headers, body = _head(head_bucket, s3, key="no-such-key")
    assert (status, body) == (404, b"")
    assert "content-length" not in headers
    status, headers, _body = _head(head_bucket, s3, key=None)
    assert status == 200
    assert (headers["x-amz-bucket-region"], headers["x-amz-bucket-arn"]) == (HOME, f"arn:aws:s3:::{head_bucket}")


def test_s3_persisted_keys_a_path_cannot_hold_survive_a_restart(tmp_path, monkeypatch):
    """Keys a file layout can't hold (a folder marker, a key under another key, doubled or dot
    segments, a sidecar-like name) are saved, read back and reloaded as written."""
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    keys = ["dir/", "dir/child.txt", "dir/deeper/x.txt", "a", "a/b", "x//y", "p/./q", "../up", "n.meta.json",
            "c/d/e", "c/d", "k" * 300]
    for key in keys:
        s3mod._persist_object("keys-bucket", key, {"body": key.encode(), "etag": '"e"', "size": len(key.encode()),
                                                    "last_modified": s3mod.now_iso()})
    for key in keys:
        assert s3mod._read_body("keys-bucket", key, {"body": None}) == key.encode(), key
    # Athena and Glue read ordinary keys at their own path.
    assert open(tmp_path / "000000000000" / "keys-bucket" / "dir" / "child.txt", "rb").read() == b"dir/child.txt"

    s3mod._buckets._data.pop(("000000000000", "keys-bucket"), None)
    s3mod._load_persisted_bucket("000000000000", "keys-bucket", str(tmp_path / "000000000000" / "keys-bucket"))
    restored = s3mod._buckets._data[("000000000000", "keys-bucket")]["objects"]
    assert sorted(restored) == sorted(keys)
    assert all(restored[key]["size"] == len(key.encode()) for key in keys)

    # Once the key above it is gone, a rewrite moves the key back to its own path.
    s3mod._delete_persisted_object("keys-bucket", "a/b")
    s3mod._delete_persisted_object("keys-bucket", "a")
    s3mod._persist_object("keys-bucket", "a/b", {"body": b"moved", "etag": '"e"', "size": 5,
                                                 "last_modified": s3mod.now_iso()})
    assert open(tmp_path / "000000000000" / "keys-bucket" / "a" / "b", "rb").read() == b"moved"
    s3mod._buckets._data.pop(("000000000000", "keys-bucket"), None)
