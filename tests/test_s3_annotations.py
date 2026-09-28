"""S3 object annotations: PutObjectAnnotation, GetObjectAnnotation,
ListObjectAnnotations and DeleteObjectAnnotation, and how annotations follow
object versions, CopyObject, deletes, replication, encryption, Object Lock,
restarts and event notifications.

Evidence: the S3 API Reference pages for the four operations, the S3 User
Guide's "Annotating your objects", "Event notification types and
destinations" and "Event message structure" pages, and the botocore s3 model.
Not validated against a real AWS account."""

import base64
import hashlib
import json
import os
import struct
import time
import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import make_client


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


def _crc64nvme_bitwise(data: bytes) -> int:
    """Bit-at-a-time CRC-64/NVME, independent of the server's table."""
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x9A6C9329AC4BC9B5 if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFFFFFFFFFF


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


def _bypassing(operation):
    raw = make_client("s3")

    def bypass(request, **_):
        request.headers["x-amz-bypass-governance-retention"] = "true"

    raw.meta.events.register(f"before-sign.s3.{operation}", bypass)
    return raw


def _refused(call, message):
    with pytest.raises(ClientError) as exc:
        call()
    assert _code(exc) == "AccessDenied"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    assert exc.value.response["Error"]["Message"] == message


def _locked(s3, **lock):
    bucket = _bucket(s3, ObjectLockEnabledForBucket=True)
    s3.put_object(Bucket=bucket, Key="k", Body=b"x", **lock)
    return bucket


def _until():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 3600))


RETENTION = "Access Denied because object protected by object lock retention."
LEGAL_HOLD = "Access Denied because object protected by object lock legal hold."


def test_governance_retention_needs_the_bypass_to_put_or_delete_an_annotation(s3):
    bucket = _locked(s3, ObjectLockMode="GOVERNANCE", ObjectLockRetainUntilDate=_until())
    _refused(lambda: s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1"), RETENTION)
    _bypassing("PutObjectAnnotation").put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")
    _refused(lambda: s3.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"2"), RETENTION)
    _refused(lambda: s3.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a"), RETENTION)
    _bypassing("DeleteObjectAnnotation").delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")


def test_compliance_retention_refuses_annotation_writes_even_with_the_bypass(s3):
    bucket = _locked(s3, ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=_until())
    def put(c):
        return c.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")

    def delete(c):
        return c.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")

    _refused(lambda: put(s3), RETENTION)
    _refused(lambda: put(_bypassing("PutObjectAnnotation")), RETENTION)
    _refused(lambda: delete(s3), RETENTION)
    _refused(lambda: delete(_bypassing("DeleteObjectAnnotation")), RETENTION)


def test_a_legal_hold_refuses_annotation_writes_even_with_the_bypass(s3):
    bucket = _locked(s3, ObjectLockLegalHoldStatus="ON")
    def put(c):
        return c.put_object_annotation(Bucket=bucket, Key="k", AnnotationName="a", AnnotationPayload=b"1")

    def delete(c):
        return c.delete_object_annotation(Bucket=bucket, Key="k", AnnotationName="a")

    _refused(lambda: put(s3), LEGAL_HOLD)
    _refused(lambda: put(_bypassing("PutObjectAnnotation")), LEGAL_HOLD)
    _refused(lambda: delete(s3), LEGAL_HOLD)
    _refused(lambda: delete(_bypassing("DeleteObjectAnnotation")), LEGAL_HOLD)


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


@pytest.fixture
def s3_persist(tmp_path, monkeypatch):
    """S3 in this process with persistence on, over a throwaway DATA_DIR."""
    from ministack.core import responses as respmod
    from ministack.services import s3 as s3mod

    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(s3mod, "get_account_id", lambda: "000000000000")
    monkeypatch.setattr(respmod, "get_account_id", lambda: "000000000000")
    stores = (s3mod._buckets._data, s3mod._object_versions._data, s3mod._bucket_versioning._data, s3mod._object_annotations._data)
    before = [set(store) for store in stores]
    yield s3mod
    for store, keys in zip(stores, before):
        for key in set(store) - keys:
            store.pop(key, None)


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
