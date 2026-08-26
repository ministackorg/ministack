"""
Integration tests for the AWS Signer emulator (synchronous, synthetic signing).

The contract under test is the S3 side effect: StartSigningJob validates the
source object against the local S3 store and synchronously writes a JSON
signature marker to `destination.prefix + jobId` — production callers read
that key and never poll DescribeSigningJob.
"""
import hashlib
import json
import re
import time
import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture()
def buckets(s3):
    """A fresh (source, destination) bucket pair, both unversioned."""
    src = f"signer-src-{_uid()}"
    dst = f"signer-dst-{_uid()}"
    s3.create_bucket(Bucket=src)
    s3.create_bucket(Bucket=dst)
    return src, dst


def _start(signer, src, dst, *, key, profile=None, prefix="signed/", version="null"):
    return signer.start_signing_job(
        source={"s3": {"bucketName": src, "key": key, "version": version}},
        destination={"s3": {"bucketName": dst, "prefix": prefix}},
        profileName=profile or f"profile-{_uid()}",
    )


# ---------------------------------------------------------------------------
# StartSigningJob — the S3 side effect
# ---------------------------------------------------------------------------

def test_signer_start_writes_marker_at_prefix_plus_job_id(signer, s3, buckets):
    src, dst = buckets
    payload = b"firmware-image-bytes " + uuid.uuid4().bytes
    s3.put_object(Bucket=src, Key="fw/image.bin", Body=payload)

    resp = _start(signer, src, dst, key="fw/image.bin", profile="fw-profile")
    job_id = resp["jobId"]
    assert job_id
    assert re.fullmatch(r"\d{12}", resp["jobOwner"])

    # Cross-API: the marker object must be readable through the S3 API at
    # exactly destination.prefix + jobId.
    obj = s3.get_object(Bucket=dst, Key=f"signed/{job_id}")
    marker = json.loads(obj["Body"].read())
    assert marker["jobId"] == job_id
    assert marker["profileName"] == "fw-profile"
    assert marker["source"]["bucketName"] == src
    assert marker["source"]["key"] == "fw/image.bin"
    assert marker["sourceSha256"] == hashlib.sha256(payload).hexdigest()


def test_signer_empty_prefix_lands_marker_at_job_id(signer, s3, buckets):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="cfg.json", Body=b"{}")

    resp = _start(signer, src, dst, key="cfg.json", prefix="")
    job_id = resp["jobId"]
    s3.head_object(Bucket=dst, Key=job_id)  # raises if absent


def test_signer_unversioned_source_with_version_omitted(s3, buckets):
    """A caller on an unversioned bucket may send NO `version` at all (the
    model requires it, so boto3's client-side validation has to be off — the
    wire request simply lacks the member, like a non-Python SDK's optional
    struct field)."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="plain.bin", Body=b"unversioned-bytes")
    raw_signer = make_client("signer", {"parameter_validation": False})

    resp = raw_signer.start_signing_job(
        source={"s3": {"bucketName": src, "key": "plain.bin"}},
        destination={"s3": {"bucketName": dst, "prefix": "out/"}},
        profileName=f"profile-{_uid()}",
    )
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"out/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"unversioned-bytes").hexdigest()


def test_signer_versioned_source_signs_the_named_version(signer, s3, buckets):
    src, dst = buckets
    s3.put_bucket_versioning(
        Bucket=src, VersioningConfiguration={"Status": "Enabled"}
    )
    v1 = s3.put_object(Bucket=src, Key="fw.bin", Body=b"version-one")["VersionId"]
    s3.put_object(Bucket=src, Key="fw.bin", Body=b"version-two")

    resp = _start(signer, src, dst, key="fw.bin", version=v1)
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"signed/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"version-one").hexdigest()


def test_signer_null_version_reads_the_literal_null_version(signer, s3, buckets):
    """`version="null"` must read the stored literal-"null" version, not the
    current object. A pre-versioning write stays addressable as VersionId
    "null" once versioning is enabled on top of it, so on such a bucket the
    two hold DIFFERENT bytes — remapping "null" to "current" signs the wrong
    ones."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="fw.bin", Body=b"pre-versioning-bytes")
    s3.put_bucket_versioning(
        Bucket=src, VersioningConfiguration={"Status": "Enabled"}
    )
    s3.put_object(Bucket=src, Key="fw.bin", Body=b"current-bytes")
    # Sanity: S3 itself serves the two differently.
    null_body = s3.get_object(Bucket=src, Key="fw.bin", VersionId="null")
    assert null_body["Body"].read() == b"pre-versioning-bytes"

    resp = _start(signer, src, dst, key="fw.bin", version="null")
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"signed/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"pre-versioning-bytes").hexdigest()
    assert marker["source"]["version"] == "null"


def test_signer_marker_write_fires_s3_object_created_notification(
    signer, s3, sqs, buckets
):
    """The marker write goes through the same S3 write path as a caller's own
    PutObject, so a bucket notification on the destination must fire for the
    signed-object key."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="notify.bin", Body=b"notify")
    queue_url = sqs.create_queue(QueueName=f"signer-evt-{_uid()}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    s3.put_bucket_notification_configuration(
        Bucket=dst,
        NotificationConfiguration={
            "QueueConfigurations": [
                {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}
            ],
        },
    )

    job_id = _start(signer, src, dst, key="notify.bin")["jobId"]

    time.sleep(0.5)
    msgs = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
    )
    bodies = [json.loads(m["Body"]) for m in msgs.get("Messages", [])]
    keys = [
        r["s3"]["object"]["key"]
        for b in bodies if "Records" in b
        for r in b["Records"] if r.get("eventSource") == "aws:s3"
    ]
    assert f"signed/{job_id}" in keys


def test_signer_missing_source_404_and_no_job_recorded(signer, buckets):
    src, dst = buckets
    missing_key = f"never-uploaded-{_uid()}.bin"
    with pytest.raises(ClientError) as exc:
        _start(signer, src, dst, key=missing_key)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # Synchronous divergence: the failed Start records nothing, so no job in
    # any status references the missing source.
    jobs = signer.list_signing_jobs()["jobs"]
    assert not any(
        j["source"]["s3"]["key"] == missing_key for j in jobs if "source" in j
    )


def test_signer_missing_destination_bucket_404_and_no_job_recorded(signer, s3, buckets):
    src, _dst = buckets
    key = f"orphan-{_uid()}.bin"
    s3.put_object(Bucket=src, Key=key, Body=b"data")
    with pytest.raises(ClientError) as exc:
        _start(signer, src, f"no-such-bucket-{_uid()}", key=key)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    jobs = signer.list_signing_jobs()["jobs"]
    assert not any(
        j["source"]["s3"]["key"] == key for j in jobs if "source" in j
    )


# ---------------------------------------------------------------------------
# DescribeSigningJob / ListSigningJobs
# ---------------------------------------------------------------------------

def test_signer_describe_job_succeeded_with_signed_object(signer, s3, buckets):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="app.bin", Body=b"app")
    resp = _start(signer, src, dst, key="app.bin", profile="describe-profile")
    job_id = resp["jobId"]

    job = signer.describe_signing_job(jobId=job_id)
    assert job["jobId"] == job_id
    assert job["status"] == "Succeeded"
    # Live-measured Describe wording on real AWS for a succeeded job.
    assert job["statusReason"] == "Signing Succeeded"
    assert job["profileName"] == "describe-profile"
    assert job["signedObject"]["s3"] == {
        "bucketName": dst, "key": f"signed/{job_id}",
    }
    assert job["source"]["s3"]["bucketName"] == src
    # Timestamp shapes (epoch seconds on the wire) — boto3 parses datetimes.
    assert job["completedAt"] >= job["createdAt"]
    assert job["jobOwner"] == resp["jobOwner"]
    assert job["requestedBy"]


def test_signer_describe_unknown_job_404(signer):
    with pytest.raises(ClientError) as exc:
        signer.describe_signing_job(jobId=str(uuid.uuid4()))
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_signer_list_jobs_filters_by_status(signer, s3, buckets):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="list.bin", Body=b"list")
    job_id = _start(signer, src, dst, key="list.bin")["jobId"]

    succeeded = signer.list_signing_jobs(status="Succeeded")["jobs"]
    assert any(j["jobId"] == job_id for j in succeeded)
    assert all(j["status"] == "Succeeded" for j in succeeded)

    # Nothing ever fails asynchronously here, so the Failed page is empty.
    failed = signer.list_signing_jobs(status="Failed")["jobs"]
    assert not any(j["jobId"] == job_id for j in failed)


def test_signer_list_jobs_malformed_max_results_is_validation_error(signer):
    """A non-numeric maxResults is a 400 ValidationException, not a 500."""
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.list_signing_jobs(maxResults="not-a-number")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# Signing profiles
# ---------------------------------------------------------------------------

def test_signer_profile_put_get_round_trip(signer):
    name = f"explicit-profile-{_uid()}"
    put = signer.put_signing_profile(
        profileName=name, platformId="AWSLambda-SHA384-ECDSA"
    )
    assert put["arn"].endswith(f":/signing-profiles/{name}")
    assert put["profileVersionArn"] == f"{put['arn']}/{put['profileVersion']}"

    got = signer.get_signing_profile(profileName=name)
    assert got["profileName"] == name
    assert got["platformId"] == "AWSLambda-SHA384-ECDSA"
    assert got["profileVersion"] == put["profileVersion"]
    assert got["status"] == "Active"


def test_signer_get_unknown_profile_404(signer):
    with pytest.raises(ClientError) as exc:
        signer.get_signing_profile(profileName=f"ghost-{_uid()}")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_signer_put_profile_requires_platform_id(signer):
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.put_signing_profile(profileName=f"no-platform-{_uid()}")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_start_with_unknown_profile_auto_registers_it(signer, s3, buckets):
    """Deliberate divergence: an unknown profileName is accepted (real AWS
    rejects it) and an implicit minimal profile appears in the store."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="auto.bin", Body=b"auto")
    name = f"implicit-{_uid()}"

    job_id = _start(signer, src, dst, key="auto.bin", profile=name)["jobId"]
    assert job_id

    profile = signer.get_signing_profile(profileName=name)
    assert profile["profileName"] == name
    assert profile["platformId"]  # the implicit default platform
    assert signer.describe_signing_job(jobId=job_id)["profileVersion"] == (
        profile["profileVersion"]
    )
