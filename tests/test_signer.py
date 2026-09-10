# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Integration tests for the AWS Signer emulator (synchronous, synthetic signing).

The contract under test is the S3 side effect: StartSigningJob validates the
source object against the local S3 store and synchronously writes a JSON
signature marker to `destination.prefix + jobId`, so a caller whose contract
is the signed object at `prefix + jobId` never has to poll
DescribeSigningJob.
"""
import hashlib
import json
import re
import urllib.error
import urllib.request
import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import ENDPOINT, make_client

_IOT_PLATFORM = "AWSIoTDeviceManagement-SHA256-ECDSA"
_LAMBDA_PLATFORM = "AWSLambda-SHA384-ECDSA"

_SIGNER_AUTH = (
    "AWS4-HMAC-SHA256 "
    "Credential=test/20260811/us-east-1/signer/aws4_request, "
    "SignedHeaders=host, Signature=fake"
)


def _uid():
    return uuid.uuid4().hex[:8]


def _raw(method, path, payload):
    """Raw HTTP with the signer credential scope; returns (status, body)."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=json.dumps(payload).encode(),
        method=method,
        headers={"Authorization": _SIGNER_AUTH, "content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@pytest.fixture()
def buckets(s3):
    """A fresh (source, destination) bucket pair, both unversioned."""
    src = f"signer-src-{_uid()}"
    dst = f"signer-dst-{_uid()}"
    s3.create_bucket(Bucket=src)
    s3.create_bucket(Bucket=dst)
    return src, dst


@pytest.fixture()
def profile(signer):
    """A signing profile on the IoT platform (the one whose signed-object
    key is exactly prefix + jobId)."""
    name = f"profile_{_uid()}"
    signer.put_signing_profile(profileName=name, platformId=_IOT_PLATFORM)
    return name


def _start(signer, src, dst, *, key, profile, prefix="signed/", version="null"):
    return signer.start_signing_job(
        source={"s3": {"bucketName": src, "key": key, "version": version}},
        destination={"s3": {"bucketName": dst, "prefix": prefix}},
        profileName=profile,
    )


def _keys(s3, bucket):
    return [o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])]


# ---------------------------------------------------------------------------
# StartSigningJob — the S3 side effect
# ---------------------------------------------------------------------------

def test_signer_start_writes_marker_at_prefix_plus_job_id(signer, s3, buckets, profile):
    src, dst = buckets
    payload = b"firmware-image-bytes " + uuid.uuid4().bytes
    s3.put_object(Bucket=src, Key="fw/image.bin", Body=payload)

    resp = _start(signer, src, dst, key="fw/image.bin", profile=profile)
    job_id = resp["jobId"]
    assert job_id
    assert re.fullmatch(r"\d{12}", resp["jobOwner"])

    # Cross-API: the marker object must be readable through the S3 API at
    # exactly destination.prefix + jobId.
    obj = s3.get_object(Bucket=dst, Key=f"signed/{job_id}")
    marker = json.loads(obj["Body"].read())
    assert marker["jobId"] == job_id
    assert marker["profileName"] == profile
    assert marker["source"]["bucketName"] == src
    assert marker["source"]["key"] == "fw/image.bin"
    assert marker["sourceSha256"] == hashlib.sha256(payload).hexdigest()


def test_signer_empty_prefix_lands_marker_at_job_id(signer, s3, buckets, profile):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="cfg.json", Body=b"{}")

    resp = _start(signer, src, dst, key="cfg.json", profile=profile, prefix="")
    s3.head_object(Bucket=dst, Key=resp["jobId"])  # raises if absent


def test_signer_lambda_platform_appends_zip_for_a_zip_source(signer, s3, buckets):
    """Measured on the live service: a `.zip` source signed on
    AWSLambda-SHA384-ECDSA lands at prefix + jobId + ".zip", while the IoT
    platform writes exactly prefix + jobId."""
    src, dst = buckets
    name = f"lambda_{_uid()}"
    signer.put_signing_profile(profileName=name, platformId=_LAMBDA_PLATFORM)
    s3.put_object(Bucket=src, Key="source/src.zip", Body=b"PK-zip-bytes")
    s3.put_object(Bucket=src, Key="source/src.bin", Body=b"bin-bytes")

    zipped = _start(signer, src, dst, key="source/src.zip", profile=name)["jobId"]
    plain = _start(signer, src, dst, key="source/src.bin", profile=name)["jobId"]

    assert f"signed/{zipped}.zip" in _keys(s3, dst)
    assert f"signed/{plain}" in _keys(s3, dst)
    described = signer.describe_signing_job(jobId=zipped)
    assert described["signedObject"]["s3"]["key"] == f"signed/{zipped}.zip"
    assert described["platformId"] == _LAMBDA_PLATFORM


def test_signer_iot_platform_keeps_the_plain_key_for_a_zip_source(signer, s3, buckets, profile):
    """The `.zip` suffix rides on the PLATFORM, not on the source extension:
    the same `.zip` source that gets `prefix + jobId + ".zip"` on
    AWSLambda-SHA384-ECDSA lands at exactly `prefix + jobId` on
    AWSIoTDeviceManagement-SHA256-ECDSA. The measurement (2026-08-26)
    attributes the suffix to the Lambda platform; a `.zip` source on the IoT
    platform was not itself measured live, so this pins the emulator's rule."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="fw/firmware.zip", Body=b"PK-firmware")

    job_id = _start(signer, src, dst, key="fw/firmware.zip", profile=profile)["jobId"]

    assert _keys(s3, dst) == [f"signed/{job_id}"]
    described = signer.describe_signing_job(jobId=job_id)
    assert described["platformId"] == _IOT_PLATFORM
    assert described["signedObject"]["s3"]["key"] == f"signed/{job_id}"


def test_signer_unversioned_source_with_version_omitted(s3, signer, buckets, profile):
    """A caller on an unversioned bucket may send NO `version` at all (the
    model requires it, so boto3's client-side validation has to be off — the
    wire request simply lacks the member, like a non-Python SDK's optional
    struct field). The job then records S3's "null" id so the required
    member is always present on Describe."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="plain.bin", Body=b"unversioned-bytes")
    raw_signer = make_client("signer", {"parameter_validation": False})

    resp = raw_signer.start_signing_job(
        source={"s3": {"bucketName": src, "key": "plain.bin"}},
        destination={"s3": {"bucketName": dst, "prefix": "out/"}},
        profileName=profile,
    )
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"out/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"unversioned-bytes").hexdigest()
    job = signer.describe_signing_job(jobId=resp["jobId"])
    assert job["source"]["s3"]["version"] == "null"


def test_signer_omitted_version_records_the_stored_version_id(s3, signer, buckets, profile):
    """With no `version` on the wire the current object is signed, and the
    job names the version id S3 really holds for it. "null" is only correct
    for a bucket without versioning (S3 user guide: objects stored before
    the versioning state is set have a version ID of null)."""
    src, dst = buckets
    s3.put_bucket_versioning(
        Bucket=src, VersioningConfiguration={"Status": "Enabled"}
    )
    s3.put_object(Bucket=src, Key="fw.bin", Body=b"older-bytes")
    current = s3.put_object(Bucket=src, Key="fw.bin", Body=b"current-bytes")["VersionId"]
    assert current != "null"
    raw_signer = make_client("signer", {"parameter_validation": False})

    resp = raw_signer.start_signing_job(
        source={"s3": {"bucketName": src, "key": "fw.bin"}},
        destination={"s3": {"bucketName": dst, "prefix": "out/"}},
        profileName=profile,
    )

    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"out/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"current-bytes").hexdigest()
    assert marker["source"]["version"] == current
    job = signer.describe_signing_job(jobId=resp["jobId"])
    assert job["source"]["s3"]["version"] == current


def test_signer_versioned_source_signs_the_named_version(signer, s3, buckets, profile):
    src, dst = buckets
    s3.put_bucket_versioning(
        Bucket=src, VersioningConfiguration={"Status": "Enabled"}
    )
    v1 = s3.put_object(Bucket=src, Key="fw.bin", Body=b"version-one")["VersionId"]
    s3.put_object(Bucket=src, Key="fw.bin", Body=b"version-two")

    resp = _start(signer, src, dst, key="fw.bin", profile=profile, version=v1)
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"signed/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"version-one").hexdigest()
    assert signer.describe_signing_job(jobId=resp["jobId"])["source"]["s3"]["version"] == v1


def test_signer_null_version_reads_the_literal_null_version(signer, s3, buckets, profile):
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

    resp = _start(signer, src, dst, key="fw.bin", profile=profile, version="null")
    marker = json.loads(
        s3.get_object(Bucket=dst, Key=f"signed/{resp['jobId']}")["Body"].read()
    )
    assert marker["sourceSha256"] == hashlib.sha256(b"pre-versioning-bytes").hexdigest()
    assert marker["source"]["version"] == "null"


def test_signer_marker_write_fires_s3_object_created_notification(
    signer, s3, sqs, buckets, profile
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

    job_id = _start(signer, src, dst, key="notify.bin", profile=profile)["jobId"]

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


def test_signer_client_request_token_replays_the_first_response(signer, s3, buckets, profile):
    """API reference: "All calls after the first that use this token return
    the same response as the first call." A retry with the same token must
    not write a second marker."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="idem.bin", Body=b"idem")
    token = str(uuid.uuid4())
    kwargs = dict(
        source={"s3": {"bucketName": src, "key": "idem.bin", "version": "null"}},
        destination={"s3": {"bucketName": dst, "prefix": "signed/"}},
        profileName=profile,
        clientRequestToken=token,
    )
    first = signer.start_signing_job(**kwargs)
    second = signer.start_signing_job(**kwargs)
    assert second["jobId"] == first["jobId"]
    assert second["jobOwner"] == first["jobOwner"]
    assert _keys(s3, dst) == [f"signed/{first['jobId']}"]
    assert [j["jobId"] for j in signer.list_signing_jobs()["jobs"]].count(first["jobId"]) == 1


def test_signer_raw_wire_start_without_client_request_token(s3, signer, buckets, profile):
    """The model requires clientRequestToken and SDKs autofill it; a raw
    request without one is still accepted (nothing to replay against)."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="raw.bin", Body=b"raw")
    status, body = _raw("POST", "/signing-jobs", {
        "source": {"s3": {"bucketName": src, "key": "raw.bin", "version": "null"}},
        "destination": {"s3": {"bucketName": dst, "prefix": "raw/"}},
        "profileName": profile,
    })
    assert status == 200, body
    assert re.fullmatch(r"\d{12}", body["jobOwner"])
    s3.head_object(Bucket=dst, Key=f"raw/{body['jobId']}")


@pytest.mark.parametrize("source_s3", [
    {"bucketName": ["not", "a", "string"], "key": "k", "version": "null"},
    {"bucketName": "bkt", "key": {"nested": True}, "version": "null"},
    {"bucketName": "bkt", "key": "k", "version": 7},
])
def test_signer_raw_wire_non_string_source_members_are_refused(source_s3, buckets, profile):
    """A raw body with a non-string bucketName / key / version is a 400,
    not a server error from the S3 lookup."""
    _src, dst = buckets
    status, body = _raw("POST", "/signing-jobs", {
        "source": {"s3": source_s3},
        "destination": {"s3": {"bucketName": dst}},
        "profileName": profile,
        "clientRequestToken": str(uuid.uuid4()),
    })
    assert status == 400
    assert body["__type"] == "ValidationException"


@pytest.mark.parametrize("field, value", [
    ("prefix", ["signed/"]),
    ("clientRequestToken", 12345),
])
def test_signer_raw_wire_non_string_scalars_are_refused(field, value, s3, buckets, profile):
    """A raw body with a non-string destination.s3.prefix or
    clientRequestToken is a 400, not a TypeError deeper in the handler."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="scalar.bin", Body=b"scalar")
    destination = {"bucketName": dst, "prefix": "signed/"}
    payload = {
        "source": {"s3": {"bucketName": src, "key": "scalar.bin", "version": "null"}},
        "destination": {"s3": destination},
        "profileName": profile,
        "clientRequestToken": str(uuid.uuid4()),
    }
    if field == "prefix":
        destination["prefix"] = value
    else:
        payload[field] = value

    status, body = _raw("POST", "/signing-jobs", payload)

    assert status == 400, body
    assert body["__type"] == "ValidationException"
    assert _keys(s3, dst) == []


def test_signer_missing_source_404_and_no_job_recorded(signer, buckets, profile):
    src, dst = buckets
    missing_key = f"never-uploaded-{_uid()}.bin"
    with pytest.raises(ClientError) as exc:
        _start(signer, src, dst, key=missing_key, profile=profile)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # Synchronous divergence: the failed Start records nothing, so no job in
    # any status references the missing source.
    jobs = signer.list_signing_jobs()["jobs"]
    assert not any(
        j["source"]["s3"]["key"] == missing_key for j in jobs if "source" in j
    )


def test_signer_missing_destination_bucket_404_and_no_job_recorded(
    signer, s3, buckets, profile
):
    src, _dst = buckets
    key = f"orphan-{_uid()}.bin"
    s3.put_object(Bucket=src, Key=key, Body=b"data")
    with pytest.raises(ClientError) as exc:
        _start(signer, src, f"no-such-bucket-{_uid()}", key=key, profile=profile)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    jobs = signer.list_signing_jobs()["jobs"]
    assert not any(
        j["source"]["s3"]["key"] == key for j in jobs if "source" in j
    )


def test_signer_start_with_unknown_profile_is_resource_not_found(signer, s3, buckets):
    """Live-measured: an unknown profileName is refused with
    ResourceNotFoundException "profile with name <name> does not exist".
    Nothing is written and no job is recorded."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="auto.bin", Body=b"auto")
    name = f"ghost_{_uid()}"
    with pytest.raises(ClientError) as exc:
        _start(signer, src, dst, key="auto.bin", profile=name)
    error = exc.value.response["Error"]
    assert error["Code"] == "ResourceNotFoundException"
    assert error["Message"] == f"profile with name {name} does not exist"
    assert _keys(s3, dst) == []
    assert not any(j["profileName"] == name for j in signer.list_signing_jobs()["jobs"])


# ---------------------------------------------------------------------------
# DescribeSigningJob / ListSigningJobs
# ---------------------------------------------------------------------------

def test_signer_describe_job_succeeded_with_signed_object(signer, s3, buckets, profile):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="app.bin", Body=b"app")
    resp = _start(signer, src, dst, key="app.bin", profile=profile)
    job_id = resp["jobId"]

    job = signer.describe_signing_job(jobId=job_id)
    assert job["jobId"] == job_id
    assert job["status"] == "Succeeded"
    # Live-measured Describe wording on real AWS for a succeeded job.
    assert job["statusReason"] == "Signing Succeeded"
    assert job["profileName"] == profile
    assert job["platformId"] == _IOT_PLATFORM
    assert job["signedObject"]["s3"] == {
        "bucketName": dst, "key": f"signed/{job_id}",
    }
    assert job["source"]["s3"]["bucketName"] == src
    # Timestamp shapes (epoch seconds on the wire) — boto3 parses datetimes.
    assert job["completedAt"] >= job["createdAt"]
    assert job["jobOwner"] == resp["jobOwner"]
    assert job["requestedBy"]


def test_signer_describe_job_carries_the_profile_material_and_expiry(signer, s3, buckets):
    """signingMaterial, overrides and signingParameters come from the
    profile; signatureExpiresAt is createdAt plus the profile's
    signatureValidityPeriod."""
    src, dst = buckets
    name = f"material_{_uid()}"
    cert = "arn:aws:acm:us-east-1:000000000000:certificate/9ec626ca-0bbb-4be5-83a2-ee563f8386ca"
    signer.put_signing_profile(
        profileName=name,
        platformId=_IOT_PLATFORM,
        signingMaterial={"certificateArn": cert},
        overrides={"signingConfiguration": {"hashAlgorithm": "SHA256"}},
        signingParameters={"build": "42"},
        signatureValidityPeriod={"type": "DAYS", "value": 10},
    )
    s3.put_object(Bucket=src, Key="m.bin", Body=b"m")
    job_id = _start(signer, src, dst, key="m.bin", profile=name)["jobId"]

    job = signer.describe_signing_job(jobId=job_id)
    assert job["signingMaterial"] == {"certificateArn": cert}
    assert job["overrides"] == {"signingConfiguration": {"hashAlgorithm": "SHA256"}}
    assert job["signingParameters"] == {"build": "42"}
    assert (job["signatureExpiresAt"] - job["createdAt"]).days == 10
    listed = next(j for j in signer.list_signing_jobs()["jobs"] if j["jobId"] == job_id)
    assert listed["signingMaterial"] == {"certificateArn": cert}
    assert listed["signatureExpiresAt"] == job["signatureExpiresAt"]


def test_signer_default_signature_validity_is_135_months(signer, s3, buckets, profile):
    """PutSigningProfile reference: "If unspecified, the default is 135
    months." """
    src, dst = buckets
    s3.put_object(Bucket=src, Key="d.bin", Body=b"d")
    job_id = _start(signer, src, dst, key="d.bin", profile=profile)["jobId"]
    job = signer.describe_signing_job(jobId=job_id)
    created, expires = job["createdAt"], job["signatureExpiresAt"]
    months = (expires.year - created.year) * 12 + expires.month - created.month
    assert months == 135


def test_signer_describe_unknown_job_404(signer):
    with pytest.raises(ClientError) as exc:
        signer.describe_signing_job(jobId=str(uuid.uuid4()))
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_signer_list_jobs_filters_by_status(signer, s3, buckets, profile):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="list.bin", Body=b"list")
    job_id = _start(signer, src, dst, key="list.bin", profile=profile)["jobId"]

    succeeded = signer.list_signing_jobs(status="Succeeded")["jobs"]
    assert any(j["jobId"] == job_id for j in succeeded)
    assert all(j["status"] == "Succeeded" for j in succeeded)

    # Nothing ever fails asynchronously here, so the Failed page is empty.
    failed = signer.list_signing_jobs(status="Failed")["jobs"]
    assert not any(j["jobId"] == job_id for j in failed)


def test_signer_list_jobs_status_outside_documented_values_is_refused(signer):
    """Valid Values: InProgress | Failed | Succeeded; botocore does not check
    enums client-side, so a typo reaches the server."""
    with pytest.raises(ClientError) as exc:
        signer.list_signing_jobs(status="Bogus")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_list_jobs_is_revoked_filter(signer, s3, buckets, profile):
    """Nothing is ever revoked here: isRevoked=true is an empty page,
    isRevoked=false lists the job with isRevoked False."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="rev.bin", Body=b"rev")
    job_id = _start(signer, src, dst, key="rev.bin", profile=profile)["jobId"]

    assert signer.list_signing_jobs(isRevoked=True)["jobs"] == []
    not_revoked = signer.list_signing_jobs(isRevoked=False)["jobs"]
    match = next(j for j in not_revoked if j["jobId"] == job_id)
    assert match["isRevoked"] is False


def test_signer_list_jobs_platform_id_filter(signer, s3, buckets, profile):
    src, dst = buckets
    lam = f"lam_{_uid()}"
    signer.put_signing_profile(profileName=lam, platformId=_LAMBDA_PLATFORM)
    s3.put_object(Bucket=src, Key="p.bin", Body=b"p")
    iot_job = _start(signer, src, dst, key="p.bin", profile=profile)["jobId"]
    lam_job = _start(signer, src, dst, key="p.bin", profile=lam)["jobId"]

    lam_ids = [j["jobId"] for j in signer.list_signing_jobs(platformId=_LAMBDA_PLATFORM)["jobs"]]
    assert lam_job in lam_ids and iot_job not in lam_ids
    assert signer.list_signing_jobs(platformId="NoSuchPlatform")["jobs"] == []


def test_signer_list_jobs_requested_by_and_job_invoker_filters(signer, s3, buckets, profile):
    src, dst = buckets
    s3.put_object(Bucket=src, Key="who.bin", Body=b"who")
    job_id = _start(signer, src, dst, key="who.bin", profile=profile)["jobId"]
    job = signer.describe_signing_job(jobId=job_id)

    by_requester = signer.list_signing_jobs(requestedBy=job["requestedBy"])["jobs"]
    assert any(j["jobId"] == job_id for j in by_requester)
    assert signer.list_signing_jobs(requestedBy="arn:aws:iam::000000000000:user/nobody")["jobs"] == []

    by_invoker = signer.list_signing_jobs(jobInvoker=job["jobInvoker"])["jobs"]
    assert any(j["jobId"] == job_id for j in by_invoker)
    assert signer.list_signing_jobs(jobInvoker="999999999999")["jobs"] == []


def test_signer_list_jobs_max_results_pages_with_a_next_token(signer, s3, buckets, profile):
    """maxResults limits the page and the response carries a nextToken while
    jobs remain, so a paginator reaches all of them."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="t.bin", Body=b"t")
    started = {_start(signer, src, dst, key="t.bin", profile=profile)["jobId"]
               for _ in range(3)}

    page = signer.list_signing_jobs(maxResults=1)
    assert len(page["jobs"]) == 1
    assert page.get("nextToken")

    # Walk to the end: the suite shares one store, so the three jobs of this
    # test are somewhere in a longer list.
    seen = [page["jobs"][0]["jobId"]]
    token = page["nextToken"]
    for _ in range(200):
        page = signer.list_signing_jobs(maxResults=1, nextToken=token)
        seen.extend(job["jobId"] for job in page["jobs"])
        token = page.get("nextToken")
        if not token:
            break
    assert not token, "the walk never reached the last page"
    assert started.issubset(set(seen))
    assert len(seen) == len(set(seen)), seen

    # The paginator boto3 builds from the model walks the same list.
    paginated = [job["jobId"] for page in
                 signer.get_paginator("list_signing_jobs").paginate(
                     PaginationConfig={"PageSize": 1})
                 for job in page["jobs"]]
    assert started.issubset(set(paginated))


def test_signer_list_jobs_last_page_has_no_next_token(signer, s3, buckets, profile):
    """A page that exhausts the list carries no token, which is what stops a
    paginator."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="t.bin", Body=b"t")
    _start(signer, src, dst, key="t.bin", profile=profile)

    page = signer.list_signing_jobs(maxResults=25)
    assert page["jobs"]
    assert "nextToken" not in page


def test_signer_job_records_the_calling_principal(signer, s3, buckets, profile):
    """AWS reports the IAM principal that requested the job. A job started
    with an IAM user's access key records that user, not the account root,
    so the requestedBy filter can find it again."""
    import boto3

    iam = make_client("iam")
    user_name = f"signer-caller-{_uid()}"
    user_arn = iam.create_user(UserName=user_name)["User"]["Arn"]
    key = iam.create_access_key(UserName=user_name)["AccessKey"]
    src, dst = buckets
    s3.put_object(Bucket=src, Key="who.bin", Body=b"w")
    try:
        as_user = boto3.client(
            "signer",
            endpoint_url=signer.meta.endpoint_url,
            region_name=signer.meta.region_name,
            aws_access_key_id=key["AccessKeyId"],
            aws_secret_access_key=key["SecretAccessKey"],
        )
        job = _start(as_user, src, dst, key="who.bin", profile=profile)
        described = signer.describe_signing_job(jobId=job["jobId"])
        assert described["requestedBy"] == user_arn
        assert signer.list_signing_jobs(requestedBy=user_arn)["jobs"]
    finally:
        iam.delete_access_key(UserName=user_name, AccessKeyId=key["AccessKeyId"])
        iam.delete_user(UserName=user_name)


def test_signer_list_jobs_token_is_bound_to_its_filter_set(signer, s3, buckets, profile):
    """A token is only meaningful for the list it came from, so resuming with
    a different filter is refused rather than served from a position that
    means nothing for the new list."""
    src, dst = buckets
    s3.put_object(Bucket=src, Key="bound.bin", Body=b"b")
    for _ in range(2):
        _start(signer, src, dst, key="bound.bin", profile=profile)

    page = signer.list_signing_jobs(maxResults=1, status="Succeeded")
    assert page.get("nextToken")
    with pytest.raises(ClientError) as exc:
        signer.list_signing_jobs(maxResults=1, nextToken=page["nextToken"])
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    # The same token resumes fine under the filter set it was minted with.
    assert "jobs" in signer.list_signing_jobs(
        maxResults=1, status="Succeeded", nextToken=page["nextToken"])


def test_signer_list_jobs_revoked_filter_still_refuses_a_foreign_token(signer):
    """isRevoked=true short-circuits to an empty page, but a token that is not
    ours is a 400 there too: the same value cannot be a 400 alone and a 200
    next to another filter."""
    with pytest.raises(ClientError) as exc:
        signer.list_signing_jobs(isRevoked=True, nextToken="not-a-real-token")
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert signer.list_signing_jobs(isRevoked=True)["jobs"] == []


def test_signer_list_jobs_foreign_next_token_is_refused(signer):
    """A token MiniStack did not mint is a 400, not a silent full listing."""
    with pytest.raises(ClientError) as exc:
        signer.list_signing_jobs(nextToken="not-a-real-token")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_list_jobs_malformed_max_results_is_validation_error(signer):
    """A non-numeric maxResults is a 400 ValidationException, not a 500."""
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.list_signing_jobs(maxResults="not-a-number")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_list_jobs_max_results_above_documented_maximum_is_refused(signer):
    """The API reference documents maxResults as 1 to 25; botocore checks
    only the minimum client-side, so the server has to refuse 26."""
    with pytest.raises(ClientError) as exc:
        signer.list_signing_jobs(maxResults=26)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert isinstance(signer.list_signing_jobs(maxResults=25)["jobs"], list)


# ---------------------------------------------------------------------------
# Signing profiles
# ---------------------------------------------------------------------------

def test_signer_profile_put_get_round_trip(signer):
    name = f"explicit_profile_{_uid()}"
    put = signer.put_signing_profile(
        profileName=name, platformId=_LAMBDA_PLATFORM
    )
    assert put["arn"].endswith(f":/signing-profiles/{name}")
    assert put["profileVersionArn"] == f"{put['arn']}/{put['profileVersion']}"

    got = signer.get_signing_profile(profileName=name)
    assert got["profileName"] == name
    assert got["platformId"] == _LAMBDA_PLATFORM
    assert got["profileVersion"] == put["profileVersion"]
    assert got["status"] == "Active"


def test_signer_get_unknown_profile_404(signer):
    with pytest.raises(ClientError) as exc:
        signer.get_signing_profile(profileName=f"ghost_{_uid()}")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.parametrize("name", ["fw-profile", "p" * 65])
def test_signer_profile_name_outside_documented_pattern_is_refused(
    signer, s3, buckets, name
):
    """ProfileName is 2 to 64 characters; the reference pattern
    ^[a-zA-Z0-9_]{2,} is applied to the whole name. botocore checks only the
    minimum length client-side, so the hyphen and the 65-character name
    reach the server and must be refused on every operation that takes the
    name."""
    with pytest.raises(ClientError) as exc:
        signer.put_signing_profile(profileName=name, platformId=_LAMBDA_PLATFORM)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError) as exc:
        signer.get_signing_profile(profileName=name)
    assert exc.value.response["Error"]["Code"] == "ValidationException"

    src, dst = buckets
    s3.put_object(Bucket=src, Key="named.bin", Body=b"named")
    with pytest.raises(ClientError) as exc:
        _start(signer, src, dst, key="named.bin", profile=name)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    # Refused before anything is written: no marker, no job.
    assert _keys(s3, dst) == []


def test_signer_put_profile_requires_platform_id(signer):
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.put_signing_profile(profileName=f"no_platform_{_uid()}")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_put_profile_accepts_a_partial_validity_period(signer):
    """Both members of SignatureValidityPeriod are documented Required: No, so
    a period carrying only one of them is a valid request."""
    for period in ({"type": "DAYS"}, {"value": 12}, {}):
        name = f"partial_{_uid()}"
        signer.put_signing_profile(
            profileName=name,
            platformId=_IOT_PLATFORM,
            signatureValidityPeriod=period,
        )
        assert signer.get_signing_profile(profileName=name)["profileName"] == name


def test_signer_put_profile_rejects_an_unknown_override_enum(signer):
    """SigningPlatformOverrides.signingImageFormat is JSON | JSONEmbedded |
    JSONDetached, and the nested configuration overrides are RSA | ECDSA and
    SHA1 | SHA256. botocore does not check enums client-side."""
    for overrides in (
        {"signingImageFormat": "PNG"},
        {"signingConfiguration": {"encryptionAlgorithm": "DSA"}},
        {"signingConfiguration": {"hashAlgorithm": "MD5"}},
    ):
        with pytest.raises(ClientError) as exc:
            signer.put_signing_profile(
                profileName=f"ovr_{_uid()}",
                platformId=_IOT_PLATFORM,
                overrides=overrides,
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    # The documented values are accepted.
    name = f"ovr_{_uid()}"
    signer.put_signing_profile(
        profileName=name,
        platformId=_IOT_PLATFORM,
        overrides={
            "signingImageFormat": "JSONDetached",
            "signingConfiguration": {"encryptionAlgorithm": "ECDSA",
                                     "hashAlgorithm": "SHA256"},
        },
    )
    profile = signer.get_signing_profile(profileName=name)
    assert profile["overrides"]["signingImageFormat"] == "JSONDetached"


def test_signer_put_profile_requires_the_signing_material_certificate(signer):
    """SigningMaterial.certificateArn is the shape's one member and is
    Required: Yes."""
    # botocore enforces the required member client-side, so the server-side
    # check is reachable only from a client that does not validate.
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.put_signing_profile(
            profileName=f"mat_{_uid()}",
            platformId=_IOT_PLATFORM,
            signingMaterial={},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_put_profile_enforces_the_tag_constraints(signer):
    """Keys are 1..128 and match ^(?!aws:)[a-zA-Z+-=._:/]+$, values are at
    most 256 characters, and the map takes at most 200 entries."""
    for tags in (
        {"aws:reserved": "x"},
        {"has space": "x"},
        {"k" * 129: "x"},
        {"k": "v" * 257},
        {f"k{i}": "v" for i in range(201)},
    ):
        with pytest.raises(ClientError) as exc:
            signer.put_signing_profile(
                profileName=f"tag_{_uid()}", platformId=_IOT_PLATFORM, tags=tags)
        assert exc.value.response["Error"]["Code"] == "ValidationException", tags

    name = f"tag_{_uid()}"
    signer.put_signing_profile(
        profileName=name, platformId=_IOT_PLATFORM, tags={"team.name": "iot"})
    assert signer.get_signing_profile(profileName=name)["tags"] == {"team.name": "iot"}


def test_signer_rejects_malformed_account_id_members(signer, s3, buckets, profile):
    """jobInvoker on ListSigningJobs and profileOwner on StartSigningJob are
    both a fixed length of 12 digits."""
    raw_signer = make_client("signer", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw_signer.list_signing_jobs(jobInvoker="not-an-account")
    assert exc.value.response["Error"]["Code"] == "ValidationException"

    src, dst = buckets
    s3.put_object(Bucket=src, Key="owner.bin", Body=b"x")
    with pytest.raises(ClientError) as exc:
        raw_signer.start_signing_job(
            source={"s3": {"bucketName": src, "key": "owner.bin"}},
            destination={"s3": {"bucketName": dst}},
            profileName=profile,
            profileOwner="12345",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_signer_put_profile_rejects_unknown_validity_unit(signer):
    """SignatureValidityPeriod.type: DAYS | MONTHS | YEARS."""
    with pytest.raises(ClientError) as exc:
        signer.put_signing_profile(
            profileName=f"unit_{_uid()}",
            platformId=_IOT_PLATFORM,
            signatureValidityPeriod={"type": "WEEKS", "value": 2},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
