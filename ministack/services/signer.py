"""
AWS Signer service emulator.
REST-JSON protocol — /signing-jobs and /signing-profiles paths (signing
name `signer`). URIs/methods verified against botocore's signer 2017-08-25
service model (StartSigningJob is POST /signing-jobs, DescribeSigningJob is
GET /signing-jobs/{jobId}, the profile pair lives on
/signing-profiles/{profileName}).

Supports:
  Jobs:     StartSigningJob, DescribeSigningJob, ListSigningJobs
  Profiles: PutSigningProfile, GetSigningProfile

Deliberate divergences from AWS, each pinned by a test:

  * Signing is SYNCHRONOUS and SYNTHETIC. Real Signer queues an async job;
    here StartSigningJob validates the source object against MiniStack's own
    S3 store in-process, writes a JSON signature marker (source reference +
    SHA-256 of the source bytes, not real cryptography) to
    `destination.s3.bucketName` at key `prefix + jobId`, and returns with the
    job already `Succeeded`. Callers whose contract is the S3 side effect
    (signed object expected at `prefix + jobId`) work without polling
    DescribeSigningJob; the signature BYTES are not a real signature.
  * A missing source (or destination) object/bucket fails at Start with
    ResourceNotFoundException and records NO job — there is no async
    pipeline that could fail later, so nothing ever lists as `Failed`.
  * Profiles are implicit: StartSigningJob with an unknown `profileName`
    auto-registers a minimal profile instead of rejecting. Callers that
    provision profiles out of band (consoles, ops scripts) don't have to
    replay that provisioning locally; PutSigningProfile still works for
    callers that do create them.
  * `source.s3.version` is required by the service model, but a caller on an
    unversioned bucket forwards whatever `head_object` gave it — which is no
    VersionId at all, or S3's literal `"null"` sentinel (which the real
    service accepts: live-probed, StartSigningJob with version `"null"` on a
    suspended-versioning bucket creates the job). An absent/None version
    reads the current object; `"null"` reads the stored literal-`"null"`
    version when one exists (suspended-bucket writes and pre-versioning
    objects are addressable that way) and only falls back to the current
    object when it doesn't; any other version id reads that version.
  * `clientRequestToken` (Required in the model; SDKs autofill it) is
    accepted and stored on the job for reference, but a raw-wire request
    without one is tolerated instead of rejected — there is no idempotency
    replay to key off it.
  * The signed object is ALWAYS written at `prefix + jobId`. On real AWS the
    key is platform-dependent: AWSIoTDeviceManagement-SHA256-ECDSA writes
    exactly `prefix + jobId` (live-measured), while AWSLambda-SHA384-ECDSA
    appends the source archive extension (`prefix + jobId + ".zip"`,
    live-measured). Lambda-platform callers see that divergence here.
  * ListSigningJobs ignores the model's other filters (`platformId`,
    `requestedBy`, `jobInvoker`, `isRevoked`, `signatureExpiresBefore`,
    `signatureExpiresAfter`); only `status` filters and `maxResults`
    truncates.
"""

import copy
import hashlib
import json
import logging
import time
import urllib.parse
import uuid

import ministack.services.s3 as s3_svc
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("signer")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_jobs = AccountRegionScopedDict()      # jobId -> job record (camelCase fields)
_profiles = AccountRegionScopedDict()  # profileName -> profile record


def reset():
    _jobs.clear()
    _profiles.clear()


def get_state():
    return {
        "jobs": copy.deepcopy(_jobs),
        "profiles": copy.deepcopy(_profiles),
    }


def restore_state(data):
    if not data:
        return
    _jobs.update(data.get("jobs", {}))
    _profiles.update(data.get("profiles", {}))


try:
    _restored = load_state("signer")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted signer state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# platformId assigned to an implicitly-registered profile (see module
# docstring); a real platform id, so a client that reads it back can map it.
_IMPLICIT_PLATFORM_ID = "AWSIoTDeviceManagement-SHA256-ECDSA"


def _now():
    return int(time.time())


def _error(status, code, message):
    return error_response_json(code, message, status)


def _profile_arn(name):
    # Signer profile ARNs carry a `/` before the resource type:
    # arn:aws:signer:<region>:<account>:/signing-profiles/<name>[/<version>]
    return (
        f"arn:aws:signer:{get_region()}:{get_account_id()}:"
        f"/signing-profiles/{name}"
    )


def _new_profile_version():
    # ProfileVersion is a 10-char alphanumeric token on AWS.
    return uuid.uuid4().hex[:10]


def _requested_by():
    # AWS reports the caller's IAM principal; there is no per-request
    # principal here, so the account root stands in.
    return f"arn:aws:iam::{get_account_id()}:root"


def _drop_none(record):
    return {k: v for k, v in record.items() if v is not None}


def _register_profile(name, body):
    version = _new_profile_version()
    arn = _profile_arn(name)
    profile = {
        "profileName": name,
        "profileVersion": version,
        "profileVersionArn": f"{arn}/{version}",
        "arn": arn,
        "platformId": body.get("platformId"),
        "signingMaterial": body.get("signingMaterial"),
        "signatureValidityPeriod": body.get("signatureValidityPeriod"),
        "overrides": body.get("overrides"),
        "signingParameters": body.get("signingParameters"),
        "tags": body.get("tags"),
        "status": "Active",
    }
    _profiles[name] = profile
    return profile


def _ensure_profile(name):
    """Return the stored profile, auto-registering an implicit minimal one
    for an unknown name (deliberate divergence — see module docstring)."""
    profile = _profiles.get(name)
    if profile is None:
        profile = _register_profile(name, {"platformId": _IMPLICIT_PLATFORM_ID})
    return profile


def _resolve_source(bucket, key, version):
    """Return `(data, version_id)` for the source object, or `(None, None)`
    when it doesn't exist.

    The model requires `source.s3.version`, but a caller on an unversioned
    bucket sends what `head_object` reported: nothing, or S3's literal
    `"null"` sentinel. Absent/None/"" reads the current object. `"null"` is
    trickier: MiniStack's S3 stores an addressable literal-`"null"` version
    (suspended-bucket writes, pre-versioning objects), and on a bucket whose
    versioning was enabled later that version can hold DIFFERENT bytes than
    the current object — so try the `"null"` version first and fall back to
    the current object only when no such version exists. Anything else is a
    real version id and reads exactly that version."""
    if version in (None, ""):
        return s3_svc._get_object_data(bucket, key), None
    if version == "null":
        data = s3_svc._get_object_data(bucket, key, version_id="null")
        if data is not None:
            return data, "null"
        return s3_svc._get_object_data(bucket, key), None
    return s3_svc._get_object_data(bucket, key, version_id=version), version


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _start_signing_job(body):
    s3_source = (body.get("source") or {}).get("s3") or {}
    s3_dest = (body.get("destination") or {}).get("s3") or {}
    profile_name = body.get("profileName")

    src_bucket = s3_source.get("bucketName")
    src_key = s3_source.get("key")
    if not src_bucket or not src_key:
        return _error(400, "ValidationException",
                      "source.s3.bucketName and source.s3.key are required.")
    dst_bucket = s3_dest.get("bucketName")
    if not dst_bucket:
        return _error(400, "ValidationException",
                      "destination.s3.bucketName is required.")
    if not profile_name:
        return _error(400, "ValidationException", "profileName is required.")

    data, version_id = _resolve_source(src_bucket, src_key, s3_source.get("version"))
    if data is None:
        # Synchronous divergence: the real service would accept the job and
        # fail it later; there is no later here, so the missing source fails
        # the Start itself and no job is recorded.
        return _error(404, "ResourceNotFoundException",
                      f"Source object s3://{src_bucket}/{src_key} not found.")

    if s3_svc._ensure_bucket(dst_bucket) is None:
        # Synchronous divergence: the real service would accept the job and
        # fail it later; there is no later here, so the missing destination
        # fails the Start itself and no job is recorded.
        return _error(404, "ResourceNotFoundException",
                      f"Destination bucket {dst_bucket} not found.")

    profile = _ensure_profile(profile_name)
    job_id = str(uuid.uuid4())
    prefix = s3_dest.get("prefix") or ""
    signed_key = f"{prefix}{job_id}"
    now = _now()

    marker = {
        "jobId": job_id,
        "profileName": profile_name,
        "platformId": profile.get("platformId"),
        "source": {
            "bucketName": src_bucket,
            "key": src_key,
            "version": version_id,
        },
        "sourceSha256": hashlib.sha256(data).hexdigest(),
        "signedAt": now,
        "signedBy": "ministack-signer",
    }
    marker_bytes = json.dumps(marker, ensure_ascii=False).encode("utf-8")
    status, _headers, resp_body = s3_svc._put_object(
        dst_bucket, signed_key, marker_bytes,
        {"content-type": "application/json",
         "content-length": str(len(marker_bytes))},
    )
    if status >= 300:
        # Bucket existence was checked above, so this is some other S3
        # rejection (Object Lock, SSE config, ...) — surface it as the
        # service-side failure it is instead of mislabeling it "not found".
        logger.error(
            "Signed-object write to s3://%s/%s failed with S3 status %s: %s",
            dst_bucket, signed_key, status, resp_body,
        )
        return _error(500, "InternalServiceErrorException",
                      f"Writing the signed object to s3://{dst_bucket}/"
                      f"{signed_key} failed with S3 status {status}.")

    account = get_account_id()
    _jobs[job_id] = {
        "jobId": job_id,
        "source": {"s3": _drop_none({
            "bucketName": src_bucket,
            "key": src_key,
            "version": version_id,
        })},
        "signedObject": {"s3": {"bucketName": dst_bucket, "key": signed_key}},
        "profileName": profile_name,
        "profileVersion": profile.get("profileVersion"),
        "platformId": profile.get("platformId"),
        "status": "Succeeded",
        "statusReason": "Signing Succeeded",  # live-measured Describe wording
        "createdAt": now,
        "completedAt": now,
        "requestedBy": _requested_by(),
        "jobOwner": account,
        "jobInvoker": account,
        # Required in the model and autofilled by SDKs; tolerated when a
        # raw-wire caller omits it (see module docstring). Stored for
        # reference only — not a member of any response shape.
        "clientRequestToken": body.get("clientRequestToken"),
    }
    return json_response({"jobId": job_id, "jobOwner": account})


def _describe_signing_job(job_id):
    job = _jobs.get(job_id)
    if job is None:
        return _error(404, "ResourceNotFoundException",
                      f"Signing job {job_id} not found.")
    resp = _drop_none(job)
    # Stored on the job but not a DescribeSigningJobResponse member.
    resp.pop("clientRequestToken", None)
    return json_response(resp)


# Real AWS ListSigningJobs returns SigningJob summaries, not the Describe
# shape (no requestedBy). Keep in sync with the AWS SigningJob shape.
_LISTED_JOB_FIELDS = (
    "jobId", "source", "signedObject", "createdAt", "status",
    "profileName", "profileVersion", "platformId", "jobOwner", "jobInvoker",
)


def _list_signing_jobs(query):
    """One page, no nextToken: the store is a local dict, so every job fits a
    single unbounded response — maxResults still truncates, but no follow-up
    token is issued and an incoming nextToken is ignored. Of the model's
    filters only `status` is honored; platformId, requestedBy, jobInvoker,
    isRevoked and signatureExpiresBefore/-After are ignored (nothing here is
    ever revoked or expiring)."""
    status_filter = query.get("status")
    try:
        max_results = int(query.get("maxResults", 1000))
    except (TypeError, ValueError):
        return _error(400, "ValidationException",
                      f"Invalid value for maxResults: "
                      f"{query.get('maxResults')!r}.")
    jobs = []
    for job in _jobs.values():
        if status_filter and job.get("status") != status_filter:
            continue
        summary = _drop_none({k: job.get(k) for k in _LISTED_JOB_FIELDS})
        summary["isRevoked"] = False
        jobs.append(summary)
    return json_response({"jobs": jobs[:max_results]})


def _put_signing_profile(name, body):
    if not body.get("platformId"):
        return _error(400, "ValidationException", "platformId is required.")
    profile = _register_profile(name, body)
    return json_response({
        "arn": profile["arn"],
        "profileVersion": profile["profileVersion"],
        "profileVersionArn": profile["profileVersionArn"],
    })


def _get_signing_profile(name):
    profile = _profiles.get(name)
    if profile is None:
        return _error(404, "ResourceNotFoundException",
                      f"Signing profile {name} not found.")
    return json_response(_drop_none(profile))


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # POST /signing-jobs -- StartSigningJob
    if path == "/signing-jobs" and method == "POST":
        return _start_signing_job(body)

    # GET /signing-jobs -- ListSigningJobs
    if path == "/signing-jobs" and method == "GET":
        return _list_signing_jobs(query)

    # GET /signing-jobs/{jobId} -- DescribeSigningJob
    if path.startswith("/signing-jobs/") and method == "GET":
        job_id = urllib.parse.unquote(path[len("/signing-jobs/"):])
        if job_id and "/" not in job_id:
            return _describe_signing_job(job_id)

    # PUT/GET /signing-profiles/{profileName} -- PutSigningProfile / GetSigningProfile
    if path.startswith("/signing-profiles/"):
        name = urllib.parse.unquote(path[len("/signing-profiles/"):])
        if name and "/" not in name:
            if method == "PUT":
                return _put_signing_profile(name, body)
            if method == "GET":
                return _get_signing_profile(name)

    return _error(400, "ValidationException", f"No route for {method} {path}")
