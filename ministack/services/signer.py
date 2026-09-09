# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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

Behaviour that follows the live service (measured 2026-08-26 unless noted):

  * An unknown `profileName` on StartSigningJob is ResourceNotFoundException
    "profile with name <name> does not exist"; nothing is written and no job
    is recorded. PutSigningProfile is the way to create a profile.
  * `clientRequestToken` is an idempotency token: a repeat with a known
    token answers the first call's `{jobId, jobOwner}` again and writes
    nothing (API reference: "All calls after the first that use this token
    return the same response as the first call"). A raw-wire request
    without one (the model requires it; SDKs autofill it) is still accepted.
  * The signed-object key is `prefix + jobId`, with `.zip` appended when the
    profile's platform is AWSLambda-SHA384-ECDSA and the source key ends in
    `.zip` (both measured: the IoT platform wrote exactly `prefix + jobId`,
    the Lambda platform wrote `prefix + jobId + ".zip"` for a `.zip`
    source). Other platforms and extensions are unmeasured and get the
    plain key.
  * `source.s3.version` is required by the model, but a caller on an
    unversioned bucket forwards what `head_object` gave it: no VersionId at
    all, or S3's literal `"null"` sentinel (the live service accepts
    `"null"` on a suspended-versioning bucket). An absent version reads the
    current object and records the version id S3 stores for it, which is
    `"null"` only when the bucket never had versioning enabled (S3 user
    guide: "Objects that are stored in your bucket before you set the
    versioning state have a version ID of null"). `"null"` reads the stored
    literal-`"null"` version when one exists (suspended-bucket writes,
    pre-versioning objects), falls back to the current object otherwise,
    and records `"null"`. Either way the required member is always present
    on Describe/List.
  * A job carries the profile's `signingMaterial`, `overrides` and
    `signingParameters`, and `signatureExpiresAt` = createdAt plus the
    profile's `signatureValidityPeriod` (default 135 months, per the
    PutSigningProfile reference).

Deliberate divergences from AWS, each pinned by a test:

  * Signing is SYNCHRONOUS and SYNTHETIC. Real Signer queues an async job;
    here StartSigningJob validates the source object against MiniStack's own
    S3 store in-process, writes a JSON signature marker (source reference +
    SHA-256 of the source bytes, not real cryptography) to
    `destination.s3.bucketName`, and returns with the job already
    `Succeeded`. Callers whose contract is the S3 side effect work without
    polling DescribeSigningJob; the signature BYTES are not a real signature.
  * A missing source object or destination bucket fails at Start with
    ResourceNotFoundException and records NO job — there is no async
    pipeline that could fail later, so nothing ever lists as `Failed`.
  * ListSigningJobs pages: `maxResults` (1..25) limits the page and a
    `nextToken` carries the sort position of the last job returned, so a
    paginator walks the whole list. `status`, `isRevoked`,
    `platformId`, `requestedBy` and `jobInvoker` filter; nothing is ever
    revoked, so `isRevoked=true` is an empty page. `signatureExpiresBefore`
    / `signatureExpiresAfter` are ignored.
  * `destination.s3.bucketName` is optional in the model but required here
    (the live behaviour without it is unmeasured).

Documented input constraints that are enforced (the live refusal wording
was not measured, so the messages are MiniStack's own): `profileName` is 2
to 64 characters and the reference gives the pattern `^[a-zA-Z0-9_]{2,}`.
That pattern is start-anchored; MiniStack applies it to the whole name, as
the Terraform provider does (`^[0-9A-Za-z_]{0,64}$`), so a hyphen anywhere
is refused. `ListSigningJobs.maxResults` is 1 to 25, `status` is one of
`InProgress | Failed | Succeeded`, and `signatureValidityPeriod.type` is one
of `DAYS | MONTHS | YEARS`.
"""

import base64
import calendar
import copy
import hashlib
import json
import logging
import re
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone

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
_tokens = AccountRegionScopedDict()    # clientRequestToken -> first StartSigningJob response


def reset():
    _jobs.clear()
    _profiles.clear()
    _tokens.clear()


def get_state():
    return {
        "jobs": copy.deepcopy(_jobs),
        "profiles": copy.deepcopy(_profiles),
        "tokens": copy.deepcopy(_tokens),
    }


def restore_state(data):
    if not data:
        return
    _jobs.update(data.get("jobs", {}))
    _profiles.update(data.get("profiles", {}))
    _tokens.update(data.get("tokens", {}))


try:
    _restored = load_state("signer")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted signer state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# ProfileName shape: length 2..64, pattern ^[a-zA-Z0-9_]{2,} in the API
# reference (StartSigningJob / PutSigningProfile / GetSigningProfile). The
# reference pattern is start-anchored only; it is applied to the whole name
# here, as the Terraform provider does, so a hyphen anywhere is refused.
_PROFILE_NAME_RE = re.compile(r"[a-zA-Z0-9_]{2,64}")

_JOB_STATUSES = ("InProgress", "Failed", "Succeeded")
_VALIDITY_UNITS = ("DAYS", "MONTHS", "YEARS")
# SigningPlatformOverrides / SigningConfigurationOverrides valid values, and
# the tags map constraints PutSigningProfile documents.
_IMAGE_FORMATS = ("JSON", "JSONEmbedded", "JSONDetached")
_ENCRYPTION_ALGORITHMS = ("RSA", "ECDSA")
_HASH_ALGORITHMS = ("SHA1", "SHA256")
_MAX_TAGS = 200
_TAG_KEY_RE = re.compile(r"[a-zA-Z+\-=._:/]+")
_ACCOUNT_ID_RE = re.compile(r"[0-9]{12}")
# PutSigningProfile reference: "If unspecified, the default is 135 months."
_DEFAULT_VALIDITY = {"type": "MONTHS", "value": 135}
# The one platform whose signed-object key was measured to differ.
_LAMBDA_PLATFORM_ID = "AWSLambda-SHA384-ECDSA"


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


def _validate_profile_name(name):
    """Return an error tuple when `name` is outside the documented
    ProfileName constraints, else None (wording not measured, see module
    docstring)."""
    if not isinstance(name, str) or not _PROFILE_NAME_RE.fullmatch(name):
        return _error(400, "ValidationException",
                      f"profileName {name!r} must be 2 to 64 characters "
                      "matching ^[a-zA-Z0-9_]{2,}.")
    return None


def _validate_overrides(overrides):
    """The two enums SigningPlatformOverrides documents, plus the pair on the
    SigningConfigurationOverrides it nests. botocore does not check enums
    client-side, so an unknown value reaches the service, which refuses it."""
    if overrides is None:
        return None
    if not isinstance(overrides, dict):
        return _error(400, "ValidationException", "overrides must be a structure.")
    image_format = overrides.get("signingImageFormat")
    if image_format is not None and image_format not in _IMAGE_FORMATS:
        return _error(400, "ValidationException",
                      f"Invalid value for overrides.signingImageFormat: "
                      f"{image_format!r}; expected one of "
                      f"{', '.join(_IMAGE_FORMATS)}.")
    config = overrides.get("signingConfiguration")
    if config is None:
        return None
    if not isinstance(config, dict):
        return _error(400, "ValidationException",
                      "overrides.signingConfiguration must be a structure.")
    for member, allowed in (("encryptionAlgorithm", _ENCRYPTION_ALGORITHMS),
                            ("hashAlgorithm", _HASH_ALGORITHMS)):
        value = config.get(member)
        if value is not None and value not in allowed:
            return _error(400, "ValidationException",
                          f"Invalid value for overrides.signingConfiguration."
                          f"{member}: {value!r}; expected one of "
                          f"{', '.join(allowed)}.")
    return None


def _validate_signing_material(material):
    """certificateArn is the shape's one member and is Required: Yes. The
    certificate itself is not resolved: nothing signs for real here, so there
    is no ACM lookup to make."""
    if material is None:
        return None
    if not isinstance(material, dict):
        return _error(400, "ValidationException",
                      "signingMaterial must be a structure.")
    if not isinstance(material.get("certificateArn"), str) or not material["certificateArn"]:
        return _error(400, "ValidationException",
                      "signingMaterial.certificateArn is required.")
    return None


def _validate_tags(tags):
    """The map constraints PutSigningProfile documents: at most 200 entries,
    keys 1..128 matching `^(?!aws:)[a-zA-Z+-=._:/]+$`, values at most 256."""
    if tags is None:
        return None
    if not isinstance(tags, dict):
        return _error(400, "ValidationException", "tags must be a map.")
    if len(tags) > _MAX_TAGS:
        return _error(400, "ValidationException",
                      f"tags must have at most {_MAX_TAGS} entries, got {len(tags)}.")
    for key, value in tags.items():
        if not isinstance(key, str) or not 1 <= len(key) <= 128:
            return _error(400, "ValidationException",
                          f"Invalid tag key: {key!r}; keys are 1 to 128 characters.")
        if key.startswith("aws:") or not _TAG_KEY_RE.fullmatch(key):
            return _error(400, "ValidationException",
                          f"Invalid tag key: {key!r}; keys match "
                          "^(?!aws:)[a-zA-Z+-=._:/]+$.")
        if not isinstance(value, str) or len(value) > 256:
            return _error(400, "ValidationException",
                          f"Invalid tag value for {key!r}; values are at most "
                          "256 characters.")
    return None


def _validate_account_id(value, field):
    """jobInvoker and profileOwner are both a fixed length of 12 digits."""
    if value is None:
        return None
    if not isinstance(value, str) or not _ACCOUNT_ID_RE.fullmatch(value):
        return _error(400, "ValidationException",
                      f"Invalid value for {field}: {value!r}; expected 12 digits.")
    return None


def _signature_expires_at(created_at, period):
    """createdAt plus the profile's signatureValidityPeriod (DAYS / MONTHS /
    YEARS), default 135 months. Month arithmetic clamps the day to the
    target month's length."""
    if not isinstance(period, dict) or not period.get("value"):
        period = _DEFAULT_VALIDITY
    unit = period.get("type") or "MONTHS"
    value = int(period["value"])
    start = datetime.fromtimestamp(created_at, tz=timezone.utc)
    if unit == "DAYS":
        return int((start + timedelta(days=value)).timestamp())
    months = value * (12 if unit == "YEARS" else 1)
    total = start.month - 1 + months
    year = start.year + total // 12
    month = total % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    return int(start.replace(year=year, month=month, day=day).timestamp())


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


def _current_version_id(bucket, key):
    """The version id S3 holds for the CURRENT object at `key`, or `"null"`.

    MiniStack's S3 puts `version_id` on the object record only for a write
    that landed while versioning was Enabled (services/s3.py
    `_record_object_version`), and `_object_response_headers` emits
    `x-amz-version-id` from that same field — so this is the id a GetObject
    on the current object reports. AWS uses the literal `null` as the
    version id of an object stored before versioning was set (S3 user
    guide, "Unversioned, versioning-enabled, and versioning-suspended
    buckets")."""
    bucket_record = s3_svc._ensure_bucket(bucket)
    if bucket_record is None:
        return "null"
    obj = bucket_record["objects"].get(key)
    return (obj or {}).get("version_id") or "null"


def _resolve_source(bucket, key, version):
    """Return `(data, version_id)` for the source object, or `(None, None)`
    when it doesn't exist.

    The model requires `source.s3.version`, but a caller on an unversioned
    bucket sends what `head_object` reported: nothing, or S3's literal
    `"null"` sentinel. Absent/None/"" reads the current object and records
    the version id that object really carries, so a job on a versioned
    bucket names the version it signed; `"null"` is recorded only when the
    bucket has no versioning. `"null"` as the request value is trickier:
    MiniStack's S3 stores an addressable literal-`"null"` version
    (suspended-bucket writes, pre-versioning objects), and on a bucket whose
    versioning was enabled later that version can hold DIFFERENT bytes than
    the current object — so try the `"null"` version first and fall back to
    the current object only when no such version exists; both record
    `"null"`. Anything else is a real version id and reads exactly that
    version."""
    if version in (None, ""):
        data = s3_svc._get_object_data(bucket, key)
        if data is None:
            return None, None
        return data, _current_version_id(bucket, key)
    if version == "null":
        data = s3_svc._get_object_data(bucket, key, version_id="null")
        if data is not None:
            return data, "null"
        return s3_svc._get_object_data(bucket, key), "null"
    return s3_svc._get_object_data(bucket, key, version_id=version), version


def _signed_key(prefix, job_id, platform_id, src_key):
    key = f"{prefix}{job_id}"
    if platform_id == _LAMBDA_PLATFORM_ID and src_key.endswith(".zip"):
        key += ".zip"
    return key


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _start_signing_job(body):
    s3_source = (body.get("source") or {}).get("s3") or {}
    s3_dest = (body.get("destination") or {}).get("s3") or {}
    profile_name = body.get("profileName")

    # profileOwner is accepted without effect, but its shape is documented and
    # a malformed value is a 400 on AWS rather than a silently ignored member.
    owner_problem = _validate_account_id(body.get("profileOwner"), "profileOwner")
    if owner_problem is not None:
        return owner_problem

    src_bucket = s3_source.get("bucketName")
    src_key = s3_source.get("key")
    src_version = s3_source.get("version")
    if not (isinstance(src_bucket, str) and src_bucket
            and isinstance(src_key, str) and src_key):
        return _error(400, "ValidationException",
                      "source.s3.bucketName and source.s3.key are required strings.")
    if src_version is not None and not isinstance(src_version, str):
        return _error(400, "ValidationException",
                      "source.s3.version must be a string.")
    dst_bucket = s3_dest.get("bucketName")
    if not (isinstance(dst_bucket, str) and dst_bucket):
        return _error(400, "ValidationException",
                      "destination.s3.bucketName is required.")
    prefix = s3_dest.get("prefix") or ""
    if not isinstance(prefix, str):
        return _error(400, "ValidationException",
                      "destination.s3.prefix must be a string.")
    if not profile_name:
        return _error(400, "ValidationException", "profileName is required.")
    name_err = _validate_profile_name(profile_name)
    if name_err:
        return name_err
    token = body.get("clientRequestToken")
    if token is not None and not isinstance(token, str):
        return _error(400, "ValidationException",
                      "clientRequestToken must be a string.")

    if token:
        replay = _tokens.get(token)
        if replay is not None:
            # Idempotent replay: the first call's response, nothing written.
            return json_response(dict(replay))

    profile = _profiles.get(profile_name)
    if profile is None:
        # Live-measured wording.
        return _error(404, "ResourceNotFoundException",
                      f"profile with name {profile_name} does not exist")

    data, version_id = _resolve_source(src_bucket, src_key, src_version)
    if data is None:
        # Synchronous divergence: the real service would accept the job and
        # fail it later; there is no later here, so the missing source fails
        # the Start itself and no job is recorded.
        return _error(404, "ResourceNotFoundException",
                      f"Source object s3://{src_bucket}/{src_key} not found.")

    if s3_svc._ensure_bucket(dst_bucket) is None:
        # Same synchronous divergence for the destination bucket.
        return _error(404, "ResourceNotFoundException",
                      f"Destination bucket {dst_bucket} not found.")

    job_id = str(uuid.uuid4())
    platform_id = profile.get("platformId")
    signed_key = _signed_key(prefix, job_id, platform_id, src_key)
    now = _now()

    marker = {
        "jobId": job_id,
        "profileName": profile_name,
        "platformId": platform_id,
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
        "source": {"s3": {
            "bucketName": src_bucket,
            "key": src_key,
            "version": version_id,
        }},
        "signedObject": {"s3": {"bucketName": dst_bucket, "key": signed_key}},
        "profileName": profile_name,
        "profileVersion": profile.get("profileVersion"),
        "platformId": platform_id,
        "signingMaterial": profile.get("signingMaterial"),
        "overrides": profile.get("overrides"),
        "signingParameters": profile.get("signingParameters"),
        "signatureExpiresAt": _signature_expires_at(
            now, profile.get("signatureValidityPeriod")
        ),
        "status": "Succeeded",
        "statusReason": "Signing Succeeded",  # live-measured Describe wording
        "createdAt": now,
        "completedAt": now,
        "requestedBy": _requested_by(),
        "jobOwner": account,
        "jobInvoker": account,
    }
    response = {"jobId": job_id, "jobOwner": account}
    if token:
        _tokens[token] = dict(response)
    return json_response(response)


def _describe_signing_job(job_id):
    job = _jobs.get(job_id)
    if job is None:
        return _error(404, "ResourceNotFoundException",
                      f"Signing job {job_id} not found.")
    return json_response(_drop_none(job))


# Real AWS ListSigningJobs returns SigningJob summaries, not the Describe
# shape (no requestedBy, statusReason, completedAt). Keep in sync with the
# AWS SigningJob shape.
_LISTED_JOB_FIELDS = (
    "jobId", "source", "signedObject", "createdAt", "status",
    "profileName", "profileVersion", "platformId", "jobOwner", "jobInvoker",
    "signingMaterial", "signatureExpiresAt",
)


def _job_sort_key(job):
    """Jobs are ordered by creation, with the job id breaking a tie: two jobs
    started in the same second must still have a total order, or a nextToken
    pointing at one of them cannot say where the next page starts."""
    return (job.get("createdAt") or 0, job.get("jobId") or "")


_TOKEN_FILTERS = ("status", "isRevoked", "platformId", "requestedBy", "jobInvoker")


def _token_filter_digest(query):
    """A fingerprint of the filters a listing was made with. A token is only
    meaningful for the list it was minted from, so it carries the fingerprint
    and a call that changes a filter mid-walk is refused rather than resumed
    at a position that means nothing for the new list."""
    filters = [(name, query.get(name)) for name in _TOKEN_FILTERS]
    raw = json.dumps(filters, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _encode_job_token(job, query):
    """A token carries the sort position of the last job on the page, not an
    offset: an offset would skip a job whenever one was created between two
    calls."""
    raw = json.dumps(list(_job_sort_key(job)) + [_token_filter_digest(query)])
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_job_token(token, query):
    """The sort key a token carries, or None when the token is not ours or was
    minted for a different filter set."""
    try:
        raw = json.loads(base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8"))
        if not isinstance(raw, list) or len(raw) != 3:
            return None
        if raw[2] != _token_filter_digest(query):
            return None
        return (float(raw[0]), str(raw[1]))
    except Exception:
        return None


def _list_signing_jobs(query):
    """`maxResults` (1..25) limits the page; when jobs remain, the response
    carries a `nextToken` that a following call resumes from, as the API
    documents ("If additional jobs remain to be listed, AWS Signer returns a
    nextToken value"). Without `maxResults` the whole list is returned in one
    page: the service's own default page size is not documented and was not
    measured. The order (createdAt, then jobId) is MiniStack's own; the
    reference does not document one. `status`, `isRevoked`, `platformId`,
    `requestedBy` and `jobInvoker` filter (nothing is ever revoked, so
    isRevoked=true is empty); signatureExpiresBefore/-After are ignored.
    `maxResults` must be 1..25 and `status` one of the documented values; the
    refusal wording is MiniStack's own."""
    status_filter = query.get("status")
    if status_filter is not None and status_filter not in _JOB_STATUSES:
        return _error(400, "ValidationException",
                      f"Invalid value for status: {status_filter!r}; expected "
                      f"one of {', '.join(_JOB_STATUSES)}.")
    raw_max = query.get("maxResults")
    max_results = None
    if raw_max is not None:
        try:
            max_results = int(raw_max)
        except (TypeError, ValueError):
            return _error(400, "ValidationException",
                          f"Invalid value for maxResults: {raw_max!r}.")
        if not 1 <= max_results <= 25:
            return _error(400, "ValidationException",
                          f"maxResults must be between 1 and 25, got "
                          f"{max_results}.")
    is_revoked = query.get("isRevoked")
    if is_revoked is not None:
        if str(is_revoked).lower() not in ("true", "false"):
            return _error(400, "ValidationException",
                          f"Invalid value for isRevoked: {is_revoked!r}.")
    token = query.get("nextToken")
    cursor = None
    if token is not None:
        cursor = _decode_job_token(token, query)
        if cursor is None:
            return _error(400, "ValidationException",
                          f"Invalid value for nextToken: {token!r}.")
    # Nothing is ever revoked, so the revoked listing is empty whatever the
    # rest of the query says. The token is still validated first, or the same
    # bad token would be a 400 alone and a 200 next to isRevoked=true.
    if is_revoked is not None and str(is_revoked).lower() == "true":
        return json_response({"jobs": []})
    invoker_problem = _validate_account_id(query.get("jobInvoker"), "jobInvoker")
    if invoker_problem is not None:
        return invoker_problem
    equality = {
        field: query[field]
        for field in ("platformId", "requestedBy", "jobInvoker")
        if query.get(field) is not None
    }
    matched = []
    for job in _jobs.values():
        if status_filter and job.get("status") != status_filter:
            continue
        if any(job.get(field) != value for field, value in equality.items()):
            continue
        matched.append(job)
    matched.sort(key=_job_sort_key)
    if cursor is not None:
        matched = [job for job in matched if _job_sort_key(job) > cursor]
    page, remaining = matched, []
    if max_results is not None:
        page, remaining = matched[:max_results], matched[max_results:]
    jobs = []
    for job in page:
        summary = _drop_none({k: job.get(k) for k in _LISTED_JOB_FIELDS})
        summary["isRevoked"] = False
        jobs.append(summary)
    result = {"jobs": jobs}
    if remaining:
        result["nextToken"] = _encode_job_token(page[-1], query)
    return json_response(result)


def _put_signing_profile(name, body):
    name_err = _validate_profile_name(name)
    if name_err:
        return name_err
    if not body.get("platformId"):
        return _error(400, "ValidationException", "platformId is required.")
    period = body.get("signatureValidityPeriod")
    if period is not None:
        # Both members are documented Required: No, so a period carrying only
        # one of them is a valid request; _signature_expires_at falls back to
        # MONTHS for a missing type and to the 135-month default for a missing
        # value. Only the shapes the model cannot express are refused.
        if not isinstance(period, dict):
            return _error(400, "ValidationException",
                          "signatureValidityPeriod must be a structure.")
        unit = period.get("type")
        if unit is not None and unit not in _VALIDITY_UNITS:
            return _error(400, "ValidationException",
                          f"Invalid value for signatureValidityPeriod.type: {unit!r}; "
                          f"expected one of {', '.join(_VALIDITY_UNITS)}.")
        value = period.get("value")
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            return _error(400, "ValidationException",
                          "signatureValidityPeriod.value must be an integer.")
    for problem in (_validate_overrides(body.get("overrides")),
                    _validate_signing_material(body.get("signingMaterial")),
                    _validate_tags(body.get("tags"))):
        if problem is not None:
            return problem
    profile = _register_profile(name, body)
    return json_response({
        "arn": profile["arn"],
        "profileVersion": profile["profileVersion"],
        "profileVersionArn": profile["profileVersionArn"],
    })


def _get_signing_profile(name):
    name_err = _validate_profile_name(name)
    if name_err:
        return name_err
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
    if not isinstance(body, dict):
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
