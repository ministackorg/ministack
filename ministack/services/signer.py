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
  Profiles: PutSigningProfile, GetSigningProfile, CancelSigningProfile,
            AddProfilePermission, ListProfilePermissions, RemoveProfilePermission

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

  * Signing is SYNCHRONOUS. Real Signer queues an async job; here
    StartSigningJob validates the source object against MiniStack's own S3
    store in-process, writes the signed object to
    `destination.s3.bucketName`, and returns with the job already
    `Succeeded`. Callers whose contract is the S3 side effect work without
    polling DescribeSigningJob.
  * On AWSIoTDeviceManagement-SHA256-ECDSA, when ACM holds the private
    key of the profile's certificate, the job signs the source bytes with
    it and writes the JSON document AWS writes; a non-EC key fails the job
    as on AWS. Otherwise the signed object is a JSON receipt (source
    reference + SHA-256 of the source bytes), which is not a signature, and
    a warning is logged when AWS would have signed with the certificate.
  * A missing source object or destination bucket fails at Start with
    ResourceNotFoundException and records NO job — there is no async
    pipeline that could fail later. The only job that lists as `Failed` is
    the non-EC key above.
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

import ministack.services.acm as acm_svc
import ministack.services.s3 as s3_svc
from ministack.core.responses import (
    REST_JSON_CONTENT_TYPE,
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
_TOKEN_CACHE_MAX = 1024                # same bound apigateway_v1 uses for its authorizer cache


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


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _jobs.update(data.get("jobs", {}))
    _profiles.update(data.get("profiles", {}))
    _tokens.update(data.get("tokens", {}))




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
# botocore signer/2017-08-25 TagKey.pattern is `^(?!aws:)[a-zA-Z+-=._:/]+$`.
# `+-=` there is a RANGE (U+002B..U+003D), so it also admits `,` `-` `.`
# `/` the digits `0-9` `:` `;` `<` `=`. Escaping the hyphen narrows the
# class and refused a tag key AWS accepts (`env1`, `2024`).
_TAG_KEY_RE = re.compile(r"[a-zA-Z+-=._:/]+")
_ACCOUNT_ID_RE = re.compile(r"[0-9]{12}")
# PutSigningProfile reference: "If unspecified, the default is 135 months."
_DEFAULT_VALIDITY = {"type": "MONTHS", "value": 135}
# The one platform whose signed-object key was measured to differ.
_LAMBDA_PLATFORM_ID = "AWSLambda-SHA384-ECDSA"
# The platform whose signed object is emulated.
_IOT_PLATFORM_ID = "AWSIoTDeviceManagement-SHA256-ECDSA"
# Platforms that sign with the profile's ACM certificate on AWS.
_ACM_SIGNED_PLATFORMS = (_IOT_PLATFORM_ID, "AmazonFreeRTOS-Default", "AmazonFreeRTOS-TI-CC3220SF")


def _now():
    return int(time.time())


def _error(status, code, message):
    """A signer error body.

    signer/2017-08-25 is ``rest-json``, so the body is ``application/json``
    rather than the json-protocol content type. Its exception shapes model the
    message member as lowercase ``message`` (locationName ``message``), which
    is already the helper's default, so the casing is left alone here.

    Those shapes also carry an optional ``code`` member. It is not populated:
    nothing here knows the string the live service puts in it, and a guess
    would read as if it had been measured.
    """
    return error_response_json(code, message, status,
                               content_type=REST_JSON_CONTENT_TYPE)


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


def _requested_by(headers):
    """The caller's IAM principal, which is what AWS reports on the job.

    The access key in the request is resolved the way the IAM layer resolves
    it, so a job started under an assumed role or an IAM user records that
    principal and the requestedBy filter can find it again. Without AUTH
    there is nothing to resolve, and an unknown or unresolvable key is not an
    error here, so the account root stands in as it did before.
    """
    from ministack.core.iam_evaluator import PrincipalInfo, resolve_principal
    from ministack.core.router import extract_access_key_id

    account = get_account_id()
    access_key = extract_access_key_id(headers or {})
    if access_key:
        principal = resolve_principal(access_key, account)
        if isinstance(principal, PrincipalInfo) and principal.arn:
            return principal.arn
    return f"arn:aws:iam::{account}:root"


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
    """certificateArn is required; StartSigningJob resolves it, so an ARN ACM does not hold is accepted."""
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


def _acm_private_key(profile):
    """(key, None) for the private key ACM holds for the profile's certificate, else (None, reason)."""
    arn = (profile.get("signingMaterial") or {}).get("certificateArn")
    if not arn:
        return None, "the profile has no signingMaterial"
    cert = acm_svc._get_local_certificate(arn)
    if cert is None:
        return None, f"ACM in this account and region has no certificate {arn}"
    pem = cert.get("_private_key")
    if not pem:
        if cert.get("Type") == "IMPORTED" and "_private_key" not in cert:
            return None, f"the private key of {arn} is not persisted across restarts; import it again"
        return None, f"ACM holds no private key for {arn}"
    try:
        from cryptography.exceptions import UnsupportedAlgorithm
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except ImportError:
        return None, "the cryptography package is not installed"
    try:
        return load_pem_private_key(pem.encode(), password=None), None
    except (ValueError, TypeError, UnsupportedAlgorithm):
        return None, f"the private key of {arn} cannot be loaded"


def _signing_key(profile):
    """The key that signs the profile's jobs, or None for the receipt, with a warning where AWS would sign."""
    platform_id = profile.get("platformId")
    if platform_id not in _ACM_SIGNED_PLATFORMS:
        return None
    if platform_id == _IOT_PLATFORM_ID:
        key, reason = _acm_private_key(profile)
    else:
        key, reason = None, f"the {platform_id} signed object format is not emulated"
    if key is None:
        logger.warning("Signer profile %s writes a receipt instead of a signature: %s",
                       profile.get("profileName"), reason)
    return key


def _iot_signed_object(key, data, src_bucket, src_key, version_id):
    """The compact JSON AWS writes on the IoT platform, or None for a non-EC key."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec

    if not isinstance(key, ec.EllipticCurvePrivateKey):
        return None
    signature = key.sign(data, ec.ECDSA(hashes.SHA256()))
    document = {
        "rawPayloadSize": len(data),
        "signature": base64.b64encode(signature).decode("ascii"),
        "signatureAlgorithm": "SHA256withECDSA",
        "payloadLocation": {"s3": {
            "bucketName": src_bucket,
            "key": src_key,
            "version": version_id,
        }},
    }
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _start_signing_job(body, headers=None):
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

    key = _signing_key(profile)
    failed = False
    if key is not None:
        signed_bytes = _iot_signed_object(key, data, src_bucket, src_key, version_id)
        # AWS fails the job and writes nothing.
        failed = signed_bytes is None
        content_type = "application/octet-stream"
    else:
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
        signed_bytes = json.dumps(marker, ensure_ascii=False).encode("utf-8")
        content_type = "application/json"
    if not failed:
        status, _headers, resp_body = s3_svc._put_object(
            dst_bucket, signed_key, signed_bytes,
            {"content-type": content_type,
             "content-length": str(len(signed_bytes))},
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
    # A failed job has no signedObject and no signatureExpiresAt.
    _jobs[job_id] = {
        "jobId": job_id,
        "source": {"s3": {
            "bucketName": src_bucket,
            "key": src_key,
            "version": version_id,
        }},
        "signedObject": None if failed else {"s3": {"bucketName": dst_bucket, "key": signed_key}},
        "profileName": profile_name,
        "profileVersion": profile.get("profileVersion"),
        "platformId": platform_id,
        "signingMaterial": profile.get("signingMaterial"),
        "overrides": profile.get("overrides"),
        "signingParameters": profile.get("signingParameters"),
        "signatureExpiresAt": None if failed else _signature_expires_at(
            now, profile.get("signatureValidityPeriod")
        ),
        "status": "Failed" if failed else "Succeeded",
        "statusReason": "can't identify EC private key." if failed else "Signing Succeeded",
        "createdAt": now,
        "completedAt": now,
        "requestedBy": _requested_by(headers),
        "jobOwner": account,
        "jobInvoker": account,
    }
    response = {"jobId": job_id, "jobOwner": account}
    if token:
        _remember_token(token, response)
    return json_response(response)


def _remember_token(token: str, response: dict) -> None:
    """Record a clientRequestToken's first response so a replay returns it.

    Bounded like ``_AUTHORIZER_CACHE_MAX`` in ``apigateway_v1``: the key is the
    caller's, so an unbounded map grows with every distinct token and is
    persisted with the rest of the service state. The oldest entries go first;
    replaying a token older than the bound re-runs the job, which is the same
    outcome as never having sent one.
    """
    if len(_tokens) >= _TOKEN_CACHE_MAX:
        for stale in list(_tokens)[: len(_tokens) - _TOKEN_CACHE_MAX + 1]:
            _tokens.pop(stale, None)
    _tokens[token] = dict(response)


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
    if name in _profiles:
        # A profile name is taken for good, Active or Canceled.
        return _coded_error(400, "ValidationException", "ProfileAlreadyExists",
                            f"Profile with name {name} already exists")
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
    shown = _drop_none({k: v for k, v in profile.items() if k != "policy"})
    # AWS reports the 135-month default on a profile put without a period.
    shown.setdefault("signatureValidityPeriod", dict(_DEFAULT_VALIDITY))
    return json_response(shown)


# Profile permissions live on the profile record under "policy".

_PERMISSION_ACTIONS = ("signer:StartSigningJob", "signer:GetSigningProfile",
                       "signer:RevokeSignature")
_STATEMENT_ID_RE = re.compile(r"[A-Za-z0-9_-]+")
_PERMISSION_PRINCIPAL_RE = re.compile(r"[0-9]{12}|arn:aws:iam::[0-9]{12}:role/.+")
_MAX_POLICY_BYTES = 2000


def _coded_error(status, error_type, code, message):
    """A signer error carrying the `code` member."""
    return error_response_json(error_type, message, status, extra={"code": code},
                               content_type=REST_JSON_CONTENT_TYPE)


def _policy_statement(profile, permission, expand_principal):
    principal = permission["principal"]
    if expand_principal and _ACCOUNT_ID_RE.fullmatch(principal):
        principal = f"arn:aws:iam::{principal}:root"
    statement = {"Sid": permission["statementId"], "Effect": "Allow",
                 "Principal": {"AWS": principal}, "Action": permission["action"],
                 "Resource": profile["arn"]}
    if permission.get("profileVersion"):
        statement["Condition"] = {
            "StringEquals": {"signer:ProfileVersion": permission["profileVersion"]}}
    return statement


def _policy_size(profile, permissions, expand_principal=True):
    """Compact JSON length of the statements (policySizeBytes, or the 2000-byte limit with principals as given)."""
    statements = [_policy_statement(profile, p, expand_principal) for p in permissions]
    return len(json.dumps(statements, separators=(",", ":")))


def _active_profile_for_permissions(name, verb):
    """(profile, None) for an Active profile, else (None, error)."""
    name_err = _validate_profile_name(name)
    if name_err:
        return None, name_err
    profile = _profiles.get(name)
    if profile is None:
        return None, _coded_error(404, "ResourceNotFoundException", "ProfileNotFound",
                                  f"SigningProfile with name {name} does not exist")
    if verb and profile.get("status") != "Active":
        return None, _coded_error(400, "ValidationException", "ProfileNotActive",
                                  f"Cannot {verb} a profile in {profile.get('status')} state")
    return profile, None


def _add_profile_permission(name, body):
    profile, err = _active_profile_for_permissions(name, "add permission to")
    if err:
        return err
    sid = body.get("statementId")
    if not isinstance(sid, str) or not sid:
        return _coded_error(400, "ValidationException", "ValidationException",
                            "statementId cannot be empty.")
    if not _STATEMENT_ID_RE.fullmatch(sid):
        return _coded_error(400, "ValidationException", "InvalidPermissionStatement",
                            "Statement ID must contain only alphanumeric characters, "
                            "underscores, and dashes")
    if len(sid) > 64:
        # The message says "less than 64", yet a 64-character id is accepted.
        return _coded_error(400, "ValidationException", "InvalidPermissionStatement",
                            "Statement ID must be less than 64 characters length")
    # Only the shape is checked. AWS also answers 404 "Principal not found"
    # for an account or role it cannot resolve, which is not emulated.
    principal = body.get("principal")
    if not isinstance(principal, str) or not _PERMISSION_PRINCIPAL_RE.fullmatch(principal):
        return _coded_error(400, "ValidationException", "InvalidPrincipal",
                            "Principal is not a valid AWS account id or IAM Role Arn")
    action = body.get("action")
    if action not in _PERMISSION_ACTIONS:
        return _coded_error(400, "ValidationException", "ValidationException",
                            f"Action {action} not supported for platform "
                            f"{profile.get('platformId')}. Action must be one of "
                            f"[{', '.join(_PERMISSION_ACTIONS)}]")
    version = body.get("profileVersion")
    if version is not None and version != profile.get("profileVersion"):
        return _coded_error(404, "ResourceNotFoundException", "ProfileVersionNotFound",
                            f"Version {version} does not exist for SigningProfile {name}.")
    permission = _drop_none({"action": action, "principal": principal,
                             "statementId": sid, "profileVersion": version})
    policy = profile.get("policy")
    if policy is not None:
        for existing in policy["statements"]:
            if existing["statementId"] != sid:
                continue
            # Repeating a statement exactly is a no-op answered with the
            # current revision, before any revisionId check.
            if existing == permission:
                return json_response({"revisionId": policy["revisionId"]})
            return _coded_error(400, "ValidationException", "InvalidPermissionStatement",
                                f"Statement with statementId {sid} is already in the policy")
    revision = body.get("revisionId")
    if policy is None and revision is not None:
        return _coded_error(400, "ValidationException", "ValidationException",
                            "Resource Policy does not exist but revisionId was "
                            "specified in the request")
    if policy is not None:
        if revision is None:
            return _coded_error(400, "ValidationException", "ValidationException",
                                f"Resource Policy exists for Profile Name {name}. "
                                "But no revisionId was provided in the request")
        if revision != policy["revisionId"]:
            return _revision_mismatch(revision, policy)
    statements = (policy["statements"] if policy else []) + [permission]
    if _policy_size(profile, statements, expand_principal=False) > _MAX_POLICY_BYTES:
        return _coded_error(402, "ServiceLimitExceededException", "ServiceLimitExceeded",
                            f"Policy cannot exceed maximum size of {_MAX_POLICY_BYTES} bytes")
    profile["policy"] = {"revisionId": str(uuid.uuid4()), "statements": statements}
    return json_response({"revisionId": profile["policy"]["revisionId"]})


def _revision_mismatch(revision, policy):
    return _coded_error(409, "ConflictException", "PolicyRevisionIdMismatch",
                        f"Specified revisionId ({revision}) does not match the "
                        f"revisionId of the existing policy ({policy['revisionId']})")


def _no_policy(name):
    return _coded_error(404, "ResourceNotFoundException", "PolicyNotFound",
                        f"No policies associated with profile {name}")


def _list_profile_permissions(name):
    """The whole policy in one page; nextToken is ignored and never returned."""
    profile, err = _active_profile_for_permissions(name, None)
    if err:
        return err
    policy = profile.get("policy")
    if policy is None:
        return _no_policy(name)
    return json_response({
        "revisionId": policy["revisionId"],
        "policySizeBytes": _policy_size(profile, policy["statements"]),
        "permissions": [dict(p) for p in policy["statements"]],
    })


def _remove_profile_permission(name, sid, revision):
    profile, err = _active_profile_for_permissions(name, "remove permission from")
    if err:
        return err
    policy = profile.get("policy")
    if policy is None:
        return _no_policy(name)
    if revision != policy["revisionId"]:
        return _revision_mismatch(revision, policy)
    remaining = [p for p in policy["statements"] if p["statementId"] != sid]
    if len(remaining) == len(policy["statements"]):
        return _coded_error(404, "ResourceNotFoundException", "PolicyNotFound",
                            f"No policy named {sid} associated with {name} profile")
    if remaining:
        profile["policy"] = {"revisionId": str(uuid.uuid4()), "statements": remaining}
        new_revision = profile["policy"]["revisionId"]
    else:
        # The last statement takes the policy with it: a listing is
        # PolicyNotFound again and the next add needs no revisionId.
        profile.pop("policy")
        new_revision = str(uuid.uuid4())
    return json_response({"revisionId": new_revision})


def _cancel_signing_profile(name):
    """Cancel a profile; its name stays taken and its permissions stay listable."""
    name_err = _validate_profile_name(name)
    if name_err:
        return name_err
    profile = _profiles.get(name)
    if profile is None:
        return _coded_error(404, "ResourceNotFoundException", "ProfileNotFound",
                            f"Signing Profile with name {name} does not exist.")
    if profile.get("status") == "Active" and profile.get("policy"):
        profile["policy"] = dict(profile["policy"], revisionId=str(uuid.uuid4()))
    profile["status"] = "Canceled"
    return json_response({})


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
        return _start_signing_job(body, headers)

    # GET /signing-jobs -- ListSigningJobs
    if path == "/signing-jobs" and method == "GET":
        return _list_signing_jobs(query)

    # GET /signing-jobs/{jobId} -- DescribeSigningJob
    if path.startswith("/signing-jobs/") and method == "GET":
        job_id = urllib.parse.unquote(path[len("/signing-jobs/"):])
        if job_id and "/" not in job_id:
            return _describe_signing_job(job_id)

    # PUT/GET/DELETE /signing-profiles/{profileName} -- PutSigningProfile /
    # GetSigningProfile / CancelSigningProfile
    if path.startswith("/signing-profiles/"):
        name = urllib.parse.unquote(path[len("/signing-profiles/"):])
        if name and "/" not in name:
            if method == "PUT":
                return _put_signing_profile(name, body)
            if method == "GET":
                return _get_signing_profile(name)
            if method == "DELETE":
                return _cancel_signing_profile(name)
        # /signing-profiles/{profileName}/permissions[/{statementId}] --
        # AddProfilePermission / ListProfilePermissions / RemoveProfilePermission
        parts = path[len("/signing-profiles/"):].split("/")
        if len(parts) in (2, 3) and parts[0] and parts[1] == "permissions":
            name = urllib.parse.unquote(parts[0])
            if len(parts) == 2 and method == "POST":
                return _add_profile_permission(name, body)
            if len(parts) == 2 and method == "GET":
                return _list_profile_permissions(name)
            if len(parts) == 3 and parts[2] and method == "DELETE":
                return _remove_profile_permission(
                    name, urllib.parse.unquote(parts[2]), query.get("revisionId"))

    return _error(400, "ValidationException", f"No route for {method} {path}")
