"""
S3 Service Emulator – AWS-compatible.
Supports: CreateBucket, DeleteBucket, ListBuckets, HeadBucket,
          PutObject, GetObject, GetObjectAttributes, DeleteObject, HeadObject, CopyObject,
          ListObjectsV1 (with Marker/NextMarker pagination),
          ListObjectsV2 (with ContinuationToken pagination),
          DeleteObjects (batch),
          Multipart Upload (Create, UploadPart, Complete, Abort, List, ListParts),
          Object Tagging (Get, Put, Delete),
          ListObjectVersions,
          Bucket sub-resources (Policy, Versioning, Encryption, Lifecycle,
          CORS, ACL, Tagging, Notification, Logging, Accelerate, RequestPayment,
          Website),
          Object Lock (PutObjectLockConfiguration, GetObjectLockConfiguration,
          PutObjectRetention, GetObjectRetention,
          PutObjectLegalHold, GetObjectLegalHold),
          Replication (PutBucketReplication, GetBucketReplication,
          DeleteBucketReplication),
          Range requests (206 Partial Content),
          Content-MD5 validation, encoding-type=url,
          x-amz-metadata-directive, x-amz-copy-source-if-match preconditions.
Storage: In-memory (optionally backed by S3_DATA_DIR).
"""

import base64
import contextvars
import copy
import datetime as _dt
import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import struct
import threading
import time
import zlib
from urllib.parse import parse_qs as _parse_qs
from urllib.parse import quote as url_quote
from urllib.parse import unquote as url_unquote
from urllib.parse import urlparse as _urlparse
from xml.etree.ElementTree import Element, ParseError, SubElement, tostring
from xml.sax.saxutils import escape as _esc

from defusedxml.ElementTree import fromstring

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
    iso_to_rfc7231,
    md5_hash,
    new_uuid,
    now_iso,
    set_request_account_id,
    set_request_region,
)

logger = logging.getLogger("s3")

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
XML_DECL = b'<?xml version="1.0" encoding="UTF-8"?>'

# ---------------------------------------------------------------------------
# In-memory storage
# ---------------------------------------------------------------------------
_buckets = AccountScopedDict()

_bucket_policies = AccountScopedDict()
_bucket_notifications = AccountScopedDict()
_bucket_tags = AccountScopedDict()
_bucket_versioning = AccountScopedDict()
_bucket_encryption = AccountScopedDict()
_bucket_lifecycle = AccountScopedDict()
_bucket_cors = AccountScopedDict()
_bucket_acl = AccountScopedDict()
_bucket_websites = AccountScopedDict()
_bucket_logging_config = AccountScopedDict()
_bucket_accelerate_config = AccountScopedDict()
_bucket_request_payment_config = AccountScopedDict()

_object_tags = AccountScopedDict()
_object_acl = AccountScopedDict()  # (bucket, key, version_id) -> stored ACL XML string
_object_versions = AccountScopedDict()  # (bucket, key) -> [{version_id, obj_record}, ...]

_bucket_object_lock = AccountScopedDict()
_bucket_replication = AccountScopedDict()
_object_retention = AccountScopedDict()
_object_legal_hold = AccountScopedDict()

_multipart_uploads = AccountScopedDict()
# Completed uploads, keyed by upload id -> the (status, headers, body) response.
# CompleteMultipartUpload is idempotent: a retry with the same upload id and
# parts returns the original 200 result rather than NoSuchUpload (which real S3
# only returns later, once the upload id is garbage-collected).
_completed_multipart_uploads = AccountScopedDict()

# ── Persistence (metadata only — object bodies are NOT persisted here) ────

# Module-level registry of per-bucket dicts that round-trip through s3.json.
# One entry per module global: adding a new _bucket_* dict means one line here,
# not two separate edits in get_state/restore_state. Must sit below every
# _bucket_* declaration above — the dict literal holds live references.
# Excludes _buckets (has bespoke objects-stripping + legacy fallback) and
# per-bucket keys like _ownership_controls / _public_access_block that live
# on _buckets[name] and travel with buckets_meta.
_PERSISTED_BUCKET_DICTS = {
    "bucket_versioning": _bucket_versioning,
    "bucket_notifications": _bucket_notifications,
    "bucket_tags": _bucket_tags,
    "bucket_policies": _bucket_policies,
    "bucket_encryption": _bucket_encryption,
    "bucket_lifecycle": _bucket_lifecycle,
    "bucket_cors": _bucket_cors,
    "bucket_acl": _bucket_acl,
    "bucket_websites": _bucket_websites,
    "bucket_logging_config": _bucket_logging_config,
    "bucket_accelerate_config": _bucket_accelerate_config,
    "bucket_request_payment_config": _bucket_request_payment_config,
    "bucket_object_lock": _bucket_object_lock,
    "bucket_replication": _bucket_replication,
}


def get_state():
    # Persist bucket metadata without object bodies.
    # Use _data directly to capture ALL accounts, not just the current one.
    buckets_meta = AccountScopedDict()
    for scoped_key, bkt in _buckets._data.items():
        meta = {k: v for k, v in bkt.items() if k != "objects"}
        buckets_meta._data[scoped_key] = meta
    state = {"buckets_meta": copy.deepcopy(buckets_meta)}
    for key, d in _PERSISTED_BUCKET_DICTS.items():
        state[key] = copy.deepcopy(d)
    return state


def restore_state(data):
    if not data:
        return
    bm = data.get("buckets_meta", {})
    if isinstance(bm, AccountScopedDict):
        # Restore all accounts' buckets directly via _data
        for scoped_key, meta in bm._data.items():
            if scoped_key not in _buckets._data:
                _buckets._data[scoped_key] = {**meta, "objects": {}}
    else:
        # Legacy plain-dict format (pre-multi-tenancy)
        for name, meta in bm.items():
            if name not in _buckets:
                _buckets[name] = {**meta, "objects": {}}
    for key, d in _PERSISTED_BUCKET_DICTS.items():
        d.update(data.get(key, {}))


try:
    _restored = load_state("s3")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/ministack-data/s3")
S3_PERSIST = os.environ.get("S3_PERSIST", "0") == "1"

# Headers preserved from PUT requests and returned on GET/HEAD.
_PRESERVED_HEADERS = (
    "cache-control",
    "content-disposition",
    "content-language",
    "expires",
    "x-amz-website-redirect-location",
)

# Per botocore/data/s3/2006-03-01/service-2.json (StorageClass enum).
_VALID_STORAGE_CLASSES = frozenset({
    "STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA",
    "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "OUTPOSTS",
    "GLACIER_IR", "SNOW", "EXPRESS_ONEZONE", "FSX_OPENZFS",
})


def _resolve_storage_class(headers: dict, default: str = "STANDARD"):
    """Read x-amz-storage-class. Returns (value, error_response_or_None)."""
    sc = headers.get("x-amz-storage-class", "")
    if not sc:
        return default, None
    if sc not in _VALID_STORAGE_CLASSES:
        return None, _error(
            "InvalidStorageClass",
            "The storage class you specified is not valid",
            400,
        )
    return sc, None


# ---------------------------------------------------------------------------
# Glacier restore (RestoreObject, x-amz-restore, s3:ObjectRestore:* events)
# ---------------------------------------------------------------------------

# Storage classes whose objects are unreadable until restored. GLACIER_IR is
# deliberately absent: instant retrieval is an active tier, readable directly,
# and RestoreObject against it fails ObjectAlreadyInActiveTierError like any
# other active class.
_ARCHIVE_STORAGE_CLASSES = frozenset({"GLACIER", "DEEP_ARCHIVE"})

# Simulated retrieval latency. Fixed on purpose: every caller passes through
# the ongoing-request="true" state it must handle against real AWS, and the
# emulator compresses Glacier's hours to the same few seconds for everyone.
_RESTORE_DELAY_SECONDS = 2.0


def _restore_expiry_epoch(start: float, days: int) -> float:
    """AWS expires the temporary copy at midnight UTC after the restore
    period (expiry-date always reads 00:00:00 GMT), not at an hour offset."""
    return float((int(start + days * 86400) // 86400 + 1) * 86400)


def _restore_expiry_iso(expires_at: float) -> str:
    return _dt.datetime.fromtimestamp(
        expires_at, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _sync_restore(bucket_name: str, key: str, obj: dict | None) -> str | None:
    """Derive the object's restore phase from its timestamps.

    Returns "in_progress", "restored", or None (no active restore). State is
    derived rather than flipped by a timer so it survives a restart under
    S3_PERSIST=1. A lapsed temporary copy reverts the object to archived here,
    on the next touch, and fires s3:ObjectRestore:Delete — an expiry is Days
    out, so it rides this lazy sweep instead of a sleeping thread."""
    restore = (obj or {}).get("restore")
    if not restore:
        return None
    now = time.time()
    if now < restore["available_at"]:
        return "in_progress"
    if now < restore["expires_at"]:
        return "restored"
    obj.pop("restore", None)
    _fire_s3_event_async(bucket_name, key, "s3:ObjectRestore:Delete",
                         size=obj.get("size", 0), etag=obj.get("etag", ""))
    return None


def _invalid_object_state(bucket_name: str, key: str, storage_class: str) -> tuple:
    """403 InvalidObjectState, carrying the StorageClass member the error
    shape models."""
    root = Element("Error")
    SubElement(root, "Code").text = "InvalidObjectState"
    SubElement(root, "Message").text = (
        "The operation is not valid for the object's storage class")
    SubElement(root, "StorageClass").text = storage_class
    SubElement(root, "Resource").text = f"/{bucket_name}/{key}"
    SubElement(root, "RequestId").text = new_uuid()
    return 403, {"Content-Type": "application/xml"}, _xml_body(root)


def _schedule_restore_completed(bucket_name: str, key: str,
                                requested_at: float) -> None:
    """Fire s3:ObjectRestore:Completed once the simulated retrieval lands.

    Same copied-context daemon-thread shape as _fire_s3_event_async (#876).
    The requested_at stamp guards the sleep window: if the object is deleted,
    overwritten, or the restore superseded meanwhile, the timer wakes up,
    sees a different stamp, and fires nothing."""
    ctx = contextvars.copy_context()

    def _worker():
        bucket = _buckets.get(bucket_name)
        obj = bucket["objects"].get(key) if bucket else None
        restore = (obj or {}).get("restore")
        if not restore or restore.get("requested_at") != requested_at:
            return
        delay = restore["available_at"] - time.time()
        if delay > 0:
            time.sleep(delay)
        bucket = _buckets.get(bucket_name)
        obj = bucket["objects"].get(key) if bucket else None
        restore = (obj or {}).get("restore")
        if not restore or restore.get("requested_at") != requested_at:
            return
        restore["completed_fired"] = True
        _fire_s3_event(
            bucket_name, key, "s3:ObjectRestore:Completed",
            size=obj.get("size", 0), etag=obj.get("etag", ""),
            restore_event_data={
                "lifecycleRestorationExpiryTime":
                    _restore_expiry_iso(restore["expires_at"]),
                "lifecycleRestoreStorageClass":
                    obj.get("storage_class") or "GLACIER",
            })

    t = threading.Thread(target=ctx.run, args=(_worker,), daemon=True)
    t.start()


def _schedule_restore_expiry(bucket_name: str, key: str,
                             expires_at: float) -> None:
    """Fire s3:ObjectRestore:Delete when the temporary copy lapses.

    Real S3 removes the restored copy and emits the event at expiry time, not
    on the next read, so a non-polling consumer still hears it. The expires_at
    stamp guards the sleep: an extension re-arms a new timer and the stale one
    wakes, sees a different stamp, and exits. The lazy check in _sync_restore
    stays as a belt-and-braces path for reads that race the timer."""
    ctx = contextvars.copy_context()

    def _worker():
        delay = expires_at - time.time()
        if delay > 0:
            time.sleep(delay)
        bucket = _buckets.get(bucket_name)
        obj = bucket["objects"].get(key) if bucket else None
        restore = (obj or {}).get("restore")
        if not restore or restore.get("expires_at") != expires_at:
            return
        _sync_restore(bucket_name, key, obj)

    t = threading.Thread(target=ctx.run, args=(_worker,), daemon=True)
    t.start()


def _restore_object(bucket_name: str, key: str, body: bytes) -> tuple:
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    obj = bucket["objects"].get(key)
    if obj is None:
        return _error("NoSuchKey", "The specified key does not exist.",
                      404, f"/{bucket_name}/{key}")

    sc = obj.get("storage_class") or "STANDARD"
    if sc == "GLACIER_IR":
        # Real AWS distinguishes instant retrieval from active tiers here:
        # restore on GLACIER_IR fails InvalidObjectState ("Restore is not
        # allowed..."), not ObjectAlreadyInActiveTierError.
        return _error(
            "InvalidObjectState",
            "Restore is not allowed for the object's current storage class.",
            403, f"/{bucket_name}/{key}")
    if sc not in _ARCHIVE_STORAGE_CLASSES:
        return _error(
            "ObjectAlreadyInActiveTierError",
            "This action is not allowed against this storage tier.",
            403, f"/{bucket_name}/{key}")

    days = None
    tier = "Standard"
    if body:
        try:
            root = fromstring(body)
        except ParseError:
            return _error(
                "MalformedXML",
                "The XML you provided was not well-formed or did not "
                "validate against our published schema",
                400, f"/{bucket_name}/{key}")
        for el in root.iter():
            name = el.tag.rsplit("}", 1)[-1]
            if name == "Days" and el.text:
                try:
                    days = int(el.text.strip())
                except ValueError:
                    days = None
            elif name == "Tier" and el.text:
                # Covers both GlacierJobParameters/Tier and top-level Tier.
                tier = el.text.strip()
    if days is None:
        # The Days element is required for regular (non-select) restores.
        return _error(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate "
            "against our published schema",
            400, f"/{bucket_name}/{key}")

    now = time.time()
    phase = _sync_restore(bucket_name, key, obj)
    if phase == "in_progress":
        return _error("RestoreAlreadyInProgress",
                      "Object restore is already in progress.",
                      409, f"/{bucket_name}/{key}")
    if phase == "restored":
        # Re-request while the copy is live: only the expiry moves, relative
        # to now, and AWS answers 200 rather than 202.
        obj["restore"]["expires_at"] = _restore_expiry_epoch(now, days)
        obj["restore"]["days"] = days
        _schedule_restore_expiry(bucket_name, key, obj["restore"]["expires_at"])
        return 200, {}, b""

    obj["restore"] = {
        "requested_at": now,
        "available_at": now + _RESTORE_DELAY_SECONDS,
        "expires_at": _restore_expiry_epoch(now + _RESTORE_DELAY_SECONDS, days),
        "days": days,
        "tier": tier,
        "completed_fired": False,
    }
    _fire_s3_event_async(bucket_name, key, "s3:ObjectRestore:Post",
                         size=obj.get("size", 0), etag=obj.get("etag", ""))
    _schedule_restore_completed(bucket_name, key, now)
    _schedule_restore_expiry(bucket_name, key, obj["restore"]["expires_at"])
    return 202, {}, b""


def _reschedule_restores() -> None:
    """Re-arm pending s3:ObjectRestore:Completed timers after a restart.

    Invoked once at the end of module import, after persisted state loads:
    a restore in flight when the process died fires on schedule, and one that
    landed while the process was down (completed_fired still False) fires
    immediately — the consumer it exists for wasn't polling. A copy that
    lapsed entirely while down is left to the lazy sweep, which fires
    s3:ObjectRestore:Delete on the next touch."""
    try:
        entries = list(_buckets._data.items())
    except Exception:
        return
    now = time.time()
    for (account_id, bucket_name), bucket in entries:
        for key, obj in list((bucket.get("objects") or {}).items()):
            restore = obj.get("restore")
            if not restore:
                continue
            # The workers copy their context at schedule time, so pin the
            # owning account around the calls (#876 shape).
            set_request_account_id(account_id)
            try:
                if now >= restore["expires_at"]:
                    # Lapsed while down: revert to archived and fire :Delete.
                    _sync_restore(bucket_name, key, obj)
                    continue
                if not restore.get("completed_fired"):
                    _schedule_restore_completed(bucket_name, key,
                                                restore["requested_at"])
                _schedule_restore_expiry(bucket_name, key,
                                         restore["expires_at"])
            finally:
                set_request_account_id("")


# ---------------------------------------------------------------------------
# Server-side encryption (SSE-S3 / SSE-KMS / SSE-C)
#
# MiniStack does not encrypt at rest — like the ACL plane, SSE is contract
# state: validated on write, persisted with the object, echoed on every
# response, and enforced on reads for SSE-C (whose whole point is that the
# object is unreadable without the key).  The state rides the object record's
# `preserved_headers`, so it follows versions, copies and multipart completes
# through the existing plumbing.  The customer key itself is NEVER stored —
# only its MD5, which is all AWS keeps visible too.
# ---------------------------------------------------------------------------

# The SSE headers persisted on an object (the customer KEY is deliberately
# not among them).
_SSE_STORED_HEADERS = (
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key-md5",
)


def _validate_sse_c_headers(c_alg: str, c_key: str, c_md5: str):
    """Validate an SSE-C header trio; None when coherent, an error otherwise.

    AWS requires all three headers together: the algorithm (AES256 only), a
    base64 256-bit key, and the base64 MD5 of the raw key bytes, which it
    checks to catch corruption in transit."""
    if not c_alg:
        return _error(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys "
            "must provide a valid encryption algorithm.", 400)
    if c_alg != "AES256":
        return _error(
            "InvalidArgument",
            f"Invalid x-amz-server-side-encryption-customer-algorithm value: {c_alg}", 400)
    if not c_key:
        return _error(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys "
            "must provide an appropriate secret key.", 400)
    try:
        raw_key = base64.b64decode(c_key, validate=True)
    except Exception:
        raw_key = b""
    if len(raw_key) != 32:
        return _error(
            "InvalidArgument",
            "The secret key was invalid for the specified algorithm.", 400)
    if not c_md5:
        return _error(
            "InvalidArgument",
            "Requests specifying Server Side Encryption with Customer provided keys "
            "must provide the object encryption key MD5.", 400)
    if base64.b64encode(hashlib.md5(raw_key).digest()).decode() != c_md5:
        return _error(
            "InvalidArgument",
            "The calculated MD5 hash of the key did not match the hash that was provided.", 400)
    return None


def _resolve_sse_write_headers(headers: dict, bucket_name: str):
    """Validate a write's SSE headers and return (headers_to_persist, error).

    SSE-S3/KMS and SSE-C are mutually exclusive (InvalidArgument together, as
    AWS answers).  `aws:kms` must name a key: MiniStack has no implicit
    account `aws/s3` KMS key to fall back to, and ceph/s3-tests pins the
    refusal.  A write with no SSE headers takes the bucket's explicit default
    encryption configuration, so the applied algorithm is visible on later
    reads and not only stamped on the PUT reply."""
    sse = headers.get("x-amz-server-side-encryption", "")
    c_alg = headers.get("x-amz-server-side-encryption-customer-algorithm", "")
    c_key = headers.get("x-amz-server-side-encryption-customer-key", "")
    c_md5 = headers.get("x-amz-server-side-encryption-customer-key-md5", "")

    if sse and (c_alg or c_key or c_md5):
        return None, _error(
            "InvalidArgument",
            "Server Side Encryption with Customer provided key is incompatible "
            "with the encryption method specified.", 400)
    if sse:
        if sse not in ("AES256", "aws:kms"):
            return None, _error(
                "InvalidArgument",
                f"Invalid x-amz-server-side-encryption value: {sse}", 400)
        out = {"x-amz-server-side-encryption": sse}
        if sse == "aws:kms":
            kms = headers.get("x-amz-server-side-encryption-aws-kms-key-id", "")
            if not kms:
                return None, _error(
                    "InvalidArgument",
                    "Server-side encryption with aws:kms requires a key id in "
                    "x-amz-server-side-encryption-aws-kms-key-id.", 400)
            out["x-amz-server-side-encryption-aws-kms-key-id"] = kms
        return out, None
    if c_alg or c_key or c_md5:
        err = _validate_sse_c_headers(c_alg, c_key, c_md5)
        if err is not None:
            return None, err
        return {
            "x-amz-server-side-encryption-customer-algorithm": "AES256",
            "x-amz-server-side-encryption-customer-key-md5": c_md5,
        }, None
    return dict(_bucket_default_sse_headers(bucket_name)), None


def _stored_sse_headers(obj: dict) -> dict:
    return {k: v for k, v in obj.get("preserved_headers", {}).items()
            if k in _SSE_STORED_HEADERS}


def _check_sse_c_against(stored_md5, c_alg: str, c_key: str, c_md5: str,
                         part: bool = False):
    """The SSE-C access rule shared by reads, copy sources and upload parts.

    An SSE-C object (or upload) is inaccessible without its key: the request
    must carry a coherent trio whose key MD5 matches the stored one.  Keyless
    and incoherent requests are 400s, but AWS splits the wrong key: a read
    answers 403 AccessDenied, a part request answers 400.  And a request
    offering SSE-C parameters for something not stored that way is refused
    too."""
    if stored_md5:
        if not (c_alg or c_key or c_md5):
            if part:
                return _error(
                    "InvalidRequest",
                    "The multipart upload initiate requested encryption. "
                    "Subsequent part requests must include the appropriate "
                    "encryption parameters.", 400)
            return _error(
                "InvalidRequest",
                "The object was stored using a form of Server Side Encryption. "
                "The correct parameters must be provided to retrieve the object.", 400)
        err = _validate_sse_c_headers(c_alg, c_key, c_md5)
        if err is not None:
            return err
        if c_md5 != stored_md5:
            if part:
                return _error(
                    "InvalidRequest",
                    "The provided encryption parameters did not match the ones "
                    "used originally.", 400)
            return _error(
                "AccessDenied",
                "Requests specifying Server Side Encryption with Customer "
                "provided keys must provide the correct secret key.", 403)
    elif c_alg or c_key or c_md5:
        return _error(
            "InvalidRequest",
            "The encryption parameters are not applicable to this object.", 400)
    return None


def _check_sse_read_headers(headers: dict, obj: dict):
    """Gate a GET/HEAD on the object's SSE state; None to proceed.

    x-amz-server-side-encryption is a write-request header — AWS rejects it
    on reads."""
    if headers.get("x-amz-server-side-encryption"):
        return _error(
            "InvalidArgument",
            "x-amz-server-side-encryption is not valid on a read request.", 400)
    return _check_sse_c_against(
        obj.get("preserved_headers", {}).get(
            "x-amz-server-side-encryption-customer-key-md5"),
        headers.get("x-amz-server-side-encryption-customer-algorithm", ""),
        headers.get("x-amz-server-side-encryption-customer-key", ""),
        headers.get("x-amz-server-side-encryption-customer-key-md5", ""))


def _check_sse_c_copy_source(headers: dict, src_obj: dict):
    """Gate reading a copy source, using the x-amz-copy-source-server-side-
    encryption-customer-* trio the way _check_sse_read_headers gates GET."""
    return _check_sse_c_against(
        src_obj.get("preserved_headers", {}).get(
            "x-amz-server-side-encryption-customer-key-md5"),
        headers.get("x-amz-copy-source-server-side-encryption-customer-algorithm", ""),
        headers.get("x-amz-copy-source-server-side-encryption-customer-key", ""),
        headers.get("x-amz-copy-source-server-side-encryption-customer-key-md5", ""))


def _check_sse_c_part(headers: dict, upload: dict):
    """Each part of an SSE-C multipart upload — and its completion — must
    carry the key the upload was initiated with; a part of a non-SSE-C
    upload must carry none."""
    return _check_sse_c_against(
        upload.get("preserved_headers", {}).get(
            "x-amz-server-side-encryption-customer-key-md5"),
        headers.get("x-amz-server-side-encryption-customer-algorithm", ""),
        headers.get("x-amz-server-side-encryption-customer-key", ""),
        headers.get("x-amz-server-side-encryption-customer-key-md5", ""),
        part=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
_IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


def _qp(params: dict, key: str, default: str = "") -> str:
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _xml_body(root: Element) -> bytes:
    return XML_DECL + b"\n" + tostring(root, encoding="unicode").encode("utf-8")


def _error(code: str, message: str, status: int, resource: str = "") -> tuple:
    root = Element("Error")
    SubElement(root, "Code").text = code
    SubElement(root, "Message").text = message
    SubElement(root, "Resource").text = resource
    SubElement(root, "RequestId").text = new_uuid()
    return status, {"Content-Type": "application/xml"}, _xml_body(root)


def _get_object_data(bucket_name: str, key: str, version_id: str | None = None) -> bytes | None:
    """Return raw object bytes, or None if not found. Used by Lambda S3 code fetch.

    When `version_id` is provided, returns the bytes for that specific
    version from `_object_versions` — matches AWS GetObject(VersionId)."""
    bucket = _buckets.get(bucket_name)
    if bucket is None:
        return None
    if version_id:
        for v in _object_versions.get((bucket_name, key), []):
            if v["version_id"] == version_id:
                data = v.get("data")
                if data is not None:
                    return data
                obj = bucket["objects"].get(key)
                return _read_body(bucket_name, key, obj) if obj else None
        return None
    obj = bucket["objects"].get(key)
    if obj is None:
        return None
    return _read_body(bucket_name, key, obj)


def _ensure_bucket(name: str):
    return _buckets.get(name)


def _no_such_bucket(name: str) -> tuple:
    return _error(
        "NoSuchBucket", "The specified bucket does not exist", 404, f"/{name}"
    )


def _validate_bucket_name(name: str) -> bool:
    if not name or len(name) < 3 or len(name) > 63:
        return False
    if not _BUCKET_NAME_RE.match(name):
        return False
    if ".." in name:
        return False
    if _IP_RE.match(name):
        return False
    return True


def _url_encode(value: str) -> str:
    # S3's `encoding-type=url` percent-encodes key names (spaces, `+`, unicode,
    # control chars) but leaves the forward slash intact, so a delimiter-collapsed
    # `CommonPrefixes`/`Delimiter`/`Key` keeps its `/` separators readable — matching
    # real S3 and RGW. Encoding `/` as %2F broke folder-tree listings. (#1322)
    return url_quote(value, safe="/")


def _parse_bucket_key(path: str, headers: dict):
    # Vhost extraction lives in app.py:_extract_s3_vhost_bucket, which
    # rewrites the path to /{bucket}{key} before this handler runs. By the
    # time we get here, every request is path-style.
    if path.startswith(("http://", "https://")):
        path = _urlparse(path).path
    parts = path.lstrip("/").split("/", 1)
    bucket = parts[0] if parts else ""
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _parse_range(range_header: str, total: int):
    m = re.match(r"bytes=(\d*)-(\d*)", range_header)
    if not m:
        return None
    s, e = m.group(1), m.group(2)
    if s == "" and e == "":
        return None
    if s == "":
        suffix = int(e)
        if suffix == 0:
            return None
        start = max(0, total - suffix)
        return start, total - 1
    start = int(s)
    if start >= total:
        return None
    end = int(e) if e else total - 1
    end = min(end, total - 1)
    if start > end:
        return None
    return start, end


_RESPONSE_OVERRIDE_PARAMS = (
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
)


def _apply_response_overrides(resp_headers: dict, query_params: dict) -> None:
    """Apply the six ``response-*`` GetObject query-string overrides to the
    outgoing response headers, matching real S3:

      response-cache-control       → Cache-Control
      response-content-disposition → Content-Disposition
      response-content-encoding    → Content-Encoding
      response-content-language    → Content-Language
      response-content-type        → Content-Type
      response-expires             → Expires

    Each override REPLACES the corresponding response header. Empty values
    are ignored (boto3 doesn't send the param if you pass ``None``). The
    signedness gate runs separately in ``_reject_response_overrides_if_unsigned``;
    by the time this helper runs we've already accepted the request.
    """
    overrides = (
        ("response-cache-control",       "Cache-Control"),
        ("response-content-disposition", "Content-Disposition"),
        ("response-content-encoding",    "Content-Encoding"),
        ("response-content-language",    "Content-Language"),
        ("response-content-type",        "Content-Type"),
        ("response-expires",             "Expires"),
    )
    for qkey, hkey in overrides:
        val = _qp(query_params, qkey, "")
        if not val:
            continue
        # Replace, don't append: the put-side preserved-headers dict stores
        # keys lowercased (cache-control, content-disposition, …) and the
        # default headers dict uses mixed case (Cache-Control). HTTP libs
        # serialise both, so without an explicit case-insensitive remove the
        # client sees the override appended to the original (e.g.
        # "max-age=600, no-store"). Strip every case variant of the target
        # before writing the override value.
        target_lc = hkey.lower()
        for existing in list(resp_headers):
            if existing.lower() == target_lc:
                del resp_headers[existing]
        resp_headers[hkey] = val


def _reject_response_overrides_if_unsigned(
    headers: dict, query_params: dict, bucket_name: str, key: str,
):
    """Implement the AWS rule for GetObject's six ``response-*`` query params.

    Per the AWS API reference:

      "When you use these parameters, you must sign the request by using
       either an Authorization header or a presigned URL. These parameters
       cannot be used with an unsigned (anonymous) request."

    A request is considered signed if it carries an ``Authorization`` header
    or a presigned-URL marker (``X-Amz-Signature`` / ``X-Amz-Algorithm`` in
    the query string — boto3 lower-cases query keys when calling our
    handlers, so check both cases). Otherwise the six ``response-*`` params
    are rejected with ``InvalidRequest`` (400) and the AWS-canonical message
    "Request specific response headers cannot be used for anonymous GET
    requests."
    """
    has_override = any(
        _qp(query_params, p, "") for p in _RESPONSE_OVERRIDE_PARAMS
    )
    if not has_override:
        return None
    if headers.get("authorization"):
        return None
    if (_qp(query_params, "X-Amz-Signature", "")
            or _qp(query_params, "x-amz-signature", "")
            or _qp(query_params, "X-Amz-Algorithm", "")
            or _qp(query_params, "x-amz-algorithm", "")):
        return None
    return _error(
        "InvalidRequest",
        "Request specific response headers cannot be used for anonymous GET requests.",
        400,
        f"/{bucket_name}/{key}",
    )


def _validate_content_md5(headers: dict, body: bytes):
    md5_header = headers.get("content-md5", "")
    if not md5_header:
        return None
    try:
        expected = base64.b64decode(md5_header, validate=True)
    except Exception:
        return _error(
            "InvalidDigest", "The Content-MD5 you specified is not valid.", 400
        )
    # A valid MD5 digest is exactly 16 bytes. A wrong-length (e.g. truncated) value
    # is malformed, so AWS answers InvalidDigest, not BadDigest (which is reserved
    # for a well-formed digest that simply doesn't match the body). (#1322)
    if len(expected) != 16:
        return _error(
            "InvalidDigest", "The Content-MD5 you specified is not valid.", 400
        )
    actual = hashlib.md5(body).digest()
    if expected != actual:
        return _error(
            "BadDigest",
            "The Content-MD5 you specified did not match what we received.",
            400,
        )
    return None


def _check_put_preconditions(headers: dict, existing_obj: dict | None):
    """Evaluate `If-Match` and `If-None-Match` on a conditional write.

    AWS S3 added native conditional writes — on PutObject and
    CompleteMultipartUpload alike — in November 2024:
      - `If-None-Match: "*"` — succeed only when no object exists at the key.
        Used to implement create-once semantics (idempotent writes, distributed
        leader election via S3, two-file pair serialization).
      - `If-None-Match: "<etag>"` — succeed only when the existing object's
        ETag does NOT match.
      - `If-Match: "*"` — succeed only when an object already exists.
      - `If-Match: "<etag>"` — succeed only when the existing object's ETag
        matches.

    Returns an error tuple when a condition is violated — 412
    PreconditionFailed, except that If-Match against a missing key is 404
    NoSuchKey — otherwise returns None and the caller proceeds with the write.

    ETag comparison strips surrounding quotes on both sides — S3 stores ETags
    with quotes but client code is inconsistent about including them.
    """
    if_none_match = headers.get("if-none-match", "").strip()
    if_match = headers.get("if-match", "").strip()

    if not if_none_match and not if_match:
        return None

    existing_etag = (
        existing_obj["etag"].strip('"') if existing_obj is not None else None
    )

    if if_none_match:
        # "*" form: any existing object violates the condition.
        if if_none_match == "*":
            if existing_obj is not None:
                return _error(
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    412,
                )
        # ETag form: existing object with matching ETag violates the condition.
        elif existing_obj is not None and if_none_match.strip('"') == existing_etag:
            return _error(
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
                412,
            )

    if if_match:
        if existing_obj is None:
            # AWS S3 specifically returns 404 (NoSuchKey) — not the RFC 7232
            # 412 — when If-Match targets a key that doesn't exist (or whose
            # current version is a delete marker), for the "*" form as well as
            # the ETag form.  Documented under "Conditional write behavior" in
            # the user guide:
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response
            return _error(
                "NoSuchKey",
                "The specified key does not exist.",
                404,
            )
        if if_match != "*" and if_match.strip('"') != existing_etag:
            return _error(
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
                412,
            )

    return None


def _etag_condition_matches(field_value: str, etag: str) -> bool:
    """Whether an `If-Match`/`If-None-Match` field value selects this object.

    The value is a comma-separated list of entity tags, or `*` for "any
    representation" — which is always satisfied here, since the caller has
    already resolved the object. A weak tag compares equal to its strong form:
    S3 only ever mints strong ETags, and If-None-Match uses the weak comparison
    anyway (RFC 9110 13.1.2). `etag` is expected unquoted.
    """
    if field_value == "*":
        return True
    for candidate in field_value.split(","):
        candidate = candidate.strip()
        if candidate[:2].upper() == "W/":
            candidate = candidate[2:]
        if candidate.strip('"') == etag:
            return True
    return False


def _not_modified(resp_headers: dict):
    """A 304 for a read whose caller already holds the current representation.

    Keeps the validators the 200 would have carried — the ETag above all, which
    is how the client confirms which representation it just revalidated — and
    drops the headers that describe a payload, since a 304 ends at the header
    section and can never carry one (RFC 9110 15.4.5).
    """
    payload_headers = {
        "content-length", "content-type", "content-encoding", "accept-ranges",
    }

    def describes_payload(name: str) -> bool:
        lowered = name.lower()
        # x-amz-checksum-* is a checksum of bytes this response does not carry.
        # boto3 asks for them by default and, seeing one, wraps the body in a
        # validating stream it then cannot parse — the same hazard that keeps
        # them off a 206 partial response.
        return lowered in payload_headers or lowered.startswith("x-amz-checksum-")

    return (
        304,
        {k: v for k, v in resp_headers.items() if not describes_payload(k)},
        b"",
    )


def _check_read_preconditions(headers: dict, obj: dict, resp_headers: dict):
    """Evaluate the four conditional-read headers on GetObject/HeadObject.

    Returns a finished response tuple — 412 PreconditionFailed when the request
    asked for a representation this object no longer is, or 304 Not Modified
    when the caller's copy is still current — otherwise None, and the read
    proceeds normally. Answering conditional reads is what lets caches, `curl
    -z`, and every "download only if it changed" client avoid refetching a body
    they already have.

    Precedence follows RFC 9110 13.2.2 and the AWS GetObject reference: the
    entity-tag header wins over its date counterpart, so If-Unmodified-Since is
    consulted only without If-Match, and If-Modified-Since only without
    If-None-Match.
    """
    _precond_msg = "At least one of the pre-conditions you specified did not hold"
    etag = (obj.get("etag") or "").strip('"')
    mtime = _object_mtime_dt(obj)

    if_match = (headers.get("if-match") or "").strip()
    if if_match:
        if not _etag_condition_matches(if_match, etag):
            return _error("PreconditionFailed", _precond_msg, 412)
    else:
        unmod = _parse_http_date(headers.get("if-unmodified-since", ""))
        # "Unmodified since" fails when the object was written after that time.
        if unmod and mtime and mtime > unmod:
            return _error("PreconditionFailed", _precond_msg, 412)

    if_none_match = (headers.get("if-none-match") or "").strip()
    if if_none_match:
        if _etag_condition_matches(if_none_match, etag):
            return _not_modified(resp_headers)
    else:
        mod = _parse_http_date(headers.get("if-modified-since", ""))
        # Not modified since that time means the caller's copy is still current.
        if mod and mtime and mtime <= mod:
            return _not_modified(resp_headers)

    return None


def _find_xml_tag(parent, tag_name, ns=S3_NS):
    el = parent.find("{%s}%s" % (ns, tag_name))
    if el is None:
        el = parent.find(tag_name)
    return el


def _iter_tag_pairs(xml_root):
    """Yield (key, value) for each <Tag> element, preserving duplicate keys."""
    for tag_el in xml_root.iter():
        local = tag_el.tag.split("}")[-1] if "}" in tag_el.tag else tag_el.tag
        if local == "Tag":
            key_text = val_text = None
            for child in tag_el:
                child_local = (
                    child.tag.split("}")[-1] if "}" in child.tag else child.tag
                )
                if child_local == "Key":
                    key_text = child.text
                elif child_local == "Value":
                    val_text = child.text
            if key_text is not None:
                yield key_text, val_text or ""

def _parse_tags_xml(body: bytes) -> dict:
    """Parse a <Tag> set into {key: value}. Duplicate keys collapse last-writer-wins."""
    return {key: value for key, value in _iter_tag_pairs(fromstring(body))}

def _duplicate_tag_error(xml_root, resource: str = ""):
    """Return the 500 InternalError that real S3 raises when a CreateBucket
    <Tags> body repeats a tag key, or None when every key is unique."""
    seen = set()
    for key, _value in _iter_tag_pairs(xml_root):
        if key in seen:
            return _error(
                "InternalError",
                "We encountered an internal error. Please try again.",
                500, resource,
            )
        seen.add(key)
    return None


def _validate_bucket_tags(tags: dict, resource: str = ""):
    """Validate an already-parsed {key: value} bucket tag set against the S3
    tag constraints. Returns an S3 error-response tuple on the first violation,
    or None when the tag set is valid:
      key   : 1-128 Unicode chars, cannot use the reserved "aws:" prefix
      value : 0-256 Unicode chars (an empty value is allowed)
      at most 50 tags per bucket.
    """
    for key, value in tags.items():
        if not (1 <= len(key) <= 128):
            return _error(
                "InvalidTag",
                "The TagKey you have provided is invalid",
                400, resource,
            )
        if len(value) > 256:
            return _error(
                "InvalidTag",
                "The TagValue you have provided is invalid",
                400, resource,
            )
        if key.startswith("aws:"):
            return _error(
                "InvalidTag",
                'User-defined tag keys can\'t start with "aws:". This prefix is '
                'reserved for system tags. Remove "aws:" from your tag keys and '
                "try again.",
                400, resource,
            )
    if len(tags) > 50:
        return _error(
            "BadRequest", "Bucket tag count cannot be greater than 50", 400, resource,
        )
    return None


def _extract_user_metadata(headers: dict) -> dict:
    meta = {}
    for k, v in headers.items():
        if k.lower().startswith("x-amz-meta-"):
            meta[k] = v
    return meta


def _build_object_record(body: bytes, headers: dict, etag: str = None,
                         checksums: dict | None = None) -> dict:
    content_type = headers.get("content-type", "application/octet-stream")
    content_encoding = headers.get("content-encoding")
    preserved = {}
    for h in _PRESERVED_HEADERS:
        val = headers.get(h)
        if val is not None:
            preserved[h] = val

    return {
        "body": body,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "etag": etag or f'"{md5_hash(body)}"',
        "last_modified": now_iso(),
        "size": len(body),
        "metadata": _extract_user_metadata(headers),
        "preserved_headers": preserved,
        "storage_class": headers.get("x-amz-storage-class") or "STANDARD",
        # AWS-shape checksums (SHA256 / SHA1 / CRC32 / CRC32C / CRC64NVME),
        # stored uppercase-keyed and base64-encoded per the S3 wire contract.
        # Surfaced on Get/HeadObject via the `x-amz-checksum-*` headers only
        # when the caller sends `x-amz-checksum-mode: ENABLED`.
        "checksums": checksums or {},
    }


# ---------------------------------------------------------------------------
# AWS-shape checksum handling (SHA256 / SHA1 / CRC32 / CRC32C / CRC64NVME)
# ---------------------------------------------------------------------------

_S3_CHECKSUM_HEADERS = ("crc32", "crc32c", "crc64nvme", "sha1", "sha256")

# CRC-64/NVME is the algorithm current AWS SDKs and the CLI checksum uploads
# with by default, so refusing it fails a stock `aws s3 cp` before it starts.
# It is plain arithmetic — the reflected form of polynomial 0xAD93D23594C93659
# with all-ones init and xorout — so a byte-at-a-time table keeps it in the
# stdlib instead of pulling in a native CRC library. Checked against the
# algorithm's published check value in tests: b"123456789" hashes to
# 0xAE8B14860A799888.
_CRC64NVME_POLY = 0x9A6C9329AC4BC9B5
_CRC64NVME_INIT = 0xFFFFFFFFFFFFFFFF


def _build_crc64nvme_table() -> tuple:
    table = []
    for byte in range(256):
        crc = byte
        for _ in range(8):
            crc = (crc >> 1) ^ _CRC64NVME_POLY if crc & 1 else crc >> 1
        table.append(crc)
    return tuple(table)


_CRC64NVME_TABLE = _build_crc64nvme_table()


def _crc64nvme(body: bytes) -> int:
    crc = _CRC64NVME_INIT
    table = _CRC64NVME_TABLE
    for byte in body:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ _CRC64NVME_INIT


def _compute_s3_checksum(algorithm: str, body: bytes) -> str | None:
    """Return base64-encoded checksum for the given AWS S3 algorithm name.

    SHA256 / SHA1 / CRC32 come from the stdlib and CRC64NVME from the table
    above. CRC32C is the one left: it needs the optional native `google-crc32c`
    that isn't bundled, so we return None for it and the caller refuses the
    request rather than storing a value it could never verify.
    """
    algo = (algorithm or "").upper().replace("_", "")
    if algo == "SHA256":
        return base64.b64encode(hashlib.sha256(body).digest()).decode()
    if algo == "SHA1":
        return base64.b64encode(hashlib.sha1(body).digest()).decode()
    if algo == "CRC32":
        crc = zlib.crc32(body) & 0xFFFFFFFF
        return base64.b64encode(struct.pack(">I", crc)).decode()
    if algo == "CRC64NVME":
        return base64.b64encode(struct.pack(">Q", _crc64nvme(body))).decode()
    return None


def _resolve_object_checksums(body: bytes, headers: dict):
    """Build the stored checksum dict and validate any client-supplied values.

    AWS PutObject contract:
      - `x-amz-checksum-{alg}` headers carry a client-computed value, which
        the server recomputes from the body and MUST reject with `BadDigest`
        (HTTP 400) when the two disagree.  A value that is never checked is
        worse than none: it round-trips on Get as though the body had been
        verified.
      - `x-amz-sdk-checksum-algorithm: ALG` asks the server to compute that
        algorithm; the resulting value is returned alongside the response.

    Ministack-specific: CRC32C requires an optional native library that the
    "no new dependencies" rule forbids us from adding. Rather than silently
    accept an unverifiable checksum (which would round-trip on Get without ever
    being validated against the body — a worse failure mode than refusing the
    request), we reject the put with a clear error. SHA256 / SHA1 / CRC32 /
    CRC64NVME work end-to-end.

    Returns ``(checksums_dict, error_response_or_None)``.
    """
    provided = {}
    unverifiable = set()
    for alg in _S3_CHECKSUM_HEADERS:
        val = headers.get(f"x-amz-checksum-{alg}")
        if val:
            provided[alg.upper()] = val
            if _compute_s3_checksum(alg.upper(), b"") is None:
                unverifiable.add(alg.upper())

    sdk_alg_raw = headers.get("x-amz-sdk-checksum-algorithm")
    if sdk_alg_raw:
        sdk_key = sdk_alg_raw.upper().replace("_", "")
        if _compute_s3_checksum(sdk_alg_raw, b"") is None:
            unverifiable.add(sdk_key)

    if unverifiable:
        return {}, _error(
            "InvalidRequest",
            (
                f"Checksum algorithm not supported in this ministack build: "
                f"{', '.join(sorted(unverifiable))}. Supported: SHA256, SHA1, "
                f"CRC32, CRC64NVME. CRC32C requires an optional native "
                f"dependency that ministack does not bundle; use CRC64NVME "
                f"instead, or omit the checksum header."
            ),
            400,
        )

    # Every supplied value is verified against the body, whether or not the
    # request also names an algorithm for the server to compute: the client
    # asked for the object to be checked, and an unverified value would be
    # stored and echoed as if it had been.
    checksums = dict(provided)
    for alg, supplied in provided.items():
        computed = _compute_s3_checksum(alg, body)
        if computed is not None and supplied != computed:
            return {}, _error(
                "BadDigest",
                f"The {alg} you specified did not match the calculated checksum.",
                400,
            )

    if sdk_alg_raw:
        sdk_key = sdk_alg_raw.upper().replace("_", "")
        computed = _compute_s3_checksum(sdk_alg_raw, body)
        if computed is not None:
            checksums[sdk_key] = computed
    return checksums, None


def _resolve_multipart_checksum_algorithm(headers: dict):
    """Read the algorithm CreateMultipartUpload names for its parts.

    Returns ``(algorithm_or_None, error_response_or_None)``.  An algorithm
    ministack cannot compute is refused here rather than at completion, where
    the caller has already uploaded every part.
    """
    raw = (headers.get("x-amz-checksum-algorithm")
           or headers.get("x-amz-sdk-checksum-algorithm"))
    if not raw:
        return None, None
    algorithm = raw.upper().replace("_", "")
    if _compute_s3_checksum(algorithm, b"") is None:
        return None, _error(
            "InvalidRequest",
            (
                f"Checksum algorithm not supported in this ministack build: "
                f"{algorithm}. Supported: SHA256, SHA1, CRC32, CRC64NVME."
            ),
            400,
        )
    return algorithm, None


def _resolve_part_checksums(body: bytes, headers: dict, upload: dict):
    """Build a part's checksum dict, validating whatever the caller supplied.

    A part carrying its own ``x-amz-checksum-*`` header is validated against
    the bytes the same way PutObject validates one.  A part that carries none
    still gets the upload's algorithm computed for it, since the completion
    builds the composite out of every part's digest and AWS asks the caller
    for one part checksum at a time rather than for all of them.

    Returns ``(checksums_dict, error_response_or_None)``.
    """
    checksums, err = _resolve_object_checksums(body, headers)
    if err:
        return {}, err

    algorithm = upload.get("checksum_algorithm")
    if algorithm and algorithm not in checksums:
        computed = _compute_s3_checksum(algorithm, body)
        if computed is not None:
            checksums[algorithm] = computed
    return checksums, None


def _composite_checksum(algorithm: str, part_checksums: list) -> str | None:
    """Build the multipart composite: the digest of the parts' digests.

    AWS hashes the raw part digests end to end, base64s the result and suffixes
    the part count -- so a composite reads as ``<digest>-<parts>`` and cannot be
    confused with the whole-object checksum of a single PUT.  Returns None when
    a part is missing its checksum, since a composite over some of the parts
    would be a value no caller could reproduce.
    """
    if not algorithm or not part_checksums:
        return None
    digests = b""
    for value in part_checksums:
        if not value:
            return None
        try:
            digests += base64.b64decode(value)
        except ValueError:  # binascii.Error subclasses it
            return None
    combined = _compute_s3_checksum(algorithm, digests)
    if combined is None:
        return None
    return f"{combined}-{len(part_checksums)}"


def _object_response_headers(obj: dict, bucket_name: str = "", key: str = "",
                             include_checksums: bool = False) -> dict:
    h = {
        "Content-Type": obj["content_type"],
        "ETag": obj["etag"],
        "Last-Modified": iso_to_rfc7231(obj["last_modified"]),
        "Content-Length": str(obj["size"]),
        "Accept-Ranges": "bytes",
    }
    if obj.get("content_encoding"):
        h["Content-Encoding"] = obj["content_encoding"]
    for k, val in obj.get("preserved_headers", {}).items():
        h[k] = val
    h.update(obj.get("metadata", {}))
    if obj.get("version_id"):
        h["x-amz-version-id"] = obj["version_id"]
    sc = obj.get("storage_class") or "STANDARD"
    if sc != "STANDARD":
        # AWS omits the header for STANDARD; SDKs default to STANDARD when absent.
        h["x-amz-storage-class"] = sc
    restore = obj.get("restore")
    if restore:
        now = time.time()
        if now < restore["available_at"]:
            h["x-amz-restore"] = 'ongoing-request="true"'
        elif now < restore["expires_at"]:
            expiry = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                   time.gmtime(restore["expires_at"]))
            h["x-amz-restore"] = (
                f'ongoing-request="false", expiry-date="{expiry}"')
    if bucket_name and key:
        retention = _object_retention.get((bucket_name, key))
        if retention:
            h["x-amz-object-lock-mode"] = retention["Mode"]
            h["x-amz-object-lock-retain-until-date"] = retention["RetainUntilDate"]
        hold = _object_legal_hold.get((bucket_name, key))
        if hold:
            h["x-amz-object-lock-legal-hold"] = hold
    # AWS only returns x-amz-checksum-* headers when the request opted in via
    # `x-amz-checksum-mode: ENABLED` — silent on Head/Get otherwise to match
    # the documented contract and avoid leaking checksums into clients that
    # didn't ask for them.
    if include_checksums:
        stored = obj.get("checksums") or {}
        for alg, val in stored.items():
            h[f"x-amz-checksum-{alg.lower()}"] = val
        if stored:
            # A single PutObject hashes the whole body; a completed multipart
            # object hashes its parts' digests, which AWS names COMPOSITE.
            h["x-amz-checksum-type"] = (obj.get("checksum_type")
                                        or "FULL_OBJECT")
    return h


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------


_SIGV4_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"


def _uri_encode(value: str, encode_slash: bool = True) -> str:
    """RFC3986 encoding per the SigV4 spec: unreserved chars (A-Za-z0-9-_.~)
    stay literal, everything else is percent-encoded. ``/`` is preserved in
    the canonical URI (path separators) and encoded everywhere else."""
    safe = "-_.~" + ("" if encode_slash else "/")
    return url_quote(value, safe=safe)


def _sigv4_signing_key(secret: str, date_stamp: str, region: str, service: str) -> bytes:
    def _h(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
    k_date = _h(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = _h(k_date, region)
    k_service = _h(k_region, service)
    return _h(k_service, "aws4_request")


def _resolve_presign_secret(access_key_id):
    """The secret a presigned URL was signed with.

    STS temporary credentials are signed with the unique secret STS issued (not
    the server's static one), so a presigned URL from an AssumeRole / session
    token would never recompute against ``AWS_SECRET_ACCESS_KEY``. STS records
    each issued secret by access key id; resolve it here, falling back to the
    static server secret for a long-term (non-session) credential.
    """
    try:
        from ministack.services import sts
        session = sts._sessions.get(access_key_id)
        if session and session.get("SecretAccessKey"):
            return session["SecretAccessKey"]
    except Exception:
        pass
    return os.environ.get("AWS_SECRET_ACCESS_KEY", "test")


def _verify_presigned_sigv4(method, path, headers, query_params):
    """Verify a SigV4 presigned S3 URL. Returns an error tuple for a bad
    signature, or None when the request is not a SigV4 presigned URL (header-
    signed and anonymous requests are handled elsewhere / left lax).

    MiniStack has no IAM secret store, so it verifies against its own secret
    (``AWS_SECRET_ACCESS_KEY``, default ``test``) — the same credential the
    server and its Lambda runtimes use. A URL signed with any other secret, or
    one whose signed headers (content-type, content-length, ...) were tampered
    with after signing, does not recompute to the same signature and is
    rejected with 403 SignatureDoesNotMatch, matching real S3.
    """
    signature = (_qp(query_params, "X-Amz-Signature", "")
                 or _qp(query_params, "x-amz-signature", ""))
    if not signature:
        return None  # not a presigned URL
    algorithm = (_qp(query_params, "X-Amz-Algorithm", "")
                 or _qp(query_params, "x-amz-algorithm", ""))
    if algorithm != "AWS4-HMAC-SHA256":
        return None  # only SigV4 presigned URLs are verified

    def _bad_signature():
        return _error(
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature "
            "you provided. Check your key and signing method.",
            403, path,
        )

    credential = (_qp(query_params, "X-Amz-Credential", "")
                  or _qp(query_params, "x-amz-credential", ""))
    amz_date = (_qp(query_params, "X-Amz-Date", "")
                or _qp(query_params, "x-amz-date", ""))
    signed_headers = (_qp(query_params, "X-Amz-SignedHeaders", "")
                      or _qp(query_params, "x-amz-signedheaders", ""))
    cred_parts = credential.split("/")
    if len(cred_parts) != 5 or not amz_date or not signed_headers:
        return _bad_signature()
    _akid, date_stamp, region, service, _terminator = cred_parts

    # Expiry: a presigned URL past X-Amz-Date + X-Amz-Expires is rejected by S3
    # with 403 AccessDenied "Request has expired", independent of the signature.
    expires = (_qp(query_params, "X-Amz-Expires", "")
               or _qp(query_params, "x-amz-expires", ""))
    if expires:
        try:
            signed_at = _dt.datetime.strptime(
                amz_date, "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=_dt.timezone.utc)
            if _dt.datetime.now(_dt.timezone.utc) > signed_at + _dt.timedelta(
                seconds=int(expires)
            ):
                return _error("AccessDenied", "Request has expired", 403, path)
        except (ValueError, TypeError):
            pass

    # Canonical query string: every query param except X-Amz-Signature,
    # RFC3986-encoded, sorted by encoded key then value.
    pairs = []
    for name, values in query_params.items():
        if name.lower() == "x-amz-signature":
            continue
        vlist = values if isinstance(values, list) else [values]
        for v in vlist:
            pairs.append((_uri_encode(name), _uri_encode(v)))
    pairs.sort()
    canonical_qs = "&".join(f"{k}={v}" for k, v in pairs)

    # Canonical headers: the signed headers, lowercased names, trimmed values.
    canonical_headers = ""
    for hname in (h for h in signed_headers.split(";") if h):
        raw = headers.get(hname, headers.get(hname.lower(), ""))
        canonical_headers += f"{hname.lower()}:{' '.join(str(raw).split())}\n"

    canonical_request = "\n".join([
        method,
        _uri_encode(path, encode_slash=False),
        canonical_qs,
        canonical_headers,
        signed_headers,
        _SIGV4_UNSIGNED_PAYLOAD,
    ])

    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        f"{date_stamp}/{region}/{service}/aws4_request",
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    secret = _resolve_presign_secret(_akid)
    signing_key = _sigv4_signing_key(secret, date_stamp, region, service)
    computed = hmac.new(signing_key, string_to_sign.encode("utf-8"),
                        hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed, signature):
        return _bad_signature()
    return None


# SigV4 / SigV2 presign machinery: query parameters that are part of the
# signature itself, never a hoisted request header.
_PRESIGN_SIGNING_PARAMS = {
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
    "x-amz-expires",
    "x-amz-signedheaders",
    "x-amz-signature",
    "x-amz-security-token",
    "x-amz-content-sha256",
}


def _merge_hoisted_amz_headers(headers: dict, query_params: dict) -> dict:
    """Fold a presigned URL's hoisted ``x-amz-*`` query params into headers.

    A presigned URL is handed to a caller who sends nothing but the URL and a
    body, so the ``x-amz-*`` headers the operation asked for — user metadata
    (``x-amz-meta-*``), ACL, storage class, tagging, SSE, copy source — cannot
    travel as headers. SigV4 allows them to be *hoisted* into the query string
    instead, where the signature covers them exactly as a signed header would,
    and several SDKs presign that way (some hoist every ``x-amz-`` header, some
    only what the operation set). Real S3 applies those params as the headers
    they stand for.

    MiniStack only ever looked at headers, so a presigned upload carrying
    metadata in its query string stored the object without any: the PUT
    succeeded and the metadata was silently dropped.

    An explicitly sent header always wins over its hoisted twin.
    """
    hoisted = None
    for name, values in query_params.items():
        lname = name.lower()
        if not lname.startswith("x-amz-") or lname in _PRESIGN_SIGNING_PARAMS:
            continue
        if lname in headers:
            continue
        value = values[0] if isinstance(values, list) else values
        if value is None:
            continue
        if hoisted is None:
            hoisted = dict(headers)
        hoisted[lname] = value
    return hoisted if hoisted is not None else headers


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict,
    signed_path: str | None = None,
) -> tuple:
    bucket, key = _parse_bucket_key(path, headers)

    # A virtual-hosted request is rewritten to path-style before it reaches here
    # (``/{bucket}{path}``), but the client signed the canonical URI it actually
    # sent (``{path}``, no bucket) against the vhost Host header. ``signed_path``
    # carries that original URI so the presigned signature recomputes correctly;
    # path-style requests pass None and verify against ``path`` as before.
    sig_error = _verify_presigned_sigv4(
        method, signed_path if signed_path is not None else path, headers, query_params)
    if sig_error is not None:
        status, resp_headers, resp_body = sig_error
        resp_headers.setdefault("x-amz-request-id", new_uuid())
        resp_headers.setdefault("x-amz-id-2", base64.b64encode(os.urandom(48)).decode())
        return status, resp_headers, resp_body

    # A presigned URL may carry its `x-amz-*` headers in the query string; fold
    # them back in before routing so metadata, ACL, storage class and friends
    # reach the handlers exactly as a header-signed request delivers them.
    headers = _merge_hoisted_amz_headers(headers, query_params)

    result = _dispatch(method, bucket, key, headers, body, query_params)

    status, resp_headers, resp_body = result
    resp_headers.setdefault("x-amz-request-id", new_uuid())
    resp_headers.setdefault("x-amz-id-2", base64.b64encode(os.urandom(48)).decode())

    # HEAD responses must not carry a body per HTTP/1.1 spec.
    if method == "HEAD":
        resp_body = b""

    return status, resp_headers, resp_body


def _dispatch(
    method: str, bucket: str, key: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    if method == "GET" and not bucket:
        return _list_buckets()

    # ---- Routes with key ----
    if key:
        if method == "GET":
            if "uploadId" in query_params:
                return _list_parts(bucket, key, query_params)
            if "tagging" in query_params:
                return _get_object_tagging(bucket, key, query_params)
            if "retention" in query_params:
                return _get_object_retention(bucket, key)
            if "legal-hold" in query_params:
                return _get_object_legal_hold(bucket, key)
            if "acl" in query_params:
                return _get_object_acl(bucket, key, query_params)
            if "attributes" in query_params:
                return _get_object_attributes(bucket, key, headers, query_params)
            return _get_object(bucket, key, headers, query_params)

        if method == "PUT":
            if "partNumber" in query_params and "uploadId" in query_params:
                if "x-amz-copy-source" in headers:
                    return _upload_part_copy(bucket, key, query_params, headers)
                return _upload_part(bucket, key, body, query_params, headers)
            if "tagging" in query_params:
                return _put_object_tagging(bucket, key, body, query_params)
            if "retention" in query_params:
                return _put_object_retention(bucket, key, body, headers)
            if "legal-hold" in query_params:
                return _put_object_legal_hold(bucket, key, body)
            if "acl" in query_params:
                return _put_object_acl(bucket, key, body, headers, query_params)
            if "x-amz-copy-source" in headers:
                return _copy_object(bucket, key, headers)
            return _put_object(bucket, key, body, headers)

        if method == "POST":
            if "restore" in query_params:
                return _restore_object(bucket, key, body)
            if "uploads" in query_params:
                return _create_multipart_upload(bucket, key, headers)
            if "uploadId" in query_params:
                return _complete_multipart_upload(bucket, key, body, query_params, headers)
            return _error(
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
                405,
            )

        if method == "HEAD":
            return _head_object(bucket, key, headers, query_params)

        if method == "DELETE":
            if "uploadId" in query_params:
                return _abort_multipart_upload(bucket, key, query_params)
            if "tagging" in query_params:
                return _delete_object_tagging(bucket, key, query_params)
            return _delete_object(bucket, key, headers, query_params)

        return _error(
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
            405,
        )

    # ---- Routes without key (bucket-level) ----
    if not bucket:
        return _error(
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
            405,
        )

    if method == "GET":
        if "uploads" in query_params:
            return _list_multipart_uploads(bucket, query_params)
        if "versions" in query_params:
            return _list_object_versions(bucket, query_params)
        if "list-type" in query_params and _qp(query_params, "list-type") == "2":
            return _list_objects_v2(bucket, query_params)
        if "location" in query_params:
            return _get_bucket_location(bucket)
        if "policy" in query_params:
            return _get_bucket_policy(bucket)
        if "versioning" in query_params:
            return _get_bucket_versioning(bucket)
        if "encryption" in query_params:
            return _get_bucket_encryption(bucket)
        if "logging" in query_params:
            return _get_bucket_logging(bucket)
        if "notification" in query_params:
            return _get_bucket_notification(bucket)
        if "tagging" in query_params:
            return _get_bucket_tagging(bucket)
        if "cors" in query_params:
            return _get_bucket_cors(bucket)
        if "acl" in query_params:
            return _get_bucket_acl(bucket)
        if "lifecycle" in query_params:
            return _get_bucket_lifecycle(bucket)
        if "accelerate" in query_params:
            return _get_bucket_accelerate(bucket)
        if "request-payment" in query_params:
            return _get_bucket_request_payment(bucket)
        if "website" in query_params:
            return _get_bucket_website(bucket)
        if "object-lock" in query_params:
            return _get_object_lock_configuration(bucket)
        if "replication" in query_params:
            return _get_bucket_replication(bucket)
        if "ownershipControls" in query_params:
            return _get_bucket_ownership_controls(bucket)
        if "publicAccessBlock" in query_params:
            return _get_public_access_block(bucket)
        return _list_objects_v1(bucket, query_params)

    if method == "PUT":
        if "policy" in query_params:
            return _put_bucket_policy(bucket, body)
        if "notification" in query_params:
            return _put_bucket_notification(bucket, body)
        if "tagging" in query_params:
            return _put_bucket_tagging(bucket, body)
        if "versioning" in query_params:
            return _put_bucket_versioning(bucket, body)
        if "encryption" in query_params:
            return _put_bucket_encryption(bucket, body)
        if "lifecycle" in query_params:
            return _put_bucket_lifecycle(bucket, body)
        if "cors" in query_params:
            return _put_bucket_cors(bucket, body)
        if "acl" in query_params:
            return _put_bucket_acl(bucket, body, headers)
        if "website" in query_params:
            return _put_bucket_website(bucket, body)
        if "logging" in query_params:
            return _put_bucket_logging(bucket, body)
        if "accelerate" in query_params:
            return _put_bucket_accelerate(bucket, body)
        if "requestPayment" in query_params:
            return _put_bucket_request_payment(bucket, body)
        if "object-lock" in query_params:
            return _put_object_lock_configuration(bucket, body)
        if "replication" in query_params:
            return _put_bucket_replication(bucket, body)
        if "ownershipControls" in query_params:
            return _put_bucket_ownership_controls(bucket, body)
        if "publicAccessBlock" in query_params:
            return _put_public_access_block(bucket, body)
        return _create_bucket(bucket, body, headers)

    if method == "DELETE":
        if "policy" in query_params:
            return _delete_bucket_policy(bucket)
        if "tagging" in query_params:
            return _delete_bucket_tagging(bucket)
        if "cors" in query_params:
            return _delete_bucket_cors(bucket)
        if "lifecycle" in query_params:
            return _delete_bucket_lifecycle(bucket)
        if "encryption" in query_params:
            return _delete_bucket_encryption(bucket)
        if "website" in query_params:
            return _delete_bucket_website(bucket)
        if "replication" in query_params:
            return _delete_bucket_replication(bucket)
        if "ownershipControls" in query_params:
            return _delete_bucket_ownership_controls(bucket)
        if "publicAccessBlock" in query_params:
            return _delete_public_access_block(bucket)
        return _delete_bucket(bucket)

    if method == "HEAD":
        return _head_bucket(bucket)

    if method == "POST":
        if "delete" in query_params:
            return _delete_objects(bucket, body, headers)
        if headers.get("content-type", "").startswith("multipart/form-data"):
            return _post_object(bucket, body, headers)
        return _error(
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
            405,
        )

    return _error(
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
        405,
    )


# ---------------------------------------------------------------------------
# Bucket operations
# ---------------------------------------------------------------------------


def _canonical_owner_id() -> str:
    """The S3 canonical user ID for the current account.

    Real S3 reports an opaque 64-character hex canonical ID (not the account
    ID) and returns the same value from every Owner / CanonicalUser field
    (ListBuckets, GetBucketAcl, GetObjectAcl, object listings). MiniStack
    derives a stable one from the account so those fields agree (#1459)."""
    return hashlib.sha256(get_account_id().encode()).hexdigest()


def _list_buckets():
    root = Element("ListAllMyBucketsResult", xmlns=S3_NS)
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = _canonical_owner_id()
    SubElement(owner, "DisplayName").text = "ministack"
    buckets_el = SubElement(root, "Buckets")
    for name, data in sorted(_buckets.items()):
        b = SubElement(buckets_el, "Bucket")
        SubElement(b, "Name").text = name
        SubElement(b, "CreationDate").text = data["created"]
        SubElement(b, "BucketRegion").text = data.get("region") or os.environ.get("MINISTACK_REGION", "us-east-1")
        SubElement(b, "BucketArn").text = f"arn:aws:s3:::{name}"
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _create_bucket(name: str, body: bytes, headers: dict = None):
    headers = headers or {}
    if not _validate_bucket_name(name):
        return _error(
            "InvalidBucketName", "The specified bucket is not valid.", 400, f"/{name}"
        )
    # A canned ACL supplied at CreateBucket time is validated up front and
    # stored below, so GetBucketAcl reflects it instead of dropping it.
    canned_acl = headers.get("x-amz-acl")
    if canned_acl and canned_acl not in _CANNED_BUCKET_ACLS:
        return _error("InvalidArgument", f"Invalid x-amz-acl value: {canned_acl}", 400)
    if name in _buckets:
        # Idempotent: same account already owns it — return 200 like real AWS
        return 200, {"Location": f"/{name}"}, b""

    region = None
    tags = {}
    if body:
        try:
            xml_root = fromstring(body)
            loc_el = _find_xml_tag(xml_root, "LocationConstraint")
            if loc_el is not None and loc_el.text:
                region = loc_el.text

            err = _duplicate_tag_error(xml_root, resource=f"/{name}")
            if err is not None:
                return err

            tags = _parse_tags_xml(body)
        except Exception:
            pass

    err = _validate_bucket_tags(tags, resource=f"/{name}")
    if err is not None:
        return err

    # No explicit LocationConstraint: the bucket lands in the region the
    # request was signed for, like real AWS.
    region = region or get_region()
    _buckets[name] = {"created": now_iso(), "objects": {}, "region": region}
    if tags:
        _bucket_tags[name] = tags
    if canned_acl:
        _bucket_acl[name] = _canned_acl_policy_xml(canned_acl, _canonical_owner_id())

    if headers.get("x-amz-bucket-object-lock-enabled", "").lower() == "true":
        _bucket_object_lock[name] = {"enabled": True, "default_retention": None}
        _bucket_versioning[name] = "Enabled"

    if S3_PERSIST:
        # Account-scope the on-disk dir to match where objects are actually
        # written (_object_disk_path / _persist_object). Omitting the account id
        # here created a spurious empty folder at DATA_DIR/<bucket> (#824).
        os.makedirs(os.path.join(DATA_DIR, get_account_id(), name), exist_ok=True)
    logger.info("S3 bucket created: %s%s", name, f" (region={region})" if region else "")
    return 200, {"Location": f"/{name}"}, b""


def _delete_bucket(name: str):
    bucket = _ensure_bucket(name)
    if bucket is None:
        return _no_such_bucket(name)
    if bucket["objects"]:
        return _error(
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
            409,
            f"/{name}",
        )
    del _buckets[name]
    _bucket_policies.pop(name, None)
    _bucket_notifications.pop(name, None)
    _bucket_tags.pop(name, None)
    _bucket_versioning.pop(name, None)
    _bucket_encryption.pop(name, None)
    _bucket_lifecycle.pop(name, None)
    _bucket_cors.pop(name, None)
    _bucket_acl.pop(name, None)
    _bucket_websites.pop(name, None)
    _bucket_logging_config.pop(name, None)
    _bucket_accelerate_config.pop(name, None)
    _bucket_request_payment_config.pop(name, None)
    _bucket_object_lock.pop(name, None)
    _bucket_replication.pop(name, None)
    for k in [k for k in _object_tags if k[0] == name]:
        del _object_tags[k]
    for k in [k for k in _object_acl if k[0] == name]:
        del _object_acl[k]
    for k in [k for k in _object_retention if k[0] == name]:
        del _object_retention[k]
    for k in [k for k in _object_legal_hold if k[0] == name]:
        del _object_legal_hold[k]
    if S3_PERSIST:
        _delete_persisted_bucket(name)
    return 204, {}, b""


def _head_bucket(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    return (
        200,
        {
            "Content-Type": "application/xml",
            "x-amz-bucket-region": _buckets[name].get("region") or os.environ.get("MINISTACK_REGION", "us-east-1"),
        },
        b"",
    )


def _get_bucket_location(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    root = Element("LocationConstraint", xmlns=S3_NS)
    region = _buckets[name].get("region") or os.environ.get("MINISTACK_REGION", "us-east-1")
    # AWS returns an empty LocationConstraint only for us-east-1 buckets;
    # every other region is echoed back verbatim.
    if region != "us-east-1":
        root.text = region
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


# ---------------------------------------------------------------------------
# Bucket sub-resources
# ---------------------------------------------------------------------------


def _get_bucket_policy(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    policy = _bucket_policies.get(name)
    if not policy:
        return _error(
            "NoSuchBucketPolicy", "The bucket policy does not exist", 404, f"/{name}"
        )
    return 200, {"Content-Type": "application/json"}, policy.encode("utf-8")


def _put_bucket_policy(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_policies[name] = body.decode("utf-8")
    return 204, {}, b""


def _delete_bucket_policy(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_policies.pop(name, None)
    return 204, {}, b""


def _get_bucket_versioning(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    root = Element("VersioningConfiguration", xmlns=S3_NS)
    status = _bucket_versioning.get(name)
    if status:
        SubElement(root, "Status").text = status
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_versioning(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    try:
        xml_root = fromstring(body)
        status_el = _find_xml_tag(xml_root, "Status")
        if status_el is not None and status_el.text:
            _bucket_versioning[name] = status_el.text
    except Exception:
        pass
    return 200, {}, b""


def _get_bucket_encryption(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    config = _bucket_encryption.get(name)
    if config:
        return 200, {"Content-Type": "application/xml"}, config
    # Since 5 Jan 2023 every S3 bucket has SSE-S3 (AES256) default encryption, so
    # GetBucketEncryption returns that default configuration rather than the
    # historical ServerSideEncryptionConfigurationNotFoundError when nothing was
    # explicitly PUT.
    default = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<ServerSideEncryptionConfiguration xmlns="{S3_NS}">'
        "<Rule><ApplyServerSideEncryptionByDefault>"
        "<SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault>"
        "<BucketKeyEnabled>false</BucketKeyEnabled></Rule>"
        "</ServerSideEncryptionConfiguration>"
    )
    return 200, {"Content-Type": "application/xml"}, default


def _put_bucket_encryption(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_encryption[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _delete_bucket_encryption(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_encryption.pop(name, None)
    return 204, {}, b""


def _bucket_default_sse_headers(bucket_name: str) -> dict:
    """SSE headers a PutObject echoes when the bucket has explicit default
    encryption (PutBucketEncryption). Real S3 stamps the applied algorithm on the
    reply (`x-amz-server-side-encryption`, plus the KMS key id for `aws:kms`), which
    lets a client confirm the object was encrypted as configured. (#1322)"""
    raw = _bucket_encryption.get(bucket_name)
    if not raw:
        return {}
    try:
        root = fromstring(raw)
    except Exception:
        return {}

    def _first_text(local_name):
        # SSEAlgorithm / KMSMasterKeyID sit under Rule/ApplyServerSideEncryptionByDefault,
        # so search descendants by local name (namespace-agnostic), not direct children.
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag == local_name and el.text and el.text.strip():
                return el.text.strip()
        return None

    algo = _first_text("SSEAlgorithm")
    if not algo:
        return {}
    out = {"x-amz-server-side-encryption": algo}
    if algo == "aws:kms":
        kms = _first_text("KMSMasterKeyID")
        if kms:
            out["x-amz-server-side-encryption-aws-kms-key-id"] = kms
    return out


def _get_bucket_lifecycle(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    rules = _bucket_lifecycle.get(name)
    if rules is not None:
        xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml += '<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        for rule in rules:
            xml += "<Rule>"
            if rule.get("ID"):
                xml += f"<ID>{_esc(rule['ID'])}</ID>"
            # Filter
            filt = rule.get("Filter", {})
            xml += "<Filter>"
            if "Prefix" in filt:
                xml += f"<Prefix>{_esc(filt['Prefix'])}</Prefix>"
            if "Tag" in filt:
                xml += f"<Tag><Key>{_esc(filt['Tag']['Key'])}</Key><Value>{_esc(filt['Tag']['Value'])}</Value></Tag>"
            if "ObjectSizeGreaterThan" in filt:
                xml += f"<ObjectSizeGreaterThan>{filt['ObjectSizeGreaterThan']}</ObjectSizeGreaterThan>"
            if "ObjectSizeLessThan" in filt:
                xml += f"<ObjectSizeLessThan>{filt['ObjectSizeLessThan']}</ObjectSizeLessThan>"
            if "And" in filt:
                and_f = filt["And"]
                xml += "<And>"
                # AWS always echoes Prefix (empty when unset) and ObjectSizeGreaterThan
                # (0 when unset) inside an And operator; omitting either breaks the
                # aws-sdk-go equality waiter (the provider expands them as "" / 0).
                xml += f"<Prefix>{_esc(and_f.get('Prefix', ''))}</Prefix>"
                for tag in and_f.get("Tags", []):
                    xml += f"<Tag><Key>{_esc(tag['Key'])}</Key><Value>{_esc(tag['Value'])}</Value></Tag>"
                xml += f"<ObjectSizeGreaterThan>{and_f.get('ObjectSizeGreaterThan', 0)}</ObjectSizeGreaterThan>"
                if and_f.get("ObjectSizeLessThan"):
                    xml += f"<ObjectSizeLessThan>{and_f['ObjectSizeLessThan']}</ObjectSizeLessThan>"
                xml += "</And>"
            xml += "</Filter>"
            xml += f"<Status>{rule.get('Status', 'Enabled')}</Status>"
            for t in rule.get("Transitions", []):
                xml += "<Transition>"
                if "Days" in t:
                    xml += f"<Days>{t['Days']}</Days>"
                if "Date" in t:
                    xml += f"<Date>{t['Date']}</Date>"
                xml += f"<StorageClass>{t.get('StorageClass', 'STANDARD_IA')}</StorageClass>"
                xml += "</Transition>"
            for t in rule.get("NoncurrentVersionTransitions", []):
                xml += "<NoncurrentVersionTransition>"
                if "NoncurrentDays" in t:
                    xml += f"<NoncurrentDays>{t['NoncurrentDays']}</NoncurrentDays>"
                if "NewerNoncurrentVersions" in t:
                    xml += f"<NewerNoncurrentVersions>{t['NewerNoncurrentVersions']}</NewerNoncurrentVersions>"
                xml += f"<StorageClass>{t.get('StorageClass', 'STANDARD_IA')}</StorageClass>"
                xml += "</NoncurrentVersionTransition>"
            if "Expiration" in rule:
                exp = rule["Expiration"]
                xml += "<Expiration>"
                if "Days" in exp:
                    xml += f"<Days>{exp['Days']}</Days>"
                if "Date" in exp:
                    xml += f"<Date>{exp['Date']}</Date>"
                if "ExpiredObjectDeleteMarker" in exp:
                    xml += f"<ExpiredObjectDeleteMarker>{str(exp['ExpiredObjectDeleteMarker']).lower()}</ExpiredObjectDeleteMarker>"
                xml += "</Expiration>"
            if "NoncurrentVersionExpiration" in rule:
                nve = rule["NoncurrentVersionExpiration"]
                xml += "<NoncurrentVersionExpiration>"
                if "NoncurrentDays" in nve:
                    xml += f"<NoncurrentDays>{nve['NoncurrentDays']}</NoncurrentDays>"
                if "NewerNoncurrentVersions" in nve:
                    xml += f"<NewerNoncurrentVersions>{nve['NewerNoncurrentVersions']}</NewerNoncurrentVersions>"
                xml += "</NoncurrentVersionExpiration>"
            if rule.get("AbortIncompleteMultipartUpload"):
                aimu = rule["AbortIncompleteMultipartUpload"]
                xml += "<AbortIncompleteMultipartUpload>"
                xml += f"<DaysAfterInitiation>{aimu.get('DaysAfterInitiation', 7)}</DaysAfterInitiation>"
                xml += "</AbortIncompleteMultipartUpload>"
            xml += "</Rule>"
        xml += "</LifecycleConfiguration>"
        return 200, {
            "Content-Type": "application/xml",
            "x-amz-transition-default-minimum-object-size": "all_storage_classes_128K",
        }, xml.encode()
    return _error(
        "NoSuchLifecycleConfiguration",
        "The lifecycle configuration does not exist",
        404,
        f"/{name}",
    )


def _put_bucket_lifecycle(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    # Parse incoming XML into structured rules for canonical GET responses.
    rules = []
    try:
        from defusedxml import ElementTree as ET
        root = ET.fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        for rule_el in root.findall("Rule", ns) or root.findall("s3:Rule", ns):
            rule: dict = {}

            def _lc_text(el, tag):
                return el.findtext(tag) or el.findtext(f"s3:{tag}", namespaces=ns) or ""

            def _lc_find(el, tag):
                return el.find(tag) or el.find(f"s3:{tag}", ns)

            def _lc_findall(el, tag):
                return el.findall(tag) or el.findall(f"s3:{tag}", ns)

            id_val = _lc_text(rule_el, "ID")
            if id_val:
                rule["ID"] = id_val
            rule["Status"] = _lc_text(rule_el, "Status") or "Enabled"
            # Filter
            filt_el = _lc_find(rule_el, "Filter")
            filt: dict = {}
            if filt_el is not None:
                prefix = _lc_text(filt_el, "Prefix")
                if prefix or _lc_find(filt_el, "Prefix") is not None:
                    filt["Prefix"] = prefix
                tag_el = _lc_find(filt_el, "Tag")
                if tag_el is not None:
                    filt["Tag"] = {"Key": _lc_text(tag_el, "Key"), "Value": _lc_text(tag_el, "Value")}
                f_gt = _lc_text(filt_el, "ObjectSizeGreaterThan")
                if f_gt:
                    filt["ObjectSizeGreaterThan"] = int(f_gt)
                f_lt = _lc_text(filt_el, "ObjectSizeLessThan")
                if f_lt:
                    filt["ObjectSizeLessThan"] = int(f_lt)
                and_el = _lc_find(filt_el, "And")
                if and_el is not None:
                    and_data: dict = {}
                    p = _lc_text(and_el, "Prefix")
                    if p or _lc_find(and_el, "Prefix") is not None:
                        and_data["Prefix"] = p
                    tags = []
                    for t in _lc_findall(and_el, "Tag"):
                        tags.append({"Key": _lc_text(t, "Key"), "Value": _lc_text(t, "Value")})
                    if tags:
                        and_data["Tags"] = tags
                    gt = _lc_text(and_el, "ObjectSizeGreaterThan")
                    if gt:
                        and_data["ObjectSizeGreaterThan"] = int(gt)
                    lt = _lc_text(and_el, "ObjectSizeLessThan")
                    if lt:
                        and_data["ObjectSizeLessThan"] = int(lt)
                    filt["And"] = and_data
            rule["Filter"] = filt
            # Transitions
            transitions = []
            for t in _lc_findall(rule_el, "Transition"):
                td: dict = {}
                days = _lc_text(t, "Days")
                if days:
                    td["Days"] = int(days)
                date = _lc_text(t, "Date")
                if date:
                    td["Date"] = date
                td["StorageClass"] = _lc_text(t, "StorageClass") or "STANDARD_IA"
                transitions.append(td)
            if transitions:
                rule["Transitions"] = transitions
            # NoncurrentVersionTransitions
            nv_transitions = []
            for t in _lc_findall(rule_el, "NoncurrentVersionTransition"):
                td = {}
                days = _lc_text(t, "NoncurrentDays")
                if days:
                    td["NoncurrentDays"] = int(days)
                newer = _lc_text(t, "NewerNoncurrentVersions")
                if newer:
                    td["NewerNoncurrentVersions"] = int(newer)
                td["StorageClass"] = _lc_text(t, "StorageClass") or "STANDARD_IA"
                nv_transitions.append(td)
            if nv_transitions:
                rule["NoncurrentVersionTransitions"] = nv_transitions
            # Expiration
            exp_el = _lc_find(rule_el, "Expiration")
            if exp_el is not None:
                exp: dict = {}
                days = _lc_text(exp_el, "Days")
                if days:
                    exp["Days"] = int(days)
                date = _lc_text(exp_el, "Date")
                if date:
                    exp["Date"] = date
                eodm = _lc_text(exp_el, "ExpiredObjectDeleteMarker")
                if eodm:
                    exp["ExpiredObjectDeleteMarker"] = eodm.lower() == "true"
                rule["Expiration"] = exp
            # NoncurrentVersionExpiration
            nve_el = _lc_find(rule_el, "NoncurrentVersionExpiration")
            if nve_el is not None:
                nve: dict = {}
                days = _lc_text(nve_el, "NoncurrentDays")
                if days:
                    nve["NoncurrentDays"] = int(days)
                newer = _lc_text(nve_el, "NewerNoncurrentVersions")
                if newer:
                    nve["NewerNoncurrentVersions"] = int(newer)
                rule["NoncurrentVersionExpiration"] = nve
            # AbortIncompleteMultipartUpload
            aimu_el = _lc_find(rule_el, "AbortIncompleteMultipartUpload")
            if aimu_el is not None:
                days = _lc_text(aimu_el, "DaysAfterInitiation")
                rule["AbortIncompleteMultipartUpload"] = {
                    "DaysAfterInitiation": int(days) if days else 7
                }
            rules.append(rule)
    except Exception:
        # Fallback: store raw if parsing fails
        _bucket_lifecycle[name] = body.decode("utf-8", errors="replace")
        return 200, {"x-amz-transition-default-minimum-object-size": "all_storage_classes_128K"}, b""
    _bucket_lifecycle[name] = rules
    return 200, {"x-amz-transition-default-minimum-object-size": "all_storage_classes_128K"}, b""


def _delete_bucket_lifecycle(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_lifecycle.pop(name, None)
    return 204, {}, b""


def _get_bucket_cors(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    config = _bucket_cors.get(name)
    if config:
        return 200, {"Content-Type": "application/xml"}, config
    return _error(
        "NoSuchCORSConfiguration",
        "The CORS configuration does not exist",
        404,
        f"/{name}",
    )


def _put_bucket_cors(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_cors[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _delete_bucket_cors(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_cors.pop(name, None)
    return 204, {}, b""


def _get_bucket_acl(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_acl.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    _oid = _canonical_owner_id().encode()
    body = (
        XML_DECL + b"\n"
        b'<AccessControlPolicy xmlns="' + S3_NS.encode() + b'">'
        b"<Owner><ID>" + _oid + b"</ID><DisplayName>ministack</DisplayName></Owner>"
        b"<AccessControlList><Grant>"
        b'<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">'
        b"<ID>" + _oid + b"</ID><DisplayName>ministack</DisplayName></Grantee>"
        b"<Permission>FULL_CONTROL</Permission>"
        b"</Grant></AccessControlList></AccessControlPolicy>"
    )
    return 200, {"Content-Type": "application/xml"}, body


def _put_bucket_acl(name: str, body: bytes, headers: dict | None = None):
    headers = headers or {}
    if name not in _buckets:
        return _no_such_bucket(name)

    # Canned ACL from x-amz-acl header takes precedence and is mutually
    # exclusive with an XML body per the AWS API reference. SDK callers send
    # the canned value as a header with an EMPTY body, so ignoring the header
    # silently dropped every `put-bucket-acl --acl public-read`. Either path
    # stores the resulting policy XML so GetBucketAcl round-trips the value
    # the caller set, matching what real AWS would return.
    canned = headers.get("x-amz-acl")
    if canned:
        if canned not in _CANNED_BUCKET_ACLS:
            return _error("InvalidArgument",
                          f"Invalid x-amz-acl value: {canned}", 400)
        _bucket_acl[name] = _canned_acl_policy_xml(canned, _canonical_owner_id())
        return 200, {}, b""

    if not body:
        return _error("MissingSecurityHeader",
                      "Your request was missing a required header.", 400)
    try:
        # Validate XML well-formedness — real AWS rejects malformed bodies
        # with MalformedACLError. We don't enforce grantee/permission
        # semantics on the data plane, so any well-formed AccessControlPolicy
        # is accepted and round-tripped verbatim.
        fromstring(body)
    except Exception:
        return _error("MalformedACLError",
                      "The XML you provided was not well-formed or did not validate "
                      "against our published schema.", 400)
    _bucket_acl[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _get_bucket_tagging(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    tags = _bucket_tags.get(name)
    if not tags:
        return _error("NoSuchTagSet", "The TagSet does not exist", 404, f"/{name}")
    root = Element("Tagging", xmlns=S3_NS)
    tag_set = SubElement(root, "TagSet")
    for k, v in tags.items():
        tag = SubElement(tag_set, "Tag")
        SubElement(tag, "Key").text = k
        SubElement(tag, "Value").text = v
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_tagging(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    try:
        tags = _parse_tags_xml(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)
    if len(tags) > 50:
        return _error("BadRequest", "Object tags cannot be greater than 50", 400)
    _bucket_tags[name] = tags
    return 204, {}, b""


def _delete_bucket_tagging(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_tags.pop(name, None)
    return 204, {}, b""


def _put_bucket_ownership_controls(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _buckets[name]["_ownership_controls"] = body.decode("utf-8", errors="replace")
    _buckets[name].pop("_ownership_controls_deleted", None)
    return 200, {}, b""


def _get_bucket_ownership_controls(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _buckets[name].get("_ownership_controls")
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    if _buckets[name].get("_ownership_controls_deleted"):
        # Explicitly deleted: real S3 returns 404 (not a default block) so the
        # Terraform delete waiter can complete.
        return _error(
            "OwnershipControlsNotFoundError",
            "The bucket ownership controls were not found",
            404,
            f"/{name}",
        )
    # Never configured: real S3 reports the default Object Ownership.
    root = Element("OwnershipControls", xmlns=S3_NS)
    rule = SubElement(root, "Rule")
    SubElement(rule, "ObjectOwnership").text = "BucketOwnerEnforced"
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _delete_bucket_ownership_controls(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _buckets[name].pop("_ownership_controls", None)
    _buckets[name]["_ownership_controls_deleted"] = True
    return 204, {}, b""


def _put_public_access_block(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _buckets[name]["_public_access_block"] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _get_public_access_block(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _buckets[name].get("_public_access_block")
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    # No configuration set (never put, or deleted): real S3 returns 404 rather
    # than a default block, so DeletePublicAccessBlock is observable and the
    # Terraform delete waiter can complete.
    return _error(
        "NoSuchPublicAccessBlockConfiguration",
        "The public access block configuration was not found",
        404,
        f"/{name}",
    )


def _delete_public_access_block(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _buckets[name].pop("_public_access_block", None)
    return 204, {}, b""


def _get_bucket_notification(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_notifications.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    root = Element("NotificationConfiguration", xmlns=S3_NS)
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_notification(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    raw = body.decode("utf-8", errors="replace")
    configs = _parse_notification_config_raw(raw)
    bucket_region = _notification_bucket_region(name)
    validation_error = _validate_notification_configs(configs, bucket_region)
    if validation_error:
        return validation_error
    _bucket_notifications[name] = raw
    # Fire the s3:TestEvent synchronously so it's delivered before PutBucketNotification
    # returns — matches AWS's effective behaviour and avoids a race where the
    # client polls the destination queue/topic before the background thread has
    # delivered the message (also loses the caller's account contextvar across
    # threads, which broke multi-tenant tests).
    _fire_s3_test_event(name)
    return 200, {}, b""


def _get_bucket_logging(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_logging_config.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    root = Element("BucketLoggingStatus", xmlns=S3_NS)
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_logging(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_logging_config[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _get_bucket_accelerate(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_accelerate_config.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    root = Element("AccelerateConfiguration", xmlns=S3_NS)
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_accelerate(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_accelerate_config[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _get_bucket_request_payment(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_request_payment_config.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    root = Element("RequestPaymentConfiguration", xmlns=S3_NS)
    SubElement(root, "Payer").text = "BucketOwner"
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_bucket_request_payment(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_request_payment_config[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _get_bucket_website(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _bucket_websites.get(name)
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    return _error(
        "NoSuchWebsiteConfiguration",
        "The specified bucket does not have a website configuration",
        404,
        f"/{name}",
    )


def _put_bucket_website(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_websites[name] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


def _delete_bucket_website(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _bucket_websites.pop(name, None)
    return 204, {}, b""


def _list_object_versions(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    prefix = _qp(query_params, "prefix", "")
    key_marker = _qp(query_params, "key-marker", "")
    version_id_marker = _qp(query_params, "version-id-marker", "")
    max_keys = int(_qp(query_params, "max-keys", "1000"))

    root = Element("ListVersionsResult", xmlns=S3_NS)
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = prefix
    SubElement(root, "KeyMarker").text = key_marker
    SubElement(root, "VersionIdMarker").text = version_id_marker
    SubElement(root, "MaxKeys").text = str(max_keys)

    owner_id = _canonical_owner_id()

    # Collect all keys: from objects AND from version history (deleted objects).
    # When a version-id-marker is supplied we must resume *within* key-marker,
    # so that key is included; otherwise key-marker is exclusive.
    def _in_page(k):
        if not k.startswith(prefix):
            return False
        if version_id_marker:
            return k >= key_marker
        return k > key_marker

    all_keys = set(k for k in bucket["objects"] if _in_page(k))
    for (bn, k) in _object_versions:
        if bn == bucket_name and _in_page(k):
            all_keys.add(k)
    keys = sorted(all_keys)

    is_truncated = False
    is_truncated_el = SubElement(root, "IsTruncated")
    is_truncated_el.text = "false"

    count = 0
    next_key_marker = None
    next_version_id_marker = None

    def _emit_version(k, v):
        if v.get("is_delete_marker"):
            dm = SubElement(root, "DeleteMarker")
            SubElement(dm, "Key").text = k
            SubElement(dm, "VersionId").text = v["version_id"]
            SubElement(dm, "IsLatest").text = "true" if v["is_latest"] else "false"
            SubElement(dm, "LastModified").text = v["last_modified"]
            owner = SubElement(dm, "Owner")
        else:
            ver = SubElement(root, "Version")
            SubElement(ver, "Key").text = k
            SubElement(ver, "VersionId").text = v["version_id"]
            SubElement(ver, "IsLatest").text = "true" if v["is_latest"] else "false"
            SubElement(ver, "LastModified").text = v["last_modified"]
            SubElement(ver, "ETag").text = v["etag"]
            SubElement(ver, "Size").text = str(v["size"])
            SubElement(ver, "StorageClass").text = (
                v.get("storage_class")
                or bucket["objects"].get(k, {}).get("storage_class")
                or "STANDARD"
            )
            owner = SubElement(ver, "Owner")
        SubElement(owner, "ID").text = owner_id
        SubElement(owner, "DisplayName").text = "ministack"

    for k in keys:
        if count >= max_keys:
            is_truncated = True
            break
        vkey = (bucket_name, k)
        versions = _object_versions.get(vkey)
        if versions:
            # Resume within key-marker: skip versions up to and including the
            # supplied version-id-marker (versions are stored oldest-first, so
            # newest-first iteration matches the S3 listing order).
            skipping = bool(version_id_marker and k == key_marker)
            for v in reversed(versions):
                if skipping:
                    if v["version_id"] == version_id_marker:
                        skipping = False
                    continue
                if count >= max_keys:
                    is_truncated = True
                    break
                _emit_version(k, v)
                next_key_marker, next_version_id_marker = k, v["version_id"]
                count += 1
        else:
            # No version history — return current object with null version
            obj = bucket["objects"].get(k)
            if not obj:
                continue
            ver = SubElement(root, "Version")
            SubElement(ver, "Key").text = k
            SubElement(ver, "VersionId").text = obj.get("version_id", "null")
            SubElement(ver, "IsLatest").text = "true"
            SubElement(ver, "LastModified").text = obj["last_modified"]
            SubElement(ver, "ETag").text = obj["etag"]
            SubElement(ver, "Size").text = str(obj["size"])
            SubElement(ver, "StorageClass").text = obj.get("storage_class") or "STANDARD"
            owner = SubElement(ver, "Owner")
            SubElement(owner, "ID").text = owner_id
            SubElement(owner, "DisplayName").text = "ministack"
            next_key_marker, next_version_id_marker = k, obj.get("version_id", "null")
            count += 1

    is_truncated_el.text = "true" if is_truncated else "false"
    # A truncated response must carry the continuation markers, or a paginating
    # client loops on page one (or, as boto3 does, rejects KeyMarker=None).
    if is_truncated and next_key_marker is not None:
        SubElement(root, "NextKeyMarker").text = next_key_marker
        SubElement(root, "NextVersionIdMarker").text = next_version_id_marker or "null"

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


# ---------------------------------------------------------------------------
# S3 Event Notifications
# ---------------------------------------------------------------------------


def _parse_notification_config(bucket_name: str) -> list[dict]:
    """Parse the stored notification XML into structured config dicts."""
    return _parse_notification_config_raw(_bucket_notifications.get(bucket_name))


def _parse_notification_config_raw(raw: str | None) -> list[dict]:
    """Parse raw notification XML into structured config dicts."""
    if not raw:
        return []

    try:
        root = fromstring(raw)
    except Exception:
        return []

    configs: list[dict] = []

    # Real S3 accepts two ARN-tag forms for Lambda-targeted notifications.
    # boto3's botocore wire serializes `LambdaFunctionArn` as the legacy
    # `<CloudFunction>` tag, so MS used to parse only that. Other clients
    # (AWS SDK for Java v2, Go SDK, hand-crafted XML, Terraform's
    # `aws_s3_bucket_notification` provider) send the modern
    # `<LambdaFunctionArn>` tag — MS silently dropped those configs, so
    # uploads succeeded but the Lambda never fired (issue #649).
    _CONFIG_MAP = {
        "QueueConfiguration": ("sqs", ("Queue",)),
        "TopicConfiguration": ("sns", ("Topic",)),
        "CloudFunctionConfiguration": ("lambda", ("CloudFunction", "Function")),
        "LambdaFunctionConfiguration": (
            "lambda", ("LambdaFunctionArn", "CloudFunction", "Function"),
        ),
    }

    for tag_suffix, (target_type, arn_tags) in _CONFIG_MAP.items():
        for cfg_el in list(root.findall(f"{{{S3_NS}}}{tag_suffix}")) + list(
            root.findall(tag_suffix)
        ):
            arn = ""
            for at in arn_tags:
                el = _find_xml_tag(cfg_el, at)
                if el is not None and el.text:
                    arn = el.text.strip()
                    break
            if not arn:
                continue

            id_el = _find_xml_tag(cfg_el, "Id")
            config_id = id_el.text if id_el is not None and id_el.text else new_uuid()

            events: list[str] = []
            for ev_el in list(cfg_el.findall(f"{{{S3_NS}}}Event")) + list(
                cfg_el.findall("Event")
            ):
                if ev_el.text:
                    events.append(ev_el.text.strip())

            filter_prefix = None
            filter_suffix = None
            filter_el = _find_xml_tag(cfg_el, "Filter")
            if filter_el is not None:
                s3key_el = _find_xml_tag(filter_el, "S3Key")
                if s3key_el is not None:
                    for rule_el in list(
                        s3key_el.findall(f"{{{S3_NS}}}FilterRule")
                    ) + list(s3key_el.findall("FilterRule")):
                        name_el = _find_xml_tag(rule_el, "Name")
                        val_el = _find_xml_tag(rule_el, "Value")
                        if name_el is not None and name_el.text and val_el is not None:
                            rule_name = name_el.text.strip().lower()
                            rule_val = val_el.text or ""
                            if rule_name == "prefix":
                                filter_prefix = rule_val
                            elif rule_name == "suffix":
                                filter_suffix = rule_val

            configs.append(
                {
                    "type": target_type,
                    "arn": arn,
                    "id": config_id,
                    "events": events,
                    "filter_prefix": filter_prefix,
                    "filter_suffix": filter_suffix,
                }
            )

    return configs


def _invalid_notification_config(message: str) -> tuple:
    return _error("InvalidArgument", message, 400)


def _notification_bucket_region(bucket_name: str) -> str:
    bucket = _buckets.get(bucket_name, {})
    return bucket.get("region") or os.environ.get("MINISTACK_REGION", "us-east-1")


_NOTIFICATION_TARGET_SERVICES = {
    "sqs": "sqs",
    "sns": "sns",
    "lambda": "lambda",
}


def _queue_name_from_sqs_arn_spec(spec) -> str | None:
    if spec.service != "sqs" or not spec.resource or ":" in spec.resource or "/" in spec.resource:
        return None
    return spec.resource


def _topic_name_from_sns_arn_spec(spec) -> str | None:
    if spec.service != "sns" or not spec.resource or ":" in spec.resource or "/" in spec.resource:
        return None
    return spec.resource


def _lambda_name_from_arn_spec(spec) -> str | None:
    if spec.service != "lambda":
        return None
    parts = spec.resource.split(":", 2)
    if len(parts) < 2 or parts[0] != "function" or not parts[1]:
        return None
    return parts[1]


def _parse_notification_target_arn(target_type: str, arn: str, bucket_region: str):
    expected_service = _NOTIFICATION_TARGET_SERVICES.get(target_type)
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, "destination ARN is not in the correct format"

    if spec.service != expected_service:
        return spec, f"expected {expected_service} ARN, got {spec.service}"
    if not spec.account_id:
        return spec, "destination ARN must include an account ID"
    if spec.account_id != get_account_id():
        return spec, "destination account must match bucket owner account"
    if not spec.region:
        return spec, "destination ARN must include a region"
    if spec.region != bucket_region:
        return spec, "destination region must match bucket region"
    return spec, None


def _validate_notification_target_arn(target_type: str, arn: str, bucket_region: str) -> tuple | None:
    spec, error = _parse_notification_target_arn(target_type, arn, bucket_region)
    if error:
        return _invalid_notification_config(
            f"Unable to validate destination configuration: {error}"
        )

    if target_type == "sqs" and not _queue_name_from_sqs_arn_spec(spec):
        return _invalid_notification_config(
            "Unable to validate destination configuration: invalid SQS queue ARN"
        )
    if target_type == "sns" and not _topic_name_from_sns_arn_spec(spec):
        return _invalid_notification_config(
            "Unable to validate destination configuration: invalid SNS topic ARN"
        )
    if target_type == "lambda" and not _lambda_name_from_arn_spec(spec):
        return _invalid_notification_config(
            "Unable to validate destination configuration: invalid Lambda function ARN"
        )
    return None


def _validate_notification_configs(configs: list[dict], bucket_region: str) -> tuple | None:
    for cfg in configs:
        error = _validate_notification_target_arn(cfg["type"], cfg["arn"], bucket_region)
        if error:
            return error
    return None


def _event_matches(event_name: str, patterns: list[str]) -> bool:
    """Check if event_name matches any of the configured event patterns.

    Supports wildcards: ``s3:ObjectCreated:*`` matches ``s3:ObjectCreated:Put``.
    """
    for pat in patterns:
        if pat == event_name:
            return True
        if pat.endswith(":*"):
            prefix = pat[:-1]
            if event_name.startswith(prefix):
                return True
        if pat == "s3:*":
            return True
    return False


def _key_matches_filter(key: str, prefix: str | None, suffix: str | None) -> bool:
    if prefix is not None and not key.startswith(prefix):
        return False
    if suffix is not None and not key.endswith(suffix):
        return False
    return True


# Amazon S3 → EventBridge uses a fixed set of detail-types (per event family) and a per-API `reason`. 
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html
_S3_EVENTBRIDGE_DETAIL_TYPE = {
    "ObjectCreated": "Object Created",
    "ObjectRemoved": "Object Deleted",
}
# The restore family is the one family whose events map to distinct
# detail-types (and whose details carry no `reason` field).
_S3_EVENTBRIDGE_EVENT_DETAIL_TYPE = {
    "ObjectRestore:Post": "Object Restore Initiated",
    "ObjectRestore:Completed": "Object Restore Completed",
    "ObjectRestore:Delete": "Object Restore Expired",
}
# `reason` reflects the S3 API that produced the event.
_S3_EVENTBRIDGE_REASON = {
    "Put": "PutObject",
    "Post": "POST Object",
    "Copy": "CopyObject",
    "CompleteMultipartUpload": "CompleteMultipartUpload",
    "Delete": "DeleteObject",
}


def _s3_event_to_eventbridge(event_name: str) -> tuple[str, str | None]:
    """Map an S3 notification event name (e.g. ``s3:ObjectCreated:Put``) to the EventBridge
    ``detail-type`` and ``reason`` Amazon S3 emits. Created/Removed collapse per family
    exactly as real S3 does; the restore events each carry their own detail-type and no
    reason."""
    parts = event_name.split(":")
    family = parts[1] if len(parts) > 1 else ""
    action = parts[2] if len(parts) > 2 else ""
    per_event = _S3_EVENTBRIDGE_EVENT_DETAIL_TYPE.get(f"{family}:{action}")
    if per_event is not None:
        return per_event, None
    detail_type = _S3_EVENTBRIDGE_DETAIL_TYPE.get(family, "Object Created")
    reason = _S3_EVENTBRIDGE_REASON.get(
        action, "DeleteObject" if family == "ObjectRemoved" else "PutObject"
    )
    return detail_type, reason


def _fire_s3_event(
    bucket_name: str,
    key: str,
    event_name: str,
    size: int = 0,
    etag: str = "",
    deletion_type: str | None = None,
    restore_event_data: dict | None = None,
) -> None:
    """Build and deliver an S3 event notification. Best-effort — errors are logged."""
    try:
        configs = _parse_notification_config(bucket_name)
        raw_xml = _bucket_notifications.get(bucket_name, "")
        has_eventbridge = "EventBridgeConfiguration" in raw_xml
        if not configs and not has_eventbridge:
            return
        bucket_region = _notification_bucket_region(bucket_name)

        short_event = event_name.replace("s3:", "", 1)
        event_time = now_iso()
        request_id = new_uuid()
        clean_etag = etag.strip('"')

        event_payload = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": bucket_region,
                    "eventTime": event_time,
                    "eventName": short_event,
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": request_id,
                        "x-amz-id-2": "EXAMPLE",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "",
                        "bucket": {
                            "name": bucket_name,
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": f"arn:aws:s3:::{bucket_name}",
                        },
                        "object": {
                            "key": key,
                            "size": size,
                            "eTag": clean_etag,
                            "sequencer": "0",
                        },
                    },
                }
            ],
        }
        if restore_event_data:
            # glacierEventData appears only on s3:ObjectRestore:Completed.
            event_payload["Records"][0]["glacierEventData"] = {
                "restoreEventData": restore_event_data,
            }

        for cfg in configs:
            try:
                if not _event_matches(event_name, cfg["events"]):
                    continue
                if not _key_matches_filter(
                    key, cfg["filter_prefix"], cfg["filter_suffix"]
                ):
                    continue

                payload = dict(event_payload)
                payload["Records"] = [dict(payload["Records"][0])]
                payload["Records"][0]["s3"] = dict(payload["Records"][0]["s3"])
                payload["Records"][0]["s3"]["configurationId"] = cfg["id"]

                if cfg["type"] == "sqs":
                    _deliver_event_to_sqs(cfg["arn"], payload, bucket_region)
                elif cfg["type"] == "sns":
                    _deliver_event_to_sns(cfg["arn"], payload, bucket_region)
                elif cfg["type"] == "lambda":
                    _deliver_event_to_lambda(cfg["arn"], payload, bucket_region)
            except Exception:
                logger.exception(
                    "S3 notification delivery failed for config %s", cfg.get("id")
                )

        # S3 → EventBridge delivery (if EventBridgeConfiguration is enabled)
        try:
            if has_eventbridge:
                from ministack.services import eventbridge as _eb
                detail_type, reason = _s3_event_to_eventbridge(event_name)
                detail = {
                    "version": "0",
                    "event-version": "1.0",
                    "bucket": {"name": bucket_name},
                    "object": {"key": key, "size": size, "etag": clean_etag, "sequencer": "0"},
                    "request-id": request_id,
                    "requester": get_account_id(),
                    "source-ip-address": "127.0.0.1",
                }
                if reason is not None:
                    detail["reason"] = reason
                if detail_type == "Object Deleted":
                    # AWS always carries a deletion-type on Object Deleted; default to the
                    # unversioned/permanent case unless the caller created a delete marker.
                    detail["deletion-type"] = deletion_type or "Permanently Deleted"
                eb_event = {
                    "EventId": request_id,
                    "Source": "aws.s3",
                    "DetailType": detail_type,
                    "Detail": json.dumps(detail),
                    "EventBusName": "default",
                    "Time": event_time,
                    "Resources": [f"arn:aws:s3:::{bucket_name}"],
                    "Account": get_account_id(),
                    "Region": bucket_region,
                }
                previous_region = get_region()
                set_request_region(bucket_region)
                try:
                    _eb._dispatch_event(eb_event)
                finally:
                    set_request_region(previous_region)
                logger.debug("S3→EventBridge: %s (%s) for %s/%s", detail_type, event_name, bucket_name, key)
        except Exception:
            logger.exception("S3→EventBridge delivery failed for %s/%s", bucket_name, key)

    except Exception:
        logger.exception(
            "S3 event notification fire failed for %s/%s", bucket_name, key
        )


def _parse_delivery_notification_target(target_type: str, arn: str, bucket_region: str):
    spec, error = _parse_notification_target_arn(target_type, arn, bucket_region)
    if error:
        logger.warning(
            "S3 notification: invalid %s target ARN %s: %s",
            target_type.upper(),
            arn,
            error,
        )
        return None
    return spec


def _deliver_event_to_sqs(arn: str, event_payload: dict, bucket_region: str) -> None:
    from ministack.services import sqs as _sqs

    spec = _parse_delivery_notification_target("sqs", arn, bucket_region)
    if not spec:
        return
    queue_name = _queue_name_from_sqs_arn_spec(spec)
    if not queue_name:
        logger.warning("S3 notification: invalid SQS queue ARN %s", arn)
        return
    queue = _sqs._queue_by_arn(str(spec))
    if not queue:
        logger.warning("S3 notification: SQS queue %s not found", queue_name)
        return

    body = json.dumps(event_payload)
    now = time.time()
    msg = {
        "id": new_uuid(),
        "body": body,
        "md5": hashlib.md5(body.encode()).hexdigest(),
        "receipt_handle": None,
        "sent_at": now,
        "visible_at": now,
        "receive_count": 0,
    }
    _sqs._ensure_msg_fields(msg)
    queue["messages"].append(msg)
    logger.info("S3 notification → SQS %s", queue_name)


def _deliver_event_to_sns(arn: str, event_payload: dict, bucket_region: str) -> None:
    from ministack.services import sns as _sns

    spec = _parse_delivery_notification_target("sns", arn, bucket_region)
    if not spec:
        return
    if not _topic_name_from_sns_arn_spec(spec):
        logger.warning("S3 notification: invalid SNS topic ARN %s", arn)
        return
    topic = _sns._topics.get(arn)
    if not topic:
        logger.warning("S3 notification: SNS topic %s not found", arn)
        return

    message = json.dumps(event_payload)
    msg_id = new_uuid()
    _sns._fanout(arn, msg_id, message, subject="Amazon S3 Notification")
    logger.info("S3 notification → SNS %s", arn)


def _deliver_event_to_lambda(arn: str, event_payload: dict, bucket_region: str) -> None:
    from ministack.services import lambda_svc as _lambda

    spec = _parse_delivery_notification_target("lambda", arn, bucket_region)
    if not spec:
        return
    if not _lambda_name_from_arn_spec(spec):
        logger.warning("S3 notification: invalid Lambda function ARN %s", arn)
        return
    func, config, func_name = _lambda._get_func_record_for_ref(arn)
    if not func or not config:
        logger.warning("S3 notification: Lambda function %s not found", func_name)
        return

    # Real S3 → Lambda uses asynchronous invocation, which means retries
    # (MaximumRetryAttempts, default 2) and routing to the function's DLQ /
    # DestinationConfig.OnFailure on final failure. Shared helper keeps the
    # semantics identical to direct Invoke(InvocationType=Event).
    _lambda.invoke_async_with_retry(_lambda._execution_record_for_config(func, config), event_payload)
    logger.info("S3 notification → Lambda %s (async with retry+DLQ)", func_name)


def _fire_s3_event_async(
    bucket_name: str,
    key: str,
    event_name: str,
    size: int = 0,
    etag: str = "",
    deletion_type: str | None = None,
) -> None:
    """Fire S3 event notification in a background thread (non-blocking)."""
    if bucket_name not in _bucket_notifications:
        return
    # threading.Thread does not copy contextvars, so without this snapshot the
    # worker runs under the default account (000000000000): the account-scoped
    # _bucket_notifications lookup comes back empty and the event is silently
    # dropped for any non-default account, and SQS/SNS/Lambda/EventBridge
    # targets resolve under the wrong account. Carry the request context in.
    # See issue #876.
    ctx = contextvars.copy_context()
    t = threading.Thread(
        target=ctx.run,
        args=(_fire_s3_event, bucket_name, key, event_name, size, etag, deletion_type),
        daemon=True,
    )
    t.start()


def _fire_s3_test_event(bucket_name: str) -> None:
    """Deliver an s3:TestEvent to every destination in the bucket notification config."""
    try:
        configs = _parse_notification_config(bucket_name)
        if not configs:
            return
        bucket_region = _notification_bucket_region(bucket_name)
        payload = {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "Bucket": bucket_name,
            "RequestId": new_uuid(),
            "HostId": new_uuid(),
        }
        for cfg in configs:
            try:
                if cfg["type"] == "sqs":
                    _deliver_event_to_sqs(cfg["arn"], payload, bucket_region)
                elif cfg["type"] == "sns":
                    _deliver_event_to_sns(cfg["arn"], payload, bucket_region)
                elif cfg["type"] == "lambda":
                    _deliver_event_to_lambda(cfg["arn"], payload, bucket_region)
            except Exception:
                logger.exception(
                    "S3 test-event delivery failed for config %s", cfg.get("id")
                )
    except Exception:
        logger.exception("S3 test-event fire failed for %s", bucket_name)


# ---------------------------------------------------------------------------
# Object operations
# ---------------------------------------------------------------------------


def _put_object(bucket_name: str, key: str, body: bytes, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    md5_err = _validate_content_md5(headers, body)
    if md5_err:
        return md5_err

    _, sc_err = _resolve_storage_class(headers)
    if sc_err:
        return sc_err

    precondition_err = _check_put_preconditions(headers, bucket.get("objects", {}).get(key))
    if precondition_err:
        return precondition_err

    checksums, csum_err = _resolve_object_checksums(body, headers)
    if csum_err:
        return csum_err

    sse_headers, sse_err = _resolve_sse_write_headers(headers, bucket_name)
    if sse_err:
        return sse_err

    # A canned ACL supplied at PutObject time is validated up front (an invalid
    # value rejects the whole request, as AWS does) and applied below once the
    # version id is assigned, so GetObjectAcl reflects it instead of dropping
    # it. (#1322, rest of defect 9)
    canned_acl = headers.get("x-amz-acl")
    if canned_acl and canned_acl not in _CANNED_OBJECT_ACLS:
        return _error("InvalidArgument", f"Invalid x-amz-acl value: {canned_acl}", 400)

    etag = f'"{md5_hash(body)}"'
    obj = _build_object_record(body, headers, etag=etag, checksums=checksums)
    obj["preserved_headers"].update(sse_headers)
    prior_obj = bucket["objects"].get(key)
    bucket["objects"][key] = obj

    # --- Object Lock headers on PutObject ---
    _apply_object_lock_from_headers(bucket_name, key, headers)

    # --- x-amz-tagging header on PutObject ---
    # Parse + validate up front so the count error returns before persist/event,
    # but defer the dict write until version_id is assigned (tags are per-version).
    pending_tags = None
    tagging_header = headers.get("x-amz-tagging", "")
    if tagging_header:
        pending_tags = {k: v[0] for k, v in _parse_qs(tagging_header, keep_blank_values=True).items()}
        if len(pending_tags) > 10:
            return _error("BadRequest", "Object tags cannot be greater than 10", 400)

    _fire_s3_event_async(
        bucket_name, key, "s3:ObjectCreated:Put", size=obj["size"], etag=obj["etag"]
    )

    resp_headers = {"ETag": obj["etag"], "Content-Length": "0"}
    resp_headers.update(sse_headers)
    version_id = _record_object_version(bucket_name, key, prior_obj, obj, body)
    if version_id:
        resp_headers["x-amz-version-id"] = version_id

    # Persist only after the versioning block: the .meta.json sidecar must
    # carry the version_id assigned above (#1058).
    if S3_PERSIST:
        _persist_object(bucket_name, key, obj)

    if pending_tags is not None:
        _object_tags[(bucket_name, key, obj.get("version_id"))] = pending_tags
    if canned_acl:
        _object_acl[(bucket_name, key, obj.get("version_id"))] = (
            _canned_acl_policy_xml(canned_acl, _canonical_owner_id()))
    return 200, resp_headers, b""


def _parse_multipart_form(content_type: str, body: bytes) -> list[tuple]:
    """Return [(name, filename, headers, value_bytes), ...] in form order.

    Returns [] if the body is malformed or the boundary cannot be found.
    """
    import re as _re
    m = _re.search(r'boundary=("?)([^";\s]+)\1', content_type, _re.IGNORECASE)
    if not m:
        return []
    boundary = b"--" + m.group(2).encode()
    chunks = body.split(boundary)
    out = []
    # First chunk is preamble (often empty); last is "--\r\n" terminator.
    for chunk in chunks[1:-1]:
        if chunk.startswith(b"\r\n"):
            chunk = chunk[2:]
        if chunk.endswith(b"\r\n"):
            chunk = chunk[:-2]
        head_blob, sep, value = chunk.partition(b"\r\n\r\n")
        if not sep:
            continue
        part_headers: dict = {}
        for line in head_blob.split(b"\r\n"):
            if b":" in line:
                k, _, v = line.partition(b":")
                part_headers[k.strip().decode("latin-1").lower()] = v.strip().decode("latin-1")
        cd = part_headers.get("content-disposition", "")
        # RFC 2183 allows both quoted-string ('name="x"') and token ('name=x')
        # forms. .NET's MultipartFormDataContent emits the token form for
        # ASCII-clean values; real S3 accepts both, so we do too.
        name_m = _re.search(r'name=(?:"([^"]*)"|([^;\s]+))', cd)
        fn_m = _re.search(r'filename=(?:"([^"]*)"|([^;\s]+))', cd)
        out.append((
            (name_m.group(1) or name_m.group(2)) if name_m else "",
            (fn_m.group(1) or fn_m.group(2)) if fn_m else None,
            part_headers,
            value,
        ))
    return out


def _enforce_post_policy_size(policy_b64: str, size: int):
    """Apply the `content-length-range` condition from a POST policy.

    Returns an error response when size is outside [min, max], else None.
    Other policy conditions (key, starts-with, signature) are not enforced —
    same lenient stance as ministack's presigned-URL handling.
    """
    if not policy_b64:
        return None
    try:
        decoded = base64.b64decode(policy_b64 + "==").decode("utf-8")
        policy = json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return None
    for cond in policy.get("conditions", []):
        if (isinstance(cond, list) and len(cond) == 3
                and isinstance(cond[0], str)
                and cond[0].lower() == "content-length-range"):
            try:
                lo = int(cond[1])
                hi = int(cond[2])
            except (TypeError, ValueError):
                continue
            if size < lo:
                return _error(
                    "EntityTooSmall",
                    "Your proposed upload is smaller than the minimum allowed object size.",
                    400,
                )
            if size > hi:
                return _error(
                    "EntityTooLarge",
                    "Your proposed upload exceeds the maximum allowed size.",
                    400,
                )
    return None


def _post_object(bucket_name: str, body: bytes, headers: dict):
    """Browser-based form upload (RFC 1867 / S3 PostObject).

    Per https://docs.aws.amazon.com/AmazonS3/latest/API/RTPM-mpuoverview.html,
    callers POST a multipart/form-data body to the bucket root. We honour
    `key` (with `${filename}` substitution), `Content-Type`, `x-amz-meta-*`,
    `x-amz-storage-class`, `x-amz-tagging`, `success_action_status`, and
    `success_action_redirect`. Policy and signature fields are accepted and
    ignored — same lenient stance as ministack's presigned-URL handling.
    """
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    parts = _parse_multipart_form(headers.get("content-type", ""), body)
    if not parts:
        return _error("MalformedPOSTRequest",
                      "The body of your POST request is not well-formed multipart/form-data.",
                      400)

    fields: dict[str, str] = {}
    file_value: bytes | None = None
    file_filename: str | None = None
    file_headers: dict = {}
    for name, filename, ph, value in parts:
        # Only the field literally named "file" is the object body. A filename
        # attribute on any other field does NOT make it the body — browsers and
        # HTTP libraries (e.g. Python requests' files=) set filename on ordinary
        # form fields, and S3 treats those as form fields, not content.
        if name == "file":
            file_value = value
            file_filename = filename or ""
            file_headers = ph
        else:
            try:
                fields[name.lower()] = value.decode("utf-8")
            except UnicodeDecodeError:
                fields[name.lower()] = value.decode("latin-1")

    if file_value is None:
        return _error("InvalidArgument",
                      "POST requires a file field.", 400)

    err = _enforce_post_policy_size(fields.get("policy", ""), len(file_value))
    if err:
        return err

    key_template = fields.get("key", "")
    if not key_template:
        return _error("InvalidArgument",
                      "Bucket POST must contain a field named 'key'.", 400)
    key = key_template.replace("${filename}", file_filename or "")

    # Build a synthetic header dict so _build_object_record reuses the PUT path.
    synth = {}
    if "content-type" in fields:
        synth["content-type"] = fields["content-type"]
    elif file_headers.get("content-type"):
        synth["content-type"] = file_headers["content-type"]
    for fname, fval in fields.items():
        if fname.startswith("x-amz-meta-"):
            synth[fname] = fval
    for h in ("cache-control", "content-disposition", "content-encoding",
              "content-language", "expires", "x-amz-storage-class",
              "x-amz-tagging", "x-amz-acl",
              "x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date",
              "x-amz-object-lock-legal-hold"):
        if h in fields:
            synth[h] = fields[h]
    # The POST form names its canned-ACL field `acl` (not `x-amz-acl`).
    if "acl" in fields:
        synth["x-amz-acl"] = fields["acl"]

    _, sc_err = _resolve_storage_class(synth)
    if sc_err:
        return sc_err

    canned_acl = synth.get("x-amz-acl")
    if canned_acl and canned_acl not in _CANNED_OBJECT_ACLS:
        return _error("InvalidArgument", f"Invalid x-amz-acl value: {canned_acl}", 400)

    # The POST form names its SSE fields exactly like the request headers.
    sse_fields = {h: fields[h] for h in (
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key",
        "x-amz-server-side-encryption-customer-key-md5") if h in fields}
    sse_headers, sse_err = _resolve_sse_write_headers(sse_fields, bucket_name)
    if sse_err:
        return sse_err

    etag = f'"{md5_hash(file_value)}"'
    obj = _build_object_record(file_value, synth, etag=etag)
    obj["preserved_headers"].update(sse_headers)
    prior_obj = bucket["objects"].get(key)
    bucket["objects"][key] = obj
    _apply_object_lock_from_headers(bucket_name, key, synth)

    # Defer tag write until version_id is assigned (tags are per-version).
    pending_tags = None
    tagging_header = synth.get("x-amz-tagging", "")
    if tagging_header:
        parsed = {k: v[0] for k, v in _parse_qs(tagging_header, keep_blank_values=True).items()}
        if len(parsed) <= 10:
            pending_tags = parsed

    _fire_s3_event_async(
        bucket_name, key, "s3:ObjectCreated:Post", size=obj["size"], etag=etag
    )

    version_id = _record_object_version(bucket_name, key, prior_obj, obj, file_value)

    # Persist only after the versioning block: the .meta.json sidecar must
    # carry the version_id assigned above (#1058).
    if S3_PERSIST:
        _persist_object(bucket_name, key, obj)

    if pending_tags is not None:
        _object_tags[(bucket_name, key, version_id)] = pending_tags
    if canned_acl:
        _object_acl[(bucket_name, key, version_id)] = (
            _canned_acl_policy_xml(canned_acl, _canonical_owner_id()))

    location = f"http://{bucket_name}.s3.amazonaws.com/{url_quote(key, safe='/')}"
    base_resp = {"ETag": etag, "Location": location}
    if version_id:
        base_resp["x-amz-version-id"] = version_id

    redirect = fields.get("success_action_redirect") or fields.get("redirect")
    if redirect:
        sep = "&" if "?" in redirect else "?"
        target = (f"{redirect}{sep}bucket={bucket_name}"
                  f"&key={url_quote(key, safe='/')}"
                  f"&etag={url_quote(etag, safe='')}")
        return 303, {**base_resp, "Location": target}, b""

    status_str = fields.get("success_action_status", "204")
    try:
        status = int(status_str)
    except (TypeError, ValueError):
        status = 204
    if status not in (200, 201, 204):
        status = 204

    if status == 201:
        root = Element("PostResponse")
        SubElement(root, "Location").text = location
        SubElement(root, "Bucket").text = bucket_name
        SubElement(root, "Key").text = key
        SubElement(root, "ETag").text = etag
        return 201, {**base_resp, "Content-Type": "application/xml"}, _xml_body(root)

    return status, base_resp, b""


def _apply_object_lock_from_headers(bucket_name: str, key: str, headers: dict):
    lock_mode = headers.get("x-amz-object-lock-mode", "")
    lock_until = headers.get("x-amz-object-lock-retain-until-date", "")
    lock_legal = headers.get("x-amz-object-lock-legal-hold", "") or headers.get("x-amz-object-lock-legal-hold-status", "")

    if lock_mode and lock_until:
        _object_retention[(bucket_name, key)] = {
            "Mode": lock_mode,
            "RetainUntilDate": lock_until,
        }
    elif not lock_mode and not lock_until:
        # Apply bucket default retention if no explicit retention
        lock_cfg = _bucket_object_lock.get(bucket_name)
        if lock_cfg and lock_cfg.get("default_retention"):
            dr = lock_cfg["default_retention"]
            days = dr.get("Days", 0)
            years = dr.get("Years", 0)
            now = _dt.datetime.now(_dt.timezone.utc)
            if days:
                until = now + _dt.timedelta(days=days)
            elif years:
                until = now.replace(year=now.year + years)
            else:
                return
            _object_retention[(bucket_name, key)] = {
                "Mode": dr["Mode"],
                "RetainUntilDate": until.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            }

    if lock_legal in ("ON", "OFF"):
        _object_legal_hold[(bucket_name, key)] = lock_legal


def _object_tagging_count_header(bucket_name: str, key: str, version_id) -> dict:
    """GetObject returns ``x-amz-tagging-count`` (boto3 surfaces it as ``TagCount``)
    only when the object carries at least one tag; AWS omits the header at zero (#1026)."""
    n = len(_object_tags.get((bucket_name, key, version_id), {}))
    return {"x-amz-tagging-count": str(n)} if n else {}


def _get_object(bucket_name: str, key: str, headers: dict, query_params: dict = None):
    query_params = query_params or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    # AWS rule: the six response-* override query parameters require a signed
    # request (Authorization header or presigned URL). An unsigned/anonymous
    # GET that carries any of them is rejected with InvalidRequest (400).
    # See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    err = _reject_response_overrides_if_unsigned(headers, query_params, bucket_name, key)
    if err is not None:
        return err

    version_id = _qp(query_params, "versionId", "")
    if version_id == "null" and not any(
            v["version_id"] == "null"
            for v in _object_versions.get((bucket_name, key), [])):
        # The literal "null" addresses the pre-versioning object; before any
        # versioned write records it in the index, that is the current object.
        obj = bucket["objects"].get(key)
        if obj is not None and not obj.get("version_id"):
            version_id = ""
    if version_id:
        vkey = (bucket_name, key)
        versions = _object_versions.get(vkey, [])
        for v in versions:
            if v["version_id"] == version_id:
                # Route through the shared header builder (same path as versioned
                # HeadObject) so a version's user metadata (x-amz-meta-*),
                # preserved headers and content-encoding are emitted — not just
                # the five wire basics. Versioned reads honor
                # `x-amz-checksum-mode: ENABLED` exactly as current-version reads do.
                include_checksums = (headers.get("x-amz-checksum-mode") or "").upper() == "ENABLED"
                vobj = _object_record_from_version(v)
                sse_gate = _check_sse_read_headers(headers, vobj)
                if sse_gate is not None:
                    return sse_gate
                vsc = vobj.get("storage_class") or "STANDARD"
                if vsc in _ARCHIVE_STORAGE_CLASSES:
                    # Restore state lives on the current object record;
                    # noncurrent archived versions are simply unreadable
                    # (restore-by-versionId is not implemented).
                    cur = bucket["objects"].get(key)
                    cur_phase = (
                        _sync_restore(bucket_name, key, cur)
                        if cur is not None and cur.get("version_id") == version_id
                        else None)
                    if cur_phase != "restored":
                        return _invalid_object_state(bucket_name, key, vsc)
                resp_headers = _object_response_headers(
                    vobj, bucket_name, key, include_checksums=include_checksums)
                resp_headers.update(_object_tagging_count_header(bucket_name, key, version_id))
                precondition = _check_read_preconditions(headers, vobj, resp_headers)
                if precondition is not None:
                    return precondition
                body = v.get("data")
                if body is None:
                    body = _read_body(bucket_name, key, bucket["objects"].get(key, {}))
                return 200, resp_headers, body
        return _error("NoSuchVersion", "The specified version does not exist.", 404, f"/{bucket_name}/{key}")

    if key not in bucket["objects"]:
        status, err_headers, err_body = _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )
        # A key hidden by a delete marker announces the marker on its 404.
        err_headers.update(_delete_marker_404_headers(bucket_name, key))
        return status, err_headers, err_body

    obj = bucket["objects"][key]
    sse_gate = _check_sse_read_headers(headers, obj)
    if sse_gate is not None:
        return sse_gate
    restore_phase = _sync_restore(bucket_name, key, obj)
    sc = obj.get("storage_class") or "STANDARD"
    if sc in _ARCHIVE_STORAGE_CLASSES and restore_phase != "restored":
        # Archived objects are unreadable until a restore lands; HeadObject
        # deliberately keeps working — that asymmetry is how callers poll.
        return _invalid_object_state(bucket_name, key, sc)
    range_header = headers.get("range", "")
    # AWS returns whole-object checksums only on full-object responses (HTTP
    # 200). On a 206 Partial Content reply the bytes are a slice, and a
    # whole-object checksum can't validate them — boto3 raises
    # `FlexibleChecksumError` if it sees one alongside sliced bytes.
    checksum_mode_on = (headers.get("x-amz-checksum-mode") or "").upper() == "ENABLED"
    include_checksums = checksum_mode_on and not range_header
    resp_headers = _object_response_headers(obj, bucket_name, key,
                                            include_checksums=include_checksums)
    resp_headers.update(_object_tagging_count_header(bucket_name, key, obj.get("version_id")))

    # Evaluated before the range and partNumber slicing below: a failed
    # precondition preempts the read entirely, and a 304 describes the whole
    # representation the caller already has, not the slice they asked for.
    precondition = _check_read_preconditions(headers, obj, resp_headers)
    if precondition is not None:
        return precondition

    body = _read_body(bucket_name, key, obj)

    # partNumber: return a single part of a completed multipart object as 206
    # Partial Content with a Content-Range and x-amz-mp-parts-count, rather than
    # the whole object. Parts are matched by their part number (which may be
    # non-contiguous), and their byte offset is the sum of the preceding sizes.
    part_number = _qp(query_params, "partNumber") if query_params else None
    if part_number and obj.get("parts"):
        try:
            pn = int(part_number)
        except (TypeError, ValueError):
            pn = None
        parts = obj["parts"]
        idx = next((i for i, p in enumerate(parts) if p["PartNumber"] == pn), None)
        if idx is not None:
            offset = sum(p["Size"] for p in parts[:idx])
            length = parts[idx]["Size"]
            slice_body = body[offset : offset + length]
            resp_headers["Content-Length"] = str(len(slice_body))
            resp_headers["Content-Range"] = f"bytes {offset}-{offset + length - 1}/{obj['size']}"
            resp_headers["x-amz-mp-parts-count"] = str(len(parts))
            _apply_response_overrides(resp_headers, query_params)
            return 206, resp_headers, slice_body

    if range_header:
        rng = _parse_range(range_header, obj["size"])
        if rng is None:
            return (
                416,
                {
                    "Content-Type": "application/xml",
                    "Content-Range": f"bytes */{obj['size']}",
                },
                _xml_body(_range_error_xml(bucket_name, key)),
            )
        start, end = rng
        slice_body = body[start : end + 1]
        resp_headers["Content-Length"] = str(len(slice_body))
        resp_headers["Content-Range"] = f"bytes {start}-{end}/{obj['size']}"
        _apply_response_overrides(resp_headers, query_params)
        return 206, resp_headers, slice_body

    _apply_response_overrides(resp_headers, query_params)
    return 200, resp_headers, body


def _append_object_parts(root: Element, record: dict, headers: dict) -> None:
    """Emit the ObjectParts (ListParts) block. Multipart-completed objects carry
    retained `parts`; single-PUT objects have no parts and list an empty page."""
    op = SubElement(root, "ObjectParts")
    try:
        max_parts = int(headers.get("x-amz-max-parts", 1000))
    except (TypeError, ValueError):
        max_parts = 1000
    try:
        marker = int(headers.get("x-amz-part-number-marker", 0))
    except (TypeError, ValueError):
        marker = 0
    SubElement(op, "PartNumberMarker").text = str(marker)
    SubElement(op, "MaxParts").text = str(max_parts)

    parts = record.get("parts") or []
    after = [p for p in parts if p["PartNumber"] > marker]
    page = after[:max_parts] if max_parts >= 0 else after
    truncated = len(after) > len(page)
    SubElement(op, "IsTruncated").text = "true" if truncated else "false"
    SubElement(op, "NextPartNumberMarker").text = str(
        page[-1]["PartNumber"] if page else 0)
    for p in page:
        pe = SubElement(op, "Part")
        SubElement(pe, "PartNumber").text = str(p["PartNumber"])
        SubElement(pe, "Size").text = str(p["Size"])
    # TotalPartsCount is reported only for genuine multipart objects.
    if parts:
        SubElement(op, "PartsCount").text = str(len(parts))


def _get_object_attributes(bucket_name: str, key: str, headers: dict,
                           query_params: dict):
    """S3 GetObjectAttributes — GET /{bucket}/{key}?attributes. Returns only the
    root-level fields named in the required `x-amz-object-attributes` header."""
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    raw_attrs = headers.get("x-amz-object-attributes")
    if not raw_attrs:
        return _error(
            "InvalidRequest",
            "The x-amz-object-attributes header is required for GetObjectAttributes.",
            400, f"/{bucket_name}/{key}")
    requested = {a.strip() for a in raw_attrs.split(",") if a.strip()}

    resp_headers = {"Content-Type": "application/xml"}
    version_id = _qp(query_params, "versionId", "")
    if version_id:
        record = None
        for v in _object_versions.get((bucket_name, key), []):
            if v["version_id"] == version_id:
                record = {
                    "etag": v["etag"],
                    "size": v["size"],
                    "last_modified": v["last_modified"],
                    "storage_class": v.get("storage_class") or "STANDARD",
                    "checksums": v.get("checksums") or {},
                    "parts": v.get("parts"),
                }
                break
        if record is None:
            return _error("NoSuchVersion", "The specified version does not exist.",
                          404, f"/{bucket_name}/{key}")
        resp_headers["x-amz-version-id"] = version_id
    else:
        obj = bucket["objects"].get(key)
        if obj is None:
            return _error("NoSuchKey", "The specified key does not exist.",
                          404, f"/{bucket_name}/{key}")
        record = obj
        if obj.get("version_id"):
            resp_headers["x-amz-version-id"] = obj["version_id"]

    resp_headers["Last-Modified"] = iso_to_rfc7231(record["last_modified"])

    # Emit only requested attributes, in the AWS response order.
    root = Element("GetObjectAttributesResponse", xmlns=S3_NS)
    if "ETag" in requested:
        # GetObjectAttributes returns the ETag WITHOUT the surrounding quotes
        # that the ETag HTTP header carries on Get/HeadObject.
        SubElement(root, "ETag").text = (record.get("etag") or "").strip('"')
    if "Checksum" in requested:
        checksums = record.get("checksums") or {}
        cks = SubElement(root, "Checksum")
        for alg, val in checksums.items():
            SubElement(cks, f"Checksum{alg}").text = val
        if checksums:
            SubElement(cks, "ChecksumType").text = (
                "COMPOSITE" if record.get("parts") else "FULL_OBJECT")
    if "ObjectParts" in requested:
        _append_object_parts(root, record, headers)
    if "StorageClass" in requested:
        sc = record.get("storage_class") or "STANDARD"
        # AWS returns StorageClass for every class except S3 Standard.
        if sc != "STANDARD":
            SubElement(root, "StorageClass").text = sc
    if "ObjectSize" in requested:
        SubElement(root, "ObjectSize").text = str(record["size"])

    return 200, resp_headers, _xml_body(root)


def _range_error_xml(bucket_name: str, key: str) -> Element:
    root = Element("Error")
    SubElement(root, "Code").text = "InvalidRange"
    SubElement(root, "Message").text = "The requested range is not satisfiable"
    SubElement(root, "Resource").text = f"/{bucket_name}/{key}"
    SubElement(root, "RequestId").text = new_uuid()
    return root


def _head_object(bucket_name: str, key: str, headers: dict | None = None,
                 query_params: dict | None = None):
    headers = headers or {}
    query_params = query_params or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    # A `?versionId=` selects a specific version's metadata, matching AWS
    # HeadObject(VersionId); without it, the current object is used.
    version_id = _qp(query_params, "versionId", "")
    if version_id == "null" and not any(
            v["version_id"] == "null"
            for v in _object_versions.get((bucket_name, key), [])):
        # The literal "null" addresses the pre-versioning object; before any
        # versioned write records it in the index, that is the current object.
        obj = bucket["objects"].get(key)
        if obj is not None and not obj.get("version_id"):
            version_id = ""
    if version_id:
        ventry = next(
            (v for v in _object_versions.get((bucket_name, key), [])
             if v["version_id"] == version_id),
            None,
        )
        if ventry is None or ventry.get("is_delete_marker"):
            return _error(
                "NoSuchVersion",
                "The specified version does not exist.",
                404,
                f"/{bucket_name}/{key}",
            )
        obj = _object_record_from_version(ventry)
    else:
        if key not in bucket["objects"]:
            status, err_headers, err_body = _error(
                "NoSuchKey",
                "The specified key does not exist.",
                404,
                f"/{bucket_name}/{key}",
            )
            # A key hidden by a delete marker announces the marker on its 404.
            err_headers.update(_delete_marker_404_headers(bucket_name, key))
            return status, err_headers, err_body
        obj = bucket["objects"][key]
        # Lazily lapse an expired restore so the x-amz-restore header (and the
        # s3:ObjectRestore:Delete event) reflect reality on the poll path.
        _sync_restore(bucket_name, key, obj)

    sse_gate = _check_sse_read_headers(headers, obj)
    if sse_gate is not None:
        return sse_gate
    include_checksums = (headers.get("x-amz-checksum-mode") or "").upper() == "ENABLED"
    resp_headers = _object_response_headers(obj, bucket_name, key,
                                            include_checksums=include_checksums)
    if version_id:
        resp_headers["x-amz-version-id"] = version_id
    precondition = _check_read_preconditions(headers, obj, resp_headers)
    if precondition is not None:
        return precondition
    return 200, resp_headers, b""


def _purge_current_object(bucket_name: str, key: str, bucket: dict):
    """Remove the current object plus its key-level metadata and on-disk copy."""
    bucket["objects"].pop(key, None)
    _object_tags.pop((bucket_name, key, None), None)
    _object_retention.pop((bucket_name, key), None)
    _object_legal_hold.pop((bucket_name, key), None)
    _object_acl.pop((bucket_name, key, None), None)
    _delete_persisted_object(bucket_name, key)


def _object_record_from_version(v: dict) -> dict:
    """Rebuild a current-object record from a stored version entry.

    Used when a version delete removes the current version/marker and an older
    real version becomes current again — the version index keeps the body plus
    the wire-relevant metadata (etag/size/checksums/storage class), which is
    enough to serve Head/GetObject without a VersionId."""
    return {
        "body": v.get("data"),
        "content_type": v.get("content_type", "application/octet-stream"),
        "content_encoding": v.get("content_encoding"),
        "etag": v["etag"],
        "last_modified": v["last_modified"],
        "size": v["size"],
        "metadata": v.get("metadata", {}),
        "preserved_headers": v.get("preserved_headers", {}),
        "storage_class": v.get("storage_class") or "STANDARD",
        "checksums": v.get("checksums") or {},
        # The "null" version promotes to a current object WITHOUT a version
        # id, the same shape a pre-versioning object has — per-version tags
        # and subresources key it as None, and the literal "null" is only a
        # wire-level name for it.
        "version_id": None if v["version_id"] == "null" else v["version_id"],
    }


def _version_entry_from_record(obj: dict, version_id: str, data) -> dict:
    """A version-index entry projected from a current-object record."""
    return {
        "version_id": version_id,
        "last_modified": obj["last_modified"],
        "etag": obj["etag"],
        "size": obj["size"],
        "is_latest": False,
        "data": data,
        "content_type": obj.get("content_type") or "application/octet-stream",
        "content_encoding": obj.get("content_encoding"),
        "metadata": obj.get("metadata", {}),
        "preserved_headers": obj.get("preserved_headers", {}),
        "storage_class": obj.get("storage_class") or "STANDARD",
        "checksums": obj.get("checksums") or {},
        "parts": obj.get("parts"),
    }


def _preserve_null_version(bucket_name: str, key: str, versions: list,
                           prior_obj: dict | None):
    """Keep the pre-versioning object addressable as the "null" version.

    An object written before versioning was enabled has no version id; the
    first versioned write (or delete marker) that lands on top of it must
    leave it in the version index under VersionId "null" rather than silently
    discarding it — AWS keeps the null version in the stack."""
    if prior_obj is None or prior_obj.get("version_id"):
        return
    if any(v["version_id"] == "null" for v in versions):
        return
    versions.append(_version_entry_from_record(
        prior_obj, "null", _read_body(bucket_name, key, prior_obj)))


def _record_object_version(bucket_name: str, key: str, prior_obj: dict | None,
                           obj: dict, data) -> str | None:
    """Version bookkeeping for a write landing at `key`.

    An Enabled bucket mints a new version id; a Suspended bucket stores the
    write as the "null" version, REPLACING any previous null version, and
    answers without x-amz-version-id — so this returns the id to put on the
    response, or None for unversioned and suspended writes.  `prior_obj` is
    the current-object record the write displaced (captured before the
    overwrite), preserved as the null version when it predates versioning."""
    versioning = _bucket_versioning.get(bucket_name)
    if versioning not in ("Enabled", "Suspended"):
        return None
    vkey = (bucket_name, key)
    versions = _object_versions.setdefault(vkey, [])
    if versioning == "Enabled":
        _preserve_null_version(bucket_name, key, versions, prior_obj)
        version_id = new_uuid()
        obj["version_id"] = version_id
    else:
        version_id = "null"
        versions[:] = [v for v in versions if v["version_id"] != "null"]
    entry = _version_entry_from_record(obj, version_id, data)
    entry["is_latest"] = True
    for v in versions:
        v["is_latest"] = False
    versions.append(entry)
    return version_id if versioning == "Enabled" else None


def _record_delete_marker(bucket_name: str, key: str,
                          prior_obj: dict | None) -> str:
    """Append a delete marker per the bucket's versioning state and return its
    version id: a fresh id on Enabled, the literal "null" on Suspended — where
    the marker REPLACES any existing null version, as AWS does."""
    vkey = (bucket_name, key)
    versions = _object_versions.setdefault(vkey, [])
    if _bucket_versioning.get(bucket_name) == "Enabled":
        _preserve_null_version(bucket_name, key, versions, prior_obj)
        marker_id = new_uuid()
    else:
        marker_id = "null"
        versions[:] = [v for v in versions if v["version_id"] != "null"]
    for v in versions:
        v["is_latest"] = False
    versions.append({
        "version_id": marker_id,
        "last_modified": now_iso(),
        "etag": "",
        "size": 0,
        "is_latest": True,
        "is_delete_marker": True,
    })
    return marker_id


def _delete_marker_404_headers(bucket_name: str, key: str) -> dict:
    """Headers a 404 for `key` must carry when its current version is a delete
    marker: AWS marks the miss with x-amz-delete-marker and the marker's id."""
    versions = _object_versions.get((bucket_name, key)) or []
    latest = versions[-1] if versions else None
    if latest is not None and latest.get("is_delete_marker") and latest.get("is_latest"):
        return {"x-amz-delete-marker": "true",
                "x-amz-version-id": latest["version_id"]}
    return {}


def _delete_object_version(bucket: dict, bucket_name: str, key: str,
                           version_id: str) -> tuple[bool, bool]:
    """Physically remove the exact version (or delete marker) addressed by
    `version_id`, then reconcile the current-object pointer and is_latest flags.

    Returns (found, was_delete_marker). `found` is False when no version matched
    — S3 reports that as an error in the batch API but 204s the single delete.
    """
    vkey = (bucket_name, key)
    versions = _object_versions.get(vkey)

    # No tracked history: the only addressable version is the current object,
    # exposed under the "null" id (objects put before versioning was enabled).
    # The guard on version_id matters: "null" only addresses a current object
    # that actually IS the null version — one without a version id of its own.
    if not versions:
        if (version_id == "null" and key in bucket["objects"]
                and not bucket["objects"][key].get("version_id")):
            _purge_current_object(bucket_name, key, bucket)
            return True, False
        return False, False

    idx = next(
        (i for i, v in enumerate(versions) if v["version_id"] == version_id), None
    )
    if idx is None:
        if (version_id == "null" and key in bucket["objects"]
                and not bucket["objects"][key].get("version_id")):
            _purge_current_object(bucket_name, key, bucket)
            return True, False
        return False, False

    removed = versions.pop(idx)
    was_delete_marker = bool(removed.get("is_delete_marker"))
    # Per-version tags and ACLs travel with the version being removed.
    _object_tags.pop((bucket_name, key, version_id), None)
    _object_acl.pop((bucket_name, key, version_id), None)

    if not versions:
        # History is now empty — drop the index entry and the current object.
        _object_versions.pop(vkey, None)
        _purge_current_object(bucket_name, key, bucket)
        return True, was_delete_marker

    # The newest surviving entry becomes latest (list is append-ordered).
    for v in versions:
        v["is_latest"] = False
    latest = versions[-1]
    latest["is_latest"] = True

    # Reconcile the current-object pointer (used by Head/GetObject without a
    # VersionId): a delete marker hides the object; a real version exposes it.
    if latest.get("is_delete_marker"):
        bucket["objects"].pop(key, None)
    else:
        bucket["objects"][key] = _object_record_from_version(latest)
    return True, was_delete_marker


def _delete_object(bucket_name: str, key: str, headers: dict | None = None,
                   query_params: dict | None = None):
    headers = headers or {}
    query_params = query_params or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    # Conditional delete. Measured against S3 in eu-west-1: the condition is
    # evaluated against the current object and nothing else, matching the
    # guide's "Conditional delete evaluations only apply to the current
    # version of the object."
    #
    #   no current object (absent, or a delete marker on top)  -> 404 NoSuchKey
    #   current object, ETag differs                           -> 412
    #   current object, matching ETag or If-Match: *           -> 204
    #
    # S3 does not look under a delete marker, so a key whose history still
    # holds a real version is as gone as one that was never written. An
    # absent key is not an already-completed delete either: the request is
    # conditional, and there is nothing for the condition to hold against.
    if_match = (headers.get("if-match") or "").strip()
    if if_match:
        _cur = bucket["objects"].get(key)
        if _cur is None:
            return _error("NoSuchKey", "The specified key does not exist.",
                          404, f"/{bucket_name}/{key}")
        if if_match != "*" and if_match.strip('"') != _cur["etag"].strip('"'):
            return _error(
                "PreconditionFailed",
                "At least one of the preconditions you specified did not hold.",
                412,
                f"/{bucket_name}/{key}",
            )

    if key in bucket["objects"]:
        lock_err = _check_object_lock(bucket_name, key, headers)
        if lock_err:
            return lock_err

    # An explicit VersionId permanently removes exactly that version (or that
    # specific delete marker) — it never creates a new marker. Only a delete
    # WITHOUT a VersionId falls through to the delete-marker path below.
    version_id = _qp(query_params, "versionId", "")
    if version_id:
        _found, was_delete_marker = _delete_object_version(
            bucket, bucket_name, key, version_id
        )
        # S3 returns 204 whether or not the version existed, echoing the
        # addressed VersionId (and delete-marker flag when one was removed).
        resp_headers = {"x-amz-version-id": version_id}
        if was_delete_marker:
            resp_headers["x-amz-delete-marker"] = "true"
        if _found:
            _fire_s3_event_async(bucket_name, key, "s3:ObjectRemoved:Delete")
        return 204, resp_headers, b""

    versioning = _bucket_versioning.get(bucket_name, "")
    if versioning in ("Enabled", "Suspended"):
        # Add a delete marker instead of removing version history
        delete_marker_id = _record_delete_marker(
            bucket_name, key, bucket["objects"].get(key))
        existed = key in bucket["objects"]
        bucket["objects"].pop(key, None)
        if existed:
            _fire_s3_event_async(
                bucket_name, key, "s3:ObjectRemoved:Delete", deletion_type="Delete Marker Created"
            )
        return 204, {"x-amz-delete-marker": "true", "x-amz-version-id": delete_marker_id}, b""

    existed = key in bucket["objects"]
    bucket["objects"].pop(key, None)
    _object_tags.pop((bucket_name, key, None), None)
    _object_retention.pop((bucket_name, key), None)
    _object_legal_hold.pop((bucket_name, key), None)
    _object_acl.pop((bucket_name, key, None), None)
    _delete_persisted_object(bucket_name, key)

    if existed:
        _fire_s3_event_async(bucket_name, key, "s3:ObjectRemoved:Delete")
    return 204, {}, b""


def _check_object_lock(bucket_name: str, key: str, headers: dict) -> tuple | None:
    hold = _object_legal_hold.get((bucket_name, key))
    if hold == "ON":
        return _error(
            "AccessDenied",
            "Access Denied because object protected by object lock.",
            403,
        )

    retention = _object_retention.get((bucket_name, key))
    if retention:
        retain_until = retention.get("RetainUntilDate", "")
        if retain_until and retain_until > now_iso():
            mode = retention.get("Mode", "")
            if mode == "COMPLIANCE":
                return _error(
                    "AccessDenied",
                    "Access Denied because object protected by object lock.",
                    403,
                )
            if mode == "GOVERNANCE":
                bypass = headers.get("x-amz-bypass-governance-retention", "").lower()
                if bypass != "true":
                    return _error(
                        "AccessDenied",
                        "Access Denied because object protected by object lock.",
                        403,
                    )
    return None


def _parse_http_date(value: str):
    """Parse an RFC 7231 HTTP-date header into a tz-aware UTC datetime, or None."""
    from email.utils import parsedate_to_datetime
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt


def _object_mtime_dt(obj: dict):
    """The object's Last-Modified as a tz-aware datetime at second granularity
    (Last-Modified is second-precise, so preconditions must compare at that resolution)."""
    iso = obj.get("last_modified")
    if not iso:
        return None
    try:
        dt = _dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(microsecond=0)


def _copy_object(bucket_name: str, dest_key: str, headers: dict):
    # Split the raw header at "?" before percent-decoding: a key legitimately
    # containing "?versionId" arrives encoded (%3FversionId) and must stay
    # part of the key, while a real versionId qualifier arrives as a bare "?".
    raw_source = headers.get("x-amz-copy-source", "").lstrip("/")
    raw_path, _, src_query = raw_source.partition("?")
    src_path = url_unquote(raw_path)
    src_parts = src_path.split("/", 1)
    if len(src_parts) < 2:
        return _error(
            "InvalidArgument",
            "Copy Source must mention the source bucket and key: /sourcebucket/sourcekey",
            400,
        )

    src_bucket_name, src_key = src_parts
    src_bucket = _ensure_bucket(src_bucket_name)
    if src_bucket is None:
        return _no_such_bucket(src_bucket_name)

    # A `?versionId=` on the copy source selects a specific version to copy,
    # rather than the current object (AWS copies that exact version).
    src_version_id = None
    if src_query:
        src_version_id = _parse_qs(src_query, keep_blank_values=True).get(
            "versionId", [None]
        )[0]

    dest_bucket = _ensure_bucket(bucket_name)
    if dest_bucket is None:
        return _no_such_bucket(bucket_name)

    if src_version_id:
        ventry = next(
            (v for v in _object_versions.get((src_bucket_name, src_key), [])
             if v["version_id"] == src_version_id),
            None,
        )
        if ventry is None or ventry.get("is_delete_marker"):
            return _error(
                "NoSuchVersion",
                "The specified version does not exist.",
                404,
                f"/{src_bucket_name}/{src_key}",
            )
        # Synthesize a source object from the stored version — the index keeps
        # the body plus the wire metadata, so a COPY metadata-directive carries
        # the version's user metadata and headers like a current-object copy.
        src_obj = {
            "body": ventry.get("data", b""),
            "etag": ventry["etag"],
            "size": ventry["size"],
            "last_modified": ventry["last_modified"],
            "content_type": ventry.get("content_type") or "application/octet-stream",
            "content_encoding": ventry.get("content_encoding"),
            "metadata": ventry.get("metadata", {}),
            "preserved_headers": ventry.get("preserved_headers", {}),
            "storage_class": ventry.get("storage_class") or "STANDARD",
            "checksums": ventry.get("checksums") or {},
            "version_id": src_version_id,
        }
    else:
        if src_key not in src_bucket["objects"]:
            return _error(
                "NoSuchKey",
                "The specified key does not exist.",
                404,
                f"/{src_bucket_name}/{src_key}",
            )
        src_obj = src_bucket["objects"][src_key]

    sse_src_err = _check_sse_c_copy_source(headers, src_obj)
    if sse_src_err is not None:
        return sse_src_err

    # AWS echoes the copied source version on a versioned source.
    copy_src_vid = src_version_id or src_obj.get("version_id")

    # Copy-source preconditions. Per the AWS CopyObject reference (RFC 7232
    # precedence): x-amz-copy-source-if-match takes precedence over
    # -if-unmodified-since, and -if-none-match over -if-modified-since, so the
    # date header only applies when its ETag counterpart is absent.
    _precond_msg = "At least one of the pre-conditions you specified did not hold"
    src_mtime = _object_mtime_dt(src_obj)

    if_match = headers.get("x-amz-copy-source-if-match", "")
    if if_match:
        if if_match.strip('"') != src_obj["etag"].strip('"'):
            return _error("PreconditionFailed", _precond_msg, 412)
    else:
        unmod = _parse_http_date(headers.get("x-amz-copy-source-if-unmodified-since", ""))
        # "Unmodified since" fails when the source was modified after the given time.
        if unmod and src_mtime and src_mtime > unmod:
            return _error("PreconditionFailed", _precond_msg, 412)

    if_none_match = headers.get("x-amz-copy-source-if-none-match", "")
    if if_none_match:
        if if_none_match.strip('"') == src_obj["etag"].strip('"'):
            return _error("PreconditionFailed", _precond_msg, 412)
    else:
        mod = _parse_http_date(headers.get("x-amz-copy-source-if-modified-since", ""))
        # "Modified since" fails when the source has NOT changed since the given time.
        if mod and src_mtime and src_mtime <= mod:
            return _error("PreconditionFailed", _precond_msg, 412)

    directive = headers.get("x-amz-metadata-directive", "COPY").upper()
    if directive == "REPLACE":
        metadata = _extract_user_metadata(headers)
        content_type = headers.get("content-type", src_obj["content_type"])
        content_encoding = headers.get(
            "content-encoding", src_obj.get("content_encoding")
        )
        preserved = {}
        for h in _PRESERVED_HEADERS:
            val = headers.get(h)
            if val is not None:
                preserved[h] = val
    else:
        metadata = dict(src_obj.get("metadata", {}))
        content_type = src_obj["content_type"]
        content_encoding = src_obj.get("content_encoding")
        preserved = dict(src_obj.get("preserved_headers", {}))

    # The destination's encryption always comes from THIS request (or the
    # destination bucket's default), never from the source — a COPY metadata
    # directive must not smuggle the source's SSE state across.
    dest_sse, dest_sse_err = _resolve_sse_write_headers(headers, bucket_name)
    if dest_sse_err is not None:
        return dest_sse_err
    for h in _SSE_STORED_HEADERS:
        preserved.pop(h, None)
    preserved.update(dest_sse)

    dest_sc, sc_err = _resolve_storage_class(
        headers, default=src_obj.get("storage_class") or "STANDARD"
    )
    if sc_err:
        return sc_err

    new_etag = src_obj["etag"]
    last_modified = now_iso()
    src_body = _read_body(src_bucket_name, src_key, src_obj)
    # AWS CopyObject preserves the source's whole-object checksum unless the
    # caller asks for a different algorithm via `x-amz-checksum-algorithm` /
    # `x-amz-sdk-checksum-algorithm`. The latter case is handled by the same
    # resolver as PutObject, against the copied body.
    dest_checksums = dict(src_obj.get("checksums") or {})
    if headers.get("x-amz-sdk-checksum-algorithm") or any(
        headers.get(f"x-amz-checksum-{a}") for a in _S3_CHECKSUM_HEADERS
    ):
        resolved, csum_err = _resolve_object_checksums(src_body, headers)
        if csum_err:
            return csum_err
        dest_checksums.update(resolved)
    dest_obj = {
        "body": src_body,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "etag": new_etag,
        "last_modified": last_modified,
        "size": src_obj["size"],
        "metadata": metadata,
        "preserved_headers": preserved,
        "storage_class": dest_sc,
        "checksums": dest_checksums,
    }
    dest_prior_obj = dest_bucket["objects"].get(dest_key)
    dest_bucket["objects"][dest_key] = dest_obj

    # --- Resolve tag payload now; commit after dest version_id is assigned
    #     (object tags are per-version per AWS).
    tagging_directive = headers.get("x-amz-tagging-directive", "COPY").upper()
    pending_dest_tags: dict | None = None
    if tagging_directive == "REPLACE":
        tagging_header = headers.get("x-amz-tagging", "")
        if tagging_header:
            pending_dest_tags = {
                k: v[0] for k, v in _parse_qs(tagging_header, keep_blank_values=True).items()
            }
    else:
        src_tags = _object_tags.get(
            (src_bucket_name, src_key, src_obj.get("version_id"))
        )
        if src_tags:
            pending_dest_tags = dict(src_tags)

    # --- Preserve lock / retention ---
    src_retention = _object_retention.get((src_bucket_name, src_key))
    if src_retention:
        _object_retention[(bucket_name, dest_key)] = dict(src_retention)
    else:
        _object_retention.pop((bucket_name, dest_key), None)

    src_hold = _object_legal_hold.get((src_bucket_name, src_key))
    if src_hold:
        _object_legal_hold[(bucket_name, dest_key)] = src_hold
    else:
        _object_legal_hold.pop((bucket_name, dest_key), None)

    _fire_s3_event_async(
        bucket_name,
        dest_key,
        "s3:ObjectCreated:Copy",
        size=dest_obj["size"],
        etag=new_etag,
    )

    resp_headers = {"Content-Type": "application/xml"}
    resp_headers.update(dest_sse)
    if copy_src_vid:
        resp_headers["x-amz-copy-source-version-id"] = copy_src_vid
    if dest_sc != "STANDARD":
        resp_headers["x-amz-storage-class"] = dest_sc
    version_id = _record_object_version(
        bucket_name, dest_key, dest_prior_obj, dest_obj, src_body)
    if version_id:
        resp_headers["x-amz-version-id"] = version_id

    # Persist only after the versioning block: the .meta.json sidecar must
    # carry the version_id assigned above (#1058).
    if S3_PERSIST:
        _persist_object(bucket_name, dest_key, dest_obj)

    dest_version_id = dest_obj.get("version_id")
    if pending_dest_tags is not None:
        _object_tags[(bucket_name, dest_key, dest_version_id)] = pending_dest_tags
    else:
        _object_tags.pop((bucket_name, dest_key, dest_version_id), None)

    root = Element("CopyObjectResult", xmlns=S3_NS)
    SubElement(root, "LastModified").text = last_modified
    SubElement(root, "ETag").text = new_etag
    return 200, resp_headers, _xml_body(root)


# ---------------------------------------------------------------------------
# Object tagging
# ---------------------------------------------------------------------------


def _resolve_subresource_version(query_params: dict, bucket: dict, key: str):
    """Resolve the (key, version_id) pair an object subresource op acts on.

    Per AWS, object tags and ACLs are per-version: when ``?versionId=`` is
    present the op targets that specific version; otherwise it targets the
    current object. The literal ``versionId=null`` means the pre-versioning
    object (stored as ``None`` in our key tuple)."""
    vid = _qp(query_params or {}, "versionId", "")
    if vid:
        return None if vid == "null" else vid
    obj = bucket["objects"].get(key)
    return obj.get("version_id") if obj else None


def _get_object_tagging(bucket_name: str, key: str, query_params: dict | None = None):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    version_id = _resolve_subresource_version(query_params, bucket, key)
    tags = _object_tags.get((bucket_name, key, version_id), {})
    root = Element("Tagging", xmlns=S3_NS)
    tag_set = SubElement(root, "TagSet")
    for k, v in tags.items():
        tag = SubElement(tag_set, "Tag")
        SubElement(tag, "Key").text = k
        SubElement(tag, "Value").text = v
    resp_headers = {"Content-Type": "application/xml"}
    if version_id:
        resp_headers["x-amz-version-id"] = version_id
    return 200, resp_headers, _xml_body(root)


def _put_object_tagging(
    bucket_name: str, key: str, body: bytes, query_params: dict | None = None
):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )
    try:
        tags = _parse_tags_xml(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)
    if len(tags) > 10:
        return _error("BadRequest", "Object tags cannot be greater than 10", 400)
    version_id = _resolve_subresource_version(query_params, bucket, key)
    _object_tags[(bucket_name, key, version_id)] = tags
    resp_headers = {"Content-Type": "application/xml"}
    if version_id:
        resp_headers["x-amz-version-id"] = version_id
    return 200, resp_headers, b""


def _delete_object_tagging(
    bucket_name: str, key: str, query_params: dict | None = None
):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )
    version_id = _resolve_subresource_version(query_params, bucket, key)
    _object_tags.pop((bucket_name, key, version_id), None)
    resp_headers = {}
    if version_id:
        resp_headers["x-amz-version-id"] = version_id
    return 204, resp_headers, b""


# ---------------------------------------------------------------------------
# Object Lock
# ---------------------------------------------------------------------------


def _get_object_lock_configuration(bucket_name: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    lock = _bucket_object_lock.get(bucket_name)
    if not lock:
        return _error(
            "ObjectLockConfigurationNotFoundError",
            "Object Lock configuration does not exist for this bucket",
            404,
            f"/{bucket_name}",
        )

    root = Element("ObjectLockConfiguration", xmlns=S3_NS)
    SubElement(root, "ObjectLockEnabled").text = "Enabled"
    retention = lock.get("default_retention")
    if retention:
        rule_el = SubElement(root, "Rule")
        ret_el = SubElement(rule_el, "DefaultRetention")
        SubElement(ret_el, "Mode").text = retention["Mode"]
        if "Days" in retention:
            SubElement(ret_el, "Days").text = str(retention["Days"])
        if "Years" in retention:
            SubElement(ret_el, "Years").text = str(retention["Years"])
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_object_lock_configuration(bucket_name: str, body: bytes):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    versioning = _bucket_versioning.get(bucket_name, "")
    if versioning != "Enabled":
        return _error(
            "InvalidBucketState",
            "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
            409,
            f"/{bucket_name}",
        )

    try:
        xml_root = fromstring(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    enabled_el = _find_xml_tag(xml_root, "ObjectLockEnabled")
    if enabled_el is None or enabled_el.text != "Enabled":
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    default_retention = None
    rule_el = _find_xml_tag(xml_root, "Rule")
    if rule_el is not None:
        ret_el = _find_xml_tag(rule_el, "DefaultRetention")
        if ret_el is None:
            return _error(
                "MalformedXML", "The XML you provided was not well-formed", 400
            )

        mode_el = _find_xml_tag(ret_el, "Mode")
        days_el = _find_xml_tag(ret_el, "Days")
        years_el = _find_xml_tag(ret_el, "Years")

        if mode_el is None or mode_el.text not in ("GOVERNANCE", "COMPLIANCE"):
            return _error(
                "MalformedXML", "The XML you provided was not well-formed", 400
            )

        has_days = days_el is not None and days_el.text
        has_years = years_el is not None and years_el.text
        if (has_days and has_years) or (not has_days and not has_years):
            return _error(
                "MalformedXML", "The XML you provided was not well-formed", 400
            )

        default_retention = {"Mode": mode_el.text}
        try:
            if has_days:
                default_retention["Days"] = int(days_el.text)
            if has_years:
                default_retention["Years"] = int(years_el.text)
        except (ValueError, TypeError):
            return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    _bucket_object_lock[bucket_name] = {
        "enabled": True,
        "default_retention": default_retention,
    }
    return 200, {"Content-Type": "application/xml"}, b""


def _get_object_retention(bucket_name: str, key: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    lock = _bucket_object_lock.get(bucket_name)
    if not lock:
        return _error(
            "InvalidRequest", "Bucket is missing Object Lock Configuration", 400
        )

    retention = _object_retention.get((bucket_name, key))
    if not retention:
        return _error(
            "NoSuchObjectLockConfiguration",
            "The specified object does not have a ObjectLock configuration",
            404,
        )

    root = Element("Retention", xmlns=S3_NS)
    SubElement(root, "Mode").text = retention["Mode"]
    SubElement(root, "RetainUntilDate").text = retention["RetainUntilDate"]
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_object_retention(bucket_name: str, key: str, body: bytes, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    lock = _bucket_object_lock.get(bucket_name)
    if not lock:
        return _error(
            "InvalidRequest", "Bucket is missing Object Lock Configuration", 400
        )

    try:
        xml_root = fromstring(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    mode_el = _find_xml_tag(xml_root, "Mode")
    date_el = _find_xml_tag(xml_root, "RetainUntilDate")

    if mode_el is None or mode_el.text not in ("GOVERNANCE", "COMPLIANCE"):
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)
    if date_el is None or not date_el.text:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    retain_until = date_el.text

    existing = _object_retention.get((bucket_name, key))
    if existing:
        is_reducing = existing.get("RetainUntilDate", "") > retain_until or (
            mode_el.text == "GOVERNANCE" and existing.get("Mode") == "COMPLIANCE"
        )
        if is_reducing:
            if existing.get("Mode") == "COMPLIANCE":
                return _error(
                    "AccessDenied",
                    "Access Denied because object protected by object lock.",
                    403,
                )
            if existing.get("Mode") == "GOVERNANCE":
                bypass = headers.get("x-amz-bypass-governance-retention", "").lower()
                if bypass != "true":
                    return _error(
                        "AccessDenied",
                        "Access Denied because object protected by object lock.",
                        403,
                    )

    _object_retention[(bucket_name, key)] = {
        "Mode": mode_el.text,
        "RetainUntilDate": retain_until,
    }
    return 200, {"Content-Type": "application/xml"}, b""


def _get_object_legal_hold(bucket_name: str, key: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    lock = _bucket_object_lock.get(bucket_name)
    if not lock:
        return _error(
            "InvalidRequest", "Bucket is missing Object Lock Configuration", 400
        )

    status = _object_legal_hold.get((bucket_name, key))
    if status is None:
        return _error(
            "NoSuchObjectLockConfiguration",
            "The specified object does not have a ObjectLock configuration",
            404,
        )

    root = Element("LegalHold", xmlns=S3_NS)
    SubElement(root, "Status").text = status
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_object_legal_hold(bucket_name: str, key: str, body: bytes):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    lock = _bucket_object_lock.get(bucket_name)
    if not lock:
        return _error(
            "InvalidRequest", "Bucket is missing Object Lock Configuration", 400
        )

    try:
        xml_root = fromstring(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    status_el = _find_xml_tag(xml_root, "Status")
    if status_el is None or status_el.text not in ("ON", "OFF"):
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    _object_legal_hold[(bucket_name, key)] = status_el.text
    return 200, {"Content-Type": "application/xml"}, b""


# ---------------------------------------------------------------------------
# Object ACL (?acl subresource)
# ---------------------------------------------------------------------------

# Canned ACLs accepted by PutObjectAcl `x-amz-acl` header per the AWS S3 API
# reference. Stored verbatim — ministack does not enforce ACL semantics on the
# data plane, only round-trips the value so SDK callers that read it back
# (terraform, CDK, custom code) see what they set.
_CANNED_OBJECT_ACLS = {
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
}

# Canned ACLs accepted by CreateBucket / PutBucketAcl `x-amz-acl`: the object
# set minus the object-only variants, plus log-delivery-write (bucket-only).
_CANNED_BUCKET_ACLS = {
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "log-delivery-write",
}


_ACL_GROUP_ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"
_ACL_GROUP_AUTH_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
_ACL_GROUP_LOG_DELIVERY = "http://acs.amazonaws.com/groups/s3/LogDelivery"

# Group grants each canned ACL adds on top of the owner's FULL_CONTROL, per the
# AWS S3 "Canned ACL" reference. The bucket-owner-* / aws-exec-read variants add
# only owner/bucket-owner grants, which collapse to the owner in MiniStack's
# single-account model, so they carry no extra group grant here.
_CANNED_ACL_GROUP_GRANTS = {
    "public-read": [(_ACL_GROUP_ALL_USERS, "READ")],
    "public-read-write": [(_ACL_GROUP_ALL_USERS, "READ"), (_ACL_GROUP_ALL_USERS, "WRITE")],
    "authenticated-read": [(_ACL_GROUP_AUTH_USERS, "READ")],
    "log-delivery-write": [(_ACL_GROUP_LOG_DELIVERY, "WRITE"), (_ACL_GROUP_LOG_DELIVERY, "READ_ACP")],
}


def _canned_acl_policy_xml(canned: str, owner_id: str) -> str:
    """AccessControlPolicy XML for a canned ACL: the owner's FULL_CONTROL plus the
    group grants the canned value implies (e.g. public-read grants AllUsers READ),
    so GetObjectAcl reflects what the canned ACL actually means rather than a bare
    owner grant. (#1322, rest of defect 9)"""
    grants = [
        '<Grant>'
        '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:type="CanonicalUser">'
        f"<ID>{owner_id}</ID><DisplayName>ministack</DisplayName></Grantee>"
        "<Permission>FULL_CONTROL</Permission></Grant>"
    ]
    for uri, perm in _CANNED_ACL_GROUP_GRANTS.get(canned, []):
        grants.append(
            '<Grant>'
            '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            f'xsi:type="Group"><URI>{uri}</URI></Grantee>'
            f"<Permission>{perm}</Permission></Grant>"
        )
    return (
        XML_DECL.decode() + "\n"
        f'<AccessControlPolicy xmlns="{S3_NS}">'
        f"<Owner><ID>{owner_id}</ID><DisplayName>ministack</DisplayName></Owner>"
        f'<AccessControlList>{"".join(grants)}</AccessControlList>'
        "</AccessControlPolicy>"
    )


def _default_object_acl_xml() -> bytes:
    """Default ACL real AWS returns when no ACL has been set on an object:
    a single Grant of FULL_CONTROL to the bucket owner (CanonicalUser).
    The canonical-user ID is derived from the request's account so cross-
    account callers don't all collide on the same fake ID."""
    owner_id = _canonical_owner_id()
    return (
        XML_DECL + b"\n"
        b'<AccessControlPolicy xmlns="' + S3_NS.encode() + b'">'
        b"<Owner><ID>" + owner_id.encode() + b"</ID>"
        b"<DisplayName>ministack</DisplayName></Owner>"
        b"<AccessControlList><Grant>"
        b'<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        b'xsi:type="CanonicalUser">'
        b"<ID>" + owner_id.encode() + b"</ID>"
        b"<DisplayName>ministack</DisplayName></Grantee>"
        b"<Permission>FULL_CONTROL</Permission>"
        b"</Grant></AccessControlList></AccessControlPolicy>"
    )


def _get_object_acl(bucket_name: str, key: str, query_params: dict | None = None):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    # ACLs are per-version, like tags: a `?versionId=` reads that version's
    # ACL, and a version that never had one set reads as the default policy.
    version_id = _resolve_subresource_version(query_params, bucket, key)
    stored = _object_acl.get((bucket_name, key, version_id))
    body = stored.encode("utf-8") if stored else _default_object_acl_xml()
    resp_headers = {"Content-Type": "application/xml"}
    if version_id:
        resp_headers["x-amz-version-id"] = version_id
    return 200, resp_headers, body


def _put_object_acl(bucket_name: str, key: str, body: bytes, headers: dict,
                    query_params: dict | None = None):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    # A `?versionId=` sets that specific version's ACL; without it the current
    # version's. Later versions are separate objects and keep their defaults.
    version_id = _resolve_subresource_version(query_params, bucket, key)

    # Canned ACL from x-amz-acl header takes precedence and is mutually
    # exclusive with an XML body per the AWS API reference. Either path
    # stores the resulting policy XML so GetObjectAcl round-trips the value
    # the caller set, matching what real AWS would return.
    canned = headers.get("x-amz-acl")
    if canned:
        if canned not in _CANNED_OBJECT_ACLS:
            return _error("InvalidArgument",
                          f"Invalid x-amz-acl value: {canned}", 400)
        _object_acl[(bucket_name, key, version_id)] = (
            _canned_acl_policy_xml(canned, _canonical_owner_id()))
        return 200, {}, b""

    if not body:
        return _error("MissingSecurityHeader",
                      "Your request was missing a required header.", 400)
    try:
        # Validate XML well-formedness — real AWS rejects malformed bodies
        # with MalformedACLError. We don't enforce grantee/permission
        # semantics on the data plane, so any well-formed AccessControlPolicy
        # is accepted and round-tripped verbatim.
        fromstring(body)
    except Exception:
        return _error("MalformedACLError",
                      "The XML you provided was not well-formed or did not validate "
                      "against our published schema.", 400)
    _object_acl[(bucket_name, key, version_id)] = body.decode("utf-8", errors="replace")
    return 200, {}, b""


# ---------------------------------------------------------------------------
# Replication Configuration
# ---------------------------------------------------------------------------


def _put_bucket_replication(bucket_name: str, body: bytes):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    versioning = _bucket_versioning.get(bucket_name, "")
    if versioning != "Enabled":
        return _error(
            "InvalidRequest",
            "Versioning must be 'Enabled' on the bucket to apply a replication configuration",
            400,
            f"/{bucket_name}",
        )

    try:
        xml_root = fromstring(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    role_el = _find_xml_tag(xml_root, "Role")
    role = role_el.text if role_el is not None and role_el.text else ""

    rules = []
    for rule_el in list(xml_root.findall("{%s}Rule" % S3_NS)) or list(
        xml_root.findall("Rule")
    ):
        rule: dict = {}
        id_el = _find_xml_tag(rule_el, "ID")
        rule["ID"] = id_el.text if id_el is not None and id_el.text else new_uuid()[:8]
        status_el = _find_xml_tag(rule_el, "Status")
        rule["Status"] = (
            status_el.text if status_el is not None and status_el.text else "Enabled"
        )
        prefix_el = _find_xml_tag(rule_el, "Prefix")
        if prefix_el is not None and prefix_el.text is not None:
            rule["Prefix"] = prefix_el.text
        dest_el = _find_xml_tag(rule_el, "Destination")
        if dest_el is not None:
            dest: dict = {}
            bucket_el = _find_xml_tag(dest_el, "Bucket")
            if bucket_el is not None and bucket_el.text:
                dest["Bucket"] = bucket_el.text
                # Validate destination bucket
                dest_name = (
                    bucket_el.text.split(":::")[-1]
                    if ":::" in bucket_el.text
                    else bucket_el.text
                )
                dest_bucket = _ensure_bucket(dest_name)
                if dest_bucket is not None:
                    dest_versioning = _bucket_versioning.get(dest_name, "")
                    if dest_versioning != "Enabled":
                        return _error(
                            "InvalidRequest",
                            "Destination bucket must have versioning enabled.",
                            400,
                        )
            sc_el = _find_xml_tag(dest_el, "StorageClass")
            if sc_el is not None and sc_el.text:
                dest["StorageClass"] = sc_el.text
            rule["Destination"] = dest
        rules.append(rule)

    if not rules:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    _bucket_replication[bucket_name] = {"Role": role, "Rules": rules}
    return 200, {"Content-Type": "application/xml"}, b""


def _get_bucket_replication(bucket_name: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    repl = _bucket_replication.get(bucket_name)
    if repl is None:
        return _error(
            "ReplicationConfigurationNotFoundError",
            "The replication configuration was not found",
            404,
            f"/{bucket_name}",
        )

    root = Element("ReplicationConfiguration", xmlns=S3_NS)
    SubElement(root, "Role").text = repl.get("Role", "")
    for rule in repl.get("Rules", []):
        rule_el = SubElement(root, "Rule")
        SubElement(rule_el, "ID").text = rule.get("ID", "")
        SubElement(rule_el, "Status").text = rule.get("Status", "Enabled")
        if "Prefix" in rule:
            SubElement(rule_el, "Prefix").text = rule["Prefix"]
        dest = rule.get("Destination", {})
        if dest:
            dest_el = SubElement(rule_el, "Destination")
            if "Bucket" in dest:
                SubElement(dest_el, "Bucket").text = dest["Bucket"]
            if "StorageClass" in dest:
                SubElement(dest_el, "StorageClass").text = dest["StorageClass"]
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _delete_bucket_replication(bucket_name: str):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)
    _bucket_replication.pop(bucket_name, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# List objects
# ---------------------------------------------------------------------------


def _collect_list_entries(
    bucket_objects: dict, prefix: str, delimiter: str, max_keys: int, start_after: str
):
    """Collect contents and common prefixes with pagination as an ordered list of
    "rows": a delimiter-collapsed key becomes a single common-prefix row (value =
    the prefix), any other key is its own contents row (value = the key). Rows are
    globally sorted, ``marker`` excludes rows at or before it, and ``next_marker``
    is the value of the LAST row emitted — so when a page ends on a collapsed group
    the continuation marker is the prefix (e.g. ``boo/``), not an underlying key
    (``boo/baz/xyzzy``), and resuming from it skips the whole group instead of
    re-walking it. (#1322, defect 5)

    Returns (contents, common_prefixes, is_truncated, next_marker).
    """
    rows: list[tuple[str, bool]] = []  # (value, is_common_prefix)
    seen_prefixes: set[str] = set()
    for k in sorted(k for k in bucket_objects if k.startswith(prefix)):
        if delimiter:
            suffix = k[len(prefix):]
            delim_idx = suffix.find(delimiter)
            if delim_idx >= 0:
                cp = prefix + suffix[: delim_idx + len(delimiter)]
                if cp not in seen_prefixes:
                    seen_prefixes.add(cp)
                    rows.append((cp, True))
                continue
        rows.append((k, False))

    if start_after:
        rows = [row for row in rows if row[0] > start_after]

    contents: list[str] = []
    common_prefixes: list[str] = []
    is_truncated = False
    next_marker = ""
    for i, (value, is_prefix) in enumerate(rows):
        if i >= max_keys:
            is_truncated = True
            break
        if is_prefix:
            common_prefixes.append(value)
        else:
            contents.append(value)
        next_marker = value

    return contents, common_prefixes, is_truncated, next_marker


def _list_objects_v1(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    prefix = _qp(query_params, "prefix", "")
    delimiter = _qp(query_params, "delimiter", "")
    max_keys = int(_qp(query_params, "max-keys", "1000"))
    marker = _qp(query_params, "marker", "")
    encoding_type = _qp(query_params, "encoding-type", "")
    encode = encoding_type == "url"

    contents, common_prefixes, is_truncated, next_marker = _collect_list_entries(
        bucket["objects"],
        prefix,
        delimiter,
        max_keys,
        marker,
    )

    root = Element("ListBucketResult", xmlns=S3_NS)
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = (
        _url_encode(prefix) if encode and prefix else prefix
    )
    SubElement(root, "Marker").text = (
        _url_encode(marker) if encode and marker else marker
    )
    if delimiter:
        SubElement(root, "Delimiter").text = (
            _url_encode(delimiter) if encode else delimiter
        )
    if encoding_type:
        SubElement(root, "EncodingType").text = encoding_type
    SubElement(root, "MaxKeys").text = str(max_keys)
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    # AWS only returns NextMarker when delimiter is specified.
    if is_truncated and next_marker and delimiter:
        SubElement(root, "NextMarker").text = (
            _url_encode(next_marker) if encode else next_marker
        )

    for k in contents:
        obj = bucket["objects"][k]
        c = SubElement(root, "Contents")
        SubElement(c, "Key").text = _url_encode(k) if encode else k
        SubElement(c, "LastModified").text = obj["last_modified"]
        SubElement(c, "ETag").text = obj["etag"]
        SubElement(c, "Size").text = str(obj["size"])
        SubElement(c, "StorageClass").text = obj.get("storage_class") or "STANDARD"
        owner = SubElement(c, "Owner")
        SubElement(owner, "ID").text = _canonical_owner_id()
        SubElement(owner, "DisplayName").text = "ministack"

    for cp in sorted(common_prefixes):
        cpe = SubElement(root, "CommonPrefixes")
        SubElement(cpe, "Prefix").text = _url_encode(cp) if encode else cp

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _list_objects_v2(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    prefix = _qp(query_params, "prefix", "")
    delimiter = _qp(query_params, "delimiter", "")
    max_keys = int(_qp(query_params, "max-keys", "1000"))
    continuation = _qp(query_params, "continuation-token", "")
    start_after = _qp(query_params, "start-after", "")
    fetch_owner = _qp(query_params, "fetch-owner", "").lower() == "true"
    encoding_type = _qp(query_params, "encoding-type", "")
    encode = encoding_type == "url"

    if continuation:
        try:
            effective_start = base64.b64decode(continuation).decode("utf-8")
        except Exception:
            effective_start = continuation
    else:
        effective_start = start_after

    contents, common_prefixes, is_truncated, next_marker = _collect_list_entries(
        bucket["objects"],
        prefix,
        delimiter,
        max_keys,
        effective_start,
    )

    root = Element("ListBucketResult", xmlns=S3_NS)
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = (
        _url_encode(prefix) if encode and prefix else prefix
    )
    if delimiter:
        SubElement(root, "Delimiter").text = (
            _url_encode(delimiter) if encode else delimiter
        )
    if encoding_type:
        SubElement(root, "EncodingType").text = encoding_type
    SubElement(root, "MaxKeys").text = str(max_keys)
    SubElement(root, "KeyCount").text = str(len(contents) + len(common_prefixes))
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    if continuation:
        SubElement(root, "ContinuationToken").text = continuation
    if start_after:
        SubElement(root, "StartAfter").text = (
            _url_encode(start_after) if encode else start_after
        )

    if is_truncated and next_marker:
        token = base64.b64encode(next_marker.encode("utf-8")).decode("utf-8")
        SubElement(root, "NextContinuationToken").text = token

    for k in contents:
        obj = bucket["objects"][k]
        c = SubElement(root, "Contents")
        SubElement(c, "Key").text = _url_encode(k) if encode else k
        SubElement(c, "LastModified").text = obj["last_modified"]
        SubElement(c, "ETag").text = obj["etag"]
        SubElement(c, "Size").text = str(obj["size"])
        SubElement(c, "StorageClass").text = obj.get("storage_class") or "STANDARD"
        if fetch_owner:
            owner = SubElement(c, "Owner")
            SubElement(owner, "ID").text = _canonical_owner_id()
            SubElement(owner, "DisplayName").text = "ministack"

    for cp in sorted(common_prefixes):
        cpe = SubElement(root, "CommonPrefixes")
        SubElement(cpe, "Prefix").text = _url_encode(cp) if encode else cp

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


# ---------------------------------------------------------------------------
# Batch delete
# ---------------------------------------------------------------------------


def _delete_objects(bucket_name: str, body: bytes, headers: dict = None):
    headers = headers or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    try:
        xml_root = fromstring(body)
    except Exception:
        return _error("MalformedXML", "The XML you provided was not well-formed", 400)

    quiet = False
    quiet_el = _find_xml_tag(xml_root, "Quiet")
    if quiet_el is not None and quiet_el.text and quiet_el.text.lower() == "true":
        quiet = True

    deleted: list[dict] = []
    errors: list[dict] = []
    for obj_el in list(xml_root.findall("{%s}Object" % S3_NS)) or list(
        xml_root.findall("Object")
    ):
        key_el = _find_xml_tag(obj_el, "Key")
        if key_el is None or not key_el.text:
            continue
        k = key_el.text
        vid_el = _find_xml_tag(obj_el, "VersionId")
        version_id = vid_el.text if (vid_el is not None and vid_el.text) else ""

        if k in bucket["objects"]:
            lock_err = _check_object_lock(bucket_name, k, headers)
            if lock_err:
                errors.append({
                    "key": k,
                    "version_id": version_id,
                    "code": "AccessDenied",
                    "msg": "Access Denied because object protected by object lock.",
                })
                continue

        # Conditional delete: an ObjectIdentifier carrying an ETag deletes only if it
        # matches the current object, and `*` asks only that a current object exist.
        # The condition is evaluated against the current version alone, so a key
        # hidden by a delete marker does not exist for this purpose. A failed
        # condition is reported under Error, not Deleted, and the object survives:
        # "If the precondition fails then the response for that object will be
        # captured in the <Error> element", and "If the object doesn't exist when
        # evaluating either of the preconditions, S3 rejects the request and returns
        # a Not Found error response."
        etag_el = _find_xml_tag(obj_el, "ETag")
        cond_etag = etag_el.text if (etag_el is not None and etag_el.text) else ""
        if cond_etag:
            _cur = bucket["objects"].get(k)
            if _cur is None:
                errors.append({
                    "key": k,
                    "version_id": version_id,
                    "code": "NoSuchKey",
                    "msg": "The specified key does not exist.",
                })
                continue
            if cond_etag != "*" and cond_etag.strip('"') != _cur["etag"].strip('"'):
                errors.append({
                    "key": k,
                    "version_id": version_id,
                    "code": "PreconditionFailed",
                    "msg": "At least one of the preconditions you specified did not hold.",
                })
                continue

        if version_id:
            # Explicit VersionId → permanently purge that exact version/marker.
            # S3 reports the delete as successful even if the version was absent.
            _found, was_marker = _delete_object_version(
                bucket, bucket_name, k, version_id
            )
            deleted.append({"key": k, "version_id": version_id, "was_marker": was_marker})
        elif _bucket_versioning.get(bucket_name) in ("Enabled", "Suspended"):
            # No VersionId on a versioned bucket: create a delete marker —
            # even for a key that never existed — exactly as the single-object
            # DELETE does, and report it on the Deleted entry.
            marker_id = _record_delete_marker(bucket_name, k, bucket["objects"].get(k))
            bucket["objects"].pop(k, None)
            deleted.append({"key": k, "version_id": "", "was_marker": False,
                            "marker_created": marker_id})
        else:
            # No VersionId → plain delete of the current object.
            bucket["objects"].pop(k, None)
            _object_tags.pop((bucket_name, k, None), None)
            _object_retention.pop((bucket_name, k), None)
            _object_legal_hold.pop((bucket_name, k), None)
            _object_acl.pop((bucket_name, k, None), None)
            _delete_persisted_object(bucket_name, k)
            deleted.append({"key": k, "version_id": "", "was_marker": False})

    resp = Element("DeleteResult", xmlns=S3_NS)
    if not quiet:
        for d in deleted:
            el = SubElement(resp, "Deleted")
            SubElement(el, "Key").text = d["key"]
            if d["version_id"]:
                SubElement(el, "VersionId").text = d["version_id"]
                # AWS echoes the delete-marker flag when the purged entry was one.
                if d["was_marker"]:
                    SubElement(el, "DeleteMarker").text = "true"
                    SubElement(el, "DeleteMarkerVersionId").text = d["version_id"]
            elif d.get("marker_created"):
                # A versionless delete on a versioned bucket minted a marker.
                SubElement(el, "DeleteMarker").text = "true"
                SubElement(el, "DeleteMarkerVersionId").text = d["marker_created"]
    for e in errors:
        el = SubElement(resp, "Error")
        SubElement(el, "Key").text = e["key"]
        if e["version_id"]:
            SubElement(el, "VersionId").text = e["version_id"]
        SubElement(el, "Code").text = e["code"]
        SubElement(el, "Message").text = e["msg"]

    return 200, {"Content-Type": "application/xml"}, _xml_body(resp)


# ---------------------------------------------------------------------------
# Multipart upload
# ---------------------------------------------------------------------------


def _create_multipart_upload(bucket_name: str, key: str, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    storage_class, sc_err = _resolve_storage_class(headers)
    if sc_err:
        return sc_err

    sse_headers, sse_err = _resolve_sse_write_headers(headers, bucket_name)
    if sse_err:
        return sse_err

    checksum_algorithm, csum_err = _resolve_multipart_checksum_algorithm(headers)
    if csum_err:
        return csum_err

    upload_id = new_uuid()
    content_type = headers.get("content-type", "application/octet-stream")
    content_encoding = headers.get("content-encoding")
    metadata = _extract_user_metadata(headers)
    preserved = {}
    for h in _PRESERVED_HEADERS:
        val = headers.get(h)
        if val is not None:
            preserved[h] = val
    preserved.update(sse_headers)

    _multipart_uploads[upload_id] = {
        "bucket": bucket_name,
        "key": key,
        "parts": {},
        "metadata": metadata,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "preserved_headers": preserved,
        "storage_class": storage_class,
        "checksum_algorithm": checksum_algorithm,
        "created": now_iso(),
    }

    root = Element("InitiateMultipartUploadResult", xmlns=S3_NS)
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id
    # AWS echoes the upload's encryption on the initiate response, and the
    # checksum algorithm the parts are expected to carry.
    csum_headers = {}
    if checksum_algorithm:
        csum_headers["x-amz-checksum-algorithm"] = checksum_algorithm
        csum_headers["x-amz-checksum-type"] = "COMPOSITE"
    return (200, {"Content-Type": "application/xml", **sse_headers,
                  **csum_headers}, _xml_body(root))


def _upload_part(
    bucket_name: str, key: str, body: bytes, query_params: dict, headers: dict
):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    upload_id = _qp(query_params, "uploadId")
    part_number = _qp(query_params, "partNumber")

    if upload_id not in _multipart_uploads:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    upload = _multipart_uploads[upload_id]
    if upload["bucket"] != bucket_name or upload["key"] != key:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    sse_part_err = _check_sse_c_part(headers, upload)
    if sse_part_err is not None:
        return sse_part_err

    try:
        pn = int(part_number)
    except (ValueError, TypeError):
        return _error(
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive.",
            400,
        )
    if pn < 1 or pn > 10000:
        return _error(
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive.",
            400,
        )

    md5_err = _validate_content_md5(headers, body)
    if md5_err:
        return md5_err

    part_checksums, csum_err = _resolve_part_checksums(body, headers, upload)
    if csum_err:
        return csum_err

    etag = f'"{md5_hash(body)}"'
    upload["parts"][pn] = {
        "body": body,
        "etag": etag,
        "size": len(body),
        "last_modified": now_iso(),
        "checksums": part_checksums,
    }
    # AWS returns the part's checksum so the caller can name it again on the
    # completion, which is where the composite is built from.
    csum_headers = {f"x-amz-checksum-{alg.lower()}": val
                    for alg, val in part_checksums.items()}
    return (200, {"ETag": etag, **_stored_sse_headers(upload), **csum_headers},
            b"")


def _upload_part_copy(bucket_name: str, dest_key: str, query_params: dict, headers: dict):
    """UploadPartCopy — copy a range from an existing object as a multipart part."""
    upload_id = _qp(query_params, "uploadId")
    part_number = int(_qp(query_params, "partNumber", "1"))

    if upload_id not in _multipart_uploads:
        return _error("NoSuchUpload", "The specified multipart upload does not exist.", 404)

    sse_part_err = _check_sse_c_part(headers, _multipart_uploads[upload_id])
    if sse_part_err is not None:
        return sse_part_err

    # Split the raw header at "?" before percent-decoding, exactly as
    # CopyObject does: a key containing "?versionId" arrives encoded and must
    # stay part of the key, while a real versionId qualifier is a bare "?".
    raw_source = headers.get("x-amz-copy-source", "").lstrip("/")
    raw_path, _, src_query = raw_source.partition("?")
    src_parts = url_unquote(raw_path).split("/", 1)
    if len(src_parts) < 2:
        return _error("InvalidArgument", "Copy Source must mention the source bucket and key", 400)

    src_bucket_name, src_key = src_parts
    src_bucket = _ensure_bucket(src_bucket_name)
    if src_bucket is None:
        return _no_such_bucket(src_bucket_name)

    # A `?versionId=` on the copy source selects the exact version to copy the
    # range from, not the current object.
    src_version_id = None
    if src_query:
        src_version_id = _parse_qs(src_query, keep_blank_values=True).get(
            "versionId", [None]
        )[0]

    if src_version_id:
        ventry = next(
            (v for v in _object_versions.get((src_bucket_name, src_key), [])
             if v["version_id"] == src_version_id),
            None,
        )
        if ventry is None or ventry.get("is_delete_marker"):
            return _error(
                "NoSuchVersion",
                "The specified version does not exist.",
                404,
                f"/{src_bucket_name}/{src_key}",
            )
        src_body = ventry.get("data") or b""
    else:
        if src_key not in src_bucket["objects"]:
            return _error("NoSuchKey", "The specified key does not exist.", 404)
        src_obj = src_bucket["objects"][src_key]
        src_version_id = src_obj.get("version_id")
        src_body = _read_body(src_bucket_name, src_key, src_obj)
        ventry = src_obj

    # An SSE-C source must be addressed with its create-time key, whichever
    # version the range is copied from.
    sse_src_err = _check_sse_c_copy_source(headers, ventry)
    if sse_src_err is not None:
        return sse_src_err

    # Handle x-amz-copy-source-range
    copy_range = headers.get("x-amz-copy-source-range", "")
    if copy_range:
        _malformed = _error(
            "InvalidArgument",
            "The x-amz-copy-source-range value must be of the form "
            "bytes=first-last where first and last are the zero-based offsets "
            "of the first and last bytes to copy",
            400,
        )
        if not copy_range.startswith("bytes=") or "," in copy_range:
            return _malformed
        rng = copy_range[len("bytes="):]
        parts = rng.split("-")
        if len(parts) != 2 or parts[0] == "" or parts[1] == "":
            return _malformed
        try:
            start, end = int(parts[0]), int(parts[1])
        except ValueError:
            return _malformed
        if start < 0 or end < start:
            return _malformed
        object_size = len(src_body)
        if start > object_size - 1 or end > object_size - 1:
            return _error(
                "InvalidArgument",
                f"Range specified is not valid for source object of size: {object_size}",
                400,
            )
        src_body = src_body[start:end + 1]

    etag = f'"{md5_hash(src_body)}"'
    _multipart_uploads[upload_id]["parts"][part_number] = {
        "body": src_body,
        "etag": etag,
        "size": len(src_body),
        "last_modified": now_iso(),
    }

    root = Element("CopyPartResult", xmlns=S3_NS)
    SubElement(root, "ETag").text = etag
    SubElement(root, "LastModified").text = now_iso()
    resp_headers = {"Content-Type": "application/xml",
                    **_stored_sse_headers(_multipart_uploads[upload_id])}
    if src_version_id:
        # AWS echoes the copied source version on a versioned source.
        resp_headers["x-amz-copy-source-version-id"] = src_version_id
    return 200, resp_headers, _xml_body(root)


def _complete_multipart_upload(
    bucket_name: str, key: str, body: bytes, query_params: dict, headers: dict | None = None
):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    upload_id = _qp(query_params, "uploadId")
    # Idempotent retry: if the upload already completed, replay its response
    # instead of returning NoSuchUpload (real S3 stays 200 until the id is GC'd).
    cached = _completed_multipart_uploads.get(upload_id)
    if cached is not None and cached.get("key") == key:
        return cached["response"]
    if upload_id not in _multipart_uploads:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    upload = _multipart_uploads[upload_id]
    if upload["bucket"] != bucket_name or upload["key"] != key:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    # An SSE-C upload's completion must present the create-time key again, as
    # every part did; a plain upload's completion must present none. (The
    # idempotent replay above deliberately skips this — the original request
    # already presented the key when it committed.)
    sse_part_err = _check_sse_c_part(headers or {}, upload)
    if sse_part_err is not None:
        return sse_part_err

    # CompleteMultipartUpload takes the same If-Match / If-None-Match
    # preconditions PutObject does, evaluated against the current object at
    # complete time.  (The idempotent replay above deliberately skips them —
    # the original request already passed its conditions when it committed.)
    precondition_err = _check_put_preconditions(
        headers or {}, bucket.get("objects", {}).get(key))
    if precondition_err:
        return precondition_err

    try:
        xml_root = fromstring(body)
    except ParseError:
        return _error(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate "
            "against our published schema.",
            400,
            f"/{bucket_name}/{key}",
        )
    ordered_parts: list[tuple[int, str | None, dict]] = []
    for part_el in xml_root.iter():
        local = part_el.tag.split("}")[-1] if "}" in part_el.tag else part_el.tag
        if local == "Part":
            pn_text = etag_text = None
            part_checksums = {}
            for child in part_el:
                child_local = (
                    child.tag.split("}")[-1] if "}" in child.tag else child.tag
                )
                if child_local == "PartNumber":
                    pn_text = child.text
                elif child_local == "ETag":
                    etag_text = child.text
                elif child_local.startswith("Checksum"):
                    part_checksums[child_local[len("Checksum"):].upper()] = (
                        child.text)
            if pn_text is not None:
                ordered_parts.append((int(pn_text), etag_text, part_checksums))

    ordered_parts.sort(key=lambda x: x[0])

    md5_digests = b""
    combined = b""
    part_records = []
    checksum_algorithm = upload.get("checksum_algorithm")
    part_digests = []
    for pn, req_etag, req_checksums in ordered_parts:
        if pn not in upload["parts"]:
            return _error(
                "InvalidPart",
                "One or more of the specified parts could not be found.",
                400,
            )
        stored = upload["parts"][pn]
        if req_etag and req_etag.strip('"') != stored["etag"].strip('"'):
            return _error(
                "InvalidPart",
                "One or more of the specified parts could not be found. "
                "The following part numbers are invalid: " + str(pn),
                400,
            )
        # A part checksum named here must be the one the part was stored
        # with: the composite is built from what ministack holds, so a caller
        # naming a different value would get a composite it cannot reproduce.
        stored_checksums = stored.get("checksums") or {}
        for alg, value in req_checksums.items():
            if value and stored_checksums.get(alg) != value:
                return _error(
                    "InvalidPart",
                    "One or more of the specified parts could not be found. "
                    "The following part numbers are invalid: " + str(pn),
                    400,
                )
        if checksum_algorithm:
            part_digests.append(stored_checksums.get(checksum_algorithm))
        md5_digests += hashlib.md5(stored["body"]).digest()
        combined += stored["body"]
        # Retained so GetObjectAttributes can report ObjectParts (ListParts
        # functionality) for the completed multipart object.
        part_records.append({"PartNumber": pn, "Size": len(stored["body"])})

    composite = _composite_checksum(checksum_algorithm, part_digests)
    if checksum_algorithm:
        requested = (headers or {}).get(
            f"x-amz-checksum-{checksum_algorithm.lower()}")
        if requested and requested != composite:
            return _error(
                "BadDigest",
                f"The {checksum_algorithm} you specified did not match the "
                f"calculated checksum.",
                400,
                f"/{bucket_name}/{key}",
            )

    final_md5 = hashlib.md5(md5_digests).hexdigest()
    final_etag = f'"{final_md5}-{len(ordered_parts)}"'

    obj = {
        "body": combined,
        "content_type": upload["content_type"],
        "content_encoding": upload.get("content_encoding"),
        "etag": final_etag,
        "last_modified": now_iso(),
        "size": len(combined),
        "metadata": upload["metadata"],
        "preserved_headers": upload.get("preserved_headers", {}),
        "storage_class": upload.get("storage_class") or "STANDARD",
        "parts": part_records,
        "checksums": {checksum_algorithm: composite} if composite else {},
        "checksum_type": "COMPOSITE" if composite else None,
    }
    prior_obj = bucket["objects"].get(key)
    bucket["objects"][key] = obj

    del _multipart_uploads[upload_id]

    _fire_s3_event_async(
        bucket_name,
        key,
        "s3:ObjectCreated:CompleteMultipartUpload",
        size=obj["size"],
        etag=final_etag,
    )

    resp_headers = {"Content-Type": "application/xml"}
    resp_headers.update(_stored_sse_headers(obj))
    version_id = _record_object_version(bucket_name, key, prior_obj, obj, combined)
    if version_id:
        resp_headers["x-amz-version-id"] = version_id

    # Persist only after the versioning block: the .meta.json sidecar must
    # carry the version_id assigned above (#1058).
    if S3_PERSIST:
        _persist_object(bucket_name, key, obj)

    root = Element("CompleteMultipartUploadResult", xmlns=S3_NS)
    # Location echoes the endpoint the client actually reached (real S3 returns the
    # request host), so it is correct on any port/host — not a hard-coded 4566. Falls
    # back to the configured host only when the request carried no Host header. (#1322)
    request_host = (headers or {}).get("host")
    if request_host:
        scheme = (headers or {}).get("x-forwarded-proto") or "http"
        s3_host = f"{scheme}://{request_host}"
    else:
        s3_host = os.environ.get("MINISTACK_HOST", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"))
    SubElement(root, "Location").text = f"{s3_host}/{bucket_name}/{key}"
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "Key").text = key
    SubElement(root, "ETag").text = final_etag
    if composite:
        SubElement(
            root, f"Checksum{checksum_algorithm}").text = composite
        SubElement(root, "ChecksumType").text = "COMPOSITE"
    response = (200, resp_headers, _xml_body(root))
    # Retain the response so an idempotent retry (same upload id) replays it
    # rather than 404ing; the versioning block above is NOT re-run on replay,
    # so a retry never mints a second object version.
    _completed_multipart_uploads[upload_id] = {"key": key, "response": response}
    return response


def _abort_multipart_upload(bucket_name: str, key: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    upload_id = _qp(query_params, "uploadId")
    if upload_id not in _multipart_uploads:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    upload = _multipart_uploads[upload_id]
    if upload["bucket"] != bucket_name or upload["key"] != key:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    del _multipart_uploads[upload_id]
    return 204, {}, b""


def _list_multipart_uploads(bucket_name: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    prefix = _qp(query_params, "prefix", "")
    delimiter = _qp(query_params, "delimiter", "")
    max_uploads = int(_qp(query_params, "max-uploads", "1000"))
    key_marker = _qp(query_params, "key-marker", "")
    upload_id_marker = _qp(query_params, "upload-id-marker", "")

    root = Element("ListMultipartUploadsResult", xmlns=S3_NS)
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "KeyMarker").text = key_marker
    SubElement(root, "UploadIdMarker").text = upload_id_marker
    SubElement(root, "MaxUploads").text = str(max_uploads)
    if prefix:
        SubElement(root, "Prefix").text = prefix
    if delimiter:
        SubElement(root, "Delimiter").text = delimiter

    uploads = []
    for uid, upload in _multipart_uploads.items():
        if upload["bucket"] != bucket_name:
            continue
        if prefix and not upload["key"].startswith(prefix):
            continue
        if key_marker and upload["key"] < key_marker:
            continue
        if (
            key_marker
            and upload["key"] == key_marker
            and upload_id_marker
            and uid <= upload_id_marker
        ):
            continue
        uploads.append((uid, upload))

    uploads.sort(key=lambda x: (x[1]["key"], x[0]))

    is_truncated = len(uploads) > max_uploads
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    for uid, upload in uploads[:max_uploads]:
        u = SubElement(root, "Upload")
        SubElement(u, "Key").text = upload["key"]
        SubElement(u, "UploadId").text = uid
        initiator = SubElement(u, "Initiator")
        SubElement(initiator, "ID").text = get_account_id()
        SubElement(initiator, "DisplayName").text = "ministack"
        owner = SubElement(u, "Owner")
        SubElement(owner, "ID").text = _canonical_owner_id()
        SubElement(owner, "DisplayName").text = "ministack"
        SubElement(u, "StorageClass").text = upload.get("storage_class") or "STANDARD"
        SubElement(u, "Initiated").text = upload["created"]

    if is_truncated and uploads:
        last = uploads[max_uploads - 1]
        SubElement(root, "NextKeyMarker").text = last[1]["key"]
        SubElement(root, "NextUploadIdMarker").text = last[0]

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _list_parts(bucket_name: str, key: str, query_params: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    upload_id = _qp(query_params, "uploadId")
    if upload_id not in _multipart_uploads:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    upload = _multipart_uploads[upload_id]
    if upload["bucket"] != bucket_name or upload["key"] != key:
        return _error(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    max_parts = int(_qp(query_params, "max-parts", "1000"))
    part_marker = int(_qp(query_params, "part-number-marker", "0"))

    root = Element("ListPartsResult", xmlns=S3_NS)
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id

    initiator = SubElement(root, "Initiator")
    SubElement(initiator, "ID").text = get_account_id()
    SubElement(initiator, "DisplayName").text = "ministack"
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = _canonical_owner_id()
    SubElement(owner, "DisplayName").text = "ministack"
    SubElement(root, "StorageClass").text = upload.get("storage_class") or "STANDARD"
    SubElement(root, "PartNumberMarker").text = str(part_marker)
    SubElement(root, "MaxParts").text = str(max_parts)

    sorted_parts = sorted(pn for pn in upload["parts"] if pn > part_marker)
    is_truncated = len(sorted_parts) > max_parts
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    for pn in sorted_parts[:max_parts]:
        part = upload["parts"][pn]
        p = SubElement(root, "Part")
        SubElement(p, "PartNumber").text = str(pn)
        SubElement(p, "LastModified").text = part.get("last_modified", now_iso())
        SubElement(p, "ETag").text = part["etag"]
        SubElement(p, "Size").text = str(part["size"])
        for alg, value in (part.get("checksums") or {}).items():
            SubElement(p, f"Checksum{alg}").text = value

    if is_truncated and sorted_parts:
        SubElement(root, "NextPartNumberMarker").text = str(sorted_parts[max_parts - 1])

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _object_disk_path(bucket: str, key: str, account_id: str = None) -> str | None:
    """Resolve the on-disk path for an object and verify it lives under DATA_DIR.

    Returns None if the resolved path escapes DATA_DIR (e.g. key contains `..`
    or absolute path). Callers must treat None as "skip operation".
    """
    if account_id is None:
        account_id = get_account_id()
    root = os.path.realpath(DATA_DIR)
    candidate = os.path.realpath(os.path.join(DATA_DIR, account_id, bucket, key))
    try:
        if os.path.commonpath([root, candidate]) != root:
            logger.warning("S3 persist: path traversal blocked for %s/%s", bucket, key)
            return None
    except ValueError:
        # commonpath raises on mixed drives / unrelated paths — treat as escape.
        logger.warning("S3 persist: path traversal blocked for %s/%s", bucket, key)
        return None
    return candidate


def _atomic_write(fpath: str, data: bytes, *, text: bool = False):
    """Write `data` to `fpath` atomically with mode 0o600."""
    tmp = fpath + ".tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "w" if text else "wb") as f:
            f.write(data)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    os.replace(tmp, fpath)


def _persist_object(bucket: str, key: str, obj):
    try:
        fpath = _object_disk_path(bucket, key)
        if fpath is None:
            return
        os.makedirs(os.path.dirname(fpath), mode=0o700, exist_ok=True)
        data = obj["body"] if isinstance(obj, dict) else obj
        _atomic_write(fpath, data)
        if isinstance(obj, dict):
            meta = {
                "content_type": obj.get("content_type", "application/octet-stream"),
                "content_encoding": obj.get("content_encoding"),
                "etag": obj.get("etag", ""),
                "last_modified": obj.get("last_modified", ""),
                "size": obj.get("size", 0),
                "metadata": obj.get("metadata", {}),
                "preserved_headers": obj.get("preserved_headers", {}),
                "storage_class": obj.get("storage_class", "STANDARD"),
                "checksums": obj.get("checksums", {}),
                "version_id": obj.get("version_id"),
            }
            _atomic_write(fpath + ".meta.json", json.dumps(meta), text=True)
        # Drop body from in-memory record to save RAM
        if isinstance(obj, dict):
            obj["body"] = None
    except Exception as e:
        logger.warning("Failed to persist S3 object %s/%s: %s", bucket, key, e)


def _read_body(bucket_name: str, key: str, obj: dict) -> bytes:
    """Return object body — from memory if available, else from disk."""
    body = obj.get("body")
    if body is not None:
        return body
    if not S3_PERSIST:
        return b""
    try:
        fpath = _object_disk_path(bucket_name, key)
        if fpath is None:
            return b""
        with open(fpath, "rb") as f:
            return f.read()
    except Exception as e:
        logger.warning("Failed to read persisted S3 object %s/%s: %s", bucket_name, key, e)
        return b""


def _delete_persisted_object(bucket_name: str, key: str):
    """Remove an object's data and metadata files from disk."""
    if not S3_PERSIST:
        return
    try:
        fpath = _object_disk_path(bucket_name, key)
        if fpath is None:
            return
        if os.path.exists(fpath):
            os.remove(fpath)
        meta_path = fpath + ".meta.json"
        if os.path.exists(meta_path):
            os.remove(meta_path)
    except Exception as e:
        logger.warning("Failed to delete persisted S3 object %s/%s: %s", bucket_name, key, e)


def _delete_persisted_bucket(name: str):
    """Remove a bucket's account-scoped on-disk directory when the bucket is deleted.

    Mirrors the account scoping in _object_disk_path so we clean up exactly the
    directory _create_bucket / _persist_object create. Without this the bucket
    folder is orphaned on disk after DeleteBucket (#824).

    Only the new account-scoped layout (DATA_DIR/<account>/<bucket>) is removed;
    legacy unscoped data (DATA_DIR/<bucket>) from pre-account-scoping versions is
    left in place — same as _delete_persisted_object, which only touches the
    account-scoped path."""
    if not S3_PERSIST or not name:
        return
    try:
        account_id = get_account_id()
        root = os.path.realpath(DATA_DIR)
        account_root = os.path.realpath(os.path.join(DATA_DIR, account_id))
        bucket_dir = os.path.realpath(os.path.join(DATA_DIR, account_id, name))
        # Never remove DATA_DIR, the account directory itself, or anything that
        # escapes DATA_DIR — only the one bucket's subtree. rmtree is destructive,
        # so guard the primitive rather than relying solely on the validated caller.
        if (
            bucket_dir in (root, account_root)
            or os.path.commonpath([root, bucket_dir]) != root
        ):
            logger.warning("S3 persist: refusing to delete bucket dir %s (outside its bucket scope)", name)
            return
        if os.path.isdir(bucket_dir):
            # No ignore_errors: let a partial-failure OSError reach the except
            # below so cleanup failures are logged instead of silently leaking.
            shutil.rmtree(bucket_dir)
    except (ValueError, OSError) as e:
        logger.warning("Failed to delete persisted S3 bucket dir %s: %s", name, e)


def _load_persisted_data():
    if not S3_PERSIST or not os.path.isdir(DATA_DIR):
        return
    try:
        # Support both layouts:
        #   New: DATA_DIR/<account_id>/<bucket>/<key>
        #   Legacy: DATA_DIR/<bucket>/<key>
        for entry in os.listdir(DATA_DIR):
            entry_path = os.path.join(DATA_DIR, entry)
            if not os.path.isdir(entry_path):
                continue
            # Detect if this entry is an account ID directory (12-digit or has bucket subdirs)
            if entry.isdigit() and len(entry) == 12:
                # New layout: entry is an account ID
                _load_persisted_account(entry, entry_path)
            else:
                # Legacy layout: entry is a bucket name under default account
                _load_persisted_bucket("000000000000", entry, entry_path)
        logger.info("Loaded persisted S3 data from %s", DATA_DIR)
    except Exception as e:
        logger.warning("Failed to load persisted S3 data: %s", e)


def _load_persisted_account(account_id, account_path):
    """Load all buckets for a given account from disk."""
    for bucket_name in os.listdir(account_path):
        bucket_path = os.path.join(account_path, bucket_name)
        if os.path.isdir(bucket_path):
            _load_persisted_bucket(account_id, bucket_name, bucket_path)


def _load_persisted_bucket(account_id, bucket_name, bucket_path):
    """Load a single bucket's objects from disk into the correct account scope."""
    # Skip empty directories (may be leftover from layout migration)
    has_files = any(
        not f.endswith(".meta.json") for _, _, files in os.walk(bucket_path) for f in files
    )
    if not has_files and not os.listdir(bucket_path):
        return
    scoped_key = (account_id, bucket_name)
    if scoped_key not in _buckets._data:
        _buckets._data[scoped_key] = {
            "created": now_iso(),
            "objects": {},
            "region": None,
        }
    bucket = _buckets._data[scoped_key]
    for dirpath, _dirnames, filenames in os.walk(bucket_path):
        for fname in filenames:
            if fname.endswith(".meta.json"):
                continue
            abs_path = os.path.join(dirpath, fname)
            key = os.path.relpath(abs_path, bucket_path)
            meta_path = abs_path + ".meta.json"
            meta = {}
            if os.path.exists(meta_path):
                try:
                    with open(meta_path) as mf:
                        meta = json.load(mf)
                except Exception:
                    pass
            # Body stays on disk — only load metadata into memory.
            # Compute size/etag from meta sidecar; fall back to reading
            # the file only when the sidecar is missing or incomplete.
            size = meta.get("size")
            etag = meta.get("etag")
            if size is None or not etag:
                with open(abs_path, "rb") as f:
                    data = f.read()
                size = len(data)
                etag = etag or f'"{md5_hash(data)}"'
            bucket["objects"][key] = {
                "body": None,
                "content_type": meta.get("content_type", "application/octet-stream"),
                "content_encoding": meta.get("content_encoding"),
                "etag": etag,
                "last_modified": meta.get("last_modified") or now_iso(),
                "size": size,
                "metadata": meta.get("metadata", {}),
                "preserved_headers": meta.get("preserved_headers", {}),
                "storage_class": meta.get("storage_class", "STANDARD"),
                "checksums": meta.get("checksums", {}),
            }
            if meta.get("version_id"):
                bucket["objects"][key]["version_id"] = meta["version_id"]
                vkey = (bucket_name, key)
                scoped_vkey = (account_id, vkey)
                if scoped_vkey not in _object_versions._data:
                    _object_versions._data[scoped_vkey] = []
                _object_versions._data[scoped_vkey].append({
                    "version_id": meta["version_id"],
                    "last_modified": meta.get("last_modified") or now_iso(),
                    "etag": etag,
                    "size": size,
                    "is_latest": True,
                    "data": None,
                    "content_type": meta.get("content_type", "application/octet-stream"),
                    "content_encoding": meta.get("content_encoding"),
                    "metadata": meta.get("metadata", {}),
                    "preserved_headers": meta.get("preserved_headers", {}),
                    "storage_class": meta.get("storage_class", "STANDARD"),
                    "checksums": meta.get("checksums", {}),
                })


_load_persisted_data()


def reset():
    """Wipe all in-memory state (used by /_ministack/reset)."""
    global _buckets, _bucket_policies, _bucket_notifications, _bucket_tags
    global _bucket_versioning, _bucket_encryption, _bucket_lifecycle, _bucket_cors
    global _bucket_acl, _bucket_websites, _bucket_logging_config
    global _bucket_accelerate_config, _bucket_request_payment_config
    global _object_tags, _multipart_uploads, _object_versions, _object_acl
    global \
        _bucket_object_lock, \
        _bucket_replication, \
        _object_retention, \
        _object_legal_hold
    for d in (
        _buckets,
        _bucket_policies,
        _bucket_notifications,
        _bucket_tags,
        _bucket_versioning,
        _bucket_encryption,
        _bucket_lifecycle,
        _bucket_cors,
        _bucket_acl,
        _bucket_websites,
        _bucket_logging_config,
        _bucket_accelerate_config,
        _bucket_request_payment_config,
        _object_tags,
        _object_acl,
        _multipart_uploads,
        _completed_multipart_uploads,
        _bucket_object_lock,
        _bucket_replication,
        _object_retention,
        _object_legal_hold,
        _object_versions,
    ):
        d.clear()


# Re-arm restore-completion timers for state loaded from disk. Must run after
# every function above is defined: the workers resolve _fire_s3_event by name.
_reschedule_restores()
